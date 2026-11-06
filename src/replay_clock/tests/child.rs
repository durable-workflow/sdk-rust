use super::*;
use crate::{ChildWorkflowFailureKind, ChildWorkflowOptions};

const CHILD_TIME: &str = "2026-01-01T00:00:05.654321Z";

fn child(sequence: u64, kind: &str, time: &str) -> Vec<HistoryEvent> {
    let mut payload = json!({
        "sequence":sequence,
        "child_call_id":format!("call-{sequence}"),
        "child_workflow_instance_id":format!("child-{sequence}"),
        "child_workflow_run_id":format!("run-{sequence}"),
        "child_workflow_type":"clock-child",
    });
    let schedule = event("ChildWorkflowScheduled", payload.clone(), START);
    let started = event("ChildRunStarted", payload.clone(), START);
    if kind == "ChildRunCompleted" {
        payload["result"] =
            encode_typed_envelope(&AvroValue::String("child-result".into()), DEFAULT_CODEC)
                .unwrap();
    } else {
        payload["message"] = json!("child failure");
    }
    // Core's scalar child branches use the parent resolution record, not the
    // child's close time or the reused-time rule used for successful activities.
    payload["closed_at"] = json!("2098-01-01T00:00:00Z");
    payload["reused_recorded_at"] = json!("2099-01-01T00:00:00Z");
    vec![schedule, started, event(kind, payload, time)]
}

#[test]
fn replay_now_child_completion_and_caught_failure_cold_replay_exact_times() {
    for failed_time in ["2025-12-31T23:59:59.123456Z", CHILD_TIME] {
        let mut history = vec![start()];
        history.extend(child(1, "ChildRunCompleted", CHILD_TIME));
        history.extend(child(2, "ChildRunFailed", failed_time));
        let execute = || {
            let ctx = context(history.clone());
            let inner = ctx.clone();
            let result = ready(
                &ctx,
                Box::pin(async move {
                    assert_eq!(read(&inner), START);
                    let result = inner
                        .start_child_workflow(
                            "clock-child",
                            ChildWorkflowOptions::new("children"),
                            json!([]),
                        )
                        .await?;
                    assert_eq!(result.child.run_id.as_deref(), Some("run-1"));
                    assert_eq!(result.result, json!("child-result"));
                    let completed = read(&inner);
                    assert_eq!(completed, read(&inner));
                    assert_eq!(completed, CHILD_TIME);
                    let Err(Error::ChildWorkflowFailed(failure)) = inner
                        .start_child_workflow(
                            "clock-child",
                            ChildWorkflowOptions::new("children"),
                            json!([]),
                        )
                        .await
                    else {
                        panic!("expected the recorded child failure");
                    };
                    assert_eq!(failure.kind, ChildWorkflowFailureKind::Failed);
                    assert_eq!(failure.child_workflow_run_id.as_deref(), Some("run-2"));
                    Ok(AvroValue::from_serialize(
                        &json!({"completed":completed,"failed":read(&inner)}),
                    )?)
                }),
            );
            assert_eq!(ctx.state.lock().unwrap().command_cursor, 2);
            assert!(ctx.take_commands().unwrap().is_empty());
            assert!(ctx.replay_now().is_err());
            result
        };
        let first = execute();
        assert_eq!(first, execute());
        assert_eq!(first.into_json().unwrap()["failed"], failed_time);
    }
}

#[test]
fn replay_now_child_terminal_categories_use_their_own_parent_record() {
    for kind in [
        "ChildRunCompleted",
        "ChildRunFailed",
        "ChildRunCancelled",
        "ChildRunTerminated",
    ] {
        let mut history = vec![start()];
        history.extend(child(1, kind, CHILD_TIME));
        let ctx = context(history);
        let inner = ctx.clone();
        ready(
            &ctx,
            Box::pin(async move {
                let outcome = inner
                    .start_child_workflow(
                        "clock-child",
                        ChildWorkflowOptions::new("children"),
                        json!([]),
                    )
                    .await;
                match kind {
                    "ChildRunCompleted" => assert!(outcome.is_ok()),
                    _ => assert!(matches!(outcome, Err(Error::ChildWorkflowFailed(_)))),
                }
                assert_eq!(read(&inner), CHILD_TIME);
                Ok(AvroValue::Null)
            }),
        );
    }
}

#[test]
fn replay_now_child_missing_or_conflicting_identity_preserves_legacy_result_without_time() {
    for field in [
        "child_call_id",
        "child_workflow_instance_id",
        "child_workflow_run_id",
    ] {
        for index in 0..3 {
            for replacement in [
                None,
                Some(json!("foreign-child")),
                Some(json!("")),
                Some(Value::Null),
                Some(json!(17)),
            ] {
                let mut history = child(1, "ChildRunCompleted", CHILD_TIME);
                match replacement {
                    Some(value) => {
                        history[index].payload[field] = value;
                    }
                    None => {
                        history[index]
                            .payload
                            .as_object_mut()
                            .unwrap()
                            .remove(field);
                    }
                }
                let ctx = context(history);
                let inner = ctx.clone();
                ready(
                    &ctx,
                    Box::pin(async move {
                        assert_eq!(
                            inner
                                .start_child_workflow(
                                    "clock-child",
                                    ChildWorkflowOptions::new("children"),
                                    json!([])
                                )
                                .await?
                                .result,
                            json!("child-result")
                        );
                        assert!(matches!(
                            inner.replay_now(),
                            Err(Error::ReplayClockUnavailable)
                        ));
                        Ok(AvroValue::Null)
                    }),
                );
                assert!(ctx.take_commands().unwrap().is_empty());
            }
        }
    }
    let mut missing_type = child(1, "ChildRunCompleted", CHILD_TIME);
    for event in &mut missing_type {
        event
            .payload
            .as_object_mut()
            .unwrap()
            .remove("child_workflow_type");
    }
    let ctx = context(missing_type);
    let inner = ctx.clone();
    ready(
        &ctx,
        Box::pin(async move {
            inner
                .start_child_workflow(
                    "clock-child",
                    ChildWorkflowOptions::new("children"),
                    json!([]),
                )
                .await?;
            assert!(inner.replay_now().is_err());
            Ok(AvroValue::Null)
        }),
    );
}

#[test]
fn replay_now_child_invalid_time_and_duplicate_terminal_do_not_borrow_other_authority() {
    for bad_time in [
        None,
        Some(Value::Null),
        Some(json!("invalid")),
        Some(json!(42)),
    ] {
        let mut history = vec![start()];
        history.extend(child(1, "ChildRunCompleted", CHILD_TIME));
        match bad_time {
            Some(value) => {
                history[3].raw.insert("recorded_at".into(), value);
            }
            None => {
                history[3].raw.remove("recorded_at");
            }
        }
        let ctx = context(history);
        let inner = ctx.clone();
        ready(
            &ctx,
            Box::pin(async move {
                inner
                    .start_child_workflow(
                        "clock-child",
                        ChildWorkflowOptions::new("children"),
                        json!([]),
                    )
                    .await?;
                assert!(inner.replay_now().is_err());
                Ok(AvroValue::Null)
            }),
        );
    }
    let mut history = child(1, "ChildRunCompleted", CHILD_TIME);
    history.remove(1); // Legacy duplicate delivery without a separate start record.
    let mut duplicate = history.last().unwrap().clone();
    duplicate
        .raw
        .insert("recorded_at".into(), json!("2099-01-01T00:00:00Z"));
    history.push(duplicate);
    let ctx = context(history);
    let inner = ctx.clone();
    ready(
        &ctx,
        Box::pin(async move {
            inner
                .start_child_workflow(
                    "clock-child",
                    ChildWorkflowOptions::new("children"),
                    json!([]),
                )
                .await?;
            assert!(inner.replay_now().is_err());
            Ok(AvroValue::Null)
        }),
    );
}

#[test]
fn replay_now_child_future_result_is_not_consumed_and_pending_schedule_has_no_time() {
    let mut history = vec![start()];
    history.extend(child(1, "ChildRunCompleted", CHILD_TIME));
    history.extend(child(2, "ChildRunCompleted", "2099-01-01T00:00:00Z"));
    let ctx = context(history);
    let inner = ctx.clone();
    let mut future: WorkflowFuture = Box::pin(async move {
        inner
            .start_child_workflow(
                "clock-child",
                ChildWorkflowOptions::new("children"),
                json!([]),
            )
            .await?;
        assert_eq!(read(&inner), CHILD_TIME);
        std::future::pending().await
    });
    assert!(Worker::poll_workflow_future(&ctx, &mut future)
        .unwrap()
        .is_pending());
    assert_eq!(ctx.state.lock().unwrap().command_cursor, 1);
    assert!(ctx.replay_now().is_err());
    let mut history = vec![start()];
    history.extend(
        child(1, "ChildRunCompleted", CHILD_TIME)
            .into_iter()
            .take(2),
    );
    let ctx = context(history);
    let inner = ctx.clone();
    let mut future: WorkflowFuture = Box::pin(async move {
        inner
            .start_child_workflow(
                "clock-child",
                ChildWorkflowOptions::new("children"),
                json!([]),
            )
            .await?;
        panic!("a pending child must stay pending");
    });
    assert!(Worker::poll_workflow_future(&ctx, &mut future)
        .unwrap()
        .is_pending());
    let _guard = Guard::enter(&ctx);
    assert_eq!(read(&ctx), START);
    assert!(ctx.take_commands().unwrap().is_empty());
}

#[test]
fn replay_now_child_poll_scope_restores_outer_context_after_failure_and_interleaving() {
    let outer = context(vec![start()]);
    let _outer_guard = Guard::enter(&outer);
    let mut futures = vec![];
    for time in [CHILD_TIME, "2026-01-01T00:00:02.654321Z"] {
        let ctx = context(child(1, "ChildRunCompleted", time));
        let inner = ctx.clone();
        let future: WorkflowFuture = Box::pin(async move {
            inner
                .start_child_workflow(
                    "clock-child",
                    ChildWorkflowOptions::new("children"),
                    json!([]),
                )
                .await?;
            assert_eq!(read(&inner), time);
            tokio::task::yield_now().await;
            assert_eq!(read(&inner), time);
            Err(Error::WorkerLoop("expected workflow failure".into()))
        });
        futures.push((ctx, future));
    }
    for (ctx, future) in &mut futures {
        assert!(Worker::poll_workflow_future(ctx, future)
            .unwrap()
            .is_pending());
        assert!(ctx.replay_now().is_err());
        assert_eq!(read(&outer), START);
    }
    for (ctx, future) in &mut futures {
        assert!(matches!(
            Worker::poll_workflow_future(ctx, future).unwrap(),
            Poll::Ready(Err(Error::WorkerLoop(_)))
        ));
        assert!(ctx.replay_now().is_err());
        assert_eq!(read(&outer), START);
    }
}

#[tokio::test]
async fn replay_now_child_snapshot_and_reconstructed_queries_use_their_actual_prefix() {
    let mut worker = Worker::new(Client::new("http://127.0.0.1:8080").unwrap(), "clock-tests");
    worker.register_replayed_workflow(
        "clock-child-parent",
        || 0_u64,
        |ctx, _args, _state| async move {
            ctx.start_child_workflow(
                "clock-child",
                ChildWorkflowOptions::new("children"),
                json!([]),
            )
            .await?;
            ctx.start_child_workflow(
                "clock-child",
                ChildWorkflowOptions::new("children"),
                json!([]),
            )
            .await?;
            Ok(Value::Null)
        },
    );
    worker.register_query("clock-child-parent", "snapshot", |ctx, _args| async move {
        Ok(json!(ctx.replay_now().ok().map(
            |time| time.to_rfc3339_opts(chrono::SecondsFormat::Micros, true)
        )))
    });
    worker.register_replayed_query::<u64, _, _>(
        "clock-child-parent",
        "reconstructed",
        |ctx, _state, _args| async move {
            Ok(json!(ctx.replay_now().ok().map(
                |time| time.to_rfc3339_opts(chrono::SecondsFormat::Micros, true)
            )))
        },
    );
    let mut history = child(1, "ChildRunCompleted", CHILD_TIME);
    for (query, expected) in [
        ("snapshot", json!(CHILD_TIME)),
        ("reconstructed", json!(CHILD_TIME)),
    ] {
        if query == "reconstructed" {
            history.extend(
                child(2, "ChildRunCompleted", "2099-01-01T00:00:00Z")
                    .into_iter()
                    .take(2),
            );
        }
        let task: QueryTask=serde_json::from_value(json!({"query_task_id":"child-query","workflow_type":"clock-child-parent","query_name":query,"payload_codec":DEFAULT_CODEC,"history_events":history})).unwrap();
        assert_eq!(
            worker
                .execute_query_task(task)
                .await
                .unwrap()
                .into_json()
                .unwrap(),
            expected
        );
    }
    assert!(snapshot_time(&history, DEFAULT_CODEC).is_none());
}

#[test]
fn replay_now_child_lifecycle_order_and_command_correlation_are_required() {
    let mut history = child(1, "ChildRunCompleted", CHILD_TIME);
    history.swap(0, 2);
    let ctx = context(history);
    let inner = ctx.clone();
    ready(
        &ctx,
        Box::pin(async move {
            assert_eq!(
                inner
                    .start_child_workflow(
                        "clock-child",
                        ChildWorkflowOptions::new("children"),
                        json!([])
                    )
                    .await?
                    .result,
                json!("child-result")
            );
            assert!(inner.replay_now().is_err());
            Ok(AvroValue::Null)
        }),
    );
    let mut history = vec![start()];
    history.extend(child(1, "ChildRunCompleted", CHILD_TIME));
    let ctx = context(history);
    let inner = ctx.clone();
    ready(
        &ctx,
        Box::pin(async move {
            assert!(matches!(
                inner
                    .start_child_workflow(
                        "foreign-type",
                        ChildWorkflowOptions::new("children"),
                        json!([])
                    )
                    .await,
                Err(Error::NonDeterministicReplay(_))
            ));
            assert_eq!(read(&inner), START);
            assert_eq!(inner.state.lock().unwrap().command_cursor, 0);
            Ok(AvroValue::Null)
        }),
    );
    for (field, value) in [
        ("sequence", json!(2)),
        ("child_workflow_type", json!("foreign-type")),
    ] {
        let mut history = child(1, "ChildRunCompleted", CHILD_TIME);
        history[2].payload[field] = value;
        assert!(
            WorkflowState::new(history, "clock-tests".into(), DEFAULT_CODEC.into(), None).is_err()
        );
    }
}
