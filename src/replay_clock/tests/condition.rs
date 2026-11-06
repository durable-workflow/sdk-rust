use super::*;

const RESOLVED: &str = "2026-01-01T00:00:05.654321Z";

fn condition(sequence: u64, ordinal: u64, timed_out: bool, time: &str) -> Vec<HistoryEvent> {
    let mut payload = json!({"sequence":sequence,"condition_wait_id":format!("wait-{sequence}"),
        "condition_wait_occurrence_id":format!("rust:condition-wait:{ordinal}"),
        "condition_key":"ready","condition_definition_fingerprint":"sha256:ready-v1"});
    if timed_out {
        payload["timeout_seconds"] = json!(30);
    }
    vec![
        event("ConditionWaitOpened", payload.clone(), START),
        event(
            if timed_out {
                "ConditionWaitTimedOut"
            } else {
                "ConditionWaitSatisfied"
            },
            payload,
            time,
        ),
    ]
}

fn options(timed_out: bool) -> ConditionWaitOptions {
    let options = ConditionWaitOptions::new("ready", "sha256:ready-v1");
    if timed_out {
        options.timeout(Duration::from_secs(30))
    } else {
        options
    }
}

fn observe(history: Vec<HistoryEvent>, timed_out: bool) -> Value {
    let ctx = context(history);
    let inner = ctx.clone();
    let result = ready(&ctx, Box::pin(async move {
        let result = inner.wait_condition(options(timed_out), || panic!("persisted outcome must not evaluate a predicate")).await?;
        Ok(AvroValue::from_serialize(&json!({"result":format!("{result:?}"),"time":inner.replay_now().ok().map(|t|t.to_rfc3339_opts(chrono::SecondsFormat::Micros,true))}))?)
    })).into_json().unwrap();
    assert!(ctx.take_commands().unwrap().is_empty());
    assert!(ctx.replay_now().is_err());
    result
}

#[test]
fn replay_now_persisted_condition_results_cold_replay_equal_and_backward_times() {
    for timed_out in [false, true] {
        for last in [RESOLVED, "2025-12-31T23:59:59.123456Z"] {
            let mut history = vec![start()];
            history.extend(condition(1, 0, false, RESOLVED));
            history.extend(condition(2, 1, timed_out, last));
            let execute = || {
                let ctx = context(history.clone());
                let inner = ctx.clone();
                let result = ready(
                    &ctx,
                    Box::pin(async move {
                        assert_eq!(read(&inner), START);
                        assert_eq!(
                            inner.wait_condition(options(false), || Ok(false)).await?,
                            ConditionWaitResult::Satisfied
                        );
                        assert_eq!(read(&inner), RESOLVED);
                        let result = inner
                            .wait_condition(options(timed_out), || Ok(false))
                            .await?;
                        assert_eq!(
                            result,
                            if timed_out {
                                ConditionWaitResult::TimedOut
                            } else {
                                ConditionWaitResult::Satisfied
                            }
                        );
                        assert_eq!(read(&inner), read(&inner));
                        Ok(AvroValue::from_serialize(&read(&inner))?)
                    }),
                )
                .into_json()
                .unwrap();
                assert!(ctx.take_commands().unwrap().is_empty());
                result
            };
            assert_eq!(execute(), json!(last));
            assert_eq!(execute(), execute());
        }
    }
}

#[test]
fn replay_now_condition_uses_only_consumed_resolution_not_timer_reuse_or_future_rows() {
    let mut history = condition(1, 0, true, RESOLVED);
    history[1].payload["reused_recorded_at"] = json!("2099-01-01T00:00:00Z");
    let timer = json!({"sequence":1,"condition_wait_id":"wait-1","condition_wait_occurrence_id":"rust:condition-wait:0","timer_kind":"condition_timeout","timer_id":"timeout-1","delay_seconds":30});
    history.insert(1, event("TimerScheduled", timer.clone(), START));
    history.insert(2, event("TimerFired", timer, "2099-01-01T00:00:00Z"));
    history.extend(condition(2, 1, false, "2099-01-01T00:00:00Z"));
    let output = observe(history.clone(), true);
    assert_eq!(output["result"], json!("TimedOut"));
    assert_eq!(output["time"], json!(RESOLVED));
    assert_eq!(
        snapshot_time(&history[..4], DEFAULT_CODEC)
            .unwrap()
            .to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
        RESOLVED
    );
    assert!(snapshot_time(&history[..3], DEFAULT_CODEC).is_none());
    history.remove(3);
    assert!(
        observe(history, true)["time"].is_null(),
        "a timeout fire without the consumed resolution has no clock authority"
    );
}

#[test]
fn replay_now_condition_requires_explicit_counter_and_complete_definition() {
    for index in [0, 1] {
        let mut history = condition(1, 0, false, RESOLVED);
        history[index]
            .payload
            .as_object_mut()
            .unwrap()
            .remove("sequence");
        history[index].raw.insert("sequence".into(), json!(1));
        assert!(observe(history, false)["time"].is_null());
        let mut history = condition(1, 0, false, RESOLVED);
        history[index].payload["workflow_sequence"] = json!(2);
        assert!(observe(history, false)["time"].is_null());
    }
    for field in ["condition_key", "condition_definition_fingerprint"] {
        let mut history = condition(1, 0, false, RESOLVED);
        history[1].payload.as_object_mut().unwrap().remove(field);
        assert!(observe(history, false)["time"].is_null());
    }
    let mut history = condition(1, 0, true, RESOLVED);
    history[1]
        .payload
        .as_object_mut()
        .unwrap()
        .remove("timeout_seconds");
    assert!(observe(history, true)["time"].is_null());
}

#[test]
fn replay_now_condition_invalid_identity_and_duplicate_results_keep_legacy_refusal() {
    for field in [
        "condition_wait_id",
        "condition_wait_occurrence_id",
        "condition_key",
        "condition_definition_fingerprint",
    ] {
        let mut history = condition(1, 0, false, RESOLVED);
        history[1].payload[field] = json!("foreign");
        assert!(
            WorkflowState::new(history, "clock-tests".into(), DEFAULT_CODEC.into(), None).is_err()
        );
    }
    let mut history = condition(1, 0, false, RESOLVED);
    history.push(history[1].clone());
    assert!(WorkflowState::new(history, "clock-tests".into(), DEFAULT_CODEC.into(), None).is_err());
    let mut history = condition(1, 0, false, RESOLVED);
    history.swap(0, 1);
    assert!(observe(history, false)["time"].is_null());
}

#[test]
fn replay_now_condition_missing_invalid_time_does_not_borrow_other_rows() {
    for value in [
        None,
        Some(Value::Null),
        Some(json!("invalid")),
        Some(json!(42)),
    ] {
        let mut history = vec![start()];
        history.extend(condition(1, 0, false, RESOLVED));
        if let Some(value) = value {
            history[2].raw.insert("recorded_at".into(), value);
        } else {
            history[2].raw.remove("recorded_at");
        }
        assert!(observe(history, false)["time"].is_null());
    }
    let mut history = condition(1, 0, false, RESOLVED);
    history[1].raw.insert("timestamp".into(), json!("invalid"));
    assert!(observe(history, false)["time"].is_null());
}

#[test]
fn replay_now_condition_reopen_uses_the_final_actually_consumed_physical_wait() {
    let mut history = condition(1, 0, false, RESOLVED);
    history.extend(condition(2, 0, false, "2026-01-01T00:00:03.123456Z"));
    assert_eq!(
        observe(history, false)["time"],
        json!("2026-01-01T00:00:03.123456Z")
    );
}

#[test]
fn replay_now_condition_pending_predicate_and_inline_paths_keep_the_prior_prefix() {
    let mut history = vec![start()];
    history.push(condition(1, 0, false, RESOLVED).remove(0));
    let ctx = context(history);
    let inner = ctx.clone();
    let predicate = ctx.clone();
    let mut future: WorkflowFuture = Box::pin(async move {
        inner
            .wait_condition(options(false), move || {
                assert!(predicate.replay_now().is_err());
                Ok(false)
            })
            .await?;
        panic!("an open condition without a result is pending")
    });
    assert!(Worker::poll_workflow_future(&ctx, &mut future)
        .unwrap()
        .is_pending());
    let _guard = Guard::enter(&ctx);
    assert_eq!(read(&ctx), START);
    drop(_guard);
    for zero in [false, true] {
        let ctx = context(vec![start()]);
        let inner = ctx.clone();
        let predicate = ctx.clone();
        ready(
            &ctx,
            Box::pin(async move {
                let options = if zero {
                    options(false).timeout(Duration::ZERO)
                } else {
                    options(false)
                };
                inner
                    .wait_condition(options, move || {
                        assert!(predicate.replay_now().is_err());
                        Ok(!zero)
                    })
                    .await?;
                assert_eq!(
                    read(&inner),
                    START,
                    "a local result supplies no new committed boundary"
                );
                Ok(AvroValue::Null)
            }),
        );
        assert!(ctx.take_commands().unwrap().is_empty());
    }
}

#[test]
fn replay_now_grouped_condition_remains_unavailable_after_actual_result() {
    let mut history = vec![start()];
    let mut recorded = condition(1, 0, false, RESOLVED);
    for event in &mut recorded {
        crate::apply_parallel_group_path(
            event.payload.as_object_mut().unwrap(),
            &[crate::parallel_group_entry(1, 1, 0, "condition")],
        );
    }
    history.extend(recorded);
    let ctx = context(history);
    let inner = ctx.clone();
    ready(
        &ctx,
        Box::pin(async move {
            inner
                .parallel(vec![crate::ParallelOperation::condition(
                    options(false),
                    || Ok(false),
                )])
                .await?;
            assert!(inner.replay_now().is_err());
            Ok(AvroValue::Null)
        }),
    );
    assert!(ctx.take_commands().unwrap().is_empty());
}

#[tokio::test]
async fn replay_now_condition_queries_use_the_actual_selected_prefix() {
    let mut worker = Worker::new(Client::new("http://127.0.0.1:8080").unwrap(), "clock-tests");
    worker.register_replayed_workflow(
        "condition-clock",
        || 0_u64,
        |ctx, _, _| async move {
            ctx.wait_condition(options(false), || Ok(false)).await?;
            ctx.sleep(Duration::from_secs(1)).await?;
            Ok(Value::Null)
        },
    );
    worker.register_query("condition-clock", "snapshot", |ctx, _| async move {
        Ok(json!(ctx
            .replay_now()?
            .to_rfc3339_opts(chrono::SecondsFormat::Micros, true)))
    });
    worker.register_replayed_query::<u64, _, _>(
        "condition-clock",
        "reconstructed",
        |ctx, _, _| async move {
            Ok(json!(ctx
                .replay_now()?
                .to_rfc3339_opts(chrono::SecondsFormat::Micros, true)))
        },
    );
    let mut history = condition(1, 0, false, RESOLVED);
    for query in ["snapshot", "reconstructed"] {
        if query == "reconstructed" {
            history.push(event(
                "TimerScheduled",
                json!({"sequence":2,"timer_id":"future-timer","delay_seconds":1}),
                "2099-01-01T00:00:00Z",
            ));
        }
        let task: QueryTask = serde_json::from_value(json!({"query_task_id":"condition-q","workflow_type":"condition-clock","query_name":query,"payload_codec":DEFAULT_CODEC,"history_events":history})).unwrap();
        assert_eq!(
            worker
                .execute_query_task(task)
                .await
                .unwrap()
                .into_json()
                .unwrap(),
            json!(RESOLVED)
        );
    }
    assert!(snapshot_time(&history, DEFAULT_CODEC).is_none());
}
