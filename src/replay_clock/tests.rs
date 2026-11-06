use super::*;
use crate::{
    decode_wire_value, encode_typed_envelope, json, AvroValue, Client, ConditionWaitOptions,
    ConditionWaitResult, QueryTask, Worker, WorkflowFuture, WorkflowTask, DEFAULT_CODEC,
};
use std::{task::Poll, time::Duration};

const START: &str = "2026-01-01T00:00:00.123456Z";

fn event(kind: &str, payload: Value, time: &str) -> HistoryEvent {
    serde_json::from_value(json!({"event_type":kind,"payload":payload,"recorded_at":time})).unwrap()
}

fn start() -> HistoryEvent {
    event(
        "WorkflowStarted",
        json!({"recovery_kind":"redrive","replayed_started_at":START}),
        "2026-02-01T00:00:00Z",
    )
}

fn timer(sequence: u64, time: &str) -> Vec<HistoryEvent> {
    let payload =
        json!({"sequence":sequence,"timer_id":format!("timer-{sequence}"),"delay_seconds":1});
    vec![
        event("TimerScheduled", payload.clone(), START),
        event("TimerFired", payload, time),
    ]
}

fn context(history: Vec<HistoryEvent>) -> WorkflowContext {
    WorkflowContext {
        state: Arc::new(Mutex::new(
            WorkflowState::new(history, "clock-tests".into(), DEFAULT_CODEC.into(), None).unwrap(),
        )),
        cancellation_scope_id: "root".into(),
    }
}

fn read(context: &WorkflowContext) -> String {
    context
        .replay_now()
        .unwrap()
        .to_rfc3339_opts(chrono::SecondsFormat::Micros, true)
}

fn ready(context: &WorkflowContext, mut future: WorkflowFuture) -> AvroValue {
    match Worker::poll_workflow_future(context, &mut future).unwrap() {
        Poll::Ready(result) => result.unwrap(),
        Poll::Pending => panic!("expected ready replay"),
    }
}

#[test]
fn replay_now_deadline_loop_advances_and_cold_replays_identically() {
    let mut history = vec![start()];
    for i in 1..=3 {
        history.extend(timer(i, &format!("2026-01-01T00:00:0{i}.123456Z")));
    }
    let execute = || {
        let ctx = context(history.clone());
        let future_ctx = ctx.clone();
        ready(&ctx,Box::pin(async move {
            let initial=future_ctx.replay_now()?; let deadline=initial+chrono::Duration::seconds(3);
            let mut times=vec![];
            while future_ctx.replay_now()?<deadline {
                assert_eq!(future_ctx.replay_now()?,future_ctx.replay_now()?);
                future_ctx.sleep(Duration::from_secs(1)).await?;
                times.push(read(&future_ctx));
                assert!(times.len()<=3);
            }
            Ok(AvroValue::from_serialize(&json!({"times":times,"elapsed":(future_ctx.replay_now()?-initial).num_seconds()}))?)
        })).into_json().unwrap()
    };
    let first = execute();
    assert_eq!(first, execute());
    assert_eq!(first["elapsed"], 3);
    assert_eq!(first["times"].as_array().unwrap().len(), 3);
}

#[test]
fn replay_now_uses_only_consumed_result_and_preserves_precision_and_backward_time() {
    let mut history = vec![start()];
    history.extend(timer(1, "2025-12-31T23:59:59.654321Z"));
    history.extend(timer(2, "2099-01-01T00:00:00Z"));
    let ctx = context(history);
    let inner = ctx.clone();
    let result = ready(
        &ctx,
        Box::pin(async move {
            assert_eq!(read(&inner), START);
            inner.sleep(Duration::from_secs(1)).await?;
            Ok(AvroValue::from_serialize(&read(&inner))?)
        }),
    )
    .into_json()
    .unwrap();
    assert_eq!(result, "2025-12-31T23:59:59.654321Z");
    assert!(matches!(
        ctx.replay_now(),
        Err(Error::ReplayClockUnavailable)
    ));
}

#[test]
fn replay_now_activity_result_is_correlated_and_redrive_aware() {
    let result = encode_typed_envelope(&AvroValue::String("done".into()), DEFAULT_CODEC).unwrap();
    let mut complete = event(
        "ActivityCompleted",
        json!({"sequence":1,"activity_type":"a","result":result,"reused_recorded_at":"2026-01-01T00:00:05.654321Z"}),
        "2026-02-01T00:00:00Z",
    );
    for expected in [Some("2026-01-01T00:00:05.654321Z"), None] {
        if expected.is_none() {
            complete.payload["reused_recorded_at"] = json!("invalid");
        }
        let ctx = context(vec![start(), complete.clone()]);
        let inner = ctx.clone();
        ready(
            &ctx,
            Box::pin(async move {
                assert_eq!(inner.activity("a", json!([])).await?, json!("done"));
                match expected {
                    Some(value) => assert_eq!(read(&inner), value),
                    None => assert!(matches!(
                        inner.replay_now(),
                        Err(Error::ReplayClockUnavailable)
                    )),
                };
                Ok(AvroValue::Null)
            }),
        );
    }
}

#[test]
fn replay_now_missing_start_time_is_not_history_insertion_or_wall_time() {
    for history in [
        vec![],
        vec![event("WorkflowStarted", json!({}), START)],
        vec![event(
            "WorkflowStarted",
            json!({"recovery_kind":"redrive","replayed_started_at":"invalid"}),
            START,
        )],
    ] {
        let ctx = context(history);
        let _guard = Guard::enter(&ctx);
        assert!(matches!(
            ctx.replay_now(),
            Err(Error::ReplayClockUnavailable)
        ));
    }
    let mut history = vec![start()];
    let mut events = timer(1, START);
    events[1].raw.remove("recorded_at");
    history.extend(events);
    let ctx = context(history);
    let inner = ctx.clone();
    ready(
        &ctx,
        Box::pin(async move {
            inner.sleep(Duration::from_secs(1)).await?;
            assert!(matches!(
                inner.replay_now(),
                Err(Error::ReplayClockUnavailable)
            ));
            Ok(AvroValue::Null)
        }),
    );
}

#[test]
fn replay_now_nested_contexts_and_error_exit_restore_scope() {
    let a = context(vec![start()]);
    let b = context(vec![start()]);
    let _outer = Guard::enter(&a);
    assert_eq!(read(&a), START);
    assert!(b.replay_now().is_err());
    let result: Result<()> = (|| {
        let _inner = Guard::enter(&b);
        assert!(a.replay_now().is_err());
        assert_eq!(read(&b), START);
        Err(Error::WorkerLoop("test error".into()))
    })();
    assert!(result.is_err());
    assert_eq!(read(&a), START);
    {
        let _blocked = Guard::unavailable();
        assert!(a.replay_now().is_err());
    }
    assert_eq!(read(&a), START);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replay_now_concurrent_workflows_have_independent_scopes() {
    let a = tokio::spawn(async {
        let ctx = context(vec![start()]);
        let inner = ctx.clone();
        let result = ready(
            &ctx,
            Box::pin(async move { Ok(AvroValue::from_serialize(&read(&inner))?) }),
        );
        assert!(ctx.replay_now().is_err());
        result
    });
    let b = tokio::spawn(async {
        let mut start = start();
        start.payload["replayed_started_at"] = json!("2026-01-01T00:00:10Z");
        let ctx = context(vec![start]);
        let inner = ctx.clone();
        ready(
            &ctx,
            Box::pin(async move { Ok(AvroValue::from_serialize(&read(&inner))?) }),
        )
    });
    assert_ne!(a.await.unwrap(), b.await.unwrap());
}

#[test]
fn replay_now_inline_marker_is_unavailable_on_first_pass_and_cold_replay() {
    let recorded = event(
        "SideEffectRecorded",
        json!({"sequence":1,"result":encode_typed_envelope(&AvroValue::String("fixed".into()),DEFAULT_CODEC).unwrap()}),
        "2026-01-01T00:00:01Z",
    );
    for history in [vec![start()], vec![start(), recorded]] {
        let ctx = context(history);
        let inner = ctx.clone();
        ready(
            &ctx,
            Box::pin(async move {
                let value: String = inner.side_effect(|| "fixed".to_string())?;
                assert_eq!(value, "fixed");
                assert!(inner.replay_now().is_err());
                Ok(AvroValue::Null)
            }),
        );
    }
}

#[test]
fn replay_now_legacy_version_default_keeps_same_prefix() {
    let mut history = vec![start()];
    history.extend(timer(1, "2026-01-01T00:00:01Z"));
    let ctx = context(history);
    let _guard = Guard::enter(&ctx);
    assert_eq!(ctx.get_version("old-patch", -1, 1).unwrap(), -1);
    assert_eq!(read(&ctx), START);
    assert!(ctx.state.lock().unwrap().commands.is_empty());
}

#[test]
fn replay_now_snapshot_prefix_and_update_handler_are_exact_and_immutable() {
    let mut history = vec![start()];
    history.extend(timer(1, "2026-01-01T00:00:02Z"));
    assert_eq!(
        snapshot_time(&history, DEFAULT_CODEC)
            .unwrap()
            .timestamp_subsec_micros(),
        0
    );
    let saved = serde_json::to_value(&history).unwrap();
    let at = snapshot_time(&history, DEFAULT_CODEC);
    assert_eq!(at, snapshot_time(&history, DEFAULT_CODEC));
    assert_eq!(saved, serde_json::to_value(&history).unwrap());
    history.push(event(
        "UnknownFutureBoundary",
        json!({}),
        "2099-01-01T00:00:00Z",
    ));
    assert!(snapshot_time(&history, DEFAULT_CODEC).is_none());
    let accepted = event(
        "UpdateAccepted",
        json!({"update_id":"u1","update_name":"inspect"}),
        START,
    );
    assert_eq!(
        update_time(&[accepted.clone()], "u1")
            .unwrap()
            .timestamp_subsec_micros(),
        123456
    );
    assert!(update_time(&[accepted.clone()], "u2").is_none());
    assert!(update_time(&[accepted.clone(), accepted], "u1").is_none());
}

#[tokio::test]
async fn replay_now_actual_query_and_update_callbacks_use_their_own_prefix() {
    let mut worker = Worker::new(Client::new("http://127.0.0.1:8080").unwrap(), "clock-tests");
    worker.register_workflow("clock", |_ctx, _args| async { Ok(Value::Null) });
    worker.register_query("clock", "inspect", |ctx, _args| async move {
        Ok(json!(ctx
            .replay_now()
            .map(|t| t.to_rfc3339_opts(chrono::SecondsFormat::Micros, true))
            .ok()))
    });
    worker.register_update("clock", "inspect", |ctx, _args| async move {
        Ok(json!(ctx
            .replay_now()
            .map(|t| t.to_rfc3339_opts(chrono::SecondsFormat::Micros, true))
            .ok()))
    });
    let mut history = vec![start()];
    history.extend(timer(1, "2026-01-01T00:00:02.456789Z"));
    let query:QueryTask=serde_json::from_value(json!({"query_task_id":"q1","workflow_type":"clock","query_name":"inspect","payload_codec":DEFAULT_CODEC,"history_events":history})).unwrap();
    for _ in 0..2 {
        assert_eq!(
            worker
                .execute_query_task(query.clone())
                .await
                .unwrap()
                .into_json()
                .unwrap(),
            json!("2026-01-01T00:00:02.456789Z")
        );
    }
    let accepted = event(
        "UpdateAccepted",
        json!({"update_id":"u1","update_name":"inspect"}),
        START,
    );
    let task:WorkflowTask=serde_json::from_value(json!({"task_id":"u1","workflow_type":"clock","payload_codec":DEFAULT_CODEC,"workflow_update_id":"u1","update_name":"inspect","history_events":[accepted]})).unwrap();
    let commands = worker.execute_update_task(&task, "u1").unwrap();
    assert_eq!(commands[0]["type"], "complete_update");
    assert_eq!(
        decode_wire_value(&commands[0]["result"], DEFAULT_CODEC).unwrap(),
        json!(START)
    );
    let mut unbound = task;
    unbound.history_events.clear();
    let commands = worker.execute_update_task(&unbound, "u1").unwrap();
    assert_eq!(
        decode_wire_value(&commands[0]["result"], DEFAULT_CODEC).unwrap(),
        Value::Null
    );
}

#[test]
fn replay_now_direct_predicate_is_unavailable_and_restores_workflow_scope() {
    let ctx = context(vec![start()]);
    let inner = ctx.clone();
    let predicate = ctx.clone();
    ready(
        &ctx,
        Box::pin(async move {
            let before = inner.replay_now()?;
            let result = inner
                .wait_condition(
                    ConditionWaitOptions::new("ready", "clock-test"),
                    move || {
                        assert!(matches!(
                            predicate.replay_now(),
                            Err(Error::ReplayClockUnavailable)
                        ));
                        Ok(true)
                    },
                )
                .await?;
            assert_eq!(result, ConditionWaitResult::Satisfied);
            assert_eq!(before, inner.replay_now()?);
            Ok(AvroValue::Null)
        }),
    );
    assert!(ctx.replay_now().is_err());
    assert!(ctx.state.lock().unwrap().commands.is_empty());
}

#[test]
fn replay_now_reads_real_published_activity_evidence_without_inventing_entry_time() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../tests/fixtures/replay-regressions/published-php-complete-explicit-backoff.json"
    ))
    .unwrap();
    let history: Vec<HistoryEvent> = serde_json::from_value(fixture["history"].clone()).unwrap();
    let mut state =
        WorkflowState::new(history, "clock-tests".into(), DEFAULT_CODEC.into(), None).unwrap();
    assert!(state.replay_time().is_err());
    let sequence = state
        .recorded_commands
        .iter()
        .find_map(|command| match command {
            RecordedCommand::Activity {
                sequence,
                outcome: Some(_),
                ..
            } => Some(*sequence),
            _ => None,
        })
        .unwrap();
    state.observe_replay_sequence(sequence);
    let actual = state.replay_time().unwrap();
    let selected = state
        .history_events
        .iter()
        .find(|event| {
            event.event_type == "ActivityCompleted"
                && durable_event_sequence(event) == Some(sequence)
        })
        .unwrap();
    assert_eq!(actual, recorded_time(selected, true).unwrap());
}

#[test]
fn replay_now_inline_local_result_has_no_persistence_time_on_either_path() {
    let ctx = context(vec![start()]);
    let inner = ctx.clone();
    let mut call = inner.local_activity("local", json!([]));
    let outcome = Ok(AvroValue::String("local-result".into()));
    call.local_result = Some(Arc::new(Mutex::new(Some(outcome))));
    ready(
        &ctx,
        Box::pin(async move {
            assert_eq!(call.await?, json!("local-result"));
            assert!(inner.replay_now().is_err());
            Ok(AvroValue::Null)
        }),
    );
    let complete = event(
        "ActivityCompleted",
        json!({"sequence":1,"activity_type":"local","execution_mode":"local","result":encode_typed_envelope(&AvroValue::String("local-result".into()),DEFAULT_CODEC).unwrap()}),
        "2026-01-01T00:00:04Z",
    );
    let ctx = context(vec![start(), complete]);
    let inner = ctx.clone();
    ready(
        &ctx,
        Box::pin(async move {
            assert_eq!(
                inner.local_activity("local", json!([])).await?,
                json!("local-result")
            );
            assert!(inner.replay_now().is_err());
            Ok(AvroValue::Null)
        }),
    );
}

#[test]
fn replay_now_failure_uses_its_consumed_throw_time_and_equal_events_keep_order() {
    let failure = event(
        "ActivityFailed",
        json!({"sequence":1,"activity_type":"a","message":"expected failure","reused_recorded_at":START}),
        "2026-01-01T00:00:05.999999Z",
    );
    let ctx = context(vec![start(), failure]);
    let inner = ctx.clone();
    ready(
        &ctx,
        Box::pin(async move {
            assert!(matches!(
                inner.activity("a", json!([])).await,
                Err(Error::ActivityFailed(_))
            ));
            assert_eq!(read(&inner), "2026-01-01T00:00:05.999999Z");
            Ok(AvroValue::Null)
        }),
    );
    let mut history = vec![start()];
    history.extend(timer(1, START));
    history.extend(timer(2, START));
    let ctx = context(history);
    let inner = ctx.clone();
    ready(
        &ctx,
        Box::pin(async move {
            inner.sleep(Duration::from_secs(1)).await?;
            assert_eq!(read(&inner), START);
            inner.sleep(Duration::from_secs(1)).await?;
            assert_eq!(read(&inner), START);
            assert_eq!(inner.state.lock().unwrap().command_cursor, 2);
            Ok(AvroValue::Null)
        }),
    );
}

#[tokio::test]
async fn replay_now_reconstructed_query_uses_consumed_prefix_not_snapshot_tail() {
    let mut worker = Worker::new(Client::new("http://127.0.0.1:8080").unwrap(), "clock-tests");
    worker.register_replayed_workflow(
        "clock",
        || 0_u64,
        |ctx, _args, _state| async move {
            ctx.sleep(Duration::from_secs(1)).await?;
            ctx.sleep(Duration::from_secs(1)).await?;
            Ok(Value::Null)
        },
    );
    worker.register_replayed_query::<u64, _, _>(
        "clock",
        "inspect",
        |ctx, _state, _args| async move {
            Ok(json!(ctx
                .replay_now()?
                .to_rfc3339_opts(chrono::SecondsFormat::Micros, true)))
        },
    );
    let mut history = vec![start()];
    history.extend(timer(1, "2026-01-01T00:00:03.765432Z"));
    history.push(event(
        "TimerScheduled",
        json!({"sequence":2,"timer_id":"timer-2","delay_seconds":1}),
        "2099-01-01T00:00:00Z",
    ));
    let task:QueryTask=serde_json::from_value(json!({"query_task_id":"q1","workflow_type":"clock","query_name":"inspect","payload_codec":DEFAULT_CODEC,"history_events":history})).unwrap();
    assert_eq!(
        worker
            .execute_query_task(task)
            .await
            .unwrap()
            .into_json()
            .unwrap(),
        json!("2026-01-01T00:00:03.765432Z")
    );
}

mod child;
mod signal;

mod condition;
