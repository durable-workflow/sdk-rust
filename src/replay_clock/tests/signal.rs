use super::*;
use crate::ResumeSignal;

const SIGNAL_TIME: &str = "2026-01-01T00:00:05.654321Z";

fn signal(sequence: u64, time: &str) -> Vec<HistoryEvent> {
    let identity = json!({"sequence":sequence,"signal_name":"ready","signal_wait_id":format!("wait-{sequence}")});
    let mut applied = identity.clone();
    applied["workflow_command_id"] = json!(format!("signal-command-{sequence}"));
    applied["value"] = encode_typed_envelope(
        &AvroValue::Array(vec![AvroValue::String(format!("delivery-{sequence}"))]),
        DEFAULT_CODEC,
    )
    .unwrap();
    vec![
        event("SignalWaitOpened", identity, START),
        event("SignalApplied", applied, time),
    ]
}

fn time(ctx: &WorkflowContext) -> Option<String> {
    ctx.replay_now()
        .ok()
        .map(|time| time.to_rfc3339_opts(chrono::SecondsFormat::Micros, true))
}

fn observe(history: Vec<HistoryEvent>) -> Value {
    let ctx = context(history);
    let inner = ctx.clone();
    let output = ready(
        &ctx,
        Box::pin(async move {
            let arguments = inner.wait_signal("ready").await?;
            let observed = time(&inner);
            assert_eq!(observed, time(&inner));
            Ok(AvroValue::from_serialize(
                &json!({"arguments":arguments,"time":observed}),
            )?)
        }),
    )
    .into_json()
    .unwrap();
    assert!(ctx.take_commands().unwrap().is_empty());
    assert!(ctx.replay_now().is_err());
    output
}

#[test]
fn replay_now_signal_consumption_cold_replays_backward_and_equal_times() {
    for last in ["2025-12-31T23:59:59.123456Z", SIGNAL_TIME] {
        let mut history = vec![start()];
        history.extend(signal(1, SIGNAL_TIME));
        history.extend(signal(2, last));
        let execute = || {
            let ctx = context(history.clone());
            let inner = ctx.clone();
            let result = ready(
                &ctx,
                Box::pin(async move {
                    assert_eq!(read(&inner), START);
                    assert_eq!(inner.wait_signal("ready").await?, vec![json!("delivery-1")]);
                    assert_eq!(read(&inner), SIGNAL_TIME);
                    assert_eq!(read(&inner), read(&inner));
                    assert_eq!(inner.wait_signal("ready").await?, vec![json!("delivery-2")]);
                    Ok(AvroValue::from_serialize(&time(&inner))?)
                }),
            )
            .into_json()
            .unwrap();
            assert_eq!(ctx.state.lock().unwrap().command_cursor, 2);
            assert!(ctx.take_commands().unwrap().is_empty());
            assert!(ctx.replay_now().is_err());
            result
        };
        assert_eq!(execute(), json!(last));
        assert_eq!(execute(), execute());
    }
}

#[test]
fn replay_now_signal_future_and_buffered_rows_are_not_consumption() {
    let received = event(
        "SignalReceived",
        json!({"signal_name":"ready","signal_wait_id":"unconsumed","arguments":encode_typed_envelope(&AvroValue::String("buffered".into()),DEFAULT_CODEC).unwrap()}),
        "2099-01-01T00:00:00Z",
    );
    let mut buffered = signal(3, "2099-01-01T00:00:00Z").remove(1);
    buffered.payload.as_object_mut().unwrap().remove("sequence");
    buffered.payload["signal_wait_id"] = json!("signal-command:buffered");
    // Its outer history order must not become an authored wait sequence.
    buffered.raw.insert("sequence".into(), json!(1));
    let ctx = context(vec![start(), received.clone(), buffered.clone()]);
    let inner = ctx.clone();
    let mut future: WorkflowFuture = Box::pin(async move {
        inner.wait_signal("ready").await?;
        panic!("unapplied receipt is not a committed wait result")
    });
    assert!(Worker::poll_workflow_future(&ctx, &mut future)
        .unwrap()
        .is_pending());
    {
        let _guard = Guard::enter(&ctx);
        assert_eq!(read(&ctx), START);
    }
    assert_eq!(ctx.take_commands().unwrap()[0]["type"], "open_signal_wait");
    let mut history = vec![start(), received, buffered];
    history.extend(signal(1, SIGNAL_TIME));
    history.extend(signal(2, "2099-01-01T00:00:00Z"));
    let ctx = context(history);
    let inner = ctx.clone();
    let mut future: WorkflowFuture = Box::pin(async move {
        inner.wait_signal("ready").await?;
        assert_eq!(read(&inner), SIGNAL_TIME);
        std::future::pending().await
    });
    assert!(Worker::poll_workflow_future(&ctx, &mut future)
        .unwrap()
        .is_pending());
    assert_eq!(ctx.state.lock().unwrap().command_cursor, 1);
    assert!(ctx.replay_now().is_err());
}

#[test]
fn replay_now_signal_identity_and_explicit_counter_are_required_without_changing_results() {
    for index in 0..2 {
        for replacement in [
            None,
            Some(Value::Null),
            Some(json!("")),
            Some(json!(17)),
            Some(json!("other-wait")),
        ] {
            let mut history = signal(1, SIGNAL_TIME);
            match replacement {
                Some(value) => {
                    history[index].payload["signal_wait_id"] = value;
                }
                None => {
                    history[index]
                        .payload
                        .as_object_mut()
                        .unwrap()
                        .remove("signal_wait_id");
                }
            }
            let output = observe(history);
            assert_eq!(output["arguments"], json!(["delivery-1"]));
            assert!(output["time"].is_null());
        }
        let mut history = signal(1, SIGNAL_TIME);
        history[index]
            .payload
            .as_object_mut()
            .unwrap()
            .remove("sequence");
        history[index].raw.insert("sequence".into(), json!(1));
        assert!(observe(history)["time"].is_null());
        let mut history = signal(1, SIGNAL_TIME);
        history[index].payload["workflow_sequence"] = json!(2);
        assert!(observe(history)["time"].is_null());
        let mut history = signal(1, SIGNAL_TIME);
        history[index]
            .payload
            .as_object_mut()
            .unwrap()
            .remove("signal_name");
        history[index]
            .raw
            .insert("signal_name".into(), json!("ready"));
        assert!(observe(history)["time"].is_null());
    }
}

#[test]
fn replay_now_signal_time_and_reuse_do_not_borrow_unrelated_authority() {
    for bad in [
        None,
        Some(Value::Null),
        Some(json!("invalid")),
        Some(json!(42)),
    ] {
        let mut history = vec![start()];
        history.extend(signal(1, SIGNAL_TIME));
        match bad {
            Some(value) => {
                history[2].raw.insert("recorded_at".into(), value);
            }
            None => {
                history[2].raw.remove("recorded_at");
            }
        }
        assert!(observe(history)["time"].is_null());
    }
    let mut history = signal(1, SIGNAL_TIME);
    history[1].payload["reused_recorded_at"] = json!("2025-12-01T00:00:00.123456Z");
    assert_eq!(
        observe(history.clone())["time"],
        json!("2025-12-01T00:00:00.123456Z")
    );
    assert_eq!(
        snapshot_time(&history, DEFAULT_CODEC)
            .unwrap()
            .to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
        "2025-12-01T00:00:00.123456Z"
    );
    history[1].payload["reused_recorded_at"] = json!("invalid");
    assert!(observe(history)["time"].is_null());
    let mut history = signal(1, SIGNAL_TIME);
    history[1].raw.insert("timestamp".into(), json!("invalid"));
    assert!(observe(history)["time"].is_null());
}

#[test]
fn replay_now_signal_ambiguous_order_payload_and_invalid_timeout_are_unavailable() {
    let mut history = signal(1, SIGNAL_TIME);
    history.swap(0, 1);
    assert!(observe(history)["time"].is_null());
    for kind in ["TimerScheduled", "TimerFired", "TimerCancelled"] {
        let mut history = signal(1, SIGNAL_TIME);
        history.push(event(kind,json!({"sequence":1,"timer_kind":"signal_timeout","signal_wait_id":"wait-1","timer_id":"signal-timeout"}),"2099-01-01T00:00:00Z"));
        assert!(observe(history)["time"].is_null());
    }
    let mut history = signal(1, SIGNAL_TIME);
    history[0].payload["timeout_seconds"] = json!("5");
    assert!(observe(history)["time"].is_null());
    let mut history = signal(1, SIGNAL_TIME);
    history[1].payload.as_object_mut().unwrap().remove("value");
    let result = observe(history);
    assert_eq!(result["arguments"], json!([]));
    assert!(result["time"].is_null());
    for duplicate in [0, 1] {
        let mut history = signal(1, SIGNAL_TIME);
        history.push(history[duplicate].clone());
        assert!(
            WorkflowState::new(history, "clock-tests".into(), DEFAULT_CODEC.into(), None).is_err()
        );
    }
    let mut history = signal(1, SIGNAL_TIME);
    history[1].payload["signal_name"] = json!("different");
    assert!(WorkflowState::new(history, "clock-tests".into(), DEFAULT_CODEC.into(), None).is_err());
}

#[test]
fn replay_now_signal_task_resume_without_applied_record_stays_unavailable() {
    for opened in [false, true] {
        let mut history = vec![start()];
        if opened {
            history.push(signal(1, SIGNAL_TIME).remove(0));
        }
        let ctx = context(history);
        ctx.state.lock().unwrap().resume_signal = Some(ResumeSignal {
            signal_name: "ready".into(),
            arguments: vec![AvroValue::String("task-delivery".into())],
        });
        let inner = ctx.clone();
        ready(
            &ctx,
            Box::pin(async move {
                assert_eq!(
                    inner.wait_signal("ready").await?,
                    vec![json!("task-delivery")]
                );
                assert!(inner.replay_now().is_err());
                Ok(AvroValue::Null)
            }),
        );
    }
}

#[test]
fn replay_now_signal_interleaved_error_polls_restore_the_outer_scope() {
    let outer = context(vec![start()]);
    let _guard = Guard::enter(&outer);
    let mut tasks = vec![];
    for expected in [SIGNAL_TIME, "2026-01-01T00:00:02.123456Z"] {
        let ctx = context(signal(1, expected));
        let inner = ctx.clone();
        let future: WorkflowFuture = Box::pin(async move {
            inner.wait_signal("ready").await?;
            assert_eq!(read(&inner), expected);
            tokio::task::yield_now().await;
            assert_eq!(read(&inner), expected);
            Err(Error::WorkerLoop("expected error after signal".into()))
        });
        tasks.push((ctx, future));
    }
    for (ctx, future) in &mut tasks {
        assert!(Worker::poll_workflow_future(ctx, future)
            .unwrap()
            .is_pending());
        assert!(ctx.replay_now().is_err());
        assert_eq!(read(&outer), START);
    }
    for (ctx, future) in &mut tasks {
        assert!(matches!(
            Worker::poll_workflow_future(ctx, future).unwrap(),
            Poll::Ready(Err(Error::WorkerLoop(_)))
        ));
        assert!(ctx.replay_now().is_err());
        assert_eq!(read(&outer), START);
    }
}

#[tokio::test]
async fn replay_now_signal_queries_use_the_selected_existing_prefix() {
    let mut worker = Worker::new(Client::new("http://127.0.0.1:8080").unwrap(), "clock-tests");
    worker.register_replayed_workflow(
        "signal-clock",
        || 0_u64,
        |ctx, _, _| async move {
            ctx.wait_signal("ready").await?;
            ctx.wait_signal("ready").await?;
            Ok(Value::Null)
        },
    );
    worker.register_query("signal-clock", "snapshot", |ctx, _| async move {
        Ok(json!(ctx
            .replay_now()?
            .to_rfc3339_opts(chrono::SecondsFormat::Micros, true)))
    });
    worker.register_replayed_query::<u64, _, _>(
        "signal-clock",
        "reconstructed",
        |ctx, _, _| async move {
            Ok(json!(ctx
                .replay_now()?
                .to_rfc3339_opts(chrono::SecondsFormat::Micros, true)))
        },
    );
    let mut history = signal(1, SIGNAL_TIME);
    for query in ["snapshot", "reconstructed"] {
        if query == "reconstructed" {
            history.push(signal(2, "2099-01-01T00:00:00Z").remove(0));
        }
        let task:QueryTask=serde_json::from_value(json!({"query_task_id":"signal-q","workflow_type":"signal-clock","query_name":query,"payload_codec":DEFAULT_CODEC,"history_events":history})).unwrap();
        assert_eq!(
            worker
                .execute_query_task(task)
                .await
                .unwrap()
                .into_json()
                .unwrap(),
            json!(SIGNAL_TIME)
        );
    }
    assert!(snapshot_time(&history, DEFAULT_CODEC).is_none());
}

mod timed;
