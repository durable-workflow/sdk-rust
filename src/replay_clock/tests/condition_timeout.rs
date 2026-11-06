use super::*;

const FIRED: &str = "2026-01-01T00:00:05.654321Z";

fn options(seconds: u64) -> ConditionWaitOptions {
    ConditionWaitOptions::new("ready", "sha256:ready-v1").timeout(Duration::from_secs(seconds))
}

fn timeout(sequence: u64, ordinal: u64, seconds: u64, time: &str) -> Vec<HistoryEvent> {
    let mut payload = json!({"sequence":sequence,"condition_wait_id":format!("wait-{sequence}"),"condition_wait_occurrence_id":format!("rust:condition-wait:{ordinal}"),"condition_key":"ready","condition_definition_fingerprint":"sha256:ready-v1","timeout_seconds":seconds});
    let opened = event("ConditionWaitOpened", payload.clone(), START);
    payload.as_object_mut().unwrap().remove("timeout_seconds");
    payload["timer_id"] = json!(format!("timeout-{sequence}"));
    payload["timer_kind"] = json!("condition_timeout");
    payload["delay_seconds"] = json!(seconds);
    payload["fire_at"] = json!("2026-01-01T00:00:30Z");
    let scheduled = event("TimerScheduled", payload.clone(), START);
    if seconds == 0 {
        payload.as_object_mut().unwrap().remove("fire_at");
    }
    payload["fired_at"] = json!("2099-01-01T00:00:00Z");
    vec![opened, scheduled, event("TimerFired", payload, time)]
}

fn observe(history: Vec<HistoryEvent>, seconds: u64) -> Value {
    let ctx = context(history);
    let inner = ctx.clone();
    let output = ready(
        &ctx,
        Box::pin(async move {
            let result = inner
                .wait_condition(options(seconds), || {
                    panic!("persisted timeout must not evaluate a predicate")
                })
                .await?;
            assert_eq!(result, ConditionWaitResult::TimedOut);
            let time = inner.replay_now().ok();
            assert_eq!(time, inner.replay_now().ok());
            AvroValue::from_serialize(&json!(
                time.map(|time| time.to_rfc3339_opts(chrono::SecondsFormat::Micros, true))
            ))
        }),
    )
    .into_json()
    .unwrap();
    assert!(ctx.take_commands().unwrap().is_empty());
    assert!(ctx.replay_now().is_err());
    output
}

#[test]
fn replay_now_condition_fired_only_consumes_exact_event_with_cold_equal_and_backward_time() {
    for seconds in [0, 30] {
        for time in [START, FIRED, "2025-12-31T23:59:59.123456Z"] {
            let mut history = vec![start()];
            history.extend(timeout(1, 0, seconds, time));
            assert_eq!(observe(history.clone(), seconds), json!(time));
            assert_eq!(observe(history.clone(), seconds), observe(history, seconds));
        }
    }
}

#[test]
fn replay_now_condition_timeout_ignores_payload_reuse_deadline_and_unconsumed_future_times() {
    let mut history = timeout(1, 0, 30, FIRED);
    history[2].payload["reused_recorded_at"] = json!("2099-01-01T00:00:00Z");
    history[2]
        .raw
        .insert("timestamp".into(), json!("2099-01-01T00:00:00Z"));
    history.extend(timeout(2, 1, 30, "2099-01-01T00:00:00Z"));
    assert_eq!(observe(history, 30), json!(FIRED));
    for time in [None, Some(Value::Null), Some(json!("bad")), Some(json!(42))] {
        let mut history = timeout(1, 0, 30, FIRED);
        match time {
            Some(value) => {
                history[2].raw.insert("recorded_at".into(), value);
            }
            None => {
                history[2].raw.remove("recorded_at");
            }
        }
        history[2].raw.insert("created_at".into(), json!(FIRED));
        history[2].raw.insert("timestamp".into(), json!(FIRED));
        assert!(observe(history, 30).is_null());
    }
}

#[test]
fn replay_now_condition_timeout_requires_complete_native_definition_and_explicit_counters() {
    let original = timeout(1, 0, 30, FIRED);
    for index in [1, 2] {
        for field in [
            "condition_wait_occurrence_id",
            "condition_key",
            "condition_definition_fingerprint",
        ] {
            for replacement in [None, Some(json!("foreign")), Some(Value::Null)] {
                let mut history = original.clone();
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
                assert!(observe(history, 30).is_null());
            }
        }
        for sequence in [json!("1"), json!(2)] {
            let mut history = original.clone();
            history[index].payload["sequence"] = sequence;
            assert!(observe(history, 30).is_null());
        }
        let mut history = original.clone();
        history[index].payload["workflow_sequence"] = json!(2);
        assert!(observe(history, 30).is_null());
    }
    let mut history = original;
    history[0].payload["sequence"] = json!("1");
    assert!(observe(history, 30).is_null());
}

#[test]
fn replay_now_condition_timeout_preserves_legacy_timer_identity_and_duplicate_refusals() {
    for mutation in ["duplicate", "timer_id", "delay"] {
        let mut history = timeout(1, 0, 30, FIRED);
        match mutation {
            "duplicate" => history.push(history[2].clone()),
            "timer_id" => history[2].payload["timer_id"] = json!("foreign"),
            "delay" => history[2].payload["delay_seconds"] = json!(31),
            _ => unreachable!(),
        }
        assert!(matches!(
            WorkflowState::new(history, "clock-tests".into(), DEFAULT_CODEC.into(), None),
            Err(Error::NonDeterministicReplay(_))
        ));
    }
}

#[test]
fn replay_now_condition_timeout_refuses_cancelled_aliased_and_out_of_order_evidence() {
    let original = timeout(1, 0, 30, FIRED);
    let mut history = original.clone();
    let mut cancel = history[2].clone();
    cancel.event_type = "TimerCancelled".into();
    history.push(cancel);
    assert!(observe(history, 30).is_null());
    let mut history = original.clone();
    let mut alias = history[2].clone();
    alias.payload["condition_wait_id"] = json!("foreign");
    history.push(alias);
    assert!(observe(history, 30).is_null());
    let mut history = original.clone();
    history.swap(1, 2);
    assert!(observe(history, 30).is_null());
    let mut history = original;
    let mut other = history[0].clone();
    other.payload["sequence"] = json!(2);
    other.payload["condition_wait_occurrence_id"] = json!("rust:condition-wait:1");
    history.push(other);
    assert!(observe(history, 30).is_null());
}

#[test]
fn replay_now_condition_timeout_uses_final_physical_wait_not_an_orphaned_early_timeout() {
    let first = timeout(1, 0, 30, "2099-01-01T00:00:00Z");
    let second = timeout(2, 0, 20, FIRED);
    let history = vec![
        first[0].clone(),
        first[1].clone(),
        second[0].clone(),
        first[2].clone(),
        second[1].clone(),
        second[2].clone(),
    ];
    assert_eq!(observe(history.clone(), 30), json!(FIRED));
    let ctx = context(history[..4].to_vec());
    let inner = ctx.clone();
    let mut future: WorkflowFuture = Box::pin(async move {
        inner.wait_condition(options(30), || Ok(false)).await?;
        Ok(AvroValue::Null)
    });
    assert!(Worker::poll_workflow_future(&ctx, &mut future)
        .unwrap()
        .is_pending());
    let _guard = Guard::enter(&ctx);
    assert!(ctx.replay_now().is_err());
}

#[test]
fn replay_now_condition_timeout_group_stays_unavailable_after_actual_consumption() {
    let mut history = timeout(1, 0, 30, FIRED);
    for event in &mut history {
        crate::apply_parallel_group_path(
            event.payload.as_object_mut().unwrap(),
            &[crate::parallel_group_entry(1, 1, 0, "condition")],
        );
    }
    let ctx = context(history);
    let inner = ctx.clone();
    ready(
        &ctx,
        Box::pin(async move {
            inner
                .parallel(vec![crate::ParallelOperation::condition(
                    options(30),
                    || Ok(false),
                )])
                .await?;
            assert!(inner.replay_now().is_err());
            Ok(AvroValue::Null)
        }),
    );
}

#[tokio::test]
async fn replay_now_condition_fired_only_does_not_grant_snapshot_or_reconstructed_query_time() {
    let mut worker = Worker::new(Client::new("http://127.0.0.1:8080").unwrap(), "clock-tests");
    worker.register_replayed_workflow(
        "timeout-clock",
        || 0_u64,
        |ctx, _, _| async move {
            ctx.wait_condition(options(30), || Ok(false)).await?;
            assert!(ctx.replay_now().is_err());
            Ok(Value::Null)
        },
    );
    worker.register_query("timeout-clock", "snapshot", |ctx, _| async move {
        Ok(json!(ctx.replay_now().is_err()))
    });
    worker.register_replayed_query::<u64, _, _>(
        "timeout-clock",
        "reconstructed",
        |ctx, _, _| async move { Ok(json!(ctx.replay_now().is_err())) },
    );
    for query in ["snapshot", "reconstructed"] {
        let task:QueryTask=serde_json::from_value(json!({"query_task_id":"timeout-q","workflow_type":"timeout-clock","query_name":query,"payload_codec":DEFAULT_CODEC,"history_events":timeout(1,0,30,FIRED)})).unwrap();
        assert_eq!(
            worker
                .execute_query_task(task)
                .await
                .unwrap()
                .into_json()
                .unwrap(),
            json!(true)
        );
    }
    assert_eq!(observe(timeout(1, 0, 30, FIRED), 30), json!(FIRED));
}
