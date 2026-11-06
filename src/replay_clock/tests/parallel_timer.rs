use super::*;
use crate::{ParallelOperation, ParallelResult};

fn operations(nested: bool) -> Vec<ParallelOperation> {
    let timer = || ParallelOperation::timer(Duration::from_secs(1));
    if nested {
        vec![timer(), ParallelOperation::group(vec![timer()])]
    } else {
        vec![timer(), timer()]
    }
}

fn group(base: u64, times: [&str; 2], nested: bool) -> Vec<HistoryEvent> {
    let descriptors = crate::parallel_descriptors(operations(nested), base).unwrap();
    let mut schedules = Vec::new();
    let mut fires = Vec::new();
    for (index, descriptor) in descriptors.iter().enumerate() {
        let sequence = base + index as u64;
        let mut payload = json!({"sequence":sequence,"timer_id":format!("timer-{sequence}"),"delay_seconds":1,"fire_at":"2099-01-01T00:00:00Z"});
        crate::apply_parallel_group_path(payload.as_object_mut().unwrap(), &descriptor.group_path);
        schedules.push(event("TimerScheduled", payload.clone(), START));
        payload["fired_at"] = json!("2099-01-01T00:00:01Z");
        fires.push(event("TimerFired", payload, times[index]));
    }
    schedules.extend(fires);
    schedules
}

fn observe(history: Vec<HistoryEvent>, nested: bool) -> Value {
    let ctx = context(history);
    let inner = ctx.clone();
    let output = ready(
        &ctx,
        Box::pin(async move {
            let values = inner.parallel(operations(nested)).await?;
            assert_eq!(values[0], ParallelResult::Timer);
            assert_eq!(
                values[1],
                if nested {
                    ParallelResult::Group(vec![ParallelResult::Timer])
                } else {
                    ParallelResult::Timer
                }
            );
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
fn replay_now_timer_all_uses_descriptor_order_core_ms_ties_and_full_precision_on_cold_replay() {
    for nested in [false, true] {
        for (times, expected) in [
            (
                ["2026-01-01T00:00:05.123100Z", "2026-01-01T00:00:05.123400Z"],
                "2026-01-01T00:00:05.123100Z",
            ),
            (
                ["2026-01-01T00:00:05.123499Z", "2026-01-01T00:00:05.123500Z"],
                "2026-01-01T00:00:05.123500Z",
            ),
            (
                ["1969-12-31T23:59:59.999500Z", "1969-12-31T23:59:59.999501Z"],
                "1969-12-31T23:59:59.999501Z",
            ),
            (
                ["2025-12-31T23:59:59.654321Z", "2025-12-31T23:59:58.654321Z"],
                "2025-12-31T23:59:59.654321Z",
            ),
        ] {
            let mut history = vec![start()];
            history.extend(group(1, times, nested));
            history.swap(3, 4);
            assert_eq!(observe(history.clone(), nested), json!(expected));
            assert_eq!(observe(history.clone(), nested), observe(history, nested));
        }
    }
}

#[test]
fn replay_now_timer_all_never_borrows_reuse_payload_or_other_groups_future_times() {
    let mut history = group(1, [START; 2], false);
    for event in &mut history[2..] {
        event.payload["reused_recorded_at"] = json!("2099-01-01T00:00:00Z");
        event
            .raw
            .insert("timestamp".into(), json!("2099-01-01T00:00:00Z"));
    }
    history.extend(group(3, ["invalid-future-time"; 2], false));
    assert_eq!(observe(history, false), json!(START));
}

#[test]
fn replay_now_timer_all_missing_or_invalid_time_has_no_scalar_fallback() {
    for value in [None, Some(Value::Null), Some(json!("bad")), Some(json!(42))] {
        let mut history = group(1, [START; 2], false);
        match value {
            Some(value) => {
                history[2].raw.insert("recorded_at".into(), value);
            }
            None => {
                history[2].raw.remove("recorded_at");
            }
        }
        history[2].payload["reused_recorded_at"] = json!(START);
        assert!(observe(history, false).is_null());
    }
    let mut history = group(1, [START; 2], false);
    history[2].raw.remove("recorded_at");
    history[2]
        .raw
        .insert("created_at".into(), json!("2026-01-01T00:00:10.654321Z"));
    assert_eq!(
        observe(history, false),
        json!("2026-01-01T00:00:10.654321Z")
    );
    for time in ["2500-01-01T00:00:00Z", "2026-01-01T00:00:00.123456789Z"] {
        assert!(observe(group(1, [time; 2], false), false).is_null());
    }
}

#[test]
fn replay_now_timer_all_requires_typed_counter_unique_identity_and_complete_plain_timer_evidence() {
    let original = group(1, [START; 2], false);
    for index in 0..4 {
        let sequence = if index % 2 == 0 { 1 } else { 2 };
        let mut history = original.clone();
        history[index].payload["sequence"] = json!(sequence.to_string());
        assert!(observe(history, false).is_null());
        let mut history = original.clone();
        history[index]
            .payload
            .as_object_mut()
            .unwrap()
            .remove("sequence");
        history[index]
            .raw
            .insert("sequence".into(), json!(sequence));
        assert!(observe(history, false).is_null());
        let mut history = original.clone();
        history[index].payload["workflow_sequence"] = json!(99);
        assert!(observe(history, false).is_null());
        let mut history = original.clone();
        history[index].payload["timer_kind"] = json!("unknown-kind");
        assert!(observe(history, false).is_null());
    }
    let mut history = original.clone();
    for event in &mut history {
        event.payload["timer_id"] = json!("one-aliased-timer");
    }
    assert!(observe(history, false).is_null());
    let mut history = original.clone();
    let mut cancel = history[2].clone();
    cancel.event_type = "TimerCancelled".into();
    history.push(cancel);
    assert!(observe(history, false).is_null());
    let mut history = original;
    history.swap(0, 2);
    assert!(observe(history, false).is_null());
}

#[test]
fn replay_now_timer_all_keeps_legacy_duplicate_and_mismatched_timer_refusals() {
    for change in ["duplicate", "identity", "delay"] {
        let mut history = group(1, [START; 2], false);
        match change {
            "duplicate" => history.push(history[2].clone()),
            "identity" => history[2].payload["timer_id"] = json!("foreign"),
            "delay" => history[2].payload["delay_seconds"] = json!(2),
            _ => unreachable!(),
        }
        assert!(matches!(
            WorkflowState::new(history, "clock-tests".into(), DEFAULT_CODEC.into(), None),
            Err(Error::NonDeterministicReplay(_))
        ));
    }
}

#[test]
fn replay_now_pending_timer_group_has_no_aggregate_and_empty_group_keeps_previous_prefix() {
    let mut history = group(1, [START; 2], false);
    history.pop();
    let ctx = context(history);
    let inner = ctx.clone();
    let mut future: WorkflowFuture = Box::pin(async move {
        inner.parallel(operations(false)).await?;
        Ok(AvroValue::Null)
    });
    assert!(Worker::poll_workflow_future(&ctx, &mut future)
        .unwrap()
        .is_pending());
    {
        let _guard = Guard::enter(&ctx);
        assert!(ctx.replay_now().is_err());
    }
    let ctx = context(vec![start()]);
    let inner = ctx.clone();
    ready(
        &ctx,
        Box::pin(async move {
            inner.parallel(vec![]).await?;
            assert_eq!(read(&inner), START);
            Ok(AvroValue::Null)
        }),
    );
}

#[tokio::test]
async fn replay_now_timer_groups_do_not_supply_snapshot_or_reconstructed_query_time() {
    let mut worker = Worker::new(Client::new("http://127.0.0.1:8080").unwrap(), "clock-tests");
    worker.register_replayed_workflow(
        "timer-group-clock",
        || 0_u64,
        |ctx, _, _| async move {
            ctx.parallel(operations(false)).await?;
            assert!(ctx.replay_now().is_err());
            ctx.sleep(Duration::from_secs(1)).await?;
            Ok(Value::Null)
        },
    );
    worker.register_query("timer-group-clock", "snapshot", |ctx, _| async move {
        Ok(json!(ctx.replay_now().is_err()))
    });
    worker.register_replayed_query::<u64, _, _>(
        "timer-group-clock",
        "reconstructed",
        |ctx, _, _| async move { Ok(json!(ctx.replay_now().is_err())) },
    );
    let mut history = group(1, [START; 2], false);
    for query in ["snapshot", "reconstructed"] {
        if query == "reconstructed" {
            history.push(event(
                "TimerScheduled",
                json!({"sequence":3,"timer_id":"later","delay_seconds":1}),
                "2099-01-01T00:00:00Z",
            ));
        }
        let task:QueryTask=serde_json::from_value(json!({"query_task_id":"timer-group-q","workflow_type":"timer-group-clock","query_name":query,"payload_codec":DEFAULT_CODEC,"history_events":history})).unwrap();
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
    assert_eq!(observe(group(1, [START; 2], false), false), json!(START));
}
