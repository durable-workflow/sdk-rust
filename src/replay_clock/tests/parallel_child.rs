use super::*;
use crate::{ChildWorkflowOptions, ParallelOperation};

fn operations(nested: bool) -> Vec<ParallelOperation> {
    let child = || {
        ParallelOperation::child_workflow(
            "clock-child",
            ChildWorkflowOptions::new("children"),
            json!([]),
        )
    };
    if nested {
        vec![child(), ParallelOperation::group(vec![child()])]
    } else {
        vec![child(), child()]
    }
}

fn group(base: u64, kinds: [&str; 2], times: [&str; 2], nested: bool) -> Vec<HistoryEvent> {
    let descriptors = crate::parallel_descriptors(operations(nested), base).unwrap();
    let mut opens = Vec::new();
    let mut terminals = Vec::new();
    for (index, descriptor) in descriptors.iter().enumerate() {
        let sequence = base + index as u64;
        let mut payload = json!({"sequence":sequence,"child_call_id":format!("call-{sequence}"),"child_workflow_instance_id":format!("instance-{sequence}"),"child_workflow_run_id":format!("run-{sequence}"),"child_workflow_type":"clock-child"});
        crate::apply_parallel_group_path(payload.as_object_mut().unwrap(), &descriptor.group_path);
        opens.push(event("ChildWorkflowScheduled", payload.clone(), START));
        if !kinds[index].is_empty() {
            if kinds[index] == "ChildRunCompleted" {
                payload["result"] = encode_typed_envelope(
                    &AvroValue::String(format!("child-{index}")),
                    DEFAULT_CODEC,
                )
                .unwrap();
            } else {
                payload["message"] = json!(format!("failed-{index}"));
            }
            terminals.push(event(kinds[index], payload, times[index]));
        }
    }
    opens.extend(terminals);
    opens
}

fn observe(history: Vec<HistoryEvent>, nested: bool) -> Value {
    let ctx = context(history);
    let inner = ctx.clone();
    let output = ready(&ctx, Box::pin(async move {
        let failed = match inner.parallel(operations(nested)).await {
            Ok(values) => { assert_eq!(values.len(), 2); None },
            Err(Error::ParallelFailed(failure)) => Some(failure.member_path),
            other => panic!("unexpected child group result: {other:?}"),
        };
        let time = inner.replay_now().ok();
        assert_eq!(time,inner.replay_now().ok());
        AvroValue::from_serialize(&json!({"time":time.map(|time|time.to_rfc3339_opts(chrono::SecondsFormat::Micros,true)),"failed":failed}))
    })).into_json().unwrap();
    assert!(ctx.take_commands().unwrap().is_empty());
    assert!(ctx.replay_now().is_err());
    output
}

#[test]
fn replay_now_child_all_uses_consumed_descriptor_order_v1_ties_and_full_precision_on_cold_replay() {
    for nested in [false, true] {
        for (times, expected) in [
            (
                ["2026-01-01T00:00:05.123100Z", "2026-01-01T00:00:05.123400Z"],
                "2026-01-01T00:00:05.123100Z",
            ),
            (
                ["3000-01-01T00:00:05.123499Z", "3000-01-01T00:00:05.123500Z"],
                "3000-01-01T00:00:05.123500Z",
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
            history.extend(group(1, ["ChildRunCompleted"; 2], times, nested));
            history.swap(3, 4);
            let first = observe(history.clone(), nested);
            assert_eq!(first["time"], json!(expected));
            assert_eq!(first, observe(history, nested));
        }
    }
}

#[test]
fn replay_now_child_all_failure_preserves_legacy_member_and_only_agreed_v1_time() {
    for kind in ["ChildRunFailed", "ChildRunCancelled", "ChildRunTerminated"] {
        for sibling in ["ChildRunCompleted", ""] {
            let history = group(
                1,
                [kind, sibling],
                ["2026-01-01T00:00:05.123500Z", "invalid-unused-time"],
                false,
            );
            let output = observe(history.clone(), false);
            assert_eq!(output["failed"], json!([0]));
            assert_eq!(output["time"], json!("2026-01-01T00:00:05.124000Z"));
            assert_eq!(output, observe(history, false));
        }
    }
    for (times, expected) in [
        (
            ["1969-12-31T23:59:59.999500Z", "1970-01-01T00:00:00Z"],
            Some("1969-12-31T23:59:59.999000Z"),
        ),
        (["2026-01-01T00:00:10Z", "2026-01-01T00:00:05Z"], None),
    ] {
        let output = observe(group(1, ["ChildRunFailed"; 2], times, false), false);
        assert_eq!(output["failed"], json!([0]));
        assert_eq!(output["time"], json!(expected));
    }
}

#[test]
fn replay_now_child_all_uses_parent_resolution_not_reuse_alias_closed_or_future_group() {
    let mut history = group(1, ["ChildRunCompleted"; 2], [START; 2], false);
    for event in &mut history[2..] {
        event.payload["closed_at"] = json!("2098-01-01T00:00:00Z");
        event.payload["reused_recorded_at"] = json!("2099-01-01T00:00:00Z");
        event
            .raw
            .insert("timestamp".into(), json!("2099-01-01T00:00:00Z"));
    }
    history.extend(group(
        3,
        ["ChildRunCompleted"; 2],
        ["invalid-future-time"; 2],
        false,
    ));
    assert_eq!(observe(history, false)["time"], json!(START));
    for invalid in [
        None,
        Some(Value::Null),
        Some(json!(42)),
        Some(json!("invalid")),
    ] {
        let mut history = group(1, ["ChildRunCompleted"; 2], [START; 2], false);
        match invalid {
            Some(value) => {
                history[2].raw.insert("recorded_at".into(), value);
            }
            None => {
                history[2].raw.remove("recorded_at");
            }
        }
        history[2].payload["closed_at"] = json!(START);
        assert!(observe(history, false)["time"].is_null());
    }
    let mut history = group(1, ["ChildRunCompleted"; 2], [START; 2], false);
    history[2].raw.insert("recorded_at".into(), Value::Null);
    history[2]
        .raw
        .insert("created_at".into(), json!("2026-01-01T00:00:10.654321Z"));
    assert_eq!(
        observe(history, false)["time"],
        json!("2026-01-01T00:00:10.654321Z")
    );
    assert!(observe(
        group(
            1,
            ["ChildRunCompleted"; 2],
            ["2026-01-01T00:00:00.123456789Z"; 2],
            false
        ),
        false
    )["time"]
        .is_null());
}

#[test]
fn replay_now_child_all_requires_canonical_lifecycle_identities_typed_counters_and_unique_calls() {
    let original = group(1, ["ChildRunCompleted"; 2], [START; 2], false);
    for index in 0..4 {
        for field in [
            "child_call_id",
            "child_workflow_instance_id",
            "child_workflow_run_id",
            "child_workflow_type",
        ] {
            let mut history = original.clone();
            history[index]
                .payload
                .as_object_mut()
                .unwrap()
                .remove(field);
            assert!(observe(history, false)["time"].is_null(), "{index} {field}");
        }
        let sequence = if index % 2 == 0 { 1 } else { 2 };
        let mut history = original.clone();
        history[index].payload["sequence"] = json!(sequence.to_string());
        assert!(observe(history, false)["time"].is_null());
        let mut history = original.clone();
        history[index].payload["workflow_sequence"] = json!(99);
        assert!(observe(history, false)["time"].is_null());
    }
    let mut history = original.clone();
    for event in &mut history {
        event.payload["child_call_id"] = json!("one-aliased-call");
    }
    assert!(observe(history, false)["time"].is_null());
    let mut history = original;
    for event in &mut history {
        event.payload["child_workflow_instance_id"] = json!("shared-instance");
        event.payload["child_workflow_run_id"] = json!("shared-run");
    }
    assert_eq!(
        observe(history, false)["time"],
        json!(START),
        "distinct calls may reuse a run"
    );
}

#[test]
fn replay_now_child_all_refuses_duplicate_resolution_replacement_starts_and_wrong_order() {
    let original = group(1, ["ChildRunCompleted"; 2], [START; 2], false);
    let mut duplicate = original.clone();
    duplicate.push(duplicate[2].clone());
    assert!(observe(duplicate, false)["time"].is_null());
    let mut started = original.clone();
    let mut event = started[0].clone();
    event.event_type = "ChildRunStarted".into();
    started.insert(2, event.clone());
    assert_eq!(observe(started.clone(), false)["time"], json!(START));
    let mut changed = started.clone();
    changed[2].payload["child_workflow_run_id"] = json!("foreign-run");
    assert!(observe(changed, false)["time"].is_null());
    started.insert(3, event);
    assert!(observe(started, false)["time"].is_null());
    let mut reordered = original;
    reordered.swap(0, 2);
    assert!(observe(reordered, false)["time"].is_null());
}

#[test]
fn replay_now_child_all_partial_group_has_no_aggregate_time() {
    let mut history = vec![start()];
    history.extend(group(1, ["ChildRunCompleted", ""], [START; 2], false));
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
}

#[tokio::test]
async fn replay_now_child_groups_do_not_supply_query_time_and_restore_executor_scope() {
    let mut worker = Worker::new(Client::new("http://127.0.0.1:8080").unwrap(), "clock-tests");
    worker.register_replayed_workflow(
        "child-group-clock",
        || 0_u64,
        |ctx, _, _| async move {
            ctx.parallel(operations(false)).await?;
            assert!(ctx.replay_now().is_err());
            ctx.sleep(Duration::from_secs(1)).await?;
            Ok(Value::Null)
        },
    );
    worker.register_query("child-group-clock", "snapshot", |ctx, _| async move {
        Ok(json!(ctx.replay_now().is_err()))
    });
    worker.register_replayed_query::<u64, _, _>(
        "child-group-clock",
        "reconstructed",
        |ctx, _, _| async move { Ok(json!(ctx.replay_now().is_err())) },
    );
    let mut history = group(1, ["ChildRunCompleted"; 2], [START; 2], false);
    for query in ["snapshot", "reconstructed"] {
        if query == "reconstructed" {
            history.push(event(
                "TimerScheduled",
                json!({"sequence":3,"timer_id":"later","delay_seconds":1}),
                "2099-01-01T00:00:00Z",
            ));
        }
        let task:QueryTask=serde_json::from_value(json!({"query_task_id":"child-group-q","workflow_type":"child-group-clock","query_name":query,"payload_codec":DEFAULT_CODEC,"history_events":history})).unwrap();
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
    assert_eq!(
        observe(group(1, ["ChildRunCompleted"; 2], [START; 2], false), false)["time"],
        json!(START)
    );
}
