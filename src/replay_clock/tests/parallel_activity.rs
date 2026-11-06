use super::*;
use crate::{ParallelOperation, ParallelResult};

fn operations(nested: bool) -> Vec<ParallelOperation> {
    let a = ParallelOperation::activity("clock-a", json!([]));
    let b = ParallelOperation::activity("clock-b", json!([]));
    if nested {
        vec![a, ParallelOperation::group(vec![b])]
    } else {
        vec![a, b]
    }
}

fn group(base: u64, kinds: [&str; 2], times: [&str; 2], nested: bool) -> Vec<HistoryEvent> {
    let descriptors = crate::parallel_descriptors(operations(nested), base).unwrap();
    let mut schedules = Vec::new();
    let mut terminals = Vec::new();
    for (index, descriptor) in descriptors.iter().enumerate() {
        let sequence = base + index as u64;
        let mut payload = json!({"sequence":sequence,"activity_execution_id":format!("execution-{sequence}"),"activity_type":if index==0 {"clock-a"} else {"clock-b"}});
        crate::apply_parallel_group_path(payload.as_object_mut().unwrap(), &descriptor.group_path);
        schedules.push(event("ActivityScheduled", payload.clone(), START));
        if !kinds[index].is_empty() {
            if kinds[index] == "ActivityCompleted" {
                payload["result"] = encode_typed_envelope(
                    &AvroValue::String(format!("result-{index}")),
                    DEFAULT_CODEC,
                )
                .unwrap();
            } else {
                payload["message"] = json!(format!("failed-{index}"));
            }
            terminals.push(event(kinds[index], payload, times[index]));
        }
    }
    schedules.extend(terminals);
    schedules
}

fn observe(history: Vec<HistoryEvent>, nested: bool) -> Value {
    let ctx = context(history);
    let inner = ctx.clone();
    let output = ready(&ctx,Box::pin(async move {
        let result = inner.parallel(operations(nested)).await;
        let failed_index = match result {
            Ok(results) => {
                assert_eq!(results[0], ParallelResult::Activity(json!("result-0")));
                None
            },
            Err(Error::ParallelFailed(failure)) => Some(failure.member_path),
            other => panic!("unexpected group outcome: {other:?}"),
        };
        let time=inner.replay_now().ok();
        assert_eq!(time,inner.replay_now().ok());
        AvroValue::from_serialize(&json!({"time":time.map(|t|t.to_rfc3339_opts(chrono::SecondsFormat::Micros,true)),"failed":failed_index}))
    })).into_json().unwrap();
    assert!(ctx.take_commands().unwrap().is_empty());
    assert!(ctx.replay_now().is_err());
    output
}

#[test]
fn replay_now_group_success_uses_core_ms_comparator_first_tie_and_full_precision() {
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
                ["2026-01-01T00:00:05.123400Z", "2026-01-01T00:00:03.123499Z"],
                "2026-01-01T00:00:05.123400Z",
            ),
        ] {
            let mut history = vec![start()];
            history.extend(group(1, ["ActivityCompleted"; 2], times, nested));
            // Stream order is not descriptor order or business-time order.
            history.swap(3, 4);
            let first = observe(history.clone(), nested);
            assert_eq!(first["time"], json!(expected));
            assert_eq!(first, observe(history, nested));
        }
    }
}

#[test]
fn replay_now_group_failure_preserves_legacy_winner_and_core_ms_reconstruction() {
    for kind in ["ActivityFailed", "ActivityTimedOut", "ActivityCancelled"] {
        let history = group(
            1,
            [kind, "ActivityCompleted"],
            ["2026-01-01T00:00:05.123500Z", "2099-01-01T00:00:00Z"],
            false,
        );
        let output = observe(history.clone(), false);
        assert_eq!(output["time"], json!("2026-01-01T00:00:05.124000Z"));
        assert_eq!(output["failed"], json!([0]));
        assert_eq!(output, observe(history, false));
    }
    for (times, expected) in [
        (
            ["1970-01-01T00:00:00.000500Z", "1970-01-01T00:00:00.001000Z"],
            Some("1970-01-01T00:00:00.001000Z"),
        ),
        (
            ["1969-12-31T23:59:59.999500Z", "1970-01-01T00:00:00Z"],
            Some("1969-12-31T23:59:59.999000Z"),
        ),
        (["2026-01-01T00:00:10Z", "2026-01-01T00:00:05Z"], None),
    ] {
        let output = observe(group(1, ["ActivityFailed"; 2], times, false), false);
        assert_eq!(
            output["failed"],
            json!([0]),
            "clock must not rewrite legacy failure selection"
        );
        assert_eq!(output["time"], json!(expected));
    }
}

#[test]
fn replay_now_group_time_is_scoped_to_consumed_group_not_future_or_reuse() {
    let mut history = group(
        1,
        ["ActivityCompleted"; 2],
        ["2026-01-01T00:00:05.123100Z", "2026-01-01T00:00:05.123400Z"],
        false,
    );
    for event in &mut history[2..] {
        event.payload["reused_recorded_at"] = json!("2099-01-01T00:00:00Z");
    }
    history.extend(group(
        3,
        ["ActivityCompleted"; 2],
        ["2099-01-01T00:00:00Z"; 2],
        false,
    ));
    assert_eq!(
        observe(history, false)["time"],
        json!("2026-01-01T00:00:05.123100Z")
    );
}

#[test]
fn replay_now_group_requires_unique_numeric_correlated_lifecycle_even_if_legacy_result_survives() {
    let original = group(1, ["ActivityCompleted"; 2], [START; 2], false);
    for index in 0..4 {
        for field in ["activity_execution_id", "activity_type"] {
            let mut history = original.clone();
            history[index]
                .payload
                .as_object_mut()
                .unwrap()
                .remove(field);
            // Legacy can infer a missing identity from the other lifecycle row.
            assert!(observe(history, false)["time"].is_null());
        }
        let mut history = original.clone();
        history[index].payload["sequence"] = json!(if index % 2 == 0 { "1" } else { "2" });
        assert!(observe(history, false)["time"].is_null());
        let mut history = original.clone();
        history[index].payload["workflow_sequence"] = json!(99);
        assert!(observe(history, false)["time"].is_null());
    }
    let mut history = original.clone();
    let mut duplicate = history[2].clone();
    duplicate.payload["sequence"] = json!("1");
    history.push(duplicate);
    // String and numeric counters share the legacy index. Neither may hide a
    // duplicate terminal row from the stricter business clock.
    assert!(WorkflowState::new(history, "clock-tests".into(), DEFAULT_CODEC.into(), None).is_err());
    let mut history = original.clone();
    history.push(history[2].clone());
    assert!(observe(history, false)["time"].is_null());
    let mut history = original;
    history.swap(0, 2);
    assert!(observe(history, false)["time"].is_null());
}

#[test]
fn replay_now_group_requires_all_success_times_and_never_substitutes_other_authority() {
    for value in [
        None,
        Some(Value::Null),
        Some(json!("invalid")),
        Some(json!(42)),
    ] {
        let mut history = group(1, ["ActivityCompleted"; 2], [START; 2], false);
        match value {
            Some(value) => {
                history[2].raw.insert("recorded_at".into(), value);
            }
            None => {
                history[2].raw.remove("recorded_at");
            }
        }
        history[2].payload["reused_recorded_at"] = json!(START);
        assert!(observe(history, false)["time"].is_null());
    }
    let mut history = group(1, ["ActivityCompleted"; 2], [START; 2], false);
    history[2].raw.remove("recorded_at");
    history[2]
        .raw
        .insert("created_at".into(), json!("2026-01-01T00:00:10.123456Z"));
    assert_eq!(
        observe(history, false)["time"],
        json!("2026-01-01T00:00:10.123456Z")
    );
    for time in ["2026-01-01T00:00:00.123456789Z"] {
        assert!(
            observe(group(1, ["ActivityCompleted"; 2], [time; 2], false), false)["time"].is_null()
        );
    }
}

#[test]
fn replay_now_group_pending_and_empty_paths_do_not_invent_an_aggregate() {
    let ctx = context(group(1, ["ActivityCompleted", ""], [START; 2], false));
    let inner = ctx.clone();
    let mut future: WorkflowFuture = Box::pin(async move {
        inner.parallel(operations(false)).await?;
        Ok(AvroValue::Null)
    });
    assert!(Worker::poll_workflow_future(&ctx, &mut future)
        .unwrap()
        .is_pending());
    let _guard = Guard::enter(&ctx);
    assert!(ctx.replay_now().is_err());
    drop(_guard);
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
async fn replay_now_group_query_does_not_borrow_executor_aggregate() {
    let mut worker = Worker::new(Client::new("http://127.0.0.1:8080").unwrap(), "clock-tests");
    worker.register_replayed_workflow(
        "group-clock",
        || 0_u64,
        |ctx, _, _| async move {
            ctx.parallel(operations(false)).await?;
            assert!(
                ctx.replay_now().is_err(),
                "query reconstruction has no Core group send time"
            );
            ctx.sleep(Duration::from_secs(1)).await?;
            Ok(Value::Null)
        },
    );
    worker.register_query("group-clock", "snapshot", |ctx, _| async move {
        Ok(json!(ctx.replay_now().is_err()))
    });
    worker.register_replayed_query::<u64, _, _>(
        "group-clock",
        "reconstructed",
        |ctx, _, _| async move { Ok(json!(ctx.replay_now().is_err())) },
    );
    let mut history = group(1, ["ActivityCompleted"; 2], [START; 2], false);
    for query in ["snapshot", "reconstructed"] {
        if query == "reconstructed" {
            history.push(event(
                "TimerScheduled",
                json!({"sequence":3,"timer_id":"later","delay_seconds":1}),
                "2099-01-01T00:00:00Z",
            ));
        }
        let task:QueryTask=serde_json::from_value(json!({"query_task_id":"group-q","workflow_type":"group-clock","query_name":query,"payload_codec":DEFAULT_CODEC,"history_events":history})).unwrap();
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
    // Query execution must restore the ordinary executor's ambient mode.
    assert!(
        observe(group(1, ["ActivityCompleted"; 2], [START; 2], false), false)["time"].is_string()
    );
}

#[test]
fn replay_now_group_carbon_reference_matches_measured_rounding_and_reconstruction() {
    // These attributed PHP 8.4/Carbon observations coincide with v1 on these
    // inputs. They are not universal runtime or producer-context authority.
    let reference: Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/replay-clock-carbon-reference.json"
    ))
    .unwrap();
    for row in reference["vectors"].as_array().unwrap() {
        let time = DateTime::parse_from_rfc3339(row["input"].as_str().unwrap())
            .unwrap()
            .with_timezone(&Utc);
        let ms = super::super::parallel::milliseconds(time).unwrap();
        assert_eq!(json!(ms), row["milliseconds"]);
        assert_eq!(
            DateTime::<Utc>::from_timestamp_millis(ms)
                .unwrap()
                .to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
            row["reconstructed"].as_str().unwrap()
        );
    }
}

#[test]
fn replay_now_group_refuses_reused_execution_identity_and_keeps_legacy_group_shape_checks() {
    let mut history = group(1, ["ActivityCompleted"; 2], [START; 2], false);
    for event in &mut history {
        event.payload["activity_execution_id"] = json!("one-foreign-execution");
    }
    assert!(observe(history, false)["time"].is_null());
    let mut history = group(1, ["ActivityCompleted"; 2], [START; 2], false);
    history[2].payload["activity_execution_id"] = json!("wrong-execution");
    assert!(observe(history, false)["time"].is_null());
    let mut history = group(1, ["ActivityCompleted"; 2], [START; 2], false);
    for event in &mut history {
        event.payload["parallel_group_index"] = json!(0);
        event.payload["parallel_group_path"][0]["parallel_group_index"] = json!(0);
    }
    assert!(matches!(
        WorkflowState::new(history, "clock-tests".into(), DEFAULT_CODEC.into(), None),
        Err(Error::NonDeterministicReplay(_))
    ));
}

#[test]
fn replay_now_mixed_activity_timer_group_does_not_borrow_activity_only_aggregation() {
    let operations = || {
        vec![
            ParallelOperation::activity("clock-a", json!([])),
            ParallelOperation::timer(Duration::from_secs(1)),
        ]
    };
    let descriptors = crate::parallel_descriptors(operations(), 1).unwrap();
    let mut history = group(1, ["ActivityCompleted"; 2], [START; 2], false);
    for index in [0, 2] {
        crate::apply_parallel_group_path(
            history[index].payload.as_object_mut().unwrap(),
            &descriptors[0].group_path,
        );
    }
    for (index, kind) in [(1, "TimerScheduled"), (3, "TimerFired")] {
        history[index] = event(
            kind,
            json!({"sequence":2,"timer_id":"mixed-timer","delay_seconds":1}),
            START,
        );
        crate::apply_parallel_group_path(
            history[index].payload.as_object_mut().unwrap(),
            &descriptors[1].group_path,
        );
    }
    let ctx = context(history);
    let inner = ctx.clone();
    ready(
        &ctx,
        Box::pin(async move {
            assert_eq!(inner.parallel(operations()).await?.len(), 2);
            assert!(inner.replay_now().is_err());
            Ok(AvroValue::Null)
        }),
    );
}
