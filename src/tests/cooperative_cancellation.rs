use super::*;

fn context_snapshot() -> Value {
    serde_json::from_str(include_str!(
        "fixtures/cooperative-cancellation-context.json"
    ))
    .unwrap()
}

fn scoped_run_snapshot(name: &str) -> Value {
    let fixtures: Value = serde_json::from_str(include_str!(
        "fixtures/scoped-run-cancellation-context.json"
    ))
    .unwrap();
    fixtures[name].clone()
}

fn scoped_run_history() -> Vec<HistoryEvent> {
    vec![serde_json::from_value(json!({
        "event_type":"CooperativeCancellationRequested", "timestamp":"2026-10-04T00:00:05Z",
        "payload": {"workflow_run_id":"child-run", "workflow_instance_id":"child-instance",
            "workflow_command_id":"child-request", "reason":"maintenance",
            "cleanup_deadline_at":"2026-10-04T00:00:15.123456Z", "cancellation":scoped_run_snapshot("child")}
    })).unwrap(), remaining_event("CooperativeCancellationDelivered", json!({
        "workflow_run_id":"child-run", "workflow_command_id":"child-request", "sequence":1,
        "call_kind":"timer", "cancellation":scoped_run_snapshot("child")
    }), "2026-10-04T00:00:08Z")]
}

fn scoped_run_task(history: Vec<HistoryEvent>) -> WorkflowTask {
    let mut task = cancellation_task(history);
    task.run_id = Some("child-run".into());
    task
}

#[test]
fn scoped_run_native_child_and_grandchild_preserve_every_origin_and_original_budget() {
    for name in ["child", "grandchild"] {
        let snapshot = scoped_run_snapshot(name);
        let context = CancellationContext::from_value(&snapshot).unwrap();
        assert_eq!(context.to_value(), snapshot);
        assert_eq!(
            CancellationContext::from_value(&context.to_value()).unwrap(),
            context
        );
        assert_eq!(context.root_request_id(), "root-request");
        assert_eq!(context.reason(), Some("maintenance"));
        assert_eq!(context.source(), "api");
        assert_eq!(context.requester()["id"], "operator-1");
        assert_eq!(
            context
                .requested_at()
                .to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
            "2026-10-04T00:00:00.123456Z"
        );
        let origin = context.scope_origin().unwrap();
        assert_eq!(
            origin
                .root_deadline()
                .to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
            "2026-10-04T00:00:30.123456Z"
        );
        assert_eq!(origin.requested_at(), context.requested_at());
        let (scopes, parent, deadline) = if name == "child" {
            (
                vec!["outer", "inner"],
                "inner-request",
                "2026-10-04T00:00:15.123456Z",
            )
        } else {
            (
                vec!["outer", "inner", "root", "child-scope"],
                "child-scope-request",
                "2026-10-04T00:00:12.123456Z",
            )
        };
        assert_eq!(context.parent_request_id(), Some(parent));
        assert_eq!(
            origin
                .lineage()
                .iter()
                .map(|hop| hop.scope_id())
                .collect::<Vec<_>>(),
            scopes
        );
        assert_eq!(
            context
                .deadline()
                .to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
            deadline
        );
        assert!(matches!(
            context.remaining(),
            Err(Error::InvalidCooperativeCancellation(_))
        ));
    }
}

#[test]
fn scoped_run_avro_timezone_and_detached_metadata_keep_the_original_tree() {
    let original = scoped_run_snapshot("grandchild");
    let mut snapshot = original.clone();
    snapshot["requested_at"] = json!("2026-10-03T20:00:00.123456-04:00");
    snapshot["cleanup_deadline_at"] = json!("2026-10-03T20:00:12.123456-04:00");
    snapshot["scope_authority_deadline_at"] = snapshot["cleanup_deadline_at"].clone();
    let decoded = decode_wire_value(&fixture_envelope(snapshot), DEFAULT_CODEC).unwrap();
    let context = CancellationContext::from_value(&decoded).unwrap();
    assert_eq!(context.to_value(), original);
    let origin = context.scope_origin().unwrap();
    let mut detached = origin.to_value();
    detached["lineage"][3]["scope_id"] = json!("changed");
    assert_eq!(origin.scope_id(), "child-scope");
    assert_eq!(origin.root_scope_id(), "outer");
    assert_eq!(origin.request_id(), "child-scope-request");
    assert_eq!(origin.parent_request_id(), Some("child-request"));
    assert_eq!(origin.workflow_instance_id(), "child-instance");
    assert_eq!(origin.workflow_run_id(), "child-run");
    assert_eq!(origin.to_value(), original["scope_origin"]);
}

#[test]
fn scoped_run_cold_cleanup_replay_consumes_the_same_narrowed_original_clock() {
    let mut worker = cancellation_worker();
    worker.register_workflow("cancel", |ctx, _| async move {
        assert!(ctx.cancellation_context()?.is_none());
        let Err(Error::CooperativeCancellationRequested(cancelled)) = ctx.sleep(Duration::from_secs(10)).await else {
            panic!("expected original cancellation");
        };
        let context = cancelled.request.context.unwrap();
        assert_eq!(context.to_value(), scoped_run_snapshot("child"));
        assert_eq!(ctx.cancellation_context()?, Some(context.clone()));
        let before = context.remaining()?.as_secs_f64(); assert_eq!(before, 7.123456);
        let _shield = ctx.cancellation_shield()?;
        ctx.activity("cleanup", json!([])).await?;
        Ok(json!({"remaining":[before,context.remaining()?.as_secs_f64()], "cancellation":context.to_value()}))
    });
    let mut history = scoped_run_history();
    let initial = worker
        .execute_workflow_task(scoped_run_task(history.clone()))
        .unwrap();
    assert_eq!(initial.len(), 1);
    assert_eq!(initial[0]["activity_type"], "cleanup");
    let mut completion = completed_activity(2, "cleanup", json!("cleaned"));
    completion
        .last_mut()
        .unwrap()
        .raw
        .insert("timestamp".into(), json!("2026-10-04T00:00:12Z"));
    history.extend(completion);
    history.push(remaining_event(
        "WorkflowTaskScheduled",
        json!({}),
        "2026-10-04T00:00:29Z",
    ));
    for _replacement in 0..2 {
        let result = worker
            .execute_workflow_task(scoped_run_task(history.clone()))
            .unwrap();
        assert_eq!(result[0]["type"], "complete_workflow");
        assert_eq!(
            decode_wire_value(&result[0]["result"], DEFAULT_CODEC).unwrap(),
            json!({
                "remaining":[7.123456,3.123456],"cancellation":scoped_run_snapshot("child")
            })
        );
    }
}

#[test]
fn scoped_run_canonical_delivery_cannot_replace_an_original_scope() {
    let mut history = scoped_run_history();
    history[1].payload["cancellation"]["scope_origin"]["lineage"][1]["scope_id"] =
        json!("different");
    let mut worker = cancellation_worker();
    worker.register_workflow("cancel", |ctx, _| async move {
        ctx.sleep(Duration::from_secs(10)).await?;
        Ok(Value::Null)
    });
    assert!(worker
        .execute_workflow_task(scoped_run_task(history))
        .is_err());
}

#[test]
fn scoped_run_invalid_origin_identity_and_budget_are_refused() {
    let original = scoped_run_snapshot("grandchild");
    for (path, value) in [
        ("/schema", json!("durable-workflow.cancellation-context/v1")),
        ("/root_request_id", json!("other")),
        ("/root_workflow_instance_id", json!("other")),
        ("/root_workflow_run_id", json!("other")),
        ("/parent_request_id", json!("root-request")),
        ("/reason", json!("other")),
        ("/source", json!("other")),
        ("/requested_at", json!("2026-10-04T00:00:01.123456Z")),
        ("/scope_origin", json!([])),
        ("/cleanup_deadline_at", json!("2026-10-04T00:00:15.123456Z")),
        (
            "/scope_authority_deadline_at",
            json!("2026-10-04T00:00:15.123456Z"),
        ),
        ("/requester/id", json!("other")),
        ("/lineage/1/request_id", json!("child-request")),
        ("/lineage/2/workflow_run_id", json!("child-run")),
        (
            "/scope_origin/lineage/3/cleanup_deadline_at",
            json!("2026-10-04T00:00:16.123456Z"),
        ),
        (
            "/scope_origin/lineage/3/workflow_instance_id",
            json!("other"),
        ),
        ("/scope_origin/lineage/3/request_id", json!("inner-request")),
        ("/scope_origin/lineage/3/scope_id", json!("root")),
        (
            "/scope_origin/root_context/schema",
            json!("durable-workflow.cancellation-context/v2"),
        ),
    ] {
        let mut snapshot = original.clone();
        *snapshot.pointer_mut(path).unwrap() = value;
        assert!(
            matches!(
                CancellationContext::from_value(&snapshot),
                Err(Error::InvalidCooperativeCancellation(_))
            ),
            "{path}"
        );
    }
    for key in [
        "scope_origin",
        "scope_authority_deadline_at",
        "parent_request_id",
    ] {
        let mut snapshot = original.clone();
        snapshot.as_object_mut().unwrap().remove(key);
        assert!(
            CancellationContext::from_value(&snapshot).is_err(),
            "missing {key}"
        );
    }
    for fault in [
        "widened global budget",
        "reused request",
        "discarded root",
        "run reentry",
        "unsupported field",
    ] {
        let mut snapshot = original.clone();
        match fault {
            "widened global budget" => {
                snapshot["cleanup_deadline_at"] = json!("2026-10-04T00:00:30.123456Z");
                snapshot["scope_authority_deadline_at"] = snapshot["cleanup_deadline_at"].clone();
            }
            "reused request" => {
                snapshot["request_id"] = json!("inner-request");
                snapshot["lineage"][2]["request_id"] = json!("inner-request");
            }
            "discarded root" => {
                snapshot["scope_origin"]["lineage"]
                    .as_array_mut()
                    .unwrap()
                    .remove(0);
            }
            "run reentry" => {
                snapshot["scope_origin"]["lineage"][3]["workflow_run_id"] = json!("root-run");
                snapshot["scope_origin"]["lineage"][3]["workflow_instance_id"] =
                    json!("root-instance");
            }
            _ => snapshot["scope_origin"]["lineage"][3]["authority"] = json!("unrecorded"),
        }
        assert!(
            matches!(
                CancellationContext::from_value(&snapshot),
                Err(Error::InvalidCooperativeCancellation(_))
            ),
            "{fault}"
        );
    }
}

fn context_request() -> HistoryEvent {
    serde_json::from_value(json!({
        "event_type": "CooperativeCancellationRequested", "recorded_at": "2026-10-01T00:00:05Z",
        "payload": {
            "workflow_command_id": "request-1", "workflow_run_id": "run-1", "workflow_instance_id": "child-instance",
            "reason": "maintenance", "cleanup_deadline_at": "2026-10-01T00:00:30.123456Z",
            "cancellation": context_snapshot(),
        }
    })).unwrap()
}

fn context_delivery() -> HistoryEvent {
    event(
        "CooperativeCancellationDelivered",
        json!({
            "workflow_command_id": "request-1", "workflow_run_id": "run-1",
            "sequence": 1, "call_kind": "timer", "cancellation": context_snapshot(),
        }),
    )
}

fn context_observation() -> CancellationRequest {
    CancellationRequest::from_observation(&json!({
        "request_id": "request-1", "requested_at": "2026-10-01T00:00:05Z",
        "cleanup_deadline_at": "2026-10-01T00:00:30.123456Z",
        "history_refresh_page_token": "opaque-first-page", "cancellation": context_snapshot(),
    }))
    .unwrap()
}

fn context_task(events: Vec<HistoryEvent>) -> WorkflowTask {
    let mut task = cancellation_task(events);
    task.run_id = Some("run-1".into());
    task
}

fn remaining_delivery() -> HistoryEvent {
    let mut delivery = context_delivery();
    delivery
        .raw
        .insert("timestamp".into(), json!("2026-10-01T00:00:08Z"));
    delivery
}

fn remaining_event(kind: &str, payload: Value, time: &str) -> HistoryEvent {
    let mut result = event(kind, payload);
    result.raw.insert("timestamp".into(), json!(time));
    result
}

#[test]
fn cancellation_remaining_preserves_memo_decision_and_consumes_the_timer_on_cold_replay() {
    let mut worker = cancellation_worker();
    worker.register_workflow("cancel", |ctx, _| async move {
        let Err(Error::CooperativeCancellationRequested(cancelled)) =
            ctx.sleep(Duration::from_secs(10)).await
        else {
            panic!("expected delivery");
        };
        let context = cancelled.request.context.unwrap();
        assert_eq!(context.remaining()?, Duration::new(22, 123456000));
        assert_eq!(
            ctx.cancellation_context()?.unwrap().remaining()?,
            context.remaining()?
        );
        assert_eq!(
            CancellationContext::from_value(&context.to_value())?,
            context
        );
        let _shield = ctx.cancellation_shield()?;
        ctx.upsert_memo(json!({"phase": "cleanup"}))?;
        let delay = if context.remaining()? == Duration::new(22, 123456000) {
            1
        } else {
            2
        };
        ctx.sleep(Duration::from_secs(delay)).await?;
        Ok(json!(context.remaining()?.as_secs_f64()))
    });
    let initial = vec![context_request(), remaining_delivery()];
    let commands = worker
        .execute_workflow_task(context_task(initial.clone()))
        .unwrap();
    assert_eq!(commands[0]["type"], "upsert_memo");
    assert_eq!(commands[1]["delay_seconds"], 1);
    let mut history = initial;
    history.push(remaining_event(
        "MemoUpserted",
        json!({"sequence":2,"entries":commands[0]["entries"],"merged":commands[0]["entries"]}),
        "2026-10-01T00:00:12Z",
    ));
    history.push(event(
        "TimerScheduled",
        json!({"sequence":3,"timer_id":"cleanup-timer","delay_seconds":1}),
    ));
    // A later unrelated row must not move an earlier application decision.
    history.push(remaining_event(
        "WorkflowTaskScheduled",
        json!({}),
        "2026-10-01T00:00:29Z",
    ));
    assert!(worker
        .execute_workflow_task(context_task(history.clone()))
        .unwrap()
        .is_empty());
    history.push(remaining_event(
        "TimerFired",
        json!({"sequence":3,"timer_id":"cleanup-timer","delay_seconds":1}),
        "2026-10-01T00:00:25Z",
    ));
    for _restart in 0..2 {
        let commands = worker
            .execute_workflow_task(context_task(history.clone()))
            .unwrap();
        assert_eq!(
            decode_wire_value(&commands[0]["result"], DEFAULT_CODEC).unwrap(),
            json!(5.123456)
        );
    }
}

#[test]
fn cancellation_remaining_parallel_excludes_later_failure_siblings_and_keeps_skew_monotonic() {
    for failed in [true, false] {
        let mut worker = cancellation_worker();
        worker.register_workflow("cancel", |ctx, _| async move {
            let Err(Error::CooperativeCancellationRequested(cancelled)) =
                ctx.sleep(Duration::from_secs(10)).await
            else {
                panic!("expected delivery");
            };
            let context = cancelled.request.context.unwrap();
            let _shield = ctx.cancellation_shield()?;
            let outcome = ctx
                .parallel(vec![
                    ParallelOperation::activity("a", json!([])),
                    ParallelOperation::activity("b", json!([])),
                ])
                .await;
            Ok(json!({"failed":outcome.is_err(),"remaining":context.remaining()?.as_secs_f64()}))
        });
        let mut history = vec![context_request(), remaining_delivery()];
        let commands = worker
            .execute_workflow_task(context_task(history.clone()))
            .unwrap();
        let mut payloads = Vec::new();
        for (offset, command) in commands.iter().enumerate() {
            let mut payload = command.clone();
            payload.as_object_mut().unwrap().remove("type");
            payload["sequence"] = json!(2 + offset);
            history.push(event("ActivityScheduled", payload.clone()));
            payloads.push(payload);
        }
        if failed {
            payloads[0]["message"] = json!("failed");
            payloads[0]["exception_type"] = json!("ExampleFailure");
            history.push(remaining_event(
                "ActivityFailed",
                payloads[0].clone(),
                "2026-10-01T00:00:12Z",
            ));
            payloads[1]["result"] = fixture_envelope(Value::Null);
            history.push(remaining_event(
                "ActivityCompleted",
                payloads[1].clone(),
                "2026-10-01T00:00:25Z",
            ));
        } else {
            for (offset, time) in ["2026-10-01T00:00:25Z", "2026-10-01T00:00:20Z"]
                .into_iter()
                .enumerate()
            {
                payloads[offset]["result"] = fixture_envelope(Value::Null);
                history.push(remaining_event(
                    "ActivityCompleted",
                    payloads[offset].clone(),
                    time,
                ));
            }
        }
        for _restart in 0..2 {
            let commands = worker
                .execute_workflow_task(context_task(history.clone()))
                .unwrap();
            assert_eq!(
                decode_wire_value(&commands[0]["result"], DEFAULT_CODEC).unwrap(),
                json!({"failed":failed,"remaining":if failed {18.123456} else {5.123456}})
            );
        }
    }
}

#[test]
fn cancellation_remaining_refuses_detached_ended_and_missing_or_invalid_delivery_time() {
    assert!(CancellationContext::from_value(&context_snapshot())
        .unwrap()
        .remaining()
        .is_err());
    for time in [
        Value::Null,
        json!("tomorrow"),
        json!("2026-02-30T00:00:08Z"),
        json!("2026-10-01T00:00:08"),
    ] {
        let mut worker = cancellation_worker();
        worker.register_workflow("cancel", |ctx, _| async move {
            let Err(Error::CooperativeCancellationRequested(cancelled)) =
                ctx.sleep(Duration::from_secs(10)).await
            else {
                panic!("expected delivery");
            };
            assert!(cancelled.request.context.unwrap().remaining().is_err());
            Ok(Value::Null)
        });
        let mut delivery = context_delivery();
        delivery.raw.insert("timestamp".into(), time);
        assert_eq!(
            worker
                .execute_workflow_task(context_task(vec![context_request(), delivery]))
                .unwrap()[0]["type"],
            "complete_workflow"
        );
    }
    let captured = Arc::new(Mutex::new(None));
    let capture = captured.clone();
    let mut worker = cancellation_worker();
    worker.register_workflow("cancel", move |ctx, _| {
        let capture = capture.clone();
        async move {
            let Err(Error::CooperativeCancellationRequested(cancelled)) =
                ctx.sleep(Duration::from_secs(10)).await
            else {
                panic!("expected delivery");
            };
            let context = cancelled.request.context.unwrap();
            assert_eq!(context.remaining()?, Duration::new(22, 123456000));
            *capture.lock().unwrap() = Some(context);
            Ok(Value::Null)
        }
    });
    worker
        .execute_workflow_task(context_task(vec![context_request(), remaining_delivery()]))
        .unwrap();
    assert!(captured
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .remaining()
        .is_err());
}

#[test]
fn cancellation_remaining_refuses_missing_consumed_result_and_clamps_expiry_with_timezone_offsets()
{
    for time in [None, Some("2026-10-01T08:00:32+08:00")] {
        let mut worker = cancellation_worker();
        worker.register_workflow("cancel", |ctx, _| async move {
            let Err(Error::CooperativeCancellationRequested(cancelled)) =
                ctx.sleep(Duration::from_secs(10)).await
            else {
                panic!("expected delivery");
            };
            let context = cancelled.request.context.unwrap();
            let _shield = ctx.cancellation_shield()?;
            ctx.sleep(Duration::from_secs(1)).await?;
            Ok(json!(context
                .remaining()
                .map(|remaining| remaining.as_secs_f64())
                .ok()))
        });
        let mut fired = event(
            "TimerFired",
            json!({"sequence":2,"timer_id":"cleanup-timer","delay_seconds":1}),
        );
        if let Some(time) = time {
            fired.raw.insert("timestamp".into(), json!(time));
        }
        let history = vec![
            context_request(),
            remaining_delivery(),
            event(
                "TimerScheduled",
                json!({"sequence":2,"timer_id":"cleanup-timer","delay_seconds":1}),
            ),
            fired,
        ];
        let commands = worker.execute_workflow_task(context_task(history)).unwrap();
        assert_eq!(
            decode_wire_value(&commands[0]["result"], DEFAULT_CODEC).unwrap(),
            if time.is_some() {
                json!(0.0)
            } else {
                Value::Null
            }
        );
    }
}

#[test]
fn cancellation_remaining_selection_uses_winner_then_handle_and_first_cancel_receipt() {
    for cancel_slow in [false, true] {
        let mut worker = cancellation_worker();
        worker.register_workflow("cancel", |ctx, _| async move {
            let Err(Error::CooperativeCancellationRequested(cancelled)) = ctx.sleep(Duration::from_secs(10)).await else { panic!("expected delivery"); };
            let context = cancelled.request.context.unwrap();
            let _shield = ctx.cancellation_shield()?;
            let selection = ctx.select_keyed(vec![("slow", ParallelOperation::activity("slow", json!([]))), ("fast", ParallelOperation::activity("fast", json!([])))]).await?;
            let selected = context.remaining()?.as_secs_f64();
            selection.winner.await_result().await?;
            let winner = context.remaining()?.as_secs_f64();
            let result = selection.handle(&SelectionKey::from("slow")).unwrap().await_result().await;
            Ok(json!({"selected":selected,"winner":winner,"slow":context.remaining()?.as_secs_f64(),"cancelled":matches!(result, Err(Error::DurableOperationCancelled(_)))}))
        });
        let mut history = vec![context_request(), remaining_delivery()];
        let commands = worker
            .execute_workflow_task(context_task(history.clone()))
            .unwrap();
        assert_eq!(commands.len(), 2);
        let mut payloads = Vec::new();
        for (offset, command) in commands.into_iter().enumerate() {
            let mut payload = command;
            payload.as_object_mut().unwrap().remove("type");
            payload["sequence"] = json!(2 + offset);
            payload["activity_execution_id"] = json!(if offset == 0 {
                "activity-slow"
            } else {
                "activity-fast"
            });
            history.push(event("ActivityScheduled", payload.clone()));
            payloads.push(payload);
        }
        payloads[1]["result"] = fixture_envelope(json!("fast"));
        let mut completed = remaining_event(
            "ActivityCompleted",
            payloads[1].clone(),
            "2026-10-01T00:00:10Z",
        );
        completed.raw.insert("id".into(), json!("event-fast"));
        history.push(completed);
        history.push(remaining_event("SelectionResolved", json!({
            "selection_group_id":"select-calls:2:2","selection_group_base_sequence":2,"selection_group_size":2,
            "member_key":"fast","member_index":1,"member_base_sequence":3,"member_size":1,
            "operation_kind":"activity","operation_identity":"activity-fast","outcome":"completed",
            "resolution_event_id":"event-fast","resolution_event_type":"ActivityCompleted"
        }), "2026-10-01T00:00:12Z"));
        if cancel_slow {
            let receipt = json!({"selection_group_id":"select-calls:2:2","member_key":"slow","member_index":0,"member_base_sequence":2,"member_size":1,"operation_kind":"activity","operation_identity":"activity-slow"});
            history.push(remaining_event(
                "SelectionOperationCancelled",
                receipt.clone(),
                "2026-10-01T00:00:15Z",
            ));
            history.push(remaining_event(
                "SelectionOperationCancelled",
                receipt,
                "2026-10-01T00:00:28Z",
            ));
        } else {
            payloads[0]["result"] = fixture_envelope(json!("slow"));
            history.push(remaining_event(
                "ActivityCompleted",
                payloads[0].clone(),
                "2026-10-01T00:00:20Z",
            ));
        }
        for _restart in 0..2 {
            let commands = worker
                .execute_workflow_task(context_task(history.clone()))
                .unwrap();
            assert_eq!(
                decode_wire_value(&commands[0]["result"], DEFAULT_CODEC).unwrap(),
                json!({"selected":18.123456,"winner":18.123456,"slow":if cancel_slow {15.123456} else {10.123456},"cancelled":cancel_slow})
            );
        }
    }
}

#[test]
fn cancellation_remaining_parallel_consumes_the_final_physical_condition_wait() {
    let mut worker = cancellation_worker();
    worker.register_workflow("cancel", |ctx, _| async move {
        let Err(Error::CooperativeCancellationRequested(cancelled)) =
            ctx.sleep(Duration::from_secs(10)).await
        else {
            panic!("expected delivery");
        };
        let context = cancelled.request.context.unwrap();
        let _shield = ctx.cancellation_shield()?;
        ctx.parallel(vec![
            ParallelOperation::timer(Duration::from_secs(1)),
            ParallelOperation::condition(
                ConditionWaitOptions::new("ready", "sha256:ready"),
                || Ok(false),
            ),
        ])
        .await?;
        Ok(json!(context.remaining()?.as_secs_f64()))
    });
    let mut history = vec![context_request(), remaining_delivery()];
    let commands = worker
        .execute_workflow_task(context_task(history.clone()))
        .unwrap();
    let mut timer = commands[0].clone();
    timer.as_object_mut().unwrap().remove("type");
    timer["sequence"] = json!(2);
    timer["timer_id"] = json!("timer-2");
    history.push(event("TimerScheduled", timer.clone()));
    let mut condition = commands[1].clone();
    condition.as_object_mut().unwrap().remove("type");
    condition["sequence"] = json!(3);
    condition["condition_wait_id"] = json!("condition-3");
    history.push(event("ConditionWaitOpened", condition.clone()));
    history.push(remaining_event(
        "ConditionWaitSatisfied",
        condition.clone(),
        "2026-10-01T00:00:12Z",
    ));
    condition["sequence"] = json!(4);
    condition["condition_wait_id"] = json!("condition-4");
    history.push(event("ConditionWaitOpened", condition.clone()));
    history.push(remaining_event(
        "ConditionWaitSatisfied",
        condition,
        "2026-10-01T00:00:25Z",
    ));
    history.push(remaining_event("TimerFired", timer, "2026-10-01T00:00:10Z"));
    for _restart in 0..2 {
        let commands = worker
            .execute_workflow_task(context_task(history.clone()))
            .unwrap();
        assert_eq!(
            decode_wire_value(&commands[0]["result"], DEFAULT_CODEC).unwrap(),
            json!(5.123456)
        );
    }
}

#[test]
fn cancellation_remaining_cannot_escape_into_an_unrelated_nested_worker_poll() {
    let mut worker = cancellation_worker();
    worker.register_workflow("cancel", |ctx, _| async move {
        let Err(Error::CooperativeCancellationRequested(cancelled)) =
            ctx.sleep(Duration::from_secs(10)).await
        else {
            panic!("expected delivery");
        };
        let context = cancelled.request.context.unwrap();
        let nested_context = context.clone();
        let mut nested = cancellation_worker();
        nested.register_workflow("cancel", move |_, _| {
            let nested_context = nested_context.clone();
            async move {
                assert!(
                    nested_context.remaining().is_err(),
                    "another workflow cannot borrow the outer replay binding"
                );
                Ok(Value::Null)
            }
        });
        nested.execute_workflow_task(context_task(Vec::new()))?;
        assert_eq!(
            context.remaining()?,
            Duration::new(22, 123456000),
            "outer binding is restored after nested poll"
        );
        Ok(Value::Null)
    });
    worker
        .execute_workflow_task(context_task(vec![context_request(), remaining_delivery()]))
        .unwrap();
}

#[test]
fn cancellation_remaining_refuses_a_legacy_signal_without_committed_result_time() {
    let mut worker = cancellation_worker();
    worker.register_workflow("cancel", |ctx, _| async move {
        let _ = ctx.sleep(Duration::from_secs(10)).await;
        let context = ctx.cancellation_context()?.unwrap();
        assert_eq!(context.remaining()?, Duration::new(22, 123456000));
        let _shield = ctx.cancellation_shield()?;
        assert_eq!(ctx.wait_signal("resume").await?, vec![json!(7)]);
        assert!(
            context.remaining().is_err(),
            "missing signal time cannot reuse the delivery budget"
        );
        Ok(Value::Null)
    });
    let mut task = context_task(vec![context_request(), remaining_delivery()]);
    task.signal_name = Some("resume".into());
    task.signal_arguments = Some(fixture_envelope(json!([7])));
    worker.execute_workflow_task(task).unwrap();
}

#[test]
fn cooperative_context_retains_original_snapshot_and_detached_copies() {
    let mut original = context_snapshot();
    let context = CancellationContext::from_value(&original).unwrap();
    original["requester"]["id"] = json!("changed");
    original["lineage"][0]["request_id"] = json!("changed");
    let mut detached = context.to_value();
    detached["reason"] = json!("changed");
    assert_eq!(context.to_value(), context_snapshot());
    assert_eq!(context.request_id(), "request-1");
    assert_eq!(context.root_request_id(), "root-1");
    assert_eq!(context.parent_request_id(), Some("root-1"));
    assert_eq!(context.root_workflow_instance_id(), "parent-instance");
    assert_eq!(context.root_workflow_run_id(), "parent-run");
    assert_eq!(context.reason(), Some("maintenance"));
    assert_eq!(context.source(), "control_plane");
    assert_eq!(context.requester()["id"], "operator-1");
    assert_eq!(
        context.lineage()[1].workflow_instance_id(),
        "child-instance"
    );
    assert_eq!(
        context.deadline() - context.requested_at(),
        chrono::Duration::seconds(30)
    );
}

#[test]
fn cooperative_context_normalizes_equivalent_timezone_and_object_order() {
    let mut value = context_snapshot();
    value["requested_at"] = json!("2026-09-30T20:00:00.123456-04:00");
    value["cleanup_deadline_at"] = json!("2026-09-30T20:00:30.123456-04:00");
    assert_eq!(
        CancellationContext::from_value(&value).unwrap(),
        CancellationContext::from_value(&context_snapshot()).unwrap()
    );
}

#[test]
fn cooperative_context_rejects_invalid_snapshots() {
    for (field, value) in [
        ("schema", json!("unknown")),
        ("request_id", json!("")),
        ("source", json!(" ")),
        ("parent_request_id", json!("wrong")),
        ("root_request_id", json!("wrong")),
        ("root_workflow_run_id", json!("wrong")),
        ("reason", json!([])),
        ("requester", json!([])),
        ("lineage", json!([])),
        ("requested_at", json!("2026-02-30T00:00:00Z")),
        ("cleanup_deadline_at", json!("2026-10-01T00:00:00.123456Z")),
    ] {
        let mut snapshot = context_snapshot();
        snapshot[field] = value;
        assert!(
            CancellationContext::from_value(&snapshot).is_err(),
            "{field}"
        );
    }
}

#[test]
fn cooperative_context_rejects_lineage_cycles_order_and_unrelated_requester_metadata() {
    for field in ["request_id", "workflow_run_id"] {
        let mut value = context_snapshot();
        value["lineage"][1][field] = value["lineage"][0][field].clone();
        assert!(CancellationContext::from_value(&value).is_err());
    }
    let mut value = context_snapshot();
    value["lineage"].as_array_mut().unwrap().reverse();
    assert!(CancellationContext::from_value(&value).is_err());
    let mut value = context_snapshot();
    value["requester"]["authorization"] = json!("unsupported");
    assert!(CancellationContext::from_value(&value).is_err());
}

#[test]
fn cooperative_context_child_keeps_root_time_after_expired_local_admission() {
    let mut request = context_request();
    request
        .raw
        .insert("recorded_at".into(), json!("2026-10-01T00:00:31Z"));
    let state = CancellationHistory::from_events(&[request], "run-1", None).unwrap();
    let request = state.request.unwrap();
    assert_eq!(request.requested_at, "2026-10-01T00:00:00.123456Z");
    assert_eq!(request.cleanup_deadline_at, "2026-10-01T00:00:30.123456Z");
    assert_eq!(request.context.unwrap().to_value(), context_snapshot());
    assert!(state.delivery.is_none());
}

#[test]
fn cooperative_context_observation_keeps_refresh_route_and_history_supplies_metadata() {
    let mut observation = context_observation();
    observation.context = Some(CancellationContext::from_value(&context_snapshot()).unwrap());
    let pending = CancellationHistory::from_events(&[], "run-1", Some(&observation)).unwrap();
    assert!(pending.request.unwrap().context.is_none());
    let state = CancellationHistory::from_events(
        &[context_request(), context_delivery()],
        "run-1",
        Some(&observation),
    )
    .unwrap();
    let request = state.request.unwrap();
    assert_eq!(
        request.history_refresh_page_token.as_deref(),
        Some("opaque-first-page")
    );
    assert_eq!(request.requested_at, "2026-10-01T00:00:00.123456Z");
    assert_eq!(request.context.unwrap().to_value(), context_snapshot());
}

#[test]
fn cooperative_context_must_match_canonical_local_request_and_run() {
    for (field, value) in [
        ("workflow_command_id", json!("other")),
        ("workflow_instance_id", json!("other")),
        ("cleanup_deadline_at", json!("2026-10-01T00:00:35Z")),
        ("reason", json!("changed")),
        ("cancellation", Value::Null),
    ] {
        let mut request = context_request();
        request.payload[field] = value;
        assert!(
            CancellationHistory::from_events(&[request], "run-1", None).is_err(),
            "{field}"
        );
    }
    let mut request = context_request();
    request.payload["cancellation"]["lineage"][1]["workflow_run_id"] = json!("other");
    assert!(CancellationHistory::from_events(&[request], "run-1", None).is_err());
    let mut request = context_request();
    request
        .raw
        .insert("recorded_at".into(), json!("2026-09-30T23:59:59Z"));
    assert!(CancellationHistory::from_events(&[request], "run-1", None).is_err());
}

#[test]
fn cooperative_context_delivery_cannot_change_the_accepted_snapshot() {
    let mut delivery = context_delivery();
    delivery.payload["cancellation"]["reason"] = json!("changed");
    assert!(
        CancellationHistory::from_events(&[context_request(), delivery], "run-1", None).is_err()
    );
}

#[test]
fn cooperative_context_cold_worker_replay_restores_same_delivery_and_cleanup() {
    let mut worker = cancellation_worker();
    worker.register_workflow("cancel", |ctx, _input| async move {
        assert!(ctx.cancellation_context()?.is_none());
        let Err(Error::CooperativeCancellationRequested(cancelled)) =
            ctx.sleep(Duration::from_secs(10)).await
        else {
            panic!("expected canonical cancellation");
        };
        let context = cancelled.request.context.unwrap();
        assert_eq!(ctx.cancellation_context()?, Some(context.clone()));
        let _shield = ctx.cancellation_shield()?;
        ctx.throw_if_cancellation_requested()?;
        ctx.activity("cleanup", json!([])).await?;
        Ok(context.to_value())
    });
    let first = worker
        .execute_workflow_task(context_task(vec![context_request(), context_delivery()]))
        .unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(first[0]["type"], "schedule_activity");
    assert_eq!(first[0]["activity_type"], "cleanup");
    let mut history = vec![context_request(), context_delivery()];
    history.extend(completed_activity(2, "cleanup", json!("cleaned")));
    for _restart in 0..2 {
        let result = worker
            .execute_workflow_task(context_task(history.clone()))
            .unwrap();
        assert_eq!(result[0]["type"], "complete_workflow");
        assert_eq!(
            decode_wire_value(&result[0]["result"], DEFAULT_CODEC).unwrap(),
            context_snapshot()
        );
    }
}

#[test]
fn cooperative_context_explicit_check_retains_delivered_metadata() {
    let mut worker = cancellation_worker();
    worker.register_workflow("cancel", |ctx, _input| async move {
        let _ = ctx.sleep(Duration::from_secs(10)).await;
        let Err(Error::CooperativeCancellationRequested(cancelled)) =
            ctx.throw_if_cancellation_requested()
        else {
            panic!("expected the delivered cancellation");
        };
        assert_eq!(
            cancelled.request.context.unwrap().to_value(),
            context_snapshot()
        );
        Ok(Value::Null)
    });
    worker
        .execute_workflow_task(context_task(vec![context_request(), context_delivery()]))
        .unwrap();
}

fn request_observation() -> Value {
    json!({
        "request_id": "original-request",
        "requested_at": "2026-10-01T08:00:00Z",
        "cleanup_deadline_at": "2026-10-01T08:10:00Z",
        "history_refresh_page_token": "opaque-server-token"
    })
}

fn event(kind: &str, payload: Value) -> HistoryEvent {
    serde_json::from_value(json!({"event_type":kind, "payload":payload})).unwrap()
}

fn canonical_request() -> HistoryEvent {
    serde_json::from_value(json!({
        "event_type":"CooperativeCancellationRequested", "workflow_command_id":"original-request",
        "recorded_at":"2026-10-01T08:00:00.000120Z", "payload":{
            "workflow_command_id":"original-request", "workflow_run_id":"run",
            "cleanup_deadline_at":"2026-10-01T08:10:00Z"
        }
    }))
    .unwrap()
}

fn canonical_delivery(sequence: u64, kind: &str) -> HistoryEvent {
    event(
        "CooperativeCancellationDelivered",
        json!({
            "workflow_command_id":"original-request", "workflow_run_id":"run",
            "sequence":sequence, "call_kind":kind
        }),
    )
}

fn history(events: &[HistoryEvent]) -> Result<CancellationHistory> {
    CancellationHistory::from_events(events, "run", None)
}

fn cancellation_worker() -> Worker {
    Worker::new(
        Client::builder("http://127.0.0.1:1").build().unwrap(),
        "queue",
    )
}

fn cancellation_task(events: Vec<HistoryEvent>) -> WorkflowTask {
    let mut task = workflow_task("cancel", events, DEFAULT_CODEC);
    task.run_id = Some("run".into());
    task
}

fn scheduled_timer(sequence: u64) -> HistoryEvent {
    event(
        "TimerScheduled",
        json!({"sequence":sequence, "timer_id":format!("timer-{sequence}"), "delay_seconds":5}),
    )
}

fn completed_activity(sequence: u64, name: &str, value: Value) -> Vec<HistoryEvent> {
    vec![
        event(
            "ActivityScheduled",
            json!({"sequence":sequence,"activity_type":name,"task_queue":"queue"}),
        ),
        event(
            "ActivityCompleted",
            json!({"sequence":sequence,"activity_type":name,"result":fixture_envelope(value)}),
        ),
    ]
}

fn cancelled_selection_history() -> Vec<HistoryEvent> {
    let mut delivery = canonical_delivery(3, "selection_handle");
    delivery.payload["operation_sequence"] = json!(1);
    vec![
        selection_activity_event("ActivityScheduled", 0, "slow", None),
        selection_activity_event("ActivityScheduled", 1, "fast", None),
        selection_activity_event("ActivityCompleted", 1, "fast", Some(json!("winner-value"))),
        selection_winner_marker(),
        canonical_request(),
        delivery,
    ]
}

fn cancelled_parallel_history() -> Vec<HistoryEvent> {
    let paths = nested_parallel_paths();
    let mut delivery = canonical_delivery(1, "parallel");
    delivery.payload["sequence_span"] = json!(3);
    vec![
        parallel_history_event(
            "ActivityScheduled",
            1,
            "activity_type",
            "first",
            paths[0].clone(),
            None,
        ),
        parallel_history_event(
            "ChildWorkflowScheduled",
            2,
            "workflow_type",
            "second",
            paths[1].clone(),
            None,
        ),
        parallel_history_event(
            "ActivityScheduled",
            3,
            "activity_type",
            "third",
            paths[2].clone(),
            None,
        ),
        canonical_request(),
        delivery,
    ]
}

fn intent_task(events: Vec<HistoryEvent>) -> WorkflowTask {
    let mut task = transport_task();
    task.history_events = events;
    task
}

fn original_observation() -> CancellationRequest {
    CancellationRequest::from_observation(&request_observation()).unwrap()
}

async fn authored_scalar(ctx: WorkflowContext, kind: &str) -> Result<()> {
    match kind {
        "activity" => {
            ctx.activity("forward", json!([])).await?;
        }
        "timer" => {
            ctx.sleep(Duration::from_secs(5)).await?;
        }
        "child" => {
            ctx.start_child_workflow("child", ChildWorkflowOptions::new("queue"), json!([]))
                .await?;
        }
        "signal" => {
            ctx.wait_signal("resume").await?;
        }
        "condition" => {
            ctx.wait_condition(ConditionWaitOptions::new("ready", "sha256:ready"), || {
                Ok(false)
            })
            .await?;
        }
        _ => unreachable!(),
    }
    Ok(())
}

fn pending_scalar_event(kind: &str) -> HistoryEvent {
    match kind {
        "activity" => event(
            "ActivityScheduled",
            json!({"sequence":1,"activity_type":"forward","task_queue":"queue"}),
        ),
        "timer" => scheduled_timer(1),
        "child" => event(
            "ChildWorkflowScheduled",
            json!({"sequence":1,"workflow_type":"child"}),
        ),
        "signal" => event(
            "SignalWaitOpened",
            json!({"sequence":1,"signal_name":"resume"}),
        ),
        "condition" => event(
            "ConditionWaitOpened",
            json!({"sequence":1,"condition_wait_id":"condition-1",
            "condition_wait_occurrence_id":"rust:condition-wait:0","condition_key":"ready","condition_definition_fingerprint":"sha256:ready"}),
        ),
        _ => unreachable!(),
    }
}

#[test]
fn cooperative_intent_suspends_actual_worker_scalar_calls_without_exposing_an_error() {
    for kind in ["activity", "timer", "child", "signal", "condition"] {
        for scheduled in [false, true] {
            let resumed = Arc::new(AtomicBool::new(false));
            let observed = resumed.clone();
            let mut worker = cancellation_worker();
            worker.register_workflow("cancel", move |ctx, _input| {
                let observed = observed.clone();
                async move {
                    assert!(!ctx.is_cancellation_requested()?);
                    // Even catching every error cannot expose cancellation
                    // before the worker commits its delivery and replays.
                    let _ = authored_scalar(ctx, kind).await;
                    observed.store(true, Ordering::SeqCst);
                    Ok(Value::Null)
                }
            });
            let mut events = Vec::new();
            if scheduled {
                events.push(pending_scalar_event(kind));
            }
            events.push(canonical_request());
            let decision = worker
                .execute_workflow_task_decision_with_cancellation(
                    intent_task(events),
                    Some(&original_observation()),
                )
                .unwrap();
            assert!(!resumed.load(Ordering::SeqCst), "{kind}:{scheduled}");
            assert!(decision.commands.is_empty(), "{kind}:{scheduled}");
            let delivery = decision.cancellation_delivery.unwrap();
            assert_eq!(delivery.sequence, 1);
            assert_eq!(serde_json::to_value(delivery.call_kind).unwrap(), kind);
            assert_eq!(delivery.request_id, "original-request");
        }
    }
}

#[test]
fn cooperative_intent_suspends_a_resolution_committed_after_the_request() {
    let resumed = Arc::new(AtomicBool::new(false));
    let observed = resumed.clone();
    let mut worker = cancellation_worker();
    worker.register_workflow("cancel", move |ctx, _input| {
        let observed = observed.clone();
        async move {
            let _ = ctx.sleep(Duration::from_secs(5)).await;
            observed.store(true, Ordering::SeqCst);
            Ok(Value::Null)
        }
    });
    let decision = worker
        .execute_workflow_task_decision_with_cancellation(
            intent_task(vec![
                scheduled_timer(1),
                canonical_request(),
                event(
                    "TimerFired",
                    json!({"sequence":1,"timer_id":"timer-1","delay_seconds":5}),
                ),
            ]),
            Some(&original_observation()),
        )
        .unwrap();
    assert!(!resumed.load(Ordering::SeqCst));
    assert!(decision.commands.is_empty());
    assert_eq!(decision.cancellation_delivery, Some(transport_delivery()));
}

#[test]
fn cooperative_intent_preserves_earlier_commands_and_replayed_side_effects() {
    let side_effects = Arc::new(Mutex::new(0));
    let observed = side_effects.clone();
    let mut worker = cancellation_worker();
    worker.register_workflow("cancel", move |ctx, _input| {
        let observed = observed.clone();
        async move {
            let value: Value = ctx.side_effect(|| {
                *observed.lock().unwrap() += 1;
                json!(7)
            })?;
            assert_eq!(value, 7);
            ctx.sleep(Duration::from_secs(5)).await?;
            Ok(Value::Null)
        }
    });
    let first = worker
        .execute_workflow_task_decision_with_cancellation(
            intent_task(vec![canonical_request()]),
            Some(&original_observation()),
        )
        .unwrap();
    assert_eq!(first.commands.len(), 1);
    assert_eq!(first.commands[0]["type"], "record_side_effect");
    assert_eq!(first.cancellation_delivery.unwrap().sequence, 2);
    let second = worker
        .execute_workflow_task_decision_with_cancellation(
            intent_task(vec![
                event(
                    "SideEffectRecorded",
                    json!({"sequence":1,"result":fixture_envelope(json!(7))}),
                ),
                canonical_request(),
            ]),
            Some(&original_observation()),
        )
        .unwrap();
    assert!(second.commands.is_empty());
    assert_eq!(second.cancellation_delivery.unwrap().sequence, 2);
    assert_eq!(*side_effects.lock().unwrap(), 1);
}

#[test]
fn cooperative_intent_replays_committed_delivery_with_the_original_observation() {
    let delivered = Arc::new(Mutex::new(None));
    let observed = delivered.clone();
    let mut worker = cancellation_worker();
    worker.register_workflow("cancel", move |ctx, _input| {
        let observed = observed.clone();
        async move {
            match ctx.sleep(Duration::from_secs(5)).await {
                Err(Error::CooperativeCancellationRequested(cancellation)) => {
                    *observed.lock().unwrap() = Some(cancellation.request);
                }
                other => panic!("{other:?}"),
            }
            Ok(Value::Null)
        }
    });
    let observation = original_observation();
    let decision = worker
        .execute_workflow_task_decision_with_cancellation(
            intent_task(vec![
                scheduled_timer(1),
                canonical_request(),
                canonical_delivery(1, "timer"),
            ]),
            Some(&observation),
        )
        .unwrap();
    assert!(decision.cancellation_delivery.is_none());
    assert_eq!(decision.commands.len(), 1);
    assert_eq!(decision.commands[0]["type"], "complete_workflow");
    assert_eq!(*delivered.lock().unwrap(), Some(observation));
}

#[test]
fn cooperative_intent_cannot_publish_a_terminal_result_after_an_ignored_pending_call() {
    let mut worker = cancellation_worker();
    worker.register_workflow("cancel", |ctx, _input| async move {
        let mut timer = Box::pin(ctx.sleep(Duration::from_secs(5)));
        let mut cx = TaskContext::from_waker(noop_waker_ref());
        assert!(matches!(timer.as_mut().poll(&mut cx), Poll::Pending));
        Ok(json!("ignored pending timer"))
    });
    let decision = worker
        .execute_workflow_task_decision_with_cancellation(
            intent_task(vec![canonical_request()]),
            Some(&original_observation()),
        )
        .unwrap();
    assert!(decision.commands.is_empty());
    assert_eq!(decision.cancellation_delivery, Some(transport_delivery()));
}

#[test]
fn cooperative_intent_cannot_publish_commands_after_an_ignored_pending_call() {
    let mut worker = cancellation_worker();
    worker.register_workflow("cancel", |ctx, _input| async move {
        let mut timer = Box::pin(ctx.sleep(Duration::from_secs(5)));
        let mut cx = TaskContext::from_waker(noop_waker_ref());
        assert!(matches!(timer.as_mut().poll(&mut cx), Poll::Pending));
        let _: Value = ctx.side_effect(|| json!("authored after pending timer"))?;
        Ok(Value::Null)
    });
    assert!(
        matches!(worker.execute_workflow_task_decision_with_cancellation(
        intent_task(vec![canonical_request()]), Some(&original_observation()),
    ), Err(Error::NonDeterministicReplay(ReplayFailure { reason, .. }))
        if reason == "cooperative_cancellation_pending_call_escaped")
    );
}

#[test]
fn cooperative_intent_preserves_a_prior_selection_winner_then_suspends_the_loser_await() {
    for late in [false, true] {
        let resumed = Arc::new(AtomicBool::new(false));
        let observed = resumed.clone();
        let mut worker = cancellation_worker();
        worker.register_workflow("cancel", move |ctx, _input| {
            let observed = observed.clone();
            async move {
                let selected = keyed_activity_selection(&ctx).await?;
                assert_eq!(selected.key, SelectionKey::Name("fast".into()));
                assert_eq!(
                    selected.value,
                    Some(ParallelResult::Activity(json!("winner-value")))
                );
                let _ = selected
                    .handle(&SelectionKey::Name("slow".into()))
                    .unwrap()
                    .await_result()
                    .await;
                observed.store(true, Ordering::SeqCst);
                Ok(Value::Null)
            }
        });
        let mut events = cancelled_selection_history();
        events.pop();
        if late {
            events.push(selection_activity_event(
                "ActivityCompleted",
                0,
                "slow",
                Some(json!("late")),
            ));
        }
        let decision = worker
            .execute_workflow_task_decision_with_cancellation(
                intent_task(events),
                Some(&original_observation()),
            )
            .unwrap();
        assert!(!resumed.load(Ordering::SeqCst));
        assert!(decision.commands.is_empty());
        let delivery = decision.cancellation_delivery.unwrap();
        assert_eq!(delivery.call_kind, CancellationCallKind::SelectionHandle);
        assert_eq!(delivery.sequence, 3);
        assert_eq!(delivery.operation_sequence, Some(1));
        assert_eq!(delivery.operation_sequence_span, 1);
    }
}

#[test]
fn cooperative_intent_validates_all_handle_fields_before_checking_prior_resolution() {
    for changed in 0..7 {
        let mut worker = cancellation_worker();
        worker.register_workflow("cancel", move |ctx, _input| async move {
            let selected = keyed_activity_selection(&ctx).await?;
            let mut handle = selected
                .handle(&SelectionKey::Name("slow".into()))
                .unwrap()
                .clone();
            match changed {
                0 => handle.key = SelectionKey::Name("changed".into()),
                1 => handle.index = 9,
                2 => handle.kind = "timer".into(),
                3 => handle.identity = "changed".into(),
                4 => handle.selection_group_id = "changed".into(),
                5 => handle.base_sequence = 2,
                6 => handle.size = 2,
                _ => unreachable!(),
            }
            handle.await_result().await?;
            Ok(Value::Null)
        });
        let mut events = cancelled_selection_history();
        events.pop();
        assert!(
            matches!(
                worker.execute_workflow_task_decision_with_cancellation(
                    intent_task(events),
                    Some(&original_observation()),
                ),
                Err(Error::NonDeterministicReplay(_))
            ),
            "{changed}"
        );
    }
}

#[test]
fn cooperative_intent_suspends_nested_parallel_and_an_uncommitted_selection_winner() {
    for (selection, mode) in [
        (false, "new"),
        (false, "scheduled"),
        (false, "partial"),
        (true, "new"),
        (true, "scheduled"),
    ] {
        let resumed = Arc::new(AtomicBool::new(false));
        let observed = resumed.clone();
        let mut worker = cancellation_worker();
        worker.register_workflow("cancel", move |ctx, _input| {
            let observed = observed.clone();
            async move {
                if selection {
                    let _ = keyed_activity_selection(&ctx).await;
                } else {
                    let _ = ctx.parallel(nested_parallel_operations()).await;
                }
                observed.store(true, Ordering::SeqCst);
                Ok(Value::Null)
            }
        });
        let events = if mode == "new" {
            vec![canonical_request()]
        } else if selection {
            vec![
                selection_activity_event("ActivityScheduled", 0, "slow", None),
                selection_activity_event("ActivityScheduled", 1, "fast", None),
                canonical_request(),
                selection_activity_event("ActivityCompleted", 1, "fast", Some(json!("late"))),
                selection_winner_marker(),
            ]
        } else {
            let mut events = cancelled_parallel_history();
            events.pop();
            if mode == "partial" {
                events.insert(
                    3,
                    parallel_history_event(
                        "ActivityCompleted",
                        1,
                        "activity_type",
                        "first",
                        nested_parallel_paths()[0].clone(),
                        Some(json!(7)),
                    ),
                );
            }
            events.push(event("ActivityFailed", json!({"sequence":3,"activity_type":"third",
                "exception_type":"LateFailure","exception":{"class":"LateFailure","message":"too late"}})));
            events
        };
        let decision = worker
            .execute_workflow_task_decision_with_cancellation(
                intent_task(events),
                Some(&original_observation()),
            )
            .unwrap();
        assert!(!resumed.load(Ordering::SeqCst));
        assert!(decision.commands.is_empty());
        let delivery = decision.cancellation_delivery.unwrap();
        assert_eq!(delivery.call_kind, CancellationCallKind::Parallel);
        assert_eq!(delivery.sequence, 1);
        assert_eq!(delivery.sequence_span, if selection { 2 } else { 3 });
    }
}

#[test]
fn cooperative_intent_rejects_changed_later_leaf_details_before_exporting_delivery() {
    let mut worker = cancellation_worker();
    worker.register_workflow("cancel", |ctx, _input| async move {
        ctx.parallel(vec![
            ParallelOperation::activity("first", json!([])),
            ParallelOperation::group(vec![
                ParallelOperation::child_workflow(
                    "second",
                    ChildWorkflowOptions::new("child-workers"),
                    json!([]),
                ),
                ParallelOperation::activity("changed-third", json!([])),
            ]),
        ])
        .await?;
        Ok(Value::Null)
    });
    let mut events = cancelled_parallel_history();
    events.pop();
    assert!(matches!(
        worker.execute_workflow_task_decision_with_cancellation(
            intent_task(events),
            Some(&original_observation()),
        ),
        Err(Error::NonDeterministicReplay(_))
    ));
}

#[test]
fn cooperative_intent_uses_the_pending_physical_condition_reopen() {
    let payload = |sequence| {
        json!({"sequence":sequence,"condition_wait_id":format!("condition:{sequence}"),
        "condition_wait_occurrence_id":"rust:condition-wait:0","condition_key":"ready",
        "condition_definition_fingerprint":"sha256:ready"})
    };
    let mut worker = cancellation_worker();
    worker.register_workflow("cancel", |ctx, _input| async move {
        ctx.wait_condition(ConditionWaitOptions::new("ready", "sha256:ready"), || {
            panic!("delivery proposal must suspend before reevaluating the predicate")
        })
        .await?;
        Ok(Value::Null)
    });
    let decision = worker
        .execute_workflow_task_decision_with_cancellation(
            intent_task(vec![
                event("ConditionWaitOpened", payload(1)),
                event("ConditionWaitSatisfied", payload(1)),
                event("ConditionWaitOpened", payload(2)),
                canonical_request(),
            ]),
            Some(&original_observation()),
        )
        .unwrap();
    assert!(decision.commands.is_empty());
    let delivery = decision.cancellation_delivery.unwrap();
    assert_eq!(delivery.call_kind, CancellationCallKind::Condition);
    assert_eq!(delivery.sequence, 2);
}

#[test]
fn cooperative_intent_keeps_explicit_shielded_cleanup_pending_without_delivery() {
    let mut worker = cancellation_worker();
    worker.register_workflow("cancel", |ctx, _input| async move {
        let _shield = ctx.cancellation_shield()?;
        ctx.sleep(Duration::from_secs(5)).await?;
        Ok(Value::Null)
    });
    let decision = worker
        .execute_workflow_task_decision_with_cancellation(
            intent_task(vec![scheduled_timer(1), canonical_request()]),
            Some(&original_observation()),
        )
        .unwrap();
    assert!(decision.commands.is_empty());
    assert!(decision.cancellation_delivery.is_none());
}

#[test]
fn cooperative_intent_rejects_unproven_observation_or_claim_before_workflow_code() {
    let invoked = Arc::new(AtomicBool::new(false));
    let observed = invoked.clone();
    let mut worker = cancellation_worker();
    worker.register_workflow("cancel", move |_ctx, _input| {
        observed.store(true, Ordering::SeqCst);
        async { Ok(Value::Null) }
    });
    for changed in 0..6 {
        let mut task = intent_task(vec![canonical_request()]);
        let mut observation = original_observation();
        match changed {
            0 => task.history_events.clear(),
            1 => task.lease_owner = None,
            2 => task.run_id = Some("other-run".into()),
            3 => observation.request_id = "other-request".into(),
            4 => observation.cleanup_deadline_at = "2026-10-01T08:11:00Z".into(),
            5 => observation.history_refresh_page_token = None,
            _ => unreachable!(),
        }
        assert!(worker
            .execute_workflow_task_decision_with_cancellation(task, Some(&observation))
            .is_err());
        assert!(!invoked.load(Ordering::SeqCst));
    }
}

#[test]
fn cooperative_parallel_replays_one_original_cancellation_for_nested_mixed_leaves() {
    for _cold_restart in 0..2 {
        let mut worker = cancellation_worker();
        worker.register_workflow("cancel", |ctx, _input| async move {
            ctx.parallel(nested_parallel_operations()).await?;
            Ok(Value::Null)
        });
        let commands = worker
            .execute_workflow_task(cancellation_task(cancelled_parallel_history()))
            .unwrap();
        assert_eq!(commands.len(), 1);
        assert_eq!(
            commands[0]["exception_type"],
            "WorkflowCancellationRequested"
        );
        assert_eq!(
            commands[0]["exception"]["properties"]["request_id"],
            "original-request"
        );
        assert_eq!(commands[0]["non_retryable"], true);
    }
}

#[test]
fn cooperative_parallel_preserves_prior_result_and_overrides_a_late_leaf_failure() {
    let mut events = cancelled_parallel_history();
    events.insert(
        3,
        parallel_history_event(
            "ActivityCompleted",
            1,
            "activity_type",
            "first",
            nested_parallel_paths()[0].clone(),
            Some(json!(7)),
        ),
    );
    events.push(event("ActivityFailed", json!({"sequence":3, "activity_type":"third", "exception_type":"LateFailure", "exception":{"class":"LateFailure", "message":"too late"}})));
    let ctx = workflow_context(events);
    let mut call = Box::pin(ctx.parallel(nested_parallel_operations()));
    let mut cx = TaskContext::from_waker(noop_waker_ref());
    let Poll::Ready(Err(Error::CooperativeCancellationRequested(cancellation))) =
        call.as_mut().poll(&mut cx)
    else {
        panic!("group cancellation must retain priority")
    };
    assert_eq!(cancellation.delivery.sequence_span, 3);
    assert_eq!(ctx.state.lock().unwrap().command_cursor, 3);
    ctx.ensure_history_consumed().unwrap();
    assert!(ctx.take_commands().unwrap().is_empty());
}

#[test]
fn cooperative_parallel_does_not_hide_changed_later_leaf_details() {
    let mut events = cancelled_parallel_history();
    events[2].payload["activity_type"] = json!("changed-third");
    let ctx = workflow_context(events);
    let mut call = Box::pin(ctx.parallel(nested_parallel_operations()));
    let mut cx = TaskContext::from_waker(noop_waker_ref());
    assert!(
        matches!(call.as_mut().poll(&mut cx), Poll::Ready(Err(Error::NonDeterministicReplay(ReplayFailure { reason, .. }))) if reason == "recorded_command_detail_mismatch")
    );
    assert!(ctx.take_commands().unwrap().is_empty());
}

#[test]
fn cooperative_parallel_rejects_changed_span_nesting_scalar_and_cleanup_scope() {
    for change in 0..4 {
        let ctx = workflow_context(cancelled_parallel_history());
        let _shield = if change == 3 {
            Some(ctx.cancellation_shield().unwrap())
        } else {
            None
        };
        let operations = match change {
            0 => vec![ParallelOperation::activity("first", json!([]))],
            1 => vec![
                ParallelOperation::activity("first", json!([])),
                ParallelOperation::child_workflow(
                    "second",
                    ChildWorkflowOptions::new("child-workers"),
                    json!([]),
                ),
                ParallelOperation::activity("third", json!([])),
            ],
            _ => nested_parallel_operations(),
        };
        let mut cx = TaskContext::from_waker(noop_waker_ref());
        let outcome = if change == 2 {
            Box::pin(ctx.activity("first", json!([])))
                .as_mut()
                .poll(&mut cx)
                .map_ok(|_| ())
        } else {
            Box::pin(ctx.parallel(operations))
                .as_mut()
                .poll(&mut cx)
                .map_ok(|_| ())
        };
        assert!(matches!(
            outcome,
            Poll::Ready(Err(Error::NonDeterministicReplay(_)))
        ));
        assert!(ctx.take_commands().unwrap().is_empty());
    }
}

#[test]
fn cooperative_parallel_cold_delivery_covers_all_scalar_leaf_kinds_without_scheduling() {
    let mut delivery = canonical_delivery(1, "parallel");
    delivery.payload["sequence_span"] = json!(5);
    let ctx = workflow_context(vec![canonical_request(), delivery]);
    let mut call = Box::pin(ctx.parallel(vec![
        ParallelOperation::activity("work", json!([])),
        ParallelOperation::child_workflow(
            "child",
            ChildWorkflowOptions::new("child-workers"),
            json!([]),
        ),
        ParallelOperation::timer(Duration::from_secs(5)),
        ParallelOperation::signal("ready"),
        ParallelOperation::condition(ConditionWaitOptions::new("ready", "sha256:ready"), || {
            Ok(false)
        }),
    ]));
    let mut cx = TaskContext::from_waker(noop_waker_ref());
    assert!(matches!(
        call.as_mut().poll(&mut cx),
        Poll::Ready(Err(Error::CooperativeCancellationRequested(_)))
    ));
    assert_eq!(ctx.state.lock().unwrap().command_cursor, 5);
    ctx.ensure_history_consumed().unwrap();
    assert!(ctx.take_commands().unwrap().is_empty());
}

#[test]
fn cooperative_parallel_validates_recorded_timer_signal_and_condition_leaves() {
    for changed in 0..3 {
        let names = [
            ("ActivityScheduled", "activity_type", "work"),
            ("ChildWorkflowScheduled", "workflow_type", "child"),
            ("TimerScheduled", "timer_id", "timer"),
            ("SignalWaitOpened", "signal_name", "ready"),
            ("ConditionWaitOpened", "condition_wait_id", "condition"),
        ];
        let mut events = names
            .into_iter()
            .enumerate()
            .map(|(index, (event_type, field, name))| {
                parallel_history_event(
                    event_type,
                    index as u64 + 1,
                    field,
                    name,
                    vec![parallel_group_entry(1, 5, index, "mixed")],
                    None,
                )
            })
            .collect::<Vec<_>>();
        events[2].payload["delay_seconds"] = json!(if changed == 1 { 500 } else { 5 });
        events[4].payload["condition_wait_occurrence_id"] = json!("rust:condition-wait:0");
        events[4].payload["condition_key"] = json!("ready");
        events[4].payload["condition_definition_fingerprint"] = json!(if changed == 2 {
            "changed"
        } else {
            "sha256:ready"
        });
        let mut delivery = canonical_delivery(1, "parallel");
        delivery.payload["sequence_span"] = json!(5);
        events.extend([canonical_request(), delivery]);
        let ctx = workflow_context(events);
        let mut call = Box::pin(ctx.parallel(vec![
            ParallelOperation::activity("work", json!([])),
            ParallelOperation::child_workflow(
                "child",
                ChildWorkflowOptions::new("child-workers"),
                json!([]),
            ),
            ParallelOperation::timer(Duration::from_secs(5)),
            ParallelOperation::signal("ready"),
            ParallelOperation::condition(
                ConditionWaitOptions::new("ready", "sha256:ready"),
                || panic!("delivered condition must not reevaluate its predicate"),
            ),
        ]));
        let mut cx = TaskContext::from_waker(noop_waker_ref());
        match call.as_mut().poll(&mut cx) {
            Poll::Ready(Err(Error::CooperativeCancellationRequested(_))) if changed == 0 => {
                ctx.ensure_history_consumed().unwrap();
            }
            Poll::Ready(Err(Error::NonDeterministicReplay(failure))) if changed == 1 => {
                assert_eq!(failure.reason, "timer_delay_mismatch");
            }
            Poll::Ready(Err(Error::NonDeterministicReplay(failure))) if changed == 2 => {
                assert_eq!(failure.reason, "condition_wait_predicate_mismatch");
            }
            other => panic!("{changed}: {other:?}"),
        }
        assert!(ctx.take_commands().unwrap().is_empty());
    }
}

#[test]
fn cooperative_selection_group_delivers_before_an_uncommitted_winner() {
    let mut delivery = canonical_delivery(1, "parallel");
    delivery.payload["sequence_span"] = json!(2);
    let events = vec![
        selection_activity_event("ActivityScheduled", 0, "slow", None),
        selection_activity_event("ActivityScheduled", 1, "fast", None),
        canonical_request(),
        delivery,
        selection_activity_event("ActivityCompleted", 1, "fast", Some(json!("too-late"))),
        selection_winner_marker(),
    ];
    let ctx = workflow_context(events);
    let mut select = Box::pin(keyed_activity_selection(&ctx));
    let mut cx = TaskContext::from_waker(noop_waker_ref());
    assert!(matches!(
        select.as_mut().poll(&mut cx),
        Poll::Ready(Err(Error::CooperativeCancellationRequested(_)))
    ));
    ctx.ensure_history_consumed().unwrap();
    assert!(ctx.take_commands().unwrap().is_empty());
}

#[test]
fn cooperative_parallel_saga_cleanup_uses_the_slot_after_the_entire_group() {
    let mut events = cancelled_parallel_history();
    events.extend(completed_activity(4, "undo", Value::Null));
    let mut worker = cancellation_worker();
    worker.register_workflow("cancel", |ctx, _input| async move {
        let mut saga = ctx.saga();
        saga.add_compensation("undo", json!([]))?;
        saga.finish(ctx.parallel(nested_parallel_operations()).await)
            .await?;
        Ok(Value::Null)
    });
    let commands = worker
        .execute_workflow_task(cancellation_task(events))
        .unwrap();
    assert_eq!(commands.len(), 1);
    assert_eq!(
        commands[0]["exception_type"],
        "WorkflowCancellationRequested"
    );
    assert_eq!(
        commands[0]["exception"]["properties"]["request_id"],
        "original-request"
    );
}

#[test]
fn cooperative_selection_handle_replays_original_request_after_committed_winner() {
    for _cold_restart in 0..2 {
        let mut worker = cancellation_worker();
        worker.register_workflow("cancel", |ctx, _input| async move {
            let selected = keyed_activity_selection(&ctx).await?;
            assert_eq!(selected.key, SelectionKey::Name("fast".into()));
            assert_eq!(
                selected.value,
                Some(ParallelResult::Activity(json!("winner-value")))
            );
            assert!(!ctx.is_cancellation_requested()?);
            selected
                .handle(&SelectionKey::Name("slow".into()))
                .unwrap()
                .await_result()
                .await?;
            Ok(Value::Null)
        });
        let commands = worker
            .execute_workflow_task(cancellation_task(cancelled_selection_history()))
            .unwrap();
        assert_eq!(commands.len(), 1);
        assert_eq!(
            commands[0]["exception_type"],
            "WorkflowCancellationRequested"
        );
        assert_eq!(
            commands[0]["exception"]["properties"]["request_id"],
            "original-request"
        );
        assert_eq!(
            commands[0]["exception"]["properties"]["cleanup_deadline_at"],
            "2026-10-01T08:10:00Z"
        );
    }
}

#[test]
fn cooperative_selection_handle_delivery_owns_a_late_loser_completion() {
    for before_delivery in [true, false] {
        let mut events = cancelled_selection_history();
        let late =
            selection_activity_event("ActivityCompleted", 0, "slow", Some(json!("too-late")));
        if before_delivery {
            events.insert(events.len() - 1, late);
        } else {
            events.push(late);
        }
        let ctx = workflow_context(events);
        let mut select = Box::pin(keyed_activity_selection(&ctx));
        let mut cx = TaskContext::from_waker(noop_waker_ref());
        let Poll::Ready(Ok(selected)) = select.as_mut().poll(&mut cx) else {
            panic!("committed winner must retain priority")
        };
        assert_eq!(
            selected.value,
            Some(ParallelResult::Activity(json!("winner-value")))
        );
        let mut loser = Box::pin(
            selected
                .handle(&SelectionKey::Name("slow".into()))
                .unwrap()
                .await_result(),
        );
        let Poll::Ready(Err(Error::CooperativeCancellationRequested(cancellation))) =
            loser.as_mut().poll(&mut cx)
        else {
            panic!("late completion must not replace the committed cancellation")
        };
        assert_eq!(cancellation.delivery.sequence, 3);
        assert_eq!(cancellation.delivery.operation_sequence, Some(1));
        ctx.ensure_history_consumed().unwrap();
        assert!(ctx.take_commands().unwrap().is_empty());
    }
}

#[test]
fn cooperative_selection_handle_rejects_changed_public_identity_and_member_range() {
    for changed in 0..7 {
        let ctx = workflow_context(cancelled_selection_history());
        let mut select = Box::pin(keyed_activity_selection(&ctx));
        let mut cx = TaskContext::from_waker(noop_waker_ref());
        let Poll::Ready(Ok(selected)) = select.as_mut().poll(&mut cx) else {
            panic!("winner")
        };
        let mut handle = selected
            .handle(&SelectionKey::Name("slow".into()))
            .unwrap()
            .clone();
        match changed {
            0 => handle.key = SelectionKey::Name("changed".into()),
            1 => handle.index = 9,
            2 => handle.kind = "timer".into(),
            3 => handle.identity = "changed".into(),
            4 => handle.selection_group_id = "changed".into(),
            5 => handle.base_sequence = 2,
            6 => handle.size = 2,
            _ => unreachable!(),
        }
        let mut wait = Box::pin(handle.await_result());
        assert!(
            matches!(wait.as_mut().poll(&mut cx), Poll::Ready(Err(Error::NonDeterministicReplay(ReplayFailure { reason, .. }))) if reason == "cooperative_cancellation_call_mismatch")
        );
        assert!(!ctx.is_cancellation_requested().unwrap());
        assert!(ctx.take_commands().unwrap().is_empty());
    }
}

#[test]
fn cooperative_selection_handle_request_only_keeps_the_loser_pending() {
    let mut events = cancelled_selection_history();
    events.pop();
    let ctx = workflow_context(events);
    let mut select = Box::pin(keyed_activity_selection(&ctx));
    let mut cx = TaskContext::from_waker(noop_waker_ref());
    let Poll::Ready(Ok(selected)) = select.as_mut().poll(&mut cx) else {
        panic!("winner")
    };
    let mut wait = Box::pin(
        selected
            .handle(&SelectionKey::Name("slow".into()))
            .unwrap()
            .await_result(),
    );
    assert!(matches!(wait.as_mut().poll(&mut cx), Poll::Pending));
    assert!(!ctx.is_cancellation_requested().unwrap());
    assert!(ctx.take_commands().unwrap().is_empty());
}

#[test]
fn cooperative_selection_handle_cannot_be_skipped_or_replaced_by_a_scalar_call() {
    for skip in [true, false] {
        let mut worker = cancellation_worker();
        worker.register_workflow("cancel", move |ctx, _input| async move {
            let selected = keyed_activity_selection(&ctx).await?;
            if !skip {
                ctx.sleep(Duration::from_secs(5)).await?;
            }
            selected.into_result()?;
            Ok(Value::Null)
        });
        assert!(matches!(
            worker.execute_workflow_task(cancellation_task(cancelled_selection_history())),
            Err(Error::NonDeterministicReplay(_))
        ));
    }
}

#[test]
fn cooperative_selection_handle_cannot_hide_delivery_inside_a_cleanup_shield() {
    let ctx = workflow_context(cancelled_selection_history());
    let mut select = Box::pin(keyed_activity_selection(&ctx));
    let mut cx = TaskContext::from_waker(noop_waker_ref());
    let Poll::Ready(Ok(selected)) = select.as_mut().poll(&mut cx) else {
        panic!("winner")
    };
    let _shield = ctx.cancellation_shield().unwrap();
    let mut wait = Box::pin(
        selected
            .handle(&SelectionKey::Name("slow".into()))
            .unwrap()
            .await_result(),
    );
    assert!(matches!(
        wait.as_mut().poll(&mut cx),
        Poll::Ready(Err(Error::NonDeterministicReplay(_)))
    ));
}

#[test]
fn cooperative_selection_handle_runs_saga_cleanup_as_the_next_durable_command() {
    let mut worker = cancellation_worker();
    worker.register_workflow("cancel", |ctx, _input| async move {
        let selected = keyed_activity_selection(&ctx).await?;
        let mut saga = ctx.saga();
        saga.add_compensation("undo", json!([]))?;
        let result = selected
            .handle(&SelectionKey::Name("slow".into()))
            .unwrap()
            .await_result()
            .await;
        saga.finish(result).await?;
        Ok(Value::Null)
    });
    let commands = worker
        .execute_workflow_task(cancellation_task(cancelled_selection_history()))
        .unwrap();
    assert_eq!(commands.len(), 1);
    assert_eq!(commands[0]["type"], "schedule_activity");
    assert_eq!(commands[0]["activity_type"], "undo");
}

#[test]
fn cooperative_replay_preserves_completed_forward_result_and_saga_cleanup_identity() {
    for _cold_restart in 0..2 {
        let mut events = completed_activity(1, "forward", json!(7));
        events.extend([
            scheduled_timer(2),
            canonical_request(),
            canonical_delivery(2, "timer"),
        ]);
        events.extend(completed_activity(3, "undo", Value::Null));
        let mut worker = cancellation_worker();
        worker.register_workflow("cancel", |ctx, _input| async move {
            let result = ctx.activity("forward", json!([])).await?;
            assert_eq!(result, json!(7));
            assert!(!ctx.is_cancellation_requested()?);
            let mut saga = ctx.saga();
            saga.add_compensation("undo", json!([]))?;
            saga.finish(ctx.sleep(Duration::from_secs(5)).await).await?;
            Ok(Value::Null)
        });
        let commands = worker
            .execute_workflow_task(cancellation_task(events))
            .unwrap();
        assert_eq!(commands.len(), 1);
        assert_eq!(
            commands[0]["exception_type"],
            "WorkflowCancellationRequested"
        );
        assert_eq!(
            commands[0]["exception"]["properties"]["request_id"],
            "original-request"
        );
        assert_eq!(
            commands[0]["exception"]["properties"]["cleanup_deadline_at"],
            "2026-10-01T08:10:00Z"
        );
    }
}

#[test]
fn cooperative_replay_saga_schedules_cleanup_once_after_committed_delivery() {
    let mut events = completed_activity(1, "forward", json!(7));
    events.extend([
        scheduled_timer(2),
        canonical_request(),
        canonical_delivery(2, "timer"),
    ]);
    let mut worker = cancellation_worker();
    worker.register_workflow("cancel", |ctx, _input| async move {
        ctx.activity("forward", json!([])).await?;
        let mut saga = ctx.saga();
        saga.add_compensation("undo", json!([]))?;
        saga.finish(ctx.sleep(Duration::from_secs(5)).await).await?;
        Ok(Value::Null)
    });
    let commands = worker
        .execute_workflow_task(cancellation_task(events))
        .unwrap();
    assert_eq!(commands.len(), 1);
    assert_eq!(commands[0]["type"], "schedule_activity");
    assert_eq!(commands[0]["activity_type"], "undo");
}

#[test]
fn cooperative_replay_consumes_reopened_condition_once_at_its_physical_delivery() {
    let payload = |sequence| {
        json!({"sequence":sequence,"condition_wait_id":format!("condition:{sequence}"),
        "condition_wait_occurrence_id":"rust:condition-wait:0", "condition_key":"ready",
        "condition_definition_fingerprint":"sha256:ready"})
    };
    let events = vec![
        event("ConditionWaitOpened", payload(1)),
        event("ConditionWaitSatisfied", payload(1)),
        event("ConditionWaitOpened", payload(2)),
        canonical_request(),
        canonical_delivery(2, "condition"),
    ];
    for _cold_restart in 0..2 {
        let ctx = workflow_context(events.clone());
        let mut wait = Box::pin(
            ctx.wait_condition(ConditionWaitOptions::new("ready", "sha256:ready"), || {
                Ok(false)
            }),
        );
        let mut cx = TaskContext::from_waker(noop_waker_ref());
        let Poll::Ready(Err(Error::CooperativeCancellationRequested(cancellation))) =
            wait.as_mut().poll(&mut cx)
        else {
            panic!("reopened condition must receive canonical cancellation")
        };
        assert_eq!(cancellation.delivery.sequence, 2);
        assert_eq!(
            ctx.state.lock().unwrap().condition_wait_occurrence_counter,
            1
        );
        ctx.ensure_history_consumed().unwrap();
        assert!(ctx.take_commands().unwrap().is_empty());
    }
}

fn cancelled_reopened_group_history() -> Vec<HistoryEvent> {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../tests/fixtures/replay-regressions/cooperative-grouped-condition-reopen.json"
    ))
    .unwrap();
    let mut events: Vec<HistoryEvent> = serde_json::from_value(fixture["history"].clone()).unwrap();
    for event in &mut events {
        if matches!(
            event.event_type.as_str(),
            "CooperativeCancellationRequested" | "CooperativeCancellationDelivered"
        ) {
            event.payload["workflow_run_id"] = json!("run");
        }
    }
    events
}

#[test]
fn cooperative_group_delivery_consumes_pending_physical_condition_without_reopening() {
    for reopens in 1..=2 {
        let mut events = cancelled_reopened_group_history();
        if reopens == 2 {
            let mut satisfied = events[3].clone();
            satisfied.event_type = "ConditionWaitSatisfied".into();
            let mut reopened = events[3].clone();
            reopened.payload["sequence"] = json!(4);
            reopened.payload["condition_wait_id"] = json!("condition-4");
            events.splice(4..4, [satisfied, reopened]);
        }
        let ctx = workflow_context(events);
        let mut call = Box::pin(ctx.parallel(vec![
            ParallelOperation::timer(Duration::from_secs(300)),
            ParallelOperation::condition(
                ConditionWaitOptions::new("two-votes", "sha256:two-votes-v1"),
                || panic!("canonical cancellation must not reevaluate or reopen the condition"),
            ),
        ]));
        let mut cx = TaskContext::from_waker(noop_waker_ref());
        assert!(matches!(
            call.as_mut().poll(&mut cx),
            Poll::Ready(Err(Error::CooperativeCancellationRequested(_)))
        ));
        ctx.ensure_history_consumed().unwrap();
        assert!(ctx.take_commands().unwrap().is_empty());
    }
}

#[test]
fn cooperative_group_eligibility_uses_the_latest_physical_condition_result() {
    for latest_satisfied in [false, true] {
        let mut events = cancelled_reopened_group_history();
        events.pop();
        let mut fired = events[0].clone();
        fired.event_type = "TimerFired".into();
        events.insert(4, fired);
        if latest_satisfied {
            let mut satisfied = events[3].clone();
            satisfied.event_type = "ConditionWaitSatisfied".into();
            events.insert(5, satisfied);
        }
        let cancellation = history(&events).unwrap();
        assert_eq!(cancellation.eligible(1, 2), !latest_satisfied);
    }
}

#[test]
fn cooperative_replay_keeps_adjacent_condition_occurrences_and_cleanup_scopes_separate() {
    let payload = |sequence, occurrence| {
        json!({"sequence":sequence,"condition_wait_id":format!("condition:{sequence}"),
        "condition_wait_occurrence_id":format!("rust:condition-wait:{occurrence}"), "condition_key":"ready",
        "condition_definition_fingerprint":"sha256:ready"})
    };
    let ctx = workflow_context(vec![
        event("ConditionWaitOpened", payload(1, 0)),
        event("ConditionWaitSatisfied", payload(1, 0)),
        event("ConditionWaitOpened", payload(2, 1)),
        canonical_request(),
        canonical_delivery(2, "condition"),
    ]);
    let shield = ctx.cancellation_shield().unwrap();
    let mut cx = TaskContext::from_waker(noop_waker_ref());
    let mut first = Box::pin(
        ctx.wait_condition(ConditionWaitOptions::new("ready", "sha256:ready"), || {
            Ok(false)
        }),
    );
    assert!(matches!(
        first.as_mut().poll(&mut cx),
        Poll::Ready(Ok(ConditionWaitResult::Satisfied))
    ));
    drop(shield);
    let mut second = Box::pin(
        ctx.wait_condition(ConditionWaitOptions::new("ready", "sha256:ready"), || {
            Ok(false)
        }),
    );
    assert!(matches!(
        second.as_mut().poll(&mut cx),
        Poll::Ready(Err(Error::CooperativeCancellationRequested(_)))
    ));
    assert_eq!(
        ctx.state.lock().unwrap().condition_wait_occurrence_counter,
        2
    );
    ctx.ensure_history_consumed().unwrap();
}

#[test]
fn cooperative_replay_marker_keeps_priority_over_resolution_committed_after_request() {
    let ctx = workflow_context(vec![
        scheduled_timer(1),
        canonical_request(),
        canonical_delivery(1, "timer"),
        event(
            "TimerFired",
            json!({"sequence":1,"timer_id":"timer-1","delay_seconds":5}),
        ),
    ]);
    let mut timer = Box::pin(ctx.sleep(Duration::from_secs(5)));
    let mut cx = TaskContext::from_waker(noop_waker_ref());
    assert!(matches!(
        timer.as_mut().poll(&mut cx),
        Poll::Ready(Err(Error::CooperativeCancellationRequested(_)))
    ));
    ctx.ensure_history_consumed().unwrap();
}

#[test]
fn cooperative_replay_actual_worker_delivers_scalar_calls_with_original_identity() {
    for kind in ["activity", "timer", "child", "signal", "condition"] {
        for _cold_restart in 0..2 {
            let mut worker = cancellation_worker();
            worker.register_workflow("cancel", move |ctx, _input| async move {
                match kind {
                    "activity" => {
                        ctx.activity("forward", json!([])).await?;
                    }
                    "timer" => {
                        ctx.sleep(Duration::from_secs(5)).await?;
                    }
                    "child" => {
                        ctx.start_child_workflow(
                            "child",
                            ChildWorkflowOptions::new("queue"),
                            json!([]),
                        )
                        .await?;
                    }
                    "signal" => {
                        ctx.wait_signal("resume").await?;
                    }
                    "condition" => {
                        ctx.wait_condition(
                            ConditionWaitOptions::new("ready", "sha256:ready"),
                            || Ok(false),
                        )
                        .await?;
                    }
                    _ => unreachable!(),
                }
                Ok(Value::Null)
            });
            let commands = worker
                .execute_workflow_task(cancellation_task(vec![
                    canonical_request(),
                    canonical_delivery(1, kind),
                ]))
                .unwrap();
            assert_eq!(commands.len(), 1, "{kind}");
            assert_eq!(commands[0]["type"], "fail_workflow", "{kind}");
            assert_eq!(
                commands[0]["exception_type"], "WorkflowCancellationRequested",
                "{kind}"
            );
            assert_eq!(
                commands[0]["exception"]["properties"]["request_id"], "original-request",
                "{kind}"
            );
            assert_eq!(
                commands[0]["exception"]["properties"]["cleanup_deadline_at"],
                "2026-10-01T08:10:00Z",
                "{kind}"
            );
            assert_eq!(commands[0]["non_retryable"], true, "{kind}");
        }
    }
}

#[test]
fn cooperative_replay_observed_request_and_task_flag_do_not_inject_without_delivery() {
    let mut worker = cancellation_worker();
    worker.register_workflow("cancel", |ctx, _input| async move {
        assert!(!ctx.is_cancellation_requested()?);
        ctx.throw_if_cancellation_requested()?;
        ctx.sleep(Duration::from_secs(5)).await?;
        Ok(Value::Null)
    });
    let mut task = cancellation_task(vec![scheduled_timer(1), canonical_request()]);
    task.cancel_requested = true;
    assert!(worker.execute_workflow_task(task).unwrap().is_empty());
}

#[test]
fn cooperative_replay_recorded_timer_validates_authored_details_before_delivery() {
    for delay in [5, 500] {
        let ctx = workflow_context(vec![
            scheduled_timer(1),
            canonical_request(),
            canonical_delivery(1, "timer"),
        ]);
        let mut timer = Box::pin(ctx.sleep(Duration::from_secs(delay)));
        let mut cx = TaskContext::from_waker(noop_waker_ref());
        match timer.as_mut().poll(&mut cx) {
            Poll::Ready(Err(Error::CooperativeCancellationRequested(cancellation)))
                if delay == 5 =>
            {
                assert_eq!(cancellation.request.request_id, "original-request");
                assert_eq!(cancellation.delivery.sequence, 1);
                ctx.ensure_history_consumed().unwrap();
            }
            Poll::Ready(Err(Error::NonDeterministicReplay(failure))) if delay == 500 => {
                assert_eq!(failure.reason, "timer_delay_mismatch");
            }
            other => panic!("{delay}: {other:?}"),
        }
        assert!(ctx.take_commands().unwrap().is_empty());
    }
}

#[test]
fn cooperative_replay_changed_call_kind_and_unconsumed_boundary_are_rejected() {
    let mut worker = cancellation_worker();
    worker.register_workflow("cancel", |ctx, _input| async move {
        ctx.activity("changed", json!([])).await?;
        Ok(Value::Null)
    });
    assert!(
        matches!(worker.execute_workflow_task(cancellation_task(vec![canonical_request(), canonical_delivery(1, "timer")])),
        Err(Error::NonDeterministicReplay(ReplayFailure { reason, .. })) if reason == "cooperative_cancellation_call_mismatch")
    );
    let mut worker = cancellation_worker();
    worker.register_workflow("cancel", |_ctx, _input| async { Ok(Value::Null) });
    assert!(
        matches!(worker.execute_workflow_task(cancellation_task(vec![canonical_request(), canonical_delivery(1, "timer")])),
        Err(Error::NonDeterministicReplay(ReplayFailure { reason, .. })) if reason == "recorded_commands_unconsumed")
    );
}

#[test]
fn cooperative_replay_rejects_a_marker_that_skips_unrecorded_authored_calls() {
    let mut worker = cancellation_worker();
    worker.register_workflow("cancel", |ctx, _input| async move {
        ctx.sleep(Duration::from_secs(5)).await?;
        Ok(Value::Null)
    });
    assert!(
        matches!(worker.execute_workflow_task(cancellation_task(vec![canonical_request(), canonical_delivery(999, "timer")])),
        Err(Error::NonDeterministicReplay(ReplayFailure { reason, .. })) if reason == "cooperative_cancellation_call_mismatch")
    );
}

#[test]
fn cooperative_replay_shields_nested_cleanup_and_preserves_original_request_after_drop() {
    let ctx = workflow_context(vec![canonical_request(), canonical_delivery(1, "timer")]);
    let mut timer = Box::pin(ctx.sleep(Duration::from_secs(5)));
    let mut cx = TaskContext::from_waker(noop_waker_ref());
    assert!(matches!(
        timer.as_mut().poll(&mut cx),
        Poll::Ready(Err(Error::CooperativeCancellationRequested(_)))
    ));
    assert!(ctx.is_cancellation_requested().unwrap());
    let outer = ctx.cancellation_shield().unwrap();
    let inner = ctx.cancellation_shield().unwrap();
    ctx.throw_if_cancellation_requested().unwrap();
    drop(inner);
    ctx.throw_if_cancellation_requested().unwrap();
    drop(outer);
    let Error::CooperativeCancellationRequested(cancellation) =
        ctx.throw_if_cancellation_requested().unwrap_err()
    else {
        panic!("original cancellation missing")
    };
    assert_eq!(cancellation.request.request_id, "original-request");
    assert_eq!(
        cancellation.request.cleanup_deadline_at,
        "2026-10-01T08:10:00Z"
    );
}

#[test]
fn cooperative_replay_cannot_shield_away_a_committed_authored_delivery() {
    let ctx = workflow_context(vec![canonical_request(), canonical_delivery(1, "timer")]);
    let _shield = ctx.cancellation_shield().unwrap();
    let mut timer = Box::pin(ctx.sleep(Duration::from_secs(5)));
    let mut cx = TaskContext::from_waker(noop_waker_ref());
    assert!(matches!(
        timer.as_mut().poll(&mut cx),
        Poll::Ready(Err(Error::NonDeterministicReplay(_)))
    ));
}

#[test]
fn cooperative_history_preserves_observation_without_authorizing_delivery() {
    let observation = CancellationRequest::from_observation(&request_observation()).unwrap();
    let state = CancellationHistory::from_events(&[canonical_request()], "run", Some(&observation))
        .unwrap();
    assert_eq!(state.request.unwrap(), observation);
    assert_eq!(state.delivery, None);
    let pending = history(&[canonical_request()]).unwrap();
    assert!(pending.eligible(1, 1));
    assert_eq!(pending.request.unwrap().history_refresh_page_token, None);
}

#[test]
fn cooperative_history_preserves_earlier_results_and_allows_later_unresolved_calls() {
    let state = history(&[
        event("ActivityCompleted", json!({"sequence":1})),
        canonical_request(),
        event("TimerFired", json!({"sequence":2})),
    ])
    .unwrap();
    assert!(!state.eligible(1, 1));
    assert!(state.eligible(2, 1));
    assert!(state.eligible(1, 2));
}

#[test]
fn cooperative_history_preserves_prior_parallel_failure_and_committed_selection() {
    for prior in [
        event("ActivityFailed", json!({"sequence":1})),
        event(
            "SelectionResolved",
            json!({
                "selection_group_base_sequence":1,"selection_group_size":2
            }),
        ),
    ] {
        let state = history(&[prior.clone(), canonical_request()]).unwrap();
        assert!(!state.eligible(1, 2));
        let mut marker = canonical_delivery(1, "parallel");
        marker.payload["sequence_span"] = json!(2);
        assert!(matches!(
            history(&[prior, canonical_request(), marker]),
            Err(Error::NonDeterministicReplay(_))
        ));
    }
}

#[test]
fn cooperative_history_preserves_cancelled_selection_member_ranges() {
    let state = history(&[
        event(
            "SelectionOperationCancelled",
            json!({"member_base_sequence":3,"member_size":2}),
        ),
        canonical_request(),
    ])
    .unwrap();
    assert!(!state.eligible(3, 2));
    assert!(state.eligible(4, 2));
}

#[test]
fn cooperative_history_cold_delivery_retains_parallel_and_selection_operation_ranges() {
    let mut parallel = canonical_delivery(2, "parallel");
    parallel.payload["sequence_span"] = json!(3);
    let state = history(&[canonical_request(), parallel]).unwrap();
    assert_eq!(state.request_index, 0);
    assert_eq!(state.delivery_index, Some(1));
    assert!(!state.eligible(5, 1));
    let delivery = state.delivery.unwrap();
    assert_eq!(
        (1..=5)
            .map(|sequence| delivery.interrupts(sequence))
            .collect::<Vec<_>>(),
        vec![false, true, true, true, false]
    );
    let mut selected = canonical_delivery(6, "selection_handle");
    selected.payload["operation_sequence"] = json!(2);
    selected.payload["operation_sequence_span"] = json!(2);
    let delivered = history(&[canonical_request(), selected])
        .unwrap()
        .delivery
        .unwrap();
    assert!(delivered.interrupts(2));
    assert!(delivered.interrupts(3));
    assert!(!delivered.interrupts(4));
    assert!(!delivered.interrupts(6));
}

#[test]
fn cooperative_history_rejects_reordered_duplicate_and_mismatched_markers() {
    for events in [
        vec![canonical_delivery(2, "timer")],
        vec![canonical_request(), canonical_request()],
        vec![
            canonical_request(),
            canonical_delivery(2, "timer"),
            canonical_delivery(2, "timer"),
        ],
        vec![canonical_delivery(2, "timer"), canonical_request()],
    ] {
        assert!(matches!(
            history(&events),
            Err(Error::NonDeterministicReplay(_))
        ));
    }
    for (field, value) in [
        ("workflow_command_id", json!("different")),
        ("workflow_run_id", json!("different")),
        ("workflow_command_id", json!(" ")),
        ("workflow_run_id", Value::Null),
    ] {
        let mut marker = canonical_delivery(2, "timer");
        marker.payload[field] = value;
        assert!(matches!(
            history(&[canonical_request(), marker]),
            Err(Error::NonDeterministicReplay(_))
        ));
    }
    let mut request = canonical_request();
    request
        .raw
        .insert("workflow_command_id".into(), json!("different"));
    assert!(matches!(
        history(&[request]),
        Err(Error::NonDeterministicReplay(_))
    ));
}

#[test]
fn cooperative_history_rejects_changed_observation_identity_deadline_and_malformed_time() {
    for (field, value) in [
        ("request_id", json!("different")),
        ("cleanup_deadline_at", json!("2026-10-01T08:11:00Z")),
        ("requested_at", json!("no timestamp")),
        ("history_refresh_page_token", json!(" ")),
    ] {
        let mut observation = request_observation();
        observation[field] = value;
        match CancellationRequest::from_observation(&observation) {
            Ok(observed) => assert!(matches!(
                CancellationHistory::from_events(&[canonical_request()], "run", Some(&observed)),
                Err(Error::NonDeterministicReplay(_))
            )),
            Err(error) => assert!(matches!(error, Error::InvalidCooperativeCancellation(_))),
        }
    }
    let mut request = canonical_request();
    request.raw.insert("recorded_at".into(), json!("bad"));
    assert!(matches!(
        history(&[request]),
        Err(Error::NonDeterministicReplay(_))
    ));
}

#[test]
fn cooperative_history_accepts_equivalent_deadline_timezone_without_changing_original_text() {
    let mut observation = request_observation();
    observation["cleanup_deadline_at"] = json!("2026-10-01T10:10:00+02:00");
    let observed = CancellationRequest::from_observation(&observation).unwrap();
    let state =
        CancellationHistory::from_events(&[canonical_request()], "run", Some(&observed)).unwrap();
    assert_eq!(state.request.unwrap(), observed);
}

#[test]
fn cooperative_history_rejects_invalid_call_ranges_and_portable_integer_overflow() {
    for (field, value) in [
        ("sequence", json!(0)),
        ("sequence", json!(true)),
        ("sequence", json!("2")),
        ("sequence", json!(i64::MAX)),
        ("sequence", json!(u64::MAX)),
        ("call_kind", json!("unknown")),
        ("sequence_span", json!(2)),
        ("sequence_span", Value::Null),
        ("operation_sequence", json!(1)),
        ("operation_sequence_span", json!(2)),
    ] {
        let mut marker = canonical_delivery(2, "timer");
        marker.payload[field] = value;
        assert!(matches!(
            history(&[canonical_request(), marker]),
            Err(Error::NonDeterministicReplay(_))
        ));
    }
    for (base, span) in [(0, 1), (6, 1), (5, 2), (2, 1001)] {
        let mut selected = canonical_delivery(6, "selection_handle");
        selected.payload["operation_sequence"] = json!(base);
        selected.payload["operation_sequence_span"] = json!(span);
        assert!(matches!(
            history(&[canonical_request(), selected]),
            Err(Error::NonDeterministicReplay(_))
        ));
    }
    let mut parallel = canonical_delivery(2, "parallel");
    parallel.payload["sequence_span"] = json!(1001);
    assert!(matches!(
        history(&[canonical_request(), parallel]),
        Err(Error::NonDeterministicReplay(_))
    ));
    assert!(!history(&[canonical_request()])
        .unwrap()
        .eligible(u64::MAX, 1));
}

#[test]
fn cooperative_history_rejects_delivery_over_an_earlier_resolved_call() {
    for kind in [
        "ActivityCompleted",
        "ActivityFailed",
        "ActivityCancelled",
        "ActivityTimedOut",
        "TimerFired",
        "TimerCancelled",
        "ConditionWaitSatisfied",
        "ConditionWaitTimedOut",
        "SignalApplied",
        "ChildRunCompleted",
        "ChildRunFailed",
        "ChildRunCancelled",
        "ChildRunTerminated",
    ] {
        assert!(
            matches!(
                history(&[
                    event(kind, json!({"sequence":2})),
                    canonical_request(),
                    canonical_delivery(2, "timer")
                ]),
                Err(Error::NonDeterministicReplay(_))
            ),
            "{kind}"
        );
    }
}

fn responses(path: &str, _body: &str, number: usize) -> Option<(&'static str, String)> {
    let case = path.split('/').nth(1).unwrap_or_default();
    if path.ends_with("/api/cluster/info") {
        if case == "discovery-http" {
            return Some(("403 Forbidden", r#"{"reason":"forbidden"}"#.into()));
        }
        if case == "budget" {
            thread::sleep(Duration::from_millis(2500));
        }
        let mut discovery = json!({"worker_protocol": {
            "version": "1.20", "server_capabilities": {"cooperative_cancellation": true}
        }});
        match case {
            "unsupported" => {
                discovery["worker_protocol"]["server_capabilities"]["cooperative_cancellation"] =
                    json!(false)
            }
            "missing" => discovery = json!({}),
            "string-flag" => {
                discovery["worker_protocol"]["server_capabilities"]["cooperative_cancellation"] =
                    json!("true")
            }
            "old" => discovery["worker_protocol"]["version"] = json!("1.19"),
            "future-major" => discovery["worker_protocol"]["version"] = json!("2.20"),
            "malformed-version" => discovery["worker_protocol"]["version"] = json!("1.+20"),
            "new-minor" => discovery["worker_protocol"]["version"] = json!("1.21"),
            _ => {}
        }
        return Some(("200 OK", discovery.to_string()));
    }
    if !path.ends_with("/request-cancellation") {
        return None;
    }
    if case == "refused" {
        return Some((
            "409 Conflict",
            r#"{"reason":"active_claim_cancellation_not_supported"}"#.into(),
        ));
    }
    if case == "budget" {
        thread::sleep(Duration::from_secs(3));
    }
    let mut response = json!({
        "accepted": true, "duplicate": number > 1,
        "workflow_id": "workflow", "run_id": "run",
        "cancellation_request": request_observation()
    });
    match case {
        "escaped" => {
            response["workflow_id"] = json!("workflow/with?path");
            response["run_id"] = json!("run/selected");
        }
        "wrong-workflow" => response["workflow_id"] = json!("other"),
        "wrong-run" => response["run_id"] = json!("other"),
        "empty-run" => response["run_id"] = json!(" "),
        "not-accepted" => response["accepted"] = json!(false),
        "accepted-string" => response["accepted"] = json!("true"),
        "duplicate-integer" => response["duplicate"] = json!(0),
        "missing-request" => response["cancellation_request"] = Value::Null,
        "blank-request" => response["cancellation_request"]["request_id"] = json!(" "),
        "blank-token" => {
            response["cancellation_request"]["history_refresh_page_token"] = json!(" ")
        }
        "no-timezone" => {
            response["cancellation_request"]["requested_at"] = json!("2026-10-01T08:00:00")
        }
        "invalid-date" => {
            response["cancellation_request"]["requested_at"] = json!("2026-02-30T08:00:00Z")
        }
        "equal-deadline" => {
            response["cancellation_request"]["cleanup_deadline_at"] = json!("2026-10-01T08:00:00Z")
        }
        "earlier-deadline" => {
            response["cancellation_request"]["cleanup_deadline_at"] = json!("2026-10-01T07:59:00Z")
        }
        _ => {}
    }
    Some(("202 Accepted", response.to_string()))
}

fn server() -> MockWorkerServer {
    MockWorkerServer::start_with_behavior(MockWorkerBehavior {
        request_override: Some(responses),
        ..MockWorkerBehavior::default()
    })
}

fn client(server: &MockWorkerServer, case: &str) -> Client {
    Client::builder(format!("{}/{case}", server.base_url()))
        .control_token(Some("control-only".to_string()))
        .worker_token(Some("worker-only".to_string()))
        .namespace("caller-namespace")
        .build()
        .unwrap()
}

fn transport_task() -> WorkflowTask {
    let mut task = cancellation_task(vec![scheduled_timer(1)]);
    task.task_id = "task/selected".into();
    task.workflow_task_attempt = 7;
    task.lease_owner = Some("actual-owner".into());
    task
}

fn transport_delivery() -> CancellationDelivery {
    CancellationDelivery::from_payload(&canonical_delivery(1, "timer").payload).unwrap()
}

fn pending_child_delivery(task_id: &str) -> Value {
    json!({"delivered":false, "task_id":task_id, "workflow_run_id":"run",
        "reason":"cancellation_waiting_for_child", "claim_released":true,
        "request_id":null, "sequence":null, "call_kind":null, "sequence_span":null,
        "operation_sequence":null, "operation_sequence_span":null})
}

fn transport_responses(path: &str, body: &str, number: usize) -> Option<(&'static str, String)> {
    let case = path.split('/').nth(1).unwrap_or_default();
    let body: Value = serde_json::from_str(body).unwrap();
    if case == "refused" || (case == "lost-ack" && path.ends_with("/deliver-cancellation")) {
        return Some((
            "409 Conflict",
            json!({"reason":"lease_owner_mismatch"}).to_string(),
        ));
    }
    if case == "budget" {
        thread::sleep(Duration::from_secs(3));
    }
    if case == "heartbeat-budget" {
        thread::sleep(Duration::from_millis(5200));
    }
    if case == "claim-budget" {
        thread::sleep(Duration::from_secs(3));
    }
    if path.ends_with("/workflow-tasks/poll") {
        if case == "claim-retry" && number == 1 {
            // The first HTTP reply is unreadable after acquisition. Retrying
            // must reuse the same durable poll identity rather than claim twice.
            return Some(("invalid-status", String::new()));
        }
        if matches!(case, "claim-idle" | "claim-stop") {
            return Some((
                "200 OK",
                json!({"task":null,
                    "poll_status":if case == "claim-stop" { "draining" } else { "idle" },
                    "reason":if case == "claim-stop" { "worker_draining" } else { "no_tasks" },
                    "protocol_version":"1.20"
                })
                .to_string(),
            ));
        }
        let mut task = json!({
            "task_id":"task/selected", "workflow_type":"cancel", "run_id":"run",
            "lease_owner":"actual-owner", "workflow_task_attempt":7,
            "payload_codec":DEFAULT_CODEC, "history_events":[],
            "cancel_requested":true, "cancellation_request":request_observation()
        });
        match case {
            "claim-no-observation" => {
                task.as_object_mut().unwrap().remove("cancellation_request");
            }
            "claim-missing-attempt" => {
                task.as_object_mut()
                    .unwrap()
                    .remove("workflow_task_attempt");
            }
            "claim-zero-attempt" => task["workflow_task_attempt"] = json!(0),
            "claim-owner" => task["lease_owner"] = json!("replacement"),
            "claim-missing-owner" => task["lease_owner"] = Value::Null,
            "claim-missing-run" => task["run_id"] = Value::Null,
            "claim-missing-id" => task["task_id"] = json!(" "),
            "claim-bad-observation" => task["cancellation_request"] = json!(false),
            "claim-bad-token" => {
                task["cancellation_request"]["history_refresh_page_token"] = json!(" ")
            }
            _ => {}
        }
        if matches!(
            case,
            "claim-history"
                | "claim-budget"
                | "cycle"
                | "page-bound"
                | "empty-progress"
                | "oversized-page"
                | "wrong-task"
                | "wrong-attempt"
                | "malformed-event"
                | "empty-token"
                | "missing-token"
        ) {
            task["next_history_page_token"] = json!("opaque-server-token");
        }
        return Some((
            "200 OK",
            json!({"task":task,"protocol_version":"1.20",
                "server_capabilities":{"workflow_memo_updates":{"supported":true}}
            })
            .to_string(),
        ));
    }
    if path.ends_with("/deliver-cancellation")
        && (case.starts_with("child-wait") || case.starts_with("activity-wait"))
    {
        let mut pending = pending_child_delivery("task/selected");
        if case.starts_with("activity-wait") {
            pending["reason"] = json!("cancellation_waiting_for_activity");
        }
        let suffix = case
            .strip_prefix("child-wait")
            .or_else(|| case.strip_prefix("activity-wait"))
            .unwrap();
        match suffix {
            "-wrong-task" => pending["task_id"] = json!("other"),
            "-wrong-run" => pending["workflow_run_id"] = json!("other"),
            "-not-released" => pending["claim_released"] = json!(false),
            "-string-released" => pending["claim_released"] = json!("true"),
            "-missing-release" => {
                pending.as_object_mut().unwrap().remove("claim_released");
            }
            "-wrong-reason" => pending["reason"] = json!("other"),
            "-request" => pending["request_id"] = body["request_id"].clone(),
            "-sequence" => pending["sequence"] = json!(1),
            "-kind" => pending["call_kind"] = json!("child"),
            "-span" => pending["sequence_span"] = json!(1),
            "-operation" => pending["operation_sequence"] = json!(1),
            "-operation-span" => pending["operation_sequence_span"] = json!(1),
            "-string-delivered" => pending["delivered"] = json!("false"),
            _ => {}
        }
        return Some(("200 OK", pending.to_string()));
    }
    let mut response = if path.ends_with("/deliver-cancellation") {
        json!({"delivered":true, "task_id":"task/selected", "workflow_run_id":"run",
            "request_id":body["request_id"], "sequence":body["sequence"], "call_kind":body["call_kind"],
            "sequence_span":body["sequence_span"], "operation_sequence":body["operation_sequence"],
            "operation_sequence_span":body["operation_sequence_span"], "reason":null})
    } else if path.ends_with("/heartbeat") {
        json!({"task_id":"task/selected", "workflow_task_attempt":7,
            "lease_owner":"actual-owner", "renewed":true,
            "lease_expires_at":"2026-10-01T08:00:30Z", "run_status":"running",
            "task_status":"leased", "reason":null, "cancellation_request":request_observation()})
    } else if path.ends_with("/history") {
        let first = body["next_history_page_token"] == "opaque-server-token";
        let events = if first {
            vec![scheduled_timer(1), canonical_request()]
        } else {
            vec![canonical_delivery(1, "timer")]
        };
        let events = events
            .into_iter()
            .map(|event| {
                let mut value = event.raw;
                value.insert("event_type".into(), json!(event.event_type));
                value.insert("payload".into(), event.payload);
                Value::Object(value.into_iter().collect())
            })
            .collect::<Vec<_>>();
        let mut page = json!({"task_id":"task/selected", "workflow_task_attempt":7,
            "history_events":events, "total_history_events":3,
            "next_history_page_token":if first { json!("page-next") } else { Value::Null }});
        match case {
            "cycle" if !first => page["next_history_page_token"] = json!("opaque-server-token"),
            "page-bound" => {
                page["history_events"] =
                    json!([{"event_type":"OpaqueUnrelatedEvent", "payload":{}}]);
                page["next_history_page_token"] = json!(format!("next-{number}"));
            }
            "empty-progress" => page["history_events"] = json!([]),
            "oversized-page" => {
                page["history_events"] = json!(vec![
                    json!({"event_type":"OpaqueUnrelatedEvent"});
                    WORKFLOW_HISTORY_PAGE_SIZE as usize + 1
                ])
            }
            "missing-request" => {
                page["history_events"] = if first {
                    json!([{"event_type":"TimerScheduled", "payload":{"sequence":1,"delay_seconds":5}}])
                } else {
                    json!([])
                };
            }
            "changed-canonical" if !first => {
                page["history_events"][0]["payload"]["workflow_command_id"] = json!("changed")
            }
            "malformed-event" => page["history_events"] = json!([{"event_type":false}]),
            "wrong-attempt" => page["workflow_task_attempt"] = json!(8),
            "empty-token" => page["next_history_page_token"] = json!(""),
            "missing-token" => {
                page.as_object_mut()
                    .unwrap()
                    .remove("next_history_page_token");
            }
            _ => {}
        }
        page
    } else {
        return None;
    };
    match case {
        "wrong-task" => response["task_id"] = json!("other-task"),
        "wrong-run" => response["workflow_run_id"] = json!("other-run"),
        "heartbeat-attempt" => response["workflow_task_attempt"] = json!(8),
        "heartbeat-owner" => response["lease_owner"] = json!("replacement-owner"),
        "heartbeat-not-renewed" => response["renewed"] = json!(false),
        "heartbeat-string-renewed" => response["renewed"] = json!("true"),
        "heartbeat-closed" => response["run_status"] = json!("cancelled"),
        "heartbeat-unknown-run" => response["run_status"] = json!("unknown"),
        "heartbeat-finished-task" => response["task_status"] = json!("completed"),
        "heartbeat-expiry" => response["lease_expires_at"] = json!("bad"),
        "heartbeat-expiry-no-zone" => response["lease_expires_at"] = json!("2026-10-01T08:00:30"),
        "heartbeat-malformed-request" => response["cancellation_request"] = json!(false),
        "heartbeat-missing-token" => {
            response["cancellation_request"]
                .as_object_mut()
                .unwrap()
                .remove("history_refresh_page_token");
        }
        "heartbeat-changed-request" => {
            response["cancellation_request"]["request_id"] = json!("changed")
        }
        "heartbeat-changed-request-time" => {
            response["cancellation_request"]["requested_at"] = json!("2026-10-01T08:00:01Z")
        }
        "heartbeat-changed-deadline" => {
            response["cancellation_request"]["cleanup_deadline_at"] = json!("2026-10-01T08:11:00Z")
        }
        "heartbeat-equivalent-time" => {
            response["cancellation_request"]["requested_at"] = json!("2026-10-01T10:00:00+02:00");
            response["cancellation_request"]["cleanup_deadline_at"] =
                json!("2026-10-01T10:10:00+02:00");
            response["cancellation_request"]["history_refresh_page_token"] =
                json!("fresh-server-token");
        }
        "heartbeat-no-request" => {
            response
                .as_object_mut()
                .unwrap()
                .remove("cancellation_request");
        }
        "wrong-request" => response["request_id"] = json!("other-request"),
        "wrong-sequence" => response["sequence"] = json!(2),
        "wrong-kind" => response["call_kind"] = json!("activity"),
        "wrong-span" => response["sequence_span"] = json!(2),
        "false-delivered" => response["delivered"] = json!(false),
        "string-delivered" => response["delivered"] = json!("true"),
        "wrong-reason" => response["reason"] = json!("not_committed"),
        "missing-span" => {
            response.as_object_mut().unwrap().remove("sequence_span");
        }
        "missing-operation" => {
            response
                .as_object_mut()
                .unwrap()
                .remove("operation_sequence");
        }
        "missing-operation-span" => {
            response
                .as_object_mut()
                .unwrap()
                .remove("operation_sequence_span");
        }
        _ => {}
    }
    Some(("200 OK", response.to_string()))
}

fn transport_server() -> MockWorkerServer {
    MockWorkerServer::start_with_behavior(MockWorkerBehavior {
        request_override: Some(transport_responses),
        ..Default::default()
    })
}

fn remote_activity_responses(
    path: &str,
    body: &str,
    number: usize,
) -> Option<(&'static str, String)> {
    let case = path.split('/').nth(1).unwrap_or_default();
    if path.ends_with("/cluster/info") {
        return responses(path, body, number);
    }
    if path.ends_with("/worker/register") {
        let request: Value = serde_json::from_str(body).unwrap();
        let mut reply = json!({"registered":true,"worker_id":request["worker_id"],
            "namespace":"caller-namespace","task_queue":request["task_queue"],
            "capabilities":request["capabilities"],"heartbeat_interval_seconds":3600,
            "protocol_version":"1.20","server_capabilities":{"cooperative_cancellation":true}});
        match case {
            "register-old-protocol" => reply["protocol_version"] = json!("1.19"),
            "register-wrong-major" => reply["protocol_version"] = json!("2.0"),
            "register-malformed-protocol" => reply["protocol_version"] = json!("1.20garbage"),
            "register-missing-protocol" => {
                reply.as_object_mut().unwrap().remove("protocol_version");
            }
            "register-unsupported" | "register-cleanup-refused" => {
                reply["server_capabilities"]["cooperative_cancellation"] = json!(false)
            }
            "register-string-support" => {
                reply["server_capabilities"]["cooperative_cancellation"] = json!("true")
            }
            "register-missing-support" => reply["server_capabilities"] = json!({}),
            "register-wrong-worker" => reply["worker_id"] = json!("other-worker"),
            "register-wrong-namespace" => reply["namespace"] = json!("other-namespace"),
            "register-wrong-queue" => reply["task_queue"] = json!("other-queue"),
            "register-missing-capability" => {
                reply["capabilities"] = json!(["cooperative_cancellation"])
            }
            "register-no-cooperative" => reply["capabilities"]
                .as_array_mut()
                .unwrap()
                .retain(|value| value != "cooperative_cancellation"),
            "register-declined" => reply["registered"] = json!(false),
            "register-newer" => reply["protocol_version"] = json!("1.21"),
            "register-replaced" if number >= 2 => reply["worker_id"] = json!("replacement"),
            _ => {}
        }
        return Some(("200 OK", reply.to_string()));
    }
    if path.ends_with("/worker/heartbeat") {
        return Some(("200 OK", json!({}).to_string()));
    }
    if path.ends_with("/worker/registrations/actual-owner") {
        if case == "register-cleanup-refused" {
            return Some((
                "503 Service Unavailable",
                json!({"reason":"backend_unavailable"}).to_string(),
            ));
        }
        return Some(("200 OK", json!({"worker_id":"actual-owner","outcome":"deregistered","recovered_workflow_task_count":1}).to_string()));
    }
    if path.ends_with("/activity-tasks/poll") {
        if number > if case == "remote-concurrency" { 3 } else { 1 } {
            return Some((
                "200 OK",
                json!({"task":null,"poll_status":"timeout"}).to_string(),
            ));
        }
        let mut task = json!({"task_id":"activity","activity_attempt_id":"attempt-original",
            "activity_type":"work","payload_codec":"avro","lease_owner":"actual-owner","attempt_number":3});
        if case == "remote-concurrency" {
            task["task_id"] = json!(format!("activity-{number}"));
            task["activity_attempt_id"] = json!(format!("attempt-{number}"));
        }
        match case {
            "remote-missing-owner" => {
                task.as_object_mut().unwrap().remove("lease_owner");
            }
            "remote-missing-attempt" => {
                task.as_object_mut().unwrap().remove("activity_attempt_id");
            }
            "remote-invented-owner" => task["lease_owner"] = json!("other-worker"),
            "remote-conflicting-attempt" => task["attempt_id"] = json!("other-attempt"),
            _ => {}
        }
        return Some((
            "200 OK",
            json!({"task":task,"poll_status":"leased"}).to_string(),
        ));
    }
    if path.ends_with("/status") {
        let request: Value = serde_json::from_str(body).unwrap();
        if case == "remote-refused" || (case == "remote-replaced" && number >= 2) {
            return Some((
                "409 Conflict",
                json!({"reason":"lease_owner_mismatch"}).to_string(),
            ));
        }
        if case == "remote-offline" {
            return Some((
                "503 Service Unavailable",
                json!({"reason":"backend_unavailable"}).to_string(),
            ));
        }
        if case == "remote-status-slow" {
            std::thread::sleep(Duration::from_millis(5250));
        }
        let mut reply = json!({"task_id":if path.ends_with("/activity%2Fselected/status") { "activity/selected" } else { "activity" },
            "activity_attempt_id":request["activity_attempt_id"],"lease_owner":request["lease_owner"],
            "can_continue":true,"cancel_requested":false,"reason":null,"heartbeat_recorded":false,
            "lease_expires_at":"2099-01-01T00:00:00Z","deadlines":null,"worker_session":null,
            "task_status":"leased","attempt_status":"running","activity_status":"running"});
        if case == "remote-concurrency" {
            reply["task_id"] = json!(path.rsplit('/').nth(1).unwrap());
        }
        match case {
            "remote-wrong-task" => reply["task_id"] = json!("other-task"),
            "remote-wrong-attempt" => reply["activity_attempt_id"] = json!("other-attempt"),
            "remote-wrong-owner" => reply["lease_owner"] = json!("other-worker"),
            "remote-malformed" => reply["can_continue"] = json!("true"),
            "remote-recorded-progress" => reply["heartbeat_recorded"] = json!(true),
            "remote-closed-task" => reply["task_status"] = json!("completed"),
            "remote-closed-attempt" => reply["attempt_status"] = json!("completed"),
            "remote-closed-activity" => reply["activity_status"] = json!("completed"),
            "remote-expired" => reply["lease_expires_at"] = json!("2000-01-01T00:00:00Z"),
            "remote-no-timezone" => reply["lease_expires_at"] = json!("2099-01-01T00:00:00"),
            "remote-deadline" => reply["deadlines"] = json!({"heartbeat":"2000-01-01T00:00:00Z"}),
            "remote-bad-deadlines" => reply["deadlines"] = json!(false),
            "remote-session-expired" => {
                reply["worker_session"] = json!({"status":"active","lease_owner":"actual-owner","lease_expires_at":"2099-01-01T00:00:00Z","ttl_expires_at":"2000-01-01T00:00:00Z"})
            }
            "remote-session-replaced" => {
                reply["worker_session"] = json!({"status":"active","lease_owner":"replacement","lease_expires_at":"2099-01-01T00:00:00Z","ttl_expires_at":"2099-01-01T00:00:00Z"})
            }
            "remote-cancelled" | "remote-late-result"
                if case == "remote-cancelled" || number >= 2 =>
            {
                reply["can_continue"] = json!(false);
                reply["cancel_requested"] = json!(true);
                reply["reason"] = json!("activity_cancelled");
            }
            "remote-heartbeat-post-loss" if number >= 3 => reply["can_continue"] = json!(false),
            _ => {}
        }
        if case.starts_with("remote-stop-joined-") && number >= 2 {
            reply["can_continue"] = json!(false);
            reply["cancel_requested"] = json!(true);
            reply["reason"] = json!("activity_cancelled");
            reply["cancellation_acknowledgement"] = json!({"request_id":"local-request","root_request_id":"root-request",
                "cleanup_deadline_at":"2099-01-01T00:00:30Z","cancellation_history_event_id":"cancel-history","callback_state":"unknown"});
        }
        return Some(("200 OK", reply.to_string()));
    }
    if path.ends_with("/acknowledge-cancellation") {
        let request: Value = serde_json::from_str(body).unwrap();
        if case.starts_with("remote-stop-joined-")
            && std::fs::read_to_string(std::env::temp_dir().join(case))
                .ok()
                .as_deref()
                != Some("dropped")
        {
            return Some((
                "409 Conflict",
                json!({"reason":"callback_not_dropped"}).to_string(),
            ));
        }
        if case == "remote-ack-refused" {
            return Some((
                "409 Conflict",
                json!({"reason":"cancellation_request_mismatch"}).to_string(),
            ));
        }
        let mut reply = json!({"task_id":"activity/selected","activity_attempt_id":request["activity_attempt_id"],
            "lease_owner":request["lease_owner"],"request_id":request["request_id"],"acknowledged":true,
            "duplicate":number > 1,"history_event_id":"stop-history","reason":null,"heartbeat_recorded":false});
        if case.starts_with("remote-stop-joined-") {
            reply["task_id"] = json!("activity");
        }
        match case {
            "remote-ack-task" => reply["task_id"] = json!("wrong"),
            "remote-ack-attempt" => reply["activity_attempt_id"] = json!("wrong"),
            "remote-ack-owner" => reply["lease_owner"] = json!("wrong"),
            "remote-ack-request" => reply["request_id"] = json!("wrong"),
            "remote-ack-unproved" => reply["history_event_id"] = Value::Null,
            "remote-ack-declined" => reply["acknowledged"] = json!(false),
            "remote-ack-progress" => reply["heartbeat_recorded"] = json!(true),
            "remote-ack-duplicate" => reply["duplicate"] = json!("true"),
            "remote-ack-slow" => std::thread::sleep(Duration::from_millis(5250)),
            _ => {}
        }
        return Some(("200 OK", reply.to_string()));
    }
    if path.ends_with("/activity/heartbeat") {
        let mut reply = json!({"task_id":"activity","activity_attempt_id":"attempt-original",
            "lease_owner":"actual-owner","can_continue":true,"cancel_requested":false,"heartbeat_recorded":true});
        match case {
            "remote-heartbeat-wrong-attempt" => reply["activity_attempt_id"] = json!("replacement"),
            "remote-heartbeat-cancelled" => reply["cancel_requested"] = json!(true),
            _ => {}
        }
        return Some(("200 OK", reply.to_string()));
    }
    if path.ends_with("/complete") || path.ends_with("/fail") {
        return Some(("200 OK", json!({"recorded":true}).to_string()));
    }
    None
}

fn remote_activity_server() -> MockWorkerServer {
    MockWorkerServer::start_with_behavior(MockWorkerBehavior {
        request_override: Some(remote_activity_responses),
        ..Default::default()
    })
}

#[tokio::test]
async fn cooperative_registration_uses_explicit_protocol_and_preserves_other_clients() {
    for case in ["register-valid", "register-newer"] {
        let server = remote_activity_server();
        let original = client(&server, case);
        let worker = Worker::new(original.clone(), "queue")
            .worker_id("actual-owner")
            .cooperative_cancellation(true);
        let registration = worker.register().await.unwrap();
        assert!(registration.registered);
        assert!(worker
            .cooperative_registration_confirmed
            .load(Ordering::SeqCst));
        original
            .poll_activity_task("actual-owner", "queue", Duration::ZERO)
            .await
            .unwrap();
        let requests = server.requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].worker_protocol.as_deref(), Some("1.20"));
        assert_eq!(requests[1].worker_protocol.as_deref(), Some("1.19"));
        assert_eq!(
            requests[0].authorization.as_deref(),
            Some("Bearer worker-only")
        );
        assert_eq!(requests[0].namespace.as_deref(), Some("caller-namespace"));
        let body: Value = serde_json::from_str(&requests[0].body).unwrap();
        assert!(body["capabilities"]
            .as_array()
            .unwrap()
            .contains(&json!("cooperative_cancellation")));
        assert_eq!(
            body["capability_manifest"],
            portable_worker_affinity_capability_manifest()
        );
    }
}

#[tokio::test]
async fn cooperative_registration_refuses_incompatible_receipts_before_polling() {
    for case in [
        "register-old-protocol",
        "register-wrong-major",
        "register-malformed-protocol",
        "register-missing-protocol",
        "register-unsupported",
        "register-string-support",
        "register-missing-support",
        "register-wrong-worker",
        "register-wrong-namespace",
        "register-wrong-queue",
        "register-missing-capability",
        "register-no-cooperative",
        "register-declined",
    ] {
        let server = remote_activity_server();
        let worker = coordinator_worker(&server, case);
        assert!(
            matches!(
                worker.register().await,
                Err(Error::CooperativeCancellationUnavailable(_))
            ),
            "{case}"
        );
        assert!(
            matches!(
                worker.run_once().await,
                Err(Error::CooperativeCancellationUnavailable(_))
            ),
            "{case}"
        );
        let requests = server.requests.lock().unwrap();
        assert_eq!(
            requests.len(),
            if matches!(case, "register-wrong-worker" | "register-declined") {
                1
            } else {
                2
            },
            "{case}"
        );
        assert!(requests[0].path.ends_with("/register"));
        assert!(
            !requests
                .iter()
                .any(|request| request.path.ends_with("/poll")),
            "{case}"
        );
        if requests.len() > 1 {
            assert_eq!(requests[1].method, "DELETE");
            assert!(requests[1].path.ends_with("/registrations/actual-owner"));
            assert_eq!(requests[1].worker_protocol.as_deref(), Some("1.20"));
        }
    }
}

#[tokio::test]
async fn cooperative_registration_preserves_cleanup_failure_and_requires_registration() {
    let server = remote_activity_server();
    let worker = coordinator_worker(&server, "register-cleanup-refused");
    assert!(matches!(
        worker.run_once().await,
        Err(Error::CooperativeCancellationUnavailable(_))
    ));
    assert!(server.requests.lock().unwrap().is_empty());
    let error = worker.register().await.unwrap_err();
    assert!(
        matches!(error, Error::WorkerShutdown {primary, deregistration}
        if matches!(*primary, Error::CooperativeCancellationUnavailable(_))
        && matches!(*deregistration, Error::Http { status, .. } if status.as_u16() == 503))
    );
    assert!(!worker
        .cooperative_registration_confirmed
        .load(Ordering::SeqCst));
}

#[tokio::test]
async fn cooperative_registration_refusal_invalidates_previous_confirmation_and_changed_identity() {
    let server = remote_activity_server();
    let worker = coordinator_worker(&server, "register-replaced");
    worker.register().await.unwrap();
    assert!(worker
        .cooperative_registration_confirmed
        .load(Ordering::SeqCst));
    assert!(matches!(
        worker.register().await,
        Err(Error::CooperativeCancellationUnavailable(_))
    ));
    assert!(matches!(
        worker.run_once().await,
        Err(Error::CooperativeCancellationUnavailable(_))
    ));
    assert_eq!(server.requests.lock().unwrap().len(), 2);

    let worker = coordinator_worker(&server, "register-valid");
    worker.register().await.unwrap();
    let changed = worker.worker_id("replacement");
    assert!(matches!(
        changed.run_once().await,
        Err(Error::CooperativeCancellationUnavailable(_))
    ));
    assert_eq!(server.requests.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn cooperative_registration_default_and_disabled_workers_keep_ordinary_protocol() {
    for explicit in [false, true] {
        let server = remote_activity_server();
        let worker =
            Worker::new(client(&server, "register-valid"), "queue").worker_id("actual-owner");
        let worker = if explicit {
            worker
                .cooperative_cancellation(true)
                .cooperative_cancellation(false)
        } else {
            worker
        };
        worker.register().await.unwrap();
        let requests = server.requests.lock().unwrap();
        assert_eq!(requests[0].worker_protocol.as_deref(), Some("1.19"));
        let body: Value = serde_json::from_str(&requests[0].body).unwrap();
        assert!(!body["capabilities"]
            .as_array()
            .unwrap()
            .contains(&json!("cooperative_cancellation")));
    }
}

#[tokio::test]
async fn cooperative_registration_requires_only_worker_credentials() {
    let server = remote_activity_server();
    let client = Client::builder(format!("{}/register-valid", server.base_url()))
        .worker_token(Some("worker-only".into()))
        .namespace("caller-namespace")
        .build()
        .unwrap();
    let worker = Worker::new(client, "queue")
        .worker_id("actual-owner")
        .cooperative_cancellation(true);
    worker.register().await.unwrap();
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0].authorization.as_deref(),
        Some("Bearer worker-only")
    );
    assert!(requests[0].control_protocol.is_none());
}

struct PendingActivityCallback(Arc<AtomicBool>);

impl Future for PendingActivityCallback {
    type Output = Result<Value>;

    fn poll(self: Pin<&mut Self>, _: &mut TaskContext<'_>) -> Poll<Self::Output> {
        Poll::Pending
    }
}

impl Drop for PendingActivityCallback {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn cooperative_activity_observation_uses_the_exact_worker_claim_without_renewal() {
    let server = remote_activity_server();
    let reply = client(&server, "remote-valid")
        .activity_task_status("activity/selected", "attempt-original", "actual-owner")
        .await
        .unwrap();
    assert_eq!(reply["heartbeat_recorded"], false);
    assert_eq!(reply["can_continue"], true);
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0].path,
        "/remote-valid/api/worker/activity-tasks/activity%2Fselected/status"
    );
    assert_eq!(requests[0].worker_protocol.as_deref(), Some("1.20"));
    assert_eq!(
        requests[0].authorization.as_deref(),
        Some("Bearer worker-only")
    );
    assert_eq!(requests[0].namespace.as_deref(), Some("caller-namespace"));
    let body: Value = serde_json::from_str(&requests[0].body).unwrap();
    assert_eq!(
        body,
        json!({"activity_attempt_id":"attempt-original","lease_owner":"actual-owner"})
    );
}

#[tokio::test]
async fn cooperative_activity_observation_rejects_changed_receipts_and_preserves_refusals() {
    for case in [
        "remote-wrong-task",
        "remote-wrong-attempt",
        "remote-wrong-owner",
        "remote-malformed",
        "remote-recorded-progress",
    ] {
        let server = remote_activity_server();
        assert!(
            matches!(
                client(&server, case)
                    .activity_task_status("activity", "attempt-original", "actual-owner")
                    .await,
                Err(Error::InvalidCooperativeCancellation(_))
            ),
            "{case}"
        );
    }
    let server = remote_activity_server();
    let result = client(&server, "remote-refused")
        .activity_task_status("activity", "attempt-original", "actual-owner")
        .await;
    assert!(
        matches!(result, Err(Error::ActivityTaskRejected(ref error)) if error.operation == "status"
        && error.status == 409 && error.reason == "lease_owner_mismatch")
    );
    for (task, attempt, owner) in [
        ("", "attempt", "owner"),
        ("task", "", "owner"),
        ("task", "attempt", ""),
    ] {
        assert!(client(&server, "remote-valid")
            .activity_task_status(task, attempt, owner)
            .await
            .is_err());
    }
    let control_only = Client::builder(format!("{}/remote-valid", server.base_url()))
        .control_token(Some("control-only".to_string()))
        .build()
        .unwrap();
    assert!(matches!(
        control_only
            .activity_task_status("activity", "attempt", "owner")
            .await,
        Err(Error::MissingRoleCredentials { role: "worker", .. })
    ));
    assert_eq!(server.requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn cooperative_activity_stop_receipt_preserves_worker_authentication_and_duplicate_identity()
{
    let server = remote_activity_server();
    let original = client(&server, "remote-ack-valid");
    let first = original
        .acknowledge_activity_cancellation(
            "activity/selected",
            "attempt-original",
            "actual-owner",
            "local-request",
        )
        .await
        .unwrap();
    let duplicate = original
        .acknowledge_activity_cancellation(
            "activity/selected",
            "attempt-original",
            "actual-owner",
            "local-request",
        )
        .await
        .unwrap();
    assert_eq!(first["history_event_id"], duplicate["history_event_id"]);
    assert_eq!(first["duplicate"], false);
    assert_eq!(duplicate["duplicate"], true);
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    for request in requests.iter() {
        assert!(request
            .path
            .ends_with("/activity%2Fselected/acknowledge-cancellation"));
        assert_eq!(request.worker_protocol.as_deref(), Some("1.20"));
        assert_eq!(request.authorization.as_deref(), Some("Bearer worker-only"));
        assert_eq!(request.namespace.as_deref(), Some("caller-namespace"));
        let body: Value = serde_json::from_str(&request.body).unwrap();
        assert_eq!(
            body,
            json!({"activity_attempt_id":"attempt-original","lease_owner":"actual-owner","request_id":"local-request"})
        );
    }
    assert_eq!(WORKER_PROTOCOL_VERSION, "1.19");
}

#[tokio::test]
async fn cooperative_activity_stop_receipt_refuses_changed_fences_unproved_receipts_and_missing_identity(
) {
    for case in [
        "remote-ack-task",
        "remote-ack-attempt",
        "remote-ack-owner",
        "remote-ack-request",
        "remote-ack-unproved",
        "remote-ack-declined",
        "remote-ack-progress",
        "remote-ack-duplicate",
    ] {
        let server = remote_activity_server();
        assert!(
            matches!(
                client(&server, case)
                    .acknowledge_activity_cancellation(
                        "activity/selected",
                        "attempt-original",
                        "actual-owner",
                        "local-request"
                    )
                    .await,
                Err(Error::InvalidCooperativeCancellation(_))
            ),
            "{case}"
        );
    }
    let server = remote_activity_server();
    assert!(
        matches!(client(&server, "remote-ack-refused").acknowledge_activity_cancellation(
        "activity/selected", "attempt-original", "actual-owner", "local-request").await,
        Err(Error::ActivityTaskRejected(ref error)) if error.operation == "acknowledge-cancellation" && error.reason == "cancellation_request_mismatch")
    );
    for (task, attempt, owner, request) in [
        ("", "attempt", "owner", "request"),
        ("task", "", "owner", "request"),
        ("task", "attempt", "", "request"),
        ("task", "attempt", "owner", ""),
    ] {
        assert!(client(&server, "remote-ack-valid")
            .acknowledge_activity_cancellation(task, attempt, owner, request)
            .await
            .is_err());
    }
    assert_eq!(server.requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn cooperative_activity_stop_receipt_has_one_five_second_budget() {
    let server = remote_activity_server();
    let started = Instant::now();
    assert!(matches!(
        client(&server, "remote-ack-slow")
            .acknowledge_activity_cancellation(
                "activity/selected",
                "attempt-original",
                "actual-owner",
                "local-request"
            )
            .await,
        Err(Error::Timeout)
    ));
    assert!(started.elapsed() < Duration::from_millis(5200));
    assert_eq!(server.requests.lock().unwrap().len(), 1);
}

struct StoppedCallbackMarker(std::path::PathBuf);

impl Drop for StoppedCallbackMarker {
    fn drop(&mut self) {
        std::fs::write(&self.0, "dropped").unwrap();
    }
}

#[tokio::test]
async fn cooperative_activity_callback_is_dropped_before_stop_receipt_and_cloned_context_stays_fenced(
) {
    let case = unique_request_id("remote-stop-joined");
    let marker = std::env::temp_dir().join(&case);
    let server = remote_activity_server();
    let mut worker = coordinator_worker(&server, &case);
    let saved = Arc::new(Mutex::new(None::<ActivityContext>));
    let capture = Arc::clone(&saved);
    let callback_marker = marker.clone();
    worker.register_activity("work", move |ctx, _| {
        *capture.lock().unwrap() = Some(ctx);
        let stopped = StoppedCallbackMarker(callback_marker.clone());
        async move {
            let _stopped = stopped;
            std::future::pending::<()>().await;
            Ok(Value::Null)
        }
    });
    let result = worker.poll_activity_once().await;
    let dropped = std::fs::read_to_string(&marker);
    let _ = std::fs::remove_file(&marker);
    assert_eq!(result.unwrap(), ManagedPollOutcome::Handled);
    assert_eq!(dropped.unwrap(), "dropped");
    let before = server.requests.lock().unwrap().len();
    let context = saved.lock().unwrap().as_ref().unwrap().clone();
    assert!(matches!(
        context.heartbeat(json!({"late":true})).await,
        Err(Error::ActivityExecutionAbandoned(_))
    ));
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), before);
    assert_eq!(
        requests.last().unwrap().path.rsplit('/').next(),
        Some("acknowledge-cancellation")
    );
    assert!(!requests
        .iter()
        .any(|request| request.path.ends_with("/heartbeat")
            || request.path.ends_with("/complete")
            || request.path.ends_with("/fail")));
}

#[tokio::test]
async fn cooperative_activity_observation_has_one_five_second_budget() {
    let server = remote_activity_server();
    let start = Instant::now();
    assert!(matches!(
        client(&server, "remote-status-slow")
            .activity_task_status("activity", "attempt-original", "actual-owner")
            .await,
        Err(Error::Timeout)
    ));
    assert!(start.elapsed() < Duration::from_millis(5200));
    assert_eq!(server.requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn cooperative_activity_refuses_execution_without_current_claim_deadlines_and_session() {
    for case in [
        "remote-refused",
        "remote-offline",
        "remote-cancelled",
        "remote-wrong-task",
        "remote-wrong-attempt",
        "remote-wrong-owner",
        "remote-malformed",
        "remote-recorded-progress",
        "remote-expired",
        "remote-closed-task",
        "remote-closed-attempt",
        "remote-closed-activity",
        "remote-no-timezone",
        "remote-deadline",
        "remote-bad-deadlines",
        "remote-session-expired",
        "remote-session-replaced",
    ] {
        let server = remote_activity_server();
        let mut worker = coordinator_worker(&server, case);
        let called = Arc::new(AtomicBool::new(false));
        let invoked = Arc::clone(&called);
        worker.register_activity("work", move |_, _| {
            invoked.store(true, Ordering::SeqCst);
            async { Ok(Value::Null) }
        });
        assert_eq!(
            worker.poll_activity_once().await.unwrap(),
            ManagedPollOutcome::Handled,
            "{case}"
        );
        assert!(!called.load(Ordering::SeqCst), "{case}");
        let requests = server.requests.lock().unwrap();
        assert_eq!(requests.len(), 2, "{case}");
        assert!(requests.last().unwrap().path.ends_with("/status"), "{case}");
    }
    for case in [
        "remote-missing-owner",
        "remote-missing-attempt",
        "remote-invented-owner",
        "remote-conflicting-attempt",
    ] {
        let server = remote_activity_server();
        let worker = coordinator_worker(&server, case);
        assert!(
            matches!(
                worker.poll_activity_once().await,
                Err(Error::InvalidCooperativeCancellation(_))
            ),
            "{case}"
        );
        assert_eq!(server.requests.lock().unwrap().len(), 1, "{case}");
    }
}

#[tokio::test]
async fn cooperative_activity_fences_success_failure_heartbeat_and_late_context() {
    let details: Value = serde_json::from_str(include_str!(
        "../../tests/fixtures/activity-heartbeat-progress.json"
    ))
    .unwrap();
    for fail in [false, true] {
        let server = remote_activity_server();
        let mut worker = coordinator_worker(&server, "remote-valid");
        let saved = Arc::new(Mutex::new(None::<ActivityContext>));
        let capture = Arc::clone(&saved);
        let progress = details.clone();
        worker.register_activity("work", move |ctx, _| {
            *capture.lock().unwrap() = Some(ctx.clone());
            let progress = progress.clone();
            async move {
                assert!(ctx.heartbeat(progress).await?.heartbeat_recorded);
                if fail {
                    return Err(Error::WorkerLoop("application failure".into()));
                }
                Ok(json!("done"))
            }
        });
        assert_eq!(
            worker.poll_activity_once().await.unwrap(),
            ManagedPollOutcome::Handled
        );
        let context = saved.lock().unwrap().as_ref().unwrap().clone();
        let before = server.requests.lock().unwrap().len();
        assert!(matches!(
            context.heartbeat(json!({"late":true})).await,
            Err(Error::ActivityExecutionAbandoned(_))
        ));
        let requests = server.requests.lock().unwrap();
        assert_eq!(requests.len(), before);
        let paths = requests
            .iter()
            .map(|r| r.path.rsplit('/').next().unwrap())
            .collect::<Vec<_>>();
        let mut expected = vec!["poll", "status", "status", "heartbeat", "status", "status"];
        if !fail {
            expected.push("info");
        }
        expected.push(if fail { "fail" } else { "complete" });
        assert_eq!(paths, expected);
        let heartbeat_body: Value = serde_json::from_str(
            &requests
                .iter()
                .find(|r| r.path.ends_with("/heartbeat"))
                .unwrap()
                .body,
        )
        .unwrap();
        assert_eq!(heartbeat_body["details"], details);
        assert_eq!(heartbeat_body["activity_attempt_id"], "attempt-original");
        assert_eq!(heartbeat_body["lease_owner"], "actual-owner");
        let body: Value = serde_json::from_str(&requests.last().unwrap().body).unwrap();
        assert_eq!(body["activity_attempt_id"], "attempt-original");
        assert_eq!(body["lease_owner"], "actual-owner");
        if fail {
            assert_eq!(
                body["failure"]["message"],
                "worker loop error: application failure"
            );
        }
    }
}

#[tokio::test]
async fn cooperative_activity_discards_results_and_failure_after_attempt_replacement() {
    for (case, fail) in [
        ("remote-replaced", false),
        ("remote-replaced", true),
        ("remote-late-result", false),
    ] {
        let server = remote_activity_server();
        let mut worker = coordinator_worker(&server, case);
        worker.register_activity("work", move |_, _| async move {
            if fail {
                Err(Error::WorkerLoop("application failure".into()))
            } else {
                Ok(json!("late"))
            }
        });
        assert_eq!(
            worker.poll_activity_once().await.unwrap(),
            ManagedPollOutcome::Handled
        );
        let requests = server.requests.lock().unwrap();
        assert_eq!(requests.len(), 3);
        assert!(requests.last().unwrap().path.ends_with("/status"));
        assert!(!requests.iter().any(|r| r.path.ends_with("/complete")
            || r.path.ends_with("/fail")
            || r.path.ends_with("/cluster/info")));
    }
}

#[tokio::test]
async fn cooperative_activity_heartbeat_abandons_changed_context_or_lost_authority() {
    for case in [
        "remote-context-changed",
        "remote-heartbeat-wrong-attempt",
        "remote-heartbeat-cancelled",
        "remote-heartbeat-post-loss",
    ] {
        let server = remote_activity_server();
        let mut worker = coordinator_worker(&server, case);
        let changed = case == "remote-context-changed";
        worker.register_activity("work", move |mut ctx, _| async move {
            if changed {
                ctx.activity_attempt_id = "replacement".into();
            }
            assert!(matches!(
                ctx.heartbeat(json!({"progress":1})).await,
                Err(Error::ActivityExecutionAbandoned(_))
            ));
            // Catching abandonment cannot restore publication authority.
            Ok(json!("late"))
        });
        assert_eq!(
            worker.poll_activity_once().await.unwrap(),
            ManagedPollOutcome::Handled
        );
        let requests = server.requests.lock().unwrap();
        assert!(
            !requests
                .iter()
                .any(|r| r.path.ends_with("/complete") || r.path.ends_with("/fail")),
            "{case}"
        );
        assert_eq!(
            requests
                .iter()
                .filter(|r| r.path.ends_with("/activity/heartbeat"))
                .count(),
            if changed { 0 } else { 1 }
        );
    }
}

#[tokio::test]
async fn cooperative_activity_observer_drops_a_pending_callback_and_fences_its_context() {
    let server = remote_activity_server();
    let mut worker = coordinator_worker(&server, "remote-replaced");
    let saved = Arc::new(Mutex::new(None::<ActivityContext>));
    let capture = Arc::clone(&saved);
    let dropped = Arc::new(AtomicBool::new(false));
    let finished = Arc::clone(&dropped);
    worker.register_activity("work", move |ctx, _| {
        *capture.lock().unwrap() = Some(ctx);
        PendingActivityCallback(Arc::clone(&finished))
    });
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), worker.poll_activity_once())
            .await
            .unwrap()
            .unwrap(),
        ManagedPollOutcome::Handled
    );
    let context = saved.lock().unwrap().as_ref().unwrap().clone();
    assert!(dropped.load(Ordering::SeqCst));
    let before = server.requests.lock().unwrap().len();
    assert!(matches!(
        context.heartbeat(Value::Null).await,
        Err(Error::ActivityExecutionAbandoned(_))
    ));
    assert_eq!(server.requests.lock().unwrap().len(), before);
    assert_eq!(before, 3);
}

#[tokio::test]
async fn cooperative_activity_worker_shutdown_abandons_a_pending_callback_before_deregistration() {
    let server = remote_activity_server();
    let mut worker = coordinator_worker(&server, "remote-valid");
    let saved = Arc::new(Mutex::new(None::<ActivityContext>));
    let capture = Arc::clone(&saved);
    let started = Arc::new(tokio::sync::Notify::new());
    let invoked = Arc::clone(&started);
    let dropped = Arc::new(AtomicBool::new(false));
    let finished = Arc::clone(&dropped);
    worker.register_activity("work", move |ctx, _| {
        *capture.lock().unwrap() = Some(ctx);
        invoked.notify_one();
        PendingActivityCallback(Arc::clone(&finished))
    });
    tokio::time::timeout(Duration::from_secs(3), worker.run_until(started.notified()))
        .await
        .unwrap()
        .unwrap();
    let context = saved.lock().unwrap().as_ref().unwrap().clone();
    assert!(dropped.load(Ordering::SeqCst));
    let before = server.requests.lock().unwrap().len();
    assert!(matches!(
        context.heartbeat(Value::Null).await,
        Err(Error::ActivityExecutionAbandoned(_))
    ));
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), before);
    let deregistration = requests.last().unwrap();
    assert!(deregistration
        .path
        .ends_with("/worker/registrations/actual-owner"));
    assert_eq!(deregistration.method, "DELETE");
    assert!(!requests
        .iter()
        .any(|r| r.path.ends_with("/complete") || r.path.ends_with("/fail")));
}

#[tokio::test]
async fn cooperative_activity_dropped_poll_future_abandons_cloned_context() {
    let server = remote_activity_server();
    let mut worker = coordinator_worker(&server, "remote-valid");
    let saved = Arc::new(Mutex::new(None::<ActivityContext>));
    let capture = Arc::clone(&saved);
    let started = Arc::new(tokio::sync::Notify::new());
    let invoked = Arc::clone(&started);
    let dropped = Arc::new(AtomicBool::new(false));
    let finished = Arc::clone(&dropped);
    worker.register_activity("work", move |ctx, _| {
        *capture.lock().unwrap() = Some(ctx);
        invoked.notify_one();
        PendingActivityCallback(Arc::clone(&finished))
    });
    let poll = tokio::spawn(async move { worker.poll_activity_once().await });
    tokio::time::timeout(Duration::from_secs(3), started.notified())
        .await
        .unwrap();
    poll.abort();
    assert!(poll.await.unwrap_err().is_cancelled());
    assert!(dropped.load(Ordering::SeqCst));
    let context = saved.lock().unwrap().as_ref().unwrap().clone();
    let before = server.requests.lock().unwrap().len();
    assert!(matches!(
        context.heartbeat(Value::Null).await,
        Err(Error::ActivityExecutionAbandoned(_))
    ));
    assert_eq!(server.requests.lock().unwrap().len(), before);
}

#[tokio::test]
async fn cooperative_activity_concurrency_reuses_slots_and_stops_before_deregistering() {
    struct ActiveCallback(Arc<AtomicUsize>);
    impl Drop for ActiveCallback {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }
    let server = remote_activity_server();
    let mut worker =
        coordinator_worker(&server, "remote-concurrency").max_concurrent_activity_tasks(2);
    let active = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let (entered_tx, mut entered_rx) = tokio::sync::mpsc::unbounded_channel();
    let callbacks = Arc::clone(&active);
    let permits = Arc::clone(&release);
    worker.register_activity("work", move |ctx, _| {
        let callbacks = Arc::clone(&callbacks);
        let permits = Arc::clone(&permits);
        let entered_tx = entered_tx.clone();
        async move {
            callbacks.fetch_add(1, Ordering::SeqCst);
            let _active = ActiveCallback(callbacks);
            entered_tx.send(ctx.task_id).unwrap();
            permits.acquire().await.unwrap().forget();
            Ok(Value::Null)
        }
    });
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let running = tokio::spawn(async move {
        worker
            .run_until(async {
                let _ = shutdown_rx.await;
            })
            .await
    });
    let mut entered = Vec::new();
    for _ in 0..2 {
        entered.push(
            tokio::time::timeout(Duration::from_secs(3), entered_rx.recv())
                .await
                .expect("both configured slots must start callbacks")
                .unwrap(),
        );
    }
    assert_eq!(active.load(Ordering::SeqCst), 2);
    assert!(
        tokio::time::timeout(Duration::from_millis(100), entered_rx.recv())
            .await
            .is_err(),
        "a third callback cannot exceed the configured capacity"
    );
    release.add_permits(1);
    entered.push(
        tokio::time::timeout(Duration::from_secs(3), entered_rx.recv())
            .await
            .expect("a settled callback must release its slot")
            .unwrap(),
    );
    entered.sort();
    assert_eq!(entered, ["activity-1", "activity-2", "activity-3"]);
    assert_eq!(active.load(Ordering::SeqCst), 2);
    shutdown_tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(3), running)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        active.load(Ordering::SeqCst),
        0,
        "shutdown must drop all managed callbacks"
    );
    let requests = server.requests.lock().unwrap();
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.path.ends_with("/registrations/actual-owner"))
            .count(),
        1
    );
    assert!(
        requests
            .last()
            .unwrap()
            .path
            .ends_with("/registrations/actual-owner"),
        "all lanes must stop before deregistration"
    );
}

fn coordinator_responses(path: &str, body: &str, number: usize) -> Option<(&'static str, String)> {
    let case = path.split('/').nth(1).unwrap_or_default();
    if path.ends_with("/cluster/info") {
        return responses(path, body, number);
    }
    if path.ends_with("/poll") {
        if case == "coordinator-retry" && number == 1 {
            return Some(("invalid-status", String::new()));
        }
        let (status, response) = transport_responses(path, body, number)?;
        let mut response: Value = serde_json::from_str(&response).unwrap();
        if case == "coordinator-prefix" && number >= 2 {
            response["task"]["task_id"] = json!("successor");
            response["task"]["workflow_task_attempt"] = json!(8);
        }
        if case == "coordinator-saga" && number >= 2 {
            response["task"]["task_id"] = json!("cleanup");
            response["task"]["workflow_task_attempt"] = json!(9);
        }
        if case == "coordinator-child-wait" && number == 2 {
            response["task"]["task_id"] = json!("other-task");
            response["task"]["workflow_type"] = json!("other");
            response["task"]["run_id"] = json!("other-run");
            response["task"]["cancel_requested"] = json!(false);
            response["task"]
                .as_object_mut()
                .unwrap()
                .remove("cancellation_request");
        }
        if case == "coordinator-child-wait" && number >= 3 {
            response["task"]["task_id"] = json!("parent-resume-task");
            response["task"]["workflow_task_attempt"] = json!(8);
        }
        return Some((status, response.to_string()));
    }
    if path.ends_with("/history") {
        if case == "coordinator-owner-lost" && number >= 2 {
            return Some((
                "409 Conflict",
                json!({"reason":"lease_owner_mismatch"}).to_string(),
            ));
        }
        let prefix = case == "coordinator-prefix";
        let successor = prefix && path.ends_with("/successor/history");
        let child_wait = case == "coordinator-child-wait";
        let child_resume = child_wait && path.ends_with("/parent-resume-task/history");
        let mut events = if child_wait {
            vec![pending_scalar_event("child"), canonical_request()]
        } else if prefix {
            vec![canonical_request()]
        } else {
            vec![scheduled_timer(1), canonical_request()]
        };
        if successor {
            events.push(event(
                "SideEffectRecorded",
                json!({"sequence":1,"result":fixture_envelope(json!(7))}),
            ));
        }
        let cleanup = case == "coordinator-saga" && path.ends_with("/cleanup/history");
        let delivered = if child_wait {
            child_resume && number >= 2
        } else if prefix {
            successor && number >= 2
        } else {
            number >= 2 || case == "coordinator-cold" || cleanup
        };
        if delivered && !matches!(case, "coordinator-unproved" | "coordinator-shield") {
            events.push(canonical_delivery(
                if prefix { 2 } else { 1 },
                if child_wait {
                    "child"
                } else if case == "coordinator-mismatch" {
                    "activity"
                } else {
                    "timer"
                },
            ));
        }
        if cleanup {
            events.extend(completed_activity(2, "undo", Value::Null));
        }
        if case == "coordinator-missing-request" {
            events.retain(|event| event.event_type != "CooperativeCancellationRequested");
        }
        let events = events
            .into_iter()
            .map(|event| {
                let mut value = event.raw;
                value.insert("event_type".into(), json!(event.event_type));
                value.insert("payload".into(), event.payload);
                Value::Object(value.into_iter().collect())
            })
            .collect::<Vec<_>>();
        return Some((
            "200 OK",
            json!({
            "task_id":if successor { "successor" } else if child_resume { "parent-resume-task" } else if cleanup { "cleanup" } else { "task/selected" },
            "workflow_task_attempt":if successor || child_resume { 8 } else if cleanup { 9 } else { 7 },
                "total_history_events":events.len(),"history_events":events,
                "next_history_page_token":null,
            })
            .to_string(),
        ));
    }
    if path.ends_with("/deliver-cancellation") {
        if case == "coordinator-child-wait"
            && !path.ends_with("/parent-resume-task/deliver-cancellation")
        {
            return Some((
                "200 OK",
                pending_child_delivery("task/selected").to_string(),
            ));
        }
        if case == "coordinator-lost-ack" {
            return Some((
                "409 Conflict",
                json!({"reason":"lease_owner_mismatch"}).to_string(),
            ));
        }
        let (status, response) = transport_responses(path, body, number)?;
        let mut response: Value = serde_json::from_str(&response).unwrap();
        if case == "coordinator-prefix" {
            response["task_id"] = json!("successor");
        }
        if case == "coordinator-child-wait" {
            response["task_id"] = json!("parent-resume-task");
        }
        return Some((status, response.to_string()));
    }
    if path.ends_with("/complete") || path.ends_with("/fail") {
        return Some(("200 OK", json!({"recorded":true}).to_string()));
    }
    None
}

fn coordinator_server() -> MockWorkerServer {
    MockWorkerServer::start_with_behavior(MockWorkerBehavior {
        request_override: Some(coordinator_responses),
        ..Default::default()
    })
}

fn coordinator_worker(server: &MockWorkerServer, case: &str) -> Worker {
    let mut worker = Worker::new(client(server, case), "queue")
        .worker_id("actual-owner")
        .cooperative_cancellation(true);
    worker.poll_timeout = Duration::ZERO;
    worker.retry_policy = WorkerRetryPolicy {
        max_retries: 1,
        initial_backoff: Duration::from_millis(1),
        max_backoff: Duration::from_millis(1),
    };
    worker
}

fn register_coordinator_timer(
    worker: &mut Worker,
    observed: Arc<Mutex<Vec<CooperativeCancellationRequested>>>,
) {
    worker.register_workflow("cancel", move |ctx, _| {
        let observed = observed.clone();
        async move {
            match ctx.sleep(Duration::from_secs(5)).await {
                Err(Error::CooperativeCancellationRequested(request)) => {
                    observed.lock().unwrap().push(request)
                }
                result => result?,
            }
            Ok(Value::Null)
        }
    });
}

#[tokio::test]
async fn cooperative_coordinator_commits_delivery_proves_history_and_replays_before_completion() {
    for case in [
        "coordinator-valid",
        "coordinator-lost-ack",
        "coordinator-cold",
    ] {
        let server = coordinator_server();
        let mut worker = coordinator_worker(&server, case);
        let observed = Arc::new(Mutex::new(Vec::new()));
        register_coordinator_timer(&mut worker, observed.clone());
        assert_eq!(
            worker.poll_workflow_once().await.unwrap(),
            ManagedPollOutcome::Handled
        );
        let observed = observed.lock().unwrap();
        assert_eq!(observed.len(), 1);
        assert_eq!(observed[0].request, original_observation());
        assert_eq!(observed[0].delivery, transport_delivery());
        let requests = server.requests.lock().unwrap();
        let delivery_count = requests
            .iter()
            .filter(|request| request.path.ends_with("/deliver-cancellation"))
            .count();
        assert_eq!(
            delivery_count,
            if case == "coordinator-cold" { 0 } else { 1 }
        );
        let completion = requests.last().unwrap();
        assert!(completion.path.ends_with("/complete"));
        assert!(requests[requests.len() - 2].path.ends_with("/history"));
        let body: Value = serde_json::from_str(&completion.body).unwrap();
        assert_eq!(body["lease_owner"], "actual-owner");
        assert_eq!(body["workflow_task_attempt"], 7);
        assert_eq!(body["commands"][0]["type"], "complete_workflow");
        assert!(!requests
            .iter()
            .any(|request| request.path.ends_with("/fail")));
    }
}

#[tokio::test]
async fn cooperative_coordinator_returns_to_other_work_then_replays_the_parent_on_a_new_claim() {
    let server = coordinator_server();
    let mut worker = coordinator_worker(&server, "coordinator-child-wait");
    let observed = Arc::new(Mutex::new(Vec::new()));
    let cleanup = Arc::clone(&observed);
    worker.register_workflow("cancel", move |ctx, _| {
        let cleanup = Arc::clone(&cleanup);
        async move {
            match ctx
                .start_child_workflow("child", ChildWorkflowOptions::new("queue"), json!([]))
                .await
            {
                Err(Error::CooperativeCancellationRequested(request)) => {
                    cleanup.lock().unwrap().push(request)
                }
                result => {
                    result?;
                }
            }
            Ok(Value::Null)
        }
    });
    worker.register_workflow("other", |_, _| async { Ok(Value::Null) });
    assert_eq!(
        worker.poll_workflow_once().await.unwrap(),
        ManagedPollOutcome::Handled
    );
    assert!(observed.lock().unwrap().is_empty());
    assert!(!server
        .requests
        .lock()
        .unwrap()
        .iter()
        .any(|request| request.path.ends_with("/complete") || request.path.ends_with("/fail")));

    assert_eq!(
        worker.poll_workflow_once().await.unwrap(),
        ManagedPollOutcome::Handled
    );
    assert!(observed.lock().unwrap().is_empty());
    assert!(server
        .requests
        .lock()
        .unwrap()
        .iter()
        .any(|request| request.path.ends_with("/other-task/complete")));

    assert_eq!(
        worker.poll_workflow_once().await.unwrap(),
        ManagedPollOutcome::Handled
    );
    let observed = observed.lock().unwrap();
    assert_eq!(observed.len(), 1);
    assert_eq!(observed[0].request, original_observation());
    assert_eq!(observed[0].delivery.call_kind, CancellationCallKind::Child);
    assert_eq!(observed[0].delivery.sequence, 1);
    let requests = server.requests.lock().unwrap();
    let deliveries = requests
        .iter()
        .filter(|request| request.path.ends_with("/deliver-cancellation"))
        .map(|request| serde_json::from_str::<Value>(&request.body).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(deliveries.len(), 2);
    assert_eq!(deliveries[0]["workflow_task_attempt"], 7);
    assert_eq!(deliveries[1]["workflow_task_attempt"], 8);
    assert!(deliveries
        .iter()
        .all(|body| body["request_id"] == "original-request"));
    assert!(!requests
        .iter()
        .any(|request| request.path.ends_with("/fail")));
}

#[tokio::test]
async fn cooperative_coordinator_refuses_unproved_changed_or_lost_claim_history_without_publication(
) {
    for case in [
        "coordinator-unproved",
        "coordinator-mismatch",
        "coordinator-owner-lost",
        "coordinator-missing-request",
    ] {
        let server = coordinator_server();
        let mut worker = coordinator_worker(&server, case);
        let observed = Arc::new(Mutex::new(Vec::new()));
        register_coordinator_timer(&mut worker, observed.clone());
        let result = worker.poll_workflow_once().await;
        if case == "coordinator-owner-lost" {
            assert!(matches!(result, Err(Error::Http { status, .. }) if status.as_u16() == 409));
        } else {
            assert!(
                matches!(result, Err(Error::InvalidCooperativeCancellation(_))),
                "{case}: {result:?}"
            );
        }
        assert!(observed.lock().unwrap().is_empty());
        let requests = server.requests.lock().unwrap();
        assert!(!requests
            .iter()
            .any(|request| request.path.ends_with("/complete") || request.path.ends_with("/fail")));
    }
}

#[tokio::test]
async fn cooperative_coordinator_commits_earlier_prefix_and_delivers_only_on_its_successor_claim() {
    let server = coordinator_server();
    let mut worker = coordinator_worker(&server, "coordinator-prefix");
    let side_effects = Arc::new(Mutex::new(0));
    let executions = side_effects.clone();
    let observed = Arc::new(Mutex::new(Vec::new()));
    let errors = observed.clone();
    worker.register_workflow("cancel", move |ctx, _| {
        let executions = executions.clone();
        let errors = errors.clone();
        async move {
            let value: Value = ctx.side_effect(|| {
                *executions.lock().unwrap() += 1;
                json!(7)
            })?;
            assert_eq!(value, 7);
            match ctx.sleep(Duration::from_secs(5)).await {
                Err(Error::CooperativeCancellationRequested(request)) => {
                    errors.lock().unwrap().push(request)
                }
                result => result?,
            }
            Ok(Value::Null)
        }
    });
    assert_eq!(
        worker.poll_workflow_once().await.unwrap(),
        ManagedPollOutcome::Handled
    );
    assert!(observed.lock().unwrap().is_empty());
    {
        let requests = server.requests.lock().unwrap();
        assert_eq!(requests.len(), 3);
        let completion: Value = serde_json::from_str(&requests[2].body).unwrap();
        assert_eq!(completion["commands"].as_array().unwrap().len(), 1);
        assert_eq!(completion["commands"][0]["type"], "record_side_effect");
        assert_eq!(completion["workflow_task_attempt"], 7);
        assert!(!requests
            .iter()
            .any(|request| request.path.ends_with("/deliver-cancellation")));
    }
    let result = worker.poll_workflow_once().await;
    assert!(
        matches!(result, Ok(ManagedPollOutcome::Handled)),
        "{result:?}, paths: {:?}",
        server
            .requests
            .lock()
            .unwrap()
            .iter()
            .map(|request| &request.path)
            .collect::<Vec<_>>()
    );
    assert_eq!(*side_effects.lock().unwrap(), 1);
    let observed = observed.lock().unwrap();
    assert_eq!(observed.len(), 1);
    assert_eq!(observed[0].delivery.sequence, 2);
    assert_eq!(observed[0].request, original_observation());
    let requests = server.requests.lock().unwrap();
    let delivery = requests
        .iter()
        .find(|request| request.path.ends_with("/deliver-cancellation"))
        .unwrap();
    assert!(delivery.path.ends_with("/successor/deliver-cancellation"));
    let body: Value = serde_json::from_str(&delivery.body).unwrap();
    assert_eq!(body["workflow_task_attempt"], 8);
    assert_eq!(body["sequence"], 2);
    let body: Value = serde_json::from_str(&requests.last().unwrap().body).unwrap();
    assert_eq!(body["commands"].as_array().unwrap().len(), 1);
    assert_eq!(body["commands"][0]["type"], "complete_workflow");
}

#[tokio::test]
async fn cooperative_coordinator_leaves_shielded_cleanup_pending_without_delivery() {
    let server = coordinator_server();
    let mut worker = coordinator_worker(&server, "coordinator-shield");
    worker.register_workflow("cancel", |ctx, _| async move {
        let _shield = ctx.cancellation_shield()?;
        ctx.sleep(Duration::from_secs(5)).await?;
        Ok(Value::Null)
    });
    assert_eq!(
        worker.poll_workflow_once().await.unwrap(),
        ManagedPollOutcome::Handled
    );
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 3);
    assert!(requests.last().unwrap().path.ends_with("/fail"));
    let body: Value = serde_json::from_str(&requests.last().unwrap().body).unwrap();
    assert_eq!(
        body["failure"]["type"],
        WORKFLOW_TASK_WAITING_FOR_HISTORY_TYPE
    );
    assert!(!requests
        .iter()
        .any(|request| request.path.ends_with("/deliver-cancellation")));
}

#[tokio::test]
async fn cooperative_coordinator_unhandled_canonical_request_publishes_typed_workflow_cancellation()
{
    let server = coordinator_server();
    let mut worker = coordinator_worker(&server, "coordinator-valid");
    worker.register_workflow("cancel", |ctx, _| async move {
        ctx.sleep(Duration::from_secs(5)).await?;
        Ok(Value::Null)
    });
    assert_eq!(
        worker.poll_workflow_once().await.unwrap(),
        ManagedPollOutcome::Handled
    );
    let requests = server.requests.lock().unwrap();
    let completion = requests.last().unwrap();
    assert!(completion.path.ends_with("/complete"));
    let body: Value = serde_json::from_str(&completion.body).unwrap();
    assert_eq!(body["commands"].as_array().unwrap().len(), 1);
    let command = &body["commands"][0];
    assert_eq!(command["type"], "fail_workflow");
    assert_eq!(command["exception_type"], "WorkflowCancellationRequested");
    assert_eq!(
        command["exception_class"],
        "durable_workflow::CooperativeCancellationRequested"
    );
    assert_eq!(command["non_retryable"], true);
    assert_eq!(command["exception"]["properties"]["reason"], "cancelled");
    assert_eq!(
        command["exception"]["properties"]["request_id"],
        "original-request"
    );
    assert_eq!(
        command["exception"]["properties"]["cleanup_deadline_at"],
        "2026-10-01T08:10:00Z"
    );
    assert!(!requests
        .iter()
        .any(|request| request.path.ends_with("/fail")));
}

#[tokio::test]
async fn cooperative_coordinator_replays_saga_cleanup_on_a_cold_successor_before_terminal_cancellation(
) {
    let server = coordinator_server();
    let mut worker = coordinator_worker(&server, "coordinator-saga");
    worker.register_workflow("cancel", |ctx, _| async move {
        let mut saga = ctx.saga();
        saga.add_compensation("undo", json!([]))?;
        let result = ctx.sleep(Duration::from_secs(5)).await;
        saga.finish(result).await?;
        Ok(Value::Null)
    });
    assert_eq!(
        worker.poll_workflow_once().await.unwrap(),
        ManagedPollOutcome::Handled
    );
    {
        let requests = server.requests.lock().unwrap();
        let body: Value = serde_json::from_str(&requests.last().unwrap().body).unwrap();
        assert_eq!(body["commands"].as_array().unwrap().len(), 1);
        assert_eq!(body["commands"][0]["type"], "schedule_activity");
        assert_eq!(body["commands"][0]["activity_type"], "undo");
        assert_eq!(body["workflow_task_attempt"], 7);
    }
    // A fresh Worker represents process loss after cleanup was scheduled. The
    // Server history proves its activity completion before terminal publication.
    let mut replacement = coordinator_worker(&server, "coordinator-saga");
    replacement.register_workflow("cancel", |ctx, _| async move {
        let mut saga = ctx.saga();
        saga.add_compensation("undo", json!([]))?;
        let result = ctx.sleep(Duration::from_secs(5)).await;
        saga.finish(result).await?;
        Ok(Value::Null)
    });
    assert_eq!(
        replacement.poll_workflow_once().await.unwrap(),
        ManagedPollOutcome::Handled
    );
    let requests = server.requests.lock().unwrap();
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.path.ends_with("/deliver-cancellation"))
            .count(),
        1
    );
    let completion = requests.last().unwrap();
    assert!(completion.path.ends_with("/cleanup/complete"));
    let body: Value = serde_json::from_str(&completion.body).unwrap();
    assert_eq!(body["workflow_task_attempt"], 9);
    assert_eq!(body["commands"].as_array().unwrap().len(), 1);
    let command = &body["commands"][0];
    assert_eq!(command["type"], "fail_workflow");
    assert_eq!(command["exception_type"], "WorkflowCancellationRequested");
    assert_eq!(
        command["exception_class"],
        "durable_workflow::CooperativeCancellationRequested"
    );
    assert_eq!(command["non_retryable"], true);
    assert_eq!(command["exception"]["properties"]["reason"], "cancelled");
    assert_eq!(
        command["exception"]["properties"]["request_id"],
        "original-request"
    );
    assert_eq!(
        command["exception"]["properties"]["cleanup_deadline_at"],
        "2026-10-01T08:10:00Z"
    );
}

#[tokio::test]
async fn cooperative_coordinator_poll_retry_retains_the_same_claim_acquisition_identity() {
    let server = coordinator_server();
    let mut worker = coordinator_worker(&server, "coordinator-retry");
    register_coordinator_timer(&mut worker, Arc::new(Mutex::new(Vec::new())));
    assert_eq!(
        worker.poll_workflow_once().await.unwrap(),
        ManagedPollOutcome::Handled
    );
    let requests = server.requests.lock().unwrap();
    let polls = requests
        .iter()
        .filter(|request| request.path.ends_with("/poll"))
        .collect::<Vec<_>>();
    assert_eq!(polls.len(), 2);
    assert_eq!(polls[0].body, polls[1].body);
    assert_eq!(polls[0].worker_protocol.as_deref(), Some("1.20"));
}

#[tokio::test]
async fn cooperative_poll_retains_the_actual_claim_observation_and_fenced_history() {
    let server = transport_server();
    let response = client(&server, "claim-history")
        .poll_cooperative_workflow_task("actual-owner", "queue", Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(response.outcome, WorkerPollOutcome::Task);
    assert_eq!(response.protocol_version.as_deref(), Some("1.20"));
    assert_eq!(
        response.server_capabilities.unwrap()["workflow_memo_updates"]["supported"],
        true
    );
    let claim = response.task.unwrap();
    assert_eq!(claim.task().task_id, "task/selected");
    assert_eq!(claim.task().lease_owner.as_deref(), Some("actual-owner"));
    assert_eq!(claim.task().workflow_task_attempt, 7);
    assert_eq!(claim.cancellation_request(), Some(&original_observation()));
    assert_eq!(claim.task().history_events.len(), 3);
    assert_eq!(claim.task().next_history_page_token, None);
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 3);
    for request in requests.iter() {
        assert_eq!(request.worker_protocol.as_deref(), Some("1.20"));
        assert_eq!(request.authorization.as_deref(), Some("Bearer worker-only"));
        assert_eq!(request.namespace.as_deref(), Some("caller-namespace"));
        let body: Value = serde_json::from_str(&request.body).unwrap();
        if request.path.ends_with("/poll") {
            assert_eq!(body["worker_id"], "actual-owner");
            assert_eq!(body["task_queue"], "queue");
            assert_eq!(body["history_page_size"], WORKFLOW_HISTORY_PAGE_SIZE);
            assert!(!body["poll_request_id"].as_str().unwrap().is_empty());
        } else {
            assert!(request.path.ends_with("/task%2Fselected/history"));
            assert_eq!(body["lease_owner"], "actual-owner");
            assert_eq!(body["workflow_task_attempt"], 7);
        }
    }
}

#[tokio::test]
async fn cooperative_poll_rejects_invented_claims_and_malformed_observations() {
    for case in [
        "claim-missing-attempt",
        "claim-zero-attempt",
        "claim-owner",
        "claim-missing-owner",
        "claim-missing-run",
        "claim-missing-id",
        "claim-bad-observation",
        "claim-bad-token",
    ] {
        let server = transport_server();
        let result = client(&server, case)
            .poll_cooperative_workflow_task("actual-owner", "queue", Duration::ZERO)
            .await;
        assert!(
            matches!(result, Err(Error::InvalidCooperativeCancellation(_))),
            "{case}: {result:?}"
        );
        assert_eq!(server.requests.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn cooperative_poll_transport_retry_preserves_the_same_acquisition_identity() {
    let server = transport_server();
    let response = client(&server, "claim-retry")
        .poll_cooperative_workflow_task("actual-owner", "queue", Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(response.task.unwrap().task().workflow_task_attempt, 7);
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].body, requests[1].body);
    let body: Value = serde_json::from_str(&requests[0].body).unwrap();
    assert!(!body["poll_request_id"].as_str().unwrap().is_empty());
}

#[tokio::test]
async fn cooperative_claim_history_rejects_changed_claims_bad_progress_and_unbounded_pages() {
    for case in [
        "cycle",
        "page-bound",
        "empty-progress",
        "oversized-page",
        "wrong-task",
        "wrong-attempt",
        "malformed-event",
        "empty-token",
        "missing-token",
    ] {
        let server = transport_server();
        let result = client(&server, case)
            .poll_cooperative_workflow_task("actual-owner", "queue", Duration::ZERO)
            .await;
        assert!(
            matches!(result, Err(Error::InvalidCooperativeCancellation(_))),
            "{case}: {result:?}"
        );
        assert!(server.requests.lock().unwrap().len() <= 129);
    }
}

#[tokio::test]
async fn cooperative_claim_heartbeat_preserves_the_first_observation_and_refuses_replacement() {
    let server = transport_server();
    let mut claim = client(&server, "valid")
        .poll_cooperative_workflow_task("actual-owner", "queue", Duration::ZERO)
        .await
        .unwrap()
        .task
        .unwrap();
    assert!(claim.task().history_events.is_empty());
    claim
        .heartbeat(&client(&server, "heartbeat-equivalent-time"))
        .await
        .unwrap();
    assert_eq!(claim.cancellation_request(), Some(&original_observation()));
    assert!(matches!(
        claim.heartbeat(&client(&server, "heartbeat-owner")).await,
        Err(Error::InvalidCooperativeCancellation(_))
    ));
    assert_eq!(claim.cancellation_request(), Some(&original_observation()));
    assert_eq!(claim.task().lease_owner.as_deref(), Some("actual-owner"));
    assert_eq!(claim.task().workflow_task_attempt, 7);
    assert!(claim.task().history_events.is_empty());
}

#[tokio::test]
async fn cooperative_poll_preserves_idle_stop_and_no_observation_despite_task_flags() {
    let server = transport_server();
    for case in ["claim-idle", "claim-stop"] {
        let response = client(&server, case)
            .poll_cooperative_workflow_task("actual-owner", "queue", Duration::ZERO)
            .await
            .unwrap();
        assert!(response.task.is_none());
        assert_eq!(response.outcome.should_stop(), case == "claim-stop");
    }
    let mut claim = client(&server, "claim-no-observation")
        .poll_cooperative_workflow_task("actual-owner", "queue", Duration::ZERO)
        .await
        .unwrap()
        .task
        .unwrap();
    assert!(claim.task().cancel_requested);
    assert!(claim.cancellation_request().is_none());
    claim.heartbeat(&client(&server, "valid")).await.unwrap();
    assert_eq!(claim.cancellation_request(), Some(&original_observation()));
}

#[tokio::test]
async fn cooperative_poll_preflights_inputs_and_shares_the_poll_and_history_budget() {
    let server = transport_server();
    let client = client(&server, "claim-budget");
    for (owner, queue, timeout) in [
        (" ", "queue", Duration::ZERO),
        ("actual-owner", " ", Duration::ZERO),
        ("actual-owner", "queue", Duration::MAX),
    ] {
        assert!(matches!(
            client
                .poll_cooperative_workflow_task(owner, queue, timeout)
                .await,
            Err(Error::InvalidCooperativeCancellation(_))
        ));
    }
    assert!(server.requests.lock().unwrap().is_empty());
    let control_only = Client::builder(server.base_url())
        .control_token(Some("control-only".into()))
        .build()
        .unwrap();
    assert!(matches!(
        control_only
            .poll_cooperative_workflow_task("actual-owner", "queue", Duration::ZERO)
            .await,
        Err(Error::MissingRoleCredentials { role: "worker", .. })
    ));
    assert!(server.requests.lock().unwrap().is_empty());
    let started = Instant::now();
    assert!(matches!(
        client
            .poll_cooperative_workflow_task("actual-owner", "queue", Duration::ZERO)
            .await,
        Err(Error::Timeout)
    ));
    assert!(started.elapsed() < Duration::from_secs(6));
    assert_eq!(server.requests.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn ordinary_workflow_heartbeat_uses_119_with_the_same_claim_fences() {
    let server = transport_server();
    let task = transport_task();
    let receipt = client(&server, "heartbeat-equivalent-time")
        .heartbeat_workflow_task_with_protocol(&task, None, WORKER_PROTOCOL_VERSION)
        .await
        .unwrap();
    assert_eq!(receipt.task_id, task.task_id);
    assert_eq!(receipt.workflow_task_attempt, task.workflow_task_attempt);
    assert_eq!(receipt.lease_owner, "actual-owner");
    let requests = server.requests.lock().unwrap();
    let request = &requests[0];
    assert_eq!(request.worker_protocol.as_deref(), Some("1.19"));
}

#[tokio::test]
async fn cooperative_heartbeat_renews_the_exact_worker_claim_and_retains_original_observation() {
    let server = transport_server();
    let original = original_observation();
    let receipt = client(&server, "heartbeat-equivalent-time")
        .heartbeat_workflow_task(&transport_task(), Some(&original))
        .await
        .unwrap();
    assert_eq!(receipt.task_id, "task/selected");
    assert_eq!(receipt.workflow_task_attempt, 7);
    assert_eq!(receipt.lease_owner, "actual-owner");
    assert_eq!(receipt.cancellation_request, Some(original));
    let requests = server.requests.lock().unwrap();
    let request = &requests[0];
    assert_eq!(
        request.path,
        "/heartbeat-equivalent-time/api/worker/workflow-tasks/task%2Fselected/heartbeat"
    );
    assert_eq!(request.worker_protocol.as_deref(), Some("1.20"));
    assert_eq!(request.authorization.as_deref(), Some("Bearer worker-only"));
    assert_eq!(request.namespace.as_deref(), Some("caller-namespace"));
    let body: Value = serde_json::from_str(&request.body).unwrap();
    assert_eq!(
        body,
        json!({"lease_owner":"actual-owner", "workflow_task_attempt":7})
    );
}

#[tokio::test]
async fn cooperative_heartbeat_rejects_changed_claims_malformed_renewal_and_changed_identity() {
    for case in [
        "wrong-task",
        "heartbeat-attempt",
        "heartbeat-owner",
        "heartbeat-not-renewed",
        "heartbeat-string-renewed",
        "heartbeat-closed",
        "heartbeat-unknown-run",
        "heartbeat-finished-task",
        "heartbeat-expiry",
        "heartbeat-expiry-no-zone",
        "heartbeat-malformed-request",
        "heartbeat-missing-token",
        "heartbeat-changed-request",
        "heartbeat-changed-request-time",
        "heartbeat-changed-deadline",
        "heartbeat-no-request",
        "wrong-reason",
    ] {
        let server = transport_server();
        let result = client(&server, case)
            .heartbeat_workflow_task(&transport_task(), Some(&original_observation()))
            .await;
        assert!(
            matches!(result, Err(Error::InvalidCooperativeCancellation(_))),
            "{case}: {result:?}"
        );
    }
}

#[tokio::test]
async fn cooperative_heartbeat_accepts_new_observation_and_does_not_invent_a_request() {
    for case in ["valid", "heartbeat-no-request"] {
        let server = transport_server();
        let receipt = client(&server, case)
            .heartbeat_workflow_task(&transport_task(), None)
            .await
            .unwrap();
        assert_eq!(
            receipt.cancellation_request,
            if case == "valid" {
                Some(original_observation())
            } else {
                None
            }
        );
        assert_eq!(receipt.lease_expires_at, "2026-10-01T08:00:30Z");
    }
}

#[tokio::test]
async fn cooperative_heartbeat_validates_the_claim_and_original_observation_before_network_io() {
    let server = transport_server();
    let client = client(&server, "valid");
    let mut task = transport_task();
    task.lease_owner = None;
    assert!(matches!(
        client.heartbeat_workflow_task(&task, None).await,
        Err(Error::InvalidCooperativeCancellation(_))
    ));
    let mut original = original_observation();
    original.history_refresh_page_token = None;
    assert!(matches!(
        client
            .heartbeat_workflow_task(&transport_task(), Some(&original))
            .await,
        Err(Error::InvalidCooperativeCancellation(_))
    ));
    assert!(server.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn cooperative_heartbeat_preserves_ownership_refusal_and_the_total_budget() {
    let server = transport_server();
    assert!(
        matches!(client(&server, "refused").heartbeat_workflow_task(&transport_task(), None).await,
        Err(Error::Http { status, .. }) if status.as_u16() == 409)
    );
    let started = Instant::now();
    assert!(matches!(
        client(&server, "heartbeat-budget")
            .heartbeat_workflow_task(&transport_task(), None)
            .await,
        Err(Error::Timeout)
    ));
    assert!(started.elapsed() < Duration::from_secs(6));
}

#[tokio::test]
async fn cooperative_transport_delivers_the_exact_claim_with_worker_credentials() {
    let server = transport_server();
    let client = client(&server, "valid");
    let task = transport_task();
    let delivery = transport_delivery();
    let reply = client
        .deliver_workflow_cancellation(&task, &delivery)
        .await
        .unwrap();
    let CancellationDeliveryReply::Delivered(receipt) = reply else {
        panic!("delivery must be committed")
    };
    assert_eq!(receipt.task_id, task.task_id);
    assert_eq!(receipt.run_id, "run");
    assert_eq!(receipt.delivery, delivery);
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    let request = &requests[0];
    assert_eq!(
        request.path,
        "/valid/api/worker/workflow-tasks/task%2Fselected/deliver-cancellation"
    );
    assert_eq!(request.worker_protocol.as_deref(), Some("1.20"));
    assert_eq!(request.authorization.as_deref(), Some("Bearer worker-only"));
    assert_eq!(request.namespace.as_deref(), Some("caller-namespace"));
    let body: Value = serde_json::from_str(&request.body).unwrap();
    assert_eq!(body["lease_owner"], "actual-owner");
    assert_eq!(body["workflow_task_attempt"], 7);
    assert_eq!(body["request_id"], "original-request");
}

#[tokio::test]
async fn cooperative_transport_accepts_only_explicit_cancellation_claim_release() {
    for (case, kinds) in [
        (
            "child-wait",
            vec![
                CancellationCallKind::Child,
                CancellationCallKind::Parallel,
                CancellationCallKind::SelectionHandle,
            ],
        ),
        (
            "activity-wait",
            vec![
                CancellationCallKind::Activity,
                CancellationCallKind::LocalActivity,
                CancellationCallKind::Parallel,
                CancellationCallKind::SelectionHandle,
            ],
        ),
    ] {
        for kind in kinds {
            let server = transport_server();
            let mut delivery = transport_delivery();
            delivery.call_kind = kind;
            if kind == CancellationCallKind::SelectionHandle {
                delivery.sequence = 2;
                delivery.operation_sequence = Some(1);
            }
            assert_eq!(
                client(&server, case)
                    .deliver_workflow_cancellation(&transport_task(), &delivery)
                    .await
                    .unwrap(),
                CancellationDeliveryReply::ClaimReleased {
                    task_id: "task/selected".into(),
                    run_id: "run".into()
                }
            );
        }
    }
    for (case, kind) in [
        ("child-wait", CancellationCallKind::Timer),
        ("activity-wait", CancellationCallKind::Timer),
        ("child-wait", CancellationCallKind::Activity),
        ("activity-wait", CancellationCallKind::Child),
    ] {
        let server = transport_server();
        let mut delivery = transport_delivery();
        delivery.call_kind = kind;
        assert!(matches!(
            client(&server, case)
                .deliver_workflow_cancellation(&transport_task(), &delivery)
                .await,
            Err(Error::InvalidCooperativeCancellation(_))
        ));
    }
    for (prefix, kind) in [
        ("child-wait", CancellationCallKind::Child),
        ("activity-wait", CancellationCallKind::Activity),
    ] {
        for suffix in [
            "wrong-task",
            "wrong-run",
            "not-released",
            "string-released",
            "missing-release",
            "wrong-reason",
            "request",
            "sequence",
            "kind",
            "span",
            "operation",
            "operation-span",
            "string-delivered",
        ] {
            let case = format!("{prefix}-{suffix}");
            let server = transport_server();
            let mut delivery = transport_delivery();
            delivery.call_kind = kind;
            let result = client(&server, &case)
                .deliver_workflow_cancellation(&transport_task(), &delivery)
                .await;
            assert!(
                matches!(result, Err(Error::InvalidCooperativeCancellation(_))),
                "{case}: {result:?}"
            );
        }
    }
}

#[tokio::test]
async fn cooperative_transport_rejects_malformed_or_changed_delivery_acknowledgments() {
    for case in [
        "wrong-task",
        "wrong-run",
        "wrong-request",
        "wrong-sequence",
        "wrong-kind",
        "wrong-span",
        "false-delivered",
        "string-delivered",
        "wrong-reason",
        "missing-span",
        "missing-operation",
        "missing-operation-span",
    ] {
        let server = transport_server();
        let result = client(&server, case)
            .deliver_workflow_cancellation(&transport_task(), &transport_delivery())
            .await;
        assert!(
            matches!(result, Err(Error::InvalidCooperativeCancellation(_))),
            "{case}: {result:?}"
        );
    }
}

#[tokio::test]
async fn cooperative_transport_refreshes_with_the_opaque_token_and_preserves_the_snapshot() {
    let server = transport_server();
    let client = client(&server, "valid");
    let task = transport_task();
    let observation = CancellationRequest::from_observation(&request_observation()).unwrap();
    let original = observation.clone();
    let fresh = client
        .refresh_workflow_cancellation_history(&task, &observation)
        .await
        .unwrap();
    assert_eq!(fresh.len(), 3);
    assert_eq!(task.history_events.len(), 1);
    assert_eq!(observation, original);
    let canonical = CancellationHistory::from_events(&fresh, "run", Some(&observation)).unwrap();
    assert_eq!(canonical.delivery, Some(transport_delivery()));
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    for (index, request) in requests.iter().enumerate() {
        assert_eq!(request.worker_protocol.as_deref(), Some("1.20"));
        let body: Value = serde_json::from_str(&request.body).unwrap();
        assert_eq!(body["lease_owner"], "actual-owner");
        assert_eq!(body["workflow_task_attempt"], 7);
        assert_eq!(body["history_page_size"], WORKFLOW_HISTORY_PAGE_SIZE);
        assert_eq!(
            body["next_history_page_token"],
            if index == 0 {
                "opaque-server-token"
            } else {
                "page-next"
            }
        );
    }
}

#[tokio::test]
async fn cooperative_transport_rejects_changed_claims_bad_pages_and_token_cycles() {
    for case in [
        "wrong-task",
        "wrong-attempt",
        "cycle",
        "empty-progress",
        "oversized-page",
        "missing-request",
        "changed-canonical",
        "malformed-event",
        "empty-token",
        "missing-token",
    ] {
        let server = transport_server();
        let observation = CancellationRequest::from_observation(&request_observation()).unwrap();
        let result = client(&server, case)
            .refresh_workflow_cancellation_history(&transport_task(), &observation)
            .await;
        assert!(
            matches!(
                result,
                Err(Error::InvalidCooperativeCancellation(_) | Error::NonDeterministicReplay(_))
            ),
            "{case}: {result:?}"
        );
        assert!(server.requests.lock().unwrap().len() <= 2);
    }
}

#[tokio::test]
async fn cooperative_transport_bounds_unique_history_pages() {
    let server = transport_server();
    let observation = CancellationRequest::from_observation(&request_observation()).unwrap();
    let result = client(&server, "page-bound")
        .refresh_workflow_cancellation_history(&transport_task(), &observation)
        .await;
    assert!(matches!(
        result,
        Err(Error::InvalidCooperativeCancellation(_))
    ));
    assert_eq!(server.requests.lock().unwrap().len(), 128);
}

#[tokio::test]
async fn cooperative_transport_can_prove_committed_history_after_a_lost_acknowledgment() {
    let server = transport_server();
    let client = client(&server, "lost-ack");
    let task = transport_task();
    let intent = transport_delivery();
    assert!(matches!(
        client.deliver_workflow_cancellation(&task, &intent).await,
        Err(Error::Http { status, .. }) if status.as_u16() == 409
    ));
    let observation = CancellationRequest::from_observation(&request_observation()).unwrap();
    let fresh = client
        .refresh_workflow_cancellation_history(&task, &observation)
        .await
        .unwrap();
    assert_eq!(
        CancellationHistory::from_events(&fresh, "run", Some(&observation))
            .unwrap()
            .delivery,
        Some(intent)
    );
}

#[tokio::test]
async fn cooperative_transport_validates_inputs_and_never_substitutes_control_credentials() {
    let server = transport_server();
    let client = client(&server, "valid");
    for changed in 0..6 {
        let mut task = transport_task();
        match changed {
            0 => task.task_id.clear(),
            1 => task.workflow_task_attempt = 0,
            2 => task.workflow_task_attempt = u64::MAX,
            3 => task.run_id = None,
            4 => task.lease_owner = None,
            5 => task.lease_owner = Some(" ".into()),
            _ => unreachable!(),
        }
        assert!(matches!(
            client
                .deliver_workflow_cancellation(&task, &transport_delivery())
                .await,
            Err(Error::InvalidCooperativeCancellation(_))
        ));
    }
    let mut invalid = transport_delivery();
    invalid.sequence_span = 2;
    assert!(matches!(
        client
            .deliver_workflow_cancellation(&transport_task(), &invalid)
            .await,
        Err(Error::InvalidCooperativeCancellation(_))
    ));
    let mut observation = CancellationRequest::from_observation(&request_observation()).unwrap();
    observation.history_refresh_page_token = None;
    assert!(matches!(
        client
            .refresh_workflow_cancellation_history(&transport_task(), &observation)
            .await,
        Err(Error::InvalidCooperativeCancellation(_))
    ));
    let control_only = Client::builder(server.base_url())
        .control_token(Some("control-only".into()))
        .build()
        .unwrap();
    assert!(matches!(
        control_only
            .deliver_workflow_cancellation(&transport_task(), &transport_delivery())
            .await,
        Err(Error::MissingRoleCredentials { role: "worker", .. })
    ));
    assert!(server.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn cooperative_transport_preserves_claim_refusals_and_bounds_the_total_exchange() {
    let server = transport_server();
    assert!(matches!(
        client(&server, "refused")
            .deliver_workflow_cancellation(&transport_task(), &transport_delivery())
            .await,
        Err(Error::Http { status, .. }) if status.as_u16() == 409
    ));
    let started = Instant::now();
    let observation = CancellationRequest::from_observation(&request_observation()).unwrap();
    let result = client(&server, "budget")
        .refresh_workflow_cancellation_history(&transport_task(), &observation)
        .await;
    assert!(matches!(result, Err(Error::Timeout)));
    assert!(started.elapsed() < Duration::from_secs(6));
    assert_eq!(
        server
            .requests
            .lock()
            .unwrap()
            .iter()
            .filter(|request| request.path.ends_with("/history"))
            .count(),
        2
    );
}

#[tokio::test]
async fn cooperative_request_uses_control_role_namespace_and_preserves_server_identity() {
    let server = server();
    let client = client(&server, "valid");
    let options = CooperativeCancellationOptions {
        reason: Some("caller stopped".into()),
        cleanup_timeout_seconds: Some(60),
    };
    let first = client
        .request_workflow_cancellation("workflow", options)
        .await
        .unwrap();
    let repeated = client
        .request_workflow_cancellation(
            "workflow",
            CooperativeCancellationOptions {
                reason: Some("changed reason".into()),
                cleanup_timeout_seconds: Some(120),
            },
        )
        .await
        .unwrap();
    assert!(!first.duplicate);
    assert!(repeated.duplicate);
    assert_eq!(first.cancellation_request, repeated.cancellation_request);
    assert_eq!(first.cancellation_request.request_id, "original-request");
    assert_eq!(
        first.cancellation_request.cleanup_deadline_at,
        "2026-10-01T08:10:00Z"
    );
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 4);
    for request in requests.iter() {
        assert_eq!(
            request.authorization.as_deref(),
            Some("Bearer control-only")
        );
        assert_eq!(request.namespace.as_deref(), Some("caller-namespace"));
        assert_eq!(request.control_protocol.as_deref(), Some("2"));
        assert_eq!(request.worker_protocol, None);
    }
    assert_eq!(requests[0].method, "GET");
    assert_eq!(requests[1].method, "POST");
    assert_eq!(
        serde_json::from_str::<Value>(&requests[1].body).unwrap(),
        json!({
            "reason":"caller stopped", "cleanup_timeout_seconds":60
        })
    );
}

#[tokio::test]
async fn cooperative_request_refuses_missing_unsupported_or_incompatible_discovery_before_mutation()
{
    for case in [
        "unsupported",
        "missing",
        "string-flag",
        "old",
        "future-major",
        "malformed-version",
    ] {
        let server = server();
        let result = client(&server, case)
            .request_workflow_cancellation("workflow", Default::default())
            .await;
        assert!(
            matches!(result, Err(Error::CooperativeCancellationUnavailable(_))),
            "{case}: {result:?}"
        );
        assert_eq!(server.requests.lock().unwrap().len(), 1, "{case}");
    }
}

#[tokio::test]
async fn cooperative_request_accepts_additive_minor_discovery_without_changing_worker_default() {
    let server = server();
    client(&server, "new-minor")
        .request_workflow_cancellation("workflow", Default::default())
        .await
        .unwrap();
    assert_eq!(WORKER_PROTOCOL_VERSION, "1.19");
    let ordinary = Client::builder(server.base_url()).build().unwrap();
    let mut worker = Worker::new(ordinary, "queue");
    worker.register_workflow("example", |_ctx, _input| async { Ok(Value::Null) });
    worker.register().await.unwrap();
    let requests = server.requests.lock().unwrap();
    let registration = requests
        .iter()
        .find(|request| request.path.ends_with("/worker/register"))
        .unwrap();
    assert_eq!(registration.worker_protocol.as_deref(), Some("1.19"));
    let body: Value = serde_json::from_str(&registration.body).unwrap();
    assert!(!body["capabilities"]
        .as_array()
        .unwrap()
        .contains(&json!("cooperative_cancellation")));
}

#[tokio::test]
async fn cooperative_request_validates_selected_run_and_encodes_path_segments() {
    let server = server();
    let result = client(&server, "escaped")
        .request_workflow_run_cancellation("workflow/with?path", "run/selected", Default::default())
        .await
        .unwrap();
    assert_eq!(result.run_id, "run/selected");
    assert_eq!(server.request_count("/escaped/api/workflows/workflow%2Fwith%3Fpath/runs/run%2Fselected/request-cancellation"), 1);
}

#[tokio::test]
async fn cooperative_request_rejects_malformed_or_mismatched_acknowledgments() {
    for case in [
        "wrong-workflow",
        "wrong-run",
        "empty-run",
        "not-accepted",
        "accepted-string",
        "duplicate-integer",
        "missing-request",
        "blank-request",
        "blank-token",
        "no-timezone",
        "invalid-date",
        "equal-deadline",
        "earlier-deadline",
    ] {
        let server = server();
        let result = client(&server, case)
            .request_workflow_run_cancellation("workflow", "run", Default::default())
            .await;
        assert!(
            matches!(result, Err(Error::InvalidCooperativeCancellation(_))),
            "{case}: {result:?}"
        );
    }
}

#[tokio::test]
async fn cooperative_request_rejects_invalid_inputs_without_network_activity() {
    let server = server();
    let client = client(&server, "valid");
    for options in [
        CooperativeCancellationOptions {
            cleanup_timeout_seconds: Some(0),
            ..Default::default()
        },
        CooperativeCancellationOptions {
            cleanup_timeout_seconds: Some(3601),
            ..Default::default()
        },
        CooperativeCancellationOptions {
            reason: Some("x".repeat(1001)),
            ..Default::default()
        },
    ] {
        assert!(matches!(
            client
                .request_workflow_cancellation("workflow", options)
                .await,
            Err(Error::InvalidCooperativeCancellation(_))
        ));
    }
    assert!(matches!(
        client
            .request_workflow_cancellation(" ", Default::default())
            .await,
        Err(Error::InvalidCooperativeCancellation(_))
    ));
    assert!(matches!(
        client
            .request_workflow_run_cancellation("workflow", " ", Default::default())
            .await,
        Err(Error::InvalidCooperativeCancellation(_))
    ));
    assert!(server.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn cooperative_request_does_not_substitute_worker_credentials() {
    let server = server();
    let client = Client::builder(server.base_url())
        .worker_token(Some("worker-only".into()))
        .build()
        .unwrap();
    assert!(matches!(
        client
            .request_workflow_cancellation("workflow", Default::default())
            .await,
        Err(Error::MissingRoleCredentials {
            role: "control",
            ..
        })
    ));
    assert!(server.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn cooperative_request_preserves_discovery_and_active_claim_refusals() {
    for (case, expected) in [
        ("discovery-http", reqwest::StatusCode::FORBIDDEN),
        ("refused", reqwest::StatusCode::CONFLICT),
    ] {
        let server = server();
        let error = client(&server, case)
            .request_workflow_cancellation("workflow", Default::default())
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Http {status, ..} if status==expected));
        assert_eq!(
            server.requests.lock().unwrap().len(),
            if case == "discovery-http" { 1 } else { 2 }
        );
    }
}

#[tokio::test]
async fn cooperative_request_handles_keep_current_and_selected_run_routing_separate() {
    let server = server();
    let handle = WorkflowHandle {
        client: client(&server, "valid"),
        workflow_id: "workflow".into(),
        run_id: Some("run".into()),
        workflow_type: "example".into(),
    };
    handle
        .request_cancellation(Default::default())
        .await
        .unwrap();
    handle
        .request_selected_run_cancellation(Default::default())
        .await
        .unwrap();
    assert_eq!(
        server.request_count("/valid/api/workflows/workflow/request-cancellation"),
        1
    );
    assert_eq!(
        server.request_count("/valid/api/workflows/workflow/runs/run/request-cancellation"),
        1
    );
    let missing = WorkflowHandle {
        run_id: None,
        ..handle
    };
    assert!(matches!(
        missing
            .request_selected_run_cancellation(Default::default())
            .await,
        Err(Error::InvalidCooperativeCancellation(_))
    ));
    assert_eq!(server.requests.lock().unwrap().len(), 4);
}

#[tokio::test]
async fn cooperative_request_bounds_discovery_and_mutation_with_one_total_budget() {
    let server = server();
    let started = Instant::now();
    let result = client(&server, "budget")
        .request_workflow_cancellation("workflow", Default::default())
        .await;
    assert!(matches!(result, Err(Error::Timeout)), "{result:?}");
    assert!(started.elapsed() >= Duration::from_secs(4));
    assert!(
        started.elapsed() < Duration::from_secs(7),
        "total budget exceeded: {:?}",
        started.elapsed()
    );
    assert_eq!(server.requests.lock().unwrap().len(), 2);
}
