use super::*;

fn fixture() -> Value {
    let source: Value = serde_json::from_str(include_str!(
        "../../tests/fixtures/populated-scope-single-calls.json"
    ))
    .unwrap();
    source["timer"].clone()
}

fn task(value: &Value) -> WorkflowTask {
    serde_json::from_value(json!({"task_id":"task-one", "workflow_id":value["task"]["workflow_id"],
        "run_id":value["task"]["run_id"], "workflow_type":"scope-replay", "payload_codec":DEFAULT_CODEC,
        "lease_owner":"original", "workflow_task_attempt":1, "history_events":value["history"]})).unwrap()
}

fn worker(cleanup: Arc<AtomicUsize>, changed_delay: bool) -> Worker {
    let mut worker = Worker::new(Client::new("http://unused.invalid").unwrap(), "queue")
        .cooperative_cancellation(true)
        .candidate_cancellation_scope_authoring(true)
        .candidate_cancellation_scope_delivery(true);
    worker.register_workflow("scope-replay", move |ctx, _| {
            let cleanup = Arc::clone(&cleanup);
            async move {
                let parent = ctx.clone();
                let result = ctx.cancellation_scope(false, move |outer| async move {
                    outer.cancellation_scope(false, move |inner| async move {
                        assert_eq!(inner.activity("prior-step", json!([])).await?, json!("prior-value"));
                        match inner.sleep(Duration::from_secs(if changed_delay { 3599 } else { 3600 })).await {
                            Ok(()) => Ok(json!("ordinary-result")),
                            Err(Error::CancellationScopeRequested(cancellation)) => {
                                cleanup.fetch_add(1, Ordering::SeqCst);
                                assert!(inner.is_cancellation_requested()?);
                                assert_eq!(inner.scoped_cancellation_context()?.unwrap(), cancellation.context);
                                let before = cancellation.context.remaining()?;
                                    assert_eq!(before, Duration::from_secs(21));
                                let _shield = inner.cancellation_shield()?;
                                inner.sleep(Duration::from_secs(2)).await?;
                                let after = cancellation.context.remaining()?;
                                    assert_eq!(after, Duration::from_secs(19));
                                Ok(json!({"cleaned":true, "request_id":cancellation.context.request_id()}))
                            }
                            Err(error) => Err(error),
                        }
                    }).await
                }).await?;
                assert!(!parent.is_cancellation_requested()?);
                assert!(parent.scoped_cancellation_context()?.is_none());
                parent.sleep(Duration::from_secs(1)).await?;
                Ok(result)
            }
        });
    worker
}

fn prefix(value: &Value, before: &str) -> Value {
    let mut value = value.clone();
    let events = value["history"].as_array_mut().unwrap();
    let index = events
        .iter()
        .position(|event| event["event_type"] == before)
        .unwrap();
    events.truncate(index);
    value
}

fn append(value: &mut Value, kind: &str, payload: Value, timestamp: &str) {
    let events = value["history"].as_array_mut().unwrap();
    let sequence = events.last().unwrap()["sequence"].as_u64().unwrap() + 1;
    let namespace = events[0]["namespace"].clone();
    events.push(
        json!({"id":format!("cleanup-event-{sequence}"), "sequence":sequence,
        "namespace":namespace, "event_type":kind, "payload":payload, "timestamp":timestamp}),
    );
}

fn cleanup_snapshot(value: &Value) -> Value {
    let delivery = value["history"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["event_type"] == "CancellationScopeDelivered")
        .unwrap();
    let context =
        ScopedCancellationContext::from_value(&delivery["payload"]["cancellation"]).unwrap();
    json!({"scope_id":context.scope_id(), "operation_scope_id":context.scope_id(),
        "request_id":context.request_id(), "root_request_id":context.root_context().root_request_id(),
        "delivery_history_event_id":delivery["id"],
        "preparation_history_event_id":delivery["payload"]["preparation_history_event_id"],
        "cleanup_deadline_at":context.deadline().to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
        "authority_deadline_at":delivery["payload"]["authority_deadline_at"]})
}

#[test]
fn cancellation_scope_replay_waits_for_original_prepare_and_committed_delivery() {
    let original = fixture();
    let cleanup = Arc::new(AtomicUsize::new(0));
    let worker = worker(Arc::clone(&cleanup), false);
    for before in [
        "CancellationScopeDeliveryPrepared",
        "CancellationScopeDelivered",
    ] {
        let decision = worker
            .execute_workflow_task_decision(task(&prefix(&original, before)))
            .unwrap();
        let intent = decision.cancellation_scope_delivery.unwrap();
        assert_eq!(intent.boundary.sequence, 4);
        assert_eq!(intent.boundary.call_kind, CancellationCallKind::Timer);
        assert!(decision.commands.is_empty());
        assert_eq!(cleanup.load(Ordering::SeqCst), 0);
    }
    let decision = worker
        .execute_workflow_task_decision(task(&original))
        .unwrap();
    assert!(decision.cancellation_scope_delivery.is_none());
    assert_eq!(decision.commands.len(), 1);
    assert_eq!(decision.commands[0]["type"], "start_timer");
    assert_eq!(decision.commands[0]["delay_seconds"], 2);
    assert!(decision.commands[0]["cancellation_scope_id"].is_string());
    let snapshot = cleanup_snapshot(&original);
    assert_eq!(
        decision.commands[0]["cancellation_cleanup"],
        json!({
        "scope_id":snapshot["scope_id"], "request_id":snapshot["request_id"],
        "delivery_history_event_id":snapshot["delivery_history_event_id"]})
    );
    assert_eq!(cleanup.load(Ordering::SeqCst), 1);
}

#[test]
fn cancellation_scope_replay_replacement_keeps_context_clock_and_parent_unaffected() {
    let mut source = fixture();
    let snapshot = cleanup_snapshot(&source);
    let scope = source["history"]
        .as_array()
        .unwrap()
        .iter()
        .find(|event| event["event_type"] == "CancellationScopeDelivered")
        .unwrap()["payload"]["scope_id"]
        .clone();
    append(
        &mut source,
        "TimerScheduled",
        json!({"sequence":5, "timer_id":"cleanup-timer",
        "delay_seconds":2, "cancellation_scope_id":scope,
        "fire_at":"2026-10-04T00:00:11.123456Z", "cancellation_cleanup":snapshot}),
        "2026-10-04T00:00:09.123456Z",
    );
    append(
        &mut source,
        "TimerFired",
        json!({"sequence":5, "timer_id":"cleanup-timer", "delay_seconds":2,
        "cancellation_scope_id":scope}),
        "2026-10-04T00:00:11.123456Z",
    );
    let replacement = worker(Arc::new(AtomicUsize::new(0)), false).worker_id("replacement");
    let mut claim = task(&source);
    claim.lease_owner = Some("replacement".into());
    claim.workflow_task_attempt = 2;
    let decision = replacement.execute_workflow_task_decision(claim).unwrap();
    assert_eq!(decision.commands.len(), 1);
    assert_eq!(decision.commands[0]["delay_seconds"], 1);
    assert!(decision.commands[0].get("cancellation_scope_id").is_none());
    append(
        &mut source,
        "TimerScheduled",
        json!({"sequence":6, "timer_id":"parent-timer", "delay_seconds":1}),
        "2026-10-04T00:00:11.123456Z",
    );
    append(
        &mut source,
        "TimerFired",
        json!({"sequence":6, "timer_id":"parent-timer", "delay_seconds":1}),
        "2026-10-04T00:00:12.123456Z",
    );
    let decision = replacement
        .execute_workflow_task_decision(task(&source))
        .unwrap();
    assert_eq!(decision.commands.len(), 1);
    assert_eq!(decision.commands[0]["type"], "complete_workflow");
}

#[test]
fn cancellation_scope_replay_cleanup_timer_refuses_changed_authority_before_factory() {
    for field in [
        "request_id",
        "delivery_history_event_id",
        "authority_deadline_at",
        "cleanup_deadline_at",
        "operation_scope_id",
    ] {
        let mut value = fixture();
        let mut snapshot = cleanup_snapshot(&value);
        let scope = snapshot["scope_id"].clone();
        snapshot[field] = json!("changed");
        append(
            &mut value,
            "TimerScheduled",
            json!({"sequence":5, "timer_id":"cleanup-timer",
            "delay_seconds":2, "cancellation_scope_id":scope,
            "fire_at":"2026-10-04T00:00:11.123456Z", "cancellation_cleanup":snapshot}),
            "2026-10-04T00:00:09.123456Z",
        );
        let factories = Arc::new(AtomicUsize::new(0));
        let called = Arc::clone(&factories);
        let mut worker = worker(Arc::new(AtomicUsize::new(0)), false);
        worker.register_workflow("scope-replay", move |_, _| {
            called.fetch_add(1, Ordering::SeqCst);
            async { Ok(Value::Null) }
        });
        assert!(worker.execute_workflow_task_decision(task(&value)).is_err());
        assert_eq!(factories.load(Ordering::SeqCst), 0);
    }
}

#[test]
fn cancellation_scope_replay_descriptor_change_cannot_enter_cleanup() {
    let cleanup = Arc::new(AtomicUsize::new(0));
    let result =
        worker(Arc::clone(&cleanup), true).execute_workflow_task_decision(task(&fixture()));
    assert!(
        matches!(result, Err(Error::NonDeterministicReplay(failure)) if failure.reason == "timer_delay_mismatch")
    );
    assert_eq!(cleanup.load(Ordering::SeqCst), 0);
}

#[test]
fn cancellation_scope_replay_default_and_incomplete_opt_in_refuse_before_factory() {
    for (authoring, delivery) in [(false, false), (true, false), (false, true)] {
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&calls);
        let mut worker = Worker::new(Client::new("http://unused.invalid").unwrap(), "queue")
            .cooperative_cancellation(true)
            .candidate_cancellation_scope_authoring(authoring)
            .candidate_cancellation_scope_delivery(delivery);
        worker.register_workflow("scope-replay", move |_, _| {
            observed.fetch_add(1, Ordering::SeqCst);
            async { Ok(json!("must-not-enter")) }
        });
        assert!(matches!(
            worker.execute_workflow_task_decision(task(&fixture())),
            Err(Error::CancellationScopeExecutionUnavailable)
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
}

#[test]
fn cancellation_scope_replay_bad_projection_refuses_before_factory() {
    let mut source = fixture();
    for event in source["history"].as_array_mut().unwrap() {
        if event["event_type"] == "CancellationScopeDeliveryPrepared" {
            event["payload"]["timer_members"][0]["descriptor_hash"] = json!("0".repeat(64));
        }
    }
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    let mut worker = Worker::new(Client::new("http://unused.invalid").unwrap(), "queue")
        .cooperative_cancellation(true)
        .candidate_cancellation_scope_authoring(true)
        .candidate_cancellation_scope_delivery(true);
    worker.register_workflow("scope-replay", move |_, _| {
        observed.fetch_add(1, Ordering::SeqCst);
        async { Ok(json!("must-not-enter")) }
    });
    assert!(worker
        .execute_workflow_task_decision(task(&source))
        .is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}
