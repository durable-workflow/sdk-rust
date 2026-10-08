use super::*;

fn scope_event(kind: &str, payload: Value) -> HistoryEvent {
    serde_json::from_value(json!({"event_type": kind, "payload": payload})).unwrap()
}

fn scope_histories() -> Vec<HistoryEvent> {
    let mut events = [
        "CancellationScopeOpened",
        "CancellationScopeRequested",
        "CancellationScopeDeliveryPrepared",
        "CancellationScopeDelivered",
        "CancellationScopeRequestConflicted",
    ]
    .into_iter()
    .map(|kind| scope_event(kind, json!({"scope_id": "scope-one", "sequence": 1})))
    .collect::<Vec<_>>();
    for location in [
        None,
        Some("activity"),
        Some("timer"),
        Some("child_workflow"),
    ] {
        let membership = json!({"cancellation_scope_id": "scope-one"});
        let payload = location.map_or_else(|| membership.clone(), |name| json!({name: membership}));
        events.push(scope_event("TimerScheduled", payload));
    }
    for malformed in [
        Value::Null,
        json!(true),
        json!(1),
        json!(""),
        json!([]),
        json!({}),
    ] {
        events.push(scope_event(
            "TimerScheduled",
            json!({"cancellation_scope_id": malformed}),
        ));
    }
    events
}

fn scope_worker() -> Worker {
    Worker::new(Client::new("http://127.0.0.1:1").unwrap(), "scope-queue").worker_id("scope-worker")
}

#[test]
fn cancellation_scope_admission_precedes_the_application_factory() {
    for event in scope_histories() {
        let calls = Arc::new(AtomicUsize::new(0));
        let factory_calls = Arc::clone(&calls);
        let mut worker = scope_worker();
        worker.register_workflow("scope-probe", move |_ctx, _input| {
            factory_calls.fetch_add(1, Ordering::SeqCst);
            async move { Ok(json!("unexpected")) }
        });
        let error = worker
            .execute_workflow_task(workflow_task("scope-probe", vec![event], DEFAULT_CODEC))
            .expect_err("unqualified scope history must not run application code");
        assert!(error
            .to_string()
            .contains("cancellation_scope_execution_not_supported"));
        assert!(error.to_string().contains("Rust"));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn cancellation_scope_admission_precedes_replayed_query_factories_and_handlers() {
    let calls = Arc::new(AtomicUsize::new(0));
    let factory_calls = Arc::clone(&calls);
    let body_calls = Arc::clone(&calls);
    let query_calls = Arc::clone(&calls);
    let mut worker = scope_worker();
    worker.register_replayed_workflow(
        "scope-probe",
        move || {
            factory_calls.fetch_add(1, Ordering::SeqCst);
            0_u64
        },
        move |_ctx, _input, _state| {
            body_calls.fetch_add(1, Ordering::SeqCst);
            async move { Ok(Value::Null) }
        },
    );
    worker.register_replayed_query::<u64, _, _>("scope-probe", "state", move |_, _, _| {
        query_calls.fetch_add(1, Ordering::SeqCst);
        async move { Ok(Value::Null) }
    });
    let task = serde_json::from_value(json!({
        "query_task_id": "scope-query", "query_task_attempt": 1,
        "workflow_type": "scope-probe", "query_name": "state", "payload_codec": "avro",
        "workflow_arguments": fixture_envelope(json!([])),
        "query_arguments": fixture_envelope(json!([])),
        "history_events": [{"event_type": "CancellationScopeOpened", "payload": {"scope_id": "scope-one"}}],
        "run_status": "running"
    })).unwrap();
    let error = worker
        .execute_query_task(task)
        .await
        .expect_err("scope query must refuse replay");
    assert!(error
        .message
        .contains("cancellation_scope_execution_not_supported"));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[test]
fn cancellation_scope_admission_preserves_omitted_and_explicit_root_membership() {
    for explicit_root in [false, true] {
        let mut worker = scope_worker();
        worker.register_workflow("root-probe", |ctx, _| async move {
            ctx.sleep(Duration::from_secs(1)).await?;
            Ok(json!("done"))
        });
        let mut payload = json!({"sequence": 1, "timer_id": "timer-one", "delay_seconds": 1});
        if explicit_root {
            payload["cancellation_scope_id"] = json!("root");
        }
        let history = vec![
            scope_event("TimerScheduled", payload.clone()),
            scope_event("TimerFired", payload),
        ];
        let commands = worker
            .execute_workflow_task(workflow_task("root-probe", history, DEFAULT_CODEC))
            .unwrap();
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0]["type"], "complete_workflow");
        assert_eq!(
            decode_wire_value(&commands[0]["result"], DEFAULT_CODEC).unwrap(),
            json!("done")
        );
    }
}

#[test]
fn cancellation_scope_admission_preserves_scope_named_application_data() {
    let value = json!({"cancellation_scope_id": "application-value", "activity": {"cancellation_scope_id": null}});
    let history = vec![scope_event(
        "SideEffectRecorded",
        json!({"sequence": 1, "result": fixture_envelope(value.clone())}),
    )];
    let mut worker = scope_worker();
    worker.register_workflow("scope-data", |ctx, _| async move {
        let recorded: Value =
            ctx.side_effect(|| panic!("recorded side effect must not execute"))?;
        Ok(recorded)
    });
    let commands = worker
        .execute_workflow_task(workflow_task("scope-data", history, DEFAULT_CODEC))
        .unwrap();
    assert_eq!(
        decode_wire_value(&commands[0]["result"], DEFAULT_CODEC).unwrap(),
        value
    );
}

#[tokio::test]
async fn cancellation_scope_admission_publishes_no_completion_or_task_failure() {
    let server = MockWorkerServer::start();
    let worker = Worker::new(Client::new(server.base_url()).unwrap(), "scope-queue");
    let error = worker
        .settle_workflow_task_decision(
            "scope-task",
            "scope-worker",
            2,
            Some("scope-run"),
            Err(Error::CancellationScopeExecutionUnavailable),
            true,
            None,
        )
        .await
        .expect_err("unsupported scope must retain an explicit capability refusal");
    assert!(matches!(
        error,
        Error::CancellationScopeExecutionUnavailable
    ));
    assert!(server.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn cancellation_scope_admission_keeps_explicit_snapshot_inspection_available() {
    let mut worker = scope_worker();
    worker.register_workflow("scope-probe", |_, _| async move {
        panic!("snapshot inspection must not replay the workflow")
    });
    worker.register_query("scope-probe", "inspect", |ctx, _| async move {
        Ok(json!(ctx.history_events[0].event_type))
    });
    let task = serde_json::from_value(json!({
        "query_task_id": "scope-inspect", "query_task_attempt": 1,
        "workflow_type": "scope-probe", "query_name": "inspect", "payload_codec": "avro",
        "workflow_arguments": fixture_envelope(json!([])), "query_arguments": fixture_envelope(json!([])),
        "history_events": [{"event_type": "CancellationScopeOpened", "payload": {"scope_id": "scope-one"}}],
        "run_status": "running"
    })).unwrap();
    assert_eq!(
        worker
            .execute_query_task(task)
            .await
            .unwrap()
            .into_json()
            .unwrap(),
        json!("CancellationScopeOpened")
    );
}
