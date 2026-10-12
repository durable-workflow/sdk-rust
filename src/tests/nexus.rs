use super::*;

fn service_event(kind: &str) -> HistoryEvent {
    serde_json::from_value(json!({"event_type":kind,"payload":{
        "sequence":1,"service_call_id":"original-call","endpoint_name":"endpoint",
        "service_name":"service","operation_name":"operation","operation_mode":"sync","wait_for":"completed",
        "response_payload":if kind == "ServiceCallCompleted" { fixture_envelope(json!({"answer":42})) } else {Value::Null},
        "service_call":{"service_call_id":"original-call","target_namespace":"shared","idempotency_key":"original-key"}
    }})).unwrap()
}

fn options() -> NexusServiceOptions {
    NexusServiceOptions {
        target_namespace: Some("shared".into()),
        idempotency_key: Some("original-key".into()),
        ..NexusServiceOptions::default()
    }
}

fn worker() -> Worker {
    let mut worker = Worker::new(
        Client::builder("http://127.0.0.1:1").build().unwrap(),
        "queue",
    )
    .cooperative_cancellation(true);
    worker.register_workflow("nexus", |ctx, _| async move {
        let result = ctx
            .call_nexus_service_with_options(
                "endpoint",
                "service",
                "operation",
                options(),
                json!(["world"]),
            )
            .await?;
        result.result.into_json()
    });
    worker
}

fn request() -> HistoryEvent {
    serde_json::from_value(
        json!({"event_type":"CooperativeCancellationRequested","timestamp":"2026-10-12T14:00:00Z",
        "payload":{"workflow_command_id":"original-request","workflow_run_id":"run-nexus",
            "cleanup_deadline_at":"2026-10-12T14:00:30Z"}}),
    )
    .unwrap()
}

fn delivery() -> HistoryEvent {
    serde_json::from_value(
        json!({"event_type":"CooperativeCancellationDelivered","timestamp":"2026-10-12T14:00:02Z",
        "payload":{"workflow_command_id":"original-request","workflow_run_id":"run-nexus",
            "sequence":1,"call_kind":"service_operation"}}),
    )
    .unwrap()
}

#[test]
fn authored_service_admission_uses_the_leased_avro_command() {
    let commands = worker()
        .execute_workflow_task(workflow_task("nexus", vec![], DEFAULT_CODEC))
        .unwrap();
    assert_eq!(commands.len(), 1);
    assert_eq!(commands[0]["type"], "start_service_operation");
    assert_eq!(commands[0]["namespace"], "shared");
    assert_eq!(commands[0]["idempotency_key"], "original-key");
    assert_eq!(commands[0]["mode_override"], "sync");
    assert_eq!(commands[0]["wait_for"], "completed");
    assert_eq!(
        decode_wire_value(&commands[0]["request_payload"], DEFAULT_CODEC).unwrap(),
        json!(["world"])
    );
}

#[test]
fn original_pending_call_is_never_readmitted_and_terminal_result_resumes_it() {
    for _ in 0..3 {
        let commands = worker()
            .execute_workflow_task(workflow_task(
                "nexus",
                vec![service_event("ServiceCallStarted")],
                DEFAULT_CODEC,
            ))
            .unwrap();
        assert!(commands.is_empty());
    }
    let commands = worker()
        .execute_workflow_task(workflow_task(
            "nexus",
            vec![
                service_event("ServiceCallStarted"),
                service_event("ServiceCallCompleted"),
            ],
            DEFAULT_CODEC,
        ))
        .unwrap();
    assert_eq!(commands.len(), 1);
    assert_eq!(commands[0]["type"], "complete_workflow");
    assert_eq!(
        decode_wire_value(&commands[0]["result"], DEFAULT_CODEC).unwrap(),
        json!({"answer":42})
    );
}

#[test]
fn terminal_service_failure_is_typed_at_the_original_call() {
    for (kind, status) in [
        ("ServiceCallFailed", "failed"),
        ("ServiceCallCancelled", "cancelled"),
    ] {
        let mut worker = worker();
        worker.workflows.clear();
        worker.registration_definitions.clear();
        worker.register_workflow("nexus", move |ctx, _| async move {
            let Err(Error::NexusOperationFailed(failure)) = ctx
                .call_nexus_service_with_options(
                    "endpoint",
                    "service",
                    "operation",
                    options(),
                    json!(["world"]),
                )
                .await
            else {
                panic!("typed service failure expected")
            };
            assert_eq!(failure.service_call_id, "original-call");
            assert_eq!(failure.status, status);
            Ok(json!(failure.exception_type))
        });
        let commands = worker
            .execute_workflow_task(workflow_task(
                "nexus",
                vec![service_event("ServiceCallStarted"), service_event(kind)],
                DEFAULT_CODEC,
            ))
            .unwrap();
        assert_eq!(
            decode_wire_value(&commands[0]["result"], DEFAULT_CODEC).unwrap(),
            json!(format!("service_call_{status}"))
        );
    }
}

#[test]
fn root_request_selects_original_service_boundary_without_readmission() {
    let mut task = workflow_task(
        "nexus",
        vec![service_event("ServiceCallStarted"), request()],
        DEFAULT_CODEC,
    );
    task.lease_owner = Some("original-owner".into());
    let observation = CancellationRequest {
        request_id: "original-request".into(),
        requested_at: "2026-10-12T14:00:00Z".into(),
        cleanup_deadline_at: "2026-10-12T14:00:30Z".into(),
        history_refresh_page_token: Some("original-page".into()),
        context: None,
    };
    let decision = worker()
        .execute_workflow_task_decision_with_cancellation(task, Some(&observation))
        .unwrap();
    assert!(decision.commands.is_empty());
    let intent = decision.cancellation_delivery.unwrap();
    assert_eq!(intent.call_kind, CancellationCallKind::ServiceOperation);
    assert_eq!(intent.sequence, 1);
    assert_eq!(intent.request_id, "original-request");
}

#[test]
fn canonical_service_delivery_cold_replays_original_cleanup() {
    let mut worker = worker();
    worker.workflows.clear();
    worker.registration_definitions.clear();
    worker.register_workflow("nexus", |ctx, _| async move {
        let Err(Error::CooperativeCancellationRequested(cancelled)) = ctx
            .call_nexus_service_with_options(
                "endpoint",
                "service",
                "operation",
                options(),
                json!(["world"]),
            )
            .await
        else {
            panic!("original canonical delivery expected")
        };
        assert_eq!(
            cancelled.request.cleanup_deadline_at,
            "2026-10-12T14:00:30Z"
        );
        let _shield = ctx.cancellation_shield()?;
        ctx.sleep(Duration::from_secs(1)).await?;
        Ok(json!(cancelled.request.request_id))
    });
    let mut history = vec![
        service_event("ServiceCallStarted"),
        request(),
        service_event("ServiceCallCancelled"),
        delivery(),
    ];
    let commands = worker
        .execute_workflow_task(workflow_task("nexus", history.clone(), DEFAULT_CODEC))
        .unwrap();
    assert_eq!(commands.len(), 1);
    assert_eq!(commands[0]["type"], "start_timer");
    history.push(serde_json::from_value(json!({"event_type":"TimerScheduled","payload":{"sequence":2,"timer_id":"cleanup","delay_seconds":1}})).unwrap());
    assert!(worker
        .execute_workflow_task(workflow_task("nexus", history.clone(), DEFAULT_CODEC))
        .unwrap()
        .is_empty());
    history.push(
        serde_json::from_value(
            json!({"event_type":"TimerFired","payload":{"sequence":2,"timer_id":"cleanup","delay_seconds":1}}),
        )
        .unwrap(),
    );
    let commands = worker
        .execute_workflow_task(workflow_task("nexus", history, DEFAULT_CODEC))
        .unwrap();
    assert_eq!(
        decode_wire_value(&commands[0]["result"], DEFAULT_CODEC).unwrap(),
        json!("original-request")
    );
}

#[test]
fn results_committed_before_request_are_not_replaced_by_delivery() {
    let history = vec![
        service_event("ServiceCallStarted"),
        service_event("ServiceCallCompleted"),
        request(),
    ];
    let decision = worker()
        .execute_workflow_task_decision(workflow_task("nexus", history.clone(), DEFAULT_CODEC))
        .unwrap();
    assert!(decision.cancellation_delivery.is_none());
    assert_eq!(decision.commands.len(), 1);
    let mut invalid = history;
    invalid.push(delivery());
    assert!(worker()
        .execute_workflow_task(workflow_task("nexus", invalid, DEFAULT_CODEC))
        .is_err());
}

#[test]
fn changed_original_namespace_is_refused_before_cancellation() {
    let mut start = service_event("ServiceCallStarted");
    start.payload["service_call"]["target_namespace"] = json!("another-tenant");
    for suffix in [vec![], vec![request()], vec![request(), delivery()]] {
        let mut history = vec![start.clone()];
        history.extend(suffix);
        assert!(matches!(
            worker().execute_workflow_task(workflow_task("nexus", history, DEFAULT_CODEC)),
            Err(Error::NonDeterministicReplay(_))
        ));
    }
}

#[test]
fn acceptance_policy_preserves_original_admission_after_terminal_arrives() {
    for (mode, wait_for) in [
        (NexusOperationMode::Async, NexusWaitFor::Completed),
        (NexusOperationMode::Sync, NexusWaitFor::Accepted),
    ] {
        let mut worker = worker();
        worker.workflows.clear();
        worker.registration_definitions.clear();
        worker.register_workflow("nexus", move |ctx, _| async move {
            let result = ctx
                .call_nexus_service_with_options(
                    "endpoint",
                    "service",
                    "operation",
                    NexusServiceOptions {
                        mode,
                        wait_for,
                        ..options()
                    },
                    json!(["world"]),
                )
                .await?;
            Ok(json!({"status":result.status,"call":result.service_call_id}))
        });
        let mut start = service_event("ServiceCallStarted");
        start.payload["operation_mode"] = json!(mode);
        start.payload["wait_for"] = json!(wait_for);
        for history in [
            vec![start.clone()],
            vec![start.clone(), service_event("ServiceCallCompleted")],
        ] {
            let commands = worker
                .execute_workflow_task(workflow_task("nexus", history, DEFAULT_CODEC))
                .unwrap();
            assert_eq!(
                decode_wire_value(&commands[0]["result"], DEFAULT_CODEC).unwrap(),
                json!({"status":"started","call":"original-call"})
            );
        }
    }
}
