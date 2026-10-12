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
            assert_eq!(failure.service_call_id.as_deref(), Some("original-call"));
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
        let mut terminal = service_event("ServiceCallCompleted");
        terminal.payload["operation_mode"] = json!(mode);
        terminal.payload["wait_for"] = json!(wait_for);
        for history in [vec![start.clone()], vec![start.clone(), terminal]] {
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

#[test]
fn service_history_refuses_changed_caller_target_and_original_policy() {
    for (field, value) in [
        ("caller_workflow_instance_id", "another-workflow"),
        ("caller_workflow_run_id", "another-run"),
        ("target_namespace", "another-tenant"),
        ("endpoint_name", "another-endpoint"),
        ("service_name", "another-service"),
        ("operation_name", "another-operation"),
        ("idempotency_key", "another-key"),
        ("operation_mode", "async"),
        ("wait_for", "accepted"),
        ("linked_workflow_instance_id", "another-target"),
        ("linked_workflow_run_id", "another-target-run"),
        ("call_deadline_at", "2026-10-12T14:04:00Z"),
    ] {
        let mut start = service_event("ServiceCallStarted");
        start.payload["caller_workflow_instance_id"] = json!("wf-nexus");
        start.payload["caller_workflow_run_id"] = json!("run-nexus");
        start.payload["linked_workflow_instance_id"] = json!("original-target");
        start.payload["linked_workflow_run_id"] = json!("original-target-run");
        start.payload["service_call"]["call_deadline_at"] = json!("2026-10-12T14:03:00Z");
        let mut terminal = service_event("ServiceCallCompleted");
        terminal.payload[field] = json!(value);
        assert!(
            matches!(
                worker().execute_workflow_task(workflow_task(
                    "nexus",
                    vec![start, terminal],
                    DEFAULT_CODEC,
                )),
                Err(Error::NonDeterministicReplay(_))
            ),
            "changed {field}"
        );
    }
    for bad in [json!(false), json!(" ")] {
        let mut start = service_event("ServiceCallStarted");
        start.payload["caller_workflow_run_id"] = bad;
        assert!(worker()
            .execute_workflow_task(workflow_task("nexus", vec![start], DEFAULT_CODEC,))
            .is_err());
    }
    assert!(worker()
        .execute_workflow_task(workflow_task(
            "nexus",
            vec![
                service_event("ServiceCallCompleted"),
                service_event("ServiceCallStarted"),
            ],
            DEFAULT_CODEC
        ))
        .is_err());
}

#[test]
fn partial_terminal_projection_preserves_original_target() {
    let mut start = service_event("ServiceCallStarted");
    start.payload["linked_workflow_instance_id"] = json!("original-target");
    start.payload["linked_workflow_run_id"] = json!("original-target-run");
    let terminal: HistoryEvent = serde_json::from_value(json!({
        "event_type":"ServiceCallCompleted", "payload":{
            "sequence":1,"service_call_id":"original-call",
            "response_payload":fixture_envelope(json!({"answer":42}))
        }
    }))
    .unwrap();
    let call = crate::nexus::recorded_call(
        &[&start, &terminal],
        1,
        DEFAULT_CODEC,
        &WorkflowIdentity {
            workflow_id: Some("wf-nexus".into()),
            run_id: Some("run-nexus".into()),
        },
    )
    .unwrap();
    let result = call.outcome.unwrap().unwrap();
    assert_eq!(result.target_namespace.as_deref(), Some("shared"));
    assert_eq!(
        result.linked_workflow_instance_id.as_deref(),
        Some("original-target")
    );
    assert_eq!(
        result.linked_workflow_run_id.as_deref(),
        Some("original-target-run")
    );
}

#[test]
fn immediate_service_outcome_and_pre_admission_refusal_resume_the_original_call() {
    for kind in [
        "ServiceCallCompleted",
        "ServiceCallFailed",
        "ServiceCallCancelled",
    ] {
        let commands = worker()
            .execute_workflow_task(workflow_task(
                "nexus",
                vec![service_event(kind)],
                DEFAULT_CODEC,
            ))
            .unwrap();
        assert_eq!(commands.len(), 1);
        assert_eq!(
            commands[0]["type"],
            if kind == "ServiceCallCompleted" {
                "complete_workflow"
            } else {
                "fail_workflow"
            }
        );
    }
    let mut refusal = service_event("ServiceCallFailed");
    refusal.payload["service_call_id"] = Value::Null;
    refusal.payload["service_call"]["service_call_id"] = Value::Null;
    refusal.payload["exception_type"] = json!("endpoint_not_found");
    let commands = worker()
        .execute_workflow_task(workflow_task("nexus", vec![refusal], DEFAULT_CODEC))
        .unwrap();
    assert_eq!(commands[0]["exception_type"], "NexusOperationFailed");
    assert_eq!(
        commands[0]["exception"]["properties"]["caller_observed_error_type"],
        "endpoint_not_found"
    );
    assert!(commands[0]["exception"]["properties"]["service_call_id"].is_null());
}

#[test]
fn changed_authored_service_arguments_are_refused_before_delivery() {
    let mut start = service_event("ServiceCallStarted");
    start.payload["request_payload"] = fixture_envelope(json!(["other-argument"]));
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
fn authored_nexus_payload_preserves_the_request_value_shape() {
    for request in [
        Value::Null,
        json!("single-value"),
        json!({"invoice":"42"}),
        json!(["argument"]),
    ] {
        let mut worker = Worker::new(Client::new("http://127.0.0.1:1").unwrap(), "queue");
        let original = request.clone();
        worker.register_workflow("nexus", move |ctx, _| {
            let request = request.clone();
            async move {
                ctx.call_nexus_service("endpoint", "service", "operation", request)
                    .await?;
                Ok(Value::Null)
            }
        });
        let commands = worker
            .execute_workflow_task(workflow_task("nexus", vec![], DEFAULT_CODEC))
            .unwrap();
        assert_eq!(
            decode_wire_value(&commands[0]["request_payload"], DEFAULT_CODEC).unwrap(),
            original
        );
    }
}

fn capabilities() -> Value {
    json!({"cooperative_cancellation":true,"nexus_cooperative_cancellation":true,
        "nexus_cooperative_cancellation_binding_kinds":["workflow_run"]})
}

fn transport_response(path: &str, body: &str, number: usize) -> Option<(&'static str, String)> {
    let case = path.split('/').nth(1).unwrap_or_default();
    if path.ends_with("/cluster/info") {
        let mut caps = capabilities();
        match case {
            "no-nexus" => caps["nexus_cooperative_cancellation"] = json!(false),
            "no-cooperation" => caps["cooperative_cancellation"] = json!(false),
            "string-support" => caps["nexus_cooperative_cancellation"] = json!("true"),
            "wrong-binding" => {
                caps["nexus_cooperative_cancellation_binding_kinds"] = json!(["workflow_query"])
            }
            _ => {}
        }
        return Some((
            "200 OK",
            json!({"worker_protocol":{"server_capabilities":caps}}).to_string(),
        ));
    }
    if path.ends_with("/worker/register") {
        let request: Value = serde_json::from_str(body).unwrap();
        let mut reply = json!({"registered":true,"worker_id":request["worker_id"],
            "namespace":"nexus-test","task_queue":request["task_queue"],
            "capabilities":request["capabilities"],"heartbeat_interval_seconds":3600,
            "protocol_version":"1.20","server_capabilities":capabilities()});
        match case {
            "ack-no-nexus" => {
                reply["server_capabilities"]["nexus_cooperative_cancellation"] = json!(false)
            }
            "ack-wrong-binding" => {
                reply["server_capabilities"]["nexus_cooperative_cancellation_binding_kinds"] =
                    json!(["workflow_query"])
            }
            "ack-no-capability" => reply["capabilities"]
                .as_array_mut()
                .unwrap()
                .retain(|value| value != "nexus_cooperative_cancellation"),
            "ack-old-protocol" => reply["protocol_version"] = json!("1.19"),
            _ => {}
        }
        return Some(("200 OK", reply.to_string()));
    }
    if path.ends_with("/deliver-cancellation") {
        if case == "lost-ack" {
            return Some(("invalid-status", String::new()));
        }
        let mut reply = json!({"delivered":false,"task_id":"task-nexus","workflow_run_id":"run-nexus",
            "reason":"cancellation_waiting_for_service_operation","claim_released":true});
        match case {
            "wrong-task" => reply["task_id"] = json!("another-task"),
            "wrong-run" => reply["workflow_run_id"] = json!("another-run"),
            "no-release" => reply["claim_released"] = json!(false),
            "string-release" => reply["claim_released"] = json!("true"),
            "wrong-reason" => reply["reason"] = json!("cancellation_waiting_for_child"),
            "false-delivery" => reply["request_id"] = json!("original-request"),
            _ => {}
        }
        return Some(("200 OK", reply.to_string()));
    }
    let mut requested = request();
    requested.payload["cleanup_deadline_at"] = json!("2099-01-01T00:00:00Z");
    let mut events = vec![service_event("ServiceCallStarted"), requested];
    if case == "lost-ack" && path.ends_with("/history") && number >= 2 {
        events.push(service_event("ServiceCallCancelled"));
        events.push(delivery());
    }
    let history = events
        .into_iter()
        .map(|event| {
            let mut raw = event.raw;
            raw.insert("event_type".into(), json!(event.event_type));
            raw.insert("payload".into(), event.payload);
            Value::Object(raw.into_iter().collect())
        })
        .collect::<Vec<_>>();
    if path.ends_with("/workflow-tasks/poll") {
        return Some(("200 OK", if number > 1 {json!({"task":null,"poll_status":"idle"})} else {json!({"task":{
            "task_id":"task-nexus","workflow_id":"wf-nexus","run_id":"run-nexus","workflow_type":"nexus",
            "workflow_task_attempt":1,"lease_owner":"nexus-worker","payload_codec":"avro",
            "arguments":fixture_envelope(json!([])),"history_events":history,
            "cancel_requested":true,"cancellation_request":{
                "request_id":"original-request","requested_at":"2026-10-12T14:00:00Z",
                "cleanup_deadline_at":"2099-01-01T00:00:00Z","history_refresh_page_token":"original-page"
            }
        },"protocol_version":"1.20"})}.to_string()));
    }
    if path.ends_with("/history") {
        return Some((
            "200 OK",
            json!({"task_id":"task-nexus","workflow_task_attempt":1,
            "history_events":history,"total_history_events":history.len(),"next_history_page_token":null})
            .to_string(),
        ));
    }
    Some(("200 OK", json!({"outcome":"deregistered"}).to_string()))
}

fn transport_server() -> MockWorkerServer {
    MockWorkerServer::start_with_behavior(MockWorkerBehavior {
        request_override: Some(transport_response),
        ..Default::default()
    })
}

fn transport_worker(server: &MockWorkerServer, case: &str) -> Worker {
    let mut worker = worker();
    worker.client = Client::builder(format!("{}/{case}", server.base_url()))
        .namespace("nexus-test")
        .worker_token(Some("worker-only".into()))
        .control_token(Some("control-only".into()))
        .build()
        .unwrap();
    worker
        .worker_id("nexus-worker")
        .nexus_cooperative_cancellation(true)
}

#[tokio::test]
async fn nexus_registration_requires_installed_discovery_and_exact_acknowledgment() {
    for case in [
        "supported",
        "no-nexus",
        "no-cooperation",
        "string-support",
        "wrong-binding",
        "ack-no-nexus",
        "ack-wrong-binding",
        "ack-no-capability",
        "ack-old-protocol",
    ] {
        let server = transport_server();
        let worker = transport_worker(&server, case);
        let result = worker.register().await;
        assert_eq!(result.is_ok(), case == "supported", "{case}: {result:?}");
        assert_eq!(
            worker.nexus_registration_confirmed.load(Ordering::SeqCst),
            case == "supported"
        );
        let requests = server.requests.lock().unwrap();
        assert_eq!(requests[0].method, "GET");
        assert!(requests[0].path.ends_with("/cluster/info"));
        for request in requests.iter() {
            assert_eq!(request.authorization.as_deref(), Some("Bearer worker-only"));
            assert_eq!(request.namespace.as_deref(), Some("nexus-test"));
            assert_eq!(request.worker_protocol.as_deref(), Some("1.20"));
            assert!(request.control_protocol.is_none());
        }
        let registration = requests
            .iter()
            .find(|request| request.path.ends_with("/register"));
        assert_eq!(
            registration.is_some(),
            case == "supported" || case.starts_with("ack-")
        );
        if let Some(registration) = registration {
            let body: Value = serde_json::from_str(&registration.body).unwrap();
            for flag in ["cooperative_cancellation", "nexus_cooperative_cancellation"] {
                assert!(body["capabilities"]
                    .as_array()
                    .unwrap()
                    .contains(&json!(flag)));
            }
            assert_eq!(
                body["capability_manifest"]["nexus_cooperative_cancellation"]["binding_kinds"],
                json!(["workflow_run"])
            );
        }
        if case.starts_with("ack-") {
            assert!(
                requests
                    .iter()
                    .any(|request| request.path.ends_with("/registrations/nexus-worker")),
                "invalid registration must be retired"
            );
        }
    }
}

#[tokio::test]
async fn ordinary_cooperation_needs_no_nexus_discovery_and_changed_worker_loses_confirmation() {
    let server = transport_server();
    let worker = transport_worker(&server, "no-nexus").nexus_cooperative_cancellation(false);
    worker.register().await.unwrap();
    assert!(!server
        .captured_paths()
        .iter()
        .any(|path| path.ends_with("/cluster/info")));
    let worker = transport_worker(&server, "supported");
    worker.register().await.unwrap();
    let worker = worker.worker_id("replacement-worker");
    assert!(!worker.nexus_registration_confirmed.load(Ordering::SeqCst));
}

#[tokio::test]
async fn pending_service_cleanup_releases_only_the_matching_claim() {
    let server = transport_server();
    let mut task = workflow_task("nexus", vec![], DEFAULT_CODEC);
    task.task_id = "task-nexus".into();
    task.lease_owner = Some("nexus-worker".into());
    let delivery = CancellationDelivery::from_payload(&delivery().payload).unwrap();
    for case in [
        "supported",
        "wrong-task",
        "wrong-run",
        "no-release",
        "string-release",
        "wrong-reason",
        "false-delivery",
    ] {
        let worker = transport_worker(&server, case);
        let result = worker
            .client
            .deliver_workflow_cancellation(&task, &delivery)
            .await;
        if case == "supported" {
            assert!(
                matches!(result,Ok(CancellationDeliveryReply::ClaimReleased{task_id,run_id}) if task_id == "task-nexus" && run_id == "run-nexus")
            );
        } else {
            assert!(result.is_err(), "{case}");
        }
    }
}

#[tokio::test]
async fn worker_returns_to_polling_without_settling_a_released_service_claim() {
    let server = transport_server();
    let worker = transport_worker(&server, "supported");
    worker.register().await.unwrap();
    assert_eq!(
        worker.poll_workflow_once().await.unwrap(),
        ManagedPollOutcome::Handled
    );
    assert_eq!(
        worker.poll_workflow_once().await.unwrap(),
        ManagedPollOutcome::Idle
    );
    let paths = server.captured_paths();
    assert_eq!(
        paths
            .iter()
            .filter(|path| path.ends_with("/history"))
            .count(),
        1
    );
    assert_eq!(
        paths
            .iter()
            .filter(|path| path.ends_with("/deliver-cancellation"))
            .count(),
        1
    );
    assert!(!paths
        .iter()
        .any(|path| path.ends_with("/complete") || path.ends_with("/fail")));
}

#[tokio::test]
async fn lost_service_delivery_acknowledgment_reloads_the_original_canonical_boundary() {
    let server = transport_server();
    let worker = transport_worker(&server, "lost-ack");
    worker.register().await.unwrap();
    assert_eq!(
        worker.poll_workflow_once().await.unwrap(),
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
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.path.ends_with("/history"))
            .count(),
        2
    );
    assert!(!requests
        .iter()
        .any(|request| request.path.ends_with("/fail")));
    let completion = requests
        .iter()
        .find(|request| request.path.ends_with("/complete"))
        .unwrap();
    let body: Value = serde_json::from_str(&completion.body).unwrap();
    assert_eq!(body["commands"].as_array().unwrap().len(), 1);
    assert_eq!(
        body["commands"][0]["exception_type"],
        "WorkflowCancellationRequested"
    );
    assert_eq!(
        body["commands"][0]["exception"]["properties"]["request_id"],
        "original-request"
    );
}
