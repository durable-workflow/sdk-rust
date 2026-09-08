use super::*;

const REFERENCE_ID: &str = "ep_00000000000000000000000001";
const FETCH_PATH: &str = "/api/external-payloads/v1/ep_00000000000000000000000001";

fn fixture() -> Value {
    serde_json::from_str(include_str!(
        "../../tests/fixtures/codec-regressions/runtime-external-activity-result.json"
    ))
    .unwrap()
}

fn blob() -> String {
    fixture()["framing"]["wire_base64"]
        .as_str()
        .unwrap()
        .to_owned()
}

fn envelope() -> Value {
    json!({"codec":"avro", "external_payload": {
        "schema":"durable-workflow.v2.runtime-external-payload-reference.v1",
        "codec":"avro", "reference_id": REFERENCE_ID,
        "size_bytes":blob().len(), "sha256":format!("{:x}", Sha256::digest(blob().as_bytes()))
    }})
}

fn response(path: &str) -> Option<(&'static str, String)> {
    if path == FETCH_PATH {
        return Some(("200 OK", blob()));
    }
    let value = if path.ends_with("/poll") {
        json!({"task": {
            "task_id":"external-task", "workflow_task_attempt":1,
            "workflow_id":"external-workflow", "run_id":"external-run",
            "workflow_type":"external.workflow", "payload_codec":"avro",
            "arguments": encode_value_envelope(&json!([]), DEFAULT_CODEC).unwrap(),
            "history_events":[{"event_type":"ActivityCompleted", "payload":{
                "sequence":1, "activity_type":"external.activity", "result":envelope(),
                "activity":{"result":envelope()}
            }}]
        }})
    } else {
        json!({"output_envelope":envelope(), "memo":{"external_payload":"business data"}})
    };
    Some(("200 OK", value.to_string()))
}

#[tokio::test]
async fn runtime_external_activity_result_resolves_before_cold_replay() {
    let server = MockWorkerServer::start_with_behavior(MockWorkerBehavior {
        response_override: Some(response),
        ..MockWorkerBehavior::default()
    });
    let client = Client::builder(server.base_url())
        .worker_token(Some("worker-only".to_owned()))
        .namespace("external-test")
        .build()
        .unwrap();
    let task = client
        .poll_workflow_task("worker", "queue", Duration::from_secs(1))
        .await
        .unwrap()
        .unwrap();
    validate_workflow_task_payloads(&task)
        .expect("runtime references must be resolved before replay");
    let result = &task.history_events[0].payload["result"];
    assert_eq!(
        decode_wire_avro_value(result, DEFAULT_CODEC).unwrap(),
        AvroValue::String(fixture()["value"]["value"].as_str().unwrap().to_owned())
    );
    for _ in 0..2 {
        let context = workflow_context(task.history_events.clone());
        let mut activity = Box::pin(context.activity("external.activity", json!([])));
        let mut task_context = TaskContext::from_waker(noop_waker_ref());
        assert!(
            matches!(activity.as_mut().poll(&mut task_context), Poll::Ready(Ok(result)) if result == fixture()["value"]["value"])
        );
        context.ensure_history_consumed().unwrap();
        assert!(
            context.take_commands().unwrap().is_empty(),
            "cold replay must not reschedule the activity"
        );
    }
    assert_eq!(
        server.request_count(FETCH_PATH),
        1,
        "duplicate nested result shares a response cache"
    );
    assert_eq!(
        server.authorization_for(FETCH_PATH).as_deref(),
        Some("Bearer worker-only")
    );
    assert_eq!(
        server.namespace_for(FETCH_PATH).as_deref(),
        Some("external-test")
    );
    assert_eq!(
        server.worker_protocol_for(FETCH_PATH).as_deref(),
        Some(WORKER_PROTOCOL_VERSION)
    );
    assert_eq!(server.control_protocol_for(FETCH_PATH), None);
}

#[tokio::test]
async fn runtime_external_client_result_preserves_projections_and_role() {
    let server = MockWorkerServer::start_with_behavior(MockWorkerBehavior {
        response_override: Some(response),
        ..MockWorkerBehavior::default()
    });
    let client = Client::builder(server.base_url())
        .control_token(Some("client-only".to_owned()))
        .build()
        .unwrap();
    let value: Value = client
        .request_json(
            reqwest::Method::GET,
            "/workflows/external",
            RequestProtocol::ControlPlane,
            None::<&Value>,
        )
        .await
        .unwrap();
    assert_eq!(value["output_envelope"]["blob"], blob());
    assert_eq!(value["memo"]["external_payload"], "business data");
    assert_eq!(
        server.authorization_for(FETCH_PATH).as_deref(),
        Some("Bearer client-only")
    );
    assert_eq!(
        server.control_protocol_for(FETCH_PATH).as_deref(),
        Some(CONTROL_PLANE_VERSION)
    );
    assert_eq!(server.worker_protocol_for(FETCH_PATH), None);
    let description = client.describe_workflow("external").await.unwrap();
    assert_eq!(
        description.output,
        Some(fixture()["value"]["value"].clone())
    );
    assert_eq!(
        server.request_count(FETCH_PATH),
        2,
        "cache does not outlive a response"
    );
}

#[tokio::test]
async fn runtime_external_invalid_references_never_fetch() {
    let server = MockWorkerServer::start_with_behavior(MockWorkerBehavior {
        response_override: Some(response),
        ..MockWorkerBehavior::default()
    });
    let client = Client::new(server.base_url()).unwrap();
    for (path, bad) in [
        ("/codec", json!("json")),
        ("/external_payload/schema", json!("unknown")),
        ("/external_payload/codec", json!("json")),
        (
            "/external_payload/reference_id",
            json!("https://elsewhere.example/steal"),
        ),
        ("/external_payload/reference_id", json!("../../secrets")),
        (
            "/external_payload/reference_id",
            json!("ep_IIIIIIIIIIIIIIIIIIIIIIIIII"),
        ),
        (
            "/external_payload/reference_id",
            json!("ep_0000000000000000000000000?"),
        ),
        ("/external_payload/sha256", json!("invalid")),
        ("/external_payload/size_bytes", json!(-1)),
        ("/external_payload/size_bytes", json!(1.5)),
        ("/external_payload/size_bytes", json!("48")),
    ] {
        let mut value = envelope();
        *value.pointer_mut(path).unwrap() = bad;
        let mut value = json!({"output_envelope":value});
        let error = client
            .resolve_runtime_payloads(&mut value, "/workflows/test", RequestProtocol::ControlPlane)
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("external_payload_unsupported"),
            "{error}"
        );
    }
    for extra_reference_field in [false, true] {
        let mut value = envelope();
        if extra_reference_field {
            value["external_payload"]["uri"] = json!("file:///must-not-read");
        } else {
            value["blob"] = json!(blob());
        }
        let error = client
            .resolve_runtime_payloads(
                &mut json!({"output_envelope":value}),
                "/workflows/test",
                RequestProtocol::ControlPlane,
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("external_payload_unsupported"));
    }
    assert!(server.captured_paths().is_empty());
}

#[tokio::test]
async fn runtime_external_downloads_are_bounded_across_references() {
    let server = MockWorkerServer::start_with_behavior(MockWorkerBehavior {
        response_override: Some(response),
        ..MockWorkerBehavior::default()
    });
    let client = Client::builder(server.base_url())
        .max_external_payload_bytes(blob().len())
        .build()
        .unwrap();
    let mut duplicate = json!({"input_envelope":envelope(), "output_envelope":envelope()});
    client
        .resolve_runtime_payloads(
            &mut duplicate,
            "/workflows/test",
            RequestProtocol::ControlPlane,
        )
        .await
        .unwrap();
    assert_eq!(server.request_count(FETCH_PATH), 1);
    let mut second = envelope();
    second["external_payload"]["reference_id"] = json!("ep_00000000000000000000000002");
    let mut multiple = json!({"input_envelope":envelope(), "output_envelope":second});
    let error = client
        .resolve_runtime_payloads(
            &mut multiple,
            "/workflows/test",
            RequestProtocol::ControlPlane,
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("external_payload_oversized"));
    assert_eq!(server.request_count(FETCH_PATH), 2);
    assert_eq!(
        server.request_count("/api/external-payloads/v1/ep_00000000000000000000000002"),
        0
    );
    let client = Client::builder(server.base_url())
        .max_external_payload_bytes(0)
        .build()
        .unwrap();
    let error = client
        .resolve_runtime_payloads(
            &mut json!({"output_envelope":envelope()}),
            "/workflows/test",
            RequestProtocol::ControlPlane,
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("external_payload_oversized"));
    assert_eq!(server.request_count(FETCH_PATH), 2);
}

fn corrupted_response(path: &str) -> Option<(&'static str, String)> {
    if path == FETCH_PATH {
        Some(("200 OK", "x".repeat(blob().len())))
    } else {
        response(path)
    }
}
fn short_response(path: &str) -> Option<(&'static str, String)> {
    if path == FETCH_PATH {
        Some(("200 OK", blob()[1..].to_owned()))
    } else {
        response(path)
    }
}
fn oversized_response(path: &str) -> Option<(&'static str, String)> {
    if path == FETCH_PATH {
        Some(("200 OK", blob().repeat(2)))
    } else {
        response(path)
    }
}
fn missing_response(path: &str) -> Option<(&'static str, String)> {
    if path == FETCH_PATH {
        Some((
            "404 Not Found",
            r#"{"reason":"external_payload_not_found"}"#.to_owned(),
        ))
    } else {
        response(path)
    }
}
fn unavailable_response(path: &str) -> Option<(&'static str, String)> {
    if path == FETCH_PATH {
        Some((
            "503 Service Unavailable",
            r#"{"reason":"external_payload_unavailable"}"#.to_owned(),
        ))
    } else {
        response(path)
    }
}
fn redirect_response(path: &str) -> Option<(&'static str, String)> {
    if path == FETCH_PATH {
        Some(("302 Found\r\nlocation: /must-not-follow", String::new()))
    } else {
        response(path)
    }
}

#[tokio::test]
async fn runtime_external_fetch_failures_propagate_without_following_redirects() {
    type Response = fn(&str) -> Option<(&'static str, String)>;
    for (handler, reason) in [
        (
            corrupted_response as Response,
            "external_payload_integrity_mismatch",
        ),
        (short_response, "external_payload_integrity_mismatch"),
        (oversized_response, "external_payload_oversized"),
        (missing_response, "external_payload_not_found"),
        (unavailable_response, "external_payload_unavailable"),
        (redirect_response, "http 302"),
    ] {
        let server = MockWorkerServer::start_with_behavior(MockWorkerBehavior {
            response_override: Some(handler),
            ..MockWorkerBehavior::default()
        });
        let client = Client::new(server.base_url()).unwrap();
        let error = client
            .poll_workflow_task("worker", "queue", Duration::from_secs(1))
            .await
            .unwrap_err();
        assert!(error.to_string().contains(reason), "{error}");
        assert_eq!(server.request_count(FETCH_PATH), 1);
        assert_eq!(server.request_count("/must-not-follow"), 0);
    }
}

#[tokio::test]
async fn runtime_external_protocol_paths_resolve_without_rewriting_user_data() {
    let server = MockWorkerServer::start_with_behavior(MockWorkerBehavior {
        response_override: Some(response),
        ..MockWorkerBehavior::default()
    });
    let client = Client::new(server.base_url()).unwrap();
    let history = json!([{"event_type":"ActivityCompleted", "payload":{
        "arguments":envelope(), "result":envelope(), "output":envelope(),
        "activity":{"arguments":envelope(), "result":envelope()},
        "command":{"payload":envelope()}, "exception":{"details":envelope()},
        "metadata":envelope()
    }}]);
    let export = json!({"history_events":history, "activities":[{"arguments":envelope(), "result":envelope()}],
        "payloads":{"arguments":{"data":envelope()}, "output":{"data":envelope()}},
        "commands":[{"payload":envelope()}], "signals":[{"arguments":envelope()}],
        "timeline":[{"command":{"payload":envelope()}}], "updates":[{"arguments":envelope(), "result":envelope()}]});
    let worker = RequestProtocol::Worker(WORKER_PROTOCOL_VERSION);
    let control = RequestProtocol::ControlPlane;
    for (path, protocol, mut value, pointers) in [
        (
            "/worker/activity-tasks/poll",
            worker,
            json!({"task":{"arguments":envelope()}}),
            vec!["/task/arguments"],
        ),
        (
            "/worker/query-tasks/poll",
            worker,
            json!({"task":{"query_arguments":envelope(), "workflow_arguments":envelope(), "history_events":history}}),
            vec![
                "/task/query_arguments",
                "/task/workflow_arguments",
                "/task/history_events/0/payload/result",
            ],
        ),
        (
            "/worker/workflow-tasks/test/history",
            worker,
            json!({"history_events":history}),
            vec!["/history_events/0/payload/activity/result"],
        ),
        (
            "/worker/workflow-tasks/poll",
            worker,
            json!({"task":{"signal_arguments":envelope(), "update_arguments":envelope(), "history_export":export}}),
            vec![
                "/task/signal_arguments",
                "/task/update_arguments",
                "/task/history_export/activities/0/result",
            ],
        ),
        (
            "/workflows/test/history?limit=10",
            control,
            json!({"events":history}),
            vec![
                "/events/0/payload/result",
                "/events/0/payload/exception/details",
            ],
        ),
        (
            "/workflows/test/export",
            control,
            export,
            vec![
                "/payloads/output/data",
                "/timeline/0/command/payload",
                "/updates/0/result",
                "/signals/0/arguments",
            ],
        ),
        (
            "/workflows/test/query/info",
            control,
            json!({"result_envelope":envelope()}),
            vec!["/result_envelope"],
        ),
        (
            "/activities/test",
            control,
            json!({"result":envelope()}),
            vec!["/result"],
        ),
        (
            "/schedules",
            control,
            json!({"schedules":[{"action":{"input":envelope()}}]}),
            vec!["/schedules/0/action/input"],
        ),
    ] {
        value["memo"] = envelope();
        value["output"] = envelope();
        client
            .resolve_runtime_payloads(&mut value, path, protocol)
            .await
            .unwrap();
        for pointer in pointers {
            assert_eq!(
                value.pointer(pointer).unwrap()["blob"],
                blob(),
                "{path}: {pointer}"
            );
        }
        assert_eq!(value["memo"], envelope());
        assert_eq!(value["output"], envelope());
    }
}
