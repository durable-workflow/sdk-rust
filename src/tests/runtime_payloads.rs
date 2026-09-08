use super::*;

const REFERENCE_ID: &str = "ep_00000000000000000000000001";
const FETCH_PATH: &str = "/api/external-payloads/v1/ep_00000000000000000000000001";

fn fixture() -> Value {
    serde_json::from_str(include_str!("../../tests/fixtures/codec-regressions/runtime-external-activity-result.json")).unwrap()
}

fn blob() -> String {
    fixture()["framing"]["wire_base64"].as_str().unwrap().to_owned()
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
        .build().unwrap();
    let task = client.poll_workflow_task("worker", "queue", Duration::from_secs(1))
        .await.unwrap().unwrap();
    validate_workflow_task_payloads(&task).expect("runtime references must be resolved before replay");
    let result = &task.history_events[0].payload["result"];
    assert_eq!(decode_wire_avro_value(result, DEFAULT_CODEC).unwrap(), AvroValue::String(fixture()["value"]["value"].as_str().unwrap().to_owned()));
    assert_eq!(server.request_count(FETCH_PATH), 1, "duplicate nested result shares a response cache");
    assert_eq!(server.authorization_for(FETCH_PATH).as_deref(), Some("Bearer worker-only"));
    assert_eq!(server.namespace_for(FETCH_PATH).as_deref(), Some("external-test"));
    assert_eq!(server.worker_protocol_for(FETCH_PATH).as_deref(), Some(WORKER_PROTOCOL_VERSION));
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
        .build().unwrap();
    let value: Value = client.request_json(reqwest::Method::GET, "/workflows/external", RequestProtocol::ControlPlane, None::<&Value>).await.unwrap();
    assert_eq!(value["output_envelope"]["blob"], blob());
    assert_eq!(value["memo"]["external_payload"], "business data");
    assert_eq!(server.authorization_for(FETCH_PATH).as_deref(), Some("Bearer client-only"));
    assert_eq!(server.control_protocol_for(FETCH_PATH).as_deref(), Some(CONTROL_PLANE_VERSION));
    assert_eq!(server.worker_protocol_for(FETCH_PATH), None);
}
