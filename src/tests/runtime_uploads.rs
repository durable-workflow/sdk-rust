use super::*;

const UPLOAD: &str = "/api/external-payloads/v1";
const DISCOVERY: &str = "/api/cluster/info";
const COMPLETE: &str = "/worker/workflow-tasks/test/complete";

fn policy() -> Value {
    json!({"limits":{"max_payload_bytes":2048}, "namespace":{"external_payload_storage":{
        "status":"available", "threshold_bytes":64, "transport":{
            "schema":"durable-workflow.v2.runtime-external-payload-transport.v1", "version":1,
            "reference_schema":crate::runtime_payloads::SCHEMA, "mode":"authenticated_namespace_runtime",
            "upload":{"method":"POST", "path":UPLOAD},
            "fetch":{"method":"GET", "path_template":"/api/external-payloads/v1/{referenceId}"},
            "limits":{"max_payload_bytes":1048576, "request_timeout_seconds":1}
        }
    }}})
}

fn reference(blob: &str) -> Value {
    let hash = format!("{:x}", Sha256::digest(blob.as_bytes()));
    json!({"schema":crate::runtime_payloads::SCHEMA, "codec":"avro",
        "reference_id":format!("ep_{}", hash[..26].to_uppercase()),
        "size_bytes":blob.len(), "sha256":hash})
}

fn payload() -> Value {
    encode_value_envelope(&json!("a".repeat(128)), DEFAULT_CODEC).unwrap()
}

fn responses(path: &str, blob: &str, number: usize) -> Option<(&'static str, String)> {
    if path.ends_with(DISCOVERY) {
        let mut value = policy();
        let storage = &mut value["namespace"]["external_payload_storage"];
        match path.split('/').nth(1).unwrap_or("") {
            "unavailable" => storage["status"] = json!("unavailable"),
            "aggregate" => storage["threshold_bytes"] = json!(1048576),
            "boundary" => {
                storage["threshold_bytes"] = json!(payload()["blob"].as_str().unwrap().len())
            }
            "schema" => storage["transport"]["schema"] = json!("invalid"),
            "version" => storage["transport"]["version"] = json!(2),
            "threshold" => storage["threshold_bytes"] = json!(0),
            "limit" => storage["transport"]["limits"]["max_payload_bytes"] = json!(2),
            "timeout" => storage["transport"]["limits"]["request_timeout_seconds"] = json!(0),
            "upload-uri" => {
                storage["transport"]["upload"]["path"] = json!("https://elsewhere.invalid/steal")
            }
            "fetch-uri" => {
                storage["transport"]["fetch"]["path_template"] =
                    json!("https://elsewhere.invalid/{referenceId}")
            }
            "request-limit" => value["limits"]["max_payload_bytes"] = json!(-1),
            _ => {}
        }
        return Some(("200 OK", value.to_string()));
    }
    if path.ends_with(UPLOAD) {
        if path.starts_with("/pressure/") && number <= 2 {
            return Some((
                "503 Service Unavailable",
                storage_refusal(None, false, false).to_string(),
            ));
        }
        if path.starts_with("/stopped/") {
            return Some((
                "503 Service Unavailable",
                storage_refusal(None, false, false).to_string(),
            ));
        }
        let mut value = json!({"schema":"durable-workflow.v2.runtime-external-payload-upload.v1",
            "transport_version":1, "reference":reference(blob)});
        match path.split('/').nth(1).unwrap_or("") {
            "bad-sha" => value["reference"]["sha256"] = json!("0".repeat(64)),
            "bad-size" => value["reference"]["size_bytes"] = json!(1),
            "bad-codec" => value["reference"]["codec"] = json!("json"),
            "bad-id" => value["reference"]["reference_id"] = json!("../other"),
            "bad-extra" => value["reference"]["extra"] = json!(true),
            "bad-response" => value["transport_version"] = json!(2),
            "bad-json" => return Some(("200 OK", "broken".into())),
            "huge-response" => return Some(("200 OK", "x".repeat(65537))),
            "unauthorized" => return Some(("403 Forbidden", "access denied".into())),
            "missing" => {
                return Some((
                    "404 Not Found",
                    r#"{"reason":"external_payload_not_found"}"#.into(),
                ))
            }
            "slow" => thread::sleep(Duration::from_secs(2)),
            _ => {}
        }
        return Some(("201 Created", value.to_string()));
    }
    Some(("200 OK", "{}".into()))
}

fn server() -> MockWorkerServer {
    MockWorkerServer::start_with_behavior(MockWorkerBehavior {
        request_override: Some(responses),
        ..MockWorkerBehavior::default()
    })
}

async fn send(client: &Client, path: &str, worker: bool, body: Value) -> Result<Value> {
    client
        .request_json(
            reqwest::Method::POST,
            path,
            if worker {
                RequestProtocol::Worker(WORKER_PROTOCOL_VERSION)
            } else {
                RequestProtocol::ControlPlane
            },
            Some(&body),
        )
        .await
}

#[tokio::test]
async fn runtime_upload_preserves_encoded_types_role_headers_and_base_path() {
    let server = server();
    let client = Client::builder(format!("{}/runtime", server.base_url()))
        .worker_token(Some("worker-only".into()))
        .namespace("tenant-a")
        .build()
        .unwrap();
    let typed = AvroValue::Map(BTreeMap::from([
        ("long".into(), AvroValue::Long(7)),
        ("double".into(), AvroValue::Double(7.0)),
        ("zero".into(), AvroValue::Double(-0.0)),
        ("bytes".into(), AvroValue::Bytes(vec![0, 255, 1])),
        ("text".into(), AvroValue::String("a".repeat(128))),
    ]));
    let envelope = encode_typed_envelope(&typed, DEFAULT_CODEC).unwrap();
    send(
        &client,
        COMPLETE,
        true,
        json!({"commands":[{"type":"complete_workflow", "result":envelope}]}),
    )
    .await
    .unwrap();
    let requests = server.requests.lock().unwrap();
    let upload = requests
        .iter()
        .find(|request| request.path.ends_with(UPLOAD))
        .unwrap();
    assert_eq!(upload.body, envelope["blob"].as_str().unwrap());
    assert_eq!(decode_avro_value_blob(&upload.body).unwrap(), typed);
    let headers = upload.headers.to_lowercase();
    assert!(headers.contains("content-type: application/octet-stream"));
    assert!(headers.contains("x-durable-workflow-payload-codec: avro"));
    assert!(headers.contains(&format!(
        "x-durable-workflow-payload-size: {}",
        upload.body.len()
    )));
    assert!(headers.contains(&format!(
        "x-durable-workflow-payload-sha256: {:x}",
        Sha256::digest(upload.body.as_bytes())
    )));
    for request in requests.iter() {
        assert!(request.path.starts_with("/runtime/api/"));
        assert_eq!(request.namespace.as_deref(), Some("tenant-a"));
        assert_eq!(request.authorization.as_deref(), Some("Bearer worker-only"));
    }
    let discovery = &requests[0];
    assert_eq!(
        discovery.control_protocol.as_deref(),
        Some(CONTROL_PLANE_VERSION)
    );
    assert!(discovery.worker_protocol.is_none());
    assert_eq!(
        upload.worker_protocol.as_deref(),
        Some(WORKER_PROTOCOL_VERSION)
    );
    assert!(upload.control_protocol.is_none());
    let completed: Value = serde_json::from_str(&requests.last().unwrap().body).unwrap();
    assert_eq!(
        completed["commands"][0]["result"]["external_payload"],
        reference(&upload.body)
    );
}

#[tokio::test]
async fn runtime_upload_deduplicates_per_request_but_keeps_role_cache_separate() {
    let server = server();
    let client = Client::builder(server.base_url())
        .control_token(Some("client".into()))
        .worker_token(Some("worker".into()))
        .build()
        .unwrap();
    let body = json!({"commands":[
        {"type":"schedule_activity", "arguments":payload()},
        {"type":"complete_workflow", "result":payload()}
    ]});
    for _ in 0..2 {
        send(&client, COMPLETE, true, body.clone()).await.unwrap();
    }
    assert_eq!(server.request_count(UPLOAD), 2);
    assert_eq!(server.request_count(DISCOVERY), 1);
    assert_identical_requests(&server, UPLOAD, 2);
    assert_identical_requests(&server, &format!("/api{COMPLETE}"), 2);
    send(&client, "/workflows", false, json!({"input":payload()}))
        .await
        .unwrap();
    assert_eq!(server.request_count(DISCOVERY), 2);
    let requests = server.requests.lock().unwrap();
    let roles: Vec<_> = requests
        .iter()
        .filter(|r| r.path == DISCOVERY)
        .map(|r| r.authorization.as_deref())
        .collect();
    assert_eq!(roles, [Some("Bearer worker"), Some("Bearer client")]);
}

#[tokio::test]
async fn runtime_upload_covers_only_protocol_payload_positions() {
    for (path, worker, body, pointer) in [
        (
            "/workflows",
            false,
            json!({"input":payload()}),
            "/input/external_payload",
        ),
        (
            "/activities",
            false,
            json!({"input":payload()}),
            "/input/external_payload",
        ),
        (
            "/workflows/w/signal/s",
            false,
            json!({"input":payload()}),
            "/input/external_payload",
        ),
        (
            "/workflows/w/runs/r/query/q",
            false,
            json!({"input":payload()}),
            "/input/external_payload",
        ),
        (
            "/workflows/w/update/u",
            false,
            json!({"input":payload()}),
            "/input/external_payload",
        ),
        (
            "/workflows/w/message-streams/s/messages",
            false,
            json!({"input":payload()}),
            "/input/external_payload",
        ),
        (
            "/schedules/s",
            false,
            json!({"action":{"input":payload()}}),
            "/action/input/external_payload",
        ),
        (
            "/service-endpoints/e/services/s/operations/o/execute",
            false,
            json!({"arguments":payload()}),
            "/arguments/external_payload",
        ),
        (
            "/worker/activity-tasks/t/complete",
            true,
            json!({"result":payload()}),
            "/result/external_payload",
        ),
        (
            "/worker/activity-tasks/t/fail",
            true,
            json!({"failure":{"details":payload()}}),
            "/failure/details/external_payload",
        ),
        (
            "/worker/query-tasks/t/complete",
            true,
            json!({"result":"duplicate".repeat(1024),"result_envelope":payload()}),
            "/result_envelope/external_payload",
        ),
        (
            "/workflows/w/runs/r/streams/s/items",
            false,
            json!({"items":[{"payload":payload()["blob"], "payload_codec":"avro"}]}),
            "/items/0/payload_reference",
        ),
    ] {
        let server = server();
        let client = Client::new(server.base_url()).unwrap();
        send(&client, path, worker, body).await.unwrap();
        let actual = server.request_body(&format!("/api{path}"));
        assert_eq!(
            actual.pointer(pointer),
            Some(&reference(payload()["blob"].as_str().unwrap())),
            "{path}"
        );
        if pointer.starts_with("/result_envelope") {
            assert!(actual["result"].is_null());
        }
        if pointer.ends_with("payload_reference") {
            assert!(actual["items"][0].get("payload").is_none());
        }
    }
    for kind in [
        "complete_workflow",
        "complete_update",
        "record_side_effect",
        "schedule_activity",
        "start_child_workflow",
        "continue_as_new",
        "start_service_operation",
        "upsert_memo",
    ] {
        let server = server();
        let client = Client::new(server.base_url()).unwrap();
        let field = workflow_command_payload_field(kind).unwrap();
        send(
            &client,
            COMPLETE,
            true,
            json!({"commands":[{"type":kind, field:payload()}], "memo":{"business":payload()}}),
        )
        .await
        .unwrap();
        let actual = server.request_body(&format!("/api{COMPLETE}"));
        assert!(actual["commands"][0][field]
            .get("external_payload")
            .is_some());
        assert_eq!(actual["memo"]["business"], payload());
    }
}

#[tokio::test]
async fn runtime_upload_plans_aggregate_limits_before_any_effect() {
    let server = server();
    let client = Client::new(format!("{}/aggregate", server.base_url())).unwrap();
    let envelope = encode_value_envelope(&json!("x".repeat(900)), DEFAULT_CODEC).unwrap();
    send(&client, COMPLETE, true, json!({"commands":[
        {"type":"schedule_activity", "arguments":envelope}, {"type":"complete_workflow", "result":envelope}
    ]})).await.unwrap();
    assert_eq!(server.request_count(&format!("/aggregate{UPLOAD}")), 1);
    let requests = server.requests.lock().unwrap();
    assert!(requests.last().unwrap().body.len() <= 2048);
    drop(requests);
    for body in [
        json!({"input":payload(), "metadata":"x".repeat(3000)}),
        json!({"input":{"codec":"avro","blob":"x".repeat(1048577)}}),
    ] {
        let before = server.request_count(&format!("/aggregate{UPLOAD}"));
        assert!(send(&client, "/workflows", false, body).await.is_err());
        assert_eq!(server.request_count(&format!("/aggregate{UPLOAD}")), before);
    }
}

#[tokio::test]
async fn runtime_upload_obeys_threshold_and_unavailable_storage() {
    for prefix in ["boundary", "unavailable"] {
        let server = server();
        let client = Client::new(format!("{}/{prefix}", server.base_url())).unwrap();
        let small = if prefix == "boundary" {
            payload()
        } else {
            encode_value_envelope(&Value::Null, DEFAULT_CODEC).unwrap()
        };
        send(&client, "/workflows", false, json!({"input":small}))
            .await
            .unwrap();
        assert_eq!(server.request_count(&format!("/{prefix}{UPLOAD}")), 0);
        if prefix == "unavailable" {
            let error = send(&client, "/workflows", false, json!({"input":payload()}))
                .await
                .unwrap_err();
            assert!(error.to_string().contains("external_payload_unavailable"));
        }
    }
}

#[tokio::test]
async fn runtime_upload_rejects_invalid_discovery_before_upload() {
    for prefix in [
        "schema",
        "version",
        "threshold",
        "limit",
        "timeout",
        "upload-uri",
        "fetch-uri",
        "request-limit",
    ] {
        let server = server();
        let client = Client::new(format!("{}/{prefix}", server.base_url())).unwrap();
        let error = send(&client, "/workflows", false, json!({"input":payload()}))
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("external_payload_unsupported"),
            "{prefix}: {error}"
        );
        assert_eq!(server.captured_paths(), [format!("/{prefix}{DISCOVERY}")]);
    }
}

#[tokio::test]
async fn runtime_upload_rejects_corrupt_responses_and_preserves_http_errors() {
    for (prefix, reason) in [
        ("bad-sha", "external_payload_integrity_mismatch"),
        ("bad-size", "external_payload_integrity_mismatch"),
        ("bad-codec", "external_payload_unsupported"),
        ("bad-id", "external_payload_unsupported"),
        ("bad-extra", "external_payload_unsupported"),
        ("bad-response", "external_payload_unsupported"),
        ("bad-json", "external_payload_unsupported"),
        ("huge-response", "external_payload_unsupported"),
        ("unauthorized", "http 403"),
        ("missing", "external_payload_not_found"),
        ("slow", "transport error"),
    ] {
        let server = server();
        let client = Client::new(format!("{}/{prefix}", server.base_url())).unwrap();
        let error = send(&client, "/workflows", false, json!({"input":payload()}))
            .await
            .unwrap_err();
        assert!(error.to_string().contains(reason), "{prefix}: {error}");
        assert_eq!(server.request_count(&format!("/{prefix}/api/workflows")), 0);
    }
}

#[tokio::test]
async fn runtime_upload_storage_admission_reuses_identical_bytes_and_shutdown_interrupts() {
    for prefix in ["pressure", "stopped"] {
        let server = server();
        let mut client = Client::new(format!("{}/{prefix}", server.base_url())).unwrap();
        let stop = Arc::new(AtomicBool::new(prefix == "stopped"));
        client.worker_storage_admission = Some(WorkerStorageAdmission {
            stop,
            policy: WorkerRetryPolicy {
                max_backoff: Duration::from_millis(1),
                ..WorkerRetryPolicy::default()
            },
        });
        let outcome = send(
            &client,
            COMPLETE,
            true,
            json!({"commands":[{"type":"complete_workflow","result":payload()}]}),
        )
        .await;
        if prefix == "pressure" {
            outcome.unwrap();
            assert_identical_requests(&server, &format!("/{prefix}{UPLOAD}"), 3);
            assert_eq!(server.request_count(&format!("/{prefix}/api{COMPLETE}")), 1);
        } else {
            assert!(worker_storage_admission_body(&outcome.unwrap_err()).is_some());
            assert_eq!(server.request_count(&format!("/{prefix}{UPLOAD}")), 1);
            assert_eq!(server.request_count(&format!("/{prefix}/api{COMPLETE}")), 0);
        }
    }
}

#[tokio::test]
async fn runtime_upload_low_level_references_are_strict_and_never_reuploaded() {
    let server = server();
    let client = Client::new(server.base_url()).unwrap();
    let envelope =
        json!({"codec":"avro","external_payload":reference(payload()["blob"].as_str().unwrap())});
    client
        .complete_workflow_task(
            "test",
            "worker",
            1,
            vec![json!({"type":"complete_workflow","result":envelope})],
        )
        .await
        .unwrap();
    assert_eq!(server.request_count(UPLOAD), 0);
    assert_eq!(server.request_count(DISCOVERY), 0);
    for (path, bad) in [
        ("/codec", json!("json")),
        ("/external_payload/reference_id", json!("../../other")),
        ("/external_payload/size_bytes", json!(-1)),
    ] {
        let mut value = envelope.clone();
        *value.pointer_mut(path).unwrap() = bad;
        assert!(client
            .complete_workflow_task(
                "test",
                "worker",
                1,
                vec![json!({"type":"complete_workflow","result":value})]
            )
            .await
            .is_err());
    }
    assert_eq!(server.request_count(&format!("/api{COMPLETE}")), 1);
}
