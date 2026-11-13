use super::runtime_uploads::{policy, reference};
use super::*;

const WORKFLOW: &str = "/worker/workflow-tasks/test/complete";
const CONTINUATION: &str = "/worker/activity-tasks/test/continuation/complete";

fn vectors() -> Value {
    // Server e69df7c37d675681ca301b9eedc2398bc1a3ac18, #1136/#1203.
    serde_json::from_str(include_str!(
        "../../tests/fixtures/workflow-task-completion-limits-v1-vectors.json"
    ))
    .unwrap()
}

fn responses(path: &str, body: &str, _number: usize) -> Option<(&'static str, String)> {
    let profile = path.split('/').nth(1).unwrap_or_default();
    if path.ends_with("/api/cluster/info") {
        let mut info = policy();
        if let Some(id) = profile.strip_prefix("vector-") {
            let vectors = vectors();
            let case = vectors["negotiation_cases"]
                .as_array()
                .unwrap()
                .iter()
                .find(|case| case["id"] == id)
                .unwrap();
            info["limits"] = case["limits"].clone();
            info["namespace"]["external_payload_storage"] = json!({"status":"unconfigured"});
            return Some(("200 OK", info.to_string()));
        }
        let limit = match profile {
            "null" => Value::Null,
            "zero" => json!(0),
            "negative" => json!(-1),
            "fraction" => json!(8192.5),
            "string" => json!("8192"),
            "boolean" => json!(true),
            "list" => json!([8192]),
            "object" => json!({"bytes":8192}),
            "small" => json!(1024),
            _ => json!(8192),
        };
        if profile != "legacy" {
            info["limits"]["max_workflow_task_completion_bytes"] = limit;
        }
        let storage = &mut info["namespace"]["external_payload_storage"];
        storage["threshold_bytes"] = json!(2048);
        match profile {
            "available" | "small" => {}
            "transient" => storage["status"] = json!("unavailable"),
            _ => {
                storage["status"] = json!(match profile {
                    "disabled" => "disabled",
                    "unsupported" => "unsupported",
                    _ => "unconfigured",
                });
                storage["transport"] = Value::Null;
            }
        }
        return Some(("200 OK", info.to_string()));
    }
    if profile == "vector-positive-smaller" {
        return Some((
            "413 Payload Too Large",
            json!({
                "reason":"payload_too_large", "limit_kind":"workflow_task_completion_bytes",
                "limit":1024
            })
            .to_string(),
        ));
    }
    if path.ends_with("/api/external-payloads/v1") {
        return Some((
            "201 Created",
            json!({
                "schema":"durable-workflow.v2.runtime-external-payload-upload.v1",
                "transport_version":1, "reference":reference(body)
            })
            .to_string(),
        ));
    }
    if profile == "refused" && body.contains("schedule_activity") {
        return Some((
            "413 Payload Too Large",
            json!({
                "reason":"payload_too_large", "limit_kind":"workflow_task_completion_bytes",
                "limit":8192, "message":"completion exceeds fixture cap"
            })
            .to_string(),
        ));
    }
    Some((
        "200 OK",
        json!({"outcome":"completed","recorded":true}).to_string(),
    ))
}

#[tokio::test]
async fn completion_limits_consume_shared_negotiation_vectors() {
    let vectors = vectors();
    for case in vectors["negotiation_cases"].as_array().unwrap() {
        let profile = format!("vector-{}", case["id"].as_str().unwrap());
        let (server, client) = setup(&profile);
        let body = batch(3);
        let outcome = send(&client, WORKFLOW, true, &body).await;
        match case["expected"].as_str().unwrap() {
            "ordinary_fallback" => {
                outcome.unwrap();
            }
            "completion_cap"
                if body.to_string().len() as u64 <= case["completion_cap"].as_u64().unwrap() =>
            {
                outcome.unwrap();
            }
            "completion_cap" => {
                let Error::WorkflowTaskCompletionTooLarge(refusal) = outcome.unwrap_err() else {
                    panic!("{profile}");
                };
                assert_eq!(refusal.limit, case["completion_cap"].as_u64());
                assert_eq!(refusal.size, body.to_string().len() as u64);
            }
            "refuse_completion_planning_before_upload_or_submission" => {
                assert!(outcome
                    .unwrap_err()
                    .to_string()
                    .contains("invalid workflow completion request limit"));
                assert_eq!(
                    server.captured_paths(),
                    [format!("/{profile}/api/cluster/info")]
                );
            }
            expected => panic!("unknown vector outcome {expected}"),
        }
        assert_eq!(
            server.request_count(&format!("/{profile}/api/external-payloads/v1")),
            0
        );
    }
}

#[tokio::test]
async fn completion_limits_leave_unsupported_raw_continuation_offload_unchanged() {
    let (server, client) = setup("available");
    let body = json!({"result":batch(1)["commands"][0]["arguments"],
        "continuation":{"commands":batch(8)["commands"]}});
    send(&client, CONTINUATION, true, &body).await.unwrap();
    assert_eq!(
        server.captured_paths(),
        [format!("/available/api{CONTINUATION}")]
    );
    assert_eq!(
        server.request_body(&format!("/available/api{CONTINUATION}")),
        body
    );
}

fn setup(profile: &str) -> (MockWorkerServer, Client) {
    let server = MockWorkerServer::start_with_behavior(MockWorkerBehavior {
        request_override: Some(responses),
        ..MockWorkerBehavior::default()
    });
    let client = Client::new(format!("{}/{profile}", server.base_url())).unwrap();
    (server, client)
}

fn batch(count: usize) -> Value {
    let arguments = encode_value_envelope(&json!(["x".repeat(1000)]), DEFAULT_CODEC).unwrap();
    json!({"lease_owner":"worker", "workflow_task_attempt":2, "commands":(0..count).map(|index| {
        json!({"type":"schedule_activity", "activity_type":"fixture.bytes", "activity_id":index.to_string(),
            "arguments":arguments})
    }).collect::<Vec<_>>()})
}

async fn send(client: &Client, path: &str, worker: bool, body: &Value) -> Result<Value> {
    client
        .request_json(
            reqwest::Method::POST,
            path,
            if worker {
                RequestProtocol::Worker(WORKER_PROTOCOL_VERSION)
            } else {
                RequestProtocol::ControlPlane
            },
            Some(body),
        )
        .await
}

#[tokio::test]
async fn completion_limits_preserve_inline_atomic_batches_without_storage() {
    let body = batch(3);
    assert!(body.to_string().len() > 2048 && body.to_string().len() < 8192);
    for profile in ["unconfigured", "disabled", "unsupported"] {
        let path = WORKFLOW;
        let (server, client) = setup(profile);
        send(&client, path, true, &body).await.unwrap();
        assert_eq!(
            server.request_bodies(&format!("/{profile}/api{path}")),
            [body.clone()]
        );
        assert_eq!(
            server.request_count(&format!("/{profile}/api/external-payloads/v1")),
            0
        );
    }
}

#[tokio::test]
async fn completion_limits_preserve_old_producer_and_individual_value_limits() {
    let (server, client) = setup("legacy");
    let error = send(&client, WORKFLOW, true, &batch(3)).await.unwrap_err();
    assert!(
        error.to_string().contains("external_payload_unavailable"),
        "{error}"
    );
    assert_eq!(server.captured_paths(), ["/legacy/api/cluster/info"]);

    let (server, client) = setup("unconfigured");
    let oversized = encode_value_envelope(&json!("x".repeat(3000)), DEFAULT_CODEC).unwrap();
    let body = json!({"commands":[{"type":"schedule_activity","arguments":oversized}]});
    let error = send(&client, WORKFLOW, true, &body).await.unwrap_err();
    assert!(
        error.to_string().contains("external_payload_oversized"),
        "{error}"
    );
    assert_eq!(server.captured_paths(), ["/unconfigured/api/cluster/info"]);
}

#[tokio::test]
async fn completion_limits_validate_explicit_values_only_on_completion_routes() {
    for profile in [
        "null", "zero", "negative", "fraction", "string", "boolean", "list", "object",
    ] {
        let path = WORKFLOW;
        let (server, client) = setup(profile);
        let error = send(&client, path, true, &batch(3)).await.unwrap_err();
        assert!(
            error.to_string().contains(
                "external_payload_unsupported: invalid workflow completion request limit"
            ),
            "{profile} {path}: {error}"
        );
        assert_eq!(
            server.captured_paths(),
            [format!("/{profile}/api/cluster/info")]
        );
        let (server, client) = setup(profile);
        let body = json!({"input":encode_value_envelope(&Value::Null, DEFAULT_CODEC).unwrap()});
        send(&client, "/workflows", false, &body).await.unwrap();
        assert_eq!(
            server.request_body(&format!("/{profile}/api/workflows")),
            body
        );
    }
}

#[tokio::test]
async fn completion_limits_do_not_enlarge_ordinary_or_failure_requests() {
    for (path, worker, field) in [
        ("/workflows", false, "input"),
        ("/worker/activity-tasks/test/complete", true, "result"),
        ("/worker/query-tasks/test/complete", true, "result_envelope"),
        ("/worker/activity-tasks/test/fail", true, "failure"),
    ] {
        let (server, client) = setup("unconfigured");
        let envelope = encode_value_envelope(&Value::Null, DEFAULT_CODEC).unwrap();
        let value = if field == "failure" {
            json!({"details":envelope})
        } else {
            envelope
        };
        let body = json!({field:value, "metadata":"x".repeat(3000)});
        assert!(send(&client, path, worker, &body).await.is_err(), "{path}");
        assert_eq!(server.captured_paths(), ["/unconfigured/api/cluster/info"]);
        assert_eq!(
            client
                .known_failure_request_limit(RequestProtocol::DefaultWorker)
                .unwrap(),
            2048
        );
    }
}

#[tokio::test]
async fn completion_limits_use_explicit_smaller_caps_and_plan_uploads_before_effects() {
    let path = WORKFLOW;
    let (server, client) = setup("available");
    send(&client, path, true, &batch(3)).await.unwrap();
    assert_eq!(
        server.request_count("/available/api/external-payloads/v1"),
        0
    );
    send(&client, path, true, &batch(8)).await.unwrap();
    assert!(server.request_count("/available/api/external-payloads/v1") > 0);
    assert!(
        server.request_bodies(&format!("/available/api{path}"))[1]
            .to_string()
            .len()
            <= 8192
    );

    let (server, client) = setup("small");
    send(&client, path, true, &batch(1)).await.unwrap();
    assert_eq!(server.request_count("/small/api/external-payloads/v1"), 1);
    assert!(
        server
            .request_body(&format!("/small/api{path}"))
            .to_string()
            .len()
            <= 1024
    );
    let body = json!({"commands":batch(3)["commands"], "metadata":"x".repeat(1500)});
    let before = server.request_count("/small/api/external-payloads/v1");
    assert!(send(&client, path, true, &body).await.is_err());
    assert_eq!(
        server.request_count("/small/api/external-payloads/v1"),
        before
    );
}

#[tokio::test]
async fn completion_limits_preserve_configured_storage_refusals() {
    let (server, client) = setup("transient");
    let error = send(&client, WORKFLOW, true, &batch(8)).await.unwrap_err();
    assert!(
        error.to_string().contains("external_payload_unavailable"),
        "{error}"
    );
    assert_eq!(server.captured_paths(), ["/transient/api/cluster/info"]);
}

#[tokio::test]
async fn completion_limits_keep_authoritative_413_and_settle_without_retrying_the_batch() {
    let (server, client) = setup("refused");
    let body = batch(8);
    assert!(body.to_string().len() > 8192);
    let error = send(&client, WORKFLOW, true, &body).await.unwrap_err();
    let Error::WorkflowTaskCompletionTooLarge(refusal) = error else {
        panic!("{error:?}");
    };
    assert_eq!(refusal.status, 413);
    assert_eq!(refusal.limit, Some(8192));
    assert_eq!(refusal.size, body.to_string().len() as u64);
    assert_eq!(server.request_count("/refused/api/external-payloads/v1"), 0);

    let (server, client) = setup("refused");
    let worker = Worker::new(client, "fixture-queue");
    let commands = body["commands"].as_array().unwrap().clone();
    let outcome = worker
        .settle_workflow_task_decision(
            "test",
            "worker",
            2,
            Some("run"),
            Ok(WorkflowTaskDecision::without_message_streams(commands)),
            false,
            None,
        )
        .await
        .unwrap();
    assert!(matches!(outcome, ManagedPollOutcome::Handled));
    let sent = server.request_bodies("/refused/api/worker/workflow-tasks/test/complete");
    assert_eq!(sent.len(), 2);
    assert_eq!(sent[0]["commands"], body["commands"]);
    assert_eq!(sent[1]["commands"].as_array().unwrap().len(), 1);
    assert_eq!(sent[1]["commands"][0]["type"], "fail_workflow");
    assert_eq!(sent[1]["commands"][0]["non_retryable"], true);
    assert_eq!(sent[1]["lease_owner"], "worker");
    assert_eq!(sent[1]["workflow_task_attempt"], 2);
    assert_eq!(
        server.request_count("/refused/api/worker/workflow-tasks/test/fail"),
        0
    );
}
