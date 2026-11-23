use super::*;

const TASK: &str = "continue-target-task";
const WORKER: &str = "target-worker";
const QUEUE: &str = "target-queue";
const WORKFLOW: &str = "continue.target";
const COMPLETE: &str = "/api/worker/workflow-tasks/continue-target-task/complete";
const FAIL: &str = "/api/worker/workflow-tasks/continue-target-task/fail";

fn respond(path: &str, body: &str, number: usize) -> Option<(&'static str, String)> {
    if path.ends_with("/worker/register") {
        let request: Value = serde_json::from_str(body).unwrap();
        let mut ack = json!({
            "registered": true,
            "worker_id": request["worker_id"],
            "namespace": "default",
            "task_queue": request["task_queue"],
            "protocol_version": "1.21",
            "server_capabilities": {"continue_as_new_target_build": true},
        });
        if path.starts_with("/legacy/") || number > 1 {
            ack["protocol_version"] = json!("1.20");
        }
        if path.starts_with("/wrong-queue/") {
            ack["task_queue"] = json!("another-queue");
        }
        if path.starts_with("/missing-capability/") {
            ack["server_capabilities"] = json!({});
        }
        if path.starts_with("/wrong-capability-type/") {
            ack["server_capabilities"]["continue_as_new_target_build"] = json!("true");
        }
        if path.starts_with("/wrong-major/") {
            ack["protocol_version"] = json!("2.21");
        }
        return Some(("201 Created", ack.to_string()));
    }
    if path.ends_with("/worker/workflow-tasks/poll") && number <= 2 {
        let task = json!({
            "task_id": TASK,
            "workflow_id": "instance",
            "run_id": "run",
            "workflow_type": WORKFLOW,
            "payload_codec": DEFAULT_CODEC,
            "arguments": encode_value_envelope(&json!([]), DEFAULT_CODEC).unwrap(),
            "history_events": [],
            "workflow_task_attempt": number + 2,
            "lease_owner": WORKER,
        });
        return Some(("200 OK", json!({"task": task}).to_string()));
    }
    if path.ends_with("/worker/activity-tasks/poll") {
        return Some(("200 OK", json!({"task": null}).to_string()));
    }
    if path.ends_with(COMPLETE) {
        if (path.starts_with("/refuse/") || path.starts_with("/ambiguous/")) && number == 1 {
            return Some((
                "409 Conflict",
                json!({"reason":"promoted_build_unavailable",
                    "retryable": true, "request_admitted": path.starts_with("/ambiguous/")})
                .to_string(),
            ));
        }
        return Some(("200 OK", json!({"recorded":true}).to_string()));
    }
    if path.ends_with(FAIL) {
        return Some(("200 OK", json!({"recorded":true}).to_string()));
    }
    if path.ends_with("/api/worker/registrations/target-worker") {
        return Some((
            "200 OK",
            json!({"worker_id":WORKER,"outcome":"deregistered","recovered_workflow_task_count":0})
                .to_string(),
        ));
    }
    if path == "/upload/api/cluster/info" {
        return Some(("200 OK", runtime_uploads::policy().to_string()));
    }
    if path.ends_with("/api/cluster/info") {
        return Some((
            "200 OK",
            json!({"limits":{"max_payload_bytes":1048576}}).to_string(),
        ));
    }
    if path == "/upload/api/external-payloads/v1" {
        return Some((
            "201 Created",
            json!({"schema":"durable-workflow.v2.runtime-external-payload-upload.v1", "transport_version":1,
                "reference": runtime_uploads::reference(body)}).to_string(),
        ));
    }
    None
}

fn client(case: &str) -> (MockWorkerServer, Client) {
    let server = MockWorkerServer::start_with_behavior(MockWorkerBehavior {
        request_override: Some(respond),
        ..MockWorkerBehavior::default()
    });
    let client = Client::new(format!("{}/{case}", server.base_url())).unwrap();
    (server, client)
}

fn worker(client: Client, target: Option<ContinueAsNewTarget>) -> Worker {
    let mut worker = Worker::new(client, QUEUE).worker_id(WORKER);
    worker.register_workflow(WORKFLOW, move |ctx, _| async move {
        match target {
            Some(target) => ctx.continue_as_new_with_target(
                ContinueAsNewOptions::new().task_queue("successor-queue"),
                target,
                json!(["next state"]),
            ),
            None => ctx.continue_as_new(json!(["next state"])),
        }
    });
    worker
}

#[tokio::test]
async fn continue_as_new_promoted_refusal_reports_once_and_worker_handles_next_attempt() {
    let (server, client) = client("refuse");
    let worker = worker(client, Some(ContinueAsNewTarget::Promoted));
    worker.register().await.unwrap();
    assert_eq!(worker.run_once().await.unwrap(), 1);
    assert_eq!(worker.run_once().await.unwrap(), 1);

    let complete = format!("/refuse{COMPLETE}");
    let fail = format!("/refuse{FAIL}");
    let reports = server.request_bodies(&complete);
    assert_eq!(reports.len(), 2);
    assert_eq!(reports[0]["workflow_task_attempt"], 3);
    assert_eq!(reports[1]["workflow_task_attempt"], 4);
    assert_eq!(reports[0]["commands"][0]["target_build"], "promoted");
    assert_eq!(reports[0]["commands"][0]["queue"], "successor-queue");
    assert_eq!(server.request_count(&fail), 1);
    let failure = server.request_body(&fail);
    assert_eq!(failure["workflow_task_attempt"], 3);
    assert_eq!(failure["lease_owner"], WORKER);
    assert_eq!(failure["failure"]["reason"], "promoted_build_unavailable");
    assert_eq!(
        server.worker_protocol_for(&complete).as_deref(),
        Some("1.21")
    );
    assert_eq!(server.worker_protocol_for(&fail).as_deref(), Some("1.21"));
    for baseline in [
        "/refuse/api/worker/register",
        "/refuse/api/worker/workflow-tasks/poll",
        "/refuse/api/worker/activity-tasks/poll",
    ] {
        assert_eq!(
            server.worker_protocol_for(baseline).as_deref(),
            Some("1.20")
        );
    }
}

#[tokio::test]
async fn continue_as_new_ambiguous_conflict_has_no_failure_report() {
    let (server, client) = client("ambiguous");
    let worker = worker(client, Some(ContinueAsNewTarget::Promoted));
    worker.register().await.unwrap();
    assert!(
        matches!(worker.run_once().await, Err(Error::Http { status, .. })
        if status == reqwest::StatusCode::CONFLICT)
    );
    assert_eq!(server.request_count(&format!("/ambiguous{COMPLETE}")), 1);
    assert_eq!(server.request_count(&format!("/ambiguous{FAIL}")), 0);
}

#[tokio::test]
async fn continue_as_new_requires_a_matching_registration_for_each_explicit_target() {
    for case in [
        "legacy",
        "wrong-queue",
        "missing-capability",
        "wrong-capability-type",
        "wrong-major",
    ] {
        let (server, client) = client(case);
        let worker = worker(client, Some(ContinueAsNewTarget::Inherited));
        worker.register().await.unwrap();
        assert!(matches!(
            worker.run_once().await,
            Err(Error::ContinueAsNewTargetUnsupported)
        ));
        assert_eq!(server.request_count(&format!("/{case}{COMPLETE}")), 0);
        assert_eq!(server.request_count(&format!("/{case}{FAIL}")), 0);
    }
}

#[tokio::test]
async fn continue_as_new_direct_reports_cannot_reuse_a_deregistered_or_replaced_announcement() {
    let (server, client) = client("replace");
    let worker = worker(client.clone(), None);
    worker.register().await.unwrap();
    let promoted = json!({"type":"continue_as_new", "target_build":"promoted",
        "arguments":encode_value_envelope(&json!([]), DEFAULT_CODEC).unwrap()});
    assert!(matches!(
        client
            .complete_workflow_task(TASK, "another-worker", 1, vec![promoted.clone()])
            .await,
        Err(Error::ContinueAsNewTargetUnsupported)
    ));
    for invalid in [
        Value::Null,
        json!(true),
        json!(0),
        json!([]),
        json!({}),
        json!("Promoted"),
    ] {
        let mut command = promoted.clone();
        command["target_build"] = invalid;
        assert!(matches!(
            client
                .complete_workflow_task(TASK, WORKER, 1, vec![command])
                .await,
            Err(Error::InvalidContinueAsNewTarget)
        ));
    }
    assert!(client.worker_supports_continue_as_new_target(WORKER));
    client.deregister_worker_registration(WORKER).await.unwrap();
    assert!(!client.worker_supports_continue_as_new_target(WORKER));
    worker.register().await.unwrap();
    assert!(matches!(
        client
            .complete_workflow_task(TASK, WORKER, 1, vec![promoted])
            .await,
        Err(Error::ContinueAsNewTargetUnsupported)
    ));
    assert!(server
        .captured_paths()
        .iter()
        .all(|path| !path.ends_with(COMPLETE)));
}

#[tokio::test]
async fn continue_as_new_default_omits_target_in_both_protocols() {
    for (case, protocol) in [("legacy", "1.20"), ("supported", "1.21")] {
        let (server, client) = client(case);
        let worker = worker(client, None);
        worker.register().await.unwrap();
        assert_eq!(worker.run_once().await.unwrap(), 1);
        let path = format!("/{case}{COMPLETE}");
        assert_eq!(server.worker_protocol_for(&path).as_deref(), Some(protocol));
        assert!(server.request_body(&path)["commands"][0]
            .get("target_build")
            .is_none());
    }
}

#[tokio::test]
async fn continue_as_new_promoted_keeps_payload_upload_on_default_protocol() {
    let (server, client) = client("upload");
    worker(client.clone(), None).register().await.unwrap();
    client
        .complete_workflow_task(TASK, WORKER, 1, vec![json!({
            "type":"continue_as_new","target_build":"promoted",
            "arguments":encode_value_envelope(&json!(["x".repeat(256)]), DEFAULT_CODEC).unwrap(),
        })])
        .await
        .unwrap();
    assert_eq!(
        server
            .worker_protocol_for("/upload/api/external-payloads/v1")
            .as_deref(),
        Some("1.20")
    );
    assert_eq!(
        server
            .worker_protocol_for(&format!("/upload{COMPLETE}"))
            .as_deref(),
        Some("1.21")
    );
}

#[test]
fn continue_as_new_cold_replay_compares_the_recorded_target() {
    let client = Client::new("http://127.0.0.1:1").unwrap();
    let recorded = workflow_task(
        WORKFLOW,
        vec![history_event(
            "WorkflowContinuedAsNew",
            json!({"sequence": 1, "continued_to_run_id": "next", "build_transition": {
                "from_compatibility":null, "to_compatibility":"build-b", "resolution":"promoted",
            }}),
        )],
        DEFAULT_CODEC,
    );
    assert!(worker(client.clone(), Some(ContinueAsNewTarget::Promoted))
        .execute_workflow_task(recorded.clone())
        .unwrap()
        .is_empty());
    let error = worker(client, None)
        .execute_workflow_task(recorded)
        .unwrap_err();
    assert!(
        matches!(error, Error::NonDeterministicReplay(failure) if failure.reason == "continue_as_new_target_mismatch")
    );
}

#[test]
fn continue_as_new_offline_history_replay_completes_without_a_new_command() {
    let client = Client::new("http://127.0.0.1:1").unwrap();
    for (target, transition) in [
        (None, None),
        (
            Some(ContinueAsNewTarget::Promoted),
            Some(json!({
                "from_compatibility": "build-a", "to_compatibility": "build-b", "resolution": "promoted"
            })),
        ),
    ] {
        let mut payload = json!({"sequence":1, "continued_to_run_id":"next"});
        if let Some(transition) = transition {
            payload["build_transition"] = transition;
        }
        let history = testing::WorkflowHistory::from_history(
            WORKFLOW,
            json!([]),
            json!([{"event_type":"WorkflowContinuedAsNew", "payload":payload}]),
        )
        .unwrap();
        let report = testing::WorkflowEnvironment::from_worker(worker(client.clone(), target))
            .replay(&history)
            .unwrap();
        assert!(report.completed);
        assert!(report.commands.is_empty());
    }
}
