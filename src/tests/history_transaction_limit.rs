//! durable-workflow/.github#228: native Server 3.13.0 refuses a workflow-task
//! completion whose history transaction would exceed its budget with a typed
//! 409. The worker fails the run once with a structural-limit failure instead
//! of retrying the task into the same refusal.
use super::*;

const LIMIT: u64 = 65_536;
const MEASURED: u64 = 214_380;

/// Native Server 3.13.0's refusal (`history_meter.rs`), field for field.
fn typed_409(task: &str) -> Value {
    json!({"task_id":task,"workflow_task_attempt":2,"outcome":"completed","recorded":false,
        "run_id":null,"run_status":null,"created_task_ids":[],
        "reason":"history_transaction_limit_exceeded","limit_kind":"history_transaction_bytes",
        "current_value":MEASURED,"configured_limit":LIMIT,
        "message":format!("Structural limit exceeded: history_transaction_bytes: this workflow task completion would write {MEASURED} history bytes in one transaction (limit {LIMIT}).")})
}

fn refusals(path: &str, _body: &str, request: usize) -> Option<(&'static str, String)> {
    let task = path
        .strip_prefix("/api/worker/workflow-tasks/")?
        .strip_suffix("/complete")?;
    let recorded = json!({"outcome":"completed","recorded":true}).to_string();
    match (task, request) {
        ("typed-409" | "typed-409-unrecorded" | "typed-409-twice", 1) => {
            Some(("409 Conflict", typed_409(task).to_string()))
        }
        ("typed-409", _) => Some(("200 OK", recorded)),
        ("typed-409-unrecorded", _) => Some((
            "500 Internal Server Error",
            json!({"message":"fixture"}).to_string(),
        )),
        ("typed-409-twice", _) => Some(("409 Conflict", typed_409(task).to_string())),
        ("untyped-budget", 1) => Some((
            "409 Conflict",
            json!({"reason":"completion_budget_exceeded","recorded":false}).to_string(),
        )),
        ("untyped-budget", _) => Some(("200 OK", recorded)),
        ("recorded-409", _) => {
            let mut body = typed_409(task);
            body["recorded"] = json!(true);
            Some(("409 Conflict", body.to_string()))
        }
        ("invalid-commands-409", _) => Some((
            "409 Conflict",
            json!({"reason":"invalid_commands","recorded":false}).to_string(),
        )),
        ("typed-422", _) => Some(("422 Unprocessable Entity", typed_409(task).to_string())),
        _ => None,
    }
}

fn fan_out_commands() -> Vec<Value> {
    (0..10)
        .map(|index| {
            json!({"type":"schedule_activity","activity_type":"fan.out",
                "arguments":fixture_envelope(json!([index, "x".repeat(1024)]))})
        })
        .collect()
}

fn refusal_worker() -> (MockWorkerServer, Worker) {
    let server = MockWorkerServer::start_with_behavior(MockWorkerBehavior {
        request_override: Some(refusals),
        ..MockWorkerBehavior::default()
    });
    let client = Client::builder(server.base_url())
        .timeout(Duration::from_secs(2))
        .build()
        .expect("client");
    (server, Worker::new(client, "rust-workers"))
}

async fn settle(worker: &Worker, task: &str) -> Result<ManagedPollOutcome> {
    worker
        .settle_workflow_task_decision(
            task,
            "fan-worker",
            2,
            Some("fan-run"),
            Ok(WorkflowTaskDecision::without_message_streams(
                fan_out_commands(),
            )),
            false,
            None,
        )
        .await
}

#[tokio::test]
async fn typed_history_limit_refusal_is_a_typed_error_with_the_server_measure() {
    let (server, worker) = refusal_worker();
    let error = worker
        .client
        .complete_workflow_task("typed-409", "fan-worker", 2, fan_out_commands())
        .await
        .expect_err("the typed 409 is an error");
    assert_eq!(
        server.request_count("/api/worker/workflow-tasks/typed-409/complete"),
        1
    );
    let Error::WorkflowTaskHistoryLimitExceeded(refusal) = &error else {
        panic!("expected a typed history limit refusal, got {error:?}");
    };
    assert_eq!(refusal.status, 409);
    assert_eq!(refusal.reason, "history_transaction_limit_exceeded");
    assert_eq!(refusal.limit_kind, "history_transaction_bytes");
    assert_eq!(refusal.current_value, Some(MEASURED));
    assert_eq!(refusal.configured_limit, Some(LIMIT));
    assert!(refusal.message.starts_with("Structural limit exceeded:"));
    assert_eq!(
        error.to_string(),
        format!(
            "Structural limit exceeded: history_transaction_bytes: the workflow task completion would write {MEASURED} history bytes in one history transaction, over the server limit of {LIMIT} history bytes. Split the fan-out into smaller batches, move work into child workflows, or continue-as-new; offload large payloads with external payload storage."
        )
    );
    assert!(!worker_operation_is_retryable(&error));
}

#[tokio::test]
async fn managed_worker_fails_the_run_once_on_a_typed_history_limit_refusal() {
    let (server, worker) = refusal_worker();
    let outcome = settle(&worker, "typed-409")
        .await
        .expect("the structural-limit failure settles the task");
    assert!(matches!(outcome, ManagedPollOutcome::Handled));
    let sent = server.request_bodies("/api/worker/workflow-tasks/typed-409/complete");
    assert_eq!(sent.len(), 2, "one refused completion, then one failure");
    assert_eq!(sent[0]["commands"].as_array().unwrap().len(), 10);
    assert_eq!(sent[1]["lease_owner"], "fan-worker");
    assert_eq!(sent[1]["workflow_task_attempt"], 2);
    let commands = sent[1]["commands"].as_array().unwrap();
    assert_eq!(commands.len(), 1, "the refused commands are not resent");
    let failure = &commands[0];
    assert_eq!(failure["type"], "fail_workflow");
    assert_eq!(failure["non_retryable"], true);
    assert_eq!(failure["exception_type"], "StructuralLimitExceeded");
    assert_eq!(
        failure["exception_class"],
        "durable_workflow::WorkflowTaskHistoryLimitExceeded"
    );
    let message = failure["message"].as_str().unwrap();
    assert!(
        message.starts_with("Structural limit exceeded: history_transaction_bytes: "),
        "{message}"
    );
    assert!(
        message.contains(&format!("{MEASURED} history bytes")),
        "{message}"
    );
    assert!(
        message.contains(&format!("server limit of {LIMIT} history bytes")),
        "{message}"
    );
    let properties = &failure["exception"]["properties"];
    assert_eq!(
        properties["structural_limit_kind"],
        "history_transaction_bytes"
    );
    assert_eq!(properties["structural_limit_value"], MEASURED);
    assert_eq!(properties["structural_limit_configured"], LIMIT);
    assert_eq!(
        server.request_count("/api/worker/workflow-tasks/typed-409/fail"),
        0,
        "a failed task would be retried into the same refusal"
    );
}

#[tokio::test]
async fn an_untyped_budget_refusal_names_its_reason() {
    let (server, worker) = refusal_worker();
    settle(&worker, "untyped-budget")
        .await
        .expect("the structural-limit failure settles the task");
    let sent = server.request_bodies("/api/worker/workflow-tasks/untyped-budget/complete");
    assert_eq!(sent.len(), 2);
    let failure = &sent[1]["commands"][0];
    let message = failure["message"].as_str().unwrap();
    assert!(
        message.starts_with(
            "Structural limit exceeded: completion_budget_exceeded: the workflow task completion would write more than the limit allows in one history transaction, over the server limit."
        ),
        "{message}"
    );
    assert_eq!(
        failure["exception"]["properties"]["structural_limit_value"],
        Value::Null
    );
}

#[tokio::test]
async fn managed_worker_keeps_the_typed_refusal_when_the_failure_is_not_recorded() {
    for task in ["typed-409-unrecorded", "typed-409-twice"] {
        let (server, worker) = refusal_worker();
        let error = settle(&worker, task)
            .await
            .expect_err("an unrecorded failure stays an error");
        assert!(
            matches!(error, Error::WorkflowTaskHistoryLimitExceeded(_)),
            "{task}: {error:?}"
        );
        let sent = server.request_bodies(&format!("/api/worker/workflow-tasks/{task}/complete"));
        assert_eq!(sent.len(), 2, "{task}: no resend and no retry loop");
        assert_eq!(sent[1]["commands"][0]["type"], "fail_workflow", "{task}");
    }
}

#[tokio::test]
async fn other_conflicts_keep_their_http_error() {
    for (task, status) in [
        ("recorded-409", reqwest::StatusCode::CONFLICT),
        ("invalid-commands-409", reqwest::StatusCode::CONFLICT),
        ("typed-422", reqwest::StatusCode::UNPROCESSABLE_ENTITY),
    ] {
        let (server, worker) = refusal_worker();
        let error = settle(&worker, task)
            .await
            .expect_err("the refusal remains an error");
        assert!(
            matches!(&error, Error::Http { status: observed, .. } if *observed == status),
            "{task}: {error:?}"
        );
        let sent = server.request_bodies(&format!("/api/worker/workflow-tasks/{task}/complete"));
        assert_eq!(sent.len(), 1, "{task}");
        assert_eq!(sent[0]["commands"].as_array().unwrap().len(), 10, "{task}");
    }
}

#[test]
fn only_workflow_task_completions_are_typed() {
    let body = typed_409("task").to_string();
    for path in [
        "/worker/workflow-tasks/task/fail",
        "/worker/activity-tasks/task/complete",
        "/worker/workflow-tasks//complete",
        "/worker/workflow-tasks/a/b/complete",
        "/workflows/task/signal",
    ] {
        assert!(
            workflow_task_history_limit_exceeded(reqwest::StatusCode::CONFLICT, &body, path)
                .is_none(),
            "{path}"
        );
    }
    let refusal = workflow_task_history_limit_exceeded(
        reqwest::StatusCode::CONFLICT,
        &json!({"reason":"history_transaction_limit_exceeded","limit_kind":"bad kind\n",
            "current_value":"12","configured_limit":0})
        .to_string(),
        "/worker/workflow-tasks/task/complete",
    )
    .expect("typed");
    assert_eq!(refusal.limit_kind, "history_transaction_limit_exceeded");
    assert_eq!(refusal.current_value, None);
    assert_eq!(refusal.configured_limit, None);
}
