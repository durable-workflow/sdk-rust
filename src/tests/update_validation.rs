//! Synchronous pre-accept update validators (durable-workflow/.github#234).

use super::*;
use crate::update_validation::{UPDATE_VALIDATION_WORKER_FAILED, UPDATE_VALIDATOR_REJECTED};
use std::sync::atomic::AtomicUsize;

const WORKER: &str = "validator-worker";

static HANDLER_CALLS: AtomicUsize = AtomicUsize::new(0);

fn envelope(arguments: Value) -> Value {
    encode_typed_envelope(
        &AvroValue::from_serialize(&arguments).unwrap(),
        DEFAULT_CODEC,
    )
    .unwrap()
}

fn capable_info() -> Value {
    json!({"worker_protocol":{"server_capabilities":{
        "update_validation_tasks":true,
        "synchronous_update_validation":{
            "supported":true,
            "acceptance_boundary":"validator_approved",
            "worker_capability":"update_validation_tasks",
            "workflow_contract_field":"update_validators",
            "task_poll":{
                "strategy":"multiplexed",
                "endpoint":"/worker/workflow-tasks/poll",
                "request_field":"task_kinds",
                "task_kinds":["workflow","update_validation"],
                "response_discriminator":"task.task_kind",
            },
        },
    }}})
}

fn validation_task(id: &str, update_name: &str, amount: u64) -> Value {
    json!({
        "task_kind": "update_validation",
        "update_validation_task_id": id,
        "update_id": id,
        "update_validation_attempt": 1,
        "workflow_id": "order-1",
        "run_id": "run-1",
        "workflow_type": "orders",
        "update_name": update_name,
        "update_arguments": envelope(json!([amount])),
        "payload_codec": DEFAULT_CODEC,
        "task_queue": "rust-workers",
        "lease_owner": WORKER,
        "workflow_arguments": envelope(json!([5])),
        "run_status": "running",
        "history_events": [{"sequence": 1, "event_type": "WorkflowStarted", "payload": {}}],
    })
}

#[derive(Clone, Default)]
struct Order {
    limit: u64,
    context: Option<WorkflowContext>,
}

/// An `orders` workflow whose state holds the start limit, with an `approve`
/// update that counts handler calls.
fn order_worker(client: Client) -> Worker {
    let mut worker = Worker::new(client, "rust-workers").worker_id(WORKER);
    worker.register_replayed_workflow("orders", Order::default, |ctx, input, state| async move {
        let limit = input[0].as_u64().unwrap_or_default();
        let kept = ctx.clone();
        state.update(move |order| {
            order.limit = limit;
            order.context = Some(kept);
        })?;
        ctx.sleep(Duration::from_secs(3600)).await?;
        Ok(Value::Null)
    });
    worker.register_update("orders", "approve", |_ctx, args| async move {
        HANDLER_CALLS.fetch_add(1, Ordering::SeqCst);
        Ok(args)
    });
    worker
}

fn with_limit_validator(mut worker: Worker) -> Worker {
    worker.register_replayed_update_validator(
        "orders",
        "approve",
        |_ctx, order: Arc<Order>, args| {
            let amount = args[0].as_u64().unwrap_or_default();
            if amount > order.limit {
                return Err(UpdateRejection::new(format!(
                    "amount {amount} exceeds the limit {}",
                    order.limit
                ))
                .with_type("LimitExceeded")
                .with_validation_error("amount", "exceeds the order limit"));
            }
            Ok(())
        },
    );
    worker
}

fn offline_client() -> Client {
    Client::new("http://127.0.0.1:9").unwrap()
}

#[tokio::test]
async fn replayed_validator_reads_reconstructed_state_and_rejects_or_approves() {
    let worker = with_limit_validator(order_worker(offline_client()));
    let calls = HANDLER_CALLS.load(Ordering::SeqCst);

    let (reason, rejection) = worker
        .decide_update_validation(validation_task("v1", "approve", 7))
        .await
        .expect_err("an amount above the replayed limit is rejected");
    assert_eq!(reason, UPDATE_VALIDATOR_REJECTED);
    assert_eq!(rejection.message, "amount 7 exceeds the limit 5");
    assert_eq!(rejection.failure_type.as_deref(), Some("LimitExceeded"));
    assert_eq!(
        rejection.validation_errors,
        BTreeMap::from([(
            "amount".to_string(),
            vec!["exceeds the order limit".to_string()]
        )])
    );

    worker
        .decide_update_validation(validation_task("v2", "approve", 3))
        .await
        .expect("an amount within the replayed limit is approved");

    // Validation never runs the update handler.
    assert_eq!(HANDLER_CALLS.load(Ordering::SeqCst), calls);
}

#[tokio::test]
async fn validator_that_schedules_work_is_refused() {
    let mut worker = order_worker(offline_client());
    worker.register_replayed_update_validator(
        "orders",
        "approve",
        |_ctx, order: Arc<Order>, _args| {
            // The reconstructed run's context is reachable through state, but
            // a validator cannot add durable work through it.
            let context = order.context.as_ref().expect("replayed context");
            context.upsert_memo(json!({"approved_by": "validator"}))?;
            Ok(())
        },
    );
    let (reason, rejection) = worker
        .decide_update_validation(validation_task("v1", "approve", 1))
        .await
        .expect_err("scheduling work from a validator is refused");
    assert_eq!(reason, UPDATE_VALIDATOR_REJECTED);
    assert_eq!(
        rejection.failure_type.as_deref(),
        Some("UpdateValidatorCommandRefused")
    );
    assert!(rejection.message.contains("read-only"), "{rejection}");
}

#[tokio::test]
async fn snapshot_validator_sees_history_and_arguments_without_state() {
    let mut worker = order_worker(offline_client());
    worker.register_update_validator("orders", "approve", |ctx, args| {
        assert_eq!(ctx.history_events().len(), 1);
        assert_eq!(ctx.workflow_input(), &json!([5]));
        if args == json!([0]) {
            return Err(UpdateRejection::new("amount must be positive"));
        }
        Ok(())
    });
    let (reason, rejection) = worker
        .decide_update_validation(validation_task("v1", "approve", 0))
        .await
        .unwrap_err();
    assert_eq!(reason, UPDATE_VALIDATOR_REJECTED);
    assert_eq!(rejection.message, "amount must be positive");
    assert_eq!(rejection.failure_type, None);
    worker
        .decide_update_validation(validation_task("v2", "approve", 2))
        .await
        .unwrap();
}

#[tokio::test]
async fn validator_execution_failures_are_not_application_rejections() {
    let mut worker = order_worker(offline_client());
    worker.register_update_validator("orders", "approve", |_ctx, _args| {
        panic!("validator bug");
    });
    let (reason, rejection) = worker
        .decide_update_validation(validation_task("v1", "approve", 1))
        .await
        .unwrap_err();
    assert_eq!(reason, UPDATE_VALIDATION_WORKER_FAILED);
    assert_eq!(
        rejection.failure_type.as_deref(),
        Some("UpdateValidatorPanicked")
    );

    // An update without a declared validator, or not handled at all.
    let (reason, rejection) = worker
        .decide_update_validation(validation_task("v2", "missing", 1))
        .await
        .unwrap_err();
    assert_eq!(reason, UPDATE_VALIDATION_WORKER_FAILED);
    assert!(rejection.message.contains("unknown update"), "{rejection}");
    worker.register_update("orders", "plain", |_ctx, args| async move { Ok(args) });
    let (reason, rejection) = worker
        .decide_update_validation(validation_task("v3", "plain", 1))
        .await
        .unwrap_err();
    assert_eq!(reason, UPDATE_VALIDATION_WORKER_FAILED);
    assert!(rejection.message.contains("is not declared"), "{rejection}");

    // A replayed validator on a workflow without replayable state.
    let mut plain = Worker::new(offline_client(), "rust-workers");
    plain.register_workflow("orders", |_ctx, input| async move { Ok(input) });
    plain.register_update("orders", "approve", |_ctx, args| async move { Ok(args) });
    plain.register_replayed_update_validator(
        "orders",
        "approve",
        |_ctx, _state: Arc<u64>, _args| Ok(()),
    );
    let (reason, _) = plain
        .decide_update_validation(validation_task("v4", "approve", 1))
        .await
        .unwrap_err();
    assert_eq!(reason, UPDATE_VALIDATION_WORKER_FAILED);
}

fn validating_server(path: &str, _body: &str, number: usize) -> Option<(&'static str, String)> {
    match path {
        "/api/cluster/info" => Some(("200 OK", capable_info().to_string())),
        "/api/worker/workflow-tasks/poll" => {
            let task = match number {
                1 => validation_task("val-reject", "approve", 7),
                2 => validation_task("val-approve", "approve", 3),
                _ => Value::Null,
            };
            Some(("200 OK", json!({"task": task}).to_string()))
        }
        "/api/worker/update-validation-tasks/val-reject/reject"
        | "/api/worker/update-validation-tasks/val-approve/approve" => Some((
            "200 OK",
            json!({"outcome": "recorded", "status": 200}).to_string(),
        )),
        _ => None,
    }
}

#[tokio::test]
async fn worker_advertises_validators_and_answers_multiplexed_validation_tasks() {
    let server = MockWorkerServer::start_with_behavior(MockWorkerBehavior {
        request_override: Some(validating_server),
        ..MockWorkerBehavior::default()
    });
    let worker = with_limit_validator(order_worker(Client::new(server.base_url()).unwrap()));
    let calls = HANDLER_CALLS.load(Ordering::SeqCst);
    worker
        .register()
        .await
        .expect("capable Server registration");

    let registration = server.request_body("/api/worker/register");
    assert_eq!(
        registration["workflow_command_contracts"]["orders"]["update_validators"],
        json!(["approve"])
    );
    assert!(registration["capabilities"]
        .as_array()
        .unwrap()
        .contains(&json!(UPDATE_VALIDATION_TASKS_CAPABILITY)));

    assert_eq!(worker.run_once().await.unwrap(), 1);
    assert_eq!(worker.run_once().await.unwrap(), 1);

    for poll in server.request_bodies("/api/worker/workflow-tasks/poll") {
        assert_eq!(poll["task_kinds"], json!(["workflow", "update_validation"]));
    }
    // Activity polls are not multiplexed.
    for poll in server.request_bodies("/api/worker/activity-tasks/poll") {
        assert!(poll.get("task_kinds").is_none(), "{poll}");
    }

    let rejected = server.request_body("/api/worker/update-validation-tasks/val-reject/reject");
    assert_eq!(rejected["lease_owner"], WORKER);
    assert_eq!(rejected["update_validation_attempt"], 1);
    assert_eq!(
        rejected["failure"],
        json!({
            "message": "amount 7 exceeds the limit 5",
            "reason": "update_validator_rejected",
            "type": "LimitExceeded",
            "validation_errors": {"amount": ["exceeds the order limit"]},
        })
    );
    let approved = server.request_body("/api/worker/update-validation-tasks/val-approve/approve");
    assert_eq!(
        approved,
        json!({"lease_owner": WORKER, "update_validation_attempt": 1})
    );

    // Neither validation completed a workflow task, so nothing reached the
    // run's history from this worker, and the update handler never ran.
    let paths: Vec<String> = server
        .requests
        .lock()
        .unwrap()
        .iter()
        .map(|request| request.path.clone())
        .collect();
    assert!(
        !paths
            .iter()
            .any(|path| path.starts_with("/api/worker/workflow-tasks/") && !path.ends_with("/poll")),
        "{paths:?}"
    );
    assert_eq!(HANDLER_CALLS.load(Ordering::SeqCst), calls);
}

fn fencing_server(path: &str, _body: &str, number: usize) -> Option<(&'static str, String)> {
    match path {
        "/api/cluster/info" => Some(("200 OK", capable_info().to_string())),
        "/api/worker/workflow-tasks/poll" if number == 1 => Some((
            "200 OK",
            json!({"task": validation_task("val-stale", "approve", 1)}).to_string(),
        )),
        "/api/worker/update-validation-tasks/val-stale/approve" => Some((
            "409 Conflict",
            json!({"update_validation_task_id": "val-stale", "outcome": "rejected",
                "reason": "stale_update_validation_completion", "status": 409})
            .to_string(),
        )),
        _ => None,
    }
}

#[tokio::test]
async fn fenced_validation_completion_is_settled_not_a_worker_failure() {
    let server = MockWorkerServer::start_with_behavior(MockWorkerBehavior {
        request_override: Some(fencing_server),
        ..MockWorkerBehavior::default()
    });
    let worker = with_limit_validator(order_worker(Client::new(server.base_url()).unwrap()));
    worker.register().await.unwrap();
    assert_eq!(worker.run_once().await.unwrap(), 1);
    assert_eq!(
        server.request_count("/api/worker/update-validation-tasks/val-stale/approve"),
        1
    );
    assert_eq!(worker.run_once().await.unwrap(), 0);
}

#[tokio::test]
async fn registration_refuses_servers_without_synchronous_validation() {
    let server = MockWorkerServer::start();
    let worker = with_limit_validator(order_worker(Client::new(server.base_url()).unwrap()));
    let error = worker.register().await.unwrap_err();
    assert!(
        matches!(&error, Error::UpdateValidationUnavailable(message) if message.contains("does not advertise synchronous pre-accept update validation")),
        "{error}"
    );
    assert_eq!(server.request_count("/api/worker/register"), 0);
    assert_eq!(server.request_count("/api/cluster/info"), 1);

    // A worker that never confirmed registration does not poll unvalidated.
    let error = worker.run_once().await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("update_validation_registration_unconfirmed"),
        "{error}"
    );
    assert_eq!(server.request_count("/api/worker/workflow-tasks/poll"), 0);
}

#[tokio::test]
async fn registration_refuses_a_validator_without_an_update_handler() {
    let server = MockWorkerServer::start_with_behavior(MockWorkerBehavior {
        request_override: Some(validating_server),
        ..MockWorkerBehavior::default()
    });
    let mut worker = Worker::new(Client::new(server.base_url()).unwrap(), "rust-workers");
    worker.register_workflow("orders", |_ctx, input| async move { Ok(input) });
    worker.register_update_validator("orders", "approve", |_ctx, _args| Ok(()));
    let error = worker.register().await.unwrap_err();
    assert!(
        matches!(&error, Error::UpdateValidationUnavailable(message) if message.contains("orders.approve")),
        "{error}"
    );
    assert_eq!(server.request_count("/api/cluster/info"), 0);
    assert_eq!(server.request_count("/api/worker/register"), 0);
}

#[test]
fn duplicate_validators_invalidate_the_worker() {
    let mut worker = order_worker(offline_client());
    worker.register_update_validator("orders", "approve", |_ctx, _args| Ok(()));
    worker.register_update_validator_avro_value("orders", "approve", |_ctx, _args| Ok(()));
    let error = worker.validate_registration().unwrap_err();
    assert_eq!(error.handler_kind, RegistrationKind::Update);
    assert_eq!(error.handler_name, "approve#validator");
}

fn rejecting_update_server(path: &str, _body: &str, _: usize) -> Option<(&'static str, String)> {
    match path {
        "/api/workflows/order-1/update/approve" => Some((
            "422 Unprocessable Entity",
            json!({
                "workflow_id": "order-1", "run_id": "run-1", "update_name": "approve",
                "update_id": "upd-1", "accepted": false, "approved": false,
                "outcome": "rejected_invalid_arguments", "reason": "update_validator_rejected",
                "rejection_reason": "update_validator_rejected", "update_status": "rejected",
                "command_status": "rejected", "message": "amount 7 exceeds the limit 5",
                "validation_errors": {"amount": ["exceeds the order limit"]},
            })
            .to_string(),
        )),
        "/api/workflows/order-1/update/slow" => Some((
            "422 Unprocessable Entity",
            json!({
                "workflow_id": "order-1", "update_name": "slow",
                "outcome": "update_validation_failed", "reason": "update_validation_timed_out",
                "message": "Synchronous update validation did not complete before the acceptance deadline.",
                "retryable": true,
            })
            .to_string(),
        )),
        _ => None,
    }
}

#[tokio::test]
async fn client_reports_validator_rejection_distinctly_from_failed_updates() {
    let server = MockWorkerServer::start_with_behavior(MockWorkerBehavior {
        request_override: Some(rejecting_update_server),
        ..MockWorkerBehavior::default()
    });
    let client = Client::new(server.base_url()).unwrap();
    let error = client
        .update_workflow("order-1", "approve", json!([7]), Some("approve-7"))
        .await
        .unwrap_err();
    assert!(error.is_update_validator_rejected());
    assert!(!error.is_update_validation_failed());
    let rejection = error.update_validator_rejection().unwrap();
    assert_eq!(rejection.status, 422);
    assert_eq!(rejection.update_id.as_deref(), Some("upd-1"));
    assert_eq!(rejection.update_name.as_deref(), Some("approve"));
    assert_eq!(rejection.message, "amount 7 exceeds the limit 5");
    assert_eq!(
        rejection.validation_errors,
        BTreeMap::from([(
            "amount".to_string(),
            vec!["exceeds the order limit".to_string()]
        )])
    );

    let error = client
        .update_workflow("order-1", "slow", json!([1]), Some("slow-1"))
        .await
        .unwrap_err();
    assert!(error.is_update_validation_failed());
    assert!(!error.is_update_validator_rejected());
    assert!(error.update_validator_rejection().is_none());

    // An ordinary failed update is neither.
    let failed = Error::Http {
        status: reqwest::StatusCode::UNPROCESSABLE_ENTITY,
        body: json!({"outcome": "update_failed", "reason": "update_failed"}).to_string(),
    };
    assert!(!failed.is_update_validator_rejected() && !failed.is_update_validation_failed());
}

#[test]
fn server_capability_check_requires_the_multiplexed_poll_shape() {
    use crate::update_validation::server_supports_update_validation_tasks as supports;
    assert!(supports(&capable_info()));
    assert!(!supports(&json!({})));
    for pointer in [
        "/worker_protocol/server_capabilities/update_validation_tasks",
        "/worker_protocol/server_capabilities/synchronous_update_validation/supported",
        "/worker_protocol/server_capabilities/synchronous_update_validation/task_poll/strategy",
        "/worker_protocol/server_capabilities/synchronous_update_validation/task_poll/endpoint",
    ] {
        let mut info = capable_info();
        *info.pointer_mut(pointer).unwrap() = json!("other");
        assert!(!supports(&info), "{pointer}");
    }
    let mut info = capable_info();
    info["worker_protocol"]["server_capabilities"]["synchronous_update_validation"]["task_poll"]
        ["task_kinds"] = json!(["workflow"]);
    assert!(!supports(&info));
}
