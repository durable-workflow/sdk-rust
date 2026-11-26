//! Synchronous pre-accept update validation (durable-workflow/.github#234).
//!
//! The fixture delivers one update-validation task through the multiplexed
//! workflow poll. The worker reconstructs the run from committed history and
//! runs only the validator; it never completes a workflow task or runs the
//! update handler.

use super::*;
use durable_workflow::UpdateRejection;

pub(super) const WORKFLOW_TYPE: &str = "corpus.update-validation";
/// The same validated workflow, except that its body reads `replay_now()` at
/// entry and tolerates the unavailable result (.github#215).
pub(super) const ENTRY_CLOCK_WORKFLOW_TYPE: &str = "corpus.update-validation-entry-clock";

#[derive(Clone, Default)]
struct OrderLimit {
    limit: u64,
    context: Option<WorkflowContext>,
}

pub(super) fn validation_task(task: &Value) -> bool {
    task.get("update_validation_task_id").is_some()
}

pub(super) fn capable_cluster_info() -> String {
    json!({
        "limits": {"max_payload_bytes": 2097152},
        "worker_protocol": {"server_capabilities": {
            "update_validation_tasks": true,
            "synchronous_update_validation": {
                "supported": true,
                "acceptance_boundary": "validator_approved",
                "worker_capability": "update_validation_tasks",
                "workflow_contract_field": "update_validators",
                "task_poll": {
                    "strategy": "multiplexed",
                    "endpoint": "/worker/workflow-tasks/poll",
                    "request_field": "task_kinds",
                    "task_kinds": ["workflow", "update_validation"],
                    "response_discriminator": "task.task_kind",
                },
            },
        }},
    })
    .to_string()
}

fn envelope(value: &Value) -> Result<Value, String> {
    serde_json::to_value(encode_payload(value, DEFAULT_CODEC).map_err(|error| error.to_string())?)
        .map_err(|error| error.to_string())
}

pub(super) async fn execute(fixture: &Value) -> Result<Value, String> {
    let mut task = fixture["worker_task"].clone();
    let update_input = task
        .as_object_mut()
        .and_then(|fields| fields.remove("update_input"))
        .unwrap_or_else(|| json!([]));
    let workflow_type = fixture["workflow"]["type"]
        .as_str()
        .filter(|kind| [WORKFLOW_TYPE, ENTRY_CLOCK_WORKFLOW_TYPE].contains(kind))
        .ok_or("update validation fixture names an unregistered workflow type")?;
    let reads_entry_clock = workflow_type == ENTRY_CLOCK_WORKFLOW_TYPE;
    task["workflow_type"] = json!(workflow_type);
    task["workflow_arguments"] = envelope(&fixture["workflow"]["input"])?;
    task["update_arguments"] = envelope(&update_input)?;
    task["history_events"] = fixture["history"].clone();
    let task_id = task["update_validation_task_id"]
        .as_str()
        .ok_or("update validation fixture needs update_validation_task_id")?
        .to_string();
    let server = FixtureServer::start(task, 0, None);
    let client = Client::builder(server.base_url())
        .namespace("default")
        .build()
        .map_err(|error| error.to_string())?;
    let mut worker =
        Worker::new(client, "regression-corpus-queue").worker_id("regression-corpus-worker");
    worker.register_replayed_workflow(
        workflow_type,
        OrderLimit::default,
        move |ctx, input, state| async move {
            if reads_entry_clock {
                // A tolerant entry read: the rebuild must not answer from it
                // unless the task attests the run's entry instant.
                let _entry = ctx.replay_now().ok();
            }
            let limit = input[0].as_u64().unwrap_or_default();
            let kept = ctx.clone();
            state.update(move |order| {
                order.limit = limit;
                order.context = Some(kept);
            })?;
            ctx.sleep(Duration::from_secs(3600)).await?;
            Ok(Value::Null)
        },
    );
    let handler_calls = Arc::new(AtomicUsize::new(0));
    for name in ["approve", "approve-with-memo"] {
        let observed = Arc::clone(&handler_calls);
        worker.register_update(workflow_type, name, move |_ctx, args| {
            observed.fetch_add(1, Ordering::SeqCst);
            async move { Ok(args) }
        });
    }
    worker.register_replayed_update_validator(
        workflow_type,
        "approve",
        |_ctx, order: Arc<OrderLimit>, args| {
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
    worker.register_replayed_update_validator(
        workflow_type,
        "approve-with-memo",
        |_ctx, order: Arc<OrderLimit>, _args| {
            let context = order
                .context
                .as_ref()
                .ok_or_else(|| UpdateRejection::new("no replayed context"))?;
            context.upsert_memo(json!({"approved_by": "validator"}))?;
            Ok(())
        },
    );
    worker.register().await.map_err(|error| error.to_string())?;
    worker.run_once().await.map_err(|error| error.to_string())?;

    let requests = server
        .requests
        .lock()
        .expect("captured validation requests");
    let prefix = format!("/api/worker/update-validation-tasks/{task_id}/");
    let completions: Vec<Value> = requests
        .iter()
        .filter_map(|request| {
            let outcome = request.path.strip_prefix(&prefix)?;
            Some(json!({
                "outcome": outcome,
                "body": serde_json::from_str::<Value>(&request.body).unwrap_or(Value::Null),
            }))
        })
        .collect();
    let polls: Vec<Value> = requests
        .iter()
        .filter(|request| request.path == "/api/worker/workflow-tasks/poll")
        .map(|request| {
            serde_json::from_str::<Value>(&request.body).unwrap_or(Value::Null)["task_kinds"]
                .clone()
        })
        .collect();
    let workflow_task_reports = requests
        .iter()
        .filter(|request| {
            request.path.starts_with("/api/worker/workflow-tasks/")
                && (request.path.ends_with("/complete") || request.path.ends_with("/fail"))
        })
        .count();
    let actual = json!({
        "poll_task_kinds": polls,
        "validation_completions": completions,
        "workflow_task_reports": workflow_task_reports,
        "handler_invocations": handler_calls.load(Ordering::SeqCst),
    });
    fixture_matches(&fixture["expected"], &actual, "update validation")?;
    Ok(actual)
}
