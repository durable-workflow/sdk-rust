//! Explicit source qualification against an isolated Server protocol 1.20 stack.
//! Ordinary cargo tests leave these cases ignored. They require a real runtime.

use durable_workflow::{
    json, Client, CooperativeCancellationOptions, Error, Value, Worker, WorkflowHandle,
    WorkflowResultOptions,
};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

const WORKFLOW: &str = "tests.rust-cooperative-timer";
const UNDO: &str = "tests.rust-cooperative-undo";

fn client() -> Client {
    Client::builder(std::env::var("DURABLE_WORKFLOW_SERVER_URL").expect("isolated Server URL"))
        .token(Some(
            std::env::var("DURABLE_WORKFLOW_AUTH_TOKEN").expect("isolated Server token"),
        ))
        .namespace("default")
        .build()
        .unwrap()
}

fn queue() -> String {
    format!("rust-cooperative-{}", durable_workflow::Uuid::new_v4())
}

fn worker(client: &Client, queue: &str, saga: bool, prefix: bool) -> Worker {
    let mut worker = Worker::new(client.clone(), queue)
        .worker_id(format!("{queue}-{}", durable_workflow::Uuid::new_v4()))
        .cooperative_cancellation(true)
        .poll_timeout(Duration::from_secs(1));
    worker.register_workflow(WORKFLOW, move |ctx, _| async move {
        if prefix {
            assert_eq!(ctx.side_effect(|| json!(7))?, json!(7));
        }
        if saga {
            let mut saga = ctx.saga();
            saga.add_compensation(UNDO, json!([]))?;
            let result = ctx.sleep(Duration::from_secs(300)).await;
            saga.finish(result).await?;
        } else {
            ctx.sleep(Duration::from_secs(300)).await?;
        }
        Ok(Value::Null)
    });
    worker
}

async fn history(handle: &WorkflowHandle) -> Value {
    let url = std::env::var("DURABLE_WORKFLOW_SERVER_URL").unwrap();
    let token = std::env::var("DURABLE_WORKFLOW_AUTH_TOKEN").unwrap();
    let response = reqwest::Client::new()
        .get(format!(
            "{url}/api/workflows/{}/runs/{}/history",
            handle.workflow_id,
            handle.run_id.as_deref().unwrap()
        ))
        .query(&[("page_size", "1000")])
        .bearer_auth(token)
        .header("X-Namespace", "default")
        .header("X-Durable-Workflow-Control-Plane-Version", "2")
        .send()
        .await
        .unwrap();
    let status = response.status();
    let body: Value = response.json().await.unwrap();
    assert!(status.is_success(), "history {status}: {body}");
    assert!(
        body["next_page_token"].is_null(),
        "bounded scenario history was paginated"
    );
    body
}

fn count(history: &Value, kind: &str) -> usize {
    history["events"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|event| event["event_type"] == kind)
        .count()
}

async fn tick_until(worker: &Worker, handle: &WorkflowHandle, kind: &str) -> Value {
    let mut last_snapshot = Value::Null;
    let observed = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            worker.run_once().await.expect("actual Worker tick");
            let snapshot = history(handle).await;
            if count(&snapshot, kind) > 0 {
                return snapshot;
            }
            last_snapshot = snapshot;
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    observed.unwrap_or_else(|_| {
        panic!(
            "expected {kind} within 30s for {}/{:?}, last history: {last_snapshot}",
            handle.workflow_id, handle.run_id
        )
    })
}

async fn assert_cancelled(handle: &WorkflowHandle, request_id: &str, cleanup: bool) -> Value {
    let result = handle
        .result_selected_run(WorkflowResultOptions {
            timeout: Duration::from_secs(5),
            poll_interval: Duration::from_millis(50),
        })
        .await;
    assert!(
        matches!(result, Err(Error::WorkflowCancelled(_))),
        "terminal result: {result:?}"
    );
    let snapshot = history(handle).await;
    for kind in [
        "CooperativeCancellationRequested",
        "CooperativeCancellationDelivered",
        "WorkflowCancelled",
    ] {
        assert_eq!(count(&snapshot, kind), 1, "{kind}: {snapshot}");
        let event = snapshot["events"]
            .as_array()
            .unwrap()
            .iter()
            .find(|event| event["event_type"] == kind)
            .unwrap();
        assert_eq!(
            event["payload"]["workflow_command_id"], request_id,
            "{kind}: {event}"
        );
    }
    for kind in [
        "WorkflowCompleted",
        "WorkflowFailed",
        "ActivityFailed",
        "ActivityTimedOut",
    ] {
        assert_eq!(count(&snapshot, kind), 0, "{kind}: {snapshot}");
    }
    assert_eq!(count(&snapshot, "ActivityCompleted"), usize::from(cleanup));
    snapshot
}

#[tokio::test]
#[ignore = "requires an isolated cooperative Server protocol 1.20 candidate"]
async fn server_request_before_claim_replays_canonical_typed_cancellation() {
    let client = client();
    let queue = queue();
    let worker = worker(&client, &queue, false, false);
    worker.register().await.unwrap();
    let handle = client
        .start_workflow(WORKFLOW, &queue, &queue, json!([]))
        .await
        .unwrap();
    let request = handle
        .request_cancellation(CooperativeCancellationOptions::default())
        .await
        .unwrap();
    tick_until(&worker, &handle, "WorkflowCancelled").await;
    assert_cancelled(&handle, &request.cancellation_request.request_id, false).await;
}

#[tokio::test]
#[ignore = "requires an isolated cooperative Server protocol 1.20 candidate"]
async fn server_waiting_timer_observes_the_original_request_and_delivery() {
    let client = client();
    let queue = queue();
    let worker = worker(&client, &queue, false, false);
    worker.register().await.unwrap();
    let handle = client
        .start_workflow(WORKFLOW, &queue, &queue, json!([]))
        .await
        .unwrap();
    tick_until(&worker, &handle, "TimerScheduled").await;
    let request = handle
        .request_cancellation(CooperativeCancellationOptions::default())
        .await
        .unwrap();
    tick_until(&worker, &handle, "WorkflowCancelled").await;
    assert_cancelled(&handle, &request.cancellation_request.request_id, false).await;
}

#[tokio::test]
#[ignore = "requires an isolated cooperative Server protocol 1.20 candidate"]
async fn server_duplicate_request_preserves_identity_and_cleanup_deadline() {
    let client = client();
    let queue = queue();
    let worker = worker(&client, &queue, false, false);
    worker.register().await.unwrap();
    let handle = client
        .start_workflow(WORKFLOW, &queue, &queue, json!([]))
        .await
        .unwrap();
    let first = handle
        .request_cancellation(CooperativeCancellationOptions {
            cleanup_timeout_seconds: Some(60),
            reason: Some("first".into()),
        })
        .await
        .unwrap();
    let duplicate = handle
        .request_cancellation(CooperativeCancellationOptions {
            cleanup_timeout_seconds: Some(120),
            reason: Some("duplicate".into()),
        })
        .await
        .unwrap();
    assert!(duplicate.duplicate);
    assert_eq!(first.cancellation_request, duplicate.cancellation_request);
    tick_until(&worker, &handle, "WorkflowCancelled").await;
    assert_cancelled(&handle, &first.cancellation_request.request_id, false).await;
}

#[tokio::test]
#[ignore = "requires an isolated cooperative Server protocol 1.20 candidate"]
async fn server_commits_earlier_side_effect_before_cancellation_delivery() {
    let client = client();
    let queue = queue();
    let original = worker(&client, &queue, false, true);
    original.register().await.unwrap();
    let handle = client
        .start_workflow(WORKFLOW, &queue, &queue, json!([]))
        .await
        .unwrap();
    let request = handle
        .request_cancellation(CooperativeCancellationOptions::default())
        .await
        .unwrap();
    let prefix = tick_until(&original, &handle, "SideEffectRecorded").await;
    assert_eq!(count(&prefix, "CooperativeCancellationDelivered"), 0);
    drop(original);
    let successor = worker(&client, &queue, false, true);
    successor.register().await.unwrap();
    tick_until(&successor, &handle, "WorkflowCancelled").await;
    let snapshot = assert_cancelled(&handle, &request.cancellation_request.request_id, false).await;
    assert_eq!(count(&snapshot, "SideEffectRecorded"), 1);
    let delivery = snapshot["events"]
        .as_array()
        .unwrap()
        .iter()
        .find(|event| event["event_type"] == "CooperativeCancellationDelivered")
        .unwrap();
    assert_eq!(delivery["payload"]["sequence"], 2);
}

#[tokio::test]
#[ignore = "requires an isolated cooperative Server protocol 1.20 candidate"]
async fn server_cold_successor_runs_saga_cleanup_before_terminal_cancellation() {
    let client = client();
    let queue = queue();
    let original = worker(&client, &queue, true, false);
    original.register().await.unwrap();
    let handle = client
        .start_workflow(WORKFLOW, &queue, &queue, json!([]))
        .await
        .unwrap();
    let request = handle
        .request_cancellation(CooperativeCancellationOptions::default())
        .await
        .unwrap();
    let pending = tick_until(&original, &handle, "ActivityScheduled").await;
    assert_eq!(count(&pending, "ActivityCompleted"), 0);
    assert_eq!(count(&pending, "WorkflowCancelled"), 0);
    drop(original);
    let mut successor = worker(&client, &queue, true, false);
    let completed = Arc::new(AtomicUsize::new(0));
    let called = Arc::clone(&completed);
    successor.register_activity(UNDO, move |ctx, _| {
        let called = Arc::clone(&called);
        async move {
            ctx.heartbeat(json!({"cleanup":true})).await?;
            called.fetch_add(1, Ordering::SeqCst);
            Ok(Value::Null)
        }
    });
    successor.register().await.unwrap();
    tick_until(&successor, &handle, "WorkflowCancelled").await;
    assert_cancelled(&handle, &request.cancellation_request.request_id, true).await;
    assert_eq!(completed.load(Ordering::SeqCst), 1);
}
