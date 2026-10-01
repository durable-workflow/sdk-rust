//! Explicit source qualification against an isolated Server protocol 1.20 stack.
//! Ordinary cargo tests leave these cases ignored. They require a real runtime.

use durable_workflow::{
    json, ActivityContext, Client, ConditionWaitOptions, CooperativeCancellationOptions, Error,
    ParallelOperation, ParallelResult, SelectionKey, Value, Worker, WorkflowHandle,
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
const BLOCKED: &str = "tests.rust-cooperative-blocked";
const REPLAY: &str = "tests.rust-cooperative-replay";

struct CallbackDrop(Arc<AtomicUsize>);

impl Drop for CallbackDrop {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

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
    tick_until_count(worker, handle, kind, 1).await
}

async fn tick_until_count(
    worker: &Worker,
    handle: &WorkflowHandle,
    kind: &str,
    target: usize,
) -> Value {
    let mut last_snapshot = Value::Null;
    let observed = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            worker.run_once().await.expect("actual Worker tick");
            let snapshot = history(handle).await;
            if count(&snapshot, kind) >= target {
                return snapshot;
            }
            last_snapshot = snapshot;
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    observed.unwrap_or_else(|_| {
        panic!(
            "expected {target} {kind} within 30s for {}/{:?}, last history: {last_snapshot}",
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

async fn managed_remote_cancellation(user_heartbeat: bool) {
    let client = client();
    let queue = queue();
    let dropped = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(AtomicUsize::new(0));
    let (entered_tx, mut entered_rx) = tokio::sync::mpsc::unbounded_channel::<ActivityContext>();
    let (heartbeat_tx, mut heartbeat_rx) = tokio::sync::mpsc::unbounded_channel();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let mut worker = Worker::new(client.clone(), &queue)
        .worker_id(format!("{queue}-managed"))
        .cooperative_cancellation(true)
        .max_concurrent_workflow_tasks(1)
        .max_concurrent_activity_tasks(1)
        .poll_timeout(Duration::from_secs(1))
        .on_worker_heartbeat(move |observation| {
            if observation.acknowledgement["acknowledged"] == true {
                let _ = heartbeat_tx.send(());
            }
        });
    worker.register_workflow(WORKFLOW, |ctx, _| async move {
        let mut saga = ctx.saga();
        saga.add_compensation(UNDO, json!([]))?;
        let result = ctx.activity(BLOCKED, json!([])).await;
        saga.finish(result).await?;
        Ok(Value::Null)
    });
    let callback_dropped = Arc::clone(&dropped);
    worker.register_activity(BLOCKED, move |ctx, _| {
        let entered_tx = entered_tx.clone();
        let callback_dropped = Arc::clone(&callback_dropped);
        async move {
            let _drop = CallbackDrop(callback_dropped);
            if user_heartbeat {
                ctx.heartbeat(json!({"qualification":"blocked"})).await?;
            }
            entered_tx.send(ctx.clone()).unwrap();
            if user_heartbeat {
                loop {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    ctx.heartbeat(json!({"qualification":"blocked"})).await?;
                }
            } else {
                std::future::pending::<durable_workflow::Result<Value>>().await
            }
        }
    });
    let cleanup_completed = Arc::clone(&completed);
    worker.register_activity(UNDO, move |ctx, _| {
        let cleanup_completed = Arc::clone(&cleanup_completed);
        async move {
            ctx.heartbeat(json!({"qualification":"cleanup"})).await?;
            cleanup_completed.fetch_add(1, Ordering::SeqCst);
            Ok(Value::Null)
        }
    });
    let run = tokio::spawn(async move {
        worker
            .run_until(async {
                let _ = shutdown_rx.await;
            })
            .await
    });
    tokio::time::timeout(Duration::from_secs(10), heartbeat_rx.recv())
        .await
        .unwrap()
        .expect("actual accepted worker heartbeat");
    let handle = client
        .start_workflow(WORKFLOW, &queue, &queue, json!([]))
        .await
        .unwrap();
    let original = tokio::time::timeout(Duration::from_secs(10), entered_rx.recv())
        .await
        .unwrap()
        .expect("actual blocked callback entered");
    let request = handle
        .request_cancellation(CooperativeCancellationOptions::default())
        .await
        .unwrap();
    let result = handle
        .result_selected_run(WorkflowResultOptions {
            timeout: Duration::from_secs(20),
            poll_interval: Duration::from_millis(50),
        })
        .await;
    assert!(
        matches!(result, Err(Error::WorkflowCancelled(_))),
        "managed result: {result:?}"
    );
    let snapshot = assert_cancelled(&handle, &request.cancellation_request.request_id, true).await;
    assert_eq!(
        dropped.load(Ordering::SeqCst),
        1,
        "blocked callback future must drop"
    );
    assert_eq!(
        completed.load(Ordering::SeqCst),
        1,
        "cleanup runs once after freeing its only activity slot"
    );
    assert_eq!(count(&snapshot, "ActivityCancelled"), 1);
    let original_progress = snapshot["events"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|event| {
            event["event_type"] == "ActivityHeartbeatRecorded"
                && event["payload"]["activity_type"] == BLOCKED
        })
        .count();
    assert_eq!(
        original_progress > 0,
        user_heartbeat,
        "readonly checks must not record progress"
    );
    assert!(
        matches!(
            original.heartbeat(json!({"late":true})).await,
            Err(Error::ActivityExecutionAbandoned(_))
        ),
        "cloned context remains abandoned"
    );
    let completion = client
        .complete_activity_task(
            &original.task_id,
            &original.activity_attempt_id,
            &original.lease_owner,
            json!("late"),
            "avro",
        )
        .await;
    assert!(
        matches!(completion, Err(Error::ActivityTaskRejected(rejection)) if rejection.status == 409),
        "Server must reject the cancelled attempt's late completion"
    );
    let failure = client
        .fail_activity_task(
            &original.task_id,
            &original.activity_attempt_id,
            &original.lease_owner,
            "late failure",
            true,
        )
        .await;
    assert!(
        matches!(failure, Err(Error::ActivityTaskRejected(rejection)) if rejection.status == 409),
        "Server must reject the cancelled attempt's late failure"
    );
    assert_eq!(
        snapshot,
        history(&handle).await,
        "late publication changed canonical history"
    );
    shutdown_tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(10), run)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
#[ignore = "requires an isolated cooperative Server protocol 1.20 candidate"]
async fn server_managed_worker_fences_blocked_callback_without_user_heartbeats() {
    managed_remote_cancellation(false).await;
}

#[tokio::test]
#[ignore = "requires an isolated cooperative Server protocol 1.20 candidate"]
async fn server_managed_worker_fences_blocked_callback_with_user_heartbeats() {
    managed_remote_cancellation(true).await;
}

fn replay_worker(client: &Client, queue: &str, mode: &'static str) -> Worker {
    let mut worker = Worker::new(client.clone(), queue)
        .worker_id(format!("{queue}-{}", durable_workflow::Uuid::new_v4()))
        .cooperative_cancellation(true)
        .poll_timeout(Duration::from_secs(1));
    worker.register_workflow(REPLAY, move |ctx, _| async move {
        let predicate_ctx = ctx.clone();
        let condition = || ConditionWaitOptions::new("two-votes", "sha256:two-votes-v1");
        match mode {
            "condition" => {
                ctx.wait_condition(condition(), move || {
                    Ok(predicate_ctx.signals("vote")?.len() >= 2)
                })
                .await?;
            }
            "parallel" => {
                ctx.parallel(vec![
                    ParallelOperation::timer(Duration::from_secs(300)),
                    ParallelOperation::group(vec![
                        ParallelOperation::signal("never"),
                        ParallelOperation::condition(condition(), move || {
                            Ok(predicate_ctx.signals("vote")?.len() >= 2)
                        }),
                    ]),
                ])
                .await?;
            }
            "selection" => {
                ctx.select_keyed(vec![
                    ("timer", ParallelOperation::timer(Duration::from_secs(300))),
                    (
                        "votes",
                        ParallelOperation::condition(condition(), move || {
                            Ok(predicate_ctx.signals("vote")?.len() >= 2)
                        }),
                    ),
                ])
                .await?;
            }
            "winner" => {
                let selected = ctx
                    .select_keyed(vec![
                        ("slow", ParallelOperation::signal("never")),
                        ("fast", ParallelOperation::timer(Duration::from_secs(1))),
                    ])
                    .await?;
                assert_eq!(selected.key, SelectionKey::Name("fast".into()));
                let slow = selected
                    .handle(&SelectionKey::Name("slow".into()))
                    .unwrap()
                    .clone();
                assert_eq!(selected.into_result()?, ParallelResult::Timer);
                slow.await_result().await?;
            }
            _ => panic!("unknown fixture mode"),
        }
        Ok(Value::Null)
    });
    worker
}

async fn reopened_condition_cancellation(mode: &'static str, span: u64) {
    let client = client();
    let queue = queue();
    let original = replay_worker(&client, &queue, mode);
    original.register().await.unwrap();
    let handle = client
        .start_workflow(REPLAY, &queue, &queue, json!([]))
        .await
        .unwrap();
    let first = tick_until(&original, &handle, "ConditionWaitOpened").await;
    assert_eq!(count(&first, "ConditionWaitOpened"), 1);
    handle
        .signal_selected_run("vote", json!(["first"]))
        .await
        .unwrap();
    let reopened = tick_until_count(&original, &handle, "ConditionWaitOpened", 2).await;
    assert_eq!(count(&reopened, "ConditionWaitOpened"), 2);
    assert_eq!(count(&reopened, "ConditionWaitSatisfied"), 1);
    assert_eq!(count(&reopened, "SelectionResolved"), 0);
    let opens = reopened["events"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|event| event["event_type"] == "ConditionWaitOpened")
        .collect::<Vec<_>>();
    assert_eq!(
        opens[0]["payload"]["condition_wait_occurrence_id"],
        opens[1]["payload"]["condition_wait_occurrence_id"]
    );
    assert_ne!(
        opens[0]["payload"]["sequence"],
        opens[1]["payload"]["sequence"]
    );
    let pending_sequence = opens[1]["payload"]["sequence"].as_u64().unwrap();
    let request = handle
        .request_cancellation(CooperativeCancellationOptions::default())
        .await
        .unwrap();
    drop(original);
    let successor = replay_worker(&client, &queue, mode);
    successor.register().await.unwrap();
    tick_until(&successor, &handle, "WorkflowCancelled").await;
    let snapshot = assert_cancelled(&handle, &request.cancellation_request.request_id, false).await;
    let events = snapshot["events"].as_array().unwrap();
    let delivery_index = events
        .iter()
        .position(|event| event["event_type"] == "CooperativeCancellationDelivered")
        .unwrap();
    // Recorded command-prefix work can reopen a grouped physical wait before
    // delivery. Cancellation must retain its authored identity and prevent any
    // new wait after the canonical delivery boundary.
    assert!(count(&snapshot, "ConditionWaitOpened") >= 2);
    for event in events
        .iter()
        .filter(|event| event["event_type"] == "ConditionWaitOpened")
    {
        assert_eq!(
            event["payload"]["condition_wait_occurrence_id"],
            opens[0]["payload"]["condition_wait_occurrence_id"],
            "physical prefix reopen changed authored identity: {snapshot}"
        );
    }
    assert!(
        events[delivery_index + 1..]
            .iter()
            .all(|event| event["event_type"] != "ConditionWaitOpened"),
        "condition reopened after cancellation delivery: {snapshot}"
    );
    eprintln!("cooperative reopened condition {mode}: {snapshot}");
    if mode == "condition" {
        assert_eq!(count(&snapshot, "ConditionWaitOpened"), 2);
    }
    assert_eq!(count(&snapshot, "SelectionResolved"), 0);
    let delivery = snapshot["events"]
        .as_array()
        .unwrap()
        .iter()
        .find(|event| event["event_type"] == "CooperativeCancellationDelivered")
        .unwrap();
    assert_eq!(
        delivery["payload"]["sequence_span"].as_u64().unwrap_or(1),
        span
    );
    if mode == "condition" {
        assert_eq!(delivery["payload"]["call_kind"], "condition");
        assert_eq!(delivery["payload"]["sequence"], pending_sequence);
    } else {
        assert_eq!(delivery["payload"]["call_kind"], "parallel");
        assert_eq!(delivery["payload"]["sequence"], 1);
    }
}

#[tokio::test]
#[ignore = "requires an isolated cooperative Server protocol 1.20 candidate"]
async fn server_cold_condition_delivery_uses_the_pending_physical_reopen() {
    reopened_condition_cancellation("condition", 1).await;
}

#[tokio::test]
#[ignore = "requires an isolated cooperative Server protocol 1.20 candidate"]
async fn server_cold_nested_parallel_cancellation_replays_reopened_condition() {
    reopened_condition_cancellation("parallel", 3).await;
}

#[tokio::test]
#[ignore = "requires an isolated cooperative Server protocol 1.20 candidate"]
async fn server_cold_selection_cancellation_replays_reopened_condition() {
    reopened_condition_cancellation("selection", 2).await;
}

#[tokio::test]
#[ignore = "requires an isolated cooperative Server protocol 1.20 candidate"]
async fn server_cold_selection_preserves_committed_winner_before_loser_cancellation() {
    let client = client();
    let queue = queue();
    let original = replay_worker(&client, &queue, "winner");
    original.register().await.unwrap();
    let handle = client
        .start_workflow(REPLAY, &queue, &queue, json!([]))
        .await
        .unwrap();
    let winner = tick_until(&original, &handle, "SelectionResolved").await;
    assert_eq!(count(&winner, "SelectionResolved"), 1);
    assert_eq!(count(&winner, "WorkflowCompleted"), 0);
    let request = handle
        .request_cancellation(CooperativeCancellationOptions::default())
        .await
        .unwrap();
    drop(original);
    let successor = replay_worker(&client, &queue, "winner");
    successor.register().await.unwrap();
    tick_until(&successor, &handle, "WorkflowCancelled").await;
    let snapshot = assert_cancelled(&handle, &request.cancellation_request.request_id, false).await;
    assert_eq!(count(&snapshot, "SelectionResolved"), 1);
    let delivery = snapshot["events"]
        .as_array()
        .unwrap()
        .iter()
        .find(|event| event["event_type"] == "CooperativeCancellationDelivered")
        .unwrap();
    assert_eq!(delivery["payload"]["call_kind"], "selection_handle");
    assert_eq!(delivery["payload"]["operation_sequence"], 1);
    assert_eq!(
        delivery["payload"]["operation_sequence_span"]
            .as_u64()
            .unwrap_or(1),
        1
    );
}
