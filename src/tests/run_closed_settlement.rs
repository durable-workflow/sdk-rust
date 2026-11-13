//! Replay of activities the run's own close cancelled (workflow#1117,
//! server#1193): an `ActivityCancelled` with `cancellation_reason=run_closed`
//! was recorded after the workflow moved past the operation, so it never
//! resolves a selection or `all()` member and the activity replays as open.

use super::*;

fn batch_selection() -> Vec<(SelectionKey, ParallelOperation)> {
    vec![
        (
            SelectionKey::from("batch"),
            ParallelOperation::group(vec![
                ParallelOperation::activity("still-running", json!([])),
                ParallelOperation::activity("failing", json!([])),
            ]),
        ),
        (
            SelectionKey::from("other"),
            ParallelOperation::activity("other", json!([])),
        ),
    ]
}

/// ActivityScheduled events for the commands a fresh run emits, numbered
/// in authored order.
fn scheduled_history(commands: &[Value]) -> Vec<HistoryEvent> {
    commands
        .iter()
        .enumerate()
        .map(|(index, command)| {
            assert_eq!(command["type"], "schedule_activity", "{command}");
            let mut payload = serde_json::Map::new();
            for (key, value) in command.as_object().expect("command object") {
                if key.starts_with("parallel_group_") || key.starts_with("selection_member_") {
                    payload.insert(key.clone(), value.clone());
                }
            }
            payload.insert("sequence".into(), json!(index + 1));
            payload.insert(
                "activity_execution_id".into(),
                json!(format!("activity-{}", index + 1)),
            );
            payload.insert("activity_type".into(), command["activity_type"].clone());
            let mut event = history_event("ActivityScheduled", Value::Object(payload));
            event
                .raw
                .insert("id".into(), json!(format!("scheduled-{}", index + 1)));
            event
        })
        .collect()
}

fn outcome_event(
    event_type: &str,
    scheduled: &HistoryEvent,
    id: &str,
    extra: Value,
) -> HistoryEvent {
    let mut payload = scheduled.payload.clone();
    for (key, value) in extra.as_object().expect("extra payload") {
        payload[key] = value.clone();
    }
    let mut event = history_event(event_type, payload);
    event.raw.insert("id".into(), json!(id));
    event
}

/// The keys `ActivityCancellation::record` writes when `ClosingRunOpenWork`
/// settles an open activity at close (workflow#1121).
fn cancelled_at_close(scheduled: &HistoryEvent, id: &str) -> HistoryEvent {
    let execution = scheduled.payload["activity_execution_id"].clone();
    outcome_event(
        "ActivityCancelled",
        scheduled,
        id,
        json!({
            "workflow_command_id": null,
            "activity_attempt_id": format!("{}-attempt", execution.as_str().unwrap()),
            "activity_class": scheduled.payload["activity_type"],
            "attempt_number": 1,
            "cancelled_at": "2026-11-13T00:39:42.300000Z",
            "activity": {"id": execution, "status": "cancelled"},
            "activity_attempt": {"status": "cancelled"},
            "cancellation_reason": "run_closed",
        }),
    )
}

fn fresh_commands<F, Fut, T>(author: F) -> Vec<Value>
where
    F: FnOnce(WorkflowContext) -> Fut,
    Fut: Future<Output = Result<T>>,
    T: std::fmt::Debug,
{
    let ctx = workflow_context(Vec::new());
    let mut call = Box::pin(author(ctx.clone()));
    let mut task_context = TaskContext::from_waker(noop_waker_ref());
    assert!(call.as_mut().poll(&mut task_context).is_pending());
    ctx.take_commands().expect("commands")
}

fn poll_ready<Fut: Future>(future: Fut) -> Fut::Output {
    let mut future = Box::pin(future);
    let mut task_context = TaskContext::from_waker(noop_waker_ref());
    match future.as_mut().poll(&mut task_context) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("replay must resolve from recorded history"),
    }
}

fn failure_message(error: &Error) -> String {
    match error {
        Error::ParallelFailed(failure) => failure_message(&failure.cause),
        Error::ActivityFailed(failure) => format!("{:?}:{}", failure.kind, failure.message),
        other => format!("{other:?}"),
    }
}

#[test]
fn selection_winner_group_failure_is_not_replaced_by_a_run_close_cancellation() {
    let commands = fresh_commands(|ctx| async move { ctx.select_keyed(batch_selection()).await });
    assert_eq!(commands.len(), 3, "{commands:?}");
    let scheduled = scheduled_history(&commands);
    let failure = outcome_event(
        "ActivityFailed",
        &scheduled[1],
        "failed-2",
        json!({"message": "boom", "failure_category": "application", "non_retryable": true}),
    );
    let marker = history_event(
        "SelectionResolved",
        json!({
            "selection_group_id": commands[0]["parallel_group_path"][0]["parallel_group_id"],
            "selection_group_base_sequence": 1,
            "selection_group_size": 3,
            "member_key": "batch",
            "member_index": 0,
            "member_base_sequence": 1,
            "member_size": 2,
            "operation_kind": "group",
            "operation_identity": "group:1:2",
            "outcome": "failed",
            "resolution_event_id": "failed-2",
            "resolution_event_type": "ActivityFailed",
        }),
    );
    let mut history = scheduled.clone();
    history.extend([
        failure,
        marker,
        // The run completed while the batch's first member and the losing
        // member were still open; its close cancelled both, in sequence order.
        cancelled_at_close(&scheduled[0], "cancelled-1"),
        cancelled_at_close(&scheduled[2], "cancelled-3"),
        history_event(
            "WorkflowCompleted",
            json!({"output": null, "payload_codec": DEFAULT_CODEC, "abandoned_activities": []}),
        ),
    ]);

    for _restart in 0..2 {
        let ctx = workflow_context(history.clone());
        let selected = poll_ready(ctx.select_keyed(batch_selection())).expect("selection replay");
        assert_eq!(selected.key, SelectionKey::from("batch"));
        assert_eq!(
            failure_message(selected.failure.as_ref().expect("batch failed")),
            "Failed:boom"
        );
        let handle = selected
            .handle(&SelectionKey::from("other"))
            .cloned()
            .expect("loser handle");
        assert!(
            ctx.take_commands().expect("commands").is_empty(),
            "replay of a closed run emits nothing new"
        );
        // The loser replays as still open: the close's cancellation is not an
        // outcome the workflow observed.
        let mut pending = Box::pin(handle.await_result());
        let mut task_context = TaskContext::from_waker(noop_waker_ref());
        assert!(pending.as_mut().poll(&mut task_context).is_pending());
    }
}

#[test]
fn handled_all_failure_is_not_replaced_by_a_lower_index_run_close_cancellation() {
    let operations = || {
        vec![
            ParallelOperation::activity("still-running", json!([])),
            ParallelOperation::activity("failing", json!([])),
        ]
    };
    let commands = fresh_commands(|ctx| async move { ctx.parallel(operations()).await });
    let scheduled = scheduled_history(&commands);
    let mut history = scheduled.clone();
    history.extend([
        outcome_event(
            "ActivityFailed",
            &scheduled[1],
            "failed-2",
            json!({"message": "boom", "failure_category": "application"}),
        ),
        // Frozen time: the close's cancellation of member 0 shares the
        // failure's recorded time and has the lower index.
        cancelled_at_close(&scheduled[0], "cancelled-1"),
        history_event(
            "WorkflowFailed",
            json!({"message": "boom", "abandoned_activities": [], "future_close_detail": 1}),
        ),
    ]);

    for _restart in 0..2 {
        let ctx = workflow_context(history.clone());
        let error = poll_ready(ctx.parallel(operations())).expect_err("handled failure");
        assert_eq!(failure_message(&error), "Failed:boom");
    }

    // The same event without the run_closed reason is an observed outcome.
    let mut observed = history.clone();
    observed[3]
        .payload
        .as_object_mut()
        .unwrap()
        .remove("cancellation_reason");
    let ctx = workflow_context(observed);
    let error = poll_ready(ctx.parallel(operations())).expect_err("observed cancellation");
    assert_eq!(failure_message(&error), "Cancelled:activity was cancelled");
}

#[test]
fn awaited_activity_replays_open_when_only_a_run_close_cancellation_exists() {
    let commands =
        fresh_commands(|ctx| async move { ctx.activity::<Value>("work", json!([])).await });
    let scheduled = scheduled_history(&commands);
    let mut history = scheduled.clone();
    history.push(cancelled_at_close(&scheduled[0], "cancelled-1"));
    let ctx = workflow_context(history);
    let mut call = Box::pin(ctx.activity::<Value>("work", json!([])));
    let mut task_context = TaskContext::from_waker(noop_waker_ref());
    assert!(call.as_mut().poll(&mut task_context).is_pending());
    assert!(ctx.take_commands().expect("commands").is_empty());
}

#[test]
fn run_close_cancellation_is_recognized_only_by_its_reason() {
    let event = |event_type: &str, payload: Value| history_event(event_type, payload);
    assert!(settled_at_run_close(&event(
        "ActivityCancelled",
        json!({"sequence": 1, "cancellation_reason": "run_closed"})
    )));
    for (event_type, payload) in [
        ("ActivityCancelled", json!({"sequence": 1})),
        (
            "ActivityCancelled",
            json!({"sequence": 1, "cancellation_reason": "requested"}),
        ),
        (
            "ActivityFailed",
            json!({"sequence": 1, "cancellation_reason": "run_closed"}),
        ),
        (
            "TimerCancelled",
            json!({"sequence": 1, "cancellation_reason": "run_closed"}),
        ),
    ] {
        assert!(!settled_at_run_close(&event(event_type, payload)));
    }
}

const POLL: &str = "/api/worker/activity-tasks/poll";
const COMPLETE: &str = "/api/worker/activity-tasks/close-task/complete";
const FAIL: &str = "/api/worker/activity-tasks/close-task/fail";
const HEARTBEAT: &str = "/api/worker/activity-tasks/close-task/heartbeat";

fn close_delivery() -> Value {
    json!({
        "task":{
            "task_id":"close-task","workflow_id":"close-workflow","run_id":"close-run",
            "activity_execution_id":"close-execution","activity_attempt_id":"close-attempt",
            "activity_type":"CloseActivity","attempt_number":1,"lease_owner":"close-worker",
            "task_queue":"close-queue","payload_codec":"avro",
            "arguments":encode_value_envelope(&json!([]), DEFAULT_CODEC).unwrap(),
        }
    })
}

/// The Server's late-outcome refusal for a run its own code closed:
/// `recorded=false`, `reason=run_closed`, HTTP 409 (workflow#1121).
fn run_closed_outcome(outcome: &str) -> String {
    json!({
        "task_id":"close-task","activity_attempt_id":"close-attempt",
        "outcome":outcome,"recorded":false,"reason":"run_closed",
    })
    .to_string()
}

fn close_replies(path: &str, can_continue: bool) -> Option<(&'static str, String)> {
    match path {
        POLL => Some(("200 OK", close_delivery().to_string())),
        "/api/cluster/info" => Some((
            "200 OK",
            json!({"worker_protocol":{"server_capabilities":{}}}).to_string(),
        )),
        HEARTBEAT => Some((
            "200 OK",
            json!({
                "can_continue":can_continue,"cancel_requested":false,"heartbeat_recorded":false,
                "reason":if can_continue { Value::Null } else { json!("run_closed") },
                "run_closed_reason":if can_continue { Value::Null } else { json!("completed") },
            })
            .to_string(),
        )),
        COMPLETE => Some(("409 Conflict", run_closed_outcome("completed"))),
        FAIL => Some(("409 Conflict", run_closed_outcome("failed"))),
        _ => None,
    }
}

fn cancelled_at_close_replies(path: &str, _: &str, _: usize) -> Option<(&'static str, String)> {
    close_replies(path, false)
}

fn abandoned_at_close_replies(path: &str, _: &str, _: usize) -> Option<(&'static str, String)> {
    close_replies(path, true)
}

fn close_worker(server: &MockWorkerServer) -> Worker {
    Worker::new(
        Client::builder(server.base_url())
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap(),
        "close-queue",
    )
    .worker_id("close-worker")
    .poll_timeout(Duration::from_millis(10))
}

fn close_server(replies: RequestOverride) -> MockWorkerServer {
    MockWorkerServer::start_with_behavior(MockWorkerBehavior {
        request_override: Some(replies),
        ..MockWorkerBehavior::default()
    })
}

#[tokio::test]
async fn run_closed_heartbeat_and_late_outcome_are_final_not_retried() {
    for fails in [false, true] {
        let server = close_server(cancelled_at_close_replies);
        let stop_observed = Arc::new(AtomicBool::new(false));
        let observed = Arc::clone(&stop_observed);
        let mut worker = close_worker(&server);
        worker.register_activity("CloseActivity", move |ctx, _args| {
            let observed = Arc::clone(&observed);
            async move {
                let heartbeat = ctx.heartbeat(json!({"stage":"late"})).await?;
                assert_eq!(heartbeat.reason.as_deref(), Some("run_closed"));
                observed.store(heartbeat.should_stop(), Ordering::SeqCst);
                if fails {
                    return Err(Error::WorkerLoop("stopped after run close".into()));
                }
                Ok(json!({"late":true}))
            }
        });

        assert!(matches!(
            worker
                .poll_activity_once()
                .await
                .expect("run_closed settles the attempt"),
            ManagedPollOutcome::Handled
        ));
        assert!(stop_observed.load(Ordering::SeqCst));
        let (reported, other) = if fails {
            (FAIL, COMPLETE)
        } else {
            (COMPLETE, FAIL)
        };
        assert_eq!(server.request_count(reported), 1, "never retried");
        assert_eq!(server.request_count(other), 0);
    }

    let server = close_server(cancelled_at_close_replies);
    let client = close_worker(&server).client;
    let error = client
        .complete_activity_task(
            "close-task",
            "close-attempt",
            "close-worker",
            json!(1),
            DEFAULT_CODEC,
        )
        .await
        .expect_err("late outcome is refused");
    assert!(activity_task_rejection_is_final(&error));
    let Error::ActivityTaskRejected(rejection) = error else {
        panic!("expected a typed activity rejection");
    };
    assert_eq!(
        (rejection.status, rejection.reason.as_str()),
        (409, "run_closed")
    );
    assert_eq!(rejection.body["recorded"], false);
}

#[tokio::test]
async fn abandoned_at_close_activity_keeps_running_and_its_unrecorded_outcome_settles() {
    let server = close_server(abandoned_at_close_replies);
    let continued = Arc::new(AtomicBool::new(false));
    let observed = Arc::clone(&continued);
    let mut worker = close_worker(&server);
    worker.register_activity("CloseActivity", move |ctx, _args| {
        let observed = Arc::clone(&observed);
        async move {
            let heartbeat = ctx.heartbeat(json!({"stage":"abandoned"})).await?;
            observed.store(!heartbeat.should_stop(), Ordering::SeqCst);
            Ok(json!("done"))
        }
    });

    assert!(matches!(
        worker
            .poll_activity_once()
            .await
            .expect("unrecorded outcome settles"),
        ManagedPollOutcome::Handled
    ));
    assert!(continued.load(Ordering::SeqCst));
    assert_eq!(server.request_count(COMPLETE), 1, "never retried");
    assert_eq!(server.request_count(FAIL), 0);
}
