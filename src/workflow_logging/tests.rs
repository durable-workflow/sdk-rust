use super::*;
use crate::{
    encode_value_envelope, json,
    testing::{WorkflowEnvironment, WorkflowHistory},
    AvroValue, Client, Error, HistoryEvent, Value, Worker, WorkflowFuture, WorkflowTask,
    DEFAULT_CODEC,
};
use std::{
    collections::BTreeMap,
    future::Future,
    sync::{Arc, Mutex},
    task::Poll,
    time::Duration,
};

#[derive(Debug, PartialEq)]
struct Record {
    level: tracing::Level,
    fields: BTreeMap<String, String>,
}

#[derive(Default)]
struct Log {
    records: Mutex<Vec<Record>>,
    panic: bool,
    on_event: Option<Box<dyn Fn() + Send + Sync>>,
}

struct Fields(BTreeMap<String, String>);
impl tracing::field::Visit for Fields {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.0.insert(field.name().into(), value.into());
    }
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.0.insert(field.name().into(), format!("{value:?}"));
    }
}

impl tracing::Subscriber for Log {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        if event.metadata().target() != "durable_workflow::workflow" {
            return;
        }
        assert!(!self.panic, "application logger failed");
        if let Some(callback) = &self.on_event {
            callback();
        }
        let mut fields = Fields(BTreeMap::new());
        event.record(&mut fields);
        self.records.lock().unwrap().push(Record {
            level: *event.metadata().level(),
            fields: fields.0,
        });
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}

fn event(kind: &str, payload: Value) -> HistoryEvent {
    serde_json::from_value(json!({"event_type":kind,"payload":payload})).unwrap()
}

fn timers(count: u64) -> Vec<HistoryEvent> {
    (1..=count).flat_map(|sequence| {
        let payload = json!({"sequence":sequence,"timer_id":format!("timer-{sequence}"),"delay_seconds":1});
        [event("TimerScheduled",payload.clone()),event("TimerFired",payload)]
    }).collect()
}

fn context(history: Vec<HistoryEvent>) -> WorkflowContext {
    let mut state = WorkflowState::new_with_identity(
        history,
        Some("order".into()),
        Some("run-1".into()),
        "tests".into(),
        DEFAULT_CODEC.into(),
        None,
    )
    .unwrap();
    state.observation_namespace = Some("shipping".into());
    WorkflowContext {
        state: Arc::new(Mutex::new(state)),
        cancellation_scope_id: "root".into(),
    }
}

fn task(history: Vec<HistoryEvent>) -> WorkflowTask {
    serde_json::from_value(json!({
        "task_id":"task-1","workflow_type":"logged","workflow_id":"order","run_id":"run-1",
        "payload_codec":DEFAULT_CODEC,"arguments":encode_value_envelope(&json!([]),DEFAULT_CODEC).unwrap(),
        "history_events":history
    })).unwrap()
}

fn worker(logging: bool) -> Worker {
    let client = Client::builder("http://127.0.0.1:9")
        .namespace("shipping")
        .build()
        .unwrap();
    let mut worker = Worker::new(client, "tests");
    worker.register_workflow("logged", move |ctx, _| {
        if logging {
            ctx.logger().info("factory");
        }
        async move {
            for step in 0..3 {
                if logging {
                    ctx.logger().info(&format!("step-{step}"));
                }
                ctx.sleep(Duration::from_secs(1)).await?;
            }
            if logging {
                ctx.logger().info("done");
            }
            Ok(json!("complete"))
        }
    });
    worker
}

fn messages(log: &Log) -> Vec<String> {
    log.records
        .lock()
        .unwrap()
        .iter()
        .map(|r| r.fields["message"].clone())
        .collect()
}

fn poll(ctx: &WorkflowContext, future: &mut WorkflowFuture) -> Poll<crate::Result<AvroValue>> {
    Worker::poll_workflow_future(ctx, future).unwrap()
}

#[test]
fn multistep_cold_worker_replacement_logs_only_new_regions_and_preserves_commands() {
    let log = Arc::new(Log::default());
    tracing::subscriber::with_default(log.clone(), || {
        for prefix in 0..=3 {
            // A new Worker and invocation have no process-local replay memory.
            let input = task(timers(prefix));
            let plain = worker(false).execute_workflow_task(input.clone()).unwrap();
            let logged = worker(true).execute_workflow_task(input).unwrap();
            assert_eq!(plain, logged);
        }
    });
    assert_eq!(
        messages(&log),
        ["factory", "step-0", "step-1", "step-2", "done"]
    );
    for record in log.records.lock().unwrap().iter() {
        assert_eq!(record.fields.len(), 4);
        assert_eq!(record.fields["namespace"], "shipping");
        assert_eq!(record.fields["workflow_instance_id"], "order");
        assert_eq!(record.fields["workflow_run_id"], "run-1");
    }
}

#[test]
fn uncommitted_attempt_retry_can_repeat_process_logs() {
    let log = Arc::new(Log::default());
    tracing::subscriber::with_default(log.clone(), || {
        for _ in 0..2 {
            worker(true).execute_workflow_task(task(vec![])).unwrap();
        }
    });
    assert_eq!(messages(&log), ["factory", "step-0", "factory", "step-0"]);
}

#[test]
fn live_accessor_tracks_consumption_without_history_budget_or_time_heuristics() {
    let ctx = context(timers(1));
    assert_eq!(ctx.replay_state(), WorkflowReplayState::Unavailable);
    let inner = ctx.clone();
    let mut future: WorkflowFuture = Box::pin(async move {
        assert_eq!(inner.replay_state(), WorkflowReplayState::Replaying);
        inner.sleep(Duration::from_secs(1)).await?;
        assert_eq!(inner.replay_state(), WorkflowReplayState::Executing);
        Ok(AvroValue::Null)
    });
    assert!(matches!(poll(&ctx, &mut future), Poll::Ready(Ok(_))));
    assert_eq!(ctx.replay_state(), WorkflowReplayState::Unavailable);
    assert!(ctx.take_commands().unwrap().is_empty());
}

#[test]
fn pending_recorded_future_does_not_turn_cursor_exhaustion_into_live_execution() {
    let ctx = context(vec![event(
        "TimerScheduled",
        json!({"sequence":1,"timer_id":"timer-1","delay_seconds":1}),
    )]);
    let inner = ctx.clone();
    let log = Arc::new(Log::default());
    let mut future: WorkflowFuture = Box::pin(async move {
        let mut timer = Box::pin(inner.sleep(Duration::from_secs(1)));
        futures_util::future::poll_fn(|cx| {
            assert!(timer.as_mut().poll(cx).is_pending());
            assert_eq!(inner.replay_state(), WorkflowReplayState::Replaying);
            inner.logger().info("pending is replay");
            Poll::Pending
        })
        .await
    });
    tracing::subscriber::with_default(log.clone(), || {
        assert!(poll(&ctx, &mut future).is_pending());
    });
    assert!(messages(&log).is_empty());
    assert!(ctx.take_commands().unwrap().is_empty());
}

#[test]
fn query_and_offline_reconstruction_stay_quiet_after_consuming_the_last_command() {
    let ctx = context(vec![]);
    let log = Arc::new(Log::default());
    tracing::subscriber::with_default(log.clone(), || {
        {
            let _query = replay_clock::Guard::query(&ctx);
            assert_eq!(ctx.replay_state(), WorkflowReplayState::Replaying);
            ctx.logger().warn("query");
        }
        let environment = WorkflowEnvironment::from_worker(worker(true));
        let history = WorkflowHistory::from_history(
            "logged",
            json!([]),
            json!({
                "workflow_id":"order","run_id":"run-1","events":timers(3)
            }),
        )
        .unwrap();
        // Missing a terminal record makes this a divergent offline replay, but
        // reconstruction must stay silent even while discovering that result.
        let _ = environment.replay(&history);
        {
            let _live = replay_clock::Guard::enter(&ctx);
            ctx.logger().info("live restored");
        }
    });
    assert_eq!(messages(&log), ["live restored"]);
}

#[test]
fn every_terminal_history_stays_quiet_with_an_exhausted_command_cursor() {
    let log = Arc::new(Log::default());
    tracing::subscriber::with_default(log.clone(), || {
        for kind in [
            "WorkflowCompleted",
            "WorkflowFailed",
            "WorkflowCancelled",
            "WorkflowTerminated",
            "WorkflowTimedOut",
        ] {
            let ctx = context(vec![event(kind, json!({}))]);
            let _live = replay_clock::Guard::enter(&ctx);
            assert_eq!(ctx.replay_state(), WorkflowReplayState::Replaying);
            ctx.logger().error(kind);
        }
    });
    assert!(messages(&log).is_empty());
}

#[test]
fn retained_context_and_future_drop_cannot_emit_outside_their_invocation() {
    struct OnDrop(WorkflowLogger);
    impl Drop for OnDrop {
        fn drop(&mut self) {
            self.0.info("discarded future");
        }
    }
    let ctx = context(vec![]);
    let retained = ctx.logger();
    let inner = ctx.clone();
    let log = Arc::new(Log::default());
    tracing::subscriber::with_default(log.clone(), || {
        let mut future: WorkflowFuture = Box::pin(async move {
            let _on_drop = OnDrop(inner.logger());
            inner.logger().info("live");
            futures_util::future::pending().await
        });
        assert!(poll(&ctx, &mut future).is_pending());
        drop(future);
        retained.info("after disposal");
    });
    assert_eq!(messages(&log), ["live"]);
}

#[test]
fn nested_invocations_restore_identity_and_all_levels_use_host_tracing() {
    let a = context(vec![]);
    let b = context(vec![]);
    b.state.lock().unwrap().run_id = Some("run-2".into());
    let log = Arc::new(Log::default());
    tracing::subscriber::with_default(log.clone(), || {
        let _outer = replay_clock::Guard::enter(&a);
        a.logger().trace("trace");
        {
            let _inner = replay_clock::Guard::enter(&b);
            a.logger().error("wrong invocation");
            b.logger().debug("debug");
        }
        a.logger().info("info");
        a.logger().warn("warn");
        a.logger().error("error");
    });
    assert_eq!(messages(&log), ["trace", "debug", "info", "warn", "error"]);
    let records = log.records.lock().unwrap();
    assert_eq!(
        records.iter().map(|r| r.level).collect::<Vec<_>>(),
        [
            tracing::Level::TRACE,
            tracing::Level::DEBUG,
            tracing::Level::INFO,
            tracing::Level::WARN,
            tracing::Level::ERROR
        ]
    );
    assert_eq!(records[1].fields["workflow_run_id"], "run-2");
    assert_eq!(records[2].fields["workflow_run_id"], "run-1");
}

#[test]
fn missing_run_identity_is_silent_instead_of_inventing_identity() {
    let log = Arc::new(Log::default());
    tracing::subscriber::with_default(log.clone(), || {
        for field in 0..3 {
            let ctx = context(vec![]);
            {
                let mut state = ctx.state.lock().unwrap();
                match field {
                    0 => state.observation_namespace = None,
                    1 => state.workflow_id = None,
                    _ => state.run_id = None,
                }
            }
            let _live = replay_clock::Guard::enter(&ctx);
            ctx.logger().info("unbound");
        }
    });
    assert!(messages(&log).is_empty());
}

#[test]
fn host_logger_panics_preserve_commands_results_and_restore_invocation_state() {
    let log = Arc::new(Log {
        panic: true,
        ..Log::default()
    });
    tracing::subscriber::with_default(log, || {
        for prefix in 0..=3 {
            let input = task(timers(prefix));
            assert_eq!(
                worker(true).execute_workflow_task(input.clone()).unwrap(),
                worker(false).execute_workflow_task(input).unwrap()
            );
        }
        let ctx = context(vec![]);
        let _live = replay_clock::Guard::enter(&ctx);
        ctx.logger().info("panic");
        assert_eq!(ctx.replay_state(), WorkflowReplayState::Executing);
        assert!(ctx.state.lock().is_ok());
    });
}

#[test]
fn logging_does_not_turn_application_failure_into_success() {
    let mut worker = worker(false);
    worker.register_workflow("fails", |ctx, _| async move {
        ctx.logger().error("failed");
        Err(Error::WorkerLoop("business failure".into()))
    });
    let mut input = task(vec![]);
    input.workflow_type = "fails".into();
    let log = Arc::new(Log::default());
    let commands = tracing::subscriber::with_default(log.clone(), || {
        worker.execute_workflow_task(input).unwrap()
    });
    assert_eq!(messages(&log), ["failed"]);
    assert_eq!(commands.len(), 1);
    assert_eq!(commands[0]["type"], "fail_workflow");
}

#[test]
fn synchronous_recorded_side_effect_and_version_markers_use_the_live_cursor() {
    let ctx = context(vec![
        event(
            "SideEffectRecorded",
            json!({"sequence":1,"result":encode_value_envelope(&json!(7),DEFAULT_CODEC).unwrap()}),
        ),
        event(
            "VersionMarkerRecorded",
            json!({"sequence":2,"change_id":"shipping","version":1,"min_supported":-1,"max_supported":1}),
        ),
    ]);
    let log = Arc::new(Log::default());
    tracing::subscriber::with_default(log.clone(), || {
        let _live = replay_clock::Guard::enter(&ctx);
        ctx.logger().info("committed prefix");
        let value: i32 = ctx
            .side_effect(|| panic!("recorded callback must not run"))
            .unwrap();
        assert_eq!(value, 7);
        assert_eq!(ctx.replay_state(), WorkflowReplayState::Replaying);
        assert_eq!(ctx.get_version("shipping", -1, 1).unwrap(), 1);
        assert_eq!(ctx.replay_state(), WorkflowReplayState::Executing);
        ctx.logger().info("after markers");
    });
    assert_eq!(messages(&log), ["after markers"]);
    assert!(ctx.take_commands().unwrap().is_empty());

    let ctx = context(timers(1));
    let _live = replay_clock::Guard::enter(&ctx);
    assert_eq!(ctx.get_version("added-later", -1, 1).unwrap(), -1);
    assert_eq!(ctx.replay_state(), WorkflowReplayState::Replaying);
    assert!(ctx.take_commands().unwrap().is_empty());
}

#[test]
fn nested_parallel_completion_releases_the_observation_boundary() {
    use crate::ParallelOperation;
    let operations = || {
        vec![
            ParallelOperation::timer(Duration::from_secs(1)),
            ParallelOperation::group(vec![ParallelOperation::timer(Duration::from_secs(1))]),
        ]
    };
    let mut history = vec![];
    for (index, descriptor) in crate::parallel_descriptors(operations(), 1)
        .unwrap()
        .iter()
        .enumerate()
    {
        let sequence = index as u64 + 1;
        let mut payload =
            json!({"sequence":sequence,"timer_id":format!("timer-{sequence}"),"delay_seconds":1});
        crate::apply_parallel_group_path(payload.as_object_mut().unwrap(), &descriptor.group_path);
        history.extend([
            event("TimerScheduled", payload.clone()),
            event("TimerFired", payload),
        ]);
    }
    let ctx = context(history);
    let inner = ctx.clone();
    let mut future: WorkflowFuture = Box::pin(async move {
        assert_eq!(inner.replay_state(), WorkflowReplayState::Replaying);
        inner.parallel(operations()).await?;
        assert_eq!(inner.replay_state(), WorkflowReplayState::Executing);
        Ok(AvroValue::Null)
    });
    assert!(matches!(poll(&ctx, &mut future), Poll::Ready(Ok(_))));
    assert!(ctx.take_commands().unwrap().is_empty());
}

#[test]
fn a_completed_parallel_member_does_not_release_a_pending_aggregate_boundary() {
    use crate::ParallelOperation;
    let operations = || {
        vec![
            ParallelOperation::timer(Duration::from_secs(1)),
            ParallelOperation::timer(Duration::from_secs(1)),
        ]
    };
    let mut history = vec![];
    for (index, descriptor) in crate::parallel_descriptors(operations(), 1)
        .unwrap()
        .iter()
        .enumerate()
    {
        let sequence = index as u64 + 1;
        let mut payload =
            json!({"sequence":sequence,"timer_id":format!("timer-{sequence}"),"delay_seconds":1});
        crate::apply_parallel_group_path(payload.as_object_mut().unwrap(), &descriptor.group_path);
        history.push(event("TimerScheduled", payload.clone()));
        if index == 1 {
            history.push(event("TimerFired", payload));
        }
    }
    let ctx = context(history);
    let inner = ctx.clone();
    let mut future: WorkflowFuture = Box::pin(async move {
        let mut call = Box::pin(inner.parallel(operations()));
        futures_util::future::poll_fn(|cx| {
            assert!(call.as_mut().poll(cx).is_pending());
            assert_eq!(inner.replay_state(), WorkflowReplayState::Replaying);
            inner.logger().info("pending aggregate");
            Poll::Pending
        })
        .await
    });
    let log = Arc::new(Log::default());
    tracing::subscriber::with_default(
        log.clone(),
        || assert!(poll(&ctx, &mut future).is_pending()),
    );
    assert!(messages(&log).is_empty());
    assert!(ctx.take_commands().unwrap().is_empty());
}

fn selection(ctx: &WorkflowContext) -> crate::SelectCall {
    use crate::{ActivityOptions, ParallelOperation};
    ctx.select_keyed(vec![
        (
            "slow",
            ParallelOperation::activity_with_options(
                "slow-activity",
                ActivityOptions::new().task_queue("default"),
                json!([]),
            ),
        ),
        (
            "fast",
            ParallelOperation::activity_with_options(
                "fast-activity",
                ActivityOptions::new().task_queue("default"),
                json!([]),
            ),
        ),
    ])
}

#[test]
fn selection_marker_releases_live_logging_even_with_an_unfinished_loser() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../tests/fixtures/durable_selection_runtime_history.json"
    ))
    .unwrap();
    let mut history: Vec<HistoryEvent> =
        serde_json::from_value(fixture["history"].clone()).unwrap();
    history.retain(|e| !(e.event_type == "ActivityCompleted" && e.payload["sequence"] == 1));
    let ctx = context(history);
    let inner = ctx.clone();
    let mut future: WorkflowFuture = Box::pin(async move {
        inner.logger().info("before selection");
        selection(&inner).await?;
        assert_eq!(inner.replay_state(), WorkflowReplayState::Executing);
        inner.logger().info("selected");
        Ok(AvroValue::Null)
    });
    let log = Arc::new(Log::default());
    tracing::subscriber::with_default(log.clone(), || {
        assert!(matches!(poll(&ctx, &mut future), Poll::Ready(Ok(_))))
    });
    assert_eq!(messages(&log), ["selected"]);
    assert!(ctx.take_commands().unwrap().is_empty());
}

#[test]
fn terminal_members_without_a_committed_selection_marker_remain_replay() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../tests/fixtures/durable_selection_runtime_history.json"
    ))
    .unwrap();
    let mut history: Vec<HistoryEvent> =
        serde_json::from_value(fixture["history"].clone()).unwrap();
    history.retain(|e| e.event_type != "SelectionResolved");
    let ctx = context(history);
    let inner = ctx.clone();
    let mut future: WorkflowFuture = Box::pin(async move {
        let mut call = Box::pin(selection(&inner));
        futures_util::future::poll_fn(|cx| {
            assert!(call.as_mut().poll(cx).is_pending());
            assert_eq!(inner.replay_state(), WorkflowReplayState::Replaying);
            inner.logger().info("awaiting marker");
            Poll::Pending
        })
        .await
    });
    let log = Arc::new(Log::default());
    tracing::subscriber::with_default(
        log.clone(),
        || assert!(poll(&ctx, &mut future).is_pending()),
    );
    assert!(messages(&log).is_empty());
    assert!(ctx.take_commands().unwrap().is_empty());
}

#[test]
fn canonical_cancellation_is_replayed_until_its_authored_delivery_is_consumed() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../tests/fixtures/replay-regressions/cooperative-cold-activity-delivery.json"
    ))
    .unwrap();
    let state = WorkflowState::new_with_identity(
        serde_json::from_value(fixture["history"].clone()).unwrap(),
        Some("order".into()),
        Some("regression-corpus-run".into()),
        "tests".into(),
        DEFAULT_CODEC.into(),
        None,
    )
    .unwrap();
    let ctx = WorkflowContext {
        state: Arc::new(Mutex::new(state)),
        cancellation_scope_id: "root".into(),
    };
    let inner = ctx.clone();
    let mut future: WorkflowFuture = Box::pin(async move {
        assert_eq!(inner.replay_state(), WorkflowReplayState::Replaying);
        assert!(inner.activity("prepare", json!([])).await.is_err());
        assert_eq!(inner.replay_state(), WorkflowReplayState::Executing);
        Ok(AvroValue::Null)
    });
    assert!(matches!(poll(&ctx, &mut future), Poll::Ready(Ok(_))));
    assert!(ctx.take_commands().unwrap().is_empty());
}

#[test]
fn continued_history_stays_quiet_after_its_continuation_is_consumed() {
    let ctx = context(vec![event(
        "WorkflowContinuedAsNew",
        json!({"sequence":1,"continued_to_run_id":"run-2"}),
    )]);
    let _live = replay_clock::Guard::enter(&ctx);
    assert!(ctx.continue_as_new(json!([])).is_err());
    assert_eq!(ctx.replay_state(), WorkflowReplayState::Replaying);
    assert!(ctx.take_commands().unwrap().is_empty());
}

#[tokio::test]
async fn actual_instance_state_query_reconstruction_never_emits_live_workflow_logs() {
    let mut worker = worker(false);
    worker.register_replayed_workflow(
        "stateful",
        || 0_u8,
        |ctx, _, state| async move {
            ctx.logger().info("entry");
            ctx.sleep(Duration::from_secs(1)).await?;
            state.update(|n| *n = 1)?;
            ctx.logger().info("after last history boundary");
            Ok(Value::Null)
        },
    );
    worker.register_replayed_query::<u8, _, _>("stateful", "current", |_, state, _| async move {
        Ok(json!(*state))
    });
    let query: crate::QueryTask = serde_json::from_value(json!({
        "query_task_id":"query-1","workflow_type":"stateful","query_name":"current",
        "workflow_id":"order","run_id":"run-1","payload_codec":DEFAULT_CODEC,
        "workflow_arguments":encode_value_envelope(&json!([]),DEFAULT_CODEC).unwrap(),
        "query_arguments":encode_value_envelope(&json!([]),DEFAULT_CODEC).unwrap(),
        "history_events":timers(1),"run_status":"running"
    }))
    .unwrap();
    let log = Arc::new(Log::default());
    let _subscriber = tracing::subscriber::set_default(log.clone());
    for _ in 0..2 {
        assert_eq!(
            worker
                .execute_query_task(query.clone())
                .await
                .unwrap()
                .into_json()
                .unwrap(),
            json!(1)
        );
    }
    assert!(messages(&log).is_empty());
}

#[test]
fn subscriber_runs_without_a_workflow_lock_and_recursive_workflow_logs_are_silent() {
    let ctx = context(vec![]);
    let callback_ctx = ctx.clone();
    let log = Arc::new(Log {
        on_event: Some(Box::new(move || {
            assert!(callback_ctx.state.try_lock().is_ok());
            assert_eq!(
                callback_ctx.replay_state(),
                WorkflowReplayState::Unavailable
            );
            callback_ctx.logger().warn("recursive");
        })),
        ..Log::default()
    });
    tracing::subscriber::with_default(log.clone(), || {
        let _live = replay_clock::Guard::enter(&ctx);
        ctx.logger().info("outer");
        assert_eq!(ctx.replay_state(), WorkflowReplayState::Executing);
    });
    assert_eq!(messages(&log), ["outer"]);
}
