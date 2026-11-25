//! durable-workflow/.github#226: ordered worker interceptors around workflow
//! tasks, activities and queries. Workflow-task interceptors observe replay
//! state and cannot change what the worker submits.
use super::*;
use crate::interceptors::{
    ActivityInterceptorContext, ActivityNext, InterceptorFuture, QueryTaskInterceptorContext,
    QueryTaskNext, WorkflowTaskInterceptorContext, WorkflowTaskNext, WorkflowTaskOutcome,
};

type Events = Arc<Mutex<Vec<String>>>;

fn events() -> Events {
    Arc::new(Mutex::new(Vec::new()))
}

fn take(events: &Events) -> Vec<String> {
    std::mem::take(&mut *events.lock().unwrap())
}

/// Records `<name>:<hook>:before|after` around every hook.
struct Recorder {
    name: &'static str,
    events: Events,
}

impl Recorder {
    fn push(&self, event: String) {
        self.events.lock().unwrap().push(event);
    }
}

impl WorkerInterceptor for Recorder {
    fn execute_workflow_task<'a>(
        &'a self,
        context: &'a WorkflowTaskInterceptorContext,
        next: WorkflowTaskNext<'a>,
    ) -> InterceptorFuture<'a, Result<WorkflowTaskOutcome>> {
        Box::pin(async move {
            self.push(format!(
                "{}:workflow:before:{:?}",
                self.name,
                context.replay_state()
            ));
            let outcome = next.run().await;
            let observed = match &outcome {
                Ok(outcome) => format!("{} commands", outcome.commands().len()),
                Err(error) => format!("error {error}"),
            };
            self.push(format!("{}:workflow:after:{observed}", self.name));
            outcome
        })
    }

    fn execute_activity(
        &self,
        context: ActivityInterceptorContext,
        next: ActivityNext,
    ) -> InterceptorFuture<'_, Result<AvroValue>> {
        Box::pin(async move {
            self.push(format!("{}:activity:before", self.name));
            let result = next.run(context).await;
            self.push(format!("{}:activity:after:{}", self.name, result.is_ok()));
            result
        })
    }

    fn execute_query_task<'a>(
        &'a self,
        context: &'a QueryTaskInterceptorContext,
        next: QueryTaskNext<'a>,
    ) -> InterceptorFuture<'a, std::result::Result<AvroValue, QueryTaskFailure>> {
        Box::pin(async move {
            self.push(format!(
                "{}:query:before:{}",
                self.name,
                context.query_name()
            ));
            let result = next.run().await;
            self.push(format!("{}:query:after:{}", self.name, result.is_ok()));
            result
        })
    }
}

fn recorded_worker(events: &Events) -> Worker {
    Worker::new(
        Client::builder("http://127.0.0.1:9")
            .namespace("shipping")
            .build()
            .unwrap(),
        "orders",
    )
    .worker_id("interceptor-worker")
    .interceptor(Recorder {
        name: "outer",
        events: events.clone(),
    })
    .interceptors([Recorder {
        name: "inner",
        events: events.clone(),
    }])
}

fn plain_worker() -> Worker {
    Worker::new(Client::new("http://127.0.0.1:9").unwrap(), "orders")
        .worker_id("interceptor-worker")
}

fn register_ship_order(worker: &mut Worker) {
    worker.register_workflow("ship-order", |ctx, _| async move {
        ctx.activity("ship", json!(["parcel"])).await
    });
}

fn workflow_task(history: Vec<HistoryEvent>) -> WorkflowTask {
    serde_json::from_value(json!({
        "task_id": "workflow-task-1", "workflow_id": "order-1", "run_id": "run-1",
        "workflow_type": "ship-order", "payload_codec": "avro", "workflow_task_attempt": 2,
        "arguments": fixture_envelope(json!([])),
        "history_events": history,
    }))
    .unwrap()
}

/// History of a run whose first task scheduled `ship` and whose activity
/// completed: the next task replays that committed command.
fn replayed_history() -> Vec<HistoryEvent> {
    vec![
        history_event(
            "ActivityScheduled",
            json!({"sequence": 1, "activity_type": "ship"}),
        ),
        history_event(
            "ActivityCompleted",
            json!({"sequence": 1, "activity_type": "ship", "payload_codec": DEFAULT_CODEC,
                "result": fixture_envelope(json!("shipped"))}),
        ),
    ]
}

#[tokio::test]
async fn workflow_task_interceptors_run_in_order_and_see_the_submitted_commands() {
    let events = events();
    let mut worker = recorded_worker(&events);
    register_ship_order(&mut worker);
    let mut plain = plain_worker();
    register_ship_order(&mut plain);

    let task = workflow_task(Vec::new());
    let expected = plain
        .execute_workflow_with_local_activities(task.clone())
        .await
        .unwrap()
        .unwrap();
    let decision = worker
        .execute_workflow_with_local_activities(task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(decision.commands, expected.commands);
    assert_eq!(decision.commands[0]["type"], "schedule_activity");
    assert_eq!(
        take(&events),
        [
            "outer:workflow:before:Executing",
            "inner:workflow:before:Executing",
            "inner:workflow:after:1 commands",
            "outer:workflow:after:1 commands",
        ]
    );
}

#[tokio::test]
async fn workflow_task_context_carries_the_task_identity_and_replay_state() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    struct Capture(Arc<Mutex<Vec<WorkflowTaskInterceptorContext>>>);
    impl WorkerInterceptor for Capture {
        fn execute_workflow_task<'a>(
            &'a self,
            context: &'a WorkflowTaskInterceptorContext,
            next: WorkflowTaskNext<'a>,
        ) -> InterceptorFuture<'a, Result<WorkflowTaskOutcome>> {
            self.0.lock().unwrap().push(context.clone());
            next.run()
        }
    }
    let mut worker = plain_worker().interceptor(Capture(seen.clone()));
    register_ship_order(&mut worker);

    let first = worker
        .execute_workflow_with_local_activities(workflow_task(Vec::new()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.commands[0]["type"], "schedule_activity");
    let replayed = worker
        .execute_workflow_with_local_activities(workflow_task(replayed_history()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(replayed.commands[0]["type"], "complete_workflow");

    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 2, "interceptors run on replayed tasks too");
    let context = &seen[0];
    assert_eq!(context.worker_id(), "interceptor-worker");
    assert_eq!(context.task_queue(), "orders");
    assert_eq!(context.namespace(), "default");
    assert_eq!(context.task_id(), "workflow-task-1");
    assert_eq!(context.workflow_id(), Some("order-1"));
    assert_eq!(context.run_id(), Some("run-1"));
    assert_eq!(context.workflow_type(), "ship-order");
    assert_eq!(context.attempt(), 2);
    assert_eq!(context.history_event_count(), 0);
    assert_eq!(context.replay_state(), WorkflowReplayState::Executing);
    assert_eq!(seen[1].history_event_count(), 2);
    assert_eq!(seen[1].replay_state(), WorkflowReplayState::Replaying);
}

#[tokio::test]
async fn a_core_workflow_task_error_reaches_the_worker_unchanged() {
    let events = events();
    // No workflow registered: the core fails with WorkflowNotRegistered.
    let worker = recorded_worker(&events);
    let error = worker
        .execute_workflow_with_local_activities(workflow_task(Vec::new()))
        .await
        .expect_err("the core error is reported");
    assert!(
        matches!(&error, Error::WorkflowNotRegistered(name) if name == "ship-order"),
        "{error:?}"
    );
    let events = take(&events);
    assert_eq!(events.len(), 4, "{events:?}");
    assert!(
        events[2].starts_with("inner:workflow:after:error "),
        "{events:?}"
    );
}

/// Replaces any core result with its own: an error, or a stashed outcome.
struct Tamper {
    mode: &'static str,
    stash: Arc<Mutex<Option<WorkflowTaskOutcome>>>,
}

impl WorkerInterceptor for Tamper {
    fn execute_workflow_task<'a>(
        &'a self,
        context: &'a WorkflowTaskInterceptorContext,
        next: WorkflowTaskNext<'a>,
    ) -> InterceptorFuture<'a, Result<WorkflowTaskOutcome>> {
        let _ = context;
        Box::pin(async move {
            match self.mode {
                // Keep the outcome after returning and hand back an error.
                "fail-after-core" => {
                    let outcome = next.run().await?;
                    *self.stash.lock().unwrap() = Some(outcome);
                    Err(Error::WorkerLoop("audit sink unavailable".into()))
                }
                // Retain the view of the commands beyond the task.
                "retain" => {
                    let outcome = next.run().await?;
                    *self.stash.lock().unwrap() = Some(outcome.clone());
                    Ok(outcome)
                }
                // Swallow a core error by returning an earlier task's outcome.
                "swallow" => match next.run().await {
                    Ok(outcome) => Ok(outcome),
                    Err(_) => Ok(self.stash.lock().unwrap().take().expect("stashed outcome")),
                },
                // Never run the core; answer with an earlier outcome.
                "skip" => {
                    drop(next);
                    Ok(self.stash.lock().unwrap().take().expect("stashed outcome"))
                }
                // Create the core future but drop it before it is polled.
                "abandon" => {
                    drop(next.run());
                    Err(Error::WorkerLoop("abandoned".into()))
                }
                // Fail without running the rest of the chain.
                "fail-before-next" => {
                    drop(next);
                    Err(Error::WorkerLoop("tenant lookup failed".into()))
                }
                // Let the core begin, then drop it partway through.
                "abandon-started" => {
                    let mut core = next.run();
                    std::future::poll_fn(|cx| {
                        let _ = core.as_mut().poll(cx);
                        Poll::Ready(())
                    })
                    .await;
                    drop(core);
                    Err(Error::WorkerLoop("gave up".into()))
                }
                _ => unreachable!(),
            }
        })
    }
}

fn tamper_worker(mode: &'static str, stash: &Arc<Mutex<Option<WorkflowTaskOutcome>>>) -> Worker {
    plain_worker().interceptor(Tamper {
        mode,
        stash: stash.clone(),
    })
}

/// An outcome from a completed task, used to try to forge another result.
async fn stashed_outcome() -> Arc<Mutex<Option<WorkflowTaskOutcome>>> {
    let stash = Arc::new(Mutex::new(None));
    let mut worker = tamper_worker("retain", &stash);
    register_ship_order(&mut worker);
    worker
        .execute_workflow_with_local_activities(workflow_task(Vec::new()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stash.lock().unwrap().as_ref().unwrap().commands().len(), 1);
    stash
}

#[tokio::test]
async fn a_retained_outcome_cannot_change_the_submitted_commands() {
    let stash = Arc::new(Mutex::new(None));
    let mut worker = tamper_worker("retain", &stash);
    register_ship_order(&mut worker);
    let mut plain = plain_worker();
    register_ship_order(&mut plain);
    let task = workflow_task(replayed_history());
    let expected = plain
        .execute_workflow_with_local_activities(task.clone())
        .await
        .unwrap()
        .unwrap();
    let decision = worker
        .execute_workflow_with_local_activities(task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(decision.commands, expected.commands);
    assert_eq!(
        stash.lock().unwrap().as_ref().unwrap().commands(),
        expected.commands.as_slice()
    );
}

#[tokio::test]
async fn an_interceptor_cannot_swallow_a_core_error_with_another_outcome() {
    let stash = stashed_outcome().await;
    // Unregistered workflow: the core fails.
    let worker = tamper_worker("swallow", &stash);
    let error = worker
        .execute_workflow_with_local_activities(workflow_task(Vec::new()))
        .await
        .expect_err("the core failure is reported");
    assert!(
        matches!(error, Error::WorkflowNotRegistered(_)),
        "{error:?}"
    );
    assert!(
        stash.lock().unwrap().is_none(),
        "the forged outcome was used"
    );
}

#[tokio::test]
async fn skipping_next_fails_the_workflow_task_without_running_workflow_code() {
    let stash = stashed_outcome().await;
    let runs = Arc::new(AtomicUsize::new(0));
    let mut worker = plain_worker()
        .interceptor(PassthroughWorkerInterceptor)
        .interceptor(Tamper {
            mode: "skip",
            stash: stash.clone(),
        });
    let counted = runs.clone();
    worker.register_workflow("ship-order", move |ctx, _| {
        counted.fetch_add(1, Ordering::SeqCst);
        async move { ctx.activity("ship", json!([])).await }
    });
    let error = worker
        .execute_workflow_with_local_activities(workflow_task(Vec::new()))
        .await
        .expect_err("skipping next fails the task");
    let Error::WorkflowTaskInterceptor(message) = &error else {
        panic!("expected an interceptor failure, got {error:?}");
    };
    assert!(
        message.ends_with("Tamper returned without calling next"),
        "{message}"
    );
    assert!(
        message.starts_with("workflow task interceptor "),
        "{message}"
    );
    assert_eq!(runs.load(Ordering::SeqCst), 0);

    let worker = tamper_worker("abandon", &stash);
    let error = worker
        .execute_workflow_with_local_activities(workflow_task(Vec::new()))
        .await
        .expect_err("an abandoned core fails the task");
    assert!(
        matches!(&error, Error::WorkflowTaskInterceptor(message) if message.contains("without calling next")),
        "{error:?}"
    );

    // An outer interceptor that catches the violation and answers with an
    // earlier task's outcome cannot hide it: the chain never ran the core.
    let stash = stashed_outcome().await;
    let mut worker = plain_worker()
        .interceptor(Tamper {
            mode: "swallow",
            stash: stash.clone(),
        })
        .interceptor(Tamper {
            mode: "abandon",
            stash: stash.clone(),
        });
    register_ship_order(&mut worker);
    let error = worker
        .execute_workflow_with_local_activities(workflow_task(Vec::new()))
        .await
        .expect_err("a caught violation still fails the task");
    assert!(
        matches!(&error, Error::WorkflowTaskInterceptor(message) if message.contains("without calling next")),
        "{error:?}"
    );
    assert!(
        stash.lock().unwrap().is_none(),
        "the outer interceptor swallowed the error"
    );
}

#[tokio::test]
async fn an_interceptor_error_after_the_core_fails_the_workflow_task_not_the_workflow() {
    let stash = Arc::new(Mutex::new(None));
    let mut worker = tamper_worker("fail-after-core", &stash);
    register_ship_order(&mut worker);
    let error = worker
        .execute_workflow_with_local_activities(workflow_task(Vec::new()))
        .await
        .expect_err("the interceptor error fails the task");
    assert!(
        matches!(&error, Error::WorkflowTaskInterceptor(message)
            if message == "workflow task interceptor failed: worker loop error: audit sink unavailable"),
        "{error:?}"
    );
}

fn failing_interceptor_server(
    path: &str,
    _body: &str,
    request: usize,
) -> Option<(&'static str, String)> {
    if path == "/api/worker/workflow-tasks/poll" && request == 1 {
        let task = json!({"task_id":"intercepted-task","workflow_id":"order-1","run_id":"run-1",
            "workflow_type":"ship-order","payload_codec":"avro","arguments":fixture_envelope(json!([])),
            "history_events":[],"workflow_task_attempt":1,"lease_owner":"interceptor-worker"});
        return Some(("200 OK", json!({"task":task}).to_string()));
    }
    match path {
        "/api/worker/workflow-tasks/intercepted-task/fail"
        | "/api/worker/workflow-tasks/intercepted-task/complete" => Some((
            "200 OK",
            json!({"task_id":"intercepted-task","workflow_task_attempt":1,"recorded":true})
                .to_string(),
        )),
        _ => None,
    }
}

#[tokio::test]
async fn the_managed_worker_reports_an_interceptor_failure_as_a_workflow_task_failure() {
    let server = MockWorkerServer::start_with_behavior(MockWorkerBehavior {
        request_override: Some(failing_interceptor_server),
        ..MockWorkerBehavior::default()
    });
    let client = Client::builder(server.base_url())
        .timeout(Duration::from_secs(2))
        .build()
        .expect("client");
    let stash = Arc::new(Mutex::new(None));
    let mut worker = Worker::new(client, "orders")
        .worker_id("interceptor-worker")
        .poll_timeout(Duration::from_millis(10))
        .interceptor(Tamper {
            mode: "fail-after-core",
            stash: stash.clone(),
        });
    register_ship_order(&mut worker);
    assert_eq!(
        worker.poll_workflow_once().await.expect("settled"),
        ManagedPollOutcome::Handled
    );
    assert_eq!(
        server.request_count("/api/worker/workflow-tasks/intercepted-task/complete"),
        0,
        "no command is recorded"
    );
    let failures = server.request_bodies("/api/worker/workflow-tasks/intercepted-task/fail");
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0]["lease_owner"], "interceptor-worker");
    assert!(
        failures[0].to_string().contains("audit sink unavailable"),
        "{}",
        failures[0]
    );
    assert!(
        stash.lock().unwrap().is_some(),
        "the core ran before the interceptor failed"
    );
}

fn activity_task() -> ActivityTask {
    serde_json::from_value(json!({
        "task_id": "activity-task-1", "activity_type": "ship", "activity_attempt_id": "attempt-1",
        "workflow_id": "order-1", "run_id": "run-1",
        "attempt_number": 3, "payload_codec": "avro",
        "arguments": fixture_envelope(json!(["parcel"])),
    }))
    .unwrap()
}

#[tokio::test]
async fn activity_interceptors_run_in_order_and_errors_pass_through() {
    let events = events();
    let mut worker = recorded_worker(&events);
    worker.register_activity("ship", |_, args| async move {
        if args == json!(["fail"]) {
            return Err(Error::WorkerLoop("carrier down".into()));
        }
        Ok(json!({"shipped": args}))
    });
    let result = worker.execute_activity_task(activity_task()).await.unwrap();
    assert_eq!(result.into_json().unwrap(), json!({"shipped": ["parcel"]}));
    assert_eq!(
        take(&events),
        [
            "outer:activity:before",
            "inner:activity:before",
            "inner:activity:after:true",
            "outer:activity:after:true",
        ]
    );

    let mut failing = activity_task();
    failing.arguments = Some(fixture_envelope(json!(["fail"])));
    let error = worker.execute_activity_task(failing).await.unwrap_err();
    assert!(
        matches!(&error, Error::WorkerLoop(message) if message == "carrier down"),
        "{error:?}"
    );
    assert_eq!(
        take(&events),
        [
            "outer:activity:before",
            "inner:activity:before",
            "inner:activity:after:false",
            "outer:activity:after:false",
        ]
    );
}

#[tokio::test]
async fn activity_interceptors_see_the_task_and_may_transform_arguments_and_results() {
    struct Rewrite(Arc<Mutex<Vec<String>>>);
    impl WorkerInterceptor for Rewrite {
        fn execute_activity(
            &self,
            context: ActivityInterceptorContext,
            next: ActivityNext,
        ) -> InterceptorFuture<'_, Result<AvroValue>> {
            self.0.lock().unwrap().push(format!(
                "{}/{}/{}/{}/{}/{:?}/{:?}/{}/{}",
                context.worker_id(),
                context.task_queue(),
                context.namespace(),
                context.task_id(),
                context.activity_type(),
                context.workflow_id(),
                context.run_id(),
                context.attempt(),
                context.is_local(),
            ));
            let args = AvroValue::from_serialize(&json!(["rewritten"])).unwrap();
            Box::pin(async move {
                let result = next.run(context.with_args(args)).await?;
                let mut value = result.into_json()?;
                value["intercepted"] = json!(true);
                AvroValue::from_serialize(&value)
            })
        }
    }
    let seen = Arc::new(Mutex::new(Vec::new()));
    let mut worker = plain_worker().interceptor(Rewrite(seen.clone()));
    worker.register_activity("ship", |_, args| async move { Ok(json!({"args": args})) });
    let result = worker.execute_activity_task(activity_task()).await.unwrap();
    assert_eq!(
        result.into_json().unwrap(),
        json!({"args": ["rewritten"], "intercepted": true})
    );
    assert_eq!(
        *seen.lock().unwrap(),
        [
            r#"interceptor-worker/orders/default/activity-task-1/ship/Some("order-1")/Some("run-1")/3/false"#
        ]
    );
}

fn query_task(query_name: &str) -> QueryTask {
    serde_json::from_value(json!({
        "query_task_id": "query-task-1", "query_task_attempt": 1,
        "workflow_id": "order-1", "run_id": "run-1", "workflow_type": "ship-order",
        "query_name": query_name, "payload_codec": "avro",
        "workflow_arguments": fixture_envelope(json!([])),
        "query_arguments": fixture_envelope(json!([])),
        "history_events": [],
    }))
    .unwrap()
}

#[tokio::test]
async fn query_interceptors_run_in_order_and_failures_pass_through() {
    let events = events();
    let mut worker = recorded_worker(&events);
    register_ship_order(&mut worker);
    worker.register_query("ship-order", "status", |_, _| async { Ok(json!("open")) });

    let value = worker
        .execute_query_task(query_task("status"))
        .await
        .unwrap();
    assert_eq!(value.into_json().unwrap(), json!("open"));
    assert_eq!(
        take(&events),
        [
            "outer:query:before:status",
            "inner:query:before:status",
            "inner:query:after:true",
            "outer:query:after:true",
        ]
    );

    let failure = worker
        .execute_query_task(query_task("missing"))
        .await
        .unwrap_err();
    assert_eq!(failure.reason, "rejected_unknown_query");
    assert_eq!(failure.failure_type, "QueryFailed");
    assert_eq!(
        take(&events),
        [
            "outer:query:before:missing",
            "inner:query:before:missing",
            "inner:query:after:false",
            "outer:query:after:false",
        ]
    );
}

#[tokio::test]
async fn query_interceptors_may_handle_a_failure_deliberately() {
    struct Fallback;
    impl WorkerInterceptor for Fallback {
        fn execute_query_task<'a>(
            &'a self,
            context: &'a QueryTaskInterceptorContext,
            next: QueryTaskNext<'a>,
        ) -> InterceptorFuture<'a, std::result::Result<AvroValue, QueryTaskFailure>> {
            Box::pin(async move {
                assert_eq!(context.workflow_type(), "ship-order");
                assert_eq!(context.workflow_id(), Some("order-1"));
                match next.run().await {
                    Err(failure) if failure.reason() == "rejected_unknown_query" => Err(
                        QueryTaskFailure::new("audited", failure.message(), "AuditedFailure"),
                    ),
                    other => other,
                }
            })
        }
    }
    let mut worker = plain_worker().interceptor(Fallback);
    register_ship_order(&mut worker);
    worker.register_query("ship-order", "status", |_, _| async { Ok(json!("open")) });
    let failure = worker
        .execute_query_task(query_task("missing"))
        .await
        .unwrap_err();
    assert_eq!(failure.reason, "audited");
    assert_eq!(failure.message, "unknown query \"missing\"");
    assert_eq!(failure.failure_type, "AuditedFailure");
}

#[tokio::test]
async fn the_offline_environment_runs_activity_interceptors() {
    let events = events();
    let mut worker = recorded_worker(&events);
    worker.register_activity("ship", |_, args| async move { Ok(args) });
    let environment = crate::testing::WorkflowEnvironment::from_worker(worker);
    let result = environment
        .run_activity("ship", json!(["parcel"]))
        .await
        .unwrap();
    assert_eq!(result, json!(["parcel"]));
    assert_eq!(
        take(&events),
        [
            "outer:activity:before",
            "inner:activity:before",
            "inner:activity:after:true",
            "outer:activity:after:true",
        ]
    );
}

#[tokio::test]
async fn a_retained_core_failure_cannot_change_the_reported_error() {
    struct RetainFailure(Arc<Mutex<Option<WorkflowTaskFailure>>>);
    impl WorkerInterceptor for RetainFailure {
        fn execute_workflow_task<'a>(
            &'a self,
            _context: &'a WorkflowTaskInterceptorContext,
            next: WorkflowTaskNext<'a>,
        ) -> InterceptorFuture<'a, Result<WorkflowTaskOutcome>> {
            Box::pin(async move {
                let outcome = next.run().await;
                if let Err(Error::WorkflowTaskFailed(failure)) = &outcome {
                    *self.0.lock().unwrap() = Some(failure.clone());
                }
                outcome
            })
        }
    }
    let retained = Arc::new(Mutex::new(None));
    // No workflow registered: the core fails with WorkflowNotRegistered.
    let worker = plain_worker().interceptor(RetainFailure(retained.clone()));
    let error = worker
        .execute_workflow_with_local_activities(workflow_task(Vec::new()))
        .await
        .expect_err("the core failure is reported");
    assert!(
        matches!(&error, Error::WorkflowNotRegistered(name) if name == "ship-order"),
        "the original error survives a retained copy: {error:?}"
    );
    let retained = retained.lock().unwrap();
    let failure = retained.as_ref().expect("the interceptor kept the failure");
    assert_eq!(failure.kind(), "WorkflowNotRegistered");
    assert_eq!(failure.message(), error.to_string());
    assert_eq!(failure.debug(), format!("{error:?}"));
    assert_eq!(failure.to_string(), error.to_string());
}

#[tokio::test]
async fn an_interceptor_error_before_next_is_part_of_the_task_failure() {
    let stash = Arc::new(Mutex::new(None));
    let runs = Arc::new(AtomicUsize::new(0));
    let mut worker = tamper_worker("fail-before-next", &stash);
    let counted = runs.clone();
    worker.register_workflow("ship-order", move |ctx, _| {
        counted.fetch_add(1, Ordering::SeqCst);
        async move { ctx.activity("ship", json!([])).await }
    });
    let error = worker
        .execute_workflow_with_local_activities(workflow_task(Vec::new()))
        .await
        .expect_err("the interceptor error fails the task");
    let Error::WorkflowTaskInterceptor(message) = &error else {
        panic!("expected an interceptor failure, got {error:?}");
    };
    assert!(
        message.ends_with(
            "Tamper failed without calling next: worker loop error: tenant lookup failed"
        ),
        "{message}"
    );
    assert_eq!(runs.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn a_core_dropped_partway_is_reported_as_abandoned() {
    let stash = Arc::new(Mutex::new(None));
    // A local activity makes the core wait on the network, so dropping it
    // after one poll abandons it partway.
    let mut worker = tamper_worker("abandon-started", &stash).local_activities(true);
    worker.register_workflow("ship-order", |ctx, _| async move {
        ctx.local_activity("label", json!([])).await
    });
    worker.register_activity("label", |_, args| async move { Ok(args) });
    let error = worker
        .execute_workflow_with_local_activities(workflow_task(Vec::new()))
        .await
        .expect_err("an abandoned core fails the task");
    assert!(
        matches!(&error, Error::WorkflowTaskInterceptor(message)
            if message == "a workflow task interceptor dropped the workflow task before it completed: worker loop error: gave up"),
        "{error:?}"
    );
}

fn local_activity_server(
    path: &str,
    _body: &str,
    _request: usize,
) -> Option<(&'static str, String)> {
    if path.ends_with("/cluster/info") {
        return Some((
            "200 OK",
            json!({"limits":{"max_payload_bytes":1048576}}).to_string(),
        ));
    }
    if path.contains("/workflow-tasks/") && path.ends_with("/heartbeat") {
        return Some((
            "200 OK",
            json!({"task_id":"local-task","lease_owner":"interceptor-worker",
                "workflow_task_attempt":2,"renewed":true,"reason":null,"task_status":"leased",
                "run_status":"running","lease_expires_at":"2099-01-01T00:00:00Z"})
            .to_string(),
        ));
    }
    None
}

#[tokio::test]
async fn a_local_activity_runs_through_the_activity_hook_inside_the_workflow_task() {
    struct Local(Events);
    impl WorkerInterceptor for Local {
        fn execute_workflow_task<'a>(
            &'a self,
            _context: &'a WorkflowTaskInterceptorContext,
            next: WorkflowTaskNext<'a>,
        ) -> InterceptorFuture<'a, Result<WorkflowTaskOutcome>> {
            Box::pin(async move {
                self.0.lock().unwrap().push("workflow:before".into());
                let outcome = next.run().await;
                self.0.lock().unwrap().push("workflow:after".into());
                outcome
            })
        }

        fn execute_activity(
            &self,
            context: ActivityInterceptorContext,
            next: ActivityNext,
        ) -> InterceptorFuture<'_, Result<AvroValue>> {
            self.0.lock().unwrap().push(format!(
                "activity:{}:local={}:{:?}",
                context.activity_type(),
                context.is_local(),
                context.workflow_id()
            ));
            next.run(context)
        }
    }
    let server = MockWorkerServer::start_with_behavior(MockWorkerBehavior {
        request_override: Some(local_activity_server),
        ..MockWorkerBehavior::default()
    });
    let client = Client::builder(server.base_url())
        .timeout(Duration::from_secs(2))
        .build()
        .expect("client");
    let events = events();
    let mut worker = Worker::new(client, "orders")
        .worker_id("interceptor-worker")
        .local_activities(true)
        .interceptor(Local(events.clone()));
    worker.register_workflow("ship-order", |ctx, _| async move {
        ctx.local_activity("label", json!(["parcel"])).await
    });
    worker.register_activity("label", |_, args| async move { Ok(json!({"label": args})) });
    let mut task = workflow_task(Vec::new());
    task.task_id = "local-task".into();
    task.lease_owner = Some("interceptor-worker".into());
    let decision = worker
        .execute_workflow_with_local_activities(task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(decision.commands[0]["type"], "record_local_activity");
    assert_eq!(
        take(&events),
        [
            "workflow:before",
            r#"activity:label:local=true:Some("order-1")"#,
            "workflow:after",
        ]
    );
}

#[tokio::test]
async fn the_cooperative_worker_fails_the_task_for_an_interceptor_error_after_replay() {
    use super::cooperative_cancellation::{
        coordinator_server, coordinator_worker, register_coordinator_timer,
    };
    let server = coordinator_server();
    let stash = Arc::new(Mutex::new(None));
    let mut worker = coordinator_worker(&server, "coordinator-valid").interceptor(Tamper {
        mode: "fail-after-core",
        stash: stash.clone(),
    });
    let observed = Arc::new(Mutex::new(Vec::new()));
    register_coordinator_timer(&mut worker, observed.clone());
    assert_eq!(
        worker.poll_workflow_once().await.expect("settled"),
        ManagedPollOutcome::Handled
    );
    assert_eq!(observed.lock().unwrap().len(), 1, "the replay delivered");
    assert!(stash.lock().unwrap().is_some(), "the core completed");
    let requests = server.requests.lock().unwrap();
    assert!(!requests
        .iter()
        .any(|request| request.path.ends_with("/complete")));
    let failure = requests
        .iter()
        .find(|request| request.path.ends_with("/fail"))
        .expect("the workflow task is failed");
    let body: Value = serde_json::from_str(&failure.body).unwrap();
    assert_eq!(body["lease_owner"], "actual-owner");
    assert!(
        body.to_string().contains("audit sink unavailable"),
        "{body}"
    );
}

#[tokio::test]
async fn the_cooperative_worker_does_not_settle_a_replay_dropped_partway() {
    use super::cooperative_cancellation::{
        coordinator_server, coordinator_worker, register_coordinator_timer,
    };
    let server = coordinator_server();
    let stash = Arc::new(Mutex::new(None));
    let mut worker = coordinator_worker(&server, "coordinator-valid").interceptor(Tamper {
        mode: "abandon-started",
        stash,
    });
    register_coordinator_timer(&mut worker, Arc::new(Mutex::new(Vec::new())));
    let error = worker
        .poll_workflow_once()
        .await
        .expect_err("an uncertain replay is not settled");
    assert!(
        matches!(&error, Error::WorkflowTaskInterceptor(message) if message.contains("dropped the workflow task")),
        "{error:?}"
    );
    let requests = server.requests.lock().unwrap();
    assert!(!requests
        .iter()
        .any(|request| request.path.ends_with("/complete") || request.path.ends_with("/fail")));
}
