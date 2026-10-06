use super::*;

fn responses(path: &str, body: &str, number: usize) -> Option<(&'static str, String)> {
    if path.ends_with("/worker/register") {
        let request: Value = serde_json::from_str(body).unwrap();
        let mut manifest = request["capability_manifest"].clone();
        if path.starts_with("/bad-registration/") {
            manifest["local_activities"]["supported"] = json!(false);
        }
        return Some(("200 OK", json!({"registered":true,"worker_id":request["worker_id"],"namespace":"default",
            "task_queue":request["task_queue"],"capability_manifest":manifest,"protocol_version":"1.20"}).to_string()));
    }
    if path.contains("/worker/registrations/") {
        return Some(("200 OK", json!({"worker_id":"local-worker","outcome":"deregistered","recovered_workflow_task_count":0}).to_string()));
    }
    if path.ends_with("/cluster/info") {
        if path.starts_with("/external/") || path.starts_with("/upload-refused/") {
            return Some(("200 OK", runtime_uploads::policy().to_string()));
        }
        return Some((
            "200 OK",
            json!({"limits":{"max_payload_bytes":1048576}}).to_string(),
        ));
    }
    if path.ends_with("/external-payloads/v1") {
        if path.starts_with("/upload-refused/") {
            return Some((
                "409 Conflict",
                json!({"reason":"lease_expired"}).to_string(),
            ));
        }
        return Some((
            "201 Created",
            json!({"schema":"durable-workflow.v2.runtime-external-payload-upload.v1", "transport_version":1,
                "reference": runtime_uploads::reference(body)}).to_string(),
        ));
    }
    if path.contains("/workflow-tasks/") && path.ends_with("/heartbeat") {
        if path.starts_with("/closed/") && number > 1 {
            return Some((
                "409 Conflict",
                json!({"reason":"workflow_run_closed"}).to_string(),
            ));
        }
        if path.starts_with("/ambiguous/") && number > 1 {
            return Some(("200 OK", json!({"renewed":true}).to_string()));
        }
        let request: Value = serde_json::from_str(body).unwrap();
        return Some((
            "200 OK",
            json!({"task_id":"local-task", "lease_owner":request["lease_owner"],
            "workflow_task_attempt":request["workflow_task_attempt"], "renewed":true,
            "reason":null,"task_status":"leased","run_status":"running",
            "lease_expires_at":"2099-01-01T00:00:00Z"})
            .to_string(),
        ));
    }
    None
}

fn setup(case: &str) -> (MockWorkerServer, Worker, WorkflowTask) {
    let server = MockWorkerServer::start_with_behavior(MockWorkerBehavior {
        request_override: Some(responses),
        ..MockWorkerBehavior::default()
    });
    let client = Client::builder(format!("{}/{case}", server.base_url()))
        .build()
        .unwrap();
    let worker = Worker::new(client, "local-queue")
        .worker_id("local-worker")
        .local_activities(true);
    let mut task = workflow_task("local.workflow", Vec::new(), DEFAULT_CODEC);
    task.task_id = "local-task".into();
    task.run_id = Some("local-run".into());
    task.lease_owner = Some("local-worker".into());
    (server, worker, task)
}

fn committed_local_history(command: &Value) -> Vec<HistoryEvent> {
    vec![
        history_event(
            "ActivityScheduled",
            json!({"sequence":1,"activity_type":command["activity_type"],
            "execution_mode":"local","local_activity":true}),
        ),
        history_event(
            "ActivityCompleted",
            json!({"sequence":1,"activity_type":command["activity_type"],
            "execution_mode":"local","local_activity":true,"result":command["result"],"payload_codec":"avro"}),
        ),
    ]
}

#[tokio::test]
async fn local_activity_executes_typed_values_once_and_cold_replay_is_read_only() {
    let (server, mut worker, task) = setup("typed");
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    worker.register_workflow_avro_value("local.workflow", |ctx, _| async move {
        ctx.local_activity_avro_value("echo", typed_fidelity_probe())
            .await
    });
    worker.register_activity_avro_value("echo", move |ctx, args| {
        observed.fetch_add(1, Ordering::SeqCst);
        async move {
            let response = ctx.heartbeat(typed_fidelity_probe()).await?;
            assert!(response.heartbeat_recorded);
            Ok(args)
        }
    });
    let decision = worker
        .execute_workflow_with_local_activities(task.clone())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(decision.commands.len(), 2);
    let command = &decision.commands[0];
    assert_eq!(command["type"], "record_local_activity");
    assert_eq!(command["attempts"][0]["outcome"], "completed");
    assert_eq!(
        command["attempts"][0]["heartbeats"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        decode_wire_avro_value(&decision.commands[1]["result"], DEFAULT_CODEC).unwrap(),
        AvroValue::Array(vec![typed_fidelity_probe()])
    );
    assert!(server
        .captured_paths()
        .iter()
        .all(|path| !path.contains("/activity-tasks/")));
    let mut replay = task;
    replay.history_events = committed_local_history(command);
    let replayed = worker.execute_workflow_task(replay).unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(replayed, vec![decision.commands[1].clone()]);
}

#[tokio::test]
async fn unacknowledged_local_completion_can_execute_again_but_committed_result_cannot() {
    let (_server, mut worker, task) = setup("redelivery");
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    worker.register_workflow("local.workflow", |ctx, _| async move {
        ctx.local_activity("work", json!([])).await
    });
    worker.register_activity("work", move |_, _| {
        observed.fetch_add(1, Ordering::SeqCst);
        async { Ok(json!("done")) }
    });
    let first = worker
        .execute_workflow_with_local_activities(task.clone())
        .await
        .unwrap()
        .unwrap();
    let second = worker
        .clone()
        .execute_workflow_with_local_activities(task.clone())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(first.commands[0]["result"], second.commands[0]["result"]);
    let mut replay = task;
    replay.history_events = committed_local_history(&second.commands[0]);
    worker
        .clone()
        .execute_workflow_with_local_activities(replay)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn local_activity_retries_with_ordered_attempt_and_heartbeat_reports() {
    let (_server, mut worker, task) = setup("retry");
    worker.register_workflow("local.workflow", |ctx, _| async move {
        ctx.local_activity_with_options(
            "flaky",
            LocalActivityOptions::new().retry_policy(
                ActivityRetryPolicy::new(3).backoff_intervals([Duration::ZERO, Duration::ZERO]),
            ),
            json!([]),
        )
        .await
    });
    worker.register_activity("flaky", |ctx, _| async move {
        ctx.heartbeat(json!({"attempt":ctx.attempt_number})).await?;
        if ctx.attempt_number < 3 {
            return Err(Error::WorkerLoop("retry me".into()));
        }
        Ok(json!("recovered"))
    });
    let decision = worker
        .execute_workflow_with_local_activities(task)
        .await
        .unwrap()
        .unwrap();
    let attempts = decision.commands[0]["attempts"].as_array().unwrap();
    assert_eq!(attempts.len(), 3);
    let mut identities = BTreeSet::new();
    for (i, report) in attempts.iter().enumerate() {
        assert_eq!(report["attempt_number"], i + 1);
        assert_eq!(report["heartbeats"].as_array().unwrap().len(), 1);
        assert!(identities.insert(report["attempt_id"].as_str().unwrap()));
        if i < 2 {
            assert_eq!(report["retry_reason"], "failure");
            assert_eq!(report["backoff_seconds"], 0);
        } else {
            assert!(report.get("retry_reason").is_none());
        }
    }
    assert_eq!(decision.commands[0]["outcome"], "completed");
}

struct Dropped(Arc<AtomicBool>);
impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn refused_or_ambiguous_lease_stops_local_work_without_application_heartbeats() {
    for case in ["closed", "ambiguous"] {
        let (server, mut worker, task) = setup(case);
        let dropped = Arc::new(AtomicBool::new(false));
        let observed = dropped.clone();
        let context = Arc::new(Mutex::new(None));
        let saved = context.clone();
        worker.register_workflow("local.workflow", |ctx, _| async move {
            ctx.local_activity("wait", json!([])).await
        });
        worker.register_activity("wait", move |ctx, _| {
            *saved.lock().unwrap() = Some(ctx);
            let dropped = Dropped(observed.clone());
            async move {
                let _dropped = dropped;
                std::future::pending::<Result<Value>>().await
            }
        });
        let result = tokio::time::timeout(
            Duration::from_secs(3),
            worker.execute_workflow_with_local_activities(task),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(result.is_none());
        assert!(dropped.load(Ordering::SeqCst));
        let ctx = context.lock().unwrap().clone().unwrap();
        assert!(ctx.heartbeat(json!("late")).await.unwrap().should_stop());
        assert!(server
            .captured_paths()
            .iter()
            .all(|p| !p.ends_with("/complete") && !p.ends_with("/fail")));
    }
}

#[tokio::test]
async fn local_activity_timeout_drops_callback_and_replays_typed_failure() {
    for kind in ["start_to_close", "heartbeat"] {
        let (_server, mut worker, task) = setup(kind);
        worker.register_workflow("local.workflow", move |ctx, _| async move {
            let options = if kind == "start_to_close" { LocalActivityOptions::new().start_to_close_timeout(Duration::from_secs(1)) }
                else { LocalActivityOptions::new().heartbeat_timeout(Duration::from_secs(1)) };
            let result = ctx.local_activity_with_options("wait", options, json!([])).await;
            assert!(matches!(result, Err(Error::ActivityFailed(ref f)) if f.kind == ActivityFailureKind::TimedOut && f.timeout_kind.as_deref() == Some(kind)));
            Ok(json!("handled"))
        });
        let dropped = Arc::new(AtomicBool::new(false));
        let observed = dropped.clone();
        worker.register_activity("wait", move |_, _| {
            let dropped = Dropped(observed.clone());
            async move {
                let _dropped = dropped;
                std::future::pending::<Result<Value>>().await
            }
        });
        let decision = worker
            .execute_workflow_with_local_activities(task)
            .await
            .unwrap()
            .unwrap();
        assert!(dropped.load(Ordering::SeqCst));
        assert_eq!(decision.commands[0]["outcome"], "timed_out");
        assert_eq!(decision.commands[0]["timeout_kind"], kind);
        assert_eq!(decision.commands[0]["attempts"][0]["timeout_kind"], kind);
    }
}

#[tokio::test]
async fn local_activity_externalizes_input_result_and_heartbeat_details() {
    let (server, mut worker, task) = setup("external");
    worker.register_workflow("local.workflow", |ctx, _| async move {
        ctx.local_activity("large", json!(["a".repeat(128)])).await
    });
    worker.register_activity("large", |ctx, _| async move {
        ctx.heartbeat(json!("h".repeat(128))).await?;
        Ok(json!("r".repeat(128)))
    });
    let decision = worker
        .execute_workflow_with_local_activities(task)
        .await
        .unwrap()
        .unwrap();
    for field in [
        &decision.commands[0]["arguments"],
        &decision.commands[0]["result"],
        &decision.commands[0]["attempts"][0]["heartbeats"][0]["details"],
    ] {
        assert_eq!(
            field["external_payload"]["schema"],
            crate::runtime_payloads::SCHEMA
        );
    }
    assert_eq!(
        server.request_count("/external/api/external-payloads/v1"),
        3
    );
}

#[tokio::test]
async fn local_activity_refused_input_upload_prevents_callback_execution() {
    let (_server, mut worker, task) = setup("upload-refused");
    worker.register_workflow("local.workflow", |ctx, _| async move {
        ctx.local_activity("large", json!(["a".repeat(128)])).await
    });
    worker.register_activity("large", |_, _| async {
        panic!("input must be admitted before side effects")
    });
    assert!(worker
        .execute_workflow_with_local_activities(task)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn total_deadline_includes_backoff_and_prevents_another_callback() {
    let (_server, mut worker, task) = setup("total");
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    worker.register_workflow("local.workflow", |ctx, _| async move {
        let result = ctx.local_activity_with_options("flaky", LocalActivityOptions::new()
            .schedule_to_close_timeout(Duration::from_secs(1))
            .retry_policy(ActivityRetryPolicy::new(3).backoff_intervals([Duration::from_secs(3), Duration::ZERO])), json!([])).await;
        assert!(matches!(result, Err(Error::ActivityFailed(f)) if f.timeout_kind.as_deref() == Some("schedule_to_close")));
        Ok(Value::Null)
    });
    worker.register_activity("flaky", move |_, _| {
        observed.fetch_add(1, Ordering::SeqCst);
        async { Err(Error::WorkerLoop("fail before backoff".into())) }
    });
    let decision = tokio::time::timeout(
        Duration::from_secs(3),
        worker.execute_workflow_with_local_activities(task),
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        decision.commands[0]["attempts"].as_array().unwrap().len(),
        2
    );
    assert_eq!(
        decision.commands[0]["attempts"][1]["timeout_kind"],
        "schedule_to_close"
    );
}

#[tokio::test]
async fn application_heartbeats_extend_only_the_heartbeat_timeout() {
    let (_server, mut worker, task) = setup("heartbeat-extension");
    worker.register_workflow("local.workflow", |ctx, _| async move {
        ctx.local_activity_with_options(
            "wait",
            LocalActivityOptions::new()
                .heartbeat_timeout(Duration::from_secs(1))
                .schedule_to_close_timeout(Duration::from_secs(5)),
            json!([]),
        )
        .await
    });
    worker.register_activity("wait", |ctx, _| async move {
        for _ in 0..4 {
            tokio::time::sleep(Duration::from_millis(400)).await;
            ctx.heartbeat(json!("alive")).await?;
        }
        Ok(json!("done"))
    });
    let decision = worker
        .execute_workflow_with_local_activities(task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(decision.commands[0]["outcome"], "completed");
    assert_eq!(
        decision.commands[0]["attempts"][0]["heartbeats"]
            .as_array()
            .unwrap()
            .len(),
        4
    );
}

#[tokio::test]
async fn local_work_retains_the_workflow_future_across_mixed_commands() {
    let (_server, mut worker, task) = setup("mixed");
    let side_effects = Arc::new(AtomicUsize::new(0));
    let observed = side_effects.clone();
    worker.register_workflow("local.workflow", move |ctx, _| {
        let observed = observed.clone();
        async move {
            ctx.side_effect(|| {
                observed.fetch_add(1, Ordering::SeqCst);
                json!("prefix")
            })?;
            ctx.local_activity("one", json!([])).await?;
            ctx.local_activity("two", json!([])).await?;
            ctx.activity("remote", json!([])).await
        }
    });
    worker.register_activity("one", |_, _| async { Ok(json!(1)) });
    worker.register_activity("two", |_, _| async { Ok(json!(2)) });
    let decision = worker
        .execute_workflow_with_local_activities(task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(side_effects.load(Ordering::SeqCst), 1);
    assert_eq!(
        decision
            .commands
            .iter()
            .map(|c| c["type"].as_str().unwrap())
            .collect::<Vec<_>>(),
        [
            "record_side_effect",
            "record_local_activity",
            "record_local_activity",
            "schedule_activity"
        ]
    );
}

#[tokio::test]
async fn local_heartbeat_report_budget_is_enforced_even_when_handler_ignores_errors() {
    let (_server, mut worker, task) = setup("report-budget");
    worker.register_workflow("local.workflow", |ctx, _| async move {
        let failure = ctx.local_activity("spam", json!([])).await.unwrap_err();
        assert!(matches!(failure, Error::ActivityFailed(f) if f.non_retryable));
        Ok(Value::Null)
    });
    worker.register_activity("spam", |ctx, _| async move {
        for _ in 0..1001 {
            let _ = ctx.heartbeat(Value::Null).await;
        }
        Ok(Value::Null)
    });
    let decision = worker
        .execute_workflow_with_local_activities(task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(decision.commands[0]["outcome"], "failed");
    assert_eq!(
        decision.commands[0]["attempts"][0]["heartbeats"]
            .as_array()
            .unwrap()
            .len(),
        1000
    );
}

#[tokio::test]
async fn local_non_retryable_type_and_missing_registration_do_not_retry() {
    for registered in [true, false] {
        let (_server, mut worker, task) = setup("non-retryable");
        worker.register_workflow("local.workflow", |ctx, _| async move {
            let result = ctx
                .local_activity_with_options(
                    "fail",
                    LocalActivityOptions::new().retry_policy(
                        ActivityRetryPolicy::new(3).non_retryable_error_type("RustActivityError"),
                    ),
                    json!([]),
                )
                .await;
            assert!(matches!(result, Err(Error::ActivityFailed(f)) if f.non_retryable));
            Ok(Value::Null)
        });
        if registered {
            worker.register_activity("fail", |_, _| async {
                Err(Error::WorkerLoop("permanent".into()))
            });
        }
        let decision = worker
            .execute_workflow_with_local_activities(task)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(decision.commands[0]["outcome"], "failed");
        assert_eq!(
            decision.commands[0]["attempts"].as_array().unwrap().len(),
            1
        );
    }
}

#[tokio::test]
async fn state_query_replays_local_result_without_executing_activity() {
    let (server, mut worker, task) = setup("query");
    worker.register_replayed_workflow(
        "local.workflow",
        || None::<Value>,
        |ctx, _, state| async move {
            let result = ctx.local_activity("recorded", json!([])).await?;
            state.update(|current| *current = Some(result))?;
            Ok(Value::Null)
        },
    );
    worker.register_replayed_query::<Option<Value>, _, _>(
        "local.workflow",
        "inspect",
        |_, state, _| async move { Ok(state.as_ref().clone().unwrap()) },
    );
    worker.register_activity("recorded", |_, _| async {
        panic!("queries cannot execute local side effects")
    });
    let command = json!({"activity_type":"recorded", "result":fixture_envelope(json!("durable"))});
    let query: QueryTask = serde_json::from_value(json!({"query_task_id":"query-local", "workflow_type":"local.workflow",
        "query_name":"inspect", "payload_codec":"avro", "workflow_arguments":task.arguments,
        "history_events":committed_local_history(&command).into_iter().map(|e| json!({"event_type":e.event_type,"payload":e.payload})).collect::<Vec<_>>() })).unwrap();
    let result = worker.execute_query_task(query).await.unwrap();
    assert_eq!(result.into_json().unwrap(), json!("durable"));
    assert!(server.captured_paths().is_empty());
}

#[test]
fn local_call_rejects_remote_and_unmarked_history() {
    for marker in [None, Some(json!(false))] {
        let mut event = history_event(
            "ActivityScheduled",
            json!({"sequence":1,"activity_type":"work"}),
        );
        if let Some(marker) = marker {
            event.payload["local_activity"] = marker;
        }
        let ctx = workflow_context(vec![event]);
        let mut call = Box::pin(ctx.local_activity("work", json!([])));
        let mut cx = TaskContext::from_waker(noop_waker_ref());
        assert!(
            matches!(call.as_mut().poll(&mut cx), Poll::Ready(Err(Error::NonDeterministicReplay(f)))
            if f.reason == "activity_execution_mode_mismatch")
        );
    }
}

#[tokio::test]
async fn disabled_local_execution_refuses_before_callback_or_lease_request() {
    let (server, mut worker, task) = setup("disabled");
    worker = worker.local_activities(false);
    worker.register_workflow("local.workflow", |ctx, _| async move {
        ctx.local_activity("work", json!([])).await
    });
    worker.register_activity("work", |_, _| async {
        panic!("disabled local work must not execute")
    });
    assert!(
        matches!(worker.execute_workflow_with_local_activities(task).await, Err(Error::WorkerLoop(message))
        if message.starts_with("local_activities_not_enabled:"))
    );
    assert!(server.captured_paths().is_empty());
}

#[tokio::test]
async fn inline_local_registration_cannot_claim_cooperative_supervision() {
    let (server, worker, _) = setup("cooperative-inline");
    let worker = worker.cooperative_cancellation(true);
    assert!(matches!(
        worker.register().await,
        Err(Error::CooperativeCancellationUnavailable(_))
    ));
    assert!(server.captured_paths().is_empty());
}

#[tokio::test]
async fn local_registration_requires_acknowledged_capability_and_actual_worker() {
    let (server, worker, _) = setup("registration");
    assert!(worker.run_once().await.is_err());
    assert!(server.captured_paths().is_empty());
    worker.register().await.unwrap();
    let request = server.request_body("/registration/api/worker/register");
    assert_eq!(
        request["capability_manifest"]["local_activities"]["supported"],
        true
    );
    assert!(request["capabilities"]
        .as_array()
        .unwrap()
        .iter()
        .any(|capability| capability == "local_activities"));
    assert!(worker.local_registration_confirmed.load(Ordering::SeqCst));
    let changed = worker.clone().worker_id("another-worker");
    assert!(!changed.local_registration_confirmed.load(Ordering::SeqCst));
    assert!(worker.local_registration_confirmed.load(Ordering::SeqCst));

    let (server, worker, _) = setup("bad-registration");
    assert!(
        matches!(worker.register().await, Err(Error::WorkerLoop(message)) if message.starts_with("local_activity_registration_unconfirmed:"))
    );
    assert!(!worker.local_registration_confirmed.load(Ordering::SeqCst));
    assert_eq!(
        server.request_count("/bad-registration/api/worker/registrations/local-worker"),
        1
    );
}
