use super::*;

fn options() -> WorkerSessionOptions {
    WorkerSessionOptions::new("render-1")
        .queue("gpu-workers")
        .requirements(["gpu:l4"])
}

fn affinity() -> Value {
    let mut value = options().to_wire().unwrap();
    value["namespace"] = json!("default");
    value["status"] = json!("active");
    value["lease_owner"] = json!("session-worker");
    value["lease_expires_at"] = json!("2099-01-01T00:00:00Z");
    value["ttl_expires_at"] = json!("2099-01-02T00:00:00Z");
    value
}

fn near_expiry_affinity() -> Value {
    let deadline = (SystemTime::now() + Duration::from_millis(250))
        .duration_since(UNIX_EPOCH)
        .unwrap();
    let mut value = affinity();
    value["lease_expires_at"] = json!(chrono::DateTime::from_timestamp(
        deadline.as_secs() as i64,
        deadline.subsec_nanos()
    )
    .unwrap()
    .to_rfc3339());
    value
}

fn responses(path: &str, body: &str, number: usize) -> Option<(&'static str, String)> {
    let request: Value = serde_json::from_str(body).unwrap_or(Value::Null);
    if path.ends_with("/worker/register") {
        let mut response = json!({"registered":true,"worker_id":request["worker_id"],
            "namespace":"default","task_queue":request["task_queue"],
            "protocol_version":"1.20","capability_manifest":request["capability_manifest"],
            "capabilities":request["capabilities"]});
        if path.starts_with("/bad-ack/") {
            response["capability_manifest"]["worker_sessions"]["supported"] = json!(false);
        }
        if path.starts_with("/wrong-namespace/") {
            response["namespace"] = json!("other");
        }
        return Some(("201 Created", response.to_string()));
    }
    if path.contains("/worker/registrations/") {
        return Some(("200 OK", json!({"worker_id":"session-worker","outcome":"deregistered","recovered_workflow_task_count":0}).to_string()));
    }
    if path.ends_with("/worker/sessions") || path.contains("/worker/sessions/render-1") {
        let mut session = affinity();
        let mut outcome = if path.ends_with("/worker/sessions") {
            "created"
        } else {
            "heartbeat_recorded"
        };
        if request.get("reason").is_some() {
            session["status"] = json!("closed");
            outcome = "closed";
        }
        if path.starts_with("/wrong-holder/") {
            session["lease_owner"] = json!("other-worker");
        }
        if path.starts_with("/changed-ttl/") && path.ends_with("/heartbeat") {
            session["ttl_expires_at"] = json!("2099-01-03T00:00:00Z");
        }
        if path.starts_with("/ambiguous-renew/") && path.ends_with("/heartbeat") {
            return Some((
                "200 OK",
                json!({"admitted":true,"outcome":"heartbeat_recorded"}).to_string(),
            ));
        }
        return Some((
            "200 OK",
            json!({"admitted":true,"outcome":outcome,"session":session}).to_string(),
        ));
    }
    if path.ends_with("/worker/workflow-tasks/poll") {
        return Some((
            "200 OK",
            if path.starts_with("/shutdown/") {
                json!({"task":null,"poll_status":"draining"})
            } else {
                json!({"task":null})
            }
            .to_string(),
        ));
    }
    if path.ends_with("/worker/activity-tasks/poll") {
        return Some((
            "200 OK",
            if (path.starts_with("/task/") || path.starts_with("/heartbeat-renew/") || path.starts_with("/expire/")) && number == 1 {
                json!({"task":{"task_id":"session-task","activity_attempt_id":"session-attempt",
                "activity_type":"render","payload_codec":"avro","attempt_number":1,
                    "lease_owner":"session-worker","worker_session":if path.starts_with("/task/") {affinity()} else {near_expiry_affinity()}}})
            } else {
                json!({"task":null})
            }
            .to_string(),
        ));
    }
    if path.ends_with("/worker/activity-tasks/session-task/heartbeat") {
        return Some(("200 OK", json!({"task_id":"session-task","activity_attempt_id":"session-attempt",
            "lease_owner":"session-worker","heartbeat_recorded":true,"can_continue":true,"cancel_requested":false,
            "worker_session":affinity()}).to_string()));
    }
    if path.ends_with("/worker/activity-tasks/session-task/fail") {
        return Some(("200 OK", json!({"failed":true}).to_string()));
    }
    if path.ends_with("/worker/activity-tasks/session-task/complete") {
        return Some(("200 OK", json!({"completed":true}).to_string()));
    }
    if path.ends_with("/worker/heartbeat") {
        return Some(("200 OK", json!({"heartbeat_recorded":true}).to_string()));
    }
    if path.ends_with("/cluster/info") {
        return Some((
            "200 OK",
            json!({"limits":{"max_payload_bytes":1048576}}).to_string(),
        ));
    }
    None
}

fn setup(case: &str) -> (MockWorkerServer, Worker) {
    let server = MockWorkerServer::start_with_behavior(MockWorkerBehavior {
        request_override: Some(responses),
        ..MockWorkerBehavior::default()
    });
    let client = Client::new(format!("{}/{case}", server.base_url())).unwrap();
    let worker = Worker::new(client, "gpu-workers")
        .worker_id("session-worker")
        .worker_sessions(true)
        .capabilities(["gpu:l4"])
        .poll_timeout(Duration::from_millis(10));
    (server, worker)
}

#[tokio::test]
async fn worker_session_registration_refuses_unacknowledged_profiles_before_polling() {
    for case in ["bad-ack", "wrong-namespace"] {
        let (server, worker) = setup(case);
        assert!(worker.register().await.is_err());
        assert!(worker.worker_session(options()).is_err());
        assert!(worker.run_once().await.is_err());
        assert_eq!(
            server.request_count(&format!("/{case}/api/worker/activity-tasks/poll")),
            0
        );
        assert_eq!(
            server.request_count(&format!("/{case}/api/worker/registrations/session-worker")),
            1
        );
    }
}

#[tokio::test]
async fn worker_session_lifecycle_preserves_ttl_and_duplicate_close_receipt() {
    let (server, worker) = setup("lifecycle");
    assert!(worker.worker_session(options()).is_err());
    worker.register().await.unwrap();
    let session = worker.worker_session(options()).unwrap();
    assert!(!session.active());
    assert!(session.renew().await.is_err());
    let created = session.create().await.unwrap();
    assert!(session.active());
    let renewed = session.renew().await.unwrap();
    assert_eq!(
        created["session"]["ttl_expires_at"],
        renewed["session"]["ttl_expires_at"]
    );
    assert_eq!(
        session.clone().close("test_completed").await.unwrap(),
        session.close("test_completed").await.unwrap()
    );
    assert!(!session.active());
    assert!(session.create().await.is_err());
    assert_eq!(
        server.request_count("/lifecycle/api/worker/sessions/render-1"),
        1
    );
    assert_eq!(
        server.request_body("/lifecycle/api/worker/register")["capability_manifest"]
            ["sticky_execution"]["supported"],
        false
    );
}

#[tokio::test]
async fn worker_session_uncertain_receipts_never_restore_local_authority() {
    for case in ["wrong-holder", "ambiguous-renew", "changed-ttl"] {
        let (_server, worker) = setup(case);
        worker.register().await.unwrap();
        let session = worker.worker_session(options()).unwrap();
        if case == "wrong-holder" {
            assert!(session.create().await.is_err());
        } else {
            session.create().await.unwrap();
            assert!(session.renew().await.is_err());
        }
        assert!(!session.active());
    }
}

#[tokio::test]
async fn worker_session_registry_bounds_handles_and_refuses_identity_changes() {
    let (_server, worker) = setup("capacity");
    let worker = worker.max_concurrent_worker_sessions(1);
    worker.register().await.unwrap();
    let first = worker.worker_session(options()).unwrap();
    assert!(worker.worker_session(options().ttl_seconds(60)).is_err());
    assert!(worker
        .worker_session(WorkerSessionOptions::new("other"))
        .is_err());
    assert_eq!(
        first.options(),
        worker.worker_session(options()).unwrap().options()
    );
}

#[tokio::test]
async fn worker_session_task_affinity_reaches_activity_without_extra_create() {
    let (server, mut worker) = setup("task");
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    worker.register_activity("render", move |ctx, _args| {
        observed.fetch_add(1, Ordering::SeqCst);
        async move {
            let session = ctx.worker_session().expect("typed activity affinity");
            assert!(session.active());
            assert_eq!(session.options().session_id(), "render-1");
            Ok(json!(42))
        }
    });
    worker.register().await.unwrap();
    assert_eq!(
        worker.poll_activity_once().await.unwrap(),
        ManagedPollOutcome::Handled
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(server.request_count("/task/api/worker/sessions"), 0);
    assert_eq!(
        server.request_count("/task/api/worker/activity-tasks/session-task/complete"),
        1
    );
    assert_eq!(worker.session_available(), 9);
}

#[tokio::test]
async fn worker_session_shutdown_closes_owned_sessions_before_deregistration() {
    let (server, worker) = setup("shutdown");
    worker.register().await.unwrap();
    let session = worker.worker_session(options()).unwrap();
    session.create().await.unwrap();
    worker.run_until(async {}).await.unwrap();
    assert!(!session.active());
    let requests = server.requests.lock().unwrap();
    let close = requests
        .iter()
        .position(|request| {
            request.method == "DELETE" && request.path.ends_with("/sessions/render-1")
        })
        .unwrap();
    let deregister = requests
        .iter()
        .position(|request| request.path.ends_with("/registrations/session-worker"))
        .unwrap();
    assert!(close < deregister);
}

#[test]
fn worker_session_parallel_routing_covers_every_activity_leaf() {
    let ctx = workflow_context(Vec::new());
    let mut group = Box::pin(
        ctx.parallel(nested_parallel_operations())
            .in_worker_session(options()),
    );
    let mut cx = TaskContext::from_waker(noop_waker_ref());
    assert!(group.as_mut().poll(&mut cx).is_pending());
    let commands = ctx.take_commands().unwrap();
    let activities: Vec<_> = commands
        .iter()
        .filter(|command| command["type"] == "schedule_activity")
        .collect();
    assert_eq!(activities.len(), 2);
    for activity in activities {
        assert_eq!(activity["worker_session"], options().to_wire().unwrap());
    }
}

#[tokio::test]
async fn worker_session_activity_heartbeat_updates_local_session_lease() {
    let (server, mut worker) = setup("heartbeat-renew");
    worker.register_activity("render", |ctx, _| async move {
        ctx.heartbeat(json!({"phase":"running"})).await?;
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert!(ctx.worker_session().unwrap().active());
        Ok(json!(42))
    });
    worker.register().await.unwrap();
    assert_eq!(
        worker.poll_activity_once().await.unwrap(),
        ManagedPollOutcome::Handled
    );
    assert_eq!(
        server.request_count("/heartbeat-renew/api/worker/activity-tasks/session-task/complete"),
        1
    );
}

#[tokio::test]
async fn worker_session_expiry_drops_callback_without_application_heartbeats() {
    struct Dropped(Arc<AtomicBool>);
    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    let (server, mut worker) = setup("expire");
    let stopped = Arc::new(AtomicBool::new(false));
    let observed = stopped.clone();
    worker.register_activity("render", move |_ctx, _| {
        let guard = Dropped(observed.clone());
        async move {
            let _guard = guard;
            std::future::pending::<Result<Value>>().await
        }
    });
    worker.register().await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), worker.poll_activity_once())
        .await
        .unwrap()
        .unwrap();
    assert!(stopped.load(Ordering::SeqCst));
    assert_eq!(
        server.request_count("/expire/api/worker/activity-tasks/session-task/complete"),
        0
    );
    assert_eq!(
        server.request_count("/expire/api/worker/activity-tasks/session-task/heartbeat"),
        0
    );
}
