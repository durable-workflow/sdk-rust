use super::*;

const POLL: &str = "/api/worker/activity-tasks/poll";
const RENEW: &str = "/api/worker/activity-tasks/lease-task/renew-lease";
const COMPLETE: &str = "/api/worker/activity-tasks/lease-task/complete";
const FAIL: &str = "/api/worker/activity-tasks/lease-task/fail";
const HEARTBEAT: &str = "/api/worker/activity-tasks/lease-task/heartbeat";

fn timestamp(after: Duration) -> String {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap() + after;
    DateTime::from_timestamp(now.as_secs() as i64, now.subsec_nanos())
        .unwrap()
        .to_rfc3339_opts(chrono::SecondsFormat::Micros, true)
}

fn delivery() -> Value {
    json!({
        "server_capabilities":{"activity_lease_renewal":true},
        "server_time":timestamp(Duration::ZERO),
        "task":{
            "task_id":"lease-task","workflow_id":"lease-workflow","run_id":"lease-run",
            "activity_execution_id":"lease-execution","activity_attempt_id":"lease-attempt",
            "activity_type":"LeaseActivity","attempt_number":1,"lease_owner":"lease-worker",
            "namespace":"named-test","task_queue":"lease-queue","payload_codec":"avro",
            "lease_expires_at":timestamp(Duration::from_millis(450)),"deadlines":{},
            "arguments":encode_value_envelope(&json!([]), DEFAULT_CODEC).unwrap(),
        }
    })
}

fn renewal() -> Value {
    json!({
        "workflow_task_id":"lease-task","workflow_instance_id":"lease-workflow",
        "workflow_run_id":"lease-run","activity_execution_id":"lease-execution",
        "activity_attempt_id":"lease-attempt","attempt_number":1,"lease_owner":"lease-worker",
        "lease_renewed":true,"can_continue":true,"cancel_requested":false,
        "heartbeat_recorded":false,"reason":null,"server_time":timestamp(Duration::ZERO),
        "lease_expires_at":timestamp(Duration::from_millis(450)),
    })
}

fn replies(path: &str, _: &str, _: usize) -> Option<(&'static str, String)> {
    let value = match path {
        POLL => delivery(),
        RENEW => renewal(),
        "/api/cluster/info" => json!({"worker_protocol":{"server_capabilities":{
            "activity_lease_renewal":true
        }}}),
        COMPLETE | FAIL => json!({"completed":true,"recorded":true}),
        _ => return None,
    };
    Some(("200 OK", value.to_string()))
}

fn worker(server: &MockWorkerServer) -> Worker {
    Worker::new(
        Client::builder(server.base_url())
            .namespace("named-test")
            .worker_token(Some("worker-test-token".into()))
            .build()
            .unwrap(),
        "lease-queue",
    )
    .worker_id("lease-worker")
}

fn server(overrides: RequestOverride) -> MockWorkerServer {
    MockWorkerServer::start_with_behavior(MockWorkerBehavior {
        request_override: Some(overrides),
        concurrent_requests: true,
        ..MockWorkerBehavior::default()
    })
}

#[tokio::test]
async fn advertised_activity_renewal_does_not_discover_on_the_hot_path() {
    let server = server(replies);
    let mut worker = worker(&server);
    worker.register_activity("LeaseActivity", |_ctx, _args| async { Ok(json!(true)) });
    for _ in 0..3 {
        worker.poll_activity_once().await.unwrap();
        // The first completion discovers the separate inline-payload policy.
        // Subsequent activities must not add lease capability discoveries.
        assert_eq!(server.request_count("/api/cluster/info"), 1);
    }
    assert_eq!(server.request_count(RENEW), 3);
    assert_eq!(server.request_count(COMPLETE), 3);
}

#[tokio::test]
async fn automatic_renewal_keeps_a_callback_past_its_original_lease() {
    let server = server(replies);
    let mut worker = worker(&server);
    worker.register_activity("LeaseActivity", |_ctx, _args| async {
        tokio::time::sleep(Duration::from_millis(850)).await;
        Ok(json!({"value":9_223_372_036_854_775_000_i64,"optional":null,"text":"継続"}))
    });
    worker.poll_activity_once().await.unwrap();
    assert!(
        server.request_count(RENEW) >= 2,
        "original claim must be maintained"
    );
    assert_eq!(server.request_count(COMPLETE), 1);
    assert_eq!(server.request_count(HEARTBEAT), 0);
    assert_eq!(server.request_count(FAIL), 0);
}

fn missing_identity(path: &str, body: &str, number: usize) -> Option<(&'static str, String)> {
    if path == POLL {
        let mut wire = delivery();
        wire["task"].as_object_mut().unwrap().remove("run_id");
        return Some(("200 OK", wire.to_string()));
    }
    replies(path, body, number)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spawned_activity_poll_maintains_a_cooperative_callback() {
    let server = server(replies);
    let mut worker = worker(&server);
    worker.register_activity("LeaseActivity", |_ctx, _args| async {
        tokio::time::sleep(Duration::from_millis(850)).await;
        Ok(json!(true))
    });
    tokio::spawn(async move { worker.poll_activity_once().await })
        .await
        .unwrap()
        .unwrap();
    assert!(server.request_count(RENEW) >= 2);
    assert_eq!(server.request_count(COMPLETE), 1);
    assert_eq!(server.request_count(FAIL), 0);
    assert_eq!(server.request_count(HEARTBEAT), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spawned_activity_poll_maintains_a_blocking_callback_with_a_spare_thread() {
    let server = server(replies);
    let mut worker = worker(&server);
    worker.register_activity("LeaseActivity", |_ctx, _args| async {
        std::thread::sleep(Duration::from_millis(850));
        Ok(json!(true))
    });
    tokio::spawn(async move { worker.poll_activity_once().await })
        .await
        .unwrap()
        .unwrap();
    assert!(
        server.request_count(RENEW) >= 2,
        "maintenance must run on the available executor thread during the callback"
    );
    assert_eq!(server.request_count(COMPLETE), 1);
    assert_eq!(server.request_count(FAIL), 0);
    assert_eq!(server.request_count(HEARTBEAT), 0);
}

#[tokio::test]
async fn affirmative_capability_requires_original_identity_before_callback() {
    let server = server(missing_identity);
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = Arc::clone(&calls);
    let mut worker = worker(&server);
    worker.register_activity("LeaseActivity", move |_ctx, _args| {
        seen.fetch_add(1, Ordering::SeqCst);
        async { Ok(json!(true)) }
    });
    worker.poll_activity_once().await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(server.request_count(COMPLETE), 0);
    assert_eq!(server.request_count(FAIL), 0);
}

struct Dropped(Arc<AtomicBool>);
impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

fn revoked(path: &str, body: &str, number: usize) -> Option<(&'static str, String)> {
    if path == RENEW && number > 1 {
        return Some((
            "409 Conflict",
            json!({"reason":"stale_attempt","can_continue":false}).to_string(),
        ));
    }
    replies(path, body, number)
}

#[tokio::test]
async fn lost_renewal_drops_callback_and_sends_no_terminal_result() {
    let server = server(revoked);
    let dropped = Arc::new(AtomicBool::new(false));
    let completed = Arc::new(AtomicBool::new(false));
    let (drop_seen, completed_seen) = (Arc::clone(&dropped), Arc::clone(&completed));
    let mut worker = worker(&server);
    worker.register_activity("LeaseActivity", move |_ctx, _args| {
        let (drop_seen, completed_seen) = (Arc::clone(&drop_seen), Arc::clone(&completed_seen));
        async move {
            let _drop = Dropped(drop_seen);
            tokio::time::sleep(Duration::from_millis(700)).await;
            completed_seen.store(true, Ordering::SeqCst);
            Ok(json!(true))
        }
    });
    worker.poll_activity_once().await.unwrap();
    assert!(!completed.load(Ordering::SeqCst));
    assert!(dropped.load(Ordering::SeqCst));
    assert_eq!(server.request_count(COMPLETE), 0);
    assert_eq!(server.request_count(FAIL), 0);
}

fn business_deadline(path: &str, body: &str, number: usize) -> Option<(&'static str, String)> {
    if path == POLL {
        let mut wire = delivery();
        wire["task"]["deadlines"] = json!({"run":timestamp(Duration::from_millis(180))});
        return Some(("200 OK", wire.to_string()));
    }
    replies(path, body, number)
}

#[tokio::test]
async fn renewals_do_not_extend_original_run_deadline() {
    let server = server(business_deadline);
    let mut worker = worker(&server);
    worker.register_activity("LeaseActivity", |_ctx, _args| async {
        tokio::time::sleep(Duration::from_millis(500)).await;
        Ok(json!(true))
    });
    worker.poll_activity_once().await.unwrap();
    assert_eq!(server.request_count(COMPLETE), 0);
    assert_eq!(server.request_count(FAIL), 0);
}

fn legacy(path: &str, body: &str, number: usize) -> Option<(&'static str, String)> {
    if path == POLL {
        let mut wire = delivery();
        wire["server_capabilities"]["activity_lease_renewal"] = json!(false);
        wire["task"].as_object_mut().unwrap().remove("run_id");
        return Some(("200 OK", wire.to_string()));
    }
    replies(path, body, number)
}

#[tokio::test]
async fn older_runtime_keeps_its_existing_callback_path() {
    let server = server(legacy);
    let mut worker = worker(&server);
    worker.register_activity("LeaseActivity", |_ctx, _args| async { Ok(json!(true)) });
    worker.poll_activity_once().await.unwrap();
    assert_eq!(server.request_count(RENEW), 0);
    assert_eq!(server.request_count(COMPLETE), 1);
}

#[tokio::test]
async fn retained_callback_client_cannot_publish_after_worker_settlement() {
    let server = server(replies);
    let retained = Arc::new(Mutex::new(None::<Client>));
    let seen = Arc::clone(&retained);
    let mut worker = worker(&server);
    worker.register_activity("LeaseActivity", move |ctx, _args| {
        *seen.lock().unwrap() = Some(ctx.client.clone());
        async { Ok(json!(true)) }
    });
    worker.poll_activity_once().await.unwrap();
    let client = retained.lock().unwrap().take().unwrap();
    let result = client
        .complete_activity_task("lease-task", "lease-attempt", "lease-worker", true, "avro")
        .await;
    assert!(matches!(result, Err(Error::ActivityExecutionAbandoned(_))));
    assert_eq!(server.request_count(COMPLETE), 1);
}

#[tokio::test]
async fn canonical_client_discovery_cannot_be_bypassed_by_missing_poll_capabilities() {
    fn cached(path: &str, body: &str, number: usize) -> Option<(&'static str, String)> {
        if path == POLL {
            let mut wire = delivery();
            wire.as_object_mut().unwrap().remove("server_capabilities");
            wire["task"].as_object_mut().unwrap().remove("run_id");
            return Some(("200 OK", wire.to_string()));
        }
        replies(path, body, number)
    }
    let server = server(cached);
    let mut worker = worker(&server);
    worker.client.control_token = Some("control-test-token".to_owned());
    worker.client.cluster_info().await.unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = Arc::clone(&calls);
    worker.register_activity("LeaseActivity", move |_ctx, _args| {
        seen.fetch_add(1, Ordering::SeqCst);
        async { Ok(json!(true)) }
    });
    worker.poll_activity_once().await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(server.request_count(COMPLETE), 0);
}

fn heartbeat_budget(path: &str, body: &str, number: usize) -> Option<(&'static str, String)> {
    if path == POLL {
        let mut wire = delivery();
        wire["task"]["retry_policy"] = json!({"heartbeat_timeout":1});
        let delivered =
            DateTime::parse_from_rfc3339(wire["server_time"].as_str().unwrap()).unwrap();
        wire["task"]["deadlines"]["heartbeat"] = json!((delivered + chrono::TimeDelta::seconds(1))
            .to_rfc3339_opts(chrono::SecondsFormat::Micros, true));
        return Some(("200 OK", wire.to_string()));
    }
    if path == HEARTBEAT {
        let mut value = renewal();
        value["task_id"] = json!("lease-task");
        value["heartbeat_recorded"] = json!(true);
        value["last_heartbeat_at"] = json!(timestamp(Duration::ZERO));
        return Some(("200 OK", value.to_string()));
    }
    replies(path, body, number)
}

#[tokio::test]
async fn runtime_renewals_do_not_fabricate_application_progress() {
    let server = server(heartbeat_budget);
    let mut worker = worker(&server);
    worker.register_activity("LeaseActivity", |_ctx, _args| async {
        tokio::time::sleep(Duration::from_millis(1400)).await;
        Ok(json!(true))
    });
    worker.poll_activity_once().await.unwrap();
    assert!(server.request_count(RENEW) >= 2);
    assert_eq!(server.request_count(HEARTBEAT), 0);
    assert_eq!(server.request_count(COMPLETE), 0);
    assert_eq!(server.request_count(FAIL), 0);
}

#[tokio::test]
async fn real_acknowledged_progress_extends_only_its_recorded_heartbeat_interval() {
    let server = server(heartbeat_budget);
    let mut worker = worker(&server);
    worker.register_activity("LeaseActivity", |ctx, _args| async move {
        tokio::time::sleep(Duration::from_millis(600)).await;
        ctx.heartbeat(json!({"processed":1})).await?;
        tokio::time::sleep(Duration::from_millis(600)).await;
        Ok(json!(true))
    });
    worker.poll_activity_once().await.unwrap();
    assert_eq!(server.request_count(HEARTBEAT), 1);
    assert_eq!(server.request_count(COMPLETE), 1);
    assert_eq!(server.request_count(FAIL), 0);
}

#[tokio::test]
async fn repeated_transient_renewal_failures_grant_no_additional_execution_time() {
    fn unavailable(path: &str, body: &str, number: usize) -> Option<(&'static str, String)> {
        if path == RENEW && number > 1 {
            return Some((
                "503 Service Unavailable",
                json!({"reason":"backend_lock_pressure"}).to_string(),
            ));
        }
        replies(path, body, number)
    }
    let server = server(unavailable);
    let mut worker = worker(&server);
    worker.register_activity("LeaseActivity", |_ctx, _args| async {
        tokio::time::sleep(Duration::from_millis(850)).await;
        Ok(json!(true))
    });
    worker.poll_activity_once().await.unwrap();
    assert!(server.request_count(RENEW) >= 2);
    // Still no execution past the acknowledged grant, but the claim is now
    // released explicitly instead of held silently until the lease expires.
    assert_eq!(server.request_count(COMPLETE), 0);
    assert_worker_lost_release(&server);
}

/// The single release report a Worker sends for a claim it could not keep.
fn assert_worker_lost_release(server: &MockWorkerServer) {
    let bodies = server.request_bodies(FAIL);
    assert_eq!(bodies.len(), 1, "exactly one release report");
    let body = &bodies[0];
    assert_eq!(body["activity_attempt_id"], "lease-attempt");
    assert_eq!(body["lease_owner"], "lease-worker");
    let failure = &body["failure"];
    assert_eq!(failure["type"], "worker_lost");
    assert_eq!(failure["class"], "ActivityWorkerLost");
    assert_eq!(failure["non_retryable"], false);
    let message = failure["message"].as_str().unwrap();
    assert!(
        message.contains("renew-lease"),
        "names the local cause: {message}"
    );
}

#[tokio::test]
async fn wrong_renewal_run_identity_is_rejected_before_callback() {
    fn wrong_run(path: &str, body: &str, number: usize) -> Option<(&'static str, String)> {
        if path == RENEW {
            let mut reply = renewal();
            reply["workflow_run_id"] = json!("replacement-run");
            return Some(("200 OK", reply.to_string()));
        }
        replies(path, body, number)
    }
    let server = server(wrong_run);
    let mut worker = worker(&server);
    worker.register_activity("LeaseActivity", |_ctx, _args| async {
        panic!("unacknowledged callback must not start");
        #[allow(unreachable_code)]
        Ok(json!(true))
    });
    worker.poll_activity_once().await.unwrap();
    assert_eq!(server.request_count(COMPLETE), 0);
    assert_eq!(server.request_count(FAIL), 0);
}

fn required_session() -> Value {
    let mut session = WorkerSessionOptions::new("original-session")
        .queue("lease-queue")
        .to_wire()
        .unwrap();
    session["status"] = json!("active");
    session["lease_owner"] = json!("lease-worker");
    session["lease_expires_at"] = json!(timestamp(Duration::from_millis(450)));
    session["ttl_expires_at"] = json!("2099-01-01T00:00:00.000000Z");
    session
}

fn session_replies(path: &str, body: &str, number: usize) -> Option<(&'static str, String)> {
    let value = match path {
        POLL => {
            let mut wire = delivery();
            wire["task"]["worker_session"] = required_session();
            wire
        }
        RENEW => {
            let mut reply = renewal();
            reply["worker_session"] = required_session();
            reply
        }
        _ => return replies(path, body, number),
    };
    Some(("200 OK", value.to_string()))
}

#[tokio::test]
async fn acknowledged_same_session_renewal_keeps_the_callback_session_live() {
    let server = server(session_replies);
    let mut worker = worker(&server).worker_sessions(true);
    worker
        .session_registration_confirmed
        .store(true, Ordering::SeqCst);
    worker.register_activity("LeaseActivity", |ctx, _args| async move {
        tokio::time::sleep(Duration::from_millis(850)).await;
        assert!(ctx.worker_session().unwrap().active());
        Ok(json!(true))
    });
    worker.poll_activity_once().await.unwrap();
    assert_eq!(server.request_count(COMPLETE), 1);
}

#[tokio::test]
async fn missing_required_session_renewal_abandons_without_running_user_code() {
    fn missing(path: &str, body: &str, number: usize) -> Option<(&'static str, String)> {
        if path == RENEW {
            return replies(path, body, number);
        }
        session_replies(path, body, number)
    }
    let server = server(missing);
    let mut worker = worker(&server).worker_sessions(true);
    worker
        .session_registration_confirmed
        .store(true, Ordering::SeqCst);
    let called = Arc::new(AtomicBool::new(false));
    let seen = called.clone();
    worker.register_activity("LeaseActivity", move |_ctx, _args| {
        seen.store(true, Ordering::SeqCst);
        async { Ok(json!(true)) }
    });
    worker.poll_activity_once().await.unwrap();
    assert!(!called.load(Ordering::SeqCst));
    assert_eq!(server.request_count(COMPLETE), 0);
}

#[tokio::test]
async fn managed_worker_stop_physically_drops_the_callback_before_any_terminal_post() {
    let server = server(replies);
    let stop = Arc::new(AtomicBool::new(false));
    let mut worker = worker(&server);
    worker.client.worker_storage_admission = Some(WorkerStorageAdmission {
        policy: WorkerRetryPolicy::default(),
        stop: stop.clone(),
        workflow_completion_retry_deadline: None,
    });
    let dropped = Arc::new(AtomicBool::new(false));
    let seen = dropped.clone();
    worker.register_activity("LeaseActivity", move |_ctx, _args| {
        let seen = seen.clone();
        let stop = stop.clone();
        async move {
            let _guard = Dropped(seen);
            stop.store(true, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_secs(2)).await;
            Ok(json!(true))
        }
    });
    worker.poll_activity_once().await.unwrap();
    assert!(dropped.load(Ordering::SeqCst));
    assert_eq!(server.request_count(COMPLETE), 0);
    assert_eq!(server.request_count(FAIL), 0);
}

#[tokio::test]
async fn required_session_original_ttl_cannot_be_extended_by_an_activity_renewal() {
    fn changed_ttl(path: &str, body: &str, number: usize) -> Option<(&'static str, String)> {
        if path == RENEW {
            let mut value = renewal();
            value["worker_session"] = required_session();
            if number > 1 {
                value["worker_session"]["ttl_expires_at"] = json!("2100-01-01T00:00:00.000000Z");
            }
            return Some(("200 OK", value.to_string()));
        }
        session_replies(path, body, number)
    }
    let server = server(changed_ttl);
    let mut worker = worker(&server).worker_sessions(true);
    worker
        .session_registration_confirmed
        .store(true, Ordering::SeqCst);
    worker.register_activity("LeaseActivity", |_ctx, _args| async {
        tokio::time::sleep(Duration::from_millis(850)).await;
        Ok(json!(true))
    });
    worker.poll_activity_once().await.unwrap();
    assert!(server.request_count(RENEW) >= 2);
    assert_eq!(server.request_count(COMPLETE), 0);
    assert_eq!(server.request_count(FAIL), 0);
}

#[tokio::test]
async fn payload_loading_is_maintained_before_user_callback_entry() {
    const FETCH: &str = "/api/external-payloads/v1/ep_00000000000000000000000001";
    fn blob() -> String {
        encode_value_envelope(
            &json!([9_223_372_036_854_775_000_i64, null, "継続"]),
            DEFAULT_CODEC,
        )
        .unwrap()["blob"]
            .as_str()
            .unwrap()
            .to_owned()
    }
    fn external(path: &str, body: &str, number: usize) -> Option<(&'static str, String)> {
        if path == POLL {
            let mut wire = delivery();
            let blob = blob();
            wire["task"]["arguments"] = json!({"codec":"avro","external_payload":{
                "schema":"durable-workflow.v2.runtime-external-payload-reference.v1","codec":"avro",
                "reference_id":"ep_00000000000000000000000001","size_bytes":blob.len(),
                "sha256":format!("{:x}",Sha256::digest(blob.as_bytes()))
            }});
            return Some(("200 OK", wire.to_string()));
        }
        if path == FETCH {
            std::thread::sleep(Duration::from_millis(650));
            return Some(("200 OK", blob()));
        }
        replies(path, body, number)
    }
    let server = server(external);
    let mut worker = worker(&server);
    worker.register_activity("LeaseActivity", |_ctx, args| async move {
        assert_eq!(args, json!([9_223_372_036_854_775_000_i64, null, "継続"]));
        Ok(json!(true))
    });
    worker.poll_activity_once().await.unwrap();
    assert!(server.request_count(RENEW) >= 2);
    assert_eq!(server.request_count(FETCH), 1);
    assert_eq!(server.request_count(COMPLETE), 1);
}

#[tokio::test]
async fn cooperative_worker_uses_the_same_renewal_and_callback_scope() {
    fn cooperative(path: &str, body: &str, number: usize) -> Option<(&'static str, String)> {
        if path.ends_with("/activity-tasks/lease-task/status") {
            return Some(("200 OK",json!({
                "task_id":"lease-task","activity_attempt_id":"lease-attempt","lease_owner":"lease-worker",
                "can_continue":true,"cancel_requested":false,"reason":null,"heartbeat_recorded":false,
                "lease_expires_at":timestamp(Duration::from_millis(450)),"deadlines":{},"worker_session":null,
                "task_status":"leased","attempt_status":"running","activity_status":"running"
            }).to_string()));
        }
        replies(path, body, number)
    }
    let server = server(cooperative);
    let mut worker = worker(&server).cooperative_cancellation(true);
    worker.register_activity("LeaseActivity", |_ctx, _args| async {
        tokio::time::sleep(Duration::from_millis(850)).await;
        Ok(json!(true))
    });
    worker.poll_activity_once().await.unwrap();
    assert!(server.request_count(RENEW) >= 2);
    assert_eq!(server.request_count(COMPLETE), 1);
    assert_eq!(server.request_count(FAIL), 0);
}

// sdk-php#445: a slow or flaky link at activity start must not drop the claim.
// Lease grants are long enough here that one stalled renewal (at most half of
// the remaining grant) still leaves room to retry inside the original lease.
fn long_lease(path: &str, body: &str, number: usize) -> Option<(&'static str, String)> {
    let grant = Duration::from_secs(4);
    match path {
        POLL => {
            let mut wire = delivery();
            wire["task"]["lease_expires_at"] = json!(timestamp(grant));
            Some(("200 OK", wire.to_string()))
        }
        RENEW => {
            let mut reply = renewal();
            reply["lease_expires_at"] = json!(timestamp(grant));
            Some(("200 OK", reply.to_string()))
        }
        _ => replies(path, body, number),
    }
}

fn flaky_start(path: &str, body: &str, number: usize) -> Option<(&'static str, String)> {
    if path == RENEW && number == 1 {
        // Stalls past the whole original grant: the attempt times out.
        std::thread::sleep(Duration::from_millis(4_500));
    }
    long_lease(path, body, number)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slow_and_flaky_initial_renewal_keeps_the_claim_and_completes() {
    let server = MockWorkerServer::start_with_behavior(MockWorkerBehavior {
        request_override: Some(flaky_start),
        // The second renewal loses its connection without a reply.
        drop_request: Some(|path, number| path == RENEW && number == 2),
        concurrent_requests: true,
        ..MockWorkerBehavior::default()
    });
    let entered = Arc::new(AtomicBool::new(false));
    let seen = Arc::clone(&entered);
    let mut worker = worker(&server);
    worker.register_activity("LeaseActivity", move |_ctx, _args| {
        seen.store(true, Ordering::SeqCst);
        // Outlives the poll's original 4 s grant; only a later renewal keeps it.
        async {
            tokio::time::sleep(Duration::from_millis(2_500)).await;
            Ok(json!(true))
        }
    });
    worker.poll_activity_once().await.unwrap();
    assert!(
        entered.load(Ordering::SeqCst),
        "the claim must not be dropped"
    );
    assert!(server.request_count(RENEW) >= 3, "renewal must be retried");
    assert_eq!(server.request_count(COMPLETE), 1);
    assert_eq!(server.request_count(FAIL), 0);
}

#[tokio::test]
async fn unavailable_initial_renewal_runs_under_the_original_grant() {
    fn first_unavailable(path: &str, body: &str, number: usize) -> Option<(&'static str, String)> {
        if path == RENEW && number == 1 {
            return Some((
                "503 Service Unavailable",
                json!({"reason":"backend_lock_pressure"}).to_string(),
            ));
        }
        long_lease(path, body, number)
    }
    let server = server(first_unavailable);
    let mut worker = worker(&server);
    worker.register_activity("LeaseActivity", |_ctx, _args| async { Ok(json!(true)) });
    worker.poll_activity_once().await.unwrap();
    assert_eq!(server.request_count(RENEW), 1);
    assert_eq!(server.request_count(COMPLETE), 1);
    assert_eq!(server.request_count(FAIL), 0);
}

#[tokio::test]
async fn initial_renewal_that_never_recovers_retries_until_the_grant_is_gone() {
    fn always_unavailable(path: &str, body: &str, number: usize) -> Option<(&'static str, String)> {
        if path == RENEW {
            return Some((
                "503 Service Unavailable",
                json!({"reason":"backend_lock_pressure"}).to_string(),
            ));
        }
        replies(path, body, number)
    }
    let server = server(always_unavailable);
    let entered = Arc::new(AtomicBool::new(false));
    let completed = Arc::new(AtomicBool::new(false));
    let (entered_seen, completed_seen) = (Arc::clone(&entered), Arc::clone(&completed));
    let mut worker = worker(&server);
    worker.register_activity("LeaseActivity", move |_ctx, _args| {
        entered_seen.store(true, Ordering::SeqCst);
        let completed_seen = Arc::clone(&completed_seen);
        async move {
            tokio::time::sleep(Duration::from_millis(850)).await;
            completed_seen.store(true, Ordering::SeqCst);
            Ok(json!(true))
        }
    });
    worker.poll_activity_once().await.unwrap();
    assert!(entered.load(Ordering::SeqCst));
    assert!(!completed.load(Ordering::SeqCst));
    assert!(server.request_count(RENEW) >= 2, "renewal must be retried");
    assert_eq!(server.request_count(COMPLETE), 0);
    assert_worker_lost_release(&server);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn callback_that_stops_after_the_lease_is_not_released() {
    fn always_unavailable(path: &str, body: &str, number: usize) -> Option<(&'static str, String)> {
        if path == RENEW {
            return Some((
                "503 Service Unavailable",
                json!({"reason":"backend_lock_pressure"}).to_string(),
            ));
        }
        replies(path, body, number)
    }
    let server = server(always_unavailable);
    let mut worker = worker(&server);
    worker.register_activity("LeaseActivity", |_ctx, _args| async {
        // Blocks its thread past the 450 ms lease, so the dropped callback is
        // confirmed stopped only after the deadline.
        std::thread::sleep(Duration::from_millis(700));
        Ok(json!(true))
    });
    tokio::spawn(async move { worker.poll_activity_once().await })
        .await
        .unwrap()
        .unwrap();
    assert!(server.request_count(RENEW) >= 2);
    assert_eq!(server.request_count(COMPLETE), 0);
    assert_eq!(
        server.request_count(FAIL),
        0,
        "a late release must not be sent"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delivered_lease_is_measured_from_the_poll_start() {
    fn slow_poll(path: &str, body: &str, number: usize) -> Option<(&'static str, String)> {
        match path {
            POLL => {
                // The Server stamps the 450 ms grant, then the reply takes
                // 350 ms to arrive.
                let wire = delivery().to_string();
                std::thread::sleep(Duration::from_millis(350));
                Some(("200 OK", wire))
            }
            RENEW => Some((
                "503 Service Unavailable",
                json!({"reason":"backend_lock_pressure"}).to_string(),
            )),
            _ => replies(path, body, number),
        }
    }
    let server = server(slow_poll);
    let mut worker = worker(&server);
    worker.register_activity("LeaseActivity", |_ctx, _args| async {
        tokio::time::sleep(Duration::from_millis(600)).await;
        Ok(json!(true))
    });
    let started = std::time::Instant::now();
    worker.poll_activity_once().await.unwrap();
    // Measured from the reply, the grant would seem to last until ~800 ms and
    // the claim would be held past the Server's real deadline (~450 ms).
    assert!(
        started.elapsed() < Duration::from_millis(600),
        "claim held for {:?}",
        started.elapsed()
    );
    assert_eq!(server.request_count(COMPLETE), 0);
}

#[tokio::test]
async fn refused_renewal_ends_the_claim_without_a_release() {
    fn refused(path: &str, body: &str, number: usize) -> Option<(&'static str, String)> {
        if path == RENEW && number > 1 {
            return Some((
                "409 Conflict",
                json!({"reason":"lease_expired","can_continue":false}).to_string(),
            ));
        }
        replies(path, body, number)
    }
    let server = server(refused);
    let mut worker = worker(&server);
    worker.register_activity("LeaseActivity", |_ctx, _args| async {
        tokio::time::sleep(Duration::from_millis(700)).await;
        Ok(json!(true))
    });
    worker.poll_activity_once().await.unwrap();
    assert!(server.request_count(RENEW) >= 2);
    assert_eq!(server.request_count(COMPLETE), 0);
    assert_eq!(server.request_count(FAIL), 0);
}

fn failure_from(payload: Value) -> ActivityFailure {
    let event = HistoryEvent {
        event_type: "ActivityFailed".into(),
        payload,
        raw: HashMap::new(),
    };
    activity_outcome(&event, DEFAULT_CODEC, None)
        .unwrap()
        .unwrap_err()
}

#[test]
fn lease_loss_reason_names_released_and_server_settled_attempts() {
    let lease_class = "Workflow\\V2\\Exceptions\\ActivityLeaseExpiredException";
    let released = failure_from(json!({"exception_type":"worker_lost",
        "exception_class":"ActivityWorkerLost","non_retryable":false,"message":"released"}));
    assert_eq!(
        released.lease_loss_reason(),
        Some(LeaseLossReason::WorkerLost)
    );
    assert_eq!(LeaseLossReason::WorkerLost.as_str(), "worker_lost");

    let expired =
        failure_from(json!({"exception_class":lease_class,"failure_reason":"lease_expired"}));
    assert_eq!(
        expired.lease_loss_reason(),
        Some(LeaseLossReason::LeaseExpired)
    );
    assert_eq!(LeaseLossReason::LeaseExpired.as_str(), "lease_expired");

    // Server 3.16 shape: failure_reason beside exception_class, no exception.
    let revoked_beside = failure_from(json!({"exception_class":lease_class,
        "failure_category":"internal","failure_reason":"worker_deregistered",
        "lease_owner":"gone-worker","non_retryable":false,"timeout_kind":"lease"}));
    assert_eq!(
        revoked_beside.lease_loss_reason(),
        Some(LeaseLossReason::WorkerDeregistered)
    );
    assert_eq!(
        revoked_beside.exception.as_ref().unwrap()["failure_reason"],
        "worker_deregistered"
    );
    // A recorded exception keeps its own fields and gains only the reason.
    let with_exception = failure_from(json!({"exception_class":lease_class,
        "failure_reason":"worker_deregistered","exception":{"class":lease_class,"message":"lost"}}));
    assert_eq!(
        with_exception.lease_loss_reason(),
        Some(LeaseLossReason::WorkerDeregistered)
    );
    assert_eq!(
        with_exception.exception.as_ref().unwrap()["message"],
        "lost"
    );

    let revoked = failure_from(json!({"exception":{"class":lease_class,
        "properties":{"failure_reason":"worker_deregistered"}}}));
    assert_eq!(
        revoked.lease_loss_reason(),
        Some(LeaseLossReason::WorkerDeregistered)
    );
    assert_eq!(
        revoked.lease_loss_reason().unwrap().to_string(),
        "worker_deregistered"
    );

    let bare = failure_from(json!({"exception":{"class":lease_class,"message":"lost"}}));
    assert_eq!(
        bare.lease_loss_reason(),
        Some(LeaseLossReason::LeaseExpired)
    );
}

#[test]
fn other_activity_failures_have_no_lease_loss_reason() {
    let application = failure_from(json!({"exception_type":"PaymentDeclined",
        "exception_class":"payments.PaymentDeclined","message":"declined"}));
    assert_eq!(application.lease_loss_reason(), None);
    // A failure_reason alone does not make an application failure a lease loss.
    let stray = failure_from(json!({"exception_type":"Custom","failure_reason":"lease_expired"}));
    assert_eq!(stray.lease_loss_reason(), None);
    let event = HistoryEvent {
        event_type: "ActivityTimedOut".into(),
        payload: json!({"timeout_kind":"start_to_close"}),
        raw: HashMap::new(),
    };
    let timed_out = activity_outcome(&event, DEFAULT_CODEC, None)
        .unwrap()
        .unwrap_err();
    assert_eq!(timed_out.lease_loss_reason(), None);
}
