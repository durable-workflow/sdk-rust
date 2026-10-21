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
            .build().unwrap(),
        "lease-queue",
    ).worker_id("lease-worker")
}

fn server(overrides: RequestOverride) -> MockWorkerServer {
    MockWorkerServer::start_with_behavior(MockWorkerBehavior {
        request_override: Some(overrides),
        concurrent_requests: true,
        ..MockWorkerBehavior::default()
    })
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
    assert!(server.request_count(RENEW) >= 2, "original claim must be maintained");
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
    fn drop(&mut self) { self.0.store(true, Ordering::SeqCst); }
}

fn revoked(path: &str, body: &str, number: usize) -> Option<(&'static str, String)> {
    if path == RENEW && number > 1 {
        return Some(("409 Conflict", json!({"reason":"stale_attempt","can_continue":false}).to_string()));
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
    let result = client.complete_activity_task("lease-task", "lease-attempt", "lease-worker", true, "avro").await;
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
