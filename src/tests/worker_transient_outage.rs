use super::*;

const WORKFLOW_POLL: &str = "/api/worker/workflow-tasks/poll";

fn storage_unavailable() -> (&'static str, String) {
    (
        "503 Service Unavailable",
        json!({"reason":"storage_unavailable","message":"storage_unavailable"}).to_string(),
    )
}

fn warming_up(path: &str, _body: &str, number: usize) -> Option<(&'static str, String)> {
    (path == WORKFLOW_POLL && number <= 8).then(storage_unavailable)
}

fn worker(server: &MockWorkerServer) -> Worker {
    let client = Client::builder(server.base_url())
        .timeout(Duration::from_secs(2))
        .build()
        .expect("client");
    let mut worker = Worker::new(client, "rust-workers")
        .max_concurrent_workflow_tasks(1)
        .poll_timeout(Duration::from_millis(10))
        .retry_policy(WorkerRetryPolicy {
            max_retries: 5,
            initial_backoff: Duration::from_millis(2),
            max_backoff: Duration::from_millis(5),
        });
    worker.register_workflow("counter", |_ctx, _input| async move { Ok(Value::Null) });
    worker
}

#[tokio::test]
async fn default_worker_survives_storage_warm_up_503s() {
    let server = MockWorkerServer::start_with_behavior(MockWorkerBehavior {
        request_override: Some(warming_up),
        ..MockWorkerBehavior::default()
    });
    let resumed = async {
        while server.request_count(WORKFLOW_POLL) < 11 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(10), worker(&server).run_until(resumed))
        .await
        .expect("polling must resume after warm-up")
        .expect("storage_unavailable during warm-up must not stop the worker");
}

fn upstream_and_retryable(
    path: &str,
    _body: &str,
    number: usize,
) -> Option<(&'static str, String)> {
    match (path, number) {
        ("/api/worker/register", 1) => Some(storage_unavailable()),
        ("/api/worker/register", 2) => Some(("502 Bad Gateway", "<html>bad gateway</html>".into())),
        (WORKFLOW_POLL, 1..=3) => Some(("504 Gateway Timeout", String::new())),
        (WORKFLOW_POLL, 4..=10) => Some((
            "500 Internal Server Error",
            json!({"reason":"lock_pressure","retryable":true}).to_string(),
        )),
        (WORKFLOW_POLL, 11..=13) => Some((
            "503 Service Unavailable",
            json!({"reason":"backend_unavailable"}).to_string(),
        )),
        _ => None,
    }
}

#[tokio::test]
async fn registration_and_untyped_or_retryable_poll_failures_are_outlived() {
    let server = MockWorkerServer::start_with_behavior(MockWorkerBehavior {
        request_override: Some(upstream_and_retryable),
        ..MockWorkerBehavior::default()
    });
    let resumed = async {
        while server.request_count(WORKFLOW_POLL) < 16 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(10), worker(&server).run_until(resumed))
        .await
        .expect("polling must resume after the outage")
        .expect("transient failures must not stop a running worker");
    assert_eq!(server.request_count("/api/worker/register"), 3);
}

fn fatal_after_outage(
    status: &'static str,
    body: Value,
) -> impl Fn(&str, usize) -> Option<(&'static str, String)> {
    move |path, number| match (path, number) {
        (WORKFLOW_POLL, 1..=2) => Some(storage_unavailable()),
        (WORKFLOW_POLL, _) => Some((status, body.to_string())),
        _ => None,
    }
}

fn unauthorized(path: &str, _body: &str, number: usize) -> Option<(&'static str, String)> {
    fatal_after_outage("401 Unauthorized", json!({"reason":"unauthorized"}))(path, number)
}

fn not_retryable(path: &str, _body: &str, number: usize) -> Option<(&'static str, String)> {
    fatal_after_outage(
        "503 Service Unavailable",
        json!({"reason":"storage_unavailable","retryable":false}),
    )(path, number)
}

fn forbidden_registration(
    path: &str,
    _body: &str,
    _number: usize,
) -> Option<(&'static str, String)> {
    (path == "/api/worker/register")
        .then(|| ("403 Forbidden", json!({"reason":"forbidden"}).to_string()))
}

#[tokio::test]
async fn auth_and_non_retryable_failures_stop_the_worker() {
    for (name, handler, status) in [
        ("401 poll", unauthorized as RequestOverride, 401),
        ("retryable:false poll", not_retryable, 503),
        ("403 registration", forbidden_registration, 403),
    ] {
        let server = MockWorkerServer::start_with_behavior(MockWorkerBehavior {
            request_override: Some(handler),
            ..MockWorkerBehavior::default()
        });
        let error = tokio::time::timeout(Duration::from_secs(5), worker(&server).run())
            .await
            .unwrap_or_else(|_| panic!("{name}: a fatal failure must stop the worker"))
            .expect_err(name);
        let observed = match &error {
            Error::Http { status, .. } => status.as_u16(),
            Error::Protocol(failure) => failure.status,
            other => panic!("{name}: unexpected error {other:?}"),
        };
        assert_eq!(observed, status, "{name}");
    }
}

#[test]
fn transient_classification_matches_the_shared_definition() {
    let http = |status: u16, body: &str| Error::Http {
        status: reqwest::StatusCode::from_u16(status).unwrap(),
        body: body.to_owned(),
    };
    for transient in [
        http(
            503,
            r#"{"reason":"storage_unavailable","message":"storage_unavailable"}"#,
        ),
        http(503, r#"{"reason":"backend_unavailable"}"#),
        http(502, "<html>bad gateway</html>"),
        http(504, ""),
        http(503, "{}"),
        http(500, r#"{"reason":"lock_pressure","retryable":true}"#),
        http(429, r#"{"retryable":true}"#),
        Error::Timeout,
    ] {
        assert!(worker_error_is_transient(&transient), "{transient:?}");
    }
    for fatal in [
        http(503, r#"{"reason":"storage_unavailable","retryable":false}"#),
        http(401, r#"{"reason":"unauthorized"}"#),
        http(403, ""),
        http(409, r#"{"reason":"stale_attempt"}"#),
        http(429, r#"{"reason":"rate_limited"}"#),
        http(500, r#"{"reason":"internal_error"}"#),
        http(503, r#"{"reason":"draining"}"#),
        http(400, r#"{"retryable":true}"#),
    ] {
        assert!(!worker_error_is_transient(&fatal), "{fatal:?}");
    }
}

#[test]
fn outage_backoff_uses_full_jitter_up_to_four_seconds() {
    for (failures, ceiling) in [
        (1, 250),
        (2, 500),
        (3, 1_000),
        (4, 2_000),
        (5, 4_000),
        (9, 4_000),
    ] {
        let samples: Vec<_> = (0..200).map(|_| worker_outage_delay(failures)).collect();
        assert!(samples
            .iter()
            .all(|delay| *delay <= Duration::from_millis(ceiling)));
        assert!(samples
            .iter()
            .any(|delay| *delay < Duration::from_millis(ceiling / 2)));
    }
}

const ACTIVITY_POLL: &str = "/api/worker/activity-tasks/poll";
const COMPLETE: &str = "/api/worker/activity-tasks/retry-task/complete";

fn stamp(after: Duration) -> String {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap() + after;
    DateTime::from_timestamp(now.as_secs() as i64, now.subsec_nanos())
        .unwrap()
        .to_rfc3339_opts(chrono::SecondsFormat::Micros, true)
}

fn activity_delivery(lease: Duration) -> String {
    json!({
        "server_time": stamp(Duration::ZERO),
        "task": {
            "task_id":"retry-task","workflow_id":"retry-workflow","run_id":"retry-run",
            "activity_execution_id":"retry-execution","activity_attempt_id":"retry-attempt",
            "activity_type":"RetryActivity","attempt_number":1,"lease_owner":"retry-worker",
            "task_queue":"rust-workers","payload_codec":"avro",
            "lease_expires_at": stamp(lease),
            "arguments": encode_value_envelope(&json!([]), DEFAULT_CODEC).unwrap(),
        }
    })
    .to_string()
}

/// Native Server 3.17's answer to a resent completion it already recorded.
fn duplicate_completion() -> String {
    json!({"task_id":"retry-task","activity_attempt_id":"retry-attempt","outcome":"completed",
        "recorded":false,"reason":"stale_attempt","next_task_id":null,"lease_owner":"retry-worker",
        "activity_status":"completed","attempt_status":"completed","task_status":"completed",
        "protocol_version":"1.20"})
    .to_string()
}

fn committed_then_hidden(path: &str, _body: &str, number: usize) -> Option<(&'static str, String)> {
    match (path, number) {
        (ACTIVITY_POLL, 1) => Some(("200 OK", activity_delivery(Duration::from_secs(5)))),
        (ACTIVITY_POLL, _) => Some(("200 OK", json!({"task": null}).to_string())),
        // The first completion is committed, but its answer is lost upstream.
        (COMPLETE, 1) => Some(storage_unavailable()),
        (COMPLETE, _) => Some(("409 Conflict", duplicate_completion())),
        _ => None,
    }
}

fn activity_worker(server: &MockWorkerServer) -> Worker {
    let client = Client::builder(server.base_url()).build().expect("client");
    let mut worker = Worker::new(client, "rust-workers")
        .worker_id("retry-worker")
        .retry_policy(WorkerRetryPolicy {
            max_retries: 5,
            initial_backoff: Duration::from_millis(2),
            max_backoff: Duration::from_millis(5),
        });
    worker.register_activity("RetryActivity", |_ctx, _args| async { Ok(json!("done")) });
    worker
}

#[tokio::test]
async fn completion_lost_after_commit_is_resent_and_its_duplicate_counts_as_recorded() {
    let server = MockWorkerServer::start_with_behavior(MockWorkerBehavior {
        request_override: Some(committed_then_hidden),
        ..MockWorkerBehavior::default()
    });
    let outcome = activity_worker(&server).poll_activity_once().await;
    assert!(
        matches!(outcome, Ok(ManagedPollOutcome::Handled)),
        "{outcome:?}"
    );
    let bodies = server.request_bodies(COMPLETE);
    assert_eq!(bodies.len(), 2, "the same completion is resent once");
    assert_eq!(bodies[0], bodies[1], "the resend is the identical report");
}

fn never_delivered(path: &str, _body: &str, number: usize) -> Option<(&'static str, String)> {
    match (path, number) {
        (ACTIVITY_POLL, 1) => Some(("200 OK", activity_delivery(Duration::from_millis(400)))),
        (ACTIVITY_POLL, _) => Some(("200 OK", json!({"task": null}).to_string())),
        (COMPLETE, _) => Some(storage_unavailable()),
        _ => None,
    }
}

#[tokio::test]
async fn completion_resends_stop_when_the_lease_ends() {
    let server = MockWorkerServer::start_with_behavior(MockWorkerBehavior {
        request_override: Some(never_delivered),
        ..MockWorkerBehavior::default()
    });
    let started = Instant::now();
    let outcome = activity_worker(&server).poll_activity_once().await;
    assert!(
        matches!(outcome, Ok(ManagedPollOutcome::Handled)),
        "{outcome:?}"
    );
    assert!(
        started.elapsed() < Duration::from_millis(600),
        "bounded by the lease"
    );
    assert!(
        server.request_count(COMPLETE) >= 2,
        "resent while the lease lasted"
    );
}

#[test]
fn only_this_attempts_own_recorded_outcome_counts_as_a_duplicate() {
    let rejection = |body: Value| {
        Error::ActivityTaskRejected(ActivityTaskRejection {
            operation: "complete".into(),
            status: 409,
            reason: body["reason"].as_str().unwrap_or_default().into(),
            task_id: "retry-task".into(),
            activity_attempt_id: "retry-attempt".into(),
            cancel_requested: false,
            can_continue: None,
            run_closed_reason: None,
            body,
        })
    };
    let own: Value = serde_json::from_str(&duplicate_completion()).unwrap();
    let check = |body: &Value, report| {
        activity_report_already_recorded(
            &rejection(body.clone()),
            report,
            "retry-task",
            "retry-attempt",
            "retry-worker",
        )
    };
    assert!(check(&own, ActivityReport::Completed));
    assert!(!check(&own, ActivityReport::Failed));
    for (field, value) in [
        ("activity_status", "expired"),
        ("attempt_status", "failed"),
        ("task_status", "cancelled"),
        ("lease_owner", "another-worker"),
        ("activity_attempt_id", "another-attempt"),
    ] {
        let mut other = own.clone();
        other[field] = json!(value);
        assert!(!check(&other, ActivityReport::Completed), "{field}={value}");
    }
    let mut recorded = own.clone();
    recorded["recorded"] = json!(true);
    assert!(!check(&recorded, ActivityReport::Completed));
    let mut failed = own.clone();
    failed["attempt_status"] = json!("failed");
    failed["outcome"] = json!("failed");
    assert!(check(&failed, ActivityReport::Failed));
}

#[test]
fn retries_beyond_the_budget_use_the_shared_jittered_schedule() {
    let policy = WorkerRetryPolicy {
        max_retries: 2,
        initial_backoff: Duration::from_millis(100),
        max_backoff: Duration::from_secs(5),
    };
    // Within the budget: the policy's own capped exponential delay.
    assert_eq!(
        worker_poll_retry_delay(policy, 1),
        Duration::from_millis(100)
    );
    assert_eq!(
        worker_poll_retry_delay(policy, 2),
        Duration::from_millis(200)
    );
    // Beyond it: uniform(0, min(4 s, 0.25 s * 2^(n-1))) for the n-th retry
    // past the budget, never the fixed policy cap.
    for (retry, ceiling) in [
        (3, 250),
        (4, 500),
        (5, 1_000),
        (6, 2_000),
        (7, 4_000),
        (40, 4_000),
    ] {
        let samples: Vec<_> = (0..200)
            .map(|_| worker_poll_retry_delay(policy, retry))
            .collect();
        assert!(samples
            .iter()
            .all(|delay| *delay <= Duration::from_millis(ceiling)));
        assert!(
            samples
                .iter()
                .any(|delay| *delay < Duration::from_millis(ceiling / 2)),
            "retry {retry} must be jittered"
        );
        let distinct: std::collections::HashSet<_> = samples.iter().collect();
        assert!(
            distinct.len() > 100,
            "retry {retry} delays must not be in step"
        );
    }
}

#[test]
fn a_continuing_outage_escalates_to_error_at_one_minute_then_every_five() {
    let start = std::time::Instant::now();
    let at = |seconds: u64| start + Duration::from_secs(seconds);
    let mut log = OutageLog::default();
    assert_eq!(log.failed(at(0)), None);
    assert_eq!(log.failed(at(30)), None);
    assert_eq!(log.failed(at(59)), None);
    assert_eq!(log.failed(at(60)), Some(Duration::from_secs(60)));
    assert_eq!(log.failed(at(61)), None);
    assert_eq!(log.failed(at(359)), None);
    assert_eq!(log.failed(at(360)), Some(Duration::from_secs(360)));
    assert_eq!(log.failed(at(600)), None);
    assert_eq!(log.failed(at(660)), Some(Duration::from_secs(660)));
    // A success resets it: the next outage waits a full minute again.
    let mut outage = Outage { failures: 7, log };
    outage.reset();
    assert_eq!(outage.failures, 0);
    assert_eq!(outage.log.failed(at(700)), None);
    assert_eq!(outage.log.failed(at(759)), None);
    assert_eq!(outage.log.failed(at(760)), Some(Duration::from_secs(60)));
}

const CLUSTER_INFO: &str = "/api/cluster/info";

fn cluster_info_down_twice(
    path: &str,
    body: &str,
    number: usize,
) -> Option<(&'static str, String)> {
    match (path, number) {
        (CLUSTER_INFO, 1) => Some(("500 Internal Server Error", "upstream exploded".into())),
        // Request 2 is dropped without a reply (see `refused_second`).
        (CLUSTER_INFO, _) => Some((
            "200 OK",
            json!({"worker_protocol":{"server_capabilities":{"query_task_authority":true}}})
                .to_string(),
        )),
        ("/api/worker/register", _) => {
            let request: Value = serde_json::from_str(body).unwrap();
            Some((
                "200 OK",
                json!({"registered":true,"worker_id":request["worker_id"],
                    "heartbeat_interval_seconds":3600,"protocol_version":"1.20",
                    "server_capabilities":{"query_task_authority":true}})
                .to_string(),
            ))
        }
        _ => None,
    }
}

fn refused_second(path: &str, number: usize) -> bool {
    path == CLUSTER_INFO && number == 2
}

#[tokio::test]
async fn startup_cluster_info_failures_are_waited_out() {
    let server = MockWorkerServer::start_with_behavior(MockWorkerBehavior {
        request_override: Some(cluster_info_down_twice),
        drop_request: Some(refused_second),
        ..MockWorkerBehavior::default()
    });
    let mut worker = worker(&server).require_query_task_authority(true);
    worker.register_query("counter", "current", |_ctx, _args| async move {
        Ok(Value::Null)
    });
    let polling = async {
        while server.request_count(WORKFLOW_POLL) == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(10), worker.run_until(polling))
        .await
        .expect("startup must get past the outage")
        .expect("a cluster-info outage must not stop the worker");
    assert_eq!(server.request_count(CLUSTER_INFO), 3);
    assert_eq!(server.request_count("/api/worker/register"), 1);
}

fn completion_discovery_down_twice(
    path: &str,
    _body: &str,
    number: usize,
) -> Option<(&'static str, String)> {
    match (path, number) {
        (ACTIVITY_POLL, 1) => Some(("200 OK", activity_delivery(Duration::from_secs(30)))),
        (ACTIVITY_POLL, _) => Some(("200 OK", json!({"task": null}).to_string())),
        (CLUSTER_INFO, 1) => Some(("500 Internal Server Error", "upstream exploded".into())),
        (CLUSTER_INFO, _) => Some(("200 OK", json!({}).to_string())),
        (COMPLETE, _) => Some(("200 OK", json!({"recorded":true}).to_string())),
        _ => None,
    }
}

#[tokio::test]
async fn run_time_cluster_info_failures_are_waited_out() {
    let server = MockWorkerServer::start_with_behavior(MockWorkerBehavior {
        request_override: Some(completion_discovery_down_twice),
        drop_request: Some(refused_second),
        concurrent_requests: true,
        ..MockWorkerBehavior::default()
    });
    let worker = activity_worker(&server)
        .max_concurrent_activity_tasks(1)
        .poll_timeout(Duration::from_millis(10));
    let completed = async {
        while server.request_count(COMPLETE) == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(10), worker.run_until(completed))
        .await
        .expect("the completion must get past the outage")
        .expect("a cluster-info outage must not stop the worker");
    assert_eq!(server.request_count(CLUSTER_INFO), 3);
    assert_eq!(server.request_count(COMPLETE), 1);
}

#[test]
fn cluster_info_failures_other_than_refusals_are_transient() {
    let http = |status: u16, body: &str| Error::Http {
        status: reqwest::StatusCode::from_u16(status).unwrap(),
        body: body.to_owned(),
    };
    assert!(cluster_info_failure_is_transient(&http(
        500,
        "upstream exploded"
    )));
    assert!(cluster_info_failure_is_transient(&http(503, "")));
    assert!(cluster_info_failure_is_transient(&http(429, "{}")));
    assert!(cluster_info_failure_is_transient(&Error::Timeout));
    assert!(!cluster_info_failure_is_transient(&http(
        500,
        r#"{"retryable":false}"#
    )));
    assert!(!cluster_info_failure_is_transient(&http(401, "")));
    assert!(!cluster_info_failure_is_transient(&http(404, "")));
}

const UPLOAD: &str = "/api/external-payloads/v1";

fn payload_policy(threshold: usize) -> String {
    let mut policy = super::runtime_uploads::policy();
    policy["limits"]["max_payload_bytes"] = json!(4 * 1024 * 1024);
    let storage = &mut policy["namespace"]["external_payload_storage"];
    storage["threshold_bytes"] = json!(threshold);
    storage["transport"]["limits"]["max_payload_bytes"] = json!(8 * 1024 * 1024);
    policy.to_string()
}

fn uploaded(blob: &str) -> String {
    json!({"schema":"durable-workflow.v2.runtime-external-payload-upload.v1",
        "transport_version":1, "reference":super::runtime_uploads::reference(blob)})
    .to_string()
}

fn sized_worker(server: &MockWorkerServer) -> Worker {
    let client = Client::builder(server.base_url()).build().expect("client");
    let mut worker = Worker::new(client, "rust-workers")
        .worker_id("retry-worker")
        .retry_policy(WorkerRetryPolicy {
            max_retries: 5,
            initial_backoff: Duration::from_millis(2),
            max_backoff: Duration::from_millis(5),
        });
    let calls = Arc::new(AtomicUsize::new(0));
    worker.register_activity("RetryActivity", move |_ctx, _args| {
        let call = calls.fetch_add(1, Ordering::SeqCst);
        async move {
            Ok(if call == 0 {
                json!("small")
            } else {
                json!("x".repeat(500_000))
            })
        }
    });
    worker
}

fn lowered_limit(path: &str, body: &str, number: usize) -> Option<(&'static str, String)> {
    match (path, number) {
        (ACTIVITY_POLL, 1 | 2) => Some(("200 OK", activity_delivery(Duration::from_secs(30)))),
        (ACTIVITY_POLL, _) => Some(("200 OK", json!({"task": null}).to_string())),
        // Cached at 2 MB; the Server then lowers its inline limit to 100 KB.
        (CLUSTER_INFO, 1) => Some(("200 OK", payload_policy(2 * 1024 * 1024))),
        (CLUSTER_INFO, _) => Some(("200 OK", payload_policy(100 * 1024))),
        (UPLOAD, _) => Some(("201 Created", uploaded(body))),
        // The large result sent inline under the stale policy is refused.
        (COMPLETE, 2) => Some((
            "413 Payload Too Large",
            json!({"reason":"payload_too_large","message":"request exceeds 100 KB"}).to_string(),
        )),
        (COMPLETE, _) => Some(("200 OK", json!({"recorded":true}).to_string())),
        _ => None,
    }
}

#[tokio::test]
async fn a_report_refused_after_a_lowered_limit_is_externalized_and_resent_once() {
    let server = MockWorkerServer::start_with_behavior(MockWorkerBehavior {
        request_override: Some(lowered_limit),
        ..MockWorkerBehavior::default()
    });
    let worker = sized_worker(&server);
    for _ in 0..2 {
        let outcome = worker.poll_activity_once().await;
        assert!(
            matches!(outcome, Ok(ManagedPollOutcome::Handled)),
            "{outcome:?}"
        );
    }
    let completes = server.request_bodies(COMPLETE);
    assert_eq!(completes.len(), 3, "small, refused large, re-encoded large");
    assert!(
        completes[1]["result"].to_string().len() > 500_000,
        "first sent inline"
    );
    assert_eq!(
        completes[2]["result"]["external_payload"]["schema"],
        crate::runtime_payloads::SCHEMA,
        "resent with the payload externalized: {}",
        completes[2]["result"]
    );
    assert_eq!(server.request_count(UPLOAD), 1);
    assert_eq!(server.request_count(CLUSTER_INFO), 2, "rediscovered once");
}

fn policy_then_outage(path: &str, _body: &str, number: usize) -> Option<(&'static str, String)> {
    match (path, number) {
        (ACTIVITY_POLL, 1..=3) => Some(("200 OK", activity_delivery(Duration::from_secs(30)))),
        (ACTIVITY_POLL, _) => Some(("200 OK", json!({"task": null}).to_string())),
        (CLUSTER_INFO, 1) => Some(("200 OK", payload_policy(2 * 1024 * 1024))),
        (CLUSTER_INFO, _) => Some(("500 Internal Server Error", "upstream exploded".into())),
        (COMPLETE, _) => Some(("200 OK", json!({"recorded":true}).to_string())),
        _ => None,
    }
}

#[tokio::test]
async fn an_expired_policy_keeps_reports_moving_through_a_cluster_info_outage() {
    let server = MockWorkerServer::start_with_behavior(MockWorkerBehavior {
        request_override: Some(policy_then_outage),
        ..MockWorkerBehavior::default()
    });
    let worker = activity_worker(&server);
    worker.poll_activity_once().await.unwrap();
    assert_eq!(server.request_count(CLUSTER_INFO), 1);
    // Age the cached policy past its 60 s lifetime.
    {
        let mut cache = worker.client.runtime_upload_policy.lock().unwrap();
        for entry in cache.iter_mut().flatten() {
            entry.0 = std::time::Instant::now() - Duration::from_secs(61);
        }
    }
    let started = Instant::now();
    worker.poll_activity_once().await.unwrap();
    worker.poll_activity_once().await.unwrap();
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "reports did not wait for the outage"
    );
    assert_eq!(server.request_count(COMPLETE), 3, "every report was sent");
    // One refresh attempt, then the last-known policy for the next 10 s.
    assert_eq!(server.request_count(CLUSTER_INFO), 2);
}
