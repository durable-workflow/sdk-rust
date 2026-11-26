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
        // cloud#752: a database failover answered with a plain 500.
        http(500, ""),
        http(500, r#"{"message":"Server Error"}"#),
        http(500, "<html>internal server error</html>"),
        http(
            500,
            r#"{"reason":"server_error","message":"Database runtime credential authentication is enabled but unavailable."}"#,
        ),
        http(503, r#"{"reason":"server_error"}"#),
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
        http(500, r#"{"reason":"server_error","retryable":false}"#),
        http(503, r#"{"reason":"draining"}"#),
        http(501, ""),
        http(400, r#"{"retryable":true}"#),
        http(404, ""),
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

const FAIL: &str = "/api/worker/activity-tasks/retry-task/fail";

fn too_large_refusal() -> (&'static str, String) {
    (
        "413 Payload Too Large",
        json!({"reason":"payload_too_large","message":"request exceeds the limit"}).to_string(),
    )
}

#[test]
fn only_a_typed_size_refusal_allows_reencoding_a_report() {
    let (_, typed_body) = too_large_refusal();
    assert!(crate::report_payload_too_large(
        reqwest::StatusCode::PAYLOAD_TOO_LARGE,
        &typed_body,
    ));
    assert!(!crate::report_payload_too_large(
        reqwest::StatusCode::PAYLOAD_TOO_LARGE,
        "Proxy rejected the request",
    ));
    assert!(!crate::report_payload_too_large(
        reqwest::StatusCode::INTERNAL_SERVER_ERROR,
        &typed_body,
    ));
}

fn still_too_large(path: &str, body: &str, number: usize) -> Option<(&'static str, String)> {
    match (path, number) {
        (ACTIVITY_POLL, 1 | 2) => Some(("200 OK", activity_delivery(Duration::from_secs(30)))),
        (ACTIVITY_POLL, _) => Some(("200 OK", json!({"task": null}).to_string())),
        (CLUSTER_INFO, 1) => Some(("200 OK", payload_policy(2 * 1024 * 1024))),
        (CLUSTER_INFO, _) => Some(("200 OK", payload_policy(100 * 1024))),
        (UPLOAD, _) => Some(("201 Created", uploaded(body))),
        // Refused inline, and refused again after it was re-encoded.
        (COMPLETE, 2 | 3) => Some(too_large_refusal()),
        (COMPLETE, _) | (FAIL, _) => Some(("200 OK", json!({"recorded":true}).to_string())),
        _ => None,
    }
}

fn no_storage_after_rediscovery(
    path: &str,
    body: &str,
    number: usize,
) -> Option<(&'static str, String)> {
    match (path, number) {
        (CLUSTER_INFO, 2..) => {
            let mut policy: Value = serde_json::from_str(&payload_policy(100 * 1024)).unwrap();
            policy["namespace"]["external_payload_storage"]["status"] = json!("unavailable");
            Some(("200 OK", policy.to_string()))
        }
        (COMPLETE, 2) => Some(too_large_refusal()),
        _ => still_too_large(path, body, number),
    }
}

#[tokio::test]
async fn a_result_still_too_large_after_rediscovery_fails_the_attempt_not_the_worker() {
    for (case, handler) in [
        ("refused again", still_too_large as RequestOverride),
        ("no storage", no_storage_after_rediscovery),
    ] {
        let server = MockWorkerServer::start_with_behavior(MockWorkerBehavior {
            request_override: Some(handler),
            ..MockWorkerBehavior::default()
        });
        let worker = sized_worker(&server);
        for _ in 0..2 {
            let outcome = worker.poll_activity_once().await;
            assert!(
                matches!(outcome, Ok(ManagedPollOutcome::Handled)),
                "{case}: the worker keeps running: {outcome:?}"
            );
        }
        let failures = server.request_bodies(FAIL);
        assert_eq!(failures.len(), 1, "{case}");
        let failure = &failures[0]["failure"];
        assert_eq!(failure["type"], "ActivityResultTooLarge", "{case}");
        assert_eq!(failure["class"], "ActivityResultTooLarge", "{case}");
        assert_eq!(failure["non_retryable"], true, "{case}");
        assert!(
            failure["message"].as_str().unwrap().contains("too large"),
            "{case}"
        );
    }
}

// cloud#752: during a database failover the Server answered HTTP 500, then
// 503 with Retry-After. A running worker must ride that out on every loop.

const HEARTBEAT: &str = "/api/worker/heartbeat";
const QUERY_POLL: &str = "/api/worker/query-tasks/poll";
const WORKFLOW_COMPLETE: &str = "/api/worker/workflow-tasks/recover-workflow/complete";
/// The mock writes the status line verbatim, so a header can ride along.
const UNAVAILABLE_RETRY_AFTER_3: &str = "503 Service Unavailable\r\nretry-after: 3";
const UNAVAILABLE_RETRY_AFTER_2: &str = "503 Service Unavailable\r\nretry-after: 2";

/// A framework's answer to an unhandled database exception.
fn plain_500() -> (&'static str, String) {
    (
        "500 Internal Server Error",
        json!({"message":"Server Error"}).to_string(),
    )
}

fn workflow_delivery() -> String {
    include_str!("../../tests/fixtures/worker-completion-recovery.json").to_string()
}

fn recorded() -> String {
    json!({"outcome":"completed","recorded":true}).to_string()
}

/// Record every retry wait on this test's thread until the guard drops.
struct RecordedWaits;

impl RecordedWaits {
    fn start() -> Self {
        crate::server_retry_after::RECORDED.with(|waits| *waits.borrow_mut() = Some(Vec::new()));
        Self
    }

    fn waits(&self) -> Vec<Duration> {
        crate::server_retry_after::RECORDED.with(|waits| waits.borrow().clone().unwrap_or_default())
    }
}

impl Drop for RecordedWaits {
    fn drop(&mut self) {
        crate::server_retry_after::RECORDED.with(|waits| *waits.borrow_mut() = None);
    }
}

fn failover_worker(server: &MockWorkerServer, query: bool, activity: bool) -> Worker {
    let client = Client::builder(server.base_url())
        .timeout(Duration::from_secs(2))
        .build()
        .expect("client");
    let mut worker = Worker::new(client, "rust-workers")
        .worker_id("recover-worker")
        // One lane per family: a poll after a settlement proves that lane
        // got past the settlement (shutdown during a wait returns its error).
        .max_concurrent_workflow_tasks(1)
        .max_concurrent_activity_tasks(1)
        .poll_timeout(Duration::from_millis(10))
        .retry_policy(WorkerRetryPolicy {
            max_retries: 2,
            initial_backoff: Duration::from_millis(2),
            max_backoff: Duration::from_millis(5),
        });
    worker.register_workflow("recover.workflow", |_ctx, _input| async move {
        Ok(json!("done"))
    });
    if query {
        worker.register_query("recover.workflow", "state", |_ctx, _args| async move {
            Ok(Value::Null)
        });
    }
    if activity {
        worker.register_activity("RetryActivity", |_ctx, _args| async { Ok(json!("done")) });
    }
    worker
}

/// Four plain 500s (past the two-retry budget), then a 503 with
/// `Retry-After: 3`, on every poll lane and the worker heartbeat. The
/// workflow lane then delivers a task.
fn failover_on_every_loop(
    path: &str,
    _body: &str,
    number: usize,
) -> Option<(&'static str, String)> {
    let looping = matches!(path, WORKFLOW_POLL | ACTIVITY_POLL | QUERY_POLL | HEARTBEAT);
    match (path, number) {
        (_, 1..=4) if looping => Some(plain_500()),
        (_, 5) if looping => Some((UNAVAILABLE_RETRY_AFTER_3, String::new())),
        (WORKFLOW_POLL, 6) => Some(("200 OK", workflow_delivery())),
        (ACTIVITY_POLL | QUERY_POLL | WORKFLOW_POLL, _) => {
            Some(("200 OK", json!({"task": null}).to_string()))
        }
        (WORKFLOW_COMPLETE, _) => Some(("200 OK", recorded())),
        _ => None,
    }
}

#[tokio::test]
async fn plain_500s_then_retry_after_never_stop_polling_or_heartbeats() {
    let waits = RecordedWaits::start();
    let server = MockWorkerServer::start_with_behavior(MockWorkerBehavior {
        request_override: Some(failover_on_every_loop),
        ..MockWorkerBehavior::default()
    });
    let recovered = async {
        while server.request_count(WORKFLOW_COMPLETE) < 1
            || [ACTIVITY_POLL, QUERY_POLL, HEARTBEAT]
                .iter()
                .any(|path| server.request_count(path) < 6)
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    };
    tokio::time::timeout(
        Duration::from_secs(10),
        failover_worker(&server, true, true).run_until(recovered),
    )
    .await
    .expect("every loop resumes after the failover")
    .expect("plain 500s and a 503 must not stop a running worker");

    assert_eq!(
        server.request_count(WORKFLOW_COMPLETE),
        1,
        "the task completed"
    );
    let waits = waits.waits();
    assert!(waits.len() >= 4 * 5, "every loop retried: {waits:?}");
    // Bounded: the outage schedule never waits more than 4 s, and the
    // Server's Retry-After (3 s) is honored, not exceeded.
    assert!(
        waits.iter().all(|wait| *wait <= Duration::from_secs(4)),
        "{waits:?}"
    );
    assert!(
        waits
            .iter()
            .any(|wait| *wait > Duration::from_millis(2_900) && *wait <= Duration::from_secs(3)),
        "Retry-After: 3 is honored: {waits:?}"
    );
}

/// `Retry-After` as an HTTP-date about two seconds from now.
fn unavailable_until_two_seconds_from_now() -> &'static str {
    let at = SystemTime::now().duration_since(UNIX_EPOCH).unwrap() + Duration::from_secs(2);
    let at = DateTime::from_timestamp(at.as_secs() as i64, 0).unwrap();
    let status = format!(
        "503 Service Unavailable\r\nretry-after: {}",
        at.format("%a, %d %b %Y %H:%M:%S GMT")
    );
    Box::leak(status.into_boxed_str())
}

fn completion_failover(path: &str, _body: &str, number: usize) -> Option<(&'static str, String)> {
    match (path, number) {
        (WORKFLOW_POLL, 1) => Some(("200 OK", workflow_delivery())),
        (WORKFLOW_POLL, _) => Some(("200 OK", json!({"task": null}).to_string())),
        // The Server's answer when its database is unavailable to authenticate.
        (WORKFLOW_COMPLETE, 1) => Some((
            "500 Internal Server Error",
            json!({"reason":"server_error",
                "message":"Database runtime credential authentication is enabled but unavailable."})
            .to_string(),
        )),
        (WORKFLOW_COMPLETE, 2) => Some((unavailable_until_two_seconds_from_now(), String::new())),
        (WORKFLOW_COMPLETE, _) => Some(("200 OK", recorded())),
        (ACTIVITY_POLL, 1) => Some(("200 OK", activity_delivery(Duration::from_secs(60)))),
        (ACTIVITY_POLL, _) => Some(("200 OK", json!({"task": null}).to_string())),
        (COMPLETE, 1) => Some(plain_500()),
        (COMPLETE, 2) => Some((UNAVAILABLE_RETRY_AFTER_2, String::new())),
        (COMPLETE, _) => Some(("200 OK", "{}".into())),
        _ => None,
    }
}

#[tokio::test]
async fn a_completion_refused_with_500_then_retry_after_is_resent_until_recorded() {
    let waits = RecordedWaits::start();
    let server = MockWorkerServer::start_with_behavior(MockWorkerBehavior {
        request_override: Some(completion_failover),
        ..MockWorkerBehavior::default()
    });
    let worker = failover_worker(&server, false, true);
    let settled = async {
        while server.request_count(WORKFLOW_COMPLETE) < 3 || server.request_count(COMPLETE) < 3 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        // The worker keeps polling after both settlements.
        let polls = server.request_count(WORKFLOW_POLL);
        while server.request_count(WORKFLOW_POLL) < polls + 2 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(10), worker.run_until(settled))
        .await
        .expect("both completions are delivered")
        .expect("a 500 or 503 on a completion must not stop a running worker");

    for path in [WORKFLOW_COMPLETE, COMPLETE] {
        let bodies = server.request_bodies(path);
        assert_eq!(
            bodies.len(),
            3,
            "{path}: resent until recorded, then stopped"
        );
        assert!(
            bodies.iter().all(|body| *body == bodies[0]),
            "{path}: every resend is the identical report (same IDs and attempt)"
        );
    }
    let waits = waits.waits();
    assert!(
        waits.iter().all(|wait| *wait <= Duration::from_secs(4)),
        "{waits:?}"
    );
    // The HTTP-date names a whole second, so it is honored as 1-2 s.
    assert!(
        waits
            .iter()
            .any(|wait| *wait > Duration::from_millis(900) && *wait <= Duration::from_secs(2)),
        "an HTTP-date Retry-After is honored: {waits:?}"
    );
    assert!(
        waits
            .iter()
            .any(|wait| *wait > Duration::from_millis(1_900) && *wait <= Duration::from_secs(2)),
        "Retry-After: 2 is honored: {waits:?}"
    );
}

fn lease_lost_during_resends(
    path: &str,
    _body: &str,
    number: usize,
) -> Option<(&'static str, String)> {
    match (path, number) {
        (WORKFLOW_POLL, 1) => Some(("200 OK", workflow_delivery())),
        (WORKFLOW_POLL, _) => Some(("200 OK", json!({"task": null}).to_string())),
        (WORKFLOW_COMPLETE, 1) => Some(plain_500()),
        (WORKFLOW_COMPLETE, _) => Some((
            "409 Conflict",
            json!({"reason":"lease_expired","task_id":"recover-workflow",
                "workflow_task_attempt":3,"task_status":"leased"})
            .to_string(),
        )),
        _ => None,
    }
}

#[tokio::test]
async fn a_refusal_after_a_resend_ends_no_worse_than_not_resending() {
    let _waits = RecordedWaits::start();
    let server = MockWorkerServer::start_with_behavior(MockWorkerBehavior {
        request_override: Some(lease_lost_during_resends),
        ..MockWorkerBehavior::default()
    });
    let worker = failover_worker(&server, false, false);
    let continued = async {
        while server.request_count(WORKFLOW_COMPLETE) < 2 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let polls = server.request_count(WORKFLOW_POLL);
        while server.request_count(WORKFLOW_POLL) < polls + 2 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(10), worker.run_until(continued))
        .await
        .expect("polling continues")
        .expect("a lease lost while resending leaves the task to the Server");
    assert_eq!(
        server.request_count(WORKFLOW_COMPLETE),
        2,
        "a definitive refusal ends the resends"
    );
}
