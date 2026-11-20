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
