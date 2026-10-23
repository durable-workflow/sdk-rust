//! These public controls also run unchanged against the prior registry crate.
mod failure_reporting_support;
use durable_workflow::{json, ActivityError, Client, Worker};
use failure_reporting_support::{Server, MIB};
use std::time::Duration;

async fn worker_failure(cooperative: bool) {
    let original = "\u{0000}界🦀".repeat(400_000);
    assert!(original.len() > 3 * MIB);
    let server=Server::start(2*MIB,vec![json!({"task_id":"exact-task","activity_attempt_id":"exact-attempt",
        "activity_type":"large","payload_codec":"avro","attempt_number":1,"lease_owner":"failure-worker"})]).await;
    let client = Client::new(&server.url).unwrap();
    let mut worker = Worker::new(client, "failures")
        .worker_id("failure-worker")
        .poll_timeout(Duration::from_millis(1))
        .cooperative_cancellation(cooperative);
    let saved = original.clone();
    worker.register_activity("large", move |_, _| {
        let original = saved.clone();
        async move {
            Err(ActivityError::new("OriginalType", original)
                .non_retryable(true)
                .into())
        }
    });
    worker.register().await.unwrap();
    worker.run_once().await.unwrap();
    let reports = server.reports();
    assert_eq!(reports.len(), 1);
    let (path, bytes, body) = &reports[0];
    assert!(
        bytes.len() <= MIB,
        "actual encoded report {} bytes",
        bytes.len()
    );
    assert!(path.ends_with("/exact-task/fail"));
    assert_eq!(body["activity_attempt_id"], "exact-attempt");
    assert_eq!(body["lease_owner"], "failure-worker");
    assert_eq!(body["failure"]["type"], "OriginalType");
    assert_eq!(body["failure"]["non_retryable"], true);
    assert!(body["failure"]["message"]
        .as_str()
        .unwrap()
        .contains("[truncated"));
    assert_eq!(
        body["failure"]["diagnostics"]["sdk_failure_truncation"]["original_message_bytes"],
        original.len()
    );
}
#[tokio::test]
async fn normal_worker_bounds_actual_encoded_failure() {
    worker_failure(false).await;
}
#[tokio::test]
async fn cooperative_worker_preserves_type_and_retry_semantics() {
    worker_failure(true).await;
}

#[tokio::test]
async fn workflow_task_and_terminal_workflow_report_large_failure() {
    let server = Server::start(2 * MIB, vec![]).await;
    let client = Client::new(&server.url).unwrap();
    let original = "\u{0000}".repeat(3 * MIB);
    assert!(
        serde_json::to_vec(&json!({"failure":{"message":original}}))
            .unwrap()
            .len()
            > 2 * MIB,
        "retained unbounded control exceeds published transport budget"
    );
    client
        .fail_workflow_task("exact-task", "exact-owner", 5, &original)
        .await
        .unwrap();
    client.complete_workflow_task("exact-task","exact-owner",5,vec![json!({"type":"fail_workflow","message":original,
        "exception_type":"Original","non_retryable":true,"exception":{"type":"Original","message":original,"non_retryable":true}})]).await.unwrap();
    let reports = server.reports();
    assert_eq!(reports.len(), 2);
    for (_, bytes, body) in &reports {
        assert!(bytes.len() <= MIB, "actual body {} bytes", bytes.len());
        assert_eq!(body["lease_owner"], "exact-owner");
        assert_eq!(body["workflow_task_attempt"], 5);
    }
    assert_eq!(reports[0].2["failure"]["type"], "RustWorkflowTaskFailure");
    let command = &reports[1].2["commands"][0];
    assert_eq!(command["exception"]["message"], "");
    assert!(command["message"].as_str().unwrap().contains("[truncated"));
    assert_eq!(command["exception_type"], "Original");
    assert_eq!(command["non_retryable"], true);
}

#[tokio::test]
async fn fresh_workflow_tests_record_bounded_failures_and_replay_without_callbacks() {
    use durable_workflow::{
        testing::WorkflowEnvironment, ActivityOptions, ActivityRetryPolicy, Error,
        LocalActivityOptions,
    };
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    for local in [false, true] {
        let mut env = WorkflowEnvironment::new().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let saved = calls.clone();
        env.register_activity("large", move |_, _| {
            saved.fetch_add(1, Ordering::SeqCst);
            async { Err(ActivityError::new("Original", "\u{0000}界".repeat(800_000)).into()) }
        });
        env.register_workflow("large-failure", move |ctx, _| async move {
            let policy = ActivityRetryPolicy::new(2).backoff_intervals([Duration::ZERO]);
            let result = if local {
                ctx.local_activity_with_options(
                    "large",
                    LocalActivityOptions::new().retry_policy(policy),
                    json!([]),
                )
                .await
            } else {
                ctx.activity_with_options(
                    "large",
                    ActivityOptions::new().retry_policy(policy),
                    json!([]),
                )
                .await
            };
            let Err(Error::ActivityFailed(failure)) = result else {
                panic!("expected original activity failure");
            };
            assert!(failure.message.len() <= 62 * 1024);
            assert!(failure.message.contains("[truncated"));
            assert_eq!(failure.exception_type.as_deref(), Some("Original"));
            assert_eq!(failure.attempt_number, Some(2));
            assert!(!failure.non_retryable);
            Ok(json!(failure.message))
        });
        env.run_workflow("large-failure", ()).await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(env.replay(env.last_history().unwrap()).unwrap().completed);
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "committed failures replay without activities"
        );
    }
}
