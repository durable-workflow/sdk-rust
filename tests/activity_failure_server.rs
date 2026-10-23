//! Retry-policy acceptance against an isolated published Server.
//! Set DURABLE_WORKFLOW_LOCAL_ISOLATED=1 and the Server URL/auth token variables
//! used by local_activity_server. Normal cargo tests leave these cases ignored.

use durable_workflow::{
    json, ActivityError, ActivityOptions, ActivityRetryPolicy, Client, Error, LocalActivityOptions,
    Worker, WorkflowResultOptions,
};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

async fn check_typed_failures(local: bool, cooperative: bool) {
    assert_eq!(
        std::env::var("DURABLE_WORKFLOW_LOCAL_ISOLATED").as_deref(),
        Ok("1"),
        "this test requires an isolated disposable runtime"
    );
    let client = Client::builder(std::env::var("DURABLE_WORKFLOW_SERVER_URL").unwrap())
        .token(Some(std::env::var("DURABLE_WORKFLOW_AUTH_TOKEN").unwrap()))
        .build()
        .unwrap();
    let queue = format!("rust-typed-failure-{}", durable_workflow::Uuid::new_v4());
    let calls = Arc::new([
        AtomicUsize::new(0),
        AtomicUsize::new(0),
        AtomicUsize::new(0),
    ]);
    let mut worker = Worker::new(client.clone(), &queue)
        .worker_id(format!("{queue}-worker"))
        .poll_timeout(Duration::from_millis(10))
        .local_activities(local)
        .cooperative_cancellation(cooperative);
    worker.register_activity("typed.failure", {
        let calls = calls.clone();
        move |_, arguments| {
            let calls = calls.clone();
            async move {
                let case = arguments[0].as_u64().unwrap() as usize;
                calls[case].fetch_add(1, Ordering::SeqCst);
                Err(ActivityError::new("PaymentDeclined", "card declined")
                    .non_retryable(case == 2)
                    .into())
            }
        }
    });
    let activity_queue = queue.clone();
    worker.register_workflow("tests.typed-activity-failures", move |ctx, _| {
        let activity_queue = activity_queue.clone();
        async move {
            let mut failures = Vec::new();
            for case in 0..3 {
                let policy = ActivityRetryPolicy::new(3)
                    .backoff_intervals([Duration::ZERO, Duration::ZERO])
                    .non_retryable_error_types([if case == 0 {
                        "PaymentDeclined"
                    } else {
                        "AnotherFailure"
                    }]);
                let result = if local {
                    ctx.local_activity_with_options(
                        "typed.failure",
                        LocalActivityOptions::new().retry_policy(policy),
                        json!([case]),
                    )
                    .await
                } else {
                    ctx.activity_with_options(
                        "typed.failure",
                        ActivityOptions::new()
                            .task_queue(activity_queue.clone())
                            .retry_policy(policy),
                        json!([case]),
                    )
                    .await
                };
                match result {
                    Err(Error::ActivityFailed(failure)) => failures.push(json!({
                        "type": failure.exception_type,
                        "message": failure.message,
                        "non_retryable": failure.non_retryable,
                        "attempt": failure.attempt_number,
                    })),
                    other => panic!("expected terminal application failure: {other:?}"),
                }
            }
            Ok(json!(failures))
        }
    });
    worker.register().await.unwrap();
    let handle = client
        .start_workflow("tests.typed-activity-failures", &queue, &queue, json!([]))
        .await
        .unwrap();
    let watcher = handle.clone();
    tokio::time::timeout(
        Duration::from_secs(60),
        worker.run_until(async move {
            loop {
                if watcher.describe().await.unwrap().is_terminal() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }),
    )
    .await
    .expect("workflow must finish within its retry budget")
    .unwrap();
    let result = handle
        .result(WorkflowResultOptions::default())
        .await
        .unwrap();
    assert_eq!(
        result,
        json!([
            {"type":"PaymentDeclined", "message":"card declined", "non_retryable":true, "attempt":1},
            {"type":"PaymentDeclined", "message":"card declined", "non_retryable":false, "attempt":3},
            {"type":"PaymentDeclined", "message":"card declined", "non_retryable":true, "attempt":1},
        ]),
        "local={local}, cooperative={cooperative}"
    );
    assert_eq!(
        calls
            .iter()
            .map(|n| n.load(Ordering::SeqCst))
            .collect::<Vec<_>>(),
        [1, 3, 1]
    );
    println!("typed failure retry policy passed: local={local}, cooperative={cooperative}");
}

#[tokio::test]
#[ignore = "requires isolated published Server"]
async fn remote_activity_types_and_non_retryable_markers_control_retries() {
    check_typed_failures(false, false).await;
}

#[tokio::test]
#[ignore = "requires isolated published Server"]
async fn cooperative_activity_types_and_non_retryable_markers_control_retries() {
    check_typed_failures(false, true).await;
}

#[tokio::test]
#[ignore = "requires isolated published Server"]
async fn local_activity_types_and_non_retryable_markers_control_retries() {
    check_typed_failures(true, false).await;
}
