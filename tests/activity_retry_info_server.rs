//! Retry snapshot acceptance against an isolated published Server.
use durable_workflow::{
    json, ActivityAttemptLimit, ActivityError, ActivityOptions, ActivityRetryInfo,
    ActivityRetryPolicy, Client, Error, Value, Worker, WorkflowResultOptions,
};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

async fn check_received_snapshot(cooperative: bool) {
    assert_eq!(
        std::env::var("DURABLE_WORKFLOW_LOCAL_ISOLATED").as_deref(),
        Ok("1")
    );
    let client = Client::builder(std::env::var("DURABLE_WORKFLOW_SERVER_URL").unwrap())
        .token(Some(std::env::var("DURABLE_WORKFLOW_AUTH_TOKEN").unwrap()))
        .build()
        .unwrap();
    let queue = format!("rust-retry-info-{}", durable_workflow::Uuid::new_v4());
    let infos = Arc::new(Mutex::new(Vec::<(u64, ActivityRetryInfo)>::new()));
    let saved = infos.clone();
    let mut worker = Worker::new(client.clone(), &queue)
        .worker_id(format!("{queue}-worker"))
        .poll_timeout(Duration::from_millis(10))
        .cooperative_cancellation(cooperative);
    worker.register_activity("inspect",move |ctx,input| {
        let saved=saved.clone();
        async move {
            let case=input[0].as_u64().unwrap();
            let info=ctx.retry_info().clone();
            saved.lock().unwrap().push((case,info.clone()));
            assert_eq!(info.attempt_number(),ctx.attempt_number);
            assert_eq!(info.attempt_limit(),ActivityAttemptLimit::Bounded(if case==1 {1} else {3}));
            if case==2 { return Err(ActivityError::new("Permanent","stop before budget").non_retryable(true).into()); }
            if info.is_last_configured_attempt()!=Some(true) {
                return Err(ActivityError::new("Transient","use next configured attempt").into());
            }
            Ok(json!({"case":case,"attempt":info.attempt_number(),"policy":info.policy(),"last":info.is_last_configured_attempt()}))
        }
    });
    worker.register_workflow("tests.retry-info", |ctx, _| async move {
        let mut results = Vec::new();
        for case in 0..3 {
            let result = if case == 1 {
                ctx.activity("inspect", json!([case])).await
            } else {
                ctx.activity_with_options(
                    "inspect",
                    ActivityOptions::new().retry_policy(
                        ActivityRetryPolicy::new(3)
                            .backoff_intervals([Duration::ZERO, Duration::ZERO]),
                    ),
                    json!([case]),
                )
                .await
            };
            match result {
                Ok(value) => results.push(value),
                Err(Error::ActivityFailed(failure)) if case == 2 => {
                    assert!(failure.non_retryable);
                    assert_eq!(failure.attempt_number, 1);
                    results.push(json!({"case":case,"stopped_early":true}));
                }
                other => panic!("unexpected result: {other:?}"),
            }
        }
        Ok(json!(results))
    });
    worker.register().await.unwrap();
    let handle = client
        .start_workflow("tests.retry-info", &queue, &queue, json!([]))
        .await
        .unwrap();
    let watch = handle.clone();
    tokio::time::timeout(
        Duration::from_secs(60),
        worker.run_until(async move {
            while !watch.describe().await.unwrap().is_terminal() {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }),
    )
    .await
    .expect("configured retries must complete")
    .unwrap();
    let result: Value = handle
        .result(WorkflowResultOptions::default())
        .await
        .unwrap();
    assert_eq!(result[0]["attempt"], 3);
    assert_eq!(result[0]["last"], true);
    assert_eq!(result[0]["policy"]["max_attempts"], 3);
    assert_eq!(result[1]["attempt"], 1);
    assert_eq!(result[1]["last"], true);
    assert_eq!(result[1]["policy"]["max_attempts"], 1);
    assert_eq!(result[2], json!({"case":2,"stopped_early":true}));
    let infos = infos.lock().unwrap();
    assert_eq!(
        infos
            .iter()
            .map(|(case, info)| (
                *case,
                info.attempt_number(),
                info.is_last_configured_attempt()
            ))
            .collect::<Vec<_>>(),
        vec![
            (0, 1, Some(false)),
            (0, 2, Some(false)),
            (0, 3, Some(true)),
            (1, 1, Some(true)),
            (2, 1, Some(false))
        ]
    );
    let policy = infos[0].1.policy().unwrap();
    assert!(policy["snapshot_version"].is_number());
    assert_eq!(infos[1].1.policy(), Some(policy));
    assert_eq!(infos[2].1.policy(), Some(policy));
    println!("published Server retry snapshots passed: cooperative={cooperative}, attempts=[3,1,1], default=bounded(1), nonretryable stops before budget");
}
#[tokio::test]
#[ignore = "requires isolated published Server"]
async fn normal_worker_receives_bounded_default_and_early_stop_snapshots() {
    check_received_snapshot(false).await;
}
#[tokio::test]
#[ignore = "requires isolated published Server"]
async fn cooperative_worker_receives_bounded_default_and_early_stop_snapshots() {
    check_received_snapshot(true).await;
}
