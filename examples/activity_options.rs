use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use durable_workflow::{
    json, ActivityOptions, ActivityRetryPolicy, Client, Error, Result, Worker,
    WorkflowResultOptions,
};

#[tokio::main]
async fn main() -> Result<()> {
    let server_url = std::env::var("DURABLE_WORKFLOW_SERVER_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:8080".to_string());
    let token = std::env::var("DURABLE_WORKFLOW_TOKEN").ok();
    let task_queue = std::env::var("TASK_QUEUE").unwrap_or_else(|_| "rust-workers".to_string());
    let client = Client::builder(server_url)
        .token(token)
        .namespace("default")
        .build()?;
    let attempts = Arc::new(AtomicUsize::new(0));
    let mut worker = Worker::new(client.clone(), task_queue.clone())
        .worker_id(format!("rust-activity-options-{}", unique_suffix()))
        .poll_timeout(Duration::from_secs(5));

    worker.register_activity("rust.flaky", {
        let attempts = attempts.clone();
        move |ctx, _args| {
            let attempts = attempts.clone();
            async move {
                let attempt = attempts.fetch_add(1, Ordering::SeqCst) + 1;
                ctx.heartbeat(json!({"attempt": attempt})).await?;
                if attempt < 3 {
                    return Err(Error::WorkerLoop(format!("planned failure {attempt}")));
                }
                Ok(json!({"attempt": attempt, "status": "completed"}))
            }
        }
    });
    worker.register_activity("rust.terminal", |_ctx, _args| async move {
        Err(Error::WorkerLoop("planned terminal failure".to_string()))
    });

    let activity_queue = task_queue.clone();
    worker.register_workflow("rust.activity-options", move |ctx, _input| {
        let activity_queue = activity_queue.clone();
        async move {
            let retry = ActivityRetryPolicy::new(3).exponential_backoff(
                Duration::from_secs(1),
                2,
                Some(Duration::from_secs(10)),
            );
            let flaky = ctx
                .activity_with_options(
                    "rust.flaky",
                    ActivityOptions::new()
                        .task_queue(activity_queue.clone())
                        .retry_policy(retry)
                        .start_to_close_timeout(Duration::from_secs(30))
                        .schedule_to_close_timeout(Duration::from_secs(60))
                        .heartbeat_timeout(Duration::from_secs(5)),
                    json!([]),
                )
                .await?;

            let terminal = ctx
                .activity_with_options(
                    "rust.terminal",
                    ActivityOptions::new()
                        .task_queue(activity_queue)
                        .retry_policy(ActivityRetryPolicy::new(1))
                        .start_to_close_timeout(Duration::from_secs(10)),
                    json!([]),
                )
                .await;
            let failure = match terminal {
                Err(Error::ActivityFailed(failure)) => json!({
                    "kind": format!("{:?}", failure.kind),
                    "reason": failure.reason,
                    "category": failure.failure_category,
                    "activity_execution_id": failure.activity_execution_id,
                    "activity_attempt_id": failure.activity_attempt_id,
                    "timeout_kind": failure.timeout_kind,
                }),
                Err(error) => return Err(error),
                Ok(value) => json!({"unexpected_success": value}),
            };
            Ok(json!({"retry_result": flaky, "terminal_failure": failure}))
        }
    });

    worker.register().await?;
    let workflow_id = format!("rust-activity-options-{}", unique_suffix());
    let handle = client
        .start_workflow(
            "rust.activity-options",
            &task_queue,
            &workflow_id,
            json!([]),
        )
        .await?;
    let watcher = handle.clone();
    worker
        .run_until(async move {
            loop {
                if watcher
                    .describe()
                    .await
                    .is_ok_and(|description| description.is_terminal())
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        })
        .await?;

    let result = handle
        .result(WorkflowResultOptions {
            poll_interval: Duration::from_millis(500),
            timeout: Duration::from_secs(90),
        })
        .await?;
    println!("{result}");
    Ok(())
}

fn unique_suffix() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}
