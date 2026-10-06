//! Run with DURABLE_WORKFLOW_SERVER_URL and DURABLE_WORKFLOW_TOKEN set for your
//! self-hosted Server. Local effects must be idempotent across worker loss.
use durable_workflow::{
    json, ActivityRetryPolicy, Client, Error, LocalActivityOptions, Result, Worker,
    WorkflowResultOptions,
};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    let client = Client::builder(
        std::env::var("DURABLE_WORKFLOW_SERVER_URL")
            .unwrap_or_else(|_| "http://127.0.0.1:8080".into()),
    )
    .token(std::env::var("DURABLE_WORKFLOW_TOKEN").ok())
    .build()?;
    let queue = std::env::var("TASK_QUEUE").unwrap_or_else(|_| "rust-local-example".into());
    let mut worker = Worker::new(client.clone(), &queue)
        .worker_id(format!("rust-local-{}", durable_workflow::Uuid::new_v4()))
        .local_activities(true)
        .poll_timeout(Duration::from_secs(1));
    worker.register_activity("example.local", |ctx, _| async move {
        ctx.heartbeat(json!({"stage":"transforming"})).await?;
        if ctx.attempt_number == 1 {
            return Err(Error::WorkerLoop("example transient failure".into()));
        }
        Ok(json!({"value":42,"local_attempt":ctx.attempt_number}))
    });
    worker.register_activity("example.remote", |_, args| async move { Ok(args) });
    worker.register_workflow("example.local-workflow", |ctx, _| async move {
        let transformed = ctx
            .local_activity_with_options(
                "example.local",
                LocalActivityOptions::new()
                    .retry_policy(
                        ActivityRetryPolicy::new(3)
                            .backoff_intervals([Duration::from_secs(1), Duration::from_secs(2)]),
                    )
                    .start_to_close_timeout(Duration::from_secs(5))
                    .schedule_to_close_timeout(Duration::from_secs(20)),
                json!([]),
            )
            .await?;
        ctx.sleep(Duration::from_secs(1)).await?;
        ctx.activity("example.remote", transformed).await
    });
    let handle = client
        .start_workflow(
            "example.local-workflow",
            &queue,
            &format!("rust-local-example-{}", durable_workflow::Uuid::new_v4()),
            json!([]),
        )
        .await?;
    let watcher = handle.clone();
    worker
        .run_until(async move {
            loop {
                if watcher.describe().await.is_ok_and(|run| run.is_terminal()) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await?;
    println!(
        "{}",
        handle
            .result(WorkflowResultOptions {
                timeout: Duration::from_secs(30),
                poll_interval: Duration::from_millis(100),
            })
            .await?
    );
    Ok(())
}
