//! Bounded history retention with recorded-value replay after each signal.
//! Run against a self-hosted Server with DURABLE_WORKFLOW_SERVER_URL and
//! DURABLE_WORKFLOW_TOKEN set. The cache is optional and never durable state.
use durable_workflow::{
    json, Client, Result, StickyCacheOptions, Worker, WorkflowHandle, WorkflowResultOptions,
};
use std::time::Duration;

async fn drive_until_waiting(worker: &Worker, handle: &WorkflowHandle) -> Result<()> {
    loop {
        let description = handle.describe().await?;
        if description.status.as_deref() == Some("waiting") {
            return Ok(());
        }
        if description.is_terminal() {
            return Err(durable_workflow::Error::WorkerLoop(
                "example ended before its signal wait".into(),
            ));
        }
        worker.run_once().await?;
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let client = Client::builder(
        std::env::var("DURABLE_WORKFLOW_SERVER_URL")
            .unwrap_or_else(|_| "http://127.0.0.1:8080".into()),
    )
    .token(std::env::var("DURABLE_WORKFLOW_TOKEN").ok())
    .build()?;
    let queue = format!("rust-sticky-example-{}", durable_workflow::Uuid::new_v4());
    let mut worker = Worker::new(client.clone(), &queue)
        .build_id("rust-sticky-example-v1")
        .sticky_cache(
            StickyCacheOptions::new(16)
                .max_history_bytes(4 * 1024 * 1024)
                .ttl(Duration::from_secs(60)),
        )?
        .poll_timeout(Duration::from_secs(1));
    worker.register_workflow("example.sticky", |ctx, _| async move {
        let value = ctx.side_effect(|| 42)?;
        ctx.wait_signal("first").await?;
        ctx.wait_signal("second").await?;
        Ok(json!({"recorded_value":value}))
    });
    worker.declare_workflow_signals("example.sticky", &["first", "second"])?;
    worker.register().await?;
    let handle = client
        .start_workflow("example.sticky", &queue, &queue, json!([]))
        .await?;
    drive_until_waiting(&worker, &handle).await?;
    handle.signal("first", json!([])).await?;
    // Process the signal before checking the next waiting boundary.
    worker.run_once().await?;
    drive_until_waiting(&worker, &handle).await?;
    handle.signal("second", json!([])).await?;
    while !handle.describe().await?.is_terminal() {
        worker.run_once().await?;
    }
    println!("{}", handle.result(WorkflowResultOptions::default()).await?);
    println!(
        "{}",
        serde_json::to_string(&worker.sticky_cache_metrics()?)?
    );
    worker.run_until(async {}).await?;
    Ok(())
}
