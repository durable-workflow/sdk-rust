//! Audit-log every workflow task and activity attempt with one interceptor.
//!
//! Run with DURABLE_WORKFLOW_SERVER_URL and DURABLE_WORKFLOW_TOKEN set for your
//! self-hosted Server.
use durable_workflow::interceptors::{
    ActivityInterceptorContext, ActivityNext, InterceptorFuture, WorkflowTaskInterceptorContext,
    WorkflowTaskNext, WorkflowTaskOutcome,
};
use durable_workflow::{
    json, AvroValue, Client, Result, Worker, WorkerInterceptor, WorkflowResultOptions,
};
use std::time::Duration;

struct AuditLog;

impl WorkerInterceptor for AuditLog {
    fn execute_workflow_task<'a>(
        &'a self,
        context: &'a WorkflowTaskInterceptorContext,
        next: WorkflowTaskNext<'a>,
    ) -> InterceptorFuture<'a, Result<WorkflowTaskOutcome>> {
        Box::pin(async move {
            let outcome = next.run().await;
            // The commands are new for this task even after replaying history.
            // A retry of a failed task can repeat an outcome; attempt > 1 marks it.
            match &outcome {
                Ok(outcome) => println!(
                    "audit: workflow {} task {} attempt {} ({:?}) produced {} commands",
                    context.workflow_id().unwrap_or("?"),
                    context.task_id(),
                    context.attempt(),
                    context.replay_state(),
                    outcome.commands().len(),
                ),
                Err(error) => {
                    println!("audit: workflow task {} failed: {error}", context.task_id())
                }
            }
            outcome
        })
    }

    fn execute_activity(
        &self,
        context: ActivityInterceptorContext,
        next: ActivityNext,
    ) -> InterceptorFuture<'_, Result<AvroValue>> {
        Box::pin(async move {
            let activity_type = context.activity_type().to_owned();
            let attempt = context.attempt();
            let result = next.run(context).await;
            println!(
                "audit: activity {activity_type} attempt {attempt} ok={}",
                result.is_ok()
            );
            result
        })
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
    let queue = std::env::var("TASK_QUEUE").unwrap_or_else(|_| "rust-audit-example".into());
    let mut worker = Worker::new(client.clone(), &queue)
        .poll_timeout(Duration::from_secs(1))
        .interceptor(AuditLog);
    worker.register_activity("example.greet", |_, args| async move {
        Ok(json!(format!(
            "hello {}",
            args[0].as_str().unwrap_or("world")
        )))
    });
    worker.register_workflow("example.audited", |ctx, _| async move {
        ctx.activity("example.greet", json!(["audit"])).await
    });
    let handle = client
        .start_workflow(
            "example.audited",
            &queue,
            &format!("rust-audit-example-{}", durable_workflow::Uuid::new_v4()),
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
