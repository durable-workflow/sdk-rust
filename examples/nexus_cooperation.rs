//! Await a catalog service with durable cooperative cleanup.
//! Configure a `start_workflow` service binding to the target workflow and queue.
//! The installed Server must advertise Nexus workflow-run cooperation.
use durable_workflow::{json, Client, Error, Result, Worker};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    let client = Client::builder(
        std::env::var("DURABLE_WORKFLOW_RUNTIME_URL")
            .unwrap_or_else(|_| "http://127.0.0.1:8080".into()),
    )
    .namespace(
        std::env::var("DURABLE_WORKFLOW_RUNTIME_NAMESPACE").unwrap_or_else(|_| "default".into()),
    )
    .worker_token(std::env::var("DURABLE_WORKFLOW_WORKER_TOKEN").ok())
    .build()?;
    let queue = std::env::var("TASK_QUEUE").unwrap_or_else(|_| "nexus-workers".into());
    let role = std::env::var("NEXUS_ROLE").unwrap_or_else(|_| "caller".into());
    let workflow_type = std::env::var("WORKFLOW_TYPE").unwrap_or_else(|_| format!("nexus.{role}"));
    let mut worker = Worker::new(client, queue)
        .worker_id(std::env::var("WORKER_ID").unwrap_or_else(|_| format!("nexus-{role}")))
        .max_concurrent_workflow_tasks(1)
        .max_concurrent_activity_tasks(1)
        .nexus_cooperative_cancellation(true);
    if role == "target" {
        worker.register_workflow(workflow_type, |ctx, input| async move {
            match ctx.sleep(Duration::from_secs(300)).await {
                Err(Error::CooperativeCancellationRequested(cancelled)) => {
                    println!("{}",json!({"event":"target.cleanup","context":cancelled.request.context.as_ref().map(|ctx|ctx.to_value())}));
                    let _shield = ctx.cancellation_shield()?;
                    ctx.sleep(Duration::from_secs(8)).await?;
                    Err(Error::CooperativeCancellationRequested(cancelled))
                }
                Err(error) => Err(error),
                Ok(_) => Ok(input),
            }
        });
    } else if role == "caller" {
        let endpoint = std::env::var("NEXUS_ENDPOINT").unwrap_or_else(|_| "cooperative".into());
        let service = std::env::var("NEXUS_SERVICE").unwrap_or_else(|_| "cleanup".into());
        worker.register_workflow(workflow_type, move |ctx, input| {
            let endpoint = endpoint.clone();
            let service = service.clone();
            async move {
                let operation = input.get(0).and_then(|value|value.as_str())
                    .ok_or_else(||Error::WorkerLoop("expected one operation argument".into()))?;
                match ctx.call_nexus_service(endpoint,service,operation,json!([operation])).await {
                    Err(Error::CooperativeCancellationRequested(cancelled)) => {
                        println!("{}",json!({"event":"caller.cleanup","context":cancelled.request.context.as_ref().map(|ctx|ctx.to_value())}));
                        let _shield = ctx.cancellation_shield()?;
                        ctx.sleep(Duration::from_secs(1)).await?;
                        Err(Error::CooperativeCancellationRequested(cancelled))
                    }
                    Err(error) => Err(error),
                    Ok(result) => result.result.deserialize(),
                }
            }
        });
    } else {
        return Err(Error::WorkerLoop(
            "NEXUS_ROLE must be caller or target".into(),
        ));
    }
    worker.run().await
}
