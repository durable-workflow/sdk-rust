//! Await a catalog service with durable cooperative cleanup.
//! Configure a `start_workflow` service binding to the target workflow and queue.
//! The installed Server must advertise Nexus workflow-run cooperation.
use durable_workflow::{
    json, Client, Error, NexusOperationMode, NexusServiceOptions, NexusWaitFor, Result, Worker,
};
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
    let qualify_outcomes = std::env::var("NEXUS_QUALIFY_OUTCOMES").as_deref() == Ok("1");
    let qualify_admission = std::env::var("NEXUS_QUALIFY_ADMISSION").as_deref() == Ok("1");
    let mut worker = Worker::new(client, queue)
        .worker_id(std::env::var("WORKER_ID").unwrap_or_else(|_| format!("nexus-{role}")))
        .poll_timeout(Duration::from_secs(1))
        .max_concurrent_workflow_tasks(1)
        .max_concurrent_activity_tasks(1)
        .nexus_cooperative_cancellation(true);
    if std::env::var("NEXUS_QUALIFY_NEIGHBOR").as_deref() == Ok("1") {
        worker.register_workflow("nexus.qualification.neighbor", |_, _| async {
            Ok(json!({"alive":true,"runtime":"rust"}))
        });
    }
    if role == "target" {
        worker.register_workflow(workflow_type, move |ctx, input| async move {
            if qualify_admission {
                ctx.sleep(Duration::from_secs(8)).await?;
                return input.get(1).cloned().ok_or_else(|| Error::WorkerLoop("expected admission fixture value".into()));
            }
            if qualify_outcomes {
                if input.get(0).and_then(|value| value.as_str()) == Some("failure") {
                    return Err(Error::WorkerLoop("nexus-target-failure".into()));
                }
                return input.get(1).cloned().ok_or_else(|| {
                    Error::WorkerLoop("expected the original qualification value".into())
                });
            }
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
                if qualify_admission {
                    let options = admission_options(
                        operation,
                        input.get(2).and_then(|value| value.as_str()),
                        input.get(3).and_then(|value| value.as_str()),
                    )?;
                    let result = ctx.call_nexus_service_with_options(endpoint, service, operation, options, input.clone()).await?;
                    let snapshot = json!({"call_id":result.service_call_id, "status":result.status,
                        "response":result.result.deserialize::<serde_json::Value>()?,
                        "details":{"target_namespace":result.target_namespace,
                            "linked_workflow_instance_id":result.linked_workflow_instance_id,
                            "linked_workflow_run_id":result.linked_workflow_run_id}});
                    println!("{}", json!({"event":"caller.admission", "snapshot":snapshot}));
                    ctx.sleep(Duration::from_secs(3)).await?;
                    return Ok(snapshot);
                }
                let arguments = if qualify_outcomes { input.clone() } else { json!([operation]) };
                match ctx.call_nexus_service(endpoint,service,operation,arguments).await {
                    Err(Error::NexusOperationFailed(failure)) if qualify_outcomes => Ok(json!({
                        "caught_class":"NexusOperationFailed", "call_id":failure.service_call_id,
                        "message":failure.message, "status":failure.status,
                    })),
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
    worker.run_until(shutdown()).await
}

fn admission_options(
    operation: &str,
    key: Option<&str>,
    target_namespace: Option<&str>,
) -> Result<NexusServiceOptions> {
    let (mode, wait_for) = match operation {
        "sync-accepted" => (NexusOperationMode::Sync, NexusWaitFor::Accepted),
        "async-completed" => (NexusOperationMode::Async, NexusWaitFor::Completed),
        "async-accepted" => (NexusOperationMode::Async, NexusWaitFor::Accepted),
        _ => return Err(Error::WorkerLoop("unknown admission profile".into())),
    };
    let key = key
        .filter(|key| !key.is_empty())
        .ok_or_else(|| Error::WorkerLoop("expected original admission idempotency key".into()))?;
    Ok(NexusServiceOptions {
        mode,
        wait_for,
        idempotency_key: Some(key.to_owned()),
        target_namespace: target_namespace.map(str::to_owned),
        ..NexusServiceOptions::default()
    })
}

async fn shutdown() {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {},
            _ = terminate.recv() => {},
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
