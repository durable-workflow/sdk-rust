use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use durable_workflow::{json, Client, Error, Result, Uuid, Worker, WorkflowResultOptions};

#[tokio::main]
async fn main() -> Result<()> {
    let server_url = std::env::var("DURABLE_WORKFLOW_RUNTIME_URL")
        .or_else(|_| std::env::var("DURABLE_WORKFLOW_SERVER_URL"))
        .unwrap_or_else(|_| "http://127.0.0.1:8080".to_string());
    let namespace = std::env::var("DURABLE_WORKFLOW_RUNTIME_NAMESPACE")
        .unwrap_or_else(|_| "default".to_string());
    let token = std::env::var("DURABLE_WORKFLOW_TOKEN").ok();
    let client_token = std::env::var("DURABLE_WORKFLOW_CLIENT_TOKEN").ok();
    let worker_token = std::env::var("DURABLE_WORKFLOW_WORKER_TOKEN").ok();
    let task_queue = std::env::var("TASK_QUEUE").unwrap_or_else(|_| "rust-workers".to_string());

    let client = Client::builder(server_url)
        .token(token)
        .control_token(client_token)
        .worker_token(worker_token)
        .namespace(namespace)
        .build()?;

    let mut worker = Worker::new(client.clone(), task_queue.clone())
        .worker_id(format!("rust-hello-{}", Uuid::new_v4()))
        .poll_timeout(Duration::from_secs(5));

    worker.register_activity("rust.hello_activity", |_ctx, args| async move {
        let name = args
            .get(0)
            .and_then(|value| value.as_str())
            .unwrap_or("world");
        Ok(json!({"greeting": format!("Hello, {name}!")}))
    });

    worker.register_workflow("rust.hello_workflow", |ctx, input| async move {
        let name = input
            .get(0)
            .and_then(|value| value.as_str())
            .unwrap_or("Rust");
        ctx.activity("rust.hello_activity", json!([name])).await
    });

    let workflow_id = format!("rust-hello-{}", Uuid::new_v4());
    let handle = client
        .start_workflow(
            "rust.hello_workflow",
            &task_queue,
            &workflow_id,
            json!(["Rust"]),
        )
        .await?;

    let watcher = handle.clone();
    let completed = Arc::new(AtomicBool::new(false));
    let observed_completion = Arc::clone(&completed);
    worker
        .run_until(async move {
            if tokio::time::timeout(Duration::from_secs(30), async move {
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
            .await
            .is_ok()
            {
                observed_completion.store(true, Ordering::SeqCst);
            }
        })
        .await?;

    if !completed.load(Ordering::SeqCst) {
        return Err(Error::Timeout);
    }

    let result = handle
        .result(WorkflowResultOptions {
            poll_interval: Duration::from_millis(500),
            timeout: Duration::from_secs(30),
        })
        .await?;

    println!("workflow_id={workflow_id}");
    println!("result={result}");
    Ok(())
}
