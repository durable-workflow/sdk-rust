use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use durable_workflow::{
    ActivityOptions, ActivityRetryPolicy, Client, Error, Result, Uuid, Worker,
    WorkflowResultOptions,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
struct GreetingRequest {
    name: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct GreetingActivityResult {
    name: String,
    greeting: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct GreetingWorkflowResult {
    name: String,
    greeting: String,
    intentional_activity_failure: bool,
}

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

    worker.register_typed_activity(
        "rust.hello_activity",
        |_ctx, request: GreetingRequest| async move {
            Ok(GreetingActivityResult {
                greeting: format!("Hello, {}!", request.name),
                name: request.name,
            })
        },
    );
    worker.register_typed_activity(
        "rust.intentional_failure",
        |_ctx, request: GreetingRequest| async move {
            Err::<GreetingActivityResult, _>(Error::WorkerLoop(format!(
                "intentional greeting failure for {}",
                request.name
            )))
        },
    );

    let activity_queue = task_queue.clone();
    worker.register_typed_workflow(
        "rust.hello_workflow",
        move |ctx, request: GreetingRequest| {
            let activity_queue = activity_queue.clone();
            async move {
                let greeting: GreetingActivityResult = ctx
                    .activity_typed_with_options(
                        "rust.hello_activity",
                        ActivityOptions::new()
                            .task_queue(activity_queue.clone())
                            .retry_policy(ActivityRetryPolicy::new(3))
                            .start_to_close_timeout(Duration::from_secs(10))
                            .schedule_to_close_timeout(Duration::from_secs(30)),
                        request.clone(),
                    )
                    .await?;

                let intentional_activity_failure = match ctx
                    .activity_typed_with_options::<_, GreetingActivityResult>(
                        "rust.intentional_failure",
                        ActivityOptions::new()
                            .task_queue(activity_queue)
                            .retry_policy(ActivityRetryPolicy::new(1))
                            .start_to_close_timeout(Duration::from_secs(10)),
                        request,
                    )
                    .await
                {
                    Err(Error::ActivityFailed(_)) => true,
                    Err(error) => return Err(error),
                    Ok(_) => false,
                };

                Ok(GreetingWorkflowResult {
                    name: greeting.name,
                    greeting: greeting.greeting,
                    intentional_activity_failure,
                })
            }
        },
    );

    let workflow_id = format!("rust-hello-{}", Uuid::new_v4());
    let request = GreetingRequest {
        name: std::env::var("GREETING_NAME").unwrap_or_else(|_| "Rust".to_string()),
    };
    let handle = client
        .start_workflow(
            "rust.hello_workflow",
            &task_queue,
            &workflow_id,
            request.clone(),
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

    let result: GreetingWorkflowResult = handle
        .result_typed(WorkflowResultOptions {
            poll_interval: Duration::from_millis(500),
            timeout: Duration::from_secs(30),
        })
        .await?;

    if result.name != request.name || !result.intentional_activity_failure {
        return Err(Error::WorkerLoop(
            "typed workflow result did not preserve the request and failure path".to_string(),
        ));
    }

    println!("workflow_id={workflow_id}");
    println!("result={result:?}");
    Ok(())
}
