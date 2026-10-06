//! Session affinity for a process-local resource. Replacement holders rebuild it.
use durable_workflow::{
    json, Client, Result, Uuid, Worker, WorkerSessionOptions, WorkflowResultOptions,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Clone, Deserialize, Serialize)]
struct Request {
    name: String,
    session_id: String,
}
#[derive(Deserialize, Serialize)]
struct Greeting {
    greeting: String,
    resource_generation: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let url = std::env::var("DURABLE_WORKFLOW_RUNTIME_URL")
        .or_else(|_| std::env::var("DURABLE_WORKFLOW_SERVER_URL"))
        .unwrap_or_else(|_| "http://127.0.0.1:8080".into());
    let client = Client::builder(url)
        .token(std::env::var("DURABLE_WORKFLOW_TOKEN").ok())
        .control_token(std::env::var("DURABLE_WORKFLOW_CLIENT_TOKEN").ok())
        .worker_token(std::env::var("DURABLE_WORKFLOW_WORKER_TOKEN").ok())
        .namespace(
            std::env::var("DURABLE_WORKFLOW_RUNTIME_NAMESPACE")
                .unwrap_or_else(|_| "default".into()),
        )
        .build()?;
    let queue = std::env::var("TASK_QUEUE").unwrap_or_else(|_| "rust-session-example".into());
    let mut worker = Worker::new(client.clone(), &queue)
        .worker_sessions(true)
        .worker_id(format!("rust-session-example-{}", Uuid::new_v4()))
        .capabilities(["cache:example"])
        .poll_timeout(Duration::from_secs(1));
    let resources = Arc::new(Mutex::new(HashMap::<String, String>::new()));
    worker.register_typed_activity("rust.session-greet", move |ctx, request: Request| {
        let resources = resources.clone();
        async move {
            let session = ctx
                .worker_session()
                .expect("this activity requires a session");
            let resource_generation = resources
                .lock()
                .unwrap()
                .entry(session.options().session_id().into())
                .or_insert_with(|| Uuid::new_v4().to_string())
                .clone();
            ctx.heartbeat(json!({"phase":"resource_ready"})).await?;
            Ok(Greeting {
                greeting: format!("Hello, {}!", request.name),
                resource_generation,
            })
        }
    });
    worker.register_typed_workflow("rust.session-example", |ctx, request: Request| async move {
        let options =
            WorkerSessionOptions::new(&request.session_id).requirements(["cache:example"]);
        let first: Greeting = ctx
            .activity("rust.session-greet", json!(request))
            .in_worker_session(options.clone())
            .typed()
            .await?;
        let second: Greeting = ctx
            .activity("rust.session-greet", json!(request))
            .in_worker_session(options)
            .typed()
            .await?;
        Ok(vec![first, second])
    });
    let id = format!("rust-session-example-{}", Uuid::new_v4());
    let request = Request {
        name: std::env::var("GREETING_NAME").unwrap_or_else(|_| "Rust".into()),
        session_id: id.clone(),
    };
    let handle = client
        .start_workflow("rust.session-example", &queue, &id, request)
        .await?;
    let watch = handle.clone();
    worker
        .run_until(async move {
            let _ = tokio::time::timeout(Duration::from_secs(30), async move {
                while !watch
                    .describe()
                    .await
                    .is_ok_and(|description| description.is_terminal())
                {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            })
            .await;
        })
        .await?;
    println!("{}", handle.result(WorkflowResultOptions::default()).await?);
    Ok(())
}
