//! Read-only: enumerate runs, and optionally one selected run's complete history.
use durable_workflow::{Client, WorkflowHistoryOptions, WorkflowListOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::builder(std::env::var("DURABLE_WORKFLOW_SERVER_URL")?)
        .token(std::env::var("DURABLE_WORKFLOW_AUTH_TOKEN").ok())
        .namespace(std::env::var("DURABLE_WORKFLOW_NAMESPACE").unwrap_or_else(|_| "default".into()))
        .build()?;
    let mut options = WorkflowListOptions::new().page_size(50);
    if let Ok(query) = std::env::var("DURABLE_WORKFLOW_QUERY") {
        options = options.query(query);
    }
    let mut runs = client.iter_workflows(options);
    let mut count = 0;
    while let Some(run) = runs.next().await? {
        println!("{} {} {}", run.workflow_id, run.run_id, run.status);
        count += 1;
    }
    println!("read {count} workflow summaries");
    if let (Ok(workflow), Ok(run)) = (
        std::env::var("DURABLE_WORKFLOW_ID"),
        std::env::var("DURABLE_WORKFLOW_RUN_ID"),
    ) {
        let mut history = client.iter_workflow_history(
            workflow,
            run,
            WorkflowHistoryOptions::new().page_size(100),
        );
        let mut count = 0;
        while history.next().await?.is_some() {
            count += 1;
        }
        println!("read {count} history events");
    }
    Ok(())
}
