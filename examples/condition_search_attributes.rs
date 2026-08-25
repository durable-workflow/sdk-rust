use std::time::Duration;

use durable_workflow::{
    json, wait_condition, Client, ConditionWaitResult, Result, SearchAttributeUpdate, Worker,
};

#[tokio::main]
async fn main() -> Result<()> {
    let client = Client::new("http://127.0.0.1:8080")?;
    let mut worker = Worker::new(client, "approval-workers");

    worker.register_workflow("approve-order", |ctx, _input| async move {
        let predicate_ctx = ctx.clone();
        let outcome = wait_condition!(
            ctx,
            "approval-received",
            timeout: Duration::from_secs(300),
            move || Ok(!predicate_ctx.signals("approve")?.is_empty()),
        )
        .await?;

        let status = match outcome {
            ConditionWaitResult::Satisfied => "approved",
            ConditionWaitResult::TimedOut => "approval_timed_out",
        };
        let attributes = SearchAttributeUpdate::new()
            .keyword("OrderStatus", status)?
            .bool("NeedsAttention", outcome.is_timed_out())?;
        ctx.upsert_search_attributes(attributes)?;

        Ok(json!({"status": status}))
    });

    worker.run().await
}
