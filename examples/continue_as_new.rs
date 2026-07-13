use durable_workflow::{json, Client, ContinueAsNewOptions, Result, Value, Worker};

fn bounded_worker(client: Client) -> Worker {
    let mut worker = Worker::new(client, "batch-workers");
    worker.register_workflow("bounded-batch", |ctx, input| async move {
        let next = input.get(0).and_then(Value::as_u64).unwrap_or(0);
        let stop = input.get(1).and_then(Value::as_u64).unwrap_or(1_000);
        let chunk_end = next.saturating_add(100).min(stop);

        for item in next..chunk_end {
            ctx.activity("process-item", json!([item])).await?;
        }

        if chunk_end < stop {
            return ctx.continue_as_new_with_options(
                ContinueAsNewOptions::new().task_queue("batch-workers"),
                json!([chunk_end, stop]),
            );
        }

        Ok(json!({"processed": stop}))
    });
    worker
}

#[tokio::main]
async fn main() -> Result<()> {
    let client = Client::new("http://127.0.0.1:8080/api")?;
    bounded_worker(client).run().await
}
