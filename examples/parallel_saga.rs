use std::time::Duration;

use durable_workflow::{
    json, ChildWorkflowOptions, Error, ParallelOperation, Result, Value, WorkflowContext,
};

#[allow(dead_code)]
async fn nested_parallel(ctx: WorkflowContext) -> Result<Value> {
    let results = ctx
        .join(vec![
            ParallelOperation::activity("trip.quote-flight", json!([])),
            ParallelOperation::group(vec![
                ParallelOperation::child_workflow(
                    "trip.quote-hotel",
                    ChildWorkflowOptions::new("trip-workers"),
                    json!([]),
                ),
                ParallelOperation::timer(Duration::from_secs(1)),
            ]),
        ])
        .await?;

    Ok(json!({"top_level_members": results.len()}))
}

#[allow(dead_code)]
async fn inspect_parallel_failure(ctx: WorkflowContext) -> Result<Value> {
    match nested_parallel(ctx).await {
        Ok(value) => Ok(value),
        Err(Error::ParallelFailed(failure)) => Ok(json!({
            "failed_member_path": failure.member_path,
            "completed_members": failure.completed.len(),
            "cause": failure.cause.to_string(),
        })),
        Err(error) => Err(error),
    }
}

#[allow(dead_code)]
async fn trip_saga(ctx: WorkflowContext) -> Result<Value> {
    let mut saga = ctx.saga();
    let outcome = async {
        let flight = ctx.activity("trip.reserve-flight", json!([])).await?;
        saga.add_compensation("trip.cancel-flight", json!([flight]))?;

        let hotel = ctx.activity("trip.reserve-hotel", json!([])).await?;
        saga.add_compensation("trip.cancel-hotel", json!([hotel]))?;

        ctx.throw_if_cancellation_requested()?;
        ctx.activity("trip.charge", json!([])).await?;
        Ok(json!({"status": "booked"}))
    }
    .await;

    // Replay and worker restart resume the next ordinary compensation command.
    // A compensation failure returns Error::SagaCompensationFailed with both
    // the initiating failure and the compensation failure.
    saga.finish(outcome).await
}

fn main() {
    println!("register nested_parallel and trip_saga on a Durable Workflow worker");
}
