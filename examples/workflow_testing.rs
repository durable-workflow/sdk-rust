//! Run with `cargo run --example workflow_testing`; no Server is required.
use durable_workflow::{
    json,
    testing::{WorkflowEnvironment, WorkflowHistory},
    Result,
};

#[tokio::main]
async fn main() -> Result<()> {
    let mut env = WorkflowEnvironment::new()?;
    env.register_activity("greet", |_, args| async move {
        Ok(json!(format!(
            "Hello, {}!",
            args[0].as_str().unwrap_or("friend")
        )))
    });
    env.register_workflow("hello", |ctx, args| async move {
        ctx.activity("greet", args).await
    });

    // An activity unit test executes the real callback and argument adapters.
    assert_eq!(
        env.run_activity("greet", "Ada").await?,
        json!("Hello, Ada!")
    );
    // A workflow unit test may replace activity callbacks explicitly.
    env.mock_activity("greet", |_, args| async move {
        Ok(json!({"greeting_for":args[0]}))
    });
    assert_eq!(
        env.run_workflow("hello", "Ada").await?,
        json!({"greeting_for":"Ada"})
    );

    // Replay executes the same registered workflow without running activities.
    // In a deployment test, read the JSON produced by dw workflow:history-export
    // or Client::export_workflow_history instead of this captured test history.
    let history = WorkflowHistory::from_export(env.last_history().unwrap().export())?;
    assert!(env.replay(&history)?.completed);
    println!("activity, workflow and offline replay passed");
    Ok(())
}
