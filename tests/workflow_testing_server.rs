//! Installed/export acceptance against an isolated published Server.
use durable_workflow::{
    json,
    testing::{WorkflowEnvironment, WorkflowHistory},
    Client, Error, Worker, WorkflowResultOptions,
};
use std::time::Duration;

#[tokio::test]
#[ignore = "requires isolated published Server"]
async fn published_server_history_export_replays_offline_and_detects_changed_activity() {
    assert_eq!(
        std::env::var("DURABLE_WORKFLOW_LOCAL_ISOLATED").as_deref(),
        Ok("1")
    );
    let client = Client::builder(std::env::var("DURABLE_WORKFLOW_SERVER_URL").unwrap())
        .token(Some(std::env::var("DURABLE_WORKFLOW_AUTH_TOKEN").unwrap()))
        .build()
        .unwrap();
    let queue = format!("rust-offline-replay-{}", durable_workflow::Uuid::new_v4());
    let mut worker = Worker::new(client.clone(), &queue).poll_timeout(Duration::from_millis(10));
    worker.register_activity("first", |_, args| async move { Ok(args[0].clone()) });
    worker.register_activity("second", |_, args| async move { Ok(args[0].clone()) });
    worker.register_workflow("offline-replay", |ctx, args| async move {
        let number: i32 = ctx.side_effect(|| 42)?;
        let version = ctx.get_version("offline-v1", 1, 1)?;
        let first = ctx.activity("first", args).await?;
        ctx.sleep(Duration::from_secs(1)).await?;
        let second = ctx.activity("second", json!([first])).await?;
        Ok(json!({"number":number,"version":version,"result":second}))
    });
    worker.register().await.unwrap();
    let input = "x".repeat(128 * 1024);
    let handle = client
        .start_workflow("offline-replay", &queue, &queue, json!([input]))
        .await
        .unwrap();
    let watcher = handle.clone();
    tokio::time::timeout(
        Duration::from_secs(45),
        worker.run_until(async move {
            loop {
                if watcher.describe().await.unwrap().is_terminal() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        handle
            .result(WorkflowResultOptions::default())
            .await
            .unwrap(),
        json!({"number":42,"version":1,"result":input})
    );
    let export = client
        .export_workflow_history(&handle.workflow_id, handle.run_id.as_deref().unwrap())
        .await
        .unwrap();
    assert_eq!(export["schema"], "durable-workflow.v2.history-export");
    let history = WorkflowHistory::from_export(export).unwrap();
    let env = WorkflowEnvironment::from_worker(worker);
    assert!(env.replay(&history).unwrap().completed);
    let mut changed = WorkflowEnvironment::new().unwrap();
    changed.register_workflow("offline-replay", |ctx, args| async move {
        let _: i32 = ctx.side_effect(|| panic!("recorded side effect must be replayed"))?;
        ctx.get_version("offline-v1", 1, 1)?;
        ctx.activity("changed", args).await
    });
    assert!(
        matches!(changed.replay(&history),Err(Error::NonDeterministicReplay(f)) if f.sequence==Some(3) && f.expected.as_deref()==Some("activity:first") && f.actual.as_deref()==Some("activity:changed"))
    );
    println!("published Server export:128KiB original input replayed unchanged; changed activity diverged at sequence3");
}
