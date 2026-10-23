//! Stable logical identity acceptance against an isolated published Server.
use durable_workflow::{
    json, ActivityError, ActivityOptions, ActivityRetryPolicy, Client, LocalActivityOptions, Value,
    Worker, WorkerSessionOptions, WorkflowResultOptions,
};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    Remote,
    Cooperative,
    Local,
    Session,
}

async fn check_identity(mode: Mode) {
    assert_eq!(
        std::env::var("DURABLE_WORKFLOW_LOCAL_ISOLATED").as_deref(),
        Ok("1")
    );
    let client = Client::builder(std::env::var("DURABLE_WORKFLOW_SERVER_URL").unwrap())
        .token(Some(std::env::var("DURABLE_WORKFLOW_AUTH_TOKEN").unwrap()))
        .build()
        .unwrap();
    let queue = format!("rust-identity-{}", durable_workflow::Uuid::new_v4());
    let captured = Arc::new(Mutex::new(Vec::<Value>::new()));
    let saved = captured.clone();
    let mut worker = Worker::new(client.clone(), &queue)
        .worker_id(format!("{queue}-worker"))
        .poll_timeout(Duration::from_millis(10))
        .local_activities(mode == Mode::Local)
        .worker_sessions(mode == Mode::Session)
        .cooperative_cancellation(mode == Mode::Cooperative);
    if mode == Mode::Session {
        worker = worker.capabilities(["identity:fixture"]);
    }
    worker.register_activity("identity",move |ctx,input| {
        let saved=saved.clone();
        async move {
            let info=json!({"name":input[0],"logical":ctx.activity_execution_id(),"workflow":ctx.workflow_id(),
                "run":ctx.run_id(),"attempt_id":ctx.activity_attempt_id,"attempt":ctx.attempt_number,
                "session":ctx.worker_session().is_some()});
            saved.lock().unwrap().push(info.clone());
            if ctx.attempt_number==1 { return Err(ActivityError::new("Transient","one planned retry").into()); }
            Ok(info)
        }
    });
    let activity_queue = queue.clone();
    worker.register_workflow("tests.activity-identity", move |ctx, _| {
        let activity_queue = activity_queue.clone();
        async move {
            let mut results = Vec::new();
            for name in ["first", "second"] {
                let policy = ActivityRetryPolicy::new(2).backoff_intervals([Duration::ZERO]);
                let result = if mode == Mode::Local {
                    ctx.local_activity_with_options(
                        "identity",
                        LocalActivityOptions::new().retry_policy(policy),
                        json!([name]),
                    )
                    .await?
                } else {
                    let call = ctx.activity_with_options(
                        "identity",
                        ActivityOptions::new()
                            .task_queue(&activity_queue)
                            .retry_policy(policy),
                        json!([name]),
                    );
                    if mode == Mode::Session {
                        call.in_worker_session(
                            WorkerSessionOptions::new(&activity_queue)
                                .queue(&activity_queue)
                                .requirements(["identity:fixture"])
                                .lease_seconds(5)
                                .ttl_seconds(60),
                        )
                        .await?
                    } else {
                        call.await?
                    }
                };
                results.push(result);
            }
            Ok(json!(results))
        }
    });
    worker.register().await.unwrap();
    let handle = client
        .start_workflow("tests.activity-identity", &queue, &queue, json!([]))
        .await
        .unwrap();
    let watch = handle.clone();
    tokio::time::timeout(
        Duration::from_secs(60),
        worker.run_until(async move {
            while !watch.describe().await.unwrap().is_terminal() {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }),
    )
    .await
    .expect("identity retries must converge")
    .unwrap();
    let result = handle
        .result(WorkflowResultOptions::default())
        .await
        .unwrap();
    assert_eq!(result.as_array().unwrap().len(), 2);
    let captured = captured.lock().unwrap();
    assert_eq!(captured.len(), 4);
    for (i, name) in ["first", "second"].iter().enumerate() {
        let attempts: Vec<_> = captured
            .iter()
            .filter(|info| info["name"] == *name)
            .collect();
        assert_eq!(attempts.len(), 2);
        assert_eq!(attempts[0]["attempt"], 1);
        assert_eq!(attempts[1]["attempt"], 2);
        assert_ne!(attempts[0]["attempt_id"], attempts[1]["attempt_id"]);
        for info in &attempts {
            assert_eq!(info["workflow"], handle.workflow_id);
            assert_eq!(info["run"].as_str(), handle.run_id.as_deref());
            assert_eq!(info["session"], mode == Mode::Session);
            if mode == Mode::Local {
                assert!(info["logical"].is_null());
            } else {
                assert!(info["logical"].as_str().is_some_and(|id| !id.is_empty()));
            }
        }
        assert_eq!(attempts[0]["logical"], attempts[1]["logical"]);
        assert_eq!(result[i], *attempts[1]);
    }
    if mode != Mode::Local {
        assert_ne!(result[0]["logical"], result[1]["logical"]);
    }
    println!("published activity identity passed: mode={mode:?}, two logical calls, four attempts, immutable workflow/run, distinct attempt IDs");
}
#[tokio::test]
#[ignore = "requires isolated published Server"]
async fn normal_activity_retry_preserves_logical_execution_and_workflow_identity() {
    check_identity(Mode::Remote).await;
}
#[tokio::test]
#[ignore = "requires isolated published Server"]
async fn cooperative_activity_retry_preserves_logical_execution_and_workflow_identity() {
    check_identity(Mode::Cooperative).await;
}
#[tokio::test]
#[ignore = "requires isolated published Server"]
async fn local_activity_identity_reports_parent_ids_without_fabricated_execution_id() {
    check_identity(Mode::Local).await;
}
#[tokio::test]
#[ignore = "requires isolated published Server"]
async fn session_activity_retry_preserves_original_execution_and_workflow_identity() {
    check_identity(Mode::Session).await;
}
