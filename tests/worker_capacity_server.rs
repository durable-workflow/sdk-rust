//! Concurrency acceptance against an isolated published Server.

use durable_workflow::{json, Client, Error, Value, Worker, WorkflowResultOptions};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

#[tokio::test]
#[ignore = "requires isolated published Server"]
async fn configured_activity_slots_make_progress_and_recover_after_success_and_failure() {
    assert_eq!(
        std::env::var("DURABLE_WORKFLOW_CAPACITY_ISOLATED").as_deref(),
        Ok("1"),
        "this test requires an isolated disposable runtime"
    );
    let client = Client::builder(std::env::var("DURABLE_WORKFLOW_SERVER_URL").unwrap())
        .token(Some(std::env::var("DURABLE_WORKFLOW_AUTH_TOKEN").unwrap()))
        .build()
        .unwrap();
    let queue = format!("rust-capacity-{}", durable_workflow::Uuid::new_v4());
    let started = Arc::new(AtomicUsize::new(0));
    let active = Arc::new(AtomicUsize::new(0));
    let maximum = Arc::new(AtomicUsize::new(0));
    let finished = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let mut worker = Worker::new(client.clone(), &queue)
        .worker_id(format!("{queue}-worker"))
        .max_concurrent_workflow_tasks(2)
        .max_concurrent_activity_tasks(3)
        .poll_timeout(Duration::from_millis(10));
    worker.register_activity("capacity.work", {
        let (started, active, maximum, finished, gate) = (
            started.clone(),
            active.clone(),
            maximum.clone(),
            finished.clone(),
            gate.clone(),
        );
        move |_, input| {
            let (started, active, maximum, finished, gate) = (
                started.clone(),
                active.clone(),
                maximum.clone(),
                finished.clone(),
                gate.clone(),
            );
            async move {
                let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                maximum.fetch_max(current, Ordering::SeqCst);
                started.fetch_add(1, Ordering::SeqCst);
                gate.acquire().await.unwrap().forget();
                active.fetch_sub(1, Ordering::SeqCst);
                finished.fetch_add(1, Ordering::SeqCst);
                if input[0] == 1 {
                    Err(Error::WorkerLoop("planned capacity failure".into()))
                } else {
                    Ok(input)
                }
            }
        }
    });
    worker.register_workflow("tests.worker-capacity", |ctx, input| async move {
        match ctx.activity("capacity.work", input).await {
            Ok(value) => Ok(value),
            Err(Error::ActivityFailed(_)) => Ok(json!("expected failure")),
            Err(error) => Err(error),
        }
    });
    worker.register().await.unwrap();
    let mut handles = Vec::new();
    for index in 0..7 {
        handles.push(
            client
                .start_workflow(
                    "tests.worker-capacity",
                    &queue,
                    &format!("{queue}-{index}"),
                    json!([index]),
                )
                .await
                .unwrap(),
        );
    }
    let watch = handles.clone();
    let acceptance = async {
        while started.load(Ordering::SeqCst) < 3 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(active.load(Ordering::SeqCst), 3);
        assert_eq!(finished.load(Ordering::SeqCst), 0);
        gate.add_permits(1);
        while started.load(Ordering::SeqCst) < 4 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(finished.load(Ordering::SeqCst), 1);
        assert_eq!(active.load(Ordering::SeqCst), 3);
        gate.add_permits(7);
        loop {
            let mut complete = true;
            for handle in &watch {
                complete &= handle.describe().await.unwrap().is_terminal();
            }
            if complete {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(45), worker.run_until(acceptance))
        .await
        .expect("the configured three activity slots must progress")
        .unwrap();
    assert_eq!(maximum.load(Ordering::SeqCst), 3);
    assert_eq!(finished.load(Ordering::SeqCst), 7);
    for (index, handle) in handles.iter().enumerate() {
        let result: Value = handle
            .result(WorkflowResultOptions::default())
            .await
            .unwrap();
        assert_eq!(
            result,
            if index == 1 {
                json!("expected failure")
            } else {
                json!([index])
            }
        );
    }
    println!("published Server capacity passed: configured=3, maximum=3, completed=7, including one failure");
}
