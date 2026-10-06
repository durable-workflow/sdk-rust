//! Local activity source qualification against an isolated published Server.
//! Run with the same endpoint/token variables as cooperative_server and
//! DURABLE_WORKFLOW_LOCAL_ISOLATED=1. Ordinary cargo tests leave these ignored.

use durable_workflow::{
    json, ActivityRetryPolicy, AvroValue, Client, Error, LocalActivityOptions, Value, Worker,
    WorkflowCommandOptions, WorkflowHandle, WorkflowResultOptions,
};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

fn client() -> Client {
    assert_eq!(
        std::env::var("DURABLE_WORKFLOW_LOCAL_ISOLATED").as_deref(),
        Ok("1"),
        "this test requires an isolated disposable runtime"
    );
    Client::builder(std::env::var("DURABLE_WORKFLOW_SERVER_URL").unwrap())
        .token(Some(std::env::var("DURABLE_WORKFLOW_AUTH_TOKEN").unwrap()))
        .build()
        .unwrap()
}

fn queue() -> String {
    format!("rust-local-{}", durable_workflow::Uuid::new_v4())
}

fn fidelity_value() -> AvroValue {
    AvroValue::Map(BTreeMap::from([
        ("bytes".into(), AvroValue::Bytes(vec![0, 255, 7])),
        (
            "integer".into(),
            AvroValue::Long(-9_223_372_036_854_775_000),
        ),
    ]))
}

async fn history(handle: &WorkflowHandle) -> Value {
    let response = reqwest::Client::new()
        .get(format!(
            "{}/api/workflows/{}/runs/{}/history",
            std::env::var("DURABLE_WORKFLOW_SERVER_URL").unwrap(),
            handle.workflow_id,
            handle.run_id.as_deref().unwrap()
        ))
        .query(&[("page_size", "1000")])
        .bearer_auth(std::env::var("DURABLE_WORKFLOW_AUTH_TOKEN").unwrap())
        .header("X-Namespace", "default")
        .header("X-Durable-Workflow-Control-Plane-Version", "2")
        .send()
        .await
        .unwrap();
    let status = response.status();
    let body = response.json::<Value>().await.unwrap();
    assert!(status.is_success(), "history {status}: {body}");
    assert!(body["next_page_token"].is_null());
    body
}

fn count(history: &Value, event_type: &str) -> usize {
    history["events"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|e| e["event_type"] == event_type)
        .count()
}

fn replay_worker(
    client: &Client,
    queue: &str,
    callbacks: Arc<AtomicUsize>,
    replacement: bool,
) -> Worker {
    let mut worker = Worker::new(client.clone(), queue)
        .worker_id(format!(
            "{queue}-{}",
            if replacement {
                "replacement"
            } else {
                "original"
            }
        ))
        .poll_timeout(Duration::from_millis(10))
        .local_activities(true);
    worker.register_workflow_avro_value("tests.rust-local-replay", |ctx, _| async move {
        let value = ctx
            .local_activity_avro_value_with_options(
                "echo",
                LocalActivityOptions::new()
                    .retry_policy(
                        ActivityRetryPolicy::new(3)
                            .backoff_intervals([Duration::ZERO, Duration::ZERO]),
                    )
                    .start_to_close_timeout(Duration::from_secs(3)),
                fidelity_value(),
            )
            .await?;
        ctx.wait_signal("finish").await?;
        Ok(value)
    });
    worker.register_activity_avro_value("echo", move |ctx, args| {
        assert!(
            !replacement,
            "replacement must replay the committed local result"
        );
        callbacks.fetch_add(1, Ordering::SeqCst);
        async move {
            ctx.heartbeat(json!({"attempt":ctx.attempt_number})).await?;
            if ctx.attempt_number == 1 {
                return Err(Error::WorkerLoop("retry local attempt".into()));
            }
            Ok(args)
        }
    });
    worker
}

#[tokio::test]
#[ignore = "requires isolated published Server"]
async fn published_server_records_local_retries_and_cold_replacement_replays_typed_result() {
    let client = client();
    let queue = queue();
    let callbacks = Arc::new(AtomicUsize::new(0));
    let worker = replay_worker(&client, &queue, callbacks.clone(), false);
    worker.register().await.unwrap();
    let handle = client
        .start_workflow("tests.rust-local-replay", &queue, &queue, json!([]))
        .await
        .unwrap();
    assert!(worker.run_once().await.unwrap() > 0);
    assert_eq!(callbacks.load(Ordering::SeqCst), 2);
    let committed = history(&handle).await;
    assert_eq!(count(&committed, "ActivityCompleted"), 1);
    assert_eq!(count(&committed, "ActivityScheduled"), 1);
    let replacement = replay_worker(&client, &queue, callbacks.clone(), true);
    replacement.register().await.unwrap();
    handle.signal("finish", json!([])).await.unwrap();
    for _ in 0..10 {
        replacement.run_once().await.unwrap();
        if handle.describe().await.unwrap().status.as_deref() == Some("completed") {
            break;
        }
    }
    assert_eq!(
        handle
            .result_avro_value(WorkflowResultOptions::default())
            .await
            .unwrap(),
        AvroValue::Array(vec![fidelity_value()])
    );
    assert_eq!(callbacks.load(Ordering::SeqCst), 2);
    assert_eq!(count(&history(&handle).await, "ActivityCompleted"), 1);
    println!(
        "local retries, typed values and cold replacement passed: {}",
        handle.workflow_id
    );
}

#[tokio::test]
#[ignore = "requires isolated published Server"]
async fn published_server_renews_workflow_lease_through_long_local_callback() {
    let client = client();
    let queue = queue();
    let mut worker = Worker::new(client.clone(), &queue)
        .worker_id(&queue)
        .poll_timeout(Duration::from_millis(10))
        .local_activities(true);
    worker.register_workflow("tests.rust-local-long", |ctx, _| async move {
        ctx.local_activity_with_options(
            "long",
            LocalActivityOptions::new().start_to_close_timeout(Duration::from_secs(20)),
            json!([]),
        )
        .await
    });
    worker.register_activity("long", |_, _| async {
        // The qualification runtime's actual workflow-task lease is 10 seconds.
        tokio::time::sleep(Duration::from_secs(13)).await;
        Ok(json!("completed after original lease"))
    });
    worker.register().await.unwrap();
    let handle = client
        .start_workflow("tests.rust-local-long", &queue, &queue, json!([]))
        .await
        .unwrap();
    worker.run_once().await.unwrap();
    assert_eq!(
        handle
            .result(WorkflowResultOptions::default())
            .await
            .unwrap(),
        json!("completed after original lease")
    );
    assert_eq!(count(&history(&handle).await, "ActivityCompleted"), 1);
    println!(
        "local callback completed after original lease: {}",
        handle.workflow_id
    );
}

struct Dropped(Arc<AtomicBool>);
impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[tokio::test]
#[ignore = "requires isolated published Server"]
async fn published_server_terminal_cancel_drops_local_callback_without_application_heartbeats() {
    let client = client();
    let queue = queue();
    let mut worker = Worker::new(client.clone(), &queue)
        .worker_id(&queue)
        .poll_timeout(Duration::from_millis(10))
        .local_activities(true);
    worker.register_workflow("tests.rust-local-cancel", |ctx, _| async move {
        ctx.local_activity("wait", json!([])).await
    });
    let entered = Arc::new(AtomicBool::new(false));
    let observed = entered.clone();
    let dropped = Arc::new(AtomicBool::new(false));
    let stopped = dropped.clone();
    worker.register_activity("wait", move |_, _| {
        observed.store(true, Ordering::SeqCst);
        let guard = Dropped(stopped.clone());
        async move {
            let _guard = guard;
            std::future::pending::<durable_workflow::Result<Value>>().await
        }
    });
    worker.register().await.unwrap();
    let handle = client
        .start_workflow("tests.rust-local-cancel", &queue, &queue, json!([]))
        .await
        .unwrap();
    let run = tokio::spawn(async move { worker.run_once().await });
    tokio::time::timeout(Duration::from_secs(5), async {
        while !entered.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    handle
        .cancel(WorkflowCommandOptions::default())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(dropped.load(Ordering::SeqCst));
    assert!(matches!(
        handle.result(WorkflowResultOptions::default()).await,
        Err(Error::WorkflowCancelled(_))
    ));
    assert_eq!(count(&history(&handle).await, "ActivityCompleted"), 0);
    println!(
        "terminal cancellation stopped local callback without application heartbeats: {}",
        handle.workflow_id
    );
}

#[tokio::test]
#[ignore = "requires isolated published Server"]
async fn published_server_external_payloads_survive_local_completion_and_cold_replay() {
    let client = client();
    let queue = queue();
    let response = reqwest::Client::new().put(format!("{}/api/namespaces/default/external-storage",
        std::env::var("DURABLE_WORKFLOW_SERVER_URL").unwrap()))
        .bearer_auth(std::env::var("DURABLE_WORKFLOW_AUTH_TOKEN").unwrap())
        .header("X-Namespace", "default").header("X-Durable-Workflow-Control-Plane-Version", "2")
        .json(&json!({"driver":"local","threshold_bytes":256,"config":{"uri":"file:///app/database/runtime-external-payloads/default"}}))
        .send().await.unwrap();
    assert!(
        response.status().is_success(),
        "isolated namespace external storage setup: {}",
        response.text().await.unwrap()
    );
    let value = AvroValue::Bytes(vec![0, 255, 7, 128].repeat(2048));
    let input = value.clone();
    let mut worker = Worker::new(client.clone(), &queue)
        .worker_id(format!("{queue}-original"))
        .local_activities(true)
        .poll_timeout(Duration::from_millis(10));
    worker.register_workflow_avro_value("tests.rust-local-external", move |ctx, _| {
        let input = input.clone();
        async move {
            let value = ctx.local_activity_avro_value("external", input).await?;
            ctx.wait_signal("finish").await?;
            Ok(value)
        }
    });
    worker.register_activity_avro_value("external", |ctx, args| async move {
        ctx.heartbeat(AvroValue::Bytes(vec![1, 254].repeat(2048)))
            .await?;
        Ok(args)
    });
    worker.register().await.unwrap();
    let handle = client
        .start_workflow("tests.rust-local-external", &queue, &queue, json!([]))
        .await
        .unwrap();
    worker.run_once().await.unwrap();
    let committed = history(&handle).await;
    assert_eq!(count(&committed, "ActivityCompleted"), 1);
    assert!(
        committed.to_string().contains("external_payload"),
        "raw committed history must contain payload references"
    );
    let input = value.clone();
    let mut replacement = Worker::new(client.clone(), &queue)
        .worker_id(format!("{queue}-replacement"))
        .local_activities(true)
        .poll_timeout(Duration::from_millis(10));
    replacement.register_workflow_avro_value("tests.rust-local-external", move |ctx, _| {
        let input = input.clone();
        async move {
            let value = ctx.local_activity_avro_value("external", input).await?;
            ctx.wait_signal("finish").await?;
            Ok(value)
        }
    });
    replacement.register_activity_avro_value("external", |_, _| async {
        panic!("cold external replay cannot execute the local callback")
    });
    replacement.register().await.unwrap();
    handle.signal("finish", json!([])).await.unwrap();
    for _ in 0..10 {
        replacement.run_once().await.unwrap();
        if handle.describe().await.unwrap().status.as_deref() == Some("completed") {
            break;
        }
    }
    assert_eq!(
        handle
            .result_avro_value(WorkflowResultOptions::default())
            .await
            .unwrap(),
        AvroValue::Array(vec![value])
    );
    assert_eq!(count(&history(&handle).await, "ActivityCompleted"), 1);
    println!(
        "external local input, result and heartbeats survived cold replay: {}",
        handle.workflow_id
    );
}
