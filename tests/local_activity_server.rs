//! Local activity source qualification against an isolated published Server.
//! Run with the same endpoint/token variables as cooperative_server and
//! DURABLE_WORKFLOW_LOCAL_ISOLATED=1. Ordinary cargo tests leave these ignored.

use durable_workflow::{
    json, ActivityRetryPolicy, AvroValue, Client, Error, LocalActivityOptions, StickyCacheOptions,
    Value, Worker, WorkflowCommandOptions, WorkflowHandle, WorkflowResultOptions,
};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

async fn finish_on(worker: &Worker, handle: &WorkflowHandle) {
    tokio::time::timeout(Duration::from_secs(35), async {
        while handle.describe().await.unwrap().status.as_deref() != Some("completed") {
            worker.run_once().await.unwrap();
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("replacement must complete the workflow within its real lease budget");
}

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
    if std::env::var("DURABLE_WORKFLOW_STICKY_CACHE_QUALIFICATION").as_deref() == Ok("1") {
        worker = worker.sticky_cache(StickyCacheOptions::new(2)).unwrap();
    }
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

fn mixed_worker(client: &Client, queue: &str, stage: usize, calls: Arc<AtomicUsize>) -> Worker {
    let mut worker = Worker::new(client.clone(), queue)
        .worker_id(format!("{queue}-stage-{stage}"))
        .local_activities(true)
        .poll_timeout(Duration::from_millis(10));
    worker.register_workflow("tests.rust-local-mixed", |ctx, _| async move {
        let first = ctx.local_activity("first", json!([])).await?;
        let remote = ctx.activity("remote", json!([])).await?;
        ctx.sleep(Duration::from_secs(1)).await?;
        let second = ctx.local_activity("second", json!([])).await?;
        ctx.wait_signal("finish").await?;
        Ok(json!([first, remote, second]))
    });
    for (name, allowed_stage) in [("first", 0), ("remote", 0), ("second", 1)] {
        let calls = calls.clone();
        worker.register_activity(name, move |_, _| {
            assert_eq!(stage, allowed_stage, "committed {name} must replay");
            calls.fetch_add(1, Ordering::SeqCst);
            async move { Ok(json!(name)) }
        });
    }
    worker
}

#[tokio::test]
#[ignore = "requires isolated published Server"]
async fn published_server_mixed_local_remote_timer_commands_replay_on_cold_workers() {
    let client = client();
    let queue = queue();
    let calls = Arc::new(AtomicUsize::new(0));
    let original = mixed_worker(&client, &queue, 0, calls.clone());
    original.register().await.unwrap();
    let handle = client
        .start_workflow("tests.rust-local-mixed", &queue, &queue, json!([]))
        .await
        .unwrap();
    original.run_once().await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let replacement = mixed_worker(&client, &queue, 1, calls.clone());
    replacement.register().await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        while calls.load(Ordering::SeqCst) < 3 {
            replacement.run_once().await.unwrap();
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let committed = history(&handle).await;
    assert_eq!(count(&committed, "ActivityCompleted"), 3);
    assert_eq!(count(&committed, "TimerFired"), 1);
    let cold = mixed_worker(&client, &queue, 2, calls.clone());
    cold.register().await.unwrap();
    handle.signal("finish", json!([])).await.unwrap();
    finish_on(&cold, &handle).await;
    assert_eq!(
        handle
            .result(WorkflowResultOptions::default())
            .await
            .unwrap(),
        json!(["first", "remote", "second"])
    );
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert_eq!(count(&history(&handle).await, "ActivityCompleted"), 3);
    println!(
        "local, remote and timer cold replay passed: {}",
        handle.workflow_id
    );
}

fn failures_worker(client: &Client, queue: &str, replacement: bool) -> Worker {
    let mut worker = Worker::new(client.clone(), queue)
        .worker_id(format!("{queue}-{replacement}"))
        .local_activities(true)
        .poll_timeout(Duration::from_millis(10));
    worker.register_workflow("tests.rust-local-failures", |ctx, _| async move {
        let mut failures = Vec::new();
        for (kind, options) in [
            (
                "start_to_close",
                LocalActivityOptions::new().start_to_close_timeout(Duration::from_secs(1)),
            ),
            (
                "heartbeat",
                LocalActivityOptions::new().heartbeat_timeout(Duration::from_secs(1)),
            ),
            (
                "schedule_to_close",
                LocalActivityOptions::new().schedule_to_close_timeout(Duration::from_secs(1)),
            ),
            (
                "exhausted",
                LocalActivityOptions::new()
                    .retry_policy(ActivityRetryPolicy::new(2).backoff_intervals([Duration::ZERO])),
            ),
            (
                "non_retryable",
                LocalActivityOptions::new().retry_policy(
                    ActivityRetryPolicy::new(3).non_retryable_error_types(["RustActivityError"]),
                ),
            ),
        ] {
            let result = ctx
                .local_activity_with_options(
                    if kind == "exhausted" || kind == "non_retryable" {
                        "fail"
                    } else {
                        "never"
                    },
                    options,
                    json!([]),
                )
                .await;
            match result {
                Err(Error::ActivityFailed(failure)) => {
                    if kind == "exhausted" || kind == "non_retryable" {
                        assert_eq!(failure.reason, "application");
                        assert_eq!(
                            failure.attempt_number,
                            Some(if kind == "exhausted" { 2 } else { 1 })
                        );
                        assert_eq!(failure.non_retryable, kind == "non_retryable");
                    } else {
                        assert_eq!(failure.reason, kind);
                        assert_eq!(failure.timeout_kind.as_deref(), Some(kind));
                        assert_eq!(failure.attempt_number, Some(1));
                    }
                    failures.push(json!({"kind":kind, "attempt":failure.attempt_number,
                        "timeout":failure.timeout_kind, "non_retryable":failure.non_retryable}));
                }
                other => panic!("expected typed local failure for {kind}: {other:?}"),
            }
        }
        ctx.wait_signal("finish").await?;
        Ok(json!(failures))
    });
    worker.register_activity("never", move |_, _| async move {
        assert!(!replacement, "committed timeout must replay");
        std::future::pending::<durable_workflow::Result<Value>>().await
    });
    worker.register_activity("fail", move |_, _| async move {
        assert!(!replacement, "committed failure must replay");
        Err(Error::WorkerLoop("planned local failure".into()))
    });
    worker
}

#[tokio::test]
#[ignore = "requires isolated published Server"]
async fn published_server_records_local_timeouts_exhausted_retries_and_non_retryable_failures() {
    let client = client();
    let queue = queue();
    let original = failures_worker(&client, &queue, false);
    original.register().await.unwrap();
    let handle = client
        .start_workflow("tests.rust-local-failures", &queue, &queue, json!([]))
        .await
        .unwrap();
    original.run_once().await.unwrap();
    let committed = history(&handle).await;
    assert_eq!(count(&committed, "ActivityTimedOut"), 3);
    assert_eq!(count(&committed, "ActivityFailed"), 2);
    let cold = failures_worker(&client, &queue, true);
    cold.register().await.unwrap();
    handle.signal("finish", json!([])).await.unwrap();
    finish_on(&cold, &handle).await;
    let result = handle
        .result(WorkflowResultOptions::default())
        .await
        .unwrap();
    assert_eq!(result[3]["attempt"], 2);
    assert_eq!(result[4]["attempt"], 1);
    assert_eq!(result[4]["non_retryable"], true);
    println!(
        "local timeout and failure cold replay passed: {}",
        handle.workflow_id
    );
}

struct Process(std::process::Child);
impl Drop for Process {
    fn drop(&mut self) {
        if !matches!(self.0.try_wait(), Ok(Some(_))) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}
struct Scratch(PathBuf);
impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
async fn child_file(child: &mut Process, path: &Path) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !path.is_file() {
            assert!(
                child.0.try_wait().unwrap().is_none(),
                "child exited before {}",
                path.display()
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("actual child worker reached local callback");
}

fn crash_worker(client: &Client, queue: &str, worker_id: &str) -> Worker {
    let mut worker = Worker::new(client.clone(), queue)
        .worker_id(worker_id)
        .local_activities(true)
        .poll_timeout(Duration::from_millis(10));
    worker.register_workflow("tests.rust-local-crash", |ctx, _| async move {
        ctx.local_activity("effect", json!([])).await
    });
    worker
}

#[tokio::test]
async fn local_process_worker_child() {
    let Ok(queue) = std::env::var("DURABLE_WORKFLOW_LOCAL_CHILD_QUEUE") else {
        return;
    };
    let ready = std::env::var("DURABLE_WORKFLOW_LOCAL_CHILD_READY").unwrap();
    let grant = std::env::var("DURABLE_WORKFLOW_LOCAL_CHILD_GRANT").unwrap();
    let mut worker = crash_worker(&client(), &queue, &format!("{queue}-killed"));
    worker.register_activity("effect", move |ctx, _| {
        let grant = grant.clone();
        async move {
            let pending = format!("{grant}.pending");
            std::fs::write(&pending, serde_json::to_vec(&json!({
                "task_id":ctx.task_id, "owner":ctx.lease_owner, "attempt_id":ctx.activity_attempt_id,
                "worker_id":ctx.worker_id, "effect":"performed before acknowledgement"
            })).unwrap()).unwrap();
            std::fs::rename(pending, grant).unwrap();
            std::future::pending::<durable_workflow::Result<Value>>().await
        }
    });
    worker.register().await.unwrap();
    std::fs::write(ready, b"registered").unwrap();
    loop {
        worker.run_once().await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[cfg(unix)]
#[tokio::test]
#[ignore = "requires isolated published Server with real 10-second lease and repair"]
async fn published_server_sigkill_reclaims_uncommitted_local_effect_once_in_history() {
    use std::os::unix::process::ExitStatusExt;
    let client = client();
    let queue = queue();
    let scratch = Scratch(std::env::temp_dir().join(format!("{queue}-crash")));
    std::fs::create_dir(&scratch.0).unwrap();
    let ready = scratch.0.join("ready");
    let grant = scratch.0.join("grant.json");
    let mut child = Process(
        std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "local_process_worker_child", "--nocapture"])
            .env("DURABLE_WORKFLOW_LOCAL_CHILD_QUEUE", &queue)
            .env("DURABLE_WORKFLOW_LOCAL_CHILD_READY", &ready)
            .env("DURABLE_WORKFLOW_LOCAL_CHILD_GRANT", &grant)
            .spawn()
            .unwrap(),
    );
    child_file(&mut child, &ready).await;
    let handle = client
        .start_workflow("tests.rust-local-crash", &queue, &queue, json!([]))
        .await
        .unwrap();
    child_file(&mut child, &grant).await;
    let original: Value = serde_json::from_slice(&std::fs::read(&grant).unwrap()).unwrap();
    assert_eq!(count(&history(&handle).await, "ActivityCompleted"), 0);
    child.0.kill().unwrap();
    assert_eq!(child.0.wait().unwrap().signal(), Some(9));
    let replacement_grant = Arc::new(std::sync::Mutex::new(None));
    let observed = replacement_grant.clone();
    let mut replacement = crash_worker(&client, &queue, &format!("{queue}-replacement"));
    replacement.register_activity("effect", move |ctx, _| {
        *observed.lock().unwrap() =
            Some(json!({"task_id":ctx.task_id, "attempt_id":ctx.activity_attempt_id}));
        async { Ok(json!("replacement committed")) }
    });
    replacement.register().await.unwrap();
    finish_on(&replacement, &handle).await;
    let successor = replacement_grant.lock().unwrap().clone().unwrap();
    assert_ne!(successor["attempt_id"], original["attempt_id"]);
    let snapshot = history(&handle).await;
    assert_eq!(count(&snapshot, "ActivityCompleted"), 1);
    assert_eq!(count(&snapshot, "WorkflowCompleted"), 1);
    let late = client.complete_workflow_task(
        original["task_id"].as_str().unwrap(), original["owner"].as_str().unwrap(), 1,
        vec![json!({"type":"complete_workflow", "result":durable_workflow::encode_payload(&json!("stale"), "avro").unwrap(), "payload_codec":"avro"})]
    ).await;
    assert!(
        matches!(late, Err(Error::Http { status, .. }) if status == reqwest::StatusCode::CONFLICT),
        "dead claim must not publish: {late:?}"
    );
    assert_eq!(
        history(&handle).await,
        snapshot,
        "stale publication must not append history"
    );
    assert_eq!(
        handle
            .result(WorkflowResultOptions::default())
            .await
            .unwrap(),
        json!("replacement committed")
    );
    println!("actual SIGKILL and natural lease reclaim passed: original={original}, successor={successor}, workflow={}", handle.workflow_id);
}

#[tokio::test]
#[ignore = "requires isolated published Server"]
async fn published_server_shutdown_drops_pending_local_callback_and_replacement_recovers() {
    let client = client();
    let queue = queue();
    let mut original = crash_worker(&client, &queue, &format!("{queue}-shutdown"));
    let entered = Arc::new(AtomicBool::new(false));
    let observed = entered.clone();
    let dropped = Arc::new(AtomicBool::new(false));
    let stopped = dropped.clone();
    original.register_activity("effect", move |_, _| {
        observed.store(true, Ordering::SeqCst);
        let guard = Dropped(stopped.clone());
        async move {
            let _guard = guard;
            std::future::pending::<durable_workflow::Result<Value>>().await
        }
    });
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
    let run = tokio::spawn(async move {
        original
            .run_until(async {
                let _ = stop_rx.await;
            })
            .await
    });
    let handle = client
        .start_workflow("tests.rust-local-crash", &queue, &queue, json!([]))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        while !entered.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    stop_tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(dropped.load(Ordering::SeqCst));
    assert_eq!(count(&history(&handle).await, "ActivityCompleted"), 0);
    let mut replacement = crash_worker(&client, &queue, &format!("{queue}-replacement"));
    replacement.register_activity("effect", |_, _| async {
        Ok(json!("recovered after shutdown"))
    });
    replacement.register().await.unwrap();
    finish_on(&replacement, &handle).await;
    assert_eq!(count(&history(&handle).await, "ActivityCompleted"), 1);
    println!(
        "shutdown dropped inline callback and replacement recovered: {}",
        handle.workflow_id
    );
}

#[tokio::test]
#[ignore = "requires isolated published Server and local-activity-ack-proxy.mjs"]
async fn published_server_committed_local_completion_survives_lost_acknowledgement() {
    let direct = client();
    let proxy_url = std::env::var("DURABLE_WORKFLOW_LOCAL_ACK_PROXY_URL")
        .expect("run the disposable qualification acknowledgement proxy");
    let proxy = Client::builder(&proxy_url)
        .token(Some(std::env::var("DURABLE_WORKFLOW_AUTH_TOKEN").unwrap()))
        .build()
        .unwrap();
    let queue = queue();
    let callbacks = Arc::new(AtomicUsize::new(0));
    let original = replay_worker(&proxy, &queue, callbacks.clone(), false);
    original.register().await.unwrap();
    let handle = direct
        .start_workflow("tests.rust-local-replay", &queue, &queue, json!([]))
        .await
        .unwrap();
    let result = original.run_once().await;
    assert!(
        result.is_err(),
        "the SDK must actually lose the successful completion acknowledgement"
    );
    let receipt = reqwest::get(format!("{proxy_url}/__qualification/lost-ack"))
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();
    assert_eq!(receipt["acknowledgement_dropped"], true);
    assert_eq!(receipt["upstream_status"], 200);
    let committed = history(&handle).await;
    assert_eq!(count(&committed, "ActivityCompleted"), 1);
    assert_eq!(callbacks.load(Ordering::SeqCst), 2);
    let cold = replay_worker(&direct, &queue, callbacks.clone(), true);
    cold.register().await.unwrap();
    handle.signal("finish", json!([])).await.unwrap();
    finish_on(&cold, &handle).await;
    assert_eq!(
        handle
            .result_avro_value(WorkflowResultOptions::default())
            .await
            .unwrap(),
        AvroValue::Array(vec![fidelity_value()])
    );
    assert_eq!(callbacks.load(Ordering::SeqCst), 2);
    assert_eq!(count(&history(&handle).await, "ActivityCompleted"), 1);
    println!("actual Server commit with lost SDK acknowledgement survived cold replay: {}, receipt={receipt}", handle.workflow_id);
}
