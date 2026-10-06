//! Real worker-session scenarios. Ordinary cargo tests leave these ignored.
//! Require one isolated disposable Server and retain its exact artifact identity.

use durable_workflow::{
    json, ActivityOptions, ActivityRetryPolicy, Client, Error, Value, Worker, WorkerSessionOptions,
    WorkflowHandle, WorkflowResultOptions,
};
use std::{
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

fn client() -> Client {
    assert_eq!(
        std::env::var("DURABLE_WORKFLOW_SESSION_ISOLATED").as_deref(),
        Ok("1")
    );
    Client::builder(std::env::var("DURABLE_WORKFLOW_SERVER_URL").unwrap())
        .token(Some(std::env::var("DURABLE_WORKFLOW_AUTH_TOKEN").unwrap()))
        .build()
        .unwrap()
}

fn identity() -> String {
    format!("rust-session-{}", durable_workflow::Uuid::new_v4())
}
fn options(queue: &str) -> WorkerSessionOptions {
    WorkerSessionOptions::new(queue)
        .queue(queue)
        .requirements(["cache:local"])
        .lease_seconds(3)
        .ttl_seconds(45)
}
fn worker(client: &Client, queue: &str, holder: &str) -> Worker {
    Worker::new(client.clone(), queue)
        .worker_id(format!("{queue}-{holder}"))
        .worker_sessions(true)
        .capabilities(["cache:local"])
        .poll_timeout(Duration::from_millis(10))
}
fn refusal(error: Error, expected: &str) {
    match error {
        Error::Http { status, body } => {
            assert_eq!(status, reqwest::StatusCode::CONFLICT, "{body}");
            assert_eq!(
                serde_json::from_str::<Value>(&body).unwrap()["reason"],
                expected
            );
        }
        other => panic!("expected durable Server refusal {expected}: {other:?}"),
    }
}
async fn stop(worker: &Worker) {
    worker.run_until(async {}).await.unwrap();
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
fn count(history: &Value, kind: &str) -> usize {
    history["events"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|event| event["event_type"] == kind)
        .count()
}
async fn finish(worker: &Worker, handle: &WorkflowHandle) {
    tokio::time::timeout(Duration::from_secs(35), async {
        while !handle.describe().await.unwrap().is_terminal() {
            worker.run_once().await.unwrap();
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("real lease and recovery must converge within the bounded scenario");
}

#[tokio::test]
#[ignore = "requires isolated Server"]
async fn server_session_lifecycle_requirements_capacity_and_holder_fences() {
    let client = client();
    let queue = identity();
    let first = worker(&client, &queue, "first").max_concurrent_worker_sessions(1);
    let other = worker(&client, &queue, "other");
    first.register().await.unwrap();
    other.register().await.unwrap();
    let session = first.worker_session(options(&queue)).unwrap();
    let created = session.create().await.unwrap();
    assert_eq!(created["outcome"], "created");
    let original_ttl = created["session"]["ttl_expires_at"].clone();
    assert_eq!(session.create().await.unwrap()["outcome"], "reused");
    assert_eq!(
        session.renew().await.unwrap()["session"]["ttl_expires_at"],
        original_ttl
    );
    refusal(
        other
            .worker_session(options(&queue))
            .unwrap()
            .create()
            .await
            .unwrap_err(),
        "session_owned_by_another_worker",
    );
    refusal(
        client
            .create_worker_session(
                &format!("{queue}-first"),
                &WorkerSessionOptions::new(format!("{queue}-over-capacity")).queue(&queue),
            )
            .await
            .unwrap_err(),
        "worker_session_limit_exceeded",
    );
    refusal(
        client
            .create_worker_session(
                &format!("{queue}-other"),
                &WorkerSessionOptions::new(format!("{queue}-missing-resource"))
                    .queue(&queue)
                    .requirements(["gpu:absent"]),
            )
            .await
            .unwrap_err(),
        "session_requirements_not_met",
    );
    refusal(
        client
            .renew_worker_session(&format!("{queue}-other"), &queue, 3)
            .await
            .unwrap_err(),
        "session_owner_mismatch",
    );
    let closed = session.close("scenario_completed").await.unwrap();
    assert_eq!(closed["outcome"], "closed");
    assert_eq!(session.close("duplicate").await.unwrap(), closed);
    refusal(
        client
            .create_worker_session(&format!("{queue}-other"), &options(&queue))
            .await
            .unwrap_err(),
        "session_closed",
    );
    stop(&first).await;
    stop(&other).await;
    println!("session lifecycle, requirements, capacity, holder and close fences passed: {queue}");
}

#[tokio::test]
#[ignore = "requires isolated Server and real elapsed time"]
async fn server_session_lease_reacquisition_preserves_original_ttl() {
    let client = client();
    let queue = identity();
    let first = worker(&client, &queue, "first");
    let replacement = worker(&client, &queue, "replacement");
    first.register().await.unwrap();
    replacement.register().await.unwrap();
    let options = options(&queue).lease_seconds(3).ttl_seconds(8);
    let original = first
        .worker_session(options.clone())
        .unwrap()
        .create()
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(3250)).await;
    let successor = replacement.worker_session(options.clone()).unwrap();
    let admitted = successor.create().await.unwrap();
    assert_eq!(admitted["outcome"], "reacquired");
    assert_eq!(
        admitted["session"]["ttl_expires_at"],
        original["session"]["ttl_expires_at"]
    );
    refusal(
        client
            .renew_worker_session(&format!("{queue}-first"), &queue, 3)
            .await
            .unwrap_err(),
        "session_owner_mismatch",
    );
    tokio::time::sleep(Duration::from_millis(5000)).await;
    refusal(
        successor.create().await.unwrap_err(),
        "session_reacquire_disallowed",
    );
    assert!(!successor.active());
    client
        .deregister_worker_registration(&format!("{queue}-first"))
        .await
        .unwrap();
    stop(&replacement).await;
    println!("real lease reacquisition and original TTL expiry passed: {queue}");
}

fn workflow_worker(client: &Client, queue: &str, holder: &str) -> Worker {
    let mut worker = worker(client, queue, holder);
    let options = options(queue);
    worker.register_workflow("tests.rust-session-replay", move |ctx, _| {
        let options = options.clone();
        async move {
            let value: i64 = ctx
                .activity_with_options(
                    "session-echo",
                    ActivityOptions::new()
                        .retry_policy(
                            ActivityRetryPolicy::new(2).backoff_intervals([Duration::ZERO]),
                        )
                        .start_to_close_timeout(Duration::from_secs(10)),
                    json!([42]),
                )
                .in_worker_session(options)
                .typed()
                .await?;
            ctx.wait_signal("finish").await?;
            Ok(json!(value))
        }
    });
    worker
}

#[tokio::test]
#[ignore = "requires isolated Server with immutable session history"]
async fn server_session_auto_creation_and_cold_replay_are_read_only() {
    let client = client();
    let queue = identity();
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let mut first = workflow_worker(&client, &queue, "first");
    first.register_activity("session-echo", move |ctx, args| {
        observed.fetch_add(1, Ordering::SeqCst);
        async move {
            assert!(ctx.worker_session().unwrap().active());
            ctx.heartbeat(json!({"resource":"rebuilt in this process"}))
                .await?;
            Ok(args[0].clone())
        }
    });
    first.register().await.unwrap();
    let handle = client
        .start_workflow("tests.rust-session-replay", &queue, &queue, json!([]))
        .await
        .unwrap();
    for _ in 0..10 {
        first.run_once().await.unwrap();
        if count(&history(&handle).await, "ActivityCompleted") == 1 {
            break;
        }
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let recorded = history(&handle).await;
    let scheduled = recorded["events"]
        .as_array()
        .unwrap()
        .iter()
        .find(|event| event["event_type"] == "ActivityScheduled")
        .unwrap();
    assert_eq!(
        scheduled["payload"]["activity"]["worker_session"]["session_id"],
        queue
    );
    let mut replacement = workflow_worker(&client, &queue, "replacement");
    replacement.register_activity("session-echo", |_, _| async {
        panic!("committed activity must not run during cold replay")
    });
    replacement.register().await.unwrap();
    handle.signal("finish", json!([])).await.unwrap();
    finish(&replacement, &handle).await;
    assert_eq!(
        handle
            .result(WorkflowResultOptions::default())
            .await
            .unwrap(),
        json!(42)
    );
    assert_eq!(count(&history(&handle).await, "ActivityCompleted"), 1);
    assert!(!replacement
        .worker_session(options(&queue))
        .unwrap()
        .active());
    stop(&first).await;
    stop(&replacement).await;
    println!("auto creation, activity heartbeat and read-only cold replay passed: {queue}");
}

#[tokio::test]
#[ignore = "requires isolated Server"]
async fn server_session_shutdown_closes_explicit_and_auto_created_handles() {
    let client = client();
    let queue = identity();
    let first = worker(&client, &queue, "first");
    first.register().await.unwrap();
    let session = first.worker_session(options(&queue)).unwrap();
    session.create().await.unwrap();
    stop(&first).await;
    assert!(!session.active());
    assert_eq!(
        session.close("duplicate").await.unwrap()["outcome"],
        "closed"
    );
    let replacement = worker(&client, &queue, "replacement");
    replacement.register().await.unwrap();
    refusal(
        replacement
            .worker_session(options(&queue))
            .unwrap()
            .create()
            .await
            .unwrap_err(),
        "session_closed",
    );
    stop(&replacement).await;
    println!("graceful holder shutdown and terminal close passed: {queue}");
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
    tokio::time::timeout(Duration::from_secs(15), async {
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
    .expect("actual worker must reach its session activity");
}

#[tokio::test]
async fn session_process_worker_child() {
    let Ok(queue) = std::env::var("DURABLE_WORKFLOW_SESSION_CHILD_QUEUE") else {
        return;
    };
    let ready = std::env::var("DURABLE_WORKFLOW_SESSION_CHILD_READY").unwrap();
    let grant = std::env::var("DURABLE_WORKFLOW_SESSION_CHILD_GRANT").unwrap();
    let mut worker = workflow_worker(&client(), &queue, "killed");
    worker.register_activity("session-echo",move |ctx,_| {
        let grant = grant.clone();
        async move {
            let pending = format!("{grant}.pending");
            std::fs::write(&pending,serde_json::to_vec(&json!({"task_id":ctx.task_id,
                "owner":ctx.lease_owner,"attempt_id":ctx.activity_attempt_id,"worker_id":ctx.worker_id,
                "resource_generation":"killed-process-memory","session":ctx.worker_session().unwrap().snapshot()?})).unwrap()).unwrap();
            std::fs::rename(pending,grant).unwrap();
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
#[ignore = "requires isolated Server, actual SIGKILL and real attempt recovery"]
async fn server_session_sigkill_rebuilds_resources_and_fences_the_stale_attempt() {
    use std::os::unix::process::ExitStatusExt;
    let client = client();
    let queue = identity();
    let scratch = Scratch(std::env::temp_dir().join(format!("{queue}-holder")));
    std::fs::create_dir(&scratch.0).unwrap();
    let ready = scratch.0.join("ready");
    let grant = scratch.0.join("grant.json");
    let mut child = Process(
        std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "session_process_worker_child", "--nocapture"])
            .env("DURABLE_WORKFLOW_SESSION_CHILD_QUEUE", &queue)
            .env("DURABLE_WORKFLOW_SESSION_CHILD_READY", &ready)
            .env("DURABLE_WORKFLOW_SESSION_CHILD_GRANT", &grant)
            .spawn()
            .unwrap(),
    );
    child_file(&mut child, &ready).await;
    let handle = client
        .start_workflow("tests.rust-session-replay", &queue, &queue, json!([]))
        .await
        .unwrap();
    child_file(&mut child, &grant).await;
    let original: Value = serde_json::from_slice(&std::fs::read(&grant).unwrap()).unwrap();
    assert_eq!(count(&history(&handle).await, "ActivityCompleted"), 0);
    child.0.kill().unwrap();
    assert_eq!(child.0.wait().unwrap().signal(), Some(9));
    let resource = Arc::new(Mutex::new(None));
    let observed = resource.clone();
    let mut replacement = workflow_worker(&client, &queue, "replacement");
    replacement.register_activity("session-echo", move |ctx, args| {
        let session = ctx.worker_session().unwrap().snapshot().unwrap().unwrap();
        let generation = json!({"resource_generation":"replacement-process-memory",
            "attempt_id":ctx.activity_attempt_id,"session":session});
        assert!(
            observed.lock().unwrap().replace(generation).is_none(),
            "replacement must build its local resource once"
        );
        async move { Ok(args[0].clone()) }
    });
    replacement.register().await.unwrap();
    tokio::time::sleep(Duration::from_millis(3250)).await;
    let session = replacement.worker_session(options(&queue)).unwrap();
    let reacquired = session.create().await.unwrap();
    assert_eq!(reacquired["outcome"], "reacquired");
    assert_eq!(
        reacquired["session"]["ttl_expires_at"],
        original["session"]["ttl_expires_at"]
    );
    handle.signal("finish", json!([])).await.unwrap();
    finish(&replacement, &handle).await;
    assert_eq!(
        handle
            .result(WorkflowResultOptions::default())
            .await
            .unwrap(),
        json!(42)
    );
    let rebuilt = resource.lock().unwrap().clone().unwrap();
    assert_ne!(
        rebuilt["resource_generation"],
        original["resource_generation"]
    );
    assert_ne!(rebuilt["attempt_id"], original["attempt_id"]);
    assert_eq!(
        rebuilt["session"]["session_id"],
        original["session"]["session_id"]
    );
    assert_eq!(
        rebuilt["session"]["ttl_expires_at"],
        original["session"]["ttl_expires_at"]
    );
    let stale = client
        .complete_activity_task(
            original["task_id"].as_str().unwrap(),
            original["attempt_id"].as_str().unwrap(),
            original["owner"].as_str().unwrap(),
            json!("stale"),
            "avro",
        )
        .await
        .unwrap_err();
    assert!(
        matches!(stale,Error::ActivityTaskRejected(ref rejection) if rejection.status == 409),
        "stale receipt must be fenced: {stale:?}"
    );
    let recorded = history(&handle).await;
    assert_eq!(count(&recorded, "ActivityCompleted"), 1);
    assert_eq!(count(&recorded, "WorkflowCompleted"), 1);
    assert_eq!(count(&recorded, "ActivityHeartbeatRecorded"), 0);
    stop(&replacement).await;
    client
        .deregister_worker_registration(original["worker_id"].as_str().unwrap())
        .await
        .unwrap();
    println!("actual SIGKILL, real attempt recovery, resource rebuilding, original TTL and stale-attempt fence passed: {queue}");
}

#[tokio::test]
#[ignore = "requires isolated Server"]
async fn server_session_activity_capacity_bounds_parallel_callbacks() {
    let client = client();
    let queue = identity();
    let options = options(&queue).max_concurrent_activities(1);
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let mut worker = worker(&client, &queue, "parallel").max_concurrent_activity_tasks(2);
    worker.register_workflow("tests.rust-session-parallel", move |ctx, _| {
        let options = options.clone();
        async move {
            let results = ctx
                .parallel(vec![
                    durable_workflow::ParallelOperation::activity("session-echo", json!([1])),
                    durable_workflow::ParallelOperation::activity("session-echo", json!([2])),
                ])
                .in_worker_session(options)
                .await?;
            let values: Vec<_> = results
                .into_iter()
                .map(|result| match result {
                    durable_workflow::ParallelResult::Activity(value) => value,
                    other => panic!("expected activity result: {other:?}"),
                })
                .collect();
            Ok(json!(values))
        }
    });
    let observed_active = active.clone();
    let observed_peak = peak.clone();
    worker.register_activity("session-echo", move |ctx, args| {
        let active = observed_active.clone();
        let peak = observed_peak.clone();
        async move {
            assert!(ctx.worker_session().unwrap().active());
            peak.fetch_max(active.fetch_add(1, Ordering::SeqCst) + 1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(300)).await;
            active.fetch_sub(1, Ordering::SeqCst);
            Ok(args[0].clone())
        }
    });
    let handle = client
        .start_workflow("tests.rust-session-parallel", &queue, &queue, json!([]))
        .await
        .unwrap();
    let watch = handle.clone();
    tokio::time::timeout(
        Duration::from_secs(20),
        worker.run_until(async move {
            while !watch.describe().await.unwrap().is_terminal() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(peak.load(Ordering::SeqCst), 1);
    assert_eq!(active.load(Ordering::SeqCst), 0);
    assert_eq!(
        handle
            .result(WorkflowResultOptions::default())
            .await
            .unwrap(),
        json!([1, 2])
    );
    assert_eq!(count(&history(&handle).await, "ActivityCompleted"), 2);
    println!("two poll lanes obey one session activity slot and converge without backlog: {queue}");
}
