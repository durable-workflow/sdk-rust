//! Opt-in cache qualification against one isolated published Server.
//! Ordinary cargo tests leave connected cases ignored.

use durable_workflow::{
    json, Client, ConditionWaitOptions, CooperativeCancellationOptions, StickyCacheOptions, Value,
    Worker, WorkflowHandle, WorkflowResultOptions,
};
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    thread,
    time::Duration,
};

const WORKFLOW: &str = "tests.rust-sticky-history";

fn client_at(url: String) -> Client {
    assert_eq!(
        std::env::var("DURABLE_WORKFLOW_STICKY_ISOLATED").as_deref(),
        Ok("1")
    );
    Client::builder(url)
        .token(Some(std::env::var("DURABLE_WORKFLOW_AUTH_TOKEN").unwrap()))
        .build()
        .unwrap()
}
fn client() -> Client {
    client_at(std::env::var("DURABLE_WORKFLOW_SERVER_URL").unwrap())
}
fn queue() -> String {
    format!("rust-sticky-{}", durable_workflow::Uuid::new_v4())
}

fn worker(
    client: &Client,
    queue: &str,
    id: &str,
    options: StickyCacheOptions,
    callbacks: Arc<AtomicUsize>,
    build: Option<&str>,
) -> Worker {
    let mut worker = Worker::new(client.clone(), queue)
        .worker_id(id)
        .poll_timeout(Duration::from_millis(10))
        .sticky_cache(options)
        .unwrap();
    if let Some(build) = build {
        worker = worker.build_id(build);
    }
    worker.register_workflow(WORKFLOW, move |ctx, input| {
        let callbacks = callbacks.clone();
        async move {
            let batch = input[0].as_u64().unwrap();
            let mut total = 0_u64;
            for phase in 1..=3_u64 {
                if phase <= 2 {
                    for value in 0..batch {
                        let callbacks = callbacks.clone();
                        total += ctx.side_effect(move || {
                            callbacks.fetch_add(1, Ordering::SeqCst);
                            value
                        })?;
                    }
                }
                ctx.upsert_memo(json!({"sticky_phase": phase}))?;
                let predicate_ctx = ctx.clone();
                ctx.wait_condition(
                    ConditionWaitOptions::new(
                        format!("sticky-phase-{phase}"),
                        format!("sticky-phase-{phase}-v1"),
                    )
                    .timeout(Duration::from_secs(300)),
                    move || {
                        let signals = predicate_ctx.signals("advance")?;
                        Ok(signals.iter().any(|args| {
                            args.first()
                                .and_then(Value::as_u64)
                                .is_some_and(|n| n >= phase)
                        }))
                    },
                )
                .await?;
            }
            Ok(json!({"total": total, "stage": 3}))
        }
    });
    worker
        .declare_workflow_signals(WORKFLOW, &["advance"])
        .unwrap();
    worker
}
async fn start(client: &Client, queue: &str, batch: u64) -> WorkflowHandle {
    client
        .start_workflow(
            WORKFLOW,
            queue,
            &format!("{queue}-{}", durable_workflow::Uuid::new_v4()),
            json!([batch]),
        )
        .await
        .unwrap()
}
async fn phase(worker: &Worker, handle: &WorkflowHandle, phase: u64) {
    tokio::time::timeout(Duration::from_secs(35), async {
        loop {
            let description = handle.describe().await.unwrap();
            if description.status.as_deref() == Some("waiting")
                && description.raw.get("memo") == Some(&json!({"sticky_phase":phase}))
            {
                break;
            }
            assert!(
                !description.is_terminal(),
                "unexpected terminal result: {description:?}"
            );
            worker.run_once().await.unwrap();
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("workflow must reach the intended durable wait");
}
async fn finish(worker: &Worker, handle: &WorkflowHandle, from: u64, batch: u64) {
    for current in from..=3 {
        handle.signal("advance", json!([current])).await.unwrap();
        if current < 3 {
            phase(worker, handle, current + 1).await;
        }
    }
    tokio::time::timeout(Duration::from_secs(35), async {
        while !handle.describe().await.unwrap().is_terminal() {
            worker.run_once().await.unwrap();
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        handle
            .result(WorkflowResultOptions::default())
            .await
            .unwrap(),
        json!({"total":batch * (batch - 1),"stage":3})
    );
}
async fn stop(worker: &Worker) {
    worker.run_until(async {}).await.unwrap();
    assert_eq!(worker.sticky_cache_metrics().unwrap().entries, 0);
}

// Forward actual HTTP unchanged except for connection close. Count real
// lease-bound history requests instead of inferring skipped pages from hits.
struct HistoryProxy {
    url: String,
    stop: Arc<AtomicBool>,
    requests: Arc<Mutex<Vec<(String, Value)>>>,
    thread: Option<thread::JoinHandle<()>>,
}
impl HistoryProxy {
    fn start() -> Self {
        let target = std::env::var("DURABLE_WORKFLOW_SERVER_URL")
            .unwrap()
            .strip_prefix("http://")
            .expect("isolated HTTP fixture")
            .trim_end_matches('/')
            .to_owned();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let stop = Arc::new(AtomicBool::new(false));
        let stopped = stop.clone();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured = requests.clone();
        let thread = thread::spawn(move || {
            while !stopped.load(Ordering::SeqCst) {
                let (mut incoming, _) = match listener.accept() {
                    Ok(pair) => pair,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2));
                        continue;
                    }
                    Err(error) => panic!("proxy accept: {error}"),
                };
                incoming
                    .set_read_timeout(Some(Duration::from_secs(10)))
                    .unwrap();
                let mut bytes = Vec::new();
                let end = loop {
                    let mut buffer = [0; 8192];
                    let read = incoming.read(&mut buffer).unwrap();
                    if read == 0 {
                        break None;
                    }
                    bytes.extend_from_slice(&buffer[..read]);
                    if let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                        let header = String::from_utf8_lossy(&bytes[..end]);
                        let length = header
                            .lines()
                            .find_map(|line| {
                                let (key, value) = line.split_once(':')?;
                                key.eq_ignore_ascii_case("content-length")
                                    .then(|| value.trim().parse::<usize>().unwrap())
                            })
                            .unwrap_or(0);
                        if bytes.len() >= end + 4 + length {
                            break Some(end);
                        }
                    }
                };
                let Some(end) = end else {
                    continue;
                };
                let header = String::from_utf8_lossy(&bytes[..end]);
                let path = header
                    .lines()
                    .next()
                    .unwrap()
                    .split_whitespace()
                    .nth(1)
                    .unwrap()
                    .to_owned();
                captured.lock().unwrap().push((
                    path,
                    serde_json::from_slice(&bytes[end + 4..]).unwrap_or(Value::Null),
                ));
                let headers = header
                    .lines()
                    .filter(|line| !line.to_ascii_lowercase().starts_with("connection:"))
                    .collect::<Vec<_>>()
                    .join("\r\n");
                let mut outgoing = TcpStream::connect(&target).unwrap();
                outgoing
                    .set_read_timeout(Some(Duration::from_secs(30)))
                    .unwrap();
                write!(outgoing, "{headers}\r\nConnection: close\r\n\r\n").unwrap();
                outgoing.write_all(&bytes[end + 4..]).unwrap();
                let mut response = Vec::new();
                outgoing.read_to_end(&mut response).unwrap();
                let _ = incoming.write_all(&response);
            }
        });
        Self {
            url,
            stop,
            requests,
            thread: Some(thread),
        }
    }
    fn pages(&self) -> Vec<Value> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|(path, _)| {
                path.starts_with("/api/worker/workflow-tasks/") && path.ends_with("/history")
            })
            .map(|(_, body)| body.clone())
            .collect()
    }
}
impl Drop for HistoryProxy {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect(self.url.trim_start_matches("http://"));
        self.thread.take().unwrap().join().unwrap();
    }
}

#[tokio::test]
#[ignore = "requires isolated published Server"]
async fn warm_paged_replay_skips_retained_middle_and_preserves_recorded_values() {
    let proxy = HistoryProxy::start();
    let client = client_at(proxy.url.clone());
    let queue = queue();
    let callbacks = Arc::new(AtomicUsize::new(0));
    let worker = worker(
        &client,
        &queue,
        &queue,
        StickyCacheOptions::new(2),
        callbacks.clone(),
        None,
    );
    worker.register().await.unwrap();
    let handle = start(&client, &queue, 600).await;
    phase(&worker, &handle, 1).await;
    handle.signal("advance", json!([1])).await.unwrap();
    phase(&worker, &handle, 2).await;
    handle.signal("advance", json!([2])).await.unwrap();
    phase(&worker, &handle, 3).await;
    let pages = proxy.pages();
    let retained_cursor = pages.last().unwrap()["next_history_page_token"].clone();
    let before = pages.len();
    finish(&worker, &handle, 3, 600).await;
    let pages = proxy.pages();
    assert_eq!(
        pages.len() - before,
        1,
        "one current-lease tail fetch must suffice: {pages:?}"
    );
    assert_eq!(
        pages.last().unwrap()["next_history_page_token"],
        retained_cursor
    );
    assert_eq!(callbacks.load(Ordering::SeqCst), 1200);
    let metrics = worker.sticky_cache_metrics().unwrap();
    assert!(metrics.hit >= 3, "{metrics:?}");
    assert_eq!(metrics.entries, 0);
    println!("paged warm replay: {metrics:?}, final tail requests=1");
    stop(&worker).await;
}

async fn bounded(max_bytes: usize) {
    let client = client();
    let queue = queue();
    let callbacks = Arc::new(AtomicUsize::new(0));
    let worker = worker(
        &client,
        &queue,
        &queue,
        StickyCacheOptions::new(1).max_history_bytes(max_bytes),
        callbacks.clone(),
        None,
    );
    worker.register().await.unwrap();
    let first = start(&client, &queue, 2).await;
    phase(&worker, &first, 1).await;
    let second = start(&client, &queue, 2).await;
    phase(&worker, &second, 1).await;
    let metrics = worker.sticky_cache_metrics().unwrap();
    assert!(
        metrics.entries <= 1 && metrics.history_bytes <= max_bytes,
        "{metrics:?}"
    );
    if max_bytes > 1 {
        assert!(metrics.eviction >= 1, "{metrics:?}");
    }
    finish(&worker, &first, 1, 2).await;
    assert!(worker.sticky_cache_metrics().unwrap().miss >= 3);
    if max_bytes > 1 {
        assert!(worker.sticky_cache_metrics().unwrap().forced_cold_replay >= 1);
    }
    assert_eq!(callbacks.load(Ordering::SeqCst), 6);
    second
        .cancel(durable_workflow::WorkflowCommandOptions::default())
        .await
        .unwrap();
    stop(&worker).await;
}
#[tokio::test]
#[ignore = "requires isolated published Server"]
async fn entry_eviction_replays_cold_without_repeating_effects() {
    bounded(16 * 1024 * 1024).await;
}
#[tokio::test]
#[ignore = "requires isolated published Server"]
async fn oversized_history_is_not_retained() {
    bounded(1).await;
}

#[tokio::test]
#[ignore = "requires isolated published Server"]
async fn real_ttl_expiry_preserves_original_run() {
    let client = client();
    let queue = queue();
    let callbacks = Arc::new(AtomicUsize::new(0));
    let worker = worker(
        &client,
        &queue,
        &queue,
        StickyCacheOptions::new(2).ttl(Duration::from_secs(1)),
        callbacks.clone(),
        None,
    );
    worker.register().await.unwrap();
    let handle = start(&client, &queue, 2).await;
    let run = handle.run_id.clone();
    phase(&worker, &handle, 1).await;
    assert_eq!(worker.sticky_cache_metrics().unwrap().entries, 1);
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert_eq!(worker.sticky_cache_metrics().unwrap().entries, 0);
    finish(&worker, &handle, 1, 2).await;
    assert!(worker.sticky_cache_metrics().unwrap().miss >= 2);
    assert_eq!(handle.describe().await.unwrap().run_id, run);
    assert_eq!(callbacks.load(Ordering::SeqCst), 4);
    stop(&worker).await;
}

async fn replacement(build_mismatch: bool) {
    let client = client();
    let queue = queue();
    let callbacks = Arc::new(AtomicUsize::new(0));
    let build = build_mismatch.then_some("build-before");
    let original = worker(
        &client,
        &queue,
        &queue,
        StickyCacheOptions::new(2),
        callbacks.clone(),
        build,
    );
    original.register().await.unwrap();
    let handle = start(&client, &queue, 2).await;
    let run = handle.run_id.clone();
    phase(&original, &handle, 1).await;
    assert_eq!(original.sticky_cache_metrics().unwrap().entries, 1);
    stop(&original).await;
    if build_mismatch {
        let incompatible = worker(
            &client,
            &queue,
            &format!("{queue}-bad"),
            StickyCacheOptions::new(2),
            callbacks.clone(),
            Some("build-after"),
        );
        incompatible.register().await.unwrap();
        handle.signal("advance", json!([0])).await.unwrap();
        assert_eq!(incompatible.run_once().await.unwrap(), 0);
        assert_eq!(incompatible.sticky_cache_metrics().unwrap().miss, 0);
        assert_eq!(
            handle.describe().await.unwrap().raw["memo"],
            json!({"sticky_phase":1})
        );
        stop(&incompatible).await;
    }
    let replacement = worker(
        &client,
        &queue,
        &format!("{queue}-replacement"),
        StickyCacheOptions::new(2),
        callbacks.clone(),
        build,
    );
    replacement.register().await.unwrap();
    assert_eq!(replacement.sticky_cache_metrics().unwrap().entries, 0);
    finish(&replacement, &handle, 1, 2).await;
    assert!(replacement.sticky_cache_metrics().unwrap().miss >= 1);
    assert_eq!(handle.describe().await.unwrap().run_id, run);
    assert_eq!(callbacks.load(Ordering::SeqCst), 4);
    stop(&replacement).await;
}
#[tokio::test]
#[ignore = "requires isolated published Server"]
async fn graceful_replacement_replays_original_run() {
    replacement(false).await;
}
#[tokio::test]
#[ignore = "requires isolated published Server"]
async fn changed_build_is_refused_and_matching_build_recovers() {
    replacement(true).await;
}

#[tokio::test]
#[ignore = "requires isolated published Server"]
async fn cached_cancellation_keeps_canonical_delivery_and_resumes_cold_cleanup() {
    let client = client();
    let queue = queue();
    let configure = |id: &str| {
        let mut worker = Worker::new(client.clone(), &queue)
            .worker_id(id)
            .poll_timeout(Duration::from_millis(10))
            .cooperative_cancellation(true)
            .sticky_cache(StickyCacheOptions::new(2))
            .unwrap();
        worker.register_workflow("tests.sticky-cleanup", |ctx, _| async move {
            let mut saga = ctx.saga();
            saga.add_compensation("sticky-undo", json!([]))?;
            let result = ctx.sleep(Duration::from_secs(300)).await;
            saga.finish(result).await?;
            Ok(Value::Null)
        });
        worker
    };
    let original = configure(&queue);
    original.register().await.unwrap();
    let handle = client
        .start_workflow("tests.sticky-cleanup", &queue, &queue, json!([]))
        .await
        .unwrap();
    let run = handle.run_id.clone();
    while handle.describe().await.unwrap().status.as_deref() != Some("waiting") {
        original.run_once().await.unwrap();
    }
    assert_eq!(original.sticky_cache_metrics().unwrap().entries, 1);
    let request = handle
        .request_cancellation(CooperativeCancellationOptions {
            cleanup_timeout_seconds: Some(30),
            reason: Some("cache qualification".into()),
        })
        .await
        .unwrap();
    let duplicate = handle
        .request_cancellation(CooperativeCancellationOptions {
            cleanup_timeout_seconds: Some(60),
            reason: Some("duplicate".into()),
        })
        .await
        .unwrap();
    assert!(duplicate.duplicate);
    assert_eq!(request.cancellation_request, duplicate.cancellation_request);
    original.run_once().await.unwrap();
    assert!(original.sticky_cache_metrics().unwrap().hit >= 1);
    stop(&original).await;
    let calls = Arc::new(AtomicUsize::new(0));
    let mut replacement = configure(&format!("{queue}-replacement"));
    let observed = calls.clone();
    replacement.register_activity("sticky-undo", move |_, _| {
        let observed = observed.clone();
        async move {
            observed.fetch_add(1, Ordering::SeqCst);
            Ok(Value::Null)
        }
    });
    replacement.register().await.unwrap();
    tokio::time::timeout(Duration::from_secs(25), async {
        while !handle.describe().await.unwrap().is_terminal() {
            replacement.run_once().await.unwrap();
        }
    })
    .await
    .unwrap();
    let description = handle.describe().await.unwrap();
    assert_eq!(description.status.as_deref(), Some("cancelled"));
    assert_eq!(description.run_id, run);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(replacement.sticky_cache_metrics().unwrap().miss >= 1);
    assert_eq!(replacement.sticky_cache_metrics().unwrap().entries, 0);
    let history = reqwest::Client::new()
        .get(format!(
            "{}/api/workflows/{}/runs/{}/history",
            std::env::var("DURABLE_WORKFLOW_SERVER_URL").unwrap(),
            handle.workflow_id,
            run.unwrap()
        ))
        .bearer_auth(std::env::var("DURABLE_WORKFLOW_AUTH_TOKEN").unwrap())
        .header("X-Namespace", "default")
        .header("X-Durable-Workflow-Control-Plane-Version", "2")
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();
    let events = history["events"].as_array().unwrap();
    for kind in [
        "CooperativeCancellationRequested",
        "CooperativeCancellationDelivered",
        "WorkflowCancelled",
        "ActivityCompleted",
    ] {
        assert_eq!(
            events
                .iter()
                .filter(|event| event["event_type"] == kind)
                .count(),
            1,
            "{kind}: {history}"
        );
    }
    let delivery = events
        .iter()
        .find(|event| event["event_type"] == "CooperativeCancellationDelivered")
        .unwrap();
    assert_eq!(
        delivery["payload"]["workflow_command_id"],
        request.cancellation_request.request_id
    );
    stop(&replacement).await;
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
    tokio::time::timeout(Duration::from_secs(20), async {
        while !path.is_file() {
            assert!(
                child.0.try_wait().unwrap().is_none(),
                "worker exited before {}",
                path.display()
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn sticky_process_worker_child() {
    let Ok(queue) = std::env::var("STICKY_CHILD_QUEUE") else {
        return;
    };
    let ready = std::env::var("STICKY_CHILD_READY").unwrap();
    let retained = std::env::var("STICKY_CHILD_RETAINED").unwrap();
    let worker = worker(
        &client(),
        &queue,
        &queue,
        StickyCacheOptions::new(2),
        Arc::new(AtomicUsize::new(0)),
        None,
    );
    worker.register().await.unwrap();
    std::fs::write(ready, b"registered").unwrap();
    loop {
        worker.run_once().await.unwrap();
        let metrics = worker.sticky_cache_metrics().unwrap();
        if metrics.hit >= 1 && metrics.entries == 1 && !Path::new(&retained).exists() {
            std::fs::write(&retained, serde_json::to_vec(&metrics).unwrap()).unwrap();
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
#[cfg(unix)]
#[tokio::test]
#[ignore = "requires isolated published Server and actual SIGKILL"]
async fn sigkill_same_identity_replacement_replays_cold_and_completes_once() {
    use std::os::unix::process::ExitStatusExt;
    let client = client();
    let queue = queue();
    let scratch = Scratch(std::env::temp_dir().join(&queue));
    std::fs::create_dir(&scratch.0).unwrap();
    let ready = scratch.0.join("ready");
    let retained = scratch.0.join("retained");
    let mut process = Process(
        std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "sticky_process_worker_child", "--nocapture"])
            .env("STICKY_CHILD_QUEUE", &queue)
            .env("STICKY_CHILD_READY", &ready)
            .env("STICKY_CHILD_RETAINED", &retained)
            .spawn()
            .unwrap(),
    );
    child_file(&mut process, &ready).await;
    let handle = start(&client, &queue, 2).await;
    let run = handle.run_id.clone();
    // Only the child processes tasks before its actual death.
    tokio::time::timeout(Duration::from_secs(20), async {
        while handle.describe().await.unwrap().raw.get("memo") != Some(&json!({"sticky_phase":1})) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    handle.signal("advance", json!([1])).await.unwrap();
    child_file(&mut process, &retained).await;
    tokio::time::timeout(Duration::from_secs(20), async {
        while handle.describe().await.unwrap().raw.get("memo") != Some(&json!({"sticky_phase":2})) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let metrics: Value = serde_json::from_slice(&std::fs::read(&retained).unwrap()).unwrap();
    assert!(metrics["hit"].as_u64().unwrap() >= 1);
    assert_eq!(metrics["entries"], 1);
    process.0.kill().unwrap();
    assert_eq!(process.0.wait().unwrap().signal(), Some(9));
    handle.signal("advance", json!([2])).await.unwrap();
    let callbacks = Arc::new(AtomicUsize::new(0));
    let replacement = worker(
        &client,
        &queue,
        &queue,
        StickyCacheOptions::new(2),
        callbacks.clone(),
        None,
    );
    replacement.register().await.unwrap();
    assert_eq!(replacement.sticky_cache_metrics().unwrap().entries, 0);
    phase(&replacement, &handle, 3).await;
    finish(&replacement, &handle, 3, 2).await;
    assert!(replacement.sticky_cache_metrics().unwrap().miss >= 1);
    assert_eq!(
        callbacks.load(Ordering::SeqCst),
        0,
        "committed effects must not repeat after SIGKILL"
    );
    assert_eq!(handle.describe().await.unwrap().run_id, run);
    let history = reqwest::Client::new()
        .get(format!(
            "{}/api/workflows/{}/runs/{}/history",
            std::env::var("DURABLE_WORKFLOW_SERVER_URL").unwrap(),
            handle.workflow_id,
            run.unwrap()
        ))
        .bearer_auth(std::env::var("DURABLE_WORKFLOW_AUTH_TOKEN").unwrap())
        .header("X-Namespace", "default")
        .header("X-Durable-Workflow-Control-Plane-Version", "2")
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();
    assert!(history["next_page_token"].is_null());
    assert_eq!(
        history["events"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|event| event["event_type"] == "WorkflowCompleted")
            .count(),
        1
    );
    stop(&replacement).await;
}
