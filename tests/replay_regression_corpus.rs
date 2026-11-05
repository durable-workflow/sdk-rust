use std::{
    collections::{BTreeMap, HashSet},
    fs,
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    thread,
    time::Duration,
};

use durable_workflow::{
    decode_payload, encode_payload, json, ActivityOptions, ActivityRetryPolicy, AvroValue,
    ChildWorkflowOptions, Client, ConditionWaitOptions, ConditionWaitResult, Error,
    ParallelOperation, ParallelResult, PayloadEnvelope, SearchAttributeUpdate, SelectionKey,
    StickyCacheOptions, Value, Worker, WorkerRetryPolicy, WorkflowContext, WorkflowInstance,
    DEFAULT_CODEC,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const FIXTURE_SCHEMA: &str = "durable-workflow.replay-regression/v1";

#[path = "replay_regression_corpus/redrive_successor_strictness.rs"]
mod redrive_successor_strictness;

#[derive(Clone, Debug, Deserialize, Serialize)]
struct TypedReplayContract {
    message: String,
}

#[derive(Clone, Default)]
struct TypedReplayState {
    message: Option<String>,
}

#[derive(Clone, Debug)]
struct CapturedRequest {
    path: String,
    body: String,
}

struct FixtureServer {
    addr: SocketAddr,
    stop: Arc<AtomicBool>,
    requests: Arc<Mutex<Vec<CapturedRequest>>>,
    thread: Option<thread::JoinHandle<()>>,
}

struct ExternalInput {
    blob: String,
    refuse_claim: bool,
}

impl FixtureServer {
    fn start(
        task: Value,
        completion_storage_refusals: u64,
        external: Option<ExternalInput>,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind replay fixture server");
        listener
            .set_nonblocking(true)
            .expect("configure replay fixture listener");
        let addr = listener
            .local_addr()
            .expect("replay fixture server address");
        let stop = Arc::new(AtomicBool::new(false));
        let server_stop = Arc::clone(&stop);
        let requests = Arc::new(Mutex::new(Vec::new()));
        let server_requests = Arc::clone(&requests);
        let thread = thread::spawn(move || {
            while !server_stop.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        // macOS/BSD accepted sockets inherit the listener's O_NONBLOCK.
                        let _ = stream.set_nonblocking(false);
                        let _ = stream.set_write_timeout(Some(Duration::from_secs(2)));
                        handle_request(
                            &mut stream,
                            &server_requests,
                            &task,
                            completion_storage_refusals,
                            external.as_ref(),
                        );
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2));
                    }
                    Err(_) => break,
                }
            }
        });
        Self {
            addr,
            stop,
            requests,
            thread: Some(thread),
        }
    }

    fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    fn request_body(&self, path: &str) -> Option<Value> {
        self.requests
            .lock()
            .expect("captured replay fixture requests")
            .iter()
            .find(|request| request.path == path)
            .map(|request| {
                serde_json::from_str(&request.body).unwrap_or_else(|error| {
                    panic!(
                        "invalid JSON request body for {path}: {error}: {:?}",
                        request.body
                    )
                })
            })
    }
}

impl Drop for FixtureServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect(self.addr);
        if let Some(thread) = self.thread.take() {
            thread.join().expect("join replay fixture server");
        }
    }
}

fn handle_request(
    stream: &mut TcpStream,
    requests: &Arc<Mutex<Vec<CapturedRequest>>>,
    task: &Value,
    completion_storage_refusals: u64,
    external: Option<&ExternalInput>,
) {
    let _ = stream.set_read_timeout(Some(Duration::from_millis(200)));
    let mut request = Vec::new();
    let mut buffer = [0_u8; 8192];
    loop {
        match stream.read(&mut buffer) {
            Ok(0) => break,
            Ok(read) => {
                request.extend_from_slice(&buffer[..read]);
                if request_is_complete(&request) {
                    break;
                }
            }
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                break;
            }
            Err(_) => return,
        }
    }
    let request = String::from_utf8_lossy(&request);
    let path = request
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .unwrap_or_default()
        .to_string();
    let body = request
        .split_once("\r\n\r\n")
        .map(|(_, body)| body)
        .unwrap_or_default()
        .to_string();
    let registration: Value = serde_json::from_str(&body).unwrap_or(Value::Null);
    let request_number = {
        let mut requests = requests.lock().expect("captured replay fixture requests");
        requests.push(CapturedRequest {
            path: path.clone(),
            body,
        });
        requests
            .iter()
            .filter(|request| request.path == path)
            .count()
    };
    let storage_refused = path.starts_with("/api/worker/workflow-tasks/")
        && path.ends_with("/complete")
        && request_number as u64 <= completion_storage_refusals;
    let authority_refused = path.starts_with("/api/worker/workflow-tasks/")
        && path.ends_with("/heartbeat")
        && external.is_some_and(|input| input.refuse_claim);
    let body = match path.as_str() {
        "/api/cluster/info" => json!({"limits": {"max_payload_bytes": 2097152}}).to_string(),
        _ if storage_refused => json!({
            "reason": "storage_pressure",
            "storage_state": "fenced",
            "retryable": true,
            "retry_after_seconds": 1,
            "request_admitted": false
        })
        .to_string(),
        _ if authority_refused => json!({"task_id":task["task_id"], "workflow_task_attempt":1,
            "reason":"task_not_leased", "retryable":false}).to_string(),
        path if path.starts_with("/api/worker/workflow-tasks/") && path.ends_with("/heartbeat") => {
            json!({"task_id":task["task_id"], "workflow_task_attempt":task["workflow_task_attempt"],
                "lease_owner":task["lease_owner"], "renewed":true, "reason":null,
                "task_status":"leased", "run_status":"running", "lease_expires_at":"2099-01-01T00:00:00Z",
                "cancellation_request":null}).to_string()
        }
        "/api/external-payloads/v1/ep_00000000000000000000000001" if external.is_some() => {
            external.unwrap().blob.clone()
        }
        "/api/worker/register"
            if registration["capability_manifest"]["sticky_execution"]["supported"] == true =>
        {
            json!({
                "worker_id": registration["worker_id"],
            "namespace": "default",
                "task_queue": registration["task_queue"],
                "build_id": registration["build_id"],
                "registered": true,
                "protocol_version": "1.18",
                "heartbeat_interval_seconds": 3600,
                "capabilities": registration["capabilities"],
                "capability_manifest": registration["capability_manifest"],
                "server_capabilities": {"sticky_execution": {"supported": true}}
            })
            .to_string()
        }
        "/api/worker/register" if task.get("query_task_id").is_some() => json!({
            "worker_id":"regression-corpus-worker","registered":true,
            "server_capabilities":{"query_task_authority":true}
        }).to_string(),
        "/api/worker/register" => json!({
            "worker_id": "regression-corpus-worker",
            "registered": true,
            "heartbeat_interval_seconds": 3600
        })
        .to_string(),
        "/api/worker/workflow-tasks/poll" if request_number == 1 && task.get("query_task_id").is_none() => {
            json!({"task": task}).to_string()
        }
        "/api/worker/workflow-tasks/poll" | "/api/worker/activity-tasks/poll" => {
            json!({"task": null}).to_string()
        }
        "/api/worker/query-tasks/poll" if task.get("query_task_id").is_some() => json!({"task":task}).to_string(),
        path if path.starts_with("/api/worker/query-tasks/") && path.contains("/status?") => {
            let mut reply = task.clone();
            reply["can_continue"] = json!(false);
            reply["query_task_status"] = json!("timed_out");
            reply["reason"] = json!("query_task_timed_out");
            reply.to_string()
        }
        path if path.starts_with("/api/worker/query-tasks/") && (path.ends_with("/complete") || path.ends_with("/fail")) => json!({"recorded":true}).to_string(),
        path if path.starts_with("/api/worker/workflow-tasks/")
            && (path.ends_with("/complete") || path.ends_with("/fail")) =>
        {
            json!({}).to_string()
        }
        _ => json!({"message": "not found"}).to_string(),
    };
    let status = if authority_refused {
        "409 Conflict"
    } else if storage_refused {
        "503 Service Unavailable"
    } else if body.contains("\"not found\"") {
        "404 Not Found"
    } else {
        "200 OK"
    };
    let response = format!(
        "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = stream.write_all(response.as_bytes());
    let _ = stream.flush();
}

fn request_is_complete(request: &[u8]) -> bool {
    let Some(header_end) = request
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|position| position + 4)
    else {
        return false;
    };
    let headers = String::from_utf8_lossy(&request[..header_end]);
    let content_length = headers.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        name.eq_ignore_ascii_case("content-length")
            .then(|| value.trim().parse::<usize>().ok())
            .flatten()
    });
    request.len() >= header_end + content_length.unwrap_or(0)
}

fn fixture_paths() -> Result<Vec<PathBuf>, String> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let policy: Value = serde_json::from_str(
        &fs::read_to_string(root.join("regression-corpus-policy.json"))
            .map_err(|error| format!("read regression corpus policy: {error}"))?,
    )
    .map_err(|error| format!("parse regression corpus policy: {error}"))?;
    let selectors = policy["categories"]["replay"]["fixtures"]
        .as_array()
        .ok_or_else(|| "replay corpus policy has no fixture selectors".to_string())?
        .iter()
        .filter(|selector| selector["format"] == "replay-regression-v1")
        .collect::<Vec<_>>();
    if selectors.is_empty() {
        return Err("replay corpus policy has no replay-regression-v1 selector".to_string());
    }

    let mut paths = Vec::new();
    for selector in selectors {
        let pattern = selector["glob"]
            .as_str()
            .ok_or_else(|| "replay fixture selector glob must be a string".to_string())?;
        let directory = pattern
            .strip_suffix("/*.json")
            .ok_or_else(|| format!("Rust replay corpus runner cannot discover {pattern:?}"))?;
        let directory = root.join(directory);
        if !directory.is_dir() {
            continue;
        }
        for entry in fs::read_dir(&directory)
            .map_err(|error| format!("read {}: {error}", directory.display()))?
        {
            let path = entry
                .map_err(|error| format!("read {} entry: {error}", directory.display()))?
                .path();
            if path.extension().and_then(|extension| extension.to_str()) == Some("json") {
                paths.push(path);
            }
        }
    }
    paths.sort();
    paths.dedup();

    let fixtures = paths
        .into_iter()
        .map(|path| {
            let fixture: Value = serde_json::from_str(
                &fs::read_to_string(&path)
                    .map_err(|error| format!("read {}: {error}", path.display()))?,
            )
            .map_err(|error| format!("parse {}: {error}", path.display()))?;
            let identity = fixture["id"]
                .as_str()
                .ok_or_else(|| format!("{} has no fixture identity", path.display()))?
                .to_string();
            let supersedes = fixture
                .get("supersedes")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect::<Vec<_>>();
            Ok((path, identity, supersedes))
        })
        .collect::<Result<Vec<_>, String>>()?;
    let superseded = fixtures
        .iter()
        .flat_map(|(_, _, supersedes)| supersedes.iter().cloned())
        .collect::<HashSet<_>>();
    Ok(fixtures
        .into_iter()
        .filter_map(|(path, identity, _)| (!superseded.contains(&identity)).then_some(path))
        .collect())
}

fn normalize_command(command: &Value) -> Result<Value, String> {
    let mut normalized = command.clone();
    for field in ["arguments", "result"] {
        let Some(envelope) = command.get(field).and_then(Value::as_object) else {
            continue;
        };
        if envelope.get("codec").and_then(Value::as_str) == Some(DEFAULT_CODEC) {
            let envelope: PayloadEnvelope = serde_json::from_value(Value::Object(envelope.clone()))
                .map_err(|error| format!("parse replay command {field} envelope: {error}"))?;
            normalized[field] = decode_payload(&envelope)
                .map_err(|error| format!("decode replay command {field}: {error}"))?;
        }
    }
    if let Some(items) = normalized
        .get_mut("workflow_stream")
        .and_then(|directive| directive.get_mut("items"))
        .and_then(Value::as_array_mut)
    {
        for (index, item) in items.iter_mut().enumerate() {
            let Some(envelope) = item.get("payload").cloned() else {
                continue;
            };
            let envelope: PayloadEnvelope = serde_json::from_value(envelope).map_err(|error| {
                format!("parse replay workflow stream item {index} envelope: {error}")
            })?;
            item["payload"] = decode_payload(&envelope)
                .map_err(|error| format!("decode replay workflow stream item {index}: {error}"))?;
        }
    }
    Ok(normalized)
}

fn fixture_matches(expected: &Value, actual: &Value, context: &str) -> Result<(), String> {
    match expected {
        Value::Object(expected_items) => {
            let actual_items = actual
                .as_object()
                .ok_or_else(|| format!("{context} must be an object, observed {actual}"))?;
            for (key, expected_item) in expected_items {
                let actual_item = actual_items
                    .get(key)
                    .ok_or_else(|| format!("{context} is missing {key:?}"))?;
                fixture_matches(expected_item, actual_item, &format!("{context}.{key}"))?;
            }
            Ok(())
        }
        Value::Array(expected_items) => {
            let actual_items = actual
                .as_array()
                .ok_or_else(|| format!("{context} must be an array, observed {actual}"))?;
            if expected_items.len() != actual_items.len() {
                return Err(format!(
                    "{context} expected {} entries, observed {}",
                    expected_items.len(),
                    actual_items.len()
                ));
            }
            for (index, (expected_item, actual_item)) in
                expected_items.iter().zip(actual_items).enumerate()
            {
                fixture_matches(expected_item, actual_item, &format!("{context}[{index}]"))?;
            }
            Ok(())
        }
        _ if expected == actual => Ok(()),
        _ => Err(format!("{context} expected {expected}, observed {actual}")),
    }
}

async fn execute_fixture_delivery(fixture: &Value, delivery_id: &str) -> Result<Value, String> {
    if fixture["fixture_schema"] != FIXTURE_SCHEMA {
        return Err("fixture does not declare the replay-regression schema".to_string());
    }
    if !fixture["bindings"]
        .as_array()
        .is_some_and(|bindings| bindings.iter().any(|binding| binding == "rust"))
    {
        return Err("fixture does not name the Rust binding".to_string());
    }
    let fixture_id = fixture["id"].as_str().unwrap_or("<unnamed>");
    let workflow = fixture["workflow"]
        .as_object()
        .ok_or_else(|| format!("{fixture_id}.workflow must be an object"))?;
    let workflow_type = workflow
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| format!("{fixture_id}.workflow.type must be a string"))?;
    if workflow_type == "corpus.expired-query-snapshot" {
        return execute_expired_query_fixture(fixture).await;
    }
    if !matches!(
        workflow_type,
        "corpus.side-effect-version"
            | "corpus.legacy-patch-activity"
            | "corpus.retained-patch-activity"
            | "corpus.deprecated-patch-activity"
            | "corpus.frozen-zero"
            | "corpus.php-zero-full"
            | "corpus.duplicate-registration"
            | "corpus.update-snapshot"
            | "corpus.workflow-stream"
            | "corpus.message-stream-batch"
            | "corpus.message-stream-partial-batches"
            | "corpus.nested-parallel"
            | "corpus.typed-replayed"
            | "corpus.memo-signed-zero"
            | "corpus.search-attribute-type-mismatch"
            | "corpus.condition-search"
            | "corpus.condition-search-adjacent"
            | "corpus.grouped-condition-reopen"
            | "corpus.grouped-condition-timed-reopen"
            | "corpus.selected-condition-timed-reopen"
            | "corpus.two-member-condition-group"
            | "corpus.timer-separated-condition-group"
            | "corpus.cancellation-remaining-condition"
            | "corpus.durable-selection"
            | "corpus.durable-selection-portable-affinity"
            | "corpus.selection-cancel-request"
            | "corpus.redrive-boundary"
            | "corpus.redrive-reused-markers"
            | "corpus.redrive-reused-timer"
            | "corpus.redrive-reused-cancellation-clock"
            | "corpus.redrive-reused-upserts"
            | "corpus.redrive-reused-signal-wait"
            | "corpus.redrive-successor-markers"
            | "corpus.redrive-successor-timer-cancellation"
            | "corpus.child-policy-author"
            | "corpus.activity-policy-author"
            | "corpus.nexus-result"
            | "corpus.nexus-cancellation"
    ) {
        return Err(format!(
            "replay fixture {fixture_id} has no registered Rust workflow {workflow_type:?}"
        ));
    }
    let payload_codec = workflow
        .get("payload_codec")
        .and_then(Value::as_str)
        .unwrap_or(DEFAULT_CODEC);
    let input = workflow.get("input").cloned().unwrap_or_else(|| json!([]));
    if !input.is_array() {
        return Err(format!("{fixture_id}.workflow.input must be an array"));
    }
    if input != json!([])
        && !matches!(
            workflow_type,
            "corpus.typed-replayed" | "corpus.update-snapshot"
        )
    {
        return Err(format!(
            "{fixture_id}.workflow.input must use the declared empty Avro corpus input"
        ));
    }
    let input_envelope = serde_json::to_value(
        encode_payload(&input, DEFAULT_CODEC)
            .map_err(|error| format!("encode {fixture_id} Avro input: {error}"))?,
    )
    .map_err(|error| format!("serialize {fixture_id} Avro input: {error}"))?;
    let history = fixture.get("history").cloned().unwrap_or_else(|| json!([]));
    if !history.is_array() {
        return Err(format!("{fixture_id}.history must be an array"));
    }

    let task_id = fixture["worker_task"]["task_id"]
        .as_str()
        .map(str::to_string)
        .unwrap_or_else(|| format!("regression-corpus-{fixture_id}-{delivery_id}"));
    let task_payload_codec = match fixture.get("worker_task") {
        Some(worker_task) => worker_task.get("payload_codec").cloned(),
        None => Some(json!(payload_codec)),
    };
    let mut task = json!({
        "task_id": task_id,
        "workflow_id": format!("regression-corpus-{fixture_id}"),
        "run_id": "regression-corpus-run",
        "workflow_type": workflow_type,
        "arguments": input_envelope,
        "history_events": history,
        "workflow_task_attempt": 1,
        "lease_owner": "regression-corpus-worker"
    });
    if let Some(task_payload_codec) = task_payload_codec {
        task["payload_codec"] = task_payload_codec;
    }
    if let Some(workflow_command_id) = fixture
        .get("worker_task")
        .and_then(|worker_task| worker_task.get("workflow_command_id"))
    {
        task["workflow_command_id"] = workflow_command_id.clone();
    }
    for field in [
        "workflow_id",
        "workflow_update_id",
        "update_name",
        "sticky_replay_mode",
        "last_history_sequence",
        "total_history_events",
    ] {
        if let Some(value) = fixture["worker_task"].get(field) {
            task[field] = value.clone();
        }
    }
    let completion_storage_refusals = fixture["worker_task"]["completion_storage_refusals"]
        .as_u64()
        .unwrap_or(0);
    let external = if fixture["worker_task"]["external_input"] == true {
        let blob = task["arguments"]["blob"].as_str().unwrap().to_owned();
        task["arguments"] = json!({"codec":"avro", "external_payload":{
            "schema":"durable-workflow.v2.runtime-external-payload-reference.v1", "codec":"avro",
            "reference_id":"ep_00000000000000000000000001", "size_bytes":blob.len(),
            "sha256":format!("{:x}", Sha256::digest(blob.as_bytes()))}});
        Some(ExternalInput {
            blob,
            refuse_claim: fixture["worker_task"]["hydration_authority_refused"] == true,
        })
    } else {
        None
    };
    let server = FixtureServer::start(task, completion_storage_refusals, external);
    let client = Client::builder(server.base_url())
        .timeout(Duration::from_secs(2))
        .build()
        .map_err(|error| format!("create replay corpus client: {error}"))?;
    let callback_calls = Arc::new(AtomicUsize::new(0));
    let observed_calls = Arc::clone(&callback_calls);
    let handler_calls = Arc::new(AtomicUsize::new(0));
    let observed_handler_calls = Arc::clone(&handler_calls);
    let mut worker = Worker::new(client, "regression-corpus")
        .worker_id("regression-corpus-worker")
        .poll_timeout(Duration::from_millis(10))
        .retry_policy(WorkerRetryPolicy {
            max_retries: 1,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(1),
        });
    let sticky_capacity = fixture["worker_task"]["sticky_cache_capacity"].as_u64();
    if let Some(capacity) = sticky_capacity {
        worker = worker
            .build_id("regression-corpus-build")
            .sticky_cache(StickyCacheOptions::new(capacity as usize))
            .map_err(|error| format!("{fixture_id} sticky cache configuration failed: {error}"))?;
    }
    match workflow_type {
        "corpus.nexus-result" => {
            worker.register_workflow(workflow_type, |ctx, _| async move {
                ctx.call_nexus_service("endpoint", "service", "operation", json!(["world"]))
                    .await?
                    .result
                    .deserialize::<Value>()
            });
        }
        "corpus.nexus-cancellation" => {
            worker.register_workflow(workflow_type, |ctx, _| async move {
                let Err(Error::CooperativeCancellationRequested(cancelled)) = ctx
                    .call_nexus_service("endpoint", "service", "operation", json!(["world"]))
                    .await
                else {
                    return Err(Error::WorkerLoop(
                        "expected original service cancellation".into(),
                    ));
                };
                Ok(json!(cancelled.request.request_id))
            });
        }
        "corpus.php-zero-full" => {
            worker.register_workflow(workflow_type, |ctx, _input| async move {
                let first = ctx.get_version("frozen-zero", -7, 0)?;
                let patch = ctx.patched("frozen-zero")?;
                ctx.deprecate_patch("frozen-zero")?;
                let wider = ctx.get_version("frozen-zero", 0, 10)?;
                let repeat = ctx.patched("frozen-zero")?;
                let result = ctx
                    .activity_with_options(
                        "MatrixActivity",
                        ActivityOptions::new().task_queue("default").retry_policy(
                            ActivityRetryPolicy::new(1).backoff_intervals(
                                [1, 2, 5, 10, 15, 30, 60, 120].map(Duration::from_secs),
                            ),
                        ),
                        json!([]),
                    )
                    .await?;
                Ok(json!([first, patch, null, wider, repeat, result]))
            });
        }
        "corpus.duplicate-registration" => {
            worker.register_workflow(workflow_type, |_ctx, _input| async { Ok(json!("first")) });
            worker.register_workflow(workflow_type, |_ctx, _input| async { Ok(json!("second")) });
        }
        "corpus.frozen-zero" => {
            worker.register_workflow(workflow_type, |ctx, _input| async move {
                let first = ctx.get_version("frozen-zero", -7, 0)?;
                let patch = ctx.patched("frozen-zero")?;
                ctx.deprecate_patch("frozen-zero")?;
                let wider = ctx.get_version("frozen-zero", 0, 10)?;
                let repeat = ctx.patched("frozen-zero")?;
                Ok(json!([first, patch, null, wider, repeat]))
            });
        }
        "corpus.deprecated-patch-activity" => {
            worker.register_workflow(workflow_type, |ctx, _input| async move {
                ctx.deprecate_patch("retained-patch")?;
                let patch = ctx.patched("retained-patch")?;
                let version = ctx.get_version("retained-patch", 0, 10)?;
                let result = ctx.activity("old", json!([])).await?;
                Ok(json!({"patch": patch, "version": version, "result": result}))
            });
        }
        "corpus.retained-patch-activity" => {
            worker.register_workflow(workflow_type, |ctx, _input| async move {
                let first = ctx.patched("retained-patch")?;
                let second = ctx.patched("retained-patch")?;
                ctx.deprecate_patch("retained-patch")?;
                let result = ctx.activity("old", json!([])).await?;
                Ok(json!({"first": first, "second": second, "result": result}))
            });
        }
        "corpus.legacy-patch-activity" => {
            worker.register_workflow(workflow_type, |ctx, _input| async move {
                let first = ctx.patched("added-step")?;
                let second = ctx.patched("added-step")?;
                ctx.deprecate_patch("added-step")?;
                if first || second {
                    ctx.activity("new", json!([])).await?;
                }
                let result = ctx.activity("old", json!([])).await?;
                Ok(json!({"first": first, "second": second, "result": result}))
            });
        }
        "corpus.update-snapshot" => {
            worker.register_workflow(workflow_type, |_ctx, _input| async move { Ok(Value::Null) });
            worker.register_update(workflow_type, "inspect", |ctx, arguments| async move {
                Ok(json!({
                    "workflow_input": ctx.workflow_input(),
                    "signals": ctx.signals("changed"),
                    "update_arguments": arguments,
                }))
            });
        }
        "corpus.side-effect-version" => {
            worker.register_workflow(workflow_type, move |ctx, _input| {
                let observed_calls = Arc::clone(&observed_calls);
                async move {
                    let captured = ctx.side_effect(|| {
                        observed_calls.fetch_add(1, Ordering::SeqCst);
                        "captured-once".to_string()
                    })?;
                    let version = ctx.get_version("cold-restart", 1, 3)?;
                    Ok(json!({"captured": captured, "version": version}))
                }
            });
        }
        "corpus.workflow-stream" => {
            worker.register_workflow(workflow_type, |ctx, _input| async move {
                ctx.append_workflow_stream(
                    "tokens",
                    &[
                        durable_workflow::WorkflowStreamAppendItem::new(json!({"token": "hello"}))?,
                        durable_workflow::WorkflowStreamAppendItem::from_reference(
                            "s3://payloads/token-2",
                        ),
                    ],
                    None,
                )?;
                ctx.close_workflow_stream("tokens", None)?;
                Ok(json!("done"))
            });
        }
        "corpus.message-stream-batch" => {
            worker.register_workflow(workflow_type, |ctx, _input| async move {
                let messages = ctx.message_stream("orders")?.receive(2).await?;
                Ok(json!(messages
                    .into_iter()
                    .map(|message| message.message_id)
                    .collect::<Vec<_>>()))
            });
        }
        "corpus.message-stream-partial-batches" => {
            worker.register_workflow(workflow_type, |ctx, _input| async move {
                let stream = ctx.message_stream("orders")?;
                let first = stream.receive(10).await?;
                let second = stream.receive(10).await?;
                Ok(json!([
                    first
                        .into_iter()
                        .map(|message| message.message_id)
                        .collect::<Vec<_>>(),
                    second
                        .into_iter()
                        .map(|message| message.message_id)
                        .collect::<Vec<_>>(),
                ]))
            });
        }
        "corpus.cancellation-remaining-condition" => {
            worker.register_workflow(workflow_type, |ctx, _| async move {
                let _ = ctx.sleep(Duration::from_secs(10)).await;
                let context = ctx
                    .cancellation_context()?
                    .expect("committed rich cancellation");
                let _shield = ctx.cancellation_shield()?;
                ctx.parallel(vec![
                    ParallelOperation::timer(Duration::from_secs(1)),
                    ParallelOperation::condition(
                        ConditionWaitOptions::new("ready", "sha256:ready"),
                        || Ok(false),
                    ),
                ])
                .await?;
                Ok(json!(context.remaining()?.as_secs_f64()))
            });
        }
        "corpus.timer-separated-condition-group" => {
            worker.register_workflow(workflow_type, |ctx, _| async move {
                let votes = ctx.clone();
                let approvals = ctx.clone();
                ctx.parallel(vec![
                    ParallelOperation::condition(
                        ConditionWaitOptions::new("votes", "sha256:votes-v1")
                            .timeout(Duration::from_secs(300)),
                        move || Ok(votes.signals("vote")?.len() >= 2),
                    ),
                    ParallelOperation::timer(Duration::from_secs(5)),
                    ParallelOperation::condition(
                        ConditionWaitOptions::new("approvals", "sha256:approvals-v1")
                            .timeout(Duration::from_secs(300)),
                        move || Ok(approvals.signals("approve")?.len() >= 1),
                    ),
                ])
                .await?;
                Ok(json!("both"))
            });
        }
        "corpus.two-member-condition-group" => {
            worker.register_workflow(workflow_type, |ctx, _| async move {
                let votes = ctx.clone();
                let approvals = ctx.clone();
                ctx.parallel(vec![
                    ParallelOperation::condition(
                        ConditionWaitOptions::new("votes", "sha256:votes-v1")
                            .timeout(Duration::from_secs(300)),
                        move || Ok(votes.signals("vote")?.len() >= 2),
                    ),
                    ParallelOperation::condition(
                        ConditionWaitOptions::new("approvals", "sha256:approvals-v1")
                            .timeout(Duration::from_secs(300)),
                        move || Ok(approvals.signals("approve")?.len() >= 1),
                    ),
                ])
                .await?;
                Ok(json!("both"))
            });
        }
        "corpus.selected-condition-timed-reopen" => {
            worker.register_workflow(workflow_type, |ctx, _| async move {
                ctx.select_keyed(vec![
                    ("timer", ParallelOperation::timer(Duration::from_secs(300))),
                    (
                        "votes",
                        ParallelOperation::condition(
                            ConditionWaitOptions::new("two-votes", "sha256:two-votes-v1")
                                .timeout(Duration::from_secs(30)),
                            || Ok(false),
                        ),
                    ),
                ])
                .await?;
                Ok(Value::Null)
            });
        }
        "corpus.grouped-condition-timed-reopen" => {
            worker.register_workflow(workflow_type, |ctx, _| async move {
                ctx.parallel(vec![
                    ParallelOperation::timer(Duration::from_secs(300)),
                    ParallelOperation::condition(
                        ConditionWaitOptions::new("two-votes", "sha256:two-votes-v1")
                            .timeout(Duration::from_secs(30)),
                        || Ok(false),
                    ),
                ])
                .await?;
                Ok(Value::Null)
            });
        }
        "corpus.grouped-condition-reopen" => {
            worker.register_workflow(workflow_type, |ctx, _| async move {
                ctx.parallel(vec![
                    ParallelOperation::timer(Duration::from_secs(300)),
                    ParallelOperation::condition(
                        ConditionWaitOptions::new("two-votes", "sha256:two-votes-v1"),
                        || Ok(false),
                    ),
                ])
                .await?;
                Ok(Value::Null)
            });
        }
        "corpus.nested-parallel" => {
            worker.register_workflow(workflow_type, |ctx, _input| async move {
                let results = ctx
                    .parallel(vec![
                        ParallelOperation::activity("first", json!([])),
                        ParallelOperation::group(vec![
                            ParallelOperation::child_workflow(
                                "second",
                                ChildWorkflowOptions::new("child-workers"),
                                json!([]),
                            ),
                            ParallelOperation::activity("third", json!([])),
                        ]),
                    ])
                    .await?;
                let [ParallelResult::Activity(first), ParallelResult::Group(nested)] =
                    results.as_slice()
                else {
                    return Err(Error::WorkerLoop(
                        "nested parallel replay returned the wrong outer shape".to_string(),
                    ));
                };
                let [ParallelResult::ChildWorkflow(second), ParallelResult::Activity(third)] =
                    nested.as_slice()
                else {
                    return Err(Error::WorkerLoop(
                        "nested parallel replay returned the wrong inner shape".to_string(),
                    ));
                };
                if first != "one" || second.result != "two" || third != "three" {
                    return Err(Error::WorkerLoop(
                        "nested parallel replay did not preserve input-order results".to_string(),
                    ));
                }
                Ok(json!(["one", ["two", "three"]]))
            });
        }
        "corpus.typed-replayed" => {
            worker.register_typed_replayed_workflow(
                workflow_type,
                TypedReplayState::default,
                move |ctx,
                      input: TypedReplayContract,
                      state: WorkflowInstance<TypedReplayState>| {
                    let observed_handler_calls = Arc::clone(&observed_handler_calls);
                    async move {
                        observed_handler_calls.fetch_add(1, Ordering::SeqCst);
                        let result: TypedReplayContract =
                            ctx.activity_typed("corpus.typed.activity", input).await?;
                        state.update(|current| current.message = Some(result.message.clone()))?;
                        Ok(result)
                    }
                },
            );
        }
        "corpus.activity-policy-author" => {
            worker.register_workflow(workflow_type, |ctx, _input| async move {
                ctx.activity("work", json!([])).await?;
                Ok(json!("unreachable"))
            });
        }
        "corpus.child-policy-author" => {
            worker.register_workflow(workflow_type, |ctx, _input| async move {
                ctx.start_child_workflow(
                    "child",
                    ChildWorkflowOptions::new("regression-corpus"),
                    json!([]),
                )
                .await?;
                Ok(json!("unreachable"))
            });
        }
        "corpus.redrive-boundary" => {
            worker.register_workflow(workflow_type, |ctx, _input| async move {
                ctx.activity("corpus.redrive.activity", json!([])).await?;
                Ok(json!("unexpected completion"))
            });
        }
        "corpus.redrive-reused-markers" => {
            worker.register_workflow(workflow_type, move |ctx, _input| {
                let observed_calls = Arc::clone(&observed_calls);
                async move {
                    let captured = ctx.side_effect(|| {
                        observed_calls.fetch_add(1, Ordering::SeqCst);
                        "re-executed".to_string()
                    })?;
                    let version = ctx.get_version("redrive-markers", -1, 1)?;
                    ctx.activity(
                        "Tests\\Fixtures\\V2\\TestRedriveFirstActivity",
                        json!([captured, version]),
                    )
                    .await?;
                    Ok(json!("unexpected completion"))
                }
            });
        }
        "corpus.redrive-reused-timer" => {
            worker.register_workflow(workflow_type, |ctx, _input| async move {
                let greeting = ctx
                    .activity(
                        "Tests\\Fixtures\\V2\\TestRedriveFirstActivity",
                        json!(["Taylor"]),
                    )
                    .await?;
                ctx.sleep(Duration::from_secs(5)).await?;
                ctx.activity(
                    "Tests\\Fixtures\\V2\\TestRedriveSecondActivity",
                    json!([greeting]),
                )
                .await?;
                Ok(json!("unexpected completion"))
            });
        }
        "corpus.redrive-reused-cancellation-clock" => {
            worker.register_workflow(workflow_type, |ctx, _| async move {
                let _ = ctx.sleep(Duration::from_secs(10)).await;
                let context = ctx
                    .cancellation_context()?
                    .expect("committed rich cancellation");
                let _shield = ctx.cancellation_shield()?;
                ctx.activity("corpus.redrive.cleanup", json!([])).await?;
                ctx.sleep(Duration::from_secs(5)).await?;
                Ok(json!(context.remaining()?.as_secs_f64()))
            });
        }
        "corpus.redrive-reused-upserts" => {
            // Core's TestRedriveVisibilityWorkflow up to its first activity,
            // where the successor's own cancellation request is delivered.
            worker.register_workflow(workflow_type, |ctx, _| async move {
                ctx.upsert_memo(AvroValue::Map(BTreeMap::from([
                    (
                        "stage".to_string(),
                        AvroValue::String("greeting".to_string()),
                    ),
                    (
                        "customer".to_string(),
                        AvroValue::String("Taylor".to_string()),
                    ),
                ])))?;
                ctx.upsert_search_attributes(
                    SearchAttributeUpdate::new().keyword("Stage", "greeting")?,
                )?;
                redrive_successor_cancellation_cleanup(
                    &ctx,
                    "Tests\\Fixtures\\V2\\TestRedriveFirstActivity",
                    json!(["Taylor"]),
                    Vec::new(),
                )
                .await
            });
        }
        "corpus.redrive-reused-signal-wait" => {
            // Core's TestRedriveSignalWorkflow up to its first activity,
            // where the successor's own cancellation request is delivered.
            worker.register_workflow(workflow_type, |ctx, _| async move {
                let approved_by = ctx.wait_signal("approve").await?;
                let received = ctx.signals("approve")?;
                redrive_successor_cancellation_cleanup(
                    &ctx,
                    "Tests\\Fixtures\\V2\\TestRedriveFirstActivity",
                    json!(["Taylor"]),
                    vec![json!(approved_by), json!(received)],
                )
                .await
            });
        }
        "corpus.redrive-successor-markers" => {
            // Core's TestRedriveMarkersWorkflow, redriven at its second
            // activity. The reused values must reach the workflow unchanged.
            worker.register_workflow(workflow_type, move |ctx, _input| {
                let observed_calls = Arc::clone(&observed_calls);
                async move {
                    let token = ctx.side_effect(|| {
                        observed_calls.fetch_add(1, Ordering::SeqCst);
                        "re-executed".to_string()
                    })?;
                    let version = ctx.get_version("redrive-markers", -1, 1)?;
                    if token != "0123456789abcdef" || version != 1 {
                        return Err(Error::WorkerLoop(format!(
                            "reused markers changed: {token:?}, {version}"
                        )));
                    }
                    let greeting = ctx
                        .activity(
                            "Tests\\Fixtures\\V2\\TestRedriveFirstActivity",
                            json!(["Taylor"]),
                        )
                        .await?;
                    ctx.activity(
                        "Tests\\Fixtures\\V2\\TestRedriveSecondActivity",
                        json!([greeting]),
                    )
                    .await?;
                    Ok(json!("unexpected completion"))
                }
            });
        }
        "corpus.redrive-successor-timer-cancellation" => {
            // Core's TestRedriveTimerWorkflow, redriven at its second
            // activity, where the successor's own cancellation is delivered.
            worker.register_workflow(workflow_type, |ctx, _input| async move {
                let greeting = ctx
                    .activity(
                        "Tests\\Fixtures\\V2\\TestRedriveFirstActivity",
                        json!(["Taylor"]),
                    )
                    .await?;
                ctx.sleep(Duration::from_secs(5)).await?;
                redrive_successor_cancellation_cleanup(
                    &ctx,
                    "Tests\\Fixtures\\V2\\TestRedriveSecondActivity",
                    json!([greeting]),
                    Vec::new(),
                )
                .await
            });
        }
        "corpus.memo-signed-zero" => {
            worker.register_workflow(workflow_type, |ctx, _input| async move {
                ctx.upsert_memo(AvroValue::Map(BTreeMap::from([(
                    "reading".to_string(),
                    AvroValue::Double(0.0),
                )])))?;
                Ok(json!("signed-zero replay unexpectedly matched"))
            });
        }
        "corpus.search-attribute-type-mismatch" => {
            worker.register_workflow(workflow_type, |ctx, _input| async move {
                ctx.upsert_search_attributes(
                    SearchAttributeUpdate::new().string("customer_tier", "gold")?,
                )?;
                Ok(json!("unreachable"))
            });
        }
        "corpus.condition-search" => {
            worker.register_workflow(workflow_type, move |ctx, _input| async move {
                let predicate_ctx = ctx.clone();
                let outcome = ctx
                    .wait_condition(
                        ConditionWaitOptions::new("approval", "sha256:corpus-approval-v1")
                            .timeout(Duration::from_secs(30)),
                        move || Ok(!predicate_ctx.signals("approve")?.is_empty()),
                    )
                    .await?;
                let status = match outcome {
                    ConditionWaitResult::Satisfied => "approved",
                    ConditionWaitResult::TimedOut => "approval_timed_out",
                };
                ctx.upsert_search_attributes(
                    SearchAttributeUpdate::new()
                        .keyword("OrderStatus", status)?
                        .bool("NeedsAttention", outcome.is_timed_out())?,
                )?;
                Ok(json!({"status": status}))
            });
        }
        "corpus.condition-search-adjacent" => {
            worker.register_workflow(workflow_type, move |ctx, _input| async move {
                let mut outcomes = Vec::new();
                for occurrence in 0..2 {
                    let predicate_ctx = ctx.clone();
                    outcomes.push(
                        ctx.wait_condition(
                            ConditionWaitOptions::new("approval", "sha256:corpus-approval-v1")
                                .timeout(Duration::from_secs(30)),
                            move || {
                                Ok(
                                    occurrence == 0
                                        && !predicate_ctx.signals("approve")?.is_empty(),
                                )
                            },
                        )
                        .await?,
                    );
                }
                let status = if outcomes
                    .first()
                    .is_some_and(|outcome| outcome.is_satisfied())
                {
                    "approved"
                } else {
                    "approval_timed_out"
                };
                ctx.upsert_search_attributes(
                    SearchAttributeUpdate::new()
                        .keyword("OrderStatus", status)?
                        .bool("NeedsAttention", status != "approved")?,
                )?;
                Ok(json!({"status": status, "waits": outcomes}))
            });
        }
        "corpus.durable-selection"
        | "corpus.durable-selection-portable-affinity"
        | "corpus.selection-cancel-request" => {
            let cancel_request = workflow_type == "corpus.selection-cancel-request";
            worker.register_workflow(workflow_type, move |ctx, _input| async move {
                let selected = ctx
                    .select_keyed(vec![
                        (
                            "slow",
                            ParallelOperation::activity("slow-activity", json!([])),
                        ),
                        (
                            "fast",
                            ParallelOperation::activity("fast-activity", json!([])),
                        ),
                    ])
                    .await?;
                let slow = selected
                    .handle(&SelectionKey::Name("slow".to_string()))
                    .cloned()
                    .ok_or_else(|| {
                        Error::WorkerLoop(
                            "durable selection replay did not preserve the slow handle".to_string(),
                        )
                    })?;
                if cancel_request {
                    slow.cancel().await?;
                    return Ok(json!("cancel-requested"));
                }
                let winner = match &selected.key {
                    SelectionKey::Name(key) => key.clone(),
                    SelectionKey::Index(index) => index.to_string(),
                };
                let winner_identity = selected.identity.clone();
                let ParallelResult::Activity(winner_value) = selected.into_result()? else {
                    return Err(Error::WorkerLoop(
                        "durable selection replay returned a non-activity winner".to_string(),
                    ));
                };
                let ParallelResult::Activity(slow_value) = slow.await_result().await? else {
                    return Err(Error::WorkerLoop(
                        "durable selection replay returned a non-activity loser".to_string(),
                    ));
                };
                Ok(json!({
                    "winner": winner,
                    "winner_identity": winner_identity,
                    "winner_value": winner_value,
                    "slow": slow_value,
                }))
            });
        }
        _ => unreachable!("workflow type was validated above"),
    }
    let worker_registration = if let Some(expected) = fixture.get("worker_registration") {
        let registration = worker
            .register()
            .await
            .map_err(|error| format!("{fixture_id} worker registration failed: {error}"))?;
        if !registration.registered {
            return Err(format!("{fixture_id} worker registration was declined"));
        }
        let observed = server
            .request_body("/api/worker/register")
            .ok_or_else(|| format!("{fixture_id} worker registration was not captured"))?;
        fixture_matches(
            expected,
            &observed,
            &format!("{fixture_id}.worker_registration"),
        )?;
        Some(observed)
    } else {
        None
    };
    let replay = worker.run_once().await;
    if fixture["worker_task"]["hydration_authority_refused"] == true {
        let requests = server.requests.lock().unwrap();
        if requests.iter().any(|request| {
            request.path.contains("/external-payloads/")
                || request.path.ends_with("/complete")
                || request.path.ends_with("/fail")
        }) || callback_calls.load(Ordering::SeqCst) != 0
            || handler_calls.load(Ordering::SeqCst) != 0
        {
            return Err(format!(
                "{fixture_id} lost authority but fetched, executed or published"
            ));
        }
    }
    let handled = replay.map_err(|error| format!("{fixture_id} worker replay failed: {error}"))?;
    if handled != 1 {
        return Err(format!("{fixture_id} replay handled {handled} tasks"));
    }

    let completion_path = format!("/api/worker/workflow-tasks/{task_id}/complete");
    let completion = match server.request_body(&completion_path) {
        Some(completion) => completion,
        None => {
            let failure_path = format!("/api/worker/workflow-tasks/{task_id}/fail");
            if fixture["expected"].get("task_failure").is_some()
                && fixture["expected"]["waiting_for_history"] != true
            {
                let failure = server.request_body(&failure_path).ok_or_else(|| {
                    format!("{fixture_id} did not publish its typed task refusal")
                })?;
                let observed = json!({
                    "command_sequence": [],
                    "task_failure": failure,
                    "handler_invocations": handler_calls.load(Ordering::SeqCst),
                    "side_effect_callback_calls": callback_calls.load(Ordering::SeqCst),
                });
                fixture_matches(&fixture["expected"], &observed, fixture_id)?;
                return Ok(observed);
            }
            if fixture["expected"]["waiting_for_history"] == true {
                let failure = server.request_body(&failure_path).ok_or_else(|| {
                    format!("{fixture_id} did not acknowledge its pending history")
                })?;
                let observed = json!({
                    "command_sequence": [],
                    "waiting_for_history": true,
                    "task_failure": failure,
                });
                if let Some(expected_commands) = fixture.get("command_sequence") {
                    fixture_matches(
                        expected_commands,
                        &json!([]),
                        &format!("{fixture_id}.command_sequence"),
                    )?;
                }
                fixture_matches(
                    &fixture["expected"],
                    &observed,
                    &format!("{fixture_id}.expected"),
                )?;
                return Ok(observed);
            }
            let detail = server
                .request_body(&failure_path)
                .and_then(|failure| failure["failure"]["message"].as_str().map(str::to_string))
                .unwrap_or_else(|| {
                    "did not complete through the official Rust worker path".to_string()
                });
            return Err(format!(
                "{fixture_id} official Rust worker rejected replay: {detail}"
            ));
        }
    };
    let completion_attempts = {
        let requests = server
            .requests
            .lock()
            .expect("captured replay fixture requests");
        let bodies: Vec<_> = requests
            .iter()
            .filter(|request| request.path == completion_path)
            .map(|request| &request.body)
            .collect();
        if bodies.windows(2).any(|pair| pair[0] != pair[1]) {
            return Err(format!(
                "{fixture_id} changed a serialized completion during storage recovery"
            ));
        }
        bodies.len()
    };
    let commands = completion["commands"]
        .as_array()
        .ok_or_else(|| format!("{fixture_id} completion has no command sequence"))?
        .iter()
        .map(normalize_command)
        .collect::<Result<Vec<_>, _>>()?;
    if let Some(expected_commands) = fixture.get("command_sequence") {
        fixture_matches(
            expected_commands,
            &Value::Array(commands.clone()),
            &format!("{fixture_id}.command_sequence"),
        )?;
    }

    let poll_history_page_size = server
        .request_body("/api/worker/workflow-tasks/poll")
        .and_then(|body| body.get("history_page_size").cloned())
        .unwrap_or(Value::Null);
    let mut observed = serde_json::Map::from_iter([
        (
            "command_sequence".to_string(),
            Value::Array(commands.clone()),
        ),
        (
            "message_stream_cursors".to_string(),
            completion
                .get("message_stream_cursors")
                .cloned()
                .unwrap_or_else(|| json!([])),
        ),
        (
            "message_stream_waits".to_string(),
            completion
                .get("message_stream_waits")
                .cloned()
                .unwrap_or_else(|| json!([])),
        ),
        (
            "side_effect_callback_calls".to_string(),
            json!(callback_calls.load(Ordering::SeqCst)),
        ),
        (
            "poll_request".to_string(),
            json!({"history_page_size": poll_history_page_size}),
        ),
    ]);
    if let Some(worker_registration) = worker_registration {
        observed.insert("worker_registration".to_string(), worker_registration);
    }
    if sticky_capacity.is_some() {
        observed.insert(
            "sticky_cache_metrics".to_string(),
            serde_json::to_value(
                worker
                    .sticky_cache_metrics()
                    .map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string())?,
        );
    }
    observed.insert(
        "completion_attempts".to_string(),
        json!(completion_attempts),
    );
    observed.insert(
        "handler_invocations".to_string(),
        json!(handler_calls.load(Ordering::SeqCst)),
    );
    if let [Value::Object(command)] = commands.as_slice() {
        observed.extend(command.clone());
    }
    let expected = fixture["expected"]
        .as_object()
        .filter(|expected| !expected.is_empty())
        .ok_or_else(|| format!("{fixture_id}.expected must be a non-empty object"))?;
    fixture_matches(
        &Value::Object(expected.clone()),
        &Value::Object(observed.clone()),
        &format!("{fixture_id}.expected"),
    )?;
    Ok(Value::Object(observed))
}

async fn execute_expired_query_fixture(fixture: &Value) -> Result<Value, String> {
    // Synthetic canonical dispatch reproducer: the same delivered query claim
    // is terminal at observation. No history transformation or replay bypass.
    let envelope = serde_json::to_value(
        encode_payload(&json!([]), DEFAULT_CODEC).map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())?;
    let mut task = fixture["worker_task"].clone();
    task["workflow_type"] = fixture["workflow"]["type"].clone();
    task["workflow_arguments"] = envelope.clone();
    task["query_arguments"] = envelope;
    task["history_events"] = fixture["history"].clone();
    let server = FixtureServer::start(task, 0, None);
    let client = Client::builder(server.base_url())
        .namespace("default")
        .build()
        .map_err(|error| error.to_string())?;
    let mut worker =
        Worker::new(client, "regression-corpus-queue").worker_id("regression-corpus-worker");
    worker.register_workflow("corpus.expired-query-snapshot", |_, _| async {
        Ok(Value::Null)
    });
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    worker.register_query("corpus.expired-query-snapshot", "value", move |_, _| {
        observed.fetch_add(1, Ordering::SeqCst);
        async { Ok(json!(7)) }
    });
    worker.register().await.map_err(|error| error.to_string())?;
    worker.run_once().await.map_err(|error| error.to_string())?;
    let requests = server.requests.lock().expect("captured query requests");
    let actual = json!({"query_callback_calls":calls.load(Ordering::SeqCst),
        "completion_submissions":requests.iter().filter(|request| request.path == "/api/worker/query-tasks/expired-query/complete").count(),
        "failure_submissions":requests.iter().filter(|request| request.path == "/api/worker/query-tasks/expired-query/fail").count()});
    fixture_matches(&fixture["expected"], &actual, "expired query dispatch")?;
    Ok(actual)
}

/// Run the redrive successor's first non-reused activity, which receives the
/// successor's cooperative cancellation, then schedule a shielded cleanup
/// activity whose arguments carry `observed` and the remaining cleanup budget.
// The SDK workflow handler signature returns the SDK error type.
#[allow(clippy::result_large_err)]
async fn redrive_successor_cancellation_cleanup(
    ctx: &WorkflowContext,
    activity_type: &str,
    arguments: Value,
    mut observed: Vec<Value>,
) -> durable_workflow::Result<Value> {
    let first = ctx.activity(activity_type, arguments).await;
    let Err(Error::CooperativeCancellationRequested(_)) = first else {
        return Err(Error::WorkerLoop(format!(
            "expected the successor cancellation at the first activity, observed {first:?}"
        )));
    };
    let context = ctx
        .cancellation_context()?
        .expect("committed rich cancellation");
    let _shield = ctx.cancellation_shield()?;
    observed.push(json!(context.remaining()?.as_secs_f64()));
    ctx.activity("corpus.redrive.cleanup", Value::Array(observed))
        .await?;
    Ok(json!("unexpected completion"))
}

async fn execute_fixture(fixture: &Value) -> Result<Value, String> {
    execute_fixture_delivery(fixture, "default").await
}

#[tokio::test]
async fn checked_in_replay_regression_corpus_uses_official_worker_replay() {
    let paths = fixture_paths().expect("discover declared Rust replay fixtures");
    assert!(
        !paths.is_empty(),
        "Rust replay fixture selectors must resolve to durable evidence"
    );
    for path in paths {
        let fixture: Value = serde_json::from_str(
            &fs::read_to_string(&path)
                .unwrap_or_else(|error| panic!("read {}: {error}", path.display())),
        )
        .unwrap_or_else(|error| panic!("parse {}: {error}", path.display()));
        let result = execute_fixture(&fixture).await;
        if let Some(expected_failure) = fixture["expected_failure"].as_str() {
            let error = result.expect_err("frozen malformed task codec must fail closed");
            assert!(error.contains(expected_failure), "{error}");
        } else if contains_json_payload_codec(&fixture) {
            let error = result.expect_err("frozen JSON-tagged replay must fail closed");
            assert!(error.contains("unsupported_payload_codec"), "{error}");
            assert!(error.contains("HTTP document transport"), "{error}");
        } else {
            result.unwrap_or_else(|error| panic!("{}: {error}", path.display()));
        }
    }
}

#[tokio::test]
async fn typed_entry_refusal_before_history_preserves_the_task_claim() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/replay-regressions/typed-entry-input-refusal.json");
    let fixture: Value = serde_json::from_str(&fs::read_to_string(path).unwrap()).unwrap();
    execute_fixture(&fixture)
        .await
        .expect("typed nonterminal entry refusal");
}

#[tokio::test]
async fn typed_entry_refusal_with_cold_history_preserves_the_task_claim() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/replay-regressions/typed-entry-cold-history-refusal.json");
    let fixture: Value = serde_json::from_str(&fs::read_to_string(path).unwrap()).unwrap();
    execute_fixture(&fixture)
        .await
        .expect("typed nonterminal cold refusal");
}

#[tokio::test]
async fn published_php_explicit_backoff_replays_across_cold_workers() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/replay-regressions/published-php-complete-explicit-backoff.json");
    let fixture: Value = serde_json::from_str(&fs::read_to_string(path).unwrap()).unwrap();
    let first = execute_fixture_delivery(&fixture, "first").await.unwrap();
    let cold = execute_fixture_delivery(&fixture, "cold").await.unwrap();
    assert_eq!(first, cold);
}

#[tokio::test]
async fn avro_side_effect_replay_is_deterministic_across_cold_workers() {
    let fixture_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/replay-regressions/side-effect-version-cold-replay-avro.json");
    let fixture: Value = serde_json::from_str(
        &fs::read_to_string(fixture_path).expect("read checked-in replay fixture"),
    )
    .expect("parse checked-in replay fixture");
    let first = execute_fixture(&fixture)
        .await
        .expect("first Avro replay fixture must execute");
    let second = execute_fixture(&fixture)
        .await
        .expect("cold Avro replay fixture must execute");

    assert_eq!(first, second);
}

#[tokio::test]
async fn sticky_hint_replays_recorded_values_identically_after_cache_loss() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/replay-regressions/sticky-cache-start-accepted-cold-replay.json");
    let fixture: Value = serde_json::from_str(&fs::read_to_string(path).unwrap()).unwrap();
    let first = execute_fixture_delivery(&fixture, "original-worker")
        .await
        .unwrap();
    let replacement = execute_fixture_delivery(&fixture, "replacement-worker")
        .await
        .unwrap();
    assert_eq!(first, replacement);
}

#[tokio::test]
async fn recorded_activity_failure_boundary_is_stable_across_cold_workers() {
    let fixture_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/replay-regressions/redrive-activity-failure-boundary.json");
    let fixture: Value = serde_json::from_str(
        &fs::read_to_string(fixture_path).expect("read checked-in redrive fixture"),
    )
    .expect("parse checked-in redrive fixture");

    let first = execute_fixture_delivery(&fixture, "delivery-a")
        .await
        .expect("first failed-activity replay must execute");
    let restarted = execute_fixture_delivery(&fixture, "delivery-b")
        .await
        .expect("cold failed-activity replay must execute");

    assert_eq!(first, restarted);
}

async fn redrive_reused_fixture_replays_identically_on_cold_workers(
    name: &str,
    reused_event_types: &[&str],
) -> Value {
    let fixture_path = redrive_successor_fixture_path(name);
    let fixture: Value = serde_json::from_str(
        &fs::read_to_string(&fixture_path).expect("read checked-in redrive reuse fixture"),
    )
    .expect("parse checked-in redrive reuse fixture");
    let history = fixture["history"]
        .as_array()
        .expect("redrive reuse fixture history");
    for event_type in reused_event_types {
        let events: Vec<&Value> = history
            .iter()
            .filter(|event| event["event_type"] == *event_type)
            .collect();
        assert!(!events.is_empty(), "{name} must contain {event_type}");
        for event in events {
            assert!(
                event["payload"]["reused_from_run_id"].is_string()
                    && event["payload"]["reused_recorded_at"].is_string(),
                "{name} {event_type} must carry both redrive reuse keys"
            );
        }
    }

    let first = execute_fixture_delivery(&fixture, "delivery-a")
        .await
        .unwrap_or_else(|error| panic!("{name} first replay: {error}"));
    let restarted = execute_fixture_delivery(&fixture, "delivery-b")
        .await
        .unwrap_or_else(|error| panic!("{name} cold replay: {error}"));
    assert_eq!(first, restarted);
    first
}

#[tokio::test]
async fn redriven_reused_side_effect_and_version_marker_replay_without_reexecution() {
    assert_valid_redrive_successor("redrive-successor-reused-markers.json", 4);
    let observed = redrive_reused_fixture_replays_identically_on_cold_workers(
        "redrive-successor-reused-markers.json",
        &[
            "SideEffectRecorded",
            "VersionMarkerRecorded",
            "ActivityCompleted",
        ],
    )
    .await;
    // The workflow fails unless the reused token and version reach it.
    assert_eq!(observed["side_effect_callback_calls"], json!(0));
    let commands = observed["command_sequence"].as_array().expect("commands");
    assert_eq!(commands.len(), 1, "{commands:?}");
    assert_eq!(
        commands[0]["activity_type"],
        "Tests\\Fixtures\\V2\\TestRedriveSecondActivity"
    );
    assert_eq!(observed["arguments"], json!(["Hello, Taylor!"]));
}

#[tokio::test]
async fn redriven_reused_fired_timer_does_not_wait_again() {
    assert_valid_redrive_successor("redrive-successor-reused-timer.json", 3);
    let observed = redrive_reused_fixture_replays_identically_on_cold_workers(
        "redrive-successor-reused-timer.json",
        &["ActivityCompleted", "TimerFired"],
    )
    .await;
    // No new timer: the next command is the failed step's activity.
    let commands = observed["command_sequence"].as_array().expect("commands");
    assert_eq!(commands.len(), 1, "{commands:?}");
    assert_eq!(commands[0]["type"], "schedule_activity");
    assert_eq!(
        commands[0]["activity_type"],
        "Tests\\Fixtures\\V2\\TestRedriveSecondActivity"
    );
    assert_eq!(observed["arguments"], json!(["Hello, Taylor!"]));
}

#[tokio::test]
async fn workflow_stream_commands_are_stable_across_cold_worker_redelivery() {
    let fixture_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/replay-regressions/workflow-stream-command-restart.json");
    let fixture: Value = serde_json::from_str(
        &fs::read_to_string(fixture_path).expect("read checked-in workflow stream fixture"),
    )
    .expect("parse checked-in workflow stream fixture");

    let first = execute_fixture_delivery(&fixture, "delivery-a")
        .await
        .expect("first workflow stream delivery must execute");
    let restarted = execute_fixture_delivery(&fixture, "delivery-b")
        .await
        .expect("cold workflow stream redelivery must execute");

    assert_eq!(first, restarted);
}

#[tokio::test]
async fn typed_search_attribute_type_mismatch_is_stable_across_cold_workers() {
    let fixture_path = Path::new(env!("CARGO_MANIFEST_DIR")).join(
        "tests/fixtures/replay-regressions/typed-search-attribute-keyword-string-mismatch.json",
    );
    let fixture: Value = serde_json::from_str(
        &fs::read_to_string(fixture_path).expect("read checked-in typed search fixture"),
    )
    .expect("parse checked-in typed search fixture");

    let first = execute_fixture_delivery(&fixture, "delivery-a")
        .await
        .expect_err("same-value type drift must fail the first replay");
    let restarted = execute_fixture_delivery(&fixture, "delivery-b")
        .await
        .expect_err("same-value type drift must fail after a cold worker reload");

    assert_eq!(first, restarted);
    assert!(first.contains("search_attribute_type_mismatch"), "{first}");
}

#[tokio::test]
async fn workflow_stream_command_without_durable_identity_fails_closed() {
    let fixture_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/replay-regressions/workflow-stream-command-restart.json");
    let mut fixture: Value = serde_json::from_str(
        &fs::read_to_string(fixture_path).expect("read checked-in workflow stream fixture"),
    )
    .expect("parse checked-in workflow stream fixture");
    fixture["id"] = json!("rust-workflow-stream-command-missing-identity");
    fixture["worker_task"]
        .as_object_mut()
        .expect("workflow stream worker task")
        .remove("workflow_command_id");
    fixture["worker_task"]["task_id"] = json!("");

    let error = execute_fixture_delivery(&fixture, "missing-identity")
        .await
        .expect_err("workflow stream command without durable identity must fail");

    assert!(
        error.contains("workflow_stream_command_identity_missing"),
        "{error}"
    );
    assert!(error.contains("workflow_command_id"), "{error}");
}

#[tokio::test]
async fn workflow_stream_task_identity_is_stable_across_cold_worker_redelivery() {
    let fixture_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/replay-regressions/workflow-stream-task-identity.json");
    let fixture: Value = serde_json::from_str(
        &fs::read_to_string(fixture_path).expect("read ordinary-task stream fixture"),
    )
    .expect("parse ordinary-task stream fixture");
    let first = execute_fixture_delivery(&fixture, "delivery-a")
        .await
        .expect("first ordinary task delivery");
    let restarted = execute_fixture_delivery(&fixture, "delivery-b")
        .await
        .expect("replacement ordinary task delivery");
    assert_eq!(first, restarted);
}

#[tokio::test]
async fn adjacent_condition_wait_occurrences_are_deterministic_across_cold_workers() {
    let fixture_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/replay-regressions/condition-wait-occurrence-adjacent-cold-replay-avro.json");
    let fixture: Value = serde_json::from_str(
        &fs::read_to_string(fixture_path).expect("read checked-in condition replay fixture"),
    )
    .expect("parse checked-in condition replay fixture");
    let first = execute_fixture(&fixture)
        .await
        .expect("first condition replay fixture must execute");
    let second = execute_fixture(&fixture)
        .await
        .expect("cold condition replay fixture must execute");

    assert_eq!(first, second);
}

#[tokio::test]
async fn durable_selection_winner_is_deterministic_across_cold_workers() {
    let fixture_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/replay-regressions/durable-selection-recorded-winner.json");
    let fixture: Value = serde_json::from_str(
        &fs::read_to_string(fixture_path).expect("read checked-in selection replay fixture"),
    )
    .expect("parse checked-in selection replay fixture");
    let first = execute_fixture_delivery(&fixture, "delivery-a")
        .await
        .expect("first selection replay fixture must execute");
    let restarted = execute_fixture_delivery(&fixture, "delivery-b")
        .await
        .expect("cold selection replay fixture must execute");

    assert_eq!(first, restarted);
}

#[tokio::test]
async fn portable_affinity_refusals_survive_durable_selection_cold_replay() {
    let fixture_path = Path::new(env!("CARGO_MANIFEST_DIR")).join(
        "tests/fixtures/replay-regressions/durable-selection-portable-affinity-cold-replay.json",
    );
    let fixture: Value = serde_json::from_str(
        &fs::read_to_string(fixture_path)
            .expect("read checked-in portable-affinity selection fixture"),
    )
    .expect("parse checked-in portable-affinity selection fixture");
    let first = execute_fixture_delivery(&fixture, "delivery-a")
        .await
        .expect("first portable-affinity selection replay must execute");
    let restarted = execute_fixture_delivery(&fixture, "delivery-b")
        .await
        .expect("cold portable-affinity selection replay must execute");

    assert_eq!(first, restarted);
}

#[tokio::test]
async fn selection_cancellation_request_completes_the_original_task_after_cold_replay() {
    let fixture: Value = serde_json::from_str(
        &fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join(
            "tests/fixtures/replay-regressions/selection-cancellation-request-completes.json",
        ))
        .expect("read cancellation request fixture"),
    )
    .expect("parse cancellation request fixture");
    let first = execute_fixture(&fixture)
        .await
        .expect("cancel and complete original task");
    let cold = execute_fixture(&fixture)
        .await
        .expect("cold worker cancels and completes");
    assert_eq!(first, cold);
}

#[tokio::test]
async fn non_envelope_side_effect_value_is_rejected_by_official_worker() {
    let fixture_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/replay-regressions/side-effect-version-cold-replay-avro.json");
    let mut fixture: Value = serde_json::from_str(
        &fs::read_to_string(fixture_path).expect("read checked-in replay fixture"),
    )
    .expect("parse checked-in replay fixture");
    fixture["id"] = json!("rust-side-effect-version-cold-replay-non-envelope");
    fixture["history"][0]["payload"]["result"] = json!({"captured": "once"});

    let error = execute_fixture(&fixture)
        .await
        .expect_err("raw side-effect values must not execute as published replay evidence");

    assert!(error.contains("unsupported_payload_codec"), "{error}");
    assert!(error.contains("untagged durable payload"), "{error}");
}

fn contains_json_payload_codec(value: &Value) -> bool {
    match value {
        Value::Object(object) => object.iter().any(|(key, value)| {
            (matches!(key.as_str(), "codec" | "payload_codec") && value == "json")
                || contains_json_payload_codec(value)
        }),
        Value::Array(items) => items.iter().any(contains_json_payload_codec),
        _ => false,
    }
}

#[tokio::test]
async fn unconsumed_replay_fixture_cannot_satisfy_the_corpus() {
    let fixture = json!({
        "$schema": "https://example.invalid/replay-regression.json",
        "fixture_schema": FIXTURE_SCHEMA,
        "id": "unconsumed-replay-evidence",
        "protocol_version": "1.2",
        "bindings": ["rust"],
        "workflow": {
            "type": "corpus.unimplemented",
            "input": []
        },
        "command_sequence": [
            {"type": "complete_workflow"}
        ],
        "expected": {
            "type": "complete_workflow"
        }
    });

    let error = execute_fixture(&fixture)
        .await
        .expect_err("structurally valid but unconsumed evidence must fail");
    assert!(error.contains("has no registered Rust workflow"));
}

/// In a valid successor the reused activity completion and fired timer precede
/// the successor's cancellation delivery, so the cancellation clock starts
/// after them. Neither their recorded time nor their original
/// `reused_recorded_at` can reach `remaining()`; this is not evidence that the
/// clock prefers `reused_recorded_at`, which no valid history can observe.
#[tokio::test]
async fn redriven_reused_activity_and_timer_never_reach_remaining() {
    assert_valid_redrive_successor("redrive-successor-reused-cancellation-clock.json", 3);
    let observed = redrive_reused_fixture_replays_identically_on_cold_workers(
        "redrive-successor-reused-cancellation-clock.json",
        &["ActivityCompleted", "TimerFired"],
    )
    .await;
    // The second activity received the cancellation: the only new command is
    // the shielded cleanup, and nothing reused is scheduled again.
    let commands = observed["command_sequence"].as_array().expect("commands");
    assert_eq!(commands.len(), 1, "{commands:?}");
    assert_eq!(commands[0]["activity_type"], "corpus.redrive.cleanup");
    // remaining() is the deadline (00:05:31) less the delivery (00:05:02).
    assert_eq!(observed["arguments"], json!([29.0]));
    assert_reused_times_do_not_reach_remaining(
        "redrive-successor-reused-cancellation-clock.json",
        &["ActivityCompleted", "TimerFired"],
    )
    .await;
}

/// Redrive fixtures that are not histories a Workflow 2.19 redrive can write
/// (sdk-rust#174). They stay immutable and still replay in the corpus, but they
/// are not qualification evidence: each must keep failing the successor check
/// with its named error, and the valid successor fixture named beside it
/// carries its claim instead.
const NON_SUCCESSOR_REDRIVE_FIXTURES: &[(&str, &str, &str)] = &[
    // Historical replay evidence omitted Core-written cancellation fields.
    // Keep those files and replay outcomes; complete counterparts qualify the claim.
    (
        "redrive-reused-upserts-clock.json",
        "workflow_instance_id",
        "redrive-successor-reused-upserts-cancellation-clock.json",
    ),
    (
        "redrive-reused-signal-wait-clock.json",
        "workflow_instance_id",
        "redrive-successor-reused-signal-cancellation-clock.json",
    ),
    // Shipped in 4.11.1: no redrive WorkflowStarted, and the cancellation
    // before the reused prefix.
    (
        "redrive-reused-cancellation-clock.json",
        "history must open with WorkflowStarted",
        "redrive-successor-reused-cancellation-clock.json",
    ),
    // Shipped in 4.11.1 with an empty or partial redrive envelope.
    (
        "redrive-reused-markers-clock.json",
        "recovery_kind must be redrive",
        "redrive-successor-reused-markers.json",
    ),
    (
        "redrive-reused-timer-clock.json",
        "replayed_started_at must be a valid time",
        "redrive-successor-reused-timer.json",
    ),
];

/// Failed source runs that a redrive starts from. They are not successors.
const REDRIVE_SOURCE_FIXTURES: &[&str] = &["redrive-activity-failure-boundary.json"];

/// Complete cancellation metadata for existing replay scenarios. These are
/// successor-evidence vectors, not distinct replay semantics or corpus growth.
const REDRIVE_COMPLETION_FIXTURES: &[(&str, &str)] = &[
    (
        "redrive-successor-reused-upserts-cancellation-clock.json",
        "db8b1321953b0683b69e96109183622802d1894a83adc00f5ec6c25e02f365ac",
    ),
    (
        "redrive-successor-reused-signal-cancellation-clock.json",
        "7994edd538ba49f689dfa5f0ac63647c4ef435968806c2ff6dbf378c95ecd40f",
    ),
];

fn redrive_successor_fixture_path(name: &str) -> PathBuf {
    let directory = if REDRIVE_COMPLETION_FIXTURES
        .iter()
        .any(|(file, _)| *file == name)
    {
        "tests/fixtures/redrive-successor-evidence"
    } else {
        "tests/fixtures/replay-regressions"
    };
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join(directory)
        .join(name)
}

/// The valid successor fixtures and the step each resumes at.
const REDRIVE_SUCCESSOR_FIXTURES: &[(&str, u64)] = &[
    ("redrive-successor-reused-markers.json", 4),
    ("redrive-successor-reused-timer.json", 3),
    ("redrive-successor-reused-cancellation-clock.json", 3),
    (
        "redrive-successor-reused-upserts-cancellation-clock.json",
        3,
    ),
    ("redrive-successor-reused-signal-cancellation-clock.json", 2),
];

fn mentions_redrive(fixture: &Value) -> bool {
    fixture["history"].as_array().is_some_and(|history| {
        history.iter().any(|event| {
            event["payload"].get("reused_from_run_id").is_some()
                || event["payload"].get("reused_recorded_at").is_some()
                || event["payload"]["recovery_kind"] == "redrive"
        })
    })
}

#[test]
fn every_redrive_fixture_is_a_valid_successor_or_a_named_exception() {
    let directory = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/replay-regressions");
    let mut checked = Vec::new();
    let evidence =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/redrive-successor-evidence");
    for (name, sha256) in REDRIVE_COMPLETION_FIXTURES {
        assert_eq!(
            format!(
                "{:x}",
                Sha256::digest(fs::read(evidence.join(name)).unwrap())
            ),
            *sha256,
            "immutable successor evidence {name}"
        );
    }
    for entry in fs::read_dir(&directory)
        .unwrap()
        .chain(fs::read_dir(&evidence).unwrap())
    {
        let path = entry.unwrap().path();
        let name = path.file_name().unwrap().to_str().unwrap().to_string();
        if path.extension().and_then(|extension| extension.to_str()) != Some("json") {
            continue;
        }
        let fixture = redrive_successor_fixture(&name);
        if !name.starts_with("redrive-") && !mentions_redrive(&fixture) {
            continue;
        }
        if REDRIVE_SOURCE_FIXTURES.contains(&name.as_str()) {
            assert!(!mentions_redrive(&fixture), "{name} is a source run");
            continue;
        }
        if let Some((_, error, successor)) = NON_SUCCESSOR_REDRIVE_FIXTURES
            .iter()
            .find(|(historical, _, _)| *historical == name)
        {
            let failure = check_redrive_successor(&fixture)
                .expect_err(&format!("{name} is listed as no valid successor"));
            assert!(failure.contains(error), "{name}: {failure}");
            check_redrive_successor(&redrive_successor_fixture(successor))
                .unwrap_or_else(|error| panic!("{successor}: {error}"));
            continue;
        }
        check_redrive_successor(&fixture).unwrap_or_else(|error| panic!("{name}: {error}"));
        checked.push(name);
    }
    checked.sort();
    let mut expected: Vec<String> = REDRIVE_SUCCESSOR_FIXTURES
        .iter()
        .map(|(name, _)| name.to_string())
        .collect();
    expected.sort();
    assert_eq!(checked, expected, "valid redrive successor fixtures");
}

/// Change one valid successor fixture and renumber its history.
fn changed_successor(name: &str, change: impl FnOnce(&mut Value)) -> Value {
    let mut fixture = redrive_successor_fixture(name);
    change(&mut fixture);
    for (index, event) in fixture["history"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .enumerate()
    {
        event["sequence"] = json!(index + 1);
    }
    fixture
}

/// Change one valid successor so it is no longer a history redrive can write.
fn assert_successor_check_rejects(name: &str, error: &str, change: impl FnOnce(&mut Value)) {
    let failure = check_redrive_successor(&changed_successor(name, change))
        .expect_err(&format!("{name} changed to fail with {error}"));
    assert!(failure.contains(error), "{name}: {failure}");
}

/// Change one valid successor into another history redrive can write.
fn assert_successor_check_accepts(name: &str, change: impl FnOnce(&mut Value)) {
    check_redrive_successor(&changed_successor(name, change))
        .unwrap_or_else(|error| panic!("{name} changed: {error}"));
}

/// The event of this type in a fixture history.
fn history_event<'a>(fixture: &'a mut Value, event_type: &str) -> &'a mut Value {
    fixture["history"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|event| event["event_type"] == event_type)
        .unwrap_or_else(|| panic!("history must contain {event_type}"))
}

/// A successor event recorded after the reused prefix.
fn successor_event(event_type: &str, recorded_at: &str, payload: Value) -> Value {
    json!({"sequence": 0, "event_type": event_type, "recorded_at": recorded_at, "payload": payload})
}

const CANCELLATION_SUCCESSOR: &str = "redrive-successor-reused-cancellation-clock.json";
const SIGNAL_SUCCESSOR: &str = "redrive-successor-reused-signal-cancellation-clock.json";
const SOURCE_RUN: &str = "01JBREDRIVESOURCERUN000000";

/// The cancellation successor with its second activity (step 3) completed by
/// the successor itself, and the cancellation delivered to `delivered_step`.
fn later_step_cancellation(fixture: &mut Value, delivered_step: u64) {
    let history = fixture["history"].as_array_mut().unwrap();
    let activity = json!({
        "activity_execution_id": "01JBREDRIVESUCCESSORACT001",
        "activity_type": "Tests\\Fixtures\\V2\\TestRedriveSecondActivity",
        "sequence": 3,
    });
    let mut completed = activity.clone();
    completed["result"] = json!("wwHioz3/VYAiNwocSGVsbG8sIFRheWxvciE=");
    completed["payload_codec"] = json!("avro");
    history.splice(
        4..4,
        [
            successor_event("ActivityScheduled", "2026-11-03T00:05:00+00:00", activity),
            successor_event("ActivityCompleted", "2026-11-03T00:05:01+00:00", completed),
        ],
    );
    let delivery = history.last_mut().unwrap();
    delivery["payload"]["sequence"] = json!(delivered_step);
}

#[test]
fn redrive_successor_check_accepts_valid_successor_variants() {
    // Delivery to a later step the successor ran itself, not only the first
    // non-reused step.
    assert_successor_check_accepts(CANCELLATION_SUCCESSOR, |fixture| {
        later_step_cancellation(fixture, 4);
    });
    // Bounded deadlines, each at or after the start it bounds.
    assert_successor_check_accepts(CANCELLATION_SUCCESSOR, |fixture| {
        let start = &mut fixture["history"][0]["payload"];
        start["execution_deadline_at"] = json!("2026-11-03T00:00:00+00:00");
        start["run_deadline_at"] = json!("2026-11-03T00:10:00.000000Z");
    });
    // SignalReceived has no workflow step: a sequence injected into it does
    // not move the expected resume step (still 2) ...
    assert_successor_check_accepts(SIGNAL_SUCCESSOR, |fixture| {
        history_event(fixture, "SignalReceived")["payload"]["sequence"] = json!(99);
    });
    // ... and cannot justify a resume step after it.
    assert_successor_check_rejects(SIGNAL_SUCCESSOR, "resume_step_sequence", |fixture| {
        history_event(fixture, "SignalReceived")["payload"]["sequence"] = json!(99);
        fixture["history"][0]["payload"]["resume_step_sequence"] = json!(100);
    });
    // A step-bearing reused event with that sequence is not ignored.
    assert_successor_check_rejects(SIGNAL_SUCCESSOR, "in order from 1", |fixture| {
        history_event(fixture, "SignalApplied")["payload"]["sequence"] = json!(99);
    });
}

#[test]
fn redrive_successor_check_refuses_unmodeled_delivery_families() {
    for kind in ["condition", "child", "service_operation"] {
        let fixture = changed_successor(CANCELLATION_SUCCESSOR, |fixture| {
            history_event(fixture, "CooperativeCancellationDelivered")["payload"]["call_kind"] =
                json!(kind);
        });
        assert!(
            check_redrive_successor(&fixture).is_err(),
            "the single-call evidence model must explicitly refuse {kind}"
        );
    }
}

#[test]
fn redrive_successor_check_accepts_failed_and_timed_out_activity_progress() {
    for terminal in ["ActivityFailed", "ActivityTimedOut"] {
        let fixture = changed_successor(CANCELLATION_SUCCESSOR, |fixture| {
            later_step_cancellation(fixture, 4);
            let event = fixture["history"]
                .as_array_mut()
                .unwrap()
                .iter_mut()
                .find(|event| {
                    event["event_type"] == "ActivityCompleted" && event["payload"]["sequence"] == 3
                })
                .unwrap();
            event["event_type"] = json!(terminal);
            event["payload"].as_object_mut().unwrap().remove("result");
            event["payload"]
                .as_object_mut()
                .unwrap()
                .remove("payload_codec");
        });
        assert!(
            check_redrive_successor(&fixture).is_ok(),
            "a matching {terminal} must close the earlier call"
        );
    }
}

fn successor_timer_progress(fixture: &mut Value, terminal: &str) {
    later_step_cancellation(fixture, 4);
    for event in fixture["history"].as_array_mut().unwrap() {
        if event["payload"]["sequence"] != 3 {
            continue;
        }
        let kind = event["event_type"].as_str().unwrap();
        if matches!(kind, "ActivityScheduled" | "ActivityCompleted") {
            event["event_type"] = json!(if kind == "ActivityScheduled" {
                "TimerScheduled"
            } else {
                terminal
            });
            event["payload"] = json!({"sequence": 3, "timer_id": "successor-timer"});
        }
    }
}

#[test]
fn redrive_successor_check_accepts_cancelled_plain_timer_progress() {
    let fixture = changed_successor(CANCELLATION_SUCCESSOR, |fixture| {
        successor_timer_progress(fixture, "TimerCancelled");
    });
    assert!(
        check_redrive_successor(&fixture).is_ok(),
        "a matching TimerCancelled must close the earlier plain timer"
    );
}

#[test]
fn redrive_successor_check_refuses_timeout_terminals_for_plain_timers() {
    for (kind, wait_id) in [
        ("condition_timeout", "condition_wait_id"),
        ("signal_timeout", "signal_wait_id"),
    ] {
        let fixture = changed_successor(CANCELLATION_SUCCESSOR, |fixture| {
            successor_timer_progress(fixture, "TimerFired");
            let event = fixture["history"]
                .as_array_mut()
                .unwrap()
                .iter_mut()
                .find(|event| {
                    event["event_type"] == "TimerFired" && event["payload"]["sequence"] == 3
                })
                .unwrap();
            event["payload"]["timer_kind"] = json!(kind);
            event["payload"][wait_id] = json!("internal-wait");
        });
        assert!(
            check_redrive_successor(&fixture).is_err(),
            "{kind} cannot resolve a plain timer merely by sharing timer_id"
        );
    }
}

#[test]
fn redrive_successor_check_binds_activity_terminal_type() {
    let fixture = changed_successor(CANCELLATION_SUCCESSOR, |fixture| {
        later_step_cancellation(fixture, 4);
        let event = fixture["history"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|event| {
                event["event_type"] == "ActivityCompleted" && event["payload"]["sequence"] == 3
            })
            .unwrap();
        event["payload"]["activity_type"] = json!("another.activity");
    });
    assert!(
        check_redrive_successor(&fixture).is_err(),
        "a foreign activity type cannot justify reaching the next step"
    );
}

fn successor_signal_progress(fixture: &mut Value) {
    later_step_cancellation(fixture, 4);
    let instance = fixture["history"][0]["payload"]["workflow_instance_id"].clone();
    let run = fixture["history"][0]["payload"]["workflow_run_id"].clone();
    let history = fixture["history"].as_array_mut().unwrap();
    for event in history.iter_mut() {
        if event["payload"]["sequence"] != 3 {
            continue;
        }
        if event["event_type"] == "ActivityScheduled" {
            event["event_type"] = json!("SignalWaitOpened");
            event["payload"] =
                json!({"sequence": 3, "signal_wait_id": "successor-wait", "signal_name": "ready"});
        } else if event["event_type"] == "ActivityCompleted" {
            event["event_type"] = json!("SignalApplied");
            event["payload"] = json!({"sequence": 3, "signal_wait_id": "successor-wait", "signal_name": "ready",
                "workflow_command_id": "successor-signal-command", "signal_id": "successor-signal"});
        }
    }
    let applied = history
        .iter()
        .position(|event| event["event_type"] == "SignalApplied")
        .unwrap();
    history.insert(
        applied,
        successor_event(
            "SignalReceived",
            "2026-11-03T00:05:01+00:00",
            json!({
            "workflow_instance_id": instance, "workflow_run_id": run,
            "workflow_command_id": "successor-signal-command", "signal_id": "successor-signal",
            "signal_wait_id": "successor-wait", "signal_name": "ready"}),
        ),
    );
}

#[test]
fn redrive_successor_check_refuses_duplicate_or_contradictory_scalar_terminals() {
    let mut admitted = Vec::new();
    for shape in [
        "remote-failed-completed",
        "remote-completed-twice",
        "local-failed-completed",
        "local-completed-twice",
        "timer-cancelled-fired",
        "timer-fired-twice",
        "untimed-signal-applied-twice",
    ] {
        let mut fixture = changed_successor(CANCELLATION_SUCCESSOR, |fixture| {
            if shape.starts_with("timer") {
                successor_timer_progress(fixture, "TimerFired");
            } else if shape.starts_with("untimed-signal") {
                successor_signal_progress(fixture);
            } else {
                later_step_cancellation(fixture, 4);
                for event in fixture["history"].as_array_mut().unwrap() {
                    if event["payload"]["sequence"] == 3
                        && matches!(
                            event["event_type"].as_str(),
                            Some("ActivityScheduled" | "ActivityCompleted")
                        )
                    {
                        event["payload"]["local_activity"] = json!(shape.starts_with("local"));
                    }
                }
            }
        });
        check_redrive_successor(&fixture)
            .unwrap_or_else(|error| panic!("single-terminal positive {shape}: {error}"));
        let history = fixture["history"].as_array_mut().unwrap();
        let terminal = history
            .iter()
            .position(|event| {
                event["payload"]["sequence"] == 3
                    && matches!(
                        event["event_type"].as_str(),
                        Some("ActivityCompleted" | "TimerFired" | "SignalApplied")
                    )
            })
            .unwrap();
        let duplicate = history[terminal].clone();
        if shape.ends_with("failed-completed") {
            history[terminal]["event_type"] = json!("ActivityFailed");
            let payload = history[terminal]["payload"].as_object_mut().unwrap();
            payload.remove("result");
            payload.remove("payload_codec");
            payload.insert("message".into(), json!("activity failed"));
        } else if shape == "timer-cancelled-fired" {
            history[terminal]["event_type"] = json!("TimerCancelled");
        }
        history.insert(terminal + 1, duplicate);
        for (index, event) in history.iter_mut().enumerate() {
            event["sequence"] = json!(index + 1);
        }
        if check_redrive_successor(&fixture).is_ok() {
            admitted.push(shape);
        }
    }
    assert!(
        admitted.is_empty(),
        "invalid terminal multiplicity admitted: {admitted:?}"
    );
}

#[test]
fn redrive_successor_check_rejects_impossible_histories() {
    let name = CANCELLATION_SUCCESSOR;
    // The 4.11.1 order: the successor's cancellation before the reused prefix.
    assert_successor_check_rejects(name, "directly follow WorkflowStarted", |fixture| {
        let history = fixture["history"].as_array_mut().unwrap();
        let cancellation = history.split_off(4);
        history.splice(1..1, cancellation);
    });
    assert_successor_check_rejects(name, "predates the event before it", |fixture| {
        fixture["history"][4]["recorded_at"] = json!("2026-11-03T00:00:00+00:00");
    });
    assert_successor_check_rejects(name, "cleanup deadline predates", |fixture| {
        fixture["history"][4]["payload"]["cleanup_deadline_at"] =
            json!("2026-11-03T00:00:30.000000Z");
    });
    assert_successor_check_rejects(name, "after the reused prefix", |fixture| {
        fixture["history"][5]["payload"]["sequence"] = json!(1);
    });
    assert_successor_check_rejects(name, "after the reused prefix", |fixture| {
        later_step_cancellation(fixture, 2);
    });
    // Neither step 4 nor 5 is reached without the successor's own progress.
    assert_successor_check_rejects(name, "a step the successor has reached", |fixture| {
        fixture["history"][5]["payload"]["sequence"] = json!(4);
    });
    assert_successor_check_rejects(name, "a step the successor has reached", |fixture| {
        later_step_cancellation(fixture, 5);
    });
    assert_successor_check_rejects(name, "after its request", |fixture| {
        fixture["history"].as_array_mut().unwrap().swap(4, 5);
    });
    assert_successor_check_rejects(name, "must not reuse a reused step", |fixture| {
        let history = fixture["history"].as_array_mut().unwrap();
        history.insert(
            4,
            successor_event(
                "ActivityScheduled",
                "2026-11-03T00:05:00+00:00",
                json!({"sequence": 2, "activity_type": "tests.reused-step", "activity_execution_id": "invalid-reused-step"}),
            ),
        );
    });
    assert_successor_check_rejects(name, "recorded with the successor start", |fixture| {
        fixture["history"][1]["recorded_at"] = json!("2026-11-03T00:00:00+00:00");
    });
    assert_successor_check_rejects(name, "in order from 1", |fixture| {
        let history = fixture["history"].as_array_mut().unwrap();
        history.swap(1, 3);
        history.swap(1, 2);
    });
    let mut fixture = redrive_successor_fixture(name);
    fixture["history"][0]["sequence"] = json!(0);
    let error = check_redrive_successor(&fixture).unwrap_err();
    assert!(error.contains("at sequence 1"), "{error}");
    let mut fixture = redrive_successor_fixture(name);
    fixture["history"][2]["sequence"] = json!(9);
    let error = check_redrive_successor(&fixture).unwrap_err();
    assert!(error.contains("history order"), "{error}");
}

#[test]
fn redrive_successor_check_binds_the_full_start_envelope() {
    let name = CANCELLATION_SUCCESSOR;
    for key in REDRIVE_START_ENVELOPE {
        // Every envelope field Core writes must be present ...
        assert_successor_check_rejects(name, key, |fixture| {
            fixture["history"][0]["payload"]
                .as_object_mut()
                .unwrap()
                .remove(*key);
        });
        // ... and neither blank nor of the wrong type.
        for blank in [json!(""), json!("   "), json!(7)] {
            assert_successor_check_rejects(name, key, |fixture| {
                fixture["history"][0]["payload"][*key] = blank;
            });
        }
    }
    assert_successor_check_rejects(name, "recovery_kind", |fixture| {
        fixture["history"][0]["payload"]["recovery_kind"] = json!("continue_as_new");
    });
    assert_successor_check_rejects(name, "fixture workflow type", |fixture| {
        fixture["history"][0]["payload"]["workflow_type"] = json!("corpus.redrive-other");
    });
    assert_successor_check_rejects(name, "fixture workflow type", |fixture| {
        fixture["workflow"]["type"] = json!("corpus.redrive-other");
    });
    assert_successor_check_rejects(name, "workflow task must belong", |fixture| {
        fixture["worker_task"]["workflow_id"] = json!("another-instance");
    });
    assert_successor_check_rejects(name, "workflow task must belong", |fixture| {
        fixture["history"][0]["payload"]["workflow_run_id"] = json!("another-successor-run");
    });
    assert_successor_check_rejects(name, "name another run", |fixture| {
        fixture["history"][0]["payload"]["continued_from_run_id"] = json!("regression-corpus-run");
    });
    assert_successor_check_rejects(name, "replayed_started_at must not be later", |fixture| {
        fixture["history"][0]["payload"]["replayed_started_at"] =
            json!("2026-11-03T00:05:01+00:00");
    });
    for key in ["execution_deadline_at", "run_deadline_at"] {
        for invalid in [json!(""), json!("tomorrow"), json!(0)] {
            assert_successor_check_rejects(name, key, |fixture| {
                fixture["history"][0]["payload"][key] = invalid;
            });
        }
    }
    // A deadline before the start it bounds.
    assert_successor_check_rejects(name, "execution_deadline_at", |fixture| {
        fixture["history"][0]["payload"]["execution_deadline_at"] =
            json!("2026-11-02T23:59:59+00:00");
    });
    assert_successor_check_rejects(name, "run_deadline_at", |fixture| {
        fixture["history"][0]["payload"]["run_deadline_at"] = json!("2026-11-03T00:04:59+00:00");
    });
}

#[test]
fn redrive_successor_check_binds_every_event_identity() {
    let name = CANCELLATION_SUCCESSOR;
    // Reused events name their source run.
    for key in ["reused_from_run_id", "reused_recorded_at"] {
        for blank in [None, Some(json!("")), Some(json!(" ")), Some(json!(null))] {
            assert_successor_check_rejects(name, &format!("non-blank {key}"), |fixture| {
                let payload = &mut history_event(fixture, "ActivityCompleted")["payload"];
                match blank {
                    Some(blank) => payload[key] = blank,
                    None => {
                        payload.as_object_mut().unwrap().remove(key);
                    }
                }
            });
        }
    }
    assert_successor_check_rejects(name, "match continued_from_run_id", |fixture| {
        history_event(fixture, "TimerFired")["payload"]["reused_from_run_id"] =
            json!("another-run");
    });
    for time in ["2026-11-03T00:05:01+00:00", "2026-11-02T23:59:59+00:00"] {
        assert_successor_check_rejects(name, "fall in the source run", |fixture| {
            history_event(fixture, "TimerFired")["payload"]["reused_recorded_at"] = json!(time);
        });
    }
    assert_successor_check_rejects(SIGNAL_SUCCESSOR, "keeps the source run id", |fixture| {
        history_event(fixture, "SignalReceived")["payload"]["workflow_run_id"] =
            json!("regression-corpus-run");
    });
    assert_successor_check_rejects(SIGNAL_SUCCESSOR, "another workflow instance", |fixture| {
        history_event(fixture, "SignalReceived")["payload"]["workflow_instance_id"] =
            json!("another-instance");
    });

    // The successor cancellation binds its run, request and instance
    // positively, rather than passing by omission: these fields are required.
    let request = "CooperativeCancellationRequested";
    let delivery = "CooperativeCancellationDelivered";
    for (event_type, key, error) in [
        (request, "workflow_command_id", "workflow_command_id"),
        (request, "workflow_run_id", "workflow_run_id"),
        (request, "cleanup_deadline_at", "cleanup deadline"),
        (request, "cancellation", "cancellation context"),
        (delivery, "workflow_command_id", "workflow_command_id"),
        (delivery, "workflow_run_id", "workflow_run_id"),
        (delivery, "sequence", "positive workflow sequence"),
        (delivery, "call_kind", "call_kind"),
    ] {
        assert_successor_check_rejects(name, error, |fixture| {
            history_event(fixture, event_type)["payload"]
                .as_object_mut()
                .unwrap()
                .remove(key);
        });
        assert_successor_check_rejects(name, error, |fixture| {
            history_event(fixture, event_type)["payload"][key] = json!("");
        });
    }
    // Complete successor evidence includes every Core-written request field
    // and the delivery's copied context. Historical incomplete fixtures remain
    // frozen replay evidence, separately classified below.
    for (event_type, key, error) in [
        (request, "workflow_instance_id", "workflow_instance_id"),
        (request, "command_type", "command_type"),
        (delivery, "cancellation", "cancellation context"),
    ] {
        assert_successor_check_rejects(name, error, |fixture| {
            history_event(fixture, event_type)["payload"]
                .as_object_mut()
                .unwrap()
                .remove(key);
        });
        assert_successor_check_rejects(name, error, |fixture| {
            history_event(fixture, event_type)["payload"][key] = json!("");
        });
    }
    for (event_type, key, value, error) in [
        (
            request,
            "workflow_instance_id",
            json!("another-instance"),
            "workflow_instance_id",
        ),
        (
            request,
            "workflow_run_id",
            json!(SOURCE_RUN),
            "successor run",
        ),
        (request, "command_type", json!("cancel"), "command_type"),
        (
            delivery,
            "workflow_run_id",
            json!(SOURCE_RUN),
            "successor run",
        ),
        (
            delivery,
            "workflow_command_id",
            json!("another-request"),
            "name its request",
        ),
    ] {
        assert_successor_check_rejects(name, error, |fixture| {
            history_event(fixture, event_type)["payload"][key] = value;
        });
    }
    for (key, value, error) in [
        ("request_id", json!("another-request"), "this request"),
        (
            "root_workflow_run_id",
            json!(SOURCE_RUN),
            "root at the successor",
        ),
        (
            "root_workflow_instance_id",
            json!("another-instance"),
            "root at the successor",
        ),
    ] {
        assert_successor_check_rejects(name, error, |fixture| {
            history_event(fixture, request)["payload"]["cancellation"][key] = value.clone();
            history_event(fixture, delivery)["payload"]["cancellation"][key] = value;
        });
    }
    assert_successor_check_rejects(name, "lineage must end at the successor", |fixture| {
        for event_type in [request, delivery] {
            history_event(fixture, event_type)["payload"]["cancellation"]["lineage"][0]
                ["workflow_run_id"] = json!(SOURCE_RUN);
        }
    });
    assert_successor_check_rejects(name, "its request's cancellation context", |fixture| {
        history_event(fixture, delivery)["payload"]["cancellation"]["reason"] = json!("changed");
    });
}

/// Run a checked-in redrive fixture with its history changed and return the
/// commands it then emits.
async fn redrive_reused_commands_with_history(
    name: &str,
    change: impl FnOnce(&mut Vec<Value>),
) -> Result<Value, String> {
    let fixture_path = redrive_successor_fixture_path(name);
    let mut fixture: Value = serde_json::from_str(
        &fs::read_to_string(&fixture_path).expect("read checked-in redrive reuse fixture"),
    )
    .expect("parse checked-in redrive reuse fixture");
    change(fixture["history"].as_array_mut().expect("fixture history"));
    fixture.as_object_mut().unwrap().remove("command_sequence");
    fixture["expected"] = json!({"type": "schedule_activity"});
    let observed = execute_fixture_delivery(&fixture, "changed-history").await?;
    Ok(observed["command_sequence"].clone())
}

/// Retime each reused event, one at a time, to after the successor's
/// cancellation delivery (00:05:02) and before its deadline (00:05:31). Had any
/// of them reached the cancellation clock, remaining() would fall to 11s.
async fn assert_reused_times_do_not_reach_remaining(name: &str, reused_event_types: &[&str]) {
    let baseline = redrive_reused_commands_with_history(name, |_| {})
        .await
        .unwrap_or_else(|error| panic!("{name} baseline: {error}"));
    for event_type in reused_event_types {
        let commands = redrive_reused_commands_with_history(name, |history| {
            let event = history
                .iter_mut()
                .find(|event| event["event_type"] == *event_type)
                .unwrap_or_else(|| panic!("{name} must contain {event_type}"));
            event["recorded_at"] = json!("2026-11-03T00:05:20+00:00");
            event["payload"]["reused_recorded_at"] = json!("2026-11-03T00:05:20+00:00");
        })
        .await
        .unwrap_or_else(|error| panic!("{name} retimed {event_type}: {error}"));
        assert_eq!(commands, baseline, "{name} retimed {event_type}");
    }
}

fn redrive_successor_fixture(name: &str) -> Value {
    serde_json::from_str(&fs::read_to_string(redrive_successor_fixture_path(name)).unwrap())
        .unwrap()
}

/// The run id the corpus harness gives every fixture's workflow task.
const FIXTURE_RUN_ID: &str = "regression-corpus-run";

/// The redrive WorkflowStarted fields that name the workflow, the successor
/// and its source; each must be a non-blank string.
const REDRIVE_START_IDENTITY: &[&str] = &[
    "workflow_class",
    "workflow_type",
    "workflow_instance_id",
    "workflow_run_id",
    "workflow_command_id",
    "workflow_link_id",
    "continued_from_run_id",
];

/// Every field Core a2daf6a2 writes into a redrive WorkflowStarted
/// (DefaultWorkflowControlPlane::redrive()); the deadlines may be null.
const REDRIVE_START_ENVELOPE: &[&str] = &[
    "recovery_kind",
    "replayed_started_at",
    "workflow_class",
    "workflow_type",
    "workflow_instance_id",
    "workflow_run_id",
    "workflow_command_id",
    "workflow_link_id",
    "continued_from_run_id",
    "execution_deadline_at",
    "run_deadline_at",
    "resume_step_sequence",
];

fn redrive_event_fields(event: &Value) -> Result<(), String> {
    let kind = event["event_type"].as_str().unwrap_or_default();
    let fields: &[&str] = match kind {
        "MemoUpserted" | "SearchAttributesUpserted" | "SideEffectRecorded" => &["sequence"],
        "VersionMarkerRecorded" => &["sequence", "change_id"],
        "ActivityScheduled" | "ActivityCompleted" | "ActivityCancelled" => {
            &["sequence", "activity_type", "activity_execution_id"]
        }
        "TimerScheduled" | "TimerFired" => &["sequence", "timer_id"],
        "SignalWaitOpened" => &["sequence", "signal_name", "signal_wait_id"],
        "SignalReceived" => &[
            "workflow_command_id",
            "signal_id",
            "workflow_instance_id",
            "workflow_run_id",
            "signal_name",
            "signal_wait_id",
        ],
        "SignalApplied" => &[
            "workflow_command_id",
            "signal_id",
            "signal_name",
            "signal_wait_id",
            "sequence",
        ],
        "CooperativeCancellationRequested" => &[
            "workflow_command_id",
            "workflow_instance_id",
            "workflow_run_id",
        ],
        "CooperativeCancellationDelivered" => {
            &["workflow_command_id", "workflow_run_id", "sequence"]
        }
        _ => return Err(format!("unexpected successor evidence event {kind}")),
    };
    for key in fields {
        let value = &event["payload"][*key];
        let present = if *key == "sequence" {
            value.as_u64().is_some_and(|sequence| sequence > 0)
        } else {
            value.as_str().is_some_and(|value| !value.trim().is_empty())
        };
        if !present {
            return Err(if *key == "sequence" {
                format!("{kind} must carry a positive workflow sequence")
            } else {
                format!("{kind} must carry a non-blank {key}")
            });
        }
    }
    Ok(())
}

fn redrive_step_event(kind: &str) -> bool {
    matches!(
        kind,
        "MemoUpserted"
            | "SearchAttributesUpserted"
            | "SideEffectRecorded"
            | "VersionMarkerRecorded"
            | "ActivityScheduled"
            | "ActivityCompleted"
            | "ActivityCancelled"
            | "TimerScheduled"
            | "TimerFired"
            | "SignalWaitOpened"
            | "SignalApplied"
            | "CooperativeCancellationDelivered"
    )
}

fn redrive_requester_is_valid(value: &Value) -> bool {
    value.as_object().is_some_and(|requester| {
        !requester.is_empty()
            && requester.iter().all(|(key, value)| {
                matches!(key.as_str(), "type" | "id" | "label")
                    && value.as_str().is_some_and(|value| !value.is_empty())
            })
    })
}

/// Core 28893f97 root cancellation for one sequential call. Propagated scope,
/// group, selection and operation deliveries require separate evidence models.
fn check_successor_cancellation(
    events: &[Value],
    instance: &str,
    run: &str,
    resume: u64,
    started_at: chrono::DateTime<chrono::FixedOffset>,
) -> Result<(), String> {
    fn require(condition: bool, message: &str) -> Result<(), String> {
        if condition {
            Ok(())
        } else {
            Err(message.into())
        }
    }
    fn time(value: &Value, field: &str) -> Result<chrono::DateTime<chrono::FixedOffset>, String> {
        value
            .as_str()
            .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
            .ok_or_else(|| format!("invalid {field}"))
    }
    let requests: Vec<_> = events
        .iter()
        .filter(|event| event["event_type"] == "CooperativeCancellationRequested")
        .collect();
    let deliveries: Vec<_> = events
        .iter()
        .filter(|event| event["event_type"] == "CooperativeCancellationDelivered")
        .collect();
    if requests.is_empty() && deliveries.is_empty() {
        return Ok(());
    }
    require(
        requests.len() == 1 && deliveries.len() == 1,
        "successor cancellation is one request and one delivery",
    )?;
    let request = requests[0];
    let delivery = deliveries[0];
    require(
        request["sequence"].as_u64() < delivery["sequence"].as_u64(),
        "cancellation must be delivered after its request",
    )?;
    let asked = &request["payload"];
    let delivered = &delivery["payload"];
    let command = &asked["workflow_command_id"];
    require(
        asked["command_type"] == "request_cancellation",
        "request command_type must be request_cancellation",
    )?;
    require(
        delivered["workflow_command_id"] == *command,
        "cancellation delivery must name its request",
    )?;
    require(
        time(&asked["cleanup_deadline_at"], "cleanup deadline")?
            >= time(&request["recorded_at"], "request recorded_at")?,
        "cleanup deadline predates the cancellation request",
    )?;
    let context = &asked["cancellation"];
    require(
        context.is_object() && context["request_id"] == *command,
        "cancellation context must be this request",
    )?;
    require(
        delivered.get("cancellation") == Some(context),
        "delivery must carry its request's cancellation context",
    )?;
    require(
        context["schema"] == "durable-workflow.cancellation-context/v1"
            && context.get("scope_origin").is_none()
            && context.get("scope_authority_deadline_at").is_none(),
        "cancellation context schema must be root v1",
    )?;
    require(
        context["root_workflow_instance_id"] == instance && context["root_workflow_run_id"] == run,
        "cancellation must root at the successor",
    )?;
    require(
        context.get("parent_request_id") == Some(&Value::Null)
            && context["root_request_id"] == *command,
        "root request must be its own root with present null parent",
    )?;
    require(
        context["lineage"]
            == json!([{"request_id": command, "workflow_instance_id": instance, "workflow_run_id": run}]),
        "cancellation lineage must end at the successor and contain only its own address",
    )?;
    require(
        redrive_requester_is_valid(&context["requester"]),
        "context requester must identify its caller",
    )?;
    require(
        context["source"]
            .as_str()
            .is_some_and(|source| !source.trim().is_empty()),
        "context source must be non-blank",
    )?;
    require(
        context["reason"].is_null() || context["reason"].is_string(),
        "context reason must be null or a string",
    )?;
    require(
        asked.get("reason").unwrap_or(&Value::Null) == &context["reason"],
        "request reason must match its context",
    )?;
    let requested_at = time(&context["requested_at"], "requested_at")?;
    require(
        started_at <= requested_at
            && requested_at <= time(&request["recorded_at"], "request recorded_at")?,
        "context requested_at must fall between successor start and request",
    )?;
    require(
        asked["cleanup_deadline_at"] == context["cleanup_deadline_at"],
        "request cleanup_deadline_at must match its context",
    )?;
    require(
        time(&asked["cleanup_deadline_at"], "cleanup deadline")? > requested_at,
        "cleanup deadline must follow requested_at",
    )?;
    // Core uses row sequence for resolvedBeforeRequest, including equal wall times.
    let step = delivered["sequence"].as_u64().unwrap();
    require(
        step >= resume,
        "cancellation must be delivered to a step after the reused prefix",
    )?;
    let call_kind = delivered["call_kind"].as_str().unwrap_or_default();
    require(
        matches!(
            call_kind,
            "activity"
                | "local_activity"
                | "timer"
                | "condition"
                | "signal"
                | "child"
                | "service_operation"
        ),
        "delivery call_kind must be a single call kind",
    )?;
    require(
        [
            "sequence_span",
            "operation_sequence",
            "operation_sequence_span",
        ]
        .iter()
        .all(|key| delivered.get(*key).is_none()),
        "delivery must be one single call",
    )?;
    let delivery_row = delivery["sequence"].as_u64().unwrap();
    let request_row = request["sequence"].as_u64().unwrap();
    let at_step: Vec<_> = events
        .iter()
        .filter(|event| {
            !std::ptr::eq(*event, delivery)
                && redrive_step_event(event["event_type"].as_str().unwrap_or_default())
                && event["payload"]["sequence"] == json!(step)
        })
        .collect();
    let opened = at_step.iter().find(|event| {
        event["sequence"].as_u64().unwrap() < delivery_row
            && matches!(
                event["event_type"].as_str(),
                Some("ActivityScheduled" | "TimerScheduled" | "SignalWaitOpened")
            )
    });
    let resolved = |event: &&Value| {
        matches!(
            event["event_type"].as_str(),
            Some("ActivityCompleted" | "ActivityCancelled" | "TimerFired" | "SignalApplied")
        )
    };
    if let Some(opened) = opened {
        let kind = match opened["event_type"].as_str().unwrap() {
            "ActivityScheduled" if opened["payload"]["local_activity"] == true => "local_activity",
            "ActivityScheduled" => "activity",
            "TimerScheduled" => match opened["payload"]["timer_kind"].as_str() {
                Some("condition_timeout") => "condition",
                Some("signal_timeout") => "signal",
                _ => "timer",
            },
            _ => "signal",
        };
        require(
            call_kind == kind,
            "delivery call_kind must match its recorded call",
        )?;
        require(
            !at_step
                .iter()
                .any(|event| resolved(event) && event["sequence"].as_u64().unwrap() < request_row),
            "delivery targets a call resolved before its request",
        )?;
    } else {
        let before: Vec<_> = events
            .iter()
            .filter(|event| {
                event["sequence"].as_u64().unwrap() < delivery_row
                    && redrive_step_event(event["event_type"].as_str().unwrap_or_default())
            })
            .collect();
        let next_step = before
            .iter()
            .filter_map(|event| event["payload"]["sequence"].as_u64())
            .max()
            .unwrap_or(resume - 1)
            + 1;
        // Match terminal kind, step and resource identity. An unrelated or later
        // resolution cannot justify advancing this sequential fixture.
        for earlier in &before {
            let (identity, terminal_kinds): (&str, &[&str]) =
                match earlier["event_type"].as_str().unwrap() {
                    "ActivityScheduled" => (
                        "activity_execution_id",
                        &["ActivityCompleted", "ActivityCancelled"],
                    ),
                    "TimerScheduled" => ("timer_id", &["TimerFired"]),
                    "SignalWaitOpened" => ("signal_wait_id", &["SignalApplied"]),
                    _ => continue,
                };
            require(
                before.iter().any(|terminal| {
                    terminal["sequence"].as_u64() > earlier["sequence"].as_u64()
                        && terminal_kinds.contains(&terminal["event_type"].as_str().unwrap())
                        && terminal["payload"]["sequence"] == earlier["payload"]["sequence"]
                        && terminal["payload"][identity] == earlier["payload"][identity]
                }),
                "successor has reached no next step: an earlier call is unresolved",
            )?;
        }
        require(
            step == next_step && at_step.is_empty(),
            "cancellation must be delivered to a step the successor has reached",
        )?;
    }
    Ok(())
}

/// Check that a fixture is a history a Workflow 2.19 redrive can write.
///
/// Core's DefaultWorkflowControlPlane::redrive() records, in one write, a
/// WorkflowStarted carrying the successor identity and the redrive envelope,
/// then the reused prefix from FailedRunRedrivePlan::successorEvents(). Events
/// the successor records itself come after that prefix, at or after its start,
/// and belong to the successor run. Returns the first violated rule.
fn check_redrive_successor(fixture: &Value) -> Result<(), String> {
    type Time = chrono::DateTime<chrono::FixedOffset>;
    fn require(condition: bool, message: impl Into<String>) -> Result<(), String> {
        if condition {
            Ok(())
        } else {
            Err(message.into())
        }
    }
    fn time(value: &Value, message: impl Into<String>) -> Result<Time, String> {
        value
            .as_str()
            .and_then(|time| chrono::DateTime::parse_from_rfc3339(time).ok())
            .ok_or_else(|| message.into())
    }
    fn non_blank(value: &Value) -> Option<&str> {
        value.as_str().filter(|value| !value.trim().is_empty())
    }
    fn reused(event: &Value) -> bool {
        event["payload"].get("reused_from_run_id").is_some()
            || event["payload"].get("reused_recorded_at").is_some()
    }

    let history = fixture["history"].as_array().ok_or("fixture history")?;
    let started = history.first().ok_or("empty history")?;
    require(
        started["event_type"] == "WorkflowStarted" && started["sequence"] == json!(1),
        "history must open with WorkflowStarted at sequence 1",
    )?;
    let start = &started["payload"];
    require(
        start["recovery_kind"] == "redrive",
        "recovery_kind must be redrive",
    )?;
    let replayed_started_at = time(
        &start["replayed_started_at"],
        "replayed_started_at must be a valid time",
    )?;
    for key in REDRIVE_START_IDENTITY {
        require(
            non_blank(&start[*key]).is_some(),
            format!("WorkflowStarted {key} must be a non-blank string"),
        )?;
    }
    let instance = start["workflow_instance_id"].as_str().unwrap();
    let run = start["workflow_run_id"].as_str().unwrap();
    let source = start["continued_from_run_id"].as_str().unwrap();
    require(
        start["workflow_type"] == fixture["workflow"]["type"],
        "WorkflowStarted workflow_type must be the fixture workflow type",
    )?;
    require(
        fixture["worker_task"]["workflow_id"] == instance && run == FIXTURE_RUN_ID,
        "the workflow task must belong to the successor instance and run",
    )?;
    require(source != run, "continued_from_run_id must name another run")?;
    let started_at = time(&started["recorded_at"], "WorkflowStarted recorded_at")?;
    require(
        replayed_started_at <= started_at,
        "replayed_started_at must not be later than the successor start",
    )?;
    // Core writes both deadlines, null when unbounded. The execution deadline
    // bounds the workflow from its first start, the run deadline this run.
    for (key, bounded_start) in [
        ("execution_deadline_at", replayed_started_at),
        ("run_deadline_at", started_at),
    ] {
        let message =
            format!("{key} must be present and null or a valid time not before its start");
        let deadline = start.get(key).ok_or_else(|| message.clone())?;
        if !deadline.is_null() {
            require(time(deadline, message.clone())? >= bounded_start, message)?;
        }
    }

    // The reused prefix: every event redrive copies, directly after the start.
    // Redrive copies a fired timer as a TimerScheduled with a new timer id and
    // no reuse keys, directly followed by its reused TimerFired.
    let copied_timer = |index: usize| {
        let event = &history[index];
        event["event_type"] == "TimerScheduled"
            && !reused(event)
            && [
                "timer_kind",
                "cancellation_cleanup",
                "cancellation_scope_id",
                "parallel_group_id",
                "condition_wait_id",
                "signal_wait_id",
            ]
            .iter()
            .all(|key| event["payload"][*key].is_null())
            && history.get(index + 1).is_some_and(|fired| {
                fired["event_type"] == "TimerFired"
                    && reused(fired)
                    && fired["payload"]["timer_id"] == event["payload"]["timer_id"]
                    && fired["payload"]["sequence"] == event["payload"]["sequence"]
            })
    };
    let prefix_end = (1..history.len())
        .find(|index| !reused(&history[*index]) && !copied_timer(*index))
        .unwrap_or(history.len());
    require(
        !history[prefix_end..].iter().any(reused),
        "reused events must directly follow WorkflowStarted",
    )?;
    let mut previous_at = started_at;
    let mut reused_steps = Vec::new();
    for (index, event) in history.iter().enumerate().take(prefix_end).skip(1) {
        redrive_event_fields(event)?;
        let kind = event["event_type"].as_str().unwrap_or_default();
        let payload = &event["payload"];
        require(event["sequence"] == json!(index + 1), "history order")?;
        require(
            time(&event["recorded_at"], format!("{kind} recorded_at"))? == started_at,
            format!("reused {kind}: reused events are recorded with the successor start"),
        )?;
        if reused(event) {
            for key in ["reused_from_run_id", "reused_recorded_at"] {
                require(
                    non_blank(&payload[key]).is_some(),
                    format!("reused {kind} must carry a non-blank {key}"),
                )?;
            }
            require(
                payload["reused_from_run_id"] == source,
                format!("reused {kind} reused_from_run_id must match continued_from_run_id"),
            )?;
            let source_at = time(
                &payload["reused_recorded_at"],
                format!("reused {kind} reused_recorded_at must fall in the source run"),
            )?;
            require(
                replayed_started_at <= source_at && source_at <= started_at,
                format!("reused {kind} reused_recorded_at must fall in the source run"),
            )?;
            // A reused event keeps the run it was first recorded in.
            require(
                payload.get("workflow_run_id").is_none_or(|id| id == source),
                format!("reused {kind} keeps the source run id"),
            )?;
        }
        require(
            payload
                .get("workflow_instance_id")
                .is_none_or(|id| id == instance),
            format!("reused {kind} workflow_instance_id belongs to another workflow instance"),
        )?;
        // SignalReceived has no workflow step of its own; it belongs to its
        // wait, so any sequence it carries is not a reused step.
        if kind != "SignalReceived" {
            if let Some(step) = payload["sequence"].as_u64() {
                reused_steps.push(step);
            }
        }
    }
    let last_reused_step = reused_steps.iter().copied().max().unwrap_or(0);
    require(last_reused_step > 0, "no reused workflow step")?;
    // Redrive reuses every step before the failed one, in order.
    require(
        reused_steps.windows(2).all(|pair| pair[0] <= pair[1])
            && (1..=last_reused_step).all(|step| reused_steps.contains(&step)),
        "reused steps must be the source's steps in order from 1",
    )?;
    // Redrive resumes at the step after the last one it reused.
    let resume = start["resume_step_sequence"].as_u64();
    require(
        resume == Some(last_reused_step + 1),
        "resume_step_sequence must follow the last reused step",
    )?;

    // The successor's own events follow the reused prefix in history-row order.
    let mut cancellation_requested = false;
    for (index, event) in history.iter().enumerate().skip(prefix_end) {
        redrive_event_fields(event)?;
        let kind = event["event_type"].as_str().unwrap();
        if kind == "CooperativeCancellationRequested" {
            cancellation_requested = true;
        } else if kind == "CooperativeCancellationDelivered" {
            require(
                cancellation_requested,
                "cancellation must be delivered after its request",
            )?;
        }
        let payload = &event["payload"];
        require(event["sequence"] == json!(index + 1), "history order")?;
        let recorded_at = time(&event["recorded_at"], format!("{kind} recorded_at"))?;
        require(
            recorded_at >= previous_at,
            format!("history order: successor {kind} predates the event before it"),
        )?;
        previous_at = recorded_at;
        require(
            payload.get("workflow_run_id").is_none_or(|id| id == run),
            format!("successor {kind} must belong to the successor run"),
        )?;
        require(
            payload
                .get("workflow_instance_id")
                .is_none_or(|id| id == instance),
            format!("successor {kind} workflow_instance_id belongs to another workflow instance"),
        )?;
        if redrive_step_event(kind) {
            let step = payload["sequence"].as_u64().unwrap();
            require(
                step > last_reused_step,
                if kind == "CooperativeCancellationDelivered" {
                    "cancellation must be delivered to a step after the reused prefix".to_string()
                } else {
                    format!("successor {kind} must not reuse a reused step")
                },
            )?;
        }
    }
    check_successor_cancellation(
        &history[prefix_end..],
        instance,
        run,
        last_reused_step + 1,
        started_at,
    )
}

/// The successor history must be one Workflow 2.19 can write: a redrive
/// WorkflowStarted bound to the source run, then the reused prefix, then the
/// successor's own cancellation at or after its start.
fn assert_valid_redrive_successor(name: &str, resume_step_sequence: u64) {
    let fixture = redrive_successor_fixture(name);
    assert_eq!(
        fixture["history"][0]["payload"]["resume_step_sequence"],
        json!(resume_step_sequence),
        "{name}"
    );
    check_redrive_successor(&fixture).unwrap_or_else(|error| panic!("{name}: {error}"));
}

#[test]
fn redrive_successor_check_rejects_a_wrong_resume_step() {
    for (name, resume) in REDRIVE_SUCCESSOR_FIXTURES {
        assert_valid_redrive_successor(name, *resume);
        for wrong in [
            json!(resume - 1),
            json!(resume + 1),
            json!(null),
            json!(resume.to_string()),
        ] {
            let mut fixture = redrive_successor_fixture(name);
            fixture["history"][0]["payload"]["resume_step_sequence"] = wrong.clone();
            let error = check_redrive_successor(&fixture)
                .expect_err(&format!("{name} resume_step_sequence {wrong} must fail"));
            assert!(error.contains("resume_step_sequence"), "{error}");
        }
    }
}

#[tokio::test]
async fn redriven_reused_upserts_keep_values_and_never_reach_remaining() {
    assert_valid_redrive_successor(
        "redrive-successor-reused-upserts-cancellation-clock.json",
        3,
    );
    let observed = redrive_reused_fixture_replays_identically_on_cold_workers(
        "redrive-successor-reused-upserts-cancellation-clock.json",
        &["MemoUpserted", "SearchAttributesUpserted"],
    )
    .await;
    // Neither upsert is re-emitted, and the first activity received the
    // cancellation: the only new command is the shielded cleanup.
    let commands = observed["command_sequence"].as_array().expect("commands");
    assert_eq!(commands.len(), 1, "{commands:?}");
    assert_eq!(commands[0]["activity_type"], "corpus.redrive.cleanup");
    // remaining() is the deadline (00:05:31) less the delivery (00:05:02).
    assert_eq!(observed["arguments"], json!([29.0]));
    assert_reused_times_do_not_reach_remaining(
        "redrive-successor-reused-upserts-cancellation-clock.json",
        &["MemoUpserted", "SearchAttributesUpserted"],
    )
    .await;

    // A reused upsert is still checked against the workflow's own values.
    let error = redrive_reused_commands_with_history(
        "redrive-successor-reused-upserts-cancellation-clock.json",
        |history| {
            let event = history
                .iter_mut()
                .find(|event| event["event_type"] == "SearchAttributesUpserted")
                .unwrap();
            event["payload"]["attributes"]["Stage"] = json!("changed");
            event["payload"]["merged"]["Stage"] = json!("changed");
        },
    )
    .await
    .expect_err("a changed reused search attribute must not replay");
    assert!(error.contains("search_attribute_value_mismatch"), "{error}");
    let error = redrive_reused_commands_with_history(
        "redrive-successor-reused-upserts-cancellation-clock.json",
        |history| {
            let event = history
                .iter_mut()
                .find(|event| event["event_type"] == "MemoUpserted")
                .unwrap();
            // The same map with customer "Taylox".
            let changed = json!({"codec": "avro", "blob": CHANGED_MEMO_BLOB});
            event["payload"]["entries"] = changed.clone();
            event["payload"]["merged"] = changed;
        },
    )
    .await
    .expect_err("a changed reused memo must not replay");
    assert!(error.contains("memo_update_mismatch"), "{error}");
}

const CHANGED_MEMO_BLOB: &str = "wwHioz3/VYAiNw4EEGN1c3RvbWVyCgxUYXlsb3gKc3RhZ2UKEGdyZWV0aW5nAA==";

#[tokio::test]
async fn redriven_reused_signal_wait_keeps_its_value_and_never_reaches_remaining() {
    assert_valid_redrive_successor("redrive-successor-reused-signal-cancellation-clock.json", 2);
    let observed = redrive_reused_fixture_replays_identically_on_cold_workers(
        "redrive-successor-reused-signal-cancellation-clock.json",
        &["SignalWaitOpened", "SignalReceived", "SignalApplied"],
    )
    .await;
    // The wait is not reopened, and the first activity received the
    // cancellation: the only new command is the shielded cleanup.
    let commands = observed["command_sequence"].as_array().expect("commands");
    assert_eq!(commands.len(), 1, "{commands:?}");
    assert_eq!(commands[0]["activity_type"], "corpus.redrive.cleanup");
    // The applied value and the received signal come from the source run;
    // remaining() is the deadline (00:05:31) less the delivery (00:05:02).
    assert_eq!(observed["arguments"], json!([["Grace"], [["Grace"]], 29.0]));
    assert_reused_times_do_not_reach_remaining(
        "redrive-successor-reused-signal-cancellation-clock.json",
        &["SignalWaitOpened", "SignalReceived", "SignalApplied"],
    )
    .await;
}
