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
    decode_payload, encode_payload, json, AvroValue, ChildWorkflowOptions, Client,
    ConditionWaitOptions, ConditionWaitResult, Error, ParallelOperation, ParallelResult,
    PayloadEnvelope, SearchAttributeUpdate, SelectionKey, Value, Worker, WorkflowInstance,
    DEFAULT_CODEC,
};
use serde::{Deserialize, Serialize};

const FIXTURE_SCHEMA: &str = "durable-workflow.replay-regression/v1";

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

impl FixtureServer {
    fn start(task: Value) -> Self {
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
                        handle_request(&mut stream, &server_requests, &task);
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
    let body = match path.as_str() {
        "/api/worker/workflow-tasks/poll" if request_number == 1 => {
            json!({"task": task}).to_string()
        }
        "/api/worker/workflow-tasks/poll" | "/api/worker/activity-tasks/poll" => {
            json!({"task": null}).to_string()
        }
        path if path.starts_with("/api/worker/workflow-tasks/")
            && (path.ends_with("/complete") || path.ends_with("/fail")) =>
        {
            json!({}).to_string()
        }
        _ => json!({"message": "not found"}).to_string(),
    };
    let status = if body.contains("\"not found\"") {
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
    if !matches!(
        workflow_type,
        "corpus.side-effect-version"
            | "corpus.workflow-stream"
            | "corpus.message-stream-batch"
            | "corpus.message-stream-partial-batches"
            | "corpus.nested-parallel"
            | "corpus.typed-replayed"
            | "corpus.memo-signed-zero"
            | "corpus.search-attribute-type-mismatch"
            | "corpus.condition-search"
            | "corpus.condition-search-adjacent"
            | "corpus.durable-selection"
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
    if input != json!([]) && workflow_type != "corpus.typed-replayed" {
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

    let task_id = format!("regression-corpus-{fixture_id}-{delivery_id}");
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
    let server = FixtureServer::start(task);
    let client = Client::builder(server.base_url())
        .timeout(Duration::from_secs(2))
        .build()
        .map_err(|error| format!("create replay corpus client: {error}"))?;
    let callback_calls = Arc::new(AtomicUsize::new(0));
    let observed_calls = Arc::clone(&callback_calls);
    let mut worker = Worker::new(client, "regression-corpus")
        .worker_id("regression-corpus-worker")
        .poll_timeout(Duration::from_millis(10));
    match workflow_type {
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
                |ctx, input: TypedReplayContract, state: WorkflowInstance<TypedReplayState>| async move {
                    let result: TypedReplayContract =
                        ctx.activity_typed("corpus.typed.activity", input).await?;
                    state.update(|current| current.message = Some(result.message.clone()))?;
                    Ok(result)
                },
            );
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
        "corpus.durable-selection" => {
            worker.register_workflow(workflow_type, |ctx, _input| async move {
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
    let handled = worker
        .run_once()
        .await
        .map_err(|error| format!("{fixture_id} worker replay failed: {error}"))?;
    if handled != 1 {
        return Err(format!("{fixture_id} replay handled {handled} tasks"));
    }

    let completion_path = format!("/api/worker/workflow-tasks/{task_id}/complete");
    let completion = match server.request_body(&completion_path) {
        Some(completion) => completion,
        None => {
            let failure_path = format!("/api/worker/workflow-tasks/{task_id}/fail");
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
    ]);
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
