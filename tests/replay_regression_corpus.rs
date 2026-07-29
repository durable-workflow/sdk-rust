use std::{
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

use durable_workflow::{json, Client, Value, Worker, JSON_CODEC};

const FIXTURE_SCHEMA: &str = "durable-workflow.replay-regression/v1";

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
    Ok(paths)
}

fn normalize_command(command: &Value) -> Result<Value, String> {
    let mut normalized = command.clone();
    for field in ["arguments", "result"] {
        let Some(envelope) = command.get(field).and_then(Value::as_object) else {
            continue;
        };
        if envelope.get("codec").and_then(Value::as_str) == Some(JSON_CODEC) {
            let blob = envelope
                .get("blob")
                .and_then(Value::as_str)
                .ok_or_else(|| format!("{field} envelope has no blob"))?;
            normalized[field] = serde_json::from_str(blob)
                .map_err(|error| format!("decode replay command {field}: {error}"))?;
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

async fn execute_fixture(fixture: &Value) -> Result<Value, String> {
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
    if workflow_type != "corpus.side-effect-version" {
        return Err(format!(
            "replay fixture {fixture_id} has no registered Rust workflow {workflow_type:?}"
        ));
    }
    let input = workflow.get("input").cloned().unwrap_or_else(|| json!([]));
    if !input.is_array() {
        return Err(format!("{fixture_id}.workflow.input must be an array"));
    }
    let history = fixture.get("history").cloned().unwrap_or_else(|| json!([]));
    if !history.is_array() {
        return Err(format!("{fixture_id}.history must be an array"));
    }

    let task_id = format!("regression-corpus-{fixture_id}");
    let task = json!({
        "task_id": task_id,
        "workflow_id": format!("regression-corpus-{fixture_id}"),
        "run_id": "regression-corpus-run",
        "workflow_type": workflow_type,
        "payload_codec": JSON_CODEC,
        "arguments": {
            "codec": JSON_CODEC,
            "blob": serde_json::to_string(&input)
                .map_err(|error| format!("encode {fixture_id} input: {error}"))?
        },
        "history_events": history,
        "workflow_task_attempt": 1,
        "lease_owner": "regression-corpus-worker"
    });
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
    let handled = worker
        .run_once()
        .await
        .map_err(|error| format!("{fixture_id} worker replay failed: {error}"))?;
    if handled != 1 {
        return Err(format!("{fixture_id} replay handled {handled} tasks"));
    }

    let completion_path = format!("/api/worker/workflow-tasks/{task_id}/complete");
    let completion = server.request_body(&completion_path).ok_or_else(|| {
        format!("{fixture_id} did not complete through the official Rust worker path")
    })?;
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
        execute_fixture(&fixture)
            .await
            .unwrap_or_else(|error| panic!("{}: {error}", path.display()));
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
