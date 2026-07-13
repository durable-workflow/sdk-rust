use durable_workflow::{json, Client, Result, Uuid, Value, Worker, WorkflowInstance};
use serde_json::Map;
use std::{
    env, fs,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

const SIDE_EFFECT_SCENARIO: &str = "rust_side_effect_replay_after_worker_restart";
const VERSION_MARKER_SCENARIO: &str = "rust_version_marker_replay_after_code_upgrade";

#[derive(Clone, Default)]
struct ReplayState {
    captured: Option<String>,
    version: i32,
}

fn configured_worker(
    client: Client,
    queue: &str,
    max_version: i32,
    callback_calls: Arc<AtomicUsize>,
) -> Worker {
    let mut worker = Worker::new(client, queue)
        .poll_timeout(Duration::from_secs(2))
        .heartbeat_interval(Duration::from_secs(2));
    worker.register_replayed_workflow(
        "rust.replay.side-effects-and-markers",
        ReplayState::default,
        move |ctx, _input, state: WorkflowInstance<ReplayState>| {
            let callback_calls = Arc::clone(&callback_calls);
            async move {
                let captured: String = ctx.side_effect(|| {
                    callback_calls.fetch_add(1, Ordering::SeqCst);
                    Uuid::new_v4().to_string()
                })?;
                let version = ctx.get_version("rust-replay-marker", 1, max_version)?;
                state.update(|current| {
                    current.captured = Some(captured.clone());
                    current.version = version;
                })?;
                Ok(json!({"captured": captured, "version": version}))
            }
        },
    );
    worker.register_replayed_query::<ReplayState, _, _>(
        "rust.replay.side-effects-and-markers",
        "replayed-state",
        |_ctx, state, _input| async move {
            Ok(json!({"captured": state.captured, "version": state.version}))
        },
    );
    worker
}

async fn run_worker(worker: Worker, stop: Arc<AtomicBool>) -> Result<()> {
    worker
        .run_until(async move {
            while !stop.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
}

async fn exercise(server_url: &str, token: Option<String>) -> std::result::Result<Value, String> {
    let client = Client::builder(server_url)
        .token(token)
        .namespace("default")
        .build()
        .map_err(|error| error.to_string())?;
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let queue = format!("rust-replay-{nonce}");
    let workflow_id = format!("rust-replay-{nonce}");
    let callback_calls = Arc::new(AtomicUsize::new(0));

    let first_stop = Arc::new(AtomicBool::new(false));
    let first_worker = tokio::spawn(run_worker(
        configured_worker(client.clone(), &queue, 2, Arc::clone(&callback_calls)),
        Arc::clone(&first_stop),
    ));
    tokio::time::sleep(Duration::from_millis(250)).await;
    let handle = client
        .start_workflow(
            "rust.replay.side-effects-and-markers",
            &queue,
            &workflow_id,
            json!([]),
        )
        .await
        .map_err(|error| error.to_string())?;
    let original = handle
        .result(Default::default())
        .await
        .map_err(|error| error.to_string())?;
    first_stop.store(true, Ordering::SeqCst);
    first_worker
        .await
        .map_err(|error| error.to_string())?
        .map_err(|error| error.to_string())?;

    let second_stop = Arc::new(AtomicBool::new(false));
    let second_worker = tokio::spawn(run_worker(
        configured_worker(client, &queue, 3, Arc::clone(&callback_calls)),
        Arc::clone(&second_stop),
    ));
    tokio::time::sleep(Duration::from_millis(250)).await;
    let replayed = handle
        .query("replayed-state", json!([]))
        .await
        .map_err(|error| error.to_string())?;
    second_stop.store(true, Ordering::SeqCst);
    second_worker
        .await
        .map_err(|error| error.to_string())?
        .map_err(|error| error.to_string())?;

    let captured_matches = original["captured"] == replayed["captured"];
    let version_stable = original["version"] == json!(2) && replayed["version"] == json!(2);
    let callback_once = callback_calls.load(Ordering::SeqCst) == 1;
    if !captured_matches || !version_stable || !callback_once {
        return Err(format!(
            "Rust replay drifted: captured_matches={captured_matches}, version_stable={version_stable}, callback_calls={}",
            callback_calls.load(Ordering::SeqCst)
        ));
    }

    Ok(json!({
        "workflow_id": workflow_id,
        "run_id": handle.run_id,
        "original": original,
        "replayed": replayed,
        "callback_calls": callback_calls.load(Ordering::SeqCst),
        "initial_supported_range": [1, 2],
        "upgraded_supported_range": [1, 3]
    }))
}

fn argument(name: &str) -> Option<String> {
    let args: Vec<String> = env::args().collect();
    args.windows(2)
        .find(|pair| pair[0] == name)
        .map(|pair| pair[1].clone())
}

#[tokio::main]
async fn main() {
    let output = argument("--output").map(PathBuf::from);
    let server_url = argument("--server-url")
        .or_else(|| env::var("DW_SERVER_URL").ok())
        .unwrap_or_else(|| "http://127.0.0.1:8080".to_string());
    let token = argument("--token").or_else(|| env::var("DW_REPLAY_AUTH_TOKEN").ok());
    let mut versions = Map::new();
    for raw in
        env::args().filter_map(|arg| arg.strip_prefix("--artifact-version=").map(str::to_string))
    {
        if let Some((name, version)) = raw.split_once('=') {
            versions.insert(name.to_string(), json!(version));
        }
    }
    versions
        .entry("sdk-rust".to_string())
        .or_insert_with(|| json!(env!("CARGO_PKG_VERSION")));

    let started_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let exercise = exercise(&server_url, token).await;
    let (status, observed, findings) = match exercise {
        Ok(observed) => ("pass", observed, Vec::<Value>::new()),
        Err(message) => {
            let finding = json!({
                "type": "replay_conformance_failure",
                "owning_surface": "sdk-rust",
                "summary": message,
                "expected_behavior": "published Rust side effects and version markers remain stable across cold replay"
            });
            ("fail", json!({"error": message}), vec![finding])
        }
    };
    let scenario = |id: &str| {
        json!({
            "scenario_id": id,
            "status": status,
            "published_artifact_versions": versions,
            "implementation_identity": {"runtime": "sdk-rust", "package": "durable-workflow", "version": env!("CARGO_PKG_VERSION")},
            "runtime_matrix": {"runtimes": ["sdk-rust"]},
            "observed_outputs": observed,
            "linked_findings": findings
        })
    };
    let report = json!({
        "schema": "durable-workflow.v2.replay-conformance.result",
        "schema_version": 1,
        "coverage_scope": "sdk-rust-runtime-shard",
        "outcome": status,
        "started_at_unix": started_at,
        "finished_at_unix": SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs(),
        "artifact_versions": versions,
        "runtime_matrix": {"runtimes": ["sdk-rust"]},
        "scenario_results": [scenario(SIDE_EFFECT_SCENARIO), scenario(VERSION_MARKER_SCENARIO)],
        "findings": findings
    });
    let rendered =
        serde_json::to_string_pretty(&report).expect("serialize conformance report") + "\n";
    if let Some(path) = output {
        if let Err(error) = fs::write(path, &rendered) {
            eprintln!("cannot write replay conformance report: {error}");
            std::process::exit(1);
        }
    }
    print!("{rendered}");
    if status != "pass" {
        std::process::exit(1);
    }
}
