use durable_workflow::{
    decode_avro_value, encode_avro_value, json, AvroValue, Client, PayloadEnvelope, Result, Value,
    Worker, WorkflowResultOptions,
};
use std::{
    collections::BTreeMap,
    env,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

const MEMO_BLOB: &str = "wwHioz3/VYAiNw4MDGJpbmFyeQgIc2FtZQxkb3VibGUGAAAAAAAAHEAcaW52YWxpZF9iaW5hcnkIBP8ACGxvbmcEDgxuZXN0ZWQOBAphbHBoYQQCCGJldGEEBAAIdGV4dAoIc2FtZQA=";

fn memo_entries() -> AvroValue {
    AvroValue::Map(BTreeMap::from([
        ("text".to_string(), AvroValue::String("same".to_string())),
        (
            "nested".to_string(),
            AvroValue::Map(BTreeMap::from([
                ("beta".to_string(), AvroValue::Long(2)),
                ("alpha".to_string(), AvroValue::Long(1)),
            ])),
        ),
        ("long".to_string(), AvroValue::Long(7)),
        ("double".to_string(), AvroValue::Double(7.0)),
        ("binary".to_string(), AvroValue::Bytes(b"same".to_vec())),
        (
            "invalid_binary".to_string(),
            AvroValue::Bytes(vec![0xff, 0x00]),
        ),
    ]))
}

fn configured_worker(client: Client, queue: &str, worker_id: String) -> Worker {
    let mut worker = Worker::new(client, queue)
        .worker_id(worker_id)
        .poll_timeout(Duration::from_secs(1))
        .heartbeat_interval(Duration::from_secs(2));
    worker.register_workflow_avro_value("tests.memo-restart-rust", |ctx, _input| async move {
        ctx.upsert_memo(memo_entries())?;
        ctx.sleep(Duration::from_secs(5)).await?;
        Ok(AvroValue::String("rust-replayed-memo".to_string()))
    });
    worker
}

async fn run_worker(worker: Worker, stop: Arc<AtomicBool>) -> Result<()> {
    worker
        .run_until(async move {
            while !stop.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
}

async fn wait_for_waiting_memo(
    handle: &durable_workflow::WorkflowHandle,
) -> std::result::Result<Value, String> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let description = handle.describe().await.map_err(|error| error.to_string())?;
        let memo = description.raw.get("memo").cloned().unwrap_or(Value::Null);
        if description.status.as_deref() == Some("waiting") && !memo.is_null() {
            return Ok(memo);
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "workflow did not expose waiting memo state: {:?}",
                description.status
            ));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn workflow_history(
    server_url: &str,
    token: Option<&str>,
    workflow_id: &str,
    run_id: &str,
) -> std::result::Result<Value, String> {
    let mut request = reqwest::Client::new()
        .get(format!(
            "{}/api/workflows/{workflow_id}/runs/{run_id}/history",
            server_url.trim_end_matches('/')
        ))
        .header("Accept", "application/json")
        .header("X-Namespace", "default")
        .header("X-Durable-Workflow-Control-Plane-Version", "2");
    if let Some(token) = token {
        request = request.bearer_auth(token);
    }
    let response = request.send().await.map_err(|error| error.to_string())?;
    let status = response.status();
    let body = response.text().await.map_err(|error| error.to_string())?;
    if !status.is_success() {
        return Err(format!("history request failed with HTTP {status}: {body}"));
    }
    serde_json::from_str(&body).map_err(|error| error.to_string())
}

fn assert_typed_memo_history(history: &Value) -> std::result::Result<(), String> {
    let events = history
        .get("events")
        .or_else(|| history.get("history_events"))
        .and_then(Value::as_array)
        .ok_or_else(|| "history response omitted events".to_string())?;
    let memo_events = events
        .iter()
        .filter(|event| event.get("event_type").and_then(Value::as_str) == Some("MemoUpserted"))
        .collect::<Vec<_>>();
    if memo_events.len() != 1 {
        return Err(format!(
            "expected one MemoUpserted event, found {}",
            memo_events.len()
        ));
    }

    for field in ["entries", "merged"] {
        let envelope: PayloadEnvelope = serde_json::from_value(
            memo_events[0]
                .get("payload")
                .and_then(|payload| payload.get(field))
                .cloned()
                .ok_or_else(|| format!("MemoUpserted omitted {field} envelope"))?,
        )
        .map_err(|error| error.to_string())?;
        if envelope.codec != "avro" || envelope.blob != MEMO_BLOB {
            return Err(format!(
                "MemoUpserted {field} did not use the canonical envelope"
            ));
        }
        let decoded = decode_avro_value(&envelope).map_err(|error| error.to_string())?;
        if decoded != memo_entries() {
            return Err(format!(
                "MemoUpserted {field} lost long/double, bytes/text, or nested-map identity"
            ));
        }
        let AvroValue::Map(entries) = &decoded else {
            return Err(format!("MemoUpserted {field} did not decode to a map"));
        };
        if entries.get("invalid_binary") != Some(&AvroValue::Bytes(vec![0xff, 0x00])) {
            return Err(format!(
                "MemoUpserted {field} did not preserve exact invalid UTF-8 bytes ff00"
            ));
        }
    }

    Ok(())
}

async fn exercise(server_url: &str, token: Option<String>) -> std::result::Result<Value, String> {
    let expected_envelope =
        encode_avro_value(&memo_entries()).map_err(|error| error.to_string())?;
    if expected_envelope.blob != MEMO_BLOB {
        return Err("local memo encoder drifted from the shared canonical envelope".to_string());
    }

    let client = Client::builder(server_url)
        .token(token.clone())
        .namespace("default")
        .build()
        .map_err(|error| error.to_string())?;
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let queue = format!("memo-restart-rust-{nonce}");
    let workflow_id = format!("memo-restart-rust-{nonce}");

    let first_stop = Arc::new(AtomicBool::new(false));
    let first_worker = tokio::spawn(run_worker(
        configured_worker(client.clone(), &queue, format!("memo-rust-before-{nonce}")),
        Arc::clone(&first_stop),
    ));
    tokio::time::sleep(Duration::from_millis(250)).await;

    let handle = client
        .start_workflow("tests.memo-restart-rust", &queue, &workflow_id, json!([]))
        .await
        .map_err(|error| error.to_string())?;
    let waiting_memo = wait_for_waiting_memo(&handle).await?;
    let expected_projection = json!({
        "binary": {"$type": "bytes", "base64": "c2FtZQ=="},
        "double": 7,
        "invalid_binary": {"$type": "bytes", "base64": "/wA="},
        "long": 7,
        "nested": {"alpha": 1, "beta": 2},
        "text": "same",
    });
    if waiting_memo != expected_projection {
        return Err(format!(
            "waiting operator memo projection drifted: {waiting_memo}"
        ));
    }

    let run_id = handle
        .run_id
        .as_deref()
        .ok_or_else(|| "workflow handle omitted run_id".to_string())?;
    let first_history =
        workflow_history(server_url, token.as_deref(), &workflow_id, run_id).await?;
    assert_typed_memo_history(&first_history)?;

    first_stop.store(true, Ordering::SeqCst);
    first_worker
        .await
        .map_err(|error| error.to_string())?
        .map_err(|error| error.to_string())?;

    let replacement_stop = Arc::new(AtomicBool::new(false));
    let replacement_worker = tokio::spawn(run_worker(
        configured_worker(client.clone(), &queue, format!("memo-rust-after-{nonce}")),
        Arc::clone(&replacement_stop),
    ));

    let result = handle
        .result_avro_value(WorkflowResultOptions {
            poll_interval: Duration::from_millis(100),
            timeout: Duration::from_secs(30),
        })
        .await
        .map_err(|error| error.to_string())?;
    replacement_stop.store(true, Ordering::SeqCst);
    replacement_worker
        .await
        .map_err(|error| error.to_string())?
        .map_err(|error| error.to_string())?;

    if result != AvroValue::String("rust-replayed-memo".to_string()) {
        return Err(format!(
            "replacement worker returned unexpected result: {result:?}"
        ));
    }

    let completed = handle.describe().await.map_err(|error| error.to_string())?;
    if completed.raw.get("memo") != Some(&waiting_memo) {
        return Err("completed operator memo differs from waiting memo".to_string());
    }
    let final_history =
        workflow_history(server_url, token.as_deref(), &workflow_id, run_id).await?;
    assert_typed_memo_history(&final_history)?;

    Ok(json!({
        "workflow_id": workflow_id,
        "run_id": run_id,
        "result": "rust-replayed-memo",
        "memo_blob": MEMO_BLOB,
        "memo_event_count": 1,
        "worker_restart": true,
    }))
}

fn argument(name: &str) -> Option<String> {
    let args = env::args().collect::<Vec<_>>();
    args.windows(2)
        .find(|pair| pair[0] == name)
        .map(|pair| pair[1].clone())
}

#[tokio::main]
async fn main() {
    let server_url = argument("--server-url")
        .or_else(|| env::var("DURABLE_WORKFLOW_RUNTIME_URL").ok())
        .unwrap_or_else(|| "http://127.0.0.1:8080".to_string());
    let token = argument("--token")
        .or_else(|| env::var("DURABLE_WORKFLOW_AUTH_TOKEN").ok())
        .or_else(|| Some("test-token".to_string()));

    match exercise(&server_url, token).await {
        Ok(observed) => println!("{}", serde_json::to_string(&observed).unwrap_or_default()),
        Err(message) => {
            eprintln!("portable memo restart failed: {message}");
            std::process::exit(1);
        }
    }
}
