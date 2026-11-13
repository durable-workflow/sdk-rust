//! Synthetic installed API adapter. SQL transformations and evidence labels
//! belong to the accepted producer manifest and the outer cohort driver.
use durable_workflow::{
    json, Client, Error, PayloadEnvelope, RawRequest, UpdateWaitStage, Value, Worker,
};
use std::{io::Read, time::Duration};

fn fixture(client: Client, queue: &str, null_update: bool) -> durable_workflow::Result<Worker> {
    let mut worker = Worker::new(client, queue).poll_timeout(Duration::from_millis(10));
    for workflow_type in ["keys.wait", "keys.continue"] {
        let continuing = workflow_type == "keys.continue";
        worker.register_workflow(workflow_type, move |ctx, input| async move {
            ctx.wait_signal("finish").await?;
            if continuing && input[0] == json!(0) {
                ctx.continue_as_new(json!([1]))
            } else {
                Ok(input)
            }
        });
        worker.declare_workflow_signals(workflow_type, &["finish", "touch", "other"])?;
        for update in ["mutate", "other"] {
            worker.register_update(workflow_type, update, move |_, arguments| async move {
                Ok(if null_update { Value::Null } else { arguments })
            });
        }
    }
    Ok(worker)
}

fn error_value(error: &Error) -> Value {
    match error {
        Error::UpdateStillPending(pending) => json!({"kind":"pending", "receipt":pending.receipt,
            "update_id":pending.update_id, "run_id":pending.run_id, "request_id":pending.request_id}),
        Error::Http { status, body } => json!({"kind":"http", "status":status.as_u16(),
            "body":serde_json::from_str::<Value>(body).unwrap_or_else(|_| json!(body)),
            "request_id":error.request_id(), "conflict":error.is_request_id_conflict()}),
        Error::Transport(_) => json!({"kind":"transport", "message":error.to_string()}),
        _ => json!({"kind":"error", "message":error.to_string()}),
    }
}

async fn command_receipt(
    client: &Client,
    args: &Value,
    kind: &str,
) -> durable_workflow::Result<Value> {
    let get = |field: &str| args[field].as_str().unwrap_or_default();
    let mut body = json!({"input":PayloadEnvelope::avro(&args["input"])?,
        "request_id":get("request_id")});
    if kind == "update" {
        body["wait_for"] = json!("accepted");
    }
    let path = if get("run_id").is_empty() {
        vec!["workflows", get("workflow_id"), kind, get("name")]
    } else {
        vec![
            "workflows",
            get("workflow_id"),
            "runs",
            get("run_id"),
            kind,
            get("name"),
        ]
    };
    let response = client
        .control_plane_request(RawRequest::post(path).json(body))
        .await?;
    Ok(json!({"status":response.status(),"body":response.json()?}))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut text = String::new();
    std::io::stdin().take(65_537).read_to_string(&mut text)?;
    if text.len() > 65_536 {
        return Err("fixture input exceeds 64 KiB".into());
    }
    if std::env::var("DURABLE_WORKFLOW_LOCAL_ISOLATED").as_deref() != Ok("1") {
        return Err("explicit isolated runtime opt-in is required".into());
    }
    let args: Value = serde_json::from_str(&text)?;
    let origin = std::env::var("DURABLE_WORKFLOW_SERVER_URL")?;
    assert!(origin
        .strip_prefix("http://127.0.0.1:")
        .and_then(|port| port.parse::<u16>().ok())
        .is_some_and(|port| port > 0));
    let client = Client::builder(origin)
        .token(Some(std::env::var("DURABLE_WORKFLOW_AUTH_TOKEN")?))
        .namespace(std::env::var("DURABLE_WORKFLOW_NAMESPACE").unwrap_or_else(|_| "default".into()))
        .build()?;
    let get = |name: &str| args[name].as_str().unwrap_or_default();
    let queue = args["queue"].as_str().unwrap_or("rust-key-cohort");
    let null_update = args["null_update"] == true;
    let result: durable_workflow::Result<Value> = match get("op") {
        "register" => fixture(client, queue, null_update)?.register().await
            .map(|r| json!({"registered":r.registered, "worker_id":r.worker_id})),
        "worker" => fixture(client, queue, null_update)?.run_until(tokio::time::sleep(Duration::from_secs(
            args["lifetime"].as_u64().unwrap_or(120).clamp(1, 300)))).await.map(|_| json!({"stopped":true})),
        "start" => client.start_workflow(get("workflow_type"), queue, get("workflow_id"), args["input"].clone())
            .await.map(|r| json!({"workflow_id":r.workflow_id,"run_id":r.run_id})),
        "signal" => client.signal_workflow_with_request_id(get("workflow_id"), get("name"),
            args["input"].clone(), get("request_id")).await,
        "signal_run" => client.signal_workflow_run_with_request_id(get("workflow_id"), get("run_id"),
            get("name"), args["input"].clone(), get("request_id")).await,
        "update" => client.update_workflow(get("workflow_id"),get("name"),args["input"].clone(),
            Some(get("request_id"))).await,
        "signal_raw" => command_receipt(&client, &args, "signal").await,
        "update_raw" | "update_run_raw" => command_receipt(&client, &args, "update").await,
        "outcome" => client.update_outcome_receipt(get("workflow_id"),get("update_id"),
            UpdateWaitStage::Accepted,None).await,
        "describe" => client.describe_workflow(get("workflow_id")).await
            .map(|r| json!({"workflow_id":r.workflow_id,"run_id":r.run_id,"status":r.status,
                "closed_reason":r.closed_reason})),
        "history" => client.export_workflow_history(get("workflow_id"),get("run_id")).await,
        "create_namespace" => client.control_plane_request(RawRequest::post(["namespaces"])
            .json(json!({"name":get("name"),"description":"synthetic key cohort","retention_days":30})))
            .await.and_then(|r| Ok(json!({"status":r.status(),"body":r.json()?}))),
        _ => return Err("unknown fixture operation".into()),
    };
    let output = match result {
        Ok(value) => json!({"kind":"ok","value":value}),
        Err(error) => error_value(&error),
    };
    println!("{}", serde_json::to_string(&output)?);
    Ok(())
}
