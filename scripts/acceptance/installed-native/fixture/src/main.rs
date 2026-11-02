//! Installed-package lifecycle fixture for native Server acceptance.
//!
//! Built against one exact published `durable-workflow` crate version.
//! `worker` runs the managed worker under test. `drive-lifecycle` and
//! `drive-deadline` are the drivers; they use the same published client and
//! write their observations to /state for the independent history checker.

use durable_workflow::{
    json, ActivityError, Client, Error, Value, Worker, WorkflowCommandOptions, WorkflowHandle,
    WorkflowResultOptions, WorkflowStartOptions,
};
use std::{
    fs::OpenOptions,
    io::Write,
    path::Path,
    time::{Duration, Instant},
};

const RUNTIME: &str = "http://qualification-runtime:8080";
const QUEUE: &str = "qualification-sdk";
const WORKFLOW: &str = "qualification.lifecycle";

type Outcome<T> = std::result::Result<T, Box<dyn std::error::Error>>;

fn expected(step: i64) -> Value {
    json!({"bigint": i64::MAX, "nil": null, "zero": 0, "false": false, "unicode": "雪☃", "step": step})
}

fn append(path: &str, value: &Value) {
    let mut file = OpenOptions::new().create(true).append(true).open(path).unwrap();
    writeln!(file, "{value}").unwrap();
}

fn lines(path: &str) -> Vec<Value> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        // A line still being appended by another container is skipped until complete.
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect()
}

fn client(role: &str) -> Outcome<Client> {
    let credentials: Value =
        serde_json::from_str(&std::fs::read_to_string("/state/credentials.json")?)?;
    let token = credentials[role].as_str().map(str::to_owned);
    let builder = Client::builder(RUNTIME).namespace("default");
    let builder = if role == "worker" { builder.worker_token(token) } else { builder.token(token) };
    Ok(builder.build()?)
}

fn check(condition: bool, message: impl Into<String>) -> Outcome<()> {
    if condition { Ok(()) } else { Err(message.into().into()) }
}

/// Wait until `found` returns a value, polling the shared state files.
async fn wait_for<T>(what: &str, limit: Duration, mut found: impl FnMut() -> Option<T>) -> Outcome<T> {
    let started = Instant::now();
    loop {
        if let Some(value) = found() {
            return Ok(value);
        }
        if started.elapsed() > limit {
            return Err(format!("timed out waiting for {what}").into());
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

fn entered(workflow_id: &str, step: i64) -> Option<String> {
    lines("/state/worker-events.jsonl").into_iter().find_map(|event| {
        (event["workflow_id"] == workflow_id && event["step"] == step)
            .then(|| event["run_id"].as_str().unwrap_or_default().to_owned())
    })
}

async fn start(id: &str, mode: &str, execution: u64, run: u64) -> Outcome<WorkflowHandle> {
    start_with(id, json!([mode, 0]), execution, run).await
}

async fn start_with(id: &str, input: Value, execution: u64, run: u64) -> Outcome<WorkflowHandle> {
    let options = WorkflowStartOptions::new()
        .execution_timeout_seconds(execution)
        .run_timeout_seconds(run);
    Ok(client("operator")?
        .start_workflow_with_options(WORKFLOW, QUEUE, id, options, input)
        .await?)
}

/// Regular workflow: activity, signal and query, then a typed result.
async fn drive_regular() -> Outcome<Value> {
    let handle = start_with("lifecycle-regular", json!(["regular", 0, "ada"]), 60, 30).await?;
    wait_for("worker to await the approval signal", Duration::from_secs(15), || {
        lines("/state/worker-events.jsonl").into_iter().find(|event| event["workflow_id"] == handle.workflow_id.as_str()
            && event["phase"] == "awaiting-signal")
    }).await?;
    let before = handle.query("status", json!([])).await?;
    check(before["input"] == json!(["regular", 0, "ada"]) && before["approvals"] == json!([]),
        format!("query before signal: {before}"))?;
    handle.signal("approve", json!(["operator"])).await?;
    let value = handle.result(result_options()).await?;
    let wanted = json!({"greeting": "hello ada", "approved_by": "operator", "bigint": i64::MAX});
    check(value == wanted, format!("regular result mismatch: {value}"))?;
    let current = handle.describe().await?;
    Ok(json!({"workflow_id": handle.workflow_id, "original_run_id": handle.run_id, "final_run_id": handle.run_id,
        "status": current.status, "query_before_signal": before, "result": value}))
}

/// A non-retryable typed activity failure must reach the client as a typed workflow failure.
async fn drive_reject() -> Outcome<Value> {
    let handle = start_with("lifecycle-reject", json!(["reject", 0, "bob"]), 60, 30).await?;
    let error = handle.result(result_options()).await.expect_err("rejected workflow must not complete");
    let Error::WorkflowFailed(outcome) = &error else {
        return Err(format!("reject: expected WorkflowFailed, got {error}").into());
    };
    let detail = json!({"exception": outcome.exception, "raw": outcome.raw}).to_string();
    check(outcome.exception_type.as_deref() == Some("ActivityFailed") && detail.contains("QualificationRejected"),
        format!("reject: untyped failure {:?} {detail}", outcome.exception_type))?;
    let current = handle.describe().await?;
    Ok(json!({"workflow_id": handle.workflow_id, "original_run_id": handle.run_id, "final_run_id": handle.run_id,
        "status": current.status, "terminal": {"sdk_error": "failed", "reason": outcome.reason,
        "exception_type": outcome.exception_type, "non_retryable": outcome.non_retryable,
        "message": outcome.message, "exception": outcome.exception}}))
}

fn result_options() -> WorkflowResultOptions {
    WorkflowResultOptions { poll_interval: Duration::from_millis(50), timeout: Duration::from_secs(20) }
}

fn terminal(error: &Error) -> Value {
    let (kind, outcome) = match error {
        Error::WorkflowTimedOut(outcome) => ("timed_out", outcome),
        Error::WorkflowCancelled(outcome) => ("cancelled", outcome),
        Error::WorkflowFailed(outcome) => ("failed", outcome),
        Error::WorkflowTerminated(outcome) => ("terminated", outcome),
        other => return json!({"error": other.to_string()}),
    };
    json!({"sdk_error": kind, "reason": outcome.reason, "run_id": outcome.run_id})
}

/// A completed continue-as-new chain must return the exact typed value from a new run.
async fn expect_continued(handle: &WorkflowHandle) -> Outcome<Value> {
    let value = handle.result(result_options()).await?;
    check(value == expected(1), format!("continue-as-new result mismatch: {value}"))?;
    let current = handle.describe().await?;
    check(current.run_id.is_some() && current.run_id != handle.run_id, "continue-as-new did not advance the run")?;
    Ok(json!({"workflow_id": handle.workflow_id, "original_run_id": handle.run_id,
        "final_run_id": current.run_id, "status": current.status, "result": value}))
}

async fn drive_lifecycle() -> Outcome<Value> {
    let mut cases = serde_json::Map::new();
    cases.insert("regular".to_owned(), drive_regular().await?);
    cases.insert("reject".to_owned(), drive_reject().await?);
    for mode in ["continue", "timeout", "cancel", "after-timeout"] {
        let workflow_mode = if mode == "after-timeout" { "continue" } else { mode };
        let run_timeout = if mode == "timeout" { 2 } else { 30 };
        let handle = start(&format!("lifecycle-{mode}"), workflow_mode, 60, run_timeout).await?;
        wait_for(&format!("worker to enter {mode}"), Duration::from_secs(15), || entered(&handle.workflow_id, 0)).await?;
        let case = match mode {
            "continue" | "after-timeout" => expect_continued(&handle).await?,
            "timeout" | "cancel" => {
                if mode == "cancel" {
                    handle.cancel(WorkflowCommandOptions { reason: Some("qualification cancellation".into()), request_id: None }).await?;
                }
                let error = handle.result(result_options()).await.expect_err("workflow must not complete");
                let observed = terminal(&error);
                // The exact timeout kind is verified in durable history by check_history.py.
                let kind = if mode == "timeout" { "timed_out" } else { "cancelled" };
                check(observed["sdk_error"] == kind, format!("{mode}: unexpected terminal outcome {observed}"))?;
                let current = handle.describe_selected_run().await?;
                json!({"workflow_id": handle.workflow_id, "original_run_id": handle.run_id,
                    "final_run_id": handle.run_id, "status": current.status, "terminal": observed,
                    "described": {"closed_reason": current.closed_reason, "failure": current.failure}})
            }
            _ => unreachable!(),
        };
        cases.insert(mode.to_owned(), case);
    }
    Ok(json!({"outcome": "pass", "cell": "lifecycle", "cases": cases}))
}

/// A run or execution deadline passes while the worker's history-wait report is
/// held by the proxy. The published runtime must answer with a matching terminal
/// receipt, and the same worker process must then complete another workflow.
async fn drive_deadline() -> Outcome<Value> {
    let mut cases = serde_json::Map::new();
    for kind in ["run", "execution"] {
        std::fs::write("/state/delay.json", json!({"seconds": 3.0}).to_string())?;
        // The execution cell continues once after two seconds. Its second run
        // reaches the five-second execution deadline well before its own
        // four-second run deadline.
        let (mode, step, execution, run) = if kind == "run" { ("timeout", 0, 60, 2) } else { ("exec-timeout", 1, 5, 4) };
        let handle = start(&format!("deadline-{kind}"), mode, execution, run).await?;
        let target = wait_for(&format!("worker to enter {kind} step {step}"), Duration::from_secs(10),
            || entered(&handle.workflow_id, step)).await?;
        wait_for(&format!("{kind}: worker to schedule its timer"), Duration::from_secs(10), || {
            lines("/state/acks.jsonl").into_iter().find(|ack| ack["response"]["run_id"] == target.as_str()
                && ack["status"] == 200 && ack["path"].as_str().is_some_and(|p| p.ends_with("/complete")))
        }).await?;
        // Append to an unconsumed stream through the public client. Its durable
        // wakeup replays the workflow while the timer remains outstanding.
        client("operator")?.append_message_stream(&handle.workflow_id, "qualification", &format!("wake-{kind}"), json!([])).await?;
        let ack = wait_for(&format!("{kind}: delayed acknowledgement"), Duration::from_secs(14), || {
            lines("/state/acks.jsonl").into_iter().find(|ack| ack["delay_seconds"] == 3.0 && ack["response"]["run_id"] == target.as_str())
        }).await?;
        let receipt = &ack["response"];
        let task_id = Path::new(ack["path"].as_str().unwrap_or_default()).parent()
            .and_then(Path::file_name).and_then(|name| name.to_str()).unwrap_or_default();
        check(ack["status"] == 409 && ack["failure_type"] == "WorkflowTaskWaitingForHistory"
            && receipt["task_id"] == task_id && receipt["workflow_task_attempt"] == ack["request_attempt"]
            && receipt["run_id"] == target.as_str() && receipt["reason"] == "run_timed_out"
            && receipt["outcome"] == "completed" && receipt["recorded"] == false
            && receipt["run_status"] == "failed" && receipt["created_task_ids"] == json!([]),
            format!("{kind}: invalid timeout receipt {ack}"))?;
        let current = client("operator")?.describe_workflow_run(&handle.workflow_id, &target).await?;
        check(current.status.as_deref() == Some("failed"), format!("{kind}: run not failed: {:?}", current.status))?;
        let error = handle.result(result_options()).await.expect_err("deadline workflow must not complete");
        let observed = terminal(&error);
        // The exact timeout kind is verified in durable history by check_history.py.
        check(observed["sdk_error"] == "timed_out", format!("{kind}: unexpected SDK terminal outcome {observed}"))?;
        // The same managed worker process must finish a distinct workflow.
        let next = start(&format!("after-{kind}"), "continue", 60, 30).await?;
        let next_case = expect_continued(&next).await?;
        cases.insert(kind.to_owned(), json!({"workflow_id": handle.workflow_id, "original_run_id": handle.run_id,
            "final_run_id": target, "receipt": ack, "status": current.status, "terminal": observed,
            "described": {"closed_reason": current.closed_reason, "failure": current.failure}, "next": next_case}));
    }
    Ok(json!({"outcome": "pass", "cell": "deadline", "cases": cases}))
}

async fn run_worker() -> Outcome<()> {
    let mut worker = Worker::new(client("worker")?, QUEUE)
        .worker_id("qualification-installed")
        .poll_timeout(Duration::from_secs(1));
    worker.register_activity("qualification.greet", |_ctx, args| async move {
        let name = args.get(0).and_then(Value::as_str).unwrap_or_default().to_owned();
        Ok(json!(format!("hello {name}")))
    });
    worker.register_activity("qualification.reject", |_ctx, args| async move {
        let name = args.get(0).and_then(Value::as_str).unwrap_or_default().to_owned();
        Err(ActivityError::new("QualificationRejected", format!("rejected {name}")).non_retryable(true).into())
    });
    worker.register_query(WORKFLOW, "status", |ctx, _args| async move {
        Ok(json!({"input": ctx.workflow_input(), "approvals": ctx.signals("approve"), "run_status": ctx.run_status}))
    });
    worker.register_workflow(WORKFLOW, |ctx, input| async move {
        let mode = input.get(0).and_then(Value::as_str)
            .ok_or_else(|| Error::WorkerLoop("expected lifecycle mode".into()))?.to_owned();
        let step = input.get(1).and_then(Value::as_i64).unwrap_or(0);
        let identity = ctx.workflow_identity()?;
        append("/state/worker-events.jsonl", &json!({"mode": mode, "step": step,
            "workflow_id": identity.workflow_id, "run_id": identity.run_id, "pid": std::process::id()}));
        let name = input.get(2).and_then(Value::as_str).unwrap_or_default().to_owned();
        match (mode.as_str(), step) {
            ("regular", _) => {
                let greeting = ctx.activity("qualification.greet", json!([name])).await?;
                append("/state/worker-events.jsonl", &json!({"phase": "awaiting-signal", "workflow_id": identity.workflow_id}));
                let approval = ctx.wait_signal("approve").await?;
                let approver = approval.first().cloned().unwrap_or(Value::Null);
                Ok(json!({"greeting": greeting, "approved_by": approver, "bigint": i64::MAX}))
            }
            ("reject", _) => {
                ctx.activity("qualification.reject", json!([name])).await?;
                Err(Error::WorkerLoop("rejection activity unexpectedly succeeded".into()))
            }
            ("continue", 0) => ctx.continue_as_new(json!([mode, 1])),
            ("continue", _) => Ok(expected(step)),
            ("exec-timeout", 0) => {
                ctx.sleep(Duration::from_secs(2)).await?;
                ctx.continue_as_new(json!([mode, 1]))
            }
            _ => {
                ctx.sleep(Duration::from_secs(300)).await?;
                Err(Error::WorkerLoop("unexpected lifecycle continuation".into()))
            }
        }
    });
    worker.declare_workflow_signals(WORKFLOW, &["approve"])?;
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    worker.run_until(async move { terminate.recv().await; }).await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Outcome<()> {
    let role = std::env::args().nth(1).expect("role: worker | drive-lifecycle | drive-deadline");
    let report = match role.as_str() {
        "worker" => return run_worker().await,
        "drive-lifecycle" => drive_lifecycle().await,
        "drive-deadline" => drive_deadline().await,
        other => return Err(format!("unknown role {other}").into()),
    };
    let report = report.unwrap_or_else(|error| json!({"outcome": "fail", "error": error.to_string()}));
    std::fs::write("/state/driver.json", serde_json::to_string_pretty(&report)?)?;
    println!("{report}");
    check(report["outcome"] == "pass", "driver failed")
}
