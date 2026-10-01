use durable_workflow::{
    json, Client, ConditionWaitOptions, ConditionWaitResult, ParallelOperation, ParallelResult,
    SelectionKey, Value, Worker, WorkflowHandle,
};
use std::time::Duration;

async fn history(endpoint: &str, handle: &WorkflowHandle) -> Value {
    reqwest::Client::new()
        .get(format!(
            "{endpoint}/api/workflows/{}/runs/{}/history",
            handle.workflow_id,
            handle.run_id.as_deref().unwrap()
        ))
        .query(&[("page_size", "1000")])
        .bearer_auth("test-token")
        .header("X-Namespace", "default")
        .header("X-Durable-Workflow-Control-Plane-Version", "2")
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap()
}

fn opens(snapshot: &Value) -> usize {
    snapshot["events"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|event| event["event_type"] == "ConditionWaitOpened")
        .count()
}

async fn until_open(
    queue: &str,
    mode: &str,
    endpoint: &str,
    handle: &WorkflowHandle,
    target: usize,
) -> Result<Value, String> {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            cold_claim(queue, mode)?;
            let snapshot = history(endpoint, handle).await;
            if opens(&snapshot) >= target {
                return Ok(snapshot);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("bounded published reproduction")
}

fn cold_claim(queue: &str, mode: &str) -> Result<(), String> {
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["claim", queue, mode])
        .output()
        .map_err(|error| error.to_string())?;
    if output.status.success() {
        Ok(())
    } else {
        Err(String::from_utf8_lossy(&output.stderr).into_owned())
    }
}

#[tokio::main]
async fn main() {
    let endpoint = std::env::var("DURABLE_WORKFLOW_SERVER_URL").unwrap();
    let client = Client::builder(&endpoint)
        .token(Some("test-token".to_string()))
        .namespace("default")
        .build()
        .unwrap();
    let args = std::env::args().collect::<Vec<_>>();
    let child = args.get(1).is_some_and(|arg| arg == "claim");
    let child_mode = args.get(3).map(String::as_str);
    let mut failed = false;
    for mode in [
        "condition",
        "parallel",
        "selection",
        "selection-timeout",
        "parallel-timeout",
    ] {
        if child && child_mode != Some(mode) {
            continue;
        }
        let queue = if child {
            args[2].clone()
        } else {
            format!("workflow601-{mode}-{}", durable_workflow::Uuid::new_v4())
        };
        let mut worker = Worker::new(client.clone(), &queue)
            .worker_id(format!("{queue}-worker"))
            .poll_timeout(Duration::from_secs(1));
        worker.register_workflow(
            "tests.published-condition-reopen",
            move |ctx, _| async move {
                let predicate_ctx = ctx.clone();
                let condition = || {
                    let options = ConditionWaitOptions::new("two-votes", "sha256:two-votes-v1");
                    if mode.ends_with("timeout") {
                        options.timeout(Duration::from_secs(if mode == "parallel-timeout" {
                            6
                        } else {
                            3
                        }))
                    } else {
                        options
                    }
                };
                match mode {
                    "condition" => {
                        let observed = ctx
                            .wait_condition(condition(), move || {
                                Ok(predicate_ctx.signals("vote")?.len() >= 2)
                            })
                            .await?;
                        assert_eq!(observed, ConditionWaitResult::Satisfied);
                    }
                    "parallel" | "parallel-timeout" => {
                        let observed = ctx
                            .parallel(vec![
                                ParallelOperation::timer(Duration::from_secs(2)),
                                ParallelOperation::group(vec![
                                    ParallelOperation::signal("never"),
                                    ParallelOperation::condition(condition(), move || {
                                        Ok(predicate_ctx.signals("vote")?.len() >= 2)
                                    }),
                                ]),
                            ])
                            .await?;
                        let expected = if mode.ends_with("timeout") {
                            ConditionWaitResult::TimedOut
                        } else {
                            ConditionWaitResult::Satisfied
                        };
                        match &observed[1] {
                            ParallelResult::Group(members) => {
                                assert_eq!(members[1], ParallelResult::Condition(expected))
                            }
                            other => panic!("expected nested group: {other:?}"),
                        }
                    }
                    "selection" | "selection-timeout" => {
                        let observed = ctx
                            .select_keyed(vec![
                                ("timer", ParallelOperation::timer(Duration::from_secs(300))),
                                (
                                    "votes",
                                    ParallelOperation::condition(condition(), move || {
                                        Ok(predicate_ctx.signals("vote")?.len() >= 2)
                                    }),
                                ),
                            ])
                            .await?;
                        assert_eq!(observed.key, SelectionKey::Name("votes".into()));
                        let expected = if mode.ends_with("timeout") {
                            ConditionWaitResult::TimedOut
                        } else {
                            ConditionWaitResult::Satisfied
                        };
                        assert_eq!(observed.value, Some(ParallelResult::Condition(expected)));
                    }
                    _ => unreachable!(),
                }
                Ok(Value::Null)
            },
        );
        worker.register().await.unwrap();
        if child {
            if let Err(error) = worker.run_once().await {
                eprintln!("claim failed: {error}");
                std::process::exit(1);
            }
            return;
        }
        let handle = client
            .start_workflow(
                "tests.published-condition-reopen",
                &queue,
                &queue,
                json!([]),
            )
            .await
            .unwrap();
        until_open(&queue, mode, &endpoint, &handle, 1)
            .await
            .unwrap();
        handle
            .signal_selected_run("vote", json!(["first"]))
            .await
            .unwrap();
        let outcome = until_open(&queue, mode, &endpoint, &handle, 2).await;
        let mut next_signal = json!({"outcome":"not-run"});
        if outcome.is_ok() {
            if mode.starts_with("parallel") {
                handle
                    .signal_selected_run("never", json!(["other-member"]))
                    .await
                    .unwrap();
                tokio::time::sleep(Duration::from_secs(3)).await;
                for _ in 0..5 {
                    cold_claim(&queue, mode).unwrap();
                    let pending = history(&endpoint, &handle).await;
                    assert!(!pending["events"].as_array().unwrap().iter().any(|event| event["event_type"] == "WorkflowCompleted"),
                        "false wake must leave the nested condition pending after its other members finish");
                    if pending["events"].as_array().unwrap().iter().any(|event| {
                        event["event_type"] == "SignalApplied"
                            && event["payload"]["signal_name"] == "never"
                    }) {
                        break;
                    }
                }
            }
            let opens_before_final_signal = opens(&history(&endpoint, &handle).await);
            if !mode.ends_with("timeout") {
                handle
                    .signal_selected_run("vote", json!(["second"]))
                    .await
                    .unwrap();
            } else {
                tokio::time::sleep(Duration::from_secs(if mode == "parallel-timeout" {
                    7
                } else {
                    4
                }))
                .await;
            }
            for _ in 0..10 {
                if let Err(error) = cold_claim(&queue, mode) {
                    failed = true;
                    next_signal = json!({"outcome":"product-fail","error":error.to_string()});
                    break;
                }
                let next = history(&endpoint, &handle).await;
                if opens(&next) > opens_before_final_signal {
                    failed = true;
                    next_signal = json!({"outcome":"product-fail","reason":"true-predicate-reopened","opens":opens(&next)});
                    break;
                }
                let satisfied = next["events"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .filter(|event| event["event_type"] == "ConditionWaitSatisfied")
                    .count();
                let completed = next["events"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|event| event["event_type"] == "WorkflowCompleted");
                if completed && (mode.ends_with("timeout") || satisfied >= 2) {
                    if mode.ends_with("timeout") {
                        assert!(
                            next["events"]
                                .as_array()
                                .unwrap()
                                .iter()
                                .any(|event| event["event_type"] == "TimerFired"
                                    && event["payload"]["timer_kind"] == "condition_timeout"),
                            "condition deadline must supply the durable resolution"
                        );
                    }
                    next_signal = json!({"outcome":"pass","satisfied":satisfied,"completed":completed,"opens_before_final_signal":opens_before_final_signal});
                    break;
                }
            }
            if next_signal["outcome"] == "not-run" {
                failed = true;
                next_signal =
                    json!({"outcome":"product-fail","reason":"true-predicate-not-resolved"});
            }
        }
        let snapshot = history(&endpoint, &handle).await;
        let phases = snapshot["events"]
            .as_array()
            .unwrap()
            .iter()
            .map(|event| json!({"event_type":event["event_type"],"sequence":event["payload"]["sequence"]}))
            .collect::<Vec<_>>();
        let result = match outcome {
            Ok(_) => json!({"outcome":"pass"}),
            Err(error) => {
                failed = true;
                json!({"outcome":"product-fail","error":error.to_string()})
            }
        };
        println!(
            "{}",
            json!({"mode":mode,"result":result,"next_signal":next_signal,"phases":phases,"opens":opens(&snapshot),"claim_process":"fresh-process-per-claim"})
        );
    }
    if failed {
        std::process::exit(1);
    }
}
