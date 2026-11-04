// run.py exports this file byte-for-byte from the immutable package source H.
// It supplies the official Worker/loopback transport, not SDK implementation.
include!("official.rs");

mod installed_conditions {
    use super::*;
    use durable_workflow::{RawRequest, WorkflowResultOptions};

    type Outcome<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

    fn check(ok: bool, message: &str) -> Outcome<()> {
        if ok {
            Ok(())
        } else {
            Err(message.into())
        }
    }

    fn event(kind: &str, at: &str, payload: Value) -> Value {
        json!({"event_type": kind, "timestamp": at, "payload": payload})
    }

    // Faithful public-Worker port of H's decision_history: sequences, row order,
    // occurrence identity, timestamps and timeout values are kept unchanged.
    fn decision_history(reopen: u64, genuine_timeout: bool, tail_fire: bool) -> Vec<Value> {
        let wait = |seq: u64, at: &str, timeout: u64| {
            event(
                "ConditionWaitOpened",
                at,
                json!({
                    "sequence": seq, "condition_wait_id": format!("condition:{seq}"),
                    "condition_wait_occurrence_id": "rust:condition-wait:0",
                    "condition_key": "decision-1", "condition_definition_fingerprint": "sha256:decision-v1",
                    "timeout_seconds": timeout
                }),
            )
        };
        let timer = |kind: &str, seq: u64, at: &str, delay: u64| {
            event(
                kind,
                at,
                json!({
                    "sequence": seq, "timer_id": format!("condition-timer:{seq}"),
                    "timer_kind": "condition_timeout", "condition_wait_id": format!("condition:{seq}"),
                    "delay_seconds": delay
                }),
            )
        };
        let orphan = timer("TimerFired", 1, "2026-11-04T04:12:36.956000+00:00", 300);
        let mut history = vec![
            wait(1, "2026-11-04T04:07:36.890000+00:00", 300),
            timer("TimerScheduled", 1, "2026-11-04T04:07:36.891000+00:00", 300),
        ];
        if genuine_timeout {
            history.push(orphan.clone());
        }
        history.extend([
            wait(2, "2026-11-04T04:07:36.908000+00:00", 300),
            timer("TimerScheduled", 2, "2026-11-04T04:07:36.909000+00:00", 300),
            event("SignalReceived", "2026-11-04T04:09:36.979000+00:00", json!({
                "signal_name": "decision", "signal_id": "s1", "workflow_command_id": "command-s1",
                "arguments": encode_payload(&json!(["proposal-1"]), DEFAULT_CODEC).unwrap()
            })),
            event("ConditionWaitSatisfied", "2026-11-04T04:09:37.008000+00:00", json!({
                "sequence": 2, "condition_wait_id": "condition:2",
                "condition_wait_occurrence_id": "rust:condition-wait:0", "condition_key": "decision-1",
                "condition_definition_fingerprint": "sha256:decision-v1"
            })),
            timer("TimerCancelled", 2, "2026-11-04T04:09:37.009000+00:00", 300),
            wait(3, "2026-11-04T04:09:37.010000+00:00", reopen),
            timer("TimerScheduled", 3, "2026-11-04T04:09:37.011000+00:00", reopen),
        ]);
        if !genuine_timeout {
            history.push(orphan);
        }
        if tail_fire {
            history.push(timer(
                "TimerFired",
                3,
                "2026-11-04T04:12:37.216000+00:00",
                reopen,
            ));
        }
        history
    }

    async fn decision_replay(history: Vec<Value>, delivery: &str) -> Outcome<Value> {
        let id = format!("installed-orphan-{delivery}");
        let task = json!({"task_id": id, "workflow_id": "installed-orphan", "run_id": "installed-run",
            "workflow_type": "installed.decision", "arguments": encode_payload(&json!([]), DEFAULT_CODEC)?,
            "payload_codec": DEFAULT_CODEC, "history_events": history, "workflow_task_attempt": 1,
            "lease_owner": "regression-corpus-worker"});
        let server = FixtureServer::start(task, 0, None);
        let client = Client::builder(server.base_url())
            .timeout(Duration::from_secs(2))
            .build()?;
        let typed_error = Arc::new(Mutex::new(None::<Value>));
        let captured_error = Arc::clone(&typed_error);
        let mut worker =
            Worker::new(client, "regression-corpus").worker_id("regression-corpus-worker");
        worker.register_workflow("installed.decision", move |ctx, _| {
            let captured_error = Arc::clone(&captured_error);
            async move {
                let outcome = match ctx
                    .wait_condition(
                        ConditionWaitOptions::new("decision-1", "sha256:decision-v1")
                            .timeout(Duration::from_secs(300)),
                        || Ok(false),
                    )
                    .await
                {
                    Err(Error::NonDeterministicReplay(failure)) => {
                        *captured_error.lock().unwrap() = Some(json!({"reason": failure.reason,
                            "sequence": failure.sequence}));
                        return Err(Error::NonDeterministicReplay(failure));
                    }
                    result => result?,
                };
                Ok(json!(if outcome.is_satisfied() {
                    "satisfied"
                } else {
                    "timed_out"
                }))
            }
        });
        check(
            worker.run_once().await? == 1,
            "one history task must settle",
        )?;
        let completion = server.request_body(&format!("/api/worker/workflow-tasks/{id}/complete"));
        let failure = server.request_body(&format!("/api/worker/workflow-tasks/{id}/fail"));
        check(
            completion.is_some() != failure.is_some(),
            "exactly one task report is required",
        )?;
        let captured = typed_error.lock().unwrap().clone();
        Ok(json!({"completion": completion, "failure": failure, "typed_error": captured}))
    }

    #[tokio::test]
    async fn installed_history_replay() -> Outcome<()> {
        let mut results = serde_json::Map::new();
        for name in [
            "two-member-condition-group-server-reopen.json",
            "timer-separated-condition-group-server-reopen.json",
            "cooperative-grouped-condition-reopen.json",
            "condition-orphaned-duplicate-timeout-keeps-wait.json",
        ] {
            let path = Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures/replay-regressions")
                .join(name);
            let fixture: Value = serde_json::from_slice(&fs::read(path)?)?;
            let a = execute_fixture_delivery(&fixture, "installed-a").await?;
            let b = execute_fixture_delivery(&fixture, "installed-b").await?;
            check(
                a == b,
                "fresh workers must produce identical canonical replay results",
            )?;
            results.insert(name.to_string(), a);
        }
        for reopen in [300, 180] {
            for tail in [false, true] {
                let label = format!("reopen-{reopen}-tail-{tail}");
                let history = decision_history(reopen, false, tail);
                let a = decision_replay(history.clone(), "a").await?;
                let b = decision_replay(history.clone(), "b").await?;
                check(a == b, "fresh workers must preserve orphan/tail results")?;
                check(
                    a["failure"].is_null(),
                    "valid orphan history must not fail replay",
                )?;
                let commands = a["completion"]["commands"]
                    .as_array()
                    .ok_or("missing command list")?;
                check(
                    commands.len() == 1,
                    "orphan/tail replay must produce exactly one command",
                )?;
                if tail {
                    check(
                        commands[0]["type"] == "complete_workflow",
                        "tail timer must complete as timed out",
                    )?;
                    let envelope: PayloadEnvelope =
                        serde_json::from_value(commands[0]["result"].clone())?;
                    let value: Value = decode_payload(&envelope)?;
                    check(
                        value == "timed_out",
                        "tail result must be the typed timed-out value",
                    )?;
                } else {
                    check(
                        commands[0]["type"] == "open_condition_wait"
                            && commands[0]["timeout_seconds"] == 0,
                        "late orphan must keep logical wait open with zero remaining time",
                    )?;
                }
                results.insert(label, json!({"input_history": history, "observed": a}));
            }
        }
        let history = decision_history(300, true, false);
        let negative = decision_replay(history.clone(), "negative-a").await?;
        let repeat = decision_replay(history.clone(), "negative-b").await?;
        check(
            negative == repeat,
            "genuine-timeout refusal must survive fresh workers",
        )?;
        check(
            negative["completion"].is_null(),
            "genuine timeout must not publish completion",
        )?;
        check(
            negative["typed_error"]["reason"] == "condition_wait_reopened_after_timeout"
                && negative["typed_error"]["sequence"] == 2
                && negative["failure"]["failure"]["type"] == "RustWorkflowTaskFailure",
            "genuine-timeout reopen must retain its typed refusal reason",
        )?;
        results.insert(
            "genuine-timeout".into(),
            json!({"input_history": history, "observed": negative}),
        );
        fs::write(
            "/state/replay.json",
            serde_json::to_vec_pretty(&json!({"outcome": "pass", "cases": results}))?,
        )?;
        Ok(())
    }

    async fn history(client: &Client, workflow: &str, run: &str) -> Outcome<Vec<Value>> {
        let mut events = Vec::new();
        let mut next = None;
        for _ in 0..50 {
            let mut request = RawRequest::get(["workflows", workflow, "runs", run, "history"]);
            if let Some(token) = next.take() {
                request = request.query("next_page_token", token);
            }
            let response = client.control_plane_request(request).await?;
            check(response.status() == 200, "history read must succeed")?;
            let page = response.json()?;
            events.extend(
                page["events"]
                    .as_array()
                    .ok_or("history has no event list")?
                    .iter()
                    .cloned(),
            );
            next = page["next_page_token"]
                .as_str()
                .filter(|s| !s.is_empty())
                .map(str::to_owned);
            if next.is_none() {
                return Ok(events);
            }
        }
        Err("history pagination did not end".into())
    }

    async fn wait_history(
        client: &Client,
        workflow: &str,
        run: &str,
        opens: usize,
    ) -> Outcome<Vec<Value>> {
        for _ in 0..300 {
            let events = history(client, workflow, run).await?;
            check(
                !events.iter().any(|e| {
                    matches!(
                        e["event_type"].as_str(),
                        Some("WorkflowFailed" | "ReplayFailure")
                    )
                }),
                "live workflow must not record a replay failure",
            )?;
            if events
                .iter()
                .filter(|e| e["event_type"] == "ConditionWaitOpened")
                .count()
                >= opens
            {
                return Ok(events);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Err("condition opens did not arrive within the bounded wait".into())
    }

    #[tokio::test]
    #[ignore = "requires separately admitted unchanged published PHP 2.11.8 runtime"]
    async fn installed_live_group() -> Outcome<()> {
        let token = Some(fs::read_to_string("/credentials/token")?);
        let operator = Client::builder("http://qualification-origin:8080")
            .control_token(token.clone())
            .build()?;
        let mut ready = false;
        for _ in 0..120 {
            if let Ok(response) = operator
                .control_plane_request(RawRequest::get(["health"]).timeout(Duration::from_secs(2)))
                .await
            {
                if response.status() == 200 {
                    ready = true;
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        check(ready, "PHP runtime readiness deadline")?;
        let cluster = operator
            .control_plane_request(RawRequest::get(["cluster", "info"]))
            .await?;
        check(
            cluster.status() == 200,
            "cluster identity read must succeed",
        )?;
        check(
            cluster.json()?["version"] == "2.11.8",
            "wrong PHP Server version",
        )?;
        let client = Client::builder("http://qualification-origin:8080")
            .worker_token(token)
            .build()?;
        let mut worker = Worker::new(client, "installed-condition-history")
            .worker_id("installed-condition-history")
            .max_concurrent_workflow_tasks(1)
            .poll_timeout(Duration::from_millis(100));
        worker.register_workflow("installed.two-member", |ctx, _| async move {
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
                    move || Ok(!approvals.signals("approve")?.is_empty()),
                ),
            ])
            .await?;
            Ok(json!("both"))
        });
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let execution = tokio::spawn(async move {
            worker
                .run_until(async {
                    let _ = stopped.await;
                })
                .await
        });
        let driver = async {
            let handle = operator
                .start_workflow(
                    "installed.two-member",
                    "installed-condition-history",
                    "installed-two-member",
                    json!([]),
                )
                .await?;
            let run = handle
                .run_id
                .as_deref()
                .ok_or("start omitted selected run")?;
            let initial = wait_history(&operator, &handle.workflow_id, run, 2).await?;
            handle.signal("vote", json!(["one"])).await?;
            let reopened = wait_history(&operator, &handle.workflow_id, run, 4).await?;
            handle.signal("vote", json!(["two"])).await?;
            handle.signal("approve", json!(["yes"])).await?;
            let value = handle
                .result(WorkflowResultOptions {
                    poll_interval: Duration::from_millis(100),
                    timeout: Duration::from_secs(30),
                })
                .await?;
            check(
                value == "both",
                "two-member workflow must return typed both",
            )?;
            let final_history = history(&operator, &handle.workflow_id, run).await?;
            fs::write(
                "/state/live.json",
                serde_json::to_vec_pretty(&json!({"outcome": "pass",
                "workflow_id": handle.workflow_id, "run_id": run, "result": value,
                "initial": initial, "reopened": reopened, "history": final_history}))?,
            )?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        }
        .await;
        let _ = stop.send(());
        execution.await??;
        driver
    }
}
