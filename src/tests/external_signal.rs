//! durable-workflow/.github#223: `WorkflowContext::signal_external_workflow`.
use super::*;

const WORKFLOW: &str = "signal-sender";

fn advertised() -> Value {
    json!({
        "signal_external_workflow": true,
        "supported_workflow_task_commands": ["schedule_activity", "signal_external_workflow"],
    })
}

fn sender(supported: bool, run_id: Option<&'static str>, args: Value) -> Worker {
    let mut worker = Worker::new(
        Client::builder("http://127.0.0.1:1").build().unwrap(),
        "queue",
    );
    worker.register_workflow(WORKFLOW, move |ctx, _input| {
        let args = args.clone();
        async move {
            let mut call = ctx.signal_external_workflow("order-42", "approve", args);
            if let Some(run_id) = run_id {
                call = call.run_id(run_id);
            }
            let sent = call.await?;
            Ok(json!({
                "outcome": sent.outcome.as_str(),
                "reason": sent.reason,
                "message": sent.message,
                "target_workflow_id": sent.target_workflow_id,
                "target_run_id": sent.target_run_id,
                "signal_name": sent.signal_name,
                "request_id": sent.request_id,
                "target_command_id": sent.target_command_id,
                "target_signal_id": sent.target_signal_id,
                "retained": sent.retained,
            }))
        }
    });
    worker.observe_workflow_poll_capabilities(supported.then(advertised).as_ref());
    worker
}

fn task(events: Vec<Value>) -> WorkflowTask {
    let mut task = workflow_task(
        WORKFLOW,
        events
            .into_iter()
            .map(|event| serde_json::from_value(event).unwrap())
            .collect(),
        DEFAULT_CODEC,
    );
    task.run_id = Some("sender-run".into());
    task
}

fn requested(args: Value) -> Value {
    json!({"event_type": "ExternalSignalRequested", "payload": {
        "sequence": 1,
        "target_workflow_instance_id": "order-42",
        "target_workflow_run_id": null,
        "signal_name": "approve",
        "arguments": encode_value_envelope(&args, DEFAULT_CODEC).unwrap(),
        "payload_codec": "avro",
        "request_id": "workflow-signal:sender-run:1",
    }})
}

fn delivered() -> Value {
    json!({"event_type": "ExternalSignalDelivered", "payload": {
        "sequence": 1,
        "target_workflow_instance_id": "order-42",
        "target_workflow_run_id": "order-run",
        "signal_name": "approve",
        "request_id": "workflow-signal:sender-run:1",
        "outcome": "delivered",
        "target_command_id": "command-7",
        "target_signal_id": "signal-9",
        "retained": false,
    }})
}

fn failed(outcome: &str, reason: &str, run: Value, command: Value) -> Value {
    json!({"event_type": "ExternalSignalFailed", "payload": {
        "sequence": 1,
        "target_workflow_instance_id": "order-42",
        "target_workflow_run_id": run,
        "signal_name": "approve",
        "request_id": "workflow-signal:sender-run:1",
        "outcome": outcome,
        "reason": reason,
        "message": format!("{reason} detail"),
        "target_command_id": command,
    }})
}

fn completed_result(commands: &[Value]) -> Value {
    assert_eq!(commands.len(), 1, "{commands:?}");
    assert_eq!(commands[0]["type"], "complete_workflow");
    decode_wire_value(&commands[0]["result"], DEFAULT_CODEC).unwrap()
}

fn replay_failure(error: Error) -> ReplayFailure {
    match error {
        Error::NonDeterministicReplay(failure) => failure,
        other => panic!("expected a replay failure, got {other:?}"),
    }
}

#[test]
fn capability_requires_the_flag_and_the_advertised_command() {
    assert!(crate::external_signal::runtime_supports(
        Some(&advertised())
    ));
    assert!(crate::external_signal::runtime_supports(Some(&json!({
        "signal_external_workflow": {"supported": true},
        "supported_workflow_task_commands": ["signal_external_workflow"],
    }))));
    for capabilities in [
        json!({}),
        json!({"signal_external_workflow": true}),
        json!({"supported_workflow_task_commands": ["signal_external_workflow"]}),
        json!({"signal_external_workflow": false,
            "supported_workflow_task_commands": ["signal_external_workflow"]}),
        json!({"signal_external_workflow": "true",
            "supported_workflow_task_commands": ["signal_external_workflow"]}),
    ] {
        assert!(
            !crate::external_signal::runtime_supports(Some(&capabilities)),
            "{capabilities}"
        );
    }
    assert!(!crate::external_signal::runtime_supports(None));
}

#[test]
fn first_execution_emits_the_exact_contract_command() {
    let worker = sender(true, None, json!(["manager", 2]));
    let commands = worker.execute_workflow_task(task(vec![])).unwrap();
    assert_eq!(commands.len(), 1, "{commands:?}");
    let command = commands[0].as_object().unwrap();
    let mut keys: Vec<_> = command.keys().map(String::as_str).collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        ["arguments", "run_id", "signal_name", "type", "workflow_id"]
    );
    assert_eq!(command["type"], "signal_external_workflow");
    assert_eq!(command["workflow_id"], "order-42");
    assert_eq!(command["run_id"], Value::Null);
    assert_eq!(command["signal_name"], "approve");
    assert_eq!(command["arguments"]["codec"], "avro");
    assert_eq!(
        decode_wire_value(&command["arguments"], DEFAULT_CODEC).unwrap(),
        json!(["manager", 2])
    );
}

#[test]
fn run_targeting_and_empty_arguments_follow_the_contract() {
    let worker = sender(true, Some("order-run-1"), Value::Null);
    let commands = worker.execute_workflow_task(task(vec![])).unwrap();
    assert_eq!(commands[0]["run_id"], "order-run-1");
    // No arguments are sent as an encoded empty array.
    assert_eq!(
        decode_wire_value(&commands[0]["arguments"], DEFAULT_CODEC).unwrap(),
        json!([])
    );

    let worker = sender(true, None, json!("single"));
    let commands = worker.execute_workflow_task(task(vec![])).unwrap();
    assert_eq!(
        decode_wire_value(&commands[0]["arguments"], DEFAULT_CODEC).unwrap(),
        json!(["single"])
    );
}

#[test]
fn an_unadvertised_server_is_refused_in_workflow_code_before_submitting() {
    let worker = sender(false, None, json!([]));
    let error = worker.execute_workflow_task(task(vec![])).unwrap_err();
    assert!(
        matches!(error, Error::SignalExternalWorkflowUnavailable),
        "{error:?}"
    );
    assert!(error
        .to_string()
        .starts_with("signal_external_workflow_unavailable:"));

    // Workflow code can observe the refusal and choose another path; nothing
    // is submitted for the refused signal.
    let mut worker = Worker::new(
        Client::builder("http://127.0.0.1:1").build().unwrap(),
        "queue",
    );
    worker.register_workflow(WORKFLOW, |ctx, _input| async move {
        match ctx
            .signal_external_workflow("order-42", "approve", json!([]))
            .await
        {
            Err(Error::SignalExternalWorkflowUnavailable) => Ok(json!("fallback")),
            other => Ok(json!(format!("unexpected: {other:?}"))),
        }
    });
    let commands = worker.execute_workflow_task(task(vec![])).unwrap();
    assert_eq!(completed_result(&commands), json!("fallback"));

    // Advertisement is read from each workflow poll.
    worker.observe_workflow_poll_capabilities(Some(&advertised()));
    worker.observe_workflow_poll_capabilities(Some(&json!({"signal_external_workflow": true})));
    let commands = worker.execute_workflow_task(task(vec![])).unwrap();
    assert_eq!(completed_result(&commands), json!("fallback"));
}

#[test]
fn invalid_targets_are_refused_before_submitting() {
    for (workflow_id, signal_name, run_id) in [
        (String::new(), "approve".to_string(), None),
        ("x".repeat(192), "approve".to_string(), None),
        ("order-42".to_string(), " ".to_string(), None),
        ("order-42".to_string(), "s".repeat(192), None),
        ("order-42".to_string(), "é".repeat(128), None),
        ("order-42".to_string(), "approve".to_string(), Some("")),
    ] {
        let mut worker = Worker::new(
            Client::builder("http://127.0.0.1:1").build().unwrap(),
            "queue",
        );
        worker.register_workflow(WORKFLOW, move |ctx, _input| {
            let (workflow_id, signal_name) = (workflow_id.clone(), signal_name.clone());
            async move {
                let mut call = ctx.signal_external_workflow(workflow_id, signal_name, json!([]));
                if let Some(run_id) = run_id {
                    call = call.run_id(run_id);
                }
                match call.await {
                    Err(Error::InvalidExternalSignal(message)) => Ok(json!(message)),
                    other => Ok(json!(format!("unexpected: {other:?}"))),
                }
            }
        });
        worker.observe_workflow_poll_capabilities(Some(&advertised()));
        let commands = worker.execute_workflow_task(task(vec![])).unwrap();
        let message = completed_result(&commands);
        assert!(
            !message.as_str().unwrap().starts_with("unexpected"),
            "{message}"
        );
    }
    // 191 characters is accepted.
    let mut worker = Worker::new(
        Client::builder("http://127.0.0.1:1").build().unwrap(),
        "queue",
    );
    worker.register_workflow(WORKFLOW, |ctx, _input| async move {
        ctx.signal_external_workflow("é".repeat(191), "approve", json!([]))
            .await?;
        Ok(json!("sent"))
    });
    worker.observe_workflow_poll_capabilities(Some(&advertised()));
    let commands = worker.execute_workflow_task(task(vec![])).unwrap();
    assert_eq!(commands[0]["type"], "signal_external_workflow");
}

/// Run one signal name through workflow code: the refusal message, or the
/// emitted command's signal name.
fn signal_name_outcome(signal_name: String) -> std::result::Result<Value, String> {
    let mut worker = Worker::new(
        Client::builder("http://127.0.0.1:1").build().unwrap(),
        "queue",
    );
    worker.register_workflow(WORKFLOW, move |ctx, _input| {
        let signal_name = signal_name.clone();
        async move {
            match ctx
                .signal_external_workflow("order-42", signal_name, json!([]))
                .await
            {
                Err(Error::InvalidExternalSignal(message)) => Ok(json!({"refused": message})),
                other => Ok(json!(format!("unexpected: {other:?}"))),
            }
        }
    });
    worker.observe_workflow_poll_capabilities(Some(&advertised()));
    let commands = worker.execute_workflow_task(task(vec![])).unwrap();
    match commands[0]["type"].as_str() {
        Some("signal_external_workflow") => Ok(commands[0]["signal_name"].clone()),
        _ => {
            // A refusal emits no command: only the workflow's own completion.
            let result = completed_result(&commands);
            Err(result["refused"]
                .as_str()
                .unwrap_or_else(|| panic!("{result}"))
                .to_string())
        }
    }
}

#[test]
fn signal_names_follow_the_server_rule_before_any_command_is_emitted() {
    for (name, refusal) in [
        (String::new(), "signal_name must not be empty or blank"),
        (" \t ".to_string(), "signal_name must not be empty or blank"),
        (
            "ap\nprove".to_string(),
            "signal_name must not contain control characters",
        ),
        (
            "ap\u{0}prove".to_string(),
            "signal_name must not contain control characters",
        ),
        (
            "ap\u{1f}prove".to_string(),
            "signal_name must not contain control characters",
        ),
        (
            "ap\u{7f}prove".to_string(),
            "signal_name must not contain control characters",
        ),
        (
            "s".repeat(192),
            "signal_name must be at most 191 characters",
        ),
        // 128 characters but 256 bytes.
        ("é".repeat(128), "signal_name must be at most 255 bytes"),
    ] {
        assert_eq!(
            signal_name_outcome(name.clone()),
            Err(refusal.to_string()),
            "{name:?}"
        );
    }
    for name in [
        "s".repeat(191),
        // 127 characters, 254 bytes.
        "é".repeat(127),
        "approve.order-42 v2".to_string(),
    ] {
        assert_eq!(signal_name_outcome(name.clone()), Ok(json!(name)));
    }
}

#[test]
fn delivered_replay_resolves_from_history_without_resending() {
    // The capability gate applies only to new commands: replay works even
    // when the polling Server no longer advertises the command.
    for supported in [true, false] {
        let worker = sender(supported, None, json!(["manager"]));
        let commands = worker
            .execute_workflow_task(task(vec![requested(json!(["manager"])), delivered()]))
            .unwrap();
        assert_eq!(
            completed_result(&commands),
            json!({
                "outcome": "delivered",
                "reason": null,
                "message": null,
                "target_workflow_id": "order-42",
                "target_run_id": "order-run",
                "signal_name": "approve",
                "request_id": "workflow-signal:sender-run:1",
                "target_command_id": "command-7",
                "target_signal_id": "signal-9",
                "retained": false,
            })
        );
    }
}

#[test]
fn failed_outcomes_are_returned_values_with_their_reasons() {
    for (outcome, reason, run, command) in [
        (
            "target_not_found",
            "instance_not_found",
            Value::Null,
            Value::Null,
        ),
        (
            "target_not_found",
            "run_not_found",
            Value::Null,
            Value::Null,
        ),
        (
            "target_closed",
            "run_not_active",
            json!("order-run"),
            json!("command-3"),
        ),
        (
            "target_closed",
            "historical_run",
            json!("order-run-0"),
            Value::Null,
        ),
        (
            "rejected",
            "unknown_signal",
            json!("order-run"),
            json!("command-4"),
        ),
        ("rejected", "self_target", Value::Null, Value::Null),
        ("rejected", "request_id_conflict", Value::Null, Value::Null),
    ] {
        let worker = sender(true, None, json!(["manager"]));
        let commands = worker
            .execute_workflow_task(task(vec![
                requested(json!(["manager"])),
                failed(outcome, reason, run.clone(), command.clone()),
            ]))
            .unwrap();
        let result = completed_result(&commands);
        assert_eq!(result["outcome"], outcome);
        assert_eq!(result["reason"], reason);
        assert_eq!(result["message"], format!("{reason} detail"));
        assert_eq!(result["target_run_id"], run);
        assert_eq!(result["target_command_id"], command);
        assert_eq!(result["target_signal_id"], Value::Null);
    }
}

#[test]
fn retained_delivery_is_reported() {
    let mut event = delivered();
    event["payload"]["retained"] = json!(true);
    let worker = sender(true, None, json!(["manager"]));
    let commands = worker
        .execute_workflow_task(task(vec![requested(json!(["manager"])), event]))
        .unwrap();
    assert_eq!(completed_result(&commands)["retained"], true);
}

#[test]
fn a_different_target_name_run_or_arguments_is_nondeterministic() {
    let history = || vec![requested(json!(["manager"])), delivered()];
    let mut worker = Worker::new(
        Client::builder("http://127.0.0.1:1").build().unwrap(),
        "queue",
    );
    for (name, target, signal, run, args) in [
        ("target", "order-43", "approve", None, json!(["manager"])),
        ("name", "order-42", "reject", None, json!(["manager"])),
        (
            "run",
            "order-42",
            "approve",
            Some("order-run"),
            json!(["manager"]),
        ),
        (
            "arguments",
            "order-42",
            "approve",
            None,
            json!(["director"]),
        ),
    ] {
        let workflow_type = format!("{WORKFLOW}-{name}");
        worker.register_workflow(workflow_type.clone(), move |ctx, _input| {
            let args = args.clone();
            async move {
                let mut call = ctx.signal_external_workflow(target, signal, args);
                if let Some(run) = run {
                    call = call.run_id(run);
                }
                call.await?;
                Ok(json!("unreachable"))
            }
        });
        let mut replay = task(history());
        replay.workflow_type = workflow_type;
        let failure = replay_failure(worker.execute_workflow_task(replay).unwrap_err());
        assert_eq!(failure.reason, "recorded_command_detail_mismatch", "{name}");
        assert_eq!(failure.sequence, Some(1), "{name}");
        assert!(
            failure.expected.as_deref().unwrap().contains(name),
            "{name}: {failure:?}"
        );
    }
}

#[test]
fn another_command_at_the_recorded_sequence_is_nondeterministic() {
    let mut worker = Worker::new(
        Client::builder("http://127.0.0.1:1").build().unwrap(),
        "queue",
    );
    worker.register_workflow(WORKFLOW, |ctx, _input| async move {
        ctx.side_effect(|| 1)?;
        Ok(json!("unreachable"))
    });
    let failure = replay_failure(
        worker
            .execute_workflow_task(task(vec![requested(json!(["manager"])), delivered()]))
            .unwrap_err(),
    );
    assert_eq!(failure.reason, "recorded_command_mismatch");
    assert_eq!(failure.expected.as_deref(), Some("external signal"));

    let worker = sender(true, None, json!(["manager"]));
    let side_effect = json!({"event_type": "SideEffectRecorded", "payload": {
        "sequence": 1, "result": encode_value_envelope(&json!(1), DEFAULT_CODEC).unwrap(),
    }});
    let failure = replay_failure(
        worker
            .execute_workflow_task(task(vec![side_effect]))
            .unwrap_err(),
    );
    assert_eq!(failure.reason, "recorded_command_mismatch");
    assert_eq!(failure.actual.as_deref(), Some("external signal:approve"));
}

#[test]
fn a_request_without_its_outcome_waits_and_never_resends() {
    let worker = sender(true, None, json!(["manager"]));
    let decision = worker
        .execute_workflow_task_decision(task(vec![requested(json!(["manager"]))]))
        .unwrap();
    assert!(decision.commands.is_empty(), "{:?}", decision.commands);
}

#[test]
fn malformed_external_signal_history_is_refused() {
    let worker = sender(true, None, json!(["manager"]));
    let mut wrong_outcome = delivered();
    wrong_outcome["payload"]["outcome"] = json!("rejected");
    let mut unknown_outcome = failed("lost", "x", Value::Null, Value::Null);
    unknown_outcome["payload"]["outcome"] = json!("lost");
    let mut other_target = delivered();
    other_target["payload"]["target_workflow_instance_id"] = json!("order-43");
    let mut other_key = delivered();
    other_key["payload"]["request_id"] = json!("workflow-signal:sender-run:2");
    let mut no_reason = failed("rejected", "x", Value::Null, Value::Null);
    no_reason["payload"]
        .as_object_mut()
        .unwrap()
        .remove("reason");
    for (reason, events) in [
        ("external_signal_request_count", vec![delivered()]),
        (
            "external_signal_request_count",
            vec![requested(json!([])), requested(json!([])), delivered()],
        ),
        (
            "external_signal_resolution_count",
            vec![requested(json!(["manager"])), delivered(), delivered()],
        ),
        (
            "external_signal_outcome_invalid",
            vec![requested(json!(["manager"])), wrong_outcome],
        ),
        (
            "external_signal_outcome_invalid",
            vec![requested(json!(["manager"])), unknown_outcome],
        ),
        (
            "external_signal_identity_mismatch",
            vec![requested(json!(["manager"])), other_target],
        ),
        (
            "external_signal_identity_mismatch",
            vec![requested(json!(["manager"])), other_key],
        ),
        (
            "external_signal_field_missing",
            vec![requested(json!(["manager"])), no_reason],
        ),
    ] {
        let failure = replay_failure(worker.execute_workflow_task(task(events)).unwrap_err());
        assert_eq!(failure.reason, reason);
    }

    // Another command kind at the same sequence is a collision.
    let side_effect = json!({"event_type": "SideEffectRecorded", "payload": {
        "sequence": 1, "result": encode_value_envelope(&json!(1), DEFAULT_CODEC).unwrap(),
    }});
    let failure = replay_failure(
        worker
            .execute_workflow_task(task(vec![
                requested(json!(["manager"])),
                delivered(),
                side_effect,
            ]))
            .unwrap_err(),
    );
    assert_eq!(failure.reason, "durable_command_sequence_collision");
}

#[test]
fn the_next_command_takes_the_following_sequence() {
    let mut worker = Worker::new(
        Client::builder("http://127.0.0.1:1").build().unwrap(),
        "queue",
    );
    worker.register_workflow(WORKFLOW, |ctx, _input| async move {
        let sent = ctx
            .signal_external_workflow("order-42", "approve", json!(["manager"]))
            .await?;
        let after = ctx.side_effect(|| "after".to_string())?;
        Ok(json!([sent.outcome.as_str(), after]))
    });
    worker.observe_workflow_poll_capabilities(Some(&advertised()));
    let side_effect = json!({"event_type": "SideEffectRecorded", "payload": {
        "sequence": 2, "result": encode_value_envelope(&json!("after"), DEFAULT_CODEC).unwrap(),
    }});
    let commands = worker
        .execute_workflow_task(task(vec![
            requested(json!(["manager"])),
            failed(
                "target_not_found",
                "instance_not_found",
                Value::Null,
                Value::Null,
            ),
            side_effect,
        ]))
        .unwrap();
    assert_eq!(
        completed_result(&commands),
        json!(["target_not_found", "after"])
    );
}

fn environment() -> crate::testing::WorkflowEnvironment {
    // A Worker that never saw the capability: the offline environment does
    // not submit anything, so the Server gate does not apply there.
    crate::testing::WorkflowEnvironment::from_worker(sender(false, None, json!(["manager"])))
}

#[test]
fn offline_replay_resolves_recorded_signals_and_reports_new_ones_as_divergence() {
    let env = environment();
    let recorded = vec![
        requested(json!(["manager"])),
        failed(
            "target_closed",
            "run_not_active",
            json!("order-run"),
            json!("command-3"),
        ),
    ];
    let output = sender(true, None, json!(["manager"]))
        .execute_workflow_task(task(recorded.clone()))
        .unwrap()[0]["result"]
        .clone();
    let mut events = recorded;
    events.push(json!({"event_type": "WorkflowCompleted", "payload": {"output": output}}));
    let history =
        crate::testing::WorkflowHistory::from_history(WORKFLOW, json!([]), json!(events)).unwrap();
    let report = env.replay(&history).unwrap();
    assert!(report.completed);
    assert_eq!(report.commands.len(), 1, "{:?}", report.commands);
    assert_eq!(report.commands[0]["type"], "complete_workflow");

    let history =
        crate::testing::WorkflowHistory::from_history(WORKFLOW, json!([]), json!([])).unwrap();
    let failure = replay_failure(env.replay(&history).unwrap_err());
    assert_eq!(failure.reason, "unrecorded_command");
    assert_eq!(failure.sequence, Some(1));
}

#[tokio::test]
async fn offline_execution_refuses_the_integration_only_command_explicitly() {
    let mut env = environment();
    let error = env.run_workflow(WORKFLOW, json!([])).await.unwrap_err();
    assert!(
        matches!(&error, Error::Testing(crate::testing::TestingError::UnsupportedCommand(kind)) if kind == "signal_external_workflow"),
        "{error:?}"
    );
}
