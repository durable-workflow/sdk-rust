use super::*;

fn policy_worker(
    parent: ParentClosePolicy,
    operation: CancellationPolicy,
    mode: &'static str,
    enabled: bool,
) -> Worker {
    let mut worker = Worker::new(
        Client::builder("http://127.0.0.1:1").build().unwrap(),
        "queue",
    )
    .cooperative_cancellation(enabled);
    worker.register_workflow("child-policy", move |ctx, _input| async move {
        let options = ChildWorkflowOptions::new("queue")
            .parent_close_policy(parent)
            .cancellation_policy(operation);
        match mode {
            "parallel" => {
                ctx.parallel(vec![ParallelOperation::child_workflow(
                    "child",
                    options,
                    json!(["argument"]),
                )])
                .await?;
            }
            "selection" => {
                ctx.select_keyed(vec![(
                    SelectionKey::from("child"),
                    ParallelOperation::child_workflow("child", options, json!(["argument"])),
                )])
                .await?;
            }
            _ => {
                ctx.start_child_workflow("child", options, json!(["argument"]))
                    .await?;
            }
        }
        Ok(json!("finished"))
    });
    worker
}

fn policy_task(events: Vec<HistoryEvent>) -> WorkflowTask {
    let mut task = workflow_task("child-policy", events, DEFAULT_CODEC);
    task.run_id = Some("run".into());
    task
}

fn policy_event(kind: &str, payload: Value) -> HistoryEvent {
    serde_json::from_value(json!({"event_type": kind, "payload": payload})).unwrap()
}

fn scheduled_policy_history(command: &Value) -> Vec<HistoryEvent> {
    let mut payload = command.clone();
    let object = payload.as_object_mut().unwrap();
    object.remove("type");
    object.remove("arguments");
    object.remove("queue");
    object.remove("workflow_type");
    object.insert("sequence".into(), json!(1));
    object.insert("child_workflow_type".into(), json!("child"));
    object.insert("child_workflow_run_id".into(), json!("child-run"));
    vec![policy_event("ChildWorkflowScheduled", payload)]
}

fn policy_cancellation_history(mut events: Vec<HistoryEvent>, mode: &str) -> Vec<HistoryEvent> {
    events.push(serde_json::from_value(json!({
        "event_type": "CooperativeCancellationRequested", "recorded_at": "2026-10-01T00:00:00Z",
        "payload": {"workflow_run_id": "run", "workflow_command_id": "request-1", "cleanup_deadline_at": "2026-10-01T00:00:30Z"},
    })).unwrap());
    events.push(policy_event("CooperativeCancellationDelivered", json!({
        "workflow_run_id": "run", "workflow_command_id": "request-1", "sequence": 1,
        "call_kind": if mode == "sequential" { "child" } else { "parallel" }, "sequence_span": 1,
    })));
    events
}

fn assert_policy_failure(error: Error, reason: &str) {
    let Error::NonDeterministicReplay(failure) = error else {
        panic!("expected replay failure, got {error:?}");
    };
    assert_eq!(failure.reason, reason);
    assert_eq!(failure.sequence, Some(1));
}

#[test]
fn child_policies_encode_all_choices_and_replay_the_same_snapshot() {
    for parent in [
        ParentClosePolicy::Abandon,
        ParentClosePolicy::RequestCancel,
        ParentClosePolicy::RequestCancellation,
        ParentClosePolicy::Terminate,
    ] {
        for operation in [
            CancellationPolicy::Abandon,
            CancellationPolicy::TryCancel,
            CancellationPolicy::WaitCancellationCompleted,
        ] {
            let worker = policy_worker(parent, operation, "sequential", true);
            let commands = worker.execute_workflow_task(policy_task(vec![])).unwrap();
            assert_eq!(commands[0]["parent_close_policy"], parent.as_str());
            if operation == CancellationPolicy::Abandon {
                assert!(commands[0].get("cancellation_policy").is_none());
            } else {
                assert_eq!(commands[0]["cancellation_policy"], operation.as_str());
            }
            assert_eq!(
                decode_wire_value(&commands[0]["arguments"], DEFAULT_CODEC).unwrap(),
                json!(["argument"])
            );
            let mut events = scheduled_policy_history(&commands[0]);
            events.push(policy_event(
                "ChildRunStarted",
                json!({"sequence": 1, "child_workflow_type": "child"}),
            ));
            events.push(policy_event(
                "ChildRunCompleted",
                json!({"sequence": 1, "result": fixture_envelope(json!("child-result"))}),
            ));
            let completed = worker.execute_workflow_task(policy_task(events)).unwrap();
            assert_eq!(
                decode_wire_value(&completed[0]["result"], DEFAULT_CODEC).unwrap(),
                json!("finished")
            );
        }
    }
}

#[test]
fn child_policies_are_checked_in_ordinary_parallel_selection_and_cancellation_replay() {
    for mode in ["sequential", "parallel", "selection"] {
        let original = policy_worker(
            ParentClosePolicy::RequestCancellation,
            CancellationPolicy::WaitCancellationCompleted,
            mode,
            true,
        );
        let commands = original.execute_workflow_task(policy_task(vec![])).unwrap();
        let scheduled = scheduled_policy_history(&commands[0]);
        for delivered in [false, true] {
            let events = if delivered {
                policy_cancellation_history(scheduled.clone(), mode)
            } else {
                scheduled.clone()
            };
            let unchanged = original
                .execute_workflow_task(policy_task(events.clone()))
                .unwrap();
            if delivered {
                assert_eq!(unchanged[0]["type"], "fail_workflow");
            } else {
                assert!(unchanged.is_empty());
            }
            for (parent, operation) in [
                (
                    ParentClosePolicy::Abandon,
                    CancellationPolicy::WaitCancellationCompleted,
                ),
                (
                    ParentClosePolicy::RequestCancellation,
                    CancellationPolicy::TryCancel,
                ),
            ] {
                let changed = policy_worker(parent, operation, mode, true);
                assert_policy_failure(
                    changed
                        .execute_workflow_task(policy_task(events.clone()))
                        .unwrap_err(),
                    "child_workflow_policy_changed",
                );
            }
        }
    }
}

#[test]
fn child_policies_preserve_historical_defaults_and_refuse_later_changes() {
    let worker = policy_worker(
        ParentClosePolicy::Abandon,
        CancellationPolicy::Abandon,
        "sequential",
        false,
    );
    let commands = worker.execute_workflow_task(policy_task(vec![])).unwrap();
    assert!(commands[0].get("cancellation_policy").is_none());
    let mut events = scheduled_policy_history(&commands[0]);
    events[0]
        .payload
        .as_object_mut()
        .unwrap()
        .remove("parent_close_policy");
    assert!(worker
        .execute_workflow_task(policy_task(events.clone()))
        .unwrap()
        .is_empty());
    let changed = policy_worker(
        ParentClosePolicy::RequestCancellation,
        CancellationPolicy::WaitCancellationCompleted,
        "sequential",
        true,
    );
    assert_policy_failure(
        changed
            .execute_workflow_task(policy_task(events))
            .unwrap_err(),
        "child_workflow_policy_changed",
    );
}

#[test]
fn child_policies_reject_invalid_and_conflicting_history() {
    let worker = policy_worker(
        ParentClosePolicy::RequestCancellation,
        CancellationPolicy::WaitCancellationCompleted,
        "sequential",
        true,
    );
    let commands = worker.execute_workflow_task(policy_task(vec![])).unwrap();
    let scheduled = scheduled_policy_history(&commands[0]);
    for event_type in [
        "ChildRunStarted",
        "ChildRunCompleted",
        "ChildRunFailed",
        "ChildRunCancelled",
        "ChildRunTerminated",
    ] {
        for (field, conflicting) in [
            ("parent_close_policy", "terminate"),
            ("cancellation_policy", "try_cancel"),
        ] {
            let mut events = scheduled.clone();
            let mut payload = json!({"sequence": 1});
            payload[field] = json!(conflicting);
            events.push(policy_event(event_type, payload));
            assert_policy_failure(
                worker
                    .execute_workflow_task(policy_task(events))
                    .unwrap_err(),
                "child_workflow_policy_history_conflict",
            );
        }
    }
    for field in ["parent_close_policy", "cancellation_policy"] {
        for invalid in [
            json!("unknown"),
            json!(true),
            json!(1),
            json!([]),
            json!({"type": "abandon"}),
        ] {
            let mut events = scheduled.clone();
            events[0].payload[field] = invalid;
            assert_policy_failure(
                worker
                    .execute_workflow_task(policy_task(events))
                    .unwrap_err(),
                "invalid_child_workflow_policy_history",
            );
        }
    }
}

#[test]
fn child_policies_require_worker_opt_in_but_legacy_choices_remain_available() {
    for (parent, operation) in [
        (
            ParentClosePolicy::RequestCancellation,
            CancellationPolicy::Abandon,
        ),
        (ParentClosePolicy::Abandon, CancellationPolicy::TryCancel),
        (
            ParentClosePolicy::Abandon,
            CancellationPolicy::WaitCancellationCompleted,
        ),
    ] {
        let worker = policy_worker(parent, operation, "sequential", false);
        let Error::CooperativeCancellationUnavailable(message) = worker
            .execute_workflow_task(policy_task(vec![]))
            .unwrap_err()
        else {
            panic!("expected capability diagnostic");
        };
        assert!(message.contains("child_cancellation_policy_not_supported"));
        assert!(message.contains(&worker.worker_id));
        assert!(message.contains("1.20"));
    }
    for parent in [
        ParentClosePolicy::Abandon,
        ParentClosePolicy::RequestCancel,
        ParentClosePolicy::Terminate,
    ] {
        let worker = policy_worker(parent, CancellationPolicy::Abandon, "sequential", false);
        assert!(worker.execute_workflow_task(policy_task(vec![])).is_ok());
    }
}
