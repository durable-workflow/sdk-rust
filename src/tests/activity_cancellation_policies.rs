use super::*;

fn activity_policy_worker(
    policy: Option<CancellationPolicy>,
    mode: &'static str,
    enabled: bool,
) -> Worker {
    let mut worker = Worker::new(
        Client::builder("http://127.0.0.1:1").build().unwrap(),
        "queue",
    )
    .cooperative_cancellation(enabled);
    worker.register_workflow("activity-policy", move |ctx, _| async move {
        let mut options = ActivityOptions::new();
        if let Some(policy) = policy {
            options = options
                .cancellation_policy(policy)
                .schedule_to_close_timeout(Duration::from_secs(60));
        }
        match mode {
            "parallel" => {
                ctx.parallel(vec![ParallelOperation::activity_with_options(
                    "work",
                    options,
                    json!([]),
                )])
                .await?;
            }
            "selection" => {
                ctx.select_keyed(vec![(
                    SelectionKey::from("work"),
                    ParallelOperation::activity_with_options("work", options, json!([])),
                )])
                .await?;
            }
            _ => {
                ctx.activity_with_options("work", options, json!([]))
                    .await?;
            }
        }
        Ok(json!("finished"))
    });
    worker
}

fn activity_policy_event(kind: &str, payload: Value) -> HistoryEvent {
    serde_json::from_value(json!({"event_type": kind, "payload": payload})).unwrap()
}

fn activity_policy_history(command: &Value) -> Vec<HistoryEvent> {
    let mut payload = command.clone();
    let object = payload.as_object_mut().unwrap();
    object.remove("type");
    object.remove("arguments");
    object.insert("sequence".into(), json!(1));
    let mut activity = json!({"type": "work"});
    if let Some(policy) = command.get("cancellation_policy") {
        activity["cancellation_policy"] = policy.clone();
    }
    object.insert("activity".into(), activity);
    vec![activity_policy_event("ActivityScheduled", payload)]
}

fn assert_activity_policy_failure(error: Error, reason: &str) {
    let Error::NonDeterministicReplay(failure) = error else {
        panic!("expected replay failure, got {error:?}");
    };
    assert_eq!(failure.reason, reason);
    assert_eq!(failure.sequence, Some(1));
}

#[test]
fn changed_activity_cancellation_policy_is_rejected_during_replay() {
    let mut worker = Worker::new(
        Client::builder("http://127.0.0.1:1").build().unwrap(),
        "queue",
    )
    .cooperative_cancellation(true);
    worker.register_workflow("activity-policy", |ctx, _| async move {
        ctx.activity("work", json!([])).await?;
        Ok(json!("finished"))
    });
    let event = serde_json::from_value(json!({
        "event_type": "ActivityScheduled",
        "payload": {"sequence": 1, "activity_type": "work", "activity": {
            "type": "work", "cancellation_policy": "wait_cancellation_completed",
        }},
    }))
    .unwrap();
    let error = worker
        .execute_workflow_task(workflow_task("activity-policy", vec![event], DEFAULT_CODEC))
        .expect_err("changing the original Activity policy must fail replay");
    let Error::NonDeterministicReplay(failure) = error else {
        panic!("expected replay failure, got {error:?}");
    };
    assert_eq!(failure.reason, "activity_cancellation_policy_changed");
    assert_eq!(failure.sequence, Some(1));
}

#[test]
fn activity_policies_encode_and_replay_original_history_through_completion() {
    for policy in [
        None,
        Some(CancellationPolicy::TryCancel),
        Some(CancellationPolicy::WaitCancellationCompleted),
        Some(CancellationPolicy::Abandon),
    ] {
        let worker = activity_policy_worker(policy, "sequential", true);
        let commands = worker
            .execute_workflow_task(workflow_task("activity-policy", vec![], DEFAULT_CODEC))
            .unwrap();
        assert_eq!(
            commands[0]
                .get("cancellation_policy")
                .and_then(Value::as_str),
            policy.map(CancellationPolicy::as_str)
        );
        let mut history = activity_policy_history(&commands[0]);
        history.push(activity_policy_event(
            "ActivityStarted",
            json!({"sequence":1, "activity_type":"work"}),
        ));
        history.push(activity_policy_event(
            "ActivityCompleted",
            json!({"sequence":1, "activity_type":"work",
            "result": fixture_envelope(json!("recorded"))}),
        ));
        let completed = worker
            .execute_workflow_task(workflow_task(
                "activity-policy",
                history.clone(),
                DEFAULT_CODEC,
            ))
            .unwrap();
        assert_eq!(
            decode_wire_value(&completed[0]["result"], DEFAULT_CODEC).unwrap(),
            json!("finished")
        );
        let changed = activity_policy_worker(
            Some(CancellationPolicy::WaitCancellationCompleted),
            "sequential",
            true,
        );
        if policy != Some(CancellationPolicy::WaitCancellationCompleted) {
            assert_activity_policy_failure(
                changed
                    .execute_workflow_task(workflow_task("activity-policy", history, DEFAULT_CODEC))
                    .unwrap_err(),
                "activity_cancellation_policy_changed",
            );
        }
    }
}

#[test]
fn activity_policies_match_ordinary_groups_and_cancellation_delivery() {
    for mode in ["sequential", "parallel", "selection"] {
        let original = activity_policy_worker(
            Some(CancellationPolicy::WaitCancellationCompleted),
            mode,
            true,
        );
        let commands = original
            .execute_workflow_task(workflow_task("activity-policy", vec![], DEFAULT_CODEC))
            .unwrap();
        for delivered in [false, true] {
            let mut history = activity_policy_history(&commands[0]);
            if delivered {
                history.push(serde_json::from_value(json!({
                    "event_type":"CooperativeCancellationRequested", "recorded_at":"2026-10-01T00:00:00Z",
                    "payload":{"workflow_run_id":"run", "workflow_command_id":"request-1", "cleanup_deadline_at":"2026-10-01T00:00:30Z"},
                })).unwrap());
                history.push(activity_policy_event("CooperativeCancellationDelivered", json!({
                    "workflow_run_id":"run", "workflow_command_id":"request-1", "sequence":1,
                    "call_kind":if mode == "sequential" {"activity"} else {"parallel"}, "sequence_span":1,
                })));
            }
            let mut task = workflow_task("activity-policy", history, DEFAULT_CODEC);
            task.run_id = Some("run".into());
            original.execute_workflow_task(task.clone()).unwrap();
            let changed = activity_policy_worker(Some(CancellationPolicy::TryCancel), mode, true);
            assert_activity_policy_failure(
                changed.execute_workflow_task(task).unwrap_err(),
                "activity_cancellation_policy_changed",
            );
        }
    }
}

#[test]
fn malformed_and_conflicting_activity_policy_history_is_rejected() {
    let worker = activity_policy_worker(None, "sequential", true);
    for policy in [Value::Null, json!(false), json!([]), json!("unknown")] {
        let history = vec![activity_policy_event(
            "ActivityScheduled",
            json!({"sequence":1, "activity_type":"work",
            "activity":{"cancellation_policy":policy}}),
        )];
        assert_activity_policy_failure(
            worker
                .execute_workflow_task(workflow_task("activity-policy", history, DEFAULT_CODEC))
                .unwrap_err(),
            "invalid_activity_cancellation_policy_history",
        );
    }
    let commands = worker
        .execute_workflow_task(workflow_task("activity-policy", vec![], DEFAULT_CODEC))
        .unwrap();
    let mut history = activity_policy_history(&commands[0]);
    history.push(activity_policy_event(
        "ActivityStarted",
        json!({"sequence":1, "activity_type":"work",
        "activity":{"cancellation_policy":"abandon"}}),
    ));
    assert_activity_policy_failure(
        worker
            .execute_workflow_task(workflow_task("activity-policy", history, DEFAULT_CODEC))
            .unwrap_err(),
        "activity_cancellation_policy_history_conflict",
    );
}

#[test]
fn explicit_activity_policies_require_worker_opt_in_and_abandon_requires_total_lifetime() {
    for policy in [
        CancellationPolicy::TryCancel,
        CancellationPolicy::WaitCancellationCompleted,
        CancellationPolicy::Abandon,
    ] {
        let worker = activity_policy_worker(Some(policy), "sequential", false);
        let error = worker
            .execute_workflow_task(workflow_task("activity-policy", vec![], DEFAULT_CODEC))
            .unwrap_err();
        let Error::CooperativeCancellationUnavailable(message) = error else {
            panic!("unexpected error {error:?}");
        };
        assert!(message.contains("activity_cancellation_policy_not_supported"));
        assert!(message.contains("1.20"));
    }
    let absent = ActivityOptions::new()
        .cancellation_policy(CancellationPolicy::Abandon)
        .validate()
        .unwrap_err();
    assert_eq!(absent.kind, ActivityOptionsErrorKind::MissingTotalTimeout);
    let zero = ActivityOptions::new()
        .cancellation_policy(CancellationPolicy::Abandon)
        .schedule_to_close_timeout(Duration::ZERO)
        .validate()
        .unwrap_err();
    assert_eq!(zero.kind, ActivityOptionsErrorKind::TimeoutNotPositive);
}
