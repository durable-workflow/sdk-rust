use super::*;

mod descendants;
mod run_scopes;

fn group_fixture(name: &str) -> Value {
    let source: Value = serde_json::from_str(include_str!(
        "../../tests/fixtures/populated-scope-groups.json"
    ))
    .unwrap();
    source[name].clone()
}

fn group_worker(cleanup: Arc<AtomicUsize>, nested: bool, changed: &'static str) -> Worker {
    let mut worker = Worker::new(Client::new("http://unused.invalid").unwrap(), "queue")
        .cooperative_cancellation(true)
        .candidate_cancellation_scope_authoring(true)
        .candidate_cancellation_scope_delivery(true);
    worker.register_workflow("scope-replay", move |ctx, _| {
        let cleanup = Arc::clone(&cleanup);
        async move {
            let parent = ctx.clone();
            let result = ctx
                .cancellation_scope(false, move |outer| async move {
                    outer
                        .cancellation_scope(false, move |inner| async move {
                            if changed != "prefix" {
                                inner.activity("prior-step", json!([])).await?;
                            }
                            let activity = ParallelOperation::activity_with_options(
                                if changed == "activity" {
                                    "changed-activity"
                                } else {
                                    "original-activity"
                                },
                                ActivityOptions::new().cancellation_policy(if changed == "activity_policy" {
                                    CancellationPolicy::WaitCancellationCompleted
                                } else { CancellationPolicy::TryCancel }), json!([]),
                            );
                            let timer = ParallelOperation::timer(Duration::from_secs(
                                if changed == "timer" { 3599 } else { 3600 },
                            ));
                            let child = ParallelOperation::child_workflow(
                                if changed == "child" { "changed-child" } else { "original-child" },
                                ChildWorkflowOptions::new("queue")
                                    .parent_close_policy(if changed == "parent_policy" {
                                        ParentClosePolicy::Abandon
                                    } else { ParentClosePolicy::RequestCancel })
                                    .cancellation_policy(if changed == "child_policy" {
                                        CancellationPolicy::Abandon
                                    } else {
                                        CancellationPolicy::WaitCancellationCompleted
                                    }),
                                json!([]),
                            );
                            let condition = ParallelOperation::condition(ConditionWaitOptions::new(
                        if changed == "condition" { "changed-key" } else { "ready" },
                        if changed == "predicate" { "sha256:changed" } else { "sha256:3b2b1ad635030d842ab378e8bf021f4613ee6d221dafb4743565467800def433" })
                        .timeout(Duration::from_secs(if changed == "condition_timeout" { 31 } else { 30 })), || panic!("committed cancellation must not evaluate the pending predicate"));
                            let mut group = if nested {
                                vec![
                                    activity,
                                    ParallelOperation::group(vec![timer, child]),
                                    condition,
                                ]
                            } else {
                                vec![activity, timer, child, condition]
                            };
                            if changed == "size" { group.pop(); }
                            if changed == "kind" { group[0] = ParallelOperation::timer(Duration::from_secs(3600)); }
                            let _shield = if changed == "shield" {
                                Some(inner.cancellation_shield()?)
                            } else {
                                None
                            };
                            match inner.parallel(group).await {
                                Err(Error::CancellationScopeRequested(cancellation)) => {
                                    cleanup.fetch_add(1, Ordering::SeqCst);
                                    assert_eq!(
                                        cancellation.delivery.call_kind,
                                        CancellationCallKind::Parallel
                                    );
                                    assert_eq!(cancellation.delivery.sequence_span, 4);
                                    assert_eq!(
                                        cancellation.context.remaining()?,
                                Duration::from_secs(21)
                                    );
                                    let _shield = inner.cancellation_shield()?;
                                    inner.sleep(Duration::from_secs(2)).await?;
                                    assert_eq!(cancellation.context.remaining()?, Duration::from_secs(19));
                                    Ok(json!({"cleaned":true}))
                                }
                                Err(error) => Err(error),
                                Ok(_) => Ok(json!("ordinary")),
                            }
                        })
                        .await
                })
                .await?;
            assert!(!parent.is_cancellation_requested()?);
            parent.sleep(Duration::from_secs(1)).await?;
            Ok(result)
        }
    });
    worker
}

#[test]
fn cancellation_scope_replay_flat_and_nested_groups_wait_for_committed_delivery() {
    for (name, nested) in [("flat", false), ("nested", true)] {
        let source = group_fixture(name);
        let cleanup = Arc::new(AtomicUsize::new(0));
        let worker = group_worker(Arc::clone(&cleanup), nested, "");
        for before in [
            "CancellationScopeDeliveryPrepared",
            "CancellationScopeDelivered",
        ] {
            let decision = worker
                .execute_workflow_task_decision(task(&prefix(&source, before)))
                .unwrap();
            assert!(decision.commands.is_empty());
            let intent = decision.cancellation_scope_delivery.unwrap();
            assert_eq!(intent.boundary.call_kind, CancellationCallKind::Parallel);
            assert_eq!(intent.boundary.sequence, 4);
            assert_eq!(intent.boundary.sequence_span, 4);
            assert_eq!(cleanup.load(Ordering::SeqCst), 0);
        }
        let decision = worker
            .execute_workflow_task_decision(task(&source))
            .unwrap();
        assert!(decision.cancellation_scope_delivery.is_none());
        assert_eq!(cleanup.load(Ordering::SeqCst), 1);
        assert_eq!(decision.commands.len(), 1);
        assert_eq!(decision.commands[0]["type"], "start_timer");
        assert_eq!(decision.commands[0]["delay_seconds"], 2);
        assert_eq!(
            decision.commands[0]["cancellation_cleanup"]["request_id"],
            cleanup_snapshot(&source)["request_id"]
        );
    }
}

#[test]
fn cancellation_scope_replay_group_rejects_changed_members_before_cleanup() {
    for changed in [
        "activity",
        "activity_policy",
        "timer",
        "child",
        "child_policy",
        "parent_policy",
        "condition",
        "condition_timeout",
        "predicate",
        "shield",
        "shape",
        "prefix",
        "size",
        "kind",
    ] {
        let cleanup = Arc::new(AtomicUsize::new(0));
        let worker = group_worker(Arc::clone(&cleanup), changed == "shape", changed);
        assert!(
            worker
                .execute_workflow_task_decision(task(&group_fixture("flat")))
                .is_err(),
            "{changed}"
        );
        assert_eq!(cleanup.load(Ordering::SeqCst), 0, "{changed}");
    }
}

#[test]
fn cancellation_scope_replay_unqualified_groups_stop_before_factory() {
    for name in [
        "flat-local",
        "flat-other-scope",
        "flat-selection",
        "flat-signal",
        "flat-incomplete",
    ] {
        let invoked = Arc::new(AtomicUsize::new(0));
        let mut worker = Worker::new(Client::new("http://unused.invalid").unwrap(), "queue")
            .cooperative_cancellation(true)
            .candidate_cancellation_scope_authoring(true)
            .candidate_cancellation_scope_delivery(true);
        let calls = Arc::clone(&invoked);
        worker.register_workflow("scope-replay", move |_, _| {
            calls.fetch_add(1, Ordering::SeqCst);
            async { Ok(json!(null)) }
        });
        assert!(
            worker
                .execute_workflow_task_decision(task(&group_fixture(name)))
                .is_err(),
            "{name}"
        );
        assert_eq!(invoked.load(Ordering::SeqCst), 0, "{name}");
    }
}

#[test]
fn cancellation_scope_replay_group_replacement_preserves_cleanup_sequence_and_authority() {
    for (name, nested) in [("flat", false), ("nested", true)] {
        let mut source = group_fixture(name);
        let snapshot = cleanup_snapshot(&source);
        let scope = snapshot["scope_id"].clone();
        append(
            &mut source,
            "TimerScheduled",
            json!({"sequence":8, "timer_id":"group-cleanup",
            "delay_seconds":2, "cancellation_scope_id":scope, "cancellation_cleanup":snapshot,
            "fire_at":"2026-10-04T00:00:11.123456Z"}),
            "2026-10-04T00:00:09.123456Z",
        );
        append(
            &mut source,
            "TimerFired",
            json!({"sequence":8, "timer_id":"group-cleanup",
            "delay_seconds":2, "cancellation_scope_id":scope}),
            "2026-10-04T00:00:11.123456Z",
        );
        let worker =
            group_worker(Arc::new(AtomicUsize::new(0)), nested, "").worker_id("replacement");
        let mut claim = task(&source);
        claim.lease_owner = Some("replacement".into());
        claim.workflow_task_attempt = 19;
        let decision = worker.execute_workflow_task_decision(claim).unwrap();
        assert_eq!(decision.commands.len(), 1);
        assert_eq!(decision.commands[0]["delay_seconds"], 1);
        assert!(decision.commands[0].get("cancellation_scope_id").is_none());
        assert!(decision.commands[0].get("cancellation_cleanup").is_none());
        append(
            &mut source,
            "TimerScheduled",
            json!({"sequence":9, "timer_id":"parent-timer",
            "delay_seconds":1}),
            "2026-10-04T00:00:11.123456Z",
        );
        append(
            &mut source,
            "TimerFired",
            json!({"sequence":9, "timer_id":"parent-timer",
            "delay_seconds":1}),
            "2026-10-04T00:00:12.123456Z",
        );
        let decision = worker
            .execute_workflow_task_decision(task(&source))
            .unwrap();
        assert_eq!(decision.commands.len(), 1);
        assert_eq!(decision.commands[0]["type"], "complete_workflow");
    }
}

#[test]
fn cancellation_scope_replay_completed_member_does_not_hide_pending_siblings() {
    for (name, nested) in [("flat", false), ("nested", true)] {
        for prepared in [false, true] {
            let source = group_fixture(name);
            let mut value = prefix(
                &source,
                if prepared {
                    "CancellationScopeDelivered"
                } else {
                    "CancellationScopeDeliveryPrepared"
                },
            );
            let events = value["history"].as_array_mut().unwrap();
            let request = events
                .iter()
                .position(|row| row["event_type"] == "CancellationScopeRequested")
                .unwrap();
            let namespace = events[0]["namespace"].clone();
            events.insert(
                request,
                json!({"id":"completed-first-member", "sequence":1,
                "namespace":namespace, "timestamp":"2026-10-04T00:00:02.123456Z",
                "event_type":"ActivityCompleted", "payload":{"sequence":4,
                "activity_execution_id":"original-id", "activity_type":"original-activity",
                "result":{"codec":"avro", "blob":"wwHioz3/VYAiNwoWcHJpb3ItdmFsdWU="}}}),
            );
            for (index, row) in events.iter_mut().enumerate() {
                row["sequence"] = json!(index + 1);
            }
            let cleanup = Arc::new(AtomicUsize::new(0));
            let decision = group_worker(Arc::clone(&cleanup), nested, "")
                .execute_workflow_task_decision(task(&value))
                .unwrap();
            assert!(decision.commands.is_empty());
            assert_eq!(
                decision
                    .cancellation_scope_delivery
                    .unwrap()
                    .boundary
                    .sequence_span,
                4
            );
            assert_eq!(cleanup.load(Ordering::SeqCst), 0);
        }
    }
}

#[test]
fn cancellation_scope_replay_completed_group_preserves_results_and_prepares_next_call() {
    let mut source = group_fixture("flat");
    let request = source["history"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["event_type"] == "CancellationScopeRequested")
        .unwrap()["payload"]
        .clone();
    let scope = request["scope_id"].clone();
    source["history"].as_array_mut().unwrap().retain(|row| {
        !row["event_type"]
            .as_str()
            .unwrap()
            .starts_with("CancellationScopeRequest")
            && !row["event_type"]
                .as_str()
                .unwrap()
                .starts_with("CancellationScopeDeliver")
            && row["payload"]["sequence"]
                .as_u64()
                .is_none_or(|sequence| sequence < 4)
    });
    let descriptors = parallel_descriptors(
        vec![
            ParallelOperation::timer(Duration::from_secs(1)),
            ParallelOperation::timer(Duration::from_secs(1)),
        ],
        4,
    )
    .unwrap();
    for descriptor in descriptors {
        let sequence = 4 + descriptor.offset as u64;
        let delay = 1;
        let mut payload = serde_json::Map::from_iter([
            ("sequence".into(), json!(sequence)),
            ("timer_id".into(), json!(format!("timer-{sequence}"))),
            ("delay_seconds".into(), json!(delay)),
            ("cancellation_scope_id".into(), scope.clone()),
        ]);
        apply_parallel_group_path(&mut payload, &descriptor.group_path);
        append(
            &mut source,
            "TimerScheduled",
            Value::Object(payload.clone()),
            "2026-10-04T00:00:01.123456Z",
        );
        append(
            &mut source,
            "TimerFired",
            Value::Object(payload),
            "2026-10-04T00:00:02.123456Z",
        );
    }
    append(
        &mut source,
        "CancellationScopeRequested",
        request,
        "2026-10-04T00:00:03.123456Z",
    );
    let mut worker = Worker::new(Client::new("http://unused.invalid").unwrap(), "queue")
        .cooperative_cancellation(true)
        .candidate_cancellation_scope_authoring(true)
        .candidate_cancellation_scope_delivery(true);
    worker.register_workflow("scope-replay", |ctx, _| async move {
        ctx.cancellation_scope(false, |outer| async move {
            outer
                .cancellation_scope(false, |inner| async move {
                    inner.activity("prior-step", json!([])).await?;
                    let results = inner
                        .parallel(vec![
                            ParallelOperation::timer(Duration::from_secs(1)),
                            ParallelOperation::timer(Duration::from_secs(1)),
                        ])
                        .await?;
                    assert_eq!(results.len(), 2);
                    inner.sleep(Duration::from_secs(1)).await?;
                    Ok(json!(null))
                })
                .await
        })
        .await
    });
    let decision = worker
        .execute_workflow_task_decision(task(&source))
        .unwrap();
    assert!(decision.commands.is_empty());
    let intent = decision.cancellation_scope_delivery.unwrap();
    assert_eq!(intent.boundary.sequence, 6);
    assert_eq!(intent.boundary.call_kind, CancellationCallKind::Timer);
}

fn fixture() -> Value {
    let source: Value = serde_json::from_str(include_str!(
        "../../tests/fixtures/populated-scope-single-calls.json"
    ))
    .unwrap();
    source["timer"].clone()
}

fn task(value: &Value) -> WorkflowTask {
    serde_json::from_value(json!({"task_id":"task-one", "workflow_id":value["task"]["workflow_id"],
        "run_id":value["task"]["run_id"], "workflow_type":"scope-replay", "payload_codec":DEFAULT_CODEC,
        "lease_owner":"original", "workflow_task_attempt":1, "history_events":value["history"]})).unwrap()
}

fn worker(cleanup: Arc<AtomicUsize>, changed_delay: bool) -> Worker {
    let mut worker = Worker::new(Client::new("http://unused.invalid").unwrap(), "queue")
        .cooperative_cancellation(true)
        .candidate_cancellation_scope_authoring(true)
        .candidate_cancellation_scope_delivery(true);
    worker.register_workflow("scope-replay", move |ctx, _| {
            let cleanup = Arc::clone(&cleanup);
            async move {
                let parent = ctx.clone();
                let result = ctx.cancellation_scope(false, move |outer| async move {
                    outer.cancellation_scope(false, move |inner| async move {
                        assert_eq!(inner.activity("prior-step", json!([])).await?, json!("prior-value"));
                        match inner.sleep(Duration::from_secs(if changed_delay { 3599 } else { 3600 })).await {
                            Ok(()) => Ok(json!("ordinary-result")),
                            Err(Error::CancellationScopeRequested(cancellation)) => {
                                cleanup.fetch_add(1, Ordering::SeqCst);
                                assert!(inner.is_cancellation_requested()?);
                                assert_eq!(inner.scoped_cancellation_context()?.unwrap(), cancellation.context);
                                let before = cancellation.context.remaining()?;
                                    assert_eq!(before, Duration::from_secs(21));
                                let _shield = inner.cancellation_shield()?;
                                inner.sleep(Duration::from_secs(2)).await?;
                                let after = cancellation.context.remaining()?;
                                    assert_eq!(after, Duration::from_secs(19));
                                Ok(json!({"cleaned":true, "request_id":cancellation.context.request_id()}))
                            }
                            Err(error) => Err(error),
                        }
                    }).await
                }).await?;
                assert!(!parent.is_cancellation_requested()?);
                assert!(parent.scoped_cancellation_context()?.is_none());
                parent.sleep(Duration::from_secs(1)).await?;
                Ok(result)
            }
        });
    worker
}

fn prefix(value: &Value, before: &str) -> Value {
    let mut value = value.clone();
    let events = value["history"].as_array_mut().unwrap();
    let index = events
        .iter()
        .position(|event| event["event_type"] == before)
        .unwrap();
    events.truncate(index);
    value
}

fn append(value: &mut Value, kind: &str, payload: Value, timestamp: &str) {
    let events = value["history"].as_array_mut().unwrap();
    let sequence = events.last().unwrap()["sequence"].as_u64().unwrap() + 1;
    let namespace = events[0]["namespace"].clone();
    events.push(
        json!({"id":format!("cleanup-event-{sequence}"), "sequence":sequence,
        "namespace":namespace, "event_type":kind, "payload":payload, "timestamp":timestamp}),
    );
}

fn cleanup_snapshot(value: &Value) -> Value {
    let delivery = value["history"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["event_type"] == "CancellationScopeDelivered")
        .unwrap();
    let context =
        ScopedCancellationContext::from_value(&delivery["payload"]["cancellation"]).unwrap();
    json!({"scope_id":context.scope_id(), "operation_scope_id":context.scope_id(),
        "request_id":context.request_id(), "root_request_id":context.root_context().root_request_id(),
        "delivery_history_event_id":delivery["id"],
        "preparation_history_event_id":delivery["payload"]["preparation_history_event_id"],
        "cleanup_deadline_at":context.deadline().to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
        "authority_deadline_at":delivery["payload"]["authority_deadline_at"]})
}

#[test]
fn cancellation_scope_replay_waits_for_original_prepare_and_committed_delivery() {
    let original = fixture();
    let cleanup = Arc::new(AtomicUsize::new(0));
    let worker = worker(Arc::clone(&cleanup), false);
    for before in [
        "CancellationScopeDeliveryPrepared",
        "CancellationScopeDelivered",
    ] {
        let decision = worker
            .execute_workflow_task_decision(task(&prefix(&original, before)))
            .unwrap();
        let intent = decision.cancellation_scope_delivery.unwrap();
        assert_eq!(intent.boundary.sequence, 4);
        assert_eq!(intent.boundary.call_kind, CancellationCallKind::Timer);
        assert!(decision.commands.is_empty());
        assert_eq!(cleanup.load(Ordering::SeqCst), 0);
    }
    let decision = worker
        .execute_workflow_task_decision(task(&original))
        .unwrap();
    assert!(decision.cancellation_scope_delivery.is_none());
    assert_eq!(decision.commands.len(), 1);
    assert_eq!(decision.commands[0]["type"], "start_timer");
    assert_eq!(decision.commands[0]["delay_seconds"], 2);
    assert!(decision.commands[0]["cancellation_scope_id"].is_string());
    let snapshot = cleanup_snapshot(&original);
    assert_eq!(
        decision.commands[0]["cancellation_cleanup"],
        json!({
        "scope_id":snapshot["scope_id"], "request_id":snapshot["request_id"],
        "delivery_history_event_id":snapshot["delivery_history_event_id"]})
    );
    assert_eq!(cleanup.load(Ordering::SeqCst), 1);
}

#[test]
fn cancellation_scope_replay_replacement_keeps_context_clock_and_parent_unaffected() {
    let mut source = fixture();
    let snapshot = cleanup_snapshot(&source);
    let scope = source["history"]
        .as_array()
        .unwrap()
        .iter()
        .find(|event| event["event_type"] == "CancellationScopeDelivered")
        .unwrap()["payload"]["scope_id"]
        .clone();
    append(
        &mut source,
        "TimerScheduled",
        json!({"sequence":5, "timer_id":"cleanup-timer",
        "delay_seconds":2, "cancellation_scope_id":scope,
        "fire_at":"2026-10-04T00:00:11.123456Z", "cancellation_cleanup":snapshot}),
        "2026-10-04T00:00:09.123456Z",
    );
    append(
        &mut source,
        "TimerFired",
        json!({"sequence":5, "timer_id":"cleanup-timer", "delay_seconds":2,
        "cancellation_scope_id":scope}),
        "2026-10-04T00:00:11.123456Z",
    );
    let replacement = worker(Arc::new(AtomicUsize::new(0)), false).worker_id("replacement");
    let mut claim = task(&source);
    claim.lease_owner = Some("replacement".into());
    claim.workflow_task_attempt = 2;
    let decision = replacement.execute_workflow_task_decision(claim).unwrap();
    assert_eq!(decision.commands.len(), 1);
    assert_eq!(decision.commands[0]["delay_seconds"], 1);
    assert!(decision.commands[0].get("cancellation_scope_id").is_none());
    append(
        &mut source,
        "TimerScheduled",
        json!({"sequence":6, "timer_id":"parent-timer", "delay_seconds":1}),
        "2026-10-04T00:00:11.123456Z",
    );
    append(
        &mut source,
        "TimerFired",
        json!({"sequence":6, "timer_id":"parent-timer", "delay_seconds":1}),
        "2026-10-04T00:00:12.123456Z",
    );
    let decision = replacement
        .execute_workflow_task_decision(task(&source))
        .unwrap();
    assert_eq!(decision.commands.len(), 1);
    assert_eq!(decision.commands[0]["type"], "complete_workflow");
}

#[test]
fn cancellation_scope_replay_cleanup_timer_refuses_changed_authority_before_factory() {
    for field in [
        "request_id",
        "delivery_history_event_id",
        "authority_deadline_at",
        "cleanup_deadline_at",
        "operation_scope_id",
    ] {
        let mut value = fixture();
        let mut snapshot = cleanup_snapshot(&value);
        let scope = snapshot["scope_id"].clone();
        snapshot[field] = json!("changed");
        append(
            &mut value,
            "TimerScheduled",
            json!({"sequence":5, "timer_id":"cleanup-timer",
            "delay_seconds":2, "cancellation_scope_id":scope,
            "fire_at":"2026-10-04T00:00:11.123456Z", "cancellation_cleanup":snapshot}),
            "2026-10-04T00:00:09.123456Z",
        );
        let factories = Arc::new(AtomicUsize::new(0));
        let called = Arc::clone(&factories);
        let mut worker = worker(Arc::new(AtomicUsize::new(0)), false);
        worker.register_workflow("scope-replay", move |_, _| {
            called.fetch_add(1, Ordering::SeqCst);
            async { Ok(Value::Null) }
        });
        assert!(worker.execute_workflow_task_decision(task(&value)).is_err());
        assert_eq!(factories.load(Ordering::SeqCst), 0);
    }
}

#[test]
fn cancellation_scope_replay_descriptor_change_cannot_enter_cleanup() {
    let cleanup = Arc::new(AtomicUsize::new(0));
    let result =
        worker(Arc::clone(&cleanup), true).execute_workflow_task_decision(task(&fixture()));
    assert!(
        matches!(result, Err(Error::NonDeterministicReplay(failure)) if failure.reason == "timer_delay_mismatch")
    );
    assert_eq!(cleanup.load(Ordering::SeqCst), 0);
}

#[test]
fn cancellation_scope_replay_default_and_incomplete_opt_in_refuse_before_factory() {
    for (authoring, delivery) in [(false, false), (true, false), (false, true)] {
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&calls);
        let mut worker = Worker::new(Client::new("http://unused.invalid").unwrap(), "queue")
            .cooperative_cancellation(true)
            .candidate_cancellation_scope_authoring(authoring)
            .candidate_cancellation_scope_delivery(delivery);
        worker.register_workflow("scope-replay", move |_, _| {
            observed.fetch_add(1, Ordering::SeqCst);
            async { Ok(json!("must-not-enter")) }
        });
        assert!(matches!(
            worker.execute_workflow_task_decision(task(&fixture())),
            Err(Error::CancellationScopeExecutionUnavailable)
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
}

#[test]
fn cancellation_scope_replay_bad_projection_refuses_before_factory() {
    let mut source = fixture();
    for event in source["history"].as_array_mut().unwrap() {
        if event["event_type"] == "CancellationScopeDeliveryPrepared" {
            event["payload"]["timer_members"][0]["descriptor_hash"] = json!("0".repeat(64));
        }
    }
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    let mut worker = Worker::new(Client::new("http://unused.invalid").unwrap(), "queue")
        .cooperative_cancellation(true)
        .candidate_cancellation_scope_authoring(true)
        .candidate_cancellation_scope_delivery(true);
    worker.register_workflow("scope-replay", move |_, _| {
        observed.fetch_add(1, Ordering::SeqCst);
        async { Ok(json!("must-not-enter")) }
    });
    assert!(worker
        .execute_workflow_task_decision(task(&source))
        .is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}
