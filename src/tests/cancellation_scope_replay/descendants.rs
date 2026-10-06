use super::*;

type Observed = Arc<Mutex<BTreeMap<String, ScopedCancellationContext>>>;

fn descendant_fixture(layout: &str) -> Value {
    let fixtures: Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/committed-scope-descendants.json"
    ))
    .unwrap();
    fixtures[layout].clone()
}

async fn capture(
    ctx: &WorkflowContext,
    result: Result<()>,
    name: &str,
    source: &Value,
    seen: &Observed,
    cleanup: &str,
) -> Result<Value> {
    let context = match result {
        Err(Error::CancellationScopeRequested(request)) => {
            assert_eq!(
                request.delivery.request_id,
                source["contexts"]["parent"]["lineage"]
                    .as_array()
                    .unwrap()
                    .last()
                    .unwrap()["request_id"]
            );
            request.context
        }
        Err(error) => return Err(error),
        Ok(()) => panic!("{name} must retain its original accepted request"),
    };
    assert!(ctx.is_cancellation_requested()?);
    assert_eq!(ctx.scoped_cancellation_context()?.unwrap(), context);
    assert_eq!(context.to_value(), source["contexts"][name]);
    let rank = |scope| match scope {
        "grandchild" => 0,
        "child" => 1,
        _ => 2,
    };
    let fired = source["history"].as_array().unwrap().iter().any(|row| {
        row["event_type"] == "TimerFired" && row["payload"]["timer_id"] == "descendant-cleanup"
    });
    assert_eq!(
        context.remaining()?,
        Duration::from_secs(
            if fired && !cleanup.is_empty() && rank(name) > rank(cleanup) {
                15
            } else {
                17
            }
        )
    );
    seen.lock().unwrap().insert(name.into(), context);
    if cleanup == name {
        let _shield = ctx.cancellation_shield()?;
        ctx.sleep(Duration::from_secs(1)).await?;
    }
    Ok(Value::Null)
}

async fn grandchild_body(
    ctx: WorkflowContext,
    source: Value,
    seen: Observed,
    cleanup: &'static str,
    changed: &'static str,
) -> Result<Value> {
    if changed != "prefix" {
        assert_eq!(
            ctx.activity("prior-step", json!([])).await?,
            json!("prior-value")
        );
    }
    let interrupted = if source["layout"] == "timer" {
        ctx.sleep(Duration::from_secs(if changed == "timer" {
            42
        } else {
            3600
        }))
        .await
    } else {
        let predicate = source["history"]
            .as_array()
            .unwrap()
            .iter()
            .find(|row| row["event_type"] == "ConditionWaitOpened")
            .unwrap()["payload"]["condition_definition_fingerprint"]
            .as_str()
            .unwrap();
        let activity = ParallelOperation::activity_with_options(
            if changed == "activity" {
                "changed"
            } else {
                "original-activity"
            },
            ActivityOptions::new().cancellation_policy(if changed == "activity-policy" {
                CancellationPolicy::Abandon
            } else {
                CancellationPolicy::TryCancel
            }),
            json!([]),
        );
        let timer = ParallelOperation::timer(Duration::from_secs(if changed == "timer" {
            42
        } else {
            3600
        }));
        let child = ParallelOperation::child_workflow(
            if changed == "child" {
                "changed"
            } else {
                "original-child"
            },
            ChildWorkflowOptions::new("queue")
                .parent_close_policy(ParentClosePolicy::RequestCancel)
                .cancellation_policy(if changed == "child-policy" {
                    CancellationPolicy::Abandon
                } else {
                    CancellationPolicy::WaitCancellationCompleted
                }),
            json!([]),
        );
        let condition = ParallelOperation::condition(
            ConditionWaitOptions::new("ready", predicate).timeout(Duration::from_secs(30)),
            || panic!("committed cancellation must not evaluate the interrupted predicate"),
        );
        let mut group = if changed == "layout" {
            vec![activity, timer, child, condition]
        } else {
            vec![
                activity,
                ParallelOperation::group(vec![timer, child]),
                condition,
            ]
        };
        if changed == "size" {
            group.pop();
        }
        ctx.parallel(group).await.map(|_| ())
    };
    capture(&ctx, interrupted, "grandchild", &source, &seen, cleanup).await
}

async fn child_body(
    ctx: WorkflowContext,
    source: Value,
    seen: Observed,
    cleanup: &'static str,
    changed: &'static str,
) -> Result<Value> {
    let value = source.clone();
    let observed = Arc::clone(&seen);
    ctx.cancellation_scope(changed == "shield", move |inner| {
        grandchild_body(inner, value, observed, cleanup, changed)
    })
    .await?;
    capture(
        &ctx,
        ctx.throw_if_cancellation_requested(),
        "child",
        &source,
        &seen,
        cleanup,
    )
    .await
}

async fn parent_body(
    ctx: WorkflowContext,
    source: Value,
    seen: Observed,
    cleanup: &'static str,
    changed: &'static str,
) -> Result<Value> {
    ctx.cancellation_scope(true, move |shield| async move {
        assert!(!shield.is_cancellation_requested()?);
        assert!(shield.scoped_cancellation_context()?.is_none());
        shield
            .cancellation_scope(false, move |inner| async move {
                assert!(!inner.is_cancellation_requested()?);
                assert!(inner.scoped_cancellation_context()?.is_none());
                Ok(Value::Null)
            })
            .await
    })
    .await?;
    let value = source.clone();
    let observed = Arc::clone(&seen);
    ctx.cancellation_scope(false, move |inner| {
        child_body(inner, value, observed, cleanup, changed)
    })
    .await?;
    capture(
        &ctx,
        ctx.throw_if_cancellation_requested(),
        "parent",
        &source,
        &seen,
        cleanup,
    )
    .await
}

fn descendant_worker(
    source: Value,
    seen: Observed,
    cleanup: &'static str,
    changed: &'static str,
) -> Worker {
    let mut worker = Worker::new(Client::new("http://unused.invalid").unwrap(), "queue")
        .cooperative_cancellation(true)
        .candidate_cancellation_scope_authoring(true)
        .candidate_cancellation_scope_delivery(true);
    worker.register_workflow("scope-replay", move |ctx, _| {
        let source = source.clone();
        let observed = Arc::clone(&seen);
        async move {
            let value = source.clone();
            let inside = Arc::clone(&observed);
            ctx.cancellation_scope(false, move |outer| async move {
                outer
                    .cancellation_scope(false, move |parent| {
                        parent_body(parent, value, inside, cleanup, changed)
                    })
                    .await?;
                assert!(!outer.is_cancellation_requested()?);
                assert!(outer.scoped_cancellation_context()?.is_none());
                Ok(Value::Null)
            })
            .await?;
            assert!(!ctx.is_cancellation_requested()?);
            assert!(ctx.scoped_cancellation_context()?.is_none());
            ctx.sleep(Duration::from_secs(1)).await?;
            Ok(if cleanup.is_empty() {
                json!("survivor")
            } else {
                json!({"remaining":observed.lock().unwrap()[cleanup].remaining()?.as_secs()})
            })
        }
    });
    worker
}

fn next_sequence(source: &Value) -> u64 {
    if source["layout"] == "timer" {
        9
    } else {
        12
    }
}

fn snapshot_for(source: &Value, target: &str) -> Value {
    let context = ScopedCancellationContext::from_value(&source["contexts"][target]).unwrap();
    json!({"scope_id":context.scope_id(), "operation_scope_id":context.scope_id(),
        "request_id":context.request_id(), "root_request_id":context.root_context().root_request_id(),
        "delivery_history_event_id":"ancestor-delivered", "preparation_history_event_id":"ancestor-prepared",
        "cleanup_deadline_at":"2026-10-04T00:00:30.123456Z", "authority_deadline_at":"2026-10-04T00:00:26.123456Z"})
}

#[test]
fn descendant_original_contexts_restore_and_leave_outer_and_root_unaffected() {
    for layout in ["timer", "group"] {
        let source = descendant_fixture(layout);
        let seen = Arc::new(Mutex::new(BTreeMap::new()));
        let worker = descendant_worker(source.clone(), Arc::clone(&seen), "", "");
        let decision = worker
            .execute_workflow_task_decision(task(&source))
            .unwrap();
        assert_eq!(seen.lock().unwrap().len(), 3);
        assert_eq!(
            decision.commands,
            vec![json!({"type":"start_timer", "delay_seconds":1})]
        );
    }
}

#[test]
fn descendant_pending_ancestor_preserves_original_identity_and_range_without_cleanup() {
    for layout in ["timer", "group"] {
        for (before, inherited) in [
            ("CancellationScopeDeliveryPrepared", false),
            ("CancellationScopeDeliveryPrepared", true),
            ("CancellationScopeDelivered", true),
        ] {
            let mut source = prefix(&descendant_fixture(layout), before);
            if !inherited {
                let parent = source["scopes"]["parent"].clone();
                source["history"].as_array_mut().unwrap().retain(|row| {
                    row["event_type"] != "CancellationScopeRequested"
                        || row["payload"]["scope_id"] == parent
                });
            }
            let seen = Arc::new(Mutex::new(BTreeMap::new()));
            let decision = descendant_worker(source.clone(), Arc::clone(&seen), "", "")
                .execute_workflow_task_decision(task(&source))
                .unwrap();
            assert!(decision.commands.is_empty());
            assert!(seen.lock().unwrap().is_empty());
            let intent = decision.cancellation_scope_delivery.unwrap();
            assert_eq!(intent.context.to_value(), source["contexts"]["parent"]);
            assert_eq!(intent.boundary.sequence, 8);
            assert_eq!(
                intent.boundary.sequence_span,
                if layout == "timer" { 1 } else { 4 }
            );
            let mut replacement = task(&source);
            replacement.lease_owner = Some("replacement".into());
            replacement.workflow_task_attempt = 17;
            let repeated = descendant_worker(source, Arc::clone(&seen), "", "")
                .execute_workflow_task_decision(replacement)
                .unwrap()
                .cancellation_scope_delivery
                .unwrap();
            assert_eq!(repeated.context, intent.context);
            assert_eq!(repeated.boundary, intent.boundary);
            assert!(seen.lock().unwrap().is_empty());
        }
    }
}

#[test]
fn descendant_pending_ancestor_cannot_bypass_competing_intermediate_request() {
    let fixtures: Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/committed-scope-operation-projections.json"
    ))
    .unwrap();
    let mut source = prefix(&fixtures["competing"], "CancellationScopeDeliveryPrepared");
    source["history"].as_array_mut().unwrap().retain(|row| {
        row["event_type"] != "CancellationScopeRequested"
            || row["payload"]["scope_id"] != "desc-grandchild"
    });
    let claim = task(&source);
    let scopes = cancellation_scope::CancellationScopeHistory::read(
        &claim.history_events,
        claim.run_id.as_deref().unwrap(),
    )
    .unwrap();
    let committed = crate::cancellation_scope_history::CommittedCancellationScopeHistory::read(
        &claim.history_events,
        claim.run_id.as_deref().unwrap(),
        claim.workflow_id.as_deref().unwrap(),
    )
    .unwrap();
    let error = committed
        .pending_request_for_scope("desc-grandchild", &scopes)
        .unwrap_err();
    assert!(error.to_string().contains("original ancestor lineage"));
}

#[test]
fn descendant_cleanup_retains_ancestor_proof_narrower_authority_and_replacement_clock() {
    for layout in ["timer", "group"] {
        for target in ["grandchild", "child", "parent"] {
            let mut source = descendant_fixture(layout);
            let seen = Arc::new(Mutex::new(BTreeMap::new()));
            let snapshot = snapshot_for(&source, target);
            let sequence = next_sequence(&source);
            let wire = json!({"type":"start_timer", "delay_seconds":1,
                "cancellation_scope_id":snapshot["scope_id"], "cancellation_cleanup":{
                    "scope_id":snapshot["scope_id"], "request_id":snapshot["request_id"],
                    "delivery_history_event_id":snapshot["delivery_history_event_id"]}});
            assert_eq!(
                descendant_worker(source.clone(), Arc::clone(&seen), target, "")
                    .execute_workflow_task_decision(task(&source))
                    .unwrap()
                    .commands,
                vec![wire.clone()]
            );
            append(
                &mut source,
                "TimerScheduled",
                json!({"sequence":sequence,
                "timer_id":"descendant-cleanup", "delay_seconds":1,
                "cancellation_scope_id":snapshot["scope_id"], "cancellation_cleanup":snapshot,
                "fire_at":"2026-10-04T00:00:11.123456Z"}),
                "2026-10-04T00:00:10.123456Z",
            );
            let mut claim = task(&source);
            claim.lease_owner = Some("replacement".into());
            claim.workflow_task_attempt = 17;
            let blocked = descendant_worker(source.clone(), Arc::clone(&seen), target, "")
                .execute_workflow_task_decision(claim)
                .unwrap();
            assert!(
                blocked.commands.is_empty(),
                "an admitted cleanup timer must remain pending"
            );
            assert!(blocked.cancellation_scope_delivery.is_none());
            append(
                &mut source,
                "TimerFired",
                json!({"sequence":sequence,
                "timer_id":"descendant-cleanup", "delay_seconds":1,
                "cancellation_scope_id":snapshot["scope_id"]}),
                "2026-10-04T00:00:11.123456Z",
            );
            assert_eq!(
                descendant_worker(source.clone(), Arc::clone(&seen), target, "")
                    .execute_workflow_task_decision(task(&source))
                    .unwrap()
                    .commands,
                vec![json!({"type":"start_timer", "delay_seconds":1})]
            );
            append(
                &mut source,
                "TimerScheduled",
                json!({"sequence":sequence+1, "timer_id":"root-timer",
                "delay_seconds":1, "fire_at":"2026-10-04T00:00:12.123456Z"}),
                "2026-10-04T00:00:11.123456Z",
            );
            append(
                &mut source,
                "TimerFired",
                json!({"sequence":sequence+1, "timer_id":"root-timer",
                "delay_seconds":1}),
                "2026-10-04T00:00:20.123456Z",
            );
            let result = descendant_worker(source.clone(), Arc::clone(&seen), target, "")
                .execute_workflow_task_decision(task(&source))
                .unwrap();
            assert_eq!(result.commands[0]["type"], "complete_workflow");
            assert_eq!(
                decode_payload::<Value>(
                    &serde_json::from_value(result.commands[0]["result"].clone()).unwrap()
                )
                .unwrap(),
                json!({"remaining":6})
            );
        }
    }
}

#[test]
fn descendant_changed_subtree_cannot_enter_cleanup() {
    for (layout, change) in [
        ("timer", "shield"),
        ("timer", "timer"),
        ("timer", "prefix"),
        ("group", "activity"),
        ("group", "activity-policy"),
        ("group", "child"),
        ("group", "child-policy"),
        ("group", "layout"),
        ("group", "size"),
    ] {
        let source = descendant_fixture(layout);
        let seen = Arc::new(Mutex::new(BTreeMap::new()));
        assert!(
            descendant_worker(source.clone(), Arc::clone(&seen), "", change)
                .execute_workflow_task_decision(task(&source))
                .is_err(),
            "{layout}/{change}"
        );
        assert!(seen.lock().unwrap().is_empty(), "{layout}/{change}");
    }
}

#[test]
fn descendant_changed_cleanup_snapshot_refuses_before_factory() {
    for field in [
        "scope_id",
        "operation_scope_id",
        "request_id",
        "root_request_id",
        "delivery_history_event_id",
        "preparation_history_event_id",
        "cleanup_deadline_at",
        "authority_deadline_at",
        "omitted",
        "null",
        "retrograde",
    ] {
        let mut source = descendant_fixture("timer");
        let mut snapshot = snapshot_for(&source, "grandchild");
        if field == "null" {
            snapshot = Value::Null;
        } else if field != "omitted" && field != "retrograde" {
            snapshot[field] = json!("changed");
        }
        let mut payload = json!({"sequence":9, "timer_id":"descendant-cleanup", "delay_seconds":1,
            "cancellation_scope_id":"grandchild-scope", "cancellation_cleanup":snapshot,
            "fire_at":if field=="retrograde" { "2026-10-04T00:00:09.123456Z" } else { "2026-10-04T00:00:11.123456Z" }});
        if field == "omitted" {
            payload
                .as_object_mut()
                .unwrap()
                .remove("cancellation_cleanup");
        }
        append(
            &mut source,
            "TimerScheduled",
            payload,
            "2026-10-04T00:00:10.123456Z",
        );
        let invoked = Arc::new(AtomicUsize::new(0));
        let calls = Arc::clone(&invoked);
        let mut worker = descendant_worker(
            source.clone(),
            Arc::new(Mutex::new(BTreeMap::new())),
            "",
            "",
        );
        worker.register_workflow("scope-replay", move |_, _| {
            calls.fetch_add(1, Ordering::SeqCst);
            async { Ok(Value::Null) }
        });
        assert!(
            worker
                .execute_workflow_task_decision(task(&source))
                .is_err(),
            "{field}"
        );
        assert_eq!(invoked.load(Ordering::SeqCst), 0, "{field}");
    }
}

#[test]
fn descendant_competing_root_refuses_before_factory() {
    let fixtures: Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/committed-scope-operation-projections.json"
    ))
    .unwrap();
    let source = fixtures["competing"].clone();
    let invoked = Arc::new(AtomicUsize::new(0));
    let calls = Arc::clone(&invoked);
    let mut worker = descendant_worker(
        source.clone(),
        Arc::new(Mutex::new(BTreeMap::new())),
        "",
        "",
    );
    worker.register_workflow("scope-replay", move |_, _| {
        calls.fetch_add(1, Ordering::SeqCst);
        async { Ok(Value::Null) }
    });
    assert!(worker
        .execute_workflow_task_decision(task(&source))
        .is_err());
    assert_eq!(invoked.load(Ordering::SeqCst), 0);
}
