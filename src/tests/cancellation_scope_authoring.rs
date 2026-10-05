use super::*;

fn event(kind: &str, payload: Value, index: u64) -> HistoryEvent {
    serde_json::from_value(json!({"id": format!("event-{index}"), "sequence": index,
        "namespace":"tenant", "event_type":kind, "payload":payload}))
    .unwrap()
}

fn started() -> HistoryEvent {
    event("WorkflowStarted", json!({}), 1)
}

fn opening(sequence: u64, id: &str, parent: &str, shield: bool, index: u64) -> HistoryEvent {
    event(
        "CancellationScopeOpened",
        json!({"schema":"durable-workflow.cancellation-scope/v1",
        "workflow_run_id":"run-one", "sequence":sequence, "scope_id":id, "parent_scope_id":parent,
        "shield_parent":shield}),
        index,
    )
}

fn context(history: Vec<HistoryEvent>) -> WorkflowContext {
    WorkflowContext {
        cancellation_scope_id: "root".into(),
        state: Arc::new(Mutex::new(
            WorkflowState::new_with_identity_and_scopes(
                history,
                Some("workflow-one".into()),
                Some("run-one".into()),
                "queue".into(),
                DEFAULT_CODEC.into(),
                None,
                true,
            )
            .unwrap(),
        )),
    }
}

fn poll<F: Future + Unpin>(future: &mut F) -> Poll<F::Output> {
    let mut cx = TaskContext::from_waker(noop_waker_ref());
    Pin::new(future).poll(&mut cx)
}

#[test]
fn cancellation_scope_authoring_body_waits_for_canonical_opening() {
    let ctx = context(vec![started()]);
    let calls = Arc::new(AtomicUsize::new(0));
    let body_calls = Arc::clone(&calls);
    let mut scope = Box::pin(ctx.cancellation_scope(false, move |_| async move {
        body_calls.fetch_add(1, Ordering::SeqCst);
        Ok("done")
    }));
    assert!(poll(&mut scope).is_pending());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let state = ctx.state.lock().unwrap();
    let intent = state.cancellation_scope_opening.as_ref().unwrap();
    assert_eq!(intent.sequence, 1);
    assert_eq!(intent.parent_scope_id, "root");
    assert!(!intent.shield_parent);
    assert_eq!(intent.command_count, 0);
    assert!(state.commands.is_empty());
}

#[test]
fn cancellation_scope_authoring_default_context_refuses_before_body() {
    let ctx = workflow_context(Vec::new());
    let mut scope = Box::pin(ctx.cancellation_scope(false, |_| async move {
        panic!("unqualified scope cannot enter body");
        #[allow(unreachable_code)]
        Ok(())
    }));
    assert!(matches!(
        poll(&mut scope),
        Poll::Ready(Err(Error::CancellationScopeExecutionUnavailable))
    ));
}

#[test]
fn cancellation_scope_authoring_nested_context_and_deferred_timers_retain_membership() {
    let ctx = context(vec![
        started(),
        opening(1, "outer", "root", false, 2),
        opening(2, "inner", "outer", true, 3),
    ]);
    let mut scope = Box::pin(ctx.cancellation_scope(false, |outer| async move {
        let deferred = outer
            .cancellation_scope(true, |inner| async move {
                Ok(inner.sleep(Duration::from_secs(1)))
            })
            .await?;
        Ok((deferred, outer.sleep(Duration::from_secs(2))))
    }));
    let Poll::Ready(Ok((mut inner, mut outer))) = poll(&mut scope) else {
        panic!("canonical body must resume");
    };
    let mut root = ctx.sleep(Duration::from_secs(3));
    assert!(poll(&mut inner).is_pending());
    assert!(poll(&mut outer).is_pending());
    assert!(poll(&mut root).is_pending());
    let wire = ctx.take_commands().unwrap();
    assert_eq!(wire.len(), 3);
    assert_eq!(wire[0]["cancellation_scope_id"], "inner");
    assert_eq!(wire[1]["cancellation_scope_id"], "outer");
    assert!(wire[2].get("cancellation_scope_id").is_none());
    assert_eq!(ctx.cancellation_scope_id, "root");
}

#[test]
fn cancellation_scope_authoring_prefix_does_not_enter_body_and_keeps_sequence() {
    let ctx = context(vec![started()]);
    assert_eq!(ctx.side_effect(|| "prefix".to_string()).unwrap(), "prefix");
    let mut scope = Box::pin(ctx.cancellation_scope(false, |_| async move {
        panic!("opening must commit after the prefix");
        #[allow(unreachable_code)]
        Ok(())
    }));
    assert!(poll(&mut scope).is_pending());
    let state = ctx.state.lock().unwrap();
    assert_eq!(
        state.cancellation_scope_opening.as_ref().unwrap().sequence,
        2
    );
    assert_eq!(
        state
            .cancellation_scope_opening
            .as_ref()
            .unwrap()
            .command_count,
        1
    );
}

#[test]
fn cancellation_scope_authoring_changed_parent_shield_or_position_cannot_enter_body() {
    for (parent, shield, sequence) in [("root", true, 1), ("root", false, 2)] {
        let ctx = context(vec![
            started(),
            opening(sequence, "outer", parent, shield, 2),
        ]);
        let mut scope = Box::pin(ctx.cancellation_scope(false, |_| async move {
            panic!("changed opening cannot enter body");
            #[allow(unreachable_code)]
            Ok(())
        }));
        assert!(
            matches!(poll(&mut scope), Poll::Ready(Err(Error::NonDeterministicReplay(f)))
            if f.reason == "cancellation_scope_opening_changed")
        );
    }
    let ctx = context(vec![
        started(),
        opening(1, "outer", "root", false, 2),
        opening(2, "inner", "root", false, 3),
    ]);
    let mut scope = Box::pin(ctx.cancellation_scope(false, |outer| async move {
        outer
            .cancellation_scope(false, |_| async move {
                panic!("changed nested parent cannot enter body");
                #[allow(unreachable_code)]
                Ok(())
            })
            .await
    }));
    assert!(
        matches!(poll(&mut scope), Poll::Ready(Err(Error::NonDeterministicReplay(f)))
        if f.reason == "cancellation_scope_opening_changed")
    );
}

#[test]
fn cancellation_scope_authoring_invalid_tree_and_operation_membership_refuse_before_factory() {
    let base = opening(1, "outer", "root", false, 2);
    let mutations = [
        ("scope_id", json!("root")),
        ("scope_id", json!("")),
        ("scope_id", json!(true)),
        ("scope_id", json!("é".repeat(128))),
        ("parent_scope_id", json!("missing")),
        ("parent_scope_id", json!("outer")),
        ("workflow_run_id", json!("foreign")),
        ("schema", json!("foreign")),
        ("sequence", json!(true)),
        ("sequence", json!(0)),
        ("sequence", json!(u64::MAX)),
        ("shield_parent", json!(1)),
    ];
    for (field, value) in mutations {
        let mut row = base.clone();
        row.payload[field] = value;
        let calls = Arc::new(AtomicUsize::new(0));
        let factory_calls = Arc::clone(&calls);
        let mut worker = Worker::new(Client::new("http://127.0.0.1:1").unwrap(), "queue")
            .cooperative_cancellation(true)
            .candidate_cancellation_scope_authoring(true);
        worker.register_workflow("scope-probe", move |_, _| {
            factory_calls.fetch_add(1, Ordering::SeqCst);
            async move { Ok(json!("unexpected")) }
        });
        let mut task = workflow_task("scope-probe", vec![started(), row], DEFAULT_CODEC);
        task.run_id = Some("run-one".into());
        assert!(matches!(
            worker.execute_workflow_task(task),
            Err(Error::NonDeterministicReplay(_))
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
    for payload in [
        json!({"sequence":2,"cancellation_scope_id":"missing"}),
        json!({"sequence":1,"cancellation_scope_id":"outer"}),
        json!({"sequence":true,"cancellation_scope_id":"outer"}),
        json!({"sequence":2,"cancellation_scope_id":null}),
        json!({"sequence":2,"cancellation_scope_id":"outer","timer":{"cancellation_scope_id":"root"}}),
    ] {
        assert!(cancellation_scope::CancellationScopeHistory::read(
            &[started(), base.clone(), event("TimerScheduled", payload, 3)],
            "run-one"
        )
        .is_err());
    }
}

#[test]
fn cancellation_scope_authoring_timer_replay_checks_membership_and_consumes_opening() {
    let history = vec![
        started(),
        opening(1, "outer", "root", false, 2),
        event(
            "TimerScheduled",
            json!({"sequence":2,"timer_id":"timer-one","delay_seconds":1,"cancellation_scope_id":"outer"}),
            3,
        ),
        event(
            "TimerFired",
            json!({"sequence":2,"timer_id":"timer-one","delay_seconds":1}),
            4,
        ),
    ];
    for root_command in [false, true] {
        let ctx = context(history.clone());
        let mut scope = Box::pin(ctx.cancellation_scope(false, |scoped| async move {
            Ok(scoped.sleep(Duration::from_secs(1)))
        }));
        let Poll::Ready(Ok(mut timer)) = poll(&mut scope) else {
            panic!("scope must replay");
        };
        if root_command {
            timer = ctx.sleep(Duration::from_secs(1));
        }
        let actual = poll(&mut timer);
        if root_command {
            assert!(
                matches!(actual,Poll::Ready(Err(Error::NonDeterministicReplay(f)))
                if f.reason == "cancellation_scope_membership_changed")
            );
        } else {
            assert!(matches!(actual, Poll::Ready(Ok(()))));
            ctx.ensure_history_consumed().unwrap();
        }
    }
}

#[test]
fn cancellation_scope_authoring_unimplemented_delivery_and_closed_history_refuse() {
    for kind in [
        "CancellationScopeRequested",
        "CancellationScopeDeliveryPrepared",
        "CancellationScopeDelivered",
        "CancellationScopeRequestConflicted",
    ] {
        assert!(matches!(
            WorkflowState::new_with_identity_and_scopes(
                vec![
                    started(),
                    opening(1, "outer", "root", false, 2),
                    event(kind, json!({}), 3)
                ],
                None,
                Some("run-one".into()),
                "queue".into(),
                DEFAULT_CODEC.into(),
                None,
                true
            ),
            Err(Error::CancellationScopeExecutionUnavailable)
        ));
    }
    for kind in [
        "WorkflowCompleted",
        "WorkflowFailed",
        "WorkflowCancelled",
        "WorkflowTerminated",
    ] {
        let ctx = context(vec![started(), event(kind, json!({}), 2)]);
        let mut scope = Box::pin(ctx.cancellation_scope(false, |_| async move {
            panic!("closed history cannot enter new scope");
            #[allow(unreachable_code)]
            Ok(())
        }));
        assert!(
            matches!(poll(&mut scope),Poll::Ready(Err(Error::NonDeterministicReplay(f)))
            if f.reason == "cancellation_scope_opening_changed")
        );
    }
}

#[test]
fn cancellation_scope_authoring_all_supported_operation_futures_keep_creation_context() {
    let ctx = context(vec![started(), opening(1, "outer", "root", false, 2)]);
    let mut scope = Box::pin(ctx.cancellation_scope(false, |scoped| async move { Ok(scoped) }));
    let Poll::Ready(Ok(scoped)) = poll(&mut scope) else {
        panic!("scope must replay");
    };
    let mut activity = scoped.activity("remote", json!([]));
    let mut child =
        scoped.start_child_workflow("child", ChildWorkflowOptions::new("queue"), json!([]));
    let mut signal = scoped.wait_signal("resume");
    let mut condition = scoped
        .wait_condition(ConditionWaitOptions::new("ready", "sha256:ready"), || {
            Ok(false)
        });
    assert!(poll(&mut activity).is_pending());
    assert!(poll(&mut child).is_pending());
    assert!(poll(&mut signal).is_pending());
    assert!(poll(&mut condition).is_pending());
    let wire = ctx.take_commands().unwrap();
    assert_eq!(wire.len(), 4);
    for command in wire {
        assert_eq!(command["cancellation_scope_id"], "outer");
    }
    assert_eq!(ctx.cancellation_scope_id, "root");
}

#[test]
fn cancellation_scope_authoring_worker_cannot_publish_after_ignoring_pending_opening() {
    let mut worker = Worker::new(Client::new("http://127.0.0.1:1").unwrap(), "queue")
        .cooperative_cancellation(true)
        .candidate_cancellation_scope_authoring(true);
    worker.register_workflow("scope-probe", |ctx, _| async move {
        let mut opening = Box::pin(ctx.cancellation_scope(false, |_| async move {
            panic!("uncommitted scope cannot enter body");
            #[allow(unreachable_code)]
            Ok(())
        }));
        std::future::poll_fn(|cx| {
            assert!(opening.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        ctx.side_effect(|| "escaped".to_string())?;
        Ok(json!("must not publish"))
    });
    let mut task = workflow_task("scope-probe", vec![started()], DEFAULT_CODEC);
    task.run_id = Some("run-one".into());
    assert!(
        matches!(worker.execute_workflow_task(task),Err(Error::NonDeterministicReplay(f))
        if f.reason == "cancellation_scope_pending_call_escaped")
    );
}

#[test]
fn cancellation_scope_authoring_cannot_enable_protocol_or_delivery_implicitly() {
    let calls = Arc::new(AtomicUsize::new(0));
    let factory_calls = Arc::clone(&calls);
    let mut worker = Worker::new(Client::new("http://127.0.0.1:1").unwrap(), "queue")
        .candidate_cancellation_scope_authoring(true);
    worker.register_workflow("scope-probe", move |_, _| {
        factory_calls.fetch_add(1, Ordering::SeqCst);
        async move { Ok(json!("unexpected")) }
    });
    assert!(matches!(
        worker.execute_workflow_task(workflow_task("scope-probe", vec![started()], DEFAULT_CODEC)),
        Err(Error::CancellationScopeExecutionUnavailable)
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}
