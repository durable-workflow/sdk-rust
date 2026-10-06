use super::*;
use std::collections::BTreeMap;

fn source() -> Value {
    serde_json::from_str(include_str!(
        "../../../tests/fixtures/run-inherited-scope-timers.json"
    ))
    .unwrap()
}

fn at_phase(source: &Value, phase: &str) -> Value {
    let mut value = source.clone();
    let end = value["history_ranges"][phase].as_u64().unwrap() as usize;
    value["history"].as_array_mut().unwrap().truncate(end);
    value
}

fn probe(seen: Arc<Mutex<BTreeMap<String, Value>>>, changed: &'static str) -> Worker {
    let mut worker = Worker::new(Client::new("http://unused.invalid").unwrap(), "queue")
        .cooperative_cancellation(true)
        .candidate_cancellation_scope_authoring(true)
        .candidate_cancellation_scope_delivery(true);
    worker.register_workflow("scope-replay", move |ctx, _| {
        let seen = Arc::clone(&seen);
        async move {
            let root = ctx.clone();
            let scoped_seen = Arc::clone(&seen);
            let captured = Arc::new(Mutex::new(None::<WorkflowContext>));
            let captured_scope = Arc::clone(&captured);
            ctx.cancellation_scope(false, move |outer| async move {
                outer
                    .cancellation_scope(false, move |inner| async move {
                        *captured_scope.lock().unwrap() = Some(inner.clone());
                        let cancelled = match inner.sleep(Duration::from_secs(300)).await {
                            Err(Error::CancellationScopeRequested(cancelled)) => cancelled,
                            other => panic!("Expected the original scoped delivery, got {other:?}"),
                        };
                        assert!(inner.cancellation_context()?.is_none());
                        assert_eq!(
                            inner.scoped_cancellation_context()?.unwrap().to_value(),
                            cancelled.context.to_value()
                        );
                        scoped_seen
                            .lock()
                            .unwrap()
                            .insert("scoped".into(), cancelled.context.to_value());
                        scoped_seen.lock().unwrap().insert(
                            "scoped_remaining".into(),
                            json!(cancelled.context.remaining()?.as_secs_f64()),
                        );
                        let _shield = inner.cancellation_shield()?;
                        if changed == "unshielded" {
                            drop(_shield);
                        }
                        inner.sleep(Duration::from_secs(2)).await?;
                        scoped_seen.lock().unwrap().insert(
                            "scoped_after".into(),
                            json!(cancelled.context.remaining()?.as_secs_f64()),
                        );
                        Ok(Value::Null)
                    })
                    .await
            })
            .await?;
            if matches!(
                changed,
                "captured_scalar" | "captured_parallel" | "captured_selection"
            ) {
                let captured = captured.lock().unwrap().as_ref().unwrap().clone();
                match changed {
                    "captured_scalar" => captured.sleep(Duration::from_secs(300)).await?,
                    "captured_parallel" => {
                        captured
                            .parallel(vec![ParallelOperation::timer(Duration::from_secs(300))])
                            .await?;
                    }
                    _ => {
                        captured
                            .select_keyed(vec![(
                                "timer",
                                ParallelOperation::timer(Duration::from_secs(300)),
                            )])
                            .await?;
                    }
                }
            }
            if changed == "new_scope_before_root" {
                return root
                    .cancellation_scope(false, |scope| async move {
                        scope.sleep(Duration::from_secs(300)).await?;
                        Ok(Value::Null)
                    })
                    .await;
            }
            let Err(Error::CooperativeCancellationRequested(cancelled)) =
                root.sleep(Duration::from_secs(300)).await
            else {
                return Err(Error::CancellationScopeExecutionUnavailable);
            };
            let context = root.cancellation_context()?.unwrap();
            seen.lock()
                .unwrap()
                .insert("root".into(), context.to_value());
            seen.lock().unwrap().insert(
                "root_remaining".into(),
                json!(context.remaining()?.as_secs_f64()),
            );
            {
                let _shield = root.cancellation_shield()?;
                if changed == "new_scope" {
                    root.cancellation_scope(false, |scope| async move {
                        scope.sleep(Duration::from_secs(1)).await?;
                        Ok(Value::Null)
                    })
                    .await?;
                }
                root.sleep(Duration::from_secs(2)).await?;
            }
            seen.lock().unwrap().insert(
                "root_after".into(),
                json!(context.remaining()?.as_secs_f64()),
            );
            Err(Error::CooperativeCancellationRequested(cancelled))
        }
    });
    worker
}

#[test]
fn run_scope_replay_preserves_ancestor_delivery_and_descendant_cleanup_before_root() {
    let source = source();
    for phase in [
        "requested",
        "prepared",
        "scope_delivered",
        "scope_cleanup_scheduled",
        "scope_cleaned",
        "root_delivered",
        "root_cleanup_scheduled",
        "root_cleaned",
    ] {
        let seen = Arc::new(Mutex::new(BTreeMap::new()));
        let claim = task(&at_phase(&source, phase));
        let mut pending = CancellationHistory::from_events(
            &claim.history_events,
            claim.run_id.as_deref().unwrap(),
            None,
        )
        .unwrap()
        .request
        .unwrap();
        pending.history_refresh_page_token = Some("MA==".into());
        let decision = probe(Arc::clone(&seen), "")
            .execute_workflow_task_decision_with_cancellation(claim, Some(&pending))
            .unwrap_or_else(|error| panic!("{phase}: {error:?}"));
        let observed = seen.lock().unwrap();
        match phase {
            "requested" | "prepared" => {
                assert!(observed.is_empty());
                let intent = decision.cancellation_scope_delivery.unwrap();
                assert_eq!(intent.context.scope_id(), source["scopes"]["parent"]);
                assert_eq!(intent.boundary.sequence, 3);
                assert!(decision.cancellation_delivery.is_none());
                assert!(decision.commands.is_empty());
            }
            "scope_delivered" => {
                assert_eq!(observed["scoped_remaining"].as_f64(), Some(16.0));
                assert_eq!(
                    observed["scoped"]["lineage"]
                        .as_array()
                        .unwrap()
                        .last()
                        .unwrap()["scope_id"],
                    source["scopes"]["child"]
                );
                assert_eq!(
                    decision.commands[0]["cancellation_scope_id"],
                    source["scopes"]["child"]
                );
                assert_eq!(
                    decision.commands[0]["cancellation_cleanup"]["scope_id"],
                    source["scopes"]["parent"]
                );
                assert!(decision.cancellation_delivery.is_none());
            }
            "scope_cleanup_scheduled" => {
                assert!(decision.commands.is_empty());
                assert!(decision.cancellation_delivery.is_none());
                assert!(!observed.contains_key("root"));
            }
            "scope_cleaned" => {
                assert_eq!(observed["scoped_after"].as_f64(), Some(14.0));
                assert!(decision.commands.is_empty());
                assert_eq!(decision.cancellation_delivery.unwrap().sequence, 5);
                assert!(!observed.contains_key("root"));
            }
            "root_delivered" => {
                assert_eq!(observed["scoped_after"].as_f64(), Some(14.0));
                assert_eq!(observed["root"], observed["scoped"]["root_context"]);
                assert_eq!(observed["root_remaining"].as_f64(), Some(11.0));
                assert_eq!(
                    decision.commands,
                    vec![json!({"type":"start_timer", "delay_seconds":2})]
                );
            }
            "root_cleanup_scheduled" => {
                assert!(decision.commands.is_empty());
                assert!(!observed.contains_key("root_after"));
            }
            "root_cleaned" => {
                assert_eq!(observed["root_after"].as_f64(), Some(9.0));
                assert_eq!(decision.commands.len(), 1);
                assert_eq!(decision.commands[0]["type"], "fail_workflow");
                assert_eq!(
                    decision.commands[0]["exception_type"],
                    "WorkflowCancellationRequested"
                );
            }
            _ => unreachable!(),
        }
    }
}

#[test]
fn run_scope_replay_refuses_changed_shielding_and_new_scope_during_root_cleanup() {
    let source = source();
    for changed in [
        "unshielded",
        "captured_scalar",
        "captured_parallel",
        "captured_selection",
        "new_scope_before_root",
        "new_scope",
    ] {
        let seen = Arc::new(Mutex::new(BTreeMap::new()));
        assert!(
            probe(Arc::clone(&seen), changed)
                .execute_workflow_task_decision(task(&source))
                .is_err(),
            "{changed}"
        );
        let observed = seen.lock().unwrap();
        assert!(
            observed.contains_key("scoped"),
            "{changed}: valid scoped delivery must be reached"
        );
        if changed == "new_scope" {
            assert!(observed.contains_key("root"));
        } else {
            assert!(!observed.contains_key("root"));
        }
    }
}

#[test]
fn run_scope_replay_refuses_changed_native_cleanup_authority_before_factory() {
    let original = source();
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
        "extra",
    ] {
        let mut changed = original.clone();
        let index = changed["history_ranges"]["scope_cleanup_scheduled"]
            .as_u64()
            .unwrap() as usize
            - 1;
        let payload = changed["history"][index]["payload"]
            .as_object_mut()
            .unwrap();
        match field {
            "omitted" => {
                payload.remove("cancellation_cleanup");
            }
            "null" => {
                payload.insert("cancellation_cleanup".into(), Value::Null);
            }
            "extra" => {
                payload.get_mut("cancellation_cleanup").unwrap()["extra"] = json!("borrowed");
            }
            _ => {
                payload.get_mut("cancellation_cleanup").unwrap()[field] = json!("changed");
            }
        }
        let invoked = Arc::new(AtomicUsize::new(0));
        let calls = Arc::clone(&invoked);
        let mut worker = Worker::new(Client::new("http://unused.invalid").unwrap(), "queue")
            .cooperative_cancellation(true)
            .candidate_cancellation_scope_authoring(true)
            .candidate_cancellation_scope_delivery(true);
        worker.register_workflow("scope-replay", move |_, _| {
            calls.fetch_add(1, Ordering::SeqCst);
            async { Ok(Value::Null) }
        });
        assert!(
            worker
                .execute_workflow_task_decision(task(&changed))
                .is_err(),
            "{field}"
        );
        assert_eq!(invoked.load(Ordering::SeqCst), 0, "{field}");
    }
}
