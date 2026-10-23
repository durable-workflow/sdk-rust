use durable_workflow::{
    json,
    testing::{TestingError, WorkflowEnvironment, WorkflowHistory},
    ActivityError, ActivityOptions, ActivityRetryPolicy, Error, LocalActivityOptions,
    ParallelOperation, ParallelResult, SelectionKey,
};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

fn two_activities(first: &'static str, second: &'static str) -> WorkflowEnvironment {
    let mut env = WorkflowEnvironment::new().unwrap();
    env.register_workflow("two", move |ctx, _| async move {
        let a = ctx.activity(first, json!([])).await?;
        let b = ctx.activity(second, json!([])).await?;
        Ok(json!([a, b]))
    });
    env.register_activity("alpha", |_, _| async { Ok(json!("a")) });
    env.register_activity("beta", |_, _| async { Ok(json!("b")) });
    env
}

#[tokio::test]
async fn unchanged_api_history_and_cli_export_replay_without_server_or_activity_callbacks() {
    let mut env = two_activities("alpha", "beta");
    assert_eq!(
        env.run_workflow("two", ()).await.unwrap(),
        json!(["a", "b"])
    );
    let history = env.last_history().unwrap();
    let export = history.export();
    let mut replay = two_activities("alpha", "beta");
    replay.mock_activity("alpha", |_, _| async {
        panic!("replay must not execute activities")
    });
    let imported = WorkflowHistory::from_export(export).unwrap();
    assert!(replay.replay(&imported).unwrap().completed);
    let response = json!({"events":history.events(),"next_page_token":null});
    let imported = WorkflowHistory::from_history("two", (), response)
        .unwrap()
        .task_queue("test");
    assert!(replay.replay(&imported).unwrap().completed);
}

#[tokio::test]
async fn reordered_and_changed_activity_types_report_exact_divergence() {
    let mut original = two_activities("alpha", "beta");
    original.run_workflow("two", ()).await.unwrap();
    let history = original.last_history().unwrap();
    for (first, second, sequence, expected, actual) in [
        ("beta", "alpha", 1, "alpha", "beta"),
        ("alpha", "gamma", 2, "beta", "gamma"),
    ] {
        let candidate = two_activities(first, second);
        let Error::NonDeterministicReplay(failure) = candidate.replay(history).unwrap_err() else {
            panic!("divergence must retain its typed failure");
        };
        assert_eq!(failure.sequence, Some(sequence));
        assert_eq!(
            failure.expected.as_deref(),
            Some(format!("activity:{expected}").as_str())
        );
        assert_eq!(
            failure.actual.as_deref(),
            Some(format!("activity:{actual}").as_str())
        );
    }
}

#[tokio::test]
async fn real_and_mocked_activities_retries_timers_and_markers_run_in_process() {
    let mut env = WorkflowEnvironment::new().unwrap();
    let side_effects = Arc::new(AtomicUsize::new(0));
    let effects = side_effects.clone();
    env.register_workflow("retry", move |ctx, _| {
        let effects = effects.clone();
        async move {
            let snapshot = ctx.side_effect(|| effects.fetch_add(1, Ordering::SeqCst))?;
            let version = ctx.get_version("change", 1, 2)?;
            let value = ctx
                .activity_with_options(
                    "flaky",
                    ActivityOptions::new().retry_policy(
                        ActivityRetryPolicy::new(3).backoff_intervals([Duration::from_secs(3600)]),
                    ),
                    json!([version]),
                )
                .await?;
            ctx.sleep(Duration::from_secs(86400)).await?;
            Ok(json!({"snapshot":snapshot,"value":value}))
        }
    });
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    env.register_activity("flaky", move |ctx, input| {
        let count = count.clone();
        async move {
            count.fetch_add(1, Ordering::SeqCst);
            assert!(
                ctx.heartbeat(json!({"attempt":ctx.attempt_number}))
                    .await?
                    .heartbeat_recorded
            );
            if ctx.attempt_number < 3 {
                return Err(ActivityError::new("Transient", "try again").into());
            }
            Ok(input[0].clone())
        }
    });
    let result = tokio::time::timeout(Duration::from_secs(2), env.run_workflow("retry", ()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result, json!({"snapshot":0,"value":2}));
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert_eq!(side_effects.load(Ordering::SeqCst), 1);
    assert!(env.replay(env.last_history().unwrap()).unwrap().completed);
    assert_eq!(side_effects.load(Ordering::SeqCst), 1);

    env.mock_activity("flaky", |_, _| async { Ok(json!("mock")) });
    assert_eq!(
        env.run_workflow("retry", ()).await.unwrap()["value"],
        "mock"
    );
}

#[tokio::test]
async fn activity_failures_reach_workflow_as_live_typed_outcomes_in_both_modes() {
    for local in [false, true] {
        let mut env = WorkflowEnvironment::new().unwrap();
        env.register_activity("failure", |_, args| async move {
            Err(ActivityError::new("PaymentDeclined", "card declined")
                .non_retryable(args[0] == 2)
                .into())
        });
        env.register_workflow("failures",move |ctx,_| async move {
            let mut out=Vec::new();
            for case in 0..3 {
                let policy=ActivityRetryPolicy::new(3).non_retryable_error_type(if case==0 {"PaymentDeclined"} else {"Other"});
                let result=if local {
                    ctx.local_activity_with_options("failure",LocalActivityOptions::new().retry_policy(policy),json!([case])).await
                } else {
                    ctx.activity_with_options("failure",ActivityOptions::new().retry_policy(policy),json!([case])).await
                };
                let Err(Error::ActivityFailed(failure))=result else {panic!("expected typed terminal failure local={local} case={case}: {result:?}");};
                out.push(json!({"type":failure.exception_type,"message":failure.message,"attempt":failure.attempt_number,"non_retryable":failure.non_retryable}));
            }
            Ok(json!(out))
        });
        assert_eq!(
            env.run_workflow("failures", ()).await.unwrap(),
            json!([
                {"type":"PaymentDeclined","message":"card declined","attempt":1,"non_retryable":true},
                {"type":"PaymentDeclined","message":"card declined","attempt":3,"non_retryable":false},
                {"type":"PaymentDeclined","message":"card declined","attempt":1,"non_retryable":true},
            ])
        );
        assert!(env.replay(env.last_history().unwrap()).unwrap().completed);
    }
}

#[tokio::test]
async fn direct_activity_and_uncaught_workflow_failures_keep_their_types() {
    let mut env = WorkflowEnvironment::new().unwrap();
    env.register_activity("fail", |_, _| async {
        Err(ActivityError::new("Declined", "no").into())
    });
    assert!(
        matches!(env.run_activity("fail",()).await,Err(Error::ActivityError(e)) if e.exception_type=="Declined")
    );
    env.register_workflow("fail", |ctx, _| async move {
        ctx.activity("fail", json!([])).await
    });
    assert!(
        matches!(env.run_workflow("fail",()).await,Err(Error::ActivityFailed(f)) if f.exception_type.as_deref()==Some("Declined"))
    );
}

#[tokio::test]
async fn recorded_select_reuses_worker_resolution_and_loser_await_paths() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "fixtures/durable_selection_runtime_history.json"
    ))
    .unwrap();
    let history = WorkflowHistory::from_history("select", (), fixture["history"].clone())
        .unwrap()
        .task_queue("default");
    let mut env = WorkflowEnvironment::new().unwrap();
    env.register_workflow("select", |ctx, _| async move {
        let selected = ctx
            .select_keyed(vec![
                (
                    "slow",
                    ParallelOperation::activity_with_options(
                        "slow-activity",
                        ActivityOptions::new().task_queue("default"),
                        json!([]),
                    ),
                ),
                (
                    "fast",
                    ParallelOperation::activity_with_options(
                        "fast-activity",
                        ActivityOptions::new().task_queue("default"),
                        json!([]),
                    ),
                ),
            ])
            .await?;
        assert_eq!(selected.key, SelectionKey::Name("fast".into()));
        assert_eq!(
            selected.value,
            Some(ParallelResult::Activity(json!("winner-value")))
        );
        assert_eq!(
            selected
                .handle(&SelectionKey::Name("slow".into()))
                .unwrap()
                .await_result()
                .await?,
            ParallelResult::Activity(json!("loser-value"))
        );
        Ok(json!("done"))
    });
    assert!(env.replay(&history).unwrap().completed);
}

#[tokio::test]
async fn incomplete_histories_and_new_unrecorded_commands_are_refused() {
    assert!(matches!(
        WorkflowHistory::from_history("test", (), json!({"events":[],"next_page_token":"more"})),
        Err(Error::Testing(TestingError::InvalidHistory(_)))
    ));
    let env = two_activities("alpha", "beta");
    let empty = WorkflowHistory::from_history("two", (), json!([])).unwrap();
    assert!(
        matches!(env.replay(&empty),Err(Error::NonDeterministicReplay(f)) if f.reason=="unrecorded_command" && f.sequence==Some(1))
    );
    let mut bounded = two_activities("alpha", "beta").max_steps(1);
    assert!(matches!(
        bounded.run_workflow("two", ()).await,
        Err(Error::Testing(TestingError::StepLimit(1)))
    ));
    assert!(bounded.last_history().is_some());
}

#[tokio::test]
async fn pending_history_and_terminal_result_changes_have_distinct_outcomes() {
    let mut original = two_activities("alpha", "beta");
    original.run_workflow("two", ()).await.unwrap();
    let mut export = original.last_history().unwrap().export();
    let events = export["history_events"].as_array_mut().unwrap();
    // Event positions are independent of durable workflow command sequences.
    for (index, event) in events.iter_mut().enumerate() {
        event["sequence"] = json!(900 + index);
    }
    let mut changed = WorkflowEnvironment::new().unwrap();
    changed.register_workflow("two", |ctx, _| async move {
        ctx.activity("alpha", json!([])).await?;
        ctx.activity("beta", json!([])).await?;
        Ok(json!("changed output"))
    });
    let history = WorkflowHistory::from_export(export.clone()).unwrap();
    assert!(
        matches!(changed.replay(&history),Err(Error::NonDeterministicReplay(f)) if f.reason=="workflow_result_mismatch" && f.sequence==Some(3))
    );
    let events = export["history_events"].as_array_mut().unwrap();
    events.truncate(2); // Started + first ActivityScheduled, still pending.
    let pending = WorkflowHistory::from_export(export).unwrap();
    let report = original.replay(&pending).unwrap();
    assert!(!report.completed);
    assert!(report.commands.is_empty());
}

#[tokio::test]
async fn redacted_payloads_new_local_commands_and_unsupported_simulations_are_refused() {
    let mut env = two_activities("alpha", "beta");
    env.run_workflow("two", ()).await.unwrap();
    let mut export = env.last_history().unwrap().export();
    export["redaction"] = json!({"paths":["payloads.arguments.data"]});
    assert!(matches!(
        WorkflowHistory::from_export(export),
        Err(Error::Testing(TestingError::InvalidHistory(_)))
    ));
    let mut export = env.last_history().unwrap().export();
    export["history_events"][2]["payload"]["result"] = json!(null);
    assert!(matches!(
        WorkflowHistory::from_export(export),
        Err(Error::Testing(TestingError::InvalidHistory(_)))
    ));
    let mut local = WorkflowEnvironment::new().unwrap();
    local.register_workflow("local", |ctx, _| async move {
        ctx.local_activity("a", ()).await
    });
    let empty = WorkflowHistory::from_history("local", (), json!([])).unwrap();
    assert!(
        matches!(local.replay(&empty),Err(Error::NonDeterministicReplay(f)) if f.reason=="unrecorded_command")
    );
    let mut timeout = WorkflowEnvironment::new().unwrap();
    timeout.register_workflow("timeout", |ctx, _| async move {
        ctx.activity_with_options(
            "a",
            ActivityOptions::new().start_to_close_timeout(Duration::from_secs(1)),
            (),
        )
        .await
    });
    assert!(matches!(
        timeout.run_workflow("timeout", ()).await,
        Err(Error::Testing(TestingError::UnsupportedCommand(_)))
    ));
}

#[tokio::test]
async fn typed_worker_registrations_and_lossless_avro_values_are_preserved() {
    let mut env = WorkflowEnvironment::new().unwrap();
    env.worker_mut()
        .register_typed_activity("typed", |_, value: u64| async move { Ok(value) });
    env.worker_mut()
        .register_typed_workflow("typed", |ctx, value: u64| async move {
            ctx.activity_typed::<_, u64>("typed", value).await
        });
    assert_eq!(
        env.run_workflow("typed", i64::MAX as u64).await.unwrap(),
        json!(i64::MAX)
    );
    assert!(env.replay(env.last_history().unwrap()).unwrap().completed);
    assert!(matches!(
        env.run_workflow("typed", u64::MAX).await,
        Err(Error::Codec(_))
    ));
    assert!(
        env.last_history().is_none(),
        "invalid new input must not expose the previous run as its history"
    );
    env.register_activity_avro_value("bytes", |_, args| async move {
        let durable_workflow::AvroValue::Array(mut values) = args else {
            panic!("normalized args");
        };
        Ok(values.remove(0))
    });
    let value = durable_workflow::AvroValue::Bytes(vec![0, 255, 128]);
    assert_eq!(
        env.run_activity_avro_value("bytes", value.clone())
            .await
            .unwrap(),
        value
    );
}

#[tokio::test]
async fn callback_routing_and_heartbeat_lifetime_match_the_authored_attempt() {
    let mut env = WorkflowEnvironment::new().unwrap();
    let retained = Arc::new(std::sync::Mutex::new(None));
    let save = retained.clone();
    env.register_activity("route", move |ctx, _| {
        let save = save.clone();
        async move {
            assert!(ctx.heartbeat(()).await?.heartbeat_recorded);
            let result = json!(ctx.task_queue);
            *save.lock().unwrap() = Some(ctx);
            Ok(result)
        }
    });
    env.register_workflow("route", |ctx, _| async move {
        ctx.activity_with_options("route", ActivityOptions::new().task_queue("custom"), ())
            .await
    });
    assert_eq!(
        env.run_workflow("route", ()).await.unwrap(),
        json!("custom")
    );
    let retained = retained.lock().unwrap().take().unwrap();
    assert!(!retained.heartbeat(()).await.unwrap().heartbeat_recorded);
}

#[tokio::test]
async fn metadata_updates_and_ordered_parallel_commands_replay_canonically() {
    let mut env = WorkflowEnvironment::new().unwrap();
    env.register_activity("echo", |_, args| async move { Ok(args[0].clone()) });
    env.register_workflow("parallel", |ctx, _| async move {
        ctx.upsert_memo(json!({"stage":"before"}))?;
        ctx.upsert_search_attributes(
            durable_workflow::SearchAttributeUpdate::new()
                .keyword("stage", "running")
                .unwrap(),
        )?;
        let values = ctx
            .parallel(vec![
                ParallelOperation::activity("echo", json!(["a"])),
                ParallelOperation::activity("echo", json!(["b"])),
            ])
            .await?;
        assert_eq!(
            values,
            vec![
                ParallelResult::Activity(json!("a")),
                ParallelResult::Activity(json!("b"))
            ]
        );
        ctx.upsert_memo(json!({"stage":null,"done":true}))?;
        Ok(json!("done"))
    });
    assert_eq!(
        env.run_workflow("parallel", ()).await.unwrap(),
        json!("done")
    );
    assert!(env.replay(env.last_history().unwrap()).unwrap().completed);
}
