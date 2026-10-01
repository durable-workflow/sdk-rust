use super::*;

fn request_observation() -> Value {
    json!({
        "request_id": "original-request",
        "requested_at": "2026-10-01T08:00:00Z",
        "cleanup_deadline_at": "2026-10-01T08:10:00Z",
        "history_refresh_page_token": "opaque-server-token"
    })
}

fn event(kind: &str, payload: Value) -> HistoryEvent {
    serde_json::from_value(json!({"event_type":kind, "payload":payload})).unwrap()
}

fn canonical_request() -> HistoryEvent {
    serde_json::from_value(json!({
        "event_type":"CooperativeCancellationRequested", "workflow_command_id":"original-request",
        "recorded_at":"2026-10-01T08:00:00.000120Z", "payload":{
            "workflow_command_id":"original-request", "workflow_run_id":"run",
            "cleanup_deadline_at":"2026-10-01T08:10:00Z"
        }
    }))
    .unwrap()
}

fn canonical_delivery(sequence: u64, kind: &str) -> HistoryEvent {
    event(
        "CooperativeCancellationDelivered",
        json!({
            "workflow_command_id":"original-request", "workflow_run_id":"run",
            "sequence":sequence, "call_kind":kind
        }),
    )
}

fn history(events: &[HistoryEvent]) -> Result<CancellationHistory> {
    CancellationHistory::from_events(events, "run", None)
}

fn cancellation_worker() -> Worker {
    Worker::new(
        Client::builder("http://127.0.0.1:1").build().unwrap(),
        "queue",
    )
}

fn cancellation_task(events: Vec<HistoryEvent>) -> WorkflowTask {
    let mut task = workflow_task("cancel", events, DEFAULT_CODEC);
    task.run_id = Some("run".into());
    task
}

fn scheduled_timer(sequence: u64) -> HistoryEvent {
    event(
        "TimerScheduled",
        json!({"sequence":sequence, "timer_id":format!("timer-{sequence}"), "delay_seconds":5}),
    )
}

fn completed_activity(sequence: u64, name: &str, value: Value) -> Vec<HistoryEvent> {
    vec![
        event(
            "ActivityScheduled",
            json!({"sequence":sequence,"activity_type":name,"task_queue":"queue"}),
        ),
        event(
            "ActivityCompleted",
            json!({"sequence":sequence,"activity_type":name,"result":fixture_envelope(value)}),
        ),
    ]
}

#[test]
fn cooperative_replay_preserves_completed_forward_result_and_saga_cleanup_identity() {
    for _cold_restart in 0..2 {
        let mut events = completed_activity(1, "forward", json!(7));
        events.extend([
            scheduled_timer(2),
            canonical_request(),
            canonical_delivery(2, "timer"),
        ]);
        events.extend(completed_activity(3, "undo", Value::Null));
        let mut worker = cancellation_worker();
        worker.register_workflow("cancel", |ctx, _input| async move {
            let result = ctx.activity("forward", json!([])).await?;
            assert_eq!(result, json!(7));
            assert!(!ctx.is_cancellation_requested()?);
            let mut saga = ctx.saga();
            saga.add_compensation("undo", json!([]))?;
            saga.finish(ctx.sleep(Duration::from_secs(5)).await).await?;
            Ok(Value::Null)
        });
        let commands = worker
            .execute_workflow_task(cancellation_task(events))
            .unwrap();
        assert_eq!(commands.len(), 1);
        assert_eq!(
            commands[0]["exception_type"],
            "WorkflowCancellationRequested"
        );
        assert_eq!(
            commands[0]["exception"]["properties"]["request_id"],
            "original-request"
        );
        assert_eq!(
            commands[0]["exception"]["properties"]["cleanup_deadline_at"],
            "2026-10-01T08:10:00Z"
        );
    }
}

#[test]
fn cooperative_replay_saga_schedules_cleanup_once_after_committed_delivery() {
    let mut events = completed_activity(1, "forward", json!(7));
    events.extend([
        scheduled_timer(2),
        canonical_request(),
        canonical_delivery(2, "timer"),
    ]);
    let mut worker = cancellation_worker();
    worker.register_workflow("cancel", |ctx, _input| async move {
        ctx.activity("forward", json!([])).await?;
        let mut saga = ctx.saga();
        saga.add_compensation("undo", json!([]))?;
        saga.finish(ctx.sleep(Duration::from_secs(5)).await).await?;
        Ok(Value::Null)
    });
    let commands = worker
        .execute_workflow_task(cancellation_task(events))
        .unwrap();
    assert_eq!(commands.len(), 1);
    assert_eq!(commands[0]["type"], "schedule_activity");
    assert_eq!(commands[0]["activity_type"], "undo");
}

#[test]
fn cooperative_replay_consumes_reopened_condition_once_at_its_physical_delivery() {
    let payload = |sequence| {
        json!({"sequence":sequence,"condition_wait_id":format!("condition:{sequence}"),
        "condition_wait_occurrence_id":"rust:condition-wait:0", "condition_key":"ready",
        "condition_definition_fingerprint":"sha256:ready"})
    };
    let events = vec![
        event("ConditionWaitOpened", payload(1)),
        event("ConditionWaitSatisfied", payload(1)),
        event("ConditionWaitOpened", payload(2)),
        canonical_request(),
        canonical_delivery(2, "condition"),
    ];
    for _cold_restart in 0..2 {
        let ctx = workflow_context(events.clone());
        let mut wait = Box::pin(
            ctx.wait_condition(ConditionWaitOptions::new("ready", "sha256:ready"), || {
                Ok(false)
            }),
        );
        let mut cx = TaskContext::from_waker(noop_waker_ref());
        let Poll::Ready(Err(Error::CooperativeCancellationRequested(cancellation))) =
            wait.as_mut().poll(&mut cx)
        else {
            panic!("reopened condition must receive canonical cancellation")
        };
        assert_eq!(cancellation.delivery.sequence, 2);
        assert_eq!(
            ctx.state.lock().unwrap().condition_wait_occurrence_counter,
            1
        );
        ctx.ensure_history_consumed().unwrap();
        assert!(ctx.take_commands().unwrap().is_empty());
    }
}

#[test]
fn cooperative_replay_keeps_adjacent_condition_occurrences_and_cleanup_scopes_separate() {
    let payload = |sequence, occurrence| {
        json!({"sequence":sequence,"condition_wait_id":format!("condition:{sequence}"),
        "condition_wait_occurrence_id":format!("rust:condition-wait:{occurrence}"), "condition_key":"ready",
        "condition_definition_fingerprint":"sha256:ready"})
    };
    let ctx = workflow_context(vec![
        event("ConditionWaitOpened", payload(1, 0)),
        event("ConditionWaitSatisfied", payload(1, 0)),
        event("ConditionWaitOpened", payload(2, 1)),
        canonical_request(),
        canonical_delivery(2, "condition"),
    ]);
    let shield = ctx.cancellation_shield().unwrap();
    let mut cx = TaskContext::from_waker(noop_waker_ref());
    let mut first = Box::pin(
        ctx.wait_condition(ConditionWaitOptions::new("ready", "sha256:ready"), || {
            Ok(false)
        }),
    );
    assert!(matches!(
        first.as_mut().poll(&mut cx),
        Poll::Ready(Ok(ConditionWaitResult::Satisfied))
    ));
    drop(shield);
    let mut second = Box::pin(
        ctx.wait_condition(ConditionWaitOptions::new("ready", "sha256:ready"), || {
            Ok(false)
        }),
    );
    assert!(matches!(
        second.as_mut().poll(&mut cx),
        Poll::Ready(Err(Error::CooperativeCancellationRequested(_)))
    ));
    assert_eq!(
        ctx.state.lock().unwrap().condition_wait_occurrence_counter,
        2
    );
    ctx.ensure_history_consumed().unwrap();
}

#[test]
fn cooperative_replay_marker_keeps_priority_over_resolution_committed_after_request() {
    let ctx = workflow_context(vec![
        scheduled_timer(1),
        canonical_request(),
        canonical_delivery(1, "timer"),
        event(
            "TimerFired",
            json!({"sequence":1,"timer_id":"timer-1","delay_seconds":5}),
        ),
    ]);
    let mut timer = Box::pin(ctx.sleep(Duration::from_secs(5)));
    let mut cx = TaskContext::from_waker(noop_waker_ref());
    assert!(matches!(
        timer.as_mut().poll(&mut cx),
        Poll::Ready(Err(Error::CooperativeCancellationRequested(_)))
    ));
    ctx.ensure_history_consumed().unwrap();
}

#[test]
fn cooperative_replay_actual_worker_delivers_scalar_calls_with_original_identity() {
    for kind in ["activity", "timer", "child", "signal", "condition"] {
        for _cold_restart in 0..2 {
            let mut worker = cancellation_worker();
            worker.register_workflow("cancel", move |ctx, _input| async move {
                match kind {
                    "activity" => {
                        ctx.activity("forward", json!([])).await?;
                    }
                    "timer" => {
                        ctx.sleep(Duration::from_secs(5)).await?;
                    }
                    "child" => {
                        ctx.start_child_workflow(
                            "child",
                            ChildWorkflowOptions::new("queue"),
                            json!([]),
                        )
                        .await?;
                    }
                    "signal" => {
                        ctx.wait_signal("resume").await?;
                    }
                    "condition" => {
                        ctx.wait_condition(
                            ConditionWaitOptions::new("ready", "sha256:ready"),
                            || Ok(false),
                        )
                        .await?;
                    }
                    _ => unreachable!(),
                }
                Ok(Value::Null)
            });
            let commands = worker
                .execute_workflow_task(cancellation_task(vec![
                    canonical_request(),
                    canonical_delivery(1, kind),
                ]))
                .unwrap();
            assert_eq!(commands.len(), 1, "{kind}");
            assert_eq!(commands[0]["type"], "fail_workflow", "{kind}");
            assert_eq!(
                commands[0]["exception_type"], "WorkflowCancellationRequested",
                "{kind}"
            );
            assert_eq!(
                commands[0]["exception"]["properties"]["request_id"], "original-request",
                "{kind}"
            );
            assert_eq!(
                commands[0]["exception"]["properties"]["cleanup_deadline_at"],
                "2026-10-01T08:10:00Z",
                "{kind}"
            );
            assert_eq!(commands[0]["non_retryable"], true, "{kind}");
        }
    }
}

#[test]
fn cooperative_replay_observed_request_and_task_flag_do_not_inject_without_delivery() {
    let mut worker = cancellation_worker();
    worker.register_workflow("cancel", |ctx, _input| async move {
        assert!(!ctx.is_cancellation_requested()?);
        ctx.throw_if_cancellation_requested()?;
        ctx.sleep(Duration::from_secs(5)).await?;
        Ok(Value::Null)
    });
    let mut task = cancellation_task(vec![scheduled_timer(1), canonical_request()]);
    task.cancel_requested = true;
    assert!(worker.execute_workflow_task(task).unwrap().is_empty());
}

#[test]
fn cooperative_replay_recorded_timer_validates_authored_details_before_delivery() {
    for delay in [5, 500] {
        let ctx = workflow_context(vec![
            scheduled_timer(1),
            canonical_request(),
            canonical_delivery(1, "timer"),
        ]);
        let mut timer = Box::pin(ctx.sleep(Duration::from_secs(delay)));
        let mut cx = TaskContext::from_waker(noop_waker_ref());
        match timer.as_mut().poll(&mut cx) {
            Poll::Ready(Err(Error::CooperativeCancellationRequested(cancellation)))
                if delay == 5 =>
            {
                assert_eq!(cancellation.request.request_id, "original-request");
                assert_eq!(cancellation.delivery.sequence, 1);
                ctx.ensure_history_consumed().unwrap();
            }
            Poll::Ready(Err(Error::NonDeterministicReplay(failure))) if delay == 500 => {
                assert_eq!(failure.reason, "timer_delay_mismatch");
            }
            other => panic!("{delay}: {other:?}"),
        }
        assert!(ctx.take_commands().unwrap().is_empty());
    }
}

#[test]
fn cooperative_replay_changed_call_kind_and_unconsumed_boundary_are_rejected() {
    let mut worker = cancellation_worker();
    worker.register_workflow("cancel", |ctx, _input| async move {
        ctx.activity("changed", json!([])).await?;
        Ok(Value::Null)
    });
    assert!(
        matches!(worker.execute_workflow_task(cancellation_task(vec![canonical_request(), canonical_delivery(1, "timer")])),
        Err(Error::NonDeterministicReplay(ReplayFailure { reason, .. })) if reason == "cooperative_cancellation_call_mismatch")
    );
    let mut worker = cancellation_worker();
    worker.register_workflow("cancel", |_ctx, _input| async { Ok(Value::Null) });
    assert!(
        matches!(worker.execute_workflow_task(cancellation_task(vec![canonical_request(), canonical_delivery(1, "timer")])),
        Err(Error::NonDeterministicReplay(ReplayFailure { reason, .. })) if reason == "recorded_commands_unconsumed")
    );
}

#[test]
fn cooperative_replay_rejects_a_marker_that_skips_unrecorded_authored_calls() {
    let mut worker = cancellation_worker();
    worker.register_workflow("cancel", |ctx, _input| async move {
        ctx.sleep(Duration::from_secs(5)).await?;
        Ok(Value::Null)
    });
    assert!(
        matches!(worker.execute_workflow_task(cancellation_task(vec![canonical_request(), canonical_delivery(999, "timer")])),
        Err(Error::NonDeterministicReplay(ReplayFailure { reason, .. })) if reason == "cooperative_cancellation_call_mismatch")
    );
}

#[test]
fn cooperative_replay_shields_nested_cleanup_and_preserves_original_request_after_drop() {
    let ctx = workflow_context(vec![canonical_request(), canonical_delivery(1, "timer")]);
    let mut timer = Box::pin(ctx.sleep(Duration::from_secs(5)));
    let mut cx = TaskContext::from_waker(noop_waker_ref());
    assert!(matches!(
        timer.as_mut().poll(&mut cx),
        Poll::Ready(Err(Error::CooperativeCancellationRequested(_)))
    ));
    assert!(ctx.is_cancellation_requested().unwrap());
    let outer = ctx.cancellation_shield().unwrap();
    let inner = ctx.cancellation_shield().unwrap();
    ctx.throw_if_cancellation_requested().unwrap();
    drop(inner);
    ctx.throw_if_cancellation_requested().unwrap();
    drop(outer);
    let Error::CooperativeCancellationRequested(cancellation) =
        ctx.throw_if_cancellation_requested().unwrap_err()
    else {
        panic!("original cancellation missing")
    };
    assert_eq!(cancellation.request.request_id, "original-request");
    assert_eq!(
        cancellation.request.cleanup_deadline_at,
        "2026-10-01T08:10:00Z"
    );
}

#[test]
fn cooperative_replay_cannot_shield_away_a_committed_authored_delivery() {
    let ctx = workflow_context(vec![canonical_request(), canonical_delivery(1, "timer")]);
    let _shield = ctx.cancellation_shield().unwrap();
    let mut timer = Box::pin(ctx.sleep(Duration::from_secs(5)));
    let mut cx = TaskContext::from_waker(noop_waker_ref());
    assert!(matches!(
        timer.as_mut().poll(&mut cx),
        Poll::Ready(Err(Error::NonDeterministicReplay(_)))
    ));
}

#[test]
fn cooperative_history_preserves_observation_without_authorizing_delivery() {
    let observation = CancellationRequest::from_observation(&request_observation()).unwrap();
    let state = CancellationHistory::from_events(&[canonical_request()], "run", Some(&observation))
        .unwrap();
    assert_eq!(state.request.unwrap(), observation);
    assert_eq!(state.delivery, None);
    let pending = history(&[canonical_request()]).unwrap();
    assert!(pending.eligible(1, 1));
    assert_eq!(pending.request.unwrap().history_refresh_page_token, None);
}

#[test]
fn cooperative_history_preserves_earlier_results_and_allows_later_unresolved_calls() {
    let state = history(&[
        event("ActivityCompleted", json!({"sequence":1})),
        canonical_request(),
        event("TimerFired", json!({"sequence":2})),
    ])
    .unwrap();
    assert!(!state.eligible(1, 1));
    assert!(state.eligible(2, 1));
    assert!(state.eligible(1, 2));
}

#[test]
fn cooperative_history_preserves_prior_parallel_failure_and_committed_selection() {
    for prior in [
        event("ActivityFailed", json!({"sequence":1})),
        event(
            "SelectionResolved",
            json!({
                "selection_group_base_sequence":1,"selection_group_size":2
            }),
        ),
    ] {
        let state = history(&[prior.clone(), canonical_request()]).unwrap();
        assert!(!state.eligible(1, 2));
        let mut marker = canonical_delivery(1, "parallel");
        marker.payload["sequence_span"] = json!(2);
        assert!(matches!(
            history(&[prior, canonical_request(), marker]),
            Err(Error::NonDeterministicReplay(_))
        ));
    }
}

#[test]
fn cooperative_history_preserves_cancelled_selection_member_ranges() {
    let state = history(&[
        event(
            "SelectionOperationCancelled",
            json!({"member_base_sequence":3,"member_size":2}),
        ),
        canonical_request(),
    ])
    .unwrap();
    assert!(!state.eligible(3, 2));
    assert!(state.eligible(4, 2));
}

#[test]
fn cooperative_history_cold_delivery_retains_parallel_and_selection_operation_ranges() {
    let mut parallel = canonical_delivery(2, "parallel");
    parallel.payload["sequence_span"] = json!(3);
    let state = history(&[canonical_request(), parallel]).unwrap();
    assert_eq!(state.request_index, 0);
    assert_eq!(state.delivery_index, Some(1));
    assert!(!state.eligible(5, 1));
    let delivery = state.delivery.unwrap();
    assert_eq!(
        (1..=5)
            .map(|sequence| delivery.interrupts(sequence))
            .collect::<Vec<_>>(),
        vec![false, true, true, true, false]
    );
    let mut selected = canonical_delivery(6, "selection_handle");
    selected.payload["operation_sequence"] = json!(2);
    selected.payload["operation_sequence_span"] = json!(2);
    let delivered = history(&[canonical_request(), selected])
        .unwrap()
        .delivery
        .unwrap();
    assert!(delivered.interrupts(2));
    assert!(delivered.interrupts(3));
    assert!(!delivered.interrupts(4));
    assert!(!delivered.interrupts(6));
}

#[test]
fn cooperative_history_rejects_reordered_duplicate_and_mismatched_markers() {
    for events in [
        vec![canonical_delivery(2, "timer")],
        vec![canonical_request(), canonical_request()],
        vec![
            canonical_request(),
            canonical_delivery(2, "timer"),
            canonical_delivery(2, "timer"),
        ],
        vec![canonical_delivery(2, "timer"), canonical_request()],
    ] {
        assert!(matches!(
            history(&events),
            Err(Error::NonDeterministicReplay(_))
        ));
    }
    for (field, value) in [
        ("workflow_command_id", json!("different")),
        ("workflow_run_id", json!("different")),
        ("workflow_command_id", json!(" ")),
        ("workflow_run_id", Value::Null),
    ] {
        let mut marker = canonical_delivery(2, "timer");
        marker.payload[field] = value;
        assert!(matches!(
            history(&[canonical_request(), marker]),
            Err(Error::NonDeterministicReplay(_))
        ));
    }
    let mut request = canonical_request();
    request
        .raw
        .insert("workflow_command_id".into(), json!("different"));
    assert!(matches!(
        history(&[request]),
        Err(Error::NonDeterministicReplay(_))
    ));
}

#[test]
fn cooperative_history_rejects_changed_observation_identity_deadline_and_malformed_time() {
    for (field, value) in [
        ("request_id", json!("different")),
        ("cleanup_deadline_at", json!("2026-10-01T08:11:00Z")),
        ("requested_at", json!("no timestamp")),
        ("history_refresh_page_token", json!(" ")),
    ] {
        let mut observation = request_observation();
        observation[field] = value;
        match CancellationRequest::from_observation(&observation) {
            Ok(observed) => assert!(matches!(
                CancellationHistory::from_events(&[canonical_request()], "run", Some(&observed)),
                Err(Error::NonDeterministicReplay(_))
            )),
            Err(error) => assert!(matches!(error, Error::InvalidCooperativeCancellation(_))),
        }
    }
    let mut request = canonical_request();
    request.raw.insert("recorded_at".into(), json!("bad"));
    assert!(matches!(
        history(&[request]),
        Err(Error::NonDeterministicReplay(_))
    ));
}

#[test]
fn cooperative_history_accepts_equivalent_deadline_timezone_without_changing_original_text() {
    let mut observation = request_observation();
    observation["cleanup_deadline_at"] = json!("2026-10-01T10:10:00+02:00");
    let observed = CancellationRequest::from_observation(&observation).unwrap();
    let state =
        CancellationHistory::from_events(&[canonical_request()], "run", Some(&observed)).unwrap();
    assert_eq!(state.request.unwrap(), observed);
}

#[test]
fn cooperative_history_rejects_invalid_call_ranges_and_portable_integer_overflow() {
    for (field, value) in [
        ("sequence", json!(0)),
        ("sequence", json!(true)),
        ("sequence", json!("2")),
        ("sequence", json!(i64::MAX)),
        ("sequence", json!(u64::MAX)),
        ("call_kind", json!("unknown")),
        ("sequence_span", json!(2)),
        ("sequence_span", Value::Null),
        ("operation_sequence", json!(1)),
        ("operation_sequence_span", json!(2)),
    ] {
        let mut marker = canonical_delivery(2, "timer");
        marker.payload[field] = value;
        assert!(matches!(
            history(&[canonical_request(), marker]),
            Err(Error::NonDeterministicReplay(_))
        ));
    }
    for (base, span) in [(0, 1), (6, 1), (5, 2), (2, 1001)] {
        let mut selected = canonical_delivery(6, "selection_handle");
        selected.payload["operation_sequence"] = json!(base);
        selected.payload["operation_sequence_span"] = json!(span);
        assert!(matches!(
            history(&[canonical_request(), selected]),
            Err(Error::NonDeterministicReplay(_))
        ));
    }
    let mut parallel = canonical_delivery(2, "parallel");
    parallel.payload["sequence_span"] = json!(1001);
    assert!(matches!(
        history(&[canonical_request(), parallel]),
        Err(Error::NonDeterministicReplay(_))
    ));
    assert!(!history(&[canonical_request()])
        .unwrap()
        .eligible(u64::MAX, 1));
}

#[test]
fn cooperative_history_rejects_delivery_over_an_earlier_resolved_call() {
    for kind in [
        "ActivityCompleted",
        "ActivityFailed",
        "ActivityCancelled",
        "ActivityTimedOut",
        "TimerFired",
        "TimerCancelled",
        "ConditionWaitSatisfied",
        "ConditionWaitTimedOut",
        "SignalApplied",
        "ChildRunCompleted",
        "ChildRunFailed",
        "ChildRunCancelled",
        "ChildRunTerminated",
    ] {
        assert!(
            matches!(
                history(&[
                    event(kind, json!({"sequence":2})),
                    canonical_request(),
                    canonical_delivery(2, "timer")
                ]),
                Err(Error::NonDeterministicReplay(_))
            ),
            "{kind}"
        );
    }
}

fn responses(path: &str, _body: &str, number: usize) -> Option<(&'static str, String)> {
    let case = path.split('/').nth(1).unwrap_or_default();
    if path.ends_with("/api/cluster/info") {
        if case == "discovery-http" {
            return Some(("403 Forbidden", r#"{"reason":"forbidden"}"#.into()));
        }
        if case == "budget" {
            thread::sleep(Duration::from_millis(2500));
        }
        let mut discovery = json!({"worker_protocol": {
            "version": "1.20", "server_capabilities": {"cooperative_cancellation": true}
        }});
        match case {
            "unsupported" => {
                discovery["worker_protocol"]["server_capabilities"]["cooperative_cancellation"] =
                    json!(false)
            }
            "missing" => discovery = json!({}),
            "string-flag" => {
                discovery["worker_protocol"]["server_capabilities"]["cooperative_cancellation"] =
                    json!("true")
            }
            "old" => discovery["worker_protocol"]["version"] = json!("1.19"),
            "future-major" => discovery["worker_protocol"]["version"] = json!("2.20"),
            "malformed-version" => discovery["worker_protocol"]["version"] = json!("1.+20"),
            "new-minor" => discovery["worker_protocol"]["version"] = json!("1.21"),
            _ => {}
        }
        return Some(("200 OK", discovery.to_string()));
    }
    if !path.ends_with("/request-cancellation") {
        return None;
    }
    if case == "refused" {
        return Some((
            "409 Conflict",
            r#"{"reason":"active_claim_cancellation_not_supported"}"#.into(),
        ));
    }
    if case == "budget" {
        thread::sleep(Duration::from_secs(3));
    }
    let mut response = json!({
        "accepted": true, "duplicate": number > 1,
        "workflow_id": "workflow", "run_id": "run",
        "cancellation_request": request_observation()
    });
    match case {
        "escaped" => {
            response["workflow_id"] = json!("workflow/with?path");
            response["run_id"] = json!("run/selected");
        }
        "wrong-workflow" => response["workflow_id"] = json!("other"),
        "wrong-run" => response["run_id"] = json!("other"),
        "empty-run" => response["run_id"] = json!(" "),
        "not-accepted" => response["accepted"] = json!(false),
        "accepted-string" => response["accepted"] = json!("true"),
        "duplicate-integer" => response["duplicate"] = json!(0),
        "missing-request" => response["cancellation_request"] = Value::Null,
        "blank-request" => response["cancellation_request"]["request_id"] = json!(" "),
        "blank-token" => {
            response["cancellation_request"]["history_refresh_page_token"] = json!(" ")
        }
        "no-timezone" => {
            response["cancellation_request"]["requested_at"] = json!("2026-10-01T08:00:00")
        }
        "invalid-date" => {
            response["cancellation_request"]["requested_at"] = json!("2026-02-30T08:00:00Z")
        }
        "equal-deadline" => {
            response["cancellation_request"]["cleanup_deadline_at"] = json!("2026-10-01T08:00:00Z")
        }
        "earlier-deadline" => {
            response["cancellation_request"]["cleanup_deadline_at"] = json!("2026-10-01T07:59:00Z")
        }
        _ => {}
    }
    Some(("202 Accepted", response.to_string()))
}

fn server() -> MockWorkerServer {
    MockWorkerServer::start_with_behavior(MockWorkerBehavior {
        request_override: Some(responses),
        ..MockWorkerBehavior::default()
    })
}

fn client(server: &MockWorkerServer, case: &str) -> Client {
    Client::builder(format!("{}/{case}", server.base_url()))
        .control_token(Some("control-only".to_string()))
        .worker_token(Some("worker-only".to_string()))
        .namespace("caller-namespace")
        .build()
        .unwrap()
}

#[tokio::test]
async fn cooperative_request_uses_control_role_namespace_and_preserves_server_identity() {
    let server = server();
    let client = client(&server, "valid");
    let options = CooperativeCancellationOptions {
        reason: Some("caller stopped".into()),
        cleanup_timeout_seconds: Some(60),
    };
    let first = client
        .request_workflow_cancellation("workflow", options)
        .await
        .unwrap();
    let repeated = client
        .request_workflow_cancellation(
            "workflow",
            CooperativeCancellationOptions {
                reason: Some("changed reason".into()),
                cleanup_timeout_seconds: Some(120),
            },
        )
        .await
        .unwrap();
    assert!(!first.duplicate);
    assert!(repeated.duplicate);
    assert_eq!(first.cancellation_request, repeated.cancellation_request);
    assert_eq!(first.cancellation_request.request_id, "original-request");
    assert_eq!(
        first.cancellation_request.cleanup_deadline_at,
        "2026-10-01T08:10:00Z"
    );
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 4);
    for request in requests.iter() {
        assert_eq!(
            request.authorization.as_deref(),
            Some("Bearer control-only")
        );
        assert_eq!(request.namespace.as_deref(), Some("caller-namespace"));
        assert_eq!(request.control_protocol.as_deref(), Some("2"));
        assert_eq!(request.worker_protocol, None);
    }
    assert_eq!(requests[0].method, "GET");
    assert_eq!(requests[1].method, "POST");
    assert_eq!(
        serde_json::from_str::<Value>(&requests[1].body).unwrap(),
        json!({
            "reason":"caller stopped", "cleanup_timeout_seconds":60
        })
    );
}

#[tokio::test]
async fn cooperative_request_refuses_missing_unsupported_or_incompatible_discovery_before_mutation()
{
    for case in [
        "unsupported",
        "missing",
        "string-flag",
        "old",
        "future-major",
        "malformed-version",
    ] {
        let server = server();
        let result = client(&server, case)
            .request_workflow_cancellation("workflow", Default::default())
            .await;
        assert!(
            matches!(result, Err(Error::CooperativeCancellationUnavailable(_))),
            "{case}: {result:?}"
        );
        assert_eq!(server.requests.lock().unwrap().len(), 1, "{case}");
    }
}

#[tokio::test]
async fn cooperative_request_accepts_additive_minor_discovery_without_changing_worker_default() {
    let server = server();
    client(&server, "new-minor")
        .request_workflow_cancellation("workflow", Default::default())
        .await
        .unwrap();
    assert_eq!(WORKER_PROTOCOL_VERSION, "1.19");
    let ordinary = Client::builder(server.base_url()).build().unwrap();
    let mut worker = Worker::new(ordinary, "queue");
    worker.register_workflow("example", |_ctx, _input| async { Ok(Value::Null) });
    worker.register().await.unwrap();
    let requests = server.requests.lock().unwrap();
    let registration = requests
        .iter()
        .find(|request| request.path.ends_with("/worker/register"))
        .unwrap();
    assert_eq!(registration.worker_protocol.as_deref(), Some("1.19"));
    let body: Value = serde_json::from_str(&registration.body).unwrap();
    assert!(!body["capabilities"]
        .as_array()
        .unwrap()
        .contains(&json!("cooperative_cancellation")));
}

#[tokio::test]
async fn cooperative_request_validates_selected_run_and_encodes_path_segments() {
    let server = server();
    let result = client(&server, "escaped")
        .request_workflow_run_cancellation("workflow/with?path", "run/selected", Default::default())
        .await
        .unwrap();
    assert_eq!(result.run_id, "run/selected");
    assert_eq!(server.request_count("/escaped/api/workflows/workflow%2Fwith%3Fpath/runs/run%2Fselected/request-cancellation"), 1);
}

#[tokio::test]
async fn cooperative_request_rejects_malformed_or_mismatched_acknowledgments() {
    for case in [
        "wrong-workflow",
        "wrong-run",
        "empty-run",
        "not-accepted",
        "accepted-string",
        "duplicate-integer",
        "missing-request",
        "blank-request",
        "blank-token",
        "no-timezone",
        "invalid-date",
        "equal-deadline",
        "earlier-deadline",
    ] {
        let server = server();
        let result = client(&server, case)
            .request_workflow_run_cancellation("workflow", "run", Default::default())
            .await;
        assert!(
            matches!(result, Err(Error::InvalidCooperativeCancellation(_))),
            "{case}: {result:?}"
        );
    }
}

#[tokio::test]
async fn cooperative_request_rejects_invalid_inputs_without_network_activity() {
    let server = server();
    let client = client(&server, "valid");
    for options in [
        CooperativeCancellationOptions {
            cleanup_timeout_seconds: Some(0),
            ..Default::default()
        },
        CooperativeCancellationOptions {
            cleanup_timeout_seconds: Some(3601),
            ..Default::default()
        },
        CooperativeCancellationOptions {
            reason: Some("x".repeat(1001)),
            ..Default::default()
        },
    ] {
        assert!(matches!(
            client
                .request_workflow_cancellation("workflow", options)
                .await,
            Err(Error::InvalidCooperativeCancellation(_))
        ));
    }
    assert!(matches!(
        client
            .request_workflow_cancellation(" ", Default::default())
            .await,
        Err(Error::InvalidCooperativeCancellation(_))
    ));
    assert!(matches!(
        client
            .request_workflow_run_cancellation("workflow", " ", Default::default())
            .await,
        Err(Error::InvalidCooperativeCancellation(_))
    ));
    assert!(server.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn cooperative_request_does_not_substitute_worker_credentials() {
    let server = server();
    let client = Client::builder(server.base_url())
        .worker_token(Some("worker-only".into()))
        .build()
        .unwrap();
    assert!(matches!(
        client
            .request_workflow_cancellation("workflow", Default::default())
            .await,
        Err(Error::MissingRoleCredentials {
            role: "control",
            ..
        })
    ));
    assert!(server.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn cooperative_request_preserves_discovery_and_active_claim_refusals() {
    for (case, expected) in [
        ("discovery-http", reqwest::StatusCode::FORBIDDEN),
        ("refused", reqwest::StatusCode::CONFLICT),
    ] {
        let server = server();
        let error = client(&server, case)
            .request_workflow_cancellation("workflow", Default::default())
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Http {status, ..} if status==expected));
        assert_eq!(
            server.requests.lock().unwrap().len(),
            if case == "discovery-http" { 1 } else { 2 }
        );
    }
}

#[tokio::test]
async fn cooperative_request_handles_keep_current_and_selected_run_routing_separate() {
    let server = server();
    let handle = WorkflowHandle {
        client: client(&server, "valid"),
        workflow_id: "workflow".into(),
        run_id: Some("run".into()),
        workflow_type: "example".into(),
    };
    handle
        .request_cancellation(Default::default())
        .await
        .unwrap();
    handle
        .request_selected_run_cancellation(Default::default())
        .await
        .unwrap();
    assert_eq!(
        server.request_count("/valid/api/workflows/workflow/request-cancellation"),
        1
    );
    assert_eq!(
        server.request_count("/valid/api/workflows/workflow/runs/run/request-cancellation"),
        1
    );
    let missing = WorkflowHandle {
        run_id: None,
        ..handle
    };
    assert!(matches!(
        missing
            .request_selected_run_cancellation(Default::default())
            .await,
        Err(Error::InvalidCooperativeCancellation(_))
    ));
    assert_eq!(server.requests.lock().unwrap().len(), 4);
}

#[tokio::test]
async fn cooperative_request_bounds_discovery_and_mutation_with_one_total_budget() {
    let server = server();
    let started = Instant::now();
    let result = client(&server, "budget")
        .request_workflow_cancellation("workflow", Default::default())
        .await;
    assert!(matches!(result, Err(Error::Timeout)), "{result:?}");
    assert!(started.elapsed() >= Duration::from_secs(4));
    assert!(
        started.elapsed() < Duration::from_secs(7),
        "total budget exceeded: {:?}",
        started.elapsed()
    );
    assert_eq!(server.requests.lock().unwrap().len(), 2);
}
