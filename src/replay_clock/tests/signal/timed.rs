use super::*;

fn timed(sequence: u64, time: &str, parked: bool) -> Vec<HistoryEvent> {
    let mut history = signal(sequence, time);
    history[0].payload["timeout_seconds"] = json!(30);
    if parked {
        let payload = json!({"sequence":sequence,"signal_name":"ready",
            "signal_wait_id":format!("wait-{sequence}"),"timer_id":format!("timeout-{sequence}"),
            "timer_kind":"signal_timeout","delay_seconds":30,"fire_at":"2026-01-01T00:00:30Z"});
        let mut cancelled = payload.clone();
        cancelled["cancelled_at"] = json!(SIGNAL_TIME);
        history.insert(1, event("TimerScheduled", payload, START));
        history.insert(
            2,
            event("TimerCancelled", cancelled, "2099-01-01T00:00:00Z"),
        );
    }
    history
}

#[test]
fn replay_now_timed_signal_success_cold_replays_equal_and_backward_times() {
    for parked in [false, true] {
        for last in [SIGNAL_TIME, "2025-12-31T23:59:59.123456Z"] {
            let mut history = vec![start()];
            history.extend(timed(1, SIGNAL_TIME, parked));
            history.extend(timed(2, last, parked));
            let execute = || {
                let ctx = context(history.clone());
                let inner = ctx.clone();
                let result = ready(
                    &ctx,
                    Box::pin(async move {
                        assert_eq!(read(&inner), START);
                        assert_eq!(inner.wait_signal("ready").await?, vec![json!("delivery-1")]);
                        assert_eq!(read(&inner), SIGNAL_TIME);
                        assert_eq!(inner.wait_signal("ready").await?, vec![json!("delivery-2")]);
                        assert_eq!(read(&inner), read(&inner));
                        Ok(AvroValue::from_serialize(&time(&inner))?)
                    }),
                )
                .into_json()
                .unwrap();
                assert!(ctx.take_commands().unwrap().is_empty());
                assert!(ctx.replay_now().is_err());
                result
            };
            assert_eq!(execute(), json!(last));
            assert_eq!(execute(), execute());
        }
    }
    let mut immediate = timed(1, SIGNAL_TIME, false);
    immediate[0].payload["timeout_seconds"] = json!(0);
    assert_eq!(observe(immediate)["time"], json!(SIGNAL_TIME));
}

#[test]
fn replay_now_timed_signal_uses_applied_reuse_not_timer_or_future_time() {
    let mut history = timed(1, SIGNAL_TIME, true);
    history[3].payload["reused_recorded_at"] = json!("2025-12-01T00:00:00.654321Z");
    assert_eq!(
        observe(history.clone())["time"],
        json!("2025-12-01T00:00:00.654321Z")
    );
    history.extend(timed(2, "2099-01-01T00:00:00Z", true));
    assert_eq!(
        observe(history.clone())["time"],
        json!("2025-12-01T00:00:00.654321Z")
    );
    history[3].payload["reused_recorded_at"] = json!("invalid");
    assert!(observe(history)["time"].is_null());
}

#[test]
fn replay_now_timed_signal_timeout_metadata_must_be_numeric_and_consistent() {
    for bad in [json!(-1), json!(30.5), json!("30"), json!(true), json!({})] {
        let mut history = timed(1, SIGNAL_TIME, false);
        history[0].payload["timeout_seconds"] = bad;
        assert!(observe(history)["time"].is_null());
    }
    for bad in [Value::Null, json!(0), json!(1)] {
        let mut history = timed(1, SIGNAL_TIME, true);
        history[0].payload["timeout_seconds"] = bad;
        assert!(observe(history)["time"].is_null());
    }
    let mut history = timed(1, SIGNAL_TIME, false);
    history[1].payload["timeout_seconds"] = json!(31);
    assert!(observe(history)["time"].is_null());
}

#[test]
fn replay_now_timed_signal_requires_the_exact_timeout_pair() {
    for index in [1, 2] {
        for (key, bad) in [
            ("signal_wait_id", json!("foreign-wait")),
            ("signal_name", json!("foreign-signal")),
            ("timer_kind", json!("condition_timeout")),
            ("timer_id", json!("foreign-timer")),
            ("delay_seconds", json!(29)),
            ("delay_seconds", json!("30")),
            ("fire_at", json!("invalid")),
            ("fire_at", json!("2026-01-01T00:00:31Z")),
            ("sequence", json!(2)),
            ("sequence", json!("1")),
            ("workflow_sequence", json!(2)),
            ("parallel_group_id", json!("group")),
        ] {
            let mut history = timed(1, SIGNAL_TIME, true);
            history[index].payload[key] = bad;
            let output = observe(history);
            assert_eq!(output["arguments"], json!(["delivery-1"]));
            assert!(output["time"].is_null(), "index={index}, key={key}");
        }
        for key in [
            "sequence",
            "timer_id",
            "signal_wait_id",
            "signal_name",
            "delay_seconds",
            "fire_at",
        ] {
            let mut history = timed(1, SIGNAL_TIME, true);
            history[index].payload.as_object_mut().unwrap().remove(key);
            assert!(
                observe(history)["time"].is_null(),
                "index={index}, missing={key}"
            );
        }
    }
    for bad in [Value::Null, json!("invalid"), json!(42)] {
        let mut history = timed(1, SIGNAL_TIME, true);
        history[2].payload["cancelled_at"] = bad;
        assert!(observe(history)["time"].is_null());
    }
    let mut history = timed(1, SIGNAL_TIME, true);
    history[2].payload["cancellation_scope"] = json!({"scope_id":"scope"});
    assert!(observe(history)["time"].is_null());
}

#[test]
fn replay_now_timed_signal_ambiguous_fired_and_incomplete_timer_evidence_is_unavailable() {
    for remove in [1, 2] {
        let mut history = timed(1, SIGNAL_TIME, true);
        history.remove(remove);
        assert!(observe(history)["time"].is_null());
    }
    for duplicate in [1, 2] {
        let mut history = timed(1, SIGNAL_TIME, true);
        history.push(history[duplicate].clone());
        assert!(observe(history)["time"].is_null());
    }
    for (left, right) in [(0, 1), (1, 2), (2, 3)] {
        let mut history = timed(1, SIGNAL_TIME, true);
        history.swap(left, right);
        assert!(observe(history)["time"].is_null());
    }
    let mut history = timed(1, SIGNAL_TIME, true);
    history[2].event_type = "TimerFired".into();
    assert!(observe(history.clone())["time"].is_null());
    history.pop();
    let ctx = context(history);
    let inner = ctx.clone();
    let mut future: WorkflowFuture = Box::pin(async move {
        inner.wait_signal("ready").await?;
        panic!("a timeout is not successful signal application")
    });
    assert!(Worker::poll_workflow_future(&ctx, &mut future)
        .unwrap()
        .is_pending());
    let _guard = Guard::enter(&ctx);
    assert!(ctx.replay_now().is_err());
}

#[test]
fn replay_now_timed_signal_still_requires_the_canonical_applied_identity_and_value() {
    for index in [0, 3] {
        for key in ["signal_wait_id", "sequence"] {
            let mut history = timed(1, SIGNAL_TIME, true);
            history[index].payload.as_object_mut().unwrap().remove(key);
            if key == "sequence" {
                // Legacy parsing can use the outer counter. The new clock
                // still requires the explicit authored payload counter.
                history[index].raw.insert("sequence".into(), json!(1));
            }
            assert!(observe(history)["time"].is_null());
        }
    }
    let mut history = timed(1, SIGNAL_TIME, true);
    history[3].payload.as_object_mut().unwrap().remove("value");
    let output = observe(history);
    assert_eq!(output["arguments"], json!([]));
    assert!(output["time"].is_null());
}
