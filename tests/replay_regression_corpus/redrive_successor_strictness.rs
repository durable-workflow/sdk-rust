use super::*;

const REQUEST: &str = "CooperativeCancellationRequested";
const DELIVERY: &str = "CooperativeCancellationDelivered";

fn change_context(fixture: &mut Value, mut change: impl FnMut(&mut Value)) {
    for kind in [REQUEST, DELIVERY] {
        change(&mut history_event(fixture, kind)["payload"]["cancellation"]);
    }
}

fn activity(kind: &str, step: u64, id: &str) -> Value {
    successor_event(
        kind,
        "2026-11-03T00:05:01+00:00",
        json!({"sequence": step, "activity_type": "tests.second", "activity_execution_id": id}),
    )
}

fn add_activity_before_request(fixture: &mut Value, terminal: Option<Value>) {
    let history = fixture["history"].as_array_mut().unwrap();
    history.insert(4, activity("ActivityScheduled", 3, "second-execution"));
    if let Some(terminal) = terminal {
        history.insert(5, terminal);
    }
}

#[test]
fn redrive_strictness_refuses_parented_or_foreign_root_contexts() {
    for field in [
        "root_request_id",
        "root_workflow_instance_id",
        "root_workflow_run_id",
    ] {
        assert_successor_check_rejects(CANCELLATION_SUCCESSOR, "root", |fixture| {
            change_context(fixture, |context| {
                context["parent_request_id"] = json!("source-parent");
                context[field] = json!("foreign-root");
            });
        });
    }
    assert_successor_check_rejects(CANCELLATION_SUCCESSOR, "root", |fixture| {
        change_context(fixture, |context| {
            context.as_object_mut().unwrap().remove("parent_request_id");
        });
    });
}

#[test]
fn redrive_strictness_requires_only_the_successors_own_lineage() {
    assert_successor_check_rejects(CANCELLATION_SUCCESSOR, "lineage", |fixture| {
        change_context(fixture, |context| {
            context["lineage"].as_array_mut().unwrap().insert(
                0,
                json!({
                    "request_id": "source-request", "workflow_instance_id": "source-instance",
                    "workflow_run_id": SOURCE_RUN,
                }),
            );
        });
    });
}

#[test]
fn redrive_strictness_requires_core_written_request_and_delivery_fields() {
    for (kind, key) in [
        (REQUEST, "workflow_instance_id"),
        (REQUEST, "command_type"),
        (DELIVERY, "cancellation"),
    ] {
        assert_successor_check_rejects(CANCELLATION_SUCCESSOR, key, |fixture| {
            history_event(fixture, kind)["payload"]
                .as_object_mut()
                .unwrap()
                .remove(key);
        });
    }
}

#[test]
fn redrive_strictness_binds_context_schema_and_request_time() {
    for value in [
        Value::Null,
        json!("unknown/v1"),
        json!("durable-workflow.cancellation-context/v2"),
    ] {
        assert_successor_check_rejects(CANCELLATION_SUCCESSOR, "schema", |fixture| {
            change_context(fixture, |context| context["schema"] = value.clone());
        });
    }
    for value in [
        Value::Null,
        json!("2026-11-03T00:04:59Z"),
        json!("2026-11-03T00:05:02Z"),
    ] {
        assert_successor_check_rejects(CANCELLATION_SUCCESSOR, "requested_at", |fixture| {
            change_context(fixture, |context| context["requested_at"] = value.clone());
        });
    }
    for field in ["scope_origin", "scope_authority_deadline_at"] {
        assert_successor_check_rejects(CANCELLATION_SUCCESSOR, "schema", |fixture| {
            change_context(fixture, |context| context[field] = Value::Null);
        });
    }
}

#[test]
fn redrive_strictness_binds_requester_reason_source_and_deadline() {
    for requester in [
        json!({}),
        json!({"unexpected": "metadata"}),
        json!({"type": 4}),
    ] {
        assert_successor_check_rejects(CANCELLATION_SUCCESSOR, "requester", |fixture| {
            change_context(fixture, |context| context["requester"] = requester.clone());
        });
    }
    for field in ["source", "reason", "cleanup_deadline_at"] {
        assert_successor_check_rejects(CANCELLATION_SUCCESSOR, field, |fixture| {
            change_context(fixture, |context| context[field] = json!(7));
        });
    }
    assert_successor_check_rejects(CANCELLATION_SUCCESSOR, "reason", |fixture| {
        history_event(fixture, REQUEST)["payload"]["reason"] = json!("different reason");
    });
}

#[test]
fn redrive_strictness_refuses_non_single_call_deliveries() {
    for kind in ["unknown", "parallel", "selection_handle"] {
        assert_successor_check_rejects(CANCELLATION_SUCCESSOR, "call_kind", |fixture| {
            history_event(fixture, DELIVERY)["payload"]["call_kind"] = json!(kind);
        });
    }
    for field in [
        "sequence_span",
        "operation_sequence",
        "operation_sequence_span",
    ] {
        assert_successor_check_rejects(CANCELLATION_SUCCESSOR, "single call", |fixture| {
            history_event(fixture, DELIVERY)["payload"][field] = json!(1);
        });
    }
}

#[test]
fn redrive_strictness_refuses_deliveries_to_calls_resolved_before_request() {
    assert_successor_check_rejects(CANCELLATION_SUCCESSOR, "resolved before", |fixture| {
        later_step_cancellation(fixture, 3);
    });
}

#[test]
fn redrive_strictness_refuses_a_schedule_after_delivery() {
    assert_successor_check_rejects(CANCELLATION_SUCCESSOR, "successor has reached", |fixture| {
        fixture["history"]
            .as_array_mut()
            .unwrap()
            .push(activity("ActivityScheduled", 3, "late"));
        fixture["history"]
            .as_array_mut()
            .unwrap()
            .last_mut()
            .unwrap()["recorded_at"] = json!("2026-11-03T00:05:02+00:00");
    });
}

#[test]
fn redrive_strictness_refuses_unresolved_earlier_calls() {
    assert_successor_check_rejects(CANCELLATION_SUCCESSOR, "unresolved", |fixture| {
        add_activity_before_request(fixture, None);
        history_event(fixture, DELIVERY)["payload"]["sequence"] = json!(4);
    });
}

#[test]
fn redrive_strictness_requires_matching_prior_resolution_identity() {
    for (kind, step, id) in [
        ("ActivityCompleted", 3, "foreign"),
        ("ActivityCompleted", 4, "second-execution"),
        ("TimerFired", 3, "second-execution"),
    ] {
        assert_successor_check_rejects(CANCELLATION_SUCCESSOR, "unresolved", |fixture| {
            let terminal = if kind == "TimerFired" {
                successor_event(
                    kind,
                    "2026-11-03T00:05:01+00:00",
                    json!({"sequence": step, "timer_id": id}),
                )
            } else {
                activity(kind, step, id)
            };
            add_activity_before_request(fixture, Some(terminal));
            history_event(fixture, DELIVERY)["payload"]["sequence"] = json!(step.max(3) + 1);
        });
    }
}

#[test]
fn redrive_strictness_requires_typed_activity_cancelled_identity() {
    for field in ["sequence", "activity_type", "activity_execution_id"] {
        assert_successor_check_rejects(CANCELLATION_SUCCESSOR, field, |fixture| {
            let mut terminal = activity("ActivityCancelled", 3, "second-execution");
            terminal["payload"].as_object_mut().unwrap().remove(field);
            add_activity_before_request(fixture, Some(terminal));
            history_event(fixture, DELIVERY)["payload"]["sequence"] = json!(4);
        });
    }
}

#[test]
fn redrive_strictness_refuses_a_recorded_call_kind_mismatch() {
    assert_successor_check_rejects(CANCELLATION_SUCCESSOR, "recorded call", |fixture| {
        add_activity_before_request(fixture, None);
        history_event(fixture, DELIVERY)["payload"]["call_kind"] = json!("timer");
    });
}

#[test]
fn redrive_strictness_copied_timers_are_plain_timers() {
    for field in [
        "timer_kind",
        "cancellation_cleanup",
        "cancellation_scope_id",
        "parallel_group_id",
        "condition_wait_id",
        "signal_wait_id",
    ] {
        assert_successor_check_rejects(CANCELLATION_SUCCESSOR, "reused events", |fixture| {
            history_event(fixture, "TimerScheduled")["payload"][field] = json!("foreign-owner");
        });
        assert_successor_check_accepts(CANCELLATION_SUCCESSOR, |fixture| {
            history_event(fixture, "TimerScheduled")["payload"][field] = Value::Null;
        });
    }
}

#[test]
fn redrive_strictness_accepts_resolution_after_request_with_equal_times() {
    assert_successor_check_accepts(CANCELLATION_SUCCESSOR, |fixture| {
        add_activity_before_request(fixture, None);
        // The call was unresolved when the request was recorded. Equal wall
        // times do not change history-row ordering.
        fixture["history"]
            .as_array_mut()
            .unwrap()
            .insert(6, activity("ActivityCancelled", 3, "second-execution"));
    });
}

#[test]
fn redrive_strictness_accepts_the_next_typed_step_after_atomic_progress() {
    assert_successor_check_accepts(CANCELLATION_SUCCESSOR, |fixture| {
        fixture["history"].as_array_mut().unwrap().insert(
            4,
            successor_event(
                "MemoUpserted",
                "2026-11-03T00:05:01+00:00",
                json!({"sequence": 3}),
            ),
        );
        history_event(fixture, DELIVERY)["payload"]["sequence"] = json!(4);
    });
}

#[test]
fn redrive_strictness_refuses_duplicate_request_or_delivery() {
    for kind in [REQUEST, DELIVERY] {
        assert_successor_check_rejects(
            CANCELLATION_SUCCESSOR,
            "one request and one delivery",
            |fixture| {
                let event = history_event(fixture, kind).clone();
                let index = if kind == REQUEST { 5 } else { 6 };
                fixture["history"]
                    .as_array_mut()
                    .unwrap()
                    .insert(index, event);
            },
        );
    }
}
