use super::*;
use crate::cancellation_scope_history::CommittedCancellationScopeHistory;

fn scalar_fixture() -> Value {
    fixture("empty", "unshielded")
}

fn boundary(value: &Value) -> (ScopedCancellationContext, CancellationDelivery) {
    let committed = read(value).unwrap();
    let prepared = committed.preparations.values().next().unwrap();
    (prepared.context.clone(), prepared.boundary.clone())
}

fn claim(value: &Value) -> WorkflowTask {
    let mut events = value["history"].as_array().unwrap().clone();
    let position = events
        .iter()
        .position(|row| row["event_type"] == "CancellationScopeDeliveryPrepared")
        .unwrap();
    events.truncate(position);
    serde_json::from_value(json!({"task_id":"task/one", "run_id":value["task"]["run_id"],
        "workflow_id":value["task"]["workflow_id"], "workflow_type":"scope", "lease_owner":"original",
        "workflow_task_attempt":4, "payload_codec":DEFAULT_CODEC, "history_events":events})).unwrap()
}

fn scope_response(path: &str, body: &str, number: usize) -> Option<(&'static str, String)> {
    let case = path.split('/').nth(1).unwrap_or_default();
    if case == "budget" {
        thread::sleep(Duration::from_millis(900));
    }
    if matches!(case, "lost-ack" | "worker-lost-ack") && path.ends_with("/prepare") && number == 1 {
        return Some(("invalid-status", String::new()));
    }
    let mut source = match case {
        "group-flat" => fixture("groups", "flat"),
        "group-nested" => fixture("groups", "nested"),
        _ => scalar_fixture(),
    };
    if case.starts_with("worker") {
        // Synthetic transport time is safely ahead of the actual request budget.
        // The fixture has no admitted members whose descriptor contains dates.
        source =
            serde_json::from_str(&source.to_string().replace("2026-10-04", "2029-10-04")).unwrap();
        if path.ends_with("/poll") {
            let task = claim(&source);
            return Some(("200 OK", json!({"protocol_version":"1.20", "task":{
                "task_id":task.task_id, "run_id":task.run_id, "workflow_id":task.workflow_id,
                "workflow_type":task.workflow_type, "lease_owner":task.lease_owner,
                "workflow_task_attempt":task.workflow_task_attempt, "payload_codec":DEFAULT_CODEC,
                "history_events":source["history"].as_array().unwrap().iter()
                    .take_while(|event| event["event_type"] != "CancellationScopeDeliveryPrepared")
                    .cloned().collect::<Vec<_>>()
            }}).to_string()));
        }
        if path.ends_with("/complete") {
            return Some(("200 OK", "{}".into()));
        }
    }
    let delivering =
        path.ends_with("/deliver") || (path.ends_with("/history") && body.contains("deliver-"));
    if case == "substituted" && delivering {
        event_mut(&mut source, "CancellationScopeDeliveryPrepared")["id"] =
            json!("borrowed-preparation");
        event_mut(&mut source, "CancellationScopeDelivered")["payload"]
            ["preparation_history_event_id"] = json!("borrowed-preparation");
    }
    let mut prepared = event_mut(&mut source, "CancellationScopeDeliveryPrepared").clone();
    let delivered = event_mut(&mut source, "CancellationScopeDelivered").clone();
    let mut result;
    if path.ends_with("/prepare") || path.ends_with("/deliver") {
        result = prepared["payload"].clone();
        result["task_id"] = json!("task/one");
        result["lease_owner"] = json!("original");
        result["workflow_task_attempt"] = json!(4);
        result["prepared"] = json!(true);
        result["delivered"] = json!(delivering);
        result["claim_released"] = json!(false);
        result["created_task_ids"] = json!([]);
        result["reason"] = Value::Null;
        result["preparation_history_event_id"] = prepared["id"].clone();
        result["history_event_id"] = if delivering {
            delivered["id"].clone()
        } else {
            prepared["id"].clone()
        };
        result["history_refresh_page_token"] = json!(if delivering {
            "deliver-start"
        } else {
            "prepare-start"
        });
        match case {
            "ack-owner" => result["lease_owner"] = json!("replacement"),
            "ack-attempt" => result["workflow_task_attempt"] = json!(true),
            "ack-run" => result["workflow_run_id"] = json!("foreign"),
            "ack-boundary" => result["sequence"] = json!(99),
            "ack-range" => result["sequence_span"] = json!(2),
            "ack-prepared" => result["prepared"] = json!(1),
            "ack-delivered" => result["delivered"] = json!(!delivering),
            "ack-released" => result["claim_released"] = json!(true),
            "ack-new-task" => result["created_task_ids"] = json!(["new"]),
            "ack-deadline" => {
                result["authority_deadline_at"] = json!("2026-10-04T00:00:31.123456Z")
            }
            "ack-token" => result["history_refresh_page_token"] = Value::Null,
            "ack-event" => result["history_event_id"] = json!(""),
            "ack-reason" => {
                result.as_object_mut().unwrap().remove("reason");
            }
            _ => {}
        }
    } else if path.ends_with("/history") {
        let events = source["history"].as_array_mut().unwrap();
        if !delivering {
            events.pop();
        }
        match case {
            "history-namespace" => events[0]["namespace"] = json!("foreign"),
            "history-start" => events[0]["payload"]["workflow_instance_id"] = json!("foreign"),
            "history-context" => {
                events[4]["payload"]["cancellation"]["root_context"]["reason"] = json!("changed")
            }
            "history-range" => {
                prepared["payload"]
                    .as_object_mut()
                    .unwrap()
                    .remove("operation_sequence_span");
                *events.last_mut().unwrap() = prepared;
            }
            "history-preparation" => {
                events.pop();
            }
            _ => {}
        }
        let request: Value = serde_json::from_str(body).unwrap();
        let first = request["next_history_page_token"]
            .as_str()
            .unwrap()
            .ends_with("start");
        let prefix = if delivering { "deliver" } else { "prepare" };
        result = json!({"task_id":"task/one", "workflow_task_attempt":4,
            "history_events": if first { events.iter().take(2).cloned().collect::<Vec<_>>() }
                else { events.iter().skip(2).cloned().collect::<Vec<_>>() },
            "next_history_page_token": if first { json!(format!("{prefix}-next")) } else { Value::Null }});
        match case {
            "page-task" => result["task_id"] = json!("foreign"),
            "page-attempt" => result["workflow_task_attempt"] = json!(5),
            "page-cycle" => {
                result["next_history_page_token"] = request["next_history_page_token"].clone()
            }
            "page-terminal" => {
                result
                    .as_object_mut()
                    .unwrap()
                    .remove("next_history_page_token");
            }
            _ => {}
        }
    } else {
        return None;
    }
    Some(("200 OK", result.to_string()))
}

fn scope_server() -> MockWorkerServer {
    MockWorkerServer::start_with_behavior(MockWorkerBehavior {
        request_override: Some(scope_response),
        ..MockWorkerBehavior::default()
    })
}

fn scope_client(server: &MockWorkerServer, case: &str) -> Client {
    Client::builder(format!("{}/{case}", server.base_url()))
        .namespace("sdk-scope-fixture")
        .control_token(Some("control-only".into()))
        .worker_token(Some("worker-only".into()))
        .build()
        .unwrap()
}

#[tokio::test]
async fn cancellation_scope_replay_worker_coordinates_original_claim_and_lost_ack() {
    for case in ["worker", "worker-lost-ack"] {
        let server = scope_server();
        let cleanup = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&cleanup);
        let mut worker = Worker::new(scope_client(&server, case), "queue")
            .worker_id("original")
            .poll_timeout(Duration::ZERO)
            .cooperative_cancellation(true)
            .candidate_cancellation_scope_authoring(true)
            .candidate_cancellation_scope_delivery(true);
        worker.register_workflow("scope", move |ctx, _| {
            let observed = Arc::clone(&observed);
            async move {
                ctx.cancellation_scope(false, move |outer| async move {
                    outer
                        .cancellation_scope(false, move |inner| async move {
                            match inner.sleep(Duration::from_secs(3600)).await {
                                Err(Error::CancellationScopeRequested(cancellation)) => {
                                    observed.fetch_add(1, Ordering::SeqCst);
                                    assert_eq!(
                                        cancellation.context.remaining()?,
                                        Duration::from_micros(22_623_456)
                                    );
                                    let _shield = inner.cancellation_shield()?;
                                    inner.sleep(Duration::from_secs(1)).await?;
                                    Ok(Value::Null)
                                }
                                Err(error) => Err(error),
                                Ok(()) => panic!("cancelled timer cannot complete ordinarily"),
                            }
                        })
                        .await
                })
                .await
            }
        });
        assert_eq!(
            worker.poll_workflow_once().await.unwrap(),
            ManagedPollOutcome::Handled
        );
        assert_eq!(cleanup.load(Ordering::SeqCst), 1);
        let requests = server.requests.lock().unwrap();
        let preparations: Vec<_> = requests
            .iter()
            .filter(|request| request.path.ends_with("/prepare"))
            .collect();
        assert_eq!(
            preparations.len(),
            if case == "worker-lost-ack" { 2 } else { 1 }
        );
        if preparations.len() == 2 {
            assert_eq!(preparations[0].body, preparations[1].body);
        }
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.path.ends_with("/deliver"))
                .count(),
            1
        );
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.path.ends_with("/history"))
                .count(),
            4
        );
        let completion = requests.last().unwrap();
        assert_eq!(
            completion.path,
            format!("/{case}/api/worker/workflow-tasks/task/one/complete")
        );
        let body: Value = serde_json::from_str(&completion.body).unwrap();
        assert_eq!(body["workflow_task_attempt"], 4);
        assert_eq!(body["lease_owner"], "original");
        assert_eq!(body["commands"][0]["type"], "start_timer");
        assert_eq!(body["commands"][0]["delay_seconds"], 1);
    }
}

#[tokio::test]
async fn scope_history_prepare_and_delivery_prove_original_claim_and_all_pages_after_lost_ack() {
    for case in ["valid", "lost-ack", "group-flat", "group-nested"] {
        let server = scope_server();
        let client = scope_client(&server, case);
        let value = match case {
            "group-flat" => fixture("groups", "flat"),
            "group-nested" => fixture("groups", "nested"),
            _ => scalar_fixture(),
        };
        let task = claim(&value);
        let (context, boundary) = boundary(&value);
        let budget = CancellationScopeDeliveryBudget::new();
        let prepared = client
            .prepare_cancellation_scope_on_claim(&task, &context, &boundary, &budget)
            .await
            .unwrap();
        assert!(prepared.delivery_history_event_id().is_none());
        let delivered = client
            .deliver_cancellation_scope_on_claim(&task, &prepared, &budget)
            .await
            .unwrap();
        assert_eq!(
            prepared.preparation_history_event_id(),
            delivered.preparation_history_event_id()
        );
        assert_ne!(
            delivered.delivery_history_event_id().unwrap(),
            delivered.preparation_history_event_id()
        );
        assert_eq!(delivered.context(), &context);
        assert_eq!(delivered.boundary(), &boundary);
        assert_eq!(
            delivered.history().len(),
            value["history"].as_array().unwrap().len()
        );
        let requests = server.requests.lock().unwrap();
        let preparations: Vec<_> = requests
            .iter()
            .filter(|row| row.path.ends_with("/prepare"))
            .collect();
        assert_eq!(preparations.len(), if case == "lost-ack" { 2 } else { 1 });
        if case == "lost-ack" {
            assert_eq!(preparations[0].body, preparations[1].body);
        }
        for request in requests.iter() {
            assert_eq!(request.worker_protocol.as_deref(), Some("1.20"));
            assert_eq!(request.authorization.as_deref(), Some("Bearer worker-only"));
            assert_eq!(request.namespace.as_deref(), Some("sdk-scope-fixture"));
            assert!(request.path.contains("/task%2Fone/"));
            let body: Value = serde_json::from_str(&request.body).unwrap();
            assert_eq!(body["lease_owner"], "original");
            assert_eq!(body["workflow_task_attempt"], 4);
        }
    }
}

#[tokio::test]
async fn scope_history_changed_receipts_refuse_before_history_reads() {
    for case in [
        "ack-owner",
        "ack-attempt",
        "ack-run",
        "ack-boundary",
        "ack-range",
        "ack-prepared",
        "ack-delivered",
        "ack-released",
        "ack-new-task",
        "ack-deadline",
        "ack-token",
        "ack-event",
        "ack-reason",
    ] {
        let server = scope_server();
        let value = scalar_fixture();
        let (context, boundary) = boundary(&value);
        assert!(
            scope_client(&server, case)
                .prepare_cancellation_scope_on_claim(
                    &claim(&value),
                    &context,
                    &boundary,
                    &CancellationScopeDeliveryBudget::new()
                )
                .await
                .is_err(),
            "{case}"
        );
        assert_eq!(server.requests.lock().unwrap().len(), 1, "{case}");
    }
}

#[tokio::test]
async fn scope_history_incomplete_or_changed_canonical_pages_refuse_delivery_authority() {
    for case in [
        "history-namespace",
        "history-start",
        "history-context",
        "history-range",
        "history-preparation",
        "page-task",
        "page-attempt",
        "page-cycle",
        "page-terminal",
    ] {
        let server = scope_server();
        let value = scalar_fixture();
        let (context, boundary) = boundary(&value);
        assert!(
            scope_client(&server, case)
                .prepare_cancellation_scope_on_claim(
                    &claim(&value),
                    &context,
                    &boundary,
                    &CancellationScopeDeliveryBudget::new()
                )
                .await
                .is_err(),
            "{case}"
        );
        assert!(server
            .requests
            .lock()
            .unwrap()
            .iter()
            .all(|request| !request.path.ends_with("/deliver")));
    }
}

#[tokio::test]
async fn scope_history_delivery_cannot_substitute_a_different_valid_preparation() {
    let server = scope_server();
    let client = scope_client(&server, "substituted");
    let value = scalar_fixture();
    let task = claim(&value);
    let (context, boundary) = boundary(&value);
    let budget = CancellationScopeDeliveryBudget::new();
    let prepared = client
        .prepare_cancellation_scope_on_claim(&task, &context, &boundary, &budget)
        .await
        .unwrap();
    assert!(client
        .deliver_cancellation_scope_on_claim(&task, &prepared, &budget)
        .await
        .is_err());
}

#[tokio::test]
async fn scope_history_expired_budget_or_borrowed_claim_perform_no_io() {
    let server = scope_server();
    let client = scope_client(&server, "valid");
    let value = scalar_fixture();
    let (context, boundary) = boundary(&value);
    let mut budget = CancellationScopeDeliveryBudget::new();
    assert!(budget
        .restrict(DateTime::<chrono::Utc>::from_timestamp(0, 0).unwrap())
        .is_err());
    assert!(matches!(
        client
            .prepare_cancellation_scope_on_claim(&claim(&value), &context, &boundary, &budget)
            .await,
        Err(Error::Timeout)
    ));
    let mut foreign = claim(&value);
    foreign.run_id = Some("borrowed-run".into());
    assert!(client
        .prepare_cancellation_scope_on_claim(
            &foreign,
            &context,
            &boundary,
            &CancellationScopeDeliveryBudget::new()
        )
        .await
        .is_err());
    assert!(server.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn scope_history_preparation_delivery_and_history_share_one_original_budget() {
    let server = scope_server();
    let client = scope_client(&server, "budget");
    let value = scalar_fixture();
    let task = claim(&value);
    let (context, boundary) = boundary(&value);
    let budget = CancellationScopeDeliveryBudget::new();
    let started = Instant::now();
    let prepared = client
        .prepare_cancellation_scope_on_claim(&task, &context, &boundary, &budget)
        .await
        .unwrap();
    assert!(matches!(
        client
            .deliver_cancellation_scope_on_claim(&task, &prepared, &budget)
            .await,
        Err(Error::Timeout)
    ));
    assert!(started.elapsed() < Duration::from_millis(5600));
}

fn fixture(name: &str, variant: &str) -> Value {
    let source = match name {
        "operations" => {
            include_str!("../../tests/fixtures/committed-scope-operation-projections.json")
        }
        "empty" => include_str!("../../tests/fixtures/committed-scope-delivery.json"),
        "single" => include_str!("../../tests/fixtures/populated-scope-single-calls.json"),
        "groups" => include_str!("../../tests/fixtures/populated-scope-groups.json"),
        "root" => include_str!("../../tests/fixtures/run-inherited-scope-delivery.json"),
        _ => panic!("unknown fixture"),
    };
    let value: Value = serde_json::from_str(source).unwrap();
    if variant.is_empty() {
        value
    } else {
        value[variant].clone()
    }
}

fn history(value: &Value) -> Vec<HistoryEvent> {
    serde_json::from_value(value["history"].clone()).unwrap()
}

fn read(value: &Value) -> Result<CommittedCancellationScopeHistory> {
    CommittedCancellationScopeHistory::read(
        &history(value),
        value["task"]["run_id"].as_str().unwrap(),
        value["task"]["workflow_id"].as_str().unwrap(),
    )
}

fn event_mut<'a>(value: &'a mut Value, kind: &str) -> &'a mut Value {
    value["history"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|event| event["event_type"] == kind)
        .unwrap()
}

fn renumber(value: &mut Value) {
    for (index, event) in value["history"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .enumerate()
    {
        event["sequence"] = json!(index + 1);
    }
}

#[test]
fn scope_history_native_v5_operations_groups_descendants_and_competing_roots() {
    for variant in ["operations", "groups", "descendants", "competing"] {
        let value = fixture("operations", variant);
        let committed = read(&value).unwrap_or_else(|error| panic!("{variant}: {error:?}"));
        let preparation = &committed.preparations[value["scope_id"].as_str().unwrap()];
        let delivery = &committed.deliveries[&preparation.boundary.sequence];
        assert_eq!(preparation.context, delivery.context);
        assert_eq!(preparation.boundary, delivery.boundary);
        assert_eq!(preparation.authority_deadline, delivery.authority_deadline);
        assert!(committed.pending_requests.is_empty());
        let projection = &preparation.event.payload;
        assert_eq!(
            projection["timer_members"]
                .as_array()
                .unwrap()
                .iter()
                .map(|row| row["timer_id"].as_str().unwrap())
                .collect::<Vec<_>>(),
            ["timer-plain", "signal-timer-id", "condition-timer-id"]
        );
        assert_eq!(
            projection["child_members"][1]["child_workflow_run_id"],
            "child-continued-run"
        );
        assert_eq!(
            projection["child_members"]
                .as_array()
                .unwrap()
                .iter()
                .map(|row| row["cancellation_policy"].as_str().unwrap())
                .collect::<Vec<_>>(),
            ["try_cancel", "wait_cancellation_completed", "abandon"]
        );
        if matches!(variant, "descendants" | "competing") {
            assert_eq!(
                projection["descendant_members"][0]["scope_id"],
                "desc-child"
            );
            assert_eq!(
                projection["descendant_members"][1]["scope_id"],
                "desc-grandchild"
            );
            assert_eq!(
                projection["descendant_members"][0]["propagation_history_event_id"],
                if variant == "competing" {
                    "original-child-conflict"
                } else {
                    "inherited-child-accepted"
                }
            );
            assert_eq!(
                projection["descendant_members"][0]["authority_deadline_at"],
                if variant == "competing" {
                    "2026-10-04T00:00:20.123456Z"
                } else {
                    "2026-10-04T00:00:30.123456Z"
                }
            );
        } else {
            assert_eq!(projection["descendant_members"], json!([]));
        }
    }
}

#[test]
fn scope_history_empty_delivery_and_populated_scalar_policies() {
    for variant in ["shielded", "unshielded"] {
        let committed = read(&fixture("empty", variant)).unwrap();
        assert_eq!(committed.deliveries.len(), 1);
        let delivery = committed.deliveries.values().next().unwrap();
        assert_eq!(delivery.boundary.call_kind, CancellationCallKind::Timer);
        assert_eq!(
            delivery.context.request_id(),
            delivery.context.root_context().request_id()
        );
        assert_eq!(delivery.authority_deadline, delivery.context.deadline());
    }
    for variant in [
        "activity-try_cancel",
        "activity-wait_cancellation_completed",
        "activity-abandon",
        "child-try_cancel",
        "child-wait_cancellation_completed",
        "child-abandon",
        "timer",
        "condition",
        "condition-untimed",
    ] {
        let committed = read(&fixture("single", variant))
            .unwrap_or_else(|error| panic!("{variant}: {error:?}"));
        let preparation = committed.preparations.values().next().unwrap();
        let delivery = &committed.deliveries[&preparation.boundary.sequence];
        assert_eq!(delivery.boundary.sequence, 4);
        assert_eq!(delivery.context, preparation.context);
        assert!(committed.pending_requests.is_empty());
        assert!(!preparation.event.payload["activity_members"]
            .as_array()
            .unwrap()
            .is_empty());
    }
}

#[test]
fn scope_history_changed_frozen_members_and_descriptors_are_refused() {
    for variant in ["operations", "groups", "descendants", "competing"] {
        for field in [
            "activity_members",
            "timer_members",
            "wait_members",
            "child_members",
        ] {
            for failure in ["hash", "omitted", "boolean", "extra"] {
                let mut value = fixture("operations", variant);
                let members = event_mut(&mut value, "CancellationScopeDeliveryPrepared")["payload"]
                    [field]
                    .as_array_mut()
                    .unwrap();
                if members.is_empty() {
                    members.push(json!({}));
                    assert!(read(&value).is_err(), "{variant}/{field}/invented");
                    continue;
                }
                match failure {
                    "hash" => members[0]["descriptor_hash"] = json!("0".repeat(64)),
                    "omitted" => {
                        members.remove(0);
                    }
                    "boolean" => members[0]["sequence"] = json!(true),
                    "extra" => members[0]["unexpected"] = json!("borrowed"),
                    _ => unreachable!(),
                }
                assert!(read(&value).is_err(), "{variant}/{field}/{failure}");
            }
        }
    }
}

#[test]
fn scope_history_changed_request_boundary_deadline_ancestry_and_continuation_are_refused() {
    for failure in [
        "request",
        "run",
        "instance",
        "deadline",
        "boundary",
        "preparation",
        "before_preparation",
        "missing_request",
        "duplicate_request",
        "duplicate_delivery",
        "future_member",
        "continuation",
        "descendant_deadline",
        "descendant_propagation",
        "cross_shield",
        "inherited_deadline",
    ] {
        let mut value = fixture("operations", "descendants");
        match failure {
            "request" => {
                event_mut(&mut value, "CancellationScopeDelivered")["payload"]["request_id"] =
                    json!("new-request")
            }
            "run" => {
                event_mut(&mut value, "CancellationScopeDeliveryPrepared")["payload"]
                    ["workflow_run_id"] = json!("new-run")
            }
            "instance" => {
                event_mut(&mut value, "CancellationScopeDelivered")["payload"]["cancellation"]
                    ["lineage"][0]["workflow_instance_id"] = json!("new-instance")
            }
            "deadline" => {
                event_mut(&mut value, "CancellationScopeDelivered")["payload"]
                    ["authority_deadline_at"] = json!("2026-10-04T00:00:31.123456Z")
            }
            "boundary" => {
                event_mut(&mut value, "CancellationScopeDelivered")["payload"]["sequence"] =
                    json!(999)
            }
            "preparation" => {
                event_mut(&mut value, "CancellationScopeDelivered")["payload"]
                    ["preparation_history_event_id"] = json!("new-preparation")
            }
            "descendant_deadline" => {
                event_mut(&mut value, "CancellationScopeDeliveryPrepared")["payload"]
                    ["descendant_members"][0]["authority_deadline_at"] =
                    json!("2026-10-04T00:00:31.123456Z")
            }
            "descendant_propagation" => {
                event_mut(&mut value, "CancellationScopeDeliveryPrepared")["payload"]
                    ["descendant_members"][0]["propagation_history_event_id"] =
                    json!("new-propagation")
            }
            "continuation" | "cross_shield" | "inherited_deadline" => {
                let id = match failure {
                    "continuation" => "child-continued-before-preparation",
                    "cross_shield" => "desc-child-opened",
                    _ => "inherited-child-accepted",
                };
                let event = value["history"]
                    .as_array_mut()
                    .unwrap()
                    .iter_mut()
                    .find(|row| row["id"] == id)
                    .unwrap();
                match failure {
                    "continuation" => {
                        event["payload"]["child_workflow_run_id"] = json!("new-child-run")
                    }
                    "cross_shield" => event["payload"]["shield_parent"] = json!(true),
                    _ => {
                        event["payload"]["cancellation"]["lineage"][1]["cleanup_deadline_at"] =
                            json!("2026-10-04T00:00:29.123456Z")
                    }
                }
            }
            _ => {
                let events = value["history"].as_array_mut().unwrap();
                let target = match failure {
                    "missing_request" | "duplicate_request" => "CancellationScopeRequested",
                    "future_member" => "TimerScheduled",
                    _ => "CancellationScopeDelivered",
                };
                let index = events
                    .iter()
                    .position(|row| row["event_type"] == target)
                    .unwrap();
                if failure.starts_with("duplicate") {
                    let mut duplicate = events[index].clone();
                    duplicate["id"] =
                        json!(format!("{}-duplicate", duplicate["id"].as_str().unwrap()));
                    events.insert(index + 1, duplicate);
                } else {
                    let event = events.remove(index);
                    if failure == "before_preparation" {
                        let index = events
                            .iter()
                            .position(|row| {
                                row["event_type"] == "CancellationScopeDeliveryPrepared"
                            })
                            .unwrap();
                        events.insert(index, event);
                    } else if failure == "future_member" {
                        events.push(event);
                    }
                }
            }
        }
        renumber(&mut value);
        assert!(read(&value).is_err(), "{failure}");
    }
}

#[test]
fn scope_history_pending_ancestor_and_original_preparation_survive_replacement() {
    let mut value = fixture("operations", "descendants");
    value["history"].as_array_mut().unwrap().pop();
    let committed = read(&value).unwrap();
    let scopes = cancellation_scope::CancellationScopeHistory::read(
        &history(&value),
        value["task"]["run_id"].as_str().unwrap(),
    )
    .unwrap();
    let request = committed
        .pending_request_for_scope("desc-grandchild", &scopes)
        .unwrap()
        .unwrap();
    assert_eq!(
        request.context.scope_id(),
        value["scope_id"].as_str().unwrap()
    );
    let preparation = &committed.preparations[request.context.scope_id()];
    value["task"]["lease_owner"] = json!("replacement");
    value["task"]["workflow_task_attempt"] = json!(17);
    let replayed = read(&value).unwrap();
    assert_eq!(
        replayed.preparations[request.context.scope_id()].context,
        preparation.context
    );
    assert_eq!(
        replayed.preparations[request.context.scope_id()].boundary,
        preparation.boundary
    );
    assert_eq!(
        replayed.preparations[request.context.scope_id()].authority_deadline,
        preparation.authority_deadline
    );
}

#[test]
fn scope_history_run_inheritance_preserves_original_parent_and_shielding() {
    for failure in [
        "valid", "missing", "later", "shield", "metadata", "deadline",
    ] {
        let mut value = fixture("root", "");
        value["history"].as_array_mut().unwrap().truncate(5);
        match failure {
            "missing" => {
                value["history"].as_array_mut().unwrap().remove(3);
            }
            "later" => value["history"].as_array_mut().unwrap().swap(3, 4),
            "shield" => value["history"][2]["payload"]["shield_parent"] = json!(true),
            "metadata" => {
                value["history"][4]["payload"]["cancellation"]["root_context"]["reason"] =
                    json!("changed")
            }
            "deadline" => {
                value["history"][4]["payload"]["cancellation"]["lineage"][1]
                    ["cleanup_deadline_at"] = json!("2026-10-05T00:00:25.000000Z")
            }
            _ => {}
        }
        renumber(&mut value);
        let result = read(&value);
        if failure != "valid" {
            assert!(result.is_err(), "{failure}");
            continue;
        }
        let committed = result.unwrap();
        let id = value["history"][2]["payload"]["scope_id"].as_str().unwrap();
        let request = &committed.pending_requests[id];
        assert_eq!(
            request.context.request_id(),
            value["history"][4]["payload"]["request_id"]
                .as_str()
                .unwrap()
        );
        assert_eq!(
            request.context.parent_request_id(),
            value["history"][3]["payload"]["workflow_command_id"].as_str()
        );
        assert_eq!(
            request
                .context
                .lineage()
                .iter()
                .map(ScopedCancellationLineage::scope_id)
                .collect::<Vec<_>>(),
            ["root", id]
        );
    }
}
