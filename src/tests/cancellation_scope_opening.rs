use super::*;

fn receipt() -> Value {
    json!({"task_id":"task/one", "workflow_run_id":"run-one", "lease_owner":"original",
        "workflow_task_attempt":4, "sequence":2, "scope_id":"scope-two", "parent_scope_id":"scope-one",
        "shield_parent":true, "opened":true, "duplicate":false, "claim_released":false,
        "created_task_ids":[], "reason":null, "history_event_id":"event-scope-two",
        "history_refresh_page_token":"opaque-start"})
}

fn history() -> Vec<Value> {
    vec![
        json!({"id":"start", "sequence":1, "namespace":"tenant", "event_type":"WorkflowStarted", "payload":{}}),
        json!({"id":"event-scope-one", "sequence":2, "namespace":"tenant", "event_type":"CancellationScopeOpened",
            "payload":{"schema":"durable-workflow.cancellation-scope/v1", "workflow_run_id":"run-one",
                "sequence":1, "scope_id":"scope-one", "parent_scope_id":"root", "shield_parent":false}}),
        json!({"id":"event-scope-two", "sequence":3, "namespace":"tenant", "event_type":"CancellationScopeOpened",
            "payload":{"schema":"durable-workflow.cancellation-scope/v1", "workflow_run_id":"run-one",
                "sequence":2, "scope_id":"scope-two", "parent_scope_id":"scope-one", "shield_parent":true}}),
    ]
}

fn response(path: &str, body: &str, number: usize) -> Option<(&'static str, String)> {
    let case = path.split('/').nth(1).unwrap_or_default();
    if case == "budget" {
        thread::sleep(Duration::from_secs(3));
    }
    let mut value;
    if path.ends_with("/cancellation-scopes/open") {
        if case == "lost-ack" && number == 1 {
            return Some(("invalid-status", String::new()));
        }
        if case == "refused" {
            return Some((
                "409 Conflict",
                r#"{"reason":"lease_owner_mismatch"}"#.into(),
            ));
        }
        value = receipt();
        if case == "lost-ack" || case == "duplicate" {
            value["duplicate"] = json!(true);
        }
        match case {
            "ack-owner" => value["lease_owner"] = json!("replacement"),
            "ack-attempt" => value["workflow_task_attempt"] = json!(5),
            "ack-run" => value["workflow_run_id"] = json!("foreign"),
            "ack-sequence" => value["sequence"] = json!(3),
            "ack-shield" => value["shield_parent"] = json!(1),
            "ack-opened" => value["opened"] = json!(1),
            "ack-duplicate" => value["duplicate"] = json!(0),
            "ack-released" => value["claim_released"] = json!(true),
            "ack-new-task" => value["created_task_ids"] = json!(["new"]),
            "ack-root" => value["scope_id"] = json!("root"),
            "ack-token" => value["history_refresh_page_token"] = Value::Null,
            "ack-event" => value["history_event_id"] = json!(""),
            "ack-reason" => {
                value.as_object_mut().unwrap().remove("reason");
            }
            _ => {}
        }
    } else if path.ends_with("/history") {
        let mut events = history();
        if case == "accepted-prefix" {
            events.insert(
                0,
                json!({"id":"accepted", "sequence":0, "namespace":"tenant",
                "event_type":"StartAccepted", "payload":{}}),
            );
            for event in &mut events {
                event["sequence"] = json!(event["sequence"].as_u64().unwrap() + 1);
            }
        }
        match case {
            "tree-namespace" => events[2]["namespace"] = json!("foreign"),
            "tree-id" => events[2]["id"] = json!("start"),
            "tree-event-sequence" => events[2]["sequence"] = json!(2),
            "tree-run" => events[2]["payload"]["workflow_run_id"] = json!("foreign"),
            "tree-scope" => events[2]["payload"]["scope_id"] = json!("scope-one"),
            "tree-parent" => events[2]["payload"]["parent_scope_id"] = json!("unknown"),
            "tree-self" => events[2]["payload"]["parent_scope_id"] = json!("scope-two"),
            "tree-changed-parent" => events[2]["payload"]["parent_scope_id"] = json!("root"),
            "tree-shield" => events[2]["payload"]["shield_parent"] = json!(false),
            "tree-bool" => events[2]["payload"]["shield_parent"] = json!(1),
            "tree-sequence" => events[2]["payload"]["sequence"] = json!(1),
            "tree-schema" => events[2]["payload"]["schema"] = json!("other"),
            "missing-opening" => {
                events.pop();
            }
            "missing-start" => {
                events.remove(0);
            }
            "empty-history" => events.clear(),
            _ => {}
        }
        let requested: Value = serde_json::from_str(body).unwrap();
        let (batch, token) = if requested["next_history_page_token"] == "opaque-start" {
            (
                events.iter().take(1).cloned().collect::<Vec<_>>(),
                json!("opaque-next"),
            )
        } else {
            (
                events.iter().skip(1).cloned().collect::<Vec<_>>(),
                Value::Null,
            )
        };
        value = json!({"task_id":"task/one", "workflow_task_attempt":4,
            "history_events":batch,"next_history_page_token":token});
        match case {
            "page-task" => value["task_id"] = json!("foreign"),
            "page-attempt" => value["workflow_task_attempt"] = json!(5),
            "page-cycle" => value["next_history_page_token"] = json!("opaque-start"),
            "page-empty" => value["history_events"] = json!([]),
            "page-events" => value["history_events"] = json!([null]),
            "page-token" => {
                value
                    .as_object_mut()
                    .unwrap()
                    .remove("next_history_page_token");
            }
            _ => {}
        }
    } else {
        return None;
    }
    Some(("200 OK", value.to_string()))
}

fn server() -> MockWorkerServer {
    MockWorkerServer::start_with_behavior(MockWorkerBehavior {
        request_override: Some(response),
        ..MockWorkerBehavior::default()
    })
}

fn client(server: &MockWorkerServer, case: &str) -> Client {
    Client::builder(format!("{}/{case}", server.base_url()))
        .namespace("tenant")
        .control_token(Some("control-only".into()))
        .worker_token(Some("worker-only".into()))
        .build()
        .unwrap()
}

fn task() -> WorkflowTask {
    serde_json::from_value(
        json!({"task_id":"task/one", "run_id":"run-one", "workflow_type":"scope",
        "lease_owner":"original", "workflow_task_attempt":4, "payload_codec":DEFAULT_CODEC}),
    )
    .unwrap()
}

#[tokio::test]
async fn scope_opening_proves_complete_paged_tree_and_lost_ack_under_original_worker_credentials() {
    for case in ["valid", "duplicate", "lost-ack", "accepted-prefix"] {
        let server = server();
        let proof = client(&server, case)
            .open_cancellation_scope_on_claim(&task(), 2, "scope-one", true)
            .await
            .unwrap();
        assert_eq!(proof.scope_id(), "scope-two");
        assert_eq!(proof.history_event_id(), "event-scope-two");
        assert_eq!(proof.sequence(), 2);
        assert_eq!(proof.parent_scope_id(), "scope-one");
        assert!(proof.shield_parent());
        assert_eq!(proof.duplicate(), matches!(case, "duplicate" | "lost-ack"));
        assert_eq!(
            proof.history().len(),
            if case == "accepted-prefix" { 4 } else { 3 }
        );
        let requests = server.requests.lock().unwrap();
        let opens = requests
            .iter()
            .filter(|row| row.path.ends_with("/open"))
            .collect::<Vec<_>>();
        assert_eq!(opens.len(), if case == "lost-ack" { 2 } else { 1 });
        if case == "lost-ack" {
            assert_eq!(opens[0].body, opens[1].body);
        }
        for request in requests.iter() {
            assert_eq!(request.worker_protocol.as_deref(), Some("1.20"));
            assert_eq!(request.authorization.as_deref(), Some("Bearer worker-only"));
            assert_eq!(request.namespace.as_deref(), Some("tenant"));
            assert!(request.path.contains("/task%2Fone/"));
            let body: Value = serde_json::from_str(&request.body).unwrap();
            assert_eq!(body["lease_owner"], "original");
            assert_eq!(body["workflow_task_attempt"], 4);
        }
    }
}

#[tokio::test]
async fn scope_opening_refuses_changed_receipts_before_history_reads() {
    for case in [
        "ack-owner",
        "ack-attempt",
        "ack-run",
        "ack-sequence",
        "ack-shield",
        "ack-opened",
        "ack-duplicate",
        "ack-released",
        "ack-new-task",
        "ack-root",
        "ack-token",
        "ack-event",
        "ack-reason",
    ] {
        let server = server();
        assert!(
            matches!(
                client(&server, case)
                    .open_cancellation_scope_on_claim(&task(), 2, "scope-one", true)
                    .await,
                Err(Error::InvalidCooperativeCancellation(_))
            ),
            "{case}"
        );
        assert_eq!(server.requests.lock().unwrap().len(), 1, "{case}");
    }
}

#[tokio::test]
async fn scope_opening_refuses_altered_canonical_tree_and_invalid_claim_pages() {
    for case in [
        "tree-namespace",
        "tree-id",
        "tree-event-sequence",
        "tree-run",
        "tree-scope",
        "tree-parent",
        "tree-self",
        "tree-changed-parent",
        "tree-shield",
        "tree-bool",
        "tree-sequence",
        "tree-schema",
        "missing-opening",
        "missing-start",
        "empty-history",
        "page-task",
        "page-attempt",
        "page-cycle",
        "page-empty",
        "page-events",
        "page-token",
    ] {
        let server = server();
        assert!(
            matches!(
                client(&server, case)
                    .open_cancellation_scope_on_claim(&task(), 2, "scope-one", true)
                    .await,
                Err(Error::InvalidCooperativeCancellation(_))
            ),
            "{case}"
        );
    }
}

#[tokio::test]
async fn scope_opening_refuses_invalid_original_authority_before_io() {
    let server = server();
    let client = client(&server, "valid");
    for field in ["task_id", "run_id", "lease_owner", "workflow_task_attempt"] {
        let mut invalid = task();
        match field {
            "task_id" => invalid.task_id.clear(),
            "run_id" => invalid.run_id = None,
            "lease_owner" => invalid.lease_owner = Some("x".repeat(256)),
            _ => invalid.workflow_task_attempt = 0,
        }
        assert!(client
            .open_cancellation_scope_on_claim(&invalid, 2, "scope-one", true)
            .await
            .is_err());
    }
    for (sequence, parent) in [(0, "scope-one"), (u64::MAX, "scope-one"), (2, "")] {
        assert!(client
            .open_cancellation_scope_on_claim(&task(), sequence, parent, true)
            .await
            .is_err());
    }
    assert!(server.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn scope_opening_preserves_claim_refusal_and_bounds_opening_with_all_history_pages() {
    let server = server();
    assert!(
        matches!(client(&server,"refused").open_cancellation_scope_on_claim(&task(),2,"scope-one",true).await,
        Err(Error::Http { status, .. }) if status.as_u16() == 409)
    );
    assert_eq!(server.requests.lock().unwrap().len(), 1);
    let started = Instant::now();
    assert!(matches!(
        client(&server, "budget")
            .open_cancellation_scope_on_claim(&task(), 2, "scope-one", true)
            .await,
        Err(Error::Timeout)
    ));
    assert!(started.elapsed() < Duration::from_secs(6));
}
