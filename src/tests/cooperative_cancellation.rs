use super::*;

fn request_observation() -> Value {
    json!({
        "request_id": "original-request",
        "requested_at": "2026-10-01T08:00:00Z",
        "cleanup_deadline_at": "2026-10-01T08:10:00Z",
        "history_refresh_page_token": "opaque-server-token"
    })
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
        started.elapsed() < Duration::from_millis(5300),
        "total budget exceeded: {:?}",
        started.elapsed()
    );
    assert_eq!(server.requests.lock().unwrap().len(), 2);
}
