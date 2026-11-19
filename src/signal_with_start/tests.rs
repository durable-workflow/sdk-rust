//! #222: one signal-with-start request carries the start and the signal, and
//! every refusal is typed (server#1133).

use serde_json::{json, Value};

use crate::update_outcome::tests::server;
use crate::{ClosedRunPolicy, Error, OpenRunPolicy, SignalWithStartOptions, WorkflowStartOptions};

fn cluster() -> (u16, Value) {
    (
        200,
        json!({"version": "3.15.0", "capabilities": {"signal_with_start": true}}),
    )
}

#[tokio::test]
async fn one_request_carries_the_start_and_the_signal() {
    let receipt = json!({"workflow_id": "order 1", "run_id": "run-1", "command_id": "cmd-2",
        "start_command_id": "cmd-1", "start_outcome": "started_new", "outcome": "signal_received",
        "intake_group_id": "group-1", "request_id": "order-1:rev-1", "retained": false});
    let (client, requests) = server(vec![cluster(), (201, receipt.clone())]).await;

    let answer = client
        .signal_with_start(
            "orders.entity",
            "orders",
            "order 1",
            "changed",
            vec!["order 1"],
            vec![1],
            SignalWithStartOptions::new()
                .request_id("order-1:rev-1")
                .closed_run_policy(ClosedRunPolicy::StartNewRun)
                .start(WorkflowStartOptions::new().execution_timeout_seconds(7200))
                .business_key("order-1"),
        )
        .await
        .unwrap();

    assert_eq!(answer, receipt);
    let requests = requests.await.unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].path, "/api/cluster/info");
    assert_eq!(requests[1].method, "POST");
    assert_eq!(
        requests[1].path,
        "/api/workflows/order%201/signal-with-start/changed"
    );
    let body: Value = serde_json::from_str(&requests[1].body).unwrap();
    assert_eq!(body["workflow_type"], "orders.entity");
    assert_eq!(body["task_queue"], "orders");
    assert_eq!(body["request_id"], "order-1:rev-1");
    assert_eq!(body["duplicate_policy"], "use-existing");
    assert_eq!(body["closed_run_policy"], "start_new_run");
    assert_eq!(body["execution_timeout_seconds"], 7200);
    assert_eq!(body["business_key"], "order-1");
    assert_eq!(body["input"]["codec"], "avro");
    assert_eq!(body["signal_input"]["codec"], "avro");
}

#[tokio::test]
async fn a_generated_key_is_sent_and_returned() {
    let (client, requests) = server(vec![
        cluster(),
        (202, json!({"start_outcome": "returned_existing_active"})),
    ])
    .await;

    let answer = client
        .signal_with_start(
            "orders.entity",
            "orders",
            "order-1",
            "changed",
            Vec::<i64>::new(),
            vec![2],
            SignalWithStartOptions::new().open_run_policy(OpenRunPolicy::Fail),
        )
        .await
        .unwrap();

    let requests = requests.await.unwrap();
    let body: Value = serde_json::from_str(&requests[1].body).unwrap();
    let key = body["request_id"].as_str().unwrap();
    assert!(key.starts_with("sdk-rust-signal-with-start-"), "{key}");
    assert_eq!(answer["request_id"], key);
    assert_eq!(body["duplicate_policy"], "fail");
    assert_eq!(body["closed_run_policy"], "reject");
}

#[tokio::test]
async fn every_refusal_names_its_reason() {
    for (status, reason) in [
        (409, "run_not_active"),
        (409, "instance_already_started"),
        (409, "workflow_type_conflict"),
        (409, "task_queue_conflict"),
        (409, "workflow_id_reserved_in_namespace"),
        (404, "unknown_signal"),
        (422, "invalid_signal_arguments"),
        (409, "request_id_conflict"),
    ] {
        let refusal = json!({"reason": reason, "workflow_id": "order-1", "retained_command_id": "cmd-2",
            "conflict": ["arguments"], "control_plane": {"operation": "signal_with_start"}});
        let (client, requests) = server(vec![cluster(), (status, refusal)]).await;

        let error = client
            .signal_with_start(
                "orders.entity",
                "orders",
                "order-1",
                "changed",
                Vec::<i64>::new(),
                vec![1],
                SignalWithStartOptions::new().request_id("k-1"),
            )
            .await
            .unwrap_err();

        assert_eq!(
            error.signal_with_start_refusal().as_deref(),
            Some(reason),
            "{error}"
        );
        assert_eq!(
            error.is_request_id_conflict(),
            reason == "request_id_conflict",
            "{error}"
        );
        if reason == "request_id_conflict" {
            assert_eq!(error.request_id().as_deref(), Some("k-1"));
        }
        assert_eq!(requests.await.unwrap().len(), 2, "sent once: {reason}");
    }
}

#[tokio::test]
async fn a_server_without_the_capability_is_refused_before_any_write() {
    for capabilities in [json!({}), json!({"signal_with_start": false})] {
        let (client, requests) = server(vec![(
            200,
            json!({"version": "3.14.0", "capabilities": capabilities}),
        )])
        .await;

        let error = client
            .signal_with_start(
                "orders.entity",
                "orders",
                "order-1",
                "changed",
                Vec::<i64>::new(),
                vec![1],
                SignalWithStartOptions::new(),
            )
            .await
            .unwrap_err();

        assert!(
            matches!(error, Error::SignalWithStartUnavailable),
            "{error}"
        );
        let requests = requests.await.unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].method, "GET");
    }
}

#[tokio::test]
async fn an_empty_key_is_refused_before_any_request() {
    let (client, requests) = server(vec![]).await;

    let error = client
        .signal_with_start(
            "orders.entity",
            "orders",
            "order-1",
            "changed",
            Vec::<i64>::new(),
            vec![1],
            SignalWithStartOptions::new().request_id(""),
        )
        .await
        .unwrap_err();

    assert!(matches!(error, Error::Codec(_)), "{error}");
    assert!(requests.await.unwrap().is_empty());
}
