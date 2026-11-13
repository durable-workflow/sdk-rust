//! #212: pending updates are never a null result, and an accepted update's
//! outcome is read later by update id (server#1131), never by resubmitting.

use std::time::Duration;

use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::{encode_typed_envelope, AvroValue, Client, Error, UpdateWaitStage, DEFAULT_CODEC};

/// One captured request: method, path with query, and body.
#[derive(Debug)]
struct Captured {
    method: String,
    path: String,
    body: String,
}

/// Answer each connection with the next scripted reply and capture the requests.
async fn server(replies: Vec<(u16, Value)>) -> (Client, tokio::task::JoinHandle<Vec<Captured>>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = Client::builder(format!("http://{}", listener.local_addr().unwrap()))
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let requests = tokio::spawn(async move {
        let mut captured = Vec::new();
        for (status, reply) in replies {
            let (mut stream, _) = tokio::time::timeout(Duration::from_secs(5), listener.accept())
                .await
                .expect("the client sent another request")
                .unwrap();
            let mut bytes = Vec::new();
            let (head, offset) = loop {
                let mut buf = [0; 4096];
                let count = stream.read(&mut buf).await.unwrap();
                assert!(count > 0);
                bytes.extend_from_slice(&buf[..count]);
                if let Some(offset) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
                    break (
                        String::from_utf8_lossy(&bytes[..offset]).to_string(),
                        offset + 4,
                    );
                }
            };
            let length: usize = head
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse().unwrap())
                })
                .unwrap_or(0);
            while bytes.len() < offset + length {
                let mut buf = [0; 4096];
                let count = stream.read(&mut buf).await.unwrap();
                assert!(count > 0);
                bytes.extend_from_slice(&buf[..count]);
            }
            let mut request_line = head.lines().next().unwrap().split_whitespace();
            captured.push(Captured {
                method: request_line.next().unwrap().to_string(),
                path: request_line.next().unwrap().to_string(),
                body: String::from_utf8_lossy(&bytes[offset..offset + length]).to_string(),
            });
            let body = reply.to_string();
            stream
                .write_all(
                    format!(
                        "HTTP/1.1 {status} Reply\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
        }
        captured
    });
    (client, requests)
}

fn cluster() -> (u16, Value) {
    (
        200,
        json!({"version": "3.6.0", "capabilities": {"update_outcome_reads": true}}),
    )
}

fn envelope(value: AvroValue) -> Value {
    encode_typed_envelope(&value, DEFAULT_CODEC).unwrap()
}

fn pending_receipt() -> Value {
    json!({
        "workflow_id": "order-1", "run_id": "run-9", "update_id": "upd-1",
        "update_name": "rename", "request_id": "rename-1", "update_status": "accepted",
        "result": null, "result_envelope": null, "wait_for": "completed", "wait_timed_out": true,
    })
}

#[tokio::test]
async fn a_completed_read_returns_the_decoded_result_including_null() {
    let (client, requests) = server(vec![
        cluster(),
        (
            200,
            json!({"update_id": "upd-1", "update_status": "completed", "wait_timed_out": false,
                "result": null, "result_envelope": envelope(AvroValue::String("renamed".into()))}),
        ),
        (
            200,
            json!({"update_id": "upd-2", "update_status": "completed", "wait_timed_out": false,
                "result": null, "result_envelope": envelope(AvroValue::Null)}),
        ),
    ])
    .await;

    assert_eq!(
        client
            .update_outcome("order 1", "upd-1", Some(10))
            .await
            .unwrap(),
        json!("renamed")
    );
    assert_eq!(
        client
            .update_outcome("order 1", "upd-2", None)
            .await
            .unwrap(),
        Value::Null,
        "a handler that returned null is a completed null result"
    );

    // Discovery happens once; each read is a GET on the update, never a POST.
    let requests = requests.await.unwrap();
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[0].path, "/api/cluster/info");
    assert_eq!(requests[1].method, "GET");
    assert_eq!(
        requests[1].path,
        "/api/workflows/order%201/updates/upd-1?wait_for=completed&wait_timeout_seconds=10"
    );
    assert!(requests[1].body.is_empty());
    assert_eq!(requests[2].method, "GET");
    assert_eq!(
        requests[2].path,
        "/api/workflows/order%201/updates/upd-2?wait_for=completed"
    );
}

#[tokio::test]
async fn a_typed_completed_read_keeps_the_avro_value() {
    let (client, _requests) = server(vec![
        cluster(),
        (
            200,
            json!({"update_id": "upd-1", "update_status": "completed",
                "result_envelope": envelope(AvroValue::Long(42))}),
        ),
    ])
    .await;
    let handle = crate::WorkflowHandle {
        client: client.clone(),
        workflow_id: "order-1".to_string(),
        run_id: None,
        workflow_type: "orders".to_string(),
    };

    assert_eq!(
        handle
            .update_outcome_avro_value("upd-1", None)
            .await
            .unwrap(),
        AvroValue::Long(42)
    );
}

#[tokio::test]
async fn a_pending_read_is_a_typed_error_that_points_at_reading_again() {
    let (client, _requests) = server(vec![cluster(), (202, pending_receipt())]).await;

    let error = client
        .update_outcome("order-1", "upd-1", Some(1))
        .await
        .expect_err("a pending update must not return a value");
    let Error::UpdateStillPending(pending) = &error else {
        panic!("expected UpdateStillPending, got {error:?}");
    };
    assert_eq!(pending.workflow_id, "order-1");
    assert_eq!(pending.run_id.as_deref(), Some("run-9"));
    assert_eq!(pending.update_id.as_deref(), Some("upd-1"));
    assert_eq!(pending.update_name.as_deref(), Some("rename"));
    assert_eq!(pending.request_id.as_deref(), Some("rename-1"));
    assert_eq!(pending.update_status.as_deref(), Some("accepted"));
    assert!(pending.wait_timed_out);
    assert_eq!(pending.receipt, pending_receipt());
    let message = error.to_string();
    assert!(
        message.contains("update_outcome() again with update id \"upd-1\""),
        "{message}"
    );
    assert!(message.contains("never admits another update"), "{message}");
}

#[tokio::test]
async fn reading_again_after_pending_returns_the_result() {
    let (client, requests) = server(vec![
        cluster(),
        (202, pending_receipt()),
        (
            200,
            json!({"update_id": "upd-1", "update_status": "completed",
                "result_envelope": envelope(AvroValue::Boolean(true))}),
        ),
    ])
    .await;

    let Err(Error::UpdateStillPending(pending)) =
        client.update_outcome("order-1", "upd-1", None).await
    else {
        panic!("the first read was still pending");
    };
    let update_id = pending.update_id.expect("pending update id");
    assert_eq!(
        client
            .update_outcome("order-1", &update_id, None)
            .await
            .unwrap(),
        json!(true)
    );
    let requests = requests.await.unwrap();
    assert!(
        requests.iter().all(|request| request.method == "GET"),
        "reading never submits the update again: {requests:?}"
    );
}

#[tokio::test]
async fn an_accepted_receipt_read_returns_the_receipt_without_waiting() {
    let receipt = json!({"update_id": "upd-1", "update_status": "accepted",
        "result_envelope": null, "wait_timed_out": false});
    let (client, requests) = server(vec![cluster(), (202, receipt.clone())]).await;

    assert_eq!(
        client
            .update_outcome_receipt("order-1", "upd-1", UpdateWaitStage::Accepted, None)
            .await
            .unwrap(),
        receipt
    );
    let requests = requests.await.unwrap();
    assert_eq!(requests[1].path, "/api/workflows/order-1/updates/upd-1");
}

#[tokio::test]
async fn a_failed_update_surfaces_its_failure_receipt() {
    let failure = json!({"update_id": "upd-1", "update_status": "failed", "reason": "update_failed",
        "failure_id": "fail-1", "failure_message": "boom"});
    let (client, _requests) = server(vec![cluster(), (422, failure.clone())]).await;

    let error = client
        .update_outcome("order-1", "upd-1", None)
        .await
        .expect_err("a failed update has no result");
    let Error::Http { status, body } = &error else {
        panic!("expected the failure receipt, got {error:?}");
    };
    assert_eq!(status.as_u16(), 422);
    assert_eq!(serde_json::from_str::<Value>(body).unwrap(), failure);
    assert!(!error.is_update_not_found());
}

#[tokio::test]
async fn an_unknown_update_is_update_not_found() {
    let (client, _requests) = server(vec![
        cluster(),
        (
            404,
            json!({"reason": "update_not_found", "workflow_id": "order-1", "update_id": "upd-x",
                "message": "No retained update has this id."}),
        ),
    ])
    .await;

    let error = client
        .update_outcome("order-1", "upd-x", None)
        .await
        .expect_err("an unknown update has no outcome");
    assert!(error.is_update_not_found(), "{error:?}");
}

#[tokio::test]
async fn a_server_without_the_capability_is_refused_before_any_read() {
    let (client, requests) = server(vec![(
        200,
        json!({"version": "3.5.0", "capabilities": {"command_request_keys": false}}),
    )])
    .await;

    let error = client
        .update_outcome("order-1", "upd-1", None)
        .await
        .expect_err("an older Server must be refused");
    assert!(
        matches!(error, Error::UpdateOutcomeReadsUnavailable),
        "{error:?}"
    );
    assert!(error.to_string().contains("update_outcome_reads"));
    let requests = requests.await.unwrap();
    assert_eq!(
        requests.len(),
        1,
        "only discovery ran: nothing was read or resubmitted"
    );
    assert_eq!(requests[0].path, "/api/cluster/info");
}

#[tokio::test]
async fn an_empty_update_id_is_refused_without_a_request() {
    let (client, requests) = server(vec![]).await;
    let error = client
        .update_outcome_receipt("order-1", " ", UpdateWaitStage::Completed, None)
        .await
        .expect_err("an update id is required");
    assert!(matches!(error, Error::Codec(_)), "{error:?}");
    assert!(requests.await.unwrap().is_empty());
}

#[tokio::test]
async fn a_submitted_update_that_times_out_is_pending_not_null() {
    // The first inline payload discovers the request policy from cluster info.
    let (client, requests) = server(vec![
        (200, json!({"version": "3.5.0", "capabilities": {}})),
        (202, pending_receipt()),
        (202, pending_receipt()),
    ])
    .await;

    let error = client
        .update_workflow("order-1", "rename", json!(["new"]), Some("rename-1"))
        .await
        .expect_err("a timed-out update is not a null result");
    let Error::UpdateStillPending(pending) = &error else {
        panic!("expected UpdateStillPending, got {error:?}");
    };
    assert_eq!(pending.update_id.as_deref(), Some("upd-1"));
    assert_eq!(pending.request_id.as_deref(), Some("rename-1"));
    let message = error.to_string();
    assert!(message.contains("update id \"upd-1\""), "{message}");
    assert!(message.contains("request_id \"rename-1\""), "{message}");

    let typed = client
        .update_workflow_avro_value("order-1", "rename", json!(["new"]), Some("rename-1"))
        .await
        .expect_err("a timed-out typed update is pending, not a codec error");
    assert!(matches!(typed, Error::UpdateStillPending(_)), "{typed:?}");
    let requests = requests.await.unwrap();
    assert_eq!(requests[0].path, "/api/cluster/info");
    assert!(requests[1..].iter().all(|request| request.method == "POST"
        && request.path == "/api/workflows/order-1/update/rename"));
}

#[tokio::test]
async fn a_submitted_update_that_returned_null_is_a_null_result() {
    let (client, _requests) = server(vec![
        (200, json!({"version": "3.5.0", "capabilities": {}})),
        (
            200,
            json!({"workflow_id": "order-1", "update_id": "upd-1", "update_status": "completed",
                "wait_timed_out": false, "result": null, "result_envelope": envelope(AvroValue::Null)}),
        ),
    ])
    .await;

    assert_eq!(
        client
            .update_workflow("order-1", "rename", json!(["new"]), Some("rename-1"))
            .await
            .unwrap(),
        Value::Null
    );
}

#[tokio::test]
async fn dot_segment_and_empty_ids_are_refused_before_any_request() {
    let (client, requests) = server(vec![]).await;
    for (workflow_id, update_id) in [
        ("order-1", "."),
        ("order-1", ".."),
        (".", "upd-1"),
        ("..", "upd-1"),
        ("", "upd-1"),
    ] {
        let error = client
            .update_outcome(workflow_id, update_id, None)
            .await
            .expect_err("a dot segment would read another route");
        assert!(
            matches!(error, Error::InvalidPathSegment { .. }),
            "{workflow_id:?}/{update_id:?}: {error:?}"
        );
        let error = client
            .update_outcome_receipt(workflow_id, update_id, UpdateWaitStage::Accepted, None)
            .await
            .expect_err("a dot segment would read another route");
        assert!(
            matches!(error, Error::InvalidPathSegment { .. }),
            "{error:?}"
        );
    }
    assert!(
        requests.await.unwrap().is_empty(),
        "nothing was sent, not even discovery"
    );
}

#[tokio::test]
async fn a_success_that_is_not_a_receipt_for_the_update_is_never_an_outcome() {
    let describe = json!({"workflow_id": "order-1", "run_id": "run-9", "status": "completed",
        "result": "the workflow's own result"});
    let other_update = json!({"workflow_id": "order-1", "update_id": "upd-2",
        "update_status": "completed", "result_envelope": envelope(AvroValue::Long(7))});
    let other_workflow = json!({"workflow_id": "order-2", "update_id": "upd-1",
        "update_status": "completed", "result_envelope": envelope(AvroValue::Long(7))});
    let (client, _requests) = server(vec![
        cluster(),
        (200, describe.clone()),
        (200, json!({})),
        (200, other_update),
        (200, other_workflow),
        (200, describe.clone()),
    ])
    .await;

    for _ in 0..4 {
        let error = client
            .update_outcome("order-1", "upd-1", None)
            .await
            .expect_err("only a receipt for upd-1 carries its outcome");
        let Error::UnrecognizedUpdateOutcome {
            workflow_id,
            update_id,
            ..
        } = &error
        else {
            panic!("expected UnrecognizedUpdateOutcome, got {error:?}");
        };
        assert_eq!(
            (workflow_id.as_str(), update_id.as_str()),
            ("order-1", "upd-1")
        );
    }
    let error = client
        .update_outcome_receipt("order-1", "upd-1", UpdateWaitStage::Accepted, None)
        .await
        .expect_err("a describe body is not a receipt");
    let Error::UnrecognizedUpdateOutcome { receipt, .. } = error else {
        panic!("expected UnrecognizedUpdateOutcome");
    };
    assert_eq!(receipt, describe);
}

#[tokio::test]
async fn a_successful_receipt_that_is_not_completed_is_never_an_outcome() {
    let (client, _requests) = server(vec![
        cluster(),
        (
            200,
            json!({"workflow_id": "order-1", "update_id": "upd-1", "update_status": "failed",
                "wait_timed_out": false, "result": null, "result_envelope": null}),
        ),
    ])
    .await;

    let error = client
        .update_outcome("order-1", "upd-1", None)
        .await
        .expect_err("a failed status is not a null result");
    assert!(
        matches!(error, Error::UnrecognizedUpdateOutcome { .. }),
        "{error:?}"
    );
}
