//! Optional runtime lease maintenance, independent of application progress.

use super::*;

const MAX_REQUEST_WINDOW: Duration = Duration::from_secs(5);

fn unavailable(message: &str) -> Error {
    Error::ActivityLeaseRenewalUnavailable(message.to_owned())
}

impl Client {
    /// Renew one original activity claim without recording application progress.
    ///
    /// Actual worker-authenticated discovery must advertise the boolean
    /// `activity_lease_renewal=true`. Discovery, retries and renewal share
    /// `request_window`, which must be positive and at most five seconds. Use
    /// any earlier claim or execution deadline as an additional caller ceiling.
    ///
    /// This returns the Server reply. Before extending local publication
    /// authority, the caller must validate the original task/run/execution,
    /// attempt and owner, the granted lease and unchanged business deadlines.
    /// A successful HTTP response alone does not authorize continued execution.
    pub async fn renew_activity_task_lease(
        &self,
        task_id: &str,
        activity_attempt_id: &str,
        lease_owner: &str,
        request_window: Duration,
    ) -> Result<Value> {
        for identity in [task_id, activity_attempt_id, lease_owner] {
            if identity.trim().is_empty()
                || identity.trim() != identity
                || identity.chars().count() > 255
            {
                return Err(unavailable(
                    "renewal requires bounded, nonempty original claim identities",
                ));
            }
        }
        if request_window.is_zero() || request_window > MAX_REQUEST_WINDOW {
            return Err(unavailable(
                "the shared request window must be positive and at most five seconds",
            ));
        }
        let result = tokio::time::timeout(request_window, async {
            let discovery: Value = self
                .request_json_unresolved_with_timeout::<Value, Value>(
                    reqwest::Method::GET,
                    "/cluster/info",
                    RequestProtocol::DefaultWorker,
                    None,
                    request_window,
                )
                .await?;
            if discovery["worker_protocol"]["server_capabilities"]["activity_lease_renewal"]
                .as_bool()
                != Some(true)
            {
                return Err(unavailable(
                    "runtime must explicitly advertise activity_lease_renewal=true",
                ));
            }
            activity_task_response(
                self.request_json_unresolved_with_timeout(
                    reqwest::Method::POST,
                    &format!(
                        "/worker/activity-tasks/{}/renew-lease",
                        percent_encode_path_segment(task_id)
                    ),
                    RequestProtocol::DefaultWorker,
                    Some(&json!({
                        "activity_attempt_id": activity_attempt_id,
                        "lease_owner": lease_owner,
                    })),
                    request_window,
                )
                .await,
                "renew_lease",
                task_id,
                activity_attempt_id,
            )
        })
        .await;
        match result {
            Err(_) => Err(Error::Timeout),
            Ok(Err(Error::Transport(error))) if error.is_timeout() => Err(Error::Timeout),
            Ok(result) => result,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[derive(Debug)]
    struct Request {
        headers: String,
        body: Option<Value>,
    }

    fn discovery(capability: Value) -> Value {
        json!({"worker_protocol":{"server_capabilities":{
            "activity_lease_renewal":capability
        }}})
    }

    async fn server(
        replies: Vec<(u16, Value, Duration)>,
    ) -> (Client, tokio::task::JoinHandle<Vec<Request>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = Client::builder(format!("http://{}", listener.local_addr().unwrap()))
            .control_token(Some("control-test-token".to_owned()))
            .worker_token(Some("worker-test-token".to_owned()))
            .namespace("named-test")
            .build()
            .unwrap();
        let requests = tokio::spawn(async move {
            let mut captured = Vec::new();
            for (status, reply, delay) in replies {
                let (mut stream, _) =
                    tokio::time::timeout(Duration::from_secs(5), listener.accept())
                        .await
                        .unwrap()
                        .unwrap();
                let mut bytes = Vec::new();
                let offset = loop {
                    let mut buf = [0; 4096];
                    let count = stream.read(&mut buf).await.unwrap();
                    assert!(count > 0);
                    bytes.extend_from_slice(&buf[..count]);
                    if let Some(offset) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
                        break offset + 4;
                    }
                };
                let headers = String::from_utf8_lossy(&bytes[..offset]).to_ascii_lowercase();
                let length = headers
                    .lines()
                    .find_map(|line| line.strip_prefix("content-length: "))
                    .unwrap_or("0")
                    .parse::<usize>()
                    .unwrap();
                while bytes.len() < offset + length {
                    let mut buf = [0; 4096];
                    let count = stream.read(&mut buf).await.unwrap();
                    assert!(count > 0);
                    bytes.extend_from_slice(&buf[..count]);
                }
                captured.push(Request {
                    headers,
                    body: (length > 0)
                        .then(|| serde_json::from_slice(&bytes[offset..offset + length]).unwrap()),
                });
                tokio::time::sleep(delay).await;
                let body = reply.to_string();
                // Deadline controls deliberately close the client before this write.
                let _ = stream.write_all(format!("HTTP/1.1 {status} Reply\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await;
            }
            captured
        });
        (client, requests)
    }

    #[tokio::test]
    async fn renewal_uses_worker_credentials_and_only_original_claim_metadata() {
        let receipt = json!({"lease_renewed":true,"task_id":"task/1","activity_attempt_id":"0"});
        let (client, requests) = server(vec![
            (200, discovery(json!(true)), Duration::ZERO),
            (200, receipt.clone(), Duration::ZERO),
        ])
        .await;
        assert_eq!(
            client
                .renew_activity_task_lease("task/1", "0", "0", Duration::from_secs(1))
                .await
                .unwrap(),
            receipt
        );
        let requests = requests.await.unwrap();
        assert_eq!(requests.len(), 2);
        assert!(requests[0].headers.starts_with("get /api/cluster/info "));
        assert!(requests[1]
            .headers
            .starts_with("post /api/worker/activity-tasks/task%2f1/renew-lease "));
        for request in &requests {
            assert!(request
                .headers
                .contains("authorization: bearer worker-test-token\r\n"));
            assert!(!request.headers.contains("control-test-token"));
            assert!(request.headers.contains("x-namespace: named-test\r\n"));
            assert!(request
                .headers
                .contains("x-durable-workflow-protocol-version: 1.20\r\n"));
        }
        assert!(requests[0].body.is_none());
        assert_eq!(
            requests[1].body,
            Some(json!({"activity_attempt_id":"0","lease_owner":"0"}))
        );
    }

    #[tokio::test]
    async fn renewal_requires_affirmative_boolean_capability() {
        for reply in [
            discovery(json!(false)),
            discovery(json!("true")),
            discovery(json!(1)),
            discovery(Value::Null),
            json!({"worker_protocol":{"server_capabilities":{}}}),
            json!({}),
        ] {
            let (client, requests) = server(vec![(200, reply, Duration::ZERO)]).await;
            assert!(matches!(
                client
                    .renew_activity_task_lease("task", "attempt", "owner", Duration::from_secs(1))
                    .await,
                Err(Error::ActivityLeaseRenewalUnavailable(_))
            ));
            assert_eq!(requests.await.unwrap().len(), 1);
        }
    }

    #[tokio::test]
    async fn invalid_identities_and_windows_are_refused_before_transport() {
        let client = Client::new("http://127.0.0.1:1").unwrap();
        let oversized = "x".repeat(256);
        for field in 0..3 {
            for invalid in ["", " ", " task", "task ", &oversized] {
                let mut identities = ["task", "attempt", "owner"];
                identities[field] = invalid;
                assert!(matches!(
                    client
                        .renew_activity_task_lease(
                            identities[0],
                            identities[1],
                            identities[2],
                            Duration::from_secs(1)
                        )
                        .await,
                    Err(Error::ActivityLeaseRenewalUnavailable(_))
                ));
            }
        }
        for window in [Duration::ZERO, Duration::from_secs(6)] {
            assert!(matches!(
                client
                    .renew_activity_task_lease("task", "attempt", "owner", window)
                    .await,
                Err(Error::ActivityLeaseRenewalUnavailable(_))
            ));
        }
    }

    #[tokio::test]
    async fn discovery_is_inside_the_total_request_window() {
        let (client, requests) = server(vec![(
            200,
            discovery(json!(true)),
            Duration::from_millis(200),
        )])
        .await;
        let error = client
            .renew_activity_task_lease("task", "attempt", "owner", Duration::from_millis(50))
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Timeout), "{error:?}");
        assert_eq!(requests.await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn renewal_does_not_restart_the_discovery_deadline() {
        let (client, requests) = server(vec![
            (200, discovery(json!(true)), Duration::from_millis(100)),
            (
                200,
                json!({"lease_renewed":true}),
                Duration::from_millis(200),
            ),
        ])
        .await;
        assert!(matches!(
            client
                .renew_activity_task_lease("task", "attempt", "owner", Duration::from_millis(200))
                .await,
            Err(Error::Timeout)
        ));
        assert_eq!(requests.await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn renewal_preserves_typed_server_ownership_refusal() {
        let (client, requests) = server(vec![
            (200, discovery(json!(true)), Duration::ZERO),
            (409, json!({"reason":"lease_expired","task_id":"task","activity_attempt_id":"attempt","can_continue":false}), Duration::ZERO),
        ])
        .await;
        let Err(Error::ActivityTaskRejected(rejection)) = client
            .renew_activity_task_lease("task", "attempt", "owner", Duration::from_secs(1))
            .await
        else {
            panic!("expected typed expired-lease refusal")
        };
        assert_eq!(rejection.operation, "renew_lease");
        assert_eq!(rejection.status, 409);
        assert_eq!(rejection.reason, "lease_expired");
        assert_eq!(rejection.task_id, "task");
        assert_eq!(rejection.activity_attempt_id, "attempt");
        assert_eq!(rejection.can_continue, Some(false));
        assert_eq!(requests.await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn optional_renewal_keeps_the_clients_selected_protocol() {
        let (mut client, requests) = server(vec![
            (200, discovery(json!(true)), Duration::ZERO),
            (200, json!({}), Duration::ZERO),
        ])
        .await;
        client.worker_protocol_version = WorkerProtocolVersion::V119;
        client
            .renew_activity_task_lease("0", "0", "0", Duration::from_secs(1))
            .await
            .unwrap();
        for request in requests.await.unwrap() {
            assert!(request
                .headers
                .contains("x-durable-workflow-protocol-version: 1.19\r\n"));
        }
    }
}
