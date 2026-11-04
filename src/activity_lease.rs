//! Optional runtime lease maintenance, independent of application progress.

use crate::{
    activity_task_response, percent_encode_path_segment, Client, Error, RequestProtocol, Result,
};
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;

pub(crate) mod authority;

const MAX_REQUEST_WINDOW: Duration = Duration::from_secs(5);

#[derive(Debug, Default)]
pub(crate) struct Discovery {
    confirmed: AtomicBool,
    lock: tokio::sync::Mutex<()>,
}

fn unavailable(message: &str) -> Error {
    Error::ActivityLeaseRenewalUnavailable(message.to_owned())
}

impl Client {
    pub(crate) fn observe_activity_lease_capabilities(&self, capabilities: &Value) {
        if capabilities["activity_lease_renewal"].as_bool() == Some(true) {
            self.activity_lease_renewal_supported
                .store(true, Ordering::SeqCst);
        }
    }

    pub(crate) fn observe_worker_activity_lease_capabilities(&self, capabilities: &Value) {
        self.observe_activity_lease_capabilities(capabilities);
        if let Some(advertised) = capabilities.get("activity_lease_renewal") {
            self.activity_lease_discovery
                .confirmed
                .store(advertised.as_bool() == Some(true), Ordering::SeqCst);
        }
    }

    async fn discover_activity_lease_renewal(&self, request_window: Duration) -> Result<()> {
        if self
            .activity_lease_discovery
            .confirmed
            .load(Ordering::SeqCst)
        {
            return Ok(());
        }
        // Clones share discovery, but independent clients and control-plane
        // observations cannot authorize a worker request. Lock waiting remains
        // inside the caller's total renewal window.
        let _discovery = self.activity_lease_discovery.lock.lock().await;
        if self
            .activity_lease_discovery
            .confirmed
            .load(Ordering::SeqCst)
        {
            return Ok(());
        }
        let discovery: Value = self
            .request_json_unresolved_with_timeout::<Value, Value>(
                reqwest::Method::GET,
                "/cluster/info",
                RequestProtocol::DefaultWorker,
                None,
                request_window,
            )
            .await?;
        let capabilities = &discovery["worker_protocol"]["server_capabilities"];
        self.observe_worker_activity_lease_capabilities(capabilities);
        if capabilities["activity_lease_renewal"].as_bool() != Some(true) {
            return Err(unavailable(
                "runtime must explicitly advertise activity_lease_renewal=true",
            ));
        }
        Ok(())
    }

    pub(crate) fn with_activity_lease_authority(
        &self,
        authority: Option<Arc<authority::Authority>>,
    ) -> Self {
        let mut client = self.clone();
        client.activity_lease_authority = authority;
        client
    }

    /// Renew one original activity claim without recording application progress.
    ///
    /// A worker-authenticated registration, poll or discovery must advertise
    /// the boolean `activity_lease_renewal=true`. Clones share this observation
    /// until a renewal fails. Discovery, lock waiting and renewal share
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
            self.discover_activity_lease_renewal(request_window).await?;
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
        if !matches!(&result, Ok(Ok(_))) {
            // Do not replay a refused renewal. A later attempt must recheck
            // capability, while the caller handles this original claim's error.
            self.activity_lease_discovery
                .confirmed
                .store(false, Ordering::SeqCst);
        }
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
    use crate::WorkerProtocolVersion;
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

    async fn renew(client: &Client) -> Result<Value> {
        client
            .renew_activity_task_lease("task", "attempt", "owner", Duration::from_secs(1))
            .await
    }

    #[tokio::test]
    async fn registered_worker_capability_avoids_discovery_across_clones() {
        let (client, requests) = server(vec![
            (200, json!({"registered":true,"worker_id":"worker", "server_capabilities":{"activity_lease_renewal":true}}), Duration::ZERO),
            (200, json!({"lease_renewed":true}), Duration::ZERO),
            (200, json!({"lease_renewed":true}), Duration::ZERO),
        ]).await;
        client
            .register_worker("worker", "queue", vec![], vec![], 16, 16)
            .await
            .unwrap();
        renew(&client).await.unwrap();
        renew(&client.clone()).await.unwrap();
        let requests = requests.await.unwrap();
        assert_eq!(requests.len(), 3);
        assert!(requests[0]
            .headers
            .starts_with("post /api/worker/register "));
        for request in &requests[1..] {
            assert!(request
                .headers
                .starts_with("post /api/worker/activity-tasks/task/renew-lease "));
        }
    }

    #[tokio::test]
    async fn concurrent_first_renewals_share_one_worker_discovery() {
        let mut replies = vec![(200, discovery(json!(true)), Duration::from_millis(20))];
        replies.extend((0..4).map(|_| (200, json!({"lease_renewed":true}), Duration::ZERO)));
        let (client, requests) = server(replies).await;
        let other = client.clone();
        let results = tokio::join!(renew(&client), renew(&other), renew(&client), renew(&other));
        for result in [results.0, results.1, results.2, results.3] {
            result.unwrap();
        }
        let requests = requests.await.unwrap();
        assert_eq!(
            requests
                .iter()
                .filter(|r| r.headers.starts_with("get /api/cluster/info "))
                .count(),
            1
        );
        assert_eq!(
            requests
                .iter()
                .filter(|r| r
                    .headers
                    .starts_with("post /api/worker/activity-tasks/task/renew-lease "))
                .count(),
            4
        );
    }

    #[tokio::test]
    async fn control_discovery_does_not_authorize_cached_worker_renewal() {
        let (client, requests) = server(vec![
            (200, discovery(json!(true)), Duration::ZERO),
            (200, discovery(json!(false)), Duration::ZERO),
        ])
        .await;
        client.cluster_info().await.unwrap();
        assert!(matches!(
            renew(&client).await,
            Err(Error::ActivityLeaseRenewalUnavailable(_))
        ));
        let requests = requests.await.unwrap();
        assert_eq!(requests.len(), 2);
        assert!(requests[0]
            .headers
            .contains("authorization: bearer control-test-token\r\n"));
        assert!(requests[1]
            .headers
            .contains("authorization: bearer worker-test-token\r\n"));
        assert!(requests
            .iter()
            .all(|r| r.headers.starts_with("get /api/cluster/info ")));
    }

    #[tokio::test]
    async fn refused_renewal_invalidates_discovery_without_replaying_the_request() {
        let (client, requests) = server(vec![
            (200, discovery(json!(true)), Duration::ZERO),
            (200, json!({"lease_renewed":true}), Duration::ZERO),
            (409, json!({"reason":"lease_expired","task_id":"task","activity_attempt_id":"attempt","can_continue":false}), Duration::ZERO),
            (200, discovery(json!(false)), Duration::ZERO),
        ]).await;
        renew(&client).await.unwrap();
        assert!(matches!(
            renew(&client).await,
            Err(Error::ActivityTaskRejected(_))
        ));
        assert!(matches!(
            renew(&client).await,
            Err(Error::ActivityLeaseRenewalUnavailable(_))
        ));
        let requests = requests.await.unwrap();
        let methods: Vec<_> = requests
            .iter()
            .map(|r| r.headers.split_whitespace().next().unwrap())
            .collect();
        assert_eq!(methods, ["get", "post", "post", "get"]);
    }

    #[tokio::test]
    async fn discovery_cache_is_not_shared_by_independent_clients() {
        let (client, requests) = server(vec![
            (200, discovery(json!(true)), Duration::ZERO),
            (200, json!({"lease_renewed":true}), Duration::ZERO),
            (200, discovery(json!(false)), Duration::ZERO),
        ])
        .await;
        renew(&client).await.unwrap();
        let independent = Client::builder(&client.base_url)
            .worker_token(Some("worker-test-token".into()))
            .namespace("named-test")
            .build()
            .unwrap();
        assert!(matches!(
            renew(&independent).await,
            Err(Error::ActivityLeaseRenewalUnavailable(_))
        ));
        assert_eq!(requests.await.unwrap().len(), 3);
    }

    #[tokio::test]
    async fn waiting_for_shared_discovery_stays_within_the_request_window() {
        let (client, requests) = server(vec![
            (200, discovery(json!(true)), Duration::from_millis(150)),
            (200, json!({"lease_renewed":true}), Duration::ZERO),
        ])
        .await;
        let (first, waiting) = tokio::join!(
            renew(&client),
            client.renew_activity_task_lease(
                "other",
                "attempt",
                "owner",
                Duration::from_millis(20)
            )
        );
        first.unwrap();
        assert!(matches!(waiting, Err(Error::Timeout)));
        let requests = requests.await.unwrap();
        assert_eq!(requests.len(), 2);
        assert!(requests[1]
            .headers
            .starts_with("post /api/worker/activity-tasks/task/renew-lease "));
    }
}
