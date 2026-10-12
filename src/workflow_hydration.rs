//! Protect claimed input/history loading without a detached renewal task.

use super::*;

pub(super) struct HydrationClaim {
    task: WorkflowTask,
    pending: Option<CancellationRequest>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn wire() -> Value {
        json!({"task": {"task_id":"task-1", "run_id":"run-1", "workflow_type":"example",
            "namespace":"example-ns", "payload_codec":"avro", "lease_owner":"worker-1",
            "workflow_task_attempt":7, "next_history_page_token":"page-2", "history_events":[]}})
    }

    fn receipt() -> Value {
        json!({"task_id":"task-1", "workflow_task_attempt":7, "lease_owner":"worker-1",
            "renewed":true, "reason":null, "task_status":"leased", "run_status":"waiting",
            "lease_expires_at":"2099-01-01T00:00:00Z", "cancellation_request":null})
    }

    fn claim(wire: &Value) -> Option<HydrationClaim> {
        HydrationClaim::from_poll(wire, "worker-1", "example-ns").unwrap()
    }

    async fn server(replies: Vec<(u16, Value)>) -> (String, tokio::task::JoinHandle<Vec<Value>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let requests = tokio::spawn(async move {
            let mut captured = Vec::new();
            for (status, reply) in replies {
                let (mut stream, _) =
                    tokio::time::timeout(Duration::from_secs(8), listener.accept())
                        .await
                        .unwrap()
                        .unwrap();
                let mut request = Vec::new();
                let (headers, offset, length) = loop {
                    let mut bytes = [0; 4096];
                    let count = stream.read(&mut bytes).await.unwrap();
                    assert!(count > 0);
                    request.extend_from_slice(&bytes[..count]);
                    if let Some(offset) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n")
                    {
                        let headers =
                            String::from_utf8_lossy(&request[..offset]).to_ascii_lowercase();
                        let length: usize = headers
                            .lines()
                            .find_map(|line| line.strip_prefix("content-length: "))
                            .unwrap()
                            .parse()
                            .unwrap();
                        break (headers, offset + 4, length);
                    }
                };
                while request.len() < offset + length {
                    let mut bytes = [0; 4096];
                    let count = stream.read(&mut bytes).await.unwrap();
                    assert!(count > 0);
                    request.extend_from_slice(&bytes[..count]);
                }
                assert!(headers.starts_with("post /api/worker/workflow-tasks/task-1/heartbeat "));
                assert!(headers.contains("x-namespace: example-ns\r\n"));
                captured.push(json!({"headers":headers,"body":serde_json::from_slice::<Value>(&request[offset..offset + length]).unwrap()}));
                let reply = reply.to_string();
                stream.write_all(format!("HTTP/1.1 {status} Reply\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{reply}", reply.len()).as_bytes()).await.unwrap();
            }
            captured
        });
        (url, requests)
    }

    fn client(url: String) -> Client {
        Client::builder(url)
            .namespace("example-ns")
            .build()
            .unwrap()
    }

    #[test]
    fn unhydrated_claim_requires_actual_namespace_owner_and_attempt() {
        for field in [
            "namespace",
            "lease_owner",
            "workflow_task_attempt",
            "task_id",
            "run_id",
        ] {
            let mut raw = wire();
            raw["task"].as_object_mut().unwrap().remove(field);
            assert!(
                HydrationClaim::from_poll(&raw, "worker-1", "example-ns").is_err(),
                "{field}"
            );
        }
        let mut raw = wire();
        raw["task"]["namespace"] = json!("other-ns");
        assert!(HydrationClaim::from_poll(&raw, "worker-1", "example-ns").is_err());
        assert!(HydrationClaim::from_poll(&wire(), "other-worker", "example-ns").is_err());
    }

    #[test]
    fn inline_business_values_do_not_create_transport_observers() {
        let mut raw = wire();
        raw["task"]["next_history_page_token"] = Value::Null;
        raw["task"]["business_metadata"] = json!({"external_payload":"business value"});
        assert!(claim(&raw).is_none());
        raw["task"]["arguments"] = json!({"codec":"avro", "external_payload":{}});
        assert!(claim(&raw).is_some());
    }

    #[tokio::test]
    async fn renews_during_loading_and_confirms_before_returning() {
        let (url, requests) =
            server(vec![(200, receipt()), (200, receipt()), (200, receipt())]).await;
        let (value, pending) = HydrationClaim::protect(claim(&wire()), &client(url), async {
            tokio::time::sleep(Duration::from_millis(5100)).await;
            Ok(42)
        })
        .await
        .unwrap();
        assert_eq!(value, 42);
        assert!(pending.is_none());
        let requests = requests.await.unwrap();
        assert_eq!(requests.len(), 3);
        for request in requests {
            assert_eq!(
                request["body"],
                json!({"lease_owner":"worker-1", "workflow_task_attempt":7})
            );
            assert!(request["headers"]
                .as_str()
                .unwrap()
                .contains("x-durable-workflow-protocol-version: 1.20\r\n"));
        }
    }

    #[tokio::test]
    async fn unsupported_or_unconfirmed_authority_never_starts_loading() {
        for (status, response) in [
            (409, json!({"reason":"task_not_leased"})),
            (200, {
                let mut reply = receipt();
                reply["workflow_task_attempt"] = json!(8);
                reply
            }),
        ] {
            let (url, requests) = server(vec![(status, response)]).await;
            let started = AtomicBool::new(false);
            assert!(
                HydrationClaim::protect(claim(&wire()), &client(url), async {
                    started.store(true, Ordering::SeqCst);
                    Ok(())
                })
                .await
                .is_err()
            );
            assert!(!started.load(Ordering::SeqCst));
            assert_eq!(requests.await.unwrap().len(), 1);
        }
    }

    struct LoadingDrop(Arc<AtomicBool>);
    impl Drop for LoadingDrop {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn stopped_worker_drops_loading_without_retained_renewal() {
        let (url, requests) = server(vec![(200, receipt())]).await;
        let mut client = client(url);
        let stop = Arc::new(AtomicBool::new(false));
        client.worker_storage_admission = Some(WorkerStorageAdmission {
            policy: WorkerRetryPolicy::default(),
            stop: stop.clone(),
        });
        let dropped = Arc::new(AtomicBool::new(false));
        let release = LoadingDrop(dropped.clone());
        let stop_loading = async {
            tokio::time::sleep(Duration::from_millis(100)).await;
            stop.store(true, Ordering::SeqCst);
        };
        let loading = async move {
            let _release = release;
            std::future::pending::<Result<()>>().await
        };
        let (result, ()) = tokio::join!(
            HydrationClaim::protect(claim(&wire()), &client, loading),
            stop_loading
        );
        assert!(result.unwrap_err().to_string().contains("Worker stopped"));
        assert!(dropped.load(Ordering::SeqCst));
        assert_eq!(requests.await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn explicit_hydration_budget_is_not_restarted() {
        let (url, requests) = server(vec![(200, receipt())]).await;
        let client = Client::builder(url)
            .namespace("example-ns")
            .timeout(Duration::from_millis(150))
            .build()
            .unwrap();
        let started = Instant::now();
        let result = HydrationClaim::protect(
            claim(&wire()),
            &client,
            std::future::pending::<Result<()>>(),
        )
        .await;
        assert!(matches!(result, Err(Error::Timeout)));
        assert!(started.elapsed() < Duration::from_secs(1));
        assert_eq!(requests.await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn pending_cancellation_preserves_identity_and_deadline_without_delivery() {
        let observation = json!({"request_id":"request-root", "requested_at":"2026-10-12T21:00:00Z",
            "cleanup_deadline_at":"2026-10-12T21:00:30Z", "history_refresh_page_token":"root-page"});
        let mut reply = receipt();
        reply["cancellation_request"] = observation.clone();
        let (url, requests) = server(vec![(200, reply.clone()), (200, reply)]).await;
        let mut client = client(url);
        client.cooperative_worker_protocol = true;
        let (_, pending) = HydrationClaim::protect(claim(&wire()), &client, async { Ok(()) })
            .await
            .unwrap();
        assert_eq!(
            pending,
            Some(CancellationRequest::from_observation(&observation).unwrap())
        );
        assert_eq!(requests.await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn explicit_protocol_119_is_preserved() {
        let (url, requests) = server(vec![(200, receipt()), (200, receipt())]).await;
        let client = Client::builder(url)
            .namespace("example-ns")
            .worker_protocol_version(WorkerProtocolVersion::V119)
            .build()
            .unwrap();
        HydrationClaim::protect(claim(&wire()), &client, async { Ok(()) })
            .await
            .unwrap();
        for request in requests.await.unwrap() {
            assert!(request["headers"]
                .as_str()
                .unwrap()
                .contains("x-durable-workflow-protocol-version: 1.19\r\n"));
        }
    }
}

fn invalid(detail: &str) -> Error {
    Error::WorkerLoop(format!(
        "workflow_hydration_authority_unconfirmed: {detail}"
    ))
}

impl HydrationClaim {
    pub(super) fn from_poll(
        wire: &Value,
        worker_id: &str,
        namespace: &str,
    ) -> Result<Option<Self>> {
        let Some(task) = wire.get("task").filter(|task| !task.is_null()) else {
            return Ok(None);
        };
        // Inline-only tasks need no transport observation. Paged tasks can
        // contain external references that are not visible in the first page.
        if !task
            .get("next_history_page_token")
            .and_then(Value::as_str)
            .is_some_and(|token| !token.is_empty())
            && !matches!(
                task["sticky_replay_mode"].as_str(),
                Some("sticky_hit_expected" | "forced_cold_replay")
            )
            && !runtime_payloads::has_unresolved_payload(
                wire,
                "/worker/workflow-tasks/poll",
                RequestProtocol::DefaultWorker,
            )
        {
            return Ok(None);
        }
        let mut task: WorkflowTask = serde_json::from_value(task.clone())?;
        if wire["task"]["namespace"].as_str() != Some(namespace)
            || task.lease_owner.as_deref() != Some(worker_id)
            || wire["task"]["workflow_task_attempt"].as_u64() != Some(task.workflow_task_attempt)
        {
            return Err(invalid(
                "poll must identify this namespace, owner and its actual attempt",
            ));
        }
        cooperative_cancellation::cancellation_claim(&task)?;
        // Keep only authority, not another copy of application/history data.
        task.arguments = None;
        task.signal_arguments = None;
        task.history_events.clear();
        let pending = match wire["task"].get("cancellation_request") {
            None | Some(Value::Null) => None,
            Some(value) => Some(CancellationRequest::from_observation(value)?),
        };
        Ok(Some(Self { task, pending }))
    }

    async fn confirm(&mut self, client: &Client) -> Result<()> {
        let receipt = client
            .heartbeat_workflow_task_with_protocol(
                &self.task,
                self.pending.as_ref(),
                client.worker_protocol_version.as_str(),
            )
            .await?;
        self.pending = receipt.cancellation_request;
        if self.pending.is_some() && !client.cooperative_worker_protocol {
            return Err(invalid(
                "pending cancellation requires a cooperative Worker",
            ));
        }
        Ok(())
    }

    pub(super) async fn protect<T>(
        mut claim: Option<Self>,
        client: &Client,
        loading: impl Future<Output = Result<T>>,
    ) -> Result<(T, Option<CancellationRequest>)> {
        let Some(claim) = claim.as_mut() else {
            return Ok((loading.await?, None));
        };
        let stop = client
            .worker_storage_admission
            .as_ref()
            .map(|value| &*value.stop);
        let stopped = async {
            match stop {
                Some(stop) => wait_for_worker_stop(stop).await,
                None => std::future::pending::<()>().await,
            }
        };
        tokio::pin!(stopped);
        // One original total hydration budget includes confirmation, every
        // payload body and all remaining history pages. Renewals cannot reset it.
        let protected = async {
            claim.confirm(client).await?;
            tokio::pin!(loading);
            let mut renew = tokio::time::interval(Duration::from_secs(5));
            renew.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            renew.tick().await;
            loop {
                tokio::select! {
                    biased;
                    _ = renew.tick() => claim.confirm(client).await?,
                    result = &mut loading => {
                        let result = result?;
                        claim.confirm(client).await?;
                        return Ok((result, claim.pending.clone()));
                    }
                }
            }
        };
        tokio::select! {
            biased;
            _ = &mut stopped => Err(invalid("Worker stopped before hydration finished")),
            result = tokio::time::timeout(client.hydration_timeout, protected) => {
                result.map_err(|_| Error::Timeout)?
            }
        }
    }
}
