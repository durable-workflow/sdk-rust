//! Keep activity input loading inside the original claim and deadline bounds.

use super::*;

const STOPPED: &str = "activity_hydration_stopped: Worker stopped before activity input finished";

pub(super) fn stopped(error: &Error) -> bool {
    matches!(error, Error::WorkerLoop(message) if message == STOPPED)
}

#[derive(Clone, Debug)]
pub(super) struct HydrationClaim {
    task: ActivityTask,
    namespace: String,
    deadline: SystemTime,
    expires: tokio::time::Instant,
    live_observation: bool,
}

fn abandoned(message: &str) -> Error {
    Error::ActivityExecutionAbandoned(format!("activity input hydration: {message}"))
}

impl HydrationClaim {
    pub(super) fn from_poll(
        wire: &Value,
        worker_id: &str,
        client: &Client,
    ) -> Result<Option<Self>> {
        if !runtime_payloads::has_unresolved_payload(
            wire,
            "/worker/activity-tasks/poll",
            RequestProtocol::DefaultWorker,
        ) {
            return Ok(None);
        }
        let raw = &wire["task"];
        let mut task: ActivityTask = serde_json::from_value(raw.clone())?;
        if raw
            .get("namespace")
            .is_some_and(|value| value.as_str() != Some(client.namespace.as_str()))
        {
            return Err(abandoned("poll changed its authenticated namespace"));
        }
        // Validate the real task/attempt/owner even on the local-bound legacy path.
        cooperative_cancellation::ActivityClaimGuard::new(client, &task, worker_id)?;
        let parse = |value: &Value| -> Result<SystemTime> {
            let text = value
                .as_str()
                .ok_or_else(|| abandoned("missing or invalid original deadline"))?;
            let date = DateTime::parse_from_rfc3339(text)
                .map_err(|_| abandoned("original deadline must include a timezone"))?;
            let seconds = u64::try_from(date.timestamp())
                .map_err(|_| abandoned("original deadline precedes the epoch"))?;
            UNIX_EPOCH
                .checked_add(Duration::new(seconds, date.timestamp_subsec_nanos()))
                .ok_or_else(|| abandoned("original deadline is out of range"))
        };
        let mut deadline = parse(&raw["lease_expires_at"])?;
        if let Some(bounds) = raw.get("deadlines").filter(|value| !value.is_null()) {
            if !bounds.is_object() {
                return Err(abandoned("original execution deadlines must be an object"));
            }
            for kind in ["heartbeat", "start_to_close", "schedule_to_close"] {
                if let Some(bound) = bounds.get(kind).filter(|value| !value.is_null()) {
                    deadline = deadline.min(parse(bound)?);
                }
            }
        }
        let monotonic_now = tokio::time::Instant::now();
        let remaining = deadline
            .duration_since(SystemTime::now())
            .unwrap_or_default();
        let expires = monotonic_now
            .checked_add(remaining)
            .ok_or_else(|| abandoned("original deadline exceeds the monotonic clock"))?;
        // Retain identity and bounds, never a second copy of application bytes.
        task.arguments = None;
        Ok(Some(Self {
            task,
            namespace: client.namespace.clone(),
            deadline,
            expires,
            live_observation: client.worker_protocol_version == WorkerProtocolVersion::V120,
        }))
    }

    fn check_local(&self, client: &Client) -> Result<()> {
        if self.namespace != client.namespace {
            return Err(abandoned("authenticated namespace changed"));
        }
        if client
            .worker_storage_admission
            .as_ref()
            .is_some_and(|value| value.stop.load(Ordering::SeqCst))
        {
            return Err(Error::WorkerLoop(STOPPED.to_owned()));
        }
        // Check both clocks. A backwards wall-clock adjustment cannot extend the
        // original wait; a forwards adjustment cannot permit an expired bound.
        if SystemTime::now() >= self.deadline || tokio::time::Instant::now() >= self.expires {
            return Err(abandoned("original lease or execution deadline elapsed"));
        }
        Ok(())
    }

    pub(super) async fn confirm(&self, client: &Client) -> Result<()> {
        self.check_local(client)?;
        if self.live_observation {
            cooperative_cancellation::ActivityClaimGuard::new(
                client,
                &self.task,
                self.task.lease_owner.as_deref().unwrap_or_default(),
            )?
            .observe()
            .await?;
        }
        self.check_local(client)
    }

    pub(super) async fn protect<T>(
        claim: Option<&Self>,
        client: &Client,
        loading: impl Future<Output = Result<T>>,
    ) -> Result<T> {
        let Some(claim) = claim else {
            return loading.await;
        };
        let stopped = async {
            match &client.worker_storage_admission {
                Some(value) => wait_for_worker_stop(&value.stop).await,
                None => std::future::pending::<()>().await,
            }
        };
        let protected = async {
            claim.confirm(client).await?;
            tokio::pin!(loading);
            let mut observation = tokio::time::interval(Duration::from_secs(1));
            observation.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            observation.tick().await;
            loop {
                tokio::select! {
                    biased;
                    _ = observation.tick() => claim.confirm(client).await?,
                    result = &mut loading => {
                        let value = result?;
                        claim.confirm(client).await?;
                        return Ok(value);
                    }
                }
            }
        };
        // One total budget, including observation, with no spawned observer.
        tokio::select! {
            biased;
            _ = stopped => Err(Error::WorkerLoop(STOPPED.to_owned())),
            _ = tokio::time::sleep_until(claim.expires) => Err(abandoned("original lease or execution deadline elapsed")),
            result = tokio::time::timeout(client.hydration_timeout, protected) => result.map_err(|_| Error::Timeout)?,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn wire() -> Value {
        json!({"task":{"task_id":"task-1","activity_type":"activity-1",
            "activity_attempt_id":"attempt-1","lease_owner":"worker-1","payload_codec":"avro",
            "lease_expires_at":"2099-01-01T00:00:00.123456Z",
            "arguments":{"codec":"avro","external_payload":{}},
            "deadlines":{"start_to_close":"2099-01-01T01:00:00Z","schedule_to_close":"2099-01-01T02:00:00Z"}}})
    }
    fn receipt() -> Value {
        json!({"task_id":"task-1","activity_attempt_id":"attempt-1","lease_owner":"worker-1",
            "can_continue":true,"cancel_requested":false,"heartbeat_recorded":false,"reason":null,
            "task_status":"leased","attempt_status":"running","activity_status":"running",
            "lease_expires_at":"2099-01-01T00:00:00.123456Z","deadlines":{}})
    }
    fn client(url: String) -> Client {
        Client::builder(url)
            .namespace("namespace-1")
            .build()
            .unwrap()
    }
    async fn server(replies: Vec<(u16, Value)>) -> (Client, tokio::task::JoinHandle<Vec<Value>>) {
        server_delayed(replies, Duration::ZERO).await
    }

    async fn server_delayed(
        replies: Vec<(u16, Value)>,
        last_delay: Duration,
    ) -> (Client, tokio::task::JoinHandle<Vec<Value>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = client(format!("http://{}", listener.local_addr().unwrap()));
        let reply_count = replies.len();
        let requests = tokio::spawn(async move {
            let mut captured = Vec::new();
            for (status, reply) in replies {
                let (mut stream, _) =
                    tokio::time::timeout(Duration::from_secs(5), listener.accept())
                        .await
                        .unwrap()
                        .unwrap();
                let mut bytes = Vec::new();
                let (headers, offset, length) = loop {
                    let mut buf = [0; 4096];
                    let count = stream.read(&mut buf).await.unwrap();
                    assert!(count > 0);
                    bytes.extend_from_slice(&buf[..count]);
                    if let Some(offset) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
                        let headers =
                            String::from_utf8_lossy(&bytes[..offset]).to_ascii_lowercase();
                        let length: usize = headers
                            .lines()
                            .find_map(|line| line.strip_prefix("content-length: "))
                            .unwrap()
                            .parse()
                            .unwrap();
                        break (headers, offset + 4, length);
                    }
                };
                while bytes.len() < offset + length {
                    let mut buf = [0; 4096];
                    let count = stream.read(&mut buf).await.unwrap();
                    assert!(count > 0);
                    bytes.extend_from_slice(&buf[..count]);
                }
                assert!(headers.starts_with("post /api/worker/activity-tasks/task-1/status "));
                assert!(headers.contains("x-namespace: namespace-1\r\n"));
                assert!(headers.contains("x-durable-workflow-protocol-version: 1.20\r\n"));
                captured.push(serde_json::from_slice(&bytes[offset..offset + length]).unwrap());
                if captured.len() == reply_count {
                    tokio::time::sleep(last_delay).await;
                }
                let body = reply.to_string();
                stream.write_all(format!("HTTP/1.1 {status} Reply\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
            }
            captured
        });
        (client, requests)
    }
    fn claim(wire: &Value, client: &Client) -> HydrationClaim {
        HydrationClaim::from_poll(wire, "worker-1", client)
            .unwrap()
            .unwrap()
    }
    struct LoadingDrop(Arc<AtomicBool>);
    impl Drop for LoadingDrop {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[test]
    fn external_input_requires_the_real_claim_and_original_bounds() {
        let client = client("http://127.0.0.1:1".into());
        for field in [
            "task_id",
            "activity_attempt_id",
            "lease_owner",
            "lease_expires_at",
        ] {
            let mut raw = wire();
            raw["task"].as_object_mut().unwrap().remove(field);
            assert!(
                HydrationClaim::from_poll(&raw, "worker-1", &client).is_err(),
                "{field}"
            );
        }
        for (field, value) in [
            ("lease_owner", json!("other-worker")),
            ("namespace", json!("other-ns")),
            ("attempt_id", json!("other-attempt")),
            ("lease_expires_at", json!("2099-01-01 00:00:00")),
            ("deadlines", json!([])),
        ] {
            let mut raw = wire();
            raw["task"][field] = value;
            assert!(
                HydrationClaim::from_poll(&raw, "worker-1", &client).is_err(),
                "{field}"
            );
        }
        let mut raw = wire();
        raw["task"]["deadlines"]["heartbeat"] = json!("2098-01-01T00:00:00Z");
        assert!(claim(&raw, &client).deadline < claim(&wire(), &client).deadline);
        let mut inline = wire();
        inline["task"]["arguments"] = json!({"codec":"avro","blob":"inline"});
        inline["task"]["business_metadata"] = json!({"external_payload":"business value"});
        assert!(HydrationClaim::from_poll(&inline, "worker-1", &client)
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn observes_before_during_and_after_loading_without_heartbeats() {
        let (client, requests) = server(vec![(200, receipt()); 3]).await;
        let claim = claim(&wire(), &client);
        let value = HydrationClaim::protect(Some(&claim), &client, async {
            tokio::time::sleep(Duration::from_millis(1100)).await;
            Ok(42)
        })
        .await
        .unwrap();
        assert_eq!(value, 42);
        let requests = requests.await.unwrap();
        assert_eq!(requests.len(), 3);
        for body in requests {
            assert_eq!(
                body,
                json!({"activity_attempt_id":"attempt-1","lease_owner":"worker-1"})
            );
        }
    }

    #[tokio::test]
    async fn unsupported_unconfirmed_or_revoked_authority_never_starts_loading() {
        let mut mismatched = receipt();
        mismatched["activity_attempt_id"] = json!("other-attempt");
        let mut revoked = receipt();
        revoked["can_continue"] = json!(false);
        revoked["reason"] = json!("lease_expired");
        for (status, reply) in [
            (422, json!({"reason":"activity_attempt_status_unavailable"})),
            (503, json!({"reason":"busy"})),
            (200, mismatched),
            (200, revoked),
        ] {
            let (client, requests) = server(vec![(status, reply)]).await;
            let claim = claim(&wire(), &client);
            let started = AtomicBool::new(false);
            assert!(matches!(
                HydrationClaim::protect(Some(&claim), &client, async {
                    started.store(true, Ordering::SeqCst);
                    Ok(())
                })
                .await,
                Err(Error::ActivityExecutionAbandoned(_))
            ));
            assert!(!started.load(Ordering::SeqCst));
            assert_eq!(requests.await.unwrap().len(), 1);
        }
    }

    #[tokio::test]
    async fn revocation_drops_partial_loading_and_final_confirmation_refuses_return() {
        for during in [false, true] {
            let mut revoked = receipt();
            revoked["can_continue"] = json!(false);
            revoked["reason"] = json!("task_not_leased");
            let (client, requests) = server(vec![(200, receipt()), (200, revoked)]).await;
            let claim = claim(&wire(), &client);
            let dropped = Arc::new(AtomicBool::new(false));
            let release = LoadingDrop(dropped.clone());
            let result = HydrationClaim::protect(Some(&claim), &client, async move {
                let _release = release;
                if during {
                    std::future::pending::<()>().await;
                }
                Ok(42)
            })
            .await;
            assert!(matches!(result, Err(Error::ActivityExecutionAbandoned(_))));
            assert!(dropped.load(Ordering::SeqCst));
            assert_eq!(requests.await.unwrap().len(), 2);
        }
    }

    #[tokio::test]
    async fn legacy_bounds_do_not_request_unsupported_status_or_reset_after_loading() {
        let mut client = client("http://127.0.0.1:1".into());
        client.worker_protocol_version = WorkerProtocolVersion::V119;
        let mut claim = claim(&wire(), &client);
        assert!(!claim.live_observation);
        assert_eq!(
            HydrationClaim::protect(Some(&claim), &client, async { Ok(42) })
                .await
                .unwrap(),
            42
        );
        // Retained wall-clock bounds remain decisive, including equality, even
        // when the monotonic bound has not elapsed (a forwards clock change).
        claim.deadline = SystemTime::now();
        assert!(matches!(
            claim.confirm(&client).await,
            Err(Error::ActivityExecutionAbandoned(_))
        ));
        let mut claim = super::tests::claim(&wire(), &client);
        // Conversely, monotonic expiry remains decisive if wall time moves back.
        claim.expires = tokio::time::Instant::now() + Duration::from_millis(30);
        let dropped = Arc::new(AtomicBool::new(false));
        let release = LoadingDrop(dropped.clone());
        let result = HydrationClaim::protect(Some(&claim), &client, async move {
            let _release = release;
            std::future::pending::<Result<()>>().await
        })
        .await;
        assert!(matches!(result, Err(Error::ActivityExecutionAbandoned(_))));
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn shutdown_and_poll_drop_release_loading_without_a_detached_observer() {
        for shutdown in [false, true] {
            let (mut client, requests) = server(vec![(200, receipt())]).await;
            let stop = Arc::new(AtomicBool::new(false));
            client.worker_storage_admission = Some(WorkerStorageAdmission {
                policy: WorkerRetryPolicy::default(),
                stop: stop.clone(),
                workflow_completion_retry_deadline: None,
            });
            let claim = claim(&wire(), &client);
            let dropped = Arc::new(AtomicBool::new(false));
            let release = LoadingDrop(dropped.clone());
            {
                let loading = async move {
                    let _release = release;
                    std::future::pending::<Result<()>>().await
                };
                let poll = HydrationClaim::protect(Some(&claim), &client, loading);
                tokio::pin!(poll);
                tokio::select! {
                    result=&mut poll=>panic!("finished before stop: {result:?}"),
                    _=tokio::time::sleep(Duration::from_millis(100))=>{}
                }
                if shutdown {
                    stop.store(true, Ordering::SeqCst);
                    assert!(stopped(&poll.await.unwrap_err()));
                }
            }
            assert!(dropped.load(Ordering::SeqCst));
            assert_eq!(requests.await.unwrap().len(), 1);
        }
    }

    #[tokio::test]
    async fn original_transport_budget_expiry_is_distinct_from_authority_loss() {
        let mut client = client("http://127.0.0.1:1".into());
        client.worker_protocol_version = WorkerProtocolVersion::V119;
        client.hydration_timeout = Duration::from_millis(30);
        let claim = claim(&wire(), &client);
        assert!(matches!(
            HydrationClaim::protect(Some(&claim), &client, std::future::pending::<Result<()>>())
                .await,
            Err(Error::Timeout)
        ));
        assert!(claim.check_local(&client).is_ok());
    }

    #[tokio::test]
    async fn original_deadline_interrupts_an_inflight_authority_observation() {
        let (client, requests) =
            server_delayed(vec![(200, receipt()); 2], Duration::from_secs(3)).await;
        let mut claim = claim(&wire(), &client);
        assert_eq!(
            claim
                .deadline
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .subsec_nanos(),
            123_456_000
        );
        claim.expires = tokio::time::Instant::now() + Duration::from_millis(1100);
        let dropped = Arc::new(AtomicBool::new(false));
        let release = LoadingDrop(dropped.clone());
        let result = tokio::time::timeout(
            Duration::from_millis(1500),
            HydrationClaim::protect(Some(&claim), &client, async move {
                let _release = release;
                std::future::pending::<Result<()>>().await
            }),
        )
        .await
        .expect("original deadline cannot wait for a slow status response");
        assert!(matches!(result, Err(Error::ActivityExecutionAbandoned(_))));
        assert!(dropped.load(Ordering::SeqCst));
        requests.abort();
        assert!(requests.await.unwrap_err().is_cancelled());
    }

    #[tokio::test]
    async fn ordinary_callback_requires_confirmation_after_decoding() {
        let mut revoked = receipt();
        revoked["can_continue"] = json!(false);
        revoked["reason"] = json!("task_not_leased");
        let (client, requests) = server(vec![(200, revoked)]).await;
        let claim = claim(&wire(), &client);
        let mut task = claim.task.clone();
        task.arguments =
            Some(serde_json::to_value(PayloadEnvelope::avro(&json!([])).unwrap()).unwrap());
        let entered = Arc::new(AtomicBool::new(false));
        let callback_entered = entered.clone();
        let mut worker = Worker::new(client, "queue-1").worker_id("worker-1");
        worker.register_activity("activity-1", move |_, _| {
            callback_entered.store(true, Ordering::SeqCst);
            async { Ok(json!(42)) }
        });
        let result = worker
            .execute_session_activity_task(task, None, Some(&claim))
            .await;
        assert!(matches!(result, Err(Error::ActivityExecutionAbandoned(_))));
        assert!(!entered.load(Ordering::SeqCst));
        assert_eq!(requests.await.unwrap().len(), 1);
    }
}
