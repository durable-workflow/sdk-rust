//! Original query authority, captured before payload loading and never renewed.

use super::*;

#[derive(Clone, Copy, Debug)]
pub(super) struct Lost;

pub(super) struct Authority {
    identity: Value,
    timing: Value,
    bound: chrono::DateTime<chrono::Utc>,
    until: Mutex<tokio::time::Instant>,
    observed: bool,
    observing: AtomicBool,
}

fn wall_now() -> std::result::Result<chrono::DateTime<chrono::Utc>, Lost> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Lost)?;
    DateTime::from_timestamp(
        elapsed.as_secs().try_into().map_err(|_| Lost)?,
        elapsed.subsec_nanos(),
    )
    .ok_or(Lost)
}

fn timestamp(
    value: &Value,
    canonical: bool,
) -> std::result::Result<chrono::DateTime<chrono::Utc>, Lost> {
    let text = value.as_str().ok_or(Lost)?;
    let parsed = DateTime::parse_from_rfc3339(text)
        .map_err(|_| Lost)?
        .to_utc();
    if canonical && parsed.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string() != text {
        return Err(Lost);
    }
    Ok(parsed)
}

impl Authority {
    pub(super) fn capture(
        task: &Value,
        worker: &Worker,
    ) -> std::result::Result<Option<Self>, Lost> {
        let observed = worker.client.worker_protocol_version == WorkerProtocolVersion::V120
            && worker.query_authority_confirmed.load(Ordering::SeqCst);
        if worker.require_query_task_authority && !observed {
            return Err(Lost);
        }
        if !observed
            && ["lease_expires_at", "query_admitted_at", "query_deadline_at"]
                .iter()
                .all(|key| task.get(key).is_none())
        {
            // Old Servers disclose no original budget. This compatibility path
            // cannot establish live authority and never calls the 1.20 status API.
            return Ok(None);
        }
        let namespace = task
            .get("namespace")
            .and_then(Value::as_str)
            .unwrap_or(&worker.client.namespace);
        if namespace != worker.client.namespace
            || task["lease_owner"].as_str() != Some(&worker.worker_id)
            || task["task_queue"].as_str() != Some(&worker.task_queue)
            || task["query_task_id"].as_str().is_none_or(str::is_empty)
            || task["query_task_attempt"]
                .as_u64()
                .is_none_or(|attempt| attempt == 0)
            || (observed
                && ["workflow_id", "run_id"]
                    .iter()
                    .any(|key| task[key].as_str().is_none_or(str::is_empty)))
        {
            return Err(Lost);
        }
        let lease = timestamp(&task["lease_expires_at"], observed)?;
        let mut bound = lease;
        let mut timing = json!({"lease_expires_at":task["lease_expires_at"]});
        if observed
            || task.get("query_admitted_at").is_some()
            || task.get("query_deadline_at").is_some()
        {
            let admitted = timestamp(&task["query_admitted_at"], true)?;
            let deadline = timestamp(&task["query_deadline_at"], true)?;
            if deadline < admitted || (observed && lease > deadline) {
                return Err(Lost);
            }
            timing["query_admitted_at"] = task["query_admitted_at"].clone();
            timing["query_deadline_at"] = task["query_deadline_at"].clone();
            bound = bound.min(deadline);
        }
        let reference = if observed {
            timestamp(&task["server_time"], true)?
        } else {
            wall_now()?
        };
        let remaining = (bound - reference).to_std().map_err(|_| Lost)?;
        let until = tokio::time::Instant::now()
            .checked_add(remaining)
            .ok_or(Lost)?;
        let authority = Self {
            identity: json!({"namespace":namespace,"task_queue":worker.task_queue,
                "workflow_id":task["workflow_id"],"run_id":task["run_id"],
                "query_task_id":task["query_task_id"],"query_task_attempt":task["query_task_attempt"],
                "lease_owner":task["lease_owner"]}),
            timing,
            bound,
            until: Mutex::new(until),
            observed,
            observing: AtomicBool::new(false),
        };
        authority.remaining()?;
        Ok(Some(authority))
    }

    fn expires(&self) -> tokio::time::Instant {
        *self.until.lock().expect("query deadline")
    }

    fn remaining(&self) -> std::result::Result<Duration, Lost> {
        let remaining = self
            .expires()
            .saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            Err(Lost)
        } else {
            Ok(remaining)
        }
    }

    pub(super) async fn confirm(&self, client: &Client) -> std::result::Result<(), Lost> {
        let remaining = self.remaining()?;
        if !self.observed {
            return Ok(());
        }
        struct Observation<'a>(&'a AtomicBool);
        impl Drop for Observation<'_> {
            fn drop(&mut self) {
                self.0.store(false, Ordering::SeqCst);
            }
        }
        // The enclosing supervisor must keep polling an in-flight pre-handler
        // observation rather than starting another one at each interval tick.
        let _observation = Observation(&self.observing);
        self.observing.store(true, Ordering::SeqCst);
        let started = tokio::time::Instant::now();
        let path = format!(
            "/worker/query-tasks/{}/status?lease_owner={}&query_task_attempt={}",
            percent_encode_path_segment(self.identity["query_task_id"].as_str().ok_or(Lost)?),
            percent_encode_path_segment(self.identity["lease_owner"].as_str().ok_or(Lost)?),
            self.identity["query_task_attempt"].as_u64().ok_or(Lost)?
        );
        let request = client.request_json_unresolved_with_timeout::<Value, Value>(
            reqwest::Method::GET,
            &path,
            RequestProtocol::Worker("1.20"),
            None,
            remaining.min(Duration::from_secs(5)),
        );
        let reply = tokio::time::timeout(remaining.min(Duration::from_secs(5)), request)
            .await
            .map_err(|_| Lost)?
            .map_err(|_| Lost)?;
        for key in ["query_task_id", "query_task_attempt", "lease_owner"] {
            if reply[key] != self.identity[key] {
                return Err(Lost);
            }
        }
        for (key, value) in self.timing.as_object().ok_or(Lost)? {
            if reply.get(key) != Some(value) {
                return Err(Lost);
            }
        }
        if reply["can_continue"] != true
            || reply["query_task_status"] != "leased"
            || !reply["reason"].is_null()
        {
            return Err(Lost);
        }
        let reference = timestamp(&reply["server_time"], true)?;
        let remaining = (self.bound - reference).to_std().map_err(|_| Lost)?;
        // Charge the entire round trip, including server wait and response read.
        // A later observation can only shorten the original monotonic ceiling.
        let observed_until = started.checked_add(remaining).ok_or(Lost)?;
        {
            let mut until = self.until.lock().expect("query deadline");
            *until = (*until).min(observed_until);
        }
        self.remaining().map(|_| ())
    }

    pub(super) async fn protect<T>(
        authority: Option<&Self>,
        worker: &Worker,
        operation: impl Future<Output = T>,
    ) -> std::result::Result<T, Lost> {
        let Some(authority) = authority else {
            return Ok(operation.await);
        };
        authority.confirm(&worker.client).await?;
        tokio::pin!(operation);
        let stopped = async {
            match &worker.run_stop {
                Some(stop) => wait_for_worker_stop(stop).await,
                None => std::future::pending::<()>().await,
            }
        };
        tokio::pin!(stopped);
        let mut observation = tokio::time::interval(Duration::from_secs(1));
        observation.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        observation.tick().await;
        loop {
            tokio::select! {
                biased;
                _ = &mut stopped => return Err(Lost),
                _ = tokio::time::sleep_until(authority.expires()) => return Err(Lost),
                _ = observation.tick(), if authority.observed && !authority.observing.load(Ordering::SeqCst) => authority.confirm(&worker.client).await?,
                result = &mut operation => {
                    authority.remaining()?;
                    return Ok(result);
                }
            }
        }
    }
}

pub(super) async fn before_handler(
    authority: Option<&Authority>,
    client: &Client,
) -> std::result::Result<(), QueryTaskExecutionFailure> {
    match authority {
        Some(authority) => authority.confirm(client).await.map_err(|_| {
            QueryTaskExecutionFailure::new(
                "query_authority_lost",
                "Original query authority is unavailable",
                "QueryAuthorityLost",
            )
        }),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn task(budget: Duration) -> Value {
        let now = wall_now().unwrap();
        let deadline = now + chrono::Duration::from_std(budget).unwrap();
        let stamp =
            |time: chrono::DateTime<chrono::Utc>| time.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string();
        let envelope =
            serde_json::to_value(encode_payload(&json!([]), DEFAULT_CODEC).unwrap()).unwrap();
        json!({"query_task_id":"query-1","query_task_attempt":1,"lease_owner":"worker-1",
            "task_queue":"queue-1","workflow_id":"workflow-1","run_id":"run-1",
            "workflow_type":"workflow","query_name":"value","payload_codec":"avro",
            "workflow_arguments":envelope,
            "query_arguments":envelope,"history_events":[],
            "query_admitted_at":stamp(now),"query_deadline_at":stamp(deadline),
            "lease_expires_at":stamp(deadline),"server_time":stamp(now)})
    }

    fn worker(url: &str, observed: bool) -> Worker {
        let client = Client::builder(url)
            .namespace("namespace-1")
            .build()
            .unwrap();
        let worker = Worker::new(client, "queue-1").worker_id("worker-1");
        worker
            .query_authority_confirmed
            .store(observed, Ordering::SeqCst);
        worker
    }

    #[test]
    fn validates_original_claim_and_canonical_immutable_timing() {
        let worker = worker("http://127.0.0.1:1", true);
        let original = task(Duration::from_secs(5));
        assert!(Authority::capture(&original, &worker).unwrap().is_some());
        for (field, value) in [
            ("namespace", json!("foreign")),
            ("task_queue", json!("foreign")),
            ("lease_owner", json!("other")),
            ("query_task_attempt", json!(0)),
            ("query_task_id", json!("")),
            ("workflow_id", Value::Null),
            ("query_deadline_at", json!("2099-01-01T00:00:00Z")),
            ("query_admitted_at", json!("2099-01-01T00:00:00.000000Z")),
            ("server_time", json!("2026-01-01T00:00:00.000000+00:00")),
        ] {
            let mut invalid = original.clone();
            invalid[field] = value;
            assert!(Authority::capture(&invalid, &worker).is_err(), "{field}");
        }
        assert!(Authority::capture(&task(Duration::ZERO), &worker).is_err());
    }

    #[test]
    fn legacy_without_timing_stays_explicitly_weaker_and_strong_mode_refuses_it() {
        let mut worker = worker("http://127.0.0.1:1", false);
        let legacy = json!({"query_task_id":"old"});
        assert!(Authority::capture(&legacy, &worker).unwrap().is_none());
        worker.require_query_task_authority = true;
        assert!(Authority::capture(&legacy, &worker).is_err());
    }

    struct Server {
        url: String,
        paths: Arc<Mutex<Vec<String>>>,
        handle: tokio::task::JoinHandle<()>,
    }
    impl Drop for Server {
        fn drop(&mut self) {
            self.handle.abort();
        }
    }
    async fn server(
        task: Value,
        status_change: Option<(&'static str, Value)>,
        status_delay: Duration,
    ) -> Server {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let paths = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&paths);
        let handle = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                let mut buffer = [0u8; 4096];
                loop {
                    let count = socket.read(&mut buffer).await.unwrap();
                    if count == 0 {
                        break;
                    }
                    bytes.extend_from_slice(&buffer[..count]);
                    if bytes.windows(4).any(|part| part == b"\r\n\r\n") {
                        break;
                    }
                }
                let request = String::from_utf8_lossy(&bytes);
                let path = request
                    .lines()
                    .next()
                    .unwrap()
                    .split_whitespace()
                    .nth(1)
                    .unwrap()
                    .to_owned();
                captured.lock().unwrap().push(path.clone());
                let mut reply = if path.ends_with("/poll") {
                    json!({"task":task})
                } else if path.contains("/status?") {
                    tokio::time::sleep(status_delay).await;
                    let mut reply = task.clone();
                    reply["query_task_status"] = json!("leased");
                    reply["can_continue"] = json!(true);
                    reply["reason"] = Value::Null;
                    reply["server_time"] = json!(wall_now()
                        .unwrap()
                        .format("%Y-%m-%dT%H:%M:%S%.6fZ")
                        .to_string());
                    if let Some((key, value)) = &status_change {
                        reply[*key] = value.clone();
                    }
                    reply
                } else {
                    json!({"recorded":true})
                };
                if path == "/api/cluster/info" {
                    reply = json!({"worker_protocol":{"server_capabilities":{}}});
                }
                let body = reply.to_string();
                let response = format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{}", body.len(), body);
                let _ = socket.write_all(response.as_bytes()).await;
            }
        });
        Server { url, paths, handle }
    }

    #[tokio::test]
    async fn observed_refusal_or_changed_timing_never_invokes_handler_or_acknowledges() {
        for change in [
            Some(("can_continue", json!(false))),
            Some(("query_task_attempt", json!(2))),
            Some(("query_deadline_at", json!("2099-01-01T00:00:00.000000Z"))),
            Some(("lease_owner", json!("replacement"))),
        ] {
            let server = server(task(Duration::from_secs(5)), change, Duration::ZERO).await;
            let mut worker = worker(&server.url, true);
            worker.register_workflow("workflow", |_, _| async { Ok(Value::Null) });
            worker.register_query("workflow", "value", |_, _| {
                panic!("refused query callback");
                #[allow(unreachable_code)]
                async {
                    Ok(Value::Null)
                }
            });
            assert!(matches!(
                worker.poll_query_once().await.unwrap(),
                ManagedPollOutcome::Handled
            ));
            let paths = server.paths.lock().unwrap();
            assert_eq!(paths.len(), 2);
            assert!(paths[1].contains("/status?lease_owner=worker-1&query_task_attempt=1"));
        }
    }

    #[tokio::test]
    async fn timely_handler_completes_once_without_mutating_direct_client_receipts() {
        let server = server(task(Duration::from_secs(5)), None, Duration::ZERO).await;
        let mut worker = worker(&server.url, true);
        worker.register_workflow("workflow", |_, _| async { Ok(Value::Null) });
        worker.register_query("workflow", "value", |_, _| async {
            Ok(json!([7, null, "雪"]))
        });
        assert!(matches!(
            worker.poll_query_once().await.unwrap(),
            ManagedPollOutcome::Handled
        ));
        assert_eq!(
            server
                .paths
                .lock()
                .unwrap()
                .iter()
                .filter(|path| path.ends_with("/complete"))
                .count(),
            1
        );
        worker
            .client
            .complete_query_task("query-1", "worker-1", 1, json!(7), DEFAULT_CODEC)
            .await
            .unwrap();
        assert_eq!(
            server
                .paths
                .lock()
                .unwrap()
                .iter()
                .filter(|path| path.ends_with("/complete"))
                .count(),
            2
        );
    }

    #[tokio::test]
    async fn yielding_handler_is_dropped_at_original_expiry_without_failure_ack() {
        let server = server(task(Duration::from_millis(200)), None, Duration::ZERO).await;
        let mut worker = worker(&server.url, true);
        worker.register_workflow("workflow", |_, _| async { Ok(Value::Null) });
        let dropped = Arc::new(AtomicBool::new(false));
        struct Dropped(Arc<AtomicBool>);
        impl Drop for Dropped {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        let flag = Arc::clone(&dropped);
        worker.register_query("workflow", "value", move |_, _| {
            let flag = Arc::clone(&flag);
            async move {
                let _drop = Dropped(flag);
                std::future::pending::<()>().await;
                Ok(Value::Null)
            }
        });
        assert!(matches!(
            worker.poll_query_once().await.unwrap(),
            ManagedPollOutcome::Handled
        ));
        assert!(dropped.load(Ordering::SeqCst));
        assert!(!server
            .paths
            .lock()
            .unwrap()
            .iter()
            .any(|path| path.ends_with("/complete") || path.ends_with("/fail")));
    }

    #[tokio::test]
    async fn synchronous_callback_result_is_fenced_after_expiry() {
        let server = server(task(Duration::from_millis(200)), None, Duration::ZERO).await;
        let mut worker = worker(&server.url, true);
        worker.register_workflow("workflow", |_, _| async { Ok(Value::Null) });
        worker.register_query("workflow", "value", |_, _| {
            std::thread::sleep(Duration::from_millis(250));
            async { Ok(json!(7)) }
        });
        assert!(matches!(
            worker.poll_query_once().await.unwrap(),
            ManagedPollOutcome::Handled
        ));
        assert!(!server
            .paths
            .lock()
            .unwrap()
            .iter()
            .any(|path| path.ends_with("/complete") || path.ends_with("/fail")));
    }

    #[tokio::test]
    async fn status_round_trip_shortens_budget_and_never_extends_original_ceiling() {
        let task = task(Duration::from_secs(5));
        let server = server(task.clone(), None, Duration::from_millis(80)).await;
        let worker = worker(&server.url, true);
        let authority = Authority::capture(&task, &worker).unwrap().unwrap();
        let original = authority.expires();
        authority.confirm(&worker.client).await.unwrap();
        assert!(authority.expires() <= original - Duration::from_millis(60));
    }

    #[tokio::test]
    async fn required_authority_refuses_discovery_before_registration_post() {
        let server = server(Value::Null, None, Duration::ZERO).await;
        let worker = worker(&server.url, false).require_query_task_authority(true);
        assert!(worker.register().await.is_err());
        assert_eq!(*server.paths.lock().unwrap(), vec!["/api/cluster/info"]);
        let mut worker = worker;
        worker.client.worker_protocol_version = WorkerProtocolVersion::V119;
        assert!(worker.register().await.is_err());
        assert_eq!(server.paths.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn explicit_119_uses_original_local_bounds_without_status_requests() {
        let server = server(task(Duration::from_secs(5)), None, Duration::ZERO).await;
        let mut worker = worker(&server.url, true);
        worker.client.worker_protocol_version = WorkerProtocolVersion::V119;
        worker.register_workflow("workflow", |_, _| async { Ok(Value::Null) });
        worker.register_query("workflow", "value", |_, _| async { Ok(json!(7)) });
        assert!(matches!(worker.poll_query_once().await.unwrap(), ManagedPollOutcome::Handled));
        let paths = server.paths.lock().unwrap();
        assert_eq!(paths.len(), 2);
        assert_eq!(paths[1], "/api/worker/query-tasks/query-1/complete");
    }
}
