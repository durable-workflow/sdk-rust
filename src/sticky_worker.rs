//! Managed-worker caching is separate from the public full-history client API.

use super::*;
use sticky_workflow_cache::{complete_history, CacheKey, ResumeCursor, StickyWorkflowCache};

#[derive(Clone, Debug)]
pub(super) struct StickySnapshot {
    key: CacheKey,
    history: Vec<Value>,
    resume: Option<ResumeCursor>,
}

pub(super) struct ClearCacheOnDrop(pub Option<Arc<Mutex<StickyWorkflowCache>>>);
impl Drop for ClearCacheOnDrop {
    fn drop(&mut self) {
        if let Some(cache) = &self.0 {
            if let Ok(mut cache) = cache.lock() {
                cache.clear();
            }
        }
    }
}

fn invalid(message: &str) -> Error {
    Error::Codec(format!("sticky_history_invalid: {message}"))
}

impl Worker {
    /// Explicitly enable a bounded durable-history cache. Disabled by default.
    ///
    /// The encoded-byte limit excludes transient JSON decoding, replay, keys and
    /// cursor metadata. This never retains live workflow instances or session resources.
    pub fn sticky_cache(mut self, options: StickyCacheOptions) -> Result<Self> {
        if options.ttl.subsec_nanos() != 0 {
            return Err(invalid("sticky cache TTL must use whole seconds"));
        }
        let cache = StickyWorkflowCache::new(options.capacity, options.max_bytes, options.ttl)
            .map_err(invalid)?;
        self.client.sticky_cache = cache.enabled().then(|| Arc::new(Mutex::new(cache)));
        self.sticky_registration_confirmed = Arc::new(AtomicBool::new(false));
        Ok(self)
    }

    /// Select the deployment build identity used for registration, routing and cache keys.
    ///
    /// An omitted build ID uses the SDK identity for cache claims, preserving the
    /// Server's unversioned registration default. An explicit ID must be 1..=255 bytes.
    pub fn build_id(mut self, build_id: impl Into<String>) -> Self {
        self.client.worker_build_id = Some(build_id.into().trim().to_owned());
        let worker_id = self.worker_id.clone();
        self.worker_id(worker_id)
    }

    pub fn sticky_cache_metrics(&self) -> Result<StickyCacheMetrics> {
        self.client
            .sticky_cache
            .as_ref()
            .map_or(Ok(StickyCacheMetrics::default()), |cache| {
                Ok(cache
                    .lock()
                    .map_err(|_| Error::WorkflowStatePoisoned)?
                    .metrics(Instant::now()))
            })
    }

    pub(super) async fn poll_workflow_with_sticky_cache(
        &self,
        poll_request_id: &str,
    ) -> Result<(PollWorkflowTaskResponse, Option<StickySnapshot>)> {
        if self.client.sticky_cache.is_none() {
            return self
                .client
                .poll_workflow_task_response_with_request_id(
                    &self.worker_id,
                    &self.task_queue,
                    self.poll_timeout,
                    poll_request_id,
                    0,
                )
                .await
                .map(|response| (response, None));
        }
        let body = json!({"worker_id":self.worker_id,"task_queue":self.task_queue,
            "poll_request_id":poll_request_id,"timeout_seconds":long_poll_timeout_seconds(self.poll_timeout),
            "history_page_size":WORKFLOW_HISTORY_PAGE_SIZE,"build_id":self.client.worker_build_id});
        let wire: Value = self
            .client
            .poll_request_json(
                "/worker/workflow-tasks/poll",
                RequestProtocol::Worker(WORKER_PROTOCOL_VERSION),
                &body,
                self.poll_timeout + Duration::from_secs(5),
                0,
            )
            .await?;
        let mut response: PollWorkflowTaskResponse = serde_json::from_value(wire.clone())?;
        let snapshot = if let Some(task) = response.task.as_mut() {
            Some(
                self.client
                    .load_sticky_history(task, &wire["task"], &self.worker_id)
                    .await?,
            )
        } else {
            None
        };
        Ok((response, snapshot))
    }

    pub(super) fn sticky_claim(
        &self,
        snapshot: Option<&StickySnapshot>,
        commands: &[Value],
    ) -> Result<Option<Value>> {
        let Some(snapshot) = snapshot else {
            return Ok(None);
        };
        let Some(cache) = &self.client.sticky_cache else {
            return Ok(None);
        };
        let mut cache = cache.lock().map_err(|_| Error::WorkflowStatePoisoned)?;
        if commands.iter().any(|command| {
            command["type"].as_str().is_some_and(|kind| {
                matches!(
                    kind,
                    "complete_workflow"
                        | "fail_workflow"
                        | "continue_as_new"
                        | "acknowledge_cancellation"
                )
            })
        }) {
            cache.discard(&snapshot.key);
            return Ok(None);
        }
        if !cache.remember(
            snapshot.key.clone(),
            &snapshot.history,
            snapshot.resume.clone(),
            Instant::now(),
        ) {
            return Ok(None);
        }
        let metrics = cache.metrics(Instant::now());
        Ok(Some(
            json!({"worker_id":self.worker_id,"workflow_id":snapshot.key.workflow_id,
            "run_id":snapshot.key.run_id,"build_id":snapshot.key.build_id,"ttl_seconds":cache.ttl_seconds(),
            "metrics":{"hit":metrics.hit,"miss":metrics.miss,"eviction":metrics.eviction,
                "forced_cold_replay":metrics.forced_cold_replay}}),
        ))
    }

    pub(super) fn discard_sticky_snapshot(&self, snapshot: Option<&StickySnapshot>) -> Result<()> {
        if let (Some(cache), Some(snapshot)) = (&self.client.sticky_cache, snapshot) {
            cache
                .lock()
                .map_err(|_| Error::WorkflowStatePoisoned)?
                .discard(&snapshot.key);
        }
        Ok(())
    }
}

impl Client {
    pub(super) fn reset_sticky_cache(&mut self) {
        self.sticky_cache = self.sticky_cache.as_ref().map(|cache| {
            // Only immutable, validated options are copied from a poisoned cache.
            let empty = cache
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .empty();
            Arc::new(Mutex::new(empty))
        });
    }

    pub(super) fn clear_sticky_cache(&self) -> Result<()> {
        if let Some(cache) = &self.sticky_cache {
            cache
                .lock()
                .map_err(|_| Error::WorkflowStatePoisoned)?
                .clear();
        }
        Ok(())
    }

    pub(super) async fn confirm_sticky_registration(
        &self,
        response: &Value,
        worker_id: &str,
        queue: &str,
    ) -> Result<()> {
        let accepted = response["registered"] == true
            && response["worker_id"].as_str() == Some(worker_id)
            && response["namespace"].as_str() == Some(self.namespace.as_str())
            && response["task_queue"].as_str() == Some(queue)
            && response["build_id"].as_str() == self.worker_build_id.as_deref()
            && response["protocol_version"]
                .as_str()
                .and_then(|version| version.strip_prefix("1."))
                .and_then(|minor| minor.parse::<u64>().ok())
                .is_some_and(|minor| minor >= 18)
            && response["capabilities"]
                .as_array()
                .is_some_and(|caps| caps.iter().any(|cap| cap == "sticky_execution"))
            && response["capability_manifest"]["sticky_execution"]["supported"] == true
            && response["server_capabilities"]["sticky_execution"]["supported"] == true;
        if accepted {
            return Ok(());
        }
        self.clear_sticky_cache()?;
        let error = Error::WorkerLoop("sticky_registration_unconfirmed: Server must acknowledge this worker, namespace, queue, build and sticky capability".into());
        if response["registered"] == true && response["worker_id"].as_str() == Some(worker_id) {
            if let Err(deregistration) = self.deregister_worker_registration(worker_id).await {
                return Err(Error::WorkerShutdown {
                    primary: Box::new(error),
                    deregistration: Box::new(deregistration),
                });
            }
        }
        Err(error)
    }

    pub(super) async fn load_sticky_history(
        &self,
        task: &mut WorkflowTask,
        wire: &Value,
        worker_id: &str,
    ) -> Result<StickySnapshot> {
        if task.lease_owner.as_deref() != Some(worker_id)
            || wire["workflow_task_attempt"].as_u64() != Some(task.workflow_task_attempt)
        {
            return Err(invalid("poll changed the worker's actual owner or attempt"));
        }
        let key = CacheKey {
            workflow_id: task
                .workflow_id
                .clone()
                .filter(|id| !id.is_empty())
                .ok_or_else(|| invalid("workflow ID is missing"))?,
            run_id: task
                .run_id
                .clone()
                .filter(|id| !id.is_empty())
                .ok_or_else(|| invalid("run ID is missing"))?,
            build_id: self
                .worker_build_id
                .clone()
                .unwrap_or_else(|| SDK_VERSION.into()),
        };
        let inline: Vec<Value> =
            serde_json::from_value(serde_json::to_value(&task.history_events)?)?;
        let mode = wire["sticky_replay_mode"].as_str();
        let cache = self
            .sticky_cache
            .as_ref()
            .ok_or_else(|| invalid("cache profile is disabled"))?;
        let cached = if mode == Some("sticky_hit_expected") {
            cache
                .lock()
                .map_err(|_| Error::WorkflowStatePoisoned)?
                .lookup(&key, Instant::now())
        } else {
            None
        };
        let last_sequence = wire
            .get("last_history_sequence")
            .or_else(|| wire.get("total_history_events"))
            .and_then(Value::as_u64);
        if let Some((prefix, resume)) = cached {
            let overlap = inline.len().min(prefix.len());
            if complete_history(&inline)
                && inline[..overlap] == prefix[..overlap]
                && last_sequence.is_some_and(|last| last >= prefix.len() as u64)
            {
                let warmed = if let Some(cursor) = resume
                    .filter(|cursor| cursor.offset >= inline.len() && inline.len() < prefix.len())
                {
                    let tail = match self
                        .fetch_sticky_pages(task, Vec::new(), Some(cursor.token), cursor.offset)
                        .await
                    {
                        Ok(value) => Some(value),
                        Err(Error::Codec(_)) => None,
                        Err(Error::Http { status, body })
                            if status == reqwest::StatusCode::BAD_REQUEST
                                && serde_json::from_str::<Value>(&body).ok().is_some_and(
                                    |value| value["reason"] == "invalid_page_token",
                                ) =>
                        {
                            None
                        }
                        Err(error) => return Err(error),
                    };
                    tail.and_then(|(tail, resume)| {
                        let overlap = prefix.len() - cursor.offset;
                        if tail.get(..overlap) != Some(&prefix[cursor.offset..]) {
                            return None;
                        }
                        let mut history = prefix[..cursor.offset].to_vec();
                        history.extend(tail);
                        Some((history, resume))
                    })
                } else {
                    let next = task.next_history_page_token.clone();
                    let (history, resume) = self
                        .fetch_sticky_pages(task, inline.clone(), next, 0)
                        .await?;
                    (history.get(..prefix.len()) == Some(prefix.as_slice()))
                        .then_some((history, resume))
                };
                if let Some((history, resume)) = warmed.filter(|(history, _)| {
                    complete_history(history)
                        && last_sequence.is_some_and(|last| history.len() as u64 >= last)
                }) {
                    cache
                        .lock()
                        .map_err(|_| Error::WorkflowStatePoisoned)?
                        .record_replay(true, false);
                    task.history_events = serde_json::from_value(Value::Array(history.clone()))?;
                    task.next_history_page_token = None;
                    return Ok(StickySnapshot {
                        key,
                        history,
                        resume,
                    });
                }
            }
            cache
                .lock()
                .map_err(|_| Error::WorkflowStatePoisoned)?
                .discard(&key);
        }
        cache
            .lock()
            .map_err(|_| Error::WorkflowStatePoisoned)?
            .record_replay(
                false,
                matches!(mode, Some("sticky_hit_expected" | "forced_cold_replay")),
            );
        let (seed, token) = if !complete_history(&inline)
            && matches!(mode, Some("sticky_hit_expected" | "forced_cold_replay"))
        {
            (Vec::new(), Some("MA==".into()))
        } else {
            (inline, task.next_history_page_token.clone())
        };
        let (history, resume) = self.fetch_sticky_pages(task, seed, token, 0).await?;
        if !complete_history(&history)
            || last_sequence.is_some_and(|last| (history.len() as u64) < last)
        {
            return Err(invalid("cold replay lacks complete canonical history"));
        }
        task.history_events = serde_json::from_value(Value::Array(history.clone()))?;
        task.next_history_page_token = None;
        Ok(StickySnapshot {
            key,
            history,
            resume,
        })
    }

    async fn fetch_sticky_pages(
        &self,
        task: &mut WorkflowTask,
        mut history: Vec<Value>,
        mut token: Option<String>,
        offset: usize,
    ) -> Result<(Vec<Value>, Option<ResumeCursor>)> {
        let mut seen = BTreeSet::new();
        let mut resume = None;
        while let Some(current) = token.take() {
            if current.is_empty() || !seen.insert(current.clone()) {
                return Err(invalid("history cursor did not advance"));
            }
            let body = json!({"lease_owner":task.lease_owner,"workflow_task_attempt":task.workflow_task_attempt,
                "next_history_page_token":current,"history_page_size":WORKFLOW_HISTORY_PAGE_SIZE});
            let page: Value = self
                .request_json(
                    reqwest::Method::POST,
                    &format!(
                        "/worker/workflow-tasks/{}/history",
                        percent_encode_path_segment(&task.task_id)
                    ),
                    RequestProtocol::Worker(WORKER_PROTOCOL_VERSION),
                    Some(&body),
                )
                .await?;
            if page["task_id"].as_str() != Some(task.task_id.as_str())
                || page["workflow_task_attempt"].as_u64() != Some(task.workflow_task_attempt)
            {
                return Err(invalid("history page changed the current task or attempt"));
            }
            let events = page["history_events"]
                .as_array()
                .ok_or_else(|| invalid("history page has no event array"))?;
            if events.len() > WORKFLOW_HISTORY_PAGE_SIZE
                || events.iter().enumerate().any(|(index, event)| {
                    !event.is_object()
                        || event["sequence"].as_u64()
                            != Some((offset + history.len() + index + 1) as u64)
                })
            {
                return Err(invalid("history page is oversized or not contiguous"));
            }
            token = match page.get("next_history_page_token") {
                Some(Value::Null) => None,
                Some(Value::String(next)) if !next.is_empty() && !events.is_empty() => {
                    Some(next.clone())
                }
                _ => return Err(invalid("history page has an invalid cursor or no progress")),
            };
            resume = Some(ResumeCursor {
                token: current,
                offset: offset + history.len(),
            });
            history.extend_from_slice(events);
            if token.is_none()
                && page["total_history_events"]
                    .as_u64()
                    .is_some_and(|total| ((offset + history.len()) as u64) < total)
            {
                return Err(invalid("final history page omitted advertised events"));
            }
            if let Some(total) = page["total_history_events"].as_u64() {
                task.total_history_events = Some(total);
            }
        }
        Ok((history, resume))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    type Request = (String, String, Value);
    async fn server(replies: Vec<(u16, Value)>) -> (String, tokio::task::JoinHandle<Vec<Request>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let handle = tokio::spawn(async move {
            let mut captured = Vec::new();
            for (status, body) in replies {
                let (mut stream, _) =
                    tokio::time::timeout(Duration::from_secs(5), listener.accept())
                        .await
                        .unwrap()
                        .unwrap();
                let mut bytes = Vec::new();
                let (header_end, length) = loop {
                    let mut buffer = [0; 4096];
                    let count = stream.read(&mut buffer).await.unwrap();
                    assert!(count > 0);
                    bytes.extend_from_slice(&buffer[..count]);
                    if let Some(offset) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                        let header = String::from_utf8_lossy(&bytes[..offset]).to_ascii_lowercase();
                        let length = header
                            .lines()
                            .find_map(|line| line.strip_prefix("content-length: "))
                            .and_then(|value| value.parse::<usize>().ok())
                            .unwrap_or(0);
                        break (offset + 4, length);
                    }
                };
                while bytes.len() < header_end + length {
                    let mut buffer = [0; 4096];
                    let count = stream.read(&mut buffer).await.unwrap();
                    assert!(count > 0);
                    bytes.extend_from_slice(&buffer[..count]);
                }
                let headers = String::from_utf8_lossy(&bytes[..header_end]);
                let path = headers
                    .lines()
                    .next()
                    .unwrap()
                    .split_whitespace()
                    .nth(1)
                    .unwrap()
                    .to_owned();
                let protocol = headers
                    .lines()
                    .find_map(|line| {
                        line.to_ascii_lowercase()
                            .strip_prefix("x-durable-workflow-protocol-version: ")
                            .map(str::to_owned)
                    })
                    .unwrap_or_default();
                let request = if length == 0 {
                    Value::Null
                } else {
                    serde_json::from_slice(&bytes[header_end..header_end + length]).unwrap()
                };
                captured.push((path, protocol, request));
                let encoded = body.to_string();
                let response = format!("HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{encoded}", encoded.len());
                stream.write_all(response.as_bytes()).await.unwrap();
            }
            captured
        });
        (url, handle)
    }

    fn history(count: usize) -> Vec<Value> {
        (1..=count).map(|sequence| json!({"event_type":if sequence == 1 {"WorkflowStarted"} else {"SideEffectRecorded"},
            "sequence":sequence,"payload":{"identity":sequence}})).collect()
    }
    fn wire(inline: Vec<Value>, total: usize, mode: &str) -> Value {
        json!({"task_id":"task-now","workflow_id":"workflow","run_id":"run","workflow_type":"Test",
            "workflow_task_attempt":7,"lease_owner":"worker","payload_codec":"avro",
            "history_events":inline,"total_history_events":total,"next_history_page_token":"inline:500",
            "sticky_replay_mode":mode})
    }
    fn page(events: Vec<Value>, next: Option<&str>) -> Value {
        let total = if next.is_some() {
            1002
        } else {
            events
                .last()
                .and_then(|event| event["sequence"].as_u64())
                .unwrap_or(1002)
        };
        json!({"task_id":"task-now","workflow_task_attempt":7,"history_events":events,
            "total_history_events":total,"next_history_page_token":next})
    }
    fn worker(url: String, bytes: usize) -> Worker {
        Worker::new(Client::new(url).unwrap(), "queue")
            .worker_id("worker")
            .sticky_cache(StickyCacheOptions::new(2).max_history_bytes(bytes))
            .unwrap()
    }
    fn key() -> CacheKey {
        CacheKey {
            workflow_id: "workflow".into(),
            run_id: "run".into(),
            build_id: SDK_VERSION.into(),
        }
    }
    fn remember(worker: &Worker, prefix: Vec<Value>) {
        assert!(worker
            .client
            .sticky_cache
            .as_ref()
            .unwrap()
            .lock()
            .unwrap()
            .remember(
                key(),
                &prefix,
                Some(ResumeCursor {
                    token: "resume:1000".into(),
                    offset: 1000
                }),
                Instant::now()
            ));
    }

    #[tokio::test]
    async fn warm_reuse_fetches_only_tail_with_the_current_lease() {
        let authoritative = history(1002);
        let (url, requests) = server(vec![(200, page(authoritative[1000..].to_vec(), None))]).await;
        let worker = worker(url, 1_000_000);
        remember(&worker, authoritative[..1001].to_vec());
        let wire = wire(authoritative[..500].to_vec(), 1002, "sticky_hit_expected");
        let mut task = serde_json::from_value(wire.clone()).unwrap();
        let snapshot = worker
            .client
            .load_sticky_history(&mut task, &wire, "worker")
            .await
            .unwrap();
        assert_eq!(snapshot.history, authoritative);
        assert_eq!(snapshot.resume.unwrap().offset, 1000);
        assert_eq!(task.history_events.len(), 1002);
        assert_eq!(worker.sticky_cache_metrics().unwrap().hit, 1);
        let requests = requests.await.unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].0, "/api/worker/workflow-tasks/task-now/history");
        assert_eq!(requests[0].2["workflow_task_attempt"], 7);
        assert_eq!(requests[0].2["lease_owner"], "worker");
        assert_eq!(requests[0].2["next_history_page_token"], "resume:1000");
    }

    #[tokio::test]
    async fn stale_cursor_falls_back_to_full_current_claim_history() {
        let authoritative = history(1002);
        let (url, requests) = server(vec![
            (400, json!({"reason":"invalid_page_token"})),
            (
                200,
                page(authoritative[500..1000].to_vec(), Some("cold:1000")),
            ),
            (200, page(authoritative[1000..].to_vec(), None)),
        ])
        .await;
        let worker = worker(url, 1_000_000);
        remember(&worker, authoritative[..1001].to_vec());
        let wire = wire(authoritative[..500].to_vec(), 1002, "sticky_hit_expected");
        let mut task = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(
            worker
                .client
                .load_sticky_history(&mut task, &wire, "worker")
                .await
                .unwrap()
                .history,
            authoritative
        );
        let metrics = worker.sticky_cache_metrics().unwrap();
        assert_eq!(metrics.hit, 0);
        assert_eq!(metrics.miss, 1);
        assert_eq!(metrics.forced_cold_replay, 1);
        let requests = requests.await.unwrap();
        assert_eq!(
            requests
                .iter()
                .map(|request| request.2["next_history_page_token"].as_str().unwrap())
                .collect::<Vec<_>>(),
            vec!["resume:1000", "inline:500", "cold:1000"]
        );
    }

    #[tokio::test]
    async fn incomplete_inline_hint_forces_canonical_start_cursor() {
        let authoritative = history(7);
        let (url, requests) = server(vec![(200, page(authoritative.clone(), None))]).await;
        let worker = worker(url, 1_000_000);
        let wire = wire(authoritative[5..].to_vec(), 7, "forced_cold_replay");
        let mut task = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(
            worker
                .client
                .load_sticky_history(&mut task, &wire, "worker")
                .await
                .unwrap()
                .history,
            authoritative
        );
        assert_eq!(
            requests.await.unwrap()[0].2["next_history_page_token"],
            "MA=="
        );
    }

    #[tokio::test]
    async fn lost_lease_propagates_without_a_fallback_read_or_hit() {
        let authoritative = history(1002);
        let (url, requests) = server(vec![(409, json!({"reason":"lease_expired"}))]).await;
        let worker = worker(url, 1_000_000);
        remember(&worker, authoritative[..1001].to_vec());
        let wire = wire(authoritative[..500].to_vec(), 1002, "sticky_hit_expected");
        let mut task = serde_json::from_value(wire.clone()).unwrap();
        assert!(matches!(
            worker
                .client
                .load_sticky_history(&mut task, &wire, "worker")
                .await,
            Err(Error::Http {
                status: reqwest::StatusCode::CONFLICT,
                ..
            })
        ));
        assert_eq!(requests.await.unwrap().len(), 1);
        assert_eq!(worker.sticky_cache_metrics().unwrap().hit, 0);
    }

    #[tokio::test]
    async fn wrong_history_attempt_is_never_replayed() {
        let authoritative = history(1002);
        let mut wrong = page(authoritative[500..1000].to_vec(), None);
        wrong["workflow_task_attempt"] = json!(8);
        let (url, requests) = server(vec![(200, wrong)]).await;
        let worker = worker(url, 1_000_000);
        let wire = wire(authoritative[..500].to_vec(), 1002, "cold_replay");
        let mut task = serde_json::from_value(wire.clone()).unwrap();
        assert!(matches!(
            worker
                .client
                .load_sticky_history(&mut task, &wire, "worker")
                .await,
            Err(Error::Codec(_))
        ));
        assert_eq!(requests.await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn successful_final_page_cannot_omit_advertised_events() {
        let authoritative = history(1002);
        let mut incomplete = page(authoritative[500..1000].to_vec(), None);
        incomplete["total_history_events"] = json!(1002);
        let (url, requests) = server(vec![(200, incomplete)]).await;
        let worker = worker(url, 1_000_000);
        let wire = wire(authoritative[..500].to_vec(), 1002, "cold_replay");
        let mut task = serde_json::from_value(wire.clone()).unwrap();
        assert!(
            matches!(worker.client.load_sticky_history(&mut task, &wire, "worker").await,
            Err(Error::Codec(message)) if message.contains("omitted advertised events"))
        );
        assert_eq!(requests.await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn completion_claim_uses_affinity_protocol_with_older_command_minimum() {
        let (url, requests) = server(vec![(200, json!({})), (200, json!({"recorded":true}))]).await;
        let worker = worker(url, 1_000_000);
        let snapshot = StickySnapshot {
            key: key(),
            history: history(2),
            resume: None,
        };
        let entries = encode_typed_envelope(
            &AvroValue::from_serialize(&json!({"status":"waiting"})).unwrap(),
            DEFAULT_CODEC,
        )
        .unwrap();
        let commands = vec![json!({"type":"upsert_memo","entries":entries})];
        let claim = worker.sticky_claim(Some(&snapshot), &commands).unwrap();
        worker
            .client
            .complete_workflow_task_with_message_streams(
                "task-now",
                "worker",
                7,
                commands,
                Vec::new(),
                Vec::new(),
                claim,
            )
            .await
            .unwrap();
        let requests = requests.await.unwrap();
        assert_eq!(requests[0].0, "/api/cluster/info");
        assert_eq!(requests[1].1, "1.18");
        assert_eq!(requests[1].2["sticky_cache"]["run_id"], "run");
        assert_eq!(requests[1].2["workflow_task_attempt"], 7);
    }

    #[tokio::test]
    async fn cooperative_profile_keeps_protocol_120_on_cached_page_reads() {
        let authoritative = history(1002);
        let (url, requests) = server(vec![(200, page(authoritative[1000..].to_vec(), None))]).await;
        let worker = worker(url, 1_000_000).cooperative_cancellation(true);
        remember(&worker, authoritative[..1001].to_vec());
        let wire = wire(authoritative[..500].to_vec(), 1002, "sticky_hit_expected");
        let mut task = serde_json::from_value(wire.clone()).unwrap();
        worker
            .client
            .load_sticky_history(&mut task, &wire, "worker")
            .await
            .unwrap();
        assert_eq!(requests.await.unwrap()[0].1, "1.20");
    }

    #[tokio::test]
    async fn registration_refusal_deregisters_and_cannot_poll() {
        let response = json!({"registered":true,"worker_id":"worker","namespace":"default","task_queue":"wrong",
            "protocol_version":"1.19","build_id":null,"capabilities":["sticky_execution"],
            "capability_manifest":{"sticky_execution":{"supported":true}},
            "server_capabilities":{"sticky_execution":{"supported":true}}});
        let (url, requests) = server(vec![
            (200, response),
            (
                200,
                json!({"worker_id":"worker","outcome":"removed","recovered_workflow_task_count":0}),
            ),
        ])
        .await;
        let worker = worker(url, 1_000_000);
        assert!(
            matches!(worker.register().await, Err(Error::WorkerLoop(message)) if message.contains("sticky_registration_unconfirmed"))
        );
        assert!(
            matches!(worker.run_once().await, Err(Error::WorkerLoop(message)) if message.contains("sticky_registration_unconfirmed"))
        );
        let requests = requests.await.unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(
            requests[0].2["capability_manifest"]["sticky_execution"]["supported"],
            true
        );
        assert_eq!(requests[1].0, "/api/worker/registrations/worker");
    }

    #[test]
    fn claim_admission_bounds_terminal_cleanup_and_shutdown() {
        let worker = worker("http://127.0.0.1:1".into(), 1_000_000);
        let snapshot = StickySnapshot {
            key: key(),
            history: history(2),
            resume: None,
        };
        let claim = worker
            .sticky_claim(Some(&snapshot), &[json!({"type":"start_timer"})])
            .unwrap()
            .unwrap();
        assert_eq!(claim["build_id"], SDK_VERSION);
        assert_eq!(claim["worker_id"], "worker");
        assert_eq!(worker.sticky_cache_metrics().unwrap().entries, 1);
        assert!(worker
            .sticky_claim(
                Some(&snapshot),
                &[json!({"type":"acknowledge_cancellation"})]
            )
            .unwrap()
            .is_none());
        assert_eq!(worker.sticky_cache_metrics().unwrap().entries, 0);
        worker
            .sticky_claim(Some(&snapshot), &[json!({"type":"start_timer"})])
            .unwrap();
        drop(ClearCacheOnDrop(worker.client.sticky_cache.clone()));
        assert_eq!(worker.sticky_cache_metrics().unwrap().history_bytes, 0);
        let tiny = self::worker("http://127.0.0.1:1".into(), 1);
        assert!(tiny
            .sticky_claim(Some(&snapshot), &[json!({"type":"start_timer"})])
            .unwrap()
            .is_none());
        assert_eq!(tiny.sticky_cache_metrics().unwrap().entries, 0);
    }
}
