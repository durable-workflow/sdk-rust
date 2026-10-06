//! Worker-held session routing. Session memory is never durable workflow state.

use crate::{
    percent_encode_path_segment, ActivityCall, AvroValue, Client, Error, HandlerKind, HistoryEvent,
    RequestProtocol, Result, WORKER_PROTOCOL_VERSION,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Debug, Default)]
struct SessionState {
    expires: Option<Instant>,
    receipt: Option<Value>,
    close_receipt: Option<Value>,
}

/// One worker's acknowledged session lease.
///
/// Clones share lifecycle state. `active()` is a local lease hint, not authority
/// for an external side effect. Server fences activity completion. Replacement
/// holders must rebuild process-local resources rather than restore this handle.
#[derive(Clone, Debug)]
pub struct WorkerSession {
    client: Client,
    worker_id: String,
    options: WorkerSessionOptions,
    state: Arc<Mutex<SessionState>>,
    operation: Arc<tokio::sync::Mutex<()>>,
}

impl WorkerSession {
    fn new(client: Client, worker_id: String, options: WorkerSessionOptions) -> Self {
        Self {
            client,
            worker_id,
            options,
            state: Arc::new(Mutex::new(SessionState::default())),
            operation: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    pub fn options(&self) -> &WorkerSessionOptions {
        &self.options
    }

    pub fn active(&self) -> bool {
        self.state
            .lock()
            .ok()
            .and_then(|state| state.expires)
            .is_some_and(|expires| Instant::now() < expires)
    }

    /// Last validated Server receipt, including holder and original TTL.
    pub fn snapshot(&self) -> Result<Option<Value>> {
        Ok(self
            .state
            .lock()
            .map_err(|_| Error::WorkflowStatePoisoned)?
            .receipt
            .clone())
    }

    pub async fn create(&self) -> Result<Value> {
        let _operation = self.operation.lock().await;
        if self
            .state
            .lock()
            .map_err(|_| Error::WorkflowStatePoisoned)?
            .close_receipt
            .is_some()
        {
            return Err(invalid("a closed session identity cannot be recreated"));
        }
        let response = self
            .client
            .create_worker_session(&self.worker_id, &self.options)
            .await;
        let response = self.settle(response, &["created", "reused", "reacquired"])?;
        Ok(response)
    }

    /// Renew the holder lease without extending the original TTL.
    pub async fn renew(&self) -> Result<Value> {
        let _operation = self.operation.lock().await;
        if !self.active() {
            return Err(invalid(
                "cannot renew an uncreated, expired or closed local handle",
            ));
        }
        let response = self
            .client
            .renew_worker_session(
                &self.worker_id,
                self.options.session_id(),
                self.options.lease_seconds,
            )
            .await;
        let response = self.settle(response, &["heartbeat_recorded"])?;
        Ok(response)
    }

    /// Close the session after its activities drain. Duplicate close reuses its receipt.
    pub async fn close(&self, reason: &str) -> Result<Value> {
        let _operation = self.operation.lock().await;
        if let Some(receipt) = self
            .state
            .lock()
            .map_err(|_| Error::WorkflowStatePoisoned)?
            .close_receipt
            .clone()
        {
            return Ok(receipt);
        }
        self.invalidate()?;
        let response = self
            .client
            .close_worker_session(&self.worker_id, self.options.session_id(), reason)
            .await?;
        self.validate_receipt(&response, &["closed", "already_closed"], "closed")?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::WorkflowStatePoisoned)?;
        state.receipt = Some(response.clone());
        state.close_receipt = Some(response.clone());
        Ok(response)
    }

    fn invalidate(&self) -> Result<()> {
        self.state
            .lock()
            .map_err(|_| Error::WorkflowStatePoisoned)?
            .expires = None;
        Ok(())
    }

    fn validate_receipt(&self, receipt: &Value, outcomes: &[&str], status: &str) -> Result<()> {
        let session = &receipt["session"];
        if receipt["admitted"] != true
            || !receipt["outcome"]
                .as_str()
                .is_some_and(|outcome| outcomes.contains(&outcome))
            || session["session_id"].as_str() != Some(self.options.session_id())
            || session["namespace"].as_str() != Some(self.client.namespace.as_str())
            || session["lease_owner"].as_str() != Some(self.worker_id.as_str())
            || session["status"].as_str() != Some(status)
        {
            return Err(invalid(
                "Server did not acknowledge this session, namespace, holder and lifecycle outcome",
            ));
        }
        Ok(())
    }

    fn accept(&self, receipt: &Value, outcomes: &[&str]) -> Result<()> {
        self.validate_receipt(receipt, outcomes, "active")?;
        self.track(&receipt["session"])?;
        self.state
            .lock()
            .map_err(|_| Error::WorkflowStatePoisoned)?
            .receipt = Some(receipt.clone());
        Ok(())
    }

    fn settle(&self, result: Result<Value>, outcomes: &[&str]) -> Result<Value> {
        match result.and_then(|receipt| {
            self.accept(&receipt, outcomes)?;
            Ok(receipt)
        }) {
            Ok(receipt) => Ok(receipt),
            Err(error) => {
                self.invalidate()?;
                Err(error)
            }
        }
    }

    pub(super) async fn wait_until_unavailable(&self) {
        while self.active() {
            let delay = self
                .state
                .lock()
                .ok()
                .and_then(|state| state.expires)
                .map(|expires| expires.saturating_duration_since(Instant::now()))
                .unwrap_or_default()
                .min(Duration::from_millis(100));
            tokio::time::sleep(delay).await;
        }
    }

    pub(super) fn track(&self, affinity: &Value) -> Result<()> {
        let received: WorkerSessionOptions = serde_json::from_value(affinity.clone())?;
        if affinity["session_id"].as_str() != Some(self.options.session_id())
            || affinity["status"] != "active"
            || affinity["lease_owner"].as_str() != Some(self.worker_id.as_str())
            || affinity["queue"].as_str() != self.options.queue.as_deref()
            || received.to_wire()? != self.options.to_wire()?
            || affinity
                .get("namespace")
                .is_some_and(|namespace| namespace.as_str() != Some(self.client.namespace.as_str()))
        {
            return Err(invalid(
                "activity session affinity does not match the current holder and queue",
            ));
        }
        let expires = lease_deadline(affinity)?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::WorkflowStatePoisoned)?;
        state.expires = Some(expires);
        state.receipt = Some(affinity.clone());
        state.close_receipt = None;
        Ok(())
    }
}

fn lease_deadline(affinity: &Value) -> Result<Instant> {
    let now = std::time::SystemTime::now();
    let mut budget = Duration::MAX;
    for field in ["lease_expires_at", "ttl_expires_at"] {
        let deadline = affinity[field]
            .as_str()
            .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
            .ok_or_else(|| invalid("session receipt requires valid lease and TTL deadlines"))?;
        let timestamp = deadline.timestamp();
        if timestamp < 0 {
            return Err(invalid("session lease or TTL already expired"));
        }
        let deadline = std::time::UNIX_EPOCH
            + Duration::new(timestamp as u64, deadline.timestamp_subsec_nanos());
        let remaining = deadline
            .duration_since(now)
            .map_err(|_| invalid("session lease or TTL already expired"))?;
        if remaining.is_zero() {
            return Err(invalid("session lease or TTL already expired"));
        }
        budget = budget.min(remaining);
    }
    Instant::now()
        .checked_add(budget)
        .ok_or_else(|| invalid("session deadline exceeds local clock range"))
}

#[derive(Clone, Debug, Deserialize)]
pub(super) struct SessionActivityTask {
    #[serde(flatten)]
    pub(super) task: crate::ActivityTask,
    #[serde(default)]
    pub(super) worker_session: Option<Value>,
}

#[derive(Debug, Deserialize)]
pub(super) struct SessionPollResponse {
    #[serde(default)]
    pub(super) task: Option<SessionActivityTask>,
    #[serde(default)]
    poll_status: Option<String>,
    #[serde(default)]
    reason: Option<String>,
}

impl SessionPollResponse {
    pub(super) fn outcome(&self) -> crate::WorkerPollOutcome {
        crate::worker_poll_outcome(
            self.task.is_some(),
            self.poll_status.as_deref(),
            self.reason.as_deref(),
        )
    }

    pub(super) fn ordinary(self) -> crate::PollActivityTaskResponse {
        crate::PollActivityTaskResponse {
            task: self.task.map(|task| task.task),
            poll_status: self.poll_status,
            reason: self.reason,
        }
    }
}

impl crate::Worker {
    /// Opt in to remote activity sessions. Sticky execution stays unsupported.
    pub fn worker_sessions(mut self, enabled: bool) -> Self {
        self.client.worker_sessions_enabled = enabled;
        self.session_registration_confirmed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        self
    }

    pub fn max_concurrent_worker_sessions(mut self, count: usize) -> Self {
        self.client.max_concurrent_worker_sessions = count.max(1);
        self
    }

    /// Additional resource requirements this worker can actually satisfy.
    pub fn capabilities<I, S>(mut self, capabilities: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.resource_capabilities = capabilities
            .into_iter()
            .map(|value| value.into().trim().to_owned())
            .collect();
        self.resource_capabilities.sort();
        self.resource_capabilities.dedup();
        self
    }

    /// Obtain a shared handle for this registered worker and immutable session options.
    pub fn worker_session(&self, mut options: WorkerSessionOptions) -> Result<WorkerSession> {
        self.require_session_registration()?;
        if options.queue.is_none() {
            options.queue = Some(self.task_queue.clone());
        }
        options.to_wire()?;
        if options.queue.as_deref() != Some(self.task_queue.as_str()) {
            return Err(invalid("session queue must match this worker"));
        }
        let mut sessions = self
            .sessions
            .lock()
            .map_err(|_| Error::WorkflowStatePoisoned)?;
        if let Some(session) = sessions.get(options.session_id()) {
            if session.options.to_wire()? != options.to_wire()? {
                return Err(invalid("session identity already has different options"));
            }
            return Ok(session.clone());
        }
        sessions.retain(|_, session| {
            session
                .state
                .lock()
                .map(|state| {
                    state.close_receipt.is_none()
                        && (Arc::strong_count(&session.state) > 1
                            || state.expires.is_none()
                            || state
                                .expires
                                .is_some_and(|expires| Instant::now() < expires))
                })
                .unwrap_or(true)
        });
        if sessions.len() >= self.client.max_concurrent_worker_sessions {
            return Err(invalid("local worker session capacity exhausted"));
        }
        let session = WorkerSession::new(self.client.clone(), self.worker_id.clone(), options);
        sessions.insert(session.options.session_id.clone(), session.clone());
        Ok(session)
    }

    pub(super) fn require_session_registration(&self) -> Result<()> {
        if !self.client.worker_sessions_enabled
            || !self
                .session_registration_confirmed
                .load(std::sync::atomic::Ordering::SeqCst)
        {
            return Err(invalid(
                "register a session-capable worker before acquiring session handles or tasks",
            ));
        }
        Ok(())
    }

    pub(super) fn track_session_task(
        &self,
        affinity: Option<&Value>,
    ) -> Result<Option<WorkerSession>> {
        let Some(affinity) = affinity.filter(|value| !value.is_null()) else {
            return Ok(None);
        };
        let options: WorkerSessionOptions = serde_json::from_value(affinity.clone())?;
        let session = self.worker_session(options)?;
        session.track(affinity)?;
        Ok(Some(session))
    }

    pub(super) fn session_available(&self) -> usize {
        self.sessions
            .lock()
            .map(|sessions| {
                self.client
                    .max_concurrent_worker_sessions
                    .saturating_sub(sessions.values().filter(|session| session.active()).count())
            })
            .unwrap_or(0)
    }

    pub(super) async fn close_worker_sessions(&self) -> Result<()> {
        let sessions: Vec<_> = self
            .sessions
            .lock()
            .map_err(|_| Error::WorkflowStatePoisoned)?
            .values()
            .cloned()
            .collect();
        let mut first_error = None;
        for session in sessions {
            // Never issue a close for a handle that was not admitted to this holder.
            if session.snapshot()?.is_some() {
                if let Err(error) = session.close("worker_shutdown").await {
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
            }
        }
        first_error.map_or(Ok(()), Err)
    }
}

impl crate::ActivityContext {
    /// Session affinity and holder-local lifecycle for this remote activity.
    pub fn worker_session(&self) -> Option<&WorkerSession> {
        self.worker_session.as_ref()
    }
}

pub(super) fn settle_activity_heartbeat(
    session: &WorkerSession,
    result: Result<Value>,
    task_id: &str,
    attempt_id: &str,
    owner: &str,
) -> Result<crate::ActivityHeartbeatResponse> {
    let result = result.and_then(|value| {
        if value["task_id"].as_str() != Some(task_id)
            || value["activity_attempt_id"].as_str() != Some(attempt_id)
            || value["lease_owner"].as_str() != Some(owner)
            || value["can_continue"] != true
            || value["heartbeat_recorded"] != true
            || value["cancel_requested"] != false
        {
            return Err(invalid(
                "activity heartbeat did not acknowledge this claim and session",
            ));
        }
        session.track(&value["worker_session"])?;
        serde_json::from_value(value).map_err(Error::from)
    });
    result.map_err(|error| {
        let _ = session.invalidate();
        Error::ActivityExecutionAbandoned(error.to_string())
    })
}

/// Routing and lifetime options for one worker-held session.
///
/// A holder replacement must rebuild its local resources. TTL expiry and explicit
/// close are terminal for the session identity, even when lease reacquisition is
/// enabled. Options do not create a session until sent to Server.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkerSessionOptions {
    session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    connection: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    queue: Option<String>,
    #[serde(default)]
    requirements: Vec<String>,
    #[serde(default = "default_lease")]
    lease_seconds: u64,
    #[serde(default = "default_ttl")]
    ttl_seconds: u64,
    #[serde(default = "default_concurrency")]
    max_concurrent_activities: usize,
    #[serde(default = "default_true")]
    create_if_missing: bool,
    #[serde(default = "default_true")]
    allow_reacquire_after_failure: bool,
}

fn default_lease() -> u64 {
    120
}
fn default_ttl() -> u64 {
    1800
}
fn default_concurrency() -> usize {
    1
}
fn default_true() -> bool {
    true
}

impl WorkerSessionOptions {
    pub fn new(session_id: impl Into<String>) -> Self {
        Self {
            session_id: session_id.into().trim().to_owned(),
            connection: None,
            queue: None,
            requirements: Vec::new(),
            lease_seconds: default_lease(),
            ttl_seconds: default_ttl(),
            max_concurrent_activities: 1,
            create_if_missing: true,
            allow_reacquire_after_failure: true,
        }
    }

    pub fn session_id(&self) -> &str {
        &self.session_id
    }
    pub fn task_queue(&self) -> Option<&str> {
        self.queue.as_deref()
    }
    pub fn lease_duration(&self) -> Duration {
        Duration::from_secs(self.lease_seconds)
    }
    pub fn ttl_duration(&self) -> Duration {
        Duration::from_secs(self.ttl_seconds)
    }
    pub fn connection(mut self, connection: impl Into<String>) -> Self {
        self.connection = Some(connection.into().trim().to_owned());
        self
    }
    pub fn queue(mut self, queue: impl Into<String>) -> Self {
        self.queue = Some(queue.into().trim().to_owned());
        self
    }
    pub fn requirements<I, S>(mut self, requirements: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.requirements = requirements
            .into_iter()
            .map(|value| value.into().trim().to_owned())
            .collect();
        self.requirements.sort();
        self.requirements.dedup();
        self
    }
    pub fn lease_seconds(mut self, seconds: u64) -> Self {
        self.lease_seconds = seconds;
        self
    }
    pub fn ttl_seconds(mut self, seconds: u64) -> Self {
        self.ttl_seconds = seconds;
        self
    }
    pub fn max_concurrent_activities(mut self, count: usize) -> Self {
        self.max_concurrent_activities = count;
        self
    }
    pub fn create_if_missing(mut self, enabled: bool) -> Self {
        self.create_if_missing = enabled;
        self
    }
    pub fn allow_reacquire_after_failure(mut self, enabled: bool) -> Self {
        self.allow_reacquire_after_failure = enabled;
        self
    }

    pub fn to_wire(&self) -> Result<Value> {
        identifier("session_id", &self.session_id)?;
        for (field, value) in [("connection", &self.connection), ("queue", &self.queue)] {
            if let Some(value) = value {
                identifier(field, value)?;
            }
        }
        for requirement in &self.requirements {
            identifier("requirements", requirement)?;
        }
        if self.lease_seconds == 0
            || self.ttl_seconds == 0
            || self.max_concurrent_activities == 0
            || self.lease_seconds > i64::MAX as u64
            || self.ttl_seconds > i64::MAX as u64
            || self.max_concurrent_activities as u128 > i64::MAX as u128
        {
            return Err(invalid("lease, TTL and concurrency must be positive"));
        }
        let mut canonical = self.clone();
        canonical.requirements.sort();
        canonical.requirements.dedup();
        Ok(serde_json::to_value(canonical)?)
    }
}

fn invalid(message: &str) -> Error {
    Error::WorkerLoop(format!("invalid_worker_session: {message}"))
}

fn identifier(field: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() || value.trim() != value || value.chars().count() > 255 {
        return Err(invalid(&format!(
            "{field} must be a canonical non-empty string of at most 255 characters"
        )));
    }
    Ok(())
}

pub(super) fn validate_resource_capability(value: &str) -> Result<()> {
    identifier("capabilities", value)?;
    if value.starts_with("prepared_local_")
        || value.starts_with("cancellation_scope")
        || matches!(
            value,
            "local_activities"
                | "worker_sessions"
                | "sticky_execution"
                | "cooperative_cancellation"
        )
    {
        return Err(invalid(
            "built-in execution capabilities must use their explicit worker opt-in",
        ));
    }
    Ok(())
}

impl Client {
    /// Create, reuse or reacquire a session for this registered worker.
    ///
    /// Server owns admission, requirements, capacity and holder authority.
    /// An admitted reacquisition requires rebuilding worker-local resources.
    pub async fn create_worker_session(
        &self,
        worker_id: &str,
        options: &WorkerSessionOptions,
    ) -> Result<Value> {
        identifier("worker_id", worker_id)?;
        let mut body = options.to_wire()?;
        body["worker_id"] = json!(worker_id);
        self.request_json(
            reqwest::Method::POST,
            "/worker/sessions",
            RequestProtocol::Worker(WORKER_PROTOCOL_VERSION),
            Some(&body),
        )
        .await
    }

    /// Renew only the current session holder's lease. This does not extend TTL.
    pub async fn renew_worker_session(
        &self,
        worker_id: &str,
        session_id: &str,
        lease_seconds: u64,
    ) -> Result<Value> {
        identifier("worker_id", worker_id)?;
        identifier("session_id", session_id)?;
        if lease_seconds == 0 || lease_seconds > i64::MAX as u64 {
            return Err(invalid(
                "lease_seconds must be a positive signed 64-bit integer",
            ));
        }
        self.request_json(
            reqwest::Method::POST,
            &format!(
                "/worker/sessions/{}/heartbeat",
                percent_encode_path_segment(session_id)
            ),
            RequestProtocol::Worker(WORKER_PROTOCOL_VERSION),
            Some(&json!({"worker_id":worker_id,"lease_seconds":lease_seconds})),
        )
        .await
    }

    /// Close one holder's session. A closed identity cannot be reacquired.
    pub async fn close_worker_session(
        &self,
        worker_id: &str,
        session_id: &str,
        reason: &str,
    ) -> Result<Value> {
        identifier("worker_id", worker_id)?;
        identifier("session_id", session_id)?;
        self.request_json(
            reqwest::Method::DELETE,
            &format!(
                "/worker/sessions/{}",
                percent_encode_path_segment(session_id)
            ),
            RequestProtocol::Worker(WORKER_PROTOCOL_VERSION),
            Some(&json!({"worker_id":worker_id,"reason":reason})),
        )
        .await
    }
}

impl ActivityCall {
    /// Route this remote activity through a durable worker-session identity.
    ///
    /// The matching worker needs the session capability and its requirements.
    /// Options are validated before scheduling and checked during cold replay.
    pub fn in_worker_session(mut self, options: WorkerSessionOptions) -> Self {
        self.worker_session = Some(options);
        self
    }

    /// Decode this activity result directly from the lossless Avro value.
    pub async fn typed<O: serde::de::DeserializeOwned>(mut self) -> Result<O> {
        let activity_type = self.activity_type.clone();
        let result =
            std::future::poll_fn(|cx| std::pin::Pin::new(&mut self).poll_avro_value(cx)).await?;
        crate::decode_handler_result(result, HandlerKind::Activity, &activity_type)
    }

    /// Return the lossless Avro result of this activity call.
    pub async fn avro_value(mut self) -> Result<AvroValue> {
        std::future::poll_fn(|cx| std::pin::Pin::new(&mut self).poll_avro_value(cx)).await
    }
}

impl crate::ParallelCall {
    /// Route every activity leaf in this parallel group through one session.
    pub fn in_worker_session(mut self, options: WorkerSessionOptions) -> Self {
        self.worker_session = Some(options);
        self
    }
}

pub(super) fn recorded_session(events: &[&HistoryEvent], sequence: u64) -> Result<Option<Value>> {
    let mut original = None;
    for event in events {
        for source in [Some(&event.payload), event.payload.get("activity")]
            .into_iter()
            .flatten()
        {
            let Some(value) = source.get("worker_session") else {
                continue;
            };
            let session = if value.is_null() {
                None
            } else {
                let mut value = value.clone();
                if let Some(object) = value.as_object_mut() {
                    for field in ["lease_seconds", "ttl_seconds", "max_concurrent_activities"] {
                        if object.get(field).is_some_and(Value::is_null) {
                            object.remove(field);
                        }
                    }
                }
                let options = serde_json::from_value::<WorkerSessionOptions>(value)
                    .and_then(|options| options.to_wire().map_err(serde::de::Error::custom))
                    .map_err(|error| {
                        crate::invalid_recorded_history(
                            "worker_session_invalid",
                            sequence,
                            "valid worker-session routing",
                            &error.to_string(),
                            "recorded worker-session metadata is invalid",
                        )
                    })?;
                Some(options)
            };
            if original.as_ref().is_some_and(|old| old != &session) {
                return Err(crate::invalid_recorded_history(
                    "worker_session_conflict",
                    sequence,
                    "one worker-session identity",
                    "conflicting session options",
                    "activity history changes worker-session routing at one command boundary",
                ));
            }
            original = Some(session);
        }
    }
    Ok(original.flatten())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_routing_and_lifetime_are_refused_before_dispatch() {
        for options in [
            WorkerSessionOptions::new(" "),
            WorkerSessionOptions::new("render").queue(" "),
            WorkerSessionOptions::new("render").connection(""),
            WorkerSessionOptions::new("render").requirements([""]),
            WorkerSessionOptions::new("render").lease_seconds(0),
            WorkerSessionOptions::new("render").ttl_seconds(0),
            WorkerSessionOptions::new("render").max_concurrent_activities(0),
        ] {
            assert!(
                matches!(options.to_wire(), Err(Error::WorkerLoop(message)) if message.starts_with("invalid_worker_session:"))
            );
        }
    }

    #[test]
    fn session_requirements_have_one_canonical_wire_identity() {
        let a =
            WorkerSessionOptions::new(" render ").requirements(["gpu:l4", "codec:av1", "gpu:l4"]);
        let b = WorkerSessionOptions::new("render").requirements(["codec:av1", "gpu:l4"]);
        assert_eq!(a, b);
        assert_eq!(a.to_wire().unwrap(), b.to_wire().unwrap());
        assert_eq!(
            a.to_wire().unwrap()["requirements"],
            json!(["codec:av1", "gpu:l4"])
        );
    }
}
