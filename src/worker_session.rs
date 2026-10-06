//! Worker-held session routing. Session memory is never durable workflow state.

use crate::{
    percent_encode_path_segment, Client, Error, RequestProtocol, Result, WORKER_PROTOCOL_VERSION,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

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
        if lease_seconds == 0 {
            return Err(invalid("lease_seconds must be positive"));
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
