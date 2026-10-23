//! Immutable retry metadata for an activity invocation.

use serde_json::Value;

/// The configured attempt budget in an activity's retry snapshot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ActivityAttemptLimit {
    /// The snapshot or a valid limit is unavailable. This does not mean unlimited.
    Unknown,
    /// The snapshot explicitly contains `"max_attempts": null`.
    Unlimited,
    /// The snapshot contains a positive maximum count, including the first attempt.
    Bounded(u64),
}

/// Original attempt identity and retry policy, captured before invoking a handler.
///
/// Service workers retain the Server claim's JSON without filling defaults or
/// discarding unknown fields. Direct activity tests and local activities have no
/// received Server snapshot and report an unknown policy. Fresh workflow tests
/// expose the same modeled snapshot they record in their generated history.
#[derive(Clone, Debug, PartialEq)]
pub struct ActivityRetryInfo {
    attempt_number: u64,
    policy: Option<Value>,
}

impl ActivityRetryInfo {
    pub(crate) fn new(attempt_number: u64, policy: Option<Value>) -> Self {
        Self {
            attempt_number,
            policy,
        }
    }

    /// The original attempt number. Mutating the context's public compatibility
    /// field does not change this captured identity or its budget checks.
    pub fn attempt_number(&self) -> u64 {
        self.attempt_number
    }

    /// The original policy JSON. `None` means omitted, while `Some(Value::Null)`
    /// preserves an explicitly null legacy policy. Neither implies unlimited.
    pub fn policy(&self) -> Option<&Value> {
        self.policy.as_ref()
    }

    /// Interpret only a positive integer or explicit null `max_attempts` field.
    /// Missing, malformed and legacy values remain unknown and are preserved.
    pub fn attempt_limit(&self) -> ActivityAttemptLimit {
        match self
            .policy
            .as_ref()
            .and_then(|policy| policy.get("max_attempts"))
        {
            Some(Value::Null) => ActivityAttemptLimit::Unlimited,
            Some(value) => match value.as_u64().filter(|limit| *limit > 0) {
                Some(limit) => ActivityAttemptLimit::Bounded(limit),
                None => ActivityAttemptLimit::Unknown,
            },
            None => ActivityAttemptLimit::Unknown,
        }
    }

    /// Whether this is the final configured attempt (or already exceeds its budget).
    ///
    /// Returns `None` for an unknown policy or invalid zero attempt number, and
    /// `Some(false)` for explicit unlimited retries. A remaining budget does not
    /// promise another execution: non-retryable failures, cancellation, workflow
    /// closure and elapsed deadlines may end execution earlier.
    pub fn is_last_configured_attempt(&self) -> Option<bool> {
        if self.attempt_number == 0 {
            return None;
        }
        match self.attempt_limit() {
            ActivityAttemptLimit::Unknown => None,
            ActivityAttemptLimit::Unlimited => Some(false),
            ActivityAttemptLimit::Bounded(limit) => Some(self.attempt_number >= limit),
        }
    }
}

pub(super) fn deserialize_policy<'de, D>(deserializer: D) -> Result<Option<Value>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    // Option<Value>'s usual deserializer collapses explicit null into absence.
    <Value as serde::Deserialize>::deserialize(deserializer).map(Some)
}
