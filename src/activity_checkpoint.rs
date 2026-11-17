//! Read-only acknowledged heartbeat snapshots supplied by a negotiated claim.

use crate::{Error, Result, Value};
use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use std::fmt;

pub(super) const CAPABILITY: &str = "activity_heartbeat_resume";

/// This claim does not provide the supported heartbeat-resume v1 contract.
///
/// This is distinct from a supported claim that explicitly reports no checkpoint.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ActivityHeartbeatResumeUnavailable;

impl fmt::Display for ActivityHeartbeatResumeUnavailable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("activity_heartbeat_resume v1 is unavailable for this claim")
    }
}

impl std::error::Error for ActivityHeartbeatResumeUnavailable {}

impl From<ActivityHeartbeatResumeUnavailable> for Error {
    fn from(value: ActivityHeartbeatResumeUnavailable) -> Self {
        Self::Codec(value.to_string())
    }
}

/// An immutable checkpoint received with this activity attempt's claim.
///
/// Heartbeat writes never change this snapshot, including when a response is
/// rejected or lost. A later claim may carry a newer acknowledged checkpoint.
/// A checkpoint does not atomically commit an external business effect.
#[derive(Clone, PartialEq)]
pub struct ActivityHeartbeatCheckpoint {
    details: Value,
    acknowledged_at: DateTime<Utc>,
}

impl fmt::Debug for ActivityHeartbeatCheckpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ActivityHeartbeatCheckpoint")
            .field("details", &"<checkpoint>")
            .field("acknowledged_at", &self.acknowledged_at)
            .finish()
    }
}

impl ActivityHeartbeatCheckpoint {
    /// The acknowledged details, without defaults or mutation.
    pub fn details(&self) -> &Value {
        &self.details
    }

    /// Deserialize the received JSON details into an application type.
    pub fn details_as<T: DeserializeOwned>(&self) -> serde_json::Result<T> {
        serde_json::from_value(self.details.clone())
    }

    /// The producer's acknowledgement time, preserving fractional precision.
    pub fn acknowledged_at(&self) -> &DateTime<Utc> {
        &self.acknowledged_at
    }

    /// The negotiated contract version, not a checkpoint write sequence.
    /// Server v1 does not supply a per-checkpoint sequence number.
    pub fn contract_version(&self) -> u64 {
        1
    }
}

#[derive(Clone, Debug, Default)]
pub(super) enum HeartbeatResume {
    #[default]
    Unavailable,
    Available(Option<ActivityHeartbeatCheckpoint>),
}

impl HeartbeatResume {
    pub(super) fn from_poll(wire: &Value, namespace: &str) -> Result<Self> {
        let capability = &wire["server_capabilities"][CAPABILITY];
        if capability["supported"] != true || capability["version"].as_u64() != Some(1) {
            return Ok(Self::Unavailable);
        }
        let task = &wire["task"];
        let (details, at) = (
            task.get("last_heartbeat_details"),
            task.get("last_heartbeat_at"),
        );
        // Older/nonparticipating claims omit both fields. An advertisement alone
        // does not invent a supported empty checkpoint for such a delivery.
        if details.is_none() && at.is_none() {
            return Ok(Self::Unavailable);
        }
        let identity = |field: &str| task[field].as_str().is_some_and(|value| !value.is_empty());
        let attempt = task["attempt_number"].as_u64().filter(|number| *number > 0);
        if !identity("activity_execution_id")
            || !identity("activity_attempt_id")
            || !identity("workflow_id")
            || !identity("run_id")
            || task["namespace"].as_str() != Some(namespace)
            || attempt.is_none()
        {
            return Err(invalid());
        }
        match (details, at) {
            (Some(Value::Null), Some(Value::Null)) => Ok(Self::Available(None)),
            (Some(details), Some(Value::String(at))) if !details.is_null() => {
                if attempt == Some(1) {
                    return Err(invalid());
                }
                let acknowledged_at = DateTime::parse_from_rfc3339(at)
                    .map_err(|_| invalid())?
                    .with_timezone(&Utc);
                Ok(Self::Available(Some(ActivityHeartbeatCheckpoint {
                    details: details.clone(),
                    acknowledged_at,
                })))
            }
            _ => Err(invalid()),
        }
    }

    pub(super) fn version(&self) -> Option<u64> {
        match self {
            Self::Unavailable => None,
            Self::Available(_) => Some(1),
        }
    }

    pub(super) fn checkpoint(
        &self,
    ) -> std::result::Result<Option<&ActivityHeartbeatCheckpoint>, ActivityHeartbeatResumeUnavailable>
    {
        match self {
            Self::Unavailable => Err(ActivityHeartbeatResumeUnavailable),
            Self::Available(checkpoint) => Ok(checkpoint.as_ref()),
        }
    }
}

fn invalid() -> Error {
    Error::Codec("activity_heartbeat_resume v1 claim has an invalid checkpoint".into())
}
