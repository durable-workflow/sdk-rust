//! Read-only activity visibility for one explicit workflow run.

use crate::{percent_encode_path_segment, Client, RequestProtocol, Result, Value, WorkflowHandle};
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::BTreeMap;

/// A malformed or misdirected run-activity response.
///
/// Diagnostics identify the violated contract without copying response values.
#[derive(Clone, Debug, thiserror::Error)]
#[error("run activity metadata failed ({reason}): {message}")]
#[non_exhaustive]
pub struct WorkflowActivityMetadataError {
    pub reason: String,
    pub message: String,
}

fn invalid(reason: &str, message: &str) -> crate::Error {
    WorkflowActivityMetadataError {
        reason: reason.into(),
        message: message.into(),
    }
    .into()
}

/// Activity and attempt visibility returned for one selected run.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[non_exhaustive]
pub struct WorkflowActivityList {
    pub workflow_id: String,
    pub run_id: String,
    /// An explicit empty list means the Server reported no activities.
    pub activities: Vec<WorkflowActivity>,
    /// Unrecognized response fields, retained for forward compatibility.
    #[serde(flatten)]
    pub raw: BTreeMap<String, Value>,
}

/// A run's activity metadata. Missing or null optional fields stay unavailable.
///
/// Status and authority strings are Server-owned and may include unsupported,
/// diagnostic or future states. This snapshot does not grant worker authority.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[non_exhaustive]
pub struct WorkflowActivity {
    pub id: Option<String>,
    pub idempotency_key: Option<String>,
    pub sequence: Option<i64>,
    #[serde(rename = "type")]
    pub activity_type: Option<String>,
    #[serde(rename = "class")]
    pub activity_class: Option<String>,
    pub execution_mode: Option<String>,
    pub local_activity: Option<bool>,
    pub history_authority: Option<String>,
    pub diagnostic_only: Option<bool>,
    pub history_event_types: Option<Vec<String>>,
    pub history_unsupported_reason: Option<String>,
    pub reused_from_run_id: Option<String>,
    pub reused_activity_execution_id: Option<String>,
    pub reused_recorded_at: Option<String>,
    pub row_status: Option<String>,
    pub parallel_group_kind: Option<String>,
    pub parallel_group_id: Option<String>,
    pub parallel_group_base_sequence: Option<i64>,
    pub parallel_group_size: Option<i64>,
    pub parallel_group_index: Option<i64>,
    pub parallel_group_path: Option<Vec<BTreeMap<String, Value>>>,
    pub attempt_id: Option<String>,
    pub worker_attempt_id: Option<String>,
    pub status: Option<String>,
    pub attempt_count: Option<i64>,
    /// The Server's policy snapshot, preserved without reinterpreting its version.
    pub retry_policy: Option<Value>,
    pub connection: Option<String>,
    pub queue: Option<String>,
    pub last_heartbeat_at: Option<String>,
    #[serde(default, deserialize_with = "heartbeat_progress")]
    pub last_heartbeat_progress: Option<Value>,
    pub created_at: Option<String>,
    pub started_at: Option<String>,
    pub closed_at: Option<String>,
    /// `None` is unavailable; `Some(vec![])` is an explicitly empty attempt list.
    pub attempts: Option<Vec<WorkflowActivityAttempt>>,
    /// Unrecognized fields, including any producer-specific inspection values.
    #[serde(flatten)]
    pub raw: BTreeMap<String, Value>,
}

/// Recorded attempt state. These flags describe visibility, not action grants.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[non_exhaustive]
pub struct WorkflowActivityAttempt {
    pub id: Option<String>,
    pub worker_attempt_id: Option<String>,
    pub attempt_number: Option<i64>,
    pub status: Option<String>,
    pub task_id: Option<String>,
    pub lease_owner: Option<String>,
    pub lease_expires_at: Option<String>,
    pub started_at: Option<String>,
    pub last_heartbeat_at: Option<String>,
    #[serde(default, deserialize_with = "heartbeat_progress")]
    pub last_heartbeat_progress: Option<Value>,
    pub closed_at: Option<String>,
    pub can_continue: Option<bool>,
    pub cancel_requested: Option<bool>,
    pub stop_reason: Option<String>,
    pub failure_reason: Option<String>,
    #[serde(flatten)]
    pub raw: BTreeMap<String, Value>,
}

fn heartbeat_progress<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<Value>, D::Error> {
    match Option::<Value>::deserialize(deserializer)? {
        Some(value @ (Value::Array(_) | Value::Object(_))) => Ok(Some(value)),
        None => Ok(None),
        _ => Err(serde::de::Error::custom(
            "heartbeat progress must be an object, array or null",
        )),
    }
}

impl Client {
    /// Inspect activity and attempt metadata for one explicit run.
    ///
    /// This uses the existing namespace-scoped control-plane route, distinct
    /// from standalone activities. Nullable metadata stays optional, and no
    /// workflow payload is fetched or decoded. Unknown fields remain in `raw`.
    pub async fn get_run_activities(
        &self,
        workflow_id: &str,
        run_id: &str,
    ) -> Result<WorkflowActivityList> {
        let path = format!(
            "/workflows/{}/runs/{}/activities",
            percent_encode_path_segment(workflow_id),
            percent_encode_path_segment(run_id),
        );
        let document: Value = self
            .request_json(
                reqwest::Method::GET,
                &path,
                RequestProtocol::ControlPlane,
                Option::<&Value>::None,
            )
            .await?;
        let view: WorkflowActivityList = serde_json::from_value(document).map_err(|_| {
            invalid(
                "malformed_response",
                "the response does not match the run activity metadata contract",
            )
        })?;
        if view.workflow_id != workflow_id || view.run_id != run_id {
            return Err(invalid(
                "run_identity_mismatch",
                "the response belongs to another workflow or run",
            ));
        }
        Ok(view)
    }
}

impl WorkflowHandle {
    /// Inspect the run originally selected by this handle.
    ///
    /// A handle without a run ID refuses before sending a request. Use
    /// [`Client::get_run_activities`] to select a historical run explicitly.
    pub async fn get_activities(&self) -> Result<WorkflowActivityList> {
        let run_id = self.run_id.as_deref().ok_or_else(|| {
            invalid(
                "missing_run_id",
                "a selected run ID is required for activity inspection",
            )
        })?;
        self.client
            .get_run_activities(&self.workflow_id, run_id)
            .await
    }
}
