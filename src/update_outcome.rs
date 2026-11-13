//! Pending update outcomes and reading an accepted update's outcome later
//! by its update id (durable-workflow/.github#212, server#1131).
//!
//! A Server that ends a completion wait before the update handler finishes
//! answers HTTP 202 with no result. That is a pending update, not a handler
//! that returned null, so the client raises [`Error::UpdateStillPending`]
//! instead of returning `Null`. On a Server that advertises
//! `capabilities.update_outcome_reads`, [`Client::update_outcome`] reads the
//! same update again by its id without submitting it again.

use std::{fmt, sync::atomic::Ordering, time::Duration};

use serde_json::Value;

use crate::{
    decode_wire_avro_value, decode_wire_value, AvroValue, Client, Error, RawRequest,
    RequestProtocol, Result, WorkflowHandle, DEFAULT_CODEC,
};

/// The cluster capability that advertises `GET /workflows/{id}/updates/{update_id}`.
pub(crate) const UPDATE_OUTCOME_READS_CAPABILITY: &str = "update_outcome_reads";

/// Extra transport time allowed beyond a requested completion wait.
const WAIT_TRANSPORT_MARGIN: Duration = Duration::from_secs(30);

/// How long an update outcome read waits before the Server answers.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum UpdateWaitStage {
    /// Answer at once with the update's current receipt (the Server default).
    #[default]
    Accepted,
    /// Wait for the update's outcome, up to the wait timeout.
    Completed,
}

impl UpdateWaitStage {
    /// The `wait_for` value sent to the Server.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Completed => "completed",
        }
    }
}

/// An update the Server accepted that had not completed when the wait ended.
///
/// It has no result yet: this is never a handler that returned null. Read it
/// again by [`update_id`](Self::update_id) with [`Client::update_outcome`] on a
/// Server that advertises `update_outcome_reads` (native 3.6.0 and later). On
/// an older Server, call [`Client::update_workflow`] again with the same
/// [`request_id`](Self::request_id): the Server returns the retained update
/// instead of admitting a second one.
///
/// `run_id` names the run that owns the update now, which can be a
/// continue-as-new successor. `receipt` is the Server's full answer.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct UpdatePending {
    pub workflow_id: String,
    pub run_id: Option<String>,
    pub update_name: Option<String>,
    pub update_id: Option<String>,
    pub request_id: Option<String>,
    /// The Server's `update_status`, normally `accepted`.
    pub update_status: Option<String>,
    /// Whether the Server's completion wait ran out.
    pub wait_timed_out: bool,
    pub receipt: Value,
    from_outcome_read: bool,
}

impl UpdatePending {
    fn from_receipt(workflow_id: &str, receipt: Value, from_outcome_read: bool) -> Self {
        let string = |name: &str| {
            receipt
                .get(name)
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
        };
        Self {
            workflow_id: string("workflow_id").unwrap_or_else(|| workflow_id.to_string()),
            run_id: string("run_id"),
            update_name: string("update_name"),
            update_id: string("update_id"),
            request_id: string("request_id"),
            update_status: string("update_status"),
            wait_timed_out: receipt.get("wait_timed_out") == Some(&Value::Bool(true)),
            receipt,
            from_outcome_read,
        }
    }
}

impl fmt::Display for UpdatePending {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let status = self.update_status.as_deref().unwrap_or("pending");
        let update = match (&self.update_name, &self.update_id) {
            (Some(name), Some(id)) => format!("{name} ({id})"),
            (Some(name), None) => name.clone(),
            (None, Some(id)) => id.clone(),
            (None, None) => "update".to_string(),
        };
        write!(
            f,
            "update_still_pending: update {update} on workflow {} is still {status}; it has no result yet.",
            self.workflow_id
        )?;
        match (&self.update_id, &self.request_id) {
            (Some(id), _) if self.from_outcome_read => write!(
                f,
                " Call update_outcome() again with update id {id:?} to keep waiting. Reading never admits another update."
            ),
            (Some(id), Some(key)) => write!(
                f,
                " Read it again with update_outcome() and update id {id:?} on a Server that advertises update_outcome_reads, or call update_workflow() again with request_id {key:?}: the Server returns the retained update instead of admitting a second one."
            ),
            (Some(id), None) => write!(
                f,
                " Read it again with update_outcome() and update id {id:?} on a Server that advertises update_outcome_reads."
            ),
            (None, Some(key)) => write!(
                f,
                " Call update_workflow() again with request_id {key:?}: the Server returns the retained update instead of admitting a second one."
            ),
            (None, None) => Ok(()),
        }
    }
}

impl std::error::Error for UpdatePending {}

/// Whether a completion-wait receipt describes an update that has not finished.
///
/// A receipt with a result envelope is finished (an encoded null is a real
/// null result). Otherwise a timed-out wait, or any status other than
/// `completed`, `failed` or `rejected`, is pending.
pub(crate) fn update_receipt_is_pending(receipt: &Value) -> bool {
    if receipt.get("result_envelope").is_some_and(Value::is_object) {
        return false;
    }
    receipt.get("wait_timed_out") == Some(&Value::Bool(true))
        || receipt
            .get("update_status")
            .and_then(Value::as_str)
            .is_some_and(|status| !matches!(status, "completed" | "failed" | "rejected"))
}

/// Raise [`Error::UpdateStillPending`] for a pending completion-wait receipt.
pub(crate) fn reject_pending_update(
    workflow_id: &str,
    receipt: Value,
    from_outcome_read: bool,
) -> Result<Value> {
    if update_receipt_is_pending(&receipt) {
        return Err(Error::UpdateStillPending(UpdatePending::from_receipt(
            workflow_id,
            receipt,
            from_outcome_read,
        )));
    }
    Ok(receipt)
}

/// Whether a successful answer is the Server's receipt for the update that was read.
fn is_receipt_for(receipt: &Value, workflow_id: &str, update_id: &str) -> bool {
    receipt.get("update_id").and_then(Value::as_str) == Some(update_id)
        && receipt.get("update_status").is_some_and(Value::is_string)
        && receipt
            .get("workflow_id")
            .and_then(Value::as_str)
            .is_none_or(|echoed| echoed == workflow_id)
}

fn unrecognized(workflow_id: &str, update_id: &str, receipt: Value) -> Error {
    Error::UnrecognizedUpdateOutcome {
        workflow_id: workflow_id.to_string(),
        update_id: update_id.to_string(),
        receipt,
    }
}

fn completed_update_avro_value(receipt: &Value) -> Result<AvroValue> {
    let envelope = receipt
        .get("result_envelope")
        .filter(|envelope| !envelope.is_null())
        .ok_or_else(|| {
            Error::Codec(
                "missing_payload_envelope: typed update result requires result_envelope"
                    .to_string(),
            )
        })?;
    decode_wire_avro_value(envelope, DEFAULT_CODEC)
}

impl Client {
    /// Read an accepted update's outcome later by its update id, without
    /// submitting it again.
    ///
    /// This waits for the outcome, like [`Client::update_workflow`], for up to
    /// `wait_timeout_seconds` (the Server's update wait default when `None`).
    /// It returns the decoded result of a completed update, including `Null`
    /// when the handler returned null. While the update is still running it
    /// returns [`Error::UpdateStillPending`]: call it again with the same
    /// update id to keep waiting.
    ///
    /// A failed update is [`Error::Http`] with status 422 and the failure
    /// receipt, and a rejected update keeps its original rejection status. An
    /// unknown or pruned update id is 404 `update_not_found`
    /// ([`Error::is_update_not_found`]), which does not prove the update was
    /// never admitted.
    ///
    /// Needs a Server that advertises `capabilities.update_outcome_reads`
    /// (native 3.6.0 and later). Any other Server is refused with
    /// [`Error::UpdateOutcomeReadsUnavailable`] before anything is read: the
    /// SDK never resubmits an update as a substitute for reading it.
    pub async fn update_outcome(
        &self,
        workflow_id: &str,
        update_id: &str,
        wait_timeout_seconds: Option<u64>,
    ) -> Result<Value> {
        let receipt = self
            .completed_update_outcome(workflow_id, update_id, wait_timeout_seconds)
            .await?;
        match receipt
            .get("result_envelope")
            .filter(|envelope| !envelope.is_null())
        {
            Some(envelope) => decode_wire_value(envelope, DEFAULT_CODEC),
            None => Ok(receipt.get("result").cloned().unwrap_or(Value::Null)),
        }
    }

    /// [`Client::update_outcome`] that retains a bytes-capable Avro result.
    pub async fn update_outcome_avro_value(
        &self,
        workflow_id: &str,
        update_id: &str,
        wait_timeout_seconds: Option<u64>,
    ) -> Result<AvroValue> {
        let receipt = self
            .completed_update_outcome(workflow_id, update_id, wait_timeout_seconds)
            .await?;
        completed_update_avro_value(&receipt)
    }

    /// Read an accepted update's Server receipt by update id. It writes nothing.
    ///
    /// [`UpdateWaitStage::Accepted`] answers at once; a still-running update
    /// comes back as a receipt with `update_status: "accepted"`, never as an
    /// error. [`UpdateWaitStage::Completed`] waits up to
    /// `wait_timeout_seconds` first. Failures, rejections, `update_not_found`
    /// and the capability check behave as in [`Client::update_outcome`].
    pub async fn update_outcome_receipt(
        &self,
        workflow_id: &str,
        update_id: &str,
        wait_for: UpdateWaitStage,
        wait_timeout_seconds: Option<u64>,
    ) -> Result<Value> {
        if update_id.trim().is_empty() {
            return Err(Error::Codec(
                "an update outcome read needs the update id".to_string(),
            ));
        }
        // Each id must stay one path segment: "." or ".." would collapse the
        // read onto another route, such as the workflow description.
        let mut path = RawRequest::get(["workflows", workflow_id, "updates", update_id])
            .path()
            .map_err(|_| {
                Error::Codec(
                    "an update outcome read needs a workflow id and update id that are each one path segment: not empty, \".\" or \"..\"".to_string(),
                )
            })?;
        self.require_update_outcome_reads().await?;

        let mut query = Vec::new();
        if wait_for == UpdateWaitStage::Completed {
            query.push(format!("wait_for={}", wait_for.as_str()));
        }
        if let Some(seconds) = wait_timeout_seconds {
            query.push(format!("wait_timeout_seconds={seconds}"));
        }
        if !query.is_empty() {
            path.push('?');
            path.push_str(&query.join("&"));
        }
        let timeout = wait_timeout_seconds
            .map(|seconds| Duration::from_secs(seconds).saturating_add(WAIT_TRANSPORT_MARGIN))
            .unwrap_or(Duration::ZERO)
            .max(Duration::from_secs(60));
        let receipt: Value = self
            .request_json_with_timeout(
                reqwest::Method::GET,
                &path,
                RequestProtocol::ControlPlane,
                Option::<&Value>::None,
                timeout,
            )
            .await?;
        if !is_receipt_for(&receipt, workflow_id, update_id) {
            return Err(unrecognized(workflow_id, update_id, receipt));
        }
        Ok(receipt)
    }

    async fn completed_update_outcome(
        &self,
        workflow_id: &str,
        update_id: &str,
        wait_timeout_seconds: Option<u64>,
    ) -> Result<Value> {
        let receipt = self
            .update_outcome_receipt(
                workflow_id,
                update_id,
                UpdateWaitStage::Completed,
                wait_timeout_seconds,
            )
            .await?;
        let receipt = reject_pending_update(workflow_id, receipt, true)?;
        // Only a completed receipt carries an outcome; never read one into
        // any other successful answer.
        let completed = receipt.get("result_envelope").is_some_and(Value::is_object)
            || receipt.get("update_status").and_then(Value::as_str) == Some("completed");
        if !completed {
            return Err(unrecognized(workflow_id, update_id, receipt));
        }
        Ok(receipt)
    }

    /// Refuse a Server that does not advertise `update_outcome_reads`.
    ///
    /// Discovery runs until a Server advertises the capability; after that the
    /// client remembers it.
    async fn require_update_outcome_reads(&self) -> Result<()> {
        if self.update_outcome_reads_supported.load(Ordering::SeqCst) {
            return Ok(());
        }
        let discovery = self.cluster_info().await?;
        if discovery["capabilities"][UPDATE_OUTCOME_READS_CAPABILITY] != Value::Bool(true) {
            return Err(Error::UpdateOutcomeReadsUnavailable);
        }
        self.update_outcome_reads_supported
            .store(true, Ordering::SeqCst);
        Ok(())
    }
}

impl WorkflowHandle {
    /// Read one of this workflow's accepted updates by update id; see
    /// [`Client::update_outcome`].
    pub async fn update_outcome(
        &self,
        update_id: &str,
        wait_timeout_seconds: Option<u64>,
    ) -> Result<Value> {
        self.client
            .update_outcome(&self.workflow_id, update_id, wait_timeout_seconds)
            .await
    }

    /// Read one of this workflow's accepted updates by update id with a
    /// bytes-capable Avro result; see [`Client::update_outcome_avro_value`].
    pub async fn update_outcome_avro_value(
        &self,
        update_id: &str,
        wait_timeout_seconds: Option<u64>,
    ) -> Result<AvroValue> {
        self.client
            .update_outcome_avro_value(&self.workflow_id, update_id, wait_timeout_seconds)
            .await
    }
}

impl Client {
    /// Decode a submitted update's completion-wait receipt.
    pub(crate) fn submitted_update_value(workflow_id: &str, receipt: Value) -> Result<Value> {
        let receipt = reject_pending_update(workflow_id, receipt, false)?;
        if let Some(envelope) = receipt
            .get("result_envelope")
            .filter(|envelope| !envelope.is_null())
        {
            return decode_wire_value(envelope, DEFAULT_CODEC);
        }
        Ok(receipt.get("result").cloned().unwrap_or(receipt))
    }

    /// Decode a submitted update's completion-wait receipt as a typed Avro value.
    pub(crate) fn submitted_update_avro_value(
        workflow_id: &str,
        receipt: Value,
    ) -> Result<AvroValue> {
        let receipt = reject_pending_update(workflow_id, receipt, false)?;
        completed_update_avro_value(&receipt)
    }
}

#[cfg(test)]
mod tests;
