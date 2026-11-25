//! Worker side of deadline-triggered timeout cleanup (Server #1201). The
//! portable contract is Workflow's `docs/architecture/timeout-cleanup.md`.
//!
//! A worker that delivers cooperative cancellation advertises
//! `timeout_cleanup` version 1, so the Server admits starts that reserve
//! cleanup time on its queue. At the work deadline the Server records an
//! engine-owned cooperative cancellation request whose root context carries
//! `cause: "timeout"`; it reaches workflow code as
//! [`Error::CooperativeCancellationRequested`]. When workflow code lets that
//! delivered error escape, uncaught or returned unchanged, the
//! `fail_workflow` command carries `failure.interruption: "timeout_cleanup"`
//! and the Server records the cleanup as completed. Every other failure,
//! including one that wraps the interruption, stays an ordinary failure, which
//! the Server records as failed cleanup. The run ends timed out either way.
use crate::{Error, WorkflowState};
use serde_json::{json, Value};

/// The worker capability and its manifest key.
pub(super) const CAPABILITY: &str = "timeout_cleanup";
/// The language-neutral marker on `fail_workflow.failure.interruption`.
const INTERRUPTION: &str = "timeout_cleanup";
/// Timeout cleanup arrives through cooperative cancellation (protocol 1.20).
const MINIMUM_PROTOCOL_VERSION: &str = "1.20";

/// The `capability_manifest.timeout_cleanup` entry, as the PHP SDK sends it.
pub(super) fn manifest_entry() -> Value {
    json!({"supported": true, "minimum_protocol_version": MINIMUM_PROTOCOL_VERSION, "version": 1})
}

/// The `fail_workflow.failure` marker for `error`, or `None`.
///
/// `state` holds the canonical request and delivery this replay delivered to
/// workflow code. The marker is returned only when `error` is that exact
/// delivery, unchanged, and the request is the run's own root timeout
/// request: its context has `cause: "timeout"`, the request's identity, and
/// no parent. A variant match alone is not enough, so an error the
/// application builds with other identity, an ordinary or explicit
/// `cancellation` request, a scoped or descendant cancellation, a wrapped
/// interruption and a compensation failure are never marked.
pub(super) fn failure_marker(error: &Error, state: &WorkflowState) -> Option<Value> {
    let Error::CooperativeCancellationRequested(escaped) = error else {
        return None;
    };
    let delivered = &state.cancellation_history;
    let (request, delivery) = (delivered.request.as_ref()?, delivered.delivery.as_ref()?);
    let context = request.context.as_ref()?;
    let root_timeout = context.is_timeout()
        && context.request_id() == request.request_id
        && context.root_request_id() == request.request_id
        && context.parent_request_id().is_none();
    (root_timeout && escaped.request == *request && escaped.delivery == *delivery)
        .then(|| json!({"interruption": INTERRUPTION}))
}
