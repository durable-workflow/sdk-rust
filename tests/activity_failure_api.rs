//! `ActivityFailure` is a public struct with public fields, so callers can
//! build it with a struct literal. Adding a field breaks that, so the 4.x
//! field set is pinned here: this file stops compiling if a field is added,
//! removed or retyped. Change it only in a major release.

use durable_workflow::{ActivityFailure, ActivityFailureKind, LeaseLossReason};
use serde_json::json;

fn literal(exception_type: Option<&str>, exception_class: Option<&str>) -> ActivityFailure {
    // Exactly the durable-workflow 4.21.0 field set.
    ActivityFailure {
        kind: ActivityFailureKind::Failed,
        reason: "internal".to_string(),
        message: "lost".to_string(),
        activity_execution_id: Some("activity-1".to_string()),
        activity_attempt_id: Some("attempt-1".to_string()),
        activity_type: Some("echo".to_string()),
        activity_class: None,
        attempt_number: Some(1),
        failure_id: None,
        failure_category: Some("internal".to_string()),
        timeout_kind: None,
        non_retryable: false,
        exception_type: exception_type.map(str::to_string),
        exception_class: exception_class.map(str::to_string),
        code: None,
        exception: Some(json!({"message": "lost"})),
    }
}

#[test]
fn activity_failure_keeps_its_4_21_struct_literal() {
    let released = literal(Some("worker_lost"), Some("ActivityWorkerLost"));
    assert_eq!(
        released.lease_loss_reason(),
        Some(LeaseLossReason::WorkerLost)
    );
    let expired = literal(
        None,
        Some("Workflow\\V2\\Exceptions\\ActivityLeaseExpiredException"),
    );
    assert_eq!(
        expired.lease_loss_reason(),
        Some(LeaseLossReason::LeaseExpired)
    );
    assert_eq!(literal(Some("Declined"), None).lease_loss_reason(), None);
}
