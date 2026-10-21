//! Bounded portable observations, never task/receipt/lease authority.

use std::collections::HashMap;

use opentelemetry::{
    baggage::{BaggageExt, KeyValueMetadata},
    propagation::TextMapPropagator,
    trace::TraceContextExt,
    Context,
};
use opentelemetry_sdk::propagation::{BaggagePropagator, TraceContextPropagator};
use serde_json::{json, Value};

const SCHEMA: &str = "durable-workflow.trace-context";

/// Fixed diagnostic values contain no carrier data or baggage names/values.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Diagnostic {
    /// Optional metadata is malformed, oversized, unsupported or misbound.
    Discarded,
    /// Normalization removed unsupported baggage or canonicalized propagation.
    Filtered,
}

/// Accept the exact implemented v1 capability, independent of Server versions.
pub fn supported(info: &Value) -> bool {
    let value = &info["worker_protocol"]["server_capabilities"]["observability_context"];
    value.is_object()
        && value["supported"] == true
        && value["schema"] == SCHEMA
        && value["version"].as_u64() == Some(1)
        && value["dispatch_schema"] == "durable-workflow.queue-trace"
        && value["links_schema"] == "durable-workflow.trace-links"
        && value["max_context_bytes"].as_u64() == Some(4096)
        && value["max_links"].as_u64() == Some(32)
        && value["max_links_bytes"].as_u64() == Some(4096)
}

/// Identity bounds apply to UTF-8 bytes and reject Unicode controls.
pub fn identity(value: &str) -> bool {
    !value.is_empty() && value.len() <= 512 && !value.chars().any(char::is_control)
}

/// Remove only structural observations when the actual response lacks its
/// original poll identity. Serde's business compatibility defaults are not
/// evidence that an omitted generation was present on the wire.
pub(crate) fn retain_poll_observations(
    value: &mut Value,
    request: &Value,
    path: &str,
    namespace: &str,
) {
    let Some(task) = value.get_mut("task").filter(|task| task.is_object()) else {
        return;
    };
    let query = path == "/worker/query-tasks/poll";
    let activity = path == "/worker/activity-tasks/poll";
    let task_id = if query { "query_task_id" } else { "task_id" };
    let generation = if query {
        "query_task_attempt"
    } else if activity {
        "attempt_number"
    } else {
        "workflow_task_attempt"
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|elapsed| {
            chrono::DateTime::<chrono::Utc>::from_timestamp(
                i64::try_from(elapsed.as_secs()).ok()?,
                elapsed.subsec_nanos(),
            )
        });
    let valid = [task_id, "workflow_id", "run_id", "lease_owner"]
        .into_iter()
        .all(|field| task[field].as_str().is_some_and(identity))
        && request["worker_id"].as_str().is_some_and(identity)
        && task["lease_owner"] == request["worker_id"]
        && task[generation]
            .as_i64()
            .is_some_and(|generation| generation > 0)
        && task.get("namespace").is_none_or(|value| value == namespace)
        && task
            .get("task_queue")
            .is_none_or(|value| value == &request["task_queue"])
        && task
            .get("build_id")
            .is_none_or(|value| value == &request["build_id"])
        && (!activity
            || task["activity_attempt_id"]
                .as_str()
                .or(task["attempt_id"].as_str())
                .is_some_and(identity))
        && task.get("lease_expires_at").is_none_or(|deadline| {
            deadline
                .as_str()
                .and_then(|deadline| chrono::DateTime::parse_from_rfc3339(deadline).ok())
                .is_some_and(|deadline| now.is_some_and(|now| deadline > now))
        });
    if !valid {
        let task = task.as_object_mut().expect("checked object");
        for field in [
            "observability_context",
            "dispatch_observability_context",
            "observability_links",
        ] {
            task.remove(field);
        }
    }
}

fn bounded_json(value: &Value) -> bool {
    // Match Core's conservative escaped Unicode/slash JSON budget.
    serde_json::to_string(value).is_ok_and(|json| {
        json.chars()
            .map(|character| match character {
                '/' => 2,
                character if character.is_ascii() => 1,
                character if character as u32 <= 0xffff => 6,
                _ => 12,
            })
            .sum::<usize>()
            <= 4096
    })
}

fn keys(value: &Value, allowed: &[&str]) -> bool {
    value.as_object().is_some_and(|value| {
        value.len() <= allowed.len() && value.keys().all(|key| allowed.contains(&key.as_str()))
    })
}

/// Normalize with the official W3C propagators from a fresh context. Baggage
/// defaults empty and is allowed only for admission/run parents.
pub fn normalize(value: &Value, allowlist: &[String]) -> (Option<Value>, Option<Diagnostic>) {
    if value.is_null() {
        return (None, None);
    }
    let discarded = (None, Some(Diagnostic::Discarded));
    if !keys(value, &["schema", "version", "carrier"])
        || value["schema"] != SCHEMA
        || value["version"].as_u64() != Some(1)
        || !keys(&value["carrier"], &["traceparent", "tracestate", "baggage"])
        || !value["carrier"]["traceparent"].is_string()
    {
        return discarded;
    }
    let mut headers = HashMap::new();
    for (key, value) in value["carrier"].as_object().expect("checked object") {
        let Some(header) = value.as_str() else {
            return discarded;
        };
        let limit = match key.as_str() {
            "traceparent" => 256,
            "tracestate" => 512,
            _ => 2048,
        };
        if header.len() > limit {
            return discarded;
        }
        headers.insert(key.clone(), header.to_owned());
    }
    if !bounded_json(value) {
        return discarded;
    }
    // Official baggage errors may include rejected header text when an app has
    // enabled OTel internal logs. Silence only this synchronous parsing scope;
    // the application's global subscriber and business logs remain untouched.
    tracing::subscriber::with_default(tracing::subscriber::NoSubscriber::default(), || {
        let propagator = TraceContextPropagator::new();
        let context = propagator.extract_with_context(&Context::new(), &headers);
        if !context.span().span_context().is_valid() {
            return discarded;
        }
        let mut normalized = HashMap::new();
        propagator.inject_context(&context, &mut normalized);
        // Rust's official injector emits an empty tracestate. Omit empty
        // optional headers to match the portable Core envelope.
        normalized.retain(|key, value: &mut String| key != "tracestate" || !value.is_empty());
        if !allowlist.is_empty() && headers.contains_key("baggage") {
            let baggage = BaggagePropagator::new();
            let context = baggage.extract_with_context(&Context::new(), &headers);
            let retained: Vec<_> = context
                .baggage()
                .iter()
                .filter(|(key, _)| allowlist.iter().any(|allowed| allowed == key.as_str()))
                .map(|(key, (value, metadata))| {
                    KeyValueMetadata::new(key.clone(), value.clone(), metadata.clone())
                })
                .collect();
            if retained.len() > 32 {
                return discarded;
            }
            baggage.inject_context(&Context::new().with_baggage(retained), &mut normalized);
        }
        if normalized
            .get("baggage")
            .is_some_and(|header: &String| header.len() > 2048)
        {
            return discarded;
        }
        let result = json!({"schema": SCHEMA, "version": 1, "carrier": normalized});
        if !bounded_json(&result) {
            return discarded;
        }
        let diagnostic = (result["carrier"] != value["carrier"]).then_some(Diagnostic::Filtered);
        (Some(result), diagnostic)
    })
}

/// Capture the caller before starting an HTTP transport span. Propagators and
/// sampling remain application-owned. Panic/failure cannot change business I/O.
pub fn capture(allowlist: &[String]) -> (Option<Value>, Option<Diagnostic>) {
    std::panic::catch_unwind(|| {
        let mut headers = HashMap::new();
        opentelemetry::global::get_text_map_propagator(|propagator| {
            propagator.inject_context(&Context::current(), &mut headers);
        });
        headers.retain(|key, _| matches!(key.as_str(), "traceparent" | "tracestate" | "baggage"));
        if headers.is_empty() {
            return (None, None);
        }
        normalize(
            &json!({"schema": SCHEMA, "version": 1, "carrier": headers}),
            allowlist,
        )
    })
    .unwrap_or((None, Some(Diagnostic::Discarded)))
}

/// Read a task-bound queue producer link after the original claim is admitted.
/// Baggage is forbidden; this context never replaces the run parent.
pub fn dispatch(value: &Value, task_id: &str) -> (Option<Value>, Option<Diagnostic>) {
    if value.is_null() {
        return (None, None);
    }
    let discarded = (None, Some(Diagnostic::Discarded));
    if !identity(task_id)
        || !keys(value, &["schema", "version", "task_id", "context"])
        || value["schema"] != "durable-workflow.queue-trace"
        || value["version"].as_u64() != Some(1)
        || value["task_id"] != task_id
        || !keys(&value["context"]["carrier"], &["traceparent", "tracestate"])
    {
        return discarded;
    }
    let normalized = normalize(&value["context"], &[]);
    if normalized.0.is_none() || !bounded_json(value) {
        return discarded;
    }
    normalized
}

/// Read bounded secondary observations only after the enclosing original claim
/// is admitted. Nullable child producers are retained without fabrication.
pub fn links(
    value: &Value,
    task_id: &str,
    run_id: &str,
    namespace: &str,
) -> (Vec<Value>, Option<Diagnostic>) {
    if value.is_null() {
        return (Vec::new(), None);
    }
    let discarded = || (Vec::new(), Some(Diagnostic::Discarded));
    if ![task_id, run_id, namespace].into_iter().all(identity)
        || !keys(
            value,
            &[
                "schema",
                "version",
                "task_id",
                "run_id",
                "namespace",
                "links",
            ],
        )
        || value["schema"] != "durable-workflow.trace-links"
        || value["version"].as_u64() != Some(1)
        || value["task_id"] != task_id
        || value["run_id"] != run_id
        || value["namespace"] != namespace
    {
        return discarded();
    }
    let Some(entries) = value["links"]
        .as_array()
        .filter(|entries| entries.len() <= 32)
    else {
        return discarded();
    };
    let mut result = Vec::with_capacity(entries.len());
    let mut filtered = false;
    for entry in entries {
        let Some((normalized, diagnostic)) = link_entry(entry) else {
            return discarded();
        };
        result.push(normalized);
        filtered |= diagnostic.is_some();
    }
    if !bounded_json(value) {
        return discarded();
    }
    (result, filtered.then_some(Diagnostic::Filtered))
}

fn link_entry(value: &Value) -> Option<(Value, Option<Diagnostic>)> {
    let (required, optional): (&[&str], &[&str]) = match value["kind"].as_str()? {
        "command" => (&["command_id"], &[]),
        "task_outcome" => (
            &["task_id", "event_id", "sequence", "event_type"],
            &["activity_execution_id", "activity_attempt_id", "timer_id"],
        ),
        "child_outcome" => (
            &[
                "workflow_link_id",
                "child_run_id",
                "event_id",
                "sequence",
                "event_type",
            ],
            &["child_task_id"],
        ),
        _ => return None,
    };
    let allowed: Vec<_> = required
        .iter()
        .chain(optional)
        .copied()
        .chain(["kind", "context"])
        .collect();
    if !keys(value, &allowed) || required.iter().any(|key| value.get(key).is_none()) {
        return None;
    }
    for key in required.iter().chain(optional) {
        let Some(field) = value.get(key) else {
            continue;
        };
        if *key == "sequence" {
            if !field.as_i64().is_some_and(|sequence| sequence > 0) {
                return None;
            }
        } else if !(*key == "child_task_id" && field.is_null())
            && !field.as_str().is_some_and(identity)
        {
            return None;
        }
    }
    if !keys(&value["context"]["carrier"], &["traceparent", "tracestate"]) {
        return None;
    }
    let (context, diagnostic) = normalize(&value["context"], &[]);
    let mut result = value.clone();
    result["context"] = context?;
    Some((result, diagnostic))
}

#[cfg(test)]
mod tests {
    use super::*;
    const PARENT: &str = "00-12345678901234567890123456789012-1234567890123456-01";

    fn envelope() -> Value {
        json!({"schema": SCHEMA, "version": 1, "carrier": {"traceparent": PARENT}})
    }

    #[test]
    fn missing_original_generation_cannot_become_trace_authority_through_serde_defaults() {
        let request = json!({"worker_id": "worker", "task_queue": "orders"});
        let original = json!({"task": {"task_id": "task", "workflow_id": "workflow", "run_id": "run",
            "lease_owner": "worker", "workflow_task_attempt": 1, "observability_context": envelope(),
            "arguments": {"observability_context": {"business": "preserved"}}}});
        let mut valid = original.clone();
        retain_poll_observations(
            &mut valid,
            &request,
            "/worker/workflow-tasks/poll",
            "tenant",
        );
        assert_eq!(valid, original);
        for field in [
            "task_id",
            "workflow_id",
            "run_id",
            "lease_owner",
            "workflow_task_attempt",
        ] {
            let mut invalid = original.clone();
            invalid["task"].as_object_mut().unwrap().remove(field);
            retain_poll_observations(
                &mut invalid,
                &request,
                "/worker/workflow-tasks/poll",
                "tenant",
            );
            assert!(invalid["task"].get("observability_context").is_none());
            assert_eq!(invalid["task"]["arguments"], original["task"]["arguments"]);
        }
        let mut expired = original.clone();
        expired["task"]["lease_expires_at"] = json!("2000-01-01T00:00:00Z");
        retain_poll_observations(
            &mut expired,
            &request,
            "/worker/workflow-tasks/poll",
            "tenant",
        );
        assert!(expired["task"].get("observability_context").is_none());
    }

    #[test]
    fn secondary_links_require_real_event_provenance_and_never_fabricate_child_producers() {
        let value = json!({"schema": "durable-workflow.trace-links", "version": 1, "task_id": "task",
        "run_id": "run", "namespace": "tenant", "links": [
            {"kind": "command", "command_id": "command", "context": envelope()},
            {"kind": "task_outcome", "task_id": "producer", "event_id": "event", "sequence": 1,
                "event_type": "ActivityCompleted", "activity_execution_id": "activity", "context": envelope()},
            {"kind": "child_outcome", "workflow_link_id": "link", "child_run_id": "child", "child_task_id": null,
                "event_id": "terminal", "sequence": i64::MAX, "event_type": "WorkflowCancelled", "context": envelope()}
        ]});
        assert_eq!(
            links(&value, "task", "run", "tenant"),
            (value["links"].as_array().unwrap().clone(), None)
        );
        assert_eq!(
            links(&value, "wrong", "run", "tenant"),
            (Vec::new(), Some(Diagnostic::Discarded))
        );
        for sequence in [
            json!(true),
            json!(0),
            json!(-1),
            json!(1.0),
            json!("1"),
            json!(u64::MAX),
        ] {
            let mut invalid = value.clone();
            invalid["links"][1]["sequence"] = sequence;
            assert_eq!(
                links(&invalid, "task", "run", "tenant"),
                (Vec::new(), Some(Diagnostic::Discarded))
            );
        }
        for key in ["event_id", "sequence", "event_type"] {
            let mut invalid = value.clone();
            invalid["links"][2].as_object_mut().unwrap().remove(key);
            assert_eq!(
                links(&invalid, "task", "run", "tenant"),
                (Vec::new(), Some(Diagnostic::Discarded))
            );
        }
        let mut invalid = value.clone();
        invalid["links"][1]["activity_id"] = json!("ambiguous");
        assert_eq!(
            links(&invalid, "task", "run", "tenant"),
            (Vec::new(), Some(Diagnostic::Discarded))
        );
        invalid = value.clone();
        invalid["links"][0]["context"]["carrier"]["baggage"] = json!("tenant=private");
        assert_eq!(
            links(&invalid, "task", "run", "tenant"),
            (Vec::new(), Some(Diagnostic::Discarded))
        );
        invalid["links"] = json!(vec![value["links"][0].clone(); 33]);
        assert_eq!(
            links(&invalid, "task", "run", "tenant"),
            (Vec::new(), Some(Diagnostic::Discarded))
        );
    }

    #[test]
    fn exact_capability_rejects_missing_unknown_and_coerced_values() {
        let info = json!({"worker_protocol": {"server_capabilities": {"observability_context": {
            "supported": true, "schema": SCHEMA, "version": 1, "dispatch_schema": "durable-workflow.queue-trace",
            "links_schema": "durable-workflow.trace-links", "max_context_bytes": 4096, "max_links": 32, "max_links_bytes": 4096,
        }}}});
        assert!(supported(&info));
        for key in info["worker_protocol"]["server_capabilities"]["observability_context"]
            .as_object()
            .unwrap()
            .keys()
        {
            for wrong in [
                Value::Null,
                json!(false),
                json!("1"),
                json!(1.0),
                json!([]),
                json!("unknown"),
            ] {
                let mut candidate = info.clone();
                candidate["worker_protocol"]["server_capabilities"]["observability_context"][key] =
                    wrong;
                assert!(!supported(&candidate));
            }
        }
        assert!(!supported(&json!({"version": "999.0.0"})));
    }

    #[test]
    fn official_validation_refuses_invalid_context_without_ambient_fallback() {
        assert_eq!(normalize(&envelope(), &[]), (Some(envelope()), None));
        for parent in [
            "00-00000000000000000000000000000000-1234567890123456-01",
            "00-12345678901234567890123456789012-0000000000000000-01",
            "ff-12345678901234567890123456789012-1234567890123456-01",
            "00-ABCDEF78901234567890123456789012-1234567890123456-01",
            "private-header",
        ] {
            let mut value = envelope();
            value["carrier"]["traceparent"] = json!(parent);
            assert_eq!(normalize(&value, &[]), (None, Some(Diagnostic::Discarded)));
        }
        assert_eq!(normalize(&Value::Null, &[]), (None, None));
        for value in [json!(true), json!([]), json!({})] {
            assert_eq!(normalize(&value, &[]), (None, Some(Diagnostic::Discarded)));
        }
    }

    #[test]
    fn baggage_is_empty_by_default_and_allowlist_is_bounded() {
        let mut value = envelope();
        value["carrier"]["baggage"] = json!("tenant=public,credential=private");
        let original = value.clone();
        assert_eq!(
            normalize(&value, &[]),
            (Some(envelope()), Some(Diagnostic::Filtered))
        );
        let mut expected = envelope();
        expected["carrier"]["baggage"] = json!("tenant=public");
        assert_eq!(
            normalize(&value, &["tenant".into()]),
            (Some(expected), Some(Diagnostic::Filtered))
        );
        assert_eq!(value, original);
        let keys: Vec<_> = (0..33).map(|number| format!("k{number}")).collect();
        value["carrier"]["baggage"] = json!(keys
            .iter()
            .map(|key| format!("{key}=v"))
            .collect::<Vec<_>>()
            .join(","));
        assert_eq!(
            normalize(&value, &keys),
            (None, Some(Diagnostic::Discarded))
        );
        value["carrier"]["baggage"] = json!(keys[..32]
            .iter()
            .map(|key| format!("{key}=v"))
            .collect::<Vec<_>>()
            .join(","));
        assert!(normalize(&value, &keys).0.is_some());
    }

    #[test]
    fn byte_bounds_unknown_fields_and_wrong_dispatch_identity_are_discarded() {
        for (key, size) in [("traceparent", 257), ("tracestate", 513), ("baggage", 2049)] {
            let mut value = envelope();
            value["carrier"][key] = json!("x".repeat(size));
            assert_eq!(normalize(&value, &[]), (None, Some(Diagnostic::Discarded)));
        }
        let mut value = json!({"schema": "durable-workflow.queue-trace", "version": 1, "task_id": "task", "context": envelope()});
        assert_eq!(dispatch(&value, "task"), (Some(envelope()), None));
        assert_eq!(
            dispatch(&value, "other"),
            (None, Some(Diagnostic::Discarded))
        );
        value["context"]["carrier"]["baggage"] = json!("tenant=public");
        assert_eq!(
            dispatch(&value, "task"),
            (None, Some(Diagnostic::Discarded))
        );
        value["context"]["unknown"] = json!("private");
        assert_eq!(
            normalize(&value["context"], &[]),
            (None, Some(Diagnostic::Discarded))
        );
        for invalid in ["", "task\n", "task\u{0085}"] {
            assert!(!identity(invalid));
        }
        assert!(!identity(&"x".repeat(513)));
        assert!(identity(&"x".repeat(512)));
        assert!(identity("任务"));
    }
}
