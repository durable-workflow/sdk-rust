//! Bound diagnostic data before it reaches the ordinary JSON request limit.
use crate::{json, Result, Value};
use serde::Serialize;
use serde_json::Map;
use std::io::{self, Write};
use thiserror::Error as ThisError;

// Published runtimes persist a TEXT timeline summary containing a prefix,
// activity label and the message. Reserve room below its 65,535-byte boundary.
pub const MESSAGE_BYTES: usize = 62 * 1024;
pub const STACK_BYTES: usize = 128 * 1024;
pub const DIAGNOSTIC_BYTES: usize = 64 * 1024;
pub const REQUEST_BYTES: usize = 1024 * 1024;
const MARKER_RESERVE: usize = 8 * 1024;
const MARKER_KEY: &str = "sdk_failure_truncation";
const MESSAGE_MARKER: &str = "\n[truncated; see diagnostics.sdk_failure_truncation]";

/// Failure reporting refused locally; this is not an application failure decision.
#[derive(Debug, ThisError)]
#[non_exhaustive]
pub enum FailureReportError {
    #[error("failure report refused: {encoded_bytes} encoded bytes cannot fit {limit} bytes without changing required identity or failure semantics")]
    RequiredFieldsTooLarge { encoded_bytes: usize, limit: usize },
    #[error("failure report refused: {0}")]
    InvalidReport(&'static str),
}

#[derive(Default)]
struct Count(usize);
impl Write for Count {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0 = self
            .0
            .checked_add(bytes.len())
            .ok_or_else(|| io::Error::other("JSON size overflow"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub(super) fn encoded_size<T: Serialize + ?Sized>(value: &T) -> Result<usize> {
    let mut count = Count::default();
    serde_json::to_writer(&mut count, value)?;
    Ok(count.0)
}

fn prefix(value: &str, maximum: usize) -> &str {
    let mut end = maximum.min(value.len());
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    &value[..end]
}

fn clipped(value: &str, limit: usize, marker: &str, force: bool) -> String {
    if !force && value.len() <= limit {
        return value.to_owned();
    }
    let available = limit.saturating_sub(marker.len());
    format!("{}{marker}", prefix(value, available))
}

fn metadata_size(value: &Map<String, Value>) -> Result<usize> {
    let metadata: Map<_, _> = value
        .iter()
        .filter(|(key, _)| !core(key))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    encoded_size(&metadata)
}

/// Local command grammar has no diagnostic object. Keep original size in its
/// existing message field, before the workflow can observe the outcome.
#[derive(Debug)]
pub(super) struct LocalText {
    prefix: String,
    original_bytes: usize,
}
impl LocalText {
    pub(super) fn new(message: &str) -> Self {
        Self {
            prefix: prefix(message, MESSAGE_BYTES).to_owned(),
            original_bytes: message.len(),
        }
    }
    pub(super) fn render(&self, limit: usize) -> String {
        let marker = format!(
            "\n[truncated: original message {} UTF-8 bytes]",
            self.original_bytes
        );
        clipped(&self.prefix, limit, &marker, self.original_bytes > limit)
    }
}

/// Match published failure/local-command normalization's PHP trim character set.
/// Apply to the observed outcome, without rewriting the outgoing original.
pub(super) fn committed_message(message: &str) -> &str {
    message.trim_matches(|ch| matches!(ch, '\0' | '\t' | '\n' | '\r' | '\u{000b}' | ' '))
}

/// Fit only the current, not-yet-observed local command. Previously returned
/// local outcomes and unrelated business payloads are fixed inputs.
pub(super) fn fit_local(
    body: &mut Value,
    index: usize,
    messages: &[(usize, LocalText)],
    limit: usize,
) -> Result<()> {
    if messages.is_empty() {
        return Ok(());
    }
    let limit = limit.min(REQUEST_BYTES);
    let mut target = MESSAGE_BYTES;
    loop {
        let command = &mut body["commands"][index];
        let mut diagnostic_bytes = 2_usize;
        for (attempt, original) in messages {
            let message = original.render(target);
            if committed_message(&message).is_empty() {
                return Err(FailureReportError::InvalidReport(
                    "local failure message is empty after Server normalization",
                )
                .into());
            }
            diagnostic_bytes = diagnostic_bytes
                .saturating_add(encoded_size(&message)?)
                .saturating_add(1);
            command["attempts"][*attempt]["message"] = json!(message);
        }
        if command.get("message").is_some() {
            if let Some((attempt, _)) = messages.last() {
                command["message"] = command["attempts"][*attempt]["message"].clone();
            }
        }
        let size = encoded_size(body)?;
        if size <= limit && diagnostic_bytes <= DIAGNOSTIC_BYTES {
            return Ok(());
        }
        if target <= 128 {
            return Err(FailureReportError::RequiredFieldsTooLarge {
                encoded_bytes: size,
                limit,
            }
            .into());
        }
        target = (target / 2).max(128);
    }
}

fn core(key: &str) -> bool {
    matches!(
        key,
        "message"
            | "stack_trace"
            | "type"
            | "class"
            | "non_retryable"
            | "retryable"
            | "kind"
            | "reason"
            | "sequence"
            | "timeout_type"
            | "cancelled"
            | "malformed_output"
    )
}

#[derive(Clone, Copy)]
struct Budget {
    message: usize,
    stack: usize,
    metadata: usize,
    names: usize,
}
impl Default for Budget {
    fn default() -> Self {
        Self {
            message: MESSAGE_BYTES,
            stack: STACK_BYTES,
            metadata: DIAGNOSTIC_BYTES,
            names: 32,
        }
    }
}
impl Budget {
    fn shrink(&mut self) -> bool {
        if self.stack > 0 {
            self.stack /= 2;
            return true;
        }
        if self.message > 64 {
            self.message = (self.message / 2).max(64);
            return true;
        }
        if self.metadata > MARKER_RESERVE {
            self.metadata = (self.metadata / 2).max(MARKER_RESERVE);
            return true;
        }
        if self.names > 0 {
            self.names /= 2;
            return true;
        }
        false
    }
}

struct Omitted {
    count: usize,
    names: Vec<String>,
    maximum: usize,
}
impl Omitted {
    fn new(maximum: usize) -> Self {
        Self {
            count: 0,
            names: Vec::new(),
            maximum,
        }
    }
    fn add(&mut self, name: &str) -> Result<()> {
        self.count = self.count.saturating_add(1);
        if self.names.len() < self.maximum {
            self.names.push(prefix(name, 128).to_owned());
            if encoded_size(&self.names)? > 4096 {
                self.names.pop();
            }
        }
        Ok(())
    }
}

struct Failure {
    pointer: String,
    original: Map<String, Value>,
    metadata_bytes: usize,
    field_sizes: Vec<usize>,
    inline_diagnostics: bool,
}
impl Failure {
    fn take(
        body: &mut Value,
        pointer: String,
        limit: usize,
        inline_diagnostics: bool,
    ) -> Result<Self> {
        let value = body
            .pointer_mut(&pointer)
            .ok_or(FailureReportError::InvalidReport("missing failure object"))?
            .take();
        let Value::Object(original) = value else {
            return Err(FailureReportError::InvalidReport("failure must be an object").into());
        };
        if !original.get("message").is_some_and(Value::is_string) {
            return Err(
                FailureReportError::InvalidReport("failure.message must be a string").into(),
            );
        }
        if original
            .get("stack_trace")
            .is_some_and(|v| !v.is_null() && !v.is_string())
        {
            return Err(FailureReportError::InvalidReport(
                "failure.stack_trace must be a string or null",
            )
            .into());
        }
        let mut metadata_bytes = 2_usize;
        let mut metadata_fields = 0_usize;
        let mut field_sizes = Vec::new();
        let mut required_bytes = 2_usize;
        for (key, value) in &original {
            let size = encoded_size(key)?
                .saturating_add(encoded_size(value)?)
                .saturating_add(1);
            field_sizes.push(size);
            if core(key)
                && !matches!(key.as_str(), "message" | "stack_trace")
                && encoded_size(value)? > limit
            {
                return Err(FailureReportError::RequiredFieldsTooLarge {
                    encoded_bytes: encoded_size(value)?,
                    limit,
                }
                .into());
            }
            if core(key) && !matches!(key.as_str(), "message" | "stack_trace") {
                required_bytes = required_bytes.saturating_add(size).saturating_add(1);
            }
            if !core(key) {
                metadata_bytes = metadata_bytes
                    .saturating_add(size)
                    .saturating_add(usize::from(metadata_fields > 0));
                metadata_fields += 1;
            }
        }
        if required_bytes > limit {
            return Err(FailureReportError::RequiredFieldsTooLarge {
                encoded_bytes: required_bytes,
                limit,
            }
            .into());
        }
        Ok(Self {
            pointer,
            original,
            metadata_bytes,
            field_sizes,
            inline_diagnostics,
        })
    }

    fn render(&self, budget: Budget) -> Result<Value> {
        let message = self.original["message"]
            .as_str()
            .expect("validated failure message");
        let stack = self.original.get("stack_trace").and_then(Value::as_str);
        let mut truncated = message.len() > budget.message
            || stack.is_some_and(|s| budget.stack == 0 || s.len() > budget.stack)
            || self.metadata_bytes > budget.metadata;
        // Preserve byte ordering too when serde_json's preserve_order feature is
        // enabled by a downstream application.
        if !truncated {
            return Ok(Value::Object(self.original.clone()));
        }
        let mut omitted = Omitted::new(budget.names);
        let mut output = Map::new();
        let mut used = 2_usize;
        // Reserve space for diagnostics only when a limit already requires a marker.
        let metadata_limit =
            budget
                .metadata
                .saturating_sub(if truncated { MARKER_RESERVE } else { 0 });
        for (index, (key, value)) in self.original.iter().enumerate() {
            if matches!(key.as_str(), "message" | "stack_trace") {
                continue;
            }
            if core(key) {
                output.insert(key.clone(), value.clone());
                continue;
            }
            let size = self.field_sizes[index].saturating_add(1);
            if used.saturating_add(size) <= metadata_limit {
                used += size;
                output.insert(key.clone(), value.clone());
            } else {
                truncated = true;
                omitted.add(key)?;
            }
        }
        if let Some(stack) = stack {
            let marker = format!(
                "\n[truncated: original stack_trace {} UTF-8 bytes]",
                stack.len()
            );
            if budget.stack == 0 || (stack.len() > budget.stack && budget.stack < marker.len()) {
                omitted.add("stack_trace")?;
            } else {
                output.insert(
                    "stack_trace".into(),
                    Value::String(clipped(stack, budget.stack, &marker, false)),
                );
            }
        } else if let Some(value) = self.original.get("stack_trace") {
            output.insert("stack_trace".into(), value.clone());
        }
        let mut message_marker = MESSAGE_MARKER.to_owned();
        if truncated && self.inline_diagnostics {
            // Workflow/query-task failure schemas have no diagnostic field.
            // Use their existing message, with exact sizing after JSON escaping.
            message_marker = format!(
                "\n[truncated: {}]",
                serde_json::to_string(&json!({
                    "original_message_bytes":message.len(),"original_stack_trace_bytes":stack.map_or(0,str::len),
                    "original_metadata_json_bytes":self.metadata_bytes,"omitted_fields":omitted.names,"omitted_field_count":omitted.count,
                }))?
            );
        } else if truncated {
            let diagnostic = output.remove("diagnostics");
            let mut diagnostics = match diagnostic {
                Some(Value::Object(map)) => map,
                Some(Value::Null) | None => Map::new(),
                Some(other) => Map::from_iter([("original".into(), other)]),
            };
            if diagnostics.remove(MARKER_KEY).is_some() {
                omitted.add("diagnostics.sdk_failure_truncation")?;
            }
            diagnostics.insert(
                MARKER_KEY.into(),
                json!({
                    "original_message_bytes": message.len(),
                    "original_stack_trace_bytes": stack.map_or(0,str::len),
                    "original_metadata_json_bytes": self.metadata_bytes,
                    "omitted_fields": omitted.names,
                    "omitted_field_count": omitted.count,
                }),
            );
            output.insert("diagnostics".into(), Value::Object(diagnostics));
        }
        output.insert(
            "message".into(),
            Value::String(clipped(message, budget.message, &message_marker, truncated)),
        );
        // Count the actual serialized metadata after inserting markers, including
        // escaped field names and any wrapper around existing diagnostics.
        if metadata_size(&output)? > budget.metadata {
            return Err(FailureReportError::InvalidReport(
                "truncation diagnostics exceed their serialized budget",
            )
            .into());
        }
        Ok(Value::Object(output))
    }

    fn refresh_details(&mut self, body: &Value) -> Result<()> {
        if let Some(details) = body
            .pointer(&self.pointer)
            .and_then(|failure| failure.get("details"))
        {
            if self.original.contains_key("details") {
                self.original.insert("details".into(), details.clone());
                for (index, (key, value)) in self.original.iter().enumerate() {
                    if key == "details" {
                        self.field_sizes[index] = encoded_size(key)? + 1 + encoded_size(value)?;
                    }
                }
            }
        }
        Ok(())
    }
}

/// Captures originals once so later transport-budget reductions retain true sizes.
pub(super) struct Prepared {
    failures: Vec<Failure>,
    texts: Vec<(String, LocalText)>,
    mirrors: Vec<(String, String)>,
}
impl Prepared {
    pub(super) fn new(body: &mut Value, path: &str, limit: usize) -> Result<Option<Self>> {
        let parts: Vec<_> = path.trim_matches('/').split('/').collect();
        let mut pointers = Vec::new();
        let mut texts = Vec::new();
        let mut mirrors = Vec::new();
        let mut local_reports = false;
        match parts.as_slice() {
            ["worker", "activity-tasks" | "workflow-tasks" | "query-tasks", _, "fail"] => {
                pointers.push("/failure".into())
            }
            ["worker", "workflow-tasks", _, "complete"] => {
                for (index, command) in body["commands"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .enumerate()
                {
                    if command["type"] == "fail_workflow" || command["type"] == "fail_update" {
                        if command["exception"].is_object() {
                            pointers.push(format!("/commands/{index}/exception"));
                            if command.get("message").is_some() {
                                mirrors.push((
                                    format!("/commands/{index}/message"),
                                    format!("/commands/{index}/exception/message"),
                                ));
                            }
                        } else if let Some(message) = command["message"].as_str() {
                            texts.push((
                                format!("/commands/{index}/message"),
                                LocalText::new(message),
                            ));
                        }
                    }
                    if command["type"] == "record_local_activity" {
                        local_reports |= command.get("message").is_some()
                            || command["attempts"]
                                .as_array()
                                .into_iter()
                                .flatten()
                                .any(|v| v.get("message").is_some());
                    }
                }
            }
            _ => return Ok(None),
        }
        if pointers.is_empty() && texts.is_empty() && !local_reports {
            return Ok(None);
        }
        if path.len() > limit {
            return Err(FailureReportError::RequiredFieldsTooLarge {
                encoded_bytes: path.len(),
                limit,
            }
            .into());
        }
        let mut failures = Vec::new();
        let inline_diagnostics = matches!(
            parts.as_slice(),
            ["worker", "workflow-tasks" | "query-tasks", _, "fail"]
        );
        for pointer in pointers {
            failures.push(Failure::take(body, pointer, limit, inline_diagnostics)?);
        }
        let prepared = Self {
            failures,
            texts,
            mirrors,
        };
        prepared.render(body, Budget::default())?;
        Ok(Some(prepared))
    }

    fn render(&self, body: &mut Value, budget: Budget) -> Result<()> {
        for failure in &self.failures {
            *body
                .pointer_mut(&failure.pointer)
                .expect("retained failure location") = failure.render(budget)?;
        }
        for (pointer, original) in &self.texts {
            *body
                .pointer_mut(pointer)
                .expect("retained message location") = json!(original.render(budget.message));
        }
        for (target, source) in &self.mirrors {
            let message = body
                .pointer(source)
                .expect("retained exception message")
                .clone();
            *body.pointer_mut(target).expect("retained command message") = message;
        }
        Ok(())
    }

    pub(super) fn encode(&mut self, body: &mut Value, limit: usize) -> Result<Vec<u8>> {
        let limit = limit.min(REQUEST_BYTES);
        // Keep any valid reference produced by ordinary payload externalization.
        // Re-rendering must never restore its pre-upload Avro envelope.
        for failure in &mut self.failures {
            failure.refresh_details(body)?;
        }
        let mut budget = Budget::default();
        if !self
            .failures
            .iter()
            .any(|f| f.original.get("stack_trace").is_some_and(Value::is_string))
        {
            budget.stack = 0;
        }
        loop {
            self.render(body, budget)?;
            let size = encoded_size(body)?;
            if size <= limit {
                let bytes = serde_json::to_vec(body)?;
                if bytes.len() <= limit {
                    return Ok(bytes);
                }
                return Err(FailureReportError::RequiredFieldsTooLarge {
                    encoded_bytes: bytes.len(),
                    limit,
                }
                .into());
            }
            // Local history has already been observed by workflow code. Never rewrite
            // it here; the local producer must fit it before returning its outcome.
            if (self.failures.is_empty() && self.texts.is_empty()) || !budget.shrink() {
                return Err(FailureReportError::RequiredFieldsTooLarge {
                    encoded_bytes: size,
                    limit,
                }
                .into());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn encode(mut body: Value, limit: usize) -> (Value, Vec<u8>) {
        let mut prepared = Prepared::new(&mut body, "/worker/activity-tasks/task/fail", limit)
            .unwrap()
            .unwrap();
        let bytes = prepared.encode(&mut body, limit).unwrap();
        assert_eq!(serde_json::from_slice::<Value>(&bytes).unwrap(), body);
        (body, bytes)
    }
    #[test]
    fn small_failure_preserves_type_flags_and_details() {
        let input = json!({"activity_attempt_id":"attempt", "lease_owner":"owner", "failure":{"type":"Declined","message":"no","non_retryable":true,"details":null}});
        let (output, bytes) = encode(input.clone(), REQUEST_BYTES);
        assert_eq!(output, input);
        assert_eq!(bytes, serde_json::to_vec(&input).unwrap());
    }
    #[test]
    fn unicode_escaping_and_markers_are_included_in_actual_request_cap() {
        let input = json!({"activity_attempt_id":"attempt", "lease_owner":"owner", "failure":{
            "type":"Original", "non_retryable":false,"retryable":true,
            "message":"\u{0000}界🦀".repeat(600_000),"stack_trace":"\u{0001}".repeat(3*1024*1024),
            "diagnostics":{"causes":{"nested":"x".repeat(3*1024*1024)}}, "arbitrary":"y".repeat(3*1024*1024)}});
        let (output, bytes) = encode(input, REQUEST_BYTES);
        assert!(bytes.len() <= REQUEST_BYTES);
        assert_eq!(output["failure"]["type"], "Original");
        assert_eq!(output["failure"]["non_retryable"], false);
        assert_eq!(output["failure"]["retryable"], true);
        assert!(output["failure"]["message"]
            .as_str()
            .unwrap()
            .contains("[truncated"));
        assert!(output["failure"]["message"].as_str().unwrap().len() <= MESSAGE_BYTES);
        assert!(
            output["failure"]["diagnostics"][MARKER_KEY]["omitted_field_count"]
                .as_u64()
                .unwrap()
                > 0
        );
    }
    #[test]
    fn lower_known_limit_and_exact_identity_are_preserved() {
        let (output, bytes) = encode(
            json!({"activity_attempt_id":"exact-attempt", "lease_owner":"exact-owner", "failure":{"type":"Original","message":"\"\\\n".repeat(1024*1024)}}),
            4096,
        );
        assert!(bytes.len() <= 4096);
        assert_eq!(output["activity_attempt_id"], "exact-attempt");
        assert_eq!(output["lease_owner"], "exact-owner");
        let mut impossible =
            json!({"lease_owner":"x".repeat(4096),"failure":{"type":"T","message":"short"}});
        let mut p = Prepared::new(&mut impossible, "/worker/activity-tasks/task/fail", 4096)
            .unwrap()
            .unwrap();
        assert!(matches!(
            p.encode(&mut impossible, 4096),
            Err(crate::Error::FailureReporting(
                FailureReportError::RequiredFieldsTooLarge { .. }
            ))
        ));
    }

    #[test]
    fn combined_metadata_budget_includes_inserted_escaped_diagnostics() {
        let details = json!({"codec":"avro","blob":"a".repeat(20_000)});
        let (output, bytes) = encode(
            json!({"failure":{
                "message":"界".repeat(MESSAGE_BYTES),"type":"SameType","non_retryable":true,
                "details":details,"diagnostics":{"escaped":"\u{0000}".repeat(8000)},
                "runtime_diagnostics":{"causes":[{"nested":"x".repeat(8000)}]},
            }}),
            REQUEST_BYTES,
        );
        let failure = output["failure"].as_object().unwrap();
        assert!(metadata_size(failure).unwrap() <= DIAGNOSTIC_BYTES);
        assert!(bytes.len() <= REQUEST_BYTES);
        assert_eq!(failure["type"], "SameType");
        assert_eq!(failure["non_retryable"], true);
        if let Some(kept) = failure.get("details") {
            assert_eq!(kept, &details);
        }
        assert!(
            failure["diagnostics"][MARKER_KEY]["omitted_field_count"]
                .as_u64()
                .unwrap()
                > 0
        );
    }

    #[test]
    fn arbitrary_large_metadata_keys_and_values_cannot_bypass_the_cap() {
        let mut failure = Map::from_iter([
            ("message".into(), json!("small")),
            ("type".into(), json!("Original")),
        ]);
        for index in 0..2000 {
            failure.insert(
                format!("{index}{}", "\u{0000}".repeat(1000)),
                json!({"nested":"x".repeat(2000)}),
            );
        }
        let (output, bytes) = encode(json!({"failure":failure}), REQUEST_BYTES);
        assert!(bytes.len() <= REQUEST_BYTES);
        assert!(metadata_size(output["failure"].as_object().unwrap()).unwrap() <= DIAGNOSTIC_BYTES);
        let omitted = output["failure"]["diagnostics"][MARKER_KEY]["omitted_field_count"]
            .as_u64()
            .unwrap() as usize;
        let retained = output["failure"]
            .as_object()
            .unwrap()
            .keys()
            .filter(|key| !core(key) && key.as_str() != "diagnostics")
            .count();
        assert_eq!(omitted + retained, 2000);
        assert!(output["failure"]["message"]
            .as_str()
            .unwrap()
            .contains("[truncated"));
    }

    #[test]
    fn payload_reference_survives_final_render_without_restoring_envelope() {
        let mut body = json!({"failure":{"message":"x".repeat(3*1024*1024),"type":"T","details":{"codec":"avro","blob":"valid-inline-before-upload"}}});
        let mut prepared =
            Prepared::new(&mut body, "/worker/activity-tasks/task/fail", REQUEST_BYTES)
                .unwrap()
                .unwrap();
        let reference = json!({"codec":"avro","external_payload":{"schema":crate::runtime_payloads::SCHEMA,"codec":"avro","reference_id":"ep_01ARZ3NDEKTSV4RRFFQ69G5FAV","sha256":"a".repeat(64),"size_bytes":10}});
        crate::runtime_payloads::Reference::parse(&reference)
            .unwrap()
            .unwrap();
        body["failure"]["details"] = reference.clone();
        let bytes = prepared.encode(&mut body, 4096).unwrap();
        assert_eq!(body["failure"]["details"], reference);
        assert_eq!(
            body["failure"]["diagnostics"][MARKER_KEY]["original_message_bytes"],
            3 * 1024 * 1024
        );
        assert!(bytes.len() <= 4096);
    }

    #[test]
    fn terminal_workflow_duplicate_and_plain_messages_fit_actual_body() {
        for structured in [false, true] {
            let mut command = json!({"type":"fail_workflow","message":"\u{0000}".repeat(3*1024*1024),
                "exception_type":"Original","non_retryable":true,"failed_activity_execution_id":"exact-logical-id","failed_step_sequence":7});
            if structured {
                command["exception"] = json!({"type":"Original","message":command["message"],"non_retryable":true,"details":null});
            }
            let mut body =
                json!({"lease_owner":"exact-owner","workflow_task_attempt":9,"commands":[command]});
            let mut prepared =
                Prepared::new(&mut body, "/worker/workflow-tasks/task/complete", 4096)
                    .unwrap()
                    .unwrap();
            let bytes = prepared.encode(&mut body, 4096).unwrap();
            assert!(bytes.len() <= 4096);
            assert_eq!(body["lease_owner"], "exact-owner");
            assert_eq!(body["workflow_task_attempt"], 9);
            assert_eq!(
                body["commands"][0]["failed_activity_execution_id"],
                "exact-logical-id"
            );
            assert_eq!(body["commands"][0]["non_retryable"], true);
            if structured {
                assert_eq!(
                    body["commands"][0]["message"],
                    body["commands"][0]["exception"]["message"]
                );
            }
        }
    }

    #[test]
    fn local_attempt_messages_fit_before_observation_without_mutating_prior_commands() {
        let original = "\u{0000}界🦀".repeat(400_000);
        let mut attempts = Vec::new();
        let mut messages = Vec::new();
        for index in 0..100 {
            attempts.push(json!({"attempt_number":index+1,"outcome":"failed","message":"temporary","exception_type":"Original","non_retryable":false,"backoff_seconds":17}));
            messages.push((index, LocalText::new(&original)));
        }
        let prior = json!({"type":"record_local_activity","message":"already observed","attempts":[{"message":"already observed"}]});
        let mut body = json!({"lease_owner":"exact","commands":[prior.clone(),{"type":"record_local_activity","message":"terminal","attempts":attempts}]});
        fit_local(&mut body, 1, &messages, REQUEST_BYTES).unwrap();
        assert_eq!(body["commands"][0], prior);
        assert_eq!(
            body["commands"][1]["message"],
            body["commands"][1]["attempts"][99]["message"]
        );
        assert!(body["commands"][1]["message"]
            .as_str()
            .unwrap()
            .contains(&format!("original message {}", original.len())));
        let sum: usize = body["commands"][1]["attempts"]
            .as_array()
            .unwrap()
            .iter()
            .map(|a| encoded_size(&a["message"]).unwrap() + 1)
            .sum();
        assert!(sum + 2 <= DIAGNOSTIC_BYTES);
        assert!(encoded_size(&body).unwrap() <= REQUEST_BYTES);
        assert_eq!(body["commands"][1]["attempts"][0]["backoff_seconds"], 17);
        let before = body.clone();
        let mut prepared = Prepared::new(
            &mut body,
            "/worker/workflow-tasks/task/complete",
            REQUEST_BYTES,
        )
        .unwrap()
        .unwrap();
        prepared.encode(&mut body, REQUEST_BYTES).unwrap();
        assert_eq!(
            body, before,
            "transport cannot rewrite observed local messages"
        );
        body["lease_owner"] = json!("x".repeat(4096));
        assert!(matches!(
            fit_local(&mut body, 1, &messages, 4096),
            Err(crate::Error::FailureReporting(_))
        ));
        assert_eq!(body["commands"][0], prior);
    }

    #[test]
    fn local_observation_matches_php_trim_without_changing_small_wire_message() {
        let message = "\0\t \r\n\u{000b} original \0\t\r\n";
        assert_eq!(committed_message(message), "original");
        assert_eq!(
            committed_message("\u{00a0}original\u{00a0}"),
            "\u{00a0}original\u{00a0}"
        );
        let mut body = json!({"commands":[{"type":"record_local_activity","message":message,"attempts":[{"message":message}]}]});
        let before = body.clone();
        fit_local(&mut body, 0, &[(0, LocalText::new(message))], REQUEST_BYTES).unwrap();
        assert_eq!(body, before);
        assert!(fit_local(&mut body, 0, &[(0, LocalText::new("\0 \t"))], REQUEST_BYTES).is_err());
    }
}
