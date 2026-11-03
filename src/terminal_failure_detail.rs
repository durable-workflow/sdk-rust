//! Typed client errors for terminal runs whose description lacks failure detail.
//!
//! This is client-side error enrichment on `WorkflowHandle::result*`. It reads
//! a finished run's durable history to fill the failure detail; it does not
//! take part in worker replay (#156).

use serde_json::Value;

use crate::{percent_encode_path_segment, Client, RequestProtocol, WorkflowDescription};

/// History pages read to find a terminal event when a description lacks detail.
const TERMINAL_HISTORY_PAGE_LIMIT: usize = 20;

impl Client {
    /// Fill a terminal failed or timed-out description that carries no
    /// failure detail from the run's terminal history event.
    ///
    /// Some servers report `closed_reason` without the `failure` object or
    /// timeout `reason` that typed client errors are built from, while durable
    /// history still records them. This only runs on that error path; a
    /// history read that fails, exceeds its page limit or finds no terminal
    /// event leaves the description unchanged.
    pub(crate) async fn with_terminal_failure_detail(
        &self,
        mut description: WorkflowDescription,
        workflow_id: &str,
        selected_run_id: Option<&str>,
    ) -> WorkflowDescription {
        let terminal = description
            .closed_reason
            .as_deref()
            .or(description.status.as_deref())
            .unwrap_or_default()
            .to_ascii_lowercase();
        let event_type = match terminal.as_str() {
            "failed" => "WorkflowFailed",
            "timed_out" | "timedout" => "WorkflowTimedOut",
            _ => return description,
        };
        let has_detail = description.failure.as_ref().is_some_and(Value::is_object)
            || description.exception.is_some()
            || !description.failures.is_empty()
            || description.error.is_some()
            || (event_type == "WorkflowTimedOut"
                && description
                    .raw
                    .get("reason")
                    .and_then(Value::as_str)
                    .is_some());
        if has_detail {
            return description;
        }
        let Some(run_id) = description
            .run_id
            .clone()
            .or_else(|| selected_run_id.map(str::to_string))
        else {
            return description;
        };
        let workflow_id = description
            .workflow_id
            .clone()
            .unwrap_or_else(|| workflow_id.to_string());
        let Some(payload) = self
            .terminal_history_payload(&workflow_id, &run_id, event_type)
            .await
        else {
            return description;
        };
        let mut failure = serde_json::Map::new();
        for key in [
            "message",
            "exception_type",
            "exception_class",
            "failure_category",
            "non_retryable",
            "failure_id",
            "exception",
        ] {
            if let Some(value) = payload.get(key).filter(|value| !value.is_null()) {
                failure.insert(key.to_string(), value.clone());
            }
        }
        if let Some(kind) = payload.get("timeout_kind").and_then(Value::as_str) {
            failure.insert("reason".to_string(), Value::String(kind.to_string()));
        }
        failure.insert(
            "detail_source".to_string(),
            Value::String("history".to_string()),
        );
        description.failure = Some(Value::Object(failure));
        description
    }

    /// The payload of the last `event_type` event in one run's history.
    async fn terminal_history_payload(
        &self,
        workflow_id: &str,
        run_id: &str,
        event_type: &str,
    ) -> Option<Value> {
        let base = format!(
            "/workflows/{}/runs/{}/history",
            percent_encode_path_segment(workflow_id),
            percent_encode_path_segment(run_id),
        );
        let mut next_page_token: Option<String> = None;
        let mut payload = None;
        for _ in 0..TERMINAL_HISTORY_PAGE_LIMIT {
            let path = match &next_page_token {
                Some(token) => format!(
                    "{base}?next_page_token={}",
                    percent_encode_path_segment(token)
                ),
                None => base.clone(),
            };
            let page: Value = self
                .request_json(
                    reqwest::Method::GET,
                    &path,
                    RequestProtocol::ControlPlane,
                    Option::<&Value>::None,
                )
                .await
                .ok()?;
            for event in page.get("events").and_then(Value::as_array)? {
                if event.get("event_type").and_then(Value::as_str) == Some(event_type) {
                    payload = event
                        .get("payload")
                        .filter(|value| value.is_object())
                        .cloned();
                }
            }
            next_page_token = page
                .get("next_page_token")
                .and_then(Value::as_str)
                .filter(|token| !token.is_empty())
                .map(str::to_string);
            if next_page_token.is_none() {
                return payload;
            }
        }
        None
    }
}
