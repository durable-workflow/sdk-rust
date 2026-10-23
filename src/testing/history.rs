use super::TestingError;
use crate::{
    decode_wire_avro_value, encode_typed_envelope, enrich_query_history_from_export,
    hydrate_query_history_from_export, json, normalize_avro_arguments, percent_encode_path_segment,
    validate_query_task_payloads, AvroValue, Client, HistoryEvent, QueryTask, RequestProtocol,
    Result, Value, WorkflowTask, DEFAULT_CODEC,
};
use serde::Serialize;

/// Recorded workflow input and history, independent of a live Server.
#[derive(Clone, Debug)]
pub struct WorkflowHistory {
    pub(super) task: WorkflowTask,
    pub(super) task_queue: Option<String>,
}

fn invalid(message: &str) -> crate::Error {
    TestingError::InvalidHistory(message.into()).into()
}

impl WorkflowHistory {
    /// Import a `dw workflow:history-export` or SDK export bundle.
    /// Payloads must be present and resolved; redacted/pruned input is refused.
    pub fn from_export(export: Value) -> Result<Self> {
        if export["schema"] != "durable-workflow.v2.history-export" {
            return Err(invalid("expected durable-workflow.v2.history-export"));
        }
        if export
            .pointer("/redaction/paths")
            .and_then(Value::as_array)
            .is_some_and(|paths| !paths.is_empty())
        {
            return Err(invalid("replay requires an unredacted history export"));
        }
        let events: Vec<HistoryEvent> = serde_json::from_value(
            export
                .get("history_events")
                .cloned()
                .ok_or_else(|| invalid("missing history_events"))?,
        )?;
        let started = events
            .iter()
            .find(|event| event.event_type == "WorkflowStarted");
        let workflow_type = export["workflow"]["workflow_type"]
            .as_str()
            .or_else(|| started.and_then(|event| event.payload["workflow_type"].as_str()))
            .filter(|value| !value.is_empty())
            .ok_or_else(|| invalid("missing workflow type"))?;
        let codec = export["payloads"]["codec"]
            .as_str()
            .unwrap_or(DEFAULT_CODEC);
        let input = export
            .pointer("/payloads/arguments/data")
            .filter(|value| !value.is_null())
            .or_else(|| started.and_then(|event| event.payload.get("arguments")))
            .ok_or_else(|| {
                invalid(
                    "workflow input is unavailable; from_history accepts explicit original input",
                )
            })?;
        let input = decode_wire_avro_value(input, codec)?;
        let mut query: QueryTask = serde_json::from_value(json!({
            "query_task_id":"offline", "workflow_type":workflow_type, "query_name":"offline",
            "payload_codec":codec, "workflow_arguments":encode_typed_envelope(&normalize_avro_arguments(input),codec)?,
            "history_events":events, "history_export":export,
        }))?;
        validate_query_task_payloads(&query)?;
        hydrate_query_history_from_export(&mut query)?;
        enrich_query_history_from_export(&mut query)?;
        for event in &mut query.history_events {
            if event.event_type == "WorkflowCompleted"
                && event.payload.get("output").is_none_or(Value::is_null)
            {
                if let Some(output) = export
                    .pointer("/payloads/output/data")
                    .filter(|value| !value.is_null())
                {
                    event.payload["output"] = output.clone();
                }
            }
        }
        let queue = export["workflow"]["queue"]
            .as_str()
            .or_else(|| started.and_then(|event| event.payload["task_queue"].as_str()))
            .map(str::to_string);
        Self::build(
            workflow_type,
            query.workflow_arguments,
            query.history_events,
            export["workflow"]["instance_id"].as_str(),
            export["workflow"]["run_id"].as_str(),
            queue,
        )
    }

    /// Import all pages of the Server history API, supplying original input.
    /// Also accepts an array of events. A remaining page token is refused.
    pub fn from_history<T: Serialize>(
        workflow_type: &str,
        input: T,
        history: Value,
    ) -> Result<Self> {
        if history
            .get("next_page_token")
            .is_some_and(|token| !token.is_null() && token.as_str() != Some(""))
        {
            return Err(invalid(
                "history has another page; combine all pages before replay",
            ));
        }
        let raw = if history.is_array() {
            history.clone()
        } else {
            history
                .get("events")
                .or_else(|| history.get("history_events"))
                .cloned()
                .ok_or_else(|| invalid("missing events"))?
        };
        let events: Vec<HistoryEvent> = serde_json::from_value(raw)?;
        let queue = events
            .iter()
            .find(|event| event.event_type == "WorkflowStarted")
            .and_then(|event| event.payload["task_queue"].as_str())
            .map(str::to_string);
        let input = normalize_avro_arguments(AvroValue::from_serialize(&input)?);
        Self::build(
            workflow_type,
            Some(encode_typed_envelope(&input, DEFAULT_CODEC)?),
            events,
            history["workflow_id"].as_str(),
            history["run_id"].as_str(),
            queue,
        )
    }

    fn build(
        workflow_type: &str,
        input: Option<Value>,
        events: Vec<HistoryEvent>,
        workflow_id: Option<&str>,
        run_id: Option<&str>,
        task_queue: Option<String>,
    ) -> Result<Self> {
        if workflow_type.trim().is_empty() {
            return Err(invalid("workflow type must not be empty"));
        }
        for event in &events {
            let field = match event.event_type.as_str() {
                "ActivityCompleted" | "SideEffectRecorded" => Some("result"),
                "WorkflowCompleted" => Some("output"),
                "ChildRunCompleted" if event.payload.get("output").is_none_or(Value::is_null) => {
                    Some("result")
                }
                _ => None,
            };
            if field.is_some_and(|field| event.payload.get(field).is_none_or(Value::is_null)) {
                return Err(invalid("a recorded result is missing or redacted"));
            }
        }
        let task = serde_json::from_value(json!({
            "task_id":"offline-replay", "workflow_command_id":"offline-replay",
            "workflow_type":workflow_type, "workflow_id":workflow_id, "run_id":run_id,
            "payload_codec":DEFAULT_CODEC, "arguments":input, "history_events":events,
        }))?;
        Ok(Self { task, task_queue })
    }

    /// Override task-queue context omitted by a raw history response.
    pub fn task_queue(mut self, queue: impl Into<String>) -> Self {
        self.task_queue = Some(queue.into());
        self
    }

    pub fn workflow_type(&self) -> &str {
        &self.task.workflow_type
    }
    pub fn events(&self) -> &[HistoryEvent] {
        &self.task.history_events
    }

    /// Produce a portable self-contained export of this test history.
    pub fn export(&self) -> Value {
        json!({"schema":"durable-workflow.v2.history-export",
            "history_complete":self.events().iter().any(|event|matches!(event.event_type.as_str(),"WorkflowCompleted"|"WorkflowFailed"|"WorkflowContinuedAsNew")),
            "workflow":{"instance_id":self.task.workflow_id,"run_id":self.task.run_id,
                "workflow_type":self.task.workflow_type,"queue":self.task_queue},
            "payloads":{"codec":DEFAULT_CODEC,"arguments":{"available":true,"data":self.task.arguments}},
            "history_events":self.task.history_events})
    }
}

impl Client {
    /// Download a replay bundle, hydrating runtime payload references before
    /// returning it so replay itself does not need a Server connection.
    pub async fn export_workflow_history(&self, workflow_id: &str, run_id: &str) -> Result<Value> {
        self.request_json(
            reqwest::Method::GET,
            &format!(
                "/workflows/{}/runs/{}/history/export",
                percent_encode_path_segment(workflow_id),
                percent_encode_path_segment(run_id)
            ),
            RequestProtocol::ControlPlane,
            Option::<&Value>::None,
        )
        .await
    }
}
