//! Minimal Rust SDK for the Durable Workflow worker protocol.
//!
//! The crate covers the v1 Rust round-trip: start, signal, and query workflows,
//! register a Rust worker, poll workflow, activity, and read-only query tasks,
//! reconstruct typed workflow-instance state through deterministic replay,
//! heartbeat worker and activity liveness, and exchange JSON-native payloads
//! through the same `avro` generic wrapper used by the existing first-party
//! SDKs.

use std::{
    any::{Any, TypeId},
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, OnceLock,
    },
    task::{Context as TaskContext, Poll},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use apache_avro::{from_avro_datum, from_value, to_avro_datum, to_value, Schema};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use futures_util::{future::OptionFuture, task::noop_waker_ref};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
pub use serde_json::{json, Value};
use thiserror::Error;

pub const WORKER_PROTOCOL_VERSION: &str = "1.2";
pub const CONTROL_PLANE_VERSION: &str = "2";
pub const DEFAULT_CODEC: &str = "avro";
pub const JSON_CODEC: &str = "json";
pub const SDK_VERSION: &str = concat!("durable-workflow-rust/", env!("CARGO_PKG_VERSION"));
/// Worker-registration capability for server-routed read-only queries.
pub const QUERY_TASKS_CAPABILITY: &str = "query_tasks";
/// First additive worker protocol that defines query-task transport.
pub const QUERY_TASK_MINIMUM_WORKER_PROTOCOL_VERSION: &str = "1.8";

const MAX_LONG_POLL_TIMEOUT_SECONDS: u64 = 60;

const QUERY_TASK_FINAL_REJECTION_REASONS: &[&str] = &[
    "lease_expired",
    "query_task_not_found",
    "query_task_not_leased",
    "query_task_timed_out",
];

const AVRO_PAYLOAD_SCHEMA_JSON: &str = r#"{"type":"record","name":"Payload","namespace":"durable_workflow","fields":[{"name":"json","type":"string"},{"name":"version","type":"int","default":1}]}"#;
const AVRO_PAYLOAD_VERSION: i32 = 1;

static AVRO_PAYLOAD_SCHEMA: OnceLock<std::result::Result<Schema, String>> = OnceLock::new();

#[derive(Clone, Copy)]
enum RequestProtocol {
    ControlPlane,
    Worker(&'static str),
}

impl RequestProtocol {
    fn is_worker(self) -> bool {
        matches!(self, Self::Worker(_))
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("transport error: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("http {status}: {body}")]
    Http {
        status: reqwest::StatusCode,
        body: String,
    },
    #[error("codec error: {0}")]
    Codec(String),
    #[error(transparent)]
    QueryFailed(QueryFailure),
    #[error(transparent)]
    Protocol(ProtocolFailure),
    #[error("workflow handler {0:?} is not registered")]
    WorkflowNotRegistered(String),
    #[error("activity handler {0:?} is not registered")]
    ActivityNotRegistered(String),
    #[error("workflow future yielded without emitting a durable command")]
    WorkflowYieldedWithoutCommand,
    #[error("workflow state lock is poisoned")]
    WorkflowStatePoisoned,
    #[error("operation timed out")]
    Timeout,
    #[error("worker loop error: {0}")]
    WorkerLoop(String),
}

/// A stable, machine-readable workflow query or query-task settlement failure.
#[derive(Clone, Debug, Error)]
#[error("query failed ({reason}, HTTP {status}): {message}")]
pub struct QueryFailure {
    pub status: u16,
    pub reason: String,
    pub message: String,
    pub body: Value,
}

/// A stable failure returned when a server rejects an SDK protocol version.
#[derive(Clone, Debug, Error)]
#[error("protocol rejected ({reason}, HTTP {status}): {message}")]
pub struct ProtocolFailure {
    pub status: u16,
    pub reason: String,
    pub message: String,
    pub supported_version: Option<String>,
    pub requested_version: Option<String>,
    pub body: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PayloadEnvelope {
    pub codec: String,
    pub blob: String,
}

impl PayloadEnvelope {
    pub fn avro<T: Serialize>(value: &T) -> Result<Self> {
        encode_payload(value, DEFAULT_CODEC)
    }

    pub fn json<T: Serialize>(value: &T) -> Result<Self> {
        encode_payload(value, JSON_CODEC)
    }
}

pub fn encode_payload<T: Serialize>(value: &T, codec: &str) -> Result<PayloadEnvelope> {
    let value = serde_json::to_value(value)?;
    let blob = encode_value_blob(&value, codec)?;

    Ok(PayloadEnvelope {
        codec: codec.to_string(),
        blob,
    })
}

pub fn decode_payload<T: DeserializeOwned>(envelope: &PayloadEnvelope) -> Result<T> {
    let value = decode_blob(&envelope.blob, &envelope.codec)?;
    Ok(serde_json::from_value(value)?)
}

fn encode_value_envelope(value: &Value, codec: &str) -> Result<Value> {
    Ok(serde_json::to_value(encode_payload(value, codec)?)?)
}

fn encode_value_blob(value: &Value, codec: &str) -> Result<String> {
    match codec {
        JSON_CODEC => Ok(serde_json::to_string(value)?),
        DEFAULT_CODEC => encode_avro_generic(value),
        other => Err(Error::Codec(format!("unsupported payload codec {other:?}"))),
    }
}

fn decode_wire_value(value: &Value, fallback_codec: &str) -> Result<Value> {
    if value.is_null() {
        return Ok(Value::Null);
    }

    if let Some(object) = value.as_object() {
        if let (Some(codec), Some(blob)) = (
            object.get("codec").and_then(Value::as_str),
            object.get("blob").and_then(Value::as_str),
        ) {
            return decode_blob(blob, codec);
        }
    }

    if let Some(blob) = value.as_str() {
        return decode_blob(blob, fallback_codec);
    }

    Ok(value.clone())
}

fn decode_blob(blob: &str, codec: &str) -> Result<Value> {
    match codec {
        JSON_CODEC => Ok(serde_json::from_str(blob)?),
        DEFAULT_CODEC => decode_avro_generic(blob),
        other => Err(Error::Codec(format!("unsupported payload codec {other:?}"))),
    }
}

fn encode_avro_generic(value: &Value) -> Result<String> {
    let json = serde_json::to_string(value)?;
    let datum = to_value(AvroPayload {
        json,
        version: AVRO_PAYLOAD_VERSION,
    })
    .map_err(|err| Error::Codec(format!("could not convert avro generic wrapper: {err}")))?;
    let datum = to_avro_datum(avro_payload_schema()?, datum)
        .map_err(|err| Error::Codec(format!("could not encode avro generic wrapper: {err}")))?;

    let mut bytes = Vec::with_capacity(datum.len() + 1);
    bytes.push(0x00);
    bytes.extend_from_slice(&datum);
    Ok(BASE64.encode(bytes))
}

fn decode_avro_generic(blob: &str) -> Result<Value> {
    let bytes = BASE64
        .decode(blob)
        .map_err(|err| Error::Codec(format!("invalid avro base64 payload: {err}")))?;

    if bytes.is_empty() {
        return Err(Error::Codec("avro payload is empty".to_string()));
    }

    match bytes[0] {
        0x00 => {}
        0x01 => {
            return Err(Error::Codec(
                "typed avro payloads require a schema context; v1 supports the generic wrapper"
                    .to_string(),
            ));
        }
        other => {
            return Err(Error::Codec(format!(
                "unknown avro payload prefix 0x{other:02x}"
            )));
        }
    }

    let mut datum = &bytes[1..];
    let datum = from_avro_datum(avro_payload_schema()?, &mut datum, None)
        .map_err(|err| Error::Codec(format!("could not decode avro generic wrapper: {err}")))?;
    let payload: AvroPayload = from_value(&datum)
        .map_err(|err| Error::Codec(format!("invalid avro generic wrapper record: {err}")))?;

    if payload.version != AVRO_PAYLOAD_VERSION {
        return Err(Error::Codec(format!(
            "unsupported avro generic wrapper version {}",
            payload.version
        )));
    }

    Ok(serde_json::from_str(&payload.json)?)
}

#[derive(Debug, Serialize, Deserialize)]
struct AvroPayload {
    json: String,
    version: i32,
}

fn avro_payload_schema() -> Result<&'static Schema> {
    match AVRO_PAYLOAD_SCHEMA.get_or_init(|| {
        Schema::parse_str(AVRO_PAYLOAD_SCHEMA_JSON)
            .map_err(|err| format!("could not parse avro payload schema: {err}"))
    }) {
        Ok(schema) => Ok(schema),
        Err(message) => Err(Error::Codec(message.clone())),
    }
}

#[derive(Clone, Debug)]
pub struct Client {
    http: reqwest::Client,
    base_url: String,
    token: Option<String>,
    control_token: Option<String>,
    worker_token: Option<String>,
    namespace: String,
}

impl Client {
    pub fn new(base_url: impl Into<String>) -> Result<Self> {
        Self::builder(base_url).build()
    }

    pub fn builder(base_url: impl Into<String>) -> ClientBuilder {
        ClientBuilder {
            base_url: base_url.into(),
            token: None,
            control_token: None,
            worker_token: None,
            namespace: "default".to_string(),
            timeout: Duration::from_secs(60),
        }
    }

    pub async fn health(&self) -> Result<Value> {
        self.request_json(
            reqwest::Method::GET,
            "/health",
            RequestProtocol::ControlPlane,
            Option::<&Value>::None,
        )
        .await
    }

    pub async fn cluster_info(&self) -> Result<Value> {
        self.request_json(
            reqwest::Method::GET,
            "/cluster/info",
            RequestProtocol::ControlPlane,
            Option::<&Value>::None,
        )
        .await
    }

    pub async fn start_workflow<T: Serialize>(
        &self,
        workflow_type: &str,
        task_queue: &str,
        workflow_id: &str,
        input: T,
    ) -> Result<WorkflowHandle> {
        let input = serde_json::to_value(input)?;
        let input_envelope = encode_value_envelope(&normalize_arguments(input), DEFAULT_CODEC)?;
        let body = json!({
            "workflow_id": workflow_id,
            "workflow_type": workflow_type,
            "task_queue": task_queue,
            "input": input_envelope,
            "execution_timeout_seconds": 3600,
            "run_timeout_seconds": 600
        });

        let data: Value = self
            .request_json(
                reqwest::Method::POST,
                "/workflows",
                RequestProtocol::ControlPlane,
                Some(&body),
            )
            .await?;

        Ok(WorkflowHandle {
            client: self.clone(),
            workflow_id: data
                .get("workflow_id")
                .and_then(Value::as_str)
                .unwrap_or(workflow_id)
                .to_string(),
            run_id: data
                .get("run_id")
                .and_then(Value::as_str)
                .map(str::to_string),
            workflow_type: data
                .get("workflow_type")
                .and_then(Value::as_str)
                .unwrap_or(workflow_type)
                .to_string(),
        })
    }

    pub async fn signal_workflow<T: Serialize>(
        &self,
        workflow_id: &str,
        signal_name: &str,
        input: T,
    ) -> Result<Value> {
        let input = serde_json::to_value(input)?;
        let input_envelope = encode_value_envelope(&normalize_arguments(input), DEFAULT_CODEC)?;
        let body = json!({
            "input": input_envelope
        });
        let path = format!("/workflows/{workflow_id}/signal/{signal_name}");
        self.request_json(
            reqwest::Method::POST,
            &path,
            RequestProtocol::ControlPlane,
            Some(&body),
        )
        .await
    }

    /// Execute a named, read-only query against a running or completed workflow.
    ///
    /// Arguments and results use the platform payload envelope. Server and
    /// worker rejections are returned as [`Error::QueryFailed`] with a stable
    /// reason, HTTP status, and original response body.
    pub async fn query_workflow<T: Serialize>(
        &self,
        workflow_id: &str,
        query_name: &str,
        input: T,
    ) -> Result<Value> {
        let input = serde_json::to_value(input)?;
        let input_envelope = encode_value_envelope(&normalize_arguments(input), DEFAULT_CODEC)?;
        let body = json!({
            "input": input_envelope
        });
        let path = format!("/workflows/{workflow_id}/query/{query_name}");
        let response: Value = match self
            .request_json(
                reqwest::Method::POST,
                &path,
                RequestProtocol::ControlPlane,
                Some(&body),
            )
            .await
        {
            Ok(response) => response,
            Err(Error::Http { status, body }) => {
                return Err(Error::QueryFailed(query_failure(status, body)));
            }
            Err(error) => return Err(error),
        };

        if let Some(envelope) = response
            .get("result_envelope")
            .filter(|envelope| !envelope.is_null())
        {
            return decode_wire_value(envelope, DEFAULT_CODEC);
        }

        Ok(response.get("result").cloned().unwrap_or(Value::Null))
    }

    pub async fn describe_workflow(&self, workflow_id: &str) -> Result<WorkflowDescription> {
        let path = format!("/workflows/{workflow_id}");
        let mut data: WorkflowDescription = self
            .request_json(
                reqwest::Method::GET,
                &path,
                RequestProtocol::ControlPlane,
                Option::<&Value>::None,
            )
            .await?;
        data.decode_payloads()?;
        Ok(data)
    }

    pub async fn register_worker(
        &self,
        worker_id: &str,
        task_queue: &str,
        supported_workflow_types: Vec<String>,
        supported_activity_types: Vec<String>,
        max_concurrent_workflow_tasks: usize,
        max_concurrent_activity_tasks: usize,
    ) -> Result<RegisterWorkerResponse> {
        self.register_worker_with_capabilities(
            worker_id,
            task_queue,
            supported_workflow_types,
            supported_activity_types,
            max_concurrent_workflow_tasks,
            max_concurrent_activity_tasks,
            Vec::new(),
        )
        .await
    }

    /// Register a worker and explicitly advertise additive worker capabilities.
    pub async fn register_worker_with_capabilities(
        &self,
        worker_id: &str,
        task_queue: &str,
        supported_workflow_types: Vec<String>,
        supported_activity_types: Vec<String>,
        max_concurrent_workflow_tasks: usize,
        max_concurrent_activity_tasks: usize,
        capabilities: Vec<String>,
    ) -> Result<RegisterWorkerResponse> {
        let body = json!({
            "worker_id": worker_id,
            "task_queue": task_queue,
            "runtime": "rust",
            "sdk_version": SDK_VERSION,
            "supported_workflow_types": supported_workflow_types,
            "supported_activity_types": supported_activity_types,
            "capabilities": capabilities,
            "max_concurrent_workflow_tasks": max_concurrent_workflow_tasks,
            "max_concurrent_activity_tasks": max_concurrent_activity_tasks
        });

        self.request_json(
            reqwest::Method::POST,
            "/worker/register",
            RequestProtocol::Worker(WORKER_PROTOCOL_VERSION),
            Some(&body),
        )
        .await
    }

    /// Long-poll for an ephemeral, read-only workflow query task.
    pub async fn poll_query_task(
        &self,
        worker_id: &str,
        task_queue: &str,
        timeout: Duration,
    ) -> Result<Option<QueryTask>> {
        let timeout_seconds = long_poll_timeout_seconds(timeout);
        let body = json!({
            "worker_id": worker_id,
            "task_queue": task_queue,
            "poll_request_id": unique_request_id("rust-query-poll"),
            "timeout_seconds": timeout_seconds,
        });
        let data: PollQueryTaskResponse = self
            .request_json_with_timeout(
                reqwest::Method::POST,
                "/worker/query-tasks/poll",
                RequestProtocol::Worker(QUERY_TASK_MINIMUM_WORKER_PROTOCOL_VERSION),
                Some(&body),
                timeout + Duration::from_secs(5),
            )
            .await?;
        Ok(data.task)
    }

    /// Complete a query task without appending workflow history.
    pub async fn complete_query_task(
        &self,
        query_task_id: &str,
        lease_owner: &str,
        query_task_attempt: u64,
        result: Value,
        codec: &str,
    ) -> Result<Value> {
        let result_envelope = encode_value_envelope(&result, codec)?;
        self.complete_query_task_with_envelope(
            query_task_id,
            lease_owner,
            query_task_attempt,
            result,
            result_envelope,
        )
        .await
    }

    async fn complete_query_task_with_envelope(
        &self,
        query_task_id: &str,
        lease_owner: &str,
        query_task_attempt: u64,
        result: Value,
        result_envelope: Value,
    ) -> Result<Value> {
        let body = json!({
            "lease_owner": lease_owner,
            "query_task_attempt": query_task_attempt,
            "result": result,
            "result_envelope": result_envelope,
        });
        let path = format!("/worker/query-tasks/{query_task_id}/complete");
        let response = self
            .request_json(
                reqwest::Method::POST,
                &path,
                RequestProtocol::Worker(QUERY_TASK_MINIMUM_WORKER_PROTOCOL_VERSION),
                Some(&body),
            )
            .await;
        query_task_response(response)
    }

    /// Report a stable machine-readable query-task failure.
    pub async fn fail_query_task(
        &self,
        query_task_id: &str,
        lease_owner: &str,
        query_task_attempt: u64,
        message: impl Into<String>,
        reason: impl Into<String>,
        failure_type: impl Into<String>,
    ) -> Result<Value> {
        let body = json!({
            "lease_owner": lease_owner,
            "query_task_attempt": query_task_attempt,
            "failure": {
                "message": message.into(),
                "reason": reason.into(),
                "type": failure_type.into(),
            }
        });
        let path = format!("/worker/query-tasks/{query_task_id}/fail");
        let response = self
            .request_json(
                reqwest::Method::POST,
                &path,
                RequestProtocol::Worker(QUERY_TASK_MINIMUM_WORKER_PROTOCOL_VERSION),
                Some(&body),
            )
            .await;
        query_task_response(response)
    }

    pub async fn heartbeat_worker(
        &self,
        worker_id: &str,
        workflow_available: usize,
        activity_available: usize,
    ) -> Result<Value> {
        let body = json!({
            "worker_id": worker_id,
            "task_slots": {
                "workflow_available": workflow_available,
                "activity_available": activity_available
            },
            "process_metrics": {
                "process_id": std::process::id(),
                "process_uptime_seconds": 0
            }
        });

        self.request_json(
            reqwest::Method::POST,
            "/worker/heartbeat",
            RequestProtocol::Worker(WORKER_PROTOCOL_VERSION),
            Some(&body),
        )
        .await
    }

    pub async fn poll_workflow_task(
        &self,
        worker_id: &str,
        task_queue: &str,
        timeout: Duration,
    ) -> Result<Option<WorkflowTask>> {
        Ok(self
            .poll_workflow_task_response(worker_id, task_queue, timeout)
            .await?
            .task)
    }

    pub async fn poll_workflow_task_response(
        &self,
        worker_id: &str,
        task_queue: &str,
        timeout: Duration,
    ) -> Result<PollWorkflowTaskResponse> {
        let body = json!({
            "worker_id": worker_id,
            "task_queue": task_queue,
            "timeout_seconds": long_poll_timeout_seconds(timeout),
        });
        let mut data: PollWorkflowTaskResponse = self
            .request_json_with_timeout(
                reqwest::Method::POST,
                "/worker/workflow-tasks/poll",
                RequestProtocol::Worker(WORKER_PROTOCOL_VERSION),
                Some(&body),
                timeout + Duration::from_secs(5),
            )
            .await?;

        if let Some(task) = data.task.as_mut() {
            self.fetch_remaining_workflow_history(worker_id, task)
                .await?;
        }

        Ok(data)
    }

    async fn fetch_remaining_workflow_history(
        &self,
        worker_id: &str,
        task: &mut WorkflowTask,
    ) -> Result<()> {
        let mut next_token = task.next_history_page_token.clone();

        while let Some(token) = next_token.take().filter(|token| !token.is_empty()) {
            let lease_owner = task
                .lease_owner
                .clone()
                .unwrap_or_else(|| worker_id.to_string());
            let page = self
                .workflow_task_history_page(
                    &task.task_id,
                    &lease_owner,
                    task.workflow_task_attempt,
                    &token,
                )
                .await?;

            task.append_history_page(page);

            if task.next_history_page_token.as_deref() == Some(token.as_str()) {
                return Err(Error::Codec(
                    "workflow history pagination returned the same page token".to_string(),
                ));
            }

            next_token = task.next_history_page_token.clone();
        }

        Ok(())
    }

    async fn workflow_task_history_page(
        &self,
        task_id: &str,
        lease_owner: &str,
        workflow_task_attempt: u64,
        next_history_page_token: &str,
    ) -> Result<WorkflowTaskHistoryPage> {
        let body = json!({
            "lease_owner": lease_owner,
            "workflow_task_attempt": workflow_task_attempt,
            "next_history_page_token": next_history_page_token
        });
        let path = format!("/worker/workflow-tasks/{task_id}/history");

        self.request_json(
            reqwest::Method::POST,
            &path,
            RequestProtocol::Worker(WORKER_PROTOCOL_VERSION),
            Some(&body),
        )
        .await
    }

    pub async fn complete_workflow_task(
        &self,
        task_id: &str,
        lease_owner: &str,
        workflow_task_attempt: u64,
        commands: Vec<Value>,
    ) -> Result<Value> {
        let body = json!({
            "lease_owner": lease_owner,
            "workflow_task_attempt": workflow_task_attempt,
            "commands": commands
        });
        let path = format!("/worker/workflow-tasks/{task_id}/complete");
        self.request_json(
            reqwest::Method::POST,
            &path,
            RequestProtocol::Worker(WORKER_PROTOCOL_VERSION),
            Some(&body),
        )
        .await
    }

    pub async fn fail_workflow_task(
        &self,
        task_id: &str,
        lease_owner: &str,
        workflow_task_attempt: u64,
        message: impl Into<String>,
    ) -> Result<Value> {
        let body = json!({
            "lease_owner": lease_owner,
            "workflow_task_attempt": workflow_task_attempt,
            "failure": {
                "message": message.into(),
                "type": "RustWorkflowTaskFailure"
            }
        });
        let path = format!("/worker/workflow-tasks/{task_id}/fail");
        self.request_json(
            reqwest::Method::POST,
            &path,
            RequestProtocol::Worker(WORKER_PROTOCOL_VERSION),
            Some(&body),
        )
        .await
    }

    pub async fn poll_activity_task(
        &self,
        worker_id: &str,
        task_queue: &str,
        timeout: Duration,
    ) -> Result<Option<ActivityTask>> {
        let body = json!({
            "worker_id": worker_id,
            "task_queue": task_queue,
            "timeout_seconds": long_poll_timeout_seconds(timeout),
        });
        let data: PollActivityTaskResponse = self
            .request_json_with_timeout(
                reqwest::Method::POST,
                "/worker/activity-tasks/poll",
                RequestProtocol::Worker(WORKER_PROTOCOL_VERSION),
                Some(&body),
                timeout + Duration::from_secs(5),
            )
            .await?;
        Ok(data.task)
    }

    pub async fn complete_activity_task(
        &self,
        task_id: &str,
        activity_attempt_id: &str,
        lease_owner: &str,
        result: Value,
        codec: &str,
    ) -> Result<Value> {
        let result = encode_value_envelope(&result, codec)?;
        let body = json!({
            "activity_attempt_id": activity_attempt_id,
            "lease_owner": lease_owner,
            "result": result
        });
        let path = format!("/worker/activity-tasks/{task_id}/complete");
        self.request_json(
            reqwest::Method::POST,
            &path,
            RequestProtocol::Worker(WORKER_PROTOCOL_VERSION),
            Some(&body),
        )
        .await
    }

    pub async fn fail_activity_task(
        &self,
        task_id: &str,
        activity_attempt_id: &str,
        lease_owner: &str,
        message: impl Into<String>,
        non_retryable: bool,
    ) -> Result<Value> {
        let body = json!({
            "activity_attempt_id": activity_attempt_id,
            "lease_owner": lease_owner,
            "failure": {
                "message": message.into(),
                "type": "RustActivityFailure",
                "non_retryable": non_retryable
            }
        });
        let path = format!("/worker/activity-tasks/{task_id}/fail");
        self.request_json(
            reqwest::Method::POST,
            &path,
            RequestProtocol::Worker(WORKER_PROTOCOL_VERSION),
            Some(&body),
        )
        .await
    }

    pub async fn heartbeat_activity_task(
        &self,
        task_id: &str,
        activity_attempt_id: &str,
        lease_owner: &str,
        details: Value,
    ) -> Result<ActivityHeartbeatResponse> {
        let body = json!({
            "activity_attempt_id": activity_attempt_id,
            "lease_owner": lease_owner,
            "details": details
        });
        let path = format!("/worker/activity-tasks/{task_id}/heartbeat");
        self.request_json(
            reqwest::Method::POST,
            &path,
            RequestProtocol::Worker(WORKER_PROTOCOL_VERSION),
            Some(&body),
        )
        .await
    }

    async fn request_json<T: DeserializeOwned, B: Serialize + ?Sized>(
        &self,
        method: reqwest::Method,
        path: &str,
        protocol: RequestProtocol,
        body: Option<&B>,
    ) -> Result<T> {
        self.request_json_with_timeout(method, path, protocol, body, Duration::from_secs(60))
            .await
    }

    async fn request_json_with_timeout<T: DeserializeOwned, B: Serialize + ?Sized>(
        &self,
        method: reqwest::Method,
        path: &str,
        protocol: RequestProtocol,
        body: Option<&B>,
        timeout: Duration,
    ) -> Result<T> {
        let mut request = self
            .http
            .request(method, format!("{}/api{}", self.base_url, path))
            .timeout(timeout)
            .header(reqwest::header::ACCEPT, "application/json")
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .header("X-Namespace", &self.namespace);

        match protocol {
            RequestProtocol::Worker(version) => {
                request = request.header("X-Durable-Workflow-Protocol-Version", version);
            }
            RequestProtocol::ControlPlane => {
                request = request.header(
                    "X-Durable-Workflow-Control-Plane-Version",
                    CONTROL_PLANE_VERSION,
                );
            }
        }

        if let Some(token) = self.auth_token(protocol.is_worker()) {
            request = request.bearer_auth(token);
        }

        if let Some(body) = body {
            request = request.json(body);
        }

        let response = request.send().await?;
        let status = response.status();
        let bytes = response.bytes().await?;

        if !status.is_success() {
            let body = String::from_utf8_lossy(&bytes).to_string();
            if let Some(protocol) = protocol_failure(status, &body) {
                return Err(Error::Protocol(protocol));
            }
            return Err(Error::Http { status, body });
        }

        if bytes.is_empty() {
            return Ok(serde_json::from_value(Value::Null)?);
        }

        Ok(serde_json::from_slice(&bytes)?)
    }

    fn auth_token(&self, worker: bool) -> Option<&str> {
        if worker {
            self.worker_token
                .as_deref()
                .or(self.token.as_deref())
                .or(self.control_token.as_deref())
        } else {
            self.control_token
                .as_deref()
                .or(self.token.as_deref())
                .or(self.worker_token.as_deref())
        }
    }
}

fn query_failure(status: reqwest::StatusCode, raw_body: String) -> QueryFailure {
    let body = serde_json::from_str(&raw_body).unwrap_or_else(|_| json!({"message": raw_body}));
    let reason = body
        .get("reason")
        .and_then(Value::as_str)
        .unwrap_or("query_rejected")
        .to_string();
    let message = body
        .get("message")
        .or_else(|| body.get("error"))
        .and_then(Value::as_str)
        .unwrap_or("workflow query was rejected")
        .to_string();

    QueryFailure {
        status: status.as_u16(),
        reason,
        message,
        body,
    }
}

fn query_task_response(response: Result<Value>) -> Result<Value> {
    match response {
        Err(Error::Http { status, body }) => Err(Error::QueryFailed(query_failure(status, body))),
        response => response,
    }
}

fn query_task_rejection_is_final(error: &Error) -> bool {
    matches!(
        error,
        Error::QueryFailed(failure)
            if QUERY_TASK_FINAL_REJECTION_REASONS.contains(&failure.reason.as_str())
    )
}

fn protocol_failure(status: reqwest::StatusCode, raw_body: &str) -> Option<ProtocolFailure> {
    let body: Value = serde_json::from_str(raw_body).ok()?;
    let reason = body.get("reason")?.as_str()?;
    if !matches!(
        reason,
        "missing_protocol_version"
            | "unsupported_protocol_version"
            | "missing_control_plane_version"
            | "unsupported_control_plane_version"
    ) {
        return None;
    }

    Some(ProtocolFailure {
        status: status.as_u16(),
        reason: reason.to_string(),
        message: body
            .get("message")
            .or_else(|| body.get("error"))
            .and_then(Value::as_str)
            .unwrap_or("protocol version rejected")
            .to_string(),
        supported_version: body
            .get("supported_version")
            .and_then(Value::as_str)
            .map(str::to_string),
        requested_version: body
            .get("requested_version")
            .and_then(Value::as_str)
            .map(str::to_string),
        body,
    })
}

fn long_poll_timeout_seconds(timeout: Duration) -> u64 {
    timeout
        .as_secs()
        .saturating_add(u64::from(timeout.subsec_nanos() > 0))
        .min(MAX_LONG_POLL_TIMEOUT_SECONDS)
}

fn worker_operation_is_retryable(error: &Error) -> bool {
    match error {
        Error::Transport(error) => {
            error.is_timeout() || error.is_connect() || error.is_request() || error.is_body()
        }
        Error::Http { status, .. } => {
            matches!(
                *status,
                reqwest::StatusCode::REQUEST_TIMEOUT | reqwest::StatusCode::TOO_MANY_REQUESTS
            ) || status.is_server_error()
        }
        _ => false,
    }
}

fn worker_retry_delay(policy: WorkerRetryPolicy, retry: usize) -> Duration {
    let exponent = retry.saturating_sub(1).min(31) as u32;
    policy
        .initial_backoff
        .saturating_mul(1_u32 << exponent)
        .min(policy.max_backoff)
}

#[derive(Debug)]
pub struct ClientBuilder {
    base_url: String,
    token: Option<String>,
    control_token: Option<String>,
    worker_token: Option<String>,
    namespace: String,
    timeout: Duration,
}

impl ClientBuilder {
    pub fn token(mut self, token: Option<String>) -> Self {
        self.token = token;
        self
    }

    pub fn control_token(mut self, token: Option<String>) -> Self {
        self.control_token = token;
        self
    }

    pub fn worker_token(mut self, token: Option<String>) -> Self {
        self.worker_token = token;
        self
    }

    pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = namespace.into();
        self
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn build(self) -> Result<Client> {
        Ok(Client {
            http: reqwest::Client::builder().timeout(self.timeout).build()?,
            base_url: self.base_url.trim_end_matches('/').to_string(),
            token: self.token,
            control_token: self.control_token,
            worker_token: self.worker_token,
            namespace: self.namespace,
        })
    }
}

#[derive(Clone, Debug)]
pub struct WorkflowHandle {
    client: Client,
    pub workflow_id: String,
    pub run_id: Option<String>,
    pub workflow_type: String,
}

impl WorkflowHandle {
    pub async fn describe(&self) -> Result<WorkflowDescription> {
        self.client.describe_workflow(&self.workflow_id).await
    }

    pub async fn signal<T: Serialize>(&self, signal_name: &str, input: T) -> Result<Value> {
        self.client
            .signal_workflow(&self.workflow_id, signal_name, input)
            .await
    }

    /// Execute a named, read-only query against this workflow.
    pub async fn query<T: Serialize>(&self, query_name: &str, input: T) -> Result<Value> {
        self.client
            .query_workflow(&self.workflow_id, query_name, input)
            .await
    }

    pub async fn result(&self, options: WorkflowResultOptions) -> Result<Value> {
        let started = Instant::now();

        loop {
            let description = self.describe().await?;
            if description.is_completed() {
                return Ok(description.output.unwrap_or(Value::Null));
            }

            if description.is_terminal() {
                return Err(Error::Codec(format!(
                    "workflow {} closed with status {:?}",
                    self.workflow_id, description.status
                )));
            }

            if started.elapsed() >= options.timeout {
                return Err(Error::Timeout);
            }

            tokio::time::sleep(options.poll_interval).await;
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct WorkflowResultOptions {
    pub poll_interval: Duration,
    pub timeout: Duration,
}

impl Default for WorkflowResultOptions {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_millis(500),
            timeout: Duration::from_secs(30),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct WorkflowDescription {
    pub workflow_id: Option<String>,
    pub run_id: Option<String>,
    pub workflow_type: Option<String>,
    pub status: Option<String>,
    #[serde(default)]
    pub output: Option<Value>,
    #[serde(default)]
    pub output_envelope: Option<Value>,
    #[serde(flatten)]
    pub raw: HashMap<String, Value>,
}

impl WorkflowDescription {
    pub fn is_completed(&self) -> bool {
        matches!(self.status.as_deref(), Some("completed" | "Completed"))
    }

    pub fn is_terminal(&self) -> bool {
        matches!(
            self.status.as_deref(),
            Some(
                "completed"
                    | "Completed"
                    | "failed"
                    | "Failed"
                    | "cancelled"
                    | "Cancelled"
                    | "terminated"
                    | "Terminated"
                    | "timed_out"
                    | "TimedOut",
            )
        )
    }

    fn decode_payloads(&mut self) -> Result<()> {
        if let Some(envelope) = &self.output_envelope {
            self.output = Some(decode_wire_value(envelope, DEFAULT_CODEC)?);
        }

        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct RegisterWorkerResponse {
    pub worker_id: String,
    pub registered: bool,
    #[serde(default)]
    pub heartbeat_interval_seconds: Option<u64>,
    #[serde(default)]
    pub protocol_version: Option<String>,
    #[serde(default)]
    pub server_capabilities: Option<Value>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct PollWorkflowTaskResponse {
    #[serde(default)]
    pub task: Option<WorkflowTask>,
    #[serde(default)]
    pub poll_status: Option<String>,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub protocol_version: Option<String>,
    #[serde(default)]
    pub server_capabilities: Option<Value>,
}

#[derive(Clone, Debug, Deserialize)]
struct PollActivityTaskResponse {
    #[serde(default)]
    task: Option<ActivityTask>,
}

#[derive(Clone, Debug, Deserialize)]
struct PollQueryTaskResponse {
    #[serde(default)]
    task: Option<QueryTask>,
}

/// An ephemeral server-routed query task.
#[derive(Clone, Debug, Deserialize)]
pub struct QueryTask {
    pub query_task_id: String,
    #[serde(default = "default_workflow_task_attempt")]
    pub query_task_attempt: u64,
    #[serde(default)]
    pub lease_owner: Option<String>,
    #[serde(default)]
    pub workflow_id: Option<String>,
    #[serde(default)]
    pub run_id: Option<String>,
    pub workflow_type: String,
    pub query_name: String,
    #[serde(default = "default_payload_codec")]
    pub payload_codec: String,
    #[serde(default)]
    pub workflow_arguments: Option<Value>,
    #[serde(default)]
    pub query_arguments: Option<Value>,
    #[serde(default)]
    pub history_events: Vec<HistoryEvent>,
    #[serde(default)]
    pub history_export: Option<Value>,
    #[serde(default)]
    pub run_status: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct WorkflowTask {
    pub task_id: String,
    #[serde(default)]
    pub workflow_id: Option<String>,
    #[serde(default)]
    pub run_id: Option<String>,
    pub workflow_type: String,
    #[serde(default = "default_payload_codec")]
    pub payload_codec: String,
    #[serde(default)]
    pub arguments: Option<Value>,
    #[serde(default)]
    pub history_events: Vec<HistoryEvent>,
    #[serde(default)]
    pub total_history_events: Option<u64>,
    #[serde(default)]
    pub next_history_page_token: Option<String>,
    #[serde(default = "default_workflow_task_attempt")]
    pub workflow_task_attempt: u64,
    #[serde(default)]
    pub workflow_signal_id: Option<String>,
    #[serde(default)]
    pub signal_name: Option<String>,
    #[serde(default)]
    pub signal_arguments: Option<Value>,
    #[serde(default)]
    pub lease_owner: Option<String>,
}

impl WorkflowTask {
    fn append_history_page(&mut self, page: WorkflowTaskHistoryPage) {
        self.history_events.extend(page.history_events);

        if page.total_history_events.is_some() {
            self.total_history_events = page.total_history_events;
        }

        self.next_history_page_token = page
            .next_history_page_token
            .filter(|token| !token.is_empty());
    }
}

#[derive(Clone, Debug, Deserialize)]
struct WorkflowTaskHistoryPage {
    #[serde(default)]
    history_events: Vec<HistoryEvent>,
    #[serde(default)]
    total_history_events: Option<u64>,
    #[serde(default)]
    next_history_page_token: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ActivityTask {
    pub task_id: String,
    #[serde(default)]
    pub activity_attempt_id: Option<String>,
    #[serde(default)]
    pub attempt_id: Option<String>,
    pub activity_type: String,
    #[serde(default = "default_payload_codec")]
    pub payload_codec: String,
    #[serde(default)]
    pub arguments: Option<Value>,
    #[serde(default = "default_attempt_number")]
    pub attempt_number: u64,
    #[serde(default)]
    pub lease_owner: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct HistoryEvent {
    #[serde(alias = "type")]
    pub event_type: String,
    #[serde(default)]
    pub payload: Value,
    #[serde(flatten)]
    pub raw: HashMap<String, Value>,
}

/// One decoded signal in the committed workflow-history snapshot.
#[derive(Clone, Debug, PartialEq)]
pub struct QuerySignal {
    pub id: Option<String>,
    pub name: String,
    pub arguments: Vec<Value>,
    pub workflow_sequence: Option<u64>,
}

/// Immutable state supplied to a registered query handler.
///
/// This context intentionally exposes no activity, signal-wait, or command
/// APIs. Query handlers inspect committed history and return a value; query
/// completion does not append an event or advance deterministic execution.
#[derive(Clone, Debug)]
pub struct QueryContext {
    pub workflow_id: Option<String>,
    pub run_id: Option<String>,
    pub workflow_type: String,
    pub run_status: Option<String>,
    workflow_input: Value,
    history_events: Arc<Vec<HistoryEvent>>,
    signal_events: Arc<Vec<QuerySignal>>,
}

impl QueryContext {
    /// The normalized argument list used to start the workflow.
    pub fn workflow_input(&self) -> &Value {
        &self.workflow_input
    }

    /// The immutable committed history used for this query snapshot.
    pub fn history_events(&self) -> &[HistoryEvent] {
        self.history_events.as_slice()
    }

    /// All decoded signals in committed workflow order.
    pub fn signal_events(&self) -> &[QuerySignal] {
        self.signal_events.as_slice()
    }

    /// Decoded argument lists for each committed signal with `signal_name`.
    pub fn signals(&self, signal_name: &str) -> Vec<Vec<Value>> {
        self.signal_events
            .iter()
            .filter(|signal| signal.name == signal_name)
            .map(|signal| signal.arguments.clone())
            .collect()
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct ActivityHeartbeatResponse {
    #[serde(default)]
    pub cancel_requested: bool,
    #[serde(default)]
    pub heartbeat_recorded: bool,
}

fn default_payload_codec() -> String {
    DEFAULT_CODEC.to_string()
}

fn default_workflow_task_attempt() -> u64 {
    1
}

fn default_attempt_number() -> u64 {
    1
}

type WorkflowFuture = Pin<Box<dyn Future<Output = Result<Value>> + Send + 'static>>;
type WorkflowHandler = Arc<dyn Fn(WorkflowContext, Value) -> WorkflowFuture + Send + Sync>;
type ErasedWorkflowState = Arc<dyn Any + Send + Sync>;
type WorkflowStateSnapshot = Arc<dyn Fn() -> Result<ErasedWorkflowState> + Send + Sync>;
type ReplayedWorkflowHandler =
    Arc<dyn Fn(WorkflowContext, Value) -> ReplayedWorkflowInvocation + Send + Sync>;
type ActivityFuture = Pin<Box<dyn Future<Output = Result<Value>> + Send + 'static>>;
type ActivityHandler = Arc<dyn Fn(ActivityContext, Value) -> ActivityFuture + Send + Sync>;
type QueryFuture = Pin<Box<dyn Future<Output = Result<Value>> + Send + 'static>>;
type QueryHandler = Arc<dyn Fn(QueryContext, Value) -> QueryFuture + Send + Sync>;
type ReplayedQueryHandler = Arc<
    dyn Fn(QueryContext, ErasedWorkflowState, Value) -> std::result::Result<QueryFuture, String>
        + Send
        + Sync,
>;
type WorkerHeartbeatObserver = Arc<dyn Fn(&WorkerHeartbeatObservation) + Send + Sync>;

struct ReplayedWorkflowInvocation {
    future: WorkflowFuture,
    snapshot: WorkflowStateSnapshot,
}

#[derive(Clone)]
struct RegisteredWorkflow {
    execute: WorkflowHandler,
    replay: Option<ReplayedWorkflowHandler>,
    state_type: Option<TypeId>,
}

#[derive(Clone)]
enum RegisteredQuery {
    Snapshot(QueryHandler),
    Replayed {
        state_type: TypeId,
        handler: ReplayedQueryHandler,
    },
}

#[derive(Clone, Debug)]
pub struct WorkerHeartbeatObservation {
    pub worker_id: String,
    pub task_queue: String,
    pub acknowledged_at_unix_millis: u64,
    pub acknowledgement: Value,
}

/// Bounded retry policy for worker poll acquisition and worker heartbeats.
///
/// Expected empty long polls are normal successful responses. Transport
/// failures, HTTP 408/429 responses, and server errors are retried with capped
/// exponential backoff. Authentication, protocol, codec, and handler failures
/// are never retried by the worker.
#[derive(Clone, Copy, Debug)]
pub struct WorkerRetryPolicy {
    /// Number of retries after the initial request fails.
    pub max_retries: usize,
    /// Delay before the first retry.
    pub initial_backoff: Duration,
    /// Maximum delay between retries.
    pub max_backoff: Duration,
}

impl Default for WorkerRetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 5,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(5),
        }
    }
}

#[derive(Clone)]
pub struct Worker {
    client: Client,
    worker_id: String,
    task_queue: String,
    workflows: HashMap<String, RegisteredWorkflow>,
    activities: HashMap<String, ActivityHandler>,
    queries: HashMap<String, HashMap<String, RegisteredQuery>>,
    max_concurrent_workflow_tasks: usize,
    max_concurrent_activity_tasks: usize,
    poll_timeout: Duration,
    heartbeat_interval: Duration,
    retry_policy: WorkerRetryPolicy,
    heartbeat_observer: Option<WorkerHeartbeatObserver>,
}

impl Worker {
    pub fn new(client: Client, task_queue: impl Into<String>) -> Self {
        Self {
            client,
            worker_id: default_worker_id(),
            task_queue: task_queue.into(),
            workflows: HashMap::new(),
            activities: HashMap::new(),
            queries: HashMap::new(),
            max_concurrent_workflow_tasks: 10,
            max_concurrent_activity_tasks: 10,
            poll_timeout: Duration::from_secs(30),
            heartbeat_interval: Duration::from_secs(60),
            retry_policy: WorkerRetryPolicy::default(),
            heartbeat_observer: None,
        }
    }

    pub fn worker_id(mut self, worker_id: impl Into<String>) -> Self {
        self.worker_id = worker_id.into();
        self
    }

    pub fn poll_timeout(mut self, timeout: Duration) -> Self {
        self.poll_timeout = timeout;
        self
    }

    pub fn heartbeat_interval(mut self, interval: Duration) -> Self {
        self.heartbeat_interval = interval;
        self
    }

    /// Configure bounded retries for task-poll acquisition and worker heartbeats.
    pub fn retry_policy(mut self, policy: WorkerRetryPolicy) -> Self {
        self.retry_policy = policy;
        self
    }

    pub fn on_worker_heartbeat<F>(mut self, observer: F) -> Self
    where
        F: Fn(&WorkerHeartbeatObservation) + Send + Sync + 'static,
    {
        self.heartbeat_observer = Some(Arc::new(observer));
        self
    }

    pub fn max_concurrent_workflow_tasks(mut self, count: usize) -> Self {
        self.max_concurrent_workflow_tasks = count.max(1);
        self
    }

    pub fn max_concurrent_activity_tasks(mut self, count: usize) -> Self {
        self.max_concurrent_activity_tasks = count.max(1);
        self
    }

    pub fn register_workflow<F, Fut>(&mut self, workflow_type: impl Into<String>, handler: F)
    where
        F: Fn(WorkflowContext, Value) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Value>> + Send + 'static,
    {
        self.workflows.insert(
            workflow_type.into(),
            RegisteredWorkflow {
                execute: Arc::new(move |ctx, input| Box::pin(handler(ctx, input))),
                replay: None,
                state_type: None,
            },
        );
    }

    /// Register a workflow whose typed instance state can be reconstructed for queries.
    ///
    /// `state_factory` creates a fresh instance for every normal workflow task and
    /// query replay. The workflow handler is the single source of truth for state
    /// transitions: it updates [`WorkflowInstance`] after activities and signals
    /// resolve. Query replay runs this same handler over committed history and
    /// discards any commands it would emit.
    pub fn register_replayed_workflow<S, Factory, F, Fut>(
        &mut self,
        workflow_type: impl Into<String>,
        state_factory: Factory,
        handler: F,
    ) where
        S: Clone + Send + Sync + 'static,
        Factory: Fn() -> S + Send + Sync + 'static,
        F: Fn(WorkflowContext, Value, WorkflowInstance<S>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Value>> + Send + 'static,
    {
        let state_factory = Arc::new(state_factory);
        let handler = Arc::new(handler);

        let execute_factory = Arc::clone(&state_factory);
        let execute_handler = Arc::clone(&handler);
        let execute = Arc::new(move |ctx: WorkflowContext, input: Value| {
            let state = WorkflowInstance::new(execute_factory());
            let future = execute_handler(ctx, input, state);
            Box::pin(future) as WorkflowFuture
        });

        let replay = Arc::new(move |ctx: WorkflowContext, input: Value| {
            let state = WorkflowInstance::new(state_factory());
            let snapshot_state = state.clone();
            let snapshot: WorkflowStateSnapshot =
                Arc::new(move || Ok(Arc::new(snapshot_state.snapshot()?) as ErasedWorkflowState));
            let future = handler(ctx, input, state);
            ReplayedWorkflowInvocation {
                future: Box::pin(future),
                snapshot,
            }
        });

        self.workflows.insert(
            workflow_type.into(),
            RegisteredWorkflow {
                execute,
                replay: Some(replay),
                state_type: Some(TypeId::of::<S>()),
            },
        );
    }

    pub fn register_activity<F, Fut>(&mut self, activity_type: impl Into<String>, handler: F)
    where
        F: Fn(ActivityContext, Value) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Value>> + Send + 'static,
    {
        self.activities.insert(
            activity_type.into(),
            Arc::new(move |ctx, args| Box::pin(handler(ctx, args))),
        );
    }

    /// Register a named, read-only query handler for a workflow type.
    ///
    /// The workflow type must also be registered with [`Worker::register_workflow`]
    /// before the worker runs. The handler receives only an immutable committed
    /// state snapshot and normalized query arguments.
    pub fn register_query<F, Fut>(
        &mut self,
        workflow_type: impl Into<String>,
        query_name: impl Into<String>,
        handler: F,
    ) where
        F: Fn(QueryContext, Value) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Value>> + Send + 'static,
    {
        self.queries
            .entry(workflow_type.into())
            .or_default()
            .insert(
                query_name.into(),
                RegisteredQuery::Snapshot(Arc::new(move |ctx, args| Box::pin(handler(ctx, args)))),
            );
    }

    /// Register a named query against deterministically replayed instance state.
    ///
    /// The workflow type must use [`Worker::register_replayed_workflow`] with the
    /// same state type `S`. The handler receives an immutable, detached state
    /// clone, so successful and failed queries cannot affect workflow execution
    /// or the state reconstructed by a later query.
    pub fn register_replayed_query<S, F, Fut>(
        &mut self,
        workflow_type: impl Into<String>,
        query_name: impl Into<String>,
        handler: F,
    ) where
        S: Clone + Send + Sync + 'static,
        F: Fn(QueryContext, Arc<S>, Value) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Value>> + Send + 'static,
    {
        let handler = Arc::new(handler);
        let erased_handler: ReplayedQueryHandler = Arc::new(move |ctx, state, args| {
            let state = state.downcast::<S>().map_err(|_| {
                "registered query state type does not match the replayed workflow state".to_string()
            })?;
            Ok(Box::pin(handler(ctx, state, args)))
        });

        self.queries
            .entry(workflow_type.into())
            .or_default()
            .insert(
                query_name.into(),
                RegisteredQuery::Replayed {
                    state_type: TypeId::of::<S>(),
                    handler: erased_handler,
                },
            );
    }

    pub async fn register(&self) -> Result<RegisterWorkerResponse> {
        self.client
            .register_worker_with_capabilities(
                &self.worker_id,
                &self.task_queue,
                self.workflows.keys().cloned().collect(),
                self.activities.keys().cloned().collect(),
                self.max_concurrent_workflow_tasks,
                self.max_concurrent_activity_tasks,
                (!self.queries.is_empty())
                    .then(|| QUERY_TASKS_CAPABILITY.to_string())
                    .into_iter()
                    .collect(),
            )
            .await
    }

    /// Run until shutdown or a terminal worker error occurs.
    ///
    /// Empty long-poll expirations do not stop the worker. Retryable poll and
    /// heartbeat failures use [`WorkerRetryPolicy`] independently, while
    /// authentication, protocol, and other non-retryable failures are returned.
    pub async fn run(&self) -> Result<()> {
        self.run_until(std::future::pending::<()>()).await
    }

    /// Run until `shutdown` resolves or a terminal worker error occurs.
    ///
    /// This has the same liveness and terminal-error contract as [`Worker::run`].
    pub async fn run_until<F>(&self, shutdown: F) -> Result<()>
    where
        F: Future<Output = ()>,
    {
        let registration = self.register().await?;
        let mut heartbeat = tokio::time::interval(Duration::from_secs(
            registration
                .heartbeat_interval_seconds
                .unwrap_or(self.heartbeat_interval.as_secs().max(1)),
        ));
        tokio::pin!(shutdown);
        let stop = Arc::new(AtomicBool::new(false));
        // Poll responses may already have leased server-side work by the time
        // they become ready, so each poller owns its responses through
        // completion or failure instead of racing raw polls in this select.
        let mut workflow_poller = (!self.workflows.is_empty()).then(|| {
            let worker = self.clone();
            let stop = Arc::clone(&stop);
            tokio::spawn(async move { worker.poll_workflows_until_stopped(stop).await })
        });
        let mut activity_poller = (!self.activities.is_empty()).then(|| {
            let worker = self.clone();
            let stop = Arc::clone(&stop);
            tokio::spawn(async move { worker.poll_activities_until_stopped(stop).await })
        });
        let mut query_poller = (!self.queries.is_empty()).then(|| {
            let worker = self.clone();
            let stop = Arc::clone(&stop);
            tokio::spawn(async move { worker.poll_queries_until_stopped(stop).await })
        });

        loop {
            tokio::select! {
                _ = &mut shutdown => {
                    stop.store(true, Ordering::SeqCst);
                    break;
                }
                _ = heartbeat.tick() => {
                    match self.retry_worker_operation(|| {
                        self.client.heartbeat_worker(
                            &self.worker_id,
                            self.max_concurrent_workflow_tasks,
                            self.max_concurrent_activity_tasks,
                        )
                    }).await
                    {
                        Ok(acknowledgement) => {
                            if let Some(observer) = &self.heartbeat_observer {
                                observer(&WorkerHeartbeatObservation {
                                    worker_id: self.worker_id.clone(),
                                    task_queue: self.task_queue.clone(),
                                    acknowledged_at_unix_millis: SystemTime::now()
                                        .duration_since(UNIX_EPOCH)
                                        .unwrap_or_default()
                                        .as_millis()
                                        .min(u64::MAX as u128)
                                        as u64,
                                    acknowledgement,
                                });
                            }
                        }
                        Err(error) => {
                            stop.store(true, Ordering::SeqCst);
                            join_pollers(workflow_poller.take(), activity_poller.take(), query_poller.take()).await?;
                            return Err(error);
                        }
                    }
                }
                result = OptionFuture::from(workflow_poller.as_mut()), if workflow_poller.is_some() => {
                    workflow_poller = None;
                    stop.store(true, Ordering::SeqCst);
                    let poller_result = optional_poller_result("workflow", result);
                    let join_result =
                        join_pollers(workflow_poller.take(), activity_poller.take(), query_poller.take()).await;
                    poller_result?;
                    join_result?;
                    return Err(Error::WorkerLoop(
                        "workflow poller stopped unexpectedly".to_string(),
                    ));
                }
                result = OptionFuture::from(activity_poller.as_mut()), if activity_poller.is_some() => {
                    activity_poller = None;
                    stop.store(true, Ordering::SeqCst);
                    let poller_result = optional_poller_result("activity", result);
                    let join_result =
                        join_pollers(workflow_poller.take(), activity_poller.take(), query_poller.take()).await;
                    poller_result?;
                    join_result?;
                    return Err(Error::WorkerLoop(
                        "activity poller stopped unexpectedly".to_string(),
                    ));
                }
                result = OptionFuture::from(query_poller.as_mut()), if query_poller.is_some() => {
                    query_poller = None;
                    stop.store(true, Ordering::SeqCst);
                    let poller_result = optional_poller_result("query", result);
                    let join_result =
                        join_pollers(workflow_poller.take(), activity_poller.take(), query_poller.take()).await;
                    poller_result?;
                    join_result?;
                    return Err(Error::WorkerLoop(
                        "query poller stopped unexpectedly".to_string(),
                    ));
                }
            }
        }

        join_pollers(
            workflow_poller.take(),
            activity_poller.take(),
            query_poller.take(),
        )
        .await
    }

    pub async fn run_once(&self) -> Result<usize> {
        let mut handled = 0;
        if self.poll_workflow_once().await? {
            handled += 1;
        }
        if self.poll_activity_once().await? {
            handled += 1;
        }
        if !self.queries.is_empty() && self.poll_query_once().await? {
            handled += 1;
        }
        Ok(handled)
    }

    async fn poll_workflow_once(&self) -> Result<bool> {
        let Some(task) = self
            .retry_worker_operation(|| {
                self.client
                    .poll_workflow_task(&self.worker_id, &self.task_queue, self.poll_timeout)
            })
            .await?
        else {
            return Ok(false);
        };

        let task_id = task.task_id.clone();
        let attempt = task.workflow_task_attempt;
        let lease_owner = task
            .lease_owner
            .clone()
            .unwrap_or_else(|| self.worker_id.clone());

        match self.execute_workflow_task(task) {
            Ok(commands) => {
                self.client
                    .complete_workflow_task(&task_id, &lease_owner, attempt, commands)
                    .await?;
            }
            Err(error) => {
                self.client
                    .fail_workflow_task(&task_id, &lease_owner, attempt, error.to_string())
                    .await?;
            }
        }

        Ok(true)
    }

    async fn poll_workflows_until_stopped(self, stop: Arc<AtomicBool>) -> Result<()> {
        while !stop.load(Ordering::SeqCst) {
            self.poll_workflow_once().await?;
        }

        Ok(())
    }

    async fn poll_activity_once(&self) -> Result<bool> {
        let Some(task) = self
            .retry_worker_operation(|| {
                self.client
                    .poll_activity_task(&self.worker_id, &self.task_queue, self.poll_timeout)
            })
            .await?
        else {
            return Ok(false);
        };

        let task_id = task.task_id.clone();
        let attempt_id = task
            .activity_attempt_id
            .clone()
            .or(task.attempt_id.clone())
            .unwrap_or_default();
        let lease_owner = task
            .lease_owner
            .clone()
            .unwrap_or_else(|| self.worker_id.clone());
        let codec = task.payload_codec.clone();
        let result = self.execute_activity_task(task).await;
        match result {
            Ok(value) => {
                self.client
                    .complete_activity_task(&task_id, &attempt_id, &lease_owner, value, &codec)
                    .await?;
            }
            Err(error) => {
                self.client
                    .fail_activity_task(
                        &task_id,
                        &attempt_id,
                        &lease_owner,
                        error.to_string(),
                        false,
                    )
                    .await?;
            }
        }

        Ok(true)
    }

    async fn poll_activities_until_stopped(self, stop: Arc<AtomicBool>) -> Result<()> {
        while !stop.load(Ordering::SeqCst) {
            self.poll_activity_once().await?;
        }

        Ok(())
    }

    async fn poll_query_once(&self) -> Result<bool> {
        let Some(task) = self
            .retry_worker_operation(|| {
                self.client
                    .poll_query_task(&self.worker_id, &self.task_queue, self.poll_timeout)
            })
            .await?
        else {
            return Ok(false);
        };

        let query_task_id = task.query_task_id.clone();
        let attempt = task.query_task_attempt;
        let lease_owner = task
            .lease_owner
            .clone()
            .unwrap_or_else(|| self.worker_id.clone());
        let codec = task.payload_codec.clone();

        match self.execute_query_task(task).await {
            Ok(value) => {
                let result_envelope = match encode_value_envelope(&value, &codec) {
                    Ok(result_envelope) => result_envelope,
                    Err(error) => {
                        let failure = self
                            .client
                            .fail_query_task(
                                &query_task_id,
                                &lease_owner,
                                attempt,
                                error.to_string(),
                                "query_result_encode_failed",
                                "QueryResultEncodeFailed",
                            )
                            .await;
                        if let Err(error) = failure {
                            if !query_task_rejection_is_final(&error) {
                                return Err(error);
                            }
                        }
                        return Ok(true);
                    }
                };

                if let Err(error) = self
                    .client
                    .complete_query_task_with_envelope(
                        &query_task_id,
                        &lease_owner,
                        attempt,
                        value,
                        result_envelope,
                    )
                    .await
                {
                    if !query_task_rejection_is_final(&error) {
                        return Err(error);
                    }
                }
            }
            Err(failure) => {
                let result = self
                    .client
                    .fail_query_task(
                        &query_task_id,
                        &lease_owner,
                        attempt,
                        failure.message,
                        failure.reason,
                        failure.failure_type,
                    )
                    .await;
                if let Err(error) = result {
                    if !query_task_rejection_is_final(&error) {
                        return Err(error);
                    }
                }
            }
        }

        Ok(true)
    }

    async fn poll_queries_until_stopped(self, stop: Arc<AtomicBool>) -> Result<()> {
        while !stop.load(Ordering::SeqCst) {
            self.poll_query_once().await?;
        }

        Ok(())
    }

    async fn retry_worker_operation<T, F, Fut>(&self, mut operation: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let mut retries = 0;

        loop {
            match operation().await {
                Err(error)
                    if worker_operation_is_retryable(&error)
                        && retries < self.retry_policy.max_retries =>
                {
                    retries += 1;
                    tokio::time::sleep(worker_retry_delay(self.retry_policy, retries)).await;
                }
                result => return result,
            }
        }
    }

    async fn execute_query_task(
        &self,
        mut task: QueryTask,
    ) -> std::result::Result<Value, QueryTaskExecutionFailure> {
        if !matches!(task.payload_codec.as_str(), DEFAULT_CODEC | JSON_CODEC) {
            return Err(QueryTaskExecutionFailure::new(
                "query_payload_decode_failed",
                format!(
                    "cannot decode query payload with unsupported codec {:?}",
                    task.payload_codec
                ),
                "QueryPayloadDecodeFailed",
            ));
        }

        if !self.workflows.contains_key(&task.workflow_type) {
            return Err(QueryTaskExecutionFailure::new(
                "query_workflow_type_not_registered",
                format!("no workflow registered for type {:?}", task.workflow_type),
                "WorkflowTypeNotRegistered",
            ));
        }

        let Some(handlers) = self.queries.get(&task.workflow_type) else {
            return Err(QueryTaskExecutionFailure::new(
                "query_handler_unavailable",
                format!(
                    "query handlers are unavailable for workflow type {:?}",
                    task.workflow_type
                ),
                "QueryHandlerUnavailable",
            ));
        };
        let Some(query) = handlers.get(&task.query_name) else {
            return Err(QueryTaskExecutionFailure::new(
                "rejected_unknown_query",
                format!("unknown query {:?}", task.query_name),
                "QueryFailed",
            ));
        };

        let args = decode_task_arguments(task.query_arguments.as_ref(), &task.payload_codec)
            .map_err(|error| {
                QueryTaskExecutionFailure::new(
                    "query_payload_decode_failed",
                    format!("cannot decode query arguments: {error}"),
                    "QueryPayloadDecodeFailed",
                )
            })?;
        let workflow_input =
            decode_task_arguments(task.workflow_arguments.as_ref(), &task.payload_codec).map_err(
                |error| {
                    QueryTaskExecutionFailure::new(
                        "query_workflow_state_unavailable",
                        format!("cannot decode workflow start input: {error}"),
                        "QueryWorkflowStateUnavailable",
                    )
                },
            )?;
        hydrate_query_history_from_export(&mut task).map_err(|error| {
            QueryTaskExecutionFailure::new(
                "query_workflow_state_unavailable",
                format!("cannot restore query history snapshot: {error}"),
                "QueryWorkflowStateUnavailable",
            )
        })?;
        enrich_query_history_from_export(&mut task).map_err(|error| {
            QueryTaskExecutionFailure::new(
                "query_workflow_state_unavailable",
                format!("cannot restore compact query history payloads: {error}"),
                "QueryWorkflowStateUnavailable",
            )
        })?;
        let signal_events = query_signal_events(&task).map_err(|error| {
            QueryTaskExecutionFailure::new(
                "query_workflow_state_unavailable",
                format!("cannot decode committed workflow signals: {error}"),
                "QueryWorkflowStateUnavailable",
            )
        })?;
        let history_events = Arc::new(std::mem::take(&mut task.history_events));
        let context = QueryContext {
            workflow_id: task.workflow_id,
            run_id: task.run_id,
            workflow_type: task.workflow_type.clone(),
            run_status: task.run_status,
            workflow_input,
            history_events: Arc::clone(&history_events),
            signal_events: Arc::new(signal_events),
        };

        let future = match query {
            RegisteredQuery::Snapshot(handler) => handler(context, args),
            RegisteredQuery::Replayed {
                state_type,
                handler,
            } => {
                let workflow = self
                    .workflows
                    .get(&task.workflow_type)
                    .expect("workflow registration was checked above");
                if workflow.state_type != Some(*state_type) {
                    return Err(QueryTaskExecutionFailure::new(
                        "query_workflow_state_unavailable",
                        "replayed query state type does not match its workflow registration",
                        "QueryWorkflowStateUnavailable",
                    ));
                }
                let replay = workflow.replay.as_ref().ok_or_else(|| {
                    QueryTaskExecutionFailure::new(
                        "query_workflow_state_unavailable",
                        format!(
                            "workflow type {:?} is not registered for instance-state replay",
                            task.workflow_type
                        ),
                        "QueryWorkflowStateUnavailable",
                    )
                })?;
                let workflow_state = Arc::new(Mutex::new(WorkflowState {
                    history: history_events.as_ref().clone(),
                    task_queue: self.task_queue.clone(),
                    payload_codec: task.payload_codec,
                    resume_signal: None,
                    activity_cursor: 0,
                    signal_cursors: HashMap::new(),
                    commands: Vec::new(),
                }));
                let workflow_context = WorkflowContext {
                    state: workflow_state,
                };
                let mut invocation =
                    replay(workflow_context.clone(), context.workflow_input.clone());
                let mut cx = TaskContext::from_waker(noop_waker_ref());
                match invocation.future.as_mut().poll(&mut cx) {
                    Poll::Ready(Ok(_)) => {}
                    Poll::Ready(Err(error)) => {
                        return Err(QueryTaskExecutionFailure::new(
                            "query_workflow_state_unavailable",
                            format!("workflow replay failed before query: {error}"),
                            "QueryWorkflowStateUnavailable",
                        ));
                    }
                    Poll::Pending => {
                        let commands = workflow_context.take_commands().map_err(|error| {
                            QueryTaskExecutionFailure::new(
                                "query_workflow_state_unavailable",
                                format!("workflow replay failed before query: {error}"),
                                "QueryWorkflowStateUnavailable",
                            )
                        })?;
                        if commands.is_empty() {
                            return Err(QueryTaskExecutionFailure::new(
                                "query_workflow_state_unavailable",
                                "workflow replay yielded without a durable command",
                                "QueryWorkflowStateUnavailable",
                            ));
                        }
                    }
                }
                let state = (invocation.snapshot)().map_err(|error| {
                    QueryTaskExecutionFailure::new(
                        "query_workflow_state_unavailable",
                        format!("cannot snapshot replayed workflow state: {error}"),
                        "QueryWorkflowStateUnavailable",
                    )
                })?;
                handler(context, state, args).map_err(|message| {
                    QueryTaskExecutionFailure::new(
                        "query_workflow_state_unavailable",
                        message,
                        "QueryWorkflowStateUnavailable",
                    )
                })?
            }
        };

        future.await.map_err(|error| {
            QueryTaskExecutionFailure::new("query_rejected", error.to_string(), "QueryFailed")
        })
    }

    fn execute_workflow_task(&self, task: WorkflowTask) -> Result<Vec<Value>> {
        let workflow = self
            .workflows
            .get(&task.workflow_type)
            .ok_or_else(|| Error::WorkflowNotRegistered(task.workflow_type.clone()))?;
        let input = decode_task_arguments(task.arguments.as_ref(), &task.payload_codec)?;
        let resume_signal = decode_resume_signal(&task)?;
        let state = Arc::new(Mutex::new(WorkflowState {
            history: task.history_events,
            task_queue: self.task_queue.clone(),
            payload_codec: task.payload_codec.clone(),
            resume_signal,
            activity_cursor: 0,
            signal_cursors: HashMap::new(),
            commands: Vec::new(),
        }));
        let ctx = WorkflowContext { state };
        let mut future = (workflow.execute)(ctx.clone(), input);
        let mut cx = TaskContext::from_waker(noop_waker_ref());

        match future.as_mut().poll(&mut cx) {
            Poll::Ready(Ok(result)) => {
                let result = encode_value_envelope(&result, &task.payload_codec)?;
                Ok(vec![json!({
                    "type": "complete_workflow",
                    "result": result
                })])
            }
            Poll::Ready(Err(error)) => Err(error),
            Poll::Pending => {
                let commands = ctx.take_commands()?;
                if commands.is_empty() {
                    Err(Error::WorkflowYieldedWithoutCommand)
                } else {
                    Ok(commands)
                }
            }
        }
    }

    async fn execute_activity_task(&self, task: ActivityTask) -> Result<Value> {
        let handler = self
            .activities
            .get(&task.activity_type)
            .ok_or_else(|| Error::ActivityNotRegistered(task.activity_type.clone()))?;
        let args = decode_task_arguments(task.arguments.as_ref(), &task.payload_codec)?;
        let attempt_id = task
            .activity_attempt_id
            .clone()
            .or(task.attempt_id.clone())
            .unwrap_or_default();
        let lease_owner = task
            .lease_owner
            .clone()
            .unwrap_or_else(|| self.worker_id.clone());
        let ctx = ActivityContext {
            client: self.client.clone(),
            task_id: task.task_id,
            activity_attempt_id: attempt_id,
            lease_owner,
            activity_type: task.activity_type,
            attempt_number: task.attempt_number,
            task_queue: self.task_queue.clone(),
            worker_id: self.worker_id.clone(),
        };

        handler(ctx, args).await
    }
}

fn poller_result(
    kind: &str,
    result: std::result::Result<Result<()>, tokio::task::JoinError>,
) -> Result<()> {
    match result {
        Ok(result) => result,
        Err(error) => Err(Error::WorkerLoop(format!(
            "{kind} poller join error: {error}"
        ))),
    }
}

fn optional_poller_result(
    kind: &str,
    result: Option<std::result::Result<Result<()>, tokio::task::JoinError>>,
) -> Result<()> {
    match result {
        Some(result) => poller_result(kind, result),
        None => Ok(()),
    }
}

async fn join_pollers(
    workflow_poller: Option<tokio::task::JoinHandle<Result<()>>>,
    activity_poller: Option<tokio::task::JoinHandle<Result<()>>>,
    query_poller: Option<tokio::task::JoinHandle<Result<()>>>,
) -> Result<()> {
    let mut first_error = None;

    if let Some(handle) = workflow_poller {
        if let Err(error) = poller_result("workflow", handle.await) {
            first_error.get_or_insert(error);
        }
    }

    if let Some(handle) = activity_poller {
        if let Err(error) = poller_result("activity", handle.await) {
            first_error.get_or_insert(error);
        }
    }

    if let Some(handle) = query_poller {
        if let Err(error) = poller_result("query", handle.await) {
            first_error.get_or_insert(error);
        }
    }

    if let Some(error) = first_error {
        Err(error)
    } else {
        Ok(())
    }
}

fn default_worker_id() -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    format!("rust-worker-{}-{millis}", std::process::id())
}

fn unique_request_id(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{prefix}-{}-{nanos}", std::process::id())
}

#[derive(Debug)]
struct QueryTaskExecutionFailure {
    reason: String,
    message: String,
    failure_type: String,
}

impl QueryTaskExecutionFailure {
    fn new(
        reason: impl Into<String>,
        message: impl Into<String>,
        failure_type: impl Into<String>,
    ) -> Self {
        Self {
            reason: reason.into(),
            message: message.into(),
            failure_type: failure_type.into(),
        }
    }
}

/// Typed local state owned by one deterministic workflow invocation.
///
/// Use [`WorkflowInstance::update`] for the same state transitions during
/// ordinary execution and replay. A replayed query receives a detached
/// immutable `Arc<S>` rather than this mutation-capable handle.
#[derive(Clone, Debug)]
pub struct WorkflowInstance<S> {
    state: Arc<Mutex<S>>,
}

impl<S> WorkflowInstance<S> {
    fn new(state: S) -> Self {
        Self {
            state: Arc::new(Mutex::new(state)),
        }
    }

    /// Read the current workflow-instance state without changing it.
    pub fn read<R>(&self, reader: impl FnOnce(&S) -> R) -> Result<R> {
        let state = self
            .state
            .lock()
            .map_err(|_| Error::WorkflowStatePoisoned)?;
        Ok(reader(&state))
    }

    /// Apply one deterministic workflow-instance state transition.
    pub fn update<R>(&self, transition: impl FnOnce(&mut S) -> R) -> Result<R> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::WorkflowStatePoisoned)?;
        Ok(transition(&mut state))
    }
}

impl<S: Clone> WorkflowInstance<S> {
    fn snapshot(&self) -> Result<S> {
        self.read(Clone::clone)
    }
}

#[derive(Clone, Debug)]
pub struct WorkflowContext {
    state: Arc<Mutex<WorkflowState>>,
}

impl WorkflowContext {
    pub fn activity<T: Serialize>(
        &self,
        activity_type: impl Into<String>,
        args: T,
    ) -> ActivityCall {
        self.activity_on_queue(activity_type, None::<String>, args)
    }

    pub fn activity_on_queue<T, Q>(
        &self,
        activity_type: impl Into<String>,
        task_queue: Option<Q>,
        args: T,
    ) -> ActivityCall
    where
        T: Serialize,
        Q: Into<String>,
    {
        ActivityCall {
            ctx: self.clone(),
            activity_type: activity_type.into(),
            task_queue: task_queue.map(Into::into),
            args: Some(serde_json::to_value(args).map_err(Error::from)),
            scheduled: false,
        }
    }

    pub fn wait_signal(&self, signal_name: impl Into<String>) -> SignalCall {
        SignalCall {
            ctx: self.clone(),
            signal_name: signal_name.into(),
            opened_wait: false,
        }
    }

    fn take_commands(&self) -> Result<Vec<Value>> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::WorkflowStatePoisoned)?;
        Ok(std::mem::take(&mut state.commands))
    }
}

#[derive(Debug)]
struct WorkflowState {
    history: Vec<HistoryEvent>,
    task_queue: String,
    payload_codec: String,
    resume_signal: Option<ResumeSignal>,
    activity_cursor: usize,
    signal_cursors: HashMap<String, usize>,
    commands: Vec<Value>,
}

#[derive(Clone, Debug)]
struct ResumeSignal {
    signal_id: Option<String>,
    signal_name: String,
    arguments: Vec<Value>,
}

pub struct ActivityCall {
    ctx: WorkflowContext,
    activity_type: String,
    task_queue: Option<String>,
    args: Option<Result<Value>>,
    scheduled: bool,
}

impl Future for ActivityCall {
    type Output = Result<Value>;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
        let ctx = self.ctx.clone();
        let mut state = match ctx.state.lock() {
            Ok(state) => state,
            Err(_) => return Poll::Ready(Err(Error::WorkflowStatePoisoned)),
        };

        let completed = match completed_activity_results(&state.history, &state.payload_codec) {
            Ok(completed) => completed,
            Err(error) => return Poll::Ready(Err(error)),
        };

        if state.activity_cursor < completed.len() {
            let value = completed[state.activity_cursor].clone();
            state.activity_cursor += 1;
            return Poll::Ready(Ok(value));
        }

        if !self.scheduled {
            let task_queue = self
                .task_queue
                .clone()
                .unwrap_or_else(|| state.task_queue.clone());
            let args = match self.args.take().unwrap_or(Ok(Value::Null)) {
                Ok(args) => args,
                Err(error) => return Poll::Ready(Err(error)),
            };
            let arguments = normalize_arguments(args);
            let envelope = match encode_value_envelope(&arguments, &state.payload_codec) {
                Ok(envelope) => envelope,
                Err(error) => return Poll::Ready(Err(error)),
            };

            state.commands.push(json!({
                "type": "schedule_activity",
                "activity_type": self.activity_type.clone(),
                "queue": task_queue,
                "arguments": envelope
            }));
            self.scheduled = true;
        }

        Poll::Pending
    }
}

pub struct SignalCall {
    ctx: WorkflowContext,
    signal_name: String,
    opened_wait: bool,
}

impl Future for SignalCall {
    type Output = Result<Vec<Value>>;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
        let ctx = self.ctx.clone();
        let mut state = match ctx.state.lock() {
            Ok(state) => state,
            Err(_) => return Poll::Ready(Err(Error::WorkflowStatePoisoned)),
        };

        let signals = match signal_values(
            &state.history,
            &self.signal_name,
            &state.payload_codec,
            state.resume_signal.as_ref(),
        ) {
            Ok(signals) => signals,
            Err(error) => return Poll::Ready(Err(error)),
        };
        let cursor = *state.signal_cursors.get(&self.signal_name).unwrap_or(&0);

        if cursor < signals.len() {
            state
                .signal_cursors
                .insert(self.signal_name.clone(), cursor + 1);
            return Poll::Ready(Ok(signals[cursor].clone()));
        }

        if !self.opened_wait {
            state.commands.push(json!({
                "type": "open_condition_wait",
                "condition_key": format!("signal:{}", self.signal_name)
            }));
            self.opened_wait = true;
        }

        Poll::Pending
    }
}

#[derive(Clone, Debug)]
pub struct ActivityContext {
    client: Client,
    pub task_id: String,
    pub activity_attempt_id: String,
    pub lease_owner: String,
    pub activity_type: String,
    pub attempt_number: u64,
    pub task_queue: String,
    pub worker_id: String,
}

impl ActivityContext {
    pub async fn heartbeat<T: Serialize>(&self, details: T) -> Result<ActivityHeartbeatResponse> {
        self.client
            .heartbeat_activity_task(
                &self.task_id,
                &self.activity_attempt_id,
                &self.lease_owner,
                serde_json::to_value(details)?,
            )
            .await
    }
}

fn decode_task_arguments(value: Option<&Value>, codec: &str) -> Result<Value> {
    match value {
        Some(value) => Ok(normalize_arguments(decode_wire_value(value, codec)?)),
        None => Ok(Value::Array(Vec::new())),
    }
}

fn decode_resume_signal(task: &WorkflowTask) -> Result<Option<ResumeSignal>> {
    let Some(signal_name) = task
        .signal_name
        .as_deref()
        .filter(|value| !value.is_empty())
    else {
        return Ok(None);
    };
    let Some(arguments) = task.signal_arguments.as_ref() else {
        return Ok(None);
    };

    let decoded = normalize_arguments(decode_wire_value(arguments, &task.payload_codec)?);
    let Value::Array(arguments) = decoded else {
        unreachable!("normalize_arguments always returns an array");
    };

    Ok(Some(ResumeSignal {
        signal_id: task.workflow_signal_id.clone(),
        signal_name: signal_name.to_string(),
        arguments,
    }))
}

fn normalize_arguments(value: Value) -> Value {
    match value {
        Value::Null => Value::Array(Vec::new()),
        Value::Array(_) => value,
        other => Value::Array(vec![other]),
    }
}

fn completed_activity_results(events: &[HistoryEvent], fallback_codec: &str) -> Result<Vec<Value>> {
    let mut results = Vec::new();

    for event in events {
        if event.event_type != "ActivityCompleted" {
            continue;
        }

        let codec = event
            .payload
            .get("payload_codec")
            .and_then(Value::as_str)
            .unwrap_or(fallback_codec);
        let result = event.payload.get("result").unwrap_or(&Value::Null);
        results.push(decode_wire_value(result, codec)?);
    }

    Ok(results)
}

fn signal_values(
    events: &[HistoryEvent],
    signal_name: &str,
    fallback_codec: &str,
    resume_signal: Option<&ResumeSignal>,
) -> Result<Vec<Vec<Value>>> {
    let mut signals = Vec::new();

    for event in events {
        if event.event_type != "SignalApplied" && event.event_type != "SignalReceived" {
            continue;
        }

        if event.payload.get("signal_name").and_then(Value::as_str) != Some(signal_name) {
            continue;
        }

        let codec = event
            .payload
            .get("payload_codec")
            .and_then(Value::as_str)
            .unwrap_or(fallback_codec);
        let raw = event
            .payload
            .get("value")
            .or_else(|| event.payload.get("input"))
            .or_else(|| event.payload.get("arguments"));
        let decoded = match raw.filter(|value| !value.is_null()) {
            Some(value) => decode_wire_value(value, codec)?,
            None => resume_signal
                .filter(|signal| resume_signal_matches_event(signal, event, signal_name))
                .map(|signal| Value::Array(signal.arguments.clone()))
                .unwrap_or_else(|| Value::Array(Vec::new())),
        };
        let args = match normalize_arguments(decoded) {
            Value::Array(values) => values,
            _ => unreachable!("normalize_arguments always returns an array"),
        };
        signals.push(args);
    }

    Ok(signals)
}

fn hydrate_query_history_from_export(task: &mut QueryTask) -> Result<()> {
    let Some(export_events) = task
        .history_export
        .as_ref()
        .and_then(|export| export.get("history_events"))
        .and_then(Value::as_array)
    else {
        return Ok(());
    };

    if export_events.len() > task.history_events.len() {
        task.history_events = serde_json::from_value(Value::Array(export_events.clone()))?;
    }

    Ok(())
}

fn enrich_query_history_from_export(task: &mut QueryTask) -> Result<()> {
    let Some(export) = task.history_export.as_ref() else {
        return Ok(());
    };
    let signals = export
        .get("signals")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let activities = export
        .get("activities")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let export_codec = export
        .get("payloads")
        .and_then(|payloads| payloads.get("codec"))
        .and_then(Value::as_str)
        .unwrap_or(&task.payload_codec)
        .to_string();
    let mut signal_name_offsets: HashMap<String, usize> = HashMap::new();

    for event in &mut task.history_events {
        if event.event_type == "ActivityCompleted" {
            let sequence = event
                .payload
                .get("sequence")
                .or_else(|| event.payload.get("workflow_sequence"))
                .and_then(value_as_u64);
            let Some(activity) = sequence.and_then(|sequence| {
                activities.iter().find(|activity| {
                    activity.get("sequence").and_then(value_as_u64) == Some(sequence)
                })
            }) else {
                continue;
            };
            let Some(payload) = event.payload.as_object_mut() else {
                continue;
            };
            if missing_payload(payload.get("result")) {
                if let Some(result) = activity
                    .get("result")
                    .filter(|value| !missing_payload(Some(value)))
                {
                    payload.insert("result".to_string(), result.clone());
                }
            }
            for field in ["payload_codec", "activity_type"] {
                if payload
                    .get(field)
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .is_empty()
                {
                    if let Some(value) = activity.get(field) {
                        payload.insert(field.to_string(), value.clone());
                    }
                }
            }
            continue;
        }

        if event.event_type != "SignalReceived" && event.event_type != "SignalApplied" {
            continue;
        }
        let signal_id = event.payload.get("signal_id").and_then(Value::as_str);
        let command_id = event
            .payload
            .get("workflow_command_id")
            .or_else(|| event.raw.get("workflow_command_id"))
            .and_then(Value::as_str);
        let signal_name = event
            .payload
            .get("signal_name")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let matched = signals
            .iter()
            .find(|signal| {
                signal_id.is_some() && signal.get("id").and_then(Value::as_str) == signal_id
            })
            .or_else(|| {
                signals.iter().find(|signal| {
                    command_id.is_some()
                        && signal.get("command_id").and_then(Value::as_str) == command_id
                })
            })
            .or_else(|| {
                let offset = signal_name_offsets.entry(signal_name.clone()).or_default();
                let signal = signals
                    .iter()
                    .filter(|signal| {
                        signal.get("name").and_then(Value::as_str) == Some(signal_name.as_str())
                    })
                    .nth(*offset);
                if signal.is_some() {
                    *offset += 1;
                }
                signal
            });
        let Some(signal) = matched else {
            continue;
        };
        let signal_codec = signal
            .get("payload_codec")
            .and_then(Value::as_str)
            .unwrap_or(&export_codec);
        let Some(payload) = event.payload.as_object_mut() else {
            continue;
        };
        if missing_payload(payload.get("arguments")) {
            if let Some(arguments) = signal
                .get("arguments")
                .filter(|value| !missing_payload(Some(value)))
            {
                let envelope = match arguments {
                    Value::String(blob) => json!({"codec": signal_codec, "blob": blob}),
                    other => other.clone(),
                };
                payload.insert("arguments".to_string(), envelope);
            }
        }
        if payload
            .get("payload_codec")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .is_empty()
        {
            payload.insert("payload_codec".to_string(), json!(signal_codec));
        }
    }

    Ok(())
}

fn missing_payload(value: Option<&Value>) -> bool {
    match value {
        None | Some(Value::Null) => true,
        Some(Value::String(value)) => value.is_empty(),
        Some(_) => false,
    }
}

fn query_signal_events(task: &QueryTask) -> Result<Vec<QuerySignal>> {
    let export_signals = task
        .history_export
        .as_ref()
        .and_then(|export| export.get("signals"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let export_codec = task
        .history_export
        .as_ref()
        .and_then(|export| export.get("payloads"))
        .and_then(|payloads| payloads.get("codec"))
        .and_then(Value::as_str)
        .unwrap_or(&task.payload_codec);
    let mut name_offsets: HashMap<String, usize> = HashMap::new();
    let mut signals = Vec::new();

    for event in &task.history_events {
        if event.event_type != "SignalApplied" && event.event_type != "SignalReceived" {
            continue;
        }

        let name = event
            .payload
            .get("signal_name")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if name.is_empty() {
            continue;
        }
        let signal_id = event.payload.get("signal_id").and_then(Value::as_str);
        let command_id = event
            .payload
            .get("workflow_command_id")
            .or_else(|| event.raw.get("workflow_command_id"))
            .and_then(Value::as_str);
        let matched_export = export_signals
            .iter()
            .find(|candidate| {
                signal_id.is_some() && candidate.get("id").and_then(Value::as_str) == signal_id
            })
            .or_else(|| {
                export_signals.iter().find(|candidate| {
                    command_id.is_some()
                        && candidate.get("command_id").and_then(Value::as_str) == command_id
                })
            })
            .or_else(|| {
                let offset = name_offsets.entry(name.to_string()).or_default();
                let candidate = export_signals
                    .iter()
                    .filter(|candidate| candidate.get("name").and_then(Value::as_str) == Some(name))
                    .nth(*offset);
                if candidate.is_some() {
                    *offset += 1;
                }
                candidate
            });
        let codec = event
            .payload
            .get("payload_codec")
            .and_then(Value::as_str)
            .or_else(|| {
                matched_export
                    .and_then(|signal| signal.get("payload_codec"))
                    .and_then(Value::as_str)
            })
            .unwrap_or(export_codec);
        let raw_arguments = event
            .payload
            .get("value")
            .or_else(|| event.payload.get("input"))
            .or_else(|| event.payload.get("arguments"))
            .filter(|value| !value.is_null())
            .or_else(|| matched_export.and_then(|signal| signal.get("arguments")));
        let arguments = decode_query_signal_arguments(raw_arguments, codec)?;
        let workflow_sequence = event
            .payload
            .get("workflow_sequence")
            .and_then(value_as_u64)
            .or_else(|| {
                matched_export
                    .and_then(|signal| signal.get("workflow_sequence"))
                    .and_then(value_as_u64)
            });

        signals.push(QuerySignal {
            id: signal_id.map(str::to_string).or_else(|| {
                matched_export
                    .and_then(|signal| signal.get("id"))
                    .and_then(Value::as_str)
                    .map(str::to_string)
            }),
            name: name.to_string(),
            arguments,
            workflow_sequence,
        });
    }

    if signals.is_empty() {
        for signal in export_signals {
            if signal.get("status").and_then(Value::as_str) == Some("rejected") {
                continue;
            }
            let Some(name) = signal.get("name").and_then(Value::as_str) else {
                continue;
            };
            let codec = signal
                .get("payload_codec")
                .and_then(Value::as_str)
                .unwrap_or(export_codec);
            let arguments = decode_query_signal_arguments(signal.get("arguments"), codec)?;
            signals.push(QuerySignal {
                id: signal.get("id").and_then(Value::as_str).map(str::to_string),
                name: name.to_string(),
                arguments,
                workflow_sequence: signal.get("workflow_sequence").and_then(value_as_u64),
            });
        }
        signals.sort_by_key(|signal| signal.workflow_sequence.unwrap_or(u64::MAX));
    }

    Ok(signals)
}

fn decode_query_signal_arguments(raw: Option<&Value>, codec: &str) -> Result<Vec<Value>> {
    let decoded = match raw.filter(|value| !value.is_null()) {
        Some(value) => decode_wire_value(value, codec)?,
        None => Value::Array(Vec::new()),
    };
    let Value::Array(arguments) = normalize_arguments(decoded) else {
        unreachable!("normalize_arguments always returns an array");
    };
    Ok(arguments)
}

fn value_as_u64(value: &Value) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
}

fn resume_signal_matches_event(
    resume_signal: &ResumeSignal,
    event: &HistoryEvent,
    signal_name: &str,
) -> bool {
    if resume_signal.signal_name != signal_name {
        return false;
    }

    match (
        resume_signal.signal_id.as_deref(),
        event.payload.get("signal_id").and_then(Value::as_str),
    ) {
        (Some(resume_id), Some(event_id)) => resume_id == event_id,
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        io::{Read, Write},
        net::{SocketAddr, TcpListener, TcpStream},
        thread,
    };

    #[derive(Clone, Debug, Default, PartialEq)]
    struct ReplayCounterState {
        loaded: Option<String>,
        count: i64,
        finished: bool,
    }

    fn replay_counter_worker() -> Worker {
        let client = Client::new("http://127.0.0.1:8080").expect("client");
        let mut worker = Worker::new(client, "rust-workers");
        worker.register_replayed_workflow(
            "replay-counter",
            ReplayCounterState::default,
            |ctx, _input, state| async move {
                let loaded = ctx.activity("load-counter", json!([])).await?;
                state.update(|current| {
                    current.loaded = loaded.as_str().map(str::to_string);
                })?;
                for _ in 0..2 {
                    let signal = ctx.wait_signal("increment").await?;
                    let amount = signal.first().and_then(Value::as_i64).unwrap_or_default();
                    state.update(|current| current.count += amount)?;
                }
                state.update(|current| current.finished = true)?;
                state.read(|current| Ok(json!(current.count)))?
            },
        );
        worker.register_replayed_query::<ReplayCounterState, _, _>(
            "replay-counter",
            "current",
            |_ctx, state, _args| async move {
                Ok(json!({
                    "loaded": state.loaded,
                    "count": state.count,
                    "finished": state.finished,
                }))
            },
        );
        worker.register_replayed_query::<ReplayCounterState, _, _>(
            "replay-counter",
            "detached-mutation",
            |_ctx, state, _args| async move {
                let mut detached = (*state).clone();
                detached.count = 999;
                Ok(json!(detached.count))
            },
        );
        worker.register_replayed_query::<ReplayCounterState, _, _>(
            "replay-counter",
            "failed-mutation",
            |_ctx, state, _args| async move {
                let mut detached = (*state).clone();
                detached.count = 999;
                Err(Error::WorkerLoop("query refused".to_string()))
            },
        );
        worker
    }

    fn replay_counter_query(
        query_name: &str,
        history_events: Value,
        run_status: &str,
    ) -> QueryTask {
        serde_json::from_value(json!({
            "query_task_id": format!("query-{query_name}"),
            "workflow_type": "replay-counter",
            "query_name": query_name,
            "payload_codec": "json",
            "workflow_arguments": {"codec": "json", "blob": "[]"},
            "query_arguments": {"codec": "json", "blob": "[]"},
            "history_events": history_events,
            "run_status": run_status,
        }))
        .expect("query task")
    }

    #[test]
    fn avro_generic_wrapper_round_trips_json_values() {
        let value = json!({"greeting": "hello", "count": 3, "ok": true});
        let envelope = PayloadEnvelope::avro(&value).expect("encode");
        assert_eq!(envelope.codec, DEFAULT_CODEC);
        assert_eq!(decode_payload::<Value>(&envelope).expect("decode"), value);
    }

    #[test]
    fn json_codec_remains_plain_json() {
        let value = json!({"greeting": "hello", "count": 3, "ok": true});
        let envelope = PayloadEnvelope::json(&value).expect("encode");

        assert_eq!(envelope.codec, JSON_CODEC);
        assert_eq!(envelope.blob, serde_json::to_string(&value).expect("json"));
        assert_eq!(decode_payload::<Value>(&envelope).expect("decode"), value);
    }

    #[test]
    fn typed_avro_payload_without_schema_context_keeps_diagnostic() {
        let envelope = PayloadEnvelope {
            codec: DEFAULT_CODEC.to_string(),
            blob: BASE64.encode([0x01]),
        };

        let error = decode_payload::<Value>(&envelope).expect_err("typed payload must fail");
        assert_eq!(
            error.to_string(),
            "codec error: typed avro payloads require a schema context; v1 supports the generic wrapper"
        );
    }

    #[test]
    fn workflow_context_schedules_activity_until_completion_is_in_history() {
        let ctx = WorkflowContext {
            state: Arc::new(Mutex::new(WorkflowState {
                history: Vec::new(),
                task_queue: "rust-workers".to_string(),
                payload_codec: DEFAULT_CODEC.to_string(),
                resume_signal: None,
                activity_cursor: 0,
                signal_cursors: HashMap::new(),
                commands: Vec::new(),
            })),
        };

        let mut call = Box::pin(ctx.activity("hello.activity", json!(["Ada"])));
        let mut task_context = TaskContext::from_waker(noop_waker_ref());
        assert!(matches!(
            call.as_mut().poll(&mut task_context),
            Poll::Pending
        ));

        let commands = ctx.take_commands().expect("commands");
        assert_eq!(commands[0]["type"], "schedule_activity");
        assert_eq!(commands[0]["activity_type"], "hello.activity");
    }

    #[test]
    fn rust_hello_world_uses_signal_arguments_from_resume_payload() {
        let client = Client::new("http://127.0.0.1:8080").expect("client");
        let mut worker = Worker::new(client, "rust-workers");

        worker.register_workflow("rust.hello_workflow", |ctx, _input| async move {
            let signal = ctx.wait_signal("start").await?;
            let name = signal
                .first()
                .and_then(|value| value.as_str())
                .unwrap_or("world");
            let greeting = ctx.activity("rust.hello_activity", json!([name])).await?;
            Ok(json!({
                "greeting": greeting,
                "language": "rust"
            }))
        });

        let signal_arguments =
            encode_value_envelope(&json!(["Rust"]), DEFAULT_CODEC).expect("signal arguments");
        let task = WorkflowTask {
            task_id: "wft-rust-signal-1".to_string(),
            workflow_id: Some("wf-rust-hello".to_string()),
            run_id: Some("run-rust-hello".to_string()),
            workflow_type: "rust.hello_workflow".to_string(),
            payload_codec: DEFAULT_CODEC.to_string(),
            arguments: Some(encode_value_envelope(&json!([]), DEFAULT_CODEC).expect("input")),
            history_events: vec![HistoryEvent {
                event_type: "SignalReceived".to_string(),
                payload: json!({
                    "signal_id": "sig-rust-1",
                    "signal_name": "start"
                }),
                raw: HashMap::new(),
            }],
            total_history_events: Some(1),
            next_history_page_token: None,
            workflow_task_attempt: 1,
            workflow_signal_id: Some("sig-rust-1".to_string()),
            signal_name: Some("start".to_string()),
            signal_arguments: Some(signal_arguments),
            lease_owner: Some("rust-worker".to_string()),
        };

        let commands = worker.execute_workflow_task(task).expect("workflow task");

        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0]["type"], "schedule_activity");
        assert_eq!(commands[0]["activity_type"], "rust.hello_activity");
        assert_eq!(
            decode_wire_value(&commands[0]["arguments"], DEFAULT_CODEC).expect("activity args"),
            json!(["Rust"])
        );
    }

    #[test]
    fn workflow_task_appends_paginated_history_events() {
        let mut task = WorkflowTask {
            task_id: "wft-rust-pages-1".to_string(),
            workflow_id: Some("wf-rust-pages".to_string()),
            run_id: Some("run-rust-pages".to_string()),
            workflow_type: "rust.hello_workflow".to_string(),
            payload_codec: DEFAULT_CODEC.to_string(),
            arguments: Some(encode_value_envelope(&json!([]), DEFAULT_CODEC).expect("input")),
            history_events: vec![HistoryEvent {
                event_type: "WorkflowStarted".to_string(),
                payload: json!({}),
                raw: HashMap::new(),
            }],
            total_history_events: Some(3),
            next_history_page_token: Some("MQ==".to_string()),
            workflow_task_attempt: 1,
            workflow_signal_id: None,
            signal_name: None,
            signal_arguments: None,
            lease_owner: Some("rust-worker".to_string()),
        };

        task.append_history_page(WorkflowTaskHistoryPage {
            history_events: vec![
                HistoryEvent {
                    event_type: "SignalReceived".to_string(),
                    payload: json!({
                        "signal_id": "sig-rust-1",
                        "signal_name": "start",
                        "arguments": encode_value_envelope(&json!(["Rust"]), DEFAULT_CODEC)
                            .expect("signal arguments")
                    }),
                    raw: HashMap::new(),
                },
                HistoryEvent {
                    event_type: "MarkerRecorded".to_string(),
                    payload: json!({"sequence": 3}),
                    raw: HashMap::new(),
                },
            ],
            total_history_events: Some(3),
            next_history_page_token: None,
        });

        assert_eq!(task.history_events.len(), 3);
        assert_eq!(task.total_history_events, Some(3));
        assert_eq!(task.next_history_page_token, None);

        let signals =
            signal_values(&task.history_events, "start", DEFAULT_CODEC, None).expect("signals");
        assert_eq!(signals, vec![vec![json!("Rust")]]);
    }

    #[tokio::test]
    async fn query_handler_reads_ordered_cross_codec_signals_without_commands() {
        let client = Client::new("http://127.0.0.1:8080").expect("client");
        let mut worker = Worker::new(client, "rust-workers");
        worker.register_workflow("counter", |_ctx, _input| async move { Ok(Value::Null) });
        worker.register_query("counter", "current", |ctx, _args| async move {
            let mut count = 0_i64;
            for signal in ctx.signal_events() {
                let value = signal
                    .arguments
                    .first()
                    .and_then(Value::as_i64)
                    .unwrap_or_default();
                match signal.name.as_str() {
                    "increment" => count += value,
                    "set" => count = value,
                    _ => {}
                }
            }
            Ok(json!(count))
        });

        let task = QueryTask {
            query_task_id: "query-rust-counter".to_string(),
            query_task_attempt: 1,
            lease_owner: Some("rust-worker".to_string()),
            workflow_id: Some("counter-1".to_string()),
            run_id: Some("run-counter-1".to_string()),
            workflow_type: "counter".to_string(),
            query_name: "current".to_string(),
            payload_codec: DEFAULT_CODEC.to_string(),
            workflow_arguments: Some(
                encode_value_envelope(&json!([]), DEFAULT_CODEC).expect("workflow input"),
            ),
            query_arguments: Some(
                encode_value_envelope(&json!([]), DEFAULT_CODEC).expect("query arguments"),
            ),
            history_events: vec![
                HistoryEvent {
                    event_type: "SignalReceived".to_string(),
                    payload: json!({
                        "signal_id": "php-signal-1",
                        "signal_name": "increment",
                        "workflow_sequence": 1,
                        "payload_codec": DEFAULT_CODEC,
                        "arguments": encode_value_envelope(&json!([3]), DEFAULT_CODEC).expect("php avro signal")
                    }),
                    raw: HashMap::new(),
                },
                HistoryEvent {
                    event_type: "SignalReceived".to_string(),
                    payload: json!({
                        "signal_id": "python-signal-2",
                        "signal_name": "increment",
                        "workflow_sequence": 2,
                        "payload_codec": JSON_CODEC,
                        "arguments": encode_value_envelope(&json!([5]), JSON_CODEC).expect("python json signal")
                    }),
                    raw: HashMap::new(),
                },
                HistoryEvent {
                    event_type: "SignalReceived".to_string(),
                    payload: json!({
                        "signal_id": "rust-signal-3",
                        "signal_name": "set",
                        "workflow_sequence": 3,
                        "payload_codec": DEFAULT_CODEC,
                        "arguments": encode_value_envelope(&json!([0]), DEFAULT_CODEC).expect("rust avro signal")
                    }),
                    raw: HashMap::new(),
                },
            ],
            history_export: None,
            run_status: Some("completed".to_string()),
        };

        let result = worker.execute_query_task(task).await.expect("query result");
        assert_eq!(result, json!(0));
    }

    #[tokio::test]
    async fn replayed_queries_read_running_completed_and_cold_restarted_instance_state() {
        let worker = replay_counter_worker();
        let running_history = json!([
            {
                "type": "ActivityCompleted",
                "payload": {
                    "sequence": 1,
                    "activity_type": "load-counter",
                    "payload_codec": "json",
                    "result": {"codec": "json", "blob": "\"loaded\""}
                }
            },
            {
                "type": "SignalReceived",
                "payload": {
                    "signal_id": "signal-3",
                    "signal_name": "increment",
                    "payload_codec": "json",
                    "arguments": {"codec": "json", "blob": "[3]"}
                }
            }
        ]);

        let running = worker
            .execute_query_task(replay_counter_query(
                "current",
                running_history.clone(),
                "running",
            ))
            .await
            .expect("running replay query");
        assert_eq!(
            running,
            json!({"loaded": "loaded", "count": 3, "finished": false})
        );

        let detached = worker
            .execute_query_task(replay_counter_query(
                "detached-mutation",
                running_history.clone(),
                "running",
            ))
            .await
            .expect("query mutates only its detached state clone");
        assert_eq!(detached, json!(999));
        let failed = worker
            .execute_query_task(replay_counter_query(
                "failed-mutation",
                running_history.clone(),
                "running",
            ))
            .await
            .expect_err("failed query");
        assert_eq!(failed.reason, "query_rejected");
        let unchanged = worker
            .execute_query_task(replay_counter_query("current", running_history, "running"))
            .await
            .expect("later query reconstructs unchanged state");
        assert_eq!(unchanged, running);

        let restarted_worker = replay_counter_worker();
        let restarted_task: QueryTask = serde_json::from_value(json!({
            "query_task_id": "query-after-restart",
            "workflow_id": "counter-1",
            "run_id": "run-counter-1",
            "workflow_type": "replay-counter",
            "query_name": "current",
            "payload_codec": "json",
            "workflow_arguments": {"codec": "json", "blob": "[]"},
            "query_arguments": {"codec": "json", "blob": "[]"},
            "history_events": [],
            "history_export": {
                "payloads": {"codec": "json"},
                "history_events": [
                    {
                        "type": "ActivityCompleted",
                        "payload": {
                            "sequence": 1,
                            "activity_type": "load-counter",
                            "payload_codec": "json",
                            "result": null
                        }
                    },
                    {
                        "type": "SignalReceived",
                        "payload": {"signal_id": "signal-3", "signal_name": "increment"}
                    },
                    {
                        "type": "SignalReceived",
                        "payload": {"signal_id": "signal-5", "signal_name": "increment"}
                    }
                ],
                "activities": [{
                    "sequence": 1,
                    "activity_type": "load-counter",
                    "payload_codec": "json",
                    "result": {"codec": "json", "blob": "\"loaded\""}
                }],
                "signals": [
                    {
                        "id": "signal-3",
                        "name": "increment",
                        "payload_codec": "json",
                        "arguments": "[3]"
                    },
                    {
                        "id": "signal-5",
                        "name": "increment",
                        "payload_codec": "json",
                        "arguments": "[5]"
                    }
                ]
            },
            "run_status": "completed"
        }))
        .expect("cold replay query task");
        let completed = restarted_worker
            .execute_query_task(restarted_task)
            .await
            .expect("completed cold replay query");
        assert_eq!(
            completed,
            json!({"loaded": "loaded", "count": 8, "finished": true})
        );
    }

    #[tokio::test]
    async fn replayed_query_replay_failures_are_machine_readable() {
        let worker = replay_counter_worker();
        let task = replay_counter_query(
            "current",
            json!([{
                "type": "ActivityCompleted",
                "payload": {
                    "sequence": 1,
                    "payload_codec": "json",
                    "result": {"codec": "json", "blob": "{"}
                }
            }]),
            "running",
        );
        let failure = worker
            .execute_query_task(task)
            .await
            .expect_err("invalid replay history payload");
        assert_eq!(failure.reason, "query_workflow_state_unavailable");
        assert_eq!(failure.failure_type, "QueryWorkflowStateUnavailable");
    }

    #[tokio::test]
    async fn query_task_restores_compact_history_from_export() {
        let client = Client::new("http://127.0.0.1:8080").expect("client");
        let mut worker = Worker::new(client, "rust-workers");
        worker.register_workflow("counter", |_ctx, _input| async move { Ok(Value::Null) });
        worker.register_query("counter", "current", |ctx, _args| async move {
            Ok(json!(ctx.signals("increment")[0][0]))
        });
        let task: QueryTask = serde_json::from_value(json!({
            "query_task_id": "query-export",
            "workflow_type": "counter",
            "query_name": "current",
            "payload_codec": "json",
            "workflow_arguments": {"codec": "json", "blob": "[]"},
            "query_arguments": {"codec": "json", "blob": "[]"},
            "history_events": [],
            "history_export": {
                "payloads": {"codec": "json"},
                "history_events": [{
                    "type": "SignalReceived",
                    "payload": {"signal_id": "signal-export", "signal_name": "increment"}
                }],
                "signals": [{
                    "id": "signal-export",
                    "name": "increment",
                    "status": "applied",
                    "workflow_sequence": 1,
                    "payload_codec": "json",
                    "arguments": "[9]"
                }]
            }
        }))
        .expect("query task");

        let result = worker.execute_query_task(task).await.expect("query result");
        assert_eq!(result, json!(9));
    }

    #[tokio::test]
    async fn query_task_failures_have_stable_reasons() {
        let client = Client::new("http://127.0.0.1:8080").expect("client");
        let mut worker = Worker::new(client, "rust-workers");
        worker.register_workflow("counter", |_ctx, _input| async move { Ok(Value::Null) });
        worker.register_query(
            "counter",
            "current",
            |_ctx, _args| async move { Ok(json!(0)) },
        );

        let base_task = QueryTask {
            query_task_id: "query-errors".to_string(),
            query_task_attempt: 1,
            lease_owner: None,
            workflow_id: Some("counter-errors".to_string()),
            run_id: Some("run-errors".to_string()),
            workflow_type: "counter".to_string(),
            query_name: "missing".to_string(),
            payload_codec: JSON_CODEC.to_string(),
            workflow_arguments: Some(json!({"codec": "json", "blob": "[]"})),
            query_arguments: Some(json!({"codec": "json", "blob": "[]"})),
            history_events: Vec::new(),
            history_export: None,
            run_status: Some("running".to_string()),
        };

        let unknown = worker
            .execute_query_task(base_task.clone())
            .await
            .expect_err("unknown query");
        assert_eq!(unknown.reason, "rejected_unknown_query");

        let mut malformed = base_task;
        malformed.query_name = "current".to_string();
        malformed.query_arguments = Some(json!({"codec": "json", "blob": "{"}));
        let malformed = worker
            .execute_query_task(malformed)
            .await
            .expect_err("malformed payload");
        assert_eq!(malformed.reason, "query_payload_decode_failed");

        let client = Client::new("http://127.0.0.1:8080").expect("client");
        let mut unavailable_worker = Worker::new(client, "rust-workers");
        unavailable_worker
            .register_workflow("counter", |_ctx, _input| async move { Ok(Value::Null) });
        let unavailable_task: QueryTask = serde_json::from_value(json!({
            "query_task_id": "query-unavailable",
            "workflow_type": "counter",
            "query_name": "current",
            "payload_codec": "json",
            "workflow_arguments": {"codec": "json", "blob": "[]"},
            "query_arguments": {"codec": "json", "blob": "[]"}
        }))
        .expect("query task");
        let unavailable = unavailable_worker
            .execute_query_task(unavailable_task)
            .await
            .expect_err("query handler unavailable");
        assert_eq!(unavailable.reason, "query_handler_unavailable");
    }

    #[tokio::test]
    async fn client_query_decodes_result_and_typed_failure() {
        let server = MockWorkerServer::start();
        let client = Client::builder(server.base_url())
            .timeout(Duration::from_secs(2))
            .build()
            .expect("client");

        let result = client
            .query_workflow("counter-1", "current", json!([]))
            .await
            .expect("query result");
        assert_eq!(result, json!({"count": 8}));

        let error = client
            .query_workflow("counter-1", "missing", json!([]))
            .await
            .expect_err("unknown query");
        let Error::QueryFailed(failure) = error else {
            panic!("expected typed query failure");
        };
        assert_eq!(failure.status, 404);
        assert_eq!(failure.reason, "rejected_unknown_query");
    }

    #[tokio::test]
    async fn baseline_worker_endpoints_send_the_baseline_protocol() {
        let server = MockWorkerServer::start();
        let client = Client::builder(server.base_url())
            .timeout(Duration::from_secs(2))
            .build()
            .expect("client");

        client
            .register_worker("capture-worker", "capture", vec![], vec![], 1, 1)
            .await
            .expect("register");
        client
            .heartbeat_worker("capture-worker", 1, 1)
            .await
            .expect("heartbeat");
        client
            .poll_workflow_task("capture-worker", "capture", Duration::from_millis(10))
            .await
            .expect("workflow poll");
        client
            .poll_activity_task("capture-worker", "capture", Duration::from_millis(10))
            .await
            .expect("activity poll");

        for path in [
            "/api/worker/register",
            "/api/worker/heartbeat",
            "/api/worker/workflow-tasks/poll",
            "/api/worker/activity-tasks/poll",
        ] {
            assert_eq!(
                server.worker_protocol_for(path).as_deref(),
                Some(WORKER_PROTOCOL_VERSION),
                "unexpected protocol for {path}"
            );
        }

        assert_eq!(
            server.request_body("/api/worker/workflow-tasks/poll")["timeout_seconds"],
            1
        );
        assert_eq!(
            server.request_body("/api/worker/activity-tasks/poll")["timeout_seconds"],
            1
        );
    }

    #[tokio::test]
    async fn query_task_endpoints_send_the_query_feature_protocol() {
        let server = MockWorkerServer::start();
        let client = Client::builder(server.base_url())
            .timeout(Duration::from_secs(2))
            .build()
            .expect("client");

        client
            .poll_query_task("capture-worker", "capture", Duration::from_millis(10))
            .await
            .expect("query poll");
        client
            .complete_query_task("query-capture", "capture-worker", 1, json!(8), JSON_CODEC)
            .await
            .expect("query complete");
        client
            .fail_query_task(
                "query-capture",
                "capture-worker",
                1,
                "failed",
                "query_rejected",
                "QueryFailed",
            )
            .await
            .expect("query fail");

        for path in [
            "/api/worker/query-tasks/poll",
            "/api/worker/query-tasks/query-capture/complete",
            "/api/worker/query-tasks/query-capture/fail",
        ] {
            assert_eq!(
                server.worker_protocol_for(path).as_deref(),
                Some(QUERY_TASK_MINIMUM_WORKER_PROTOCOL_VERSION),
                "unexpected protocol for {path}"
            );
        }

        assert_eq!(
            server.request_body("/api/worker/query-tasks/poll")["timeout_seconds"],
            1
        );
    }

    #[tokio::test]
    async fn query_protocol_rejection_from_older_server_is_typed() {
        let server = MockWorkerServer::reject_query_protocol();
        let client = Client::builder(server.base_url())
            .timeout(Duration::from_secs(2))
            .build()
            .expect("client");

        let error = client
            .poll_query_task("capture-worker", "capture", Duration::from_millis(10))
            .await
            .expect_err("server below query protocol floor must reject");
        let Error::Protocol(failure) = error else {
            panic!("expected typed protocol failure");
        };

        assert_eq!(failure.status, 400);
        assert_eq!(failure.reason, "unsupported_protocol_version");
        assert_eq!(failure.supported_version.as_deref(), Some("1.7"));
        assert_eq!(
            failure.requested_version.as_deref(),
            Some(QUERY_TASK_MINIMUM_WORKER_PROTOCOL_VERSION)
        );
        assert_eq!(
            server
                .worker_protocol_for("/api/worker/query-tasks/poll")
                .as_deref(),
            Some(QUERY_TASK_MINIMUM_WORKER_PROTOCOL_VERSION)
        );
    }

    #[tokio::test]
    async fn run_once_without_query_handlers_keeps_pre_query_server_compatibility() {
        let server = MockWorkerServer::reject_query_protocol();
        let client = Client::builder(server.base_url())
            .timeout(Duration::from_secs(2))
            .build()
            .expect("client");
        let mut worker = Worker::new(client, "rust-workers")
            .worker_id("baseline-worker")
            .poll_timeout(Duration::from_millis(10));

        worker.register_workflow("baseline.workflow", |_ctx, _input| async move {
            Ok(Value::Null)
        });

        assert_eq!(worker.run_once().await.expect("baseline run once"), 0);
        assert_eq!(
            server
                .worker_protocol_for("/api/worker/workflow-tasks/poll")
                .as_deref(),
            Some(WORKER_PROTOCOL_VERSION)
        );
        assert_eq!(
            server.worker_protocol_for("/api/worker/query-tasks/poll"),
            None,
            "a worker without query handlers must not use the query-task endpoint"
        );
    }

    #[tokio::test]
    async fn completion_time_query_rejection_is_typed_without_stopping_worker() {
        let server = MockWorkerServer::reject_query_completion();
        let client = Client::builder(server.base_url())
            .timeout(Duration::from_secs(2))
            .build()
            .expect("client");

        let error = client
            .complete_query_task("query-late", "late-worker", 1, json!(8), JSON_CODEC)
            .await
            .expect_err("expired completion must be rejected");
        let Error::QueryFailed(failure) = error else {
            panic!("expected typed query failure");
        };
        assert_eq!(failure.status, 409);
        assert_eq!(failure.reason, "query_task_timed_out");

        let mut worker = Worker::new(client, "rust-workers")
            .worker_id("late-worker")
            .poll_timeout(Duration::from_millis(10));
        worker.register_workflow("counter", |_ctx, _input| async move { Ok(Value::Null) });
        worker.register_query(
            "counter",
            "current",
            |_ctx, _args| async move { Ok(json!(8)) },
        );

        assert_eq!(worker.run_once().await.expect("late task is handled"), 1);
        assert_eq!(
            worker
                .run_once()
                .await
                .expect("worker continues after late completion"),
            0
        );
        assert_eq!(
            server.request_count("/api/worker/query-tasks/query-late/complete"),
            2
        );
        assert_eq!(
            server.request_count("/api/worker/query-tasks/query-late/fail"),
            0,
            "a server completion rejection must not be reported as an encoding failure"
        );
    }

    #[tokio::test]
    async fn activity_only_worker_can_shutdown_without_workflow_poller() {
        let server = MockWorkerServer::start();
        let client = Client::builder(server.base_url())
            .timeout(Duration::from_secs(2))
            .build()
            .expect("client");
        let mut worker = Worker::new(client, "rust-workers")
            .worker_id("activity-only-worker")
            .poll_timeout(Duration::from_millis(10));

        worker.register_activity(
            "activity.only",
            |_ctx, _args| async move { Ok(Value::Null) },
        );

        worker.run_until(async {}).await.expect("run worker");
    }

    #[tokio::test]
    async fn workflow_only_worker_can_shutdown_without_activity_poller() {
        let server = MockWorkerServer::start();
        let client = Client::builder(server.base_url())
            .timeout(Duration::from_secs(2))
            .build()
            .expect("client");
        let mut worker = Worker::new(client, "rust-workers")
            .worker_id("workflow-only-worker")
            .poll_timeout(Duration::from_millis(10));

        worker.register_workflow(
            "workflow.only",
            |_ctx, _input| async move { Ok(Value::Null) },
        );

        worker.run_until(async {}).await.expect("run worker");
    }

    #[tokio::test]
    async fn worker_heartbeat_observer_receives_server_acknowledgements() {
        let server = MockWorkerServer::start();
        let client = Client::builder(server.base_url())
            .timeout(Duration::from_secs(2))
            .build()
            .expect("client");
        let observations = Arc::new(Mutex::new(Vec::new()));
        let observed = Arc::clone(&observations);
        let mut worker = Worker::new(client, "rust-workers")
            .worker_id("observed-heartbeat-worker")
            .poll_timeout(Duration::from_millis(10))
            .on_worker_heartbeat(move |observation| {
                observed
                    .lock()
                    .expect("heartbeat observations")
                    .push(observation.clone());
            });

        worker.register_workflow("workflow.observed", |_ctx, _input| async move {
            Ok(Value::Null)
        });
        worker
            .run_until(tokio::time::sleep(Duration::from_millis(20)))
            .await
            .expect("run worker");

        let observations = observations.lock().expect("heartbeat observations");
        let first = observations.first().expect("heartbeat acknowledgement");
        assert_eq!(first.worker_id, "observed-heartbeat-worker");
        assert_eq!(first.task_queue, "rust-workers");
        assert!(first.acknowledged_at_unix_millis > 0);
        assert_eq!(first.acknowledgement, json!({}));
    }

    #[tokio::test]
    async fn worker_retries_poll_and_heartbeat_transport_failures_independently() {
        let server = MockWorkerServer::transient_worker_failures();
        let client = Client::builder(server.base_url())
            .timeout(Duration::from_secs(2))
            .build()
            .expect("client");
        let mut worker = Worker::new(client, "rust-workers")
            .worker_id("retry-worker")
            .poll_timeout(Duration::from_millis(10))
            .retry_policy(WorkerRetryPolicy {
                max_retries: 2,
                initial_backoff: Duration::from_millis(1),
                max_backoff: Duration::from_millis(1),
            });
        worker.register_workflow("counter", |_ctx, _input| async move { Ok(Value::Null) });
        worker.register_activity(
            "counter.activity",
            |_ctx, _input| async move { Ok(Value::Null) },
        );
        worker.register_query(
            "counter",
            "current",
            |_ctx, _args| async move { Ok(json!(8)) },
        );

        worker
            .run_until(tokio::time::sleep(Duration::from_millis(75)))
            .await
            .expect("transient failures must not stop the worker");

        for path in [
            "/api/worker/heartbeat",
            "/api/worker/workflow-tasks/poll",
            "/api/worker/activity-tasks/poll",
            "/api/worker/query-tasks/poll",
        ] {
            assert!(
                server.request_count(path) >= 2,
                "{path} must continue after its transient failure"
            );
        }
    }

    #[tokio::test]
    async fn worker_bounds_transport_retries() {
        let server = MockWorkerServer::unavailable_polls();
        let client = Client::builder(server.base_url())
            .timeout(Duration::from_secs(2))
            .build()
            .expect("client");
        let mut worker = Worker::new(client, "rust-workers")
            .worker_id("bounded-retry-worker")
            .poll_timeout(Duration::from_millis(10))
            .retry_policy(WorkerRetryPolicy {
                max_retries: 2,
                initial_backoff: Duration::from_millis(1),
                max_backoff: Duration::from_millis(1),
            });
        worker.register_workflow("counter", |_ctx, _input| async move { Ok(Value::Null) });

        let error = worker.run().await.expect_err("retry bound must terminate");
        assert!(matches!(error, Error::Transport(_)));
        assert_eq!(
            server.request_count("/api/worker/workflow-tasks/poll"),
            3,
            "one initial request plus two retries"
        );
    }

    #[tokio::test]
    async fn worker_does_not_retry_authentication_failures() {
        let server = MockWorkerServer::unauthorized_polls();
        let client = Client::builder(server.base_url())
            .timeout(Duration::from_secs(2))
            .build()
            .expect("client");
        let mut worker = Worker::new(client, "rust-workers")
            .worker_id("unauthorized-worker")
            .poll_timeout(Duration::from_millis(10));
        worker.register_workflow("counter", |_ctx, _input| async move { Ok(Value::Null) });

        let error = worker
            .run()
            .await
            .expect_err("authentication must terminate");
        let Error::Http { status, body } = error else {
            panic!("expected stable HTTP authentication error");
        };
        assert_eq!(status, reqwest::StatusCode::UNAUTHORIZED);
        assert!(body.contains("authentication_failed"));
        assert_eq!(
            server.request_count("/api/worker/workflow-tasks/poll"),
            1,
            "authentication failures must not be retried"
        );
    }

    #[derive(Clone, Debug)]
    struct CapturedRequest {
        path: String,
        worker_protocol: Option<String>,
        body: String,
    }

    struct MockWorkerServer {
        addr: SocketAddr,
        stop: Arc<AtomicBool>,
        requests: Arc<Mutex<Vec<CapturedRequest>>>,
        thread: Option<thread::JoinHandle<()>>,
    }

    #[derive(Clone, Copy, Default)]
    struct MockWorkerBehavior {
        reject_query_protocol: bool,
        reject_query_completion: bool,
        poll_failures_per_path: usize,
        heartbeat_failures: usize,
        unauthorized_polls: bool,
    }

    impl MockWorkerServer {
        fn start() -> Self {
            Self::start_with_behavior(MockWorkerBehavior::default())
        }

        fn reject_query_protocol() -> Self {
            Self::start_with_behavior(MockWorkerBehavior {
                reject_query_protocol: true,
                ..MockWorkerBehavior::default()
            })
        }

        fn reject_query_completion() -> Self {
            Self::start_with_behavior(MockWorkerBehavior {
                reject_query_completion: true,
                ..MockWorkerBehavior::default()
            })
        }

        fn transient_worker_failures() -> Self {
            Self::start_with_behavior(MockWorkerBehavior {
                poll_failures_per_path: 1,
                heartbeat_failures: 1,
                ..MockWorkerBehavior::default()
            })
        }

        fn unavailable_polls() -> Self {
            Self::start_with_behavior(MockWorkerBehavior {
                poll_failures_per_path: usize::MAX,
                ..MockWorkerBehavior::default()
            })
        }

        fn unauthorized_polls() -> Self {
            Self::start_with_behavior(MockWorkerBehavior {
                unauthorized_polls: true,
                ..MockWorkerBehavior::default()
            })
        }

        fn start_with_behavior(behavior: MockWorkerBehavior) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock server");
            listener
                .set_nonblocking(true)
                .expect("configure mock listener");
            let addr = listener.local_addr().expect("mock server address");
            let stop = Arc::new(AtomicBool::new(false));
            let server_stop = Arc::clone(&stop);
            let requests = Arc::new(Mutex::new(Vec::new()));
            let server_requests = Arc::clone(&requests);
            let thread = thread::spawn(move || {
                while !server_stop.load(Ordering::SeqCst) {
                    match listener.accept() {
                        Ok((mut stream, _)) => {
                            handle_mock_worker_request(&mut stream, &server_requests, behavior)
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(5));
                        }
                        Err(_) => break,
                    }
                }
            });

            Self {
                addr,
                stop,
                requests,
                thread: Some(thread),
            }
        }

        fn base_url(&self) -> String {
            format!("http://{}", self.addr)
        }

        fn worker_protocol_for(&self, path: &str) -> Option<String> {
            self.requests
                .lock()
                .expect("captured requests")
                .iter()
                .find(|request| request.path == path)
                .and_then(|request| request.worker_protocol.clone())
        }

        fn request_count(&self, path: &str) -> usize {
            self.requests
                .lock()
                .expect("captured requests")
                .iter()
                .filter(|request| request.path == path)
                .count()
        }

        fn request_body(&self, path: &str) -> Value {
            let requests = self.requests.lock().expect("captured requests");
            let body = &requests
                .iter()
                .find(|request| request.path == path)
                .unwrap_or_else(|| panic!("missing request for {path}"))
                .body;
            serde_json::from_str(body).unwrap_or_else(|error| {
                panic!("invalid JSON request body for {path}: {error}: {body:?}")
            })
        }
    }

    impl Drop for MockWorkerServer {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::SeqCst);
            let _ = TcpStream::connect(self.addr);

            if let Some(thread) = self.thread.take() {
                thread.join().expect("join mock server");
            }
        }
    }

    fn handle_mock_worker_request(
        stream: &mut TcpStream,
        requests: &Arc<Mutex<Vec<CapturedRequest>>>,
        behavior: MockWorkerBehavior,
    ) {
        let _ = stream.set_read_timeout(Some(Duration::from_millis(200)));
        let mut buffer = [0_u8; 8192];
        let mut request = Vec::new();

        loop {
            match stream.read(&mut buffer) {
                Ok(0) => break,
                Ok(read) => {
                    request.extend_from_slice(&buffer[..read]);
                    if mock_request_is_complete(&request) {
                        break;
                    }
                }
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                    ) =>
                {
                    break;
                }
                Err(_) => return,
            }
        }

        let request = String::from_utf8_lossy(&request);
        let body = request
            .split_once("\r\n\r\n")
            .map(|(_, body)| body)
            .unwrap_or_default();
        let path = request
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .unwrap_or_default();
        let worker_protocol = request.lines().find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("X-Durable-Workflow-Protocol-Version")
                .then(|| value.trim().to_string())
        });
        let request_number = {
            let mut requests = requests.lock().expect("captured requests");
            requests.push(CapturedRequest {
                path: path.to_string(),
                worker_protocol: worker_protocol.clone(),
                body: body.to_string(),
            });
            requests
                .iter()
                .filter(|request| request.path == path)
                .count()
        };

        let is_poll = matches!(
            path,
            "/api/worker/workflow-tasks/poll"
                | "/api/worker/activity-tasks/poll"
                | "/api/worker/query-tasks/poll"
        );
        if is_poll && request_number <= behavior.poll_failures_per_path {
            return;
        }
        if path == "/api/worker/heartbeat" && request_number <= behavior.heartbeat_failures {
            return;
        }
        if behavior.unauthorized_polls && is_poll {
            write_mock_response(
                stream,
                "401 Unauthorized",
                r#"{"reason":"authentication_failed","message":"invalid worker token"}"#,
            );
            return;
        }

        if behavior.reject_query_protocol && path.starts_with("/api/worker/query-tasks/") {
            let requested_version = worker_protocol.as_deref().unwrap_or("missing");
            let body = format!(
                r#"{{"reason":"unsupported_protocol_version","message":"Query tasks require worker protocol 1.8 or newer.","supported_version":"1.7","requested_version":"{requested_version}"}}"#
            );
            write_mock_response(stream, "400 Bad Request", &body);
            return;
        }

        if behavior.reject_query_completion && path == "/api/worker/query-tasks/query-late/complete"
        {
            write_mock_response(
                stream,
                "409 Conflict",
                r#"{"reason":"query_task_timed_out","message":"query task timed out before completion"}"#,
            );
            return;
        }

        let (status, body) = match path {
            "/api/worker/register" => (
                "200 OK",
                r#"{"worker_id":"mock-worker","registered":true,"heartbeat_interval_seconds":3600}"#,
            ),
            "/api/worker/heartbeat" => ("200 OK", "{}"),
            "/api/worker/activity-tasks/poll" | "/api/worker/workflow-tasks/poll" => {
                ("200 OK", r#"{"task":null}"#)
            }
            "/api/worker/query-tasks/poll"
                if behavior.reject_query_completion && request_number == 1 =>
            {
                (
                    "200 OK",
                    r#"{"task":{"query_task_id":"query-late","query_task_attempt":1,"lease_owner":"late-worker","workflow_id":"counter-late","run_id":"run-late","workflow_type":"counter","query_name":"current","payload_codec":"json","workflow_arguments":{"codec":"json","blob":"[]"},"query_arguments":{"codec":"json","blob":"[]"},"history_events":[],"run_status":"running"}}"#,
                )
            }
            "/api/worker/query-tasks/poll" => ("200 OK", r#"{"task":null}"#),
            "/api/worker/query-tasks/query-capture/complete"
            | "/api/worker/query-tasks/query-capture/fail" => ("200 OK", "{}"),
            "/api/workflows/counter-1/query/current" => (
                "200 OK",
                r#"{"workflow_id":"counter-1","query_name":"current","result":{"count":8},"result_envelope":{"codec":"json","blob":"{\"count\":8}"}}"#,
            ),
            "/api/workflows/counter-1/query/missing" => (
                "404 Not Found",
                r#"{"workflow_id":"counter-1","query_name":"missing","reason":"rejected_unknown_query","message":"unknown query"}"#,
            ),
            _ => ("404 Not Found", r#"{"message":"not found"}"#),
        };
        write_mock_response(stream, status, body);
    }

    fn mock_request_is_complete(request: &[u8]) -> bool {
        let Some(header_end) = request
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .map(|position| position + 4)
        else {
            return false;
        };
        let headers = String::from_utf8_lossy(&request[..header_end]);
        let content_length = headers.lines().find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())
                .flatten()
        });

        request.len() >= header_end + content_length.unwrap_or(0)
    }

    fn write_mock_response(stream: &mut TcpStream, status: &str, body: &str) {
        let response = format!(
            "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        );

        let _ = stream.write_all(response.as_bytes());
        let _ = stream.flush();
    }
}
