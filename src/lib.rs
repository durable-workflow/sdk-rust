#![doc = include_str!("../README.md")]

use std::{
    any::{type_name, Any, TypeId},
    collections::{BTreeMap, HashMap},
    future::Future,
    io::{self, Read},
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, OnceLock,
    },
    task::{Context as TaskContext, Poll},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use apache_avro::{from_avro_datum, to_avro_datum, types::Value as AvroDatum, Schema};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use chrono::DateTime;
use futures_util::{future::OptionFuture, task::noop_waker_ref};
use serde::{
    de::DeserializeOwned,
    ser::{SerializeMap, SerializeSeq},
    Deserialize, Deserializer, Serialize, Serializer,
};
pub use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use thiserror::Error;
pub use uuid::Uuid;

pub const WORKER_PROTOCOL_VERSION: &str = "1.19";
/// First additive worker protocol that defines portable worker-affinity features.
pub const PORTABLE_WORKER_AFFINITY_MINIMUM_PROTOCOL_VERSION: &str = "1.18";
pub const CONTROL_PLANE_VERSION: &str = "2";
pub const DEFAULT_CODEC: &str = "avro";
pub const SDK_VERSION: &str = concat!("durable-workflow-rust/", env!("CARGO_PKG_VERSION"));
/// Worker-registration capability for authored condition-wait occurrence identity.
pub const CONDITION_WAIT_OCCURRENCE_IDENTITY_CAPABILITY: &str =
    "condition_wait_occurrence_identity";
/// Worker-registration capability for portable memo upserts.
pub const MEMO_UPSERTS_CAPABILITY: &str = "memo_upserts";
/// Worker-registration capability for server-routed read-only queries.
pub const QUERY_TASKS_CAPABILITY: &str = "query_tasks";
/// Worker-registration capability for canonical typed search attributes.
pub const TYPED_SEARCH_ATTRIBUTES_CAPABILITY: &str = "typed_search_attributes";
/// Worker-registration capability for synchronous workflow updates.
pub const WORKFLOW_UPDATES_CAPABILITY: &str = "workflow_updates";
/// Worker-registration capability for durable named input streams.
pub const MESSAGE_STREAMS_CAPABILITY: &str = "message_streams";
/// Worker-registration capability for persisted first-completion selection.
pub const DURABLE_SELECTION_CAPABILITY: &str = "durable_selection";
pub const MESSAGE_STREAMS_MINIMUM_WORKER_PROTOCOL_VERSION: &str = "1.15";
pub const MESSAGE_STREAM_SIGNAL: &str = "__durable_workflow_message_stream";
pub const MESSAGE_STREAM_SCHEMA: &str = "durable-workflow.v2.message-stream.message";
pub const MESSAGE_STREAM_CURSOR_SCHEMA: &str = "durable-workflow.v2.message-stream.cursor";
pub const MESSAGE_STREAM_MAX_BATCH: usize = 100;
/// First additive worker protocol that defines query-task transport.
pub const QUERY_TASK_MINIMUM_WORKER_PROTOCOL_VERSION: &str = "1.8";
/// First additive worker protocol that defines typed search-attribute upserts.
pub const SEARCH_ATTRIBUTE_UPDATE_MINIMUM_WORKER_PROTOCOL_VERSION: &str = "1.8";
/// First additive worker protocol that defines portable memo upserts.
pub const MEMO_UPSERT_MINIMUM_WORKER_PROTOCOL_VERSION: &str = "1.14";
/// First additive worker protocol that preserves declared search-attribute types.
pub const TYPED_SEARCH_ATTRIBUTES_MINIMUM_WORKER_PROTOCOL_VERSION: &str = "1.16";
/// First additive worker protocol that defines external durable condition waits.
pub const CONDITION_WAIT_MINIMUM_WORKER_PROTOCOL_VERSION: &str = "1.9";
/// First additive worker protocol that preserves authored condition-wait occurrences.
pub const CONDITION_WAIT_OCCURRENCE_IDENTITY_MINIMUM_WORKER_PROTOCOL_VERSION: &str = "1.17";
/// First additive worker protocol that defines durable selection groups.
pub const DURABLE_SELECTION_MINIMUM_WORKER_PROTOCOL_VERSION: &str = "1.19";

pub fn worker_protocol_supports_message_streams(version: &str) -> bool {
    let Some((major, minor)) = version.split_once('.') else {
        return false;
    };
    major == "1" && minor.parse::<u64>().is_ok_and(|minor| minor >= 15)
}

fn validate_user_signal_name(signal_name: &str) -> Result<()> {
    if signal_name == MESSAGE_STREAM_SIGNAL {
        return Err(Error::Codec(format!(
            "signal name {MESSAGE_STREAM_SIGNAL:?} is reserved by the workflow runtime"
        )));
    }
    Ok(())
}

const MAX_LONG_POLL_TIMEOUT_SECONDS: u64 = 60;
const WORKFLOW_TASK_WAITING_FOR_HISTORY_MESSAGE: &str =
    "Workflow task waiting for scheduled history.";
const WORKFLOW_TASK_WAITING_FOR_HISTORY_TYPE: &str = "WorkflowTaskWaitingForHistory";
const MISSING_TASK_PAYLOAD_CODEC: &str = "\0missing-task-payload-codec";
const NULL_TASK_PAYLOAD_CODEC: &str = "\0null-task-payload-codec";
const NON_STRING_TASK_PAYLOAD_CODEC: &str = "\0non-string-task-payload-codec";
const MAX_MEMO_ENTRIES: usize = 100;
const MAX_MEMO_VALUE_SIZE_BYTES: usize = 10_240;
const MAX_MEMO_TOTAL_SIZE_BYTES: usize = 65_536;

const QUERY_TASK_FINAL_REJECTION_REASONS: &[&str] = &[
    "lease_expired",
    "query_task_not_found",
    "query_task_not_leased",
    "query_task_timed_out",
];

/// Truthful service-worker manifest for features this SDK currently refuses.
pub fn portable_worker_affinity_capability_manifest() -> Value {
    json!({
        "local_activities": {
            "supported": false,
            "minimum_protocol_version": PORTABLE_WORKER_AFFINITY_MINIMUM_PROTOCOL_VERSION,
            "reason": "rust_worker_does_not_execute_record_local_activity",
        },
        "worker_sessions": {
            "supported": false,
            "minimum_protocol_version": PORTABLE_WORKER_AFFINITY_MINIMUM_PROTOCOL_VERSION,
            "reason": "rust_worker_has_no_typed_session_lifecycle",
        },
        "sticky_execution": {
            "supported": false,
            "minimum_protocol_version": PORTABLE_WORKER_AFFINITY_MINIMUM_PROTOCOL_VERSION,
            "reason": "rust_worker_uses_complete_durable_history_replay",
        },
    })
}

/// Canonical Avro Value schema packaged with the crate and parsed by the runtime.
pub const AVRO_VALUE_SCHEMA_JSON: &str =
    include_str!("../schema/durable_workflow.protocol.Value.v1.avsc");
pub const AVRO_VALUE_SCHEMA_FINGERPRINT_HEX: &str = "e2a33dff55802237";
pub const AVRO_VALUE_SCHEMA_FINGERPRINT: [u8; 8] = [0xe2, 0xa3, 0x3d, 0xff, 0x55, 0x80, 0x22, 0x37];
const AVRO_SINGLE_OBJECT_MAGIC: [u8; 2] = [0xc3, 0x01];

static AVRO_VALUE_SCHEMA: OnceLock<std::result::Result<Schema, String>> = OnceLock::new();
static AVRO_VALUE_ORDERED_MAP_ENCODING_SCHEMA: OnceLock<std::result::Result<Schema, String>> =
    OnceLock::new();

#[derive(Clone, Copy)]
enum RequestProtocol {
    ControlPlane,
    Worker(&'static str),
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("transport error: {0}")]
    Transport(#[from] reqwest::Error),
    #[error(
        "invalid Durable Workflow base URL: omit the SDK-owned /api suffix and pass the Server or Cloud runtime base URL; the SDK appends /api automatically"
    )]
    InvalidBaseUrl,
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
    #[error(transparent)]
    NonDeterministicReplay(ReplayFailure),
    #[error(transparent)]
    ChildWorkflowFailed(ChildWorkflowFailure),
    #[error(transparent)]
    ActivityFailed(ActivityFailure),
    #[error(transparent)]
    ParallelFailed(ParallelFailure),
    #[error(transparent)]
    SagaCompensationFailed(SagaCompensationFailure),
    #[error(transparent)]
    InvalidParallelGroup(ParallelGroupError),
    #[error(transparent)]
    DurableOperationCancelled(DurableOperationCancelled),
    #[error(transparent)]
    WorkflowCancellationRequested(WorkflowCancellationRequested),
    #[error(transparent)]
    WorkflowCommandRejected(WorkflowCommandRejection),
    #[error(transparent)]
    WorkflowFailed(WorkflowTerminalOutcome),
    #[error(transparent)]
    WorkflowCancelled(WorkflowTerminalOutcome),
    #[error(transparent)]
    WorkflowTerminated(WorkflowTerminalOutcome),
    #[error(transparent)]
    WorkflowTimedOut(WorkflowTerminalOutcome),
    #[error(transparent)]
    ActivityTaskRejected(ActivityTaskRejection),
    #[error("workflow handler {0:?} is not registered")]
    WorkflowNotRegistered(String),
    #[error("activity handler {0:?} is not registered")]
    ActivityNotRegistered(String),
    #[error(
        "{handler_kind} handler {handler_name:?} {value_kind} type {rust_type} is incompatible with the fixed Avro Value codec: {message}"
    )]
    HandlerType {
        handler_kind: HandlerKind,
        handler_name: String,
        value_kind: HandlerValueKind,
        rust_type: &'static str,
        message: String,
    },
    #[error("workflow future yielded without emitting a durable command")]
    WorkflowYieldedWithoutCommand,
    #[error(
        "workflow_stream_command_identity_missing: workflow stream authoring requires a non-empty server-provided workflow_command_id"
    )]
    MissingWorkflowCommandIdentity,
    #[error("workflow state lock is poisoned")]
    WorkflowStatePoisoned,
    #[error("timer duration is too large for the worker protocol")]
    TimerDurationOverflow,
    #[error(transparent)]
    InvalidConditionWaitOptions(#[from] ConditionWaitOptionsError),
    #[error(transparent)]
    InvalidSearchAttributeUpdate(#[from] SearchAttributeUpdateError),
    #[error("operation timed out")]
    Timeout,
    #[error(
        "missing {role}-plane credentials: configure ClientBuilder::{role}_token or ClientBuilder::token; a {opposite_role}-plane token cannot authorize this request"
    )]
    MissingRoleCredentials {
        role: &'static str,
        opposite_role: &'static str,
    },
    #[error("worker loop error: {0}")]
    WorkerLoop(String),
    #[error(
        "workflow command contract for {workflow_type:?} declares update validators, but this Rust SDK cannot execute synchronous pre-accept update validation"
    )]
    UnsupportedUpdateValidators { workflow_type: String },
    #[error("{primary}; worker deregistration also failed: {deregistration}")]
    WorkerShutdown {
        primary: Box<Error>,
        deregistration: Box<Error>,
    },
    #[error("invalid child workflow options: {0}")]
    InvalidChildWorkflowOptions(String),
    #[error("invalid workflow memo update: {0}")]
    InvalidMemoUpdate(String),
    #[error(
        "workflow_memo_updates_unavailable: the connected runtime did not advertise workflow memo update support"
    )]
    WorkflowMemoUpdatesUnavailable,
    #[error(transparent)]
    InvalidActivityOptions(ActivityOptionsError),
    #[error(transparent)]
    InvalidContinueAsNewOptions(#[from] ContinueAsNewOptionsError),
    #[doc(hidden)]
    #[error("workflow requested continue as new")]
    ContinueAsNew(ContinueAsNewRequest),
}

/// Validation failure for a durable condition-wait definition.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum ConditionWaitOptionsError {
    #[error("condition_key must be non-empty")]
    EmptyKey,
    #[error("condition_definition_fingerprint must be non-empty")]
    EmptyPredicateIdentity,
    #[error("condition timeout is too large for the worker protocol")]
    TimeoutOverflow,
}

/// Stable identity and optional durable timeout for a condition wait.
///
/// `predicate_identity` is recorded as the worker protocol's
/// `condition_definition_fingerprint` and must change whenever predicate
/// behavior changes. Prefer the [`wait_condition!`] macro when the predicate
/// is written inline; it derives this identity from the predicate tokens.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConditionWaitOptions {
    condition_key: String,
    predicate_identity: String,
    timeout: Option<Duration>,
}

impl ConditionWaitOptions {
    pub fn new(condition_key: impl Into<String>, predicate_identity: impl Into<String>) -> Self {
        Self {
            condition_key: condition_key.into(),
            predicate_identity: predicate_identity.into(),
            timeout: None,
        }
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    fn validate(
        &self,
    ) -> std::result::Result<ValidatedConditionWaitOptions, ConditionWaitOptionsError> {
        let condition_key = self.condition_key.trim();
        if condition_key.is_empty() {
            return Err(ConditionWaitOptionsError::EmptyKey);
        }
        let predicate_identity = self.predicate_identity.trim();
        if predicate_identity.is_empty() {
            return Err(ConditionWaitOptionsError::EmptyPredicateIdentity);
        }
        let timeout_seconds = self
            .timeout
            .map(|timeout| {
                timeout
                    .as_secs()
                    .checked_add(u64::from(timeout.subsec_nanos() > 0))
                    .ok_or(ConditionWaitOptionsError::TimeoutOverflow)
            })
            .transpose()?;

        Ok(ValidatedConditionWaitOptions {
            condition_key: condition_key.to_string(),
            predicate_identity: predicate_identity.to_string(),
            timeout_seconds,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ValidatedConditionWaitOptions {
    condition_key: String,
    predicate_identity: String,
    timeout_seconds: Option<u64>,
}

const CONDITION_WAIT_OCCURRENCE_PREFIX: &str = "rust:condition-wait:";

/// Unambiguous terminal result of a durable condition wait.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConditionWaitResult {
    Satisfied,
    TimedOut,
}

impl ConditionWaitResult {
    pub fn is_satisfied(self) -> bool {
        self == Self::Satisfied
    }

    pub fn is_timed_out(self) -> bool {
        self == Self::TimedOut
    }
}

/// Build the stable condition definition identity used by [`wait_condition!`].
#[doc(hidden)]
pub fn __condition_definition_fingerprint(source: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(b"durable-workflow-rust.wait-condition.v1\0");
    digest.update(source.as_bytes());
    format!("sha256:{:x}", digest.finalize())
}

/// Create a durable condition wait whose predicate definition is fingerprinted
/// from its inline Rust tokens.
///
/// The returned [`ConditionWaitCall`] must be awaited. The timeout form is
/// `wait_condition!(ctx, "approval", timeout: duration, || predicate)`.
#[macro_export]
macro_rules! wait_condition {
    ($ctx:expr, $key:expr, timeout: $timeout:expr, $predicate:expr $(,)?) => {{
        $ctx.wait_condition(
            $crate::ConditionWaitOptions::new(
                $key,
                $crate::__condition_definition_fingerprint(concat!(
                    module_path!(),
                    "\0",
                    stringify!($predicate)
                )),
            )
            .timeout($timeout),
            $predicate,
        )
    }};
    ($ctx:expr, $key:expr, $predicate:expr $(,)?) => {{
        $ctx.wait_condition(
            $crate::ConditionWaitOptions::new(
                $key,
                $crate::__condition_definition_fingerprint(concat!(
                    module_path!(),
                    "\0",
                    stringify!($predicate)
                )),
            ),
            $predicate,
        )
    }};
}

const MAX_SEARCH_ATTRIBUTES_PER_UPDATE: usize = 100;
const MAX_SEARCH_ATTRIBUTE_KEY_LENGTH: usize = 64;
const MAX_SEARCH_ATTRIBUTE_STRING_LENGTH: usize = 2_048;
const MAX_SEARCH_ATTRIBUTE_KEYWORD_LENGTH: usize = 255;
const MAX_SEARCH_ATTRIBUTE_UPDATE_BYTES: usize = 65_536;

/// Validation failure for a typed workflow search-attribute update.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum SearchAttributeUpdateError {
    #[error("search-attribute update requires at least one attribute")]
    Empty,
    #[error("search attribute key {0:?} must be 1-64 URL-safe ASCII characters")]
    InvalidKey(String),
    #[error("search-attribute update exceeds the limit of 100 attributes")]
    TooManyAttributes,
    #[error("search attribute {key:?} {kind} value exceeds {limit} bytes")]
    ValueTooLong {
        key: String,
        kind: &'static str,
        limit: usize,
    },
    #[error(
        "search attribute {0:?} must not contain an empty string value; use delete() to remove it"
    )]
    EmptyString(String),
    #[error("search attribute {0:?} has a non-finite float value")]
    NonFiniteFloat(String),
    #[error("search attribute {0:?} must use an RFC 3339 datetime with an explicit timezone")]
    InvalidDateTime(String),
    #[error("search-attribute update exceeds the 65536-byte protocol limit")]
    PayloadTooLarge,
}

/// One public typed search-attribute value.
#[derive(Clone, Debug, PartialEq)]
pub enum SearchAttributeValue {
    String(String),
    Keyword(String),
    KeywordList(Vec<String>),
    Int(i64),
    Float(f64),
    Bool(bool),
    DateTime(String),
    Delete,
}

impl SearchAttributeValue {
    fn type_name(&self) -> Option<&'static str> {
        match self {
            Self::String(_) => Some("string"),
            Self::Keyword(_) => Some("keyword"),
            Self::KeywordList(_) => Some("keyword_list"),
            Self::Int(_) => Some("int"),
            Self::Float(_) => Some("float"),
            Self::Bool(_) => Some("bool"),
            Self::DateTime(_) => Some("datetime"),
            Self::Delete => None,
        }
    }

    fn normalized(self, key: &str) -> std::result::Result<Self, SearchAttributeUpdateError> {
        let normalize_string = |value: String, kind: &'static str, limit: usize| {
            let value = value.trim().to_string();
            if value.is_empty() {
                return Err(SearchAttributeUpdateError::EmptyString(key.to_string()));
            }
            if value.len() > limit {
                return Err(SearchAttributeUpdateError::ValueTooLong {
                    key: key.to_string(),
                    kind,
                    limit,
                });
            }
            Ok(value)
        };

        match self {
            Self::String(value) => Ok(Self::String(normalize_string(
                value,
                "string",
                MAX_SEARCH_ATTRIBUTE_STRING_LENGTH,
            )?)),
            Self::Keyword(value) => Ok(Self::Keyword(normalize_string(
                value,
                "keyword",
                MAX_SEARCH_ATTRIBUTE_KEYWORD_LENGTH,
            )?)),
            Self::KeywordList(values) => {
                let values = values
                    .into_iter()
                    .map(|value| {
                        let value = value.trim().to_string();
                        if value.len() > MAX_SEARCH_ATTRIBUTE_KEYWORD_LENGTH {
                            return Err(SearchAttributeUpdateError::ValueTooLong {
                                key: key.to_string(),
                                kind: "keyword-list entry",
                                limit: MAX_SEARCH_ATTRIBUTE_KEYWORD_LENGTH,
                            });
                        }
                        Ok(value)
                    })
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                Ok(Self::KeywordList(values))
            }
            Self::Float(value) if !value.is_finite() => {
                Err(SearchAttributeUpdateError::NonFiniteFloat(key.to_string()))
            }
            Self::DateTime(value) => {
                let value =
                    normalize_string(value, "datetime", MAX_SEARCH_ATTRIBUTE_STRING_LENGTH)?;
                if DateTime::parse_from_rfc3339(&value).is_err() {
                    return Err(SearchAttributeUpdateError::InvalidDateTime(key.to_string()));
                }
                Ok(Self::DateTime(value))
            }
            value => Ok(value),
        }
    }

    fn into_json(self) -> Value {
        match self {
            Self::String(value) | Self::Keyword(value) | Self::DateTime(value) => {
                Value::String(value)
            }
            Self::KeywordList(values) => {
                Value::Array(values.into_iter().map(Value::String).collect())
            }
            Self::Int(value) => json!(value),
            Self::Float(value) => json!(value),
            Self::Bool(value) => json!(value),
            Self::Delete => Value::Null,
        }
    }
}

/// Validated typed workflow-side search-attribute mutation.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SearchAttributeUpdate {
    attributes: BTreeMap<String, SearchAttributeValue>,
}

impl SearchAttributeUpdate {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set(
        mut self,
        key: impl Into<String>,
        value: SearchAttributeValue,
    ) -> std::result::Result<Self, SearchAttributeUpdateError> {
        let key = key.into();
        validate_search_attribute_key(&key)?;
        if !self.attributes.contains_key(&key)
            && self.attributes.len() >= MAX_SEARCH_ATTRIBUTES_PER_UPDATE
        {
            return Err(SearchAttributeUpdateError::TooManyAttributes);
        }
        self.attributes.insert(key.clone(), value.normalized(&key)?);
        self.validate_size()?;
        Ok(self)
    }

    pub fn string(
        self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> std::result::Result<Self, SearchAttributeUpdateError> {
        self.set(key, SearchAttributeValue::String(value.into()))
    }

    pub fn keyword(
        self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> std::result::Result<Self, SearchAttributeUpdateError> {
        self.set(key, SearchAttributeValue::Keyword(value.into()))
    }

    pub fn keyword_list<I, V>(
        self,
        key: impl Into<String>,
        values: I,
    ) -> std::result::Result<Self, SearchAttributeUpdateError>
    where
        I: IntoIterator<Item = V>,
        V: Into<String>,
    {
        self.set(
            key,
            SearchAttributeValue::KeywordList(values.into_iter().map(Into::into).collect()),
        )
    }

    pub fn int(
        self,
        key: impl Into<String>,
        value: i64,
    ) -> std::result::Result<Self, SearchAttributeUpdateError> {
        self.set(key, SearchAttributeValue::Int(value))
    }

    pub fn float(
        self,
        key: impl Into<String>,
        value: f64,
    ) -> std::result::Result<Self, SearchAttributeUpdateError> {
        self.set(key, SearchAttributeValue::Float(value))
    }

    pub fn bool(
        self,
        key: impl Into<String>,
        value: bool,
    ) -> std::result::Result<Self, SearchAttributeUpdateError> {
        self.set(key, SearchAttributeValue::Bool(value))
    }

    pub fn datetime(
        self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> std::result::Result<Self, SearchAttributeUpdateError> {
        self.set(key, SearchAttributeValue::DateTime(value.into()))
    }

    pub fn delete(
        self,
        key: impl Into<String>,
    ) -> std::result::Result<Self, SearchAttributeUpdateError> {
        self.set(key, SearchAttributeValue::Delete)
    }

    fn validate_size(&self) -> std::result::Result<(), SearchAttributeUpdateError> {
        let (attributes, _) = self.clone().into_wire_parts();
        if serde_json::to_vec(&attributes)
            .map(|payload| payload.len() > MAX_SEARCH_ATTRIBUTE_UPDATE_BYTES)
            .unwrap_or(true)
        {
            return Err(SearchAttributeUpdateError::PayloadTooLarge);
        }
        Ok(())
    }

    fn into_wire_parts(self) -> (Value, BTreeMap<String, String>) {
        let mut attributes = serde_json::Map::new();
        let mut attribute_types = BTreeMap::new();
        for (key, value) in self.attributes {
            if let Some(type_name) = value.type_name() {
                attribute_types.insert(key.clone(), type_name.to_string());
            }
            attributes.insert(key, value.into_json());
        }
        (Value::Object(attributes), attribute_types)
    }

    fn validate(&self) -> std::result::Result<(), SearchAttributeUpdateError> {
        if self.attributes.is_empty() {
            return Err(SearchAttributeUpdateError::Empty);
        }
        self.validate_size()
    }
}

fn validate_search_attribute_key(key: &str) -> std::result::Result<(), SearchAttributeUpdateError> {
    let valid = !key.is_empty()
        && key.len() <= MAX_SEARCH_ATTRIBUTE_KEY_LENGTH
        && key
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b':'));
    if valid {
        Ok(())
    } else {
        Err(SearchAttributeUpdateError::InvalidKey(key.to_string()))
    }
}

/// The registered handler family reported by [`Error::HandlerType`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HandlerKind {
    Workflow,
    Activity,
}

impl std::fmt::Display for HandlerKind {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Workflow => "workflow",
            Self::Activity => "activity",
        })
    }
}

/// Whether a typed handler failed to adapt its input or result.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HandlerValueKind {
    Input,
    Result,
}

impl std::fmt::Display for HandlerValueKind {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Input => "input",
            Self::Result => "result",
        })
    }
}

/// The lifecycle command sent to a workflow execution.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkflowCommandKind {
    Cancel,
    Terminate,
}

impl WorkflowCommandKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Cancel => "cancel",
            Self::Terminate => "terminate",
        }
    }
}

/// Optional structured fields for a cancellation or termination request.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct WorkflowCommandOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
}

/// Server-enforced timeout policy for a workflow start.
///
/// These deadlines are distinct from [`WorkflowResultOptions::timeout`], which
/// only bounds how long the caller waits. A server deadline produces a terminal
/// [`Error::WorkflowTimedOut`] outcome whose reason is `execution_timeout` or
/// `run_timeout`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkflowStartOptions {
    pub execution_timeout_seconds: u64,
    pub run_timeout_seconds: u64,
}

impl Default for WorkflowStartOptions {
    fn default() -> Self {
        Self {
            execution_timeout_seconds: 3600,
            run_timeout_seconds: 600,
        }
    }
}

impl WorkflowStartOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn execution_timeout_seconds(mut self, seconds: u64) -> Self {
        self.execution_timeout_seconds = seconds;
        self
    }

    pub fn run_timeout_seconds(mut self, seconds: u64) -> Self {
        self.run_timeout_seconds = seconds;
        self
    }

    fn validate(&self) -> Result<()> {
        if self.execution_timeout_seconds == 0 {
            return Err(Error::Codec(
                "execution_timeout_seconds must be at least 1".to_string(),
            ));
        }
        if self.run_timeout_seconds == 0 {
            return Err(Error::Codec(
                "run_timeout_seconds must be at least 1".to_string(),
            ));
        }
        if self.run_timeout_seconds > self.execution_timeout_seconds {
            return Err(Error::Codec(
                "run_timeout_seconds cannot exceed execution_timeout_seconds".to_string(),
            ));
        }

        Ok(())
    }
}

/// Optional routing overrides for a continue-as-new transition.
///
/// Omitted values retain the current workflow type and task queue. Server-owned
/// instance metadata is not accepted here and is carried by the server.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ContinueAsNewOptions {
    pub workflow_type: Option<String>,
    pub task_queue: Option<String>,
}

impl ContinueAsNewOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn workflow_type(mut self, workflow_type: impl Into<String>) -> Self {
        self.workflow_type = Some(workflow_type.into());
        self
    }

    pub fn task_queue(mut self, task_queue: impl Into<String>) -> Self {
        self.task_queue = Some(task_queue.into());
        self
    }

    fn validate(&self) -> std::result::Result<(), ContinueAsNewOptionsError> {
        for (field, value) in [
            ("workflow_type", self.workflow_type.as_deref()),
            ("task_queue", self.task_queue.as_deref()),
        ] {
            if value.is_some_and(|value| value.trim().is_empty()) {
                return Err(ContinueAsNewOptionsError {
                    field,
                    message: format!("{field} must not be empty"),
                });
            }
        }
        Ok(())
    }
}

/// A stable validation error raised before a continue-as-new command is emitted.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
#[error("invalid continue-as-new option {field}: {message}")]
pub struct ContinueAsNewOptionsError {
    pub field: &'static str,
    pub message: String,
}

/// Public history-budget information attached to the current workflow task.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WorkflowHistoryBudget {
    pub event_count: u64,
    pub size_bytes: Option<u64>,
    pub continue_as_new_recommended: bool,
    pub pressure: Option<String>,
}

#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct ContinueAsNewRequest {
    arguments: AvroValue,
    options: ContinueAsNewOptions,
}

impl WorkflowCommandOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn reason(mut self, reason: impl Into<String>) -> Self {
        self.reason = Some(reason.into());
        self
    }

    pub fn request_id(mut self, request_id: impl Into<String>) -> Self {
        self.request_id = Some(request_id.into());
        self
    }
}

/// The accepted, machine-readable result of a lifecycle command.
#[derive(Clone, Debug, PartialEq)]
pub struct WorkflowCommandResult {
    pub command: WorkflowCommandKind,
    pub workflow_id: String,
    pub run_id: Option<String>,
    pub outcome: Option<String>,
    pub reason: Option<String>,
    pub command_status: Option<String>,
    pub raw: Value,
}

/// A stable rejection returned by instance- or selected-run lifecycle commands.
#[derive(Clone, Debug, Error)]
#[error("workflow {command:?} rejected ({reason}, HTTP {status}): {message}")]
pub struct WorkflowCommandRejection {
    pub command: WorkflowCommandKind,
    pub status: u16,
    pub reason: String,
    pub message: String,
    pub workflow_id: String,
    pub run_id: Option<String>,
    pub target_scope: Option<String>,
    pub body: Value,
}

/// Stable terminal categories returned by [`WorkflowHandle::result`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkflowTerminalKind {
    Failed,
    Cancelled,
    Terminated,
    TimedOut,
}

/// A typed terminal workflow outcome with durable identity and failure metadata.
///
/// Match the corresponding [`enum@Error`] variant and inspect these fields instead
/// of parsing its display representation. Fields remain `None` when an older
/// server did not publish that metadata.
#[derive(Clone, Debug, Error)]
#[error("workflow {workflow_id} run {run_id:?} ended as {kind:?} ({reason})")]
pub struct WorkflowTerminalOutcome {
    pub kind: WorkflowTerminalKind,
    pub workflow_id: String,
    pub run_id: Option<String>,
    pub reason: String,
    pub failure_category: Option<String>,
    pub failure_id: Option<String>,
    pub exception_type: Option<String>,
    pub exception_class: Option<String>,
    pub non_retryable: Option<bool>,
    pub message: Option<String>,
    pub exception: Option<Value>,
    pub raw: Value,
}

/// A worker-side activity settlement or heartbeat rejected by durable state.
#[derive(Clone, Debug, Error)]
#[error("activity task {operation} rejected ({reason}, HTTP {status})")]
pub struct ActivityTaskRejection {
    pub operation: String,
    pub status: u16,
    pub reason: String,
    pub task_id: String,
    pub activity_attempt_id: String,
    pub cancel_requested: bool,
    pub can_continue: Option<bool>,
    pub run_closed_reason: Option<String>,
    pub body: Value,
}

/// Stable validation categories for [`ActivityOptions`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ActivityOptionsErrorKind {
    EmptyTaskQueue,
    EmptyRetryPolicy,
    InvalidMaxAttempts,
    BackoffWithoutRetryBudget,
    TooManyBackoffIntervals,
    InvalidBackoffCoefficient,
    BackoffGenerationTooLarge,
    BackoffOverflow,
    EmptyNonRetryableErrorType,
    TimeoutNotPositive,
    TimeoutOverflow,
    TimeoutOrder,
}

/// A machine-readable activity-options validation failure.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
#[error("invalid activity options ({kind:?}, {field:?}): {message}")]
pub struct ActivityOptionsError {
    pub kind: ActivityOptionsErrorKind,
    pub field: Option<&'static str>,
    pub message: String,
}

impl ActivityOptionsError {
    fn new(
        kind: ActivityOptionsErrorKind,
        field: Option<&'static str>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            kind,
            field,
            message: message.into(),
        }
    }
}

/// Stable terminal categories returned when an awaited activity does not succeed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ActivityFailureKind {
    Failed,
    Cancelled,
    TimedOut,
}

/// A stable, machine-readable terminal activity failure.
///
/// Match [`Error::ActivityFailed`] and inspect `kind`, `reason`,
/// `failure_category`, or `timeout_kind`; display text is only diagnostic.
#[derive(Clone, Debug, Error)]
#[error("activity failed ({reason}): {message}")]
pub struct ActivityFailure {
    pub kind: ActivityFailureKind,
    pub reason: String,
    pub message: String,
    pub activity_execution_id: Option<String>,
    pub activity_attempt_id: Option<String>,
    pub activity_type: Option<String>,
    pub activity_class: Option<String>,
    pub attempt_number: Option<u64>,
    pub failure_id: Option<String>,
    pub failure_category: Option<String>,
    pub timeout_kind: Option<String>,
    pub non_retryable: bool,
    pub exception_type: Option<String>,
    pub exception_class: Option<String>,
    pub code: Option<Value>,
    pub exception: Option<Value>,
}

/// Stable terminal categories returned when an awaited child does not succeed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChildWorkflowFailureKind {
    Failed,
    Cancelled,
    Terminated,
}

/// A stable, machine-readable child workflow failure delivered to its parent.
///
/// Match [`Error::ChildWorkflowFailed`] and inspect `reason` or `kind` instead
/// of parsing the display message. Child and parent identifiers retain the
/// relationship recorded in durable history across worker restarts.
#[derive(Clone, Debug, Error)]
#[error("child workflow failed ({reason}): {message}")]
pub struct ChildWorkflowFailure {
    pub kind: ChildWorkflowFailureKind,
    pub reason: String,
    pub message: String,
    pub parent_workflow_id: Option<String>,
    pub parent_workflow_run_id: Option<String>,
    pub child_workflow_id: Option<String>,
    pub child_workflow_run_id: Option<String>,
    pub child_workflow_type: Option<String>,
    pub failure_id: Option<String>,
    pub failure_category: Option<String>,
    pub exception_type: Option<String>,
    pub exception_class: Option<String>,
    pub non_retryable: bool,
    pub code: Option<Value>,
    pub exception: Option<Value>,
}

/// The identity of one durable workflow execution.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkflowIdentity {
    pub workflow_id: Option<String>,
    pub run_id: Option<String>,
}

/// A successful child result together with its durable parent-child identity.
#[derive(Clone, Debug, PartialEq)]
pub struct ChildWorkflowResult {
    pub parent: WorkflowIdentity,
    pub child: WorkflowIdentity,
    pub child_workflow_type: Option<String>,
    pub result: Value,
}

/// Lossless successful child result for fixed Avro Value workflows.
#[derive(Clone, Debug, PartialEq)]
pub struct ChildWorkflowAvroResult {
    pub parent: WorkflowIdentity,
    pub child: WorkflowIdentity,
    pub child_workflow_type: Option<String>,
    pub result: AvroValue,
}

/// Stable user-facing identity for one member of a durable selection group.
#[derive(Clone, Debug, Deserialize, Hash, PartialEq, Eq, Serialize)]
#[serde(untagged)]
pub enum SelectionKey {
    Index(usize),
    Name(String),
}

impl From<usize> for SelectionKey {
    fn from(value: usize) -> Self {
        Self::Index(value)
    }
}

impl From<String> for SelectionKey {
    fn from(value: String) -> Self {
        Self::Name(value)
    }
}

impl From<&str> for SelectionKey {
    fn from(value: &str) -> Self {
        Self::Name(value.to_string())
    }
}

/// Typed result of explicitly awaiting a cancelled non-winning operation.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
#[error("selected {operation_kind} operation {operation_identity} was explicitly cancelled")]
pub struct DurableOperationCancelled {
    pub selection_group_id: String,
    pub member_key: SelectionKey,
    pub member_index: usize,
    pub operation_kind: String,
    pub operation_identity: String,
}

/// Stable identity for one enclosing deterministic parallel group.
///
/// The same fields are attached to every ordinary activity, timer, or child
/// workflow command in the group. Nested leaves carry an outer-to-inner path;
/// no Rust-specific wire command is introduced.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct ParallelGroupMetadata {
    pub parallel_group_id: String,
    pub parallel_group_kind: String,
    pub parallel_group_base_sequence: u64,
    pub parallel_group_size: usize,
    pub parallel_group_index: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parallel_group_mode: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selection_member_key: Option<SelectionKey>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selection_member_index: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selection_member_base_sequence: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selection_member_size: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selection_member_kind: Option<String>,
}

/// One input-ordered result returned by [`WorkflowContext::parallel`].
#[derive(Clone, Debug, PartialEq)]
pub enum ParallelResult {
    Activity(Value),
    ChildWorkflow(ChildWorkflowResult),
    Timer,
    Signal(Vec<Value>),
    Condition(ConditionWaitResult),
    Group(Vec<ParallelResult>),
}

/// Lossless fixed-Avro counterpart to [`ParallelResult`].
#[derive(Clone, Debug, PartialEq)]
pub enum ParallelAvroResult {
    Activity(AvroValue),
    ChildWorkflow(ChildWorkflowAvroResult),
    Timer,
    Signal(Vec<AvroValue>),
    Condition(ConditionWaitResult),
    Group(Vec<ParallelAvroResult>),
}

impl ParallelAvroResult {
    fn into_json_result(self) -> Result<ParallelResult> {
        match self {
            Self::Activity(value) => Ok(ParallelResult::Activity(value.into_json()?)),
            Self::ChildWorkflow(result) => Ok(ParallelResult::ChildWorkflow(ChildWorkflowResult {
                parent: result.parent,
                child: result.child,
                child_workflow_type: result.child_workflow_type,
                result: result.result.into_json()?,
            })),
            Self::Timer => Ok(ParallelResult::Timer),
            Self::Signal(values) => Ok(ParallelResult::Signal(
                values
                    .into_iter()
                    .map(AvroValue::into_json)
                    .collect::<Result<Vec<_>>>()?,
            )),
            Self::Condition(result) => Ok(ParallelResult::Condition(result)),
            Self::Group(results) => Ok(ParallelResult::Group(
                results
                    .into_iter()
                    .map(Self::into_json_result)
                    .collect::<Result<Vec<_>>>()?,
            )),
        }
    }
}

/// One successful leaf retained when another parallel member failed.
#[derive(Clone, Debug, PartialEq)]
pub struct ParallelCompletion {
    pub member_path: Vec<usize>,
    pub result: ParallelResult,
}

/// A deterministic join failed after some siblings had already completed.
///
/// `cause` retains the typed activity, child-workflow, cancellation, or codec
/// error. `completed` is declaration ordered and contains only durable
/// successes observed in the same replay. Late sibling completions can add
/// entries on a later replay without changing `member_path` or the selected
/// positional failure.
#[derive(Debug, Error)]
#[error("parallel group {group_id} member {member_path:?} failed: {cause}")]
pub struct ParallelFailure {
    pub group_id: String,
    pub member_path: Vec<usize>,
    pub group_path: Vec<ParallelGroupMetadata>,
    pub completed: Vec<ParallelCompletion>,
    #[source]
    pub cause: Box<Error>,
}

/// Stable validation error returned before an invalid group emits commands.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
#[error("invalid deterministic parallel group ({reason}): {message}")]
pub struct ParallelGroupError {
    pub reason: &'static str,
    pub member_path: Vec<usize>,
    pub message: String,
}

/// Cooperative workflow cancellation observed at an author-controlled point.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
#[error("workflow cancellation was requested")]
pub struct WorkflowCancellationRequested;

/// A forward saga failure followed by a terminal compensation failure.
#[derive(Debug, Error)]
#[error(
    "saga forward execution failed; compensation activity {compensation_activity_type} (registration {compensation_registration_order}) also failed: {compensation_failure}"
)]
pub struct SagaCompensationFailure {
    pub initiating_failure: Box<Error>,
    pub compensation_failure: Box<Error>,
    pub compensation_activity_type: String,
    pub compensation_registration_order: usize,
}

/// Server behavior when a parent closes while its child is still open.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ParentClosePolicy {
    #[default]
    Abandon,
    RequestCancel,
    Terminate,
}

impl ParentClosePolicy {
    fn as_str(self) -> &'static str {
        match self {
            Self::Abandon => "abandon",
            Self::RequestCancel => "request_cancel",
            Self::Terminate => "terminate",
        }
    }
}

/// Durable retry policy for one child workflow invocation.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ChildWorkflowRetryPolicy {
    pub max_attempts: Option<u32>,
    pub backoff_seconds: Vec<u64>,
    pub non_retryable_error_types: Vec<String>,
}

/// Options recorded with a child-workflow command.
///
/// The task queue is mandatory so routing is explicit and replay-stable.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChildWorkflowOptions {
    pub task_queue: String,
    pub parent_close_policy: ParentClosePolicy,
    pub retry_policy: Option<ChildWorkflowRetryPolicy>,
    pub execution_timeout_seconds: Option<u64>,
    pub run_timeout_seconds: Option<u64>,
}

impl ChildWorkflowOptions {
    pub fn new(task_queue: impl Into<String>) -> Self {
        Self {
            task_queue: task_queue.into(),
            parent_close_policy: ParentClosePolicy::Abandon,
            retry_policy: None,
            execution_timeout_seconds: None,
            run_timeout_seconds: None,
        }
    }

    pub fn parent_close_policy(mut self, policy: ParentClosePolicy) -> Self {
        self.parent_close_policy = policy;
        self
    }

    pub fn retry_policy(mut self, policy: ChildWorkflowRetryPolicy) -> Self {
        self.retry_policy = Some(policy);
        self
    }

    pub fn execution_timeout_seconds(mut self, seconds: u64) -> Self {
        self.execution_timeout_seconds = Some(seconds);
        self
    }

    pub fn run_timeout_seconds(mut self, seconds: u64) -> Self {
        self.run_timeout_seconds = Some(seconds);
        self
    }
}

/// Backoff intervals for one durable activity retry policy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ActivityBackoff {
    /// Use these intervals between attempts. The server repeats the final
    /// interval if the retry budget contains more attempts than entries.
    Explicit(Vec<Duration>),
    /// Generate one interval for every retry using integer exponential growth.
    Exponential {
        initial_interval: Duration,
        coefficient: u32,
        maximum_interval: Option<Duration>,
    },
}

/// Durable server-side retry policy for one activity execution.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ActivityRetryPolicy {
    pub max_attempts: Option<u32>,
    pub backoff: Option<ActivityBackoff>,
    pub non_retryable_error_types: Vec<String>,
}

impl ActivityRetryPolicy {
    /// Start a policy with a finite attempt budget, including the first attempt.
    pub fn new(max_attempts: u32) -> Self {
        Self {
            max_attempts: Some(max_attempts),
            ..Self::default()
        }
    }

    pub fn backoff_intervals(mut self, intervals: impl IntoIterator<Item = Duration>) -> Self {
        self.backoff = Some(ActivityBackoff::Explicit(intervals.into_iter().collect()));
        self
    }

    pub fn exponential_backoff(
        mut self,
        initial_interval: Duration,
        coefficient: u32,
        maximum_interval: Option<Duration>,
    ) -> Self {
        self.backoff = Some(ActivityBackoff::Exponential {
            initial_interval,
            coefficient,
            maximum_interval,
        });
        self
    }

    pub fn non_retryable_error_type(mut self, error_type: impl Into<String>) -> Self {
        self.non_retryable_error_types.push(error_type.into());
        self
    }

    pub fn non_retryable_error_types(
        mut self,
        error_types: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.non_retryable_error_types
            .extend(error_types.into_iter().map(Into::into));
        self
    }
}

/// Options recorded atomically on one deterministic `schedule_activity` command.
///
/// Durations are rounded up to whole seconds when encoded, so the server never
/// receives a shorter timeout or backoff than the caller requested.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ActivityOptions {
    pub task_queue: Option<String>,
    pub retry_policy: Option<ActivityRetryPolicy>,
    pub start_to_close_timeout: Option<Duration>,
    pub schedule_to_start_timeout: Option<Duration>,
    pub schedule_to_close_timeout: Option<Duration>,
    pub heartbeat_timeout: Option<Duration>,
}

impl ActivityOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn task_queue(mut self, task_queue: impl Into<String>) -> Self {
        self.task_queue = Some(task_queue.into());
        self
    }

    pub fn retry_policy(mut self, policy: ActivityRetryPolicy) -> Self {
        self.retry_policy = Some(policy);
        self
    }

    pub fn start_to_close_timeout(mut self, timeout: Duration) -> Self {
        self.start_to_close_timeout = Some(timeout);
        self
    }

    pub fn schedule_to_start_timeout(mut self, timeout: Duration) -> Self {
        self.schedule_to_start_timeout = Some(timeout);
        self
    }

    pub fn schedule_to_close_timeout(mut self, timeout: Duration) -> Self {
        self.schedule_to_close_timeout = Some(timeout);
        self
    }

    pub fn heartbeat_timeout(mut self, timeout: Duration) -> Self {
        self.heartbeat_timeout = Some(timeout);
        self
    }

    fn validate(&self) -> std::result::Result<ValidatedActivityOptions, ActivityOptionsError> {
        if self
            .task_queue
            .as_deref()
            .is_some_and(|queue| queue.trim().is_empty())
        {
            return Err(ActivityOptionsError::new(
                ActivityOptionsErrorKind::EmptyTaskQueue,
                Some("task_queue"),
                "task_queue must not be empty",
            ));
        }

        for (field, value) in [
            ("start_to_close_timeout", self.start_to_close_timeout),
            ("schedule_to_start_timeout", self.schedule_to_start_timeout),
            ("schedule_to_close_timeout", self.schedule_to_close_timeout),
            ("heartbeat_timeout", self.heartbeat_timeout),
        ] {
            if value.is_some_and(|value| value.is_zero()) {
                return Err(ActivityOptionsError::new(
                    ActivityOptionsErrorKind::TimeoutNotPositive,
                    Some(field),
                    format!("{field} must be positive"),
                ));
            }
        }

        validate_timeout_order(
            "heartbeat_timeout",
            self.heartbeat_timeout,
            "start_to_close_timeout",
            self.start_to_close_timeout,
        )?;
        validate_timeout_order(
            "start_to_close_timeout",
            self.start_to_close_timeout,
            "schedule_to_close_timeout",
            self.schedule_to_close_timeout,
        )?;
        validate_timeout_order(
            "schedule_to_start_timeout",
            self.schedule_to_start_timeout,
            "schedule_to_close_timeout",
            self.schedule_to_close_timeout,
        )?;

        Ok(ValidatedActivityOptions {
            task_queue: self.task_queue.clone(),
            retry_policy: self
                .retry_policy
                .as_ref()
                .map(validate_activity_retry_policy)
                .transpose()?,
            start_to_close_timeout: timeout_seconds(
                "start_to_close_timeout",
                self.start_to_close_timeout,
            )?,
            schedule_to_start_timeout: timeout_seconds(
                "schedule_to_start_timeout",
                self.schedule_to_start_timeout,
            )?,
            schedule_to_close_timeout: timeout_seconds(
                "schedule_to_close_timeout",
                self.schedule_to_close_timeout,
            )?,
            heartbeat_timeout: timeout_seconds("heartbeat_timeout", self.heartbeat_timeout)?,
        })
    }
}

/// A deferred durable leaf or nested group for [`WorkflowContext::parallel`].
///
/// Constructors capture arguments but perform no I/O. The join validates the
/// complete tree, attaches the existing parallel-group metadata to every
/// ordinary command, schedules all leaves, and then suspends.
pub enum ParallelOperation {
    Activity {
        activity_type: String,
        options: ActivityOptions,
        arguments: Result<AvroValue>,
    },
    ChildWorkflow {
        workflow_type: String,
        options: ChildWorkflowOptions,
        arguments: Result<AvroValue>,
    },
    Timer(Duration),
    Signal(String),
    Condition {
        options: ConditionWaitOptions,
        predicate: Box<dyn Fn() -> Result<bool> + Send + 'static>,
    },
    Group(Vec<ParallelOperation>),
}

impl ParallelOperation {
    pub fn activity<T: Serialize>(activity_type: impl Into<String>, args: T) -> Self {
        Self::activity_with_options(activity_type, ActivityOptions::new(), args)
    }

    pub fn activity_with_options<T: Serialize>(
        activity_type: impl Into<String>,
        options: ActivityOptions,
        args: T,
    ) -> Self {
        Self::Activity {
            activity_type: activity_type.into(),
            options,
            arguments: AvroValue::from_serialize(&args),
        }
    }

    pub fn child_workflow<T: Serialize>(
        workflow_type: impl Into<String>,
        options: ChildWorkflowOptions,
        args: T,
    ) -> Self {
        Self::ChildWorkflow {
            workflow_type: workflow_type.into(),
            options,
            arguments: AvroValue::from_serialize(&args),
        }
    }

    pub fn timer(duration: Duration) -> Self {
        Self::Timer(duration)
    }

    pub fn signal(signal_name: impl Into<String>) -> Self {
        Self::Signal(signal_name.into())
    }

    pub fn condition<F>(options: ConditionWaitOptions, predicate: F) -> Self
    where
        F: Fn() -> Result<bool> + Send + 'static,
    {
        Self::Condition {
            options,
            predicate: Box::new(predicate),
        }
    }

    pub fn group(operations: Vec<ParallelOperation>) -> Self {
        Self::Group(operations)
    }
}

#[derive(Clone, Debug)]
struct ValidatedActivityOptions {
    task_queue: Option<String>,
    retry_policy: Option<Value>,
    start_to_close_timeout: Option<u64>,
    schedule_to_start_timeout: Option<u64>,
    schedule_to_close_timeout: Option<u64>,
    heartbeat_timeout: Option<u64>,
}

fn validate_timeout_order(
    smaller_name: &'static str,
    smaller: Option<Duration>,
    larger_name: &'static str,
    larger: Option<Duration>,
) -> std::result::Result<(), ActivityOptionsError> {
    if matches!((smaller, larger), (Some(smaller), Some(larger)) if smaller > larger) {
        return Err(ActivityOptionsError::new(
            ActivityOptionsErrorKind::TimeoutOrder,
            Some(smaller_name),
            format!("{smaller_name} must be <= {larger_name}"),
        ));
    }
    Ok(())
}

fn timeout_seconds(
    field: &'static str,
    value: Option<Duration>,
) -> std::result::Result<Option<u64>, ActivityOptionsError> {
    value
        .map(|value| {
            activity_protocol_seconds(value).ok_or_else(|| {
                ActivityOptionsError::new(
                    ActivityOptionsErrorKind::TimeoutOverflow,
                    Some(field),
                    format!("{field} is too large for the worker protocol"),
                )
            })
        })
        .transpose()
}

fn duration_seconds_ceil(value: Duration) -> Option<u64> {
    value
        .as_secs()
        .checked_add(u64::from(value.subsec_nanos() > 0))
}

fn activity_protocol_seconds(value: Duration) -> Option<u64> {
    duration_seconds_ceil(value).filter(|seconds| *seconds <= i64::MAX as u64)
}

fn validate_activity_retry_policy(
    policy: &ActivityRetryPolicy,
) -> std::result::Result<Value, ActivityOptionsError> {
    if policy.max_attempts.is_none()
        && policy.backoff.is_none()
        && policy.non_retryable_error_types.is_empty()
    {
        return Err(ActivityOptionsError::new(
            ActivityOptionsErrorKind::EmptyRetryPolicy,
            Some("retry_policy"),
            "retry_policy must configure at least one field",
        ));
    }
    if policy.max_attempts == Some(0) {
        return Err(ActivityOptionsError::new(
            ActivityOptionsErrorKind::InvalidMaxAttempts,
            Some("retry_policy.max_attempts"),
            "max_attempts must be >= 1",
        ));
    }
    if policy
        .non_retryable_error_types
        .iter()
        .any(|error_type| error_type.trim().is_empty())
    {
        return Err(ActivityOptionsError::new(
            ActivityOptionsErrorKind::EmptyNonRetryableErrorType,
            Some("retry_policy.non_retryable_error_types"),
            "non_retryable_error_types must not contain empty values",
        ));
    }

    let backoff_seconds = match &policy.backoff {
        None => None,
        Some(backoff) => {
            let max_attempts = policy.max_attempts.ok_or_else(|| {
                ActivityOptionsError::new(
                    ActivityOptionsErrorKind::BackoffWithoutRetryBudget,
                    Some("retry_policy.backoff"),
                    "backoff requires max_attempts",
                )
            })?;
            let retry_count = max_attempts.saturating_sub(1) as usize;
            let intervals = match backoff {
                ActivityBackoff::Explicit(intervals) => {
                    if intervals.len() > retry_count {
                        return Err(ActivityOptionsError::new(
                            ActivityOptionsErrorKind::TooManyBackoffIntervals,
                            Some("retry_policy.backoff"),
                            "backoff interval count must not exceed max_attempts - 1",
                        ));
                    }
                    intervals.clone()
                }
                ActivityBackoff::Exponential {
                    initial_interval,
                    coefficient,
                    maximum_interval,
                } => {
                    if *coefficient < 1 {
                        return Err(ActivityOptionsError::new(
                            ActivityOptionsErrorKind::InvalidBackoffCoefficient,
                            Some("retry_policy.backoff.coefficient"),
                            "backoff coefficient must be >= 1",
                        ));
                    }
                    if retry_count > 10_000 {
                        return Err(ActivityOptionsError::new(
                            ActivityOptionsErrorKind::BackoffGenerationTooLarge,
                            Some("retry_policy.max_attempts"),
                            "generated backoff supports at most 10000 retry intervals",
                        ));
                    }
                    let mut current = *initial_interval;
                    let mut intervals = Vec::with_capacity(retry_count);
                    for _ in 0..retry_count {
                        let interval = maximum_interval
                            .map(|maximum| current.min(maximum))
                            .unwrap_or(current);
                        intervals.push(interval);
                        if maximum_interval.is_some_and(|maximum| interval == maximum) {
                            break;
                        }
                        current = current.checked_mul(*coefficient).ok_or_else(|| {
                            ActivityOptionsError::new(
                                ActivityOptionsErrorKind::BackoffOverflow,
                                Some("retry_policy.backoff"),
                                "generated backoff interval overflowed",
                            )
                        })?;
                    }
                    intervals
                }
            };
            Some(
                intervals
                    .into_iter()
                    .map(|interval| {
                        activity_protocol_seconds(interval).ok_or_else(|| {
                            ActivityOptionsError::new(
                                ActivityOptionsErrorKind::BackoffOverflow,
                                Some("retry_policy.backoff"),
                                "backoff interval is too large for the worker protocol",
                            )
                        })
                    })
                    .collect::<std::result::Result<Vec<_>, _>>()?,
            )
        }
    };

    let mut encoded = serde_json::Map::new();
    if let Some(max_attempts) = policy.max_attempts {
        encoded.insert("max_attempts".to_string(), json!(max_attempts));
    }
    if let Some(backoff_seconds) = backoff_seconds {
        encoded.insert("backoff_seconds".to_string(), json!(backoff_seconds));
    }
    if !policy.non_retryable_error_types.is_empty() {
        let mut canonical_error_types = Vec::new();
        for error_type in policy
            .non_retryable_error_types
            .iter()
            .map(|error_type| error_type.trim())
        {
            if !canonical_error_types.contains(&error_type) {
                canonical_error_types.push(error_type);
            }
        }
        encoded.insert(
            "non_retryable_error_types".to_string(),
            json!(canonical_error_types),
        );
    }
    Ok(Value::Object(encoded))
}

/// A stable, machine-readable failure raised when workflow code no longer
/// reconstructs the durable command stream recorded in history.
#[derive(Clone, Debug, Error)]
#[error("non-deterministic workflow replay ({reason}) at sequence {sequence:?}: {message}")]
pub struct ReplayFailure {
    pub reason: String,
    pub sequence: Option<u64>,
    pub expected: Option<String>,
    pub actual: Option<String>,
    pub message: String,
}

impl ReplayFailure {
    fn new(
        reason: impl Into<String>,
        sequence: Option<u64>,
        expected: Option<String>,
        actual: Option<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            reason: reason.into(),
            sequence,
            expected,
            actual,
            message: message.into(),
        }
    }
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

    /// Encode an explicit typed value, including the bytes branch that JSON
    /// serialization cannot represent.
    pub fn avro_value(value: &AvroValue) -> Result<Self> {
        encode_avro_value(value)
    }
}

/// Native adapter for the fixed language-neutral Avro Value schema.
#[derive(Clone, Debug)]
pub enum AvroValue {
    Null,
    Boolean(bool),
    Long(i64),
    Double(f64),
    Bytes(Vec<u8>),
    String(String),
    Array(Vec<AvroValue>),
    Map(BTreeMap<String, AvroValue>),
}

impl PartialEq for AvroValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Null, Self::Null) => true,
            (Self::Boolean(left), Self::Boolean(right)) => left == right,
            (Self::Long(left), Self::Long(right)) => left == right,
            (Self::Double(left), Self::Double(right)) => left.to_bits() == right.to_bits(),
            (Self::Bytes(left), Self::Bytes(right)) => left == right,
            (Self::String(left), Self::String(right)) => left == right,
            (Self::Array(left), Self::Array(right)) => left == right,
            (Self::Map(left), Self::Map(right)) => left == right,
            _ => false,
        }
    }
}

impl AvroValue {
    fn from_serialize<T: Serialize>(value: &T) -> Result<Self> {
        Self::from_serde_value(
            serde_value::to_value(value).map_err(|error| {
                Error::Codec(format!("could not adapt value for Avro: {error}"))
            })?,
        )
    }

    fn from_serde_value(value: serde_value::Value) -> Result<Self> {
        use serde_value::Value as SerdeValue;

        match value {
            SerdeValue::Unit => Ok(Self::Null),
            SerdeValue::Bool(value) => Ok(Self::Boolean(value)),
            SerdeValue::I8(value) => Ok(Self::Long(i64::from(value))),
            SerdeValue::I16(value) => Ok(Self::Long(i64::from(value))),
            SerdeValue::I32(value) => Ok(Self::Long(i64::from(value))),
            SerdeValue::I64(value) => Ok(Self::Long(value)),
            SerdeValue::U8(value) => Ok(Self::Long(i64::from(value))),
            SerdeValue::U16(value) => Ok(Self::Long(i64::from(value))),
            SerdeValue::U32(value) => Ok(Self::Long(i64::from(value))),
            SerdeValue::U64(value) => i64::try_from(value).map(Self::Long).map_err(|_| {
                Error::Codec(
                    "integer_overflow: Avro Value long must be within signed 64-bit range"
                        .to_string(),
                )
            }),
            SerdeValue::F32(value) => Self::finite_double(f64::from(value)),
            SerdeValue::F64(value) => Self::finite_double(value),
            SerdeValue::Char(value) => Ok(Self::String(value.to_string())),
            SerdeValue::String(value) => Ok(Self::String(value)),
            SerdeValue::Bytes(value) => Ok(Self::Bytes(value)),
            SerdeValue::Option(None) => Ok(Self::Null),
            SerdeValue::Option(Some(value)) | SerdeValue::Newtype(value) => {
                Self::from_serde_value(*value)
            }
            SerdeValue::Seq(values) => values
                .into_iter()
                .map(Self::from_serde_value)
                .collect::<Result<Vec<_>>>()
                .map(Self::Array),
            SerdeValue::Map(values) => values
                .into_iter()
                .map(|(key, value)| {
                    let SerdeValue::String(key) = key else {
                        return Err(Error::Codec(
                            "invalid_map_key: Avro Value map keys must be strings".to_string(),
                        ));
                    };

                    Ok((key, Self::from_serde_value(value)?))
                })
                .collect::<Result<BTreeMap<_, _>>>()
                .map(Self::Map),
        }
    }

    fn finite_double(value: f64) -> Result<Self> {
        if !value.is_finite() {
            return Err(Error::Codec(
                "non_finite_float: Avro Value doubles must be finite".to_string(),
            ));
        }

        Ok(Self::Double(value))
    }

    fn into_json(self) -> Result<Value> {
        match self {
            Self::Null => Ok(Value::Null),
            Self::Boolean(value) => Ok(Value::Bool(value)),
            Self::Long(value) => Ok(Value::Number(value.into())),
            Self::Double(value) => serde_json::Number::from_f64(value)
                .map(Value::Number)
                .ok_or_else(|| {
                    Error::Codec(
                        "non_finite_float: decoded Avro Value double is not finite".to_string(),
                    )
                }),
            Self::Bytes(value) => Ok(json!({
                "$type": "bytes",
                "base64": BASE64.encode(value),
            })),
            Self::String(value) => Ok(Value::String(value)),
            Self::Array(values) => values
                .into_iter()
                .map(Self::into_json)
                .collect::<Result<Vec<_>>>()
                .map(Value::Array),
            Self::Map(values) => values
                .into_iter()
                .map(|(key, value)| Ok((key, value.into_json()?)))
                .collect::<Result<serde_json::Map<_, _>>>()
                .map(Value::Object),
        }
    }

    fn into_serde_value(self) -> serde_value::Value {
        use serde_value::Value as SerdeValue;

        match self {
            Self::Null => SerdeValue::Unit,
            Self::Boolean(value) => SerdeValue::Bool(value),
            Self::Long(value) => SerdeValue::I64(value),
            Self::Double(value) => SerdeValue::F64(value),
            Self::Bytes(value) => SerdeValue::Bytes(value),
            Self::String(value) => SerdeValue::String(value),
            Self::Array(values) => {
                SerdeValue::Seq(values.into_iter().map(Self::into_serde_value).collect())
            }
            Self::Map(values) => SerdeValue::Map(
                values
                    .into_iter()
                    .map(|(key, value)| (SerdeValue::String(key), value.into_serde_value()))
                    .collect(),
            ),
        }
    }

    pub fn deserialize<T: DeserializeOwned>(self) -> Result<T> {
        self.into_serde_value().deserialize_into().map_err(|error| {
            Error::Codec(format!(
                "avro_value_type_mismatch: could not adapt decoded value: {error}"
            ))
        })
    }
}

impl Serialize for AvroValue {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Null => serializer.serialize_unit(),
            Self::Boolean(value) => serializer.serialize_bool(*value),
            Self::Long(value) => serializer.serialize_i64(*value),
            Self::Double(value) => serializer.serialize_f64(*value),
            Self::Bytes(value) => serializer.serialize_bytes(value),
            Self::String(value) => serializer.serialize_str(value),
            Self::Array(values) => {
                let mut sequence = serializer.serialize_seq(Some(values.len()))?;
                for value in values {
                    sequence.serialize_element(value)?;
                }
                sequence.end()
            }
            Self::Map(values) => {
                let mut map = serializer.serialize_map(Some(values.len()))?;
                for (key, value) in values {
                    map.serialize_entry(key, value)?;
                }
                map.end()
            }
        }
    }
}

pub fn encode_avro_value(value: &AvroValue) -> Result<PayloadEnvelope> {
    let datum = avro_value_to_datum(value)?;
    let datum = to_avro_datum(avro_value_ordered_map_encoding_schema()?, datum)
        .map_err(|err| Error::Codec(format!("avro_value_encode_failed: {err}")))?;
    let mut bytes = Vec::with_capacity(datum.len() + 10);
    bytes.extend_from_slice(&AVRO_SINGLE_OBJECT_MAGIC);
    bytes.extend_from_slice(&AVRO_VALUE_SCHEMA_FINGERPRINT);
    bytes.extend_from_slice(&datum);
    Ok(PayloadEnvelope {
        codec: DEFAULT_CODEC.to_string(),
        blob: BASE64.encode(bytes),
    })
}

pub fn decode_avro_value(envelope: &PayloadEnvelope) -> Result<AvroValue> {
    if envelope.codec != DEFAULT_CODEC {
        return Err(unsupported_payload_codec(&envelope.codec));
    }
    decode_avro_value_blob(&envelope.blob)
}

pub fn encode_payload<T: Serialize>(value: &T, codec: &str) -> Result<PayloadEnvelope> {
    let blob = match codec {
        DEFAULT_CODEC => encode_avro_value(&AvroValue::from_serialize(value)?)?.blob,
        other => return Err(unsupported_payload_codec(other)),
    };

    Ok(PayloadEnvelope {
        codec: codec.to_string(),
        blob,
    })
}

pub fn decode_payload<T: DeserializeOwned>(envelope: &PayloadEnvelope) -> Result<T> {
    match envelope.codec.as_str() {
        DEFAULT_CODEC => decode_avro_value(envelope)?.deserialize(),
        other => Err(unsupported_payload_codec(other)),
    }
}

fn handler_type_error<T>(
    handler_kind: HandlerKind,
    handler_name: &str,
    value_kind: HandlerValueKind,
    message: impl Into<String>,
) -> Error {
    Error::HandlerType {
        handler_kind,
        handler_name: handler_name.to_string(),
        value_kind,
        rust_type: type_name::<T>(),
        message: message.into(),
    }
}

fn decode_handler_input<T: DeserializeOwned>(
    arguments: AvroValue,
    handler_kind: HandlerKind,
    handler_name: &str,
) -> Result<T> {
    let argument = match arguments {
        AvroValue::Array(mut arguments) if arguments.len() == 1 => {
            arguments.pop().expect("one typed handler argument")
        }
        AvroValue::Array(arguments) if arguments.is_empty() => AvroValue::Null,
        AvroValue::Array(arguments) => {
            return Err(handler_type_error::<T>(
                handler_kind,
                handler_name,
                HandlerValueKind::Input,
                format!(
                    "typed handlers accept one request value, but the task carried {} arguments",
                    arguments.len()
                ),
            ));
        }
        argument => argument,
    };

    argument.deserialize().map_err(|error| {
        handler_type_error::<T>(
            handler_kind,
            handler_name,
            HandlerValueKind::Input,
            error.to_string(),
        )
    })
}

fn encode_handler_result<T: Serialize>(
    result: &T,
    handler_kind: HandlerKind,
    handler_name: &str,
) -> Result<AvroValue> {
    AvroValue::from_serialize(result).map_err(|error| {
        handler_type_error::<T>(
            handler_kind,
            handler_name,
            HandlerValueKind::Result,
            error.to_string(),
        )
    })
}

fn decode_handler_result<T: DeserializeOwned>(
    result: AvroValue,
    handler_kind: HandlerKind,
    handler_name: &str,
) -> Result<T> {
    result.deserialize().map_err(|error| {
        handler_type_error::<T>(
            handler_kind,
            handler_name,
            HandlerValueKind::Result,
            error.to_string(),
        )
    })
}

#[cfg(test)]
fn encode_value_envelope(value: &Value, codec: &str) -> Result<Value> {
    Ok(serde_json::to_value(encode_payload(value, codec)?)?)
}

fn decode_wire_value(value: &Value, fallback_codec: &str) -> Result<Value> {
    validate_payload_codec(fallback_codec)?;

    if value.is_null() {
        return Ok(Value::Null);
    }

    if let Some((codec, blob)) = payload_envelope_parts(value)? {
        return decode_blob(blob, codec);
    }

    if let Some(blob) = value.as_str() {
        return decode_blob(blob, fallback_codec);
    }

    Err(untagged_payload_value())
}

fn encode_typed_envelope(value: &AvroValue, codec: &str) -> Result<Value> {
    let envelope = match codec {
        DEFAULT_CODEC => encode_avro_value(value)?,
        other => return Err(unsupported_payload_codec(other)),
    };
    Ok(serde_json::to_value(envelope)?)
}

fn decode_wire_avro_value(value: &Value, fallback_codec: &str) -> Result<AvroValue> {
    validate_payload_codec(fallback_codec)?;

    if value.is_null() {
        return Ok(AvroValue::Null);
    }

    if let Some((codec, blob)) = payload_envelope_parts(value)? {
        validate_payload_codec(codec)?;
        return decode_avro_value_blob(blob);
    }

    if let Some(blob) = value.as_str() {
        return match fallback_codec {
            DEFAULT_CODEC => decode_avro_value_blob(blob),
            other => Err(unsupported_payload_codec(other)),
        };
    }

    Err(untagged_payload_value())
}

fn normalize_avro_arguments(value: AvroValue) -> AvroValue {
    match value {
        AvroValue::Null => AvroValue::Array(Vec::new()),
        AvroValue::Array(_) => value,
        other => AvroValue::Array(vec![other]),
    }
}

fn decode_blob(blob: &str, codec: &str) -> Result<Value> {
    match codec {
        DEFAULT_CODEC => decode_avro_value_blob(blob)?.into_json(),
        other => Err(unsupported_payload_codec(other)),
    }
}

fn validate_payload_codec(codec: &str) -> Result<()> {
    match codec {
        DEFAULT_CODEC => Ok(()),
        MISSING_TASK_PAYLOAD_CODEC => {
            Err(invalid_task_payload_codec("task payload_codec is missing"))
        }
        NULL_TASK_PAYLOAD_CODEC => Err(invalid_task_payload_codec("task payload_codec is null")),
        NON_STRING_TASK_PAYLOAD_CODEC => Err(invalid_task_payload_codec(
            "task payload_codec must be a string",
        )),
        other => Err(unsupported_payload_codec(other)),
    }
}

fn invalid_task_payload_codec(reason: &str) -> Error {
    Error::Codec(format!(
        "unsupported_payload_codec: {reason}; Durable Workflow 2.0 requires an explicit string payload_codec=\"avro\" before worker task execution"
    ))
}

fn payload_envelope_parts(value: &Value) -> Result<Option<(&str, &str)>> {
    let Some(object) = value.as_object() else {
        return Ok(None);
    };
    if !object.contains_key("codec") && !object.contains_key("blob") {
        return Ok(None);
    }

    let codec = object
        .get("codec")
        .and_then(Value::as_str)
        .ok_or_else(invalid_payload_envelope)?;
    validate_payload_codec(codec)?;
    let blob = object
        .get("blob")
        .and_then(Value::as_str)
        .ok_or_else(invalid_payload_envelope)?;
    Ok(Some((codec, blob)))
}

fn invalid_payload_envelope() -> Error {
    Error::Codec(
        "invalid_payload_envelope: durable payloads must use an object with string codec=\"avro\" and blob fields"
            .to_string(),
    )
}

fn validate_workflow_task_commands(commands: &[Value]) -> Result<()> {
    for command in commands {
        let Some(command) = command.as_object() else {
            continue;
        };
        let Some(command_type) = command.get("type").and_then(Value::as_str) else {
            continue;
        };
        let Some(payload_field) = workflow_command_payload_field(command_type) else {
            continue;
        };

        if let Some(codec) = command.get("payload_codec") {
            let codec = codec.as_str().ok_or_else(invalid_payload_envelope)?;
            validate_payload_codec(codec)?;
        }

        let payload = command
            .get(payload_field)
            .ok_or_else(invalid_payload_envelope)?;
        validate_outbound_payload_envelope(payload)?;
    }
    Ok(())
}

fn workflow_completion_protocol_version(commands: &[Value]) -> &'static str {
    if commands.iter().any(|command| {
        command.get("type").and_then(Value::as_str) == Some("open_condition_wait")
            && command
                .get("condition_wait_occurrence_id")
                .and_then(Value::as_str)
                .is_some_and(|occurrence_id| !occurrence_id.is_empty())
    }) {
        CONDITION_WAIT_OCCURRENCE_IDENTITY_MINIMUM_WORKER_PROTOCOL_VERSION
    } else if commands.iter().any(|command| {
        command.get("type").and_then(Value::as_str) == Some("upsert_search_attributes")
            && command.get("attribute_types").is_some()
    }) {
        TYPED_SEARCH_ATTRIBUTES_MINIMUM_WORKER_PROTOCOL_VERSION
    } else if commands
        .iter()
        .any(|command| command.get("type").and_then(Value::as_str) == Some("upsert_memo"))
    {
        MEMO_UPSERT_MINIMUM_WORKER_PROTOCOL_VERSION
    } else if commands
        .iter()
        .any(|command| command.get("type").and_then(Value::as_str) == Some("open_condition_wait"))
    {
        CONDITION_WAIT_MINIMUM_WORKER_PROTOCOL_VERSION
    } else if commands.iter().any(|command| {
        command.get("type").and_then(Value::as_str) == Some("upsert_search_attributes")
    }) {
        SEARCH_ATTRIBUTE_UPDATE_MINIMUM_WORKER_PROTOCOL_VERSION
    } else {
        WORKER_PROTOCOL_VERSION
    }
}

fn workflow_completion_protocol_version_with_message_streams(
    commands: &[Value],
    has_message_stream_metadata: bool,
) -> &'static str {
    let command_protocol = workflow_completion_protocol_version(commands);
    if has_message_stream_metadata && !worker_protocol_supports_message_streams(command_protocol) {
        MESSAGE_STREAMS_MINIMUM_WORKER_PROTOCOL_VERSION
    } else {
        command_protocol
    }
}

fn workflow_command_payload_field(command_type: &str) -> Option<&'static str> {
    match command_type {
        "complete_workflow" | "complete_update" | "record_side_effect" => Some("result"),
        "schedule_activity" | "start_child_workflow" | "continue_as_new" => Some("arguments"),
        "start_service_operation" => Some("request_payload"),
        "upsert_memo" => Some("entries"),
        _ => None,
    }
}

fn validate_outbound_payload_envelope(value: &Value) -> Result<()> {
    let Some((codec, blob)) = payload_envelope_parts(value)? else {
        return Err(untagged_payload_value());
    };
    validate_payload_codec(codec)?;
    decode_avro_value_blob(blob)?;
    Ok(())
}

fn unsupported_payload_codec(codec: &str) -> Error {
    Error::Codec(format!(
        "unsupported_payload_codec: workflow payload codec {codec:?} is not supported by Durable Workflow 2.0; use codec=\"avro\" with the fixed Avro Value schema and single-object framing. JSON remains the HTTP document transport, not a workflow payload codec"
    ))
}

fn untagged_payload_value() -> Error {
    Error::Codec(
        "unsupported_payload_codec: untagged durable payload values are not supported by Durable Workflow 2.0; use codec=\"avro\" with the fixed Avro Value schema and single-object framing. JSON remains the HTTP document transport, not a workflow payload codec"
            .to_string(),
    )
}

fn decode_avro_value_blob(blob: &str) -> Result<AvroValue> {
    let bytes = BASE64.decode(blob).map_err(|err| {
        Error::Codec(format!(
            "invalid_payload_framing: expected strict base64 Avro single-object bytes: {err}"
        ))
    })?;

    if serde_json::from_slice::<Value>(&bytes).is_ok() {
        return Err(unsupported_payload_codec("json"));
    }

    if bytes.len() < 10 || bytes[..2] != AVRO_SINGLE_OBJECT_MAGIC {
        return Err(Error::Codec(
            "invalid_payload_framing: expected Avro single-object magic c301".to_string(),
        ));
    }

    let fingerprint: [u8; 8] = bytes[2..10]
        .try_into()
        .map_err(|_| Error::Codec("invalid Avro fingerprint length".to_string()))?;
    if fingerprint != AVRO_VALUE_SCHEMA_FINGERPRINT {
        return Err(Error::Codec(format!(
            "unsupported_payload_schema: unknown CRC-64-AVRO fingerprint {}",
            fingerprint
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        )));
    }

    let mut datum_reader = StrictAvroDatumReader::new(&bytes[10..]);
    // The current fingerprint selects the current immutable schema, so reader
    // resolution would only re-walk the same recursive union. Future retained
    // writer fingerprints supply a distinct reader schema in this branch.
    let datum = from_avro_datum(avro_value_schema()?, &mut datum_reader, None);
    if datum_reader.truncated {
        return Err(Error::Codec(
            "invalid_payload_framing: truncated Avro Value datum".to_string(),
        ));
    }
    let datum = datum.map_err(|err| {
        Error::Codec(format!(
            "invalid_payload_framing: malformed Avro Value datum: {err}"
        ))
    })?;
    if datum_reader.remaining() != 0 {
        return Err(Error::Codec(format!(
            "invalid_payload_framing: {} trailing bytes after Avro Value datum",
            datum_reader.remaining()
        )));
    }
    avro_value_from_datum(datum)
}

struct StrictAvroDatumReader<'a> {
    bytes: &'a [u8],
    offset: usize,
    truncated: bool,
}

impl<'a> StrictAvroDatumReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            offset: 0,
            truncated: false,
        }
    }

    fn remaining(&self) -> usize {
        self.bytes.len() - self.offset
    }
}

impl Read for StrictAvroDatumReader<'_> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let count = buffer.len().min(self.remaining());
        buffer[..count].copy_from_slice(&self.bytes[self.offset..self.offset + count]);
        self.offset += count;
        if count < buffer.len() {
            self.truncated = true;
        }

        Ok(count)
    }
}

fn avro_value_to_datum(value: &AvroValue) -> Result<AvroDatum> {
    let branch = match value {
        AvroValue::Null => AvroDatum::Union(0, Box::new(AvroDatum::Null)),
        AvroValue::Boolean(value) => AvroDatum::Union(
            1,
            Box::new(AvroDatum::Record(vec![(
                "boolean".to_string(),
                AvroDatum::Boolean(*value),
            )])),
        ),
        AvroValue::Long(value) => AvroDatum::Union(
            2,
            Box::new(AvroDatum::Record(vec![(
                "long".to_string(),
                AvroDatum::Long(*value),
            )])),
        ),
        AvroValue::Double(value) => {
            if !value.is_finite() {
                return Err(Error::Codec(
                    "non_finite_float: Avro Value doubles must be finite".to_string(),
                ));
            }
            AvroDatum::Union(
                3,
                Box::new(AvroDatum::Record(vec![(
                    "double".to_string(),
                    AvroDatum::Double(*value),
                )])),
            )
        }
        AvroValue::Bytes(value) => AvroDatum::Union(
            4,
            Box::new(AvroDatum::Record(vec![(
                "bytes".to_string(),
                AvroDatum::Bytes(value.clone()),
            )])),
        ),
        AvroValue::String(value) => AvroDatum::Union(
            5,
            Box::new(AvroDatum::Record(vec![(
                "string".to_string(),
                AvroDatum::String(value.clone()),
            )])),
        ),
        AvroValue::Array(values) => AvroDatum::Union(
            6,
            Box::new(AvroDatum::Record(vec![(
                "items".to_string(),
                AvroDatum::Array(
                    values
                        .iter()
                        .map(avro_value_to_datum)
                        .collect::<Result<Vec<_>>>()?,
                ),
            )])),
        ),
        AvroValue::Map(values) => AvroDatum::Union(
            7,
            Box::new(AvroDatum::Record(vec![(
                "entries".to_string(),
                AvroDatum::Array(
                    values
                        .iter()
                        .map(|(key, value)| {
                            Ok(AvroDatum::Record(vec![
                                ("key".to_string(), AvroDatum::String(key.clone())),
                                ("value".to_string(), avro_value_to_datum(value)?),
                            ]))
                        })
                        .collect::<Result<Vec<_>>>()?,
                ),
            )])),
        ),
    };
    Ok(AvroDatum::Record(vec![("value".to_string(), branch)]))
}

fn avro_value_from_datum(datum: AvroDatum) -> Result<AvroValue> {
    let AvroDatum::Record(mut outer) = datum else {
        return Err(Error::Codec(
            "invalid_payload_framing: datum is not a Value record".to_string(),
        ));
    };
    let (_, branch) = outer
        .pop()
        .filter(|(name, _)| name == "value")
        .ok_or_else(|| Error::Codec("invalid_payload_framing: Value field missing".to_string()))?;
    let AvroDatum::Union(_, branch) = branch else {
        return Err(Error::Codec(
            "invalid_payload_framing: invalid Value union".to_string(),
        ));
    };
    match *branch {
        AvroDatum::Null => Ok(AvroValue::Null),
        AvroDatum::Record(mut fields) => {
            let (name, value) = fields.pop().ok_or_else(|| {
                Error::Codec("invalid_payload_framing: empty Value branch".to_string())
            })?;
            match (name.as_str(), value) {
                ("boolean", AvroDatum::Boolean(value)) => Ok(AvroValue::Boolean(value)),
                ("long", AvroDatum::Long(value)) => Ok(AvroValue::Long(value)),
                ("double", AvroDatum::Double(value)) if value.is_finite() => {
                    Ok(AvroValue::Double(value))
                }
                ("bytes", AvroDatum::Bytes(value)) => Ok(AvroValue::Bytes(value)),
                ("string", AvroDatum::String(value)) => Ok(AvroValue::String(value)),
                ("items", AvroDatum::Array(values)) => values
                    .into_iter()
                    .map(avro_value_from_datum)
                    .collect::<Result<Vec<_>>>()
                    .map(AvroValue::Array),
                ("entries", AvroDatum::Map(values)) => values
                    .into_iter()
                    .map(|(key, value)| Ok((key, avro_value_from_datum(value)?)))
                    .collect::<Result<BTreeMap<_, _>>>()
                    .map(AvroValue::Map),
                _ => Err(Error::Codec(
                    "invalid_payload_framing: unknown Value branch".to_string(),
                )),
            }
        }
        _ => Err(Error::Codec(
            "invalid_payload_framing: invalid Value branch".to_string(),
        )),
    }
}

fn avro_value_schema() -> Result<&'static Schema> {
    match AVRO_VALUE_SCHEMA.get_or_init(|| {
        Schema::parse_str(AVRO_VALUE_SCHEMA_JSON)
            .map_err(|err| format!("could not parse Avro Value schema: {err}"))
    }) {
        Ok(schema) => Ok(schema),
        Err(message) => Err(Error::Codec(message.clone())),
    }
}

fn avro_value_ordered_map_encoding_schema() -> Result<&'static Schema> {
    match AVRO_VALUE_ORDERED_MAP_ENCODING_SCHEMA.get_or_init(|| {
        // Apache Avro's Value::Map uses a randomized HashMap. Arrays and maps
        // have the same block representation when each ordered array record
        // contains the map key followed by its value, so this encoding-only
        // adaptation lets the official encoder retain BTreeMap wire order.
        let mut schema: Value = serde_json::from_str(AVRO_VALUE_SCHEMA_JSON)
            .map_err(|err| format!("could not read packaged Avro Value schema: {err}"))?;
        let entries_schema = schema
            .pointer_mut("/fields/0/type/7/fields/0/type")
            .ok_or_else(|| "packaged Avro Value map schema is missing".to_string())?;
        if *entries_schema != json!({"type": "map", "values": "Value"}) {
            return Err("packaged Avro Value map schema changed unexpectedly".to_string());
        }
        *entries_schema = json!({
            "type": "array",
            "items": {
                "type": "record",
                "name": "MapEntry",
                "fields": [
                    {"name": "key", "type": "string"},
                    {"name": "value", "type": "Value"}
                ]
            }
        });
        Schema::parse_str(&schema.to_string())
            .map_err(|err| format!("could not parse ordered-map Avro Value schema: {err}"))
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
        self.start_workflow_with_options(
            workflow_type,
            task_queue,
            workflow_id,
            WorkflowStartOptions::default(),
            input,
        )
        .await
    }

    /// Start a workflow with explicit server-enforced execution and run
    /// deadlines.
    pub async fn start_workflow_with_options<T: Serialize>(
        &self,
        workflow_type: &str,
        task_queue: &str,
        workflow_id: &str,
        options: WorkflowStartOptions,
        input: T,
    ) -> Result<WorkflowHandle> {
        options.validate()?;
        let input = normalize_avro_arguments(AvroValue::from_serialize(&input)?);
        let input_envelope = encode_typed_envelope(&input, DEFAULT_CODEC)?;
        let body = json!({
            "workflow_id": workflow_id,
            "workflow_type": workflow_type,
            "task_queue": task_queue,
            "input": input_envelope,
            "execution_timeout_seconds": options.execution_timeout_seconds,
            "run_timeout_seconds": options.run_timeout_seconds
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
        self.signal_workflow_target(workflow_id, None, signal_name, input)
            .await
    }

    /// Append one idempotently identified item to an instance-scoped input stream.
    pub async fn append_message_stream<T: Serialize>(
        &self,
        workflow_id: &str,
        stream_name: &str,
        message_id: &str,
        input: T,
    ) -> Result<Value> {
        let input = normalize_avro_arguments(AvroValue::from_serialize(&input)?);
        let body = json!({
            "message_id": message_id,
            "input": encode_typed_envelope(&input, DEFAULT_CODEC)?
        });
        self.request_json(
            reqwest::Method::POST,
            &format!("/workflows/{workflow_id}/message-streams/{stream_name}/messages"),
            RequestProtocol::ControlPlane,
            Some(&body),
        )
        .await
    }

    /// Signal only if `run_id` is still the current run for this instance.
    pub async fn signal_workflow_run<T: Serialize>(
        &self,
        workflow_id: &str,
        run_id: &str,
        signal_name: &str,
        input: T,
    ) -> Result<Value> {
        self.signal_workflow_target(workflow_id, Some(run_id), signal_name, input)
            .await
    }

    async fn signal_workflow_target<T: Serialize>(
        &self,
        workflow_id: &str,
        run_id: Option<&str>,
        signal_name: &str,
        input: T,
    ) -> Result<Value> {
        validate_user_signal_name(signal_name)?;
        let input = normalize_avro_arguments(AvroValue::from_serialize(&input)?);
        let input_envelope = encode_typed_envelope(&input, DEFAULT_CODEC)?;
        let body = json!({
            "input": input_envelope
        });
        let path = match run_id {
            Some(run_id) => {
                format!("/workflows/{workflow_id}/runs/{run_id}/signal/{signal_name}")
            }
            None => format!("/workflows/{workflow_id}/signal/{signal_name}"),
        };
        self.request_json(
            reqwest::Method::POST,
            &path,
            RequestProtocol::ControlPlane,
            Some(&body),
        )
        .await
    }

    /// Request cooperative cancellation of the current run for an instance.
    pub async fn cancel_workflow(
        &self,
        workflow_id: &str,
        options: WorkflowCommandOptions,
    ) -> Result<WorkflowCommandResult> {
        self.workflow_command(workflow_id, None, WorkflowCommandKind::Cancel, options)
            .await
    }

    /// Request cooperative cancellation only if `run_id` is still current.
    pub async fn cancel_workflow_run(
        &self,
        workflow_id: &str,
        run_id: &str,
        options: WorkflowCommandOptions,
    ) -> Result<WorkflowCommandResult> {
        self.workflow_command(
            workflow_id,
            Some(run_id),
            WorkflowCommandKind::Cancel,
            options,
        )
        .await
    }

    /// Forcefully terminate the current run for an instance.
    pub async fn terminate_workflow(
        &self,
        workflow_id: &str,
        options: WorkflowCommandOptions,
    ) -> Result<WorkflowCommandResult> {
        self.workflow_command(workflow_id, None, WorkflowCommandKind::Terminate, options)
            .await
    }

    /// Forcefully terminate only if `run_id` is still current.
    pub async fn terminate_workflow_run(
        &self,
        workflow_id: &str,
        run_id: &str,
        options: WorkflowCommandOptions,
    ) -> Result<WorkflowCommandResult> {
        self.workflow_command(
            workflow_id,
            Some(run_id),
            WorkflowCommandKind::Terminate,
            options,
        )
        .await
    }

    async fn workflow_command(
        &self,
        workflow_id: &str,
        run_id: Option<&str>,
        command: WorkflowCommandKind,
        options: WorkflowCommandOptions,
    ) -> Result<WorkflowCommandResult> {
        let path = match run_id {
            Some(run_id) => format!(
                "/workflows/{workflow_id}/runs/{run_id}/{}",
                command.as_str()
            ),
            None => format!("/workflows/{workflow_id}/{}", command.as_str()),
        };
        let data = match self
            .request_json(
                reqwest::Method::POST,
                &path,
                RequestProtocol::ControlPlane,
                Some(&options),
            )
            .await
        {
            Ok(data) => data,
            Err(Error::Http { status, body }) => {
                return Err(Error::WorkflowCommandRejected(workflow_command_rejection(
                    command,
                    status,
                    body,
                    workflow_id,
                    run_id,
                )));
            }
            Err(error) => return Err(error),
        };

        Ok(workflow_command_result(command, data, workflow_id, run_id))
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
        self.query_workflow_target(workflow_id, None, query_name, input)
            .await
    }

    /// Query only if `run_id` is still current, preventing accidental retargeting.
    pub async fn query_workflow_run<T: Serialize>(
        &self,
        workflow_id: &str,
        run_id: &str,
        query_name: &str,
        input: T,
    ) -> Result<Value> {
        self.query_workflow_target(workflow_id, Some(run_id), query_name, input)
            .await
    }

    /// Query a workflow and return the lossless fixed Avro Value result.
    pub async fn query_workflow_avro_value<T: Serialize>(
        &self,
        workflow_id: &str,
        query_name: &str,
        input: T,
    ) -> Result<AvroValue> {
        self.query_workflow_avro_value_target(workflow_id, None, query_name, input)
            .await
    }

    /// Query a selected run and return the lossless fixed Avro Value result.
    pub async fn query_workflow_run_avro_value<T: Serialize>(
        &self,
        workflow_id: &str,
        run_id: &str,
        query_name: &str,
        input: T,
    ) -> Result<AvroValue> {
        self.query_workflow_avro_value_target(workflow_id, Some(run_id), query_name, input)
            .await
    }

    async fn query_workflow_avro_value_target<T: Serialize>(
        &self,
        workflow_id: &str,
        run_id: Option<&str>,
        query_name: &str,
        input: T,
    ) -> Result<AvroValue> {
        let input = normalize_avro_arguments(AvroValue::from_serialize(&input)?);
        let body = json!({"input": encode_typed_envelope(&input, DEFAULT_CODEC)?});
        let path = match run_id {
            Some(run_id) => {
                format!("/workflows/{workflow_id}/runs/{run_id}/query/{query_name}")
            }
            None => format!("/workflows/{workflow_id}/query/{query_name}"),
        };
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

        let envelope = response
            .get("result_envelope")
            .filter(|envelope| !envelope.is_null())
            .ok_or_else(|| {
                Error::Codec(
                    "missing_payload_envelope: typed query result requires result_envelope"
                        .to_string(),
                )
            })?;
        decode_wire_avro_value(envelope, DEFAULT_CODEC)
    }

    async fn query_workflow_target<T: Serialize>(
        &self,
        workflow_id: &str,
        run_id: Option<&str>,
        query_name: &str,
        input: T,
    ) -> Result<Value> {
        let input = normalize_avro_arguments(AvroValue::from_serialize(&input)?);
        let input_envelope = encode_typed_envelope(&input, DEFAULT_CODEC)?;
        let body = json!({
            "input": input_envelope
        });
        let path = match run_id {
            Some(run_id) => {
                format!("/workflows/{workflow_id}/runs/{run_id}/query/{query_name}")
            }
            None => format!("/workflows/{workflow_id}/query/{query_name}"),
        };
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

    /// Send a synchronous update using fixed Avro Value arguments.
    pub async fn update_workflow<T: Serialize>(
        &self,
        workflow_id: &str,
        update_name: &str,
        input: T,
        request_id: Option<&str>,
    ) -> Result<Value> {
        let response = self
            .update_workflow_response(workflow_id, update_name, input, request_id)
            .await?;
        if let Some(envelope) = response
            .get("result_envelope")
            .filter(|envelope| !envelope.is_null())
        {
            return decode_wire_value(envelope, DEFAULT_CODEC);
        }
        Ok(response.get("result").cloned().unwrap_or(response))
    }

    /// Send a synchronous update and retain a bytes-capable Avro result.
    pub async fn update_workflow_avro_value<T: Serialize>(
        &self,
        workflow_id: &str,
        update_name: &str,
        input: T,
        request_id: Option<&str>,
    ) -> Result<AvroValue> {
        let response = self
            .update_workflow_response(workflow_id, update_name, input, request_id)
            .await?;
        let envelope = response
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

    async fn update_workflow_response<T: Serialize>(
        &self,
        workflow_id: &str,
        update_name: &str,
        input: T,
        request_id: Option<&str>,
    ) -> Result<Value> {
        let input = normalize_avro_arguments(AvroValue::from_serialize(&input)?);
        let mut body = json!({
            "input": encode_typed_envelope(&input, DEFAULT_CODEC)?,
            "wait_for": "completed",
        });
        if let Some(request_id) = request_id {
            body["request_id"] = json!(request_id);
        }
        self.request_json(
            reqwest::Method::POST,
            &format!("/workflows/{workflow_id}/update/{update_name}"),
            RequestProtocol::ControlPlane,
            Some(&body),
        )
        .await
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

    /// Describe one selected run, including historical terminal runs.
    pub async fn describe_workflow_run(
        &self,
        workflow_id: &str,
        run_id: &str,
    ) -> Result<WorkflowDescription> {
        let path = format!("/workflows/{workflow_id}/runs/{run_id}");
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

    fn workflow_stream_path(workflow_id: &str, run_id: &str, stream_name: Option<&str>) -> String {
        let mut path = format!(
            "/workflows/{}/runs/{}/streams",
            percent_encode_path_segment(workflow_id),
            percent_encode_path_segment(run_id),
        );
        if let Some(stream_name) = stream_name {
            path.push('/');
            path.push_str(&percent_encode_path_segment(stream_name));
        }
        path
    }

    /// List the run-scoped output streams already opened by a workflow.
    pub async fn list_workflow_streams(
        &self,
        workflow_id: &str,
        run_id: &str,
    ) -> Result<Vec<WorkflowStreamDescription>> {
        let response: WorkflowStreamListResponse = self
            .request_json(
                reqwest::Method::GET,
                &Self::workflow_stream_path(workflow_id, run_id, None),
                RequestProtocol::ControlPlane,
                Option::<&Value>::None,
            )
            .await?;
        Ok(response.streams)
    }

    /// Describe stream lifecycle, offsets, pending count, and terminal error.
    pub async fn describe_workflow_stream(
        &self,
        workflow_id: &str,
        run_id: &str,
        stream_name: &str,
    ) -> Result<WorkflowStreamDescription> {
        let response: WorkflowStreamDescriptionResponse = self
            .request_json(
                reqwest::Method::GET,
                &Self::workflow_stream_path(workflow_id, run_id, Some(stream_name)),
                RequestProtocol::ControlPlane,
                Option::<&Value>::None,
            )
            .await?;
        Ok(response.stream)
    }

    /// Read one bounded page beginning at a zero-based offset.
    ///
    /// Delivery is at least once: persist `next_offset` only after processing
    /// the page. The future is cancellation-safe; dropping it cancels the
    /// in-flight request. Long polling is capped at 60 seconds by the SDK and
    /// service contract.
    pub async fn subscribe_workflow_stream(
        &self,
        workflow_id: &str,
        run_id: &str,
        stream_name: &str,
        from_offset: u64,
        max_items: usize,
        wait: Duration,
    ) -> Result<WorkflowStreamPage> {
        let max_items = max_items.clamp(1, 500);
        let wait_seconds = wait.as_secs().min(MAX_LONG_POLL_TIMEOUT_SECONDS);
        let path = format!(
            "{}/items?from={from_offset}&max_items={max_items}&wait_seconds={wait_seconds}",
            Self::workflow_stream_path(workflow_id, run_id, Some(stream_name)),
        );
        let response: WorkflowStreamPageResponse = self
            .request_json_with_timeout(
                reqwest::Method::GET,
                &path,
                RequestProtocol::ControlPlane,
                Option::<&Value>::None,
                Duration::from_secs(wait_seconds.saturating_add(5).max(5)),
            )
            .await?;

        let items = response
            .items
            .into_iter()
            .map(|raw| {
                let offset = raw.get("offset").and_then(Value::as_u64).unwrap_or(0);
                let envelope = raw.get("payload").cloned();
                let payload = envelope
                    .as_ref()
                    .filter(|value| value.get("blob").is_some())
                    .map(|value| decode_wire_avro_value(value, DEFAULT_CODEC))
                    .transpose()?
                    .map(AvroValue::into_json)
                    .transpose()?;
                Ok(WorkflowStreamItem {
                    offset,
                    payload,
                    payload_envelope: envelope,
                    payload_reference: raw
                        .get("payload_reference")
                        .and_then(Value::as_str)
                        .map(str::to_string),
                    payload_codec: raw
                        .get("payload_codec")
                        .and_then(Value::as_str)
                        .map(str::to_string),
                    idempotency_key: raw
                        .get("idempotency_key")
                        .and_then(Value::as_str)
                        .map(str::to_string),
                    item_type: raw
                        .get("item_type")
                        .and_then(Value::as_str)
                        .map(str::to_string),
                    content_type: raw
                        .get("content_type")
                        .and_then(Value::as_str)
                        .map(str::to_string),
                    origin: raw
                        .get("origin")
                        .and_then(Value::as_str)
                        .map(str::to_string),
                    origin_reference: raw
                        .get("origin_reference")
                        .and_then(Value::as_str)
                        .map(str::to_string),
                    emitted_at: raw
                        .get("emitted_at")
                        .and_then(Value::as_str)
                        .map(str::to_string),
                    raw,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(WorkflowStreamPage {
            stream: response.stream,
            items,
            next_offset: response.next_offset,
            terminal: response.terminal,
        })
    }

    /// Append inline Avro envelopes or opaque external payload references.
    pub async fn append_workflow_stream(
        &self,
        workflow_id: &str,
        run_id: &str,
        stream_name: &str,
        items: &[WorkflowStreamAppendItem],
        max_pending_items: Option<u64>,
    ) -> Result<WorkflowStreamAppendResult> {
        if items.is_empty() {
            return Err(Error::Codec(
                "workflow_stream_items_empty: append requires at least one item".to_string(),
            ));
        }
        let mut body = json!({
            "items": items
                .iter()
                .map(|item| item.wire_value(None))
                .collect::<Vec<_>>(),
        });
        if let Some(max_pending_items) = max_pending_items {
            if max_pending_items == 0 {
                return Err(Error::Codec(
                    "workflow_stream_pending_limit_invalid: max_pending_items must be positive"
                        .to_string(),
                ));
            }
            body["max_pending_items"] = json!(max_pending_items);
        }
        let response: WorkflowStreamAppendResponse = self
            .request_json(
                reqwest::Method::POST,
                &format!(
                    "{}/items",
                    Self::workflow_stream_path(workflow_id, run_id, Some(stream_name)),
                ),
                RequestProtocol::ControlPlane,
                Some(&body),
            )
            .await?;
        Ok(WorkflowStreamAppendResult {
            stream: response.stream,
            accepted_offsets: response.accepted_offsets,
            accepted: response.accepted,
            deduped: response.deduped,
        })
    }

    /// Close a stream, or mark it errored when `error_reason` is supplied.
    pub async fn close_workflow_stream(
        &self,
        workflow_id: &str,
        run_id: &str,
        stream_name: &str,
        error_reason: Option<&str>,
        retention_seconds: Option<u64>,
    ) -> Result<WorkflowStreamDescription> {
        let mut body = json!({});
        if let Some(error_reason) = error_reason {
            body["error_reason"] = json!(error_reason);
        }
        if let Some(retention_seconds) = retention_seconds {
            if retention_seconds == 0 {
                return Err(Error::Codec(
                    "workflow_stream_retention_invalid: retention_seconds must be positive"
                        .to_string(),
                ));
            }
            body["retention_seconds"] = json!(retention_seconds);
        }
        let response: WorkflowStreamDescriptionResponse = self
            .request_json(
                reqwest::Method::POST,
                &format!(
                    "{}/close",
                    Self::workflow_stream_path(workflow_id, run_id, Some(stream_name)),
                ),
                RequestProtocol::ControlPlane,
                Some(&body),
            )
            .await?;
        Ok(response.stream)
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
        self.register_worker_with_command_contracts(
            worker_id,
            task_queue,
            supported_workflow_types,
            supported_activity_types,
            max_concurrent_workflow_tasks,
            max_concurrent_activity_tasks,
            capabilities,
            Value::Object(serde_json::Map::new()),
        )
        .await
    }

    /// Register a worker and advertise its named query and update handlers.
    ///
    /// This Rust SDK cannot execute synchronous pre-accept update validation,
    /// so a workflow contract with a non-empty or malformed
    /// `update_validators` declaration returns
    /// [`Error::UnsupportedUpdateValidators`] before registration transport.
    #[allow(clippy::too_many_arguments)]
    pub async fn register_worker_with_command_contracts(
        &self,
        worker_id: &str,
        task_queue: &str,
        supported_workflow_types: Vec<String>,
        supported_activity_types: Vec<String>,
        max_concurrent_workflow_tasks: usize,
        max_concurrent_activity_tasks: usize,
        capabilities: Vec<String>,
        workflow_command_contracts: Value,
    ) -> Result<RegisterWorkerResponse> {
        if let Some(contracts) = workflow_command_contracts.as_object() {
            for (workflow_type, contract) in contracts {
                let Some(update_validators) = contract.get("update_validators") else {
                    continue;
                };
                if !update_validators
                    .as_array()
                    .is_some_and(|validators| validators.is_empty())
                {
                    return Err(Error::UnsupportedUpdateValidators {
                        workflow_type: workflow_type.clone(),
                    });
                }
            }
        }

        let mut body = json!({
            "worker_id": worker_id,
            "task_queue": task_queue,
            "runtime": "rust",
            "sdk_version": SDK_VERSION,
            "supported_workflow_types": supported_workflow_types,
            "supported_activity_types": supported_activity_types,
            "capabilities": capabilities,
            "capability_manifest": portable_worker_affinity_capability_manifest(),
            "max_concurrent_workflow_tasks": max_concurrent_workflow_tasks,
            "max_concurrent_activity_tasks": max_concurrent_activity_tasks
        });
        if workflow_command_contracts
            .as_object()
            .is_some_and(|contracts| !contracts.is_empty())
        {
            body["workflow_command_contracts"] = workflow_command_contracts;
        }

        self.request_json(
            reqwest::Method::POST,
            "/worker/register",
            RequestProtocol::Worker(WORKER_PROTOCOL_VERSION),
            Some(&body),
        )
        .await
    }

    /// Gracefully remove one worker's registration through the worker plane.
    ///
    /// This operation is separate from operator-facing worker management. It
    /// uses worker-protocol authentication and returns the server's lease
    /// recovery result.
    pub async fn deregister_worker_registration(
        &self,
        worker_id: &str,
    ) -> Result<WorkerDeregistrationEnvelope> {
        let path = format!(
            "/worker/registrations/{}",
            percent_encode_path_segment(worker_id)
        );
        self.request_json(
            reqwest::Method::DELETE,
            &path,
            RequestProtocol::Worker(WORKER_PROTOCOL_VERSION),
            Option::<&Value>::None,
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
        Ok(self
            .poll_query_task_response(worker_id, task_queue, timeout)
            .await?
            .task)
    }

    /// Poll a query task while preserving server stop and drain metadata.
    pub async fn poll_query_task_response(
        &self,
        worker_id: &str,
        task_queue: &str,
        timeout: Duration,
    ) -> Result<PollQueryTaskResponse> {
        let poll_request_id = unique_request_id("rust-query-poll");
        self.poll_query_task_response_with_request_id(
            worker_id,
            task_queue,
            timeout,
            &poll_request_id,
            1,
        )
        .await
    }

    async fn poll_query_task_response_with_request_id(
        &self,
        worker_id: &str,
        task_queue: &str,
        timeout: Duration,
        poll_request_id: &str,
        transport_retries: usize,
    ) -> Result<PollQueryTaskResponse> {
        let timeout_seconds = long_poll_timeout_seconds(timeout);
        let body = json!({
            "worker_id": worker_id,
            "task_queue": task_queue,
            "poll_request_id": poll_request_id,
            "timeout_seconds": timeout_seconds,
        });
        self.poll_request_json(
            "/worker/query-tasks/poll",
            RequestProtocol::Worker(QUERY_TASK_MINIMUM_WORKER_PROTOCOL_VERSION),
            &body,
            timeout + Duration::from_secs(5),
            transport_retries,
        )
        .await
    }

    /// Complete a query task without appending workflow history.
    pub async fn complete_query_task<T: Serialize>(
        &self,
        query_task_id: &str,
        lease_owner: &str,
        query_task_attempt: u64,
        result: T,
        codec: &str,
    ) -> Result<Value> {
        let typed_result = AvroValue::from_serialize(&result)?;
        let result_envelope = encode_typed_envelope(&typed_result, codec)?;
        self.complete_query_task_with_envelope(
            query_task_id,
            lease_owner,
            query_task_attempt,
            typed_result.into_json()?,
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
        let poll_request_id = unique_request_id("rust-workflow-poll");
        self.poll_workflow_task_response_with_request_id(
            worker_id,
            task_queue,
            timeout,
            &poll_request_id,
            1,
        )
        .await
    }

    async fn poll_workflow_task_response_with_request_id(
        &self,
        worker_id: &str,
        task_queue: &str,
        timeout: Duration,
        poll_request_id: &str,
        transport_retries: usize,
    ) -> Result<PollWorkflowTaskResponse> {
        let body = json!({
            "worker_id": worker_id,
            "task_queue": task_queue,
            "poll_request_id": poll_request_id,
            "timeout_seconds": long_poll_timeout_seconds(timeout),
        });
        let mut data: PollWorkflowTaskResponse = self
            .poll_request_json(
                "/worker/workflow-tasks/poll",
                RequestProtocol::Worker(WORKER_PROTOCOL_VERSION),
                &body,
                timeout + Duration::from_secs(5),
                transport_retries,
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
        self.complete_workflow_task_with_message_streams(
            task_id,
            lease_owner,
            workflow_task_attempt,
            commands,
            Vec::new(),
            Vec::new(),
        )
        .await
    }

    async fn complete_workflow_task_with_message_streams(
        &self,
        task_id: &str,
        lease_owner: &str,
        workflow_task_attempt: u64,
        commands: Vec<Value>,
        message_stream_cursors: Vec<Value>,
        message_stream_waits: Vec<Value>,
    ) -> Result<Value> {
        validate_workflow_task_commands(&commands)?;
        let has_message_stream_metadata =
            !message_stream_cursors.is_empty() || !message_stream_waits.is_empty();
        if has_message_stream_metadata
            && !worker_protocol_supports_message_streams(WORKER_PROTOCOL_VERSION)
        {
            return Err(Error::Codec(
                "message_streams_unavailable: message stream completion metadata requires worker protocol 1.15 or newer"
                    .to_string(),
            ));
        }
        let protocol_version = workflow_completion_protocol_version_with_message_streams(
            &commands,
            has_message_stream_metadata,
        );
        let mut body = json!({
            "lease_owner": lease_owner,
            "workflow_task_attempt": workflow_task_attempt,
            "commands": commands
        });
        if !message_stream_cursors.is_empty() {
            body["message_stream_cursors"] = Value::Array(message_stream_cursors);
        }
        if !message_stream_waits.is_empty() {
            body["message_stream_waits"] = Value::Array(message_stream_waits);
        }
        let path = format!("/worker/workflow-tasks/{task_id}/complete");
        self.request_json(
            reqwest::Method::POST,
            &path,
            RequestProtocol::Worker(protocol_version),
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
        self.fail_workflow_task_with_type(
            task_id,
            lease_owner,
            workflow_task_attempt,
            message,
            "RustWorkflowTaskFailure",
        )
        .await
    }

    async fn fail_workflow_task_with_type(
        &self,
        task_id: &str,
        lease_owner: &str,
        workflow_task_attempt: u64,
        message: impl Into<String>,
        failure_type: &str,
    ) -> Result<Value> {
        let body = json!({
            "lease_owner": lease_owner,
            "workflow_task_attempt": workflow_task_attempt,
            "failure": {
                "message": message.into(),
                "type": failure_type
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
        Ok(self
            .poll_activity_task_response(worker_id, task_queue, timeout)
            .await?
            .task)
    }

    /// Poll an activity task while preserving server stop and drain metadata.
    pub async fn poll_activity_task_response(
        &self,
        worker_id: &str,
        task_queue: &str,
        timeout: Duration,
    ) -> Result<PollActivityTaskResponse> {
        let poll_request_id = unique_request_id("rust-activity-poll");
        self.poll_activity_task_response_with_request_id(
            worker_id,
            task_queue,
            timeout,
            &poll_request_id,
            1,
        )
        .await
    }

    async fn poll_activity_task_response_with_request_id(
        &self,
        worker_id: &str,
        task_queue: &str,
        timeout: Duration,
        poll_request_id: &str,
        transport_retries: usize,
    ) -> Result<PollActivityTaskResponse> {
        let body = json!({
            "worker_id": worker_id,
            "task_queue": task_queue,
            "poll_request_id": poll_request_id,
            "timeout_seconds": long_poll_timeout_seconds(timeout),
        });
        let data: PollActivityTaskResponse = self
            .poll_request_json(
                "/worker/activity-tasks/poll",
                RequestProtocol::Worker(WORKER_PROTOCOL_VERSION),
                &body,
                timeout + Duration::from_secs(5),
                transport_retries,
            )
            .await?;
        Ok(data)
    }

    pub async fn complete_activity_task<T: Serialize>(
        &self,
        task_id: &str,
        activity_attempt_id: &str,
        lease_owner: &str,
        result: T,
        codec: &str,
    ) -> Result<Value> {
        let result = encode_typed_envelope(&AvroValue::from_serialize(&result)?, codec)?;
        let body = json!({
            "activity_attempt_id": activity_attempt_id,
            "lease_owner": lease_owner,
            "result": result
        });
        let path = format!("/worker/activity-tasks/{task_id}/complete");
        activity_task_response(
            self.request_json(
                reqwest::Method::POST,
                &path,
                RequestProtocol::Worker(WORKER_PROTOCOL_VERSION),
                Some(&body),
            )
            .await,
            "complete",
            task_id,
            activity_attempt_id,
        )
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
        activity_task_response(
            self.request_json(
                reqwest::Method::POST,
                &path,
                RequestProtocol::Worker(WORKER_PROTOCOL_VERSION),
                Some(&body),
            )
            .await,
            "fail",
            task_id,
            activity_attempt_id,
        )
    }

    pub async fn heartbeat_activity_task<T: Serialize>(
        &self,
        task_id: &str,
        activity_attempt_id: &str,
        lease_owner: &str,
        details: T,
    ) -> Result<ActivityHeartbeatResponse> {
        let details = encode_typed_envelope(&AvroValue::from_serialize(&details)?, DEFAULT_CODEC)?;
        let body = json!({
            "activity_attempt_id": activity_attempt_id,
            "lease_owner": lease_owner,
            "details": details
        });
        let path = format!("/worker/activity-tasks/{task_id}/heartbeat");
        activity_task_response(
            self.request_json(
                reqwest::Method::POST,
                &path,
                RequestProtocol::Worker(WORKER_PROTOCOL_VERSION),
                Some(&body),
            )
            .await,
            "heartbeat",
            task_id,
            activity_attempt_id,
        )
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
        let auth_token = self.auth_token(protocol)?;
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

        if let Some(token) = auth_token {
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

    async fn poll_request_json<T: DeserializeOwned, B: Serialize + ?Sized>(
        &self,
        path: &str,
        protocol: RequestProtocol,
        body: &B,
        timeout: Duration,
        max_retries: usize,
    ) -> Result<T> {
        let mut retries = 0;

        loop {
            let response = self
                .request_json_with_timeout(
                    reqwest::Method::POST,
                    path,
                    protocol,
                    Some(body),
                    timeout,
                )
                .await;

            match response {
                Err(Error::Transport(_)) if retries < max_retries => retries += 1,
                response => return worker_poll_response(response),
            }
        }
    }

    fn auth_token(&self, protocol: RequestProtocol) -> Result<Option<&str>> {
        match protocol {
            RequestProtocol::Worker(_) => {
                if let Some(token) = self.worker_token.as_deref().or(self.token.as_deref()) {
                    return Ok(Some(token));
                }
                if self.control_token.is_some() {
                    return Err(Error::MissingRoleCredentials {
                        role: "worker",
                        opposite_role: "control",
                    });
                }
                Ok(None)
            }
            RequestProtocol::ControlPlane => {
                if let Some(token) = self.control_token.as_deref().or(self.token.as_deref()) {
                    return Ok(Some(token));
                }
                if self.worker_token.is_some() {
                    return Err(Error::MissingRoleCredentials {
                        role: "control",
                        opposite_role: "worker",
                    });
                }
                Ok(None)
            }
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

fn workflow_command_result(
    command: WorkflowCommandKind,
    data: Value,
    workflow_id: &str,
    run_id: Option<&str>,
) -> WorkflowCommandResult {
    WorkflowCommandResult {
        command,
        workflow_id: data
            .get("workflow_id")
            .and_then(Value::as_str)
            .unwrap_or(workflow_id)
            .to_string(),
        run_id: data
            .get("run_id")
            .and_then(Value::as_str)
            .or(run_id)
            .map(str::to_string),
        outcome: data
            .get("outcome")
            .and_then(Value::as_str)
            .map(str::to_string),
        reason: data
            .get("reason")
            .and_then(Value::as_str)
            .map(str::to_string),
        command_status: data
            .get("command_status")
            .and_then(Value::as_str)
            .map(str::to_string),
        raw: data,
    }
}

fn workflow_command_rejection(
    command: WorkflowCommandKind,
    status: reqwest::StatusCode,
    raw_body: String,
    workflow_id: &str,
    run_id: Option<&str>,
) -> WorkflowCommandRejection {
    let body = serde_json::from_str(&raw_body).unwrap_or_else(|_| json!({"message": raw_body}));
    WorkflowCommandRejection {
        command,
        status: status.as_u16(),
        reason: body
            .get("reason")
            .and_then(Value::as_str)
            .unwrap_or("workflow_command_rejected")
            .to_string(),
        message: body
            .get("message")
            .or_else(|| body.get("error"))
            .and_then(Value::as_str)
            .unwrap_or("workflow lifecycle command was rejected")
            .to_string(),
        workflow_id: body
            .get("workflow_id")
            .and_then(Value::as_str)
            .unwrap_or(workflow_id)
            .to_string(),
        run_id: body
            .get("run_id")
            .and_then(Value::as_str)
            .or(run_id)
            .map(str::to_string),
        target_scope: body
            .get("target_scope")
            .and_then(Value::as_str)
            .map(str::to_string),
        body,
    }
}

fn query_task_response(response: Result<Value>) -> Result<Value> {
    match response {
        Err(Error::Http { status, body }) => Err(Error::QueryFailed(query_failure(status, body))),
        response => response,
    }
}

fn worker_poll_response<T: DeserializeOwned>(response: Result<T>) -> Result<T> {
    match response {
        Err(Error::Http { status, body })
            if status == reqwest::StatusCode::CONFLICT && worker_poll_body_is_stop(&body) =>
        {
            Ok(serde_json::from_str(&body)?)
        }
        response => response,
    }
}

fn worker_poll_body_is_stop(body: &str) -> bool {
    serde_json::from_str::<Value>(body)
        .ok()
        .is_some_and(|body| {
            worker_poll_is_stop(
                body.get("poll_status").and_then(Value::as_str),
                body.get("reason").and_then(Value::as_str),
            )
        })
}

fn worker_poll_is_stop(poll_status: Option<&str>, reason: Option<&str>) -> bool {
    matches!(poll_status, Some("draining" | "stopped"))
        || matches!(reason, Some("worker_draining" | "worker_stopped"))
}

fn query_task_rejection_is_final(error: &Error) -> bool {
    matches!(
        error,
        Error::QueryFailed(failure)
            if QUERY_TASK_FINAL_REJECTION_REASONS.contains(&failure.reason.as_str())
    )
}

fn activity_task_response<T>(
    response: Result<T>,
    operation: &str,
    task_id: &str,
    activity_attempt_id: &str,
) -> Result<T> {
    match response {
        Err(Error::Http { status, body }) => {
            let body = serde_json::from_str(&body).unwrap_or_else(|_| json!({"message": body}));
            Err(Error::ActivityTaskRejected(ActivityTaskRejection {
                operation: operation.to_string(),
                status: status.as_u16(),
                reason: body
                    .get("reason")
                    .and_then(Value::as_str)
                    .unwrap_or("activity_task_rejected")
                    .to_string(),
                task_id: body
                    .get("task_id")
                    .and_then(Value::as_str)
                    .unwrap_or(task_id)
                    .to_string(),
                activity_attempt_id: body
                    .get("activity_attempt_id")
                    .and_then(Value::as_str)
                    .unwrap_or(activity_attempt_id)
                    .to_string(),
                cancel_requested: body
                    .get("cancel_requested")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                can_continue: body.get("can_continue").and_then(Value::as_bool),
                run_closed_reason: body
                    .get("run_closed_reason")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                body,
            }))
        }
        response => response,
    }
}

fn activity_task_rejection_is_final(error: &Error) -> bool {
    matches!(
        error,
        Error::ActivityTaskRejected(rejection)
            if matches!(
                rejection.reason.as_str(),
                "run_cancelled"
                    | "run_terminated"
                    | "attempt_closed"
                    | "stale_attempt"
                    | "activity_cancelled"
                    | "task_cancelled"
                    | "run_closed"
                    | "activity_not_running"
                    | "attempt_not_found"
            )
    )
}

fn workflow_task_completion_is_terminal_timeout(
    error: &Error,
    task_id: &str,
    workflow_task_attempt: u64,
    run_id: Option<&str>,
) -> bool {
    let Error::Http { status, body } = error else {
        return false;
    };
    if *status != reqwest::StatusCode::CONFLICT {
        return false;
    }

    let Some(run_id) = run_id else {
        return false;
    };
    let Ok(body) = serde_json::from_str::<Value>(body) else {
        return false;
    };

    body.get("recorded").and_then(Value::as_bool) == Some(false)
        && body.get("reason").and_then(Value::as_str) == Some("run_timed_out")
        && body.get("run_status").and_then(Value::as_str) == Some("failed")
        && body.get("run_id").and_then(Value::as_str) == Some(run_id)
        && body.get("task_id").and_then(Value::as_str) == Some(task_id)
        && body.get("workflow_task_attempt").and_then(Value::as_u64) == Some(workflow_task_attempt)
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
    if worker_poll_capacity_retry_after(error).is_some()
        || worker_operation_is_explicitly_non_retryable(error)
    {
        return false;
    }

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

fn worker_operation_is_explicitly_non_retryable(error: &Error) -> bool {
    let Error::Http { body, .. } = error else {
        return false;
    };

    serde_json::from_str::<Value>(body)
        .ok()
        .and_then(|body| body.get("retryable").and_then(Value::as_bool))
        == Some(false)
}

fn worker_poll_capacity_retry_after(error: &Error) -> Option<Duration> {
    let Error::Http { status, body } = error else {
        return None;
    };
    if *status != reqwest::StatusCode::TOO_MANY_REQUESTS {
        return None;
    }

    let body = serde_json::from_str::<Value>(body).ok()?;
    let capacity_exhausted = body.get("poll_status").and_then(Value::as_str)
        == Some("long_poll_capacity_exhausted")
        || body.get("reason").and_then(Value::as_str) == Some("long_poll_capacity_exhausted");
    if !capacity_exhausted || body.get("retryable").and_then(Value::as_bool) != Some(true) {
        return None;
    }

    Some(Duration::from_secs(
        body.get("retry_after_seconds")
            .and_then(Value::as_u64)
            .unwrap_or_default(),
    ))
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
        let base_url = self.base_url.trim_end_matches('/').to_string();
        let has_sdk_api_suffix = reqwest::Url::parse(&base_url)
            .map(|url| url.path().trim_end_matches('/').ends_with("/api"))
            .unwrap_or_else(|_| base_url.ends_with("/api"));

        if has_sdk_api_suffix {
            return Err(Error::InvalidBaseUrl);
        }

        Ok(Client {
            http: reqwest::Client::builder().timeout(self.timeout).build()?,
            base_url,
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
    /// Describe whichever run is current for this stable workflow instance.
    pub async fn describe(&self) -> Result<WorkflowDescription> {
        self.client.describe_workflow(&self.workflow_id).await
    }

    /// Describe the run identity originally selected by this handle.
    pub async fn describe_selected_run(&self) -> Result<WorkflowDescription> {
        let run_id = self.run_id.as_deref().ok_or_else(|| {
            Error::Codec("run_id is required for selected-run description".to_string())
        })?;
        self.client
            .describe_workflow_run(&self.workflow_id, run_id)
            .await
    }

    pub async fn signal<T: Serialize>(&self, signal_name: &str, input: T) -> Result<Value> {
        self.client
            .signal_workflow(&self.workflow_id, signal_name, input)
            .await
    }

    pub async fn append_message<T: Serialize>(
        &self,
        stream_name: &str,
        message_id: &str,
        input: T,
    ) -> Result<Value> {
        self.client
            .append_message_stream(&self.workflow_id, stream_name, message_id, input)
            .await
    }

    /// Signal only if this handle's selected run is still current.
    pub async fn signal_selected_run<T: Serialize>(
        &self,
        signal_name: &str,
        input: T,
    ) -> Result<Value> {
        let run_id = self.run_id.as_deref().ok_or_else(|| {
            Error::Codec("run_id is required for selected-run signaling".to_string())
        })?;
        self.client
            .signal_workflow_run(&self.workflow_id, run_id, signal_name, input)
            .await
    }

    /// Request cooperative cancellation of whichever run is current.
    pub async fn cancel(&self, options: WorkflowCommandOptions) -> Result<WorkflowCommandResult> {
        self.client
            .cancel_workflow(&self.workflow_id, options)
            .await
    }

    /// Request cancellation only if this handle's selected run is still current.
    pub async fn cancel_selected_run(
        &self,
        options: WorkflowCommandOptions,
    ) -> Result<WorkflowCommandResult> {
        let run_id = self.run_id.as_deref().ok_or_else(|| {
            Error::Codec("run_id is required for selected-run cancellation".to_string())
        })?;
        self.client
            .cancel_workflow_run(&self.workflow_id, run_id, options)
            .await
    }

    /// Forcefully terminate whichever run is current.
    pub async fn terminate(
        &self,
        options: WorkflowCommandOptions,
    ) -> Result<WorkflowCommandResult> {
        self.client
            .terminate_workflow(&self.workflow_id, options)
            .await
    }

    /// Terminate only if this handle's selected run is still current.
    pub async fn terminate_selected_run(
        &self,
        options: WorkflowCommandOptions,
    ) -> Result<WorkflowCommandResult> {
        let run_id = self.run_id.as_deref().ok_or_else(|| {
            Error::Codec("run_id is required for selected-run termination".to_string())
        })?;
        self.client
            .terminate_workflow_run(&self.workflow_id, run_id, options)
            .await
    }

    /// Execute a named, read-only query against this workflow.
    pub async fn query<T: Serialize>(&self, query_name: &str, input: T) -> Result<Value> {
        self.client
            .query_workflow(&self.workflow_id, query_name, input)
            .await
    }

    pub async fn query_avro_value<T: Serialize>(
        &self,
        query_name: &str,
        input: T,
    ) -> Result<AvroValue> {
        self.client
            .query_workflow_avro_value(&self.workflow_id, query_name, input)
            .await
    }

    pub async fn update<T: Serialize>(
        &self,
        update_name: &str,
        input: T,
        request_id: Option<&str>,
    ) -> Result<Value> {
        self.client
            .update_workflow(&self.workflow_id, update_name, input, request_id)
            .await
    }

    pub async fn update_avro_value<T: Serialize>(
        &self,
        update_name: &str,
        input: T,
        request_id: Option<&str>,
    ) -> Result<AvroValue> {
        self.client
            .update_workflow_avro_value(&self.workflow_id, update_name, input, request_id)
            .await
    }

    /// Query only if this handle's selected run is still current.
    pub async fn query_selected_run<T: Serialize>(
        &self,
        query_name: &str,
        input: T,
    ) -> Result<Value> {
        let run_id = self
            .run_id
            .as_deref()
            .ok_or_else(|| Error::Codec("run_id is required for selected-run query".to_string()))?;
        self.client
            .query_workflow_run(&self.workflow_id, run_id, query_name, input)
            .await
    }

    /// Await the final terminal outcome of the current continue-as-new chain.
    pub async fn result(&self, options: WorkflowResultOptions) -> Result<Value> {
        self.result_target(options, None).await
    }

    /// Await the final result without projecting Avro bytes through JSON.
    pub async fn result_avro_value(&self, options: WorkflowResultOptions) -> Result<AvroValue> {
        self.result_avro_value_target(options, None).await
    }

    /// Await the final result and decode it into a Serde application type.
    pub async fn result_typed<T: DeserializeOwned>(
        &self,
        options: WorkflowResultOptions,
    ) -> Result<T> {
        let result = self.result_avro_value(options).await?;
        decode_handler_result(result, HandlerKind::Workflow, &self.workflow_type)
    }

    /// Await only the run identity originally selected by this handle.
    pub async fn result_selected_run(&self, options: WorkflowResultOptions) -> Result<Value> {
        let run_id = self.run_id.as_deref().ok_or_else(|| {
            Error::Codec("run_id is required for selected-run result".to_string())
        })?;
        self.result_target(options, Some(run_id)).await
    }

    /// Await the selected run's result on the lossless Avro Value surface.
    pub async fn result_selected_run_avro_value(
        &self,
        options: WorkflowResultOptions,
    ) -> Result<AvroValue> {
        let run_id = self.run_id.as_deref().ok_or_else(|| {
            Error::Codec("run_id is required for selected-run result".to_string())
        })?;
        self.result_avro_value_target(options, Some(run_id)).await
    }

    /// Await the selected run and decode its result into a Serde type.
    pub async fn result_selected_run_typed<T: DeserializeOwned>(
        &self,
        options: WorkflowResultOptions,
    ) -> Result<T> {
        let result = self.result_selected_run_avro_value(options).await?;
        decode_handler_result(result, HandlerKind::Workflow, &self.workflow_type)
    }

    async fn result_avro_value_target(
        &self,
        options: WorkflowResultOptions,
        selected_run_id: Option<&str>,
    ) -> Result<AvroValue> {
        let started = Instant::now();

        loop {
            let description = match selected_run_id {
                Some(run_id) => {
                    self.client
                        .describe_workflow_run(&self.workflow_id, run_id)
                        .await?
                }
                None => self.describe().await?,
            };
            if description.is_completed() {
                return description.output_avro_value.ok_or_else(|| {
                    Error::Codec(
                        "missing_payload_envelope: typed workflow result requires output_envelope"
                            .to_string(),
                    )
                });
            }
            if description.is_terminal() {
                let outcome =
                    workflow_terminal_outcome(&description, &self.workflow_id, selected_run_id);
                return Err(match outcome.kind {
                    WorkflowTerminalKind::Failed => Error::WorkflowFailed(outcome),
                    WorkflowTerminalKind::Cancelled => Error::WorkflowCancelled(outcome),
                    WorkflowTerminalKind::Terminated => Error::WorkflowTerminated(outcome),
                    WorkflowTerminalKind::TimedOut => Error::WorkflowTimedOut(outcome),
                });
            }
            if started.elapsed() >= options.timeout {
                return Err(Error::Timeout);
            }
            tokio::time::sleep(options.poll_interval).await;
        }
    }

    async fn result_target(
        &self,
        options: WorkflowResultOptions,
        selected_run_id: Option<&str>,
    ) -> Result<Value> {
        let started = Instant::now();

        loop {
            let description = match selected_run_id {
                Some(run_id) => {
                    self.client
                        .describe_workflow_run(&self.workflow_id, run_id)
                        .await?
                }
                None => self.describe().await?,
            };
            if description.is_completed() {
                return Ok(description.output.unwrap_or(Value::Null));
            }

            if description.is_terminal() {
                let outcome =
                    workflow_terminal_outcome(&description, &self.workflow_id, selected_run_id);
                return Err(match outcome.kind {
                    WorkflowTerminalKind::Failed => Error::WorkflowFailed(outcome),
                    WorkflowTerminalKind::Cancelled => Error::WorkflowCancelled(outcome),
                    WorkflowTerminalKind::Terminated => Error::WorkflowTerminated(outcome),
                    WorkflowTerminalKind::TimedOut => Error::WorkflowTimedOut(outcome),
                });
            }

            if started.elapsed() >= options.timeout {
                return Err(Error::WorkflowTimedOut(WorkflowTerminalOutcome {
                    kind: WorkflowTerminalKind::TimedOut,
                    workflow_id: description
                        .workflow_id
                        .clone()
                        .unwrap_or_else(|| self.workflow_id.clone()),
                    run_id: description
                        .run_id
                        .clone()
                        .or_else(|| selected_run_id.map(str::to_string)),
                    reason: "result_wait_timeout".to_string(),
                    failure_category: Some("client_timeout".to_string()),
                    failure_id: None,
                    exception_type: None,
                    exception_class: None,
                    non_retryable: None,
                    message: Some(format!(
                        "workflow result was not terminal within {:?}",
                        options.timeout
                    )),
                    exception: None,
                    raw: description.raw_value(),
                }));
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
    pub closed_reason: Option<String>,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub failure: Option<Value>,
    #[serde(default)]
    pub exception: Option<Value>,
    #[serde(default)]
    pub failures: Vec<Value>,
    #[serde(default)]
    pub output: Option<Value>,
    #[serde(default)]
    pub output_envelope: Option<Value>,
    #[serde(skip)]
    pub output_avro_value: Option<AvroValue>,
    #[serde(flatten)]
    pub raw: HashMap<String, Value>,
}

/// Lifecycle and backlog metadata for one run-scoped Workflow Stream.
#[derive(Clone, Debug, Deserialize)]
pub struct WorkflowStreamDescription {
    pub stream_name: String,
    pub status: String,
    pub last_offset: i64,
    pub total_items: u64,
    pub pending_items: u64,
    #[serde(default)]
    pub opened_at: Option<String>,
    #[serde(default)]
    pub last_appended_at: Option<String>,
    #[serde(default)]
    pub closed_at: Option<String>,
    #[serde(default)]
    pub error_reason: Option<String>,
    #[serde(default)]
    pub retention_seconds: Option<u64>,
    #[serde(flatten)]
    pub raw: HashMap<String, Value>,
}

impl WorkflowStreamDescription {
    pub fn is_terminal(&self) -> bool {
        matches!(self.status.as_str(), "closed" | "errored")
    }
}

/// One item for direct or replay-safe append.
#[derive(Clone, Debug, Default)]
pub struct WorkflowStreamAppendItem {
    pub payload_envelope: Option<Value>,
    pub payload_reference: Option<String>,
    pub item_type: Option<String>,
    pub content_type: Option<String>,
    pub idempotency_key: Option<String>,
}

impl WorkflowStreamAppendItem {
    /// Encode an inline payload with the SDK's fixed Avro Value envelope.
    pub fn new<T: Serialize>(payload: T) -> Result<Self> {
        let value = AvroValue::from_serialize(&payload)?;
        Ok(Self {
            payload_envelope: Some(encode_typed_envelope(&value, DEFAULT_CODEC)?),
            ..Self::default()
        })
    }

    /// Preserve an external payload URI as an opaque service-contract reference.
    pub fn from_reference(reference: impl Into<String>) -> Self {
        Self {
            payload_reference: Some(reference.into()),
            ..Self::default()
        }
    }

    pub fn item_type(mut self, item_type: impl Into<String>) -> Self {
        self.item_type = Some(item_type.into());
        self
    }

    pub fn content_type(mut self, content_type: impl Into<String>) -> Self {
        self.content_type = Some(content_type.into());
        self
    }

    pub fn idempotency_key(mut self, idempotency_key: impl Into<String>) -> Self {
        self.idempotency_key = Some(idempotency_key.into());
        self
    }

    fn wire_value(&self, derived_idempotency_key: Option<String>) -> Value {
        let mut item = serde_json::Map::new();
        if let Some(payload) = &self.payload_envelope {
            item.insert("payload".to_string(), payload.clone());
            item.insert("payload_codec".to_string(), json!(DEFAULT_CODEC));
        }
        if let Some(reference) = &self.payload_reference {
            item.insert("payload_reference".to_string(), json!(reference));
        }
        if let Some(item_type) = &self.item_type {
            item.insert("item_type".to_string(), json!(item_type));
        }
        if let Some(content_type) = &self.content_type {
            item.insert("content_type".to_string(), json!(content_type));
        }
        if let Some(key) = derived_idempotency_key
            .as_ref()
            .or(self.idempotency_key.as_ref())
        {
            item.insert("idempotency_key".to_string(), json!(key));
        }
        Value::Object(item)
    }
}

/// One durable item at its stable zero-based offset.
#[derive(Clone, Debug)]
pub struct WorkflowStreamItem {
    pub offset: u64,
    pub payload: Option<Value>,
    pub payload_envelope: Option<Value>,
    pub payload_reference: Option<String>,
    pub payload_codec: Option<String>,
    pub idempotency_key: Option<String>,
    pub item_type: Option<String>,
    pub content_type: Option<String>,
    pub origin: Option<String>,
    pub origin_reference: Option<String>,
    pub emitted_at: Option<String>,
    pub raw: Value,
}

/// One bounded at-least-once subscription page.
#[derive(Clone, Debug)]
pub struct WorkflowStreamPage {
    pub stream: WorkflowStreamDescription,
    pub items: Vec<WorkflowStreamItem>,
    pub next_offset: u64,
    pub terminal: bool,
}

/// Durable acceptance and deduplication outcome for an append request.
#[derive(Clone, Debug)]
pub struct WorkflowStreamAppendResult {
    pub stream: WorkflowStreamDescription,
    pub accepted_offsets: Vec<u64>,
    pub accepted: u64,
    pub deduped: u64,
}

#[derive(Deserialize)]
struct WorkflowStreamListResponse {
    #[serde(default)]
    streams: Vec<WorkflowStreamDescription>,
}

#[derive(Deserialize)]
struct WorkflowStreamDescriptionResponse {
    stream: WorkflowStreamDescription,
}

#[derive(Deserialize)]
struct WorkflowStreamPageResponse {
    stream: WorkflowStreamDescription,
    #[serde(default)]
    items: Vec<Value>,
    next_offset: u64,
    terminal: bool,
}

#[derive(Deserialize)]
struct WorkflowStreamAppendResponse {
    stream: WorkflowStreamDescription,
    #[serde(default)]
    accepted_offsets: Vec<u64>,
    accepted: u64,
    deduped: u64,
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
            let value = decode_wire_avro_value(envelope, DEFAULT_CODEC)?;
            self.output = Some(value.clone().into_json()?);
            self.output_avro_value = Some(value);
        }

        Ok(())
    }

    fn raw_value(&self) -> Value {
        let mut data = self.raw.clone();
        data.insert(
            "workflow_id".to_string(),
            self.workflow_id
                .clone()
                .map(Value::String)
                .unwrap_or(Value::Null),
        );
        data.insert(
            "run_id".to_string(),
            self.run_id
                .clone()
                .map(Value::String)
                .unwrap_or(Value::Null),
        );
        data.insert(
            "workflow_type".to_string(),
            self.workflow_type
                .clone()
                .map(Value::String)
                .unwrap_or(Value::Null),
        );
        data.insert(
            "status".to_string(),
            self.status
                .clone()
                .map(Value::String)
                .unwrap_or(Value::Null),
        );
        data.insert(
            "closed_reason".to_string(),
            self.closed_reason
                .clone()
                .map(Value::String)
                .unwrap_or(Value::Null),
        );
        if let Some(failure) = &self.failure {
            data.insert("failure".to_string(), failure.clone());
        }
        if let Some(exception) = &self.exception {
            data.insert("exception".to_string(), exception.clone());
        }
        Value::Object(data.into_iter().collect())
    }
}

fn workflow_terminal_outcome(
    description: &WorkflowDescription,
    workflow_id: &str,
    run_id: Option<&str>,
) -> WorkflowTerminalOutcome {
    let terminal_kind = description
        .closed_reason
        .as_deref()
        .or(description.status.as_deref())
        .unwrap_or("failed")
        .to_ascii_lowercase();
    let kind = match terminal_kind.as_str() {
        "cancelled" | "canceled" => WorkflowTerminalKind::Cancelled,
        "terminated" => WorkflowTerminalKind::Terminated,
        "timed_out" | "timedout" => WorkflowTerminalKind::TimedOut,
        _ => WorkflowTerminalKind::Failed,
    };
    let default_reason = match kind {
        WorkflowTerminalKind::Failed => "workflow_failed",
        WorkflowTerminalKind::Cancelled => "cancelled",
        WorkflowTerminalKind::Terminated => "terminated",
        WorkflowTerminalKind::TimedOut => "timed_out",
    };
    let failure = description
        .failure
        .as_ref()
        .filter(|value| value.is_object());
    let nested_failure = failure
        .and_then(|value| value.get("failures"))
        .and_then(Value::as_array)
        .and_then(|failures| failures.last())
        .or_else(|| description.failures.last());
    let exception = description
        .exception
        .clone()
        .or_else(|| failure.and_then(|value| value.get("exception")).cloned())
        .or_else(|| {
            nested_failure
                .and_then(|value| value.get("exception_payload"))
                .cloned()
        });
    let string_field = |name: &str| {
        failure
            .and_then(|value| value.get(name))
            .and_then(Value::as_str)
            .or_else(|| {
                nested_failure
                    .and_then(|value| value.get(name))
                    .and_then(Value::as_str)
            })
            .map(str::to_string)
    };
    let exception_field = |name: &str| {
        exception
            .as_ref()
            .and_then(|value| value.get(name))
            .and_then(Value::as_str)
            .map(str::to_string)
    };
    let message = description
        .error
        .clone()
        .or_else(|| string_field("message"))
        .or_else(|| exception_field("message"));
    let reason = description
        .raw
        .get("reason")
        .and_then(Value::as_str)
        .map(str::to_string)
        .or_else(|| {
            failure
                .and_then(|value| value.get("reason"))
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .or_else(|| description.closed_reason.clone())
        .unwrap_or_else(|| default_reason.to_string());
    let failure_id = string_field("failure_id").or_else(|| {
        nested_failure
            .and_then(|value| value.get("id"))
            .and_then(Value::as_str)
            .map(str::to_string)
    });

    WorkflowTerminalOutcome {
        kind,
        workflow_id: description
            .workflow_id
            .clone()
            .unwrap_or_else(|| workflow_id.to_string()),
        run_id: description
            .run_id
            .clone()
            .or_else(|| run_id.map(str::to_string)),
        reason,
        failure_category: string_field("failure_category")
            .or_else(|| Some(default_reason.to_string())),
        failure_id,
        exception_type: string_field("exception_type").or_else(|| exception_field("type")),
        exception_class: string_field("exception_class").or_else(|| exception_field("class")),
        non_retryable: failure
            .and_then(|value| value.get("non_retryable"))
            .and_then(Value::as_bool)
            .or_else(|| {
                nested_failure
                    .and_then(|value| value.get("non_retryable"))
                    .and_then(Value::as_bool)
            }),
        message,
        exception,
        raw: description.raw_value(),
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

/// Result of gracefully removing a worker-plane registration.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct WorkerDeregistrationEnvelope {
    pub worker_id: String,
    pub outcome: String,
    pub recovered_workflow_task_count: u64,
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

impl PollWorkflowTaskResponse {
    /// Classify this response without parsing server display text.
    pub fn outcome(&self) -> WorkerPollOutcome {
        worker_poll_outcome(
            self.task.is_some(),
            self.poll_status.as_deref(),
            self.reason.as_deref(),
        )
    }
}

fn runtime_supports_workflow_memo_updates(capabilities: Option<&Value>) -> bool {
    let Some(capabilities) = capabilities.and_then(Value::as_object) else {
        return false;
    };
    let supported = capabilities
        .get("workflow_memo_updates")
        .and_then(Value::as_object)
        .and_then(|memo| memo.get("supported"))
        .and_then(Value::as_bool)
        == Some(true);
    let command_advertised = capabilities
        .get("supported_workflow_task_commands")
        .and_then(Value::as_array)
        .is_some_and(|commands| {
            commands
                .iter()
                .any(|command| command.as_str() == Some("upsert_memo"))
        });
    supported && command_advertised
}

fn commands_use_workflow_memo_updates(commands: &[Value]) -> bool {
    commands
        .iter()
        .any(|command| command.get("type").and_then(Value::as_str) == Some("upsert_memo"))
}

#[derive(Clone, Debug, Deserialize)]
pub struct PollActivityTaskResponse {
    #[serde(default)]
    pub task: Option<ActivityTask>,
    #[serde(default)]
    pub poll_status: Option<String>,
    #[serde(default)]
    pub reason: Option<String>,
}

impl PollActivityTaskResponse {
    /// Classify this response without parsing server display text.
    pub fn outcome(&self) -> WorkerPollOutcome {
        worker_poll_outcome(
            self.task.is_some(),
            self.poll_status.as_deref(),
            self.reason.as_deref(),
        )
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct PollQueryTaskResponse {
    #[serde(default)]
    pub task: Option<QueryTask>,
    #[serde(default)]
    pub poll_status: Option<String>,
    #[serde(default)]
    pub reason: Option<String>,
}

impl PollQueryTaskResponse {
    /// Classify this response without parsing server display text.
    pub fn outcome(&self) -> WorkerPollOutcome {
        worker_poll_outcome(
            self.task.is_some(),
            self.poll_status.as_deref(),
            self.reason.as_deref(),
        )
    }
}

/// Stable classification for worker poll responses.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkerPollOutcome {
    /// A task was leased and is available on the response.
    Task,
    /// No task was leased, but the worker should continue polling.
    Idle {
        poll_status: Option<String>,
        reason: Option<String>,
    },
    /// The server asked this worker to stop claiming new work.
    Stop {
        poll_status: Option<String>,
        reason: Option<String>,
    },
}

impl WorkerPollOutcome {
    pub fn should_stop(&self) -> bool {
        matches!(self, Self::Stop { .. })
    }
}

fn worker_poll_outcome(
    has_task: bool,
    poll_status: Option<&str>,
    reason: Option<&str>,
) -> WorkerPollOutcome {
    if worker_poll_is_stop(poll_status, reason) {
        return WorkerPollOutcome::Stop {
            poll_status: poll_status.map(str::to_string),
            reason: reason.map(str::to_string),
        };
    }

    if has_task {
        WorkerPollOutcome::Task
    } else {
        WorkerPollOutcome::Idle {
            poll_status: poll_status.map(str::to_string),
            reason: reason.map(str::to_string),
        }
    }
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
    #[serde(
        default = "missing_task_payload_codec",
        deserialize_with = "deserialize_task_payload_codec"
    )]
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
    pub workflow_command_id: Option<String>,
    #[serde(default)]
    pub workflow_id: Option<String>,
    #[serde(default)]
    pub run_id: Option<String>,
    pub workflow_type: String,
    #[serde(default)]
    pub cancel_requested: bool,
    #[serde(
        default = "missing_task_payload_codec",
        deserialize_with = "deserialize_task_payload_codec"
    )]
    pub payload_codec: String,
    #[serde(default)]
    pub arguments: Option<Value>,
    #[serde(default)]
    pub history_events: Vec<HistoryEvent>,
    #[serde(default)]
    pub total_history_events: Option<u64>,
    #[serde(default)]
    pub history_size_bytes: Option<u64>,
    #[serde(default)]
    pub continue_as_new_recommended: Option<bool>,
    #[serde(default)]
    pub history_budget_pressure: Option<String>,
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
    pub workflow_update_id: Option<String>,
    #[serde(default)]
    pub update_name: Option<String>,
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
    #[serde(
        default = "missing_task_payload_codec",
        deserialize_with = "deserialize_task_payload_codec"
    )]
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
    avro_arguments: Vec<AvroValue>,
    pub workflow_sequence: Option<u64>,
}

impl QuerySignal {
    /// Lossless fixed Avro Value arguments for this committed signal.
    pub fn arguments_avro_value(&self) -> &[AvroValue] {
        &self.avro_arguments
    }
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
    workflow_input_avro_value: AvroValue,
    history_events: Arc<Vec<HistoryEvent>>,
    signal_events: Arc<Vec<QuerySignal>>,
}

impl QueryContext {
    /// The normalized argument list used to start the workflow.
    pub fn workflow_input(&self) -> &Value {
        &self.workflow_input
    }

    /// The lossless fixed Avro Value argument list used to start the workflow.
    pub fn workflow_input_avro_value(&self) -> &AvroValue {
        &self.workflow_input_avro_value
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

    /// Lossless fixed Avro Value arguments for committed signals with `signal_name`.
    pub fn signals_avro_value(&self, signal_name: &str) -> Vec<Vec<AvroValue>> {
        self.signal_events
            .iter()
            .filter(|signal| signal.name == signal_name)
            .map(|signal| signal.avro_arguments.clone())
            .collect()
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct ActivityHeartbeatResponse {
    #[serde(default)]
    pub cancel_requested: bool,
    #[serde(default)]
    pub heartbeat_recorded: bool,
    #[serde(default)]
    pub can_continue: Option<bool>,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub run_closed_reason: Option<String>,
    #[serde(default)]
    pub run_closed_at: Option<String>,
    #[serde(default)]
    pub lease_expires_at: Option<String>,
    #[serde(default)]
    pub last_heartbeat_at: Option<String>,
}

impl ActivityHeartbeatResponse {
    /// Whether the activity should stop instead of attempting completion.
    pub fn should_stop(&self) -> bool {
        self.cancel_requested || self.can_continue == Some(false)
    }
}

fn missing_task_payload_codec() -> String {
    MISSING_TASK_PAYLOAD_CODEC.to_string()
}

fn deserialize_task_payload_codec<'de, D>(deserializer: D) -> std::result::Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(match Value::deserialize(deserializer)? {
        Value::String(codec) => codec,
        Value::Null => NULL_TASK_PAYLOAD_CODEC.to_string(),
        _ => NON_STRING_TASK_PAYLOAD_CODEC.to_string(),
    })
}

fn default_workflow_task_attempt() -> u64 {
    1
}

fn default_attempt_number() -> u64 {
    1
}

type WorkflowFuture = Pin<Box<dyn Future<Output = Result<AvroValue>> + Send + 'static>>;
type WorkflowHandler = Arc<dyn Fn(WorkflowContext, AvroValue) -> WorkflowFuture + Send + Sync>;
type ErasedWorkflowState = Arc<dyn Any + Send + Sync>;
type WorkflowStateSnapshot = Arc<dyn Fn() -> Result<ErasedWorkflowState> + Send + Sync>;
type ReplayedWorkflowHandler =
    Arc<dyn Fn(WorkflowContext, AvroValue) -> ReplayedWorkflowInvocation + Send + Sync>;
type ActivityFuture = Pin<Box<dyn Future<Output = Result<AvroValue>> + Send + 'static>>;
type ActivityHandler = Arc<dyn Fn(ActivityContext, AvroValue) -> ActivityFuture + Send + Sync>;
type QueryFuture = Pin<Box<dyn Future<Output = Result<AvroValue>> + Send + 'static>>;
type QueryHandler = Arc<dyn Fn(QueryContext, AvroValue) -> QueryFuture + Send + Sync>;
type UpdateHandler = Arc<dyn Fn(QueryContext, AvroValue) -> QueryFuture + Send + Sync>;
type ReplayedQueryHandler = Arc<
    dyn Fn(QueryContext, ErasedWorkflowState, AvroValue) -> std::result::Result<QueryFuture, String>
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

#[derive(Debug)]
struct WorkflowTaskDecision {
    commands: Vec<Value>,
    message_stream_cursors: Vec<Value>,
    message_stream_waits: Vec<Value>,
}

impl WorkflowTaskDecision {
    fn without_message_streams(commands: Vec<Value>) -> Self {
        Self {
            commands,
            message_stream_cursors: Vec::new(),
            message_stream_waits: Vec::new(),
        }
    }
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
/// Expected empty long polls and explicit Server capacity backpressure are
/// normal worker states. Capacity backpressure honors the advertised retry
/// delay without consuming this retry budget. Other transport failures,
/// retryable HTTP 408/429 responses, and server errors are retried with capped
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ManagedPollOutcome {
    Idle,
    Handled,
    Stop,
}

#[derive(Clone)]
pub struct Worker {
    client: Client,
    worker_id: String,
    task_queue: String,
    workflows: HashMap<String, RegisteredWorkflow>,
    activities: HashMap<String, ActivityHandler>,
    queries: HashMap<String, HashMap<String, RegisteredQuery>>,
    updates: HashMap<String, HashMap<String, UpdateHandler>>,
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
            updates: HashMap::new(),
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

    /// Register a workflow handler.
    ///
    /// An uncaught [`enum@Error`] returned by the handler fails the workflow run and
    /// is reported to clients as [`Error::WorkflowFailed`]. Errors that occur
    /// while acquiring or decoding a worker task remain worker-operation
    /// failures and do not get converted into workflow outcomes.
    pub fn register_workflow<F, Fut>(&mut self, workflow_type: impl Into<String>, handler: F)
    where
        F: Fn(WorkflowContext, Value) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Value>> + Send + 'static,
    {
        let handler = Arc::new(handler);
        self.workflows.insert(
            workflow_type.into(),
            RegisteredWorkflow {
                execute: Arc::new(move |ctx, input| {
                    let handler = Arc::clone(&handler);
                    Box::pin(async move {
                        let result = handler(ctx, input.into_json()?).await?;
                        AvroValue::from_serialize(&result)
                    })
                }),
                replay: None,
                state_type: None,
            },
        );
    }

    /// Register a workflow with one Serde request value and a Serde result.
    ///
    /// This is an ergonomic adapter over the same fixed Avro Value protocol as
    /// [`Worker::register_workflow_avro_value`]. It does not create or publish a
    /// workflow-specific schema. A task must contain zero arguments for a unit
    /// request or exactly one argument for every other request type.
    ///
    /// See the runnable
    /// [`hello_world` example](https://github.com/durable-workflow/sdk-rust/blob/main/examples/hello_world.rs)
    /// for typed workflow and activity contracts with retry and timeout policy.
    pub fn register_typed_workflow<I, O, F, Fut>(
        &mut self,
        workflow_type: impl Into<String>,
        handler: F,
    ) where
        I: DeserializeOwned + Send + 'static,
        O: Serialize + Send + 'static,
        F: Fn(WorkflowContext, I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<O>> + Send + 'static,
    {
        let workflow_type = workflow_type.into();
        let handler_name = workflow_type.clone();
        let handler = Arc::new(handler);
        self.workflows.insert(
            workflow_type,
            RegisteredWorkflow {
                execute: Arc::new(move |ctx, input| {
                    let handler = Arc::clone(&handler);
                    let handler_name = handler_name.clone();
                    Box::pin(async move {
                        let input =
                            decode_handler_input::<I>(input, HandlerKind::Workflow, &handler_name)?;
                        let result = handler(ctx, input).await?;
                        encode_handler_result(&result, HandlerKind::Workflow, &handler_name)
                    })
                }),
                replay: None,
                state_type: None,
            },
        );
    }

    /// Register a workflow on the lossless fixed Avro Value surface.
    pub fn register_workflow_avro_value<F, Fut>(
        &mut self,
        workflow_type: impl Into<String>,
        handler: F,
    ) where
        F: Fn(WorkflowContext, AvroValue) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<AvroValue>> + Send + 'static,
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
        let execute = Arc::new(move |ctx: WorkflowContext, input: AvroValue| {
            let state = WorkflowInstance::new(execute_factory());
            let handler = Arc::clone(&execute_handler);
            Box::pin(async move {
                let result = handler(ctx, input.into_json()?, state).await?;
                AvroValue::from_serialize(&result)
            }) as WorkflowFuture
        });

        let replay = Arc::new(move |ctx: WorkflowContext, input: AvroValue| {
            let state = WorkflowInstance::new(state_factory());
            let snapshot_state = state.clone();
            let snapshot: WorkflowStateSnapshot =
                Arc::new(move || Ok(Arc::new(snapshot_state.snapshot()?) as ErasedWorkflowState));
            let replay_handler = Arc::clone(&handler);
            let future = async move {
                let result = replay_handler(ctx, input.into_json()?, state).await?;
                AvroValue::from_serialize(&result)
            };
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

    /// Register a replayable workflow with one Serde request value and result.
    ///
    /// Normal task execution and instance-state query replay both decode and
    /// encode through the fixed Avro Value codec. The state factory and handler
    /// otherwise follow [`Worker::register_replayed_workflow`].
    pub fn register_typed_replayed_workflow<I, O, S, Factory, F, Fut>(
        &mut self,
        workflow_type: impl Into<String>,
        state_factory: Factory,
        handler: F,
    ) where
        I: DeserializeOwned + Send + 'static,
        O: Serialize + Send + 'static,
        S: Clone + Send + Sync + 'static,
        Factory: Fn() -> S + Send + Sync + 'static,
        F: Fn(WorkflowContext, I, WorkflowInstance<S>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<O>> + Send + 'static,
    {
        let workflow_type = workflow_type.into();
        let state_factory = Arc::new(state_factory);
        let handler = Arc::new(handler);

        let execute_name = workflow_type.clone();
        let execute_factory = Arc::clone(&state_factory);
        let execute_handler = Arc::clone(&handler);
        let execute = Arc::new(move |ctx: WorkflowContext, input: AvroValue| {
            let state = WorkflowInstance::new(execute_factory());
            let handler = Arc::clone(&execute_handler);
            let handler_name = execute_name.clone();
            Box::pin(async move {
                let input = decode_handler_input::<I>(input, HandlerKind::Workflow, &handler_name)?;
                let result = handler(ctx, input, state).await?;
                encode_handler_result(&result, HandlerKind::Workflow, &handler_name)
            }) as WorkflowFuture
        });

        let replay_name = workflow_type.clone();
        let replay = Arc::new(move |ctx: WorkflowContext, input: AvroValue| {
            let state = WorkflowInstance::new(state_factory());
            let snapshot_state = state.clone();
            let snapshot: WorkflowStateSnapshot =
                Arc::new(move || Ok(Arc::new(snapshot_state.snapshot()?) as ErasedWorkflowState));
            let handler = Arc::clone(&handler);
            let handler_name = replay_name.clone();
            let future = async move {
                let input = decode_handler_input::<I>(input, HandlerKind::Workflow, &handler_name)?;
                let result = handler(ctx, input, state).await?;
                encode_handler_result(&result, HandlerKind::Workflow, &handler_name)
            };
            ReplayedWorkflowInvocation {
                future: Box::pin(future),
                snapshot,
            }
        });

        self.workflows.insert(
            workflow_type,
            RegisteredWorkflow {
                execute,
                replay: Some(replay),
                state_type: Some(TypeId::of::<S>()),
            },
        );
    }

    /// Register a replayable workflow on the lossless fixed Avro Value surface.
    pub fn register_replayed_workflow_avro_value<S, Factory, F, Fut>(
        &mut self,
        workflow_type: impl Into<String>,
        state_factory: Factory,
        handler: F,
    ) where
        S: Clone + Send + Sync + 'static,
        Factory: Fn() -> S + Send + Sync + 'static,
        F: Fn(WorkflowContext, AvroValue, WorkflowInstance<S>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<AvroValue>> + Send + 'static,
    {
        let state_factory = Arc::new(state_factory);
        let handler = Arc::new(handler);

        let execute_factory = Arc::clone(&state_factory);
        let execute_handler = Arc::clone(&handler);
        let execute = Arc::new(move |ctx: WorkflowContext, input: AvroValue| {
            let state = WorkflowInstance::new(execute_factory());
            Box::pin(execute_handler(ctx, input, state)) as WorkflowFuture
        });

        let replay = Arc::new(move |ctx: WorkflowContext, input: AvroValue| {
            let state = WorkflowInstance::new(state_factory());
            let snapshot_state = state.clone();
            let snapshot: WorkflowStateSnapshot =
                Arc::new(move || Ok(Arc::new(snapshot_state.snapshot()?) as ErasedWorkflowState));
            ReplayedWorkflowInvocation {
                future: Box::pin(handler(ctx, input, state)),
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
        let handler = Arc::new(handler);
        self.activities.insert(
            activity_type.into(),
            Arc::new(move |ctx, args| {
                let handler = Arc::clone(&handler);
                Box::pin(async move {
                    let result = handler(ctx, args.into_json()?).await?;
                    AvroValue::from_serialize(&result)
                })
            }),
        );
    }

    /// Register an activity with one Serde request value and a Serde result.
    ///
    /// Inputs and results use the platform's fixed Avro Value schema. Shape
    /// mismatches and unsupported Serde values return [`Error::HandlerType`]
    /// with the activity name and Rust type.
    pub fn register_typed_activity<I, O, F, Fut>(
        &mut self,
        activity_type: impl Into<String>,
        handler: F,
    ) where
        I: DeserializeOwned + Send + 'static,
        O: Serialize + Send + 'static,
        F: Fn(ActivityContext, I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<O>> + Send + 'static,
    {
        let activity_type = activity_type.into();
        let handler_name = activity_type.clone();
        let handler = Arc::new(handler);
        self.activities.insert(
            activity_type,
            Arc::new(move |ctx, input| {
                let handler = Arc::clone(&handler);
                let handler_name = handler_name.clone();
                Box::pin(async move {
                    let input =
                        decode_handler_input::<I>(input, HandlerKind::Activity, &handler_name)?;
                    let result = handler(ctx, input).await?;
                    encode_handler_result(&result, HandlerKind::Activity, &handler_name)
                })
            }),
        );
    }

    /// Register an activity on the lossless fixed Avro Value surface.
    pub fn register_activity_avro_value<F, Fut>(
        &mut self,
        activity_type: impl Into<String>,
        handler: F,
    ) where
        F: Fn(ActivityContext, AvroValue) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<AvroValue>> + Send + 'static,
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
        let handler = Arc::new(handler);
        self.queries
            .entry(workflow_type.into())
            .or_default()
            .insert(
                query_name.into(),
                RegisteredQuery::Snapshot(Arc::new(move |ctx, args| {
                    let handler = Arc::clone(&handler);
                    Box::pin(async move {
                        let result = handler(ctx, args.into_json()?).await?;
                        AvroValue::from_serialize(&result)
                    })
                })),
            );
    }

    /// Register a query handler on the lossless fixed Avro Value surface.
    pub fn register_query_avro_value<F, Fut>(
        &mut self,
        workflow_type: impl Into<String>,
        query_name: impl Into<String>,
        handler: F,
    ) where
        F: Fn(QueryContext, AvroValue) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<AvroValue>> + Send + 'static,
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
            let handler = Arc::clone(&handler);
            Ok(Box::pin(async move {
                let result = handler(ctx, state, args.into_json()?).await?;
                AvroValue::from_serialize(&result)
            }))
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

    /// Register a replayed-state query on the lossless fixed Avro Value surface.
    pub fn register_replayed_query_avro_value<S, F, Fut>(
        &mut self,
        workflow_type: impl Into<String>,
        query_name: impl Into<String>,
        handler: F,
    ) where
        S: Clone + Send + Sync + 'static,
        F: Fn(QueryContext, Arc<S>, AvroValue) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<AvroValue>> + Send + 'static,
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

    /// Register a synchronous workflow update handler.
    pub fn register_update<F, Fut>(
        &mut self,
        workflow_type: impl Into<String>,
        update_name: impl Into<String>,
        handler: F,
    ) where
        F: Fn(QueryContext, Value) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Value>> + Send + 'static,
    {
        let handler = Arc::new(handler);
        self.updates
            .entry(workflow_type.into())
            .or_default()
            .insert(
                update_name.into(),
                Arc::new(move |ctx, args| {
                    let handler = Arc::clone(&handler);
                    Box::pin(async move {
                        let result = handler(ctx, args.into_json()?).await?;
                        AvroValue::from_serialize(&result)
                    })
                }),
            );
    }

    /// Register an update handler on the lossless fixed Avro Value surface.
    pub fn register_update_avro_value<F, Fut>(
        &mut self,
        workflow_type: impl Into<String>,
        update_name: impl Into<String>,
        handler: F,
    ) where
        F: Fn(QueryContext, AvroValue) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<AvroValue>> + Send + 'static,
    {
        self.updates
            .entry(workflow_type.into())
            .or_default()
            .insert(
                update_name.into(),
                Arc::new(move |ctx, args| Box::pin(handler(ctx, args))),
            );
    }

    pub async fn register(&self) -> Result<RegisterWorkerResponse> {
        let mut command_contracts = serde_json::Map::new();
        for workflow_type in self.workflows.keys() {
            let mut queries = self
                .queries
                .get(workflow_type)
                .map(|handlers| handlers.keys().cloned().collect::<Vec<_>>())
                .unwrap_or_default();
            queries.sort();
            let mut updates = self
                .updates
                .get(workflow_type)
                .map(|handlers| handlers.keys().cloned().collect::<Vec<_>>())
                .unwrap_or_default();
            updates.sort();
            command_contracts.insert(
                workflow_type.clone(),
                json!({
                    "queries": queries,
                    "query_contracts": [],
                    "signals": [],
                    "signal_contracts": [],
                    "updates": updates,
                    "update_contracts": [],
                    "update_validators": [],
                }),
            );
        }

        self.client
            .register_worker_with_command_contracts(
                &self.worker_id,
                &self.task_queue,
                self.workflows.keys().cloned().collect(),
                self.activities.keys().cloned().collect(),
                self.max_concurrent_workflow_tasks,
                self.max_concurrent_activity_tasks,
                [
                    Some(CONDITION_WAIT_OCCURRENCE_IDENTITY_CAPABILITY.to_string()),
                    Some(DURABLE_SELECTION_CAPABILITY.to_string()),
                    Some(MEMO_UPSERTS_CAPABILITY.to_string()),
                    Some(TYPED_SEARCH_ATTRIBUTES_CAPABILITY.to_string()),
                    (!self.queries.is_empty()).then(|| QUERY_TASKS_CAPABILITY.to_string()),
                    (!self.updates.is_empty()).then(|| WORKFLOW_UPDATES_CAPABILITY.to_string()),
                    worker_protocol_supports_message_streams(WORKER_PROTOCOL_VERSION)
                        .then(|| MESSAGE_STREAMS_CAPABILITY.to_string()),
                ]
                .into_iter()
                .flatten()
                .collect(),
                Value::Object(command_contracts),
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
        if !registration.registered {
            return Err(Error::WorkerLoop(format!(
                "worker registration for {:?} was not accepted",
                self.worker_id
            )));
        }
        let registered_worker_id = registration.worker_id.clone();
        let primary = self.run_registered_until(shutdown, registration).await;
        let deregistration = self
            .client
            .deregister_worker_registration(&registered_worker_id)
            .await;

        match (primary, deregistration) {
            (Ok(()), Ok(_)) => Ok(()),
            (Ok(()), Err(deregistration)) => Err(deregistration),
            (Err(primary), Ok(_)) => Err(primary),
            (Err(primary), Err(deregistration)) => Err(Error::WorkerShutdown {
                primary: Box::new(primary),
                deregistration: Box::new(deregistration),
            }),
        }
    }

    async fn run_registered_until<F>(
        &self,
        shutdown: F,
        registration: RegisterWorkerResponse,
    ) -> Result<()>
    where
        F: Future<Output = ()>,
    {
        let heartbeat_interval = Duration::from_secs(
            registration
                .heartbeat_interval_seconds
                .unwrap_or(self.heartbeat_interval.as_secs().max(1)),
        );
        // The first heartbeat is immediate. Subsequent heartbeats are scheduled
        // from the completion of the preceding attempt, including its bounded
        // retries. A fixed-epoch interval can leave an already-due tick queued
        // while an acknowledgement is slow, producing a catch-up heartbeat as
        // soon as that request completes.
        let heartbeat = tokio::time::sleep(Duration::ZERO);
        tokio::pin!(heartbeat);
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
                _ = &mut heartbeat => {
                    let result = self.retry_worker_operation(|| {
                        self.client.heartbeat_worker(
                            &self.worker_id,
                            self.max_concurrent_workflow_tasks,
                            self.max_concurrent_activity_tasks,
                        )
                    }).await;
                    heartbeat
                        .as_mut()
                        .reset(tokio::time::Instant::now() + heartbeat_interval);
                    match result {
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
                    let stopped_by_server = stop.load(Ordering::SeqCst);
                    stop.store(true, Ordering::SeqCst);
                    let poller_result = optional_poller_result("workflow", result);
                    let join_result =
                        join_pollers(workflow_poller.take(), activity_poller.take(), query_poller.take()).await;
                    poller_result?;
                    join_result?;
                    if stopped_by_server {
                        return Ok(());
                    }
                    return Err(Error::WorkerLoop(
                        "workflow poller stopped unexpectedly".to_string(),
                    ));
                }
                result = OptionFuture::from(activity_poller.as_mut()), if activity_poller.is_some() => {
                    activity_poller = None;
                    let stopped_by_server = stop.load(Ordering::SeqCst);
                    stop.store(true, Ordering::SeqCst);
                    let poller_result = optional_poller_result("activity", result);
                    let join_result =
                        join_pollers(workflow_poller.take(), activity_poller.take(), query_poller.take()).await;
                    poller_result?;
                    join_result?;
                    if stopped_by_server {
                        return Ok(());
                    }
                    return Err(Error::WorkerLoop(
                        "activity poller stopped unexpectedly".to_string(),
                    ));
                }
                result = OptionFuture::from(query_poller.as_mut()), if query_poller.is_some() => {
                    query_poller = None;
                    let stopped_by_server = stop.load(Ordering::SeqCst);
                    stop.store(true, Ordering::SeqCst);
                    let poller_result = optional_poller_result("query", result);
                    let join_result =
                        join_pollers(workflow_poller.take(), activity_poller.take(), query_poller.take()).await;
                    poller_result?;
                    join_result?;
                    if stopped_by_server {
                        return Ok(());
                    }
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

    /// Poll and settle at most one task from each enabled task family.
    ///
    /// A workflow may reach its server-enforced run deadline while this worker
    /// holds a task. When the completion endpoint authoritatively rejects that
    /// selected task and run with `recorded=false`, `reason=run_timed_out`, and
    /// terminal `run_status=failed`, the workflow tick is considered settled:
    /// the late command was not recorded and cannot replace the terminal run.
    /// Every other completion rejection remains an error. This worker-level
    /// race handling is distinct from [`WorkflowResultOptions::timeout`], which
    /// only bounds how long a client waits for a result.
    ///
    /// Direct callers of [`Client::complete_workflow_task`] continue to receive
    /// the original [`Error::Http`] status and response body.
    pub async fn run_once(&self) -> Result<usize> {
        let mut handled = 0;
        match self.poll_workflow_once().await? {
            ManagedPollOutcome::Handled => handled += 1,
            ManagedPollOutcome::Stop => return Ok(handled),
            ManagedPollOutcome::Idle => {}
        }
        match self.poll_activity_once().await? {
            ManagedPollOutcome::Handled => handled += 1,
            ManagedPollOutcome::Stop => return Ok(handled),
            ManagedPollOutcome::Idle => {}
        }
        if !self.queries.is_empty() {
            match self.poll_query_once().await? {
                ManagedPollOutcome::Handled => handled += 1,
                ManagedPollOutcome::Stop => return Ok(handled),
                ManagedPollOutcome::Idle => {}
            }
        }
        Ok(handled)
    }

    async fn poll_workflow_once(&self) -> Result<ManagedPollOutcome> {
        let poll_request_id = unique_request_id("rust-workflow-poll");
        let response = self
            .retry_worker_operation(|| {
                self.client.poll_workflow_task_response_with_request_id(
                    &self.worker_id,
                    &self.task_queue,
                    self.poll_timeout,
                    &poll_request_id,
                    0,
                )
            })
            .await;
        let Some(response) = self.settle_worker_poll_response(response).await? else {
            return Ok(ManagedPollOutcome::Idle);
        };
        if response.outcome().should_stop() {
            return Ok(ManagedPollOutcome::Stop);
        }
        let memo_updates_supported =
            runtime_supports_workflow_memo_updates(response.server_capabilities.as_ref());
        let Some(task) = response.task else {
            return Ok(ManagedPollOutcome::Idle);
        };

        let task_id = task.task_id.clone();
        let attempt = task.workflow_task_attempt;
        let run_id = task.run_id.clone();
        let lease_owner = task
            .lease_owner
            .clone()
            .unwrap_or_else(|| self.worker_id.clone());

        match self.execute_workflow_task_decision(task) {
            Ok(decision)
                if commands_use_workflow_memo_updates(&decision.commands)
                    && !memo_updates_supported =>
            {
                self.client
                    .fail_workflow_task(
                        &task_id,
                        &lease_owner,
                        attempt,
                        Error::WorkflowMemoUpdatesUnavailable.to_string(),
                    )
                    .await?;
            }
            Ok(decision) if decision.commands.is_empty() => {
                // A replay can consume a recorded pending durable command
                // without producing a new command. The standalone protocol
                // acknowledges that state through the typed waiting outcome;
                // an empty completion is rejected by servers that require at
                // least one executable command.
                self.client
                    .fail_workflow_task_with_type(
                        &task_id,
                        &lease_owner,
                        attempt,
                        WORKFLOW_TASK_WAITING_FOR_HISTORY_MESSAGE,
                        WORKFLOW_TASK_WAITING_FOR_HISTORY_TYPE,
                    )
                    .await?;
            }
            Ok(decision) => {
                let completion = self
                    .client
                    .complete_workflow_task_with_message_streams(
                        &task_id,
                        &lease_owner,
                        attempt,
                        decision.commands,
                        decision.message_stream_cursors,
                        decision.message_stream_waits,
                    )
                    .await;
                if let Err(error) = completion {
                    if !workflow_task_completion_is_terminal_timeout(
                        &error,
                        &task_id,
                        attempt,
                        run_id.as_deref(),
                    ) {
                        return Err(error);
                    }
                }
            }
            Err(error) => {
                self.client
                    .fail_workflow_task(&task_id, &lease_owner, attempt, error.to_string())
                    .await?;
            }
        }

        Ok(ManagedPollOutcome::Handled)
    }

    async fn poll_workflows_until_stopped(self, stop: Arc<AtomicBool>) -> Result<()> {
        while !stop.load(Ordering::SeqCst) {
            if self.poll_workflow_once().await? == ManagedPollOutcome::Stop {
                stop.store(true, Ordering::SeqCst);
                break;
            }
        }

        Ok(())
    }

    async fn poll_activity_once(&self) -> Result<ManagedPollOutcome> {
        let poll_request_id = unique_request_id("rust-activity-poll");
        let response = self
            .retry_worker_operation(|| {
                self.client.poll_activity_task_response_with_request_id(
                    &self.worker_id,
                    &self.task_queue,
                    self.poll_timeout,
                    &poll_request_id,
                    0,
                )
            })
            .await;
        let Some(response) = self.settle_worker_poll_response(response).await? else {
            return Ok(ManagedPollOutcome::Idle);
        };
        if response.outcome().should_stop() {
            return Ok(ManagedPollOutcome::Stop);
        }
        let Some(task) = response.task else {
            return Ok(ManagedPollOutcome::Idle);
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
                let completion = self
                    .client
                    .complete_activity_task(&task_id, &attempt_id, &lease_owner, value, &codec)
                    .await;
                if let Err(error) = completion {
                    if !activity_task_rejection_is_final(&error) {
                        return Err(error);
                    }
                }
            }
            Err(error) => {
                let failure = self
                    .client
                    .fail_activity_task(
                        &task_id,
                        &attempt_id,
                        &lease_owner,
                        error.to_string(),
                        false,
                    )
                    .await;
                if let Err(error) = failure {
                    if !activity_task_rejection_is_final(&error) {
                        return Err(error);
                    }
                }
            }
        }

        Ok(ManagedPollOutcome::Handled)
    }

    async fn poll_activities_until_stopped(self, stop: Arc<AtomicBool>) -> Result<()> {
        while !stop.load(Ordering::SeqCst) {
            if self.poll_activity_once().await? == ManagedPollOutcome::Stop {
                stop.store(true, Ordering::SeqCst);
                break;
            }
        }

        Ok(())
    }

    async fn poll_query_once(&self) -> Result<ManagedPollOutcome> {
        let poll_request_id = unique_request_id("rust-query-poll");
        let response = self
            .retry_worker_operation(|| {
                self.client.poll_query_task_response_with_request_id(
                    &self.worker_id,
                    &self.task_queue,
                    self.poll_timeout,
                    &poll_request_id,
                    0,
                )
            })
            .await;
        let Some(response) = self.settle_worker_poll_response(response).await? else {
            return Ok(ManagedPollOutcome::Idle);
        };
        if response.outcome().should_stop() {
            return Ok(ManagedPollOutcome::Stop);
        }
        let Some(task) = response.task else {
            return Ok(ManagedPollOutcome::Idle);
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
                let result_envelope = match encode_typed_envelope(&value, &codec) {
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
                        return Ok(ManagedPollOutcome::Handled);
                    }
                };

                if let Err(error) = self
                    .client
                    .complete_query_task_with_envelope(
                        &query_task_id,
                        &lease_owner,
                        attempt,
                        value.clone().into_json()?,
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

        Ok(ManagedPollOutcome::Handled)
    }

    async fn poll_queries_until_stopped(self, stop: Arc<AtomicBool>) -> Result<()> {
        while !stop.load(Ordering::SeqCst) {
            if self.poll_query_once().await? == ManagedPollOutcome::Stop {
                stop.store(true, Ordering::SeqCst);
                break;
            }
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

    async fn settle_worker_poll_response<T>(&self, response: Result<T>) -> Result<Option<T>> {
        match response {
            Ok(response) => Ok(Some(response)),
            Err(error) => {
                let Some(advertised_delay) = worker_poll_capacity_retry_after(&error) else {
                    return Err(error);
                };
                let minimum_delay = self
                    .retry_policy
                    .initial_backoff
                    .max(Duration::from_millis(1));
                let maximum_delay = self.retry_policy.max_backoff.max(minimum_delay);
                tokio::time::sleep(advertised_delay.max(minimum_delay).min(maximum_delay)).await;
                Ok(None)
            }
        }
    }

    async fn execute_query_task(
        &self,
        mut task: QueryTask,
    ) -> std::result::Result<AvroValue, QueryTaskExecutionFailure> {
        validate_query_task_payloads(&task).map_err(|error| {
            QueryTaskExecutionFailure::new(
                "query_payload_decode_failed",
                error.to_string(),
                "QueryPayloadDecodeFailed",
            )
        })?;

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

        let args = decode_task_avro_arguments(task.query_arguments.as_ref(), &task.payload_codec)
            .map_err(|error| {
            QueryTaskExecutionFailure::new(
                "query_payload_decode_failed",
                format!("cannot decode query arguments: {error}"),
                "QueryPayloadDecodeFailed",
            )
        })?;
        let workflow_input_typed =
            decode_task_avro_arguments(task.workflow_arguments.as_ref(), &task.payload_codec)
                .map_err(|error| {
                    QueryTaskExecutionFailure::new(
                        "query_workflow_state_unavailable",
                        format!("cannot decode workflow start input: {error}"),
                        "QueryWorkflowStateUnavailable",
                    )
                })?;
        let workflow_input = workflow_input_typed.clone().into_json().map_err(|error| {
            QueryTaskExecutionFailure::new(
                "query_workflow_state_unavailable",
                format!("cannot project workflow start input: {error}"),
                "QueryWorkflowStateUnavailable",
            )
        })?;
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
            workflow_input_avro_value: workflow_input_typed.clone(),
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
                let workflow_state = Arc::new(Mutex::new(
                    WorkflowState::new_with_identity(
                        history_events.as_ref().clone(),
                        context.workflow_id.clone(),
                        context.run_id.clone(),
                        self.task_queue.clone(),
                        task.payload_codec,
                        None,
                    )
                    .map_err(|error| {
                        QueryTaskExecutionFailure::new(
                            "query_workflow_state_unavailable",
                            format!("workflow replay failed before query: {error}"),
                            "QueryWorkflowStateUnavailable",
                        )
                    })?,
                ));
                let workflow_context = WorkflowContext {
                    state: workflow_state,
                };
                let mut invocation = replay(workflow_context.clone(), workflow_input_typed.clone());
                let mut cx = TaskContext::from_waker(noop_waker_ref());
                match invocation.future.as_mut().poll(&mut cx) {
                    Poll::Ready(Ok(_)) => {
                        workflow_context
                            .ensure_history_consumed()
                            .map_err(|error| {
                                QueryTaskExecutionFailure::new(
                                    "query_workflow_state_unavailable",
                                    format!("workflow replay failed before query: {error}"),
                                    "QueryWorkflowStateUnavailable",
                                )
                            })?;
                    }
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
                        if commands.is_empty()
                            && !workflow_context
                                .matched_recorded_pending()
                                .map_err(|error| {
                                    QueryTaskExecutionFailure::new(
                                        "query_workflow_state_unavailable",
                                        format!("workflow replay failed before query: {error}"),
                                        "QueryWorkflowStateUnavailable",
                                    )
                                })?
                        {
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

    #[cfg(test)]
    fn execute_workflow_task(&self, task: WorkflowTask) -> Result<Vec<Value>> {
        Ok(self.execute_workflow_task_decision(task)?.commands)
    }

    fn execute_workflow_task_decision(&self, task: WorkflowTask) -> Result<WorkflowTaskDecision> {
        validate_workflow_task_payloads(&task)?;

        if let Some(update_id) = task
            .workflow_update_id
            .as_deref()
            .filter(|update_id| !update_id.is_empty())
        {
            return self
                .execute_update_task(&task, update_id)
                .map(WorkflowTaskDecision::without_message_streams);
        }

        let workflow = self
            .workflows
            .get(&task.workflow_type)
            .ok_or_else(|| Error::WorkflowNotRegistered(task.workflow_type.clone()))?;
        let input = decode_task_avro_arguments(task.arguments.as_ref(), &task.payload_codec)?;
        let resume_signal = decode_resume_signal(&task)?;
        let history_budget = WorkflowHistoryBudget {
            event_count: task
                .total_history_events
                .unwrap_or_else(|| u64::try_from(task.history_events.len()).unwrap_or(u64::MAX)),
            size_bytes: task.history_size_bytes,
            continue_as_new_recommended: task.continue_as_new_recommended.unwrap_or(false),
            pressure: task.history_budget_pressure.clone(),
        };
        let workflow_command_identity = task
            .workflow_command_id
            .clone()
            .filter(|identity| !identity.is_empty())
            .unwrap_or_default();
        let mut workflow_state = WorkflowState::new_with_identity(
            task.history_events,
            task.workflow_id,
            task.run_id,
            self.task_queue.clone(),
            task.payload_codec.clone(),
            resume_signal,
        )?;
        workflow_state.history_budget = history_budget;
        workflow_state.workflow_command_identity = workflow_command_identity;
        workflow_state.cancel_requested = task.cancel_requested;
        let state = Arc::new(Mutex::new(workflow_state));
        let ctx = WorkflowContext { state };
        let mut future = (workflow.execute)(ctx.clone(), input);
        let mut cx = TaskContext::from_waker(noop_waker_ref());

        match future.as_mut().poll(&mut cx) {
            Poll::Ready(Ok(result)) => {
                ctx.ensure_history_consumed()?;
                let result = encode_typed_envelope(&result, &task.payload_codec)?;
                let mut commands = ctx.take_commands()?;
                commands.push(json!({
                    "type": "complete_workflow",
                    "result": result
                }));
                self.message_stream_decision(&ctx, commands)
            }
            Poll::Ready(Err(error)) => {
                if let Error::ContinueAsNew(request) = error {
                    let mut commands = ctx.take_commands()?;
                    if let Some(command) = ctx.continue_as_new_command(request)? {
                        commands.push(command);
                    }
                    ctx.ensure_history_consumed()?;
                    return self.message_stream_decision(&ctx, commands);
                }
                if workflow_task_integrity_error(&error) {
                    // Replay and protocol failures describe the workflow-task
                    // decision itself. Preserve their specific failure reason
                    // instead of replacing it with the derivative fact that
                    // recorded commands remain unconsumed.
                    return Err(error);
                }
                // A handler error must not hide a committed durable command that
                // upgraded workflow code no longer consumes.
                ctx.ensure_history_consumed()?;
                let mut commands = ctx.take_commands()?;
                commands.push(workflow_failure_command(&error));
                self.message_stream_decision(&ctx, commands)
            }
            Poll::Pending => {
                let commands = ctx.take_commands()?;
                if commands.is_empty() && !ctx.matched_recorded_pending()? {
                    Err(Error::WorkflowYieldedWithoutCommand)
                } else {
                    self.message_stream_decision(&ctx, commands)
                }
            }
        }
    }

    fn message_stream_decision(
        &self,
        ctx: &WorkflowContext,
        commands: Vec<Value>,
    ) -> Result<WorkflowTaskDecision> {
        let (message_stream_cursors, message_stream_waits) = ctx.message_stream_metadata()?;
        Ok(WorkflowTaskDecision {
            commands,
            message_stream_cursors,
            message_stream_waits,
        })
    }

    fn execute_update_task(&self, task: &WorkflowTask, update_id: &str) -> Result<Vec<Value>> {
        if !self.workflows.contains_key(&task.workflow_type) {
            return Err(Error::WorkflowNotRegistered(task.workflow_type.clone()));
        }

        let accepted = task.history_events.iter().rev().find_map(|event| {
            (event.event_type == "UpdateAccepted"
                && event.payload.get("update_id").and_then(Value::as_str) == Some(update_id))
            .then_some(&event.payload)
        });
        let update_name = accepted
            .and_then(|payload| payload.get("update_name"))
            .and_then(Value::as_str)
            .or(task.update_name.as_deref())
            .unwrap_or_default();
        let Some(handler) = self
            .updates
            .get(&task.workflow_type)
            .and_then(|handlers| handlers.get(update_name))
        else {
            return Ok(vec![json!({
                "type": "fail_update",
                "update_id": update_id,
                "message": format!(
                    "no update handler is registered for {}.{update_name}",
                    task.workflow_type
                ),
                "exception_type": "UnknownUpdate",
                "non_retryable": true,
            })]);
        };
        let arguments = accepted
            .and_then(|payload| payload.get("arguments"))
            .or(task.arguments.as_ref());
        let arguments = decode_task_avro_arguments(arguments, &task.payload_codec)?;
        let context = QueryContext {
            workflow_id: task.workflow_id.clone(),
            run_id: task.run_id.clone(),
            workflow_type: task.workflow_type.clone(),
            run_status: Some("running".to_string()),
            workflow_input: Value::Null,
            workflow_input_avro_value: AvroValue::Null,
            history_events: Arc::new(task.history_events.clone()),
            signal_events: Arc::new(Vec::new()),
        };
        let mut future = handler(context, arguments);
        let mut cx = TaskContext::from_waker(noop_waker_ref());

        match future.as_mut().poll(&mut cx) {
            Poll::Ready(Ok(result)) => Ok(vec![json!({
                "type": "complete_update",
                "update_id": update_id,
                "result": encode_typed_envelope(&result, &task.payload_codec)?,
            })]),
            Poll::Ready(Err(error)) => Ok(vec![json!({
                "type": "fail_update",
                "update_id": update_id,
                "message": error.to_string(),
                "exception_type": "UpdateFailed",
                "non_retryable": true,
            })]),
            Poll::Pending => Err(Error::WorkflowYieldedWithoutCommand),
        }
    }

    async fn execute_activity_task(&self, task: ActivityTask) -> Result<AvroValue> {
        validate_activity_task_payloads(&task)?;

        let handler = self
            .activities
            .get(&task.activity_type)
            .ok_or_else(|| Error::ActivityNotRegistered(task.activity_type.clone()))?;
        let args = decode_task_avro_arguments(task.arguments.as_ref(), &task.payload_codec)?;
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

fn percent_encode_path_segment(segment: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut encoded = String::with_capacity(segment.len());

    for byte in segment.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            encoded.push(char::from(byte));
        } else {
            encoded.push('%');
            encoded.push(char::from(HEX[(byte >> 4) as usize]));
            encoded.push(char::from(HEX[(byte & 0x0f) as usize]));
        }
    }

    encoded
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

#[derive(Clone, Debug, PartialEq)]
pub struct MessageStreamMessage {
    pub stream_name: String,
    pub message_id: String,
    pub position: u64,
    pub arguments: Vec<AvroValue>,
}

#[derive(Clone, Debug)]
pub struct MessageStream {
    ctx: WorkflowContext,
    name: String,
}

impl MessageStream {
    /// Wait for one message, then return a bounded currently-available batch.
    pub async fn receive(&self, max_items: usize) -> Result<Vec<MessageStreamMessage>> {
        if !(1..=MESSAGE_STREAM_MAX_BATCH).contains(&max_items) {
            return Err(Error::Codec(format!(
                "message stream max_items must be between 1 and {MESSAGE_STREAM_MAX_BATCH}"
            )));
        }
        loop {
            if let Some(batch) = self.ctx.take_message_stream_batch(&self.name, max_items)? {
                return Ok(batch);
            }

            self.ctx.record_message_stream_wait(&self.name)?;
            let replay_wait_sequence = self.ctx.next_message_stream_wait_sequence()?;
            let arguments = self.ctx.wait_runtime_signal(MESSAGE_STREAM_SIGNAL).await?;
            self.ctx.buffer_message_stream_delivery(arguments)?;
            if let Some(sequence) = replay_wait_sequence {
                self.ctx.buffer_message_stream_history_for_wait(sequence)?;
            }
        }
    }

    pub async fn receive_one(&self) -> Result<MessageStreamMessage> {
        self.receive(1)
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| Error::Codec("message stream resumed without a message".to_string()))
    }
}

#[derive(Clone, Debug)]
pub struct WorkflowContext {
    state: Arc<Mutex<WorkflowState>>,
}

fn valid_memo_key(key: &str) -> bool {
    let numeric_candidate = key.strip_prefix('-').unwrap_or(key);

    !key.is_empty()
        && key.len() <= 64
        && (numeric_candidate.is_empty()
            || !numeric_candidate.bytes().all(|byte| byte.is_ascii_digit()))
        && key
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b':' | b'-'))
}

fn avro_encoded_size(value: &AvroValue) -> Result<usize> {
    BASE64
        .decode(encode_avro_value(value)?.blob)
        .map(|bytes| bytes.len())
        .map_err(|error| Error::Codec(format!("memo Avro encoding was not strict base64: {error}")))
}

fn canonical_memo_entries(value: AvroValue, require_entries: bool) -> Result<AvroValue> {
    let AvroValue::Map(entries) = value else {
        return Err(Error::InvalidMemoUpdate(
            "entries must serialize to an Avro string-keyed map".to_string(),
        ));
    };
    if require_entries && entries.is_empty() {
        return Err(Error::InvalidMemoUpdate(
            "at least one entry is required".to_string(),
        ));
    }
    if entries.len() > MAX_MEMO_ENTRIES {
        return Err(Error::InvalidMemoUpdate(format!(
            "at most {MAX_MEMO_ENTRIES} entries are allowed"
        )));
    }

    for (key, value) in &entries {
        if !valid_memo_key(&key) {
            return Err(Error::InvalidMemoUpdate(
                "keys must match ^(?!-?[0-9]+$)[A-Za-z0-9_.:-]{1,64}$".to_string(),
            ));
        }
        if avro_encoded_size(value)? > MAX_MEMO_VALUE_SIZE_BYTES {
            return Err(Error::InvalidMemoUpdate(format!(
                "value {key:?} exceeds the {MAX_MEMO_VALUE_SIZE_BYTES}-byte limit"
            )));
        }
    }

    let value = AvroValue::Map(entries);
    if avro_encoded_size(&value)? > MAX_MEMO_TOTAL_SIZE_BYTES {
        return Err(Error::InvalidMemoUpdate(format!(
            "update exceeds the {MAX_MEMO_TOTAL_SIZE_BYTES}-byte total limit"
        )));
    }
    Ok(value)
}

fn decode_memo_history_map(envelope: &Value, require_entries: bool) -> Result<AvroValue> {
    let object = envelope.as_object().ok_or_else(|| {
        Error::InvalidMemoUpdate(
            "history field must use the public {codec, blob} payload envelope".to_string(),
        )
    })?;
    if object.len() != 2 || !object.contains_key("codec") || !object.contains_key("blob") {
        return Err(Error::InvalidMemoUpdate(
            "history field must use exactly the public {codec, blob} payload envelope".to_string(),
        ));
    }

    canonical_memo_entries(
        decode_wire_avro_value(envelope, DEFAULT_CODEC)?,
        require_entries,
    )
}

impl WorkflowContext {
    pub fn message_stream(&self, name: impl Into<String>) -> Result<MessageStream> {
        let name = name.into();
        if name.is_empty()
            || name.len() > 128
            || !name.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-')
            })
        {
            return Err(Error::Codec(
                "message stream names must contain 1-128 letters, numbers, periods, underscores, colons, or hyphens"
                    .to_string(),
            ));
        }
        Ok(MessageStream {
            ctx: self.clone(),
            name,
        })
    }

    fn record_message_stream_wait(&self, name: &str) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::WorkflowStatePoisoned)?;
        let position = state.message_stream_cursors.get(name).copied().unwrap_or(0);
        state
            .message_stream_waits
            .insert(name.to_string(), position);
        Ok(())
    }

    fn buffer_message_stream(&self, message: MessageStreamMessage) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::WorkflowStatePoisoned)?;
        let cursor = state
            .message_stream_cursors
            .get(&message.stream_name)
            .copied()
            .unwrap_or(0);
        if message.position <= cursor {
            return Ok(());
        }
        let pending = state
            .message_stream_messages
            .entry(message.stream_name.clone())
            .or_default();
        if pending.iter().any(|candidate| {
            candidate.position == message.position || candidate.message_id == message.message_id
        }) {
            return Ok(());
        }
        pending.push(message);
        pending.sort_by_key(|candidate| candidate.position);
        Ok(())
    }

    fn buffer_message_stream_delivery(&self, arguments: Vec<Value>) -> Result<Option<String>> {
        if let Some(delivery) = decode_message_stream_delivery(arguments)? {
            match delivery {
                MessageStreamDelivery::Message(message) => {
                    let stream_name = message.stream_name.clone();
                    self.buffer_message_stream(message)?;
                    return Ok(Some(stream_name));
                }
                MessageStreamDelivery::Cursor {
                    stream_name,
                    through_position,
                } => self.apply_message_stream_cursor(&stream_name, through_position)?,
            }
        }
        Ok(None)
    }

    fn next_message_stream_wait_sequence(&self) -> Result<Option<u64>> {
        let state = self
            .state
            .lock()
            .map_err(|_| Error::WorkflowStatePoisoned)?;
        Ok(match state.recorded_commands.get(state.command_cursor) {
            Some(RecordedCommand::SignalWait {
                sequence,
                signal_name,
                ..
            }) if signal_name == MESSAGE_STREAM_SIGNAL => Some(*sequence),
            _ => None,
        })
    }

    fn buffer_message_stream_history_for_wait(&self, wait_sequence: u64) -> Result<()> {
        let (history, payload_codec) = {
            let state = self
                .state
                .lock()
                .map_err(|_| Error::WorkflowStatePoisoned)?;
            (
                Arc::clone(&state.history_events),
                state.payload_codec.clone(),
            )
        };

        let Some(opened_index) = history.iter().position(|event| {
            event.event_type == "SignalWaitOpened"
                && durable_event_sequence(event) == Some(wait_sequence)
                && event.payload.get("signal_name").and_then(Value::as_str)
                    == Some(MESSAGE_STREAM_SIGNAL)
        }) else {
            return Ok(());
        };
        let boundary_index = history
            .iter()
            .enumerate()
            .skip(opened_index + 1)
            .find_map(|(index, event)| {
                (durable_event_sequence(event).is_some_and(|sequence| sequence > wait_sequence)
                    && is_authored_command_open_event(event))
                .then_some(index)
            })
            .unwrap_or(history.len());

        for event in history[opened_index + 1..boundary_index]
            .iter()
            .filter(|event| {
                event.event_type == "SignalReceived"
                    && event.payload.get("signal_name").and_then(Value::as_str)
                        == Some(MESSAGE_STREAM_SIGNAL)
            })
        {
            let arguments = decode_signal_event_arguments(event, &payload_codec)?
                .into_iter()
                .map(AvroValue::into_json)
                .collect::<Result<Vec<_>>>()?;
            self.buffer_message_stream_delivery(arguments)?;
        }
        Ok(())
    }

    fn apply_message_stream_cursor(&self, name: &str, through_position: u64) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::WorkflowStatePoisoned)?;
        let cursor = state
            .message_stream_cursors
            .entry(name.to_string())
            .or_default();
        *cursor = (*cursor).max(through_position);
        if let Some(pending) = state.message_stream_messages.get_mut(name) {
            pending.retain(|message| message.position > through_position);
        }
        Ok(())
    }

    fn take_message_stream_batch(
        &self,
        name: &str,
        max_items: usize,
    ) -> Result<Option<Vec<MessageStreamMessage>>> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::WorkflowStatePoisoned)?;
        let cursor = state.message_stream_cursors.get(name).copied().unwrap_or(0);
        let pending = state
            .message_stream_messages
            .entry(name.to_string())
            .or_default();
        let count = contiguous_message_stream_count(pending, cursor, max_items);
        if count == 0 {
            return Ok(None);
        }
        let batch = pending.drain(..count).collect::<Vec<_>>();
        let position = batch.last().map(|message| message.position).unwrap_or(0);
        state
            .message_stream_cursors
            .insert(name.to_string(), position);
        state.message_stream_waits.remove(name);
        Ok(Some(batch))
    }

    fn message_stream_metadata(&self) -> Result<(Vec<Value>, Vec<Value>)> {
        let state = self
            .state
            .lock()
            .map_err(|_| Error::WorkflowStatePoisoned)?;
        let mut cursors = state.message_stream_cursors.iter().collect::<Vec<_>>();
        cursors.sort_by_key(|(name, _)| *name);
        let mut waits = state.message_stream_waits.iter().collect::<Vec<_>>();
        waits.sort_by_key(|(name, _)| *name);
        Ok((
            cursors
                .into_iter()
                .map(|(name, position)| json!({"stream_name": name, "through_position": position}))
                .collect(),
            waits
                .into_iter()
                .map(|(name, position)| json!({"stream_name": name, "after_position": position}))
                .collect(),
        ))
    }
    /// Identity of the parent workflow currently being replayed.
    pub fn workflow_identity(&self) -> Result<WorkflowIdentity> {
        let state = self
            .state
            .lock()
            .map_err(|_| Error::WorkflowStatePoisoned)?;
        Ok(WorkflowIdentity {
            workflow_id: state.workflow_id.clone(),
            run_id: state.run_id.clone(),
        })
    }

    /// Return the server-published history budget for this workflow task.
    pub fn history_budget(&self) -> Result<WorkflowHistoryBudget> {
        let state = self
            .state
            .lock()
            .map_err(|_| Error::WorkflowStatePoisoned)?;
        Ok(state.history_budget.clone())
    }

    /// Continue this workflow instance as a fresh run with replacement arguments.
    ///
    /// Return this value directly from the workflow handler. The worker converts
    /// it to the terminal protocol command only after replay has consumed every
    /// recorded durable command.
    pub fn continue_as_new<T: Serialize>(&self, args: T) -> Result<Value> {
        self.continue_as_new_with_options(ContinueAsNewOptions::new(), args)
    }

    /// Continue as new with optional workflow-type and task-queue overrides.
    pub fn continue_as_new_with_options<T: Serialize>(
        &self,
        options: ContinueAsNewOptions,
        args: T,
    ) -> Result<Value> {
        options.validate()?;
        Err(Error::ContinueAsNew(ContinueAsNewRequest {
            arguments: normalize_avro_arguments(AvroValue::from_serialize(&args)?),
            options,
        }))
    }

    pub fn activity<T: Serialize>(
        &self,
        activity_type: impl Into<String>,
        args: T,
    ) -> ActivityCall {
        self.activity_with_options(activity_type, ActivityOptions::new(), args)
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
        let mut options = ActivityOptions::new();
        options.task_queue = task_queue.map(Into::into);
        self.activity_with_options(activity_type, options, args)
    }

    /// Schedule one durable activity with retry, routing, and timeout options.
    ///
    /// Options are validated before the command is emitted. Once the command is
    /// recorded, replay consumes the same activity lifecycle at this command
    /// position and never emits a duplicate schedule.
    ///
    /// ```no_run
    /// # use durable_workflow::{json, ActivityOptions, ActivityRetryPolicy, Error, Result, WorkflowContext};
    /// # use std::time::Duration;
    /// # async fn run(ctx: WorkflowContext) -> Result<durable_workflow::Value> {
    /// let result = ctx
    ///     .activity_with_options(
    ///         "charge-card",
    ///         ActivityOptions::new()
    ///             .task_queue("payments")
    ///             .retry_policy(
    ///                 ActivityRetryPolicy::new(4).exponential_backoff(
    ///                     Duration::from_secs(1),
    ///                     2,
    ///                     Some(Duration::from_secs(30)),
    ///                 ),
    ///             )
    ///             .start_to_close_timeout(Duration::from_secs(60))
    ///             .schedule_to_close_timeout(Duration::from_secs(180))
    ///             .heartbeat_timeout(Duration::from_secs(15)),
    ///         json!([{"order_id": "order-42"}]),
    ///     )
    ///     .await;
    /// match result {
    ///     Err(Error::ActivityFailed(failure)) => Ok(json!({
    ///         "reason": failure.reason,
    ///         "timeout_kind": failure.timeout_kind,
    ///     })),
    ///     other => other,
    /// }
    /// # }
    /// ```
    pub fn activity_with_options<T: Serialize>(
        &self,
        activity_type: impl Into<String>,
        options: ActivityOptions,
        args: T,
    ) -> ActivityCall {
        ActivityCall {
            ctx: self.clone(),
            activity_type: activity_type.into(),
            options,
            args: Some(AvroValue::from_serialize(&args)),
            scheduled: false,
            parallel_group_path: Vec::new(),
        }
    }

    pub async fn activity_avro_value<T: Serialize>(
        &self,
        activity_type: impl Into<String>,
        args: T,
    ) -> Result<AvroValue> {
        let mut call = self.activity(activity_type, args);
        std::future::poll_fn(|cx| Pin::new(&mut call).poll_avro_value(cx)).await
    }

    pub async fn activity_avro_value_with_options<T: Serialize>(
        &self,
        activity_type: impl Into<String>,
        options: ActivityOptions,
        args: T,
    ) -> Result<AvroValue> {
        let mut call = self.activity_with_options(activity_type, options, args);
        std::future::poll_fn(|cx| Pin::new(&mut call).poll_avro_value(cx)).await
    }

    /// Schedule an activity with a Serde request and decode its Serde result.
    pub async fn activity_typed<I, O>(&self, activity_type: impl Into<String>, args: I) -> Result<O>
    where
        I: Serialize,
        O: DeserializeOwned,
    {
        self.activity_typed_with_options(activity_type, ActivityOptions::new(), args)
            .await
    }

    /// Schedule an activity with options and decode its result into `O`.
    ///
    /// Both directions use the fixed Avro Value codec. In particular, this
    /// method does not deserialize the JSON-safe inspection projection returned
    /// by the dynamic [`ActivityCall`] future.
    pub async fn activity_typed_with_options<I, O>(
        &self,
        activity_type: impl Into<String>,
        options: ActivityOptions,
        args: I,
    ) -> Result<O>
    where
        I: Serialize,
        O: DeserializeOwned,
    {
        let activity_type = activity_type.into();
        let encoded = AvroValue::from_serialize(&args).map_err(|error| {
            handler_type_error::<I>(
                HandlerKind::Activity,
                &activity_type,
                HandlerValueKind::Input,
                error.to_string(),
            )
        });
        let mut call = ActivityCall {
            ctx: self.clone(),
            activity_type: activity_type.clone(),
            options,
            args: Some(encoded),
            scheduled: false,
            parallel_group_path: Vec::new(),
        };
        let result = std::future::poll_fn(|cx| Pin::new(&mut call).poll_avro_value(cx)).await?;
        decode_handler_result(result, HandlerKind::Activity, &activity_type)
    }

    /// Schedule and join a deterministic activity/child/timer group.
    ///
    /// Nested groups retain their input shape. Every durable leaf is scheduled
    /// before this future yields, results are assembled by declaration order,
    /// and a failure returns [`Error::ParallelFailed`] with typed cause,
    /// declaration path, stable group metadata, and completed siblings.
    pub fn parallel(&self, operations: Vec<ParallelOperation>) -> ParallelCall {
        ParallelCall::new(self.clone(), operations)
    }

    /// Alias for [`WorkflowContext::parallel`].
    pub fn join(&self, operations: Vec<ParallelOperation>) -> ParallelCall {
        self.parallel(operations)
    }

    /// Lossless fixed-Avro variant of [`WorkflowContext::parallel`].
    pub async fn parallel_avro_value(
        &self,
        operations: Vec<ParallelOperation>,
    ) -> Result<Vec<ParallelAvroResult>> {
        let mut call = self.parallel(operations);
        std::future::poll_fn(|cx| Pin::new(&mut call).poll_avro_value(cx)).await
    }

    /// Start every durable operation and resume from the one winner persisted
    /// by Server. Non-winning operations continue and remain addressable.
    pub fn select(&self, operations: Vec<ParallelOperation>) -> SelectCall {
        let operations = operations
            .into_iter()
            .enumerate()
            .map(|(index, operation)| (SelectionKey::Index(index), operation))
            .collect();
        SelectCall::new(self.clone(), operations)
    }

    /// Named-key variant of [`WorkflowContext::select`].
    pub fn select_keyed<K>(&self, operations: Vec<(K, ParallelOperation)>) -> SelectCall
    where
        K: Into<SelectionKey>,
    {
        SelectCall::new(
            self.clone(),
            operations
                .into_iter()
                .map(|(key, operation)| (key.into(), operation))
                .collect(),
        )
    }

    /// Create a workflow-local deterministic compensation registry.
    pub fn saga(&self) -> Saga {
        Saga::new(self.clone())
    }

    /// Whether the current workflow task carries a cooperative cancel request.
    pub fn is_cancellation_requested(&self) -> Result<bool> {
        let state = self
            .state
            .lock()
            .map_err(|_| Error::WorkflowStatePoisoned)?;
        Ok(state.cancel_requested)
    }

    /// Raise a typed cooperative cancellation at an author-controlled point.
    ///
    /// Passing this result to [`Saga::finish`] compensates already registered
    /// forward steps before the cancellation remains the initiating outcome.
    pub fn throw_if_cancellation_requested(&self) -> Result<()> {
        if self.is_cancellation_requested()? {
            return Err(Error::WorkflowCancellationRequested(
                WorkflowCancellationRequested,
            ));
        }
        Ok(())
    }

    pub fn wait_signal(&self, signal_name: impl Into<String>) -> SignalCall {
        SignalCall {
            ctx: self.clone(),
            signal_name: signal_name.into(),
            runtime_reserved_allowed: false,
            opened_wait: false,
            matched_pending: false,
            parallel_group_path: Vec::new(),
        }
    }

    fn wait_runtime_signal(&self, signal_name: impl Into<String>) -> SignalCall {
        SignalCall {
            ctx: self.clone(),
            signal_name: signal_name.into(),
            runtime_reserved_allowed: true,
            opened_wait: false,
            matched_pending: false,
            parallel_group_path: Vec::new(),
        }
    }

    pub async fn wait_signal_avro_value(
        &self,
        signal_name: impl Into<String>,
    ) -> Result<Vec<AvroValue>> {
        let mut call = self.wait_signal(signal_name);
        std::future::poll_fn(|cx| Pin::new(&mut call).poll_avro_value(cx)).await
    }

    /// Return every committed signal argument list with the given name.
    ///
    /// This history-backed view is deterministic and is intended for
    /// condition predicates that must be re-evaluated after a signal while the
    /// workflow is blocked on [`WorkflowContext::wait_condition`].
    pub fn signals(&self, signal_name: &str) -> Result<Vec<Vec<Value>>> {
        self.signals_avro_value(signal_name)?
            .into_iter()
            .map(|arguments| {
                arguments
                    .into_iter()
                    .map(AvroValue::into_json)
                    .collect::<Result<Vec<_>>>()
            })
            .collect()
    }

    /// Lossless fixed Avro Value view of committed signals with the given name.
    pub fn signals_avro_value(&self, signal_name: &str) -> Result<Vec<Vec<AvroValue>>> {
        let state = self
            .state
            .lock()
            .map_err(|_| Error::WorkflowStatePoisoned)?;
        state
            .history_events
            .iter()
            .filter(|event| {
                event.event_type == "SignalReceived"
                    && event.payload.get("signal_name").and_then(Value::as_str) == Some(signal_name)
            })
            .map(|event| decode_signal_event_arguments(event, &state.payload_codec))
            .collect()
    }

    /// Return every committed update argument list with the given name.
    ///
    /// Accepted and applied records for the same update ID are de-duplicated.
    /// A Server task created after an update therefore replays the workflow and
    /// re-evaluates an open condition without application polling.
    pub fn updates(&self, update_name: &str) -> Result<Vec<Vec<Value>>> {
        self.updates_avro_value(update_name)?
            .into_iter()
            .map(|arguments| {
                arguments
                    .into_iter()
                    .map(AvroValue::into_json)
                    .collect::<Result<Vec<_>>>()
            })
            .collect()
    }

    /// Lossless fixed Avro Value view of committed updates with the given name.
    pub fn updates_avro_value(&self, update_name: &str) -> Result<Vec<Vec<AvroValue>>> {
        let state = self
            .state
            .lock()
            .map_err(|_| Error::WorkflowStatePoisoned)?;
        let mut seen = Vec::new();
        let mut updates = Vec::new();
        for event in state.history_events.iter() {
            if !matches!(
                event.event_type.as_str(),
                "UpdateAccepted" | "UpdateApplied"
            ) || event.payload.get("update_name").and_then(Value::as_str) != Some(update_name)
                || event.payload.get("arguments").is_none()
            {
                continue;
            }
            if let Some(update_id) = event.payload.get("update_id").and_then(Value::as_str) {
                if seen.iter().any(|recorded| recorded == update_id) {
                    continue;
                }
                seen.push(update_id.to_string());
            }
            updates.push(decode_update_event_arguments(event, &state.payload_codec)?);
        }
        Ok(updates)
    }

    /// Wait for a deterministic predicate to become true or for its durable
    /// timeout to elapse.
    ///
    /// Prefer [`wait_condition!`] for inline predicates so changes to the Rust
    /// predicate tokens automatically change the recorded definition
    /// fingerprint. Direct callers must provide an equally stable identity in
    /// [`ConditionWaitOptions`].
    pub fn wait_condition<F>(
        &self,
        options: ConditionWaitOptions,
        predicate: F,
    ) -> ConditionWaitCall
    where
        F: Fn() -> Result<bool> + Send + 'static,
    {
        ConditionWaitCall {
            ctx: self.clone(),
            options,
            predicate: Box::new(predicate),
            occurrence_id: None,
            opened_wait: false,
            parallel_group_path: Vec::new(),
        }
    }

    /// Wait for server-backed durable time without blocking the worker executor.
    ///
    /// Polling this future emits one `start_timer` command and yields. The
    /// server records the deadline, so neither worker nor server restarts reset
    /// the wait. Replay resolves the future only from a `TimerScheduled` and
    /// `TimerFired` pair at the same position in the shared durable-command
    /// stream, with matching sequence, timer identity, and delay. Sub-second
    /// durations round up because protocol deadlines use whole seconds.
    ///
    /// ```no_run
    /// # use durable_workflow::{json, Client, Worker};
    /// # use std::time::Duration;
    /// # fn configure(client: Client) {
    /// let mut worker = Worker::new(client, "rust-workers");
    /// worker.register_workflow("delayed-greeting", |ctx, _input| async move {
    ///     ctx.sleep(Duration::from_secs(5)).await?;
    ///     Ok(json!({"status": "timer fired"}))
    /// });
    /// # }
    /// ```
    pub fn sleep(&self, duration: Duration) -> TimerCall {
        let delay_seconds = duration
            .as_secs()
            .checked_add(u64::from(duration.subsec_nanos() > 0));
        TimerCall {
            ctx: self.clone(),
            delay_seconds,
            scheduled: false,
            matched_pending: false,
            parallel_group_path: Vec::new(),
        }
    }

    /// Alias for [`WorkflowContext::sleep`] for timer-oriented workflow code.
    pub fn start_timer(&self, duration: Duration) -> TimerCall {
        self.sleep(duration)
    }

    /// Evaluate a non-deterministic callback once and durably record its typed value.
    ///
    /// On replay the callback is not invoked: the value is decoded from the
    /// sequence-matched `SideEffectRecorded` event using the workflow's payload
    /// codec. Use this for UUIDs, wall-clock snapshots, random values, and other
    /// small values that must remain fixed for the lifetime of a workflow run.
    pub fn side_effect<T, F>(&self, callback: F) -> Result<T>
    where
        T: Serialize + DeserializeOwned,
        F: FnOnce() -> T,
    {
        {
            let mut state = self
                .state
                .lock()
                .map_err(|_| Error::WorkflowStatePoisoned)?;
            if let Some(recorded) = state.recorded_commands.get(state.command_cursor).cloned() {
                return match recorded {
                    RecordedCommand::SideEffect { sequence, value } => {
                        state.command_cursor += 1;
                        value.deserialize().map_err(|error| {
                            Error::NonDeterministicReplay(ReplayFailure::new(
                                "side_effect_type_mismatch",
                                Some(sequence),
                                Some(std::any::type_name::<T>().to_string()),
                                Some(error.to_string()),
                                "recorded side-effect value is incompatible with the requested Rust type",
                            ))
                        })
                    }
                    other => Err(command_mismatch(&other, "side effect")),
                };
            }
        }

        let value = callback();
        let avro_value = AvroValue::from_serialize(&value)?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::WorkflowStatePoisoned)?;
        let result = encode_typed_envelope(&avro_value, &state.payload_codec)?;
        state.commands.push(json!({
            "type": "record_side_effect",
            "result": result,
        }));
        Ok(value)
    }

    /// Record or replay a lossless fixed Avro Value side effect.
    pub fn side_effect_avro_value<F>(&self, callback: F) -> Result<AvroValue>
    where
        F: FnOnce() -> AvroValue,
    {
        {
            let mut state = self
                .state
                .lock()
                .map_err(|_| Error::WorkflowStatePoisoned)?;
            if let Some(recorded) = state.recorded_commands.get(state.command_cursor).cloned() {
                return match recorded {
                    RecordedCommand::SideEffect { value, .. } => {
                        state.command_cursor += 1;
                        Ok(value)
                    }
                    other => Err(command_mismatch(&other, "side effect")),
                };
            }
        }

        let value = callback();
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::WorkflowStatePoisoned)?;
        let result = encode_typed_envelope(&value, &state.payload_codec)?;
        state.commands.push(json!({
            "type": "record_side_effect",
            "result": result,
        }));
        Ok(value)
    }

    /// Append output items at a deterministic workflow command boundary.
    ///
    /// Stable idempotency keys are derived from the server-provided durable
    /// workflow command identity, command ordinal, and item index. Replay
    /// consumes the recorded side effect and never emits another append.
    pub fn append_workflow_stream(
        &self,
        stream_name: impl Into<String>,
        items: &[WorkflowStreamAppendItem],
        max_pending_items: Option<u64>,
    ) -> Result<()> {
        if items.is_empty() {
            return Err(Error::Codec(
                "workflow_stream_items_empty: append requires at least one item".to_string(),
            ));
        }
        if max_pending_items == Some(0) {
            return Err(Error::Codec(
                "workflow_stream_pending_limit_invalid: max_pending_items must be positive"
                    .to_string(),
            ));
        }
        let stream_name = stream_name.into();
        if stream_name.is_empty() {
            return Err(Error::Codec(
                "workflow_stream_name_invalid: stream name must not be empty".to_string(),
            ));
        }

        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::WorkflowStatePoisoned)?;
        let command_ordinal = state.workflow_stream_command_counter;
        if let Some(recorded) = state.recorded_commands.get(state.command_cursor).cloned() {
            state.workflow_stream_command_counter += 1;
            return match recorded {
                RecordedCommand::SideEffect { .. } => {
                    state.command_cursor += 1;
                    Ok(())
                }
                other => Err(command_mismatch(&other, "workflow stream append")),
            };
        }

        let identity = Self::workflow_stream_command_identity(&state)?.to_string();
        state.workflow_stream_command_counter += 1;
        let wire_items = items
            .iter()
            .enumerate()
            .map(|(item_index, item)| {
                item.wire_value(Some(format!(
                    "dw-stream:{identity}:{command_ordinal}:{item_index}"
                )))
            })
            .collect::<Vec<_>>();
        let mut directive = json!({
            "operation": "append",
            "stream_name": stream_name,
            "command_identity": identity,
            "command_ordinal": command_ordinal,
            "items": wire_items,
        });
        if let Some(max_pending_items) = max_pending_items {
            directive["max_pending_items"] = json!(max_pending_items);
        }
        let result = encode_typed_envelope(&AvroValue::Null, &state.payload_codec)?;
        state.commands.push(json!({
            "type": "record_side_effect",
            "result": result,
            "workflow_stream": directive,
        }));
        Ok(())
    }

    /// Close a run-scoped output stream at a deterministic command boundary.
    pub fn close_workflow_stream(
        &self,
        stream_name: impl Into<String>,
        retention_seconds: Option<u64>,
    ) -> Result<()> {
        self.finish_workflow_stream(stream_name.into(), None, retention_seconds)
    }

    /// Mark a run-scoped output stream errored at a deterministic command boundary.
    pub fn error_workflow_stream(
        &self,
        stream_name: impl Into<String>,
        error_reason: impl Into<String>,
        retention_seconds: Option<u64>,
    ) -> Result<()> {
        let error_reason = error_reason.into();
        if error_reason.is_empty() {
            return Err(Error::Codec(
                "workflow_stream_error_invalid: error reason must not be empty".to_string(),
            ));
        }
        self.finish_workflow_stream(stream_name.into(), Some(error_reason), retention_seconds)
    }

    fn finish_workflow_stream(
        &self,
        stream_name: String,
        error_reason: Option<String>,
        retention_seconds: Option<u64>,
    ) -> Result<()> {
        if stream_name.is_empty() {
            return Err(Error::Codec(
                "workflow_stream_name_invalid: stream name must not be empty".to_string(),
            ));
        }
        if retention_seconds == Some(0) {
            return Err(Error::Codec(
                "workflow_stream_retention_invalid: retention_seconds must be positive".to_string(),
            ));
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::WorkflowStatePoisoned)?;
        let command_ordinal = state.workflow_stream_command_counter;
        if let Some(recorded) = state.recorded_commands.get(state.command_cursor).cloned() {
            state.workflow_stream_command_counter += 1;
            return match recorded {
                RecordedCommand::SideEffect { .. } => {
                    state.command_cursor += 1;
                    Ok(())
                }
                other => Err(command_mismatch(&other, "workflow stream close")),
            };
        }
        let identity = Self::workflow_stream_command_identity(&state)?.to_string();
        state.workflow_stream_command_counter += 1;
        let mut directive = json!({
            "operation": if error_reason.is_some() { "error" } else { "close" },
            "stream_name": stream_name,
            "command_identity": identity,
            "command_ordinal": command_ordinal,
        });
        if let Some(error_reason) = error_reason {
            directive["error_reason"] = json!(error_reason);
        }
        if let Some(retention_seconds) = retention_seconds {
            directive["retention_seconds"] = json!(retention_seconds);
        }
        let result = encode_typed_envelope(&AvroValue::Null, &state.payload_codec)?;
        state.commands.push(json!({
            "type": "record_side_effect",
            "result": result,
            "workflow_stream": directive,
        }));
        Ok(())
    }

    fn workflow_stream_command_identity(state: &WorkflowState) -> Result<&str> {
        let identity = state.workflow_command_identity.as_str();
        if identity.is_empty() {
            return Err(Error::MissingWorkflowCommandIdentity);
        }
        Ok(identity)
    }

    /// Validate, emit, or replay a typed workflow search-attribute update.
    ///
    /// The command is non-blocking within a workflow decision, but its
    /// `SearchAttributesUpserted` event occupies the same deterministic command
    /// stream as activities, timers, conditions, and other durable operations.
    pub fn upsert_search_attributes(&self, update: SearchAttributeUpdate) -> Result<()> {
        update.validate()?;
        let (attributes, attribute_types) = update.into_wire_parts();
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::WorkflowStatePoisoned)?;

        if let Some(recorded) = state.recorded_commands.get(state.command_cursor).cloned() {
            return match recorded {
                RecordedCommand::SearchAttributes {
                    sequence,
                    attributes: recorded_attributes,
                    attribute_types: recorded_attribute_types,
                } => {
                    if recorded_attributes != attributes {
                        return Err(Error::NonDeterministicReplay(ReplayFailure::new(
                            "search_attribute_value_mismatch",
                            Some(sequence),
                            Some(recorded_attributes.to_string()),
                            Some(attributes.to_string()),
                            "search-attribute values differ from the recorded durable command",
                        )));
                    }
                    if let RecordedSnapshotValue::Known(recorded_types) = recorded_attribute_types {
                        if recorded_types != attribute_types {
                            return Err(Error::NonDeterministicReplay(ReplayFailure::new(
                                "search_attribute_type_mismatch",
                                Some(sequence),
                                Some(json!(recorded_types).to_string()),
                                Some(json!(attribute_types).to_string()),
                                "search-attribute declared types differ from the recorded durable command",
                            )));
                        }
                    }
                    state.command_cursor += 1;
                    Ok(())
                }
                other => Err(command_mismatch(&other, "search-attribute update")),
            };
        }

        let mut command = serde_json::Map::from_iter([
            ("type".to_string(), json!("upsert_search_attributes")),
            ("attributes".to_string(), attributes),
        ]);
        if !attribute_types.is_empty() {
            command.insert("attribute_types".to_string(), json!(attribute_types));
        }
        state.commands.push(Value::Object(command));
        Ok(())
    }

    /// Record a UUIDv4 once and return the same UUID on every replay.
    pub fn uuid_v4(&self) -> Result<Uuid> {
        self.side_effect(Uuid::new_v4)
    }

    /// Select the newest supported version for a change, or replay the version
    /// already committed for that stable change ID.
    pub fn get_version(
        &self,
        change_id: impl Into<String>,
        min_supported: i32,
        max_supported: i32,
    ) -> Result<i32> {
        let change_id = change_id.into();
        if change_id.trim().is_empty() {
            return Err(Error::NonDeterministicReplay(ReplayFailure::new(
                "version_change_id_invalid",
                None,
                Some("non-empty change ID".to_string()),
                Some(change_id),
                "version markers require a stable non-empty change ID",
            )));
        }
        if min_supported > max_supported {
            return Err(Error::NonDeterministicReplay(ReplayFailure::new(
                "version_range_invalid",
                None,
                Some("min_supported <= max_supported".to_string()),
                Some(format!("{min_supported}..={max_supported}")),
                "version marker supported range is invalid",
            )));
        }

        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::WorkflowStatePoisoned)?;
        if let Some((version, sequence)) = state.version_markers.get(&change_id).copied() {
            ensure_version_supported(&change_id, version, min_supported, max_supported, sequence)?;
            return Ok(version);
        }

        if let Some(recorded) = state.recorded_commands.get(state.command_cursor).cloned() {
            return match recorded {
                RecordedCommand::VersionMarker {
                    sequence,
                    change_id: recorded_change_id,
                    version,
                    ..
                } => {
                    if recorded_change_id != change_id {
                        return Err(Error::NonDeterministicReplay(ReplayFailure::new(
                            "version_change_id_mismatch",
                            Some(sequence),
                            Some(recorded_change_id),
                            Some(change_id),
                            "recorded version marker change ID differs from current workflow code",
                        )));
                    }
                    ensure_version_supported(
                        &change_id,
                        version,
                        min_supported,
                        max_supported,
                        sequence,
                    )?;
                    state.command_cursor += 1;
                    state.version_markers.insert(change_id, (version, sequence));
                    Ok(version)
                }
                other => Err(command_mismatch(
                    &other,
                    format!("version marker:{change_id}"),
                )),
            };
        }

        let version = max_supported;
        state.commands.push(json!({
            "type": "record_version_marker",
            "change_id": change_id,
            "version": version,
            "min_supported": min_supported,
            "max_supported": max_supported,
        }));
        // Sequence numbers are assigned by the server. Zero identifies a marker
        // selected in this uncommitted decision batch for duplicate-call checks.
        state.version_markers.insert(change_id, (version, 0));
        Ok(version)
    }

    /// Record or replay the standard `-1` (legacy) / `1` (patched) marker.
    pub fn patched(&self, change_id: impl Into<String>) -> Result<bool> {
        Ok(self.get_version(change_id, -1, 1)? == 1)
    }

    /// Keep a patch marker in history after the legacy branch has been removed.
    pub fn deprecate_patch(&self, change_id: impl Into<String>) -> Result<()> {
        self.get_version(change_id, -1, 1).map(|_| ())
    }

    /// Merge non-indexed workflow memo metadata through durable history.
    ///
    /// Avro `null` deletes a key. The SDK encodes the complete patch in the
    /// public Avro payload envelope consumed by Server and Cloud runtimes.
    pub fn upsert_memo<T: Serialize>(&self, entries: T) -> Result<()> {
        let entries = canonical_memo_entries(AvroValue::from_serialize(&entries)?, true)?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::WorkflowStatePoisoned)?;

        if let Some(recorded) = state.recorded_commands.get(state.command_cursor).cloned() {
            return match recorded {
                RecordedCommand::Memo {
                    sequence,
                    entries: recorded_entries,
                } => {
                    if recorded_entries != entries {
                        return Err(Error::NonDeterministicReplay(ReplayFailure::new(
                            "memo_update_mismatch",
                            Some(sequence),
                            Some(format!("{recorded_entries:?}")),
                            Some(format!("{entries:?}")),
                            "recorded memo entries differ from the current workflow update",
                        )));
                    }
                    state.command_cursor += 1;
                    Ok(())
                }
                other => Err(command_mismatch(&other, "memo upsert")),
            };
        }

        let entries_envelope = encode_typed_envelope(&entries, DEFAULT_CODEC)?;
        state.commands.push(json!({
            "type": "upsert_memo",
            "entries": entries_envelope,
        }));
        Ok(())
    }

    /// Start a named durable child on an explicit queue and await its result.
    ///
    /// The command is recorded in the parent's sequence-ordered durable command
    /// stream. Replay keeps a scheduled child pending without emitting another
    /// start, or consumes its matching terminal `ChildRun*` outcome. Successful
    /// values preserve the history payload codec and include both sides of the
    /// durable relationship; failures are returned as
    /// [`Error::ChildWorkflowFailed`].
    ///
    /// ```no_run
    /// # use durable_workflow::{json, ChildWorkflowOptions, Client, ParentClosePolicy, Worker};
    /// # fn configure(client: Client) {
    /// let mut worker = Worker::new(client, "parent-workers");
    /// worker.register_workflow("order-parent", |ctx, _input| async move {
    ///     let child = ctx
    ///         .start_child_workflow(
    ///             "fulfil-order",
    ///             ChildWorkflowOptions::new("fulfilment-workers")
    ///                 .parent_close_policy(ParentClosePolicy::RequestCancel),
    ///             json!([{"order_id": "order-42"}]),
    ///         )
    ///         .await?;
    ///     Ok(child.result)
    /// });
    /// # }
    /// ```
    pub fn start_child_workflow<T: Serialize>(
        &self,
        workflow_type: impl Into<String>,
        options: ChildWorkflowOptions,
        args: T,
    ) -> ChildWorkflowCall {
        ChildWorkflowCall {
            ctx: self.clone(),
            workflow_type: workflow_type.into(),
            options,
            args: Some(AvroValue::from_serialize(&args)),
            scheduled: false,
            matched_pending: false,
            parallel_group_path: Vec::new(),
        }
    }

    pub async fn start_child_workflow_avro_value<T: Serialize>(
        &self,
        workflow_type: impl Into<String>,
        options: ChildWorkflowOptions,
        args: T,
    ) -> Result<ChildWorkflowAvroResult> {
        let mut call = self.start_child_workflow(workflow_type, options, args);
        std::future::poll_fn(|cx| Pin::new(&mut call).poll_avro_value(cx)).await
    }

    fn take_commands(&self) -> Result<Vec<Value>> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::WorkflowStatePoisoned)?;
        Ok(std::mem::take(&mut state.commands))
    }

    fn continue_as_new_command(&self, request: ContinueAsNewRequest) -> Result<Option<Value>> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::WorkflowStatePoisoned)?;

        if let Some(recorded) = state.recorded_commands.get(state.command_cursor).cloned() {
            return Err(command_mismatch(&recorded, "continue as new"));
        }
        if state.recorded_continue_as_new_sequence.is_some() {
            state.continue_as_new_consumed = true;
            return Ok(None);
        }

        let arguments = encode_typed_envelope(&request.arguments, &state.payload_codec)?;
        let mut command = serde_json::Map::from_iter([
            ("type".to_string(), json!("continue_as_new")),
            ("arguments".to_string(), arguments),
            ("queue".to_string(), json!(state.task_queue.clone())),
        ]);
        if let Some(workflow_type) = request.options.workflow_type {
            command.insert("workflow_type".to_string(), json!(workflow_type));
        }
        if let Some(task_queue) = request.options.task_queue {
            command.insert("queue".to_string(), json!(task_queue));
        }
        Ok(Some(Value::Object(command)))
    }

    fn matched_recorded_pending(&self) -> Result<bool> {
        let state = self
            .state
            .lock()
            .map_err(|_| Error::WorkflowStatePoisoned)?;
        Ok(state.matched_recorded_pending)
    }

    fn ensure_history_consumed(&self) -> Result<()> {
        let state = self
            .state
            .lock()
            .map_err(|_| Error::WorkflowStatePoisoned)?;
        if let Some(command) = state.recorded_commands.get(state.command_cursor) {
            return Err(Error::NonDeterministicReplay(ReplayFailure::new(
                "recorded_commands_unconsumed",
                Some(command.sequence()),
                Some(command.shape().to_string()),
                Some("workflow completion".to_string()),
                "workflow completed before consuming all recorded durable commands",
            )));
        }
        if let Some(sequence) = state
            .recorded_continue_as_new_sequence
            .filter(|_| !state.continue_as_new_consumed)
        {
            return Err(Error::NonDeterministicReplay(ReplayFailure::new(
                "recorded_continue_as_new_unconsumed",
                Some(sequence),
                Some("continue as new".to_string()),
                Some("workflow completion".to_string()),
                "workflow completed without consuming its recorded continue-as-new transition",
            )));
        }
        Ok(())
    }
}

fn contiguous_message_stream_count(
    pending: &[MessageStreamMessage],
    cursor: u64,
    max_items: usize,
) -> usize {
    pending
        .iter()
        .take(max_items)
        .enumerate()
        .take_while(|(offset, message)| {
            u64::try_from(*offset)
                .ok()
                .and_then(|offset| cursor.checked_add(offset + 1))
                == Some(message.position)
        })
        .count()
}

fn is_authored_command_open_event(event: &HistoryEvent) -> bool {
    matches!(
        event.event_type.as_str(),
        "ActivityScheduled"
            | "TimerScheduled"
            | "ChildWorkflowScheduled"
            | "SignalWaitOpened"
            | "ConditionWaitOpened"
            | "SearchAttributesUpserted"
            | "SideEffectRecorded"
            | "VersionMarkerRecorded"
            | "MemoUpserted"
            | "WorkflowContinuedAsNew"
    )
}

#[derive(Debug)]
struct WorkflowState {
    workflow_id: Option<String>,
    run_id: Option<String>,
    task_queue: String,
    payload_codec: String,
    history_events: Arc<Vec<HistoryEvent>>,
    history_budget: WorkflowHistoryBudget,
    cancel_requested: bool,
    resume_signal: Option<ResumeSignal>,
    recorded_commands: Vec<RecordedCommand>,
    selection_markers: Vec<SelectionMarker>,
    selection_marker_cursor: usize,
    cancelled_selection_members: Vec<SelectionCancellation>,
    recorded_continue_as_new_sequence: Option<u64>,
    continue_as_new_consumed: bool,
    command_cursor: usize,
    condition_wait_occurrence_counter: u64,
    matched_recorded_pending: bool,
    version_markers: HashMap<String, (i32, u64)>,
    workflow_command_identity: String,
    workflow_stream_command_counter: u64,
    commands: Vec<Value>,
    message_stream_messages: HashMap<String, Vec<MessageStreamMessage>>,
    message_stream_cursors: HashMap<String, u64>,
    message_stream_waits: HashMap<String, u64>,
}

impl WorkflowState {
    #[cfg(test)]
    fn new(
        history: Vec<HistoryEvent>,
        task_queue: String,
        payload_codec: String,
        resume_signal: Option<ResumeSignal>,
    ) -> Result<Self> {
        Self::new_with_identity(
            history,
            None,
            None,
            task_queue,
            payload_codec,
            resume_signal,
        )
    }

    fn new_with_identity(
        history: Vec<HistoryEvent>,
        workflow_id: Option<String>,
        run_id: Option<String>,
        task_queue: String,
        payload_codec: String,
        resume_signal: Option<ResumeSignal>,
    ) -> Result<Self> {
        let recorded_commands = recorded_commands(
            &history,
            &payload_codec,
            WorkflowIdentity {
                workflow_id: workflow_id.clone(),
                run_id: run_id.clone(),
            },
        )?;
        let selection_markers = recorded_selection_markers(&history)?;
        let cancelled_selection_members = recorded_selection_cancellations(&history)?;
        let recorded_continue_as_new = history
            .iter()
            .filter(|event| event.event_type == "WorkflowContinuedAsNew")
            .collect::<Vec<_>>();
        if recorded_continue_as_new.len() > 1 {
            return Err(invalid_recorded_history(
                "duplicate_continue_as_new_transition",
                recorded_continue_as_new
                    .last()
                    .and_then(|event| durable_event_sequence(event))
                    .unwrap_or(0),
                "one WorkflowContinuedAsNew event",
                &format!(
                    "{} WorkflowContinuedAsNew events",
                    recorded_continue_as_new.len()
                ),
                "workflow history records one continue-as-new transition more than once",
            ));
        }
        let recorded_continue_as_new_sequence = recorded_continue_as_new
            .first()
            .map(|event| {
                durable_event_sequence(event).ok_or_else(|| {
                    Error::NonDeterministicReplay(ReplayFailure::new(
                        "continue_as_new_sequence_missing",
                        None,
                        Some("recorded transition sequence".to_string()),
                        Some("missing sequence".to_string()),
                        "WorkflowContinuedAsNew history is missing its recorded sequence",
                    ))
                })
            })
            .transpose()?;
        let mut message_stream_cursors = HashMap::new();
        for event in &history {
            if !matches!(
                event.event_type.as_str(),
                "SignalReceived" | "SignalApplied"
            ) || event.payload.get("signal_name").and_then(Value::as_str)
                != Some(MESSAGE_STREAM_SIGNAL)
            {
                continue;
            }
            let arguments = decode_signal_event_arguments(event, &payload_codec)?;
            if arguments.len() != 1 {
                continue;
            }
            let envelope = arguments[0].clone().into_json()?;
            let Some(envelope) = envelope.as_object() else {
                continue;
            };
            if envelope.get("schema").and_then(Value::as_str) != Some(MESSAGE_STREAM_CURSOR_SCHEMA)
            {
                continue;
            }
            let Some(stream_name) = envelope.get("stream_name").and_then(Value::as_str) else {
                continue;
            };
            let Some(through_position) = envelope.get("through_position").and_then(Value::as_u64)
            else {
                continue;
            };
            let cursor = message_stream_cursors
                .entry(stream_name.to_string())
                .or_insert(0);
            *cursor = (*cursor).max(through_position);
        }
        let event_count = u64::try_from(history.len()).unwrap_or(u64::MAX);
        let cancel_requested = history.iter().any(|event| {
            matches!(
                event.event_type.as_str(),
                "WorkflowCancellationRequested" | "WorkflowCancelRequested"
            )
        });
        Ok(Self {
            workflow_command_identity: String::new(),
            workflow_stream_command_counter: 0,
            workflow_id,
            run_id,
            task_queue,
            payload_codec,
            history_events: Arc::new(history),
            history_budget: WorkflowHistoryBudget {
                event_count,
                ..WorkflowHistoryBudget::default()
            },
            cancel_requested,
            resume_signal,
            recorded_commands,
            selection_markers,
            selection_marker_cursor: 0,
            cancelled_selection_members,
            recorded_continue_as_new_sequence,
            continue_as_new_consumed: false,
            command_cursor: 0,
            condition_wait_occurrence_counter: 0,
            matched_recorded_pending: false,
            version_markers: HashMap::new(),
            commands: Vec::new(),
            message_stream_messages: HashMap::new(),
            message_stream_cursors,
            message_stream_waits: HashMap::new(),
        })
    }
}

enum MessageStreamDelivery {
    Message(MessageStreamMessage),
    Cursor {
        stream_name: String,
        through_position: u64,
    },
}

fn decode_message_stream_delivery(arguments: Vec<Value>) -> Result<Option<MessageStreamDelivery>> {
    if arguments.len() != 1 {
        return Ok(None);
    }
    let envelope = arguments
        .into_iter()
        .next()
        .expect("one argument was checked");
    let Some(envelope) = envelope.as_object() else {
        return Ok(None);
    };
    let Some(stream_name) = envelope.get("stream_name").and_then(Value::as_str) else {
        return Ok(None);
    };
    if envelope.get("schema").and_then(Value::as_str) == Some(MESSAGE_STREAM_CURSOR_SCHEMA) {
        let Some(through_position) = envelope.get("through_position").and_then(Value::as_u64)
        else {
            return Ok(None);
        };
        return Ok(Some(MessageStreamDelivery::Cursor {
            stream_name: stream_name.to_string(),
            through_position,
        }));
    }
    if envelope.get("schema").and_then(Value::as_str) != Some(MESSAGE_STREAM_SCHEMA) {
        return Ok(None);
    }
    let Some(message_id) = envelope.get("message_id").and_then(Value::as_str) else {
        return Ok(None);
    };
    let Some(position) = envelope
        .get("position")
        .and_then(Value::as_u64)
        .filter(|value| *value > 0)
    else {
        return Ok(None);
    };
    let Some(payload_envelope) = envelope.get("payload_envelope") else {
        return Ok(None);
    };
    let Ok(payload_envelope) = serde_json::from_value::<PayloadEnvelope>(payload_envelope.clone())
    else {
        return Ok(None);
    };
    let decoded = decode_avro_value(&payload_envelope)?;
    let AvroValue::Array(values) = decoded else {
        return Ok(None);
    };
    Ok(Some(MessageStreamDelivery::Message(MessageStreamMessage {
        stream_name: stream_name.to_string(),
        message_id: message_id.to_string(),
        position,
        arguments: values,
    })))
}

#[derive(Clone, Debug)]
enum RecordedCommand {
    Activity {
        sequence: u64,
        activity_type: Option<String>,
        options: Option<RecordedActivityOptions>,
        outcome: Option<ActivityOutcome>,
        parallel_group_path: Option<Vec<ParallelGroupMetadata>>,
    },
    Timer {
        sequence: u64,
        delay_seconds: u64,
        fired: bool,
        parallel_group_path: Option<Vec<ParallelGroupMetadata>>,
    },
    ChildWorkflow {
        sequence: u64,
        workflow_type: Option<String>,
        outcome: Option<ChildWorkflowOutcome>,
        parallel_group_path: Option<Vec<ParallelGroupMetadata>>,
    },
    SignalWait {
        sequence: u64,
        signal_name: String,
        value: Option<Vec<AvroValue>>,
        parallel_group_path: Option<Vec<ParallelGroupMetadata>>,
    },
    ConditionWait {
        sequence: u64,
        occurrence_id: String,
        condition_key: Option<String>,
        predicate_identity: String,
        timeout_seconds: Option<u64>,
        result: Option<ConditionWaitResult>,
        parallel_group_path: Option<Vec<ParallelGroupMetadata>>,
    },
    SearchAttributes {
        sequence: u64,
        attributes: Value,
        attribute_types: RecordedSnapshotValue<BTreeMap<String, String>>,
    },
    SideEffect {
        sequence: u64,
        value: AvroValue,
    },
    VersionMarker {
        sequence: u64,
        change_id: String,
        version: i32,
    },
    Memo {
        sequence: u64,
        entries: AvroValue,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct SelectionMarker {
    selection_group_id: String,
    selection_group_base_sequence: u64,
    selection_group_size: usize,
    member_key: SelectionKey,
    member_index: usize,
    member_base_sequence: u64,
    member_size: usize,
    operation_kind: String,
    operation_identity: String,
    outcome: String,
    resolution_event_id: String,
    resolution_event_type: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct SelectionCancellation {
    selection_group_id: String,
    member_key: SelectionKey,
    member_index: usize,
    member_base_sequence: u64,
    member_size: usize,
    operation_kind: String,
    operation_identity: String,
}

fn recorded_selection_markers(events: &[HistoryEvent]) -> Result<Vec<SelectionMarker>> {
    let mut markers: Vec<SelectionMarker> = Vec::new();
    for event in events
        .iter()
        .filter(|event| event.event_type == "SelectionResolved")
    {
        let payload = &event.payload;
        let base_sequence = required_selection_u64(payload, "selection_group_base_sequence")?;
        let group_size = required_selection_usize(payload, "selection_group_size")?;
        let member_base_sequence = required_selection_u64(payload, "member_base_sequence")?;
        let member_size = required_selection_usize(payload, "member_size")?;
        let member_index = required_selection_usize_allow_zero(payload, "member_index")?;
        let group_id = payload_string(payload, "selection_group_id").ok_or_else(|| {
            invalid_recorded_history(
                "selection_marker_invalid",
                base_sequence,
                "non-empty selection_group_id",
                &payload.to_string(),
                "selection winner history is missing its durable group identity",
            )
        })?;
        let expected_group_id = format!("select-calls:{base_sequence}:{group_size}");
        if group_id != expected_group_id {
            return Err(invalid_recorded_history(
                "selection_marker_invalid",
                base_sequence,
                &expected_group_id,
                &group_id,
                "selection winner history contains an incompatible group identity",
            ));
        }
        let group_end = base_sequence
            .checked_add(u64::try_from(group_size).unwrap_or(u64::MAX))
            .unwrap_or(u64::MAX);
        let member_end = member_base_sequence
            .checked_add(u64::try_from(member_size).unwrap_or(u64::MAX))
            .unwrap_or(u64::MAX);
        if member_index >= group_size
            || member_base_sequence < base_sequence
            || member_end > group_end
        {
            return Err(invalid_recorded_history(
                "selection_marker_invalid",
                base_sequence,
                "winner member within selection group bounds",
                &payload.to_string(),
                "selection winner history contains an invalid member range",
            ));
        }
        let operation_kind = payload_string(payload, "operation_kind").ok_or_else(|| {
            invalid_recorded_history(
                "selection_marker_invalid",
                base_sequence,
                "selection operation kind",
                &payload.to_string(),
                "selection winner history is missing its operation kind",
            )
        })?;
        if !matches!(
            operation_kind.as_str(),
            "activity" | "child" | "timer" | "signal" | "condition" | "group"
        ) {
            return Err(invalid_recorded_history(
                "selection_marker_invalid",
                base_sequence,
                "activity, child, timer, signal, condition, or group",
                &operation_kind,
                "selection winner history contains an unsupported operation kind",
            ));
        }
        let operation_identity =
            payload_string(payload, "operation_identity").ok_or_else(|| {
                invalid_recorded_history(
                    "selection_marker_invalid",
                    base_sequence,
                    "non-empty operation identity",
                    &payload.to_string(),
                    "selection winner history is missing its durable operation identity",
                )
            })?;
        let outcome = payload_string(payload, "outcome").ok_or_else(|| {
            invalid_recorded_history(
                "selection_marker_invalid",
                base_sequence,
                "completed or failed selection outcome",
                &payload.to_string(),
                "selection winner history is missing its outcome",
            )
        })?;
        if !matches!(outcome.as_str(), "completed" | "failed") {
            return Err(invalid_recorded_history(
                "selection_marker_invalid",
                base_sequence,
                "completed or failed selection outcome",
                &outcome,
                "selection winner history contains an unsupported outcome",
            ));
        }
        let marker = SelectionMarker {
            selection_group_id: group_id,
            selection_group_base_sequence: base_sequence,
            selection_group_size: group_size,
            member_key: selection_key_from_value(payload.get("member_key"), base_sequence)?,
            member_index,
            member_base_sequence,
            member_size,
            operation_kind,
            operation_identity,
            outcome,
            resolution_event_id: payload_string(payload, "resolution_event_id").ok_or_else(
                || {
                    invalid_recorded_history(
                        "selection_marker_invalid",
                        base_sequence,
                        "durable resolution_event_id",
                        &payload.to_string(),
                        "selection winner history is missing its terminal event identity",
                    )
                },
            )?,
            resolution_event_type: payload_string(payload, "resolution_event_type").ok_or_else(
                || {
                    invalid_recorded_history(
                        "selection_marker_invalid",
                        base_sequence,
                        "durable resolution_event_type",
                        &payload.to_string(),
                        "selection winner history is missing its terminal event type",
                    )
                },
            )?,
        };
        if let Some(existing) = markers
            .iter()
            .find(|existing| existing.selection_group_id == marker.selection_group_id)
        {
            if existing != &marker {
                return Err(invalid_recorded_history(
                    "selection_marker_conflict",
                    base_sequence,
                    &format!("one winner for {}", marker.selection_group_id),
                    &payload.to_string(),
                    "selection history records conflicting winners for one durable group",
                ));
            }
            continue;
        }
        markers.push(marker);
    }
    Ok(markers)
}

fn recorded_selection_cancellations(events: &[HistoryEvent]) -> Result<Vec<SelectionCancellation>> {
    let mut cancelled: Vec<SelectionCancellation> = Vec::new();
    for event in events
        .iter()
        .filter(|event| event.event_type == "SelectionOperationCancelled")
    {
        let group_id = payload_string(&event.payload, "selection_group_id").ok_or_else(|| {
            invalid_recorded_history(
                "selection_cancellation_invalid",
                0,
                "non-empty selection_group_id",
                &event.payload.to_string(),
                "selection cancellation history is missing its group identity",
            )
        })?;
        let member_base_sequence = required_selection_u64(&event.payload, "member_base_sequence")?;
        let marker = SelectionCancellation {
            selection_group_id: group_id,
            member_key: selection_key_from_value(
                event.payload.get("member_key"),
                member_base_sequence,
            )?,
            member_index: required_selection_usize_allow_zero(&event.payload, "member_index")?,
            member_base_sequence,
            member_size: required_selection_usize(&event.payload, "member_size")?,
            operation_kind: payload_string(&event.payload, "operation_kind").ok_or_else(|| {
                invalid_recorded_history(
                    "selection_cancellation_invalid",
                    member_base_sequence,
                    "selection operation kind",
                    &event.payload.to_string(),
                    "selection cancellation is missing its operation kind",
                )
            })?,
            operation_identity: payload_string(&event.payload, "operation_identity").ok_or_else(
                || {
                    invalid_recorded_history(
                        "selection_cancellation_invalid",
                        member_base_sequence,
                        "selection operation identity",
                        &event.payload.to_string(),
                        "selection cancellation is missing its operation identity",
                    )
                },
            )?,
        };
        if let Some(existing) = cancelled.iter().find(|recorded| {
            recorded.selection_group_id == marker.selection_group_id
                && recorded.member_base_sequence == marker.member_base_sequence
        }) {
            if existing != &marker {
                return Err(invalid_recorded_history(
                    "selection_cancellation_conflict",
                    member_base_sequence,
                    "one stable SelectionOperationCancelled marker",
                    &event.payload.to_string(),
                    "selection cancellation history contains conflicting member metadata",
                ));
            }
        } else {
            cancelled.push(marker);
        }
    }
    Ok(cancelled)
}

fn required_selection_u64(payload: &Value, field: &str) -> Result<u64> {
    payload
        .get(field)
        .and_then(value_as_u64)
        .filter(|value| *value > 0)
        .ok_or_else(|| {
            invalid_recorded_history(
                "selection_marker_invalid",
                0,
                &format!("positive integer {field}"),
                &payload.to_string(),
                "selection history contains invalid durable identity metadata",
            )
        })
}

fn required_selection_usize(payload: &Value, field: &str) -> Result<usize> {
    required_selection_usize_allow_zero(payload, field).and_then(|value| {
        if value > 0 {
            Ok(value)
        } else {
            Err(invalid_recorded_history(
                "selection_marker_invalid",
                0,
                &format!("positive integer {field}"),
                &payload.to_string(),
                "selection history contains invalid durable identity metadata",
            ))
        }
    })
}

fn required_selection_usize_allow_zero(payload: &Value, field: &str) -> Result<usize> {
    payload
        .get(field)
        .and_then(value_as_u64)
        .and_then(|value| usize::try_from(value).ok())
        .ok_or_else(|| {
            invalid_recorded_history(
                "selection_marker_invalid",
                0,
                &format!("non-negative integer {field}"),
                &payload.to_string(),
                "selection history contains invalid durable identity metadata",
            )
        })
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct RecordedActivityOptions {
    task_queue: RecordedSnapshotValue<Option<String>>,
    execution_mode: RecordedSnapshotValue<Option<String>>,
    retry_policy: ActivityRetrySnapshot,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
enum RecordedSnapshotValue<T> {
    /// Older history did not persist this field, so it cannot constrain replay.
    Unknown,
    Known(T),
}

impl<T: PartialEq> RecordedSnapshotValue<T> {
    fn matches_current(&self, current: &Self) -> bool {
        match self {
            Self::Unknown => true,
            Self::Known(recorded) => matches!(current, Self::Known(value) if value == recorded),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct ActivityRetrySnapshot {
    snapshot_version: RecordedSnapshotValue<Option<u64>>,
    max_attempts: RecordedSnapshotValue<Option<u64>>,
    backoff_seconds: RecordedSnapshotValue<Vec<u64>>,
    start_to_close_timeout: RecordedSnapshotValue<Option<u64>>,
    schedule_to_start_timeout: RecordedSnapshotValue<Option<u64>>,
    schedule_to_close_timeout: RecordedSnapshotValue<Option<u64>>,
    heartbeat_timeout: RecordedSnapshotValue<Option<u64>>,
    non_retryable_error_types: RecordedSnapshotValue<Vec<String>>,
}

impl ActivityRetrySnapshot {
    fn matches_current(&self, current: &Self) -> bool {
        self.snapshot_version
            .matches_current(&current.snapshot_version)
            && self.max_attempts.matches_current(&current.max_attempts)
            && self
                .backoff_seconds
                .matches_current(&current.backoff_seconds)
            && self
                .start_to_close_timeout
                .matches_current(&current.start_to_close_timeout)
            && self
                .schedule_to_start_timeout
                .matches_current(&current.schedule_to_start_timeout)
            && self
                .schedule_to_close_timeout
                .matches_current(&current.schedule_to_close_timeout)
            && self
                .heartbeat_timeout
                .matches_current(&current.heartbeat_timeout)
            && self
                .non_retryable_error_types
                .matches_current(&current.non_retryable_error_types)
    }
}

fn recorded_optional_u64(
    object: Option<&serde_json::Map<String, Value>>,
    field: &str,
) -> RecordedSnapshotValue<Option<u64>> {
    match object.and_then(|object| object.get(field)) {
        None => RecordedSnapshotValue::Unknown,
        Some(Value::Null) => RecordedSnapshotValue::Known(None),
        Some(value) => RecordedSnapshotValue::Known(value_as_u64(value)),
    }
}

fn recorded_optional_string(
    object: &serde_json::Map<String, Value>,
    field: &str,
) -> RecordedSnapshotValue<Option<String>> {
    match object.get(field) {
        None => RecordedSnapshotValue::Unknown,
        Some(Value::Null) => RecordedSnapshotValue::Known(None),
        Some(value) => RecordedSnapshotValue::Known(value.as_str().map(str::to_string)),
    }
}

fn recorded_activity_retry_snapshot(policy: Option<&Value>) -> ActivityRetrySnapshot {
    let policy = policy.and_then(Value::as_object);
    let backoff_seconds = policy
        .and_then(|policy| policy.get("backoff_seconds"))
        .and_then(Value::as_array)
        .map(|intervals| intervals.iter().filter_map(value_as_u64).collect())
        .map_or(RecordedSnapshotValue::Unknown, RecordedSnapshotValue::Known);
    let mut non_retryable_error_types = Vec::new();
    for error_type in policy
        .and_then(|policy| policy.get("non_retryable_error_types"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::trim)
        .filter(|error_type| !error_type.is_empty())
    {
        if !non_retryable_error_types
            .iter()
            .any(|recorded| recorded == error_type)
        {
            non_retryable_error_types.push(error_type.to_string());
        }
    }

    ActivityRetrySnapshot {
        snapshot_version: recorded_optional_u64(policy, "snapshot_version"),
        max_attempts: recorded_optional_u64(policy, "max_attempts"),
        backoff_seconds,
        start_to_close_timeout: recorded_optional_u64(policy, "start_to_close_timeout"),
        schedule_to_start_timeout: recorded_optional_u64(policy, "schedule_to_start_timeout"),
        schedule_to_close_timeout: recorded_optional_u64(policy, "schedule_to_close_timeout"),
        heartbeat_timeout: recorded_optional_u64(policy, "heartbeat_timeout"),
        non_retryable_error_types: if policy
            .is_some_and(|policy| policy.contains_key("non_retryable_error_types"))
        {
            RecordedSnapshotValue::Known(non_retryable_error_types)
        } else {
            RecordedSnapshotValue::Unknown
        },
    }
}

fn current_activity_retry_snapshot(options: &ValidatedActivityOptions) -> ActivityRetrySnapshot {
    let policy = options.retry_policy.as_ref();
    let max_attempts = match policy.and_then(|policy| policy.get("max_attempts")) {
        Some(Value::Null) => None,
        Some(value) => value_as_u64(value),
        None => Some(1),
    };
    let backoff_seconds = policy
        .and_then(|policy| policy.get("backoff_seconds"))
        .and_then(Value::as_array)
        .map(|intervals| intervals.iter().filter_map(value_as_u64).collect())
        .unwrap_or_default();
    let non_retryable_error_types = policy
        .and_then(|policy| policy.get("non_retryable_error_types"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_string)
        .collect();

    ActivityRetrySnapshot {
        snapshot_version: RecordedSnapshotValue::Known(Some(1)),
        max_attempts: RecordedSnapshotValue::Known(max_attempts),
        backoff_seconds: RecordedSnapshotValue::Known(backoff_seconds),
        start_to_close_timeout: RecordedSnapshotValue::Known(options.start_to_close_timeout),
        schedule_to_start_timeout: RecordedSnapshotValue::Known(options.schedule_to_start_timeout),
        schedule_to_close_timeout: RecordedSnapshotValue::Known(options.schedule_to_close_timeout),
        heartbeat_timeout: RecordedSnapshotValue::Known(options.heartbeat_timeout),
        non_retryable_error_types: RecordedSnapshotValue::Known(non_retryable_error_types),
    }
}

fn activity_options_description(options: &RecordedActivityOptions) -> String {
    serde_json::to_string(options).unwrap_or_else(|_| format!("{options:?}"))
}

impl RecordedCommand {
    fn sequence(&self) -> u64 {
        match self {
            Self::Activity { sequence, .. }
            | Self::Timer { sequence, .. }
            | Self::ChildWorkflow { sequence, .. }
            | Self::SignalWait { sequence, .. }
            | Self::ConditionWait { sequence, .. }
            | Self::SearchAttributes { sequence, .. }
            | Self::SideEffect { sequence, .. }
            | Self::VersionMarker { sequence, .. }
            | Self::Memo { sequence, .. } => *sequence,
        }
    }

    fn shape(&self) -> &'static str {
        match self {
            Self::Activity { .. } => "activity",
            Self::Timer { .. } => "timer",
            Self::ChildWorkflow { .. } => "child workflow",
            Self::SignalWait { .. } => "signal wait",
            Self::ConditionWait { .. } => "condition wait",
            Self::SearchAttributes { .. } => "search-attribute update",
            Self::SideEffect { .. } => "side effect",
            Self::VersionMarker { .. } => "version marker",
            Self::Memo { .. } => "memo upsert",
        }
    }
}

fn ensure_version_supported(
    change_id: &str,
    version: i32,
    min_supported: i32,
    max_supported: i32,
    sequence: u64,
) -> Result<()> {
    if (min_supported..=max_supported).contains(&version) {
        return Ok(());
    }
    Err(Error::NonDeterministicReplay(ReplayFailure::new(
        "version_marker_incompatible_range",
        (sequence != 0).then_some(sequence),
        Some(format!("{min_supported}..={max_supported}")),
        Some(format!("{change_id}:{version}")),
        "recorded workflow version is outside the range supported by current code",
    )))
}

#[derive(Clone, Debug)]
struct ResumeSignal {
    signal_name: String,
    arguments: Vec<AvroValue>,
}

const MAX_PARALLEL_OPERATIONS: usize = 1000;

fn parallel_group_prefix(kind: &str) -> &'static str {
    match kind {
        "activity" => "parallel-activities",
        "child" => "parallel-children",
        "timer" => "parallel-timers",
        _ => "parallel-calls",
    }
}

fn parallel_group_entry(
    base_sequence: u64,
    size: usize,
    index: usize,
    kind: &str,
) -> ParallelGroupMetadata {
    ParallelGroupMetadata {
        parallel_group_id: format!("{}:{base_sequence}:{size}", parallel_group_prefix(kind)),
        parallel_group_kind: kind.to_string(),
        parallel_group_base_sequence: base_sequence,
        parallel_group_size: size,
        parallel_group_index: index,
        parallel_group_mode: None,
        selection_member_key: None,
        selection_member_index: None,
        selection_member_base_sequence: None,
        selection_member_size: None,
        selection_member_kind: None,
    }
}

struct SelectionMemberMetadata {
    key: SelectionKey,
    index: usize,
    base_sequence: u64,
    size: usize,
    kind: String,
}

fn selection_group_entry(
    base_sequence: u64,
    size: usize,
    index: usize,
    kind: &str,
    member: &SelectionMemberMetadata,
) -> ParallelGroupMetadata {
    ParallelGroupMetadata {
        parallel_group_id: format!("select-calls:{base_sequence}:{size}"),
        parallel_group_kind: kind.to_string(),
        parallel_group_base_sequence: base_sequence,
        parallel_group_size: size,
        parallel_group_index: index,
        parallel_group_mode: Some("select".to_string()),
        selection_member_key: Some(member.key.clone()),
        selection_member_index: Some(member.index),
        selection_member_base_sequence: Some(member.base_sequence),
        selection_member_size: Some(member.size),
        selection_member_kind: Some(member.kind.clone()),
    }
}

fn apply_parallel_group_path(
    command: &mut serde_json::Map<String, Value>,
    path: &[ParallelGroupMetadata],
) {
    let Some(inner) = path.last() else {
        return;
    };
    command.insert(
        "parallel_group_id".to_string(),
        json!(inner.parallel_group_id),
    );
    command.insert(
        "parallel_group_kind".to_string(),
        json!(inner.parallel_group_kind),
    );
    command.insert(
        "parallel_group_base_sequence".to_string(),
        json!(inner.parallel_group_base_sequence),
    );
    command.insert(
        "parallel_group_size".to_string(),
        json!(inner.parallel_group_size),
    );
    command.insert(
        "parallel_group_index".to_string(),
        json!(inner.parallel_group_index),
    );
    if let Some(mode) = &inner.parallel_group_mode {
        command.insert("parallel_group_mode".to_string(), json!(mode));
    }
    if let Some(key) = &inner.selection_member_key {
        command.insert("selection_member_key".to_string(), json!(key));
    }
    if let Some(index) = inner.selection_member_index {
        command.insert("selection_member_index".to_string(), json!(index));
    }
    if let Some(base_sequence) = inner.selection_member_base_sequence {
        command.insert(
            "selection_member_base_sequence".to_string(),
            json!(base_sequence),
        );
    }
    if let Some(size) = inner.selection_member_size {
        command.insert("selection_member_size".to_string(), json!(size));
    }
    if let Some(kind) = &inner.selection_member_kind {
        command.insert("selection_member_kind".to_string(), json!(kind));
    }
    command.insert("parallel_group_path".to_string(), json!(path));
}

fn ensure_parallel_path_matches(
    sequence: u64,
    recorded: Option<&[ParallelGroupMetadata]>,
    expected: &[ParallelGroupMetadata],
) -> Result<()> {
    match (recorded, expected.is_empty()) {
        (None, true) => Ok(()),
        (Some(recorded), false) if recorded == expected => Ok(()),
        (None, false) => Err(invalid_recorded_history(
            "parallel_group_metadata_missing",
            sequence,
            &serde_json::to_string(expected).unwrap_or_default(),
            "<missing>",
            "recorded parallel member is missing its durable group path",
        )),
        (Some(recorded), true) => Err(invalid_recorded_history(
            "parallel_group_shape_mismatch",
            sequence,
            "sequential command",
            &serde_json::to_string(recorded).unwrap_or_default(),
            "recorded command belonged to a parallel group but current code schedules it sequentially",
        )),
        (Some(recorded), false) => Err(invalid_recorded_history(
            "parallel_group_shape_mismatch",
            sequence,
            &serde_json::to_string(recorded).unwrap_or_default(),
            &serde_json::to_string(expected).unwrap_or_default(),
            "recorded parallel-group identity or path changed during replay",
        )),
    }
}

#[derive(Clone, Debug)]
enum ParallelShape {
    Leaf,
    Group(Vec<ParallelShape>),
}

struct ParallelDescriptor {
    operation: ParallelOperation,
    offset: usize,
    member_path: Vec<usize>,
    group_path: Vec<ParallelGroupMetadata>,
}

fn parallel_leaf_count(operations: &[ParallelOperation]) -> usize {
    operations
        .iter()
        .map(|operation| match operation {
            ParallelOperation::Group(children) => parallel_leaf_count(children),
            _ => 1,
        })
        .sum()
}

fn parallel_operation_kind(operation: &ParallelOperation) -> Option<&'static str> {
    match operation {
        ParallelOperation::Activity { .. } => Some("activity"),
        ParallelOperation::ChildWorkflow { .. } => Some("child"),
        ParallelOperation::Timer(_) => Some("timer"),
        ParallelOperation::Signal(_) => Some("signal"),
        ParallelOperation::Condition { .. } => Some("condition"),
        ParallelOperation::Group(children) => parallel_group_kind(children),
    }
}

fn parallel_group_kind(operations: &[ParallelOperation]) -> Option<&'static str> {
    let mut kind = None;
    for operation in operations {
        let Some(operation_kind) = parallel_operation_kind(operation) else {
            continue;
        };
        match kind {
            None => kind = Some(operation_kind),
            Some(current) if current == operation_kind => {}
            Some(_) => return Some("mixed"),
        }
    }
    kind
}

fn validate_parallel_operations(
    operations: &[ParallelOperation],
    member_path: &mut Vec<usize>,
    root: bool,
) -> Result<()> {
    let leaves = parallel_leaf_count(operations);
    if leaves > MAX_PARALLEL_OPERATIONS {
        return Err(Error::InvalidParallelGroup(ParallelGroupError {
            reason: "fan_out_limit_exceeded",
            member_path: member_path.clone(),
            message: format!(
                "group contains {leaves} durable leaves; the limit is {MAX_PARALLEL_OPERATIONS}"
            ),
        }));
    }
    if !root && operations.is_empty() {
        return Err(Error::InvalidParallelGroup(ParallelGroupError {
            reason: "nested_group_empty",
            member_path: member_path.clone(),
            message: "a nested group must contain at least one durable leaf".to_string(),
        }));
    }

    for (index, operation) in operations.iter().enumerate() {
        member_path.push(index);
        match operation {
            ParallelOperation::Activity {
                options, arguments, ..
            } => {
                options
                    .validate()
                    .map_err(|error| Error::InvalidActivityOptions(error))?;
                if let Err(error) = arguments {
                    return Err(Error::InvalidParallelGroup(ParallelGroupError {
                        reason: "arguments_invalid",
                        member_path: member_path.clone(),
                        message: error.to_string(),
                    }));
                }
            }
            ParallelOperation::ChildWorkflow {
                options, arguments, ..
            } => {
                validate_parallel_child_options(options)?;
                if let Err(error) = arguments {
                    return Err(Error::InvalidParallelGroup(ParallelGroupError {
                        reason: "arguments_invalid",
                        member_path: member_path.clone(),
                        message: error.to_string(),
                    }));
                }
            }
            ParallelOperation::Timer(duration)
                if duration.as_secs() == u64::MAX && duration.subsec_nanos() > 0 =>
            {
                return Err(Error::TimerDurationOverflow);
            }
            ParallelOperation::Timer(_) => {}
            ParallelOperation::Signal(signal_name) => {
                validate_user_signal_name(signal_name)?;
                if signal_name.trim().is_empty() {
                    return Err(Error::InvalidParallelGroup(ParallelGroupError {
                        reason: "signal_name_empty",
                        member_path: member_path.clone(),
                        message: "signal wait name must not be empty".to_string(),
                    }));
                }
            }
            ParallelOperation::Condition { options, .. } => {
                options.validate()?;
            }
            ParallelOperation::Group(children) => {
                validate_parallel_operations(children, member_path, false)?;
            }
        }
        member_path.pop();
    }
    Ok(())
}

fn validate_parallel_child_options(options: &ChildWorkflowOptions) -> Result<()> {
    if options.task_queue.trim().is_empty() {
        return Err(Error::InvalidChildWorkflowOptions(
            "task_queue must not be empty".to_string(),
        ));
    }
    for (name, value) in [
        (
            "execution_timeout_seconds",
            options.execution_timeout_seconds,
        ),
        ("run_timeout_seconds", options.run_timeout_seconds),
    ] {
        if value == Some(0) {
            return Err(Error::InvalidChildWorkflowOptions(format!(
                "{name} must be at least 1"
            )));
        }
    }
    if options
        .retry_policy
        .as_ref()
        .is_some_and(|policy| policy.max_attempts == Some(0))
    {
        return Err(Error::InvalidChildWorkflowOptions(
            "retry_policy.max_attempts must be at least 1".to_string(),
        ));
    }
    Ok(())
}

fn parallel_shape(operations: &[ParallelOperation]) -> ParallelShape {
    ParallelShape::Group(
        operations
            .iter()
            .map(|operation| match operation {
                ParallelOperation::Group(children) => parallel_shape(children),
                _ => ParallelShape::Leaf,
            })
            .collect(),
    )
}

fn parallel_descriptors(
    operations: Vec<ParallelOperation>,
    base_sequence: u64,
) -> Result<Vec<ParallelDescriptor>> {
    let size = parallel_leaf_count(&operations);
    let kind = parallel_group_kind(&operations).unwrap_or("activity");
    let mut descriptors = Vec::with_capacity(size);
    let mut cursor = 0;

    for (index, operation) in operations.into_iter().enumerate() {
        match operation {
            ParallelOperation::Group(children) => {
                let child_base = base_sequence
                    .checked_add(u64::try_from(cursor).unwrap_or(u64::MAX))
                    .ok_or(Error::TimerDurationOverflow)?;
                for mut descriptor in parallel_descriptors(children, child_base)? {
                    let outer_index = cursor + descriptor.offset;
                    descriptor.group_path.insert(
                        0,
                        parallel_group_entry(base_sequence, size, outer_index, kind),
                    );
                    descriptor.member_path.insert(0, index);
                    descriptor.offset = outer_index;
                    descriptors.push(descriptor);
                }
                cursor = descriptors.len();
            }
            operation => {
                descriptors.push(ParallelDescriptor {
                    operation,
                    offset: cursor,
                    member_path: vec![index],
                    group_path: vec![parallel_group_entry(base_sequence, size, cursor, kind)],
                });
                cursor += 1;
            }
        }
    }
    Ok(descriptors)
}

enum ParallelLeafCall {
    Activity(ActivityCall),
    ChildWorkflow(ChildWorkflowCall),
    Timer(TimerCall),
    Signal(SignalCall),
    Condition(ConditionWaitCall),
}

fn parallel_leaf_call(
    ctx: &WorkflowContext,
    operation: ParallelOperation,
    parallel_group_path: Vec<ParallelGroupMetadata>,
) -> ParallelLeafCall {
    match operation {
        ParallelOperation::Activity {
            activity_type,
            options,
            arguments,
        } => ParallelLeafCall::Activity(ActivityCall {
            ctx: ctx.clone(),
            activity_type,
            options,
            args: Some(arguments),
            scheduled: false,
            parallel_group_path,
        }),
        ParallelOperation::ChildWorkflow {
            workflow_type,
            options,
            arguments,
        } => ParallelLeafCall::ChildWorkflow(ChildWorkflowCall {
            ctx: ctx.clone(),
            workflow_type,
            options,
            args: Some(arguments),
            scheduled: false,
            matched_pending: false,
            parallel_group_path,
        }),
        ParallelOperation::Timer(duration) => {
            let delay_seconds = duration
                .as_secs()
                .checked_add(u64::from(duration.subsec_nanos() > 0));
            ParallelLeafCall::Timer(TimerCall {
                ctx: ctx.clone(),
                delay_seconds,
                scheduled: false,
                matched_pending: false,
                parallel_group_path,
            })
        }
        ParallelOperation::Signal(signal_name) => ParallelLeafCall::Signal(SignalCall {
            ctx: ctx.clone(),
            signal_name,
            runtime_reserved_allowed: false,
            opened_wait: false,
            matched_pending: false,
            parallel_group_path,
        }),
        ParallelOperation::Condition { options, predicate } => {
            ParallelLeafCall::Condition(ConditionWaitCall {
                ctx: ctx.clone(),
                options,
                predicate,
                occurrence_id: None,
                opened_wait: false,
                parallel_group_path,
            })
        }
        ParallelOperation::Group(_) => {
            unreachable!("parallel descriptors contain only durable leaves")
        }
    }
}

impl ParallelLeafCall {
    fn poll_avro_value(&mut self, cx: &mut TaskContext<'_>) -> Poll<Result<ParallelAvroResult>> {
        match self {
            Self::Activity(call) => Pin::new(call)
                .poll_avro_value(cx)
                .map_ok(ParallelAvroResult::Activity),
            Self::ChildWorkflow(call) => Pin::new(call)
                .poll_avro_value(cx)
                .map_ok(ParallelAvroResult::ChildWorkflow),
            Self::Timer(call) => Pin::new(call)
                .poll(cx)
                .map_ok(|()| ParallelAvroResult::Timer),
            Self::Signal(call) => Pin::new(call)
                .poll_avro_value(cx)
                .map_ok(ParallelAvroResult::Signal),
            Self::Condition(call) => Pin::new(call)
                .poll(cx)
                .map_ok(ParallelAvroResult::Condition),
        }
    }
}

struct ParallelLeaf {
    call: ParallelLeafCall,
    member_path: Vec<usize>,
    group_path: Vec<ParallelGroupMetadata>,
    result: Option<ParallelAvroResult>,
}

/// Future returned by [`WorkflowContext::parallel`].
pub struct ParallelCall {
    ctx: WorkflowContext,
    operations: Option<Vec<ParallelOperation>>,
    shape: Option<ParallelShape>,
    leaves: Vec<ParallelLeaf>,
}

impl ParallelCall {
    fn new(ctx: WorkflowContext, operations: Vec<ParallelOperation>) -> Self {
        Self {
            ctx,
            operations: Some(operations),
            shape: None,
            leaves: Vec::new(),
        }
    }

    fn initialize(&mut self) -> Result<()> {
        let operations = self.operations.take().unwrap_or_default();
        validate_parallel_operations(&operations, &mut Vec::new(), true)?;
        self.shape = Some(parallel_shape(&operations));
        if operations.is_empty() {
            return Ok(());
        }

        let base_sequence = {
            let state = self
                .ctx
                .state
                .lock()
                .map_err(|_| Error::WorkflowStatePoisoned)?;
            if let Some(recorded) = state.recorded_commands.get(state.command_cursor) {
                recorded.sequence()
            } else {
                let last = state
                    .recorded_commands
                    .last()
                    .map(RecordedCommand::sequence)
                    .unwrap_or(0);
                last.checked_add(u64::try_from(state.commands.len()).unwrap_or(u64::MAX))
                    .and_then(|sequence| sequence.checked_add(1))
                    .ok_or_else(|| {
                        Error::InvalidParallelGroup(ParallelGroupError {
                            reason: "sequence_overflow",
                            member_path: Vec::new(),
                            message: "parallel group sequence identity overflowed u64".to_string(),
                        })
                    })?
            }
        };

        self.leaves = parallel_descriptors(operations, base_sequence)?
            .into_iter()
            .map(|descriptor| {
                let call = parallel_leaf_call(
                    &self.ctx,
                    descriptor.operation,
                    descriptor.group_path.clone(),
                );
                ParallelLeaf {
                    call,
                    member_path: descriptor.member_path,
                    group_path: descriptor.group_path,
                    result: None,
                }
            })
            .collect();
        Ok(())
    }

    fn poll_avro_value(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<Result<Vec<ParallelAvroResult>>> {
        if self.operations.is_some() {
            if let Err(error) = self.initialize() {
                return Poll::Ready(Err(error));
            }
        }
        if self.leaves.is_empty() {
            return Poll::Ready(Ok(Vec::new()));
        }

        let mut failures = Vec::new();
        let mut pending = false;
        for (index, leaf) in self.leaves.iter_mut().enumerate() {
            if leaf.result.is_some() {
                continue;
            }
            match leaf.call.poll_avro_value(cx) {
                Poll::Ready(Ok(result)) => leaf.result = Some(result),
                Poll::Ready(Err(error)) => failures.push((index, error)),
                Poll::Pending => pending = true,
            }
        }

        if !failures.is_empty() {
            if let Some(position) = failures
                .iter()
                .position(|(_, error)| workflow_task_integrity_error(error))
            {
                return Poll::Ready(Err(failures.remove(position).1));
            }
            failures.sort_by_key(|(index, _)| *index);
            let (failed_index, cause) = failures.remove(0);
            let failed = &self.leaves[failed_index];
            let completed = self
                .leaves
                .iter()
                .filter_map(|leaf| {
                    leaf.result
                        .clone()
                        .and_then(|result| result.into_json_result().ok())
                        .map(|result| ParallelCompletion {
                            member_path: leaf.member_path.clone(),
                            result,
                        })
                })
                .collect();
            let group_id = failed
                .group_path
                .first()
                .map(|entry| entry.parallel_group_id.clone())
                .unwrap_or_default();
            return Poll::Ready(Err(Error::ParallelFailed(ParallelFailure {
                group_id,
                member_path: failed.member_path.clone(),
                group_path: failed.group_path.clone(),
                completed,
                cause: Box::new(cause),
            })));
        }
        if pending {
            return Poll::Pending;
        }

        let mut flat_results = self
            .leaves
            .iter_mut()
            .map(|leaf| leaf.result.take().expect("completed parallel leaf"))
            .collect::<Vec<_>>()
            .into_iter();
        let results = parallel_results_for_shape(
            self.shape.as_ref().expect("initialized parallel shape"),
            &mut flat_results,
        );
        Poll::Ready(Ok(match results {
            ParallelAvroResult::Group(results) => results,
            ParallelAvroResult::Activity(_)
            | ParallelAvroResult::ChildWorkflow(_)
            | ParallelAvroResult::Timer
            | ParallelAvroResult::Signal(_)
            | ParallelAvroResult::Condition(_) => {
                unreachable!("root parallel shape is a group")
            }
        }))
    }
}

fn parallel_results_for_shape(
    shape: &ParallelShape,
    flat_results: &mut impl Iterator<Item = ParallelAvroResult>,
) -> ParallelAvroResult {
    match shape {
        ParallelShape::Leaf => flat_results.next().expect("one result per parallel leaf"),
        ParallelShape::Group(children) => ParallelAvroResult::Group(
            children
                .iter()
                .map(|child| parallel_results_for_shape(child, flat_results))
                .collect(),
        ),
    }
}

impl Future for ParallelCall {
    type Output = Result<Vec<ParallelResult>>;

    fn poll(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
        self.poll_avro_value(cx)
            .map_ok(|results| {
                results
                    .into_iter()
                    .map(ParallelAvroResult::into_json_result)
                    .collect::<Result<Vec<_>>>()
            })
            .map_ok(|result| result)
            .flatten_result()
    }
}

#[derive(Clone, Debug)]
struct SelectionMemberPlan {
    key: SelectionKey,
    index: usize,
    base_sequence: u64,
    size: usize,
    kind: String,
    shape: ParallelShape,
    leaf_start: usize,
}

fn selection_operation_kind(operation: &ParallelOperation) -> &'static str {
    match operation {
        ParallelOperation::Activity { .. } => "activity",
        ParallelOperation::ChildWorkflow { .. } => "child",
        ParallelOperation::Timer(_) => "timer",
        ParallelOperation::Signal(_) => "signal",
        ParallelOperation::Condition { .. } => "condition",
        ParallelOperation::Group(_) => "group",
    }
}

fn selection_operation_shape(operation: &ParallelOperation) -> ParallelShape {
    match operation {
        ParallelOperation::Group(children) => parallel_shape(children),
        _ => ParallelShape::Leaf,
    }
}

fn selection_descriptors(
    operations: Vec<(SelectionKey, ParallelOperation)>,
    base_sequence: u64,
) -> Result<(Vec<ParallelDescriptor>, Vec<SelectionMemberPlan>)> {
    if operations.is_empty() {
        return Err(Error::InvalidParallelGroup(ParallelGroupError {
            reason: "selection_empty",
            member_path: Vec::new(),
            message: "durable selection requires at least one operation".to_string(),
        }));
    }
    let operation_refs = operations
        .iter()
        .map(|(_, operation)| operation)
        .collect::<Vec<_>>();
    let total_size = operation_refs
        .iter()
        .map(|operation| match operation {
            ParallelOperation::Group(children) => parallel_leaf_count(children),
            _ => 1,
        })
        .sum::<usize>();
    if total_size > MAX_PARALLEL_OPERATIONS {
        return Err(Error::InvalidParallelGroup(ParallelGroupError {
            reason: "fan_out_limit_exceeded",
            member_path: Vec::new(),
            message: format!(
                "selection contains {total_size} durable leaves; the limit is {MAX_PARALLEL_OPERATIONS}"
            ),
        }));
    }
    let group_kind = {
        let mut kind = None;
        for operation in &operation_refs {
            let operation_kind = parallel_operation_kind(operation).unwrap_or("mixed");
            match kind {
                None => kind = Some(operation_kind),
                Some(current) if current == operation_kind => {}
                Some(_) => {
                    kind = Some("mixed");
                    break;
                }
            }
        }
        kind.unwrap_or("mixed")
    };

    let mut descriptors = Vec::with_capacity(total_size);
    let mut members = Vec::with_capacity(operations.len());
    let mut cursor = 0usize;
    let mut seen_keys: Vec<SelectionKey> = Vec::new();
    for (member_index, (key, operation)) in operations.into_iter().enumerate() {
        if matches!(&key, SelectionKey::Name(value) if value.is_empty()) {
            return Err(Error::InvalidParallelGroup(ParallelGroupError {
                reason: "selection_key_invalid",
                member_path: vec![member_index],
                message: "selection member keys must be non-empty strings or non-negative integers"
                    .to_string(),
            }));
        }
        if seen_keys.contains(&key) {
            return Err(Error::InvalidParallelGroup(ParallelGroupError {
                reason: "selection_key_duplicate",
                member_path: vec![member_index],
                message: format!("selection member key {key:?} is duplicated"),
            }));
        }
        seen_keys.push(key.clone());
        let member_size = match &operation {
            ParallelOperation::Group(children) => parallel_leaf_count(children),
            _ => 1,
        };
        if member_size == 0 {
            return Err(Error::InvalidParallelGroup(ParallelGroupError {
                reason: "selection_member_empty",
                member_path: vec![member_index],
                message: "a selection member must contain at least one durable leaf".to_string(),
            }));
        }
        let member_base = base_sequence
            .checked_add(u64::try_from(cursor).unwrap_or(u64::MAX))
            .ok_or(Error::TimerDurationOverflow)?;
        let member_kind = selection_operation_kind(&operation).to_string();
        let member_shape = selection_operation_shape(&operation);
        let leaf_start = descriptors.len();
        match operation {
            ParallelOperation::Group(children) => {
                validate_parallel_operations(&children, &mut vec![member_index], false)?;
                for mut descriptor in parallel_descriptors(children, member_base)? {
                    let flat_index = cursor + descriptor.offset;
                    descriptor.group_path.insert(
                        0,
                        selection_group_entry(
                            base_sequence,
                            total_size,
                            flat_index,
                            group_kind,
                            &SelectionMemberMetadata {
                                key: key.clone(),
                                index: member_index,
                                base_sequence: member_base,
                                size: member_size,
                                kind: member_kind.clone(),
                            },
                        ),
                    );
                    descriptor.member_path.insert(0, member_index);
                    descriptor.offset = flat_index;
                    descriptors.push(descriptor);
                }
            }
            operation => {
                validate_parallel_operations(
                    std::slice::from_ref(&operation),
                    &mut Vec::new(),
                    true,
                )?;
                descriptors.push(ParallelDescriptor {
                    operation,
                    offset: cursor,
                    member_path: vec![member_index],
                    group_path: vec![selection_group_entry(
                        base_sequence,
                        total_size,
                        cursor,
                        group_kind,
                        &SelectionMemberMetadata {
                            key: key.clone(),
                            index: member_index,
                            base_sequence: member_base,
                            size: member_size,
                            kind: member_kind.clone(),
                        },
                    )],
                });
            }
        }
        members.push(SelectionMemberPlan {
            key,
            index: member_index,
            base_sequence: member_base,
            size: member_size,
            kind: member_kind,
            shape: member_shape,
            leaf_start,
        });
        cursor += member_size;
    }
    Ok((descriptors, members))
}

struct SelectionLeaf {
    call: ParallelLeafCall,
    outcome: Option<Result<ParallelAvroResult>>,
}

/// Stable reference to one member of a durable selection group.
#[derive(Clone)]
pub struct DurableOperationHandle {
    ctx: WorkflowContext,
    pub key: SelectionKey,
    pub index: usize,
    pub kind: String,
    pub identity: String,
    pub base_sequence: u64,
    pub size: usize,
    pub selection_group_id: String,
    shape: ParallelShape,
}

impl std::fmt::Debug for DurableOperationHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DurableOperationHandle")
            .field("key", &self.key)
            .field("index", &self.index)
            .field("kind", &self.kind)
            .field("identity", &self.identity)
            .field("base_sequence", &self.base_sequence)
            .field("size", &self.size)
            .field("selection_group_id", &self.selection_group_id)
            .finish()
    }
}

impl DurableOperationHandle {
    /// Await this member after another member has already won the selection.
    pub fn await_result(&self) -> DurableOperationAwaitCall {
        DurableOperationAwaitCall {
            handle: self.clone(),
        }
    }

    /// Request cancellation without affecting siblings. The future resolves
    /// to unit; only `SelectionOperationCancelled` history proves cancellation
    /// beat a concurrently committed terminal result.
    pub fn cancel(&self) -> CancelDurableOperationCall {
        CancelDurableOperationCall {
            handle: self.clone(),
            emitted: false,
        }
    }
}

/// The one winner committed for a durable selection group.
#[derive(Debug)]
pub struct SelectionResult {
    pub key: SelectionKey,
    pub index: usize,
    pub kind: String,
    pub identity: String,
    pub value: Option<ParallelResult>,
    pub failure: Option<Error>,
    pub winner: DurableOperationHandle,
    pub handles: Vec<DurableOperationHandle>,
}

impl SelectionResult {
    pub fn succeeded(&self) -> bool {
        self.failure.is_none()
    }

    pub fn handle(&self, key: &SelectionKey) -> Option<&DurableOperationHandle> {
        self.handles.iter().find(|handle| &handle.key == key)
    }

    pub fn remaining(&self) -> Vec<&DurableOperationHandle> {
        self.handles
            .iter()
            .filter(|handle| handle.index != self.index)
            .collect()
    }

    pub fn into_result(self) -> Result<ParallelResult> {
        match (self.value, self.failure) {
            (Some(value), None) => Ok(value),
            (_, Some(error)) => Err(error),
            _ => Err(Error::WorkerLoop(
                "selection result contained neither a value nor a failure".to_string(),
            )),
        }
    }
}

/// Future returned by [`WorkflowContext::select`].
pub struct SelectCall {
    ctx: WorkflowContext,
    operations: Option<Vec<(SelectionKey, ParallelOperation)>>,
    members: Vec<SelectionMemberPlan>,
    leaves: Vec<SelectionLeaf>,
    group_id: Option<String>,
}

impl SelectCall {
    fn new(ctx: WorkflowContext, operations: Vec<(SelectionKey, ParallelOperation)>) -> Self {
        Self {
            ctx,
            operations: Some(operations),
            members: Vec::new(),
            leaves: Vec::new(),
            group_id: None,
        }
    }

    fn initialize(&mut self) -> Result<()> {
        let operations = self.operations.take().unwrap_or_default();
        let base_sequence = {
            let state = self
                .ctx
                .state
                .lock()
                .map_err(|_| Error::WorkflowStatePoisoned)?;
            if let Some(marker) = state.selection_markers.get(state.selection_marker_cursor) {
                marker.selection_group_base_sequence
            } else if let Some(recorded) = state.recorded_commands.get(state.command_cursor) {
                recorded.sequence()
            } else {
                let last = state
                    .recorded_commands
                    .last()
                    .map(RecordedCommand::sequence)
                    .unwrap_or(0);
                last.checked_add(u64::try_from(state.commands.len()).unwrap_or(u64::MAX))
                    .and_then(|sequence| sequence.checked_add(1))
                    .ok_or(Error::TimerDurationOverflow)?
            }
        };
        let (descriptors, members) = selection_descriptors(operations, base_sequence)?;
        let group_id = format!("select-calls:{base_sequence}:{}", descriptors.len());
        self.leaves = descriptors
            .into_iter()
            .map(|descriptor| SelectionLeaf {
                call: parallel_leaf_call(&self.ctx, descriptor.operation, descriptor.group_path),
                outcome: None,
            })
            .collect();
        self.members = members;
        self.group_id = Some(group_id);
        Ok(())
    }
}

impl Future for SelectCall {
    type Output = Result<SelectionResult>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
        if self.operations.is_some() {
            if let Err(error) = self.initialize() {
                return Poll::Ready(Err(error));
            }
        }

        for leaf in &mut self.leaves {
            if leaf.outcome.is_some() {
                continue;
            }
            if let Poll::Ready(outcome) = leaf.call.poll_avro_value(cx) {
                if outcome
                    .as_ref()
                    .err()
                    .is_some_and(workflow_task_integrity_error)
                {
                    return Poll::Ready(outcome.map(|_| unreachable!()));
                }
                leaf.outcome = Some(outcome);
            }
        }

        let all_members_terminal = self.leaves.iter().all(|leaf| leaf.outcome.is_some());
        let selection_member_range = self
            .members
            .first()
            .map(|member| member.base_sequence)
            .zip(self.leaves.len().try_into().ok())
            .map(|(base_sequence, size): (u64, u64)| {
                base_sequence..base_sequence.saturating_add(size)
            });
        let marker = {
            let mut state = match self.ctx.state.lock() {
                Ok(state) => state,
                Err(_) => return Poll::Ready(Err(Error::WorkflowStatePoisoned)),
            };
            let marker = state
                .selection_markers
                .get(state.selection_marker_cursor)
                .cloned();
            if marker.is_none()
                && all_members_terminal
                && selection_member_range.as_ref().is_some_and(|member_range| {
                    state
                        .recorded_commands
                        .iter()
                        .any(|command| member_range.contains(&command.sequence()))
                })
            {
                // Terminal member history can be committed before the server's
                // SelectionResolved marker becomes visible to this task. That
                // marker is the durable barrier the selection is still waiting
                // on, so an otherwise commandless replay remains legitimately
                // pending instead of failing as an untracked yield.
                state.matched_recorded_pending = true;
            }
            marker
        };
        let Some(marker) = marker else {
            return Poll::Pending;
        };
        if self.group_id.as_deref() != Some(marker.selection_group_id.as_str())
            || marker.selection_group_size != self.leaves.len()
            || self.members.first().map(|member| member.base_sequence)
                != Some(marker.selection_group_base_sequence)
        {
            return Poll::Ready(Err(invalid_recorded_history(
                "selection_group_shape_mismatch",
                marker.selection_group_base_sequence,
                self.group_id
                    .as_deref()
                    .unwrap_or("initialized selection group"),
                &marker.selection_group_id,
                "recorded selection group differs from current workflow code",
            )));
        }
        let Some(member_position) = self.members.iter().position(|member| {
            member.key == marker.member_key
                && member.index == marker.member_index
                && member.base_sequence == marker.member_base_sequence
                && member.size == marker.member_size
                && member.kind == marker.operation_kind
        }) else {
            return Poll::Ready(Err(invalid_recorded_history(
                "selection_member_shape_mismatch",
                marker.member_base_sequence,
                "winner member matching current workflow code",
                &format!("{:?}", marker.member_key),
                "recorded selection winner differs from the authored member identity",
            )));
        };
        let member = self.members[member_position].clone();
        let (handles, resolution_sequence) = {
            let mut state = match self.ctx.state.lock() {
                Ok(state) => state,
                Err(_) => return Poll::Ready(Err(Error::WorkflowStatePoisoned)),
            };
            let identities = self
                .members
                .iter()
                .map(|candidate| {
                    selection_operation_identity(
                        &state,
                        &candidate.kind,
                        candidate.base_sequence,
                        candidate.size,
                    )
                })
                .collect::<Vec<_>>();
            if let Some((position, missing)) = identities
                .iter()
                .enumerate()
                .find(|(_, identity)| identity.is_empty())
                .map(|(position, identity)| (position, identity.clone()))
            {
                let candidate = &self.members[position];
                return Poll::Ready(Err(invalid_recorded_history(
                    "selection_operation_identity_missing",
                    candidate.base_sequence,
                    &format!(
                        "durable {} resource identity from scheduled/open history",
                        candidate.kind
                    ),
                    &missing,
                    "selection member history is missing its canonical durable identity",
                )));
            }
            let expected_winner_identity = &identities[member_position];
            let resolution_sequence = match validated_selection_resolution_sequence(
                &state,
                &marker,
                &member,
                expected_winner_identity,
            ) {
                Ok(sequence) => sequence,
                Err(error) => return Poll::Ready(Err(error)),
            };
            let handles = self
                .members
                .iter()
                .zip(identities)
                .map(|(member, identity)| DurableOperationHandle {
                    ctx: self.ctx.clone(),
                    key: member.key.clone(),
                    index: member.index,
                    kind: member.kind.clone(),
                    identity,
                    base_sequence: member.base_sequence,
                    size: member.size,
                    selection_group_id: marker.selection_group_id.clone(),
                    shape: member.shape.clone(),
                })
                .collect::<Vec<_>>();
            if let Err(error) = validate_selection_cancellations_for_handles(&state, &handles) {
                return Poll::Ready(Err(error));
            }
            state.selection_marker_cursor += 1;
            (handles, resolution_sequence)
        };

        let mut winner_failure = None;
        let mut flat_results = Vec::with_capacity(member.size);
        if marker.outcome == "failed" {
            let resolution_offset = match resolution_sequence
                .checked_sub(member.base_sequence)
                .and_then(|offset| usize::try_from(offset).ok())
            {
                Some(offset) if offset < member.size => offset,
                _ => {
                    return Poll::Ready(Err(invalid_recorded_history(
                        "selection_resolution_event_mismatch",
                        member.base_sequence,
                        "failure event within selected member bounds",
                        &resolution_sequence.to_string(),
                        "selection failure event is outside the authored member",
                    )))
                }
            };
            let leaf = &mut self.leaves[member.leaf_start + resolution_offset];
            match leaf.outcome.take() {
                Some(Err(error)) => winner_failure = Some(error),
                _ => {
                    return Poll::Ready(Err(invalid_recorded_history(
                        "selection_winner_outcome_mismatch",
                        member.base_sequence,
                        "exact failed terminal history referenced by SelectionResolved",
                        "missing or successful resolution event",
                        "selection winner marker disagrees with terminal operation history",
                    )))
                }
            }
        } else {
            for leaf in &mut self.leaves[member.leaf_start..member.leaf_start + member.size] {
                match leaf.outcome.take() {
                    Some(Ok(result)) => flat_results.push(result),
                    Some(Err(_)) => {
                        return Poll::Ready(Err(invalid_recorded_history(
                            "selection_winner_outcome_mismatch",
                            member.base_sequence,
                            "fully completed nested selection member",
                            "failed durable leaf",
                            "completed selection winner contains a failed leaf",
                        )))
                    }
                    None => {
                        return Poll::Ready(Err(invalid_recorded_history(
                            "selection_winner_unresolved",
                            member.base_sequence,
                            "terminal history for every completed winner leaf",
                            "pending member history",
                            "completed SelectionResolved member has an unfinished durable barrier",
                        )))
                    }
                }
            }
        }
        let value = if winner_failure.is_none() {
            let mut flat_results = flat_results.into_iter();
            let value = parallel_results_for_shape(&member.shape, &mut flat_results);
            match value.into_json_result() {
                Ok(value) => Some(value),
                Err(error) => return Poll::Ready(Err(error)),
            }
        } else {
            None
        };
        let winner = handles[member_position].clone();
        Poll::Ready(Ok(SelectionResult {
            key: winner.key.clone(),
            index: winner.index,
            kind: winner.kind.clone(),
            identity: winner.identity.clone(),
            value,
            failure: winner_failure,
            winner,
            handles,
        }))
    }
}

fn selection_operation_identity(
    state: &WorkflowState,
    kind: &str,
    base_sequence: u64,
    size: usize,
) -> String {
    if kind == "group" {
        return format!("group:{base_sequence}:{size}");
    }
    let fields: &[&str] = match kind {
        "activity" => &["activity_execution_id"],
        "child" => &["child_workflow_run_id"],
        "timer" => &["timer_id"],
        "signal" => &["signal_wait_id"],
        "condition" => &["condition_wait_id"],
        _ => &[],
    };
    for sequence in base_sequence..base_sequence.saturating_add(size as u64) {
        for event in state
            .history_events
            .iter()
            .filter(|event| durable_event_sequence(event) == Some(sequence))
        {
            for field in fields {
                if let Some(identity) = event.payload.get(*field).and_then(Value::as_str) {
                    if !identity.is_empty() {
                        return identity.to_string();
                    }
                }
            }
        }
    }
    String::new()
}

fn validated_selection_resolution_sequence(
    state: &WorkflowState,
    marker: &SelectionMarker,
    member: &SelectionMemberPlan,
    expected_identity: &str,
) -> Result<u64> {
    if expected_identity.is_empty() {
        return Err(invalid_recorded_history(
            "selection_operation_identity_missing",
            member.base_sequence,
            &format!(
                "durable {} resource identity from scheduled/open history",
                member.kind
            ),
            "missing operation identity",
            "selection member history is missing its canonical durable identity",
        ));
    }
    if marker.operation_identity != expected_identity {
        return Err(invalid_recorded_history(
            "selection_operation_identity_mismatch",
            member.base_sequence,
            expected_identity,
            &marker.operation_identity,
            "selection winner identity does not match durable scheduled/open history",
        ));
    }

    let failure_types = [
        "ActivityFailed",
        "ActivityCancelled",
        "ActivityTimedOut",
        "ChildRunFailed",
        "ChildRunCancelled",
        "ChildRunTerminated",
    ];
    let success_types = [
        "ActivityCompleted",
        "ChildRunCompleted",
        "TimerFired",
        "SignalApplied",
        "ConditionWaitSatisfied",
        "ConditionWaitTimedOut",
    ];
    let terminal_types: &[&str] = if marker.outcome == "failed" {
        &failure_types
    } else {
        &success_types
    };
    let mut candidates = Vec::new();
    for event in state.history_events.iter() {
        let Some(sequence) = durable_event_sequence(event) else {
            continue;
        };
        if sequence < member.base_sequence
            || sequence >= member.base_sequence.saturating_add(member.size as u64)
            || !terminal_types.contains(&event.event_type.as_str())
        {
            continue;
        }
        let event_id = event
            .raw
            .get("id")
            .or_else(|| event.raw.get("event_id"))
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                invalid_recorded_history(
                    "selection_resolution_event_id_missing",
                    member.base_sequence,
                    "terminal selection history with a durable event id",
                    &event.payload.to_string(),
                    "selection terminal history cannot be bound to its winner marker",
                )
            })?;
        candidates.push((event_id.to_string(), event.event_type.clone(), sequence));
    }
    let resolution = if marker.outcome == "failed" {
        candidates.first()
    } else {
        candidates.last()
    };
    let Some((event_id, event_type, sequence)) = resolution else {
        return Err(invalid_recorded_history(
            "selection_resolution_event_missing",
            member.base_sequence,
            "terminal history for the selected member",
            &format!("{:?}", marker.member_key),
            "selection winner marker has no matching durable terminal event",
        ));
    };
    if event_id != &marker.resolution_event_id || event_type != &marker.resolution_event_type {
        return Err(invalid_recorded_history(
            "selection_resolution_event_mismatch",
            member.base_sequence,
            &format!("{event_type}:{event_id}"),
            &format!(
                "{}:{}",
                marker.resolution_event_type, marker.resolution_event_id
            ),
            "selection winner marker does not reference the event that made its member terminal",
        ));
    }
    Ok(*sequence)
}

fn recorded_selection_member_outcome(
    state: &WorkflowState,
    handle: &DurableOperationHandle,
) -> Result<Option<ParallelResult>> {
    for event in state.history_events.iter() {
        let Some(sequence) = durable_event_sequence(event) else {
            continue;
        };
        if sequence < handle.base_sequence
            || sequence >= handle.base_sequence.saturating_add(handle.size as u64)
            || !matches!(
                event.event_type.as_str(),
                "ActivityFailed"
                    | "ActivityCancelled"
                    | "ActivityTimedOut"
                    | "ChildRunFailed"
                    | "ChildRunCancelled"
                    | "ChildRunTerminated"
            )
        {
            continue;
        }
        let Some(command) = state
            .recorded_commands
            .iter()
            .find(|command| command.sequence() == sequence)
        else {
            continue;
        };
        match command {
            RecordedCommand::Activity {
                outcome: Some(Err(failure)),
                ..
            } => return Err(Error::ActivityFailed(failure.clone())),
            RecordedCommand::ChildWorkflow {
                outcome: Some(Err(failure)),
                ..
            } => return Err(Error::ChildWorkflowFailed(failure.clone())),
            _ => {}
        }
    }

    let mut results = Vec::with_capacity(handle.size);
    for sequence in handle.base_sequence..handle.base_sequence.saturating_add(handle.size as u64) {
        let Some(command) = state
            .recorded_commands
            .iter()
            .find(|command| command.sequence() == sequence)
        else {
            return Ok(None);
        };
        let result = match command {
            RecordedCommand::Activity { outcome, .. } => match outcome {
                Some(Ok(value)) => ParallelAvroResult::Activity(value.clone()),
                Some(Err(failure)) => return Err(Error::ActivityFailed(failure.clone())),
                None => return Ok(None),
            },
            RecordedCommand::Timer { fired, .. } => {
                if !fired {
                    return Ok(None);
                }
                ParallelAvroResult::Timer
            }
            RecordedCommand::ChildWorkflow { outcome, .. } => match outcome {
                Some(Ok(value)) => ParallelAvroResult::ChildWorkflow(value.clone()),
                Some(Err(failure)) => return Err(Error::ChildWorkflowFailed(failure.clone())),
                None => return Ok(None),
            },
            RecordedCommand::SignalWait { value, .. } => match value {
                Some(value) => ParallelAvroResult::Signal(value.clone()),
                None => return Ok(None),
            },
            RecordedCommand::ConditionWait { result, .. } => match result {
                Some(result) => ParallelAvroResult::Condition(*result),
                None => return Ok(None),
            },
            other => {
                return Err(command_mismatch(
                    other,
                    format!("selected {} member", handle.kind),
                ))
            }
        };
        results.push(result);
    }
    let mut results = results.into_iter();
    parallel_results_for_shape(&handle.shape, &mut results)
        .into_json_result()
        .map(Some)
}

fn recorded_selection_member_is_terminal(
    state: &WorkflowState,
    handle: &DurableOperationHandle,
) -> bool {
    let mut completed = 0usize;
    let mut all_completed = true;
    for sequence in handle.base_sequence..handle.base_sequence.saturating_add(handle.size as u64) {
        let Some(command) = state
            .recorded_commands
            .iter()
            .find(|command| command.sequence() == sequence)
        else {
            all_completed = false;
            continue;
        };
        let terminal = match command {
            RecordedCommand::Activity {
                outcome: Some(Err(_)),
                ..
            }
            | RecordedCommand::ChildWorkflow {
                outcome: Some(Err(_)),
                ..
            } => return true,
            RecordedCommand::Activity { outcome, .. } => outcome.is_some(),
            RecordedCommand::ChildWorkflow { outcome, .. } => outcome.is_some(),
            RecordedCommand::Timer { fired, .. } => *fired,
            RecordedCommand::SignalWait { value, .. } => value.is_some(),
            RecordedCommand::ConditionWait { result, .. } => result.is_some(),
            RecordedCommand::SearchAttributes { .. }
            | RecordedCommand::SideEffect { .. }
            | RecordedCommand::VersionMarker { .. }
            | RecordedCommand::Memo { .. } => false,
        };
        if !terminal {
            all_completed = false;
            continue;
        }
        completed += 1;
    }
    all_completed && completed == handle.size
}

fn selection_cancellation_for_handle(
    state: &WorkflowState,
    handle: &DurableOperationHandle,
) -> Result<bool> {
    let Some(marker) = state.cancelled_selection_members.iter().find(|recorded| {
        recorded.selection_group_id == handle.selection_group_id
            && recorded.member_base_sequence == handle.base_sequence
    }) else {
        return Ok(false);
    };
    validate_selection_cancellation_marker(marker, handle)?;
    Ok(true)
}

fn validate_selection_cancellations_for_handles(
    state: &WorkflowState,
    handles: &[DurableOperationHandle],
) -> Result<()> {
    let Some(group_id) = handles
        .first()
        .map(|handle| handle.selection_group_id.as_str())
    else {
        return Ok(());
    };
    for marker in state
        .cancelled_selection_members
        .iter()
        .filter(|marker| marker.selection_group_id == group_id)
    {
        let Some(handle) = handles
            .iter()
            .find(|handle| handle.base_sequence == marker.member_base_sequence)
        else {
            return Err(invalid_recorded_history(
                "selection_cancellation_member_mismatch",
                marker.member_base_sequence,
                "SelectionOperationCancelled matching an authored selection handle",
                &format!("{marker:?}"),
                "selection cancellation member base does not name an authored member",
            ));
        };
        validate_selection_cancellation_marker(marker, handle)?;
    }
    Ok(())
}

fn validate_selection_cancellation_marker(
    marker: &SelectionCancellation,
    handle: &DurableOperationHandle,
) -> Result<()> {
    if marker.selection_group_id != handle.selection_group_id
        || marker.member_key != handle.key
        || marker.member_index != handle.index
        || marker.member_base_sequence != handle.base_sequence
        || marker.member_size != handle.size
        || marker.operation_kind != handle.kind
        || marker.operation_identity != handle.identity
    {
        return Err(invalid_recorded_history(
            "selection_cancellation_member_mismatch",
            handle.base_sequence,
            "SelectionOperationCancelled matching the authored selection handle",
            &format!("{marker:?}"),
            "selection cancellation history targets different authored member metadata",
        ));
    }
    Ok(())
}

/// Future returned by [`DurableOperationHandle::await_result`].
pub struct DurableOperationAwaitCall {
    handle: DurableOperationHandle,
}

impl Future for DurableOperationAwaitCall {
    type Output = Result<ParallelResult>;

    fn poll(self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
        let state = match self.handle.ctx.state.lock() {
            Ok(state) => state,
            Err(_) => return Poll::Ready(Err(Error::WorkflowStatePoisoned)),
        };
        match selection_cancellation_for_handle(&state, &self.handle) {
            Err(error) => return Poll::Ready(Err(error)),
            Ok(false) => {}
            Ok(true) => {
                return Poll::Ready(Err(Error::DurableOperationCancelled(
                    DurableOperationCancelled {
                        selection_group_id: self.handle.selection_group_id.clone(),
                        member_key: self.handle.key.clone(),
                        member_index: self.handle.index,
                        operation_kind: self.handle.kind.clone(),
                        operation_identity: self.handle.identity.clone(),
                    },
                )));
            }
        }
        match recorded_selection_member_outcome(&state, &self.handle) {
            Ok(Some(result)) => Poll::Ready(Ok(result)),
            Ok(None) => Poll::Pending,
            Err(error) => Poll::Ready(Err(error)),
        }
    }
}

/// Future returned by [`DurableOperationHandle::cancel`].
pub struct CancelDurableOperationCall {
    handle: DurableOperationHandle,
    emitted: bool,
}

impl Future for CancelDurableOperationCall {
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
        let ctx = self.handle.ctx.clone();
        let mut state = match ctx.state.lock() {
            Ok(state) => state,
            Err(_) => return Poll::Ready(Err(Error::WorkflowStatePoisoned)),
        };
        match selection_cancellation_for_handle(&state, &self.handle) {
            Err(error) => return Poll::Ready(Err(error)),
            Ok(true) => return Poll::Ready(Ok(())),
            Ok(false) => {}
        }
        if recorded_selection_member_is_terminal(&state, &self.handle) {
            return Poll::Ready(Ok(()));
        }
        if !self.emitted {
            state.commands.push(json!({
                "type": "cancel_selection_operation",
                "selection_group_id": self.handle.selection_group_id,
                "member_key": self.handle.key,
                "member_index": self.handle.index,
                "member_base_sequence": self.handle.base_sequence,
                "member_size": self.handle.size,
                "operation_kind": self.handle.kind,
                "operation_identity": self.handle.identity,
            }));
            self.emitted = true;
        }
        // The cancellation command is only a request. Do not expose workflow
        // state after cancel until committed SelectionOperationCancelled
        // history proves that the request won the race with member completion.
        Poll::Pending
    }
}

trait PollNestedResultExt<T> {
    fn flatten_result(self) -> Poll<Result<T>>;
}

impl<T> PollNestedResultExt<T> for Poll<Result<Result<T>>> {
    fn flatten_result(self) -> Poll<Result<T>> {
        match self {
            Poll::Ready(Ok(result)) => Poll::Ready(result),
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => Poll::Pending,
        }
    }
}

struct SagaCompensation {
    activity_type: String,
    options: ActivityOptions,
    arguments: AvroValue,
    registration_order: usize,
}

/// Workflow-local deterministic saga compensation helper.
///
/// Register each compensation only after its forward step succeeds. Passing
/// the forward `Result` to [`Saga::finish`] runs compensations sequentially in
/// reverse registration order after any failure, including cooperative
/// cancellation. Each compensation is an ordinary durable activity, so replay,
/// duplicate delivery, and worker restart use existing history semantics.
pub struct Saga {
    ctx: WorkflowContext,
    compensations: Vec<SagaCompensation>,
}

impl Saga {
    fn new(ctx: WorkflowContext) -> Self {
        Self {
            ctx,
            compensations: Vec::new(),
        }
    }

    pub fn add_compensation<T: Serialize>(
        &mut self,
        activity_type: impl Into<String>,
        args: T,
    ) -> Result<&mut Self> {
        self.add_compensation_with_options(activity_type, ActivityOptions::new(), args)
    }

    pub fn add_compensation_with_options<T: Serialize>(
        &mut self,
        activity_type: impl Into<String>,
        options: ActivityOptions,
        args: T,
    ) -> Result<&mut Self> {
        let activity_type = activity_type.into();
        if activity_type.trim().is_empty() || activity_type.trim() != activity_type {
            return Err(Error::Codec(
                "saga compensation activity type must be non-empty without surrounding whitespace"
                    .to_string(),
            ));
        }
        options.validate().map_err(Error::InvalidActivityOptions)?;
        let arguments = AvroValue::from_serialize(&args)?;
        let registration_order = self.compensations.len() + 1;
        self.compensations.push(SagaCompensation {
            activity_type,
            options,
            arguments,
            registration_order,
        });
        Ok(self)
    }

    /// Compensate `initiating_failure` and return the failure that must remain.
    pub async fn compensate(mut self, initiating_failure: Error) -> Error {
        while let Some(compensation) = self.compensations.pop() {
            if let Err(compensation_failure) = self
                .ctx
                .activity_with_options(
                    compensation.activity_type.clone(),
                    compensation.options,
                    compensation.arguments,
                )
                .await
            {
                if workflow_task_integrity_error(&compensation_failure) {
                    return compensation_failure;
                }
                return Error::SagaCompensationFailed(SagaCompensationFailure {
                    initiating_failure: Box::new(initiating_failure),
                    compensation_failure: Box::new(compensation_failure),
                    compensation_activity_type: compensation.activity_type,
                    compensation_registration_order: compensation.registration_order,
                });
            }
        }
        initiating_failure
    }

    /// Return a successful forward value or compensate and preserve its failure.
    pub async fn finish<T>(self, outcome: Result<T>) -> Result<T> {
        match outcome {
            Ok(value) => Ok(value),
            Err(error) => Err(self.compensate(error).await),
        }
    }
}

pub struct ActivityCall {
    ctx: WorkflowContext,
    activity_type: String,
    options: ActivityOptions,
    args: Option<Result<AvroValue>>,
    scheduled: bool,
    parallel_group_path: Vec<ParallelGroupMetadata>,
}

impl ActivityCall {
    fn poll_avro_value(
        mut self: Pin<&mut Self>,
        _cx: &mut TaskContext<'_>,
    ) -> Poll<Result<AvroValue>> {
        let ctx = self.ctx.clone();
        let mut state = match ctx.state.lock() {
            Ok(state) => state,
            Err(_) => return Poll::Ready(Err(Error::WorkflowStatePoisoned)),
        };

        if self.scheduled {
            return Poll::Pending;
        }

        let options = match self.options.validate() {
            Ok(options) => options,
            Err(error) => {
                return Poll::Ready(Err(Error::InvalidActivityOptions(error)));
            }
        };
        let task_queue = options
            .task_queue
            .clone()
            .unwrap_or_else(|| state.task_queue.clone());
        let current_recorded_options = RecordedActivityOptions {
            task_queue: RecordedSnapshotValue::Known(Some(task_queue.clone())),
            // Rust schedules ordinary durable activities. The server records a
            // non-null mode only for a specialized execution primitive.
            execution_mode: RecordedSnapshotValue::Known(None),
            retry_policy: current_activity_retry_snapshot(&options),
        };

        if let Some(recorded) = state.recorded_commands.get(state.command_cursor).cloned() {
            let sequence = recorded.sequence();
            match recorded {
                RecordedCommand::Activity {
                    activity_type,
                    options: recorded_options,
                    outcome,
                    parallel_group_path,
                    ..
                } => {
                    if let Err(error) = ensure_parallel_path_matches(
                        sequence,
                        parallel_group_path.as_deref(),
                        &self.parallel_group_path,
                    ) {
                        return Poll::Ready(Err(error));
                    }
                    if let Some(recorded_type) = activity_type {
                        if recorded_type != self.activity_type {
                            return Poll::Ready(Err(Error::NonDeterministicReplay(
                                ReplayFailure::new(
                                    "recorded_command_detail_mismatch",
                                    Some(sequence),
                                    Some(format!("activity:{recorded_type}")),
                                    Some(format!("activity:{}", self.activity_type)),
                                    "recorded activity type differs from the current workflow command",
                                ),
                            )));
                        }
                    }
                    if let Some(recorded_options) = recorded_options {
                        if !recorded_options
                            .task_queue
                            .matches_current(&current_recorded_options.task_queue)
                        {
                            return Poll::Ready(Err(Error::NonDeterministicReplay(
                                ReplayFailure::new(
                                    "activity_task_queue_mismatch",
                                    Some(sequence),
                                    Some(activity_options_description(&recorded_options)),
                                    Some(activity_options_description(&current_recorded_options)),
                                    "recorded activity task queue differs from the current workflow command",
                                ),
                            )));
                        }
                        if !recorded_options
                            .execution_mode
                            .matches_current(&current_recorded_options.execution_mode)
                        {
                            return Poll::Ready(Err(Error::NonDeterministicReplay(
                                ReplayFailure::new(
                                    "activity_execution_mode_mismatch",
                                    Some(sequence),
                                    Some(activity_options_description(&recorded_options)),
                                    Some(activity_options_description(&current_recorded_options)),
                                    "recorded activity execution mode differs from the current workflow command",
                                ),
                            )));
                        }
                        if !recorded_options
                            .retry_policy
                            .matches_current(&current_recorded_options.retry_policy)
                        {
                            return Poll::Ready(Err(Error::NonDeterministicReplay(
                                ReplayFailure::new(
                                    "activity_retry_policy_mismatch",
                                    Some(sequence),
                                    Some(activity_options_description(&recorded_options)),
                                    Some(activity_options_description(&current_recorded_options)),
                                    "recorded activity retry policy differs from the current workflow command",
                                ),
                            )));
                        }
                    }
                    state.command_cursor += 1;
                    if let Some(outcome) = outcome {
                        return Poll::Ready(outcome.map_err(Error::ActivityFailed));
                    }
                    state.matched_recorded_pending = true;
                    self.scheduled = true;
                    return Poll::Pending;
                }
                other => {
                    return Poll::Ready(Err(command_mismatch(
                        &other,
                        format!("activity:{}", self.activity_type),
                    )));
                }
            }
        }

        if !self.scheduled {
            let args = match self.args.take().unwrap_or(Ok(AvroValue::Null)) {
                Ok(args) => args,
                Err(error) => return Poll::Ready(Err(error)),
            };
            let arguments = normalize_avro_arguments(args);
            let envelope = match encode_typed_envelope(&arguments, &state.payload_codec) {
                Ok(envelope) => envelope,
                Err(error) => return Poll::Ready(Err(error)),
            };

            let mut command = serde_json::Map::from_iter([
                ("type".to_string(), json!("schedule_activity")),
                (
                    "activity_type".to_string(),
                    json!(self.activity_type.clone()),
                ),
                ("queue".to_string(), json!(task_queue)),
                ("arguments".to_string(), envelope),
            ]);
            for (field, value) in [
                ("start_to_close_timeout", options.start_to_close_timeout),
                (
                    "schedule_to_start_timeout",
                    options.schedule_to_start_timeout,
                ),
                (
                    "schedule_to_close_timeout",
                    options.schedule_to_close_timeout,
                ),
                ("heartbeat_timeout", options.heartbeat_timeout),
            ] {
                if let Some(value) = value {
                    command.insert(field.to_string(), json!(value));
                }
            }
            if let Some(retry_policy) = options.retry_policy {
                command.insert("retry_policy".to_string(), retry_policy);
            }
            apply_parallel_group_path(&mut command, &self.parallel_group_path);
            state.commands.push(Value::Object(command));
            self.scheduled = true;
        }

        Poll::Pending
    }
}

impl Future for ActivityCall {
    type Output = Result<Value>;

    fn poll(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
        match self.poll_avro_value(cx) {
            Poll::Ready(Ok(value)) => Poll::Ready(value.into_json()),
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Future returned by [`WorkflowContext::sleep`].
pub struct TimerCall {
    ctx: WorkflowContext,
    delay_seconds: Option<u64>,
    scheduled: bool,
    matched_pending: bool,
    parallel_group_path: Vec<ParallelGroupMetadata>,
}

impl Future for TimerCall {
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
        if self.matched_pending {
            return Poll::Pending;
        }

        let ctx = self.ctx.clone();
        let Some(requested_delay) = self.delay_seconds else {
            return Poll::Ready(Err(Error::TimerDurationOverflow));
        };
        let mut state = match ctx.state.lock() {
            Ok(state) => state,
            Err(_) => return Poll::Ready(Err(Error::WorkflowStatePoisoned)),
        };

        if let Some(recorded) = state.recorded_commands.get(state.command_cursor).cloned() {
            match recorded {
                RecordedCommand::Timer {
                    sequence,
                    delay_seconds,
                    fired,
                    parallel_group_path,
                    ..
                } => {
                    if let Err(error) = ensure_parallel_path_matches(
                        sequence,
                        parallel_group_path.as_deref(),
                        &self.parallel_group_path,
                    ) {
                        return Poll::Ready(Err(error));
                    }
                    if delay_seconds != requested_delay {
                        return Poll::Ready(Err(Error::NonDeterministicReplay(
                            ReplayFailure::new(
                                "timer_delay_mismatch",
                                Some(sequence),
                                Some(format!("timer:{delay_seconds}s")),
                                Some(format!("timer:{requested_delay}s")),
                                "recorded timer delay differs from the current workflow command",
                            ),
                        )));
                    }
                    state.command_cursor += 1;
                    if fired {
                        return Poll::Ready(Ok(()));
                    }
                    state.matched_recorded_pending = true;
                    self.scheduled = true;
                    self.matched_pending = true;
                    return Poll::Pending;
                }
                other => return Poll::Ready(Err(command_mismatch(&other, "timer"))),
            }
        }

        if !self.scheduled {
            let mut command = serde_json::Map::from_iter([
                ("type".to_string(), json!("start_timer")),
                ("delay_seconds".to_string(), json!(requested_delay)),
            ]);
            apply_parallel_group_path(&mut command, &self.parallel_group_path);
            state.commands.push(Value::Object(command));
            self.scheduled = true;
        }

        Poll::Pending
    }
}

/// Future returned by [`WorkflowContext::wait_condition`].
pub struct ConditionWaitCall {
    ctx: WorkflowContext,
    options: ConditionWaitOptions,
    predicate: Box<dyn Fn() -> Result<bool> + Send + 'static>,
    occurrence_id: Option<String>,
    opened_wait: bool,
    parallel_group_path: Vec<ParallelGroupMetadata>,
}

impl Future for ConditionWaitCall {
    type Output = Result<ConditionWaitResult>;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
        if self.opened_wait {
            return Poll::Pending;
        }

        let options = match self.options.validate() {
            Ok(options) => options,
            Err(error) => return Poll::Ready(Err(Error::InvalidConditionWaitOptions(error))),
        };
        let ctx = self.ctx.clone();
        let occurrence_id = match self.occurrence_id.as_ref() {
            Some(occurrence_id) => occurrence_id.clone(),
            None => {
                let mut state = match ctx.state.lock() {
                    Ok(state) => state,
                    Err(_) => return Poll::Ready(Err(Error::WorkflowStatePoisoned)),
                };
                let ordinal = state.condition_wait_occurrence_counter;
                state.condition_wait_occurrence_counter = match ordinal.checked_add(1) {
                    Some(next) => next,
                    None => {
                        return Poll::Ready(Err(Error::WorkerLoop(
                            "condition wait occurrence counter overflowed".to_string(),
                        )))
                    }
                };
                let occurrence_id = format!("{CONDITION_WAIT_OCCURRENCE_PREFIX}{ordinal}");
                drop(state);
                self.occurrence_id = Some(occurrence_id.clone());
                occurrence_id
            }
        };

        let recorded_result = {
            let mut state = match ctx.state.lock() {
                Ok(state) => state,
                Err(_) => return Poll::Ready(Err(Error::WorkflowStatePoisoned)),
            };
            let Some(recorded) = state.recorded_commands.get(state.command_cursor).cloned() else {
                drop(state);
                return self.poll_new_condition(options);
            };
            if !matches!(recorded, RecordedCommand::ConditionWait { .. }) {
                return Poll::Ready(Err(command_mismatch(&recorded, "condition wait")));
            }

            let mut cursor = state.command_cursor;
            let mut result = None;
            loop {
                let Some(RecordedCommand::ConditionWait {
                    sequence,
                    occurrence_id: recorded_occurrence_id,
                    condition_key,
                    predicate_identity,
                    timeout_seconds,
                    result: recorded_result,
                    parallel_group_path,
                    ..
                }) = state.recorded_commands.get(cursor)
                else {
                    break;
                };

                if cursor > state.command_cursor && recorded_occurrence_id != &occurrence_id {
                    break;
                }
                if let Err(error) = ensure_parallel_path_matches(
                    *sequence,
                    parallel_group_path.as_deref(),
                    &self.parallel_group_path,
                ) {
                    return Poll::Ready(Err(error));
                }
                if let Err(error) = validate_recorded_condition_wait(
                    *sequence,
                    recorded_occurrence_id,
                    condition_key.as_deref(),
                    predicate_identity,
                    *timeout_seconds,
                    &occurrence_id,
                    &options,
                ) {
                    return Poll::Ready(Err(error));
                }
                if result == Some(ConditionWaitResult::TimedOut) {
                    return Poll::Ready(Err(Error::NonDeterministicReplay(ReplayFailure::new(
                        "condition_wait_reopened_after_timeout",
                        Some(*sequence),
                        Some("timed-out condition is terminal".to_string()),
                        Some("another physical wait-open".to_string()),
                        "condition history reopened one logical wait after its durable timeout",
                    ))));
                }
                result = *recorded_result;
                cursor += 1;
            }
            state.command_cursor = cursor;
            result
        };

        if let Some(result) = recorded_result {
            return Poll::Ready(Ok(result));
        }

        self.poll_open_condition(options)
    }
}

impl ConditionWaitCall {
    fn poll_new_condition(
        self: Pin<&mut Self>,
        options: ValidatedConditionWaitOptions,
    ) -> Poll<Result<ConditionWaitResult>> {
        self.poll_open_condition(options)
    }

    fn poll_open_condition(
        mut self: Pin<&mut Self>,
        options: ValidatedConditionWaitOptions,
    ) -> Poll<Result<ConditionWaitResult>> {
        let selection_member = self
            .parallel_group_path
            .first()
            .is_some_and(|entry| entry.parallel_group_mode.as_deref() == Some("select"));
        match (self.predicate)() {
            Ok(true) if !selection_member => {
                return Poll::Ready(Ok(ConditionWaitResult::Satisfied))
            }
            Ok(_) => {}
            Err(error) => return Poll::Ready(Err(error)),
        }
        if options.timeout_seconds == Some(0) && !selection_member {
            return Poll::Ready(Ok(ConditionWaitResult::TimedOut));
        }

        let ctx = self.ctx.clone();
        let mut state = match ctx.state.lock() {
            Ok(state) => state,
            Err(_) => return Poll::Ready(Err(Error::WorkflowStatePoisoned)),
        };
        let mut command = serde_json::Map::from_iter([
            ("type".to_string(), json!("open_condition_wait")),
            (
                "condition_wait_occurrence_id".to_string(),
                json!(self.occurrence_id.as_deref().unwrap_or_default()),
            ),
            ("condition_key".to_string(), json!(options.condition_key)),
            (
                "condition_definition_fingerprint".to_string(),
                json!(options.predicate_identity),
            ),
        ]);
        if let Some(timeout_seconds) = options.timeout_seconds {
            command.insert("timeout_seconds".to_string(), json!(timeout_seconds));
        }
        apply_parallel_group_path(&mut command, &self.parallel_group_path);
        state.commands.push(Value::Object(command));
        drop(state);
        self.opened_wait = true;
        Poll::Pending
    }
}

fn validate_recorded_condition_wait(
    sequence: u64,
    recorded_occurrence_id: &str,
    recorded_key: Option<&str>,
    recorded_predicate_identity: &str,
    recorded_timeout_seconds: Option<u64>,
    current_occurrence_id: &str,
    current: &ValidatedConditionWaitOptions,
) -> Result<()> {
    if recorded_occurrence_id != current_occurrence_id {
        return Err(Error::NonDeterministicReplay(ReplayFailure::new(
            "condition_wait_occurrence_mismatch",
            Some(sequence),
            Some(recorded_occurrence_id.to_string()),
            Some(current_occurrence_id.to_string()),
            "recorded condition occurrence differs from the current authored wait position",
        )));
    }
    if recorded_key != Some(current.condition_key.as_str()) {
        return Err(Error::NonDeterministicReplay(ReplayFailure::new(
            "condition_wait_key_mismatch",
            Some(sequence),
            recorded_key.map(str::to_string),
            Some(current.condition_key.clone()),
            "recorded condition identity differs from the current workflow wait",
        )));
    }
    if recorded_predicate_identity != current.predicate_identity {
        return Err(Error::NonDeterministicReplay(ReplayFailure::new(
            "condition_wait_predicate_mismatch",
            Some(sequence),
            Some(recorded_predicate_identity.to_string()),
            Some(current.predicate_identity.clone()),
            "recorded condition predicate behavior differs from current workflow code",
        )));
    }
    if recorded_timeout_seconds != current.timeout_seconds {
        return Err(Error::NonDeterministicReplay(ReplayFailure::new(
            "condition_wait_timeout_mismatch",
            Some(sequence),
            recorded_timeout_seconds.map(|seconds| format!("{seconds}s")),
            current.timeout_seconds.map(|seconds| format!("{seconds}s")),
            "recorded condition timeout differs from the current workflow wait",
        )));
    }
    Ok(())
}

/// Future returned by [`WorkflowContext::start_child_workflow`].
pub struct ChildWorkflowCall {
    ctx: WorkflowContext,
    workflow_type: String,
    options: ChildWorkflowOptions,
    args: Option<Result<AvroValue>>,
    scheduled: bool,
    matched_pending: bool,
    parallel_group_path: Vec<ParallelGroupMetadata>,
}

impl ChildWorkflowCall {
    fn poll_avro_value(
        mut self: Pin<&mut Self>,
        _cx: &mut TaskContext<'_>,
    ) -> Poll<Result<ChildWorkflowAvroResult>> {
        if self.matched_pending {
            return Poll::Pending;
        }

        let ctx = self.ctx.clone();
        let mut state = match ctx.state.lock() {
            Ok(state) => state,
            Err(_) => return Poll::Ready(Err(Error::WorkflowStatePoisoned)),
        };

        if let Some(recorded) = state.recorded_commands.get(state.command_cursor).cloned() {
            let sequence = recorded.sequence();
            match recorded {
                RecordedCommand::ChildWorkflow {
                    workflow_type,
                    outcome,
                    parallel_group_path,
                    ..
                } => {
                    if let Err(error) = ensure_parallel_path_matches(
                        sequence,
                        parallel_group_path.as_deref(),
                        &self.parallel_group_path,
                    ) {
                        return Poll::Ready(Err(error));
                    }
                    if let Some(recorded_type) = workflow_type {
                        if recorded_type != self.workflow_type {
                            return Poll::Ready(Err(Error::NonDeterministicReplay(
                                ReplayFailure::new(
                                    "recorded_command_detail_mismatch",
                                    Some(sequence),
                                    Some(format!("child workflow:{recorded_type}")),
                                    Some(format!("child workflow:{}", self.workflow_type)),
                                    "recorded child workflow type differs from the current workflow command",
                                ),
                            )));
                        }
                    }
                    state.command_cursor += 1;
                    if let Some(outcome) = outcome {
                        return Poll::Ready(outcome.map_err(Error::ChildWorkflowFailed));
                    }
                    state.matched_recorded_pending = true;
                    self.scheduled = true;
                    self.matched_pending = true;
                    return Poll::Pending;
                }
                other => {
                    return Poll::Ready(Err(command_mismatch(
                        &other,
                        format!("child workflow:{}", self.workflow_type),
                    )));
                }
            }
        }

        if !self.scheduled {
            if self.options.task_queue.trim().is_empty() {
                return Poll::Ready(Err(Error::InvalidChildWorkflowOptions(
                    "task_queue must not be empty".to_string(),
                )));
            }
            for (name, value) in [
                (
                    "execution_timeout_seconds",
                    self.options.execution_timeout_seconds,
                ),
                ("run_timeout_seconds", self.options.run_timeout_seconds),
            ] {
                if value == Some(0) {
                    return Poll::Ready(Err(Error::InvalidChildWorkflowOptions(format!(
                        "{name} must be at least 1"
                    ))));
                }
            }

            let args = match self.args.take().unwrap_or(Ok(AvroValue::Null)) {
                Ok(args) => args,
                Err(error) => return Poll::Ready(Err(error)),
            };
            let arguments = match encode_typed_envelope(
                &normalize_avro_arguments(args),
                &state.payload_codec,
            ) {
                Ok(arguments) => arguments,
                Err(error) => return Poll::Ready(Err(error)),
            };
            let mut command = json!({
                "type": "start_child_workflow",
                "workflow_type": self.workflow_type,
                "queue": self.options.task_queue,
                "parent_close_policy": self.options.parent_close_policy.as_str(),
                "arguments": arguments,
            });
            let object = command
                .as_object_mut()
                .expect("child workflow command is always an object");
            if let Some(policy) = &self.options.retry_policy {
                let mut retry_policy = serde_json::Map::new();
                if let Some(max_attempts) = policy.max_attempts {
                    if max_attempts == 0 {
                        return Poll::Ready(Err(Error::InvalidChildWorkflowOptions(
                            "retry_policy.max_attempts must be at least 1".to_string(),
                        )));
                    }
                    retry_policy.insert("max_attempts".to_string(), json!(max_attempts));
                }
                if !policy.backoff_seconds.is_empty() {
                    retry_policy
                        .insert("backoff_seconds".to_string(), json!(policy.backoff_seconds));
                }
                if !policy.non_retryable_error_types.is_empty() {
                    retry_policy.insert(
                        "non_retryable_error_types".to_string(),
                        json!(policy.non_retryable_error_types),
                    );
                }
                if retry_policy.is_empty() {
                    return Poll::Ready(Err(Error::InvalidChildWorkflowOptions(
                        "retry_policy must configure at least one field".to_string(),
                    )));
                }
                object.insert("retry_policy".to_string(), Value::Object(retry_policy));
            }
            if let Some(seconds) = self.options.execution_timeout_seconds {
                object.insert("execution_timeout_seconds".to_string(), json!(seconds));
            }
            if let Some(seconds) = self.options.run_timeout_seconds {
                object.insert("run_timeout_seconds".to_string(), json!(seconds));
            }
            apply_parallel_group_path(object, &self.parallel_group_path);
            state.commands.push(command);
            self.scheduled = true;
        }

        Poll::Pending
    }
}

impl Future for ChildWorkflowCall {
    type Output = Result<ChildWorkflowResult>;

    fn poll(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
        match self.poll_avro_value(cx) {
            Poll::Ready(Ok(result)) => match result.result.into_json() {
                Ok(projected) => Poll::Ready(Ok(ChildWorkflowResult {
                    parent: result.parent,
                    child: result.child,
                    child_workflow_type: result.child_workflow_type,
                    result: projected,
                })),
                Err(error) => Poll::Ready(Err(error)),
            },
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => Poll::Pending,
        }
    }
}

fn command_mismatch(recorded: &RecordedCommand, actual: impl Into<String>) -> Error {
    Error::NonDeterministicReplay(ReplayFailure::new(
        "recorded_command_mismatch",
        Some(recorded.sequence()),
        Some(recorded.shape().to_string()),
        Some(actual.into()),
        "current workflow command does not match the recorded durable command sequence",
    ))
}

pub struct SignalCall {
    ctx: WorkflowContext,
    signal_name: String,
    runtime_reserved_allowed: bool,
    opened_wait: bool,
    matched_pending: bool,
    parallel_group_path: Vec<ParallelGroupMetadata>,
}

impl SignalCall {
    fn poll_avro_value(
        mut self: Pin<&mut Self>,
        _cx: &mut TaskContext<'_>,
    ) -> Poll<Result<Vec<AvroValue>>> {
        if self.matched_pending {
            return Poll::Pending;
        }
        if !self.runtime_reserved_allowed {
            if let Err(error) = validate_user_signal_name(&self.signal_name) {
                return Poll::Ready(Err(error));
            }
        }

        let ctx = self.ctx.clone();
        let mut state = match ctx.state.lock() {
            Ok(state) => state,
            Err(_) => return Poll::Ready(Err(Error::WorkflowStatePoisoned)),
        };

        if let Some(recorded) = state.recorded_commands.get(state.command_cursor).cloned() {
            match recorded {
                RecordedCommand::SignalWait {
                    sequence,
                    signal_name,
                    value,
                    parallel_group_path,
                } => {
                    if let Err(error) = ensure_parallel_path_matches(
                        sequence,
                        parallel_group_path.as_deref(),
                        &self.parallel_group_path,
                    ) {
                        return Poll::Ready(Err(error));
                    }
                    if signal_name != self.signal_name {
                        return Poll::Ready(Err(Error::NonDeterministicReplay(
                            ReplayFailure::new(
                                "recorded_command_detail_mismatch",
                                Some(sequence),
                                Some(format!("signal wait:{signal_name}")),
                                Some(format!("signal wait:{}", self.signal_name)),
                                "recorded signal name differs from the current workflow command",
                            ),
                        )));
                    }

                    state.command_cursor += 1;
                    if let Some(value) = value {
                        return Poll::Ready(Ok(value));
                    }
                    if state
                        .resume_signal
                        .as_ref()
                        .is_some_and(|signal| signal.signal_name == self.signal_name)
                    {
                        let signal = state
                            .resume_signal
                            .take()
                            .expect("matching resume signal is present");
                        return Poll::Ready(Ok(signal.arguments));
                    }

                    state.matched_recorded_pending = true;
                    self.opened_wait = true;
                    self.matched_pending = true;
                    return Poll::Pending;
                }
                other => {
                    return Poll::Ready(Err(command_mismatch(
                        &other,
                        format!("signal wait:{}", self.signal_name),
                    )));
                }
            }
        }

        if state
            .resume_signal
            .as_ref()
            .is_some_and(|signal| signal.signal_name == self.signal_name)
        {
            let signal = state
                .resume_signal
                .take()
                .expect("matching resume signal is present");
            return Poll::Ready(Ok(signal.arguments));
        }

        if !self.opened_wait {
            let mut command = serde_json::Map::from_iter([
                ("type".to_string(), json!("open_signal_wait")),
                ("signal_name".to_string(), json!(self.signal_name)),
            ]);
            apply_parallel_group_path(&mut command, &self.parallel_group_path);
            state.commands.push(Value::Object(command));
            self.opened_wait = true;
        }

        Poll::Pending
    }
}

impl Future for SignalCall {
    type Output = Result<Vec<Value>>;

    fn poll(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
        match self.poll_avro_value(cx) {
            Poll::Ready(Ok(values)) => Poll::Ready(
                values
                    .into_iter()
                    .map(AvroValue::into_json)
                    .collect::<Result<Vec<_>>>(),
            ),
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => Poll::Pending,
        }
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
                details,
            )
            .await
    }
}

fn decode_task_avro_arguments(value: Option<&Value>, codec: &str) -> Result<AvroValue> {
    validate_payload_codec(codec)?;
    match value {
        Some(value) => Ok(normalize_avro_arguments(decode_wire_avro_value(
            value, codec,
        )?)),
        None => Ok(AvroValue::Array(Vec::new())),
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
    let decoded = decode_task_avro_arguments(task.signal_arguments.as_ref(), &task.payload_codec)?;
    let AvroValue::Array(arguments) = decoded else {
        unreachable!("normalize_avro_arguments always returns an array");
    };

    Ok(Some(ResumeSignal {
        signal_name: signal_name.to_string(),
        arguments,
    }))
}

fn validate_workflow_task_payloads(task: &WorkflowTask) -> Result<()> {
    validate_payload_codec(&task.payload_codec)?;
    validate_optional_inbound_payload(task.arguments.as_ref(), &task.payload_codec)?;
    validate_optional_inbound_payload(task.signal_arguments.as_ref(), &task.payload_codec)?;
    for event in &task.history_events {
        validate_history_event_payloads(event, &task.payload_codec)?;
    }
    Ok(())
}

fn validate_activity_task_payloads(task: &ActivityTask) -> Result<()> {
    validate_payload_codec(&task.payload_codec)?;
    validate_optional_inbound_payload(task.arguments.as_ref(), &task.payload_codec)
}

fn validate_query_task_payloads(task: &QueryTask) -> Result<()> {
    validate_payload_codec(&task.payload_codec)?;
    validate_optional_inbound_payload(task.workflow_arguments.as_ref(), &task.payload_codec)?;
    validate_optional_inbound_payload(task.query_arguments.as_ref(), &task.payload_codec)?;
    for event in &task.history_events {
        validate_history_event_payloads(event, &task.payload_codec)?;
    }

    let Some(export) = task.history_export.as_ref() else {
        return Ok(());
    };
    let export_codec = match export.get("payloads") {
        Some(payloads) => declared_payload_codec(payloads, "codec")?,
        None => None,
    }
    .unwrap_or(&task.payload_codec);
    validate_payload_codec(export_codec)?;

    if let Some(events) = export.get("history_events").and_then(Value::as_array) {
        for event in events {
            let event_type = event
                .get("event_type")
                .or_else(|| event.get("type"))
                .and_then(Value::as_str)
                .unwrap_or_default();
            if let Some(payload) = event.get("payload") {
                validate_history_payloads(event_type, payload, export_codec)?;
            }
        }
    }
    for signal in export
        .get("signals")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let codec = declared_payload_codec(signal, "payload_codec")?.unwrap_or(export_codec);
        validate_payload_codec(codec)?;
        validate_optional_inbound_payload(signal.get("arguments"), codec)?;
    }
    for activity in export
        .get("activities")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let codec = declared_payload_codec(activity, "payload_codec")?.unwrap_or(export_codec);
        validate_payload_codec(codec)?;
        validate_optional_inbound_payload(activity.get("arguments"), codec)?;
        validate_optional_inbound_payload(activity.get("result"), codec)?;
    }
    Ok(())
}

fn validate_history_event_payloads(event: &HistoryEvent, fallback_codec: &str) -> Result<()> {
    validate_history_payloads(&event.event_type, &event.payload, fallback_codec)
}

fn validate_history_payloads(
    event_type: &str,
    payload: &Value,
    fallback_codec: &str,
) -> Result<()> {
    let codec = declared_payload_codec(payload, "payload_codec")?.unwrap_or(fallback_codec);
    validate_payload_codec(codec)?;
    for field in history_payload_fields(event_type) {
        validate_optional_inbound_payload(payload.get(*field), codec)?;
    }
    Ok(())
}

const SIGNAL_HISTORY_PAYLOAD_FIELDS: &[&str] = &["value", "input", "arguments"];

fn history_payload_fields(event_type: &str) -> &'static [&'static str] {
    match event_type {
        "ActivityCompleted" => &["result"],
        "SignalReceived" | "SignalApplied" => SIGNAL_HISTORY_PAYLOAD_FIELDS,
        "UpdateAccepted" | "UpdateRejected" | "UpdateApplied" => &["arguments"],
        "UpdateCompleted" | "SideEffectRecorded" => &["result"],
        "ChildRunCompleted" => &["result", "output"],
        "WorkflowCompleted" => &["output"],
        "ServiceCallStarted"
        | "ServiceCallCompleted"
        | "ServiceCallFailed"
        | "ServiceCallCancelled" => &["request_payload", "response_payload"],
        _ => &[],
    }
}

fn signal_history_payload(payload: &Value) -> Option<&Value> {
    SIGNAL_HISTORY_PAYLOAD_FIELDS
        .iter()
        .find_map(|field| payload.get(*field))
}

fn declared_payload_codec<'a>(value: &'a Value, field: &str) -> Result<Option<&'a str>> {
    match value.get(field) {
        None => Ok(None),
        Some(Value::String(codec)) => Ok(Some(codec)),
        Some(_) => Err(invalid_payload_envelope()),
    }
}

fn validate_optional_inbound_payload(value: Option<&Value>, codec: &str) -> Result<()> {
    validate_payload_codec(codec)?;
    if let Some(value) = value.filter(|value| !value.is_null()) {
        decode_wire_avro_value(value, codec)?;
    }
    Ok(())
}

fn recorded_parallel_group_entry(payload: &Value, sequence: u64) -> Result<ParallelGroupMetadata> {
    let group_id = payload_string(payload, "parallel_group_id").ok_or_else(|| {
        invalid_recorded_history(
            "parallel_group_metadata_invalid",
            sequence,
            "non-empty parallel_group_id",
            &payload.to_string(),
            "parallel-group history is missing its stable identity",
        )
    })?;
    let kind = payload_string(payload, "parallel_group_kind").ok_or_else(|| {
        invalid_recorded_history(
            "parallel_group_metadata_invalid",
            sequence,
            "activity, child, timer, signal, condition, or mixed group kind",
            &payload.to_string(),
            "parallel-group history is missing its group kind",
        )
    })?;
    if !matches!(
        kind.as_str(),
        "activity" | "child" | "timer" | "signal" | "condition" | "mixed"
    ) {
        return Err(invalid_recorded_history(
            "parallel_group_metadata_invalid",
            sequence,
            "activity, child, timer, signal, condition, or mixed group kind",
            &kind,
            "parallel-group history contains an unsupported group kind",
        ));
    }
    let base_sequence = payload
        .get("parallel_group_base_sequence")
        .and_then(value_as_u64)
        .filter(|value| *value > 0)
        .ok_or_else(|| {
            invalid_recorded_history(
                "parallel_group_metadata_invalid",
                sequence,
                "positive parallel_group_base_sequence",
                &payload.to_string(),
                "parallel-group history contains an invalid base sequence",
            )
        })?;
    let size = payload
        .get("parallel_group_size")
        .and_then(value_as_u64)
        .and_then(|value| usize::try_from(value).ok())
        .filter(|value| (1..=MAX_PARALLEL_OPERATIONS).contains(value))
        .ok_or_else(|| {
            invalid_recorded_history(
                "parallel_group_metadata_invalid",
                sequence,
                "bounded positive parallel_group_size",
                &payload.to_string(),
                "parallel-group history contains an invalid group size",
            )
        })?;
    let index = payload
        .get("parallel_group_index")
        .and_then(value_as_u64)
        .and_then(|value| usize::try_from(value).ok())
        .filter(|value| *value < size)
        .ok_or_else(|| {
            invalid_recorded_history(
                "parallel_group_metadata_invalid",
                sequence,
                "parallel_group_index within group bounds",
                &payload.to_string(),
                "parallel-group history contains an invalid member index",
            )
        })?;
    if base_sequence.checked_add(u64::try_from(index).unwrap_or(u64::MAX)) != Some(sequence) {
        return Err(invalid_recorded_history(
            "parallel_group_metadata_invalid",
            sequence,
            "base sequence plus member index equals workflow sequence",
            &payload.to_string(),
            "parallel-group path does not preserve durable workflow position",
        ));
    }
    let mode = payload
        .get("parallel_group_mode")
        .and_then(Value::as_str)
        .unwrap_or("all");
    if !matches!(mode, "all" | "select") {
        return Err(invalid_recorded_history(
            "parallel_group_metadata_invalid",
            sequence,
            "parallel group mode all or select",
            mode,
            "parallel-group history contains an unsupported group mode",
        ));
    }
    let expected_id = if mode == "select" {
        format!("select-calls:{base_sequence}:{size}")
    } else {
        format!("{}:{base_sequence}:{size}", parallel_group_prefix(&kind))
    };
    if group_id != expected_id {
        return Err(invalid_recorded_history(
            "parallel_group_metadata_invalid",
            sequence,
            &expected_id,
            &group_id,
            "parallel-group history contains an incompatible stable group ID",
        ));
    }
    let selection_member_key = if mode == "select" {
        Some(selection_key_from_value(
            payload.get("selection_member_key"),
            sequence,
        )?)
    } else {
        None
    };
    let selection_member_index = if mode == "select" {
        Some(required_parallel_usize(
            payload,
            "selection_member_index",
            sequence,
        )?)
    } else {
        None
    };
    let selection_member_base_sequence = if mode == "select" {
        Some(
            payload
                .get("selection_member_base_sequence")
                .and_then(value_as_u64)
                .filter(|value| *value >= base_sequence)
                .ok_or_else(|| {
                    invalid_recorded_history(
                        "parallel_group_metadata_invalid",
                        sequence,
                        "selection member base within its group",
                        &payload.to_string(),
                        "selection history contains an invalid member base sequence",
                    )
                })?,
        )
    } else {
        None
    };
    let selection_member_size = if mode == "select" {
        let member_size = required_parallel_usize(payload, "selection_member_size", sequence)?;
        if member_size == 0 {
            return Err(invalid_recorded_history(
                "parallel_group_metadata_invalid",
                sequence,
                "positive selection member size",
                &payload.to_string(),
                "selection history contains an invalid member size",
            ));
        }
        Some(member_size)
    } else {
        None
    };
    let selection_member_kind = if mode == "select" {
        let kind = payload_string(payload, "selection_member_kind").ok_or_else(|| {
            invalid_recorded_history(
                "parallel_group_metadata_invalid",
                sequence,
                "selection member operation kind",
                &payload.to_string(),
                "selection history is missing its authored member kind",
            )
        })?;
        if !matches!(
            kind.as_str(),
            "activity" | "child" | "timer" | "signal" | "condition" | "group"
        ) {
            return Err(invalid_recorded_history(
                "parallel_group_metadata_invalid",
                sequence,
                "activity, child, timer, signal, condition, or group selection member kind",
                &kind,
                "selection history contains an unsupported member kind",
            ));
        }
        Some(kind)
    } else {
        None
    };
    if let (Some(member_base), Some(member_size)) =
        (selection_member_base_sequence, selection_member_size)
    {
        let member_end = member_base
            .checked_add(u64::try_from(member_size).unwrap_or(u64::MAX))
            .ok_or_else(|| {
                invalid_recorded_history(
                    "parallel_group_metadata_invalid",
                    sequence,
                    "bounded selection member range",
                    &payload.to_string(),
                    "selection member range overflowed",
                )
            })?;
        let group_end = base_sequence
            .checked_add(u64::try_from(size).unwrap_or(u64::MAX))
            .unwrap_or(u64::MAX);
        if sequence < member_base || sequence >= member_end || member_end > group_end {
            return Err(invalid_recorded_history(
                "parallel_group_metadata_invalid",
                sequence,
                "workflow sequence within one bounded selection member",
                &payload.to_string(),
                "selection member range does not contain its durable leaf",
            ));
        }
    }
    Ok(ParallelGroupMetadata {
        parallel_group_id: group_id,
        parallel_group_kind: kind,
        parallel_group_base_sequence: base_sequence,
        parallel_group_size: size,
        parallel_group_index: index,
        parallel_group_mode: (mode == "select").then(|| "select".to_string()),
        selection_member_key,
        selection_member_index,
        selection_member_base_sequence,
        selection_member_size,
        selection_member_kind,
    })
}

fn required_parallel_usize(payload: &Value, field: &str, sequence: u64) -> Result<usize> {
    payload
        .get(field)
        .and_then(value_as_u64)
        .and_then(|value| usize::try_from(value).ok())
        .ok_or_else(|| {
            invalid_recorded_history(
                "parallel_group_metadata_invalid",
                sequence,
                &format!("non-negative integer {field}"),
                &payload.to_string(),
                "selection history contains invalid member metadata",
            )
        })
}

fn selection_key_from_value(value: Option<&Value>, sequence: u64) -> Result<SelectionKey> {
    match value {
        Some(Value::String(value)) if !value.is_empty() => Ok(SelectionKey::Name(value.clone())),
        Some(value) => value_as_u64(value)
            .and_then(|value| usize::try_from(value).ok())
            .map(SelectionKey::Index)
            .ok_or_else(|| {
                invalid_recorded_history(
                    "selection_member_key_invalid",
                    sequence,
                    "non-empty string or non-negative integer member key",
                    &value.to_string(),
                    "selection history contains an invalid member key",
                )
            }),
        None => Err(invalid_recorded_history(
            "selection_member_key_missing",
            sequence,
            "selection_member_key",
            "<missing>",
            "selection history is missing its stable member key",
        )),
    }
}

fn recorded_parallel_group_path(
    events: &[&HistoryEvent],
    sequence: u64,
) -> Result<Option<Vec<ParallelGroupMetadata>>> {
    let mut recorded: Option<Vec<ParallelGroupMetadata>> = None;
    for event in events {
        let payload = &event.payload;
        let has_metadata = payload.get("parallel_group_path").is_some()
            || payload.get("parallel_group_id").is_some()
            || payload.get("parallel_group_kind").is_some()
            || payload.get("parallel_group_base_sequence").is_some()
            || payload.get("parallel_group_size").is_some()
            || payload.get("parallel_group_index").is_some()
            || payload.get("parallel_group_mode").is_some()
            || payload.get("selection_member_key").is_some();
        if !has_metadata {
            continue;
        }

        let top_level = recorded_parallel_group_entry(payload, sequence)?;
        let path = match payload.get("parallel_group_path") {
            None => vec![top_level.clone()],
            Some(Value::Array(entries)) if !entries.is_empty() => entries
                .iter()
                .map(|entry| recorded_parallel_group_entry(entry, sequence))
                .collect::<Result<Vec<_>>>()?,
            Some(value) => {
                return Err(invalid_recorded_history(
                    "parallel_group_metadata_invalid",
                    sequence,
                    "non-empty parallel_group_path list",
                    &value.to_string(),
                    "parallel-group history contains an invalid group path",
                ));
            }
        };
        if path.last() != Some(&top_level) {
            return Err(invalid_recorded_history(
                "parallel_group_metadata_invalid",
                sequence,
                &serde_json::to_string(&path.last()).unwrap_or_default(),
                &serde_json::to_string(&top_level).unwrap_or_default(),
                "parallel-group top-level fields do not match the innermost path entry",
            ));
        }
        if recorded.as_ref().is_some_and(|existing| existing != &path) {
            return Err(invalid_recorded_history(
                "parallel_group_history_conflict",
                sequence,
                &serde_json::to_string(&recorded.as_ref()).unwrap_or_default(),
                &serde_json::to_string(&path).unwrap_or_default(),
                "parallel-group metadata changed between scheduling and resolution history",
            ));
        }
        recorded = Some(path);
    }
    Ok(recorded)
}

fn recorded_commands(
    events: &[HistoryEvent],
    fallback_codec: &str,
    parent: WorkflowIdentity,
) -> Result<Vec<RecordedCommand>> {
    let mut events_by_sequence: BTreeMap<u64, Vec<&HistoryEvent>> = BTreeMap::new();
    let mut last_new_sequence = None;

    for event in events {
        let is_activity = matches!(
            event.event_type.as_str(),
            "ActivityScheduled"
                | "ActivityStarted"
                | "ActivityHeartbeatRecorded"
                | "ActivityRetryScheduled"
                | "ActivityCompleted"
                | "ActivityFailed"
                | "ActivityCancelled"
                | "ActivityTimedOut"
        );
        let is_workflow_timer = matches!(
            event.event_type.as_str(),
            "TimerScheduled" | "TimerCancelled" | "TimerFired"
        ) && !is_internal_timer_event(event);
        let is_child_workflow = matches!(
            event.event_type.as_str(),
            "ChildWorkflowScheduled"
                | "ChildRunCompleted"
                | "ChildRunFailed"
                | "ChildRunCancelled"
                | "ChildRunTerminated"
        );
        let is_signal_wait = is_recorded_signal_wait_event(event);
        let is_condition_wait = is_recorded_condition_wait_event(event);
        let is_search_attributes = event.event_type == "SearchAttributesUpserted";
        let is_side_effect = event.event_type == "SideEffectRecorded";
        let is_version_marker = event.event_type == "VersionMarkerRecorded";
        let is_memo = event.event_type == "MemoUpserted";
        if !is_activity
            && !is_workflow_timer
            && !is_child_workflow
            && !is_signal_wait
            && !is_condition_wait
            && !is_search_attributes
            && !is_side_effect
            && !is_version_marker
            && !is_memo
        {
            continue;
        }

        let sequence = durable_event_sequence(event).ok_or_else(|| {
            Error::NonDeterministicReplay(ReplayFailure::new(
                "durable_command_sequence_missing",
                None,
                Some("positive workflow sequence".to_string()),
                Some(event.event_type.clone()),
                "durable command history event has no workflow sequence",
            ))
        })?;
        if sequence == 0 {
            return Err(Error::NonDeterministicReplay(ReplayFailure::new(
                "durable_command_sequence_invalid",
                Some(sequence),
                Some("positive workflow sequence".to_string()),
                Some(sequence.to_string()),
                "durable command history uses an invalid workflow sequence",
            )));
        }
        if !events_by_sequence.contains_key(&sequence) {
            if let Some(previous) = last_new_sequence {
                if sequence < previous {
                    return Err(invalid_recorded_history(
                        "durable_command_sequence_mismatch",
                        sequence,
                        &format!("workflow sequence greater than {previous}"),
                        &sequence.to_string(),
                        "durable commands are not strictly ordered by their recorded workflow sequence",
                    ));
                }
            }
            last_new_sequence = Some(sequence);
        }
        events_by_sequence.entry(sequence).or_default().push(event);
    }

    let commands: Vec<RecordedCommand> = events_by_sequence
        .into_iter()
        .map(|(sequence, sequence_events)| {
            let activity_events: Vec<_> = sequence_events
                .iter()
                .copied()
                .filter(|event| event.event_type.starts_with("Activity"))
                .collect();
            let timer_events: Vec<_> = sequence_events
                .iter()
                .copied()
                .filter(|event| event.event_type.starts_with("Timer"))
                .collect();
            let child_events: Vec<_> = sequence_events
                .iter()
                .copied()
                .filter(|event| {
                    event.event_type == "ChildWorkflowScheduled"
                        || event.event_type.starts_with("ChildRun")
                })
                .collect();
            let signal_wait_events: Vec<_> = sequence_events
                .iter()
                .copied()
                .filter(|event| is_recorded_signal_wait_event(event))
                .collect();
            let condition_wait_events: Vec<_> = sequence_events
                .iter()
                .copied()
                .filter(|event| is_recorded_condition_wait_event(event))
                .collect();
            let search_attribute_events: Vec<_> = sequence_events
                .iter()
                .copied()
                .filter(|event| event.event_type == "SearchAttributesUpserted")
                .collect();
            let side_effect_events: Vec<_> = sequence_events
                .iter()
                .copied()
                .filter(|event| event.event_type == "SideEffectRecorded")
                .collect();
            let version_marker_events: Vec<_> = sequence_events
                .iter()
                .copied()
                .filter(|event| event.event_type == "VersionMarkerRecorded")
                .collect();
            let memo_events: Vec<_> = sequence_events
                .iter()
                .copied()
                .filter(|event| event.event_type == "MemoUpserted")
                .collect();

            let command_kind_count = usize::from(!activity_events.is_empty())
                + usize::from(!timer_events.is_empty())
                + usize::from(!child_events.is_empty())
                + usize::from(!signal_wait_events.is_empty())
                + usize::from(!condition_wait_events.is_empty())
                + usize::from(!search_attribute_events.is_empty())
                + usize::from(!side_effect_events.is_empty())
                + usize::from(!version_marker_events.is_empty())
                + usize::from(!memo_events.is_empty());
            if command_kind_count > 1 {
                let actual = [
                    (!activity_events.is_empty()).then_some("activity"),
                    (!timer_events.is_empty()).then_some("timer"),
                    (!child_events.is_empty()).then_some("child workflow"),
                    (!signal_wait_events.is_empty()).then_some("signal wait"),
                    (!condition_wait_events.is_empty()).then_some("condition wait"),
                    (!search_attribute_events.is_empty()).then_some("search-attribute update"),
                    (!side_effect_events.is_empty()).then_some("side effect"),
                    (!version_marker_events.is_empty()).then_some("version marker"),
                    (!memo_events.is_empty()).then_some("memo upsert"),
                ]
                .into_iter()
                .flatten()
                .collect::<Vec<_>>()
                .join(" and ");
                return Err(invalid_recorded_history(
                    "durable_command_sequence_collision",
                    sequence,
                    "one durable command kind",
                    &actual,
                    "one workflow sequence records more than one durable command kind",
                ));
            }

            if !activity_events.is_empty() {
                let parallel_group_path =
                    recorded_parallel_group_path(&activity_events, sequence)?;
                let scheduled_count = activity_events
                    .iter()
                    .filter(|event| event.event_type == "ActivityScheduled")
                    .count();
                if scheduled_count > 1 {
                    return Err(invalid_recorded_history(
                        "duplicate_activity_schedule",
                        sequence,
                        "at most one ActivityScheduled event",
                        "multiple ActivityScheduled events",
                        "activity history schedules more than one command at one workflow sequence",
                    ));
                }
                let activity_type = activity_events.iter().find_map(|event| {
                    event
                        .payload
                        .get("activity_type")
                        .or_else(|| event.payload.get("activity_name"))
                        .and_then(Value::as_str)
                        .map(str::to_string)
                });
                if activity_events.iter().filter_map(|event| {
                    event
                        .payload
                        .get("activity_type")
                        .or_else(|| event.payload.get("activity_name"))
                        .and_then(Value::as_str)
                }).any(|candidate| Some(candidate) != activity_type.as_deref()) {
                    return Err(invalid_recorded_history(
                        "activity_identity_mismatch",
                        sequence,
                        activity_type.as_deref().unwrap_or("one activity identity"),
                        "conflicting activity identities",
                        "activity lifecycle events at one workflow sequence disagree on identity",
                    ));
                }
                let terminal: Vec<_> = activity_events
                    .iter()
                    .copied()
                    .filter(|event| {
                        matches!(
                            event.event_type.as_str(),
                            "ActivityCompleted"
                                | "ActivityFailed"
                                | "ActivityCancelled"
                                | "ActivityTimedOut"
                        )
                    })
                    .collect();
                let duplicate_delivery = terminal.first().is_some_and(|first| {
                    terminal.iter().all(|event| {
                        event.event_type == first.event_type && event.payload == first.payload
                    })
                });
                if terminal.len() > 1 && !duplicate_delivery {
                    return Err(invalid_recorded_history(
                        "duplicate_activity_terminal_event",
                        sequence,
                        "at most one terminal activity event",
                        "multiple terminal activity events",
                        "activity history settles one command more than once",
                    ));
                }
                let outcome = terminal
                    .first()
                    .map(|event| activity_outcome(event, fallback_codec, activity_type.clone()))
                    .transpose()?;
                let options = activity_events
                    .iter()
                    .find(|event| event.event_type == "ActivityScheduled")
                    .and_then(|event| event.payload.get("activity"))
                    .and_then(Value::as_object)
                    .map(|activity| RecordedActivityOptions {
                        task_queue: recorded_optional_string(activity, "queue"),
                        execution_mode: recorded_optional_string(activity, "execution_mode"),
                        retry_policy: recorded_activity_retry_snapshot(
                            activity.get("retry_policy"),
                        ),
                    });
                return Ok(RecordedCommand::Activity {
                    sequence,
                    activity_type,
                    options,
                    outcome,
                    parallel_group_path,
                });
            }

            if !child_events.is_empty() {
                let parallel_group_path = recorded_parallel_group_path(&child_events, sequence)?;
                let scheduled: Vec<_> = child_events
                    .iter()
                    .copied()
                    .filter(|event| event.event_type == "ChildWorkflowScheduled")
                    .collect();
                if scheduled.len() != 1 {
                    return Err(invalid_recorded_history(
                        "child_workflow_schedule_missing_or_duplicate",
                        sequence,
                        "one ChildWorkflowScheduled event",
                        &format!("{} ChildWorkflowScheduled events", scheduled.len()),
                        "child workflow replay requires exactly one recorded schedule event",
                    ));
                }
                let workflow_type = child_events.iter().find_map(|event| {
                    event
                        .payload
                        .get("child_workflow_type")
                        .or_else(|| event.payload.get("workflow_type"))
                        .and_then(Value::as_str)
                        .filter(|value| !value.is_empty())
                        .map(str::to_string)
                });
                if child_events
                    .iter()
                    .filter_map(|event| {
                        event
                            .payload
                            .get("child_workflow_type")
                            .or_else(|| event.payload.get("workflow_type"))
                            .and_then(Value::as_str)
                    })
                    .any(|candidate| Some(candidate) != workflow_type.as_deref())
                {
                    return Err(invalid_recorded_history(
                        "child_workflow_identity_mismatch",
                        sequence,
                        workflow_type
                            .as_deref()
                            .unwrap_or("one child workflow type"),
                        "conflicting child workflow types",
                        "child workflow lifecycle events at one sequence disagree on type",
                    ));
                }
                let mut outcomes = child_workflow_outcomes(
                    &child_events.iter().map(|event| (*event).clone()).collect::<Vec<_>>(),
                    fallback_codec,
                    parent.clone(),
                )?;
                let terminal_events = child_events
                    .iter()
                    .copied()
                    .filter(|event| event.event_type.starts_with("ChildRun"))
                    .collect::<Vec<_>>();
                let duplicate_delivery = terminal_events.first().is_some_and(|first| {
                    terminal_events.iter().all(|event| {
                        event.event_type == first.event_type && event.payload == first.payload
                    })
                });
                if outcomes.len() > 1 && !duplicate_delivery {
                    return Err(invalid_recorded_history(
                        "duplicate_child_workflow_terminal_event",
                        sequence,
                        "at most one terminal child event",
                        "multiple terminal child events",
                        "child workflow history settles one command more than once",
                    ));
                }
                return Ok(RecordedCommand::ChildWorkflow {
                    sequence,
                    workflow_type,
                    outcome: outcomes.pop(),
                    parallel_group_path,
                });
            }

            if !signal_wait_events.is_empty() {
                let opened: Vec<_> = signal_wait_events
                    .iter()
                    .copied()
                    .filter(|event| event.event_type == "SignalWaitOpened")
                    .collect();
                if opened.len() != 1 {
                    return Err(invalid_recorded_history(
                        "signal_wait_open_missing_or_duplicate",
                        sequence,
                        "one SignalWaitOpened event",
                        &format!("{} SignalWaitOpened events", opened.len()),
                        "signal replay requires exactly one canonical wait-open event",
                    ));
                }

                let applied: Vec<_> = signal_wait_events
                    .iter()
                    .copied()
                    .filter(|event| event.event_type == "SignalApplied")
                    .collect();
                if applied.len() > 1 {
                    return Err(invalid_recorded_history(
                        "duplicate_signal_wait_apply",
                        sequence,
                        "at most one SignalApplied event",
                        "multiple SignalApplied events",
                        "signal history applies one durable wait more than once",
                    ));
                }

                let signal_names = signal_wait_events
                    .iter()
                    .map(|event| required_signal_wait_name(event, sequence))
                    .collect::<Result<Vec<_>>>()?;
                let signal_name = signal_names
                    .first()
                    .expect("signal wait events are not empty")
                    .clone();
                if signal_names.iter().any(|candidate| candidate != &signal_name) {
                    return Err(invalid_recorded_history(
                        "signal_wait_identity_mismatch",
                        sequence,
                        &signal_name,
                        "conflicting signal names",
                        "signal wait lifecycle events at one workflow sequence disagree on identity",
                    ));
                }
                let value = applied
                    .first()
                    .map(|event| decode_signal_event_arguments(event, fallback_codec))
                    .transpose()?;
                return Ok(RecordedCommand::SignalWait {
                    sequence,
                    signal_name,
                    value,
                    parallel_group_path: recorded_parallel_group_path(
                        &signal_wait_events,
                        sequence,
                    )?,
                });
            }

            if !condition_wait_events.is_empty() {
                return recorded_condition_wait(
                    sequence,
                    &condition_wait_events,
                    events,
                );
            }

            if !search_attribute_events.is_empty() {
                if search_attribute_events.len() != 1 {
                    return Err(invalid_recorded_history(
                        "duplicate_search_attribute_update",
                        sequence,
                        "one SearchAttributesUpserted event",
                        &format!(
                            "{} SearchAttributesUpserted events",
                            search_attribute_events.len()
                        ),
                        "search-attribute history records one workflow command more than once",
                    ));
                }
                let payload = &search_attribute_events[0].payload;
                let attributes = payload
                    .get("attributes")
                    .filter(|value| value.as_object().is_some_and(|values| !values.is_empty()))
                    .cloned()
                    .ok_or_else(|| {
                        invalid_recorded_history(
                            "search_attribute_update_missing",
                            sequence,
                            "non-empty attributes object",
                            "missing or invalid attributes",
                            "search-attribute history is missing its recorded mutation",
                        )
                    })?;
                let attribute_types =
                    recorded_search_attribute_types(payload, &attributes, sequence)?;
                return Ok(RecordedCommand::SearchAttributes {
                    sequence,
                    attributes,
                    attribute_types,
                });
            }

            if !side_effect_events.is_empty() {
                if side_effect_events.len() != 1 {
                    return Err(invalid_recorded_history(
                        "duplicate_side_effect_record",
                        sequence,
                        "one SideEffectRecorded event",
                        &format!("{} SideEffectRecorded events", side_effect_events.len()),
                        "side-effect history records one workflow command more than once",
                    ));
                }
                let event = side_effect_events[0];
                let result = event.payload.get("result").ok_or_else(|| {
                    invalid_recorded_history(
                        "side_effect_result_missing",
                        sequence,
                        "recorded result payload",
                        "missing result",
                        "side-effect history is missing its recorded value",
                    )
                })?;
                let has_published_envelope = result.as_str().is_some()
                    || result.as_object().is_some_and(|envelope| {
                        envelope.get("codec").and_then(Value::as_str).is_some()
                            && envelope.get("blob").and_then(Value::as_str).is_some()
                    });
                if !has_published_envelope {
                    return Err(invalid_recorded_history(
                        "side_effect_payload_malformed",
                        sequence,
                        "payload blob or {codec, blob} envelope",
                        &result.to_string(),
                        "side-effect history result does not use a published payload envelope",
                    ));
                }
                let codec = event
                    .payload
                    .get("payload_codec")
                    .and_then(Value::as_str)
                    .unwrap_or(fallback_codec);
                let value = decode_wire_avro_value(result, codec).map_err(|error| {
                    if error.to_string().contains("unsupported_payload_codec") {
                        return error;
                    }

                    invalid_recorded_history(
                        "side_effect_payload_incompatible",
                        sequence,
                        &format!("valid {codec} payload envelope"),
                        &error.to_string(),
                        "side-effect history payload cannot be decoded with its recorded codec",
                    )
                })?;
                return Ok(RecordedCommand::SideEffect { sequence, value });
            }

            if !version_marker_events.is_empty() {
                if version_marker_events.len() != 1 {
                    return Err(invalid_recorded_history(
                        "duplicate_version_marker_record",
                        sequence,
                        "one VersionMarkerRecorded event",
                        &format!("{} VersionMarkerRecorded events", version_marker_events.len()),
                        "version-marker history records one workflow command more than once",
                    ));
                }
                let payload = &version_marker_events[0].payload;
                let change_id = payload
                    .get("change_id")
                    .and_then(Value::as_str)
                    .filter(|value| !value.is_empty())
                    .map(str::to_string)
                    .ok_or_else(|| {
                        invalid_recorded_history(
                            "version_marker_field_missing",
                            sequence,
                            "non-empty change_id",
                            "missing or invalid change_id",
                            "version-marker history is missing its stable change ID",
                        )
                    })?;
                let version = required_version_i32(payload, "version", sequence)?;
                let min_supported = required_version_i32(payload, "min_supported", sequence)?;
                let max_supported = required_version_i32(payload, "max_supported", sequence)?;
                if min_supported > max_supported || version < min_supported || version > max_supported {
                    return Err(invalid_recorded_history(
                        "version_marker_history_range_invalid",
                        sequence,
                        "min_supported <= version <= max_supported",
                        &format!("{min_supported} <= {version} <= {max_supported}"),
                        "recorded version marker contains an internally incompatible range",
                    ));
                }
                return Ok(RecordedCommand::VersionMarker {
                    sequence,
                    change_id,
                    version,
                });
            }

            if !memo_events.is_empty() {
                if memo_events.len() != 1 {
                    return Err(invalid_recorded_history(
                        "duplicate_memo_upsert_record",
                        sequence,
                        "one MemoUpserted event",
                        &format!("{} MemoUpserted events", memo_events.len()),
                        "memo history records one workflow update more than once",
                    ));
                }
                let payload = &memo_events[0].payload;
                let entries = payload.get("entries").cloned().ok_or_else(|| {
                    invalid_recorded_history(
                        "memo_entries_missing",
                        sequence,
                        "memo entries object",
                        "missing entries",
                        "MemoUpserted history is missing replay identity entries",
                    )
                })?;
                let entries = decode_memo_history_map(&entries, true).map_err(|error| {
                    invalid_recorded_history(
                        "memo_entries_invalid",
                        sequence,
                        "valid canonical memo entries",
                        &error.to_string(),
                        "MemoUpserted history contains invalid replay identity entries",
                    )
                })?;
                let merged = payload.get("merged").cloned().ok_or_else(|| {
                    invalid_recorded_history(
                        "memo_merged_projection_missing",
                        sequence,
                        "merged memo projection",
                        "missing merged",
                        "MemoUpserted history is missing its merged projection",
                    )
                })?;
                decode_memo_history_map(&merged, false).map_err(|error| {
                    invalid_recorded_history(
                        "memo_merged_projection_invalid",
                        sequence,
                        "valid merged memo projection",
                        &error.to_string(),
                        "MemoUpserted history contains an invalid merged projection",
                    )
                })?;

                return Ok(RecordedCommand::Memo { sequence, entries });
            }
            let scheduled: Vec<_> = timer_events
                .iter()
                .copied()
                .filter(|event| event.event_type == "TimerScheduled")
                .collect();
            let fired: Vec<_> = timer_events
                .iter()
                .copied()
                .filter(|event| event.event_type == "TimerFired")
                .collect();
            if scheduled.len() != 1 {
                return Err(invalid_recorded_history(
                    "timer_schedule_missing_or_duplicate",
                    sequence,
                    "one TimerScheduled event",
                    &format!("{} TimerScheduled events", scheduled.len()),
                    "timer replay requires exactly one recorded schedule event",
                ));
            }
            if fired.len() > 1 {
                return Err(invalid_recorded_history(
                    "duplicate_timer_fire",
                    sequence,
                    "at most one TimerFired event",
                    "multiple TimerFired events",
                    "timer history contains more than one fire event for a workflow sequence",
                ));
            }

            let scheduled = scheduled[0];
            let timer_id = required_history_string(scheduled, "timer_id", sequence)?;
            let delay_seconds = required_history_u64(scheduled, "delay_seconds", sequence)?;
            if let Some(fired) = fired.first() {
                let fired_timer_id = required_history_string(fired, "timer_id", sequence)?;
                if fired_timer_id != timer_id {
                    return Err(invalid_recorded_history(
                        "timer_identity_mismatch",
                        sequence,
                        &timer_id,
                        &fired_timer_id,
                        "TimerFired does not correspond to the recorded TimerScheduled event",
                    ));
                }
                let fired_delay = required_history_u64(fired, "delay_seconds", sequence)?;
                if fired_delay != delay_seconds {
                    return Err(invalid_recorded_history(
                        "timer_history_delay_mismatch",
                        sequence,
                        &delay_seconds.to_string(),
                        &fired_delay.to_string(),
                        "TimerScheduled and TimerFired record different delays",
                    ));
                }
            }

            Ok(RecordedCommand::Timer {
                sequence,
                delay_seconds,
                fired: !fired.is_empty(),
                parallel_group_path: recorded_parallel_group_path(&timer_events, sequence)?,
            })
        })
        .collect::<Result<_>>()?;

    let mut marker_sequences = HashMap::new();
    for command in &commands {
        if let RecordedCommand::VersionMarker {
            sequence,
            change_id,
            ..
        } = command
        {
            if let Some(first_sequence) = marker_sequences.insert(change_id.clone(), *sequence) {
                return Err(invalid_recorded_history(
                    "duplicate_version_marker",
                    *sequence,
                    &format!("one marker for change ID {change_id:?}"),
                    &format!("markers at sequences {first_sequence} and {sequence}"),
                    "workflow history contains duplicate markers for one stable change ID",
                ));
            }
        }
    }

    Ok(commands)
}

fn required_version_i32(payload: &Value, field: &str, sequence: u64) -> Result<i32> {
    payload
        .get(field)
        .and_then(Value::as_i64)
        .and_then(|value| i32::try_from(value).ok())
        .ok_or_else(|| {
            invalid_recorded_history(
                "version_marker_field_missing",
                sequence,
                &format!("integer {field}"),
                "missing or out-of-range integer",
                "version-marker history is missing a required integer field",
            )
        })
}

fn durable_event_sequence(event: &HistoryEvent) -> Option<u64> {
    event
        .payload
        .get("sequence")
        .or_else(|| event.payload.get("workflow_sequence"))
        .or_else(|| event.raw.get("sequence"))
        .or_else(|| event.raw.get("workflow_sequence"))
        .and_then(value_as_u64)
}

fn is_internal_timer_event(event: &HistoryEvent) -> bool {
    matches!(
        event
            .payload
            .get("timer_kind")
            .or_else(|| event.raw.get("timer_kind"))
            .and_then(Value::as_str),
        Some("condition_timeout" | "signal_timeout")
    )
}

fn is_recorded_condition_wait_event(event: &HistoryEvent) -> bool {
    matches!(
        event.event_type.as_str(),
        "ConditionWaitOpened" | "ConditionWaitSatisfied" | "ConditionWaitTimedOut"
    )
}

fn recorded_condition_wait(
    sequence: u64,
    condition_events: &[&HistoryEvent],
    all_events: &[HistoryEvent],
) -> Result<RecordedCommand> {
    let opened = condition_events
        .iter()
        .copied()
        .filter(|event| event.event_type == "ConditionWaitOpened")
        .collect::<Vec<_>>();
    if opened.len() != 1 {
        return Err(invalid_recorded_history(
            "condition_wait_open_missing_or_duplicate",
            sequence,
            "one ConditionWaitOpened event",
            &format!("{} ConditionWaitOpened events", opened.len()),
            "condition replay requires exactly one canonical wait-open event",
        ));
    }
    let terminal = condition_events
        .iter()
        .copied()
        .filter(|event| {
            matches!(
                event.event_type.as_str(),
                "ConditionWaitSatisfied" | "ConditionWaitTimedOut"
            )
        })
        .collect::<Vec<_>>();
    if terminal.len() > 1 {
        return Err(invalid_recorded_history(
            "duplicate_condition_wait_terminal_event",
            sequence,
            "at most one condition terminal event",
            "multiple condition terminal events",
            "condition history settles one durable wait more than once",
        ));
    }

    let opened = opened[0];
    let condition_wait_id = required_condition_wait_id(opened, sequence)?;
    let occurrence_id = required_condition_wait_occurrence_id(opened, sequence)?;
    for event in condition_events
        .iter()
        .copied()
        .filter(|event| !std::ptr::eq(*event, opened))
    {
        let event_wait_id = required_condition_wait_id(event, sequence)?;
        if event_wait_id != condition_wait_id {
            return Err(invalid_recorded_history(
                "condition_wait_id_mismatch",
                sequence,
                &condition_wait_id,
                &event_wait_id,
                "condition lifecycle events at one sequence disagree on wait identity",
            ));
        }
        let event_occurrence_id = required_condition_wait_occurrence_id(event, sequence)?;
        if event_occurrence_id != occurrence_id {
            return Err(invalid_recorded_history(
                "condition_wait_occurrence_history_mismatch",
                sequence,
                &occurrence_id,
                &event_occurrence_id,
                "condition lifecycle events at one sequence disagree on authored occurrence identity",
            ));
        }
    }

    let condition_key = optional_non_empty_history_string(opened, "condition_key");
    let predicate_identity = opened
        .payload
        .get("condition_definition_fingerprint")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .ok_or_else(|| {
            invalid_recorded_history(
                "condition_wait_predicate_fingerprint_missing",
                sequence,
                "non-empty condition_definition_fingerprint",
                &opened.event_type,
                "canonical condition history is missing its predicate identity",
            )
        })?;
    let timeout_seconds = optional_history_u64(opened, "timeout_seconds", sequence)?;
    for event in condition_events
        .iter()
        .copied()
        .filter(|event| !std::ptr::eq(*event, opened))
    {
        for (field, opened_value) in [
            ("condition_key", condition_key.as_deref()),
            (
                "condition_definition_fingerprint",
                Some(predicate_identity.as_str()),
            ),
        ] {
            if let Some(value) = optional_non_empty_history_string(event, field) {
                if opened_value.is_some_and(|opened_value| opened_value != value) {
                    return Err(invalid_recorded_history(
                        "condition_wait_definition_history_mismatch",
                        sequence,
                        opened_value.unwrap_or_default(),
                        &value,
                        "condition lifecycle events disagree on the recorded definition",
                    ));
                }
            }
        }
        if let Some(event_timeout) = optional_history_u64(event, "timeout_seconds", sequence)? {
            if timeout_seconds.is_some_and(|opened_timeout| opened_timeout != event_timeout) {
                return Err(invalid_recorded_history(
                    "condition_wait_definition_history_mismatch",
                    sequence,
                    &format!("{}s", timeout_seconds.unwrap_or_default()),
                    &format!("{event_timeout}s"),
                    "condition lifecycle events disagree on the recorded timeout",
                ));
            }
        }
    }

    let timeout_timer_events = all_events
        .iter()
        .filter(|event| {
            matches!(
                event.event_type.as_str(),
                "TimerScheduled" | "TimerCancelled" | "TimerFired"
            ) && event.payload.get("timer_kind").and_then(Value::as_str)
                == Some("condition_timeout")
                && event
                    .payload
                    .get("condition_wait_id")
                    .and_then(Value::as_str)
                    == Some(condition_wait_id.as_str())
        })
        .collect::<Vec<_>>();
    let scheduled = timeout_timer_events
        .iter()
        .copied()
        .filter(|event| event.event_type == "TimerScheduled")
        .collect::<Vec<_>>();
    let fired = timeout_timer_events
        .iter()
        .copied()
        .filter(|event| event.event_type == "TimerFired")
        .collect::<Vec<_>>();
    if scheduled.len() > 1 || fired.len() > 1 || (!fired.is_empty() && scheduled.len() != 1) {
        return Err(invalid_recorded_history(
            "condition_wait_timeout_history_invalid",
            sequence,
            "one timeout schedule and at most one fire",
            &format!("{} schedules and {} fires", scheduled.len(), fired.len()),
            "condition timeout history has a missing or duplicate lifecycle event",
        ));
    }
    if let Some(scheduled) = scheduled.first() {
        let timer_id = required_history_string(scheduled, "timer_id", sequence)?;
        let delay_seconds = required_history_u64(scheduled, "delay_seconds", sequence)?;
        if timeout_seconds.is_some_and(|timeout| timeout != delay_seconds) {
            return Err(invalid_recorded_history(
                "condition_wait_timeout_delay_mismatch",
                sequence,
                &format!("{}s", timeout_seconds.unwrap_or_default()),
                &format!("{delay_seconds}s"),
                "condition timeout timer differs from the wait definition",
            ));
        }
        if let Some(fired) = fired.first() {
            let fired_timer_id = required_history_string(fired, "timer_id", sequence)?;
            let fired_delay = required_history_u64(fired, "delay_seconds", sequence)?;
            if fired_timer_id != timer_id || fired_delay != delay_seconds {
                return Err(invalid_recorded_history(
                    "condition_wait_timeout_identity_mismatch",
                    sequence,
                    &format!("{timer_id}:{delay_seconds}s"),
                    &format!("{fired_timer_id}:{fired_delay}s"),
                    "condition timeout fire does not match its durable schedule",
                ));
            }
        }
    }

    let result = terminal.first().map(|event| {
        if event.event_type == "ConditionWaitTimedOut" {
            ConditionWaitResult::TimedOut
        } else {
            ConditionWaitResult::Satisfied
        }
    });
    let result = if !fired.is_empty() {
        if result == Some(ConditionWaitResult::Satisfied) {
            return Err(invalid_recorded_history(
                "condition_wait_terminal_conflict",
                sequence,
                "one satisfied or timed-out outcome",
                "satisfied event and fired timeout",
                "condition history records conflicting terminal outcomes",
            ));
        }
        Some(ConditionWaitResult::TimedOut)
    } else {
        result
    };

    Ok(RecordedCommand::ConditionWait {
        sequence,
        occurrence_id,
        condition_key,
        predicate_identity,
        timeout_seconds,
        result,
        parallel_group_path: recorded_parallel_group_path(condition_events, sequence)?,
    })
}

fn required_condition_wait_occurrence_id(event: &HistoryEvent, sequence: u64) -> Result<String> {
    event
        .payload
        .get("condition_wait_occurrence_id")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .ok_or_else(|| {
            invalid_recorded_history(
                "condition_wait_occurrence_id_missing",
                sequence,
                "non-empty condition_wait_occurrence_id",
                &event.event_type,
                "condition history is missing authored occurrence identity",
            )
        })
}

fn required_condition_wait_id(event: &HistoryEvent, sequence: u64) -> Result<String> {
    event
        .payload
        .get("condition_wait_id")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .ok_or_else(|| {
            invalid_recorded_history(
                "condition_wait_id_missing",
                sequence,
                "non-empty condition_wait_id",
                &event.event_type,
                "canonical condition history is missing its durable wait identity",
            )
        })
}

fn optional_non_empty_history_string(event: &HistoryEvent, field: &str) -> Option<String> {
    event
        .payload
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn optional_history_u64(event: &HistoryEvent, field: &str, sequence: u64) -> Result<Option<u64>> {
    match event.payload.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => value_as_u64(value).map(Some).ok_or_else(|| {
            invalid_recorded_history(
                "condition_wait_definition_invalid",
                sequence,
                &format!("non-negative integer {field}"),
                &value.to_string(),
                "condition history contains an invalid numeric definition field",
            )
        }),
    }
}

fn required_signal_wait_name(event: &HistoryEvent, sequence: u64) -> Result<String> {
    event
        .payload
        .get("signal_name")
        .or_else(|| event.raw.get("signal_name"))
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .ok_or_else(|| {
            invalid_recorded_history(
                "signal_wait_name_missing",
                sequence,
                "non-empty signal_name",
                &event.event_type,
                "canonical signal-wait history is missing its signal identity",
            )
        })
}

fn is_recorded_signal_wait_event(event: &HistoryEvent) -> bool {
    matches!(
        event.event_type.as_str(),
        "SignalWaitOpened" | "SignalApplied"
    )
}

fn required_history_string(event: &HistoryEvent, field: &str, sequence: u64) -> Result<String> {
    event
        .payload
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .ok_or_else(|| {
            invalid_recorded_history(
                "timer_history_field_missing",
                sequence,
                field,
                &event.event_type,
                "timer history is missing a required identity field",
            )
        })
}

fn required_history_u64(event: &HistoryEvent, field: &str, sequence: u64) -> Result<u64> {
    event
        .payload
        .get(field)
        .and_then(value_as_u64)
        .ok_or_else(|| {
            invalid_recorded_history(
                "timer_history_field_missing",
                sequence,
                field,
                &event.event_type,
                "timer history is missing a required numeric field",
            )
        })
}

fn recorded_search_attribute_types(
    payload: &Value,
    attributes: &Value,
    sequence: u64,
) -> Result<RecordedSnapshotValue<BTreeMap<String, String>>> {
    let Some(raw_types) = payload.get("attribute_types") else {
        // This is the explicit compatibility rule for histories recorded
        // before typed identity was persisted. Values still constrain replay;
        // the unknown type snapshot does not assert a typed match.
        return Ok(RecordedSnapshotValue::Unknown);
    };
    let Some(raw_types) = raw_types.as_object() else {
        return Err(invalid_recorded_history(
            "search_attribute_types_malformed",
            sequence,
            "canonical attribute type map",
            &raw_types.to_string(),
            "search-attribute history contains malformed type identity",
        ));
    };
    let attribute_keys = attributes
        .as_object()
        .expect("recorded search attributes were validated as an object");
    let mut types = BTreeMap::new();
    for (key, value) in raw_types {
        let Some(attribute_type) = value.as_str() else {
            return Err(invalid_recorded_history(
                "search_attribute_types_malformed",
                sequence,
                "canonical string type name",
                &value.to_string(),
                "search-attribute history contains a non-string type identity",
            ));
        };
        if !attribute_keys.contains_key(key)
            || !matches!(
                attribute_type,
                "string" | "keyword" | "keyword_list" | "int" | "float" | "bool" | "datetime"
            )
        {
            return Err(invalid_recorded_history(
                "search_attribute_types_malformed",
                sequence,
                "canonical types for keys present in attributes",
                &format!("{key}:{attribute_type}"),
                "search-attribute history contains unsupported or orphaned type identity",
            ));
        }
        types.insert(key.clone(), attribute_type.to_string());
    }
    Ok(RecordedSnapshotValue::Known(types))
}

fn invalid_recorded_history(
    reason: &str,
    sequence: u64,
    expected: &str,
    actual: &str,
    message: &str,
) -> Error {
    Error::NonDeterministicReplay(ReplayFailure::new(
        reason,
        Some(sequence),
        Some(expected.to_string()),
        Some(actual.to_string()),
        message,
    ))
}

type ActivityOutcome = std::result::Result<AvroValue, ActivityFailure>;

fn activity_outcome(
    event: &HistoryEvent,
    fallback_codec: &str,
    recorded_activity_type: Option<String>,
) -> Result<ActivityOutcome> {
    if event.event_type == "ActivityCompleted" {
        let codec = event
            .payload
            .get("payload_codec")
            .and_then(Value::as_str)
            .unwrap_or(fallback_codec);
        return Ok(Ok(decode_wire_avro_value(
            event.payload.get("result").unwrap_or(&Value::Null),
            codec,
        )?));
    }

    let payload = &event.payload;
    let (kind, fallback_reason, fallback_message) = match event.event_type.as_str() {
        "ActivityFailed" => (ActivityFailureKind::Failed, "activity", "activity failed"),
        "ActivityCancelled" => (
            ActivityFailureKind::Cancelled,
            "cancelled",
            "activity was cancelled",
        ),
        "ActivityTimedOut" => (
            ActivityFailureKind::TimedOut,
            "timeout",
            "activity timed out",
        ),
        _ => unreachable!("activity_outcome is called only for terminal activity events"),
    };
    let exception = payload
        .get("exception")
        .filter(|value| !value.is_null())
        .cloned();
    let failure_category = payload_string(payload, "failure_category");
    let timeout_kind = payload_string(payload, "timeout_kind");
    let reason = payload_string(payload, "reason").unwrap_or_else(|| match kind {
        ActivityFailureKind::Failed => failure_category
            .clone()
            .unwrap_or_else(|| fallback_reason.to_string()),
        ActivityFailureKind::Cancelled => fallback_reason.to_string(),
        ActivityFailureKind::TimedOut => timeout_kind
            .clone()
            .unwrap_or_else(|| fallback_reason.to_string()),
    });
    let message = payload_string(payload, "message")
        .or_else(|| {
            exception
                .as_ref()
                .and_then(|value| payload_string(value, "message"))
        })
        .unwrap_or_else(|| fallback_message.to_string());

    Ok(Err(ActivityFailure {
        kind,
        reason,
        message,
        activity_execution_id: payload_string(payload, "activity_execution_id"),
        activity_attempt_id: payload_string(payload, "activity_attempt_id"),
        activity_type: payload_string(payload, "activity_type")
            .or_else(|| payload_string(payload, "activity_name"))
            .or(recorded_activity_type),
        activity_class: payload_string(payload, "activity_class"),
        attempt_number: payload.get("attempt_number").and_then(value_as_u64),
        failure_id: payload_string(payload, "failure_id"),
        failure_category,
        timeout_kind,
        non_retryable: payload
            .get("non_retryable")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        exception_type: payload_string(payload, "exception_type").or_else(|| {
            exception
                .as_ref()
                .and_then(|value| payload_string(value, "type"))
        }),
        exception_class: payload_string(payload, "exception_class").or_else(|| {
            exception
                .as_ref()
                .and_then(|value| payload_string(value, "class"))
        }),
        code: payload
            .get("code")
            .filter(|value| !value.is_null())
            .cloned(),
        exception,
    }))
}

type ChildWorkflowOutcome = std::result::Result<ChildWorkflowAvroResult, ChildWorkflowFailure>;

fn child_workflow_outcomes(
    events: &[HistoryEvent],
    fallback_codec: &str,
    parent: WorkflowIdentity,
) -> Result<Vec<ChildWorkflowOutcome>> {
    let mut outcomes = Vec::new();

    for event in events {
        let kind = match event.event_type.as_str() {
            "ChildRunCompleted" => None,
            "ChildRunFailed" => Some((
                ChildWorkflowFailureKind::Failed,
                "child_workflow",
                "child workflow failed",
            )),
            "ChildRunCancelled" => Some((
                ChildWorkflowFailureKind::Cancelled,
                "cancelled",
                "child workflow was cancelled",
            )),
            "ChildRunTerminated" => Some((
                ChildWorkflowFailureKind::Terminated,
                "terminated",
                "child workflow was terminated",
            )),
            _ => continue,
        };
        let payload = &event.payload;
        let child_workflow_id = payload_string(payload, "child_workflow_instance_id");
        let child_workflow_run_id = payload_string(payload, "child_workflow_run_id");
        let child_workflow_type = payload_string(payload, "child_workflow_type");

        if let Some((kind, reason, fallback_message)) = kind {
            let exception = payload
                .get("exception")
                .filter(|value| !value.is_null())
                .cloned();
            let message = payload_string(payload, "message")
                .or_else(|| {
                    exception
                        .as_ref()
                        .and_then(|value| payload_string(value, "message"))
                })
                .unwrap_or_else(|| fallback_message.to_string());
            let exception_type = payload_string(payload, "exception_type").or_else(|| {
                exception
                    .as_ref()
                    .and_then(|value| payload_string(value, "type"))
            });
            let exception_class = payload_string(payload, "exception_class").or_else(|| {
                exception
                    .as_ref()
                    .and_then(|value| payload_string(value, "class"))
            });
            outcomes.push(Err(ChildWorkflowFailure {
                kind,
                reason: reason.to_string(),
                message,
                parent_workflow_id: parent.workflow_id.clone(),
                parent_workflow_run_id: parent.run_id.clone(),
                child_workflow_id,
                child_workflow_run_id,
                child_workflow_type,
                failure_id: payload_string(payload, "failure_id"),
                failure_category: payload_string(payload, "failure_category"),
                exception_type,
                exception_class,
                non_retryable: payload
                    .get("non_retryable")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                code: payload
                    .get("code")
                    .filter(|value| !value.is_null())
                    .cloned(),
                exception,
            }));
            continue;
        }

        let codec = payload
            .get("payload_codec")
            .and_then(Value::as_str)
            .unwrap_or(fallback_codec);
        let result = payload
            .get("result")
            .or_else(|| payload.get("output"))
            .unwrap_or(&Value::Null);
        outcomes.push(Ok(ChildWorkflowAvroResult {
            parent: parent.clone(),
            child: WorkflowIdentity {
                workflow_id: child_workflow_id,
                run_id: child_workflow_run_id,
            },
            child_workflow_type,
            result: decode_wire_avro_value(result, codec)?,
        }));
    }

    Ok(outcomes)
}

fn payload_string(payload: &Value, key: &str) -> Option<String> {
    payload
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn workflow_failure_command(error: &Error) -> Value {
    let (exception_type, exception_class, properties) = match error {
        Error::ActivityFailed(failure) => (
            match failure.kind {
                ActivityFailureKind::Failed => "ActivityFailed",
                ActivityFailureKind::Cancelled => "ActivityCancelled",
                ActivityFailureKind::TimedOut => "ActivityTimedOut",
            },
            "durable_workflow::ActivityFailure",
            json!({
                "reason": failure.reason,
                "activity_execution_id": failure.activity_execution_id,
                "activity_attempt_id": failure.activity_attempt_id,
                "activity_type": failure.activity_type,
                "activity_class": failure.activity_class,
                "attempt_number": failure.attempt_number,
                "failure_id": failure.failure_id,
                "failure_category": failure.failure_category,
                "timeout_kind": failure.timeout_kind,
                "activity_non_retryable": failure.non_retryable,
                "activity_exception_type": failure.exception_type,
                "activity_exception_class": failure.exception_class,
                "activity_code": failure.code,
                "activity_exception": failure.exception,
            }),
        ),
        Error::ChildWorkflowFailed(failure) => (
            match failure.kind {
                ChildWorkflowFailureKind::Failed => "ChildWorkflowFailed",
                ChildWorkflowFailureKind::Cancelled => "ChildWorkflowCancelled",
                ChildWorkflowFailureKind::Terminated => "ChildWorkflowTerminated",
            },
            "durable_workflow::ChildWorkflowFailure",
            json!({
                "reason": failure.reason,
                "parent_workflow_id": failure.parent_workflow_id,
                "parent_workflow_run_id": failure.parent_workflow_run_id,
                "child_workflow_id": failure.child_workflow_id,
                "child_workflow_run_id": failure.child_workflow_run_id,
                "child_workflow_type": failure.child_workflow_type,
                "failure_id": failure.failure_id,
                "failure_category": failure.failure_category,
                "child_exception_type": failure.exception_type,
                "child_exception_class": failure.exception_class,
                "child_non_retryable": failure.non_retryable,
                "child_code": failure.code,
                "child_exception": failure.exception,
            }),
        ),
        Error::ParallelFailed(failure) => (
            "ParallelFailed",
            "durable_workflow::ParallelFailure",
            json!({
                "parallel_group_id": failure.group_id,
                "parallel_member_path": failure.member_path,
                "parallel_group_path": failure.group_path,
                "completed_members": failure.completed.iter().map(|completion| &completion.member_path).collect::<Vec<_>>(),
                "cause_type": workflow_error_type(&failure.cause),
                "cause_message": failure.cause.to_string(),
            }),
        ),
        Error::SagaCompensationFailed(failure) => (
            "SagaCompensationFailed",
            "durable_workflow::SagaCompensationFailure",
            json!({
                "initiating_failure_type": workflow_error_type(&failure.initiating_failure),
                "initiating_failure_message": failure.initiating_failure.to_string(),
                "compensation_activity_type": failure.compensation_activity_type,
                "compensation_registration_order": failure.compensation_registration_order,
                "compensation_failure_type": workflow_error_type(&failure.compensation_failure),
                "compensation_failure_message": failure.compensation_failure.to_string(),
            }),
        ),
        Error::WorkflowCancellationRequested(_) => (
            "WorkflowCancellationRequested",
            "durable_workflow::WorkflowCancellationRequested",
            json!({"reason": "cancelled"}),
        ),
        Error::NonDeterministicReplay(_) => (
            "NonDeterministicReplay",
            "durable_workflow::Error",
            Value::Null,
        ),
        _ => ("RustWorkflowError", "durable_workflow::Error", Value::Null),
    };
    let non_retryable = match error {
        Error::ActivityFailed(failure) => failure.non_retryable,
        Error::ChildWorkflowFailed(failure) => failure.non_retryable,
        Error::ParallelFailed(failure) => workflow_error_non_retryable(&failure.cause),
        Error::SagaCompensationFailed(failure) => {
            workflow_error_non_retryable(&failure.compensation_failure)
        }
        Error::WorkflowCancellationRequested(_) => true,
        Error::NonDeterministicReplay(_) => true,
        _ => false,
    };

    json!({
        "type": "fail_workflow",
        "message": error.to_string(),
        "exception_type": exception_type,
        "exception_class": exception_class,
        "non_retryable": non_retryable,
        "exception": {
            "type": exception_type,
            "class": exception_class,
            "message": error.to_string(),
            "properties": properties,
        }
    })
}

fn workflow_error_type(error: &Error) -> &'static str {
    match error {
        Error::ActivityFailed(failure) => match failure.kind {
            ActivityFailureKind::Failed => "ActivityFailed",
            ActivityFailureKind::Cancelled => "ActivityCancelled",
            ActivityFailureKind::TimedOut => "ActivityTimedOut",
        },
        Error::ChildWorkflowFailed(failure) => match failure.kind {
            ChildWorkflowFailureKind::Failed => "ChildWorkflowFailed",
            ChildWorkflowFailureKind::Cancelled => "ChildWorkflowCancelled",
            ChildWorkflowFailureKind::Terminated => "ChildWorkflowTerminated",
        },
        Error::ParallelFailed(_) => "ParallelFailed",
        Error::SagaCompensationFailed(_) => "SagaCompensationFailed",
        Error::WorkflowCancellationRequested(_) => "WorkflowCancellationRequested",
        Error::NonDeterministicReplay(_) => "NonDeterministicReplay",
        _ => "RustWorkflowError",
    }
}

fn workflow_error_non_retryable(error: &Error) -> bool {
    match error {
        Error::ActivityFailed(failure) => failure.non_retryable,
        Error::ChildWorkflowFailed(failure) => failure.non_retryable,
        Error::ParallelFailed(failure) => workflow_error_non_retryable(&failure.cause),
        Error::SagaCompensationFailed(failure) => {
            workflow_error_non_retryable(&failure.compensation_failure)
        }
        Error::WorkflowCancellationRequested(_) | Error::NonDeterministicReplay(_) => true,
        _ => false,
    }
}

fn workflow_task_integrity_error(error: &Error) -> bool {
    matches!(
        error,
        Error::NonDeterministicReplay(_)
            | Error::Protocol(_)
            | Error::MissingWorkflowCommandIdentity
            | Error::WorkflowStatePoisoned
    )
}

fn decode_signal_event_arguments(
    event: &HistoryEvent,
    fallback_codec: &str,
) -> Result<Vec<AvroValue>> {
    let codec = declared_payload_codec(&event.payload, "payload_codec")?.unwrap_or(fallback_codec);
    validate_payload_codec(codec)?;
    let raw = signal_history_payload(&event.payload);
    let decoded = match raw.filter(|value| !value.is_null()) {
        Some(value) => decode_wire_avro_value(value, codec)?,
        None => AvroValue::Array(Vec::new()),
    };
    let AvroValue::Array(arguments) = normalize_avro_arguments(decoded) else {
        unreachable!("normalize_avro_arguments always returns an array");
    };
    Ok(arguments)
}

fn decode_update_event_arguments(
    event: &HistoryEvent,
    fallback_codec: &str,
) -> Result<Vec<AvroValue>> {
    let codec = declared_payload_codec(&event.payload, "payload_codec")?.unwrap_or(fallback_codec);
    validate_payload_codec(codec)?;
    let decoded = match event
        .payload
        .get("arguments")
        .filter(|value| !value.is_null())
    {
        Some(value) => decode_wire_avro_value(value, codec)?,
        None => AvroValue::Array(Vec::new()),
    };
    let AvroValue::Array(arguments) = normalize_avro_arguments(decoded) else {
        unreachable!("normalize_avro_arguments always returns an array");
    };
    Ok(arguments)
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
        let raw_arguments = signal_history_payload(&event.payload)
            .filter(|value| !value.is_null())
            .or_else(|| matched_export.and_then(|signal| signal.get("arguments")));
        let (arguments, avro_arguments) = decode_query_signal_arguments(raw_arguments, codec)?;
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
            avro_arguments,
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
            let (arguments, avro_arguments) =
                decode_query_signal_arguments(signal.get("arguments"), codec)?;
            signals.push(QuerySignal {
                id: signal.get("id").and_then(Value::as_str).map(str::to_string),
                name: name.to_string(),
                arguments,
                avro_arguments,
                workflow_sequence: signal.get("workflow_sequence").and_then(value_as_u64),
            });
        }
        signals.sort_by_key(|signal| signal.workflow_sequence.unwrap_or(u64::MAX));
    }

    Ok(signals)
}

fn decode_query_signal_arguments(
    raw: Option<&Value>,
    codec: &str,
) -> Result<(Vec<Value>, Vec<AvroValue>)> {
    validate_payload_codec(codec)?;
    let decoded = match raw.filter(|value| !value.is_null()) {
        Some(value) => decode_wire_avro_value(value, codec)?,
        None => AvroValue::Array(Vec::new()),
    };
    let AvroValue::Array(avro_arguments) = normalize_avro_arguments(decoded) else {
        unreachable!("normalize_avro_arguments always returns an array");
    };
    let arguments = avro_arguments
        .iter()
        .cloned()
        .map(AvroValue::into_json)
        .collect::<Result<Vec<_>>>()?;
    Ok((arguments, avro_arguments))
}

fn value_as_u64(value: &Value) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs,
        io::{Read, Write},
        net::{SocketAddr, TcpListener, TcpStream},
        process::Command as ProcessCommand,
        sync::atomic::AtomicUsize,
        thread,
    };

    #[derive(Clone, Copy, Debug)]
    enum InvalidTaskPayloadCodec {
        Missing,
        Null,
        NonString,
    }

    impl InvalidTaskPayloadCodec {
        fn label(self) -> &'static str {
            match self {
                Self::Missing => "missing",
                Self::Null => "null",
                Self::NonString => "non-string",
            }
        }

        fn apply(self, task: &mut Value) {
            let task = task.as_object_mut().expect("task fixture object");
            match self {
                Self::Missing => {
                    task.remove("payload_codec");
                }
                Self::Null => {
                    task.insert("payload_codec".to_string(), Value::Null);
                }
                Self::NonString => {
                    task.insert("payload_codec".to_string(), json!(42));
                }
            }
        }
    }

    fn fixture_envelope(value: Value) -> Value {
        encode_value_envelope(&value, DEFAULT_CODEC).expect("encode Avro test fixture")
    }

    fn fixture_blob(value: Value) -> String {
        encode_payload(&value, DEFAULT_CODEC)
            .expect("encode Avro test fixture")
            .blob
    }

    #[test]
    fn client_builder_rejects_the_sdk_owned_api_suffix() {
        for base_url in [
            "http://127.0.0.1:8080/api",
            "http://localhost:8080/api/",
            "https://runtime.example.test/namespaces/orders/api",
        ] {
            let error = Client::builder(base_url)
                .build()
                .expect_err("SDK-owned /api suffix must be rejected during build");

            assert!(matches!(error, Error::InvalidBaseUrl), "{base_url}");
            assert!(
                error.to_string().contains("SDK appends /api automatically"),
                "the validation error must explain how to fix the endpoint"
            );
        }
    }

    #[test]
    fn client_builder_preserves_self_hosted_and_managed_runtime_prefixes() {
        for (base_url, expected) in [
            ("http://127.0.0.1:8080", "http://127.0.0.1:8080"),
            (
                "http://localhost:8080/durable-workflow/",
                "http://localhost:8080/durable-workflow",
            ),
            (
                "https://runtime.example.test/namespaces/orders",
                "https://runtime.example.test/namespaces/orders",
            ),
            (
                "https://runtime.example.test/gateway/api/namespaces/orders",
                "https://runtime.example.test/gateway/api/namespaces/orders",
            ),
            (
                "https://api.example.test/runtime/orders/",
                "https://api.example.test/runtime/orders",
            ),
        ] {
            let client = Client::builder(base_url)
                .build()
                .expect("Server and Cloud runtime base URL must remain valid");

            assert_eq!(client.base_url, expected);
        }
    }

    #[test]
    fn workflow_completion_uses_the_additive_command_protocol_floor() {
        assert_eq!(
            workflow_completion_protocol_version(&[json!({"type": "complete_workflow"})]),
            WORKER_PROTOCOL_VERSION
        );
        assert_eq!(
            workflow_completion_protocol_version(&[json!({
                "type": "upsert_search_attributes",
                "attributes": {"OrderStatus": "waiting"},
            })]),
            SEARCH_ATTRIBUTE_UPDATE_MINIMUM_WORKER_PROTOCOL_VERSION
        );
        assert_eq!(
            workflow_completion_protocol_version(&[json!({
                "type": "upsert_search_attributes",
                "attributes": {"OrderStatus": "waiting"},
                "attribute_types": {"OrderStatus": "keyword"},
            })]),
            TYPED_SEARCH_ATTRIBUTES_MINIMUM_WORKER_PROTOCOL_VERSION
        );
        assert_eq!(
            workflow_completion_protocol_version(&[
                json!({"type": "upsert_memo", "entries": {"status": "waiting"}}),
                json!({"type": "open_condition_wait", "condition_key": "ready"}),
            ]),
            MEMO_UPSERT_MINIMUM_WORKER_PROTOCOL_VERSION
        );
        assert_eq!(
            workflow_completion_protocol_version(&[
                json!({"type": "upsert_search_attributes", "attributes": {"State": "waiting"}}),
                json!({"type": "open_condition_wait", "condition_key": "ready"}),
            ]),
            CONDITION_WAIT_MINIMUM_WORKER_PROTOCOL_VERSION
        );
        assert_eq!(
            workflow_completion_protocol_version(&[json!({
                "type": "open_condition_wait",
                "condition_wait_occurrence_id": "rust:condition-wait:0",
                "condition_key": "ready",
            })]),
            CONDITION_WAIT_OCCURRENCE_IDENTITY_MINIMUM_WORKER_PROTOCOL_VERSION
        );
        assert_eq!(
            workflow_completion_protocol_version_with_message_streams(
                &[json!({"type": "upsert_memo", "entries": {"status": "waiting"}})],
                true,
            ),
            MESSAGE_STREAMS_MINIMUM_WORKER_PROTOCOL_VERSION
        );
        assert_eq!(
            workflow_completion_protocol_version_with_message_streams(
                &[json!({
                    "type": "open_condition_wait",
                    "condition_wait_occurrence_id": "rust:condition-wait:0",
                    "condition_key": "ready",
                })],
                true,
            ),
            CONDITION_WAIT_OCCURRENCE_IDENTITY_MINIMUM_WORKER_PROTOCOL_VERSION
        );
    }

    #[test]
    fn portable_worker_affinity_manifest_explicitly_refuses_unimplemented_features() {
        let manifest = portable_worker_affinity_capability_manifest();

        for capability in ["local_activities", "worker_sessions", "sticky_execution"] {
            assert_eq!(manifest[capability]["supported"], json!(false));
            assert_eq!(
                manifest[capability]["minimum_protocol_version"],
                json!(PORTABLE_WORKER_AFFINITY_MINIMUM_PROTOCOL_VERSION)
            );
            assert!(manifest[capability]["reason"]
                .as_str()
                .is_some_and(|reason| !reason.is_empty()));
        }
    }

    fn typed_fidelity_probe() -> AvroValue {
        AvroValue::Map(BTreeMap::from([
            ("bytes".to_string(), AvroValue::Bytes(vec![0, 0xff])),
            ("empty".to_string(), AvroValue::Map(BTreeMap::new())),
            (
                "numeric".to_string(),
                AvroValue::Map(BTreeMap::from([
                    ("0".to_string(), AvroValue::String("zero".to_string())),
                    ("1".to_string(), AvroValue::String("one".to_string())),
                ])),
            ),
            (
                "nested".to_string(),
                AvroValue::Array(vec![AvroValue::Map(BTreeMap::from([(
                    "enabled".to_string(),
                    AvroValue::Boolean(true),
                )]))]),
            ),
            (
                "projection_collisions".to_string(),
                AvroValue::Array(projection_collision_probe()),
            ),
        ]))
    }

    fn projection_collision_probe() -> Vec<AvroValue> {
        vec![
            AvroValue::Map(BTreeMap::from([
                ("$type".to_string(), AvroValue::String("bytes".to_string())),
                (
                    "base64".to_string(),
                    AvroValue::String("ordinary user text".to_string()),
                ),
            ])),
            AvroValue::Map(BTreeMap::from([
                ("$type".to_string(), AvroValue::String("map".to_string())),
                (
                    "entries".to_string(),
                    AvroValue::Array(vec![AvroValue::Map(BTreeMap::from([
                        ("key".to_string(), AvroValue::String("ordinary".to_string())),
                        (
                            "value".to_string(),
                            AvroValue::String("user map".to_string()),
                        ),
                    ]))]),
                ),
            ])),
        ]
    }

    #[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
    struct TypedContract {
        nested: TypedNested,
        mode: TypedMode,
        optional: Option<String>,
        absent: Option<String>,
        items: Vec<i64>,
        labels: BTreeMap<String, String>,
        bytes: serde_bytes::ByteBuf,
        signed: i64,
        finite: f64,
    }

    #[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
    struct TypedNested {
        enabled: bool,
    }

    #[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
    enum TypedMode {
        Detailed { label: String },
    }

    fn typed_contract() -> TypedContract {
        TypedContract {
            nested: TypedNested { enabled: true },
            mode: TypedMode::Detailed {
                label: "compiler-checked".to_string(),
            },
            optional: Some("present".to_string()),
            absent: None,
            items: vec![i64::MIN, 0, i64::MAX],
            labels: BTreeMap::from([
                ("language".to_string(), "rust".to_string()),
                ("wire".to_string(), "avro".to_string()),
            ]),
            bytes: serde_bytes::ByteBuf::from(vec![0, 0xff, 7]),
            signed: -9_223_372_036_854_775_000,
            finite: 12.5,
        }
    }

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
        let arguments = fixture_envelope(json!([]));
        serde_json::from_value(json!({
            "query_task_id": format!("query-{query_name}"),
            "workflow_type": "replay-counter",
            "query_name": query_name,
            "payload_codec": DEFAULT_CODEC,
            "workflow_arguments": arguments.clone(),
            "query_arguments": arguments,
            "history_events": history_events,
            "run_status": run_status,
        }))
        .expect("query task")
    }

    fn workflow_context(history: Vec<HistoryEvent>) -> WorkflowContext {
        workflow_context_with_codec(history, DEFAULT_CODEC)
    }

    fn workflow_context_with_codec(
        history: Vec<HistoryEvent>,
        payload_codec: &str,
    ) -> WorkflowContext {
        WorkflowContext {
            state: Arc::new(Mutex::new(
                WorkflowState::new_with_identity(
                    history,
                    None,
                    None,
                    "rust-workers".to_string(),
                    payload_codec.to_string(),
                    None,
                )
                .expect("valid workflow history"),
            )),
        }
    }

    fn history_event(event_type: &str, payload: Value) -> HistoryEvent {
        HistoryEvent {
            event_type: event_type.to_string(),
            payload,
            raw: HashMap::new(),
        }
    }

    fn parallel_path_entry(
        kind: &str,
        base: u64,
        size: usize,
        index: usize,
    ) -> ParallelGroupMetadata {
        parallel_group_entry(base, size, index, kind)
    }

    fn parallel_history_event(
        event_type: &str,
        sequence: u64,
        identity_field: &str,
        identity: &str,
        path: Vec<ParallelGroupMetadata>,
        result: Option<Value>,
    ) -> HistoryEvent {
        let mut payload = serde_json::Map::from_iter([
            ("sequence".to_string(), json!(sequence)),
            (identity_field.to_string(), json!(identity)),
        ]);
        let inner = path.last().expect("parallel history path");
        apply_parallel_group_path(&mut payload, std::slice::from_ref(inner));
        payload.insert("parallel_group_path".to_string(), json!(path));
        if let Some(result) = result {
            let field = if event_type == "ChildRunCompleted" {
                "result"
            } else {
                "result"
            };
            payload.insert(field.to_string(), fixture_envelope(result));
            payload.insert("payload_codec".to_string(), json!(DEFAULT_CODEC));
        }
        history_event(event_type, Value::Object(payload))
    }

    fn nested_parallel_operations() -> Vec<ParallelOperation> {
        vec![
            ParallelOperation::activity("first", json!([])),
            ParallelOperation::group(vec![
                ParallelOperation::child_workflow(
                    "second",
                    ChildWorkflowOptions::new("child-workers"),
                    json!([]),
                ),
                ParallelOperation::activity("third", json!([])),
            ]),
        ]
    }

    fn nested_parallel_paths() -> [Vec<ParallelGroupMetadata>; 3] {
        let outer = [
            parallel_path_entry("mixed", 1, 3, 0),
            parallel_path_entry("mixed", 1, 3, 1),
            parallel_path_entry("mixed", 1, 3, 2),
        ];
        [
            vec![outer[0].clone()],
            vec![outer[1].clone(), parallel_path_entry("mixed", 2, 2, 0)],
            vec![outer[2].clone(), parallel_path_entry("mixed", 2, 2, 1)],
        ]
    }

    #[test]
    fn parallel_schedules_every_nested_mixed_leaf_with_stable_metadata() {
        let ctx = workflow_context(Vec::new());
        let mut call = Box::pin(ctx.parallel(nested_parallel_operations()));
        let mut task_context = TaskContext::from_waker(noop_waker_ref());

        assert!(matches!(
            call.as_mut().poll(&mut task_context),
            Poll::Pending
        ));
        let commands = ctx.take_commands().expect("parallel commands");
        assert_eq!(
            commands
                .iter()
                .map(|command| command["type"].as_str().unwrap_or_default())
                .collect::<Vec<_>>(),
            [
                "schedule_activity",
                "start_child_workflow",
                "schedule_activity"
            ]
        );
        let paths = nested_parallel_paths();
        for (command, path) in commands.iter().zip(paths) {
            assert_eq!(command["parallel_group_path"], json!(path));
            assert_eq!(
                command["parallel_group_id"],
                json!(path.last().expect("inner group").parallel_group_id)
            );
        }
    }

    fn completed_nested_parallel_history() -> Vec<HistoryEvent> {
        let paths = nested_parallel_paths();
        let third = parallel_history_event(
            "ActivityCompleted",
            3,
            "activity_type",
            "third",
            paths[2].clone(),
            Some(json!("three")),
        );
        vec![
            parallel_history_event(
                "ActivityCompleted",
                1,
                "activity_type",
                "first",
                paths[0].clone(),
                Some(json!("one")),
            ),
            parallel_history_event(
                "ChildWorkflowScheduled",
                2,
                "child_workflow_type",
                "second",
                paths[1].clone(),
                None,
            ),
            parallel_history_event(
                "ChildRunCompleted",
                2,
                "child_workflow_type",
                "second",
                paths[1].clone(),
                Some(json!("two")),
            ),
            third.clone(),
            third,
        ]
    }

    #[test]
    fn parallel_replay_rebuilds_input_order_and_tolerates_duplicate_delivery() {
        for _restart_or_completed_replay in 0..2 {
            let ctx = workflow_context(completed_nested_parallel_history());
            let mut call = Box::pin(ctx.parallel(nested_parallel_operations()));
            let mut task_context = TaskContext::from_waker(noop_waker_ref());
            let Poll::Ready(Ok(results)) = call.as_mut().poll(&mut task_context) else {
                panic!("completed nested parallel history must replay");
            };
            assert_eq!(
                results,
                vec![
                    ParallelResult::Activity(json!("one")),
                    ParallelResult::Group(vec![
                        ParallelResult::ChildWorkflow(ChildWorkflowResult {
                            parent: WorkflowIdentity {
                                workflow_id: None,
                                run_id: None,
                            },
                            child: WorkflowIdentity {
                                workflow_id: None,
                                run_id: None,
                            },
                            child_workflow_type: Some("second".to_string()),
                            result: json!("two"),
                        }),
                        ParallelResult::Activity(json!("three")),
                    ]),
                ]
            );
            assert!(ctx.take_commands().expect("commands").is_empty());
            ctx.ensure_history_consumed().expect("history consumed");
        }
    }

    #[test]
    fn parallel_failure_keeps_typed_cause_path_and_late_completions() {
        let paths = nested_parallel_paths();
        let history = vec![
            parallel_history_event(
                "ActivityCompleted",
                1,
                "activity_type",
                "first",
                paths[0].clone(),
                Some(json!("one")),
            ),
            parallel_history_event(
                "ChildWorkflowScheduled",
                2,
                "child_workflow_type",
                "second",
                paths[1].clone(),
                None,
            ),
            parallel_history_event(
                "ChildRunFailed",
                2,
                "child_workflow_type",
                "second",
                paths[1].clone(),
                None,
            ),
            parallel_history_event(
                "ActivityCompleted",
                3,
                "activity_type",
                "third",
                paths[2].clone(),
                Some(json!("late")),
            ),
        ];
        let ctx = workflow_context(history);
        let mut call = Box::pin(ctx.parallel(nested_parallel_operations()));
        let mut task_context = TaskContext::from_waker(noop_waker_ref());
        let outcome = call.as_mut().poll(&mut task_context);
        let Poll::Ready(Err(Error::ParallelFailed(failure))) = outcome else {
            panic!("one failed child must return a typed partial failure: {outcome:?}");
        };
        assert_eq!(failure.member_path, [1, 0]);
        assert_eq!(failure.group_id, "parallel-calls:1:3");
        assert!(matches!(*failure.cause, Error::ChildWorkflowFailed(_)));
        assert_eq!(
            failure
                .completed
                .iter()
                .map(|completion| completion.member_path.clone())
                .collect::<Vec<_>>(),
            [vec![0], vec![1, 1]]
        );
    }

    #[test]
    fn pending_parallel_history_restarts_without_rescheduling_any_leaf() {
        let paths = nested_parallel_paths();
        let history = vec![
            parallel_history_event(
                "ActivityScheduled",
                1,
                "activity_type",
                "first",
                paths[0].clone(),
                None,
            ),
            parallel_history_event(
                "ChildWorkflowScheduled",
                2,
                "child_workflow_type",
                "second",
                paths[1].clone(),
                None,
            ),
            parallel_history_event(
                "ActivityScheduled",
                3,
                "activity_type",
                "third",
                paths[2].clone(),
                None,
            ),
        ];
        for _restart in 0..2 {
            let ctx = workflow_context(history.clone());
            let mut call = Box::pin(ctx.parallel(nested_parallel_operations()));
            let mut task_context = TaskContext::from_waker(noop_waker_ref());
            let outcome = call.as_mut().poll(&mut task_context);
            assert!(matches!(outcome, Poll::Pending), "{outcome:?}");
            assert!(ctx.take_commands().expect("commands").is_empty());
        }
    }

    fn selection_path(index: usize, key: &str) -> Vec<ParallelGroupMetadata> {
        vec![selection_group_entry(
            1,
            2,
            index,
            "activity",
            &SelectionMemberMetadata {
                key: SelectionKey::Name(key.to_string()),
                index,
                base_sequence: index as u64 + 1,
                size: 1,
                kind: "activity".to_string(),
            },
        )]
    }

    fn selection_activity_event(
        event_type: &str,
        index: usize,
        key: &str,
        result: Option<Value>,
    ) -> HistoryEvent {
        let sequence = index as u64 + 1;
        let mut event = parallel_history_event(
            event_type,
            sequence,
            "activity_type",
            &format!("{key}-activity"),
            selection_path(index, key),
            result,
        );
        event.payload["activity_execution_id"] = json!(format!("activity-{key}"));
        event.raw.insert(
            "id".to_string(),
            json!(if event_type == "ActivityCompleted" {
                format!("event-{key}")
            } else {
                format!("{event_type}-{key}")
            }),
        );
        event
    }

    fn selection_winner_marker() -> HistoryEvent {
        history_event(
            "SelectionResolved",
            json!({
                "selection_group_id": "select-calls:1:2",
                "selection_group_base_sequence": 1,
                "selection_group_size": 2,
                "member_key": "fast",
                "member_index": 1,
                "member_base_sequence": 2,
                "member_size": 1,
                "operation_kind": "activity",
                "operation_identity": "activity-fast",
                "outcome": "completed",
                "resolution_event_id": "event-fast",
                "resolution_event_type": "ActivityCompleted",
            }),
        )
    }

    fn keyed_activity_selection(ctx: &WorkflowContext) -> SelectCall {
        ctx.select_keyed(vec![
            (
                "slow",
                ParallelOperation::activity_with_options(
                    "slow-activity",
                    ActivityOptions::new().task_queue("default"),
                    json!([]),
                ),
            ),
            (
                "fast",
                ParallelOperation::activity_with_options(
                    "fast-activity",
                    ActivityOptions::new().task_queue("default"),
                    json!([]),
                ),
            ),
        ])
    }

    fn assert_persisted_selection_replay(history: Vec<HistoryEvent>) {
        let ctx = workflow_context(history);
        let mut call = Box::pin(keyed_activity_selection(&ctx));
        let mut task_context = TaskContext::from_waker(noop_waker_ref());
        let selected = match call.as_mut().poll(&mut task_context) {
            Poll::Ready(Ok(selected)) => selected,
            Poll::Ready(Err(error)) => panic!("persisted selection winner must replay: {error:?}"),
            Poll::Pending => panic!("persisted selection winner must replay without pending"),
        };
        assert_eq!(selected.key, SelectionKey::Name("fast".to_string()));
        assert_eq!(
            selected.value,
            Some(ParallelResult::Activity(json!("winner-value")))
        );
        let slow = selected
            .handle(&SelectionKey::Name("slow".to_string()))
            .expect("slow handle")
            .clone();
        let mut await_slow = Box::pin(slow.await_result());
        assert!(matches!(
            await_slow.as_mut().poll(&mut task_context),
            Poll::Ready(Ok(ParallelResult::Activity(value))) if value == json!("loser-value")
        ));
        assert!(ctx.take_commands().expect("commands").is_empty());
    }

    const SELECTION_COLD_REPLAY_HISTORY: &str = "DURABLE_WORKFLOW_SELECTION_COLD_REPLAY_HISTORY";

    fn canonical_selection_history() -> Vec<HistoryEvent> {
        const FIXTURE: &[u8] =
            include_bytes!("../tests/fixtures/durable_selection_runtime_history.json");
        assert_eq!(
            format!("{:x}", Sha256::digest(FIXTURE)),
            "51fd8b9c16e978dcef536a5c727b9fdc0ae724d9afc17d9a7837d219f41ee3ba",
        );
        let fixture: Value = serde_json::from_slice(FIXTURE).expect("canonical selection fixture");

        serde_json::from_value(fixture["history"].clone()).expect("canonical selection history")
    }

    #[test]
    fn selection_fresh_process_entrypoint() {
        let Ok(path) = std::env::var(SELECTION_COLD_REPLAY_HISTORY) else {
            return;
        };
        let persisted = fs::read(path).expect("persisted selection history");
        assert_eq!(
            format!("{:x}", Sha256::digest(&persisted)),
            "51fd8b9c16e978dcef536a5c727b9fdc0ae724d9afc17d9a7837d219f41ee3ba",
        );
        let fixture: Value =
            serde_json::from_slice(&persisted).expect("valid persisted selection fixture");
        let history: Vec<HistoryEvent> = serde_json::from_value(fixture["history"].clone())
            .expect("valid persisted selection history");

        assert_persisted_selection_replay(history);
    }

    #[test]
    fn selection_starts_every_member_with_stable_keys_and_group_identity() {
        let ctx = workflow_context(Vec::new());
        let mut call = Box::pin(keyed_activity_selection(&ctx));
        let mut task_context = TaskContext::from_waker(noop_waker_ref());

        assert!(matches!(
            call.as_mut().poll(&mut task_context),
            Poll::Pending
        ));
        let commands = ctx.take_commands().expect("selection commands");
        assert_eq!(commands.len(), 2);
        assert_eq!(commands[0]["selection_member_key"], json!("slow"));
        assert_eq!(commands[1]["selection_member_key"], json!("fast"));
        assert!(commands.iter().all(|command| {
            command["parallel_group_id"] == json!("select-calls:1:2")
                && command["parallel_group_mode"] == json!("select")
        }));
    }

    #[test]
    fn selection_key_domain_rejects_empty_authoring_and_malformed_history() {
        let ctx = workflow_context(Vec::new());
        let mut invalid = Box::pin(ctx.select_keyed(vec![(
            "",
            ParallelOperation::activity("invalid", json!([])),
        )]));
        let mut task_context = TaskContext::from_waker(noop_waker_ref());
        assert!(matches!(
            invalid.as_mut().poll(&mut task_context),
            Poll::Ready(Err(Error::InvalidParallelGroup(ParallelGroupError {
                reason: "selection_key_invalid",
                ..
            })))
        ));

        for invalid_key in [json!(""), json!(-1)] {
            let mut event = selection_activity_event("ActivityScheduled", 0, "slow", None);
            event.payload["selection_member_key"] = invalid_key.clone();
            event.payload["parallel_group_path"][0]["selection_member_key"] = invalid_key;
            assert!(matches!(
                WorkflowState::new_with_identity(
                    vec![event],
                    None,
                    None,
                    "rust-workers".to_string(),
                    DEFAULT_CODEC.to_string(),
                    None,
                ),
                Err(Error::NonDeterministicReplay(_))
            ));
        }
    }

    #[test]
    fn selection_preserves_valid_named_and_numeric_keys() {
        let ctx = workflow_context(Vec::new());
        let mut selection = Box::pin(ctx.select_keyed(vec![
            (
                SelectionKey::Index(0),
                ParallelOperation::activity("numeric", json!([])),
            ),
            (
                SelectionKey::Name("named".to_string()),
                ParallelOperation::timer(Duration::from_secs(1)),
            ),
        ]));
        let mut task_context = TaskContext::from_waker(noop_waker_ref());

        assert!(matches!(
            selection.as_mut().poll(&mut task_context),
            Poll::Pending
        ));
        let commands = ctx.take_commands().expect("selection commands");
        assert_eq!(commands[0]["selection_member_key"], json!(0));
        assert_eq!(commands[1]["selection_member_key"], json!("named"));
    }

    #[test]
    fn selection_replays_persisted_winner_and_loser_can_be_awaited_later() {
        let history = canonical_selection_history();
        assert_persisted_selection_replay(history.clone());

        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/durable_selection_runtime_history.json");
        let output =
            ProcessCommand::new(std::env::current_exe().expect("current Rust test binary"))
                .args([
                    "--exact",
                    "tests::selection_fresh_process_entrypoint",
                    "--nocapture",
                ])
                .env(SELECTION_COLD_REPLAY_HISTORY, &path)
                .output()
                .expect("run fresh selection replay process");

        assert!(
            output.status.success(),
            "fresh selection replay failed:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }

    #[test]
    fn selection_waits_durably_when_terminal_members_precede_the_winner_marker() {
        let mut history = canonical_selection_history();
        history.retain(|event| event.event_type != "SelectionResolved");
        let ctx = workflow_context(history);
        let mut selection = Box::pin(keyed_activity_selection(&ctx));
        let mut task_context = TaskContext::from_waker(noop_waker_ref());

        assert!(matches!(
            selection.as_mut().poll(&mut task_context),
            Poll::Pending
        ));
        assert!(ctx.take_commands().expect("commands").is_empty());
        assert!(
            ctx.matched_recorded_pending()
                .expect("selection pending state"),
            "terminal member history must keep the workflow durably pending until SelectionResolved commits"
        );
    }

    #[test]
    fn selection_terminal_condition_history_waits_durably_for_its_winner_marker() {
        for (terminal_event, predicate_satisfied, timeout_seconds) in [
            ("ConditionWaitSatisfied", true, None),
            ("ConditionWaitTimedOut", false, Some(0)),
        ] {
            let member = SelectionMemberMetadata {
                key: SelectionKey::Name("condition".to_string()),
                index: 0,
                base_sequence: 1,
                size: 1,
                kind: "condition".to_string(),
            };
            let path = vec![selection_group_entry(1, 1, 0, "condition", &member)];
            let mut payload = json!({
                "sequence": 1,
                "condition_wait_id": "condition-1",
                "condition_wait_occurrence_id": "rust:condition-wait:0",
                "condition_key": "ready",
                "condition_definition_fingerprint": "sha256:ready-v1",
                "parallel_group_path": path,
            });
            payload
                .as_object_mut()
                .expect("condition history payload")
                .extend(
                    serde_json::to_value(&path[0])
                        .expect("condition selection metadata")
                        .as_object()
                        .expect("condition selection metadata object")
                        .clone(),
                );
            if let Some(timeout_seconds) = timeout_seconds {
                payload["timeout_seconds"] = json!(timeout_seconds);
            }
            let history = vec![
                history_event("ConditionWaitOpened", payload.clone()),
                history_event(terminal_event, payload),
            ];
            let ctx = workflow_context(history);
            let mut options = ConditionWaitOptions::new("ready", "sha256:ready-v1");
            if timeout_seconds.is_some() {
                options = options.timeout(Duration::ZERO);
            }
            let mut selection = Box::pin(ctx.select_keyed(vec![(
                "condition",
                ParallelOperation::condition(options, move || Ok(predicate_satisfied)),
            )]));
            let mut task_context = TaskContext::from_waker(noop_waker_ref());

            assert!(matches!(
                selection.as_mut().poll(&mut task_context),
                Poll::Pending
            ));
            assert!(ctx.take_commands().expect("commands").is_empty());
            assert!(
                ctx.matched_recorded_pending()
                    .expect("condition selection pending state"),
                "{terminal_event} must keep the workflow durably pending until SelectionResolved commits"
            );
        }
    }

    #[test]
    fn selection_immediate_condition_members_open_a_durable_wait() {
        for predicate_satisfied in [true, false] {
            let ctx = workflow_context(Vec::new());
            let mut selection = Box::pin(ctx.select_keyed(vec![(
                "condition",
                ParallelOperation::condition(
                    ConditionWaitOptions::new("ready", "sha256:ready-v1").timeout(Duration::ZERO),
                    move || Ok(predicate_satisfied),
                ),
            )]));
            let mut task_context = TaskContext::from_waker(noop_waker_ref());

            assert!(matches!(
                selection.as_mut().poll(&mut task_context),
                Poll::Pending
            ));
            let commands = ctx.take_commands().expect("condition selection command");
            assert_eq!(commands.len(), 1);
            assert_eq!(commands[0]["type"], json!("open_condition_wait"));
            assert_eq!(commands[0]["timeout_seconds"], json!(0));
            assert_eq!(
                commands[0]["parallel_group_path"][0]["parallel_group_mode"],
                json!("select")
            );
        }
    }

    #[test]
    fn selection_loser_cancellation_is_explicit_and_idempotent() {
        let history = vec![
            selection_activity_event("ActivityScheduled", 0, "slow", None),
            selection_activity_event("ActivityCompleted", 1, "fast", Some(json!("winner"))),
            selection_winner_marker(),
        ];
        let ctx = workflow_context(history.clone());
        let mut call = Box::pin(keyed_activity_selection(&ctx));
        let mut task_context = TaskContext::from_waker(noop_waker_ref());
        let Poll::Ready(Ok(selected)) = call.as_mut().poll(&mut task_context) else {
            panic!("winner must replay");
        };
        let slow = selected
            .handle(&SelectionKey::Name("slow".to_string()))
            .expect("slow handle")
            .clone();
        let mut cancel = Box::pin(slow.cancel());
        assert!(matches!(
            cancel.as_mut().poll(&mut task_context),
            Poll::Pending
        ));
        assert!(matches!(
            cancel.as_mut().poll(&mut task_context),
            Poll::Pending
        ));
        let commands = ctx.take_commands().expect("cancel command");
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0]["type"], json!("cancel_selection_operation"));
        assert_eq!(commands[0]["member_key"], json!("slow"));

        let mut cancelled_history = history;
        cancelled_history.push(history_event(
            "SelectionOperationCancelled",
            json!({
                "selection_group_id": "select-calls:1:2",
                "member_key": "slow",
                "member_index": 0,
                "member_base_sequence": 1,
                "member_size": 1,
                "operation_kind": "activity",
                "operation_identity": "activity-slow",
                "cancelled_at": "2026-08-27T00:00:00Z",
            }),
        ));
        let replayed = workflow_context(cancelled_history);
        let mut call = Box::pin(keyed_activity_selection(&replayed));
        let Poll::Ready(Ok(selected)) = call.as_mut().poll(&mut task_context) else {
            panic!("winner must replay after cancellation");
        };
        let slow = selected
            .handle(&SelectionKey::Name("slow".to_string()))
            .expect("slow handle")
            .clone();
        let mut cancel = Box::pin(slow.cancel());
        assert!(matches!(
            cancel.as_mut().poll(&mut task_context),
            Poll::Ready(Ok(()))
        ));
        assert!(replayed.take_commands().expect("commands").is_empty());
    }

    #[test]
    fn selection_cancellation_marker_is_bound_to_every_authored_handle_field() {
        let base_history = vec![
            selection_activity_event("ActivityScheduled", 0, "slow", None),
            selection_activity_event("ActivityScheduled", 1, "fast", None),
            selection_activity_event("ActivityCompleted", 1, "fast", Some(json!("winner"))),
            selection_winner_marker(),
        ];
        for (field, corrupt) in [
            ("member_key", json!("fast")),
            ("member_index", json!(1)),
            ("member_base_sequence", json!(3)),
            ("member_size", json!(2)),
            ("operation_kind", json!("timer")),
            ("operation_identity", json!("forged")),
        ] {
            let mut cancellation = json!({
                "selection_group_id": "select-calls:1:2",
                "member_key": "slow",
                "member_index": 0,
                "member_base_sequence": 1,
                "member_size": 1,
                "operation_kind": "activity",
                "operation_identity": "activity-slow",
            });
            cancellation[field] = corrupt;
            let mut history = base_history.clone();
            history.push(history_event("SelectionOperationCancelled", cancellation));
            let ctx = workflow_context(history);
            let mut selection = Box::pin(keyed_activity_selection(&ctx));
            let mut task_context = TaskContext::from_waker(noop_waker_ref());

            assert!(matches!(
                selection.as_mut().poll(&mut task_context),
                Poll::Ready(Err(Error::NonDeterministicReplay(_)))
            ));
        }
    }

    #[test]
    fn selection_child_identity_prefers_the_durable_run_id() {
        let ctx = workflow_context(vec![history_event(
            "ChildWorkflowScheduled",
            json!({
                "sequence": 1,
                "child_workflow_type": "child",
                "child_workflow_instance_id": "child-instance",
                "child_workflow_run_id": "child-run",
            }),
        )]);
        let state = ctx.state.lock().expect("workflow state");

        assert_eq!(
            selection_operation_identity(&state, "child", 1, 1),
            "child-run"
        );
    }

    #[test]
    fn selection_activity_identity_requires_canonical_execution_id() {
        let slow = selection_activity_event("ActivityScheduled", 0, "slow", None);
        let mut fast_open = selection_activity_event("ActivityScheduled", 1, "fast", None);
        let mut fast_completed =
            selection_activity_event("ActivityCompleted", 1, "fast", Some(json!("winner")));
        for event in [&mut fast_open, &mut fast_completed] {
            event
                .payload
                .as_object_mut()
                .expect("activity payload")
                .remove("activity_execution_id");
            event.payload["activity_id"] = json!("forged-activity-id");
        }
        let mut marker = selection_winner_marker();
        marker.payload["operation_identity"] = json!("forged-activity-id");
        let ctx = workflow_context(vec![slow, fast_open, fast_completed, marker]);
        let mut selection = Box::pin(keyed_activity_selection(&ctx));
        let mut task_context = TaskContext::from_waker(noop_waker_ref());

        assert!(matches!(
            selection.as_mut().poll(&mut task_context),
            Poll::Ready(Err(Error::NonDeterministicReplay(_)))
        ));
    }

    #[test]
    fn selection_completion_before_cancellation_remains_awaitable() {
        let history = vec![
            selection_activity_event("ActivityScheduled", 0, "slow", None),
            selection_activity_event("ActivityCompleted", 1, "fast", Some(json!("winner"))),
            selection_winner_marker(),
            selection_activity_event(
                "ActivityCompleted",
                0,
                "slow",
                Some(json!("completed-first")),
            ),
        ];
        let ctx = workflow_context(history);
        let mut selection = Box::pin(keyed_activity_selection(&ctx));
        let mut task_context = TaskContext::from_waker(noop_waker_ref());
        let Poll::Ready(Ok(selected)) = selection.as_mut().poll(&mut task_context) else {
            panic!("winner must replay");
        };
        let slow = selected
            .handle(&SelectionKey::Name("slow".to_string()))
            .expect("slow handle")
            .clone();
        let mut cancel = Box::pin(slow.cancel());
        assert!(matches!(
            cancel.as_mut().poll(&mut task_context),
            Poll::Ready(Ok(()))
        ));
        let mut await_slow = Box::pin(slow.await_result());
        assert!(matches!(
            await_slow.as_mut().poll(&mut task_context),
            Poll::Ready(Ok(ParallelResult::Activity(value))) if value == json!("completed-first")
        ));
        let commands = ctx.take_commands().expect("commands");
        assert!(commands.is_empty());
    }

    #[test]
    fn selection_nested_later_failure_before_cancel_remains_the_awaited_failure() {
        let nested_member = SelectionMemberMetadata {
            key: SelectionKey::Name("nested".to_string()),
            index: 0,
            base_sequence: 1,
            size: 2,
            kind: "group".to_string(),
        };
        let deadline_member = SelectionMemberMetadata {
            key: SelectionKey::Name("deadline".to_string()),
            index: 1,
            base_sequence: 3,
            size: 1,
            kind: "timer".to_string(),
        };
        let nested_paths = [
            vec![
                selection_group_entry(1, 3, 0, "mixed", &nested_member),
                parallel_group_entry(1, 2, 0, "activity"),
            ],
            vec![
                selection_group_entry(1, 3, 1, "mixed", &nested_member),
                parallel_group_entry(1, 2, 1, "activity"),
            ],
        ];
        let deadline_path = vec![selection_group_entry(1, 3, 2, "mixed", &deadline_member)];
        let mut timer_fired = parallel_history_event(
            "TimerFired",
            3,
            "timer_id",
            "timer-3",
            deadline_path.clone(),
            None,
        );
        timer_fired.payload["delay_seconds"] = json!(0);
        timer_fired
            .raw
            .insert("id".to_string(), json!("timer-fired"));
        let mut timer_scheduled = parallel_history_event(
            "TimerScheduled",
            3,
            "timer_id",
            "timer-3",
            deadline_path,
            None,
        );
        timer_scheduled.payload["delay_seconds"] = json!(0);
        let history = vec![
            parallel_history_event(
                "ActivityScheduled",
                1,
                "activity_type",
                "nested-first",
                nested_paths[0].clone(),
                None,
            ),
            parallel_history_event(
                "ActivityScheduled",
                2,
                "activity_type",
                "nested-second",
                nested_paths[1].clone(),
                None,
            ),
            timer_scheduled,
            timer_fired,
            history_event(
                "SelectionResolved",
                json!({
                    "selection_group_id": "select-calls:1:3",
                    "selection_group_base_sequence": 1,
                    "selection_group_size": 3,
                    "member_key": "deadline",
                    "member_index": 1,
                    "member_base_sequence": 3,
                    "member_size": 1,
                    "operation_kind": "timer",
                    "operation_identity": "timer-3",
                    "outcome": "completed",
                    "resolution_event_id": "timer-fired",
                    "resolution_event_type": "TimerFired",
                }),
            ),
            parallel_history_event(
                "ActivityFailed",
                2,
                "activity_type",
                "nested-second",
                nested_paths[1].clone(),
                None,
            ),
        ];
        let ctx = workflow_context(history);
        let mut selection = Box::pin(ctx.select_keyed(vec![
            (
                "nested",
                ParallelOperation::group(vec![
                    ParallelOperation::activity("nested-first", json!([])),
                    ParallelOperation::activity("nested-second", json!([])),
                ]),
            ),
            ("deadline", ParallelOperation::timer(Duration::ZERO)),
        ]));
        let mut task_context = TaskContext::from_waker(noop_waker_ref());
        let Poll::Ready(Ok(selected)) = selection.as_mut().poll(&mut task_context) else {
            panic!("deadline winner must replay");
        };
        let nested = selected
            .handle(&SelectionKey::Name("nested".to_string()))
            .expect("nested handle")
            .clone();
        let mut cancel = Box::pin(nested.cancel());
        assert!(matches!(
            cancel.as_mut().poll(&mut task_context),
            Poll::Ready(Ok(()))
        ));
        let mut await_nested = Box::pin(nested.await_result());

        assert!(matches!(
            await_nested.as_mut().poll(&mut task_context),
            Poll::Ready(Err(Error::ActivityFailed(_)))
        ));
        assert!(ctx.take_commands().expect("commands").is_empty());
    }

    #[test]
    fn selection_supports_child_timer_signal_condition_and_nested_groups() {
        let ctx = workflow_context(Vec::new());
        let mut call = Box::pin(ctx.select(vec![
            ParallelOperation::child_workflow(
                "child",
                ChildWorkflowOptions::new("children"),
                json!([]),
            ),
            ParallelOperation::timer(Duration::from_secs(30)),
            ParallelOperation::signal("approval"),
            ParallelOperation::condition(
                ConditionWaitOptions::new("ready", "sha256:ready"),
                || Ok(false),
            ),
            ParallelOperation::group(vec![
                ParallelOperation::activity("nested-one", json!([])),
                ParallelOperation::activity("nested-two", json!([])),
            ]),
        ]));
        let mut task_context = TaskContext::from_waker(noop_waker_ref());
        assert!(matches!(
            call.as_mut().poll(&mut task_context),
            Poll::Pending
        ));
        let commands = ctx.take_commands().expect("selection commands");
        assert_eq!(
            commands
                .iter()
                .map(|command| command["type"].as_str().unwrap_or_default())
                .collect::<Vec<_>>(),
            [
                "start_child_workflow",
                "start_timer",
                "open_signal_wait",
                "open_condition_wait",
                "schedule_activity",
                "schedule_activity",
            ]
        );
        assert!(commands.iter().all(|command| {
            command["parallel_group_path"][0]["parallel_group_mode"] == json!("select")
        }));
        assert_eq!(
            commands[4]["parallel_group_path"].as_array().map(Vec::len),
            Some(2)
        );
        assert_eq!(
            commands[4]["parallel_group_path"][0]["selection_member_kind"],
            json!("group")
        );
        assert_eq!(
            commands[5]["parallel_group_path"][0]["selection_member_kind"],
            json!("group")
        );

        let one_leaf_ctx = workflow_context(Vec::new());
        let mut one_leaf = Box::pin(one_leaf_ctx.select(vec![ParallelOperation::group(vec![
            ParallelOperation::activity("nested-only", json!([])),
        ])]));
        assert!(matches!(
            one_leaf.as_mut().poll(&mut task_context),
            Poll::Pending
        ));
        let one_leaf_commands = one_leaf_ctx.take_commands().expect("one-leaf commands");
        assert_eq!(one_leaf_commands.len(), 1);
        assert_eq!(
            one_leaf_commands[0]["parallel_group_path"][0]["selection_member_kind"],
            json!("group")
        );
        assert_eq!(
            one_leaf_commands[0]["parallel_group_path"][0]["selection_member_size"],
            json!(1)
        );
    }

    async fn trip_saga(ctx: WorkflowContext) -> Result<Value> {
        let mut saga = ctx.saga();
        let outcome = async {
            let flight = ctx.activity("trip.reserve-flight", json!([])).await?;
            saga.add_compensation("trip.cancel-flight", json!([flight]))?;
            let hotel = ctx.activity("trip.reserve-hotel", json!([])).await?;
            saga.add_compensation("trip.cancel-hotel", json!([hotel]))?;
            ctx.activity("trip.charge", json!([])).await?;
            Ok(json!({"status": "booked"}))
        }
        .await;
        saga.finish(outcome).await
    }

    fn saga_activity(
        event_type: &str,
        sequence: u64,
        activity_type: &str,
        result: Option<Value>,
    ) -> HistoryEvent {
        let mut payload = json!({
            "sequence": sequence,
            "activity_type": activity_type,
            "message": format!("{activity_type} failed"),
            "exception_type": "PlannedFailure",
            "non_retryable": true,
        });
        if let Some(result) = result {
            payload["result"] = fixture_envelope(result);
        }
        history_event(event_type, payload)
    }

    #[test]
    fn saga_replays_reverse_compensation_across_restart_and_duplicate_delivery() {
        let completed_hotel_compensation = saga_activity(
            "ActivityCompleted",
            4,
            "trip.cancel-hotel",
            Some(Value::Null),
        );
        let history = vec![
            saga_activity(
                "ActivityCompleted",
                1,
                "trip.reserve-flight",
                Some(json!("flight-1")),
            ),
            saga_activity(
                "ActivityCompleted",
                2,
                "trip.reserve-hotel",
                Some(json!("hotel-1")),
            ),
            saga_activity("ActivityFailed", 3, "trip.charge", None),
            completed_hotel_compensation.clone(),
            completed_hotel_compensation,
        ];

        for _restart in 0..2 {
            let ctx = workflow_context(history.clone());
            let mut future = Box::pin(trip_saga(ctx.clone()));
            let mut task_context = TaskContext::from_waker(noop_waker_ref());
            assert!(matches!(
                future.as_mut().poll(&mut task_context),
                Poll::Pending
            ));
            let commands = ctx.take_commands().expect("compensation command");
            assert_eq!(commands.len(), 1);
            assert_eq!(commands[0]["activity_type"], "trip.cancel-flight");
        }
    }

    #[test]
    fn saga_compensation_failure_preserves_both_typed_failures() {
        let history = vec![
            saga_activity(
                "ActivityCompleted",
                1,
                "trip.reserve-flight",
                Some(json!("flight-1")),
            ),
            saga_activity(
                "ActivityCompleted",
                2,
                "trip.reserve-hotel",
                Some(json!("hotel-1")),
            ),
            saga_activity("ActivityFailed", 3, "trip.charge", None),
            saga_activity("ActivityFailed", 4, "trip.cancel-hotel", None),
        ];
        let ctx = workflow_context(history);
        let mut future = Box::pin(trip_saga(ctx));
        let mut task_context = TaskContext::from_waker(noop_waker_ref());
        let Poll::Ready(Err(Error::SagaCompensationFailed(failure))) =
            future.as_mut().poll(&mut task_context)
        else {
            panic!("compensation failure must remain structured");
        };
        assert!(matches!(
            *failure.initiating_failure,
            Error::ActivityFailed(_)
        ));
        assert!(matches!(
            *failure.compensation_failure,
            Error::ActivityFailed(_)
        ));
        assert_eq!(failure.compensation_activity_type, "trip.cancel-hotel");
        assert_eq!(failure.compensation_registration_order, 2);
    }

    #[test]
    fn saga_compensates_cooperative_cancellation() {
        let ctx = workflow_context(vec![saga_activity(
            "ActivityCompleted",
            1,
            "trip.reserve-flight",
            Some(json!("flight-1")),
        )]);
        ctx.state.lock().expect("state").cancel_requested = true;
        let run = {
            let ctx = ctx.clone();
            async move {
                let mut saga = ctx.saga();
                let outcome = async {
                    let flight = ctx.activity("trip.reserve-flight", json!([])).await?;
                    saga.add_compensation("trip.cancel-flight", json!([flight]))?;
                    ctx.throw_if_cancellation_requested()?;
                    Ok(json!("unexpected"))
                }
                .await;
                saga.finish(outcome).await
            }
        };
        let mut future = Box::pin(run);
        let mut task_context = TaskContext::from_waker(noop_waker_ref());
        assert!(matches!(
            future.as_mut().poll(&mut task_context),
            Poll::Pending
        ));
        let commands = ctx.take_commands().expect("cancellation compensation");
        assert_eq!(commands[0]["activity_type"], "trip.cancel-flight");
    }

    fn workflow_task(
        workflow_type: &str,
        history_events: Vec<HistoryEvent>,
        payload_codec: &str,
    ) -> WorkflowTask {
        WorkflowTask {
            task_id: format!("wft-{workflow_type}"),
            workflow_command_id: None,
            workflow_id: Some(format!("wf-{workflow_type}")),
            run_id: Some(format!("run-{workflow_type}")),
            workflow_type: workflow_type.to_string(),
            cancel_requested: false,
            payload_codec: payload_codec.to_string(),
            arguments: Some(
                encode_value_envelope(&json!([]), payload_codec).expect("workflow arguments"),
            ),
            total_history_events: Some(history_events.len() as u64),
            history_size_bytes: None,
            continue_as_new_recommended: None,
            history_budget_pressure: None,
            history_events,
            next_history_page_token: None,
            workflow_task_attempt: 1,
            workflow_signal_id: None,
            signal_name: None,
            signal_arguments: None,
            workflow_update_id: None,
            update_name: None,
            lease_owner: Some("rust-worker".to_string()),
        }
    }

    #[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
    struct SideEffectProbe {
        request_id: String,
        attempt: u32,
    }

    #[test]
    fn typed_side_effect_runs_callback_once_and_replay_skips_it() {
        let calls = AtomicUsize::new(0);
        let ctx = workflow_context(Vec::new());
        let value = ctx
            .side_effect(|| {
                calls.fetch_add(1, Ordering::SeqCst);
                SideEffectProbe {
                    request_id: "request-42".to_string(),
                    attempt: 3,
                }
            })
            .expect("first side effect");
        assert_eq!(value.attempt, 3);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let commands = ctx.take_commands().expect("commands");
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0]["type"], "record_side_effect");
        assert_eq!(
            decode_wire_value(&commands[0]["result"], DEFAULT_CODEC).expect("Avro result"),
            serde_json::to_value(&value).expect("value")
        );

        let replay = workflow_context(vec![history_event(
            "SideEffectRecorded",
            json!({"sequence": 1, "result": commands[0]["result"].clone()}),
        )]);
        let replayed: SideEffectProbe = replay
            .side_effect(|| {
                calls.fetch_add(1, Ordering::SeqCst);
                panic!("committed side-effect callbacks must not run during replay")
            })
            .expect("replayed side effect");
        assert_eq!(replayed, value);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(replay.take_commands().expect("commands").is_empty());
        replay.ensure_history_consumed().expect("history consumed");
    }

    #[test]
    fn side_effect_uses_avro_envelope_and_uuid_is_replay_stable() {
        let ctx = workflow_context_with_codec(Vec::new(), DEFAULT_CODEC);
        let value = ctx
            .side_effect(|| SideEffectProbe {
                request_id: "avro-request".to_string(),
                attempt: 1,
            })
            .expect("Avro side effect");
        let uuid = ctx.uuid_v4().expect("deterministic UUID");
        let commands = ctx.take_commands().expect("commands");
        assert_eq!(commands.len(), 2);
        assert_eq!(commands[0]["result"]["codec"], DEFAULT_CODEC);
        assert_eq!(commands[1]["result"]["codec"], DEFAULT_CODEC);
        assert_eq!(
            decode_wire_value(&commands[0]["result"], DEFAULT_CODEC).expect("Avro result"),
            serde_json::to_value(&value).expect("value")
        );

        let replay = workflow_context_with_codec(
            vec![
                history_event(
                    "SideEffectRecorded",
                    json!({"sequence": 1, "result": commands[0]["result"].clone()}),
                ),
                history_event(
                    "SideEffectRecorded",
                    json!({"sequence": 2, "result": commands[1]["result"].clone()}),
                ),
            ],
            DEFAULT_CODEC,
        );
        let replayed: SideEffectProbe = replay
            .side_effect(|| panic!("Avro callback must not run"))
            .expect("replayed Avro value");
        let replayed_uuid = replay.uuid_v4().expect("replayed UUID");
        assert_eq!(replayed, value);
        assert_eq!(replayed_uuid, uuid);
        assert!(replay.take_commands().expect("commands").is_empty());
    }

    #[test]
    fn typed_side_effect_replay_preserves_bytes_and_maps() {
        let ctx = workflow_context_with_codec(Vec::new(), DEFAULT_CODEC);
        let value = ctx
            .side_effect_avro_value(typed_fidelity_probe)
            .expect("typed side effect");
        let commands = ctx.take_commands().expect("side-effect command");
        assert_eq!(
            decode_wire_avro_value(&commands[0]["result"], DEFAULT_CODEC)
                .expect("recorded side effect"),
            value
        );

        let replay = workflow_context_with_codec(
            vec![history_event(
                "SideEffectRecorded",
                json!({"sequence": 1, "result": commands[0]["result"].clone()}),
            )],
            DEFAULT_CODEC,
        );
        assert_eq!(
            replay
                .side_effect_avro_value(|| panic!("replay must not invoke callback"))
                .expect("replayed typed side effect"),
            value
        );
    }

    #[test]
    fn ordered_side_effects_share_the_durable_command_stream() {
        let first = encode_value_envelope(&json!("first"), DEFAULT_CODEC).expect("first");
        let second = encode_value_envelope(&json!(29), DEFAULT_CODEC).expect("second");
        let ctx = workflow_context(vec![
            history_event(
                "SideEffectRecorded",
                json!({"sequence": 1, "result": first}),
            ),
            history_event(
                "SideEffectRecorded",
                json!({"sequence": 2, "result": second}),
            ),
        ]);
        let first: String = ctx
            .side_effect(|| panic!("first callback must not run"))
            .expect("first replay");
        let second: i32 = ctx
            .side_effect(|| panic!("second callback must not run"))
            .expect("second replay");
        assert_eq!(first, "first");
        assert_eq!(second, 29);
        ctx.ensure_history_consumed().expect("ordered history");

        let reordered = workflow_context(vec![history_event(
            "VersionMarkerRecorded",
            json!({
                "sequence": 1,
                "change_id": "before-side-effect",
                "version": 1,
                "min_supported": 1,
                "max_supported": 1,
            }),
        )]);
        let error = reordered
            .side_effect(|| "new".to_string())
            .expect_err("command reordering must fail");
        assert!(matches!(
            error,
            Error::NonDeterministicReplay(ReplayFailure { ref reason, .. })
                if reason == "recorded_command_mismatch"
        ));
    }

    #[test]
    fn version_markers_replay_across_upgrades_and_do_not_duplicate() {
        let ctx = workflow_context(Vec::new());
        assert_eq!(ctx.get_version("checkout-v2", 1, 2).expect("version"), 2);
        assert_eq!(ctx.get_version("checkout-v2", 1, 3).expect("cached"), 2);
        assert!(ctx.patched("new-search").expect("patch"));
        ctx.deprecate_patch("new-search").expect("deprecate patch");
        let commands = ctx.take_commands().expect("commands");
        assert_eq!(commands.len(), 2);
        assert_eq!(commands[0]["type"], "record_version_marker");
        assert_eq!(commands[0]["version"], 2);
        assert_eq!(commands[1]["change_id"], "new-search");

        let replay = workflow_context(vec![history_event(
            "VersionMarkerRecorded",
            json!({
                "sequence": 1,
                "change_id": "checkout-v2",
                "version": 2,
                "min_supported": 1,
                "max_supported": 2,
            }),
        )]);
        assert_eq!(replay.get_version("checkout-v2", 1, 4).expect("upgrade"), 2);
        assert_eq!(replay.get_version("checkout-v2", 2, 5).expect("repeat"), 2);
        assert!(replay.take_commands().expect("commands").is_empty());
        replay.ensure_history_consumed().expect("history consumed");
    }

    #[test]
    fn version_markers_reject_incompatible_or_malformed_history() {
        let incompatible = workflow_context(vec![history_event(
            "VersionMarkerRecorded",
            json!({
                "sequence": 1,
                "change_id": "checkout-v2",
                "version": 1,
                "min_supported": 1,
                "max_supported": 2,
            }),
        )]);
        let error = incompatible
            .get_version("checkout-v2", 2, 3)
            .expect_err("old version is unsupported");
        assert!(matches!(
            error,
            Error::NonDeterministicReplay(ReplayFailure { ref reason, .. })
                if reason == "version_marker_incompatible_range"
        ));

        for (history, reason) in [
            (
                vec![history_event("SideEffectRecorded", json!({"sequence": 1}))],
                "side_effect_result_missing",
            ),
            (
                vec![history_event(
                    "SideEffectRecorded",
                    json!({
                        "sequence": 1,
                        "result": {"codec": "avro", "blob": "not-base64"},
                    }),
                )],
                "side_effect_payload_incompatible",
            ),
            (
                vec![history_event(
                    "SideEffectRecorded",
                    json!({"sequence": 1, "result": {"unwrapped": true}}),
                )],
                "side_effect_payload_malformed",
            ),
            (
                vec![history_event(
                    "VersionMarkerRecorded",
                    json!({
                        "sequence": 1,
                        "change_id": "change",
                        "version": 1,
                        "min_supported": 2,
                        "max_supported": 1,
                    }),
                )],
                "version_marker_history_range_invalid",
            ),
        ] {
            let error = WorkflowState::new(
                history,
                "rust-workers".to_string(),
                DEFAULT_CODEC.to_string(),
                None,
            )
            .expect_err("malformed history must fail");
            assert!(matches!(
                error,
                Error::NonDeterministicReplay(ReplayFailure { reason: actual, .. })
                    if actual == reason
            ));
        }
    }

    #[test]
    fn typed_search_attributes_replay_value_and_type_identity_after_restart() {
        let history = vec![history_event(
            "SearchAttributesUpserted",
            json!({
                "sequence": 1,
                "attributes": {"customer_tier": "gold"},
                "attribute_types": {"customer_tier": "keyword"},
                "merged": {"customer_tier": "gold"}
            }),
        )];

        let matching = workflow_context(history.clone());
        matching
            .upsert_search_attributes(
                SearchAttributeUpdate::new()
                    .keyword("customer_tier", "gold")
                    .expect("keyword update"),
            )
            .expect("matching typed update must replay");
        matching
            .ensure_history_consumed()
            .expect("history consumed");

        let changed_type = workflow_context(history.clone());
        let error = changed_type
            .upsert_search_attributes(
                SearchAttributeUpdate::new()
                    .string("customer_tier", "gold")
                    .expect("string update"),
            )
            .expect_err("same JSON value with a different declaration must be nondeterministic");
        let Error::NonDeterministicReplay(failure) = error else {
            panic!("typed identity drift must be a replay failure");
        };
        assert_eq!(failure.reason, "search_attribute_type_mismatch");
        assert_eq!(failure.sequence, Some(1));

        let changed_value = workflow_context(history);
        let error = changed_value
            .upsert_search_attributes(
                SearchAttributeUpdate::new()
                    .keyword("customer_tier", "platinum")
                    .expect("keyword update"),
            )
            .expect_err("changed values must be nondeterministic");
        let Error::NonDeterministicReplay(failure) = error else {
            panic!("value drift must be a replay failure");
        };
        assert_eq!(failure.reason, "search_attribute_value_mismatch");
    }

    #[test]
    fn legacy_search_attribute_history_keeps_type_identity_unknown() {
        let history = vec![history_event(
            "SearchAttributesUpserted",
            json!({
                "sequence": 1,
                "attributes": {"customer_tier": "gold"},
                "merged": {"customer_tier": "gold"}
            }),
        )];

        for update in [
            SearchAttributeUpdate::new()
                .keyword("customer_tier", "gold")
                .expect("keyword update"),
            SearchAttributeUpdate::new()
                .string("customer_tier", "gold")
                .expect("string update"),
        ] {
            let restarted = workflow_context(history.clone());
            restarted
                .upsert_search_attributes(update)
                .expect("legacy history constrains values but has unknown type identity");
            restarted
                .ensure_history_consumed()
                .expect("history consumed");
        }
    }

    #[test]
    fn search_attribute_command_emits_canonical_types() {
        let ctx = workflow_context(Vec::new());
        ctx.upsert_search_attributes(
            SearchAttributeUpdate::new()
                .keyword("customer_tier", "gold")
                .expect("keyword update")
                .int("attempts", 3)
                .expect("int update")
                .delete("obsolete")
                .expect("delete update"),
        )
        .expect("valid search attributes");

        assert_eq!(
            ctx.take_commands().expect("commands"),
            vec![json!({
                "type": "upsert_search_attributes",
                "attributes": {
                    "attempts": 3,
                    "customer_tier": "gold",
                    "obsolete": null
                },
                "attribute_types": {
                    "attempts": "int",
                    "customer_tier": "keyword"
                }
            })]
        );
    }

    #[test]
    fn duplicate_side_effects_and_version_markers_are_rejected() {
        let duplicate_side_effect = WorkflowState::new(
            vec![
                history_event(
                    "SideEffectRecorded",
                    json!({"sequence": 1, "result": fixture_envelope(json!(1))}),
                ),
                history_event(
                    "SideEffectRecorded",
                    json!({"sequence": 1, "result": fixture_envelope(json!(2))}),
                ),
            ],
            "rust-workers".to_string(),
            DEFAULT_CODEC.to_string(),
            None,
        )
        .expect_err("duplicate side effect");
        assert!(matches!(
            duplicate_side_effect,
            Error::NonDeterministicReplay(ReplayFailure { ref reason, .. })
                if reason == "duplicate_side_effect_record"
        ));

        let marker = |sequence| {
            history_event(
                "VersionMarkerRecorded",
                json!({
                    "sequence": sequence,
                    "change_id": "same-change",
                    "version": 1,
                    "min_supported": 1,
                    "max_supported": 1,
                }),
            )
        };
        let duplicate_marker = WorkflowState::new(
            vec![marker(1), marker(3)],
            "rust-workers".to_string(),
            DEFAULT_CODEC.to_string(),
            None,
        )
        .expect_err("duplicate marker");
        assert!(matches!(
            duplicate_marker,
            Error::NonDeterministicReplay(ReplayFailure { ref reason, .. })
                if reason == "duplicate_version_marker"
        ));
    }

    #[test]
    fn workflow_stream_authoring_derives_identity_and_replay_skips_duplicate_append() {
        let mut state = WorkflowState::new(
            Vec::new(),
            "rust-workers".to_string(),
            DEFAULT_CODEC.to_string(),
            None,
        )
        .expect("workflow state");
        state.workflow_command_identity = "command-7".to_string();
        let context = WorkflowContext {
            state: Arc::new(Mutex::new(state)),
        };
        let item =
            WorkflowStreamAppendItem::from_reference("s3://bucket/item.avro").item_type("receipt");

        context
            .append_workflow_stream("output", &[item], Some(10))
            .expect("append command");
        context
            .error_workflow_stream("output", "producer failed", None)
            .expect("error command");
        let commands = context.take_commands().expect("commands");

        assert_eq!(commands[0]["type"], "record_side_effect");
        assert_eq!(
            commands[0]["workflow_stream"]["command_identity"],
            "command-7"
        );
        assert_eq!(commands[0]["workflow_stream"]["command_ordinal"], 0);
        assert_eq!(
            commands[0]["workflow_stream"]["items"][0]["idempotency_key"],
            "dw-stream:command-7:0:0"
        );
        assert_eq!(commands[1]["workflow_stream"]["operation"], "error");

        let recorded = history_event(
            "SideEffectRecorded",
            json!({"sequence": 1, "result": fixture_envelope(Value::Null)}),
        );
        let mut replay_state = WorkflowState::new(
            vec![recorded],
            "rust-workers".to_string(),
            DEFAULT_CODEC.to_string(),
            None,
        )
        .expect("replay state");
        replay_state.workflow_command_identity = "command-7".to_string();
        let replay_context = WorkflowContext {
            state: Arc::new(Mutex::new(replay_state)),
        };
        replay_context
            .append_workflow_stream(
                "output",
                &[WorkflowStreamAppendItem::from_reference(
                    "s3://bucket/item.avro",
                )],
                Some(10),
            )
            .expect("replayed append");
        assert!(replay_context
            .take_commands()
            .expect("replayed commands")
            .is_empty());
    }

    #[test]
    fn workflow_stream_authoring_requires_server_durable_command_identity() {
        let context = workflow_context(Vec::new());
        let error = context
            .append_workflow_stream(
                "output",
                &[WorkflowStreamAppendItem::from_reference(
                    "s3://bucket/item.avro",
                )],
                None,
            )
            .expect_err("stream append without durable command identity must fail closed");

        assert!(matches!(error, Error::MissingWorkflowCommandIdentity));
        assert!(context.take_commands().expect("commands").is_empty());
    }

    #[test]
    fn cold_worker_replay_does_not_repeat_committed_side_effects_or_markers() {
        fn worker(calls: Arc<AtomicUsize>) -> Worker {
            let client = Client::new("http://127.0.0.1:8080").expect("client");
            let mut worker = Worker::new(client, "rust-workers");
            worker.register_workflow("rust.side-effect-version", move |ctx, _input| {
                let calls = Arc::clone(&calls);
                async move {
                    let captured = ctx.side_effect(|| {
                        calls.fetch_add(1, Ordering::SeqCst);
                        "captured-once".to_string()
                    })?;
                    let version = ctx.get_version("cold-restart", 1, 2)?;
                    Ok(json!({"captured": captured, "version": version}))
                }
            });
            worker
        }

        fn task(history_events: Vec<HistoryEvent>) -> WorkflowTask {
            WorkflowTask {
                task_id: "wft-side-effect-version".to_string(),
                workflow_command_id: None,
                workflow_id: Some("wf-side-effect-version".to_string()),
                run_id: Some("run-side-effect-version".to_string()),
                workflow_type: "rust.side-effect-version".to_string(),
                cancel_requested: false,
                payload_codec: DEFAULT_CODEC.to_string(),
                arguments: Some(
                    encode_value_envelope(&json!([]), DEFAULT_CODEC).expect("arguments"),
                ),
                history_events,
                total_history_events: None,
                history_size_bytes: None,
                continue_as_new_recommended: None,
                history_budget_pressure: None,
                next_history_page_token: None,
                workflow_task_attempt: 1,
                workflow_signal_id: None,
                signal_name: None,
                signal_arguments: None,
                workflow_update_id: None,
                update_name: None,
                lease_owner: Some("rust-worker".to_string()),
            }
        }

        let calls = Arc::new(AtomicUsize::new(0));
        let initial = worker(Arc::clone(&calls))
            .execute_workflow_task(task(Vec::new()))
            .expect("initial execution");
        assert_eq!(
            initial
                .iter()
                .map(|command| &command["type"])
                .collect::<Vec<_>>(),
            vec![
                "record_side_effect",
                "record_version_marker",
                "complete_workflow"
            ]
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let restarted = worker(Arc::clone(&calls));
        let replayed = restarted
            .execute_workflow_task(task(vec![
                history_event(
                    "SideEffectRecorded",
                    json!({"sequence": 1, "result": initial[0]["result"].clone()}),
                ),
                history_event(
                    "VersionMarkerRecorded",
                    json!({
                        "sequence": 2,
                        "change_id": "cold-restart",
                        "version": 2,
                        "min_supported": 1,
                        "max_supported": 2,
                    }),
                ),
            ]))
            .expect("cold replay");
        assert_eq!(replayed.len(), 1);
        assert_eq!(replayed[0]["type"], "complete_workflow");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn side_effect_replay_rejects_changed_rust_value_type() {
        let result = encode_value_envelope(&json!({"value": 42}), DEFAULT_CODEC).expect("result");
        let ctx = workflow_context(vec![history_event(
            "SideEffectRecorded",
            json!({"sequence": 1, "result": result}),
        )]);
        let error = ctx
            .side_effect::<Vec<String>, _>(|| panic!("callback must not run"))
            .expect_err("changed type must fail replay");
        assert!(matches!(
            error,
            Error::NonDeterministicReplay(ReplayFailure { ref reason, .. })
                if reason == "side_effect_type_mismatch"
        ));
    }

    fn completed_retry_activity_history() -> Vec<HistoryEvent> {
        vec![
            history_event(
                "ActivityScheduled",
                json!({
                    "sequence": 1,
                    "activity_type": "flaky",
                    "activity_execution_id": "act-1",
                    "activity": {
                        "id": "act-1",
                        "sequence": 1,
                        "type": "flaky",
                        "queue": "critical-activities",
                        "execution_mode": null,
                        "retry_policy": {
                            "snapshot_version": 1,
                            "max_attempts": 3,
                            "backoff_seconds": [2, 4],
                            "start_to_close_timeout": 30,
                            "schedule_to_start_timeout": 5,
                            "schedule_to_close_timeout": 90,
                            "heartbeat_timeout": 10,
                            "non_retryable_error_types": ["PermanentError"]
                        }
                    }
                }),
            ),
            history_event(
                "ActivityStarted",
                json!({
                    "sequence": 1,
                    "activity_type": "flaky",
                    "activity_execution_id": "act-1",
                    "activity_attempt_id": "attempt-1",
                    "attempt_number": 1
                }),
            ),
            history_event(
                "ActivityRetryScheduled",
                json!({
                    "sequence": 1,
                    "activity_type": "flaky",
                    "activity_execution_id": "act-1",
                    "activity_attempt_id": "attempt-1",
                    "attempt_number": 1,
                    "retry_after_attempt": 1,
                    "retry_backoff_seconds": 2,
                    "failure_category": "activity",
                    "exception_type": "TransientError"
                }),
            ),
            history_event(
                "ActivityStarted",
                json!({
                    "sequence": 1,
                    "activity_type": "flaky",
                    "activity_execution_id": "act-1",
                    "activity_attempt_id": "attempt-2",
                    "attempt_number": 2
                }),
            ),
            history_event(
                "ActivityCompleted",
                json!({
                    "sequence": 1,
                    "activity_type": "flaky",
                    "activity_execution_id": "act-1",
                    "activity_attempt_id": "attempt-2",
                    "attempt_number": 2,
                    "payload_codec": DEFAULT_CODEC,
                    "result": fixture_envelope(json!({"status":"recovered"}))
                }),
            ),
        ]
    }

    fn retry_activity_options() -> ActivityOptions {
        ActivityOptions::new()
            .task_queue("critical-activities")
            .retry_policy(
                ActivityRetryPolicy::new(3)
                    .backoff_intervals([Duration::from_secs(2), Duration::from_secs(4)])
                    .non_retryable_error_type("PermanentError"),
            )
            .start_to_close_timeout(Duration::from_secs(30))
            .schedule_to_start_timeout(Duration::from_secs(5))
            .schedule_to_close_timeout(Duration::from_secs(90))
            .heartbeat_timeout(Duration::from_secs(10))
    }

    #[test]
    fn fixed_avro_value_round_trips_json_values() {
        let value = json!({"greeting": "hello", "count": 3, "ok": true});
        let envelope = PayloadEnvelope::avro(&value).expect("encode");
        assert_eq!(envelope.codec, DEFAULT_CODEC);
        assert_eq!(decode_payload::<Value>(&envelope).expect("decode"), value);
    }

    #[tokio::test]
    async fn typed_handler_adapters_round_trip_serde_contracts_on_the_fixed_wire() {
        let client = Client::new("http://127.0.0.1:8080").expect("client");
        let mut worker = Worker::new(client, "rust-workers");
        worker.register_typed_workflow(
            "typed.contract.workflow",
            |_ctx, input: TypedContract| async move { Ok(input) },
        );
        worker.register_typed_activity(
            "typed.contract.activity",
            |_ctx, input: TypedContract| async move { Ok(input) },
        );

        let expected = typed_contract();
        let arguments = AvroValue::Array(vec![
            AvroValue::from_serialize(&expected).expect("typed request")
        ]);
        let envelope = encode_typed_envelope(&arguments, DEFAULT_CODEC).expect("arguments");
        let mut workflow = workflow_task("typed.contract.workflow", Vec::new(), DEFAULT_CODEC);
        workflow.arguments = Some(envelope.clone());
        let commands = worker
            .execute_workflow_task(workflow)
            .expect("typed workflow task");
        let workflow_result: TypedContract =
            decode_wire_avro_value(&commands[0]["result"], DEFAULT_CODEC)
                .expect("workflow result envelope")
                .deserialize()
                .expect("workflow result type");
        assert_eq!(workflow_result, expected);

        let activity = ActivityTask {
            task_id: "typed-contract-activity".to_string(),
            activity_attempt_id: Some("typed-contract-attempt".to_string()),
            attempt_id: None,
            activity_type: "typed.contract.activity".to_string(),
            payload_codec: DEFAULT_CODEC.to_string(),
            arguments: Some(envelope),
            attempt_number: 1,
            lease_owner: Some("rust-worker".to_string()),
        };
        let activity_result: TypedContract = worker
            .execute_activity_task(activity)
            .await
            .expect("typed activity task")
            .deserialize()
            .expect("activity result type");
        assert_eq!(activity_result, expected);
    }

    #[tokio::test]
    async fn typed_handler_errors_include_handler_name_direction_and_rust_type() {
        let client = Client::new("http://127.0.0.1:8080").expect("client");
        let mut worker = Worker::new(client, "rust-workers");
        worker.register_typed_workflow(
            "typed.shape.workflow",
            |_ctx, input: TypedContract| async move { Ok(input) },
        );
        worker.register_typed_activity("typed.unsupported.activity", |_ctx, (): ()| async move {
            Ok(f64::NAN)
        });

        let mut workflow = workflow_task("typed.shape.workflow", Vec::new(), DEFAULT_CODEC);
        workflow.arguments = Some(
            encode_typed_envelope(
                &AvroValue::Array(vec![
                    AvroValue::String("first".to_string()),
                    AvroValue::String("second".to_string()),
                ]),
                DEFAULT_CODEC,
            )
            .expect("malformed typed arguments"),
        );
        let commands = worker
            .execute_workflow_task(workflow)
            .expect("shape mismatch becomes a workflow failure");
        let message = commands[0]["message"].as_str().expect("failure message");
        assert!(message.contains("workflow handler \"typed.shape.workflow\" input type"));
        assert!(message.contains(type_name::<TypedContract>()));
        assert!(message.contains("task carried 2 arguments"));

        let activity = ActivityTask {
            task_id: "typed-unsupported-activity".to_string(),
            activity_attempt_id: Some("typed-unsupported-attempt".to_string()),
            attempt_id: None,
            activity_type: "typed.unsupported.activity".to_string(),
            payload_codec: DEFAULT_CODEC.to_string(),
            arguments: Some(
                encode_typed_envelope(&AvroValue::Array(Vec::new()), DEFAULT_CODEC)
                    .expect("unit arguments"),
            ),
            attempt_number: 1,
            lease_owner: Some("rust-worker".to_string()),
        };
        let Error::HandlerType {
            handler_kind,
            handler_name,
            value_kind,
            rust_type,
            message,
        } = worker
            .execute_activity_task(activity)
            .await
            .expect_err("non-finite handler output must fail")
        else {
            panic!("expected contextual handler type failure");
        };
        assert_eq!(handler_kind, HandlerKind::Activity);
        assert_eq!(handler_name, "typed.unsupported.activity");
        assert_eq!(value_kind, HandlerValueKind::Result);
        assert_eq!(rust_type, type_name::<f64>());
        assert!(message.contains("non_finite_float"));
    }

    #[tokio::test]
    async fn typed_replayed_workflow_decodes_input_and_activity_result_losslessly() {
        #[derive(Clone, Default)]
        struct State {
            observed: Option<TypedContract>,
        }

        let client = Client::new("http://127.0.0.1:8080").expect("client");
        let mut worker = Worker::new(client, "rust-workers");
        worker.register_typed_replayed_workflow(
            "typed.contract.replayed",
            State::default,
            |ctx, input: TypedContract, state| async move {
                let result: TypedContract =
                    ctx.activity_typed("typed.contract.activity", input).await?;
                state.update(|current| current.observed = Some(result.clone()))?;
                Ok(result)
            },
        );
        worker.register_replayed_query::<State, _, _>(
            "typed.contract.replayed",
            "observed",
            |_ctx, state, _args| async move {
                Ok(json!(state.observed.as_ref().map(|value| value.signed)))
            },
        );

        let expected = typed_contract();
        let typed_value = AvroValue::from_serialize(&expected).expect("typed value");
        let workflow_arguments =
            encode_typed_envelope(&AvroValue::Array(vec![typed_value.clone()]), DEFAULT_CODEC)
                .expect("workflow arguments");
        let result = encode_typed_envelope(&typed_value, DEFAULT_CODEC).expect("activity result");
        let task = QueryTask {
            query_task_id: "typed-replay-query".to_string(),
            query_task_attempt: 1,
            lease_owner: Some("rust-worker".to_string()),
            workflow_id: Some("typed-replay".to_string()),
            run_id: Some("typed-replay-run".to_string()),
            workflow_type: "typed.contract.replayed".to_string(),
            query_name: "observed".to_string(),
            payload_codec: DEFAULT_CODEC.to_string(),
            workflow_arguments: Some(workflow_arguments),
            query_arguments: Some(
                encode_typed_envelope(&AvroValue::Array(Vec::new()), DEFAULT_CODEC)
                    .expect("query arguments"),
            ),
            history_events: vec![
                history_event(
                    "ActivityScheduled",
                    json!({
                        "sequence": 1,
                        "activity_type": "typed.contract.activity"
                    }),
                ),
                history_event(
                    "ActivityCompleted",
                    json!({
                        "sequence": 1,
                        "activity_type": "typed.contract.activity",
                        "payload_codec": DEFAULT_CODEC,
                        "result": result
                    }),
                ),
            ],
            history_export: None,
            run_status: Some("completed".to_string()),
        };

        assert_eq!(
            worker
                .execute_query_task(task)
                .await
                .expect("typed replay query")
                .deserialize::<i64>()
                .expect("query result"),
            expected.signed
        );
    }

    #[tokio::test]
    async fn typed_worker_surfaces_preserve_bytes_and_map_list_identity() {
        let client = Client::new("http://127.0.0.1:8080").expect("client");
        let mut worker = Worker::new(client, "rust-workers");
        worker.register_workflow_avro_value("typed.echo", |_ctx, input| async move { Ok(input) });
        worker
            .register_activity_avro_value("typed.activity", |_ctx, input| async move { Ok(input) });
        worker.register_query_avro_value("typed.echo", "inspect", |_ctx, input| async move {
            Ok(input)
        });
        worker.register_update_avro_value("typed.echo", "replace", |_ctx, input| async move {
            Ok(input)
        });
        worker.register_workflow_avro_value("typed.signal", |ctx, _input| async move {
            Ok(AvroValue::Array(
                ctx.wait_signal_avro_value("changed").await?,
            ))
        });

        let arguments = AvroValue::Array(vec![typed_fidelity_probe()]);
        let envelope = encode_typed_envelope(&arguments, DEFAULT_CODEC).expect("typed envelope");

        let mut workflow = workflow_task("typed.echo", Vec::new(), DEFAULT_CODEC);
        workflow.arguments = Some(envelope.clone());
        let commands = worker
            .execute_workflow_task(workflow)
            .expect("typed workflow task");
        assert_eq!(commands[0]["type"], "complete_workflow");
        assert_eq!(
            decode_wire_avro_value(&commands[0]["result"], DEFAULT_CODEC)
                .expect("typed workflow result"),
            arguments
        );

        let activity = ActivityTask {
            task_id: "activity-typed".to_string(),
            activity_attempt_id: Some("attempt-typed".to_string()),
            attempt_id: None,
            activity_type: "typed.activity".to_string(),
            payload_codec: DEFAULT_CODEC.to_string(),
            arguments: Some(envelope.clone()),
            attempt_number: 1,
            lease_owner: Some("rust-worker".to_string()),
        };
        assert_eq!(
            worker
                .execute_activity_task(activity)
                .await
                .expect("typed activity result"),
            arguments
        );

        let query = QueryTask {
            query_task_id: "query-typed".to_string(),
            query_task_attempt: 1,
            lease_owner: Some("rust-worker".to_string()),
            workflow_id: Some("typed-1".to_string()),
            run_id: Some("run-typed".to_string()),
            workflow_type: "typed.echo".to_string(),
            query_name: "inspect".to_string(),
            payload_codec: DEFAULT_CODEC.to_string(),
            workflow_arguments: Some(
                encode_typed_envelope(&AvroValue::Array(Vec::new()), DEFAULT_CODEC)
                    .expect("workflow input"),
            ),
            query_arguments: Some(envelope.clone()),
            history_events: Vec::new(),
            history_export: None,
            run_status: Some("running".to_string()),
        };
        assert_eq!(
            worker
                .execute_query_task(query)
                .await
                .expect("typed query result"),
            arguments
        );

        let mut update = workflow_task(
            "typed.echo",
            vec![history_event(
                "UpdateAccepted",
                json!({
                    "update_id": "update-typed",
                    "update_name": "replace",
                    "arguments": envelope.clone(),
                }),
            )],
            DEFAULT_CODEC,
        );
        update.workflow_update_id = Some("update-typed".to_string());
        update.update_name = Some("replace".to_string());
        let commands = worker
            .execute_workflow_task(update)
            .expect("typed update task");
        assert_eq!(commands[0]["type"], "complete_update");
        assert_eq!(
            decode_wire_avro_value(&commands[0]["result"], DEFAULT_CODEC)
                .expect("typed update result"),
            arguments
        );

        let mut signal = workflow_task(
            "typed.signal",
            vec![history_event(
                "SignalReceived",
                json!({
                    "signal_id": "signal-typed",
                    "signal_name": "changed",
                    "arguments": envelope.clone(),
                }),
            )],
            DEFAULT_CODEC,
        );
        signal.workflow_signal_id = Some("signal-typed".to_string());
        signal.signal_name = Some("changed".to_string());
        signal.signal_arguments = Some(envelope);
        let commands = worker
            .execute_workflow_task(signal)
            .expect("typed signal resume");
        assert_eq!(
            decode_wire_avro_value(&commands[0]["result"], DEFAULT_CODEC)
                .expect("typed signal result"),
            arguments
        );
    }

    #[tokio::test]
    async fn typed_helpers_never_parse_json_inspection_projection() {
        let collision_values = projection_collision_probe();
        let expected = AvroValue::Array(collision_values.clone());
        let envelope = encode_typed_envelope(&expected, DEFAULT_CODEC).expect("collision envelope");

        let activity_context = workflow_context_with_codec(
            vec![history_event(
                "ActivityCompleted",
                json!({
                    "sequence": 1,
                    "activity_type": "collision.activity",
                    "payload_codec": DEFAULT_CODEC,
                    "result": envelope.clone(),
                }),
            )],
            DEFAULT_CODEC,
        );
        assert_eq!(
            activity_context
                .activity_avro_value("collision.activity", AvroValue::Array(Vec::new()))
                .await
                .expect("typed activity collision result"),
            expected
        );

        let signal_context = workflow_context_with_codec(
            vec![
                history_event(
                    "SignalWaitOpened",
                    json!({"sequence": 1, "signal_name": "collision"}),
                ),
                history_event(
                    "SignalApplied",
                    json!({
                        "sequence": 1,
                        "signal_name": "collision",
                        "payload_codec": DEFAULT_CODEC,
                        "value": envelope.clone(),
                    }),
                ),
            ],
            DEFAULT_CODEC,
        );
        assert_eq!(
            signal_context
                .wait_signal_avro_value("collision")
                .await
                .expect("typed signal collision arguments"),
            collision_values
        );

        let child_context = workflow_context_with_codec(
            vec![
                history_event(
                    "ChildWorkflowScheduled",
                    json!({
                        "sequence": 1,
                        "child_workflow_instance_id": "collision-child",
                        "child_workflow_run_id": "collision-run",
                        "child_workflow_type": "collision.child",
                    }),
                ),
                history_event(
                    "ChildRunCompleted",
                    json!({
                        "sequence": 1,
                        "child_workflow_instance_id": "collision-child",
                        "child_workflow_run_id": "collision-run",
                        "child_workflow_type": "collision.child",
                        "payload_codec": DEFAULT_CODEC,
                        "result": envelope,
                    }),
                ),
            ],
            DEFAULT_CODEC,
        );
        let child = child_context
            .start_child_workflow_avro_value(
                "collision.child",
                ChildWorkflowOptions::new("collision-workers"),
                AvroValue::Array(Vec::new()),
            )
            .await
            .expect("typed child collision result");
        assert_eq!(child.result, expected);
    }

    #[tokio::test]
    async fn replayed_typed_query_keeps_lossless_workflow_and_query_inputs() {
        let client = Client::new("http://127.0.0.1:8080").expect("client");
        let mut worker = Worker::new(client, "rust-workers");
        worker.register_replayed_workflow_avro_value(
            "typed.replayed",
            || (),
            |_ctx, input, _state| async move { Ok(input) },
        );
        worker.register_replayed_query_avro_value::<(), _, _>(
            "typed.replayed",
            "inspect",
            |ctx, _state, args| async move {
                let mut signals = ctx.signals_avro_value("collision");
                let signal = signals
                    .pop()
                    .map(AvroValue::Array)
                    .unwrap_or_else(|| AvroValue::Array(Vec::new()));
                Ok(AvroValue::Array(vec![
                    ctx.workflow_input_avro_value().clone(),
                    signal,
                    args,
                ]))
            },
        );
        let arguments = AvroValue::Array(projection_collision_probe());
        let signal_arguments =
            encode_typed_envelope(&arguments, DEFAULT_CODEC).expect("typed query signal arguments");
        let task = QueryTask {
            query_task_id: "query-typed-replay".to_string(),
            query_task_attempt: 1,
            lease_owner: Some("rust-worker".to_string()),
            workflow_id: Some("typed-replay".to_string()),
            run_id: Some("run-typed-replay".to_string()),
            workflow_type: "typed.replayed".to_string(),
            query_name: "inspect".to_string(),
            payload_codec: DEFAULT_CODEC.to_string(),
            workflow_arguments: Some(
                encode_typed_envelope(&arguments, DEFAULT_CODEC).expect("workflow arguments"),
            ),
            query_arguments: Some(
                encode_typed_envelope(&arguments, DEFAULT_CODEC).expect("query arguments"),
            ),
            history_events: vec![history_event(
                "SignalReceived",
                json!({
                    "signal_id": "collision-signal",
                    "signal_name": "collision",
                    "workflow_sequence": 1,
                    "payload_codec": DEFAULT_CODEC,
                    "arguments": signal_arguments,
                }),
            )],
            history_export: None,
            run_status: Some("completed".to_string()),
        };

        assert_eq!(
            worker
                .execute_query_task(task)
                .await
                .expect("typed replay query"),
            AvroValue::Array(vec![arguments.clone(), arguments.clone(), arguments])
        );
    }

    #[test]
    fn public_avro_adapter_rejects_non_string_map_keys_before_json_conversion() {
        let value = BTreeMap::from([(1_i32, "integer key")]);
        let error = PayloadEnvelope::avro(&value)
            .expect_err("integer map keys must fail")
            .to_string();

        assert!(error.contains("invalid_map_key"));
    }

    #[test]
    fn json_tagged_payload_fails_closed_with_actionable_diagnostic() {
        let envelope = PayloadEnvelope {
            codec: "json".to_string(),
            blob: r#"{"greeting":"hello"}"#.to_string(),
        };

        let error = decode_payload::<Value>(&envelope).expect_err("JSON payload must fail");
        let diagnostic = error.to_string();
        assert!(diagnostic.contains("unsupported_payload_codec"));
        assert!(diagnostic.contains("codec=\"avro\""));
        assert!(diagnostic.contains("HTTP document transport"));
    }

    #[test]
    fn untagged_json_payload_value_fails_closed() {
        let error = decode_wire_value(&json!({"stale": true}), DEFAULT_CODEC)
            .expect_err("untagged JSON payload values must fail");
        let diagnostic = error.to_string();
        assert!(diagnostic.contains("unsupported_payload_codec"));
        assert!(diagnostic.contains("untagged durable payload"));
        assert!(diagnostic.contains("HTTP document transport"));
    }

    #[test]
    fn prerelease_avro_payload_without_single_object_frame_is_rejected() {
        let envelope = PayloadEnvelope {
            codec: DEFAULT_CODEC.to_string(),
            blob: BASE64.encode([0x01]),
        };

        let error = decode_payload::<Value>(&envelope).expect_err("prerelease payload must fail");
        assert!(error.to_string().contains("invalid_payload_framing"));
    }

    #[tokio::test]
    async fn workflow_completion_rejects_invalid_payload_slots_without_transport() {
        let server = MockWorkerServer::start();
        let client = Client::builder(server.base_url())
            .timeout(Duration::from_secs(2))
            .build()
            .expect("client");
        let invalid_commands = [
            json!({
                "type": "complete_workflow",
                "result": {"codec": "json", "blob": null}
            }),
            json!({
                "type": "schedule_activity",
                "arguments": {"codec": "yaml", "blob": "ignored"}
            }),
            json!({
                "type": "start_child_workflow",
                "arguments": {"codec": DEFAULT_CODEC, "blob": null}
            }),
            json!({"type": "continue_as_new", "arguments": []}),
            json!({"type": "complete_update"}),
            json!({"type": "record_side_effect", "result": null}),
            json!({
                "type": "start_service_operation",
                "payload_codec": DEFAULT_CODEC,
                "request_payload": "raw-avro-bytes"
            }),
        ];

        for command in invalid_commands {
            let error = client
                .complete_workflow_task("invalid-codec", "rust-worker", 1, vec![command])
                .await
                .expect_err("invalid durable payload must fail locally");
            let diagnostic = error.to_string();
            assert!(
                diagnostic.contains("unsupported_payload_codec")
                    || diagnostic.contains("invalid_payload_envelope")
                    || diagnostic.contains("untagged durable payload"),
                "unexpected validation diagnostic: {diagnostic}"
            );
        }

        assert_eq!(
            server.request_count("/api/worker/workflow-tasks/invalid-codec/complete"),
            0,
            "invalid command payloads must not reach HTTP transport"
        );
    }

    #[test]
    fn workflow_completion_validates_only_protocol_owned_payload_slots() {
        let envelope = fixture_envelope(json!({"codec": "customer-value"}));
        let commands = [
            json!({"type": "complete_workflow", "result": envelope.clone()}),
            json!({"type": "schedule_activity", "arguments": envelope.clone()}),
            json!({"type": "start_child_workflow", "arguments": envelope.clone()}),
            json!({"type": "continue_as_new", "arguments": envelope.clone()}),
            json!({"type": "complete_update", "result": envelope.clone()}),
            json!({"type": "record_side_effect", "result": envelope.clone()}),
            json!({
                "type": "start_service_operation",
                "payload_codec": DEFAULT_CODEC,
                "request_payload": envelope.clone()
            }),
            json!({
                "type": "complete_workflow",
                "result": envelope,
                "metadata": {
                    "codec": "json",
                    "payload_codec": "customer-codec",
                    "result": {"codec": "yaml", "blob": null}
                }
            }),
        ];

        validate_workflow_task_commands(&commands)
            .expect("customer metadata must not become a protocol codec declaration");
    }

    #[test]
    fn valid_avro_tasks_normalize_absent_and_null_arguments_to_empty_lists() {
        assert_eq!(
            decode_task_avro_arguments(None, DEFAULT_CODEC).expect("absent arguments"),
            AvroValue::Array(Vec::new())
        );
        assert_eq!(
            decode_task_avro_arguments(Some(&Value::Null), DEFAULT_CODEC).expect("null arguments"),
            AvroValue::Array(Vec::new())
        );

        let mut signal = workflow_task("missing", Vec::new(), DEFAULT_CODEC);
        signal.signal_name = Some("empty-signal".to_string());
        signal.signal_arguments = None;
        let decoded = decode_resume_signal(&signal)
            .expect("valid Avro signal")
            .expect("named signal resumes the workflow");
        assert!(decoded.arguments.is_empty());
    }

    #[tokio::test]
    async fn malformed_task_level_codecs_become_pre_handler_failures() {
        let client = Client::new("http://127.0.0.1:8080").expect("client");
        let mut worker = Worker::new(client, "rust-workers");
        let handler_calls = Arc::new(AtomicUsize::new(0));

        let calls = Arc::clone(&handler_calls);
        worker.register_workflow("codec.workflow", move |_ctx, _args| {
            calls.fetch_add(1, Ordering::SeqCst);
            async move { Ok(Value::Null) }
        });
        let calls = Arc::clone(&handler_calls);
        worker.register_activity("codec.activity", move |_ctx, _args| {
            calls.fetch_add(1, Ordering::SeqCst);
            async move { Ok(Value::Null) }
        });
        let calls = Arc::clone(&handler_calls);
        worker.register_query("codec.workflow", "known", move |_ctx, _args| {
            calls.fetch_add(1, Ordering::SeqCst);
            async move { Ok(Value::Null) }
        });

        let mut failures = Vec::new();
        for codec_case in [
            InvalidTaskPayloadCodec::Missing,
            InvalidTaskPayloadCodec::Null,
            InvalidTaskPayloadCodec::NonString,
        ] {
            let mut workflow = json!({
                "task_id": format!("workflow-{}", codec_case.label()),
                "workflow_type": "codec.workflow"
            });
            codec_case.apply(&mut workflow);
            match serde_json::from_value::<WorkflowTask>(workflow) {
                Ok(task) => match worker.execute_workflow_task(task) {
                    Err(error) if error.to_string().contains("unsupported_payload_codec") => {}
                    outcome => failures.push(format!(
                        "workflow {} codec returned {outcome:?}",
                        codec_case.label()
                    )),
                },
                Err(error) => failures.push(format!(
                    "workflow {} codec failed transport deserialization: {error}",
                    codec_case.label()
                )),
            }

            let mut activity = json!({
                "task_id": format!("activity-{}", codec_case.label()),
                "activity_attempt_id": format!("attempt-{}", codec_case.label()),
                "activity_type": "codec.activity",
                "attempt_number": 1
            });
            codec_case.apply(&mut activity);
            match serde_json::from_value::<ActivityTask>(activity) {
                Ok(task) => match worker.execute_activity_task(task).await {
                    Err(error) if error.to_string().contains("unsupported_payload_codec") => {}
                    outcome => failures.push(format!(
                        "activity {} codec returned {outcome:?}",
                        codec_case.label()
                    )),
                },
                Err(error) => failures.push(format!(
                    "activity {} codec failed transport deserialization: {error}",
                    codec_case.label()
                )),
            }

            let mut query = json!({
                "query_task_id": format!("query-{}", codec_case.label()),
                "workflow_type": "codec.workflow",
                "query_name": "known"
            });
            codec_case.apply(&mut query);
            match serde_json::from_value::<QueryTask>(query) {
                Ok(task) => match worker.execute_query_task(task).await {
                    Err(failure) if failure.message.contains("unsupported_payload_codec") => {}
                    outcome => failures.push(format!(
                        "query {} codec returned {outcome:?}",
                        codec_case.label()
                    )),
                },
                Err(error) => failures.push(format!(
                    "query {} codec failed transport deserialization: {error}",
                    codec_case.label()
                )),
            }
        }

        assert!(failures.is_empty(), "{}", failures.join("\n"));
        assert_eq!(
            handler_calls.load(Ordering::SeqCst),
            0,
            "invalid task codecs must not invoke a handler"
        );
    }

    #[tokio::test]
    async fn polled_malformed_task_codecs_are_settled_without_handler_execution() {
        for codec_case in [
            InvalidTaskPayloadCodec::Missing,
            InvalidTaskPayloadCodec::Null,
            InvalidTaskPayloadCodec::NonString,
        ] {
            let server = MockWorkerServer::invalid_task_payload_codec(codec_case);
            let client = Client::builder(server.base_url())
                .timeout(Duration::from_secs(2))
                .build()
                .expect("client");
            let mut worker = Worker::new(client, "rust-workers")
                .worker_id("codec-worker")
                .poll_timeout(Duration::from_millis(10));
            let handler_calls = Arc::new(AtomicUsize::new(0));

            let calls = Arc::clone(&handler_calls);
            worker.register_workflow("codec.workflow", move |_ctx, _args| {
                calls.fetch_add(1, Ordering::SeqCst);
                async move { Ok(Value::Null) }
            });
            let calls = Arc::clone(&handler_calls);
            worker.register_activity("codec.activity", move |_ctx, _args| {
                calls.fetch_add(1, Ordering::SeqCst);
                async move { Ok(Value::Null) }
            });
            let calls = Arc::clone(&handler_calls);
            worker.register_query("codec.workflow", "known", move |_ctx, _args| {
                calls.fetch_add(1, Ordering::SeqCst);
                async move { Ok(Value::Null) }
            });

            assert_eq!(
                worker.run_once().await.expect("invalid tasks are settled"),
                3,
                "all {} codec tasks must be handled",
                codec_case.label()
            );
            assert_eq!(
                handler_calls.load(Ordering::SeqCst),
                0,
                "{} task codecs must fail before every handler",
                codec_case.label()
            );

            for path in [
                "/api/worker/workflow-tasks/codec-workflow/fail",
                "/api/worker/activity-tasks/codec-activity/fail",
                "/api/worker/query-tasks/codec-query/fail",
            ] {
                let body = server.request_body(path);
                assert!(
                    body["failure"]["message"]
                        .as_str()
                        .is_some_and(|message| message.contains("unsupported_payload_codec")),
                    "{path} must receive the stable codec diagnostic for the {} case: {body}",
                    codec_case.label()
                );
            }
            assert_eq!(
                server.request_body("/api/worker/query-tasks/codec-query/fail")["failure"]
                    ["reason"],
                "query_payload_decode_failed"
            );
            for path in [
                "/api/worker/workflow-tasks/codec-workflow/complete",
                "/api/worker/activity-tasks/codec-activity/complete",
                "/api/worker/query-tasks/codec-query/complete",
            ] {
                assert_eq!(
                    server.request_count(path),
                    0,
                    "invalid {} codec task reached {path}",
                    codec_case.label()
                );
            }
        }
    }

    #[tokio::test]
    async fn invalid_inbound_codecs_precede_handlers_and_unrelated_outcomes() {
        let client = Client::new("http://127.0.0.1:8080").expect("client");
        let mut worker = Worker::new(client, "rust-workers");
        let handler_calls = Arc::new(AtomicUsize::new(0));

        let calls = Arc::clone(&handler_calls);
        worker.register_workflow("codec.workflow", move |_ctx, _args| {
            calls.fetch_add(1, Ordering::SeqCst);
            async move { Ok(Value::Null) }
        });
        let calls = Arc::clone(&handler_calls);
        worker.register_activity("codec.activity", move |_ctx, _args| {
            calls.fetch_add(1, Ordering::SeqCst);
            async move { Ok(Value::Null) }
        });
        let calls = Arc::clone(&handler_calls);
        worker.register_update("codec.workflow", "known", move |_ctx, _args| {
            calls.fetch_add(1, Ordering::SeqCst);
            async move { Ok(Value::Null) }
        });
        let calls = Arc::clone(&handler_calls);
        worker.register_query("codec.workflow", "known", move |_ctx, _args| {
            calls.fetch_add(1, Ordering::SeqCst);
            async move { Ok(Value::Null) }
        });

        let mut workflow = workflow_task("codec.workflow", Vec::new(), DEFAULT_CODEC);
        workflow.payload_codec = "json".to_string();
        workflow.arguments = None;
        let error = worker
            .execute_workflow_task(workflow)
            .expect_err("task codec must be checked before workflow invocation");
        assert!(error.to_string().contains("unsupported_payload_codec"));

        let activity = ActivityTask {
            task_id: "activity-invalid-codec".to_string(),
            activity_attempt_id: None,
            attempt_id: None,
            activity_type: "codec.activity".to_string(),
            payload_codec: "unknown".to_string(),
            arguments: None,
            attempt_number: 1,
            lease_owner: None,
        };
        let error = worker
            .execute_activity_task(activity)
            .await
            .expect_err("task codec must be checked before activity invocation");
        assert!(error.to_string().contains("unsupported_payload_codec"));

        let mut update = workflow_task("codec.workflow", Vec::new(), DEFAULT_CODEC);
        update.workflow_update_id = Some("update-invalid-codec".to_string());
        update.update_name = Some("known".to_string());
        update.history_events.push(history_event(
            "UpdateAccepted",
            json!({
                "update_id": "update-invalid-codec",
                "update_name": "known",
                "arguments": {"codec": "json", "blob": null}
            }),
        ));
        let error = worker
            .execute_workflow_task(update)
            .expect_err("nested update codec must be checked before handler lookup");
        assert!(error.to_string().contains("unsupported_payload_codec"));

        let query: QueryTask = serde_json::from_value(json!({
            "query_task_id": "query-invalid-codec",
            "workflow_type": "codec.workflow",
            "query_name": "known",
            "payload_codec": DEFAULT_CODEC,
            "workflow_arguments": null,
            "query_arguments": null,
            "history_export": {
                "payloads": {"codec": DEFAULT_CODEC},
                "signals": [{
                    "name": "empty",
                    "payload_codec": "json",
                    "arguments": null
                }]
            }
        }))
        .expect("query task");
        let failure = worker
            .execute_query_task(query)
            .await
            .expect_err("exported signal codec must be checked before query invocation");
        assert_eq!(failure.reason, "query_payload_decode_failed");
        assert!(failure.message.contains("unsupported_payload_codec"));

        let exported_history: QueryTask = serde_json::from_value(json!({
            "query_task_id": "query-invalid-history-codec",
            "workflow_type": "codec.workflow",
            "query_name": "known",
            "payload_codec": DEFAULT_CODEC,
            "history_export": {
                "payloads": {"codec": DEFAULT_CODEC},
                "history_events": [{
                    "type": "ActivityCompleted",
                    "payload": {"payload_codec": "unknown", "result": null}
                }]
            }
        }))
        .expect("query task");
        let failure = worker
            .execute_query_task(exported_history)
            .await
            .expect_err("exported history codec must be checked before query invocation");
        assert_eq!(failure.reason, "query_payload_decode_failed");
        assert!(failure.message.contains("unsupported_payload_codec"));
        assert_eq!(handler_calls.load(Ordering::SeqCst), 0);

        let mut unknown_workflow = workflow_task("missing", Vec::new(), DEFAULT_CODEC);
        unknown_workflow.arguments = None;
        unknown_workflow.history_events.push(history_event(
            "SignalReceived",
            json!({
                "signal_name": "empty",
                "payload_codec": "json",
                "arguments": null
            }),
        ));
        let error = worker
            .execute_workflow_task(unknown_workflow)
            .expect_err("history codec must precede unknown workflow outcome");
        assert!(error.to_string().contains("unsupported_payload_codec"));

        let unknown_activity = ActivityTask {
            task_id: "activity-unknown".to_string(),
            activity_attempt_id: None,
            attempt_id: None,
            activity_type: "missing".to_string(),
            payload_codec: "json".to_string(),
            arguments: None,
            attempt_number: 1,
            lease_owner: None,
        };
        let error = worker
            .execute_activity_task(unknown_activity)
            .await
            .expect_err("codec must precede unknown activity outcome");
        assert!(error.to_string().contains("unsupported_payload_codec"));

        let mut unknown_update = workflow_task("codec.workflow", Vec::new(), DEFAULT_CODEC);
        unknown_update.payload_codec = "json".to_string();
        unknown_update.arguments = None;
        unknown_update.workflow_update_id = Some("update-unknown".to_string());
        unknown_update.update_name = Some("missing".to_string());
        let error = worker
            .execute_workflow_task(unknown_update)
            .expect_err("codec must precede fail_update shortcut");
        assert!(error.to_string().contains("unsupported_payload_codec"));

        let unknown_query: QueryTask = serde_json::from_value(json!({
            "query_task_id": "query-unknown",
            "workflow_type": "missing",
            "query_name": "missing",
            "payload_codec": "json",
            "workflow_arguments": null,
            "query_arguments": null
        }))
        .expect("query task");
        let failure = worker
            .execute_query_task(unknown_query)
            .await
            .expect_err("codec must precede unknown query outcome");
        assert_eq!(failure.reason, "query_payload_decode_failed");
        assert!(failure.message.contains("unsupported_payload_codec"));
    }

    #[tokio::test]
    async fn invalid_signal_history_payload_aliases_precede_shortcuts() {
        let client = Client::new("http://127.0.0.1:8080").expect("client");
        let worker = Worker::new(client, "rust-workers");

        for event_type in ["SignalReceived", "SignalApplied"] {
            for (payload_field, codec) in [
                ("value", "json"),
                ("input", "unknown"),
                ("arguments", "json"),
            ] {
                let payload = json!({
                    "signal_name": "empty",
                    payload_field: {"codec": codec, "blob": null}
                });
                let workflow = workflow_task(
                    "missing",
                    vec![history_event(event_type, payload.clone())],
                    DEFAULT_CODEC,
                );
                let error = worker
                    .execute_workflow_task(workflow)
                    .expect_err("signal payload codec must precede unknown workflow outcome");
                assert!(
                    error.to_string().contains("unsupported_payload_codec"),
                    "{event_type}.{payload_field} returned an unrelated workflow error: {error}"
                );

                let query: QueryTask = serde_json::from_value(json!({
                    "query_task_id": format!("query-{event_type}-{payload_field}"),
                    "workflow_type": "missing",
                    "query_name": "missing",
                    "payload_codec": DEFAULT_CODEC,
                    "workflow_arguments": null,
                    "query_arguments": null,
                    "history_events": [{
                        "event_type": event_type,
                        "payload": payload
                    }]
                }))
                .expect("query task");
                let failure = worker
                    .execute_query_task(query)
                    .await
                    .expect_err("signal payload codec must precede unknown query outcome");
                assert_eq!(
                    failure.reason, "query_payload_decode_failed",
                    "{event_type}.{payload_field} returned an unrelated query outcome"
                );
                assert!(
                    failure.message.contains("unsupported_payload_codec"),
                    "{event_type}.{payload_field} returned an unrelated query error: {}",
                    failure.message
                );
            }
        }
    }

    #[test]
    fn workflow_context_schedules_activity_until_completion_is_in_history() {
        let ctx = WorkflowContext {
            state: Arc::new(Mutex::new(
                WorkflowState::new_with_identity(
                    Vec::new(),
                    Some("wf-parent".to_string()),
                    Some("run-parent".to_string()),
                    "rust-workers".to_string(),
                    DEFAULT_CODEC.to_string(),
                    None,
                )
                .expect("workflow state"),
            )),
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
    fn activity_options_encode_retry_policy_queue_and_every_timeout() {
        let ctx = workflow_context(Vec::new());
        let options = ActivityOptions::new()
            .task_queue("payments")
            .retry_policy(
                ActivityRetryPolicy::new(4)
                    .exponential_backoff(Duration::from_secs(1), 3, Some(Duration::from_secs(10)))
                    .non_retryable_error_type("ValidationError"),
            )
            .start_to_close_timeout(Duration::from_secs(120))
            .schedule_to_start_timeout(Duration::from_secs(10))
            .schedule_to_close_timeout(Duration::from_secs(300))
            .heartbeat_timeout(Duration::from_secs(15));
        let mut call = Box::pin(ctx.activity_with_options(
            "charge-card",
            options,
            json!([{"order_id": "o-1"}]),
        ));
        let mut task_context = TaskContext::from_waker(noop_waker_ref());

        assert!(matches!(
            call.as_mut().poll(&mut task_context),
            Poll::Pending
        ));
        assert!(matches!(
            call.as_mut().poll(&mut task_context),
            Poll::Pending
        ));

        let commands = ctx.take_commands().expect("activity command");
        assert_eq!(commands.len(), 1, "one future emits one logical schedule");
        assert_eq!(commands[0]["queue"], "payments");
        assert_eq!(
            commands[0]["retry_policy"],
            json!({
                "max_attempts": 4,
                "backoff_seconds": [1, 3, 9],
                "non_retryable_error_types": ["ValidationError"],
            })
        );
        assert_eq!(commands[0]["start_to_close_timeout"], 120);
        assert_eq!(commands[0]["schedule_to_start_timeout"], 10);
        assert_eq!(commands[0]["schedule_to_close_timeout"], 300);
        assert_eq!(commands[0]["heartbeat_timeout"], 15);
    }

    #[test]
    fn activity_options_encode_explicit_and_rounded_backoff_intervals() {
        let ctx = workflow_context(Vec::new());
        let options = ActivityOptions::new().retry_policy(
            ActivityRetryPolicy::new(3)
                .backoff_intervals([Duration::from_millis(1), Duration::from_millis(1_001)]),
        );
        let mut call = Box::pin(ctx.activity_with_options("work", options, json!([])));
        let mut task_context = TaskContext::from_waker(noop_waker_ref());

        assert!(matches!(
            call.as_mut().poll(&mut task_context),
            Poll::Pending
        ));
        assert_eq!(
            ctx.take_commands().expect("command")[0]["retry_policy"]["backoff_seconds"],
            json!([1, 2])
        );
    }

    #[test]
    fn invalid_activity_options_return_typed_errors_before_emitting_commands() {
        let cases = [
            (
                ActivityOptions::new().task_queue("  "),
                ActivityOptionsErrorKind::EmptyTaskQueue,
            ),
            (
                ActivityOptions::new().retry_policy(ActivityRetryPolicy::default()),
                ActivityOptionsErrorKind::EmptyRetryPolicy,
            ),
            (
                ActivityOptions::new().retry_policy(ActivityRetryPolicy::new(0)),
                ActivityOptionsErrorKind::InvalidMaxAttempts,
            ),
            (
                ActivityOptions::new().retry_policy(ActivityRetryPolicy {
                    max_attempts: None,
                    backoff: Some(ActivityBackoff::Explicit(vec![Duration::from_secs(1)])),
                    non_retryable_error_types: Vec::new(),
                }),
                ActivityOptionsErrorKind::BackoffWithoutRetryBudget,
            ),
            (
                ActivityOptions::new().retry_policy(
                    ActivityRetryPolicy::new(2)
                        .backoff_intervals([Duration::from_secs(1), Duration::from_secs(2)]),
                ),
                ActivityOptionsErrorKind::TooManyBackoffIntervals,
            ),
            (
                ActivityOptions::new().retry_policy(
                    ActivityRetryPolicy::new(2).exponential_backoff(
                        Duration::from_secs(1),
                        0,
                        None,
                    ),
                ),
                ActivityOptionsErrorKind::InvalidBackoffCoefficient,
            ),
            (
                ActivityOptions::new()
                    .retry_policy(ActivityRetryPolicy::new(2).non_retryable_error_type("  ")),
                ActivityOptionsErrorKind::EmptyNonRetryableErrorType,
            ),
            (
                ActivityOptions::new().retry_policy(
                    ActivityRetryPolicy::new(10_002).exponential_backoff(
                        Duration::from_secs(1),
                        1,
                        None,
                    ),
                ),
                ActivityOptionsErrorKind::BackoffGenerationTooLarge,
            ),
            (
                ActivityOptions::new().retry_policy(
                    ActivityRetryPolicy::new(2)
                        .backoff_intervals([Duration::from_secs(i64::MAX as u64 + 1)]),
                ),
                ActivityOptionsErrorKind::BackoffOverflow,
            ),
        ];

        for (options, expected_kind) in cases {
            let ctx = workflow_context(Vec::new());
            let mut call = Box::pin(ctx.activity_with_options("work", options, json!([])));
            let mut task_context = TaskContext::from_waker(noop_waker_ref());
            let Poll::Ready(Err(Error::InvalidActivityOptions(error))) =
                call.as_mut().poll(&mut task_context)
            else {
                panic!("expected typed activity validation error");
            };
            assert_eq!(error.kind, expected_kind);
            assert!(ctx.take_commands().expect("commands").is_empty());
        }
    }

    #[test]
    fn activity_options_validate_positive_and_ordered_timeouts() {
        let zero_timeout_cases = [
            ActivityOptions::new().start_to_close_timeout(Duration::ZERO),
            ActivityOptions::new().schedule_to_start_timeout(Duration::ZERO),
            ActivityOptions::new().schedule_to_close_timeout(Duration::ZERO),
            ActivityOptions::new().heartbeat_timeout(Duration::ZERO),
        ];
        for options in zero_timeout_cases {
            assert_eq!(
                options.validate().expect_err("zero timeout").kind,
                ActivityOptionsErrorKind::TimeoutNotPositive
            );
        }

        let ordering_cases = [
            ActivityOptions::new()
                .heartbeat_timeout(Duration::from_secs(11))
                .start_to_close_timeout(Duration::from_secs(10)),
            ActivityOptions::new()
                .start_to_close_timeout(Duration::from_secs(31))
                .schedule_to_close_timeout(Duration::from_secs(30)),
            ActivityOptions::new()
                .schedule_to_start_timeout(Duration::from_secs(31))
                .schedule_to_close_timeout(Duration::from_secs(30)),
        ];
        for options in ordering_cases {
            assert_eq!(
                options.validate().expect_err("timeout order").kind,
                ActivityOptionsErrorKind::TimeoutOrder
            );
        }

        assert_eq!(
            ActivityOptions::new()
                .start_to_close_timeout(Duration::from_secs(i64::MAX as u64 + 1))
                .validate()
                .expect_err("protocol integer overflow")
                .kind,
            ActivityOptionsErrorKind::TimeoutOverflow
        );
    }

    #[test]
    fn replayed_activity_retry_history_completes_without_duplicate_schedule() {
        let ctx = workflow_context(completed_retry_activity_history());
        let mut call =
            Box::pin(ctx.activity_with_options("flaky", retry_activity_options(), json!([])));
        let mut task_context = TaskContext::from_waker(noop_waker_ref());

        assert!(matches!(
            call.as_mut().poll(&mut task_context),
            Poll::Ready(Ok(result)) if result == json!({"status": "recovered"})
        ));
        assert!(ctx.take_commands().expect("commands").is_empty());
        ctx.ensure_history_consumed().expect("history consumed");
    }

    #[test]
    fn duplicate_non_retryable_types_use_one_command_and_replay_representation() {
        let mut options = retry_activity_options();
        options
            .retry_policy
            .as_mut()
            .expect("retry policy")
            .non_retryable_error_types
            .extend([" PermanentError ".to_string(), "PermanentError".to_string()]);

        let new_ctx = workflow_context(Vec::new());
        let mut new_call =
            Box::pin(new_ctx.activity_with_options("flaky", options.clone(), json!([])));
        let mut task_context = TaskContext::from_waker(noop_waker_ref());
        assert!(matches!(
            new_call.as_mut().poll(&mut task_context),
            Poll::Pending
        ));
        let commands = new_ctx.take_commands().expect("commands");
        assert_eq!(commands.len(), 1);
        assert_eq!(
            commands[0]["retry_policy"]["non_retryable_error_types"],
            json!(["PermanentError"])
        );

        let replay_ctx = workflow_context(completed_retry_activity_history());
        let mut replay_call =
            Box::pin(replay_ctx.activity_with_options("flaky", options, json!([])));
        assert!(matches!(
            replay_call.as_mut().poll(&mut task_context),
            Poll::Ready(Ok(result)) if result == json!({"status": "recovered"})
        ));
        assert!(replay_ctx.take_commands().expect("commands").is_empty());
        replay_ctx
            .ensure_history_consumed()
            .expect("history consumed");
    }

    #[test]
    fn replayed_intermediate_retry_remains_pending_across_restarts() {
        let history = completed_retry_activity_history()
            .into_iter()
            .take(3)
            .collect::<Vec<_>>();

        for _restart in 0..2 {
            let ctx = workflow_context(history.clone());
            let mut call =
                Box::pin(ctx.activity_with_options("flaky", retry_activity_options(), json!([])));
            let mut task_context = TaskContext::from_waker(noop_waker_ref());
            assert!(matches!(
                call.as_mut().poll(&mut task_context),
                Poll::Pending
            ));
            assert!(ctx.take_commands().expect("commands").is_empty());
        }
    }

    #[test]
    fn replayed_activity_rejects_changed_queue_retry_and_every_timeout_field() {
        let mut changed_queue = retry_activity_options();
        changed_queue.task_queue = Some("different-queue".to_string());

        let mut changed_max_attempts = retry_activity_options();
        let retry_policy = changed_max_attempts
            .retry_policy
            .as_mut()
            .expect("retry policy");
        retry_policy.max_attempts = Some(4);

        let mut changed_backoff = retry_activity_options();
        let retry_policy = changed_backoff.retry_policy.as_mut().expect("retry policy");
        retry_policy.backoff = Some(ActivityBackoff::Explicit(vec![
            Duration::from_secs(3),
            Duration::from_secs(4),
        ]));

        let mut changed_non_retryable_types = retry_activity_options();
        let retry_policy = changed_non_retryable_types
            .retry_policy
            .as_mut()
            .expect("retry policy");
        retry_policy.non_retryable_error_types = vec!["AnotherPermanentError".to_string()];

        let mut changed_start_to_close = retry_activity_options();
        changed_start_to_close.start_to_close_timeout = Some(Duration::from_secs(31));
        let mut changed_schedule_to_start = retry_activity_options();
        changed_schedule_to_start.schedule_to_start_timeout = Some(Duration::from_secs(6));
        let mut changed_schedule_to_close = retry_activity_options();
        changed_schedule_to_close.schedule_to_close_timeout = Some(Duration::from_secs(91));
        let mut changed_heartbeat = retry_activity_options();
        changed_heartbeat.heartbeat_timeout = Some(Duration::from_secs(11));

        let cases = [
            (changed_queue, "activity_task_queue_mismatch"),
            (changed_max_attempts, "activity_retry_policy_mismatch"),
            (changed_backoff, "activity_retry_policy_mismatch"),
            (
                changed_non_retryable_types,
                "activity_retry_policy_mismatch",
            ),
            (changed_start_to_close, "activity_retry_policy_mismatch"),
            (changed_schedule_to_start, "activity_retry_policy_mismatch"),
            (changed_schedule_to_close, "activity_retry_policy_mismatch"),
            (changed_heartbeat, "activity_retry_policy_mismatch"),
        ];

        for (options, expected_reason) in cases {
            let ctx = workflow_context(completed_retry_activity_history());
            let mut call = Box::pin(ctx.activity_with_options("flaky", options, json!([])));
            let mut task_context = TaskContext::from_waker(noop_waker_ref());
            let Poll::Ready(Err(Error::NonDeterministicReplay(failure))) =
                call.as_mut().poll(&mut task_context)
            else {
                panic!("changed activity options must fail replay");
            };
            assert_eq!(failure.reason, expected_reason);
            assert_eq!(failure.sequence, Some(1));
            assert!(ctx.take_commands().expect("commands").is_empty());
        }
    }

    #[test]
    fn replayed_activity_rejects_changed_execution_mode_and_snapshot_version() {
        let cases = [
            (
                "execution_mode",
                json!("local"),
                "activity_execution_mode_mismatch",
            ),
            (
                "snapshot_version",
                json!(2),
                "activity_retry_policy_mismatch",
            ),
        ];

        for (field, value, expected_reason) in cases {
            let mut history = completed_retry_activity_history();
            let activity = history[0].payload["activity"]
                .as_object_mut()
                .expect("activity snapshot");
            if field == "execution_mode" {
                activity.insert(field.to_string(), value);
            } else {
                activity["retry_policy"]
                    .as_object_mut()
                    .expect("retry snapshot")
                    .insert(field.to_string(), value);
            }

            let ctx = workflow_context(history);
            let mut call =
                Box::pin(ctx.activity_with_options("flaky", retry_activity_options(), json!([])));
            let mut task_context = TaskContext::from_waker(noop_waker_ref());
            let Poll::Ready(Err(Error::NonDeterministicReplay(failure))) =
                call.as_mut().poll(&mut task_context)
            else {
                panic!("changed {field} must fail replay");
            };
            assert_eq!(failure.reason, expected_reason);
            assert_eq!(failure.sequence, Some(1));
            assert!(ctx.take_commands().expect("commands").is_empty());
        }
    }

    #[test]
    fn replayed_legacy_activity_treats_missing_option_snapshot_as_unknown() {
        let mut history = completed_retry_activity_history();
        let activity = history[0].payload["activity"]
            .as_object_mut()
            .expect("activity snapshot");
        activity.remove("execution_mode");
        activity.remove("retry_policy");

        let mut current = retry_activity_options();
        current.start_to_close_timeout = Some(Duration::from_secs(45));
        current.schedule_to_start_timeout = Some(Duration::from_secs(8));
        current.schedule_to_close_timeout = Some(Duration::from_secs(120));
        current.heartbeat_timeout = Some(Duration::from_secs(12));

        let ctx = workflow_context(history);
        let mut call = Box::pin(ctx.activity_with_options("flaky", current, json!([])));
        let mut task_context = TaskContext::from_waker(noop_waker_ref());
        assert!(matches!(
            call.as_mut().poll(&mut task_context),
            Poll::Ready(Ok(result)) if result == json!({"status": "recovered"})
        ));
        assert!(ctx.take_commands().expect("commands").is_empty());
        ctx.ensure_history_consumed().expect("history consumed");
    }

    #[test]
    fn terminal_activity_failed_after_start_returns_typed_failure() {
        let history = vec![
            history_event(
                "ActivityScheduled",
                json!({
                    "sequence": 1,
                    "activity_type": "flaky",
                    "activity_execution_id": "act-terminal",
                    "activity": {
                        "id": "act-terminal",
                        "sequence": 1,
                        "type": "flaky",
                        "queue": "critical-activities",
                        "retry_policy": {
                            "snapshot_version": 1,
                            "max_attempts": 3,
                            "backoff_seconds": [2, 4],
                            "non_retryable_error_types": ["PermanentError"]
                        }
                    }
                }),
            ),
            history_event(
                "ActivityStarted",
                json!({
                    "sequence": 1,
                    "activity_type": "flaky",
                    "activity_execution_id": "act-terminal",
                    "activity_attempt_id": "attempt-1",
                    "attempt_number": 1
                }),
            ),
            history_event(
                "ActivityFailed",
                json!({
                    "sequence": 1,
                    "activity_type": "flaky",
                    "activity_execution_id": "act-terminal",
                    "activity_attempt_id": "attempt-1",
                    "attempt_number": 1,
                    "failure_id": "failure-terminal",
                    "failure_category": "activity",
                    "exception_type": "PermanentError",
                    "message": "cannot retry",
                    "non_retryable": true
                }),
            ),
        ];
        let ctx = workflow_context(history);
        let mut call =
            Box::pin(ctx.activity_with_options("flaky", retry_activity_options(), json!([])));
        let mut task_context = TaskContext::from_waker(noop_waker_ref());

        let Poll::Ready(Err(Error::ActivityFailed(failure))) =
            call.as_mut().poll(&mut task_context)
        else {
            panic!("terminal ActivityFailed must settle the activity future");
        };
        assert_eq!(failure.kind, ActivityFailureKind::Failed);
        assert_eq!(
            failure.activity_execution_id.as_deref(),
            Some("act-terminal")
        );
        assert_eq!(failure.exception_type.as_deref(), Some("PermanentError"));
        assert!(failure.non_retryable);
        assert!(ctx.take_commands().expect("commands").is_empty());
        ctx.ensure_history_consumed().expect("history consumed");
    }

    #[test]
    fn activity_terminal_events_return_machine_readable_failures() {
        let cases = [
            (
                "ActivityFailed",
                json!({
                    "sequence": 1,
                    "activity_type": "charge-card",
                    "activity_execution_id": "act-1",
                    "activity_attempt_id": "attempt-2",
                    "attempt_number": 2,
                    "failure_id": "failure-1",
                    "failure_category": "activity",
                    "exception_type": "PaymentDeclined",
                    "exception_class": "payments.PaymentDeclined",
                    "message": "card declined",
                    "non_retryable": true
                }),
                ActivityFailureKind::Failed,
                "activity",
            ),
            (
                "ActivityCancelled",
                json!({
                    "sequence": 1,
                    "activity_type": "charge-card",
                    "activity_execution_id": "act-1",
                    "activity_attempt_id": "attempt-1"
                }),
                ActivityFailureKind::Cancelled,
                "cancelled",
            ),
        ];

        for (event_type, payload, expected_kind, expected_reason) in cases {
            let ctx = workflow_context(vec![history_event(event_type, payload)]);
            let mut call = Box::pin(ctx.activity("charge-card", json!([])));
            let mut task_context = TaskContext::from_waker(noop_waker_ref());
            let Poll::Ready(Err(Error::ActivityFailed(failure))) =
                call.as_mut().poll(&mut task_context)
            else {
                panic!("expected terminal activity failure");
            };
            assert_eq!(failure.kind, expected_kind);
            assert_eq!(failure.reason, expected_reason);
            assert_eq!(failure.activity_execution_id.as_deref(), Some("act-1"));
            assert_eq!(failure.activity_type.as_deref(), Some("charge-card"));
        }
    }

    #[test]
    fn every_activity_timeout_class_is_typed() {
        for timeout_kind in [
            "start_to_close",
            "schedule_to_start",
            "schedule_to_close",
            "heartbeat",
        ] {
            let ctx = workflow_context(vec![history_event(
                "ActivityTimedOut",
                json!({
                    "sequence": 1,
                    "activity_type": "slow",
                    "activity_execution_id": "act-timeout",
                    "activity_attempt_id": "attempt-timeout",
                    "failure_category": "timeout",
                    "timeout_kind": timeout_kind,
                    "message": "deadline expired"
                }),
            )]);
            let mut call = Box::pin(ctx.activity("slow", json!([])));
            let mut task_context = TaskContext::from_waker(noop_waker_ref());
            let Poll::Ready(Err(Error::ActivityFailed(failure))) =
                call.as_mut().poll(&mut task_context)
            else {
                panic!("expected timeout failure");
            };
            assert_eq!(failure.kind, ActivityFailureKind::TimedOut);
            assert_eq!(failure.reason, timeout_kind);
            assert_eq!(failure.timeout_kind.as_deref(), Some(timeout_kind));
            assert_eq!(failure.failure_category.as_deref(), Some("timeout"));
        }
    }

    #[test]
    fn workflow_sleep_emits_one_durable_timer_and_rounds_up() {
        let ctx = workflow_context(Vec::new());
        let mut sleep = Box::pin(ctx.sleep(Duration::from_millis(1_001)));
        let mut task_context = TaskContext::from_waker(noop_waker_ref());

        assert!(matches!(
            sleep.as_mut().poll(&mut task_context),
            Poll::Pending
        ));
        assert!(matches!(
            sleep.as_mut().poll(&mut task_context),
            Poll::Pending
        ));

        let commands = ctx.take_commands().expect("timer command");
        assert_eq!(
            commands,
            vec![json!({
                "type": "start_timer",
                "delay_seconds": 2,
            })]
        );
    }

    #[test]
    fn workflow_sleep_replays_matching_schedule_and_fire_without_a_command() {
        let history = vec![
            history_event(
                "TimerScheduled",
                json!({
                    "sequence": 1,
                    "timer_id": "timer-1",
                    "delay_seconds": 5,
                    "fire_at": "2026-07-11T12:00:05Z",
                }),
            ),
            history_event(
                "TimerFired",
                json!({
                    "sequence": 1,
                    "timer_id": "timer-1",
                    "delay_seconds": 5,
                    "fire_at": "2026-07-11T12:00:05Z",
                    "fired_at": "2026-07-11T12:00:05Z",
                }),
            ),
        ];

        for _restart in 0..2 {
            let ctx = workflow_context(history.clone());
            let mut sleep = Box::pin(ctx.sleep(Duration::from_secs(5)));
            let mut task_context = TaskContext::from_waker(noop_waker_ref());
            assert!(matches!(
                sleep.as_mut().poll(&mut task_context),
                Poll::Ready(Ok(()))
            ));
            assert!(ctx.take_commands().expect("commands").is_empty());
            ctx.ensure_history_consumed().expect("history consumed");
        }
    }

    #[test]
    fn workflow_sleep_rejects_changed_delay_during_replay() {
        let ctx = workflow_context(vec![
            history_event(
                "TimerScheduled",
                json!({"sequence": 1, "timer_id": "timer-1", "delay_seconds": 5}),
            ),
            history_event(
                "TimerFired",
                json!({"sequence": 1, "timer_id": "timer-1", "delay_seconds": 5}),
            ),
        ]);
        let mut sleep = Box::pin(ctx.sleep(Duration::from_secs(500)));
        let mut task_context = TaskContext::from_waker(noop_waker_ref());

        let Poll::Ready(Err(Error::NonDeterministicReplay(failure))) =
            sleep.as_mut().poll(&mut task_context)
        else {
            panic!("changed timer delay must be rejected");
        };
        assert_eq!(failure.reason, "timer_delay_mismatch");
        assert_eq!(failure.sequence, Some(1));
    }

    #[test]
    fn workflow_condition_wait_emits_published_identity_and_timeout_contract() {
        let ctx = workflow_context(Vec::new());
        let mut wait = Box::pin(
            ctx.wait_condition(
                ConditionWaitOptions::new("approval.ready", "sha256:approval-v1")
                    .timeout(Duration::from_millis(60_001)),
                || Ok(false),
            ),
        );
        let mut task_context = TaskContext::from_waker(noop_waker_ref());

        assert!(matches!(
            wait.as_mut().poll(&mut task_context),
            Poll::Pending
        ));
        assert!(matches!(
            wait.as_mut().poll(&mut task_context),
            Poll::Pending
        ));
        assert_eq!(
            ctx.take_commands().expect("condition command"),
            vec![json!({
                "type": "open_condition_wait",
                "condition_wait_occurrence_id": "rust:condition-wait:0",
                "condition_key": "approval.ready",
                "condition_definition_fingerprint": "sha256:approval-v1",
                "timeout_seconds": 61,
            })]
        );
    }

    #[test]
    fn workflow_condition_wait_returns_explicit_immediate_results_without_commands() {
        let ctx = workflow_context(Vec::new());
        let mut satisfied = Box::pin(wait_condition!(ctx, "already-ready", || Ok(true)));
        let mut task_context = TaskContext::from_waker(noop_waker_ref());
        assert!(matches!(
            satisfied.as_mut().poll(&mut task_context),
            Poll::Ready(Ok(ConditionWaitResult::Satisfied))
        ));

        let mut timed_out = Box::pin(wait_condition!(
            ctx,
            "no-wait",
            timeout: Duration::ZERO,
            || Ok(false),
        ));
        assert!(matches!(
            timed_out.as_mut().poll(&mut task_context),
            Poll::Ready(Ok(ConditionWaitResult::TimedOut))
        ));
        assert!(ctx.take_commands().expect("commands").is_empty());
    }

    #[test]
    fn signal_and_update_history_reevaluate_open_conditions_after_restart() {
        let signal_history = vec![
            history_event(
                "ConditionWaitOpened",
                json!({
                    "sequence": 4,
                    "condition_wait_id": "condition:4",
                    "condition_wait_occurrence_id": "rust:condition-wait:0",
                    "condition_key": "approval",
                    "condition_definition_fingerprint": "sha256:approval-v1",
                    "timeout_seconds": 30,
                }),
            ),
            history_event(
                "SignalReceived",
                json!({
                    "workflow_sequence": 4,
                    "signal_name": "approve",
                    "arguments": fixture_envelope(json!(["Ada"])),
                }),
            ),
        ];
        for _worker_before_or_after_restart in 0..2 {
            let ctx = workflow_context(signal_history.clone());
            let predicate_ctx = ctx.clone();
            let mut wait = Box::pin(
                ctx.wait_condition(
                    ConditionWaitOptions::new("approval", "sha256:approval-v1")
                        .timeout(Duration::from_secs(30)),
                    move || Ok(!predicate_ctx.signals("approve")?.is_empty()),
                ),
            );
            let mut task_context = TaskContext::from_waker(noop_waker_ref());
            assert!(matches!(
                wait.as_mut().poll(&mut task_context),
                Poll::Ready(Ok(ConditionWaitResult::Satisfied))
            ));
            assert!(ctx.take_commands().expect("commands").is_empty());
            ctx.ensure_history_consumed().expect("condition consumed");
        }

        let update_history = vec![
            history_event(
                "ConditionWaitOpened",
                json!({
                    "sequence": 7,
                    "condition_wait_id": "condition:7",
                    "condition_wait_occurrence_id": "rust:condition-wait:0",
                    "condition_key": "update-approval",
                    "condition_definition_fingerprint": "sha256:update-approval-v1",
                }),
            ),
            history_event(
                "UpdateApplied",
                json!({
                    "sequence": 7,
                    "update_id": "update-1",
                    "update_name": "approve",
                    "arguments": fixture_envelope(json!([true])),
                }),
            ),
        ];
        let ctx = workflow_context(update_history);
        let predicate_ctx = ctx.clone();
        let mut wait = Box::pin(ctx.wait_condition(
            ConditionWaitOptions::new("update-approval", "sha256:update-approval-v1"),
            move || {
                Ok(predicate_ctx
                    .updates("approve")?
                    .first()
                    .and_then(|arguments| arguments.first())
                    .and_then(Value::as_bool)
                    == Some(true))
            },
        ));
        let mut task_context = TaskContext::from_waker(noop_waker_ref());
        assert!(matches!(
            wait.as_mut().poll(&mut task_context),
            Poll::Ready(Ok(ConditionWaitResult::Satisfied))
        ));
        assert!(ctx.take_commands().expect("commands").is_empty());
        ctx.ensure_history_consumed().expect("condition consumed");
    }

    #[test]
    fn condition_wait_preserves_open_satisfied_and_timed_out_replay_states() {
        let open_history = vec![
            history_event(
                "ConditionWaitOpened",
                json!({
                    "sequence": 3,
                    "condition_wait_id": "condition:3",
                    "condition_wait_occurrence_id": "rust:condition-wait:0",
                    "condition_key": "two-votes",
                    "condition_definition_fingerprint": "sha256:two-votes-v1",
                    "timeout_seconds": 120,
                }),
            ),
            history_event(
                "SignalReceived",
                json!({
                    "workflow_sequence": 3,
                    "signal_name": "vote",
                    "arguments": fixture_envelope(json!(["first"])),
                }),
            ),
        ];
        for _worker_before_or_after_restart in 0..2 {
            let ctx = workflow_context(open_history.clone());
            let predicate_ctx = ctx.clone();
            let mut wait = Box::pin(
                ctx.wait_condition(
                    ConditionWaitOptions::new("two-votes", "sha256:two-votes-v1")
                        .timeout(Duration::from_secs(120)),
                    move || Ok(predicate_ctx.signals("vote")?.len() >= 2),
                ),
            );
            let mut task_context = TaskContext::from_waker(noop_waker_ref());
            assert!(matches!(
                wait.as_mut().poll(&mut task_context),
                Poll::Pending
            ));
            assert_eq!(
                ctx.take_commands().expect("reopened condition"),
                vec![json!({
                    "type": "open_condition_wait",
                    "condition_wait_occurrence_id": "rust:condition-wait:0",
                    "condition_key": "two-votes",
                    "condition_definition_fingerprint": "sha256:two-votes-v1",
                    "timeout_seconds": 120,
                })]
            );
        }

        let satisfied_ctx = workflow_context(vec![
            history_event(
                "ConditionWaitOpened",
                json!({
                    "sequence": 5,
                    "condition_wait_id": "condition:5",
                    "condition_wait_occurrence_id": "rust:condition-wait:0",
                    "condition_key": "approval",
                    "condition_definition_fingerprint": "sha256:approval-v1",
                }),
            ),
            history_event(
                "ConditionWaitSatisfied",
                json!({
                    "sequence": 5,
                    "condition_wait_id": "condition:5",
                    "condition_wait_occurrence_id": "rust:condition-wait:0",
                    "condition_key": "approval",
                    "condition_definition_fingerprint": "sha256:approval-v1",
                }),
            ),
        ]);
        let mut satisfied = Box::pin(satisfied_ctx.wait_condition(
            ConditionWaitOptions::new("approval", "sha256:approval-v1"),
            || Ok(false),
        ));
        let mut task_context = TaskContext::from_waker(noop_waker_ref());
        assert!(matches!(
            satisfied.as_mut().poll(&mut task_context),
            Poll::Ready(Ok(ConditionWaitResult::Satisfied))
        ));

        let timed_out_ctx = workflow_context(vec![
            history_event(
                "ConditionWaitOpened",
                json!({
                    "sequence": 8,
                    "condition_wait_id": "condition:8",
                    "condition_wait_occurrence_id": "rust:condition-wait:0",
                    "condition_key": "approval-timeout",
                    "condition_definition_fingerprint": "sha256:approval-timeout-v1",
                    "timeout_seconds": 5,
                }),
            ),
            history_event(
                "TimerScheduled",
                json!({
                    "sequence": 9,
                    "timer_id": "condition-timer:9",
                    "timer_kind": "condition_timeout",
                    "condition_wait_id": "condition:8",
                    "delay_seconds": 5,
                }),
            ),
            history_event(
                "TimerFired",
                json!({
                    "sequence": 9,
                    "timer_id": "condition-timer:9",
                    "timer_kind": "condition_timeout",
                    "condition_wait_id": "condition:8",
                    "delay_seconds": 5,
                }),
            ),
        ]);
        let mut timed_out = Box::pin(
            timed_out_ctx.wait_condition(
                ConditionWaitOptions::new("approval-timeout", "sha256:approval-timeout-v1")
                    .timeout(Duration::from_secs(5)),
                || Ok(true),
            ),
        );
        assert!(matches!(
            timed_out.as_mut().poll(&mut task_context),
            Poll::Ready(Ok(ConditionWaitResult::TimedOut))
        ));
    }

    #[test]
    fn condition_wait_replays_repeated_physical_opens_as_one_logical_wait() {
        let history = vec![
            history_event(
                "ConditionWaitOpened",
                json!({
                    "sequence": 3,
                    "condition_wait_id": "condition:3",
                    "condition_wait_occurrence_id": "rust:condition-wait:0",
                    "condition_key": "two-votes",
                    "condition_definition_fingerprint": "sha256:two-votes-v1",
                }),
            ),
            history_event(
                "SignalReceived",
                json!({
                    "workflow_sequence": 3,
                    "signal_name": "vote",
                    "arguments": fixture_envelope(json!(["first"])),
                }),
            ),
            history_event(
                "ConditionWaitSatisfied",
                json!({
                    "sequence": 3,
                    "condition_wait_id": "condition:3",
                    "condition_wait_occurrence_id": "rust:condition-wait:0",
                    "condition_key": "two-votes",
                    "condition_definition_fingerprint": "sha256:two-votes-v1",
                }),
            ),
            history_event(
                "ConditionWaitOpened",
                json!({
                    "sequence": 5,
                    "condition_wait_id": "condition:5",
                    "condition_wait_occurrence_id": "rust:condition-wait:0",
                    "condition_key": "two-votes",
                    "condition_definition_fingerprint": "sha256:two-votes-v1",
                }),
            ),
            history_event(
                "SignalReceived",
                json!({
                    "workflow_sequence": 5,
                    "signal_name": "vote",
                    "arguments": fixture_envelope(json!(["second"])),
                }),
            ),
            history_event(
                "ConditionWaitSatisfied",
                json!({
                    "sequence": 5,
                    "condition_wait_id": "condition:5",
                    "condition_wait_occurrence_id": "rust:condition-wait:0",
                    "condition_key": "two-votes",
                    "condition_definition_fingerprint": "sha256:two-votes-v1",
                }),
            ),
        ];
        for _cold_worker_or_restart in 0..2 {
            let ctx = workflow_context(history.clone());
            let predicate_ctx = ctx.clone();
            let mut wait = Box::pin(ctx.wait_condition(
                ConditionWaitOptions::new("two-votes", "sha256:two-votes-v1"),
                move || Ok(predicate_ctx.signals("vote")?.len() >= 2),
            ));
            let mut task_context = TaskContext::from_waker(noop_waker_ref());

            assert!(matches!(
                wait.as_mut().poll(&mut task_context),
                Poll::Ready(Ok(ConditionWaitResult::Satisfied))
            ));
            assert!(ctx.take_commands().expect("commands").is_empty());
            ctx.ensure_history_consumed()
                .expect("every physical wait-open is consumed");
        }
    }

    #[test]
    fn condition_wait_replays_update_driven_physical_opens_as_one_occurrence() {
        let history = vec![
            history_event(
                "ConditionWaitOpened",
                json!({
                    "sequence": 3,
                    "condition_wait_id": "condition:3",
                    "condition_wait_occurrence_id": "rust:condition-wait:0",
                    "condition_key": "approved",
                    "condition_definition_fingerprint": "sha256:approved-v1",
                }),
            ),
            history_event(
                "UpdateApplied",
                json!({
                    "sequence": 3,
                    "update_id": "update-1",
                    "update_name": "approve",
                    "arguments": fixture_envelope(json!([false])),
                }),
            ),
            history_event(
                "ConditionWaitOpened",
                json!({
                    "sequence": 5,
                    "condition_wait_id": "condition:5",
                    "condition_wait_occurrence_id": "rust:condition-wait:0",
                    "condition_key": "approved",
                    "condition_definition_fingerprint": "sha256:approved-v1",
                }),
            ),
            history_event(
                "UpdateApplied",
                json!({
                    "sequence": 5,
                    "update_id": "update-2",
                    "update_name": "approve",
                    "arguments": fixture_envelope(json!([true])),
                }),
            ),
        ];

        for _cold_worker_or_restart in 0..2 {
            let ctx = workflow_context(history.clone());
            let predicate_ctx = ctx.clone();
            let mut wait = Box::pin(ctx.wait_condition(
                ConditionWaitOptions::new("approved", "sha256:approved-v1"),
                move || {
                    Ok(predicate_ctx
                        .updates("approve")?
                        .last()
                        .and_then(|arguments| arguments.first())
                        .and_then(Value::as_bool)
                        == Some(true))
                },
            ));
            let mut task_context = TaskContext::from_waker(noop_waker_ref());

            assert!(matches!(
                wait.as_mut().poll(&mut task_context),
                Poll::Ready(Ok(ConditionWaitResult::Satisfied))
            ));
            assert!(ctx.take_commands().expect("commands").is_empty());
            ctx.ensure_history_consumed()
                .expect("every update-driven reopen is consumed");
        }
    }

    #[test]
    fn condition_wait_replay_keeps_every_adjacent_authored_occurrence_distinct() {
        for (first_key, first_fingerprint, second_key, second_fingerprint) in [
            ("shared", "sha256:first", "shared", "sha256:second"),
            ("first", "sha256:shared", "second", "sha256:shared"),
            ("shared", "sha256:shared", "shared", "sha256:shared"),
            ("first", "sha256:first", "second", "sha256:second"),
        ] {
            let history = vec![
                history_event(
                    "ConditionWaitOpened",
                    json!({
                        "sequence": 3,
                        "condition_wait_id": "condition:3",
                        "condition_wait_occurrence_id": "rust:condition-wait:0",
                        "condition_key": first_key,
                        "condition_definition_fingerprint": first_fingerprint,
                    }),
                ),
                history_event(
                    "ConditionWaitSatisfied",
                    json!({
                        "sequence": 3,
                        "condition_wait_id": "condition:3",
                        "condition_wait_occurrence_id": "rust:condition-wait:0",
                        "condition_key": first_key,
                        "condition_definition_fingerprint": first_fingerprint,
                    }),
                ),
                history_event(
                    "ConditionWaitOpened",
                    json!({
                        "sequence": 4,
                        "condition_wait_id": "condition:4",
                        "condition_wait_occurrence_id": "rust:condition-wait:1",
                        "condition_key": second_key,
                        "condition_definition_fingerprint": second_fingerprint,
                    }),
                ),
                history_event(
                    "ConditionWaitSatisfied",
                    json!({
                        "sequence": 4,
                        "condition_wait_id": "condition:4",
                        "condition_wait_occurrence_id": "rust:condition-wait:1",
                        "condition_key": second_key,
                        "condition_definition_fingerprint": second_fingerprint,
                    }),
                ),
            ];
            for _cold_worker_or_restart in 0..2 {
                let ctx = workflow_context(history.clone());
                let mut task_context = TaskContext::from_waker(noop_waker_ref());
                let mut first = Box::pin(ctx.wait_condition(
                    ConditionWaitOptions::new(first_key, first_fingerprint),
                    || Ok(false),
                ));
                assert!(matches!(
                    first.as_mut().poll(&mut task_context),
                    Poll::Ready(Ok(ConditionWaitResult::Satisfied))
                ));

                let mut second = Box::pin(ctx.wait_condition(
                    ConditionWaitOptions::new(second_key, second_fingerprint),
                    || Ok(false),
                ));
                assert!(matches!(
                    second.as_mut().poll(&mut task_context),
                    Poll::Ready(Ok(ConditionWaitResult::Satisfied))
                ));
                assert!(ctx.take_commands().expect("commands").is_empty());
                ctx.ensure_history_consumed()
                    .expect("each authored wait consumes one occurrence");
            }
        }
    }

    #[test]
    fn cold_workers_replay_adjacent_condition_waits_from_one_loop_call_site() {
        fn worker() -> Worker {
            let client = Client::new("http://127.0.0.1:8080").expect("client");
            let mut worker = Worker::new(client, "rust-workers");
            worker.register_workflow("rust.condition-loop", |ctx, _input| async move {
                let mut outcomes = Vec::new();
                for _ in 0..2 {
                    outcomes.push(
                        ctx.wait_condition(
                            ConditionWaitOptions::new("shared", "sha256:shared"),
                            || Ok(false),
                        )
                        .await?,
                    );
                }
                Ok(json!(outcomes))
            });
            worker
        }

        let task = workflow_task(
            "rust.condition-loop",
            vec![
                history_event(
                    "ConditionWaitOpened",
                    json!({
                        "sequence": 1,
                        "condition_wait_id": "condition:1",
                        "condition_wait_occurrence_id": "rust:condition-wait:0",
                        "condition_key": "shared",
                        "condition_definition_fingerprint": "sha256:shared",
                    }),
                ),
                history_event(
                    "ConditionWaitSatisfied",
                    json!({
                        "sequence": 1,
                        "condition_wait_id": "condition:1",
                        "condition_wait_occurrence_id": "rust:condition-wait:0",
                        "condition_key": "shared",
                        "condition_definition_fingerprint": "sha256:shared",
                    }),
                ),
                history_event(
                    "ConditionWaitOpened",
                    json!({
                        "sequence": 2,
                        "condition_wait_id": "condition:2",
                        "condition_wait_occurrence_id": "rust:condition-wait:1",
                        "condition_key": "shared",
                        "condition_definition_fingerprint": "sha256:shared",
                    }),
                ),
                history_event(
                    "ConditionWaitSatisfied",
                    json!({
                        "sequence": 2,
                        "condition_wait_id": "condition:2",
                        "condition_wait_occurrence_id": "rust:condition-wait:1",
                        "condition_key": "shared",
                        "condition_definition_fingerprint": "sha256:shared",
                    }),
                ),
            ],
            DEFAULT_CODEC,
        );

        for _cold_worker_or_restart in 0..2 {
            let commands = worker()
                .execute_workflow_task(task.clone())
                .expect("adjacent loop waits replay deterministically");
            assert_eq!(commands.len(), 1);
            assert_eq!(commands[0]["type"], "complete_workflow");
            assert_eq!(
                decode_wire_value(&commands[0]["result"], DEFAULT_CODEC).expect("workflow output"),
                json!(["satisfied", "satisfied"])
            );
        }
    }

    #[test]
    fn condition_wait_replay_rejects_identity_predicate_and_timeout_changes() {
        let history = vec![history_event(
            "ConditionWaitOpened",
            json!({
                "sequence": 12,
                "condition_wait_id": "condition:12",
                "condition_wait_occurrence_id": "rust:condition-wait:0",
                "condition_key": "approval",
                "condition_definition_fingerprint": "sha256:approval-v1",
                "timeout_seconds": 30,
            }),
        )];
        for (options, expected_reason) in [
            (
                ConditionWaitOptions::new("changed", "sha256:approval-v1")
                    .timeout(Duration::from_secs(30)),
                "condition_wait_key_mismatch",
            ),
            (
                ConditionWaitOptions::new("approval", "sha256:approval-v2")
                    .timeout(Duration::from_secs(30)),
                "condition_wait_predicate_mismatch",
            ),
            (
                ConditionWaitOptions::new("approval", "sha256:approval-v1")
                    .timeout(Duration::from_secs(29)),
                "condition_wait_timeout_mismatch",
            ),
        ] {
            let ctx = workflow_context(history.clone());
            let mut wait = Box::pin(ctx.wait_condition(options, || Ok(false)));
            let mut task_context = TaskContext::from_waker(noop_waker_ref());
            let Poll::Ready(Err(Error::NonDeterministicReplay(failure))) =
                wait.as_mut().poll(&mut task_context)
            else {
                panic!("changed condition definition must fail replay");
            };
            assert_eq!(failure.reason, expected_reason);
            assert_eq!(failure.sequence, Some(12));
        }
    }

    #[test]
    fn condition_wait_history_requires_the_canonical_predicate_fingerprint() {
        let error = WorkflowState::new(
            vec![history_event(
                "ConditionWaitOpened",
                json!({
                    "sequence": 12,
                    "condition_wait_id": "condition:12",
                    "condition_wait_occurrence_id": "rust:condition-wait:0",
                    "condition_key": "approval",
                }),
            )],
            "rust-workers".to_string(),
            DEFAULT_CODEC.to_string(),
            None,
        )
        .expect_err("condition history without a predicate fingerprint must fail");

        assert!(matches!(
            error,
            Error::NonDeterministicReplay(ReplayFailure { ref reason, .. })
                if reason == "condition_wait_predicate_fingerprint_missing"
        ));
    }

    #[test]
    fn condition_wait_history_requires_authored_occurrence_identity() {
        let error = WorkflowState::new(
            vec![history_event(
                "ConditionWaitOpened",
                json!({
                    "sequence": 12,
                    "condition_wait_id": "condition:12",
                    "condition_key": "approval",
                    "condition_definition_fingerprint": "sha256:approval-v1",
                }),
            )],
            "rust-workers".to_string(),
            DEFAULT_CODEC.to_string(),
            None,
        )
        .expect_err("condition history without occurrence identity must fail");

        assert!(matches!(
            error,
            Error::NonDeterministicReplay(ReplayFailure { ref reason, .. })
                if reason == "condition_wait_occurrence_id_missing"
        ));
    }

    #[test]
    fn typed_search_attribute_updates_validate_emit_and_replay() {
        let update = SearchAttributeUpdate::new()
            .keyword("OrderStatus", " waiting ")
            .expect("keyword")
            .int("Attempt", 3)
            .expect("int")
            .bool("Escalated", false)
            .expect("bool")
            .keyword_list("Regions", ["us-east", "eu-west"])
            .expect("list")
            .datetime("UpdatedAt", "2026-08-22T04:00:00Z")
            .expect("datetime")
            .delete("LegacyStatus")
            .expect("delete");
        let ctx = workflow_context(Vec::new());
        ctx.upsert_search_attributes(update.clone())
            .expect("typed update");
        assert_eq!(
            ctx.take_commands().expect("search-attribute command"),
            vec![json!({
                "type": "upsert_search_attributes",
                "attributes": {
                    "Attempt": 3,
                    "Escalated": false,
                    "LegacyStatus": null,
                    "OrderStatus": "waiting",
                    "Regions": ["us-east", "eu-west"],
                    "UpdatedAt": "2026-08-22T04:00:00Z",
                },
                "attribute_types": {
                    "Attempt": "int",
                    "Escalated": "bool",
                    "OrderStatus": "keyword",
                    "Regions": "keyword_list",
                    "UpdatedAt": "datetime",
                },
            })]
        );

        let replay = workflow_context(vec![history_event(
            "SearchAttributesUpserted",
            json!({
                "sequence": 6,
                "attributes": {
                    "Attempt": 3,
                    "Escalated": false,
                    "LegacyStatus": null,
                    "OrderStatus": "waiting",
                    "Regions": ["us-east", "eu-west"],
                    "UpdatedAt": "2026-08-22T04:00:00Z",
                },
                "attribute_types": {
                    "Attempt": "int",
                    "Escalated": "bool",
                    "OrderStatus": "keyword",
                    "Regions": "keyword_list",
                    "UpdatedAt": "datetime",
                },
                "merged": {},
            }),
        )]);
        replay
            .upsert_search_attributes(update)
            .expect("matching update replays");
        assert!(replay.take_commands().expect("commands").is_empty());
        replay.ensure_history_consumed().expect("history consumed");

        let type_drift = workflow_context(vec![history_event(
            "SearchAttributesUpserted",
            json!({
                "sequence": 7,
                "attributes": {"OrderStatus": "waiting"},
                "attribute_types": {"OrderStatus": "keyword"},
                "merged": {"OrderStatus": "waiting"},
            }),
        )]);
        let error = type_drift
            .upsert_search_attributes(
                SearchAttributeUpdate::new()
                    .string("OrderStatus", "waiting")
                    .expect("string update"),
            )
            .expect_err("same JSON value with a changed type must fail replay");
        assert!(matches!(
            error,
            Error::NonDeterministicReplay(ReplayFailure { ref reason, .. })
                if reason == "search_attribute_type_mismatch"
        ));

        let malformed_types = WorkflowState::new(
            vec![history_event(
                "SearchAttributesUpserted",
                json!({
                    "sequence": 8,
                    "attributes": {"OrderStatus": "waiting"},
                    "attribute_types": {"OrderStatus": "unsupported"},
                    "merged": {"OrderStatus": "waiting"},
                }),
            )],
            "rust-workers".to_string(),
            DEFAULT_CODEC.to_string(),
            None,
        )
        .expect_err("unsupported search-attribute type metadata must fail");
        assert!(matches!(
            malformed_types,
            Error::NonDeterministicReplay(ReplayFailure { ref reason, .. })
                if reason == "search_attribute_types_malformed"
        ));

        assert!(matches!(
            SearchAttributeUpdate::new().keyword("bad key", "value"),
            Err(SearchAttributeUpdateError::InvalidKey(_))
        ));
        assert!(matches!(
            SearchAttributeUpdate::new().float("Ratio", f64::NAN),
            Err(SearchAttributeUpdateError::NonFiniteFloat(_))
        ));
        assert!(matches!(
            SearchAttributeUpdate::new().keyword("UnicodeKeyword", "é".repeat(128)),
            Err(SearchAttributeUpdateError::ValueTooLong { .. })
        ));
        assert!(matches!(
            SearchAttributeUpdate::new().datetime("UpdatedAt", "2026-02-30T04:00:00Z"),
            Err(SearchAttributeUpdateError::InvalidDateTime(_))
        ));
        assert!(matches!(
            workflow_context(Vec::new()).upsert_search_attributes(SearchAttributeUpdate::new()),
            Err(Error::InvalidSearchAttributeUpdate(
                SearchAttributeUpdateError::Empty
            ))
        ));
    }

    #[test]
    fn typed_search_attribute_text_uses_the_runtime_byte_limit() {
        let ascii = "a".repeat(MAX_SEARCH_ATTRIBUTE_STRING_LENGTH);
        let utf8 = "é".repeat(MAX_SEARCH_ATTRIBUTE_STRING_LENGTH / 2);

        assert!(SearchAttributeUpdate::new()
            .string("AsciiDescription", ascii)
            .is_ok());
        assert!(SearchAttributeUpdate::new()
            .string("Utf8Description", utf8)
            .is_ok());
        assert!(matches!(
            SearchAttributeUpdate::new().string(
                "TooLongDescription",
                "é".repeat((MAX_SEARCH_ATTRIBUTE_STRING_LENGTH / 2) + 1),
            ),
            Err(SearchAttributeUpdateError::ValueTooLong {
                kind: "string",
                limit: MAX_SEARCH_ATTRIBUTE_STRING_LENGTH,
                ..
            })
        ));
    }

    #[test]
    fn workflow_history_rejects_unpaired_or_mismatched_timer_events() {
        let lone_fire = WorkflowState::new(
            vec![history_event(
                "TimerFired",
                json!({"sequence": 1, "timer_id": "timer-1", "delay_seconds": 5}),
            )],
            "rust-workers".to_string(),
            DEFAULT_CODEC.to_string(),
            None,
        )
        .expect_err("TimerFired requires TimerScheduled");
        assert!(matches!(
            lone_fire,
            Error::NonDeterministicReplay(ReplayFailure { ref reason, .. })
                if reason == "timer_schedule_missing_or_duplicate"
        ));

        let wrong_identity = WorkflowState::new(
            vec![
                history_event(
                    "TimerScheduled",
                    json!({"sequence": 1, "timer_id": "timer-1", "delay_seconds": 5}),
                ),
                history_event(
                    "TimerFired",
                    json!({"sequence": 1, "timer_id": "timer-2", "delay_seconds": 5}),
                ),
            ],
            "rust-workers".to_string(),
            DEFAULT_CODEC.to_string(),
            None,
        )
        .expect_err("fire must match scheduled timer identity");
        assert!(matches!(
            wrong_identity,
            Error::NonDeterministicReplay(ReplayFailure { ref reason, .. })
                if reason == "timer_identity_mismatch"
        ));

        let duplicate_fire = WorkflowState::new(
            vec![
                history_event(
                    "TimerScheduled",
                    json!({"sequence": 1, "timer_id": "timer-1", "delay_seconds": 5}),
                ),
                history_event(
                    "TimerFired",
                    json!({"sequence": 1, "timer_id": "timer-1", "delay_seconds": 5}),
                ),
                history_event(
                    "TimerFired",
                    json!({"sequence": 1, "timer_id": "timer-1", "delay_seconds": 5}),
                ),
            ],
            "rust-workers".to_string(),
            DEFAULT_CODEC.to_string(),
            None,
        )
        .expect_err("a durable timer cannot fire twice");
        assert!(matches!(
            duplicate_fire,
            Error::NonDeterministicReplay(ReplayFailure { ref reason, .. })
                if reason == "duplicate_timer_fire"
        ));

        let wrong_fired_delay = WorkflowState::new(
            vec![
                history_event(
                    "TimerScheduled",
                    json!({"sequence": 1, "timer_id": "timer-1", "delay_seconds": 5}),
                ),
                history_event(
                    "TimerFired",
                    json!({"sequence": 1, "timer_id": "timer-1", "delay_seconds": 6}),
                ),
            ],
            "rust-workers".to_string(),
            DEFAULT_CODEC.to_string(),
            None,
        )
        .expect_err("timer schedule and fire delays must agree");
        assert!(matches!(
            wrong_fired_delay,
            Error::NonDeterministicReplay(ReplayFailure { ref reason, .. })
                if reason == "timer_history_delay_mismatch"
        ));
    }

    #[test]
    fn replay_rejects_activity_moved_before_recorded_timer() {
        let ctx = workflow_context(vec![
            history_event(
                "TimerScheduled",
                json!({"sequence": 1, "timer_id": "timer-1", "delay_seconds": 5}),
            ),
            history_event(
                "TimerFired",
                json!({"sequence": 1, "timer_id": "timer-1", "delay_seconds": 5}),
            ),
            history_event(
                "ActivityCompleted",
                json!({
                    "sequence": 2,
                    "activity_type": "after-timer",
                    "payload_codec": DEFAULT_CODEC,
                    "result": fixture_envelope(json!("done")),
                }),
            ),
        ]);
        let mut activity = Box::pin(ctx.activity("after-timer", json!([])));
        let mut task_context = TaskContext::from_waker(noop_waker_ref());

        let Poll::Ready(Err(Error::NonDeterministicReplay(failure))) =
            activity.as_mut().poll(&mut task_context)
        else {
            panic!("reordered durable command must be rejected");
        };
        assert_eq!(failure.reason, "recorded_command_mismatch");
        assert_eq!(failure.sequence, Some(1));
        assert_eq!(failure.expected.as_deref(), Some("timer"));
        assert_eq!(failure.actual.as_deref(), Some("activity:after-timer"));
    }

    #[test]
    fn workflow_context_emits_a_typed_named_signal_wait() {
        let ctx = workflow_context(Vec::new());
        let mut signal = Box::pin(ctx.wait_signal("finish"));
        let mut task_context = TaskContext::from_waker(noop_waker_ref());

        assert!(matches!(
            signal.as_mut().poll(&mut task_context),
            Poll::Pending
        ));
        assert_eq!(
            ctx.take_commands().expect("signal-wait command"),
            vec![json!({
                "type": "open_signal_wait",
                "signal_name": "finish",
            })]
        );
    }

    #[test]
    fn runtime_message_stream_transport_cannot_be_opened_as_a_user_signal() {
        let ctx = workflow_context(Vec::new());
        let mut signal = Box::pin(ctx.wait_signal(MESSAGE_STREAM_SIGNAL));
        let mut task_context = TaskContext::from_waker(noop_waker_ref());

        let Poll::Ready(Err(Error::Codec(message))) = signal.as_mut().poll(&mut task_context)
        else {
            panic!("runtime-reserved signal should be rejected");
        };
        assert!(message.contains("reserved by the workflow runtime"));
        assert!(ctx.take_commands().expect("commands").is_empty());
    }

    #[tokio::test]
    async fn runtime_message_stream_transport_cannot_be_sent_as_a_user_signal() {
        let client = Client::builder("http://127.0.0.1:9")
            .build()
            .expect("client");
        let error = client
            .signal_workflow("workflow-1", MESSAGE_STREAM_SIGNAL, json!(["forged"]))
            .await
            .expect_err("runtime-reserved signal should be rejected before transport");

        assert!(
            matches!(error, Error::Codec(ref message) if message.contains("reserved by the workflow runtime"))
        );
    }

    #[test]
    fn message_stream_worker_task_consumes_current_contiguous_bounded_batch() {
        fn delivery(message_id: &str, position: u64, value: &str) -> Value {
            let payload = encode_avro_value(&AvroValue::Array(vec![AvroValue::String(
                value.to_string(),
            )]))
            .expect("message payload");
            json!({
                "schema": MESSAGE_STREAM_SCHEMA,
                "stream_name": "orders",
                "message_id": message_id,
                "position": position,
                "payload_envelope": payload,
            })
        }

        fn opened(sequence: u64) -> HistoryEvent {
            history_event(
                "SignalWaitOpened",
                json!({
                    "sequence": sequence,
                    "signal_name": MESSAGE_STREAM_SIGNAL,
                }),
            )
        }

        fn applied(sequence: u64, delivery: Value) -> HistoryEvent {
            history_event(
                "SignalApplied",
                json!({
                    "sequence": sequence,
                    "signal_name": MESSAGE_STREAM_SIGNAL,
                    "value": fixture_envelope(json!([delivery])),
                }),
            )
        }

        fn received(delivery: Value) -> HistoryEvent {
            history_event(
                "SignalReceived",
                json!({
                    "signal_name": MESSAGE_STREAM_SIGNAL,
                    "arguments": fixture_envelope(json!([delivery])),
                    "payload_codec": DEFAULT_CODEC,
                }),
            )
        }

        let client = Client::new("http://127.0.0.1:8080").expect("client");
        let mut worker = Worker::new(client, "rust-workers");
        worker.register_workflow("rust.message-stream-batch", |ctx, _input| async move {
            let messages = ctx.message_stream("orders")?.receive(2).await?;
            Ok(json!(messages
                .into_iter()
                .map(|message| message.message_id)
                .collect::<Vec<_>>()))
        });

        let first = delivery("message-1", 1, "one");
        let second = delivery("message-2", 2, "two");
        let batch = worker
            .execute_workflow_task_decision(workflow_task(
                "rust.message-stream-batch",
                vec![
                    opened(1),
                    received(first.clone()),
                    applied(1, first.clone()),
                    received(first.clone()),
                    received(second),
                ],
                DEFAULT_CODEC,
            ))
            .expect("worker task consumes the available batch");

        assert_eq!(batch.commands.len(), 1);
        assert_eq!(batch.commands[0]["type"], "complete_workflow");
        assert_eq!(
            decode_wire_value(&batch.commands[0]["result"], DEFAULT_CODEC)
                .expect("workflow result"),
            json!(["message-1", "message-2"])
        );
        assert_eq!(
            batch.message_stream_cursors,
            vec![json!({"stream_name": "orders", "through_position": 2})]
        );
        assert!(batch.message_stream_waits.is_empty());

        let partial = worker
            .execute_workflow_task_decision(workflow_task(
                "rust.message-stream-batch",
                vec![opened(1), received(first.clone()), applied(1, first)],
                DEFAULT_CODEC,
            ))
            .expect("worker task returns without waiting for a missing second item");
        assert_eq!(partial.commands.len(), 1);
        assert_eq!(partial.commands[0]["type"], "complete_workflow");
        assert_eq!(
            decode_wire_value(&partial.commands[0]["result"], DEFAULT_CODEC)
                .expect("workflow result"),
            json!(["message-1"])
        );
        assert_eq!(
            partial.message_stream_cursors,
            vec![json!({"stream_name": "orders", "through_position": 1})]
        );
        assert!(partial.message_stream_waits.is_empty());
    }

    #[test]
    fn message_stream_replay_preserves_partial_batch_boundary_before_later_wait() {
        fn delivery(message_id: &str, position: u64, value: &str) -> Value {
            let payload = encode_avro_value(&AvroValue::Array(vec![AvroValue::String(
                value.to_string(),
            )]))
            .expect("message payload");
            json!({
                "schema": MESSAGE_STREAM_SCHEMA,
                "stream_name": "orders",
                "message_id": message_id,
                "position": position,
                "payload_envelope": payload,
            })
        }

        fn opened(sequence: u64) -> HistoryEvent {
            history_event(
                "SignalWaitOpened",
                json!({
                    "sequence": sequence,
                    "signal_name": MESSAGE_STREAM_SIGNAL,
                }),
            )
        }

        fn received(delivery: Value) -> HistoryEvent {
            history_event(
                "SignalReceived",
                json!({
                    "signal_name": MESSAGE_STREAM_SIGNAL,
                    "arguments": fixture_envelope(json!([delivery])),
                    "payload_codec": DEFAULT_CODEC,
                }),
            )
        }

        fn applied(sequence: u64, delivery: Value) -> HistoryEvent {
            history_event(
                "SignalApplied",
                json!({
                    "sequence": sequence,
                    "signal_name": MESSAGE_STREAM_SIGNAL,
                    "value": fixture_envelope(json!([delivery])),
                }),
            )
        }

        let client = Client::new("http://127.0.0.1:8080").expect("client");
        let mut worker = Worker::new(client, "rust-workers");
        worker.register_workflow(
            "rust.message-stream-partial-batches",
            |ctx, _input| async move {
                let stream = ctx.message_stream("orders")?;
                let first = stream.receive(10).await?;
                let second = stream.receive(10).await?;
                Ok(json!([
                    first
                        .into_iter()
                        .map(|message| message.message_id)
                        .collect::<Vec<_>>(),
                    second
                        .into_iter()
                        .map(|message| message.message_id)
                        .collect::<Vec<_>>(),
                ]))
            },
        );

        let first = delivery("message-1", 1, "one");
        let second = delivery("message-2", 2, "two");
        let decision = worker
            .execute_workflow_task_decision(workflow_task(
                "rust.message-stream-partial-batches",
                vec![
                    opened(1),
                    received(first.clone()),
                    applied(1, first),
                    opened(2),
                    received(second.clone()),
                    applied(2, second),
                ],
                DEFAULT_CODEC,
            ))
            .expect("cold replay preserves both authored receive boundaries");

        assert_eq!(decision.commands.len(), 1);
        assert_eq!(decision.commands[0]["type"], "complete_workflow");
        assert_eq!(
            decode_wire_value(&decision.commands[0]["result"], DEFAULT_CODEC)
                .expect("workflow result"),
            json!([["message-1"], ["message-2"]])
        );
        assert_eq!(
            decision.message_stream_cursors,
            vec![json!({"stream_name": "orders", "through_position": 2})]
        );
        assert!(decision.message_stream_waits.is_empty());
    }

    #[test]
    fn empty_message_stream_opens_internal_signal_wait_and_reports_position() {
        let ctx = workflow_context(Vec::new());
        let stream = ctx.message_stream("orders").expect("message stream");
        let mut receive = Box::pin(stream.receive(10));
        let mut task_context = TaskContext::from_waker(noop_waker_ref());

        assert!(matches!(
            receive.as_mut().poll(&mut task_context),
            Poll::Pending
        ));
        assert_eq!(
            ctx.take_commands().expect("message-stream wait command"),
            vec![json!({
                "type": "open_signal_wait",
                "signal_name": MESSAGE_STREAM_SIGNAL,
            })]
        );
        let (cursors, waits) = ctx.message_stream_metadata().expect("stream metadata");
        assert!(cursors.is_empty());
        assert_eq!(
            waits,
            vec![json!({"stream_name": "orders", "after_position": 0})]
        );
    }

    #[test]
    fn continue_as_new_cursor_checkpoint_preserves_global_pending_position() {
        let ctx = workflow_context(vec![history_event(
            "SignalReceived",
            json!({
                "signal_name": MESSAGE_STREAM_SIGNAL,
                "arguments": fixture_envelope(json!([{
                    "schema": MESSAGE_STREAM_CURSOR_SCHEMA,
                    "stream_name": "orders",
                    "through_position": 2,
                }])),
                "payload_codec": DEFAULT_CODEC,
            }),
        )]);
        let stream = ctx.message_stream("orders").expect("message stream");
        let mut receive = Box::pin(stream.receive(10));
        let mut task_context = TaskContext::from_waker(noop_waker_ref());

        assert!(matches!(
            receive.as_mut().poll(&mut task_context),
            Poll::Pending
        ));
        let (cursors, waits) = ctx.message_stream_metadata().expect("stream metadata");
        assert_eq!(
            cursors,
            vec![json!({"stream_name": "orders", "through_position": 2})]
        );
        assert_eq!(
            waits,
            vec![json!({"stream_name": "orders", "after_position": 2})]
        );
    }

    #[test]
    fn message_stream_delivery_preserves_typed_avro_arguments_across_replay() {
        let mut empty_map = BTreeMap::new();
        let mut nested = BTreeMap::new();
        nested.insert(
            "value".to_string(),
            AvroValue::Array(vec![AvroValue::Bytes(b"nested".to_vec())]),
        );
        let values = vec![
            AvroValue::Bytes(vec![0, 255]),
            AvroValue::Long(1),
            AvroValue::Double(1.0),
            AvroValue::Array(Vec::new()),
            AvroValue::Map(std::mem::take(&mut empty_map)),
            AvroValue::Map(nested),
        ];
        let payload = encode_avro_value(&AvroValue::Array(values.clone())).expect("payload");
        let transport = vec![json!({
            "schema": MESSAGE_STREAM_SCHEMA,
            "stream_name": "orders",
            "message_id": "message-1",
            "position": 1,
            "payload_envelope": payload,
        })];

        for _ in 0..2 {
            let Some(MessageStreamDelivery::Message(message)) =
                decode_message_stream_delivery(transport.clone()).expect("delivery")
            else {
                panic!("message delivery expected");
            };
            assert_eq!(message.arguments, values);
            assert!(matches!(message.arguments[1], AvroValue::Long(1)));
            assert!(matches!(message.arguments[2], AvroValue::Double(1.0)));
        }
    }

    #[test]
    fn cold_worker_replacement_consumes_message_stream_wait_arrivals_once_in_order() {
        fn delivery(message_id: &str, position: u64, value: &str) -> Value {
            let payload = encode_avro_value(&AvroValue::Array(vec![AvroValue::String(
                value.to_string(),
            )]))
            .expect("message payload");
            json!({
                "schema": MESSAGE_STREAM_SCHEMA,
                "stream_name": "orders",
                "message_id": message_id,
                "position": position,
                "payload_envelope": payload,
            })
        }

        fn opened(sequence: u64) -> HistoryEvent {
            history_event(
                "SignalWaitOpened",
                json!({
                    "sequence": sequence,
                    "signal_name": MESSAGE_STREAM_SIGNAL,
                }),
            )
        }

        fn applied(sequence: u64, delivery: Value) -> HistoryEvent {
            history_event(
                "SignalApplied",
                json!({
                    "sequence": sequence,
                    "signal_name": MESSAGE_STREAM_SIGNAL,
                    "value": fixture_envelope(json!([delivery])),
                }),
            )
        }

        fn worker() -> Worker {
            let client = Client::new("http://127.0.0.1:8080").expect("client");
            let mut worker = Worker::new(client, "rust-workers");
            worker.register_workflow("rust.message-stream", |ctx, _input| async move {
                let stream = ctx.message_stream("orders")?;
                let first = stream.receive_one().await?;
                let second = stream.receive_one().await?;
                Ok(json!([first.message_id, second.message_id]))
            });
            worker
        }

        fn task_with_resume(history: Vec<HistoryEvent>, delivery: Value) -> WorkflowTask {
            let mut task = workflow_task("rust.message-stream", history, DEFAULT_CODEC);
            task.signal_name = Some(MESSAGE_STREAM_SIGNAL.to_string());
            task.signal_arguments = Some(fixture_envelope(json!([delivery])));
            task
        }

        let waiting = worker()
            .execute_workflow_task_decision(workflow_task(
                "rust.message-stream",
                Vec::new(),
                DEFAULT_CODEC,
            ))
            .expect("first worker opens the stream wait");
        assert_eq!(
            waiting.commands,
            vec![json!({
                "type": "open_signal_wait",
                "signal_name": MESSAGE_STREAM_SIGNAL,
            })]
        );
        assert!(waiting.message_stream_cursors.is_empty());
        assert_eq!(
            waiting.message_stream_waits,
            vec![json!({"stream_name": "orders", "after_position": 0})]
        );

        let first_delivery = delivery("message-1", 1, "one");
        let first_arrival = worker()
            .execute_workflow_task_decision(task_with_resume(
                vec![opened(1)],
                first_delivery.clone(),
            ))
            .expect("replacement worker consumes the first arrival");
        assert_eq!(
            first_arrival.commands,
            vec![json!({
                "type": "open_signal_wait",
                "signal_name": MESSAGE_STREAM_SIGNAL,
            })]
        );
        assert_eq!(
            first_arrival.message_stream_cursors,
            vec![json!({"stream_name": "orders", "through_position": 1})]
        );
        assert_eq!(
            first_arrival.message_stream_waits,
            vec![json!({"stream_name": "orders", "after_position": 1})]
        );

        let second_delivery = delivery("message-2", 2, "two");
        let first_applied = applied(1, first_delivery);
        let completed = worker()
            .execute_workflow_task_decision(task_with_resume(
                vec![opened(1), first_applied.clone(), opened(2)],
                second_delivery.clone(),
            ))
            .expect("next replacement worker consumes the second arrival");
        assert_eq!(completed.commands.len(), 1);
        assert_eq!(completed.commands[0]["type"], "complete_workflow");
        assert_eq!(
            decode_wire_value(&completed.commands[0]["result"], DEFAULT_CODEC)
                .expect("workflow result"),
            json!(["message-1", "message-2"])
        );
        assert_eq!(
            completed.message_stream_cursors,
            vec![json!({"stream_name": "orders", "through_position": 2})]
        );
        assert!(completed.message_stream_waits.is_empty());

        let replay_history = vec![
            opened(1),
            first_applied,
            opened(2),
            applied(2, second_delivery),
        ];
        for _cold_worker_or_restart in 0..2 {
            let replayed = worker()
                .execute_workflow_task_decision(workflow_task(
                    "rust.message-stream",
                    replay_history.clone(),
                    DEFAULT_CODEC,
                ))
                .expect("cold worker replays each logical message exactly once");
            assert_eq!(replayed.commands.len(), 1);
            assert_eq!(
                decode_wire_value(&replayed.commands[0]["result"], DEFAULT_CODEC)
                    .expect("replayed workflow result"),
                json!(["message-1", "message-2"])
            );
            assert_eq!(
                replayed.message_stream_cursors,
                vec![json!({"stream_name": "orders", "through_position": 2})]
            );
            assert!(replayed.message_stream_waits.is_empty());
        }
    }

    #[test]
    fn message_stream_capability_and_completion_require_protocol_one_fifteen() {
        assert!(!worker_protocol_supports_message_streams("1.14"));
        assert!(worker_protocol_supports_message_streams("1.15"));
        assert!(worker_protocol_supports_message_streams("1.16"));
        assert!(worker_protocol_supports_message_streams(
            WORKER_PROTOCOL_VERSION
        ));
        assert_eq!(MESSAGE_STREAMS_MINIMUM_WORKER_PROTOCOL_VERSION, "1.15");
    }

    #[test]
    fn condition_wait_history_cannot_be_consumed_as_a_typed_signal_wait() {
        let ctx = workflow_context(vec![
            history_event(
                "ConditionWaitOpened",
                json!({
                    "sequence": 1,
                    "condition_wait_id": "condition:1",
                    "condition_wait_occurrence_id": "rust:condition-wait:0",
                    "condition_key": "signal:finish",
                    "condition_definition_fingerprint": "sha256:signal-finish-v1",
                }),
            ),
            history_event(
                "ConditionWaitSatisfied",
                json!({
                    "sequence": 1,
                    "condition_wait_id": "condition:1",
                    "condition_wait_occurrence_id": "rust:condition-wait:0",
                    "condition_key": "signal:finish",
                    "condition_definition_fingerprint": "sha256:signal-finish-v1",
                }),
            ),
            history_event(
                "SignalReceived",
                json!({"signal_name": "finish", "arguments": []}),
            ),
        ]);
        let mut signal = Box::pin(ctx.wait_signal("finish"));
        let mut task_context = TaskContext::from_waker(noop_waker_ref());

        let Poll::Ready(Err(Error::NonDeterministicReplay(failure))) =
            signal.as_mut().poll(&mut task_context)
        else {
            panic!("condition history must not resolve as a typed signal wait");
        };
        assert_eq!(failure.reason, "recorded_command_mismatch");
        assert_eq!(failure.expected.as_deref(), Some("condition wait"));
    }

    #[test]
    fn replay_orders_signal_waits_and_timers_in_one_command_stream() {
        let signal_then_timer = vec![
            history_event(
                "SignalWaitOpened",
                json!({"sequence": 1, "signal_name": "go"}),
            ),
            history_event(
                "SignalApplied",
                json!({
                    "sequence": 1,
                    "signal_name": "go",
                    "value": fixture_envelope(json!(["now"])),
                }),
            ),
            history_event(
                "TimerScheduled",
                json!({"sequence": 2, "timer_id": "timer-2", "delay_seconds": 5}),
            ),
            history_event(
                "TimerFired",
                json!({"sequence": 2, "timer_id": "timer-2", "delay_seconds": 5}),
            ),
        ];

        let ctx = workflow_context(signal_then_timer.clone());
        let mut signal = Box::pin(ctx.wait_signal("go"));
        let mut task_context = TaskContext::from_waker(noop_waker_ref());
        assert!(matches!(
            signal.as_mut().poll(&mut task_context),
            Poll::Ready(Ok(arguments)) if arguments == vec![json!("now")]
        ));
        let mut timer = Box::pin(ctx.sleep(Duration::from_secs(5)));
        assert!(matches!(
            timer.as_mut().poll(&mut task_context),
            Poll::Ready(Ok(()))
        ));
        ctx.ensure_history_consumed()
            .expect("signal and timer history consumed in order");

        let reordered = workflow_context(signal_then_timer);
        let mut timer_first = Box::pin(reordered.sleep(Duration::from_secs(5)));
        let Poll::Ready(Err(Error::NonDeterministicReplay(failure))) =
            timer_first.as_mut().poll(&mut task_context)
        else {
            panic!("timer cannot consume signal-wait-first history");
        };
        assert_eq!(failure.reason, "recorded_command_mismatch");
        assert_eq!(failure.sequence, Some(1));
        assert_eq!(failure.expected.as_deref(), Some("signal wait"));

        let timer_then_signal = vec![
            history_event(
                "TimerScheduled",
                json!({"sequence": 1, "timer_id": "timer-1", "delay_seconds": 5}),
            ),
            history_event(
                "TimerFired",
                json!({"sequence": 1, "timer_id": "timer-1", "delay_seconds": 5}),
            ),
            history_event(
                "SignalWaitOpened",
                json!({"sequence": 2, "signal_name": "go"}),
            ),
            history_event(
                "SignalApplied",
                json!({
                    "sequence": 2,
                    "signal_name": "go",
                    "value": fixture_envelope(json!([])),
                }),
            ),
        ];
        let reordered = workflow_context(timer_then_signal);
        let mut signal_first = Box::pin(reordered.wait_signal("go"));
        let Poll::Ready(Err(Error::NonDeterministicReplay(failure))) =
            signal_first.as_mut().poll(&mut task_context)
        else {
            panic!("signal wait cannot consume timer-first history");
        };
        assert_eq!(failure.reason, "recorded_command_mismatch");
        assert_eq!(failure.sequence, Some(1));
        assert_eq!(failure.expected.as_deref(), Some("timer"));
    }

    #[test]
    fn workflow_history_rejects_duplicate_or_colliding_command_sequences() {
        let duplicate_timer = WorkflowState::new(
            vec![
                history_event(
                    "TimerScheduled",
                    json!({"sequence": 1, "timer_id": "timer-1", "delay_seconds": 5}),
                ),
                history_event(
                    "TimerScheduled",
                    json!({"sequence": 1, "timer_id": "timer-2", "delay_seconds": 5}),
                ),
            ],
            "rust-workers".to_string(),
            DEFAULT_CODEC.to_string(),
            None,
        )
        .expect_err("one workflow sequence cannot schedule two timers");
        assert!(matches!(
            duplicate_timer,
            Error::NonDeterministicReplay(ReplayFailure { ref reason, .. })
                if reason == "timer_schedule_missing_or_duplicate"
        ));

        let colliding_kinds = WorkflowState::new(
            vec![
                history_event(
                    "TimerScheduled",
                    json!({"sequence": 1, "timer_id": "timer-1", "delay_seconds": 5}),
                ),
                history_event(
                    "ActivityCompleted",
                    json!({"sequence": 1, "activity_type": "same-sequence"}),
                ),
            ],
            "rust-workers".to_string(),
            DEFAULT_CODEC.to_string(),
            None,
        )
        .expect_err("one workflow sequence cannot identify two command kinds");
        assert!(matches!(
            colliding_kinds,
            Error::NonDeterministicReplay(ReplayFailure { ref reason, .. })
                if reason == "durable_command_sequence_collision"
        ));

        let duplicate_signal_wait = WorkflowState::new(
            vec![
                history_event(
                    "SignalWaitOpened",
                    json!({"sequence": 1, "signal_name": "go"}),
                ),
                history_event(
                    "SignalWaitOpened",
                    json!({"sequence": 1, "signal_name": "go"}),
                ),
            ],
            "rust-workers".to_string(),
            DEFAULT_CODEC.to_string(),
            None,
        )
        .expect_err("one workflow sequence cannot open two signal waits");
        assert!(matches!(
            duplicate_signal_wait,
            Error::NonDeterministicReplay(ReplayFailure { ref reason, .. })
                if reason == "signal_wait_open_missing_or_duplicate"
        ));
    }

    #[test]
    fn workflow_history_accepts_a_first_command_after_global_sequence_gaps() {
        let result = encode_value_envelope(&json!({"captured": true}), DEFAULT_CODEC)
            .expect("side-effect result");
        let ctx = workflow_context(vec![history_event(
            "SideEffectRecorded",
            json!({"sequence": 99, "result": result}),
        )]);

        let replayed: Value = ctx
            .side_effect(|| panic!("recorded side effect must not run"))
            .expect("positive global workflow sequence is valid");
        assert_eq!(replayed, json!({"captured": true}));
        ctx.ensure_history_consumed().expect("history consumed");
    }

    #[test]
    fn workflow_history_rejects_zero_and_descending_command_sequences() {
        let result =
            encode_value_envelope(&json!("captured"), DEFAULT_CODEC).expect("side-effect result");
        let zero = WorkflowState::new(
            vec![history_event(
                "SideEffectRecorded",
                json!({"sequence": 0, "result": result.clone()}),
            )],
            "rust-workers".to_string(),
            DEFAULT_CODEC.to_string(),
            None,
        )
        .expect_err("durable command sequences must be positive");
        assert!(matches!(
            zero,
            Error::NonDeterministicReplay(ReplayFailure { ref reason, .. })
                if reason == "durable_command_sequence_invalid"
        ));

        let descending = WorkflowState::new(
            vec![
                history_event(
                    "SideEffectRecorded",
                    json!({"sequence": 3, "result": result}),
                ),
                history_event(
                    "VersionMarkerRecorded",
                    json!({
                        "sequence": 2,
                        "change_id": "descending-marker",
                        "version": 1,
                        "min_supported": 1,
                        "max_supported": 1,
                    }),
                ),
            ],
            "rust-workers".to_string(),
            DEFAULT_CODEC.to_string(),
            None,
        )
        .expect_err("new durable commands must remain strictly ordered");
        let Error::NonDeterministicReplay(failure) = descending else {
            panic!("expected typed replay failure");
        };
        assert_eq!(failure.reason, "durable_command_sequence_mismatch");
        assert_eq!(failure.sequence, Some(2));
        assert_eq!(
            failure.expected.as_deref(),
            Some("workflow sequence greater than 3")
        );
        assert_eq!(failure.actual.as_deref(), Some("2"));
    }

    #[test]
    fn workflow_task_replay_completes_after_signals_create_sequence_gaps() {
        fn worker() -> Worker {
            let client = Client::new("http://127.0.0.1:8080").expect("client");
            let mut worker = Worker::new(client, "rust-workers");
            worker.register_workflow("rust.finish-after-gaps", |ctx, _input| async move {
                ctx.wait_signal("finish").await?;
                let marker: String =
                    ctx.side_effect(|| panic!("recorded side effect must not run"))?;
                assert_eq!(marker, "after-finish");
                Ok(json!("finished"))
            });
            worker
        }

        let marker = encode_value_envelope(&json!("after-finish"), DEFAULT_CODEC)
            .expect("side-effect result");
        let task = workflow_task(
            "rust.finish-after-gaps",
            vec![
                history_event(
                    "SignalWaitOpened",
                    json!({"sequence": 1, "signal_name": "finish"}),
                ),
                history_event(
                    "SignalReceived",
                    json!({
                        "signal_id": "increment-3",
                        "signal_name": "increment",
                        "workflow_sequence": 2,
                        "payload_codec": DEFAULT_CODEC,
                        "arguments": fixture_envelope(json!([3])),
                    }),
                ),
                history_event(
                    "SignalReceived",
                    json!({
                        "signal_id": "increment-5",
                        "signal_name": "increment",
                        "workflow_sequence": 3,
                        "payload_codec": DEFAULT_CODEC,
                        "arguments": fixture_envelope(json!([5])),
                    }),
                ),
                history_event(
                    "SignalReceived",
                    json!({
                        "signal_id": "finish",
                        "signal_name": "finish",
                        "workflow_sequence": 4,
                        "payload_codec": DEFAULT_CODEC,
                        "arguments": fixture_envelope(json!([])),
                    }),
                ),
                history_event(
                    "SignalApplied",
                    json!({
                        "sequence": 1,
                        "signal_id": "finish",
                        "signal_name": "finish",
                        "payload_codec": DEFAULT_CODEC,
                        "value": fixture_envelope(json!([])),
                    }),
                ),
                history_event(
                    "SideEffectRecorded",
                    json!({"sequence": 5, "result": marker}),
                ),
            ],
            DEFAULT_CODEC,
        );

        for _original_or_cold_worker in 0..2 {
            let commands = worker()
                .execute_workflow_task(task.clone())
                .expect("signal gaps preserve deterministic replay");
            assert_eq!(commands.len(), 1, "replay emits only terminal completion");
            assert_eq!(commands[0]["type"], "complete_workflow");
            assert_eq!(
                decode_wire_value(&commands[0]["result"], DEFAULT_CODEC).expect("workflow output"),
                json!("finished")
            );
        }
    }

    #[test]
    fn workflow_sleep_rejects_unrepresentable_rounded_duration() {
        let ctx = workflow_context(Vec::new());
        let mut sleep = Box::pin(ctx.start_timer(Duration::new(u64::MAX, 1)));
        let mut task_context = TaskContext::from_waker(noop_waker_ref());
        assert!(matches!(
            sleep.as_mut().poll(&mut task_context),
            Poll::Ready(Err(Error::TimerDurationOverflow))
        ));
        assert!(ctx.take_commands().expect("commands").is_empty());
    }

    #[test]
    fn workflow_memo_update_emits_canonical_command_and_replays_once() {
        let entries = AvroValue::Map(BTreeMap::from([
            ("text".to_string(), AvroValue::String("same".to_string())),
            (
                "nested".to_string(),
                AvroValue::Map(BTreeMap::from([
                    ("beta".to_string(), AvroValue::Long(2)),
                    ("alpha".to_string(), AvroValue::Long(1)),
                ])),
            ),
            ("long".to_string(), AvroValue::Long(7)),
            ("double".to_string(), AvroValue::Double(7.0)),
            ("binary".to_string(), AvroValue::Bytes(b"same".to_vec())),
        ]));
        let ctx = workflow_context(Vec::new());
        ctx.upsert_memo(entries.clone()).expect("valid memo update");
        let commands = ctx.take_commands().expect("commands");

        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0]["type"], "upsert_memo");
        let server_entries = json!({
            "codec": "avro",
            "blob": "wwHioz3/VYAiNw4KDGJpbmFyeQgIc2FtZQxkb3VibGUGAAAAAAAAHEAIbG9uZwQODG5lc3RlZA4ECmFscGhhBAIIYmV0YQQEAAh0ZXh0CghzYW1lAA==",
        });
        assert_eq!(
            commands[0]["entries"]
                .as_object()
                .expect("entries envelope")
                .keys()
                .collect::<Vec<_>>(),
            vec!["blob", "codec"]
        );
        assert_eq!(commands[0]["entries"], server_entries);
        let wire_entries =
            decode_wire_avro_value(&commands[0]["entries"], DEFAULT_CODEC).expect("memo entries");
        assert_eq!(wire_entries, entries);

        let history = vec![history_event(
            "MemoUpserted",
            json!({
                "sequence": 1,
                "entries": server_entries.clone(),
                "merged": server_entries,
            }),
        )];
        let replay = workflow_context(history.clone());
        replay
            .upsert_memo(entries.clone())
            .expect("matching replay identity");
        assert!(replay.take_commands().expect("replay commands").is_empty());

        let changed_types = AvroValue::Map(BTreeMap::from([
            ("text".to_string(), AvroValue::Bytes(b"same".to_vec())),
            (
                "nested".to_string(),
                AvroValue::Map(BTreeMap::from([
                    ("alpha".to_string(), AvroValue::Long(1)),
                    ("beta".to_string(), AvroValue::Long(2)),
                ])),
            ),
            ("long".to_string(), AvroValue::Double(7.0)),
            ("double".to_string(), AvroValue::Long(7)),
            ("binary".to_string(), AvroValue::String("same".to_string())),
        ]));
        let error = workflow_context(history)
            .upsert_memo(changed_types)
            .expect_err("memo replay identity must preserve Avro value types");
        assert!(matches!(
            error,
            Error::NonDeterministicReplay(ref failure) if failure.reason == "memo_update_mismatch"
        ));
    }

    #[test]
    fn workflow_memo_update_rejects_changed_replay_identity_and_invalid_keys() {
        let original = encode_value_envelope(&json!({"stage": "original"}), DEFAULT_CODEC)
            .expect("memo envelope");
        let replay = workflow_context(vec![history_event(
            "MemoUpserted",
            json!({
                "sequence": 1,
                "entries": original.clone(),
                "merged": original
            }),
        )]);
        let error = replay
            .upsert_memo(json!({"stage": "changed"}))
            .expect_err("changed memo update must fail replay");
        assert!(matches!(
            error,
            Error::NonDeterministicReplay(ref failure) if failure.reason == "memo_update_mismatch"
        ));

        let invalid = workflow_context(Vec::new())
            .upsert_memo(
                json!({"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx": true}),
            )
            .expect_err("oversized key");
        assert!(matches!(invalid, Error::InvalidMemoUpdate(_)));
    }

    #[test]
    fn workflow_memo_replay_distinguishes_signed_zero_identity() {
        let negative_zero = AvroValue::Map(BTreeMap::from([(
            "reading".to_string(),
            AvroValue::Double(-0.0),
        )]));
        let negative_zero_envelope =
            encode_typed_envelope(&negative_zero, DEFAULT_CODEC).expect("negative zero envelope");
        let history = vec![history_event(
            "MemoUpserted",
            json!({
                "sequence": 1,
                "entries": negative_zero_envelope.clone(),
                "merged": negative_zero_envelope,
            }),
        )];

        workflow_context(history.clone())
            .upsert_memo(negative_zero)
            .expect("matching negative-zero history identity");

        let error = workflow_context(history)
            .upsert_memo(AvroValue::Map(BTreeMap::from([(
                "reading".to_string(),
                AvroValue::Double(0.0),
            )])))
            .expect_err("positive zero must not consume negative-zero memo history");
        assert!(matches!(
            error,
            Error::NonDeterministicReplay(ref failure) if failure.reason == "memo_update_mismatch"
        ));
    }

    #[test]
    fn workflow_memo_capability_requires_flag_and_command_advertisement() {
        let supported = json!({
            "workflow_memo_updates": {"supported": true, "minimum_protocol_version": "1.14"},
            "supported_workflow_task_commands": ["complete_workflow", "upsert_memo"]
        });
        assert!(runtime_supports_workflow_memo_updates(Some(&supported)));
        assert!(!runtime_supports_workflow_memo_updates(Some(&json!({
            "workflow_memo_updates": {"supported": false},
            "supported_workflow_task_commands": ["upsert_memo"]
        }))));
        assert!(commands_use_workflow_memo_updates(&[json!({
            "type": "upsert_memo",
            "entries": {"stage": "processing"}
        })]));
    }

    #[test]
    fn workflow_task_replay_completes_without_rescheduling_recorded_commands() {
        let client = Client::new("http://127.0.0.1:8080").expect("client");
        let mut worker = Worker::new(client, "rust-workers");
        worker.register_workflow("rust.timer", |ctx, _input| async move {
            ctx.sleep(Duration::from_secs(5)).await?;
            ctx.activity("after-timer", json!([])).await
        });

        let task = |history_events| WorkflowTask {
            task_id: "wft-rust-timer-1".to_string(),
            workflow_command_id: None,
            workflow_id: Some("wf-rust-timer".to_string()),
            run_id: Some("run-rust-timer".to_string()),
            workflow_type: "rust.timer".to_string(),
            cancel_requested: false,
            payload_codec: DEFAULT_CODEC.to_string(),
            arguments: Some(
                encode_value_envelope(&json!([]), DEFAULT_CODEC).expect("workflow input"),
            ),
            history_events,
            total_history_events: None,
            history_size_bytes: None,
            continue_as_new_recommended: None,
            history_budget_pressure: None,
            next_history_page_token: None,
            workflow_task_attempt: 1,
            workflow_signal_id: None,
            signal_name: None,
            signal_arguments: None,
            workflow_update_id: None,
            update_name: None,
            lease_owner: Some("rust-worker".to_string()),
        };

        let initial = worker
            .execute_workflow_task(task(Vec::new()))
            .expect("initial timer task");
        assert_eq!(
            initial,
            vec![json!({"type": "start_timer", "delay_seconds": 5})]
        );

        let activity_result =
            encode_value_envelope(&json!("done"), DEFAULT_CODEC).expect("activity result");
        let replayed = worker
            .execute_workflow_task(task(vec![
                history_event(
                    "TimerScheduled",
                    json!({"sequence": 1, "timer_id": "timer-1", "delay_seconds": 5}),
                ),
                history_event(
                    "TimerFired",
                    json!({"sequence": 1, "timer_id": "timer-1", "delay_seconds": 5}),
                ),
                history_event(
                    "ActivityCompleted",
                    json!({
                        "sequence": 2,
                        "activity_type": "after-timer",
                        "payload_codec": DEFAULT_CODEC,
                        "result": activity_result,
                    }),
                ),
            ]))
            .expect("replayed workflow task");
        assert_eq!(replayed.len(), 1);
        assert_eq!(replayed[0]["type"], "complete_workflow");
        assert_eq!(
            decode_wire_value(&replayed[0]["result"], DEFAULT_CODEC).expect("result"),
            json!("done")
        );
    }

    #[test]
    fn workflow_continue_as_new_emits_arguments_type_and_queue_once() {
        let client = Client::new("http://127.0.0.1:8080").expect("client");
        let mut worker = Worker::new(client, "rust-workers");
        worker.register_workflow("rust.continue", |ctx, _input| async move {
            ctx.continue_as_new_with_options(
                ContinueAsNewOptions::new()
                    .workflow_type("rust.next")
                    .task_queue("next-workers"),
                json!([2, {"cursor": "next"}]),
            )
        });

        let commands = worker
            .execute_workflow_task(workflow_task("rust.continue", Vec::new(), DEFAULT_CODEC))
            .expect("continue-as-new command");

        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0]["type"], "continue_as_new");
        assert_eq!(commands[0]["workflow_type"], "rust.next");
        assert_eq!(commands[0]["queue"], "next-workers");
        assert_eq!(
            decode_wire_value(&commands[0]["arguments"], DEFAULT_CODEC)
                .expect("continue-as-new arguments"),
            json!([2, {"cursor": "next"}])
        );
    }

    #[test]
    fn continue_as_new_preserves_typed_arguments() {
        let client = Client::new("http://127.0.0.1:8080").expect("client");
        let mut worker = Worker::new(client, "rust-workers");
        worker.register_workflow_avro_value("rust.typed-continue", |ctx, _input| async move {
            ctx.continue_as_new(AvroValue::Array(vec![typed_fidelity_probe()]))?;
            unreachable!("continue-as-new returns a control-flow error")
        });

        let commands = worker
            .execute_workflow_task(workflow_task(
                "rust.typed-continue",
                Vec::new(),
                DEFAULT_CODEC,
            ))
            .expect("typed continue-as-new command");

        assert_eq!(commands[0]["type"], "continue_as_new");
        assert_eq!(
            decode_wire_avro_value(&commands[0]["arguments"], DEFAULT_CODEC)
                .expect("typed continue arguments"),
            AvroValue::Array(vec![typed_fidelity_probe()])
        );
    }

    #[test]
    fn recorded_continue_as_new_is_consumed_without_duplicate_successor_command() {
        let client = Client::new("http://127.0.0.1:8080").expect("client");
        let mut worker = Worker::new(client, "rust-workers");
        worker.register_workflow("rust.continue", |ctx, _input| async move {
            ctx.continue_as_new(json!([2]))
        });
        let task = workflow_task(
            "rust.continue",
            vec![history_event(
                "WorkflowContinuedAsNew",
                json!({"sequence": 1, "continued_to_run_id": "run-next"}),
            )],
            DEFAULT_CODEC,
        );

        for _worker_restart_or_redelivery in 0..2 {
            let commands = worker
                .execute_workflow_task(task.clone())
                .expect("recorded transition replays");
            assert!(
                commands.is_empty(),
                "replay must not emit another successor"
            );
        }
    }

    #[test]
    fn continue_as_new_rejects_invalid_overrides_before_emitting_a_command() {
        let ctx = workflow_context(Vec::new());
        let error = ctx
            .continue_as_new_with_options(ContinueAsNewOptions::new().task_queue("  "), json!([1]))
            .expect_err("blank queue must be rejected");

        let Error::InvalidContinueAsNewOptions(error) = error else {
            panic!("expected typed continue-as-new validation error");
        };
        assert_eq!(error.field, "task_queue");
        assert!(ctx.take_commands().expect("commands").is_empty());
    }

    #[test]
    fn workflow_context_exposes_server_history_budget() {
        let client = Client::new("http://127.0.0.1:8080").expect("client");
        let mut worker = Worker::new(client, "rust-workers");
        worker.register_workflow("rust.history-budget", |ctx, _input| async move {
            let budget = ctx.history_budget()?;
            Ok(json!({
                "events": budget.event_count,
                "bytes": budget.size_bytes,
                "recommended": budget.continue_as_new_recommended,
                "pressure": budget.pressure,
            }))
        });
        let task: WorkflowTask = serde_json::from_value(json!({
            "task_id": "task-history-budget",
            "workflow_type": "rust.history-budget",
            "payload_codec": DEFAULT_CODEC,
            "history_events": [],
            "total_history_events": 480,
            "history_size_bytes": 1_048_576,
            "continue_as_new_recommended": true,
            "history_budget_pressure": "continue_as_new_recommended",
        }))
        .expect("published workflow task");

        let commands = worker
            .execute_workflow_task(task)
            .expect("history-budget workflow");
        let result = decode_wire_value(&commands[0]["result"], DEFAULT_CODEC).expect("result");
        assert_eq!(result["events"], 480);
        assert_eq!(result["bytes"], 1_048_576);
        assert_eq!(result["recommended"], true);
        assert_eq!(result["pressure"], "continue_as_new_recommended");
    }

    #[test]
    fn uncaught_workflow_handler_error_emits_terminal_failure_command() {
        let client = Client::new("http://127.0.0.1:8080").expect("client");
        let mut worker = Worker::new(client, "rust-workers");
        worker.register_workflow("rust.failing", |_ctx, _input| async move {
            Err(Error::Codec("rust_conformance_failure".to_string()))
        });
        let task = WorkflowTask {
            task_id: "wft-rust-failing-1".to_string(),
            workflow_command_id: None,
            workflow_id: Some("wf-rust-failing".to_string()),
            run_id: Some("run-rust-failing".to_string()),
            workflow_type: "rust.failing".to_string(),
            cancel_requested: false,
            payload_codec: DEFAULT_CODEC.to_string(),
            arguments: Some(encode_value_envelope(&json!([]), DEFAULT_CODEC).expect("input")),
            history_events: Vec::new(),
            total_history_events: Some(0),
            history_size_bytes: None,
            continue_as_new_recommended: None,
            history_budget_pressure: None,
            next_history_page_token: None,
            workflow_task_attempt: 1,
            workflow_signal_id: None,
            signal_name: None,
            signal_arguments: None,
            workflow_update_id: None,
            update_name: None,
            lease_owner: Some("rust-worker".to_string()),
        };

        let commands = worker
            .execute_workflow_task(task)
            .expect("handler failure becomes a workflow command");

        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0]["type"], "fail_workflow");
        assert_eq!(commands[0]["exception_type"], "RustWorkflowError");
        assert_eq!(commands[0]["exception_class"], "durable_workflow::Error");
        assert_eq!(commands[0]["non_retryable"], false);
        assert_eq!(
            commands[0]["message"],
            "codec error: rust_conformance_failure"
        );
        assert_eq!(
            commands[0]["exception"]["message"],
            "codec error: rust_conformance_failure"
        );
    }

    #[test]
    fn ordinary_handler_error_preserves_commands_queued_in_the_same_decision() {
        let client = Client::new("http://127.0.0.1:8080").expect("client");
        let mut worker = Worker::new(client, "rust-workers");
        worker.register_workflow("rust.failing-after-side-effect", |ctx, _input| async move {
            let _: String = ctx.side_effect(|| "captured".to_string())?;
            Err(Error::WorkerLoop("application failure".to_string()))
        });

        let commands = worker
            .execute_workflow_task(workflow_task(
                "rust.failing-after-side-effect",
                Vec::new(),
                DEFAULT_CODEC,
            ))
            .expect("ordinary failure remains a workflow decision");

        assert_eq!(commands.len(), 2);
        assert_eq!(commands[0]["type"], "record_side_effect");
        assert_eq!(commands[1]["type"], "fail_workflow");
    }

    #[test]
    fn handler_error_cannot_hide_an_unconsumed_committed_side_effect() {
        let client = Client::new("http://127.0.0.1:8080").expect("client");
        let mut worker = Worker::new(client, "rust-workers");
        worker.register_workflow("rust.removed-side-effect", |_ctx, _input| async move {
            Err(Error::WorkerLoop("application failure".to_string()))
        });
        let result =
            encode_value_envelope(&json!("committed"), DEFAULT_CODEC).expect("side-effect result");

        let error = worker
            .execute_workflow_task(workflow_task(
                "rust.removed-side-effect",
                vec![history_event(
                    "SideEffectRecorded",
                    json!({"sequence": 1, "result": result}),
                )],
                DEFAULT_CODEC,
            ))
            .expect_err("removed committed history must not become fail_workflow");

        let Error::NonDeterministicReplay(failure) = error else {
            panic!("expected typed replay failure");
        };
        assert_eq!(failure.reason, "recorded_commands_unconsumed");
        assert_eq!(failure.sequence, Some(1));
        assert_eq!(failure.expected.as_deref(), Some("side effect"));
    }

    #[test]
    fn replay_error_discards_side_effect_queued_before_incompatible_marker_check() {
        let client = Client::new("http://127.0.0.1:8080").expect("client");
        let mut worker = Worker::new(client, "rust-workers");
        worker.register_workflow(
            "rust.side-effect-before-marker-error",
            |ctx, _input| async move {
                assert_eq!(ctx.get_version("restart-safe", 1, 1)?, 1);
                let _: String = ctx.side_effect(|| "must-not-commit".to_string())?;
                ctx.get_version("restart-safe", 2, 2)?;
                Ok(Value::Null)
            },
        );

        let error = worker
            .execute_workflow_task(workflow_task(
                "rust.side-effect-before-marker-error",
                vec![history_event(
                    "VersionMarkerRecorded",
                    json!({
                        "sequence": 1,
                        "change_id": "restart-safe",
                        "version": 1,
                        "min_supported": 1,
                        "max_supported": 1,
                    }),
                )],
                DEFAULT_CODEC,
            ))
            .expect_err("replay error must return no queued workflow commands");

        let Error::NonDeterministicReplay(failure) = error else {
            panic!("expected typed replay failure");
        };
        assert_eq!(failure.reason, "version_marker_incompatible_range");
        assert_eq!(failure.sequence, Some(1));
    }

    #[test]
    fn workflow_task_replay_keeps_recorded_unfired_timer_pending_without_rescheduling() {
        let client = Client::new("http://127.0.0.1:8080").expect("client");
        let mut worker = Worker::new(client, "rust-workers");
        worker.register_workflow("rust.timer.pending", |ctx, _input| async move {
            ctx.sleep(Duration::from_secs(5)).await?;
            Ok(json!({"status": "timer fired"}))
        });

        let task = WorkflowTask {
            task_id: "wft-rust-timer-pending".to_string(),
            workflow_command_id: None,
            workflow_id: Some("wf-rust-timer".to_string()),
            run_id: Some("run-rust-timer".to_string()),
            workflow_type: "rust.timer.pending".to_string(),
            cancel_requested: false,
            payload_codec: DEFAULT_CODEC.to_string(),
            arguments: Some(
                encode_value_envelope(&json!([]), DEFAULT_CODEC).expect("workflow input"),
            ),
            history_events: vec![history_event(
                "TimerScheduled",
                json!({"sequence": 1, "timer_id": "timer-1", "delay_seconds": 5}),
            )],
            total_history_events: Some(1),
            history_size_bytes: None,
            continue_as_new_recommended: None,
            history_budget_pressure: None,
            next_history_page_token: None,
            workflow_task_attempt: 1,
            workflow_signal_id: None,
            signal_name: None,
            signal_arguments: None,
            workflow_update_id: None,
            update_name: None,
            lease_owner: Some("rust-worker".to_string()),
        };

        for _redelivery_or_restart in 0..2 {
            let commands = worker
                .execute_workflow_task(task.clone())
                .expect("recorded timer remains pending");
            assert!(
                commands.is_empty(),
                "recorded timer must not be rescheduled"
            );
        }
    }

    #[test]
    fn workflow_task_rejects_recorded_command_removed_from_workflow_code() {
        let client = Client::new("http://127.0.0.1:8080").expect("client");
        let mut worker = Worker::new(client, "rust-workers");
        worker.register_workflow("rust.timer.removed", |_ctx, _input| async move {
            Ok(json!({"status": "completed"}))
        });
        let task = WorkflowTask {
            task_id: "wft-rust-timer-removed".to_string(),
            workflow_command_id: None,
            workflow_id: Some("wf-rust-timer".to_string()),
            run_id: Some("run-rust-timer".to_string()),
            workflow_type: "rust.timer.removed".to_string(),
            cancel_requested: false,
            payload_codec: DEFAULT_CODEC.to_string(),
            arguments: Some(
                encode_value_envelope(&json!([]), DEFAULT_CODEC).expect("workflow input"),
            ),
            history_events: vec![
                history_event(
                    "TimerScheduled",
                    json!({"sequence": 1, "timer_id": "timer-1", "delay_seconds": 5}),
                ),
                history_event(
                    "TimerFired",
                    json!({"sequence": 1, "timer_id": "timer-1", "delay_seconds": 5}),
                ),
            ],
            total_history_events: Some(2),
            history_size_bytes: None,
            continue_as_new_recommended: None,
            history_budget_pressure: None,
            next_history_page_token: None,
            workflow_task_attempt: 1,
            workflow_signal_id: None,
            signal_name: None,
            signal_arguments: None,
            workflow_update_id: None,
            update_name: None,
            lease_owner: Some("rust-worker".to_string()),
        };

        let Error::NonDeterministicReplay(failure) = worker
            .execute_workflow_task(task)
            .expect_err("removed timer must fail replay")
        else {
            panic!("expected typed replay failure");
        };
        assert_eq!(failure.reason, "recorded_commands_unconsumed");
        assert_eq!(failure.sequence, Some(1));
    }

    #[test]
    fn workflow_context_emits_explicit_child_workflow_contract() {
        let ctx = WorkflowContext {
            state: Arc::new(Mutex::new(
                WorkflowState::new_with_identity(
                    Vec::new(),
                    Some("wf-parent".to_string()),
                    Some("run-parent".to_string()),
                    "parent-workers".to_string(),
                    DEFAULT_CODEC.to_string(),
                    None,
                )
                .expect("workflow state"),
            )),
        };
        let options = ChildWorkflowOptions::new("python-workers")
            .parent_close_policy(ParentClosePolicy::RequestCancel)
            .retry_policy(ChildWorkflowRetryPolicy {
                max_attempts: Some(3),
                backoff_seconds: vec![1, 5],
                non_retryable_error_types: vec!["ValidationError".to_string()],
            })
            .execution_timeout_seconds(600)
            .run_timeout_seconds(120);
        let mut call = Box::pin(ctx.start_child_workflow(
            "python.fulfil-order",
            options,
            json!([{"order_id": "order-42"}]),
        ));
        let mut task_context = TaskContext::from_waker(noop_waker_ref());

        assert!(matches!(
            call.as_mut().poll(&mut task_context),
            Poll::Pending
        ));
        let commands = ctx.take_commands().expect("commands");
        assert_eq!(commands.len(), 1);
        let command = &commands[0];
        assert_eq!(command["type"], "start_child_workflow");
        assert_eq!(command["workflow_type"], "python.fulfil-order");
        assert_eq!(command["queue"], "python-workers");
        assert_eq!(command["parent_close_policy"], "request_cancel");
        assert_eq!(command["retry_policy"]["max_attempts"], 3);
        assert_eq!(command["execution_timeout_seconds"], 600);
        assert_eq!(command["run_timeout_seconds"], 120);
        assert_eq!(
            decode_wire_value(&command["arguments"], DEFAULT_CODEC).expect("child args"),
            json!([{"order_id": "order-42"}])
        );
    }

    fn child_parent_worker() -> Worker {
        let client = Client::new("http://127.0.0.1:8080").expect("client");
        let mut worker = Worker::new(client, "rust-parent-workers");
        worker.register_workflow("rust.parent", |ctx, _input| async move {
            let child = ctx
                .start_child_workflow(
                    "python.child",
                    ChildWorkflowOptions::new("python-child-workers")
                        .parent_close_policy(ParentClosePolicy::Terminate),
                    json!([{"codec_probe": [1, true, "rust"]}]),
                )
                .await?;
            Ok(json!({
                "parent_workflow_id": child.parent.workflow_id,
                "parent_run_id": child.parent.run_id,
                "child_workflow_id": child.child.workflow_id,
                "child_run_id": child.child.run_id,
                "child_workflow_type": child.child_workflow_type,
                "result": child.result,
            }))
        });
        worker
    }

    fn child_parent_task(event_type: &str, payload: Value) -> WorkflowTask {
        WorkflowTask {
            task_id: "wft-child-parent".to_string(),
            workflow_command_id: None,
            workflow_id: Some("wf-parent".to_string()),
            run_id: Some("run-parent".to_string()),
            workflow_type: "rust.parent".to_string(),
            cancel_requested: false,
            payload_codec: DEFAULT_CODEC.to_string(),
            arguments: Some(encode_value_envelope(&json!([]), DEFAULT_CODEC).expect("input")),
            history_events: vec![
                HistoryEvent {
                    event_type: "ChildWorkflowScheduled".to_string(),
                    payload: json!({
                        "sequence": 1,
                        "child_call_id": "call-child",
                        "child_workflow_instance_id": "wf-child",
                        "child_workflow_run_id": "run-child",
                        "child_workflow_type": "python.child",
                    }),
                    raw: HashMap::new(),
                },
                HistoryEvent {
                    event_type: event_type.to_string(),
                    payload,
                    raw: HashMap::new(),
                },
            ],
            total_history_events: Some(2),
            history_size_bytes: None,
            continue_as_new_recommended: None,
            history_budget_pressure: None,
            next_history_page_token: None,
            workflow_task_attempt: 1,
            workflow_signal_id: None,
            signal_name: None,
            signal_arguments: None,
            workflow_update_id: None,
            update_name: None,
            lease_owner: Some("rust-worker".to_string()),
        }
    }

    #[test]
    fn committed_child_result_replays_without_starting_a_duplicate() {
        let worker = child_parent_worker();
        let task = child_parent_task(
            "ChildRunCompleted",
            json!({
                "sequence": 1,
                "child_call_id": "call-child",
                "child_workflow_instance_id": "wf-child",
                "child_workflow_run_id": "run-child",
                "child_workflow_type": "python.child",
                "payload_codec": DEFAULT_CODEC,
                "result": fixture_envelope(json!({"from":"python","ok":true})),
            }),
        );

        for _restart in 0..2 {
            let commands = worker
                .execute_workflow_task(task.clone())
                .expect("replayed parent task");
            assert_eq!(commands.len(), 1);
            assert_eq!(commands[0]["type"], "complete_workflow");
            assert!(!commands
                .iter()
                .any(|command| command["type"] == "start_child_workflow"));
            let output =
                decode_wire_value(&commands[0]["result"], DEFAULT_CODEC).expect("parent output");
            assert_eq!(output["parent_workflow_id"], "wf-parent");
            assert_eq!(output["parent_run_id"], "run-parent");
            assert_eq!(output["child_workflow_id"], "wf-child");
            assert_eq!(output["child_run_id"], "run-child");
            assert_eq!(output["result"], json!({"from": "python", "ok": true}));
        }
    }

    #[test]
    fn typed_child_arguments_and_results_survive_replay() {
        let client = Client::new("http://127.0.0.1:8080").expect("client");
        let mut worker = Worker::new(client, "rust-parent-workers");
        worker.register_workflow_avro_value("rust.typed-parent", |ctx, _input| async move {
            let child = ctx
                .start_child_workflow_avro_value(
                    "python.typed-child",
                    ChildWorkflowOptions::new("python-workers"),
                    AvroValue::Array(vec![typed_fidelity_probe()]),
                )
                .await?;
            Ok(child.result)
        });

        let initial = worker
            .execute_workflow_task(workflow_task(
                "rust.typed-parent",
                Vec::new(),
                DEFAULT_CODEC,
            ))
            .expect("typed child start");
        assert_eq!(initial[0]["type"], "start_child_workflow");
        assert_eq!(
            decode_wire_avro_value(&initial[0]["arguments"], DEFAULT_CODEC)
                .expect("typed child arguments"),
            AvroValue::Array(vec![typed_fidelity_probe()])
        );

        let result = encode_typed_envelope(&typed_fidelity_probe(), DEFAULT_CODEC)
            .expect("typed child result");
        let task = workflow_task(
            "rust.typed-parent",
            vec![
                history_event(
                    "ChildWorkflowScheduled",
                    json!({
                        "sequence": 1,
                        "child_call_id": "call-typed",
                        "child_workflow_instance_id": "wf-child",
                        "child_workflow_run_id": "run-child",
                        "child_workflow_type": "python.typed-child",
                    }),
                ),
                history_event(
                    "ChildRunCompleted",
                    json!({
                        "sequence": 1,
                        "child_call_id": "call-typed",
                        "child_workflow_instance_id": "wf-child",
                        "child_workflow_run_id": "run-child",
                        "child_workflow_type": "python.typed-child",
                        "payload_codec": DEFAULT_CODEC,
                        "result": result,
                    }),
                ),
            ],
            DEFAULT_CODEC,
        );

        let commands = worker
            .execute_workflow_task(task)
            .expect("typed child replay");
        assert_eq!(commands[0]["type"], "complete_workflow");
        assert_eq!(
            decode_wire_avro_value(&commands[0]["result"], DEFAULT_CODEC)
                .expect("typed parent result"),
            typed_fidelity_probe()
        );
    }

    #[test]
    fn pending_child_replays_after_restart_without_starting_a_duplicate() {
        let worker = child_parent_worker();
        let mut task = child_parent_task("unused", Value::Null);
        task.history_events.truncate(1);
        task.total_history_events = Some(1);

        for _redelivery_or_restart in 0..2 {
            let commands = worker
                .execute_workflow_task(task.clone())
                .expect("recorded child remains pending");
            assert!(
                commands.is_empty(),
                "recorded pending child must not be started again"
            );
        }
    }

    #[test]
    fn child_cancellation_becomes_stable_parent_failure_command() {
        let worker = child_parent_worker();
        let task = child_parent_task(
            "ChildRunCancelled",
            json!({
                "sequence": 1,
                "child_workflow_instance_id": "wf-child",
                "child_workflow_run_id": "run-child",
                "child_workflow_type": "python.child",
                "failure_id": "failure-child",
                "failure_category": "cancelled",
                "message": "cancelled by parent-close policy",
            }),
        );

        let commands = worker
            .execute_workflow_task(task)
            .expect("parent settlement");
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0]["type"], "fail_workflow");
        assert_eq!(commands[0]["exception_type"], "ChildWorkflowCancelled");
        assert_eq!(
            commands[0]["exception"]["properties"]["reason"],
            "cancelled"
        );
        assert_eq!(
            commands[0]["exception"]["properties"]["child_workflow_run_id"],
            "run-child"
        );
    }

    #[test]
    fn workflow_can_handle_typed_child_failure() {
        let client = Client::new("http://127.0.0.1:8080").expect("client");
        let mut worker = Worker::new(client, "rust-parent-workers");
        worker.register_workflow("rust.handled-parent", |ctx, _input| async move {
            match ctx
                .start_child_workflow(
                    "python.child",
                    ChildWorkflowOptions::new("python-child-workers"),
                    json!([]),
                )
                .await
            {
                Err(Error::ChildWorkflowFailed(failure)) => Ok(json!({
                    "reason": failure.reason,
                    "failure_id": failure.failure_id,
                    "exception_class": failure.exception_class,
                    "child_run_id": failure.child_workflow_run_id,
                })),
                Err(error) => Err(error),
                Ok(_) => Err(Error::WorkerLoop(
                    "child unexpectedly succeeded".to_string(),
                )),
            }
        });
        let mut task = child_parent_task(
            "ChildRunFailed",
            json!({
                "sequence": 1,
                "child_workflow_instance_id": "wf-child",
                "child_workflow_run_id": "run-child",
                "child_workflow_type": "python.child",
                "failure_id": "failure-child",
                "failure_category": "child_workflow",
                "message": "payment rejected",
                "exception": {
                    "type": "PaymentRejected",
                    "class": "payments.PaymentRejected",
                    "message": "payment rejected"
                }
            }),
        );
        task.workflow_type = "rust.handled-parent".to_string();

        let commands = worker.execute_workflow_task(task).expect("handled failure");
        assert_eq!(commands[0]["type"], "complete_workflow");
        let output =
            decode_wire_value(&commands[0]["result"], DEFAULT_CODEC).expect("parent output");
        assert_eq!(output["reason"], "child_workflow");
        assert_eq!(output["failure_id"], "failure-child");
        assert_eq!(output["exception_class"], "payments.PaymentRejected");
        assert_eq!(output["child_run_id"], "run-child");
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
            workflow_command_id: None,
            workflow_id: Some("wf-rust-hello".to_string()),
            run_id: Some("run-rust-hello".to_string()),
            workflow_type: "rust.hello_workflow".to_string(),
            cancel_requested: false,
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
            history_size_bytes: None,
            continue_as_new_recommended: None,
            history_budget_pressure: None,
            next_history_page_token: None,
            workflow_task_attempt: 1,
            workflow_signal_id: Some("sig-rust-1".to_string()),
            signal_name: Some("start".to_string()),
            signal_arguments: Some(signal_arguments),
            workflow_update_id: None,
            update_name: None,
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
            workflow_command_id: None,
            workflow_id: Some("wf-rust-pages".to_string()),
            run_id: Some("run-rust-pages".to_string()),
            workflow_type: "rust.hello_workflow".to_string(),
            cancel_requested: false,
            payload_codec: DEFAULT_CODEC.to_string(),
            arguments: Some(encode_value_envelope(&json!([]), DEFAULT_CODEC).expect("input")),
            history_events: vec![HistoryEvent {
                event_type: "WorkflowStarted".to_string(),
                payload: json!({}),
                raw: HashMap::new(),
            }],
            total_history_events: Some(3),
            history_size_bytes: None,
            continue_as_new_recommended: None,
            history_budget_pressure: None,
            next_history_page_token: Some("MQ==".to_string()),
            workflow_task_attempt: 1,
            workflow_signal_id: None,
            signal_name: None,
            signal_arguments: None,
            workflow_update_id: None,
            update_name: None,
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

        let signal = task
            .history_events
            .iter()
            .find(|event| event.event_type == "SignalReceived")
            .expect("signal event");
        assert_eq!(
            decode_signal_event_arguments(signal, DEFAULT_CODEC).expect("signal arguments"),
            vec![AvroValue::String("Rust".to_string())]
        );
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
                        "payload_codec": DEFAULT_CODEC,
                        "arguments": encode_value_envelope(&json!([5]), DEFAULT_CODEC).expect("python avro signal")
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
        assert_eq!(result.into_json().expect("query projection"), json!(0));
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
                    "payload_codec": DEFAULT_CODEC,
                    "result": fixture_envelope(json!("loaded"))
                }
            },
            {
                "type": "SignalWaitOpened",
                "payload": {
                    "sequence": 3,
                    "signal_name": "increment"
                }
            },
            {
                "type": "SignalReceived",
                "payload": {
                    "signal_id": "signal-3",
                    "signal_name": "increment",
                    "workflow_sequence": 2,
                    "payload_codec": DEFAULT_CODEC,
                    "arguments": fixture_envelope(json!([3]))
                }
            },
            {
                "type": "SignalApplied",
                "payload": {
                    "sequence": 3,
                    "signal_id": "signal-3",
                    "signal_name": "increment",
                    "payload_codec": DEFAULT_CODEC,
                    "value": fixture_envelope(json!([3]))
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
            running.clone().into_json().expect("query projection"),
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
        assert_eq!(detached.into_json().expect("query projection"), json!(999));
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
        let empty_arguments = fixture_envelope(json!([]));
        let loaded_result = fixture_envelope(json!("loaded"));
        let signal_three = fixture_blob(json!([3]));
        let signal_five = fixture_blob(json!([5]));
        let restarted_task: QueryTask = serde_json::from_value(json!({
            "query_task_id": "query-after-restart",
            "workflow_id": "counter-1",
            "run_id": "run-counter-1",
            "workflow_type": "replay-counter",
            "query_name": "current",
            "payload_codec": DEFAULT_CODEC,
            "workflow_arguments": empty_arguments.clone(),
            "query_arguments": empty_arguments,
            "history_events": [],
            "history_export": {
                "payloads": {"codec": DEFAULT_CODEC},
                "history_events": [
                    {
                        "type": "ActivityCompleted",
                        "payload": {
                            "sequence": 1,
                            "activity_type": "load-counter",
                            "payload_codec": DEFAULT_CODEC,
                            "result": null
                        }
                    },
                    {
                        "type": "SignalWaitOpened",
                        "payload": {
                            "sequence": 3,
                            "signal_name": "increment"
                        }
                    },
                    {
                        "type": "SignalReceived",
                        "payload": {
                            "signal_id": "signal-3",
                            "signal_name": "increment",
                            "workflow_sequence": 2
                        }
                    },
                    {
                        "type": "SignalApplied",
                        "payload": {
                            "sequence": 3,
                            "signal_id": "signal-3",
                            "signal_name": "increment"
                        }
                    },
                    {
                        "type": "SignalWaitOpened",
                        "payload": {
                            "sequence": 5,
                            "signal_name": "increment"
                        }
                    },
                    {
                        "type": "SignalReceived",
                        "payload": {
                            "signal_id": "signal-5",
                            "signal_name": "increment",
                            "workflow_sequence": 4
                        }
                    },
                    {
                        "type": "SignalApplied",
                        "payload": {
                            "sequence": 5,
                            "signal_id": "signal-5",
                            "signal_name": "increment"
                        }
                    }
                ],
                "activities": [{
                    "sequence": 1,
                    "activity_type": "load-counter",
                    "payload_codec": DEFAULT_CODEC,
                    "result": loaded_result
                }],
                "signals": [
                    {
                        "id": "signal-3",
                        "name": "increment",
                        "workflow_sequence": 2,
                        "payload_codec": DEFAULT_CODEC,
                        "arguments": signal_three
                    },
                    {
                        "id": "signal-5",
                        "name": "increment",
                        "workflow_sequence": 4,
                        "payload_codec": DEFAULT_CODEC,
                        "arguments": signal_five
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
            completed.into_json().expect("query projection"),
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
                    "payload_codec": DEFAULT_CODEC,
                    "result": {"codec": DEFAULT_CODEC, "blob": "{"}
                }
            }]),
            "running",
        );
        let failure = worker
            .execute_query_task(task)
            .await
            .expect_err("invalid replay history payload");
        assert_eq!(failure.reason, "query_payload_decode_failed");
        assert_eq!(failure.failure_type, "QueryPayloadDecodeFailed");
        assert!(failure.message.contains("invalid_payload_framing"));
    }

    #[tokio::test]
    async fn query_task_restores_compact_history_from_export() {
        let client = Client::new("http://127.0.0.1:8080").expect("client");
        let mut worker = Worker::new(client, "rust-workers");
        worker.register_workflow("counter", |_ctx, _input| async move { Ok(Value::Null) });
        worker.register_query("counter", "current", |ctx, _args| async move {
            Ok(json!(ctx.signals("increment")[0][0]))
        });
        let empty_arguments = fixture_envelope(json!([]));
        let exported_signal = fixture_blob(json!([9]));
        let task: QueryTask = serde_json::from_value(json!({
            "query_task_id": "query-export",
            "workflow_type": "counter",
            "query_name": "current",
            "payload_codec": DEFAULT_CODEC,
            "workflow_arguments": empty_arguments.clone(),
            "query_arguments": empty_arguments,
            "history_events": [],
            "history_export": {
                "payloads": {"codec": DEFAULT_CODEC},
                "history_events": [{
                    "type": "SignalReceived",
                    "payload": {"signal_id": "signal-export", "signal_name": "increment"}
                }],
                "signals": [{
                    "id": "signal-export",
                    "name": "increment",
                    "status": "applied",
                    "workflow_sequence": 1,
                    "payload_codec": DEFAULT_CODEC,
                    "arguments": exported_signal
                }]
            }
        }))
        .expect("query task");

        let result = worker.execute_query_task(task).await.expect("query result");
        assert_eq!(result.into_json().expect("query projection"), json!(9));
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
            payload_codec: DEFAULT_CODEC.to_string(),
            workflow_arguments: Some(fixture_envelope(json!([]))),
            query_arguments: Some(fixture_envelope(json!([]))),
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
        malformed.query_arguments = Some(json!({"codec": DEFAULT_CODEC, "blob": "{"}));
        let malformed = worker
            .execute_query_task(malformed)
            .await
            .expect_err("malformed payload");
        assert_eq!(malformed.reason, "query_payload_decode_failed");

        let client = Client::new("http://127.0.0.1:8080").expect("client");
        let mut unavailable_worker = Worker::new(client, "rust-workers");
        unavailable_worker
            .register_workflow("counter", |_ctx, _input| async move { Ok(Value::Null) });
        let empty_arguments = fixture_envelope(json!([]));
        let unavailable_task: QueryTask = serde_json::from_value(json!({
            "query_task_id": "query-unavailable",
            "workflow_type": "counter",
            "query_name": "current",
            "payload_codec": DEFAULT_CODEC,
            "workflow_arguments": empty_arguments.clone(),
            "query_arguments": empty_arguments
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
    async fn public_client_surfaces_send_and_receive_lossless_avro_values() {
        let server = MockWorkerServer::start();
        let client = Client::builder(server.base_url())
            .timeout(Duration::from_secs(2))
            .build()
            .expect("client");
        let arguments = AvroValue::Array(vec![typed_fidelity_probe()]);

        client
            .start_workflow(
                "typed.echo",
                "rust-workers",
                "typed-start",
                arguments.clone(),
            )
            .await
            .expect("typed workflow start");
        assert_eq!(
            decode_wire_avro_value(
                &server.request_body("/api/workflows")["input"],
                DEFAULT_CODEC,
            )
            .expect("typed start input"),
            arguments
        );

        client
            .signal_workflow("typed-1", "changed", arguments.clone())
            .await
            .expect("typed signal");
        assert_eq!(
            decode_wire_avro_value(
                &server.request_body("/api/workflows/typed-1/signal/changed")["input"],
                DEFAULT_CODEC,
            )
            .expect("typed signal input"),
            arguments
        );

        assert_eq!(
            client
                .query_workflow_avro_value("typed-1", "inspect", arguments.clone())
                .await
                .expect("typed query"),
            typed_fidelity_probe()
        );
        assert_eq!(
            decode_wire_avro_value(
                &server.request_body("/api/workflows/typed-1/query/inspect")["input"],
                DEFAULT_CODEC,
            )
            .expect("typed query input"),
            arguments
        );

        assert_eq!(
            client
                .update_workflow_avro_value(
                    "typed-1",
                    "replace",
                    arguments.clone(),
                    Some("typed-request"),
                )
                .await
                .expect("typed update"),
            typed_fidelity_probe()
        );
        let update = server.request_body("/api/workflows/typed-1/update/replace");
        assert_eq!(update["request_id"], "typed-request");
        assert_eq!(
            decode_wire_avro_value(&update["input"], DEFAULT_CODEC).expect("typed update input"),
            arguments
        );

        let handle = WorkflowHandle {
            client: client.clone(),
            workflow_id: "typed-1".to_string(),
            run_id: Some("run-typed-1".to_string()),
            workflow_type: "typed.echo".to_string(),
        };
        assert_eq!(
            handle
                .result_avro_value(WorkflowResultOptions::default())
                .await
                .expect("typed workflow result"),
            typed_fidelity_probe()
        );

        client
            .complete_activity_task(
                "activity-typed",
                "attempt-typed",
                "rust-worker",
                typed_fidelity_probe(),
                DEFAULT_CODEC,
            )
            .await
            .expect("typed activity completion");
        assert_eq!(
            decode_wire_avro_value(
                &server.request_body("/api/worker/activity-tasks/activity-typed/complete")
                    ["result"],
                DEFAULT_CODEC,
            )
            .expect("typed activity result"),
            typed_fidelity_probe()
        );
        client
            .fail_activity_task(
                "activity-typed",
                "attempt-typed",
                "rust-worker",
                "typed failure",
                true,
            )
            .await
            .expect("activity failure");
    }

    #[tokio::test]
    async fn lifecycle_commands_support_instance_and_selected_run_targets() {
        let server = MockWorkerServer::start();
        let client = Client::builder(server.base_url())
            .timeout(Duration::from_secs(2))
            .build()
            .expect("client");

        let options = WorkflowCommandOptions::new()
            .reason("cleanup requested")
            .request_id("cancel-17");
        let cancelled = client
            .cancel_workflow("wf-lifecycle", options)
            .await
            .expect("instance cancellation");
        assert_eq!(cancelled.command, WorkflowCommandKind::Cancel);
        assert_eq!(cancelled.run_id.as_deref(), Some("run-current"));
        assert_eq!(cancelled.outcome.as_deref(), Some("cancelled"));
        assert_eq!(
            server.request_body("/api/workflows/wf-lifecycle/cancel"),
            json!({"reason":"cleanup requested","request_id":"cancel-17"})
        );

        let terminated = client
            .terminate_workflow(
                "wf-lifecycle",
                WorkflowCommandOptions::new().reason("forced stop"),
            )
            .await
            .expect("instance termination");
        assert_eq!(terminated.command, WorkflowCommandKind::Terminate);
        assert_eq!(terminated.outcome.as_deref(), Some("terminated"));

        client
            .cancel_workflow_run(
                "wf-lifecycle",
                "run-current",
                WorkflowCommandOptions::default(),
            )
            .await
            .expect("selected run cancellation");
        client
            .terminate_workflow_run(
                "wf-lifecycle",
                "run-current",
                WorkflowCommandOptions::default(),
            )
            .await
            .expect("selected run termination");

        for (command, error) in [
            (
                WorkflowCommandKind::Cancel,
                client
                    .cancel_workflow_run(
                        "wf-lifecycle",
                        "run-stale",
                        WorkflowCommandOptions::default(),
                    )
                    .await
                    .expect_err("stale cancellation must be rejected"),
            ),
            (
                WorkflowCommandKind::Terminate,
                client
                    .terminate_workflow_run(
                        "wf-lifecycle",
                        "run-stale",
                        WorkflowCommandOptions::default(),
                    )
                    .await
                    .expect_err("stale termination must be rejected"),
            ),
        ] {
            let Error::WorkflowCommandRejected(rejection) = error else {
                panic!("expected typed command rejection");
            };
            assert_eq!(rejection.command, command);
            assert_eq!(rejection.status, 409);
            assert_eq!(rejection.reason, "historical_run_command_rejected");
            assert_eq!(rejection.run_id.as_deref(), Some("run-stale"));
            assert_eq!(rejection.target_scope.as_deref(), Some("run"));
        }
    }

    #[tokio::test]
    async fn workflow_start_options_send_server_enforced_deadlines() {
        let server = MockWorkerServer::start();
        let client = Client::builder(server.base_url())
            .timeout(Duration::from_secs(2))
            .build()
            .expect("client");

        let handle = client
            .start_workflow_with_options(
                "rust.timeout",
                "rust-timeouts",
                "wf-start-options",
                WorkflowStartOptions::new()
                    .execution_timeout_seconds(30)
                    .run_timeout_seconds(1),
                json!([]),
            )
            .await
            .expect("workflow start");

        assert_eq!(handle.run_id.as_deref(), Some("run-start-options"));
        let body = server.request_body("/api/workflows");
        assert_eq!(body["execution_timeout_seconds"], 30);
        assert_eq!(body["run_timeout_seconds"], 1);

        let invalid = client
            .start_workflow_with_options(
                "rust.timeout",
                "rust-timeouts",
                "wf-invalid-options",
                WorkflowStartOptions::new()
                    .execution_timeout_seconds(1)
                    .run_timeout_seconds(2),
                json!([]),
            )
            .await
            .expect_err("invalid deadline ordering");
        assert!(invalid
            .to_string()
            .contains("run_timeout_seconds cannot exceed execution_timeout_seconds"));
    }

    #[tokio::test]
    async fn workflow_result_returns_each_typed_terminal_outcome() {
        let server = MockWorkerServer::start();
        let client = Client::builder(server.base_url())
            .timeout(Duration::from_secs(2))
            .build()
            .expect("client");
        let options = WorkflowResultOptions {
            poll_interval: Duration::ZERO,
            timeout: Duration::from_secs(1),
        };

        let failed = WorkflowHandle {
            client: client.clone(),
            workflow_id: "wf-failed".to_string(),
            run_id: Some("run-failed".to_string()),
            workflow_type: "failure".to_string(),
        }
        .result(options)
        .await
        .expect_err("failed outcome");
        let Error::WorkflowFailed(failure) = failed else {
            panic!("expected WorkflowFailed");
        };
        assert_eq!(failure.workflow_id, "wf-failed");
        assert_eq!(failure.run_id.as_deref(), Some("run-failed"));
        assert_eq!(failure.failure_id.as_deref(), Some("failure-17"));
        assert_eq!(failure.failure_category.as_deref(), Some("application"));
        assert_eq!(failure.exception_type.as_deref(), Some("PaymentError"));
        assert_eq!(
            failure.exception_class.as_deref(),
            Some("billing::PaymentError")
        );
        assert_eq!(failure.non_retryable, Some(true));

        for (workflow_id, expected_kind, expected_reason) in [
            (
                "wf-cancelled",
                WorkflowTerminalKind::Cancelled,
                "cleanup requested",
            ),
            (
                "wf-terminated",
                WorkflowTerminalKind::Terminated,
                "forced stop",
            ),
            (
                "wf-timed-out",
                WorkflowTerminalKind::TimedOut,
                "run_timeout",
            ),
        ] {
            let error = WorkflowHandle {
                client: client.clone(),
                workflow_id: workflow_id.to_string(),
                run_id: None,
                workflow_type: "terminal".to_string(),
            }
            .result(options)
            .await
            .expect_err("typed terminal outcome");
            let outcome = match error {
                Error::WorkflowCancelled(outcome) => outcome,
                Error::WorkflowTerminated(outcome) => outcome,
                Error::WorkflowTimedOut(outcome) => outcome,
                other => panic!("unexpected terminal error: {other}"),
            };
            assert_eq!(outcome.kind, expected_kind);
            assert_eq!(outcome.workflow_id, workflow_id);
            assert_eq!(outcome.reason, expected_reason);
        }

        let wait_timeout = WorkflowHandle {
            client,
            workflow_id: "wf-waiting".to_string(),
            run_id: Some("run-waiting".to_string()),
            workflow_type: "waiting".to_string(),
        }
        .result(WorkflowResultOptions {
            poll_interval: Duration::ZERO,
            timeout: Duration::ZERO,
        })
        .await
        .expect_err("client wait timeout");
        let Error::WorkflowTimedOut(timeout) = wait_timeout else {
            panic!("expected typed client timeout");
        };
        assert_eq!(timeout.reason, "result_wait_timeout");
        assert_eq!(timeout.failure_category.as_deref(), Some("client_timeout"));
        assert_eq!(timeout.run_id.as_deref(), Some("run-waiting"));
    }

    #[tokio::test]
    async fn workflow_result_follows_chain_and_selected_result_preserves_history() {
        let server = MockWorkerServer::start();
        let client = Client::builder(server.base_url())
            .timeout(Duration::from_secs(2))
            .build()
            .expect("client");

        let handle = WorkflowHandle {
            client,
            workflow_id: "wf-selected".to_string(),
            run_id: Some("run-selected".to_string()),
            workflow_type: "selected".to_string(),
        };
        let options = WorkflowResultOptions {
            poll_interval: Duration::ZERO,
            timeout: Duration::from_secs(1),
        };

        let current = handle
            .result(options)
            .await
            .expect("instance result follows the current run");
        assert_eq!(current, json!("current run output"));

        let error = handle
            .result_selected_run(options)
            .await
            .expect_err("the selected run is cancelled even though the current run completed");

        let Error::WorkflowCancelled(outcome) = error else {
            panic!("expected selected run cancellation");
        };
        assert_eq!(outcome.run_id.as_deref(), Some("run-selected"));
        assert_eq!(outcome.reason, "selected run cancelled");
        assert_eq!(
            server.request_count("/api/workflows/wf-selected/runs/run-selected"),
            1
        );
        assert_eq!(server.request_count("/api/workflows/wf-selected"), 1);
    }

    #[tokio::test]
    async fn poll_responses_decode_http_conflict_drain_as_a_stable_stop() {
        let server = MockWorkerServer::draining_polls();
        let client = Client::builder(server.base_url())
            .timeout(Duration::from_secs(2))
            .build()
            .expect("client");

        let workflow = client
            .poll_workflow_task_response("draining-worker", "rust-workers", Duration::ZERO)
            .await
            .expect("workflow drain response");
        let activity = client
            .poll_activity_task_response("draining-worker", "rust-workers", Duration::ZERO)
            .await
            .expect("activity drain response");
        let query = client
            .poll_query_task_response("draining-worker", "rust-workers", Duration::ZERO)
            .await
            .expect("query drain response");

        for outcome in [workflow.outcome(), activity.outcome(), query.outcome()] {
            assert_eq!(
                outcome,
                WorkerPollOutcome::Stop {
                    poll_status: Some("draining".to_string()),
                    reason: Some("worker_draining".to_string()),
                }
            );
        }

        assert!(client
            .poll_workflow_task("draining-worker", "rust-workers", Duration::ZERO)
            .await
            .expect("compatibility poll")
            .is_none());
    }

    #[tokio::test]
    async fn managed_worker_honors_drain_stop_for_every_task_family() {
        let server = MockWorkerServer::draining_polls();
        let client = Client::builder(server.base_url())
            .timeout(Duration::from_secs(2))
            .build()
            .expect("client");

        let mut workflow_worker = Worker::new(client.clone(), "rust-workers")
            .worker_id("draining-workflow-worker")
            .poll_timeout(Duration::ZERO);
        workflow_worker.register_workflow("counter", |_ctx, _args| async { Ok(Value::Null) });
        workflow_worker
            .run()
            .await
            .expect("workflow drain is a clean stop");

        let mut activity_worker = Worker::new(client.clone(), "rust-workers")
            .worker_id("draining-activity-worker")
            .poll_timeout(Duration::ZERO);
        activity_worker.register_activity("write", |_ctx, _args| async { Ok(Value::Null) });
        activity_worker
            .run()
            .await
            .expect("activity drain is a clean stop");

        let mut query_worker = Worker::new(client, "rust-workers")
            .worker_id("draining-query-worker")
            .poll_timeout(Duration::ZERO);
        query_worker.register_query("counter", "current", |_ctx, _args| async {
            Ok(Value::Null)
        });
        query_worker
            .run()
            .await
            .expect("query drain is a clean stop");
    }

    #[tokio::test]
    async fn activity_cancellation_and_late_completion_remain_machine_readable() {
        let server = MockWorkerServer::start();
        let client = Client::builder(server.base_url())
            .timeout(Duration::from_secs(2))
            .build()
            .expect("client");

        let heartbeat = client
            .heartbeat_activity_task(
                "activity-cancel",
                "attempt-cancel",
                "rust-worker",
                typed_fidelity_probe(),
            )
            .await
            .expect("cancellation heartbeat");
        assert!(heartbeat.cancel_requested);
        assert!(heartbeat.should_stop());
        assert_eq!(heartbeat.reason.as_deref(), Some("run_cancelled"));
        assert_eq!(heartbeat.run_closed_reason.as_deref(), Some("cancelled"));
        let heartbeat_body =
            server.request_body("/api/worker/activity-tasks/activity-cancel/heartbeat");
        assert_eq!(heartbeat_body["details"]["codec"], DEFAULT_CODEC);
        assert_eq!(
            decode_wire_avro_value(&heartbeat_body["details"], DEFAULT_CODEC)
                .expect("typed heartbeat details"),
            typed_fidelity_probe()
        );

        let error = client
            .complete_activity_task(
                "activity-cancel",
                "attempt-cancel",
                "rust-worker",
                json!({"late":true}),
                DEFAULT_CODEC,
            )
            .await
            .expect_err("late completion must be refused");
        assert!(activity_task_rejection_is_final(&error));
        let Error::ActivityTaskRejected(rejection) = error else {
            panic!("expected typed activity rejection");
        };
        assert_eq!(rejection.status, 409);
        assert_eq!(rejection.reason, "run_cancelled");
        assert!(rejection.cancel_requested);
        assert_eq!(rejection.can_continue, Some(false));
    }

    #[tokio::test]
    async fn managed_worker_survives_late_completion_and_restart_during_cancellation() {
        let server = MockWorkerServer::cancelled_activity();
        let client = Client::builder(server.base_url())
            .timeout(Duration::from_secs(2))
            .build()
            .expect("client");
        let cancellation_observed = Arc::new(AtomicBool::new(false));
        let observed = Arc::clone(&cancellation_observed);
        let mut worker = Worker::new(client.clone(), "rust-workers")
            .worker_id("rust-cancel-worker")
            .poll_timeout(Duration::from_millis(10));
        worker.register_activity("cancel-aware", move |ctx, _args| {
            let observed = Arc::clone(&observed);
            async move {
                let heartbeat = ctx.heartbeat(json!({"stage":"running"})).await?;
                observed.store(heartbeat.should_stop(), Ordering::SeqCst);
                Ok(json!({"late":"completion"}))
            }
        });

        assert_eq!(
            worker.run_once().await.expect("cancelled attempt handled"),
            1
        );
        assert!(cancellation_observed.load(Ordering::SeqCst));
        assert_eq!(
            server.request_count("/api/worker/activity-tasks/activity-cancel/complete"),
            1
        );

        let mut restarted = Worker::new(client, "rust-workers")
            .worker_id("rust-cancel-worker-restarted")
            .poll_timeout(Duration::from_millis(10));
        restarted.register_activity("cancel-aware", |_ctx, _args| async move { Ok(Value::Null) });
        assert_eq!(
            restarted
                .run_once()
                .await
                .expect("replacement worker continues polling"),
            0
        );
    }

    #[tokio::test]
    async fn managed_worker_absorbs_selected_run_terminal_timeout_completion_race() {
        let response = r#"{"task_id":"workflow-timeout-task","workflow_task_attempt":3,"outcome":"completed","recorded":false,"run_id":"run-selected-timeout","run_status":"failed","created_task_ids":[],"reason":"run_timed_out"}"#;
        let server = MockWorkerServer::workflow_completion("409 Conflict", response);
        let client = Client::builder(server.base_url())
            .timeout(Duration::from_secs(2))
            .build()
            .expect("client");

        let direct_error = client
            .complete_workflow_task(
                "workflow-timeout-task",
                "timeout-worker",
                3,
                vec![json!({
                    "type": "complete_workflow",
                    "result": fixture_envelope(Value::Null)
                })],
            )
            .await
            .expect_err("the low-level client preserves the completion rejection");
        let Error::Http { status, body } = direct_error else {
            panic!("expected the original HTTP completion rejection");
        };
        assert_eq!(status, reqwest::StatusCode::CONFLICT);
        assert_eq!(
            serde_json::from_str::<Value>(&body).expect("response body")["reason"],
            "run_timed_out"
        );

        let mut worker = Worker::new(client, "rust-workers")
            .worker_id("timeout-worker")
            .poll_timeout(Duration::from_millis(10));
        worker.register_workflow("timeout.workflow", |_ctx, _input| async move {
            Ok(json!({"late": "result"}))
        });

        assert_eq!(
            worker
                .run_once()
                .await
                .expect("authoritative selected-run timeout settles the tick"),
            1
        );
        assert_eq!(
            server.request_count("/api/worker/workflow-tasks/workflow-timeout-task/complete"),
            2,
            "both the direct client proof and managed worker must see the rejection"
        );
    }

    #[tokio::test]
    async fn managed_worker_does_not_swallow_nearby_completion_errors() {
        for (name, status, response) in [
            ("bare conflict", "409 Conflict", r#"{"message":"conflict"}"#),
            (
                "command was recorded",
                "409 Conflict",
                r#"{"task_id":"workflow-timeout-task","workflow_task_attempt":3,"recorded":true,"run_id":"run-selected-timeout","run_status":"failed","reason":"run_timed_out"}"#,
            ),
            (
                "lease conflict",
                "409 Conflict",
                r#"{"task_id":"workflow-timeout-task","workflow_task_attempt":3,"recorded":false,"run_id":"run-selected-timeout","run_status":"failed","reason":"lease_expired"}"#,
            ),
            (
                "nonterminal run",
                "409 Conflict",
                r#"{"task_id":"workflow-timeout-task","workflow_task_attempt":3,"recorded":false,"run_id":"run-selected-timeout","run_status":"waiting","reason":"run_timed_out"}"#,
            ),
            (
                "different selected run",
                "409 Conflict",
                r#"{"task_id":"workflow-timeout-task","workflow_task_attempt":3,"recorded":false,"run_id":"run-reused-workflow-current","run_status":"failed","reason":"run_timed_out"}"#,
            ),
            (
                "different task attempt",
                "409 Conflict",
                r#"{"task_id":"workflow-timeout-task","workflow_task_attempt":4,"recorded":false,"run_id":"run-selected-timeout","run_status":"failed","reason":"run_timed_out"}"#,
            ),
            (
                "authentication failure",
                "401 Unauthorized",
                r#"{"task_id":"workflow-timeout-task","workflow_task_attempt":3,"recorded":false,"run_id":"run-selected-timeout","run_status":"failed","reason":"run_timed_out"}"#,
            ),
            (
                "authorization failure",
                "403 Forbidden",
                r#"{"task_id":"workflow-timeout-task","workflow_task_attempt":3,"recorded":false,"run_id":"run-selected-timeout","run_status":"failed","reason":"run_timed_out"}"#,
            ),
            (
                "protocol failure",
                "400 Bad Request",
                r#"{"reason":"unsupported_protocol_version","message":"unsupported worker protocol","supported_version":"1.2","requested_version":"1.3"}"#,
            ),
            (
                "malformed command",
                "422 Unprocessable Entity",
                r#"{"task_id":"workflow-timeout-task","workflow_task_attempt":3,"recorded":false,"run_id":"run-selected-timeout","run_status":"failed","reason":"run_timed_out"}"#,
            ),
            (
                "transient server failure",
                "503 Service Unavailable",
                r#"{"task_id":"workflow-timeout-task","workflow_task_attempt":3,"recorded":false,"run_id":"run-selected-timeout","run_status":"failed","reason":"run_timed_out"}"#,
            ),
        ] {
            let server = MockWorkerServer::workflow_completion(status, response);
            let client = Client::builder(server.base_url())
                .timeout(Duration::from_secs(2))
                .build()
                .expect("client");
            let mut worker = Worker::new(client, "rust-workers")
                .worker_id("timeout-worker")
                .poll_timeout(Duration::from_millis(10));
            worker.register_workflow("timeout.workflow", |_ctx, _input| async move {
                Ok(json!({"late": "result"}))
            });

            let error = worker
                .run_once()
                .await
                .expect_err(&format!("{name} must remain an error"));
            assert!(
                matches!(error, Error::Http { .. } | Error::Protocol(_)),
                "{name} returned an unexpected error variant: {error}"
            );
        }
    }

    #[tokio::test]
    async fn worker_deregistration_uses_worker_plane_method_path_headers_and_result() {
        let server = MockWorkerServer::start();
        let client = Client::builder(server.base_url())
            .worker_token(Some("worker-secret".to_string()))
            .namespace("orders")
            .timeout(Duration::from_secs(2))
            .build()
            .expect("client");
        let path = "/api/worker/registrations/worker%2F%CE%B1%20space";

        let result = client
            .deregister_worker_registration("worker/α space")
            .await
            .expect("deregister worker registration");

        assert_eq!(server.method_for(path).as_deref(), Some("DELETE"));
        assert_eq!(
            server.worker_protocol_for(path).as_deref(),
            Some(WORKER_PROTOCOL_VERSION)
        );
        assert_eq!(server.control_protocol_for(path), None);
        assert_eq!(server.namespace_for(path).as_deref(), Some("orders"));
        assert_eq!(
            server.authorization_for(path).as_deref(),
            Some("Bearer worker-secret")
        );
        assert_eq!(
            result,
            WorkerDeregistrationEnvelope {
                worker_id: "deregistered-worker".to_string(),
                outcome: "deregistered".to_string(),
                recovered_workflow_task_count: 2,
            }
        );
    }

    #[tokio::test]
    async fn low_level_registration_rejects_update_validators_before_transport() {
        let server = MockWorkerServer::start();
        let client = Client::builder(server.base_url())
            .timeout(Duration::from_secs(2))
            .build()
            .expect("client");

        for update_validators in [json!(["approve"]), json!("approve")] {
            let error = client
                .register_worker_with_command_contracts(
                    "validator-claiming-worker",
                    "rust-workers",
                    vec!["orders".to_string()],
                    vec![],
                    1,
                    1,
                    vec![WORKFLOW_UPDATES_CAPABILITY.to_string()],
                    json!({
                        "orders": {
                            "queries": ["current"],
                            "updates": ["approve"],
                            "update_validators": update_validators,
                        },
                    }),
                )
                .await
                .expect_err("unsupported validator claims must fail before registration");

            let Error::UnsupportedUpdateValidators { workflow_type } = error else {
                panic!("expected typed unsupported-validator failure");
            };
            assert_eq!(workflow_type, "orders");
        }
        assert_eq!(server.request_count("/api/worker/register"), 0);
    }

    #[tokio::test]
    async fn low_level_registration_preserves_query_and_update_contracts() {
        let server = MockWorkerServer::start();
        let client = Client::builder(server.base_url())
            .timeout(Duration::from_secs(2))
            .build()
            .expect("client");
        let contracts = json!({
            "orders": {
                "queries": ["current"],
                "updates": ["approve"],
                "update_validators": [],
            },
            "payments": {
                "queries": ["status"],
                "updates": ["capture"],
            },
        });

        client
            .register_worker_with_command_contracts(
                "command-worker",
                "rust-workers",
                vec!["orders".to_string(), "payments".to_string()],
                vec![],
                1,
                1,
                vec![WORKFLOW_UPDATES_CAPABILITY.to_string()],
                contracts.clone(),
            )
            .await
            .expect("query and update contracts must remain supported");

        assert_eq!(
            server.request_body("/api/worker/register")["workflow_command_contracts"],
            contracts
        );
    }

    #[tokio::test]
    async fn role_scoped_tokens_are_never_used_for_the_opposite_plane() {
        let server = MockWorkerServer::start();
        let control_only = Client::builder(server.base_url())
            .control_token(Some("control-secret".to_string()))
            .build()
            .expect("control client");

        let error = control_only
            .register_worker("worker", "queue", vec![], vec![], 1, 1)
            .await
            .expect_err("control token must not authorize a worker request");
        assert!(matches!(
            error,
            Error::MissingRoleCredentials { role: "worker", .. }
        ));
        assert_eq!(server.request_count("/api/worker/register"), 0);

        let worker_only = Client::builder(server.base_url())
            .worker_token(Some("worker-secret".to_string()))
            .build()
            .expect("worker client");
        let error = worker_only
            .health()
            .await
            .expect_err("worker token must not authorize a control request");
        assert!(matches!(
            error,
            Error::MissingRoleCredentials {
                role: "control",
                ..
            }
        ));
        assert_eq!(server.request_count("/api/health"), 0);
    }

    #[tokio::test]
    async fn shared_token_supports_worker_and_control_planes() {
        let server = MockWorkerServer::start();
        let client = Client::builder(server.base_url())
            .token(Some("shared-secret".to_string()))
            .build()
            .expect("client");

        client.health().await.expect("control request");
        client
            .register_worker("worker", "queue", vec![], vec![], 1, 1)
            .await
            .expect("worker request");

        assert_eq!(
            server.authorization_for("/api/health").as_deref(),
            Some("Bearer shared-secret")
        );
        assert_eq!(
            server.control_protocol_for("/api/health").as_deref(),
            Some(CONTROL_PLANE_VERSION)
        );
        assert_eq!(
            server.authorization_for("/api/worker/register").as_deref(),
            Some("Bearer shared-secret")
        );
        assert_eq!(
            server
                .worker_protocol_for("/api/worker/register")
                .as_deref(),
            Some(WORKER_PROTOCOL_VERSION)
        );
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
        assert!(
            server.request_body("/api/worker/workflow-tasks/poll")["poll_request_id"]
                .as_str()
                .is_some_and(|id| id.starts_with("rust-workflow-poll-"))
        );
        assert!(
            server.request_body("/api/worker/activity-tasks/poll")["poll_request_id"]
                .as_str()
                .is_some_and(|id| id.starts_with("rust-activity-poll-"))
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
            .complete_query_task(
                "query-capture",
                "capture-worker",
                1,
                json!(8),
                DEFAULT_CODEC,
            )
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
        assert!(
            server.request_body("/api/worker/query-tasks/poll")["poll_request_id"]
                .as_str()
                .is_some_and(|id| id.starts_with("rust-query-poll-"))
        );
    }

    #[tokio::test]
    async fn disconnected_client_polls_retry_once_with_the_same_request_id() {
        let server = MockWorkerServer::transient_worker_failures();
        let client = Client::builder(server.base_url())
            .timeout(Duration::from_secs(2))
            .build()
            .expect("client");

        client
            .poll_workflow_task("capture-worker", "capture", Duration::from_millis(10))
            .await
            .expect("workflow poll retry");
        client
            .poll_activity_task("capture-worker", "capture", Duration::from_millis(10))
            .await
            .expect("activity poll retry");
        client
            .poll_query_task("capture-worker", "capture", Duration::from_millis(10))
            .await
            .expect("query poll retry");

        for path in [
            "/api/worker/workflow-tasks/poll",
            "/api/worker/activity-tasks/poll",
            "/api/worker/query-tasks/poll",
        ] {
            let bodies = server.request_bodies(path);
            assert_eq!(bodies.len(), 2, "{path} must be retried once");
            assert_eq!(
                bodies[0]["poll_request_id"], bodies[1]["poll_request_id"],
                "{path} must preserve the request binding across retry"
            );
        }
    }

    #[tokio::test]
    async fn worker_poll_retries_preserve_request_id_across_consecutive_failures() {
        let server = MockWorkerServer::consecutive_poll_failures(2);
        let client = Client::builder(server.base_url())
            .timeout(Duration::from_secs(2))
            .build()
            .expect("client");
        let mut worker = Worker::new(client, "capture")
            .worker_id("capture-worker")
            .poll_timeout(Duration::from_millis(10))
            .retry_policy(WorkerRetryPolicy {
                max_retries: 2,
                initial_backoff: Duration::from_millis(1),
                max_backoff: Duration::from_millis(1),
            });
        worker.register_workflow(
            "capture.workflow",
            |_ctx, _input| async move { Ok(Value::Null) },
        );
        worker.register_activity(
            "capture.activity",
            |_ctx, _input| async move { Ok(Value::Null) },
        );
        worker.register_query("capture.workflow", "current", |_ctx, _args| async move {
            Ok(Value::Null)
        });

        assert_eq!(worker.run_once().await.expect("poll retries"), 0);

        for path in [
            "/api/worker/workflow-tasks/poll",
            "/api/worker/activity-tasks/poll",
            "/api/worker/query-tasks/poll",
        ] {
            let bodies = server.request_bodies(path);
            assert_eq!(bodies.len(), 3, "{path} must use exactly two retries");
            assert!(
                bodies
                    .iter()
                    .all(|body| body["poll_request_id"] == bodies[0]["poll_request_id"]),
                "{path} must preserve one request binding across every retry"
            );
        }
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
            .complete_query_task("query-late", "late-worker", 1, json!(8), DEFAULT_CODEC)
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
    async fn normal_shutdown_joins_pollers_and_deregisters_once() {
        let server = MockWorkerServer::start();
        let client = Client::builder(server.base_url())
            .timeout(Duration::from_secs(2))
            .build()
            .expect("client");
        let mut worker = Worker::new(client, "rust-workers")
            .worker_id("joined-worker")
            .poll_timeout(Duration::from_millis(10));
        worker.register_workflow(
            "joined.workflow",
            |_ctx, _input| async move { Ok(Value::Null) },
        );
        worker.register_activity(
            "joined.activity",
            |_ctx, _input| async move { Ok(Value::Null) },
        );
        worker.register_query("joined.workflow", "state", |_ctx, _input| async move {
            Ok(Value::Null)
        });

        worker
            .run_until(tokio::time::sleep(Duration::from_millis(20)))
            .await
            .expect("normal shutdown");

        let deregistration_path = "/api/worker/registrations/mock-worker";
        assert_eq!(server.request_count(deregistration_path), 1);
        for poll_path in [
            "/api/worker/workflow-tasks/poll",
            "/api/worker/activity-tasks/poll",
            "/api/worker/query-tasks/poll",
        ] {
            assert!(server.request_count(poll_path) > 0, "missing {poll_path}");
        }
        assert_eq!(
            server.captured_paths().last().map(String::as_str),
            Some(deregistration_path),
            "deregistration must start only after every poller has joined"
        );
    }

    #[tokio::test]
    async fn registration_failure_does_not_deregister() {
        let server = MockWorkerServer::rejected_registration();
        let client = Client::builder(server.base_url())
            .timeout(Duration::from_secs(2))
            .build()
            .expect("client");
        let worker = Worker::new(client, "rust-workers").worker_id("never-registered");

        let error = worker
            .run_until(async {})
            .await
            .expect_err("registration must fail");
        assert!(matches!(
            error,
            Error::Http {
                status: reqwest::StatusCode::SERVICE_UNAVAILABLE,
                ..
            }
        ));
        assert!(server
            .captured_paths()
            .iter()
            .all(|path| !path.starts_with("/api/worker/registrations/")));
    }

    #[tokio::test]
    async fn protocol_116_server_rejects_occurrence_identity_worker_registration() {
        let server = MockWorkerServer::rejected_registration_protocol();
        let client = Client::builder(server.base_url())
            .timeout(Duration::from_secs(2))
            .build()
            .expect("client");
        let worker = Worker::new(client, "rust-workers").worker_id("protocol-117-worker");

        let error = worker
            .run_until(async {})
            .await
            .expect_err("a protocol 1.16 server must reject this worker");
        let Error::Protocol(failure) = error else {
            panic!("expected typed protocol rejection");
        };
        assert_eq!(failure.reason, "unsupported_protocol_version");
        assert_eq!(failure.supported_version.as_deref(), Some("1.16"));
        assert_eq!(failure.requested_version.as_deref(), Some("1.17"));
        assert_eq!(
            server
                .worker_protocol_for("/api/worker/register")
                .as_deref(),
            Some(WORKER_PROTOCOL_VERSION)
        );
    }

    #[tokio::test]
    async fn declined_registration_does_not_deregister() {
        let server = MockWorkerServer::declined_registration();
        let client = Client::builder(server.base_url())
            .timeout(Duration::from_secs(2))
            .build()
            .expect("client");
        let worker = Worker::new(client, "rust-workers").worker_id("declined-worker");

        let error = worker
            .run_until(async {})
            .await
            .expect_err("declined registration must fail");
        assert!(matches!(error, Error::WorkerLoop(_)));
        assert!(error.to_string().contains("was not accepted"));
        assert!(server
            .captured_paths()
            .iter()
            .all(|path| !path.starts_with("/api/worker/registrations/")));
    }

    #[tokio::test]
    async fn deregistration_http_failure_is_returned_after_normal_shutdown() {
        let server = MockWorkerServer::rejected_deregistration();
        let client = Client::builder(server.base_url())
            .timeout(Duration::from_secs(2))
            .build()
            .expect("client");
        let worker = Worker::new(client, "rust-workers").worker_id("forbidden-cleanup");

        let error = worker
            .run_until(async {})
            .await
            .expect_err("deregistration must fail");
        assert!(matches!(
            error,
            Error::Http {
                status: reqwest::StatusCode::FORBIDDEN,
                ..
            }
        ));
        assert_eq!(
            server.request_count("/api/worker/registrations/mock-worker"),
            1
        );
    }

    #[tokio::test]
    async fn deregistration_protocol_failure_is_returned_after_normal_shutdown() {
        let server = MockWorkerServer::rejected_deregistration_protocol();
        let client = Client::builder(server.base_url())
            .timeout(Duration::from_secs(2))
            .build()
            .expect("client");
        let worker = Worker::new(client, "rust-workers").worker_id("protocol-cleanup");

        let error = worker
            .run_until(async {})
            .await
            .expect_err("protocol rejection must fail shutdown");
        let Error::Protocol(failure) = error else {
            panic!("expected typed protocol failure");
        };
        assert_eq!(failure.reason, "unsupported_protocol_version");
        assert_eq!(
            failure.requested_version.as_deref(),
            Some(WORKER_PROTOCOL_VERSION)
        );
        assert_eq!(
            server.request_count("/api/worker/registrations/mock-worker"),
            1
        );
    }

    #[tokio::test]
    async fn primary_poller_error_retains_deregistration_failure_context() {
        let server = MockWorkerServer::unauthorized_polls_and_rejected_deregistration();
        let client = Client::builder(server.base_url())
            .timeout(Duration::from_secs(2))
            .build()
            .expect("client");
        let mut worker = Worker::new(client, "rust-workers")
            .worker_id("combined-failure")
            .poll_timeout(Duration::from_millis(10));
        worker.register_workflow("combined.workflow", |_ctx, _input| async move {
            Ok(Value::Null)
        });

        let error = worker
            .run()
            .await
            .expect_err("worker and cleanup must fail");
        let summary = error.to_string();
        assert!(summary.contains("authentication_failed"));
        assert!(summary.contains("worker cannot deregister"));
        let Error::WorkerShutdown {
            primary,
            deregistration,
        } = error
        else {
            panic!("expected combined worker shutdown error");
        };
        assert!(matches!(
            *primary,
            Error::Http {
                status: reqwest::StatusCode::UNAUTHORIZED,
                ..
            }
        ));
        assert!(matches!(
            *deregistration,
            Error::Http {
                status: reqwest::StatusCode::FORBIDDEN,
                ..
            }
        ));
        assert_eq!(
            server.request_count("/api/worker/registrations/mock-worker"),
            1
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
        let acknowledged = Arc::clone(&observations);
        worker
            .run_until(async move {
                tokio::time::timeout(Duration::from_secs(2), async move {
                    loop {
                        if !acknowledged
                            .lock()
                            .expect("heartbeat observations")
                            .is_empty()
                        {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                })
                .await
                .expect("heartbeat acknowledgement within timeout");
            })
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
    async fn delayed_worker_heartbeat_keeps_cadence_and_pollers_live() {
        let server = MockWorkerServer::delayed_heartbeat_worker();
        let client = Client::builder(server.base_url())
            .timeout(Duration::from_secs(3))
            .build()
            .expect("client");
        let observations = Arc::new(Mutex::new(Vec::new()));
        let observed = Arc::clone(&observations);
        let mut worker = Worker::new(client, "rust-snapshot-workers")
            .worker_id("rust-snapshot-worker")
            .poll_timeout(Duration::from_millis(10))
            .on_worker_heartbeat(move |observation| {
                observed
                    .lock()
                    .expect("heartbeat observations")
                    .push(observation.clone());
            });

        worker.register_workflow("snapshot", |ctx, _input| async move {
            ctx.wait_signal("finish").await?;
            Ok(json!({"status": "finished"}))
        });
        worker.register_query("snapshot", "current", |ctx, _args| async move {
            Ok(json!(ctx
                .signals("increment")
                .iter()
                .filter_map(|arguments| arguments.first().and_then(Value::as_i64))
                .sum::<i64>()))
        });
        worker.register_activity("cancel-aware", |_ctx, _args| async move {
            Ok(json!({"late": "completion"}))
        });

        worker
            .run_until(tokio::time::sleep(Duration::from_millis(3_800)))
            .await
            .expect("delayed heartbeat must allow a clean worker shutdown");

        let observations = observations.lock().expect("heartbeat observations");
        assert!(
            observations.len() >= 3,
            "the immediate heartbeat, delayed acknowledgement, and next cadence heartbeat must complete"
        );
        assert!(
            observations.windows(2).all(|pair| {
                pair[1].acknowledged_at_unix_millis
                    .saturating_sub(pair[0].acknowledged_at_unix_millis)
                    >= 850
            }),
            "successful acknowledgements must not catch up faster than the advertised one-second cadence: {observations:?}"
        );
        drop(observations);

        let heartbeat_times = server.request_times("/api/worker/heartbeat");
        let delayed_request_at = *heartbeat_times
            .get(1)
            .expect("intentionally delayed heartbeat request");
        let delay_window_start = delayed_request_at + Duration::from_millis(100);
        let delay_window_end = delayed_request_at + Duration::from_millis(1_400);
        for path in [
            "/api/worker/workflow-tasks/poll",
            "/api/worker/activity-tasks/poll",
            "/api/worker/query-tasks/poll",
        ] {
            assert!(
                server
                    .request_times(path)
                    .iter()
                    .any(|received_at| *received_at >= delay_window_start
                        && *received_at <= delay_window_end),
                "{path} must keep polling while a heartbeat acknowledgement is delayed"
            );
        }
        assert!(
            server.request_count("/api/worker/workflow-tasks/snapshot-wait-3/fail") >= 1,
            "workflow work must be settled"
        );
        assert!(
            server.request_count("/api/worker/activity-tasks/activity-cancel/complete") >= 1,
            "activity work must be settled"
        );
        assert!(
            server.request_count("/api/worker/query-tasks/snapshot-current/complete") >= 1,
            "query work must be settled"
        );
    }

    #[tokio::test]
    async fn retried_worker_heartbeat_restarts_the_advertised_cadence() {
        let server = MockWorkerServer::heartbeat_retry_worker();
        let client = Client::builder(server.base_url())
            .timeout(Duration::from_secs(2))
            .build()
            .expect("client");
        let observations = Arc::new(Mutex::new(Vec::new()));
        let observed = Arc::clone(&observations);
        let worker = Worker::new(client, "rust-workers")
            .worker_id("heartbeat-retry-worker")
            .retry_policy(WorkerRetryPolicy {
                max_retries: 1,
                initial_backoff: Duration::from_millis(300),
                max_backoff: Duration::from_millis(300),
            })
            .on_worker_heartbeat(move |observation| {
                observed
                    .lock()
                    .expect("heartbeat observations")
                    .push(observation.clone());
            });

        worker
            .run_until(tokio::time::sleep(Duration::from_millis(2_700)))
            .await
            .expect("retryable heartbeat failure must remain bounded and recover");

        let observations = observations.lock().expect("heartbeat observations");
        assert!(observations.len() >= 3, "heartbeat retry must recover");
        assert!(
            observations.windows(2).all(|pair| {
                pair[1]
                    .acknowledged_at_unix_millis
                    .saturating_sub(pair[0].acknowledged_at_unix_millis)
                    >= 850
            }),
            "a successful retry must start a fresh advertised cadence: {observations:?}"
        );
        assert_eq!(
            server.request_count("/api/worker/heartbeat"),
            observations.len() + 1,
            "one retryable failure must add exactly one bounded request"
        );
    }

    #[tokio::test]
    async fn query_enabled_worker_ignores_unmatched_signals_then_completes_once() {
        let server = MockWorkerServer::waiting_query_worker();
        let client = Client::builder(server.base_url())
            .timeout(Duration::from_secs(2))
            .build()
            .expect("client");
        let observations = Arc::new(Mutex::new(Vec::new()));
        let observed = Arc::clone(&observations);
        let mut worker = Worker::new(client, "rust-snapshot-workers")
            .worker_id("rust-snapshot-worker")
            .poll_timeout(Duration::from_millis(10))
            .on_worker_heartbeat(move |observation| {
                observed
                    .lock()
                    .expect("heartbeat observations")
                    .push(observation.clone());
            });

        worker.register_workflow("snapshot", |ctx, _input| async move {
            ctx.wait_signal("finish").await?;
            Ok(json!({"status": "finished"}))
        });
        worker.register_query("snapshot", "current", |ctx, _args| async move {
            let current = ctx
                .signals("increment")
                .iter()
                .filter_map(|arguments| arguments.first().and_then(Value::as_i64))
                .sum::<i64>();
            Ok(json!(current))
        });
        worker.register_update("snapshot", "replace", |_ctx, args| async move { Ok(args) });

        worker
            .run_until(tokio::time::sleep(Duration::from_millis(3_200)))
            .await
            .expect("pending workflow and query poller must remain live until shutdown");

        assert!(
            observations.lock().expect("heartbeat observations").len() >= 4,
            "the immediate heartbeat and at least three advertised one-second intervals must be acknowledged"
        );
        assert!(
            server.request_count("/api/worker/workflow-tasks/poll") >= 3,
            "workflow polling must continue after empty replay acknowledgements"
        );
        assert!(
            server.request_count("/api/worker/query-tasks/poll") >= 2,
            "query polling must continue after serving the current query"
        );
        assert_eq!(
            server.request_body("/api/worker/register")["capabilities"],
            json!([
                CONDITION_WAIT_OCCURRENCE_IDENTITY_CAPABILITY,
                DURABLE_SELECTION_CAPABILITY,
                MEMO_UPSERTS_CAPABILITY,
                TYPED_SEARCH_ATTRIBUTES_CAPABILITY,
                QUERY_TASKS_CAPABILITY,
                WORKFLOW_UPDATES_CAPABILITY,
                MESSAGE_STREAMS_CAPABILITY
            ])
        );
        assert_eq!(
            server.request_body("/api/worker/register")["workflow_command_contracts"]["snapshot"],
            json!({
                "queries": ["current"],
                "query_contracts": [],
                "signals": [],
                "signal_contracts": [],
                "updates": ["replace"],
                "update_contracts": [],
                "update_validators": [],
            })
        );

        let opened = server.request_body("/api/worker/workflow-tasks/snapshot-open/complete");
        assert_eq!(
            opened["commands"],
            json!([{
                "type": "open_signal_wait",
                "signal_name": "finish",
            }])
        );

        for task_id in ["snapshot-wait-3", "snapshot-wait-5"] {
            let fail_path = format!("/api/worker/workflow-tasks/{task_id}/fail");
            let completion_path = format!("/api/worker/workflow-tasks/{task_id}/complete");
            let failure = server.request_body(&fail_path);
            assert_eq!(
                failure["failure"]["type"],
                WORKFLOW_TASK_WAITING_FOR_HISTORY_TYPE
            );
            assert_eq!(server.request_count(&completion_path), 0);
        }

        let query_completion =
            server.request_body("/api/worker/query-tasks/snapshot-current/complete");
        assert_eq!(query_completion["result"], json!(8));

        let terminal_path = "/api/worker/workflow-tasks/snapshot-finish/complete";
        assert_eq!(
            server.request_count(terminal_path),
            1,
            "the matching signal must settle the workflow exactly once"
        );
        let terminal = server.request_body(terminal_path);
        assert_eq!(terminal["commands"].as_array().map(Vec::len), Some(1));
        assert_eq!(terminal["commands"][0]["type"], "complete_workflow");
        assert_eq!(
            decode_wire_value(&terminal["commands"][0]["result"], DEFAULT_CODEC)
                .expect("terminal workflow result"),
            json!({"status": "finished"})
        );
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
    async fn worker_continues_after_long_poll_capacity_backpressure() {
        let server = MockWorkerServer::capacity_limited_activity_poll();
        let client = Client::builder(server.base_url())
            .timeout(Duration::from_secs(2))
            .build()
            .expect("client");
        let mut worker = Worker::new(client, "rust-workers")
            .worker_id("capacity-worker")
            .poll_timeout(Duration::from_millis(10))
            .retry_policy(WorkerRetryPolicy {
                max_retries: 0,
                initial_backoff: Duration::from_millis(1),
                max_backoff: Duration::from_millis(1),
            });
        worker.register_activity("capacity.activity", |_ctx, _input| async move {
            Ok(json!({"handled": true}))
        });

        worker
            .run_until(tokio::time::sleep(Duration::from_millis(50)))
            .await
            .expect("capacity backpressure must not stop the worker");

        assert!(
            server.request_count("/api/worker/activity-tasks/poll") >= 2,
            "the activity poller must continue after capacity backpressure"
        );
        assert_eq!(
            server.request_count("/api/worker/activity-tasks/capacity-activity/complete"),
            1,
            "the worker must complete work returned after capacity recovers"
        );
    }

    #[test]
    fn worker_poll_capacity_backpressure_requires_the_typed_retryable_contract() {
        let capacity = Error::Http {
            status: reqwest::StatusCode::TOO_MANY_REQUESTS,
            body: r#"{"poll_status":"long_poll_capacity_exhausted","retryable":true,"retry_after_seconds":3}"#.to_string(),
        };
        assert_eq!(
            worker_poll_capacity_retry_after(&capacity),
            Some(Duration::from_secs(3))
        );

        let rejected_capacity = Error::Http {
            status: reqwest::StatusCode::TOO_MANY_REQUESTS,
            body: r#"{"reason":"long_poll_capacity_exhausted","retryable":false,"retry_after_seconds":3}"#.to_string(),
        };
        assert_eq!(worker_poll_capacity_retry_after(&rejected_capacity), None);
        assert!(!worker_operation_is_retryable(&rejected_capacity));

        let ordinary_rate_limit = Error::Http {
            status: reqwest::StatusCode::TOO_MANY_REQUESTS,
            body: r#"{"reason":"rate_limited","retryable":true,"retry_after_seconds":3}"#
                .to_string(),
        };
        assert_eq!(worker_poll_capacity_retry_after(&ordinary_rate_limit), None);
        assert!(worker_operation_is_retryable(&ordinary_rate_limit));
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
            "one initial request plus exactly two retries"
        );
    }

    #[tokio::test]
    async fn worker_retry_policy_can_disable_poll_retries() {
        let server = MockWorkerServer::unavailable_polls();
        let client = Client::builder(server.base_url())
            .timeout(Duration::from_secs(2))
            .build()
            .expect("client");
        let mut worker = Worker::new(client, "rust-workers")
            .worker_id("no-retry-worker")
            .poll_timeout(Duration::from_millis(10))
            .retry_policy(WorkerRetryPolicy {
                max_retries: 0,
                initial_backoff: Duration::from_millis(1),
                max_backoff: Duration::from_millis(1),
            });
        worker.register_workflow("counter", |_ctx, _input| async move { Ok(Value::Null) });

        let error = worker
            .run_once()
            .await
            .expect_err("disabled retries must return the first transport failure");
        assert!(matches!(error, Error::Transport(_)));
        assert_eq!(
            server.request_count("/api/worker/workflow-tasks/poll"),
            1,
            "max_retries=0 must send only the initial request"
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
        method: String,
        path: String,
        authorization: Option<String>,
        namespace: Option<String>,
        worker_protocol: Option<String>,
        control_protocol: Option<String>,
        body: String,
        received_at: Instant,
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
        waiting_query_worker: bool,
        decline_registration: bool,
        complete_named_signal: bool,
        poll_failures_per_path: usize,
        long_poll_capacity_responses_per_path: usize,
        heartbeat_failures: usize,
        heartbeat_failure_request: Option<usize>,
        delayed_heartbeat_request: Option<usize>,
        heartbeat_response_delay: Duration,
        concurrent_requests: bool,
        unauthorized_polls: bool,
        reject_registration: bool,
        reject_registration_protocol: bool,
        reject_deregistration: bool,
        reject_deregistration_protocol: bool,
        cancelled_activity: bool,
        draining_polls: bool,
        invalid_task_payload_codec: Option<InvalidTaskPayloadCodec>,
        workflow_completion_status: Option<&'static str>,
        workflow_completion_body: Option<&'static str>,
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

        fn waiting_query_worker() -> Self {
            Self::start_with_behavior(MockWorkerBehavior {
                waiting_query_worker: true,
                complete_named_signal: true,
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

        fn consecutive_poll_failures(count: usize) -> Self {
            Self::start_with_behavior(MockWorkerBehavior {
                poll_failures_per_path: count,
                ..MockWorkerBehavior::default()
            })
        }

        fn capacity_limited_activity_poll() -> Self {
            Self::start_with_behavior(MockWorkerBehavior {
                long_poll_capacity_responses_per_path: 1,
                ..MockWorkerBehavior::default()
            })
        }

        fn delayed_heartbeat_worker() -> Self {
            Self::start_with_behavior(MockWorkerBehavior {
                waiting_query_worker: true,
                delayed_heartbeat_request: Some(2),
                heartbeat_response_delay: Duration::from_millis(1_500),
                concurrent_requests: true,
                cancelled_activity: true,
                ..MockWorkerBehavior::default()
            })
        }

        fn heartbeat_retry_worker() -> Self {
            Self::start_with_behavior(MockWorkerBehavior {
                waiting_query_worker: true,
                heartbeat_failure_request: Some(2),
                concurrent_requests: true,
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

        fn rejected_registration() -> Self {
            Self::start_with_behavior(MockWorkerBehavior {
                reject_registration: true,
                ..MockWorkerBehavior::default()
            })
        }

        fn rejected_registration_protocol() -> Self {
            Self::start_with_behavior(MockWorkerBehavior {
                reject_registration_protocol: true,
                ..MockWorkerBehavior::default()
            })
        }

        fn declined_registration() -> Self {
            Self::start_with_behavior(MockWorkerBehavior {
                decline_registration: true,
                ..MockWorkerBehavior::default()
            })
        }

        fn rejected_deregistration() -> Self {
            Self::start_with_behavior(MockWorkerBehavior {
                reject_deregistration: true,
                ..MockWorkerBehavior::default()
            })
        }

        fn rejected_deregistration_protocol() -> Self {
            Self::start_with_behavior(MockWorkerBehavior {
                reject_deregistration_protocol: true,
                ..MockWorkerBehavior::default()
            })
        }

        fn unauthorized_polls_and_rejected_deregistration() -> Self {
            Self::start_with_behavior(MockWorkerBehavior {
                unauthorized_polls: true,
                reject_deregistration: true,
                ..MockWorkerBehavior::default()
            })
        }

        fn cancelled_activity() -> Self {
            Self::start_with_behavior(MockWorkerBehavior {
                cancelled_activity: true,
                ..MockWorkerBehavior::default()
            })
        }

        fn draining_polls() -> Self {
            Self::start_with_behavior(MockWorkerBehavior {
                draining_polls: true,
                ..MockWorkerBehavior::default()
            })
        }

        fn invalid_task_payload_codec(codec: InvalidTaskPayloadCodec) -> Self {
            Self::start_with_behavior(MockWorkerBehavior {
                invalid_task_payload_codec: Some(codec),
                ..MockWorkerBehavior::default()
            })
        }

        fn workflow_completion(status: &'static str, body: &'static str) -> Self {
            Self::start_with_behavior(MockWorkerBehavior {
                workflow_completion_status: Some(status),
                workflow_completion_body: Some(body),
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
                let mut request_threads = Vec::new();
                while !server_stop.load(Ordering::SeqCst) {
                    match listener.accept() {
                        Ok((mut stream, _)) => {
                            if behavior.concurrent_requests {
                                let requests = Arc::clone(&server_requests);
                                request_threads.push(thread::spawn(move || {
                                    handle_mock_worker_request(&mut stream, &requests, behavior)
                                }));
                            } else {
                                handle_mock_worker_request(&mut stream, &server_requests, behavior);
                            }
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            let mut index = 0;
                            while index < request_threads.len() {
                                if request_threads[index].is_finished() {
                                    request_threads
                                        .swap_remove(index)
                                        .join()
                                        .expect("join mock request");
                                } else {
                                    index += 1;
                                }
                            }
                            thread::sleep(Duration::from_millis(5));
                        }
                        Err(_) => break,
                    }
                }
                for request_thread in request_threads {
                    request_thread.join().expect("join mock request");
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

        fn control_protocol_for(&self, path: &str) -> Option<String> {
            self.requests
                .lock()
                .expect("captured requests")
                .iter()
                .find(|request| request.path == path)
                .and_then(|request| request.control_protocol.clone())
        }

        fn method_for(&self, path: &str) -> Option<String> {
            self.requests
                .lock()
                .expect("captured requests")
                .iter()
                .find(|request| request.path == path)
                .map(|request| request.method.clone())
        }

        fn authorization_for(&self, path: &str) -> Option<String> {
            self.requests
                .lock()
                .expect("captured requests")
                .iter()
                .find(|request| request.path == path)
                .and_then(|request| request.authorization.clone())
        }

        fn namespace_for(&self, path: &str) -> Option<String> {
            self.requests
                .lock()
                .expect("captured requests")
                .iter()
                .find(|request| request.path == path)
                .and_then(|request| request.namespace.clone())
        }

        fn request_count(&self, path: &str) -> usize {
            self.requests
                .lock()
                .expect("captured requests")
                .iter()
                .filter(|request| request.path == path)
                .count()
        }

        fn captured_paths(&self) -> Vec<String> {
            self.requests
                .lock()
                .expect("captured requests")
                .iter()
                .map(|request| request.path.clone())
                .collect()
        }

        fn request_times(&self, path: &str) -> Vec<Instant> {
            self.requests
                .lock()
                .expect("captured requests")
                .iter()
                .filter(|request| request.path == path)
                .map(|request| request.received_at)
                .collect()
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

        fn request_bodies(&self, path: &str) -> Vec<Value> {
            self.requests
                .lock()
                .expect("captured requests")
                .iter()
                .filter(|request| request.path == path)
                .map(|request| {
                    serde_json::from_str(&request.body).unwrap_or_else(|error| {
                        panic!(
                            "invalid JSON request body for {path}: {error}: {:?}",
                            request.body
                        )
                    })
                })
                .collect()
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
        let method = request
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().next())
            .unwrap_or_default();
        let authorization = request.lines().find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("Authorization")
                .then(|| value.trim().to_string())
        });
        let namespace = request.lines().find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("X-Namespace")
                .then(|| value.trim().to_string())
        });
        let worker_protocol = request.lines().find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("X-Durable-Workflow-Protocol-Version")
                .then(|| value.trim().to_string())
        });
        let control_protocol = request.lines().find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("X-Durable-Workflow-Control-Plane-Version")
                .then(|| value.trim().to_string())
        });
        let request_number = {
            let mut requests = requests.lock().expect("captured requests");
            requests.push(CapturedRequest {
                method: method.to_string(),
                path: path.to_string(),
                authorization,
                namespace,
                worker_protocol: worker_protocol.clone(),
                control_protocol,
                body: body.to_string(),
                received_at: Instant::now(),
            });
            requests
                .iter()
                .filter(|request| request.path == path)
                .count()
        };

        if path == "/api/worker/register" {
            if behavior.reject_registration_protocol {
                write_mock_response(
                    stream,
                    "400 Bad Request",
                    r#"{"reason":"unsupported_protocol_version","message":"condition-wait occurrence identity requires worker protocol 1.17","supported_version":"1.16","requested_version":"1.17"}"#,
                );
                return;
            }
            if behavior.reject_registration {
                write_mock_response(
                    stream,
                    "503 Service Unavailable",
                    r#"{"reason":"registration_unavailable","message":"registration failed"}"#,
                );
                return;
            }
        }

        if path.starts_with("/api/worker/registrations/") {
            if behavior.reject_deregistration_protocol {
                write_mock_response(
                    stream,
                    "400 Bad Request",
                    r#"{"reason":"unsupported_protocol_version","message":"unsupported worker protocol","supported_version":"1.17","requested_version":"1.19"}"#,
                );
            } else if behavior.reject_deregistration {
                write_mock_response(
                    stream,
                    "403 Forbidden",
                    r#"{"reason":"authorization_failed","message":"worker cannot deregister"}"#,
                );
            } else {
                write_mock_response(
                    stream,
                    "200 OK",
                    r#"{"worker_id":"deregistered-worker","outcome":"deregistered","recovered_workflow_task_count":2}"#,
                );
            }
            return;
        }

        let is_poll = matches!(
            path,
            "/api/worker/workflow-tasks/poll"
                | "/api/worker/activity-tasks/poll"
                | "/api/worker/query-tasks/poll"
        );
        if is_poll && request_number <= behavior.long_poll_capacity_responses_per_path {
            write_mock_response(
                stream,
                "429 Too Many Requests",
                r#"{"task":null,"poll_status":"long_poll_capacity_exhausted","reason":"long_poll_capacity_exhausted","retryable":true,"retry_after_seconds":1}"#,
            );
            return;
        }
        if is_poll && request_number <= behavior.poll_failures_per_path {
            return;
        }
        if path == "/api/worker/heartbeat" && request_number <= behavior.heartbeat_failures {
            return;
        }
        if path == "/api/worker/heartbeat"
            && behavior.heartbeat_failure_request == Some(request_number)
        {
            return;
        }
        if path == "/api/worker/heartbeat"
            && behavior.delayed_heartbeat_request == Some(request_number)
        {
            thread::sleep(behavior.heartbeat_response_delay);
        }
        if behavior.unauthorized_polls && is_poll {
            write_mock_response(
                stream,
                "401 Unauthorized",
                r#"{"reason":"authentication_failed","message":"invalid worker token"}"#,
            );
            return;
        }
        if behavior.draining_polls && is_poll {
            write_mock_response(
                stream,
                "409 Conflict",
                r#"{"task":null,"poll_status":"draining","reason":"worker_draining","worker_status":"draining","drain_intent":"draining"}"#,
            );
            return;
        }

        if let Some(codec_case) = behavior.invalid_task_payload_codec {
            if is_poll && request_number == 1 {
                let mut task = match path {
                    "/api/worker/workflow-tasks/poll" => json!({
                        "task_id": "codec-workflow",
                        "workflow_type": "codec.workflow",
                        "payload_codec": DEFAULT_CODEC,
                        "workflow_task_attempt": 1,
                        "lease_owner": "codec-worker"
                    }),
                    "/api/worker/activity-tasks/poll" => json!({
                        "task_id": "codec-activity",
                        "activity_attempt_id": "codec-activity-attempt",
                        "activity_type": "codec.activity",
                        "payload_codec": DEFAULT_CODEC,
                        "attempt_number": 1,
                        "lease_owner": "codec-worker"
                    }),
                    "/api/worker/query-tasks/poll" => json!({
                        "query_task_id": "codec-query",
                        "query_task_attempt": 1,
                        "workflow_type": "codec.workflow",
                        "query_name": "known",
                        "payload_codec": DEFAULT_CODEC,
                        "lease_owner": "codec-worker"
                    }),
                    _ => unreachable!("is_poll limits task codec probe paths"),
                };
                codec_case.apply(&mut task);
                write_mock_response(stream, "200 OK", &json!({"task": task}).to_string());
                return;
            }

            if matches!(
                path,
                "/api/worker/workflow-tasks/codec-workflow/fail"
                    | "/api/worker/activity-tasks/codec-activity/fail"
                    | "/api/worker/query-tasks/codec-query/fail"
            ) {
                write_mock_response(stream, "200 OK", r#"{"outcome":"failed"}"#);
                return;
            }
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

        if behavior.workflow_completion_status.is_some()
            && path == "/api/worker/workflow-tasks/poll"
            && request_number == 1
        {
            write_mock_response(
                stream,
                "200 OK",
                r#"{"task":{"task_id":"workflow-timeout-task","workflow_id":"reused-workflow-id","run_id":"run-selected-timeout","workflow_type":"timeout.workflow","payload_codec":"avro","arguments":{"codec":"avro","blob":"wwHioz3/VYAiNwwA"},"history_events":[],"workflow_task_attempt":3,"lease_owner":"timeout-worker"}}"#,
            );
            return;
        }

        if path == "/api/worker/workflow-tasks/workflow-timeout-task/complete" {
            if let (Some(status), Some(body)) = (
                behavior.workflow_completion_status,
                behavior.workflow_completion_body,
            ) {
                write_mock_response(stream, status, body);
                return;
            }
        }

        if behavior.waiting_query_worker {
            if behavior.complete_named_signal
                && path == "/api/worker/workflow-tasks/poll"
                && request_number == 1
            {
                let body = json!({
                    "task": {
                        "task_id": "snapshot-open",
                        "workflow_id": "snapshot-1",
                        "run_id": "snapshot-run-1",
                        "workflow_type": "snapshot",
                        "payload_codec": DEFAULT_CODEC,
                        "arguments": encode_value_envelope(&json!([]), DEFAULT_CODEC)
                            .expect("Avro workflow arguments"),
                        "history_events": [],
                        "workflow_task_attempt": 1,
                        "lease_owner": "rust-snapshot-worker"
                    }
                })
                .to_string();
                write_mock_response(stream, "200 OK", &body);
                return;
            }

            let signal_request = request_number - usize::from(behavior.complete_named_signal);
            let signal_request_limit = 2 + usize::from(behavior.complete_named_signal);
            if path == "/api/worker/workflow-tasks/poll"
                && signal_request >= 1
                && signal_request <= signal_request_limit
            {
                let finish = behavior.complete_named_signal && signal_request == 3;
                let amounts = if signal_request == 1 {
                    vec![3]
                } else {
                    vec![3, 5]
                };
                let task_id = if signal_request == 1 {
                    "snapshot-wait-3"
                } else if finish {
                    "snapshot-finish"
                } else {
                    "snapshot-wait-5"
                };
                let mut history_events = std::iter::once(json!({
                    "event_type": "SignalWaitOpened",
                    "payload": {"sequence": 1, "signal_name": "finish"}
                }))
                .chain(amounts.iter().enumerate().map(|(index, amount)| {
                    json!({
                        "event_type": "SignalReceived",
                        "payload": {
                            "signal_id": format!("increment-{amount}"),
                            "signal_name": "increment",
                            "workflow_sequence": index + 2,
                            "payload_codec": DEFAULT_CODEC,
                            "arguments": encode_value_envelope(&json!([amount]), DEFAULT_CODEC)
                                .expect("Avro signal envelope")
                        }
                    })
                }))
                .collect::<Vec<_>>();
                let (resume_id, resume_name, resume_arguments) = if finish {
                    history_events.push(json!({
                        "event_type": "SignalReceived",
                        "payload": {
                            "signal_id": "finish",
                            "signal_name": "finish",
                            "workflow_sequence": 4,
                            "payload_codec": DEFAULT_CODEC,
                            "arguments": encode_value_envelope(&json!([]), DEFAULT_CODEC)
                                .expect("Avro finish signal envelope")
                        }
                    }));
                    (
                        "finish".to_string(),
                        "finish".to_string(),
                        encode_value_envelope(&json!([]), DEFAULT_CODEC)
                            .expect("Avro finish resume signal"),
                    )
                } else {
                    let amount = amounts.last().expect("amount");
                    (
                        format!("increment-{amount}"),
                        "increment".to_string(),
                        encode_value_envelope(&json!([amount]), DEFAULT_CODEC)
                            .expect("Avro increment resume signal"),
                    )
                };
                let body = json!({
                    "task": {
                        "task_id": task_id,
                        "workflow_id": "snapshot-1",
                        "run_id": "snapshot-run-1",
                        "workflow_type": "snapshot",
                        "payload_codec": DEFAULT_CODEC,
                        "arguments": encode_value_envelope(&json!([]), DEFAULT_CODEC)
                            .expect("Avro workflow arguments"),
                        "history_events": history_events,
                        "workflow_task_attempt": 1,
                        "workflow_signal_id": resume_id,
                        "signal_name": resume_name,
                        "signal_arguments": resume_arguments,
                        "lease_owner": "rust-snapshot-worker"
                    }
                })
                .to_string();
                write_mock_response(stream, "200 OK", &body);
                return;
            }

            if path == "/api/worker/query-tasks/poll" && request_number == 1 {
                let history_events = [3, 5]
                    .into_iter()
                    .enumerate()
                    .map(|(index, amount)| {
                        json!({
                            "event_type": "SignalReceived",
                            "payload": {
                                "signal_id": format!("increment-{amount}"),
                                "signal_name": "increment",
                                "workflow_sequence": index + 2,
                                "payload_codec": DEFAULT_CODEC,
                                "arguments": encode_value_envelope(&json!([amount]), DEFAULT_CODEC)
                                    .expect("Avro query signal envelope")
                            }
                        })
                    })
                    .collect::<Vec<_>>();
                let body = json!({
                    "task": {
                        "query_task_id": "snapshot-current",
                        "query_task_attempt": 1,
                        "lease_owner": "rust-snapshot-worker",
                        "workflow_id": "snapshot-1",
                        "run_id": "snapshot-run-1",
                        "workflow_type": "snapshot",
                        "query_name": "current",
                        "payload_codec": DEFAULT_CODEC,
                        "workflow_arguments": encode_value_envelope(&json!([]), DEFAULT_CODEC)
                            .expect("Avro workflow arguments"),
                        "query_arguments": encode_value_envelope(&json!([]), DEFAULT_CODEC)
                            .expect("Avro query arguments"),
                        "history_events": history_events,
                        "run_status": "waiting"
                    }
                })
                .to_string();
                write_mock_response(stream, "200 OK", &body);
                return;
            }

            if path == "/api/worker/workflow-tasks/snapshot-wait-3/fail"
                || path == "/api/worker/workflow-tasks/snapshot-wait-5/fail"
            {
                write_mock_response(
                    stream,
                    "200 OK",
                    r#"{"outcome":"waiting_for_history","recorded":true}"#,
                );
                return;
            }

            if path == "/api/worker/workflow-tasks/snapshot-open/complete" {
                write_mock_response(stream, "200 OK", r#"{"outcome":"waiting","recorded":true}"#);
                return;
            }

            if path == "/api/worker/workflow-tasks/snapshot-finish/complete" {
                write_mock_response(
                    stream,
                    "200 OK",
                    r#"{"outcome":"completed","run_status":"completed","recorded":true}"#,
                );
                return;
            }

            if path == "/api/worker/query-tasks/snapshot-current/complete" {
                write_mock_response(stream, "200 OK", r#"{"outcome":"completed"}"#);
                return;
            }
        }

        if matches!(
            path,
            "/api/workflows/typed-1/query/inspect" | "/api/workflows/typed-1/update/replace"
        ) {
            let result = encode_typed_envelope(&typed_fidelity_probe(), DEFAULT_CODEC)
                .expect("typed mock result");
            let body = json!({
                "result": typed_fidelity_probe().into_json().expect("result projection"),
                "result_envelope": result,
            })
            .to_string();
            write_mock_response(stream, "200 OK", &body);
            return;
        }

        if path == "/api/workflows/typed-1" {
            let result = encode_typed_envelope(&typed_fidelity_probe(), DEFAULT_CODEC)
                .expect("typed mock result");
            let body = json!({
                "workflow_id": "typed-1",
                "run_id": "run-typed-1",
                "workflow_type": "typed.echo",
                "status": "completed",
                "output": typed_fidelity_probe().into_json().expect("output projection"),
                "output_envelope": result,
            })
            .to_string();
            write_mock_response(stream, "200 OK", &body);
            return;
        }

        let (status, body) = match path {
            "/api/health" => ("200 OK", r#"{"status":"ok"}"#),
            "/api/workflows" => (
                "201 Created",
                r#"{"workflow_id":"wf-start-options","run_id":"run-start-options","workflow_type":"rust.timeout"}"#,
            ),
            "/api/worker/register" if behavior.decline_registration => (
                "200 OK",
                r#"{"worker_id":"declined-worker","registered":false}"#,
            ),
            "/api/worker/register" if behavior.waiting_query_worker => (
                "200 OK",
                r#"{"worker_id":"rust-snapshot-worker","registered":true,"heartbeat_interval_seconds":1}"#,
            ),
            "/api/worker/register" => (
                "200 OK",
                r#"{"worker_id":"mock-worker","registered":true,"heartbeat_interval_seconds":3600}"#,
            ),
            "/api/worker/heartbeat" => ("200 OK", "{}"),
            "/api/worker/activity-tasks/poll"
                if behavior.cancelled_activity && request_number == 1 =>
            {
                (
                    "200 OK",
                    r#"{"task":{"task_id":"activity-cancel","activity_attempt_id":"attempt-cancel","activity_type":"cancel-aware","payload_codec":"avro","arguments":{"codec":"avro","blob":"wwHioz3/VYAiNwwA"},"attempt_number":1,"lease_owner":"rust-cancel-worker"}}"#,
                )
            }
            "/api/worker/activity-tasks/poll"
                if behavior.long_poll_capacity_responses_per_path > 0
                    && request_number
                        == behavior
                            .long_poll_capacity_responses_per_path
                            .saturating_add(1) =>
            {
                (
                    "200 OK",
                    r#"{"task":{"task_id":"capacity-activity","activity_attempt_id":"capacity-attempt","activity_type":"capacity.activity","payload_codec":"avro","arguments":{"codec":"avro","blob":"wwHioz3/VYAiNwwA"},"attempt_number":1,"lease_owner":"capacity-worker"}}"#,
                )
            }
            "/api/worker/activity-tasks/poll" | "/api/worker/workflow-tasks/poll" => {
                ("200 OK", r#"{"task":null}"#)
            }
            "/api/worker/query-tasks/poll"
                if behavior.reject_query_completion && request_number == 1 =>
            {
                (
                    "200 OK",
                    r#"{"task":{"query_task_id":"query-late","query_task_attempt":1,"lease_owner":"late-worker","workflow_id":"counter-late","run_id":"run-late","workflow_type":"counter","query_name":"current","payload_codec":"avro","workflow_arguments":{"codec":"avro","blob":"wwHioz3/VYAiNwwA"},"query_arguments":{"codec":"avro","blob":"wwHioz3/VYAiNwwA"},"history_events":[],"run_status":"running"}}"#,
                )
            }
            "/api/worker/query-tasks/poll" => ("200 OK", r#"{"task":null}"#),
            "/api/worker/query-tasks/query-capture/complete"
            | "/api/worker/query-tasks/query-capture/fail" => ("200 OK", "{}"),
            "/api/worker/activity-tasks/activity-cancel/heartbeat" => (
                "200 OK",
                r#"{"activity_attempt_id":"attempt-cancel","cancel_requested":true,"can_continue":false,"reason":"run_cancelled","run_closed_reason":"cancelled","heartbeat_recorded":false}"#,
            ),
            "/api/worker/activity-tasks/activity-cancel/complete" => (
                "409 Conflict",
                r#"{"task_id":"activity-cancel","activity_attempt_id":"attempt-cancel","reason":"run_cancelled","cancel_requested":true,"can_continue":false,"run_closed_reason":"cancelled"}"#,
            ),
            "/api/worker/activity-tasks/activity-typed/complete"
            | "/api/worker/activity-tasks/activity-typed/fail"
            | "/api/worker/activity-tasks/capacity-activity/complete"
            | "/api/workflows/typed-1/signal/changed" => ("200 OK", "{}"),
            "/api/workflows/counter-1/query/current" => (
                "200 OK",
                r#"{"workflow_id":"counter-1","query_name":"current","result":{"count":8},"result_envelope":{"codec":"avro","blob":"wwHioz3/VYAiNw4CCmNvdW50BBAA"}}"#,
            ),
            "/api/workflows/counter-1/query/missing" => (
                "404 Not Found",
                r#"{"workflow_id":"counter-1","query_name":"missing","reason":"rejected_unknown_query","message":"unknown query"}"#,
            ),
            "/api/workflows/wf-lifecycle/cancel" => (
                "200 OK",
                r#"{"workflow_id":"wf-lifecycle","run_id":"run-current","outcome":"cancelled","reason":"cleanup requested","command_status":"accepted"}"#,
            ),
            "/api/workflows/wf-lifecycle/terminate" => (
                "200 OK",
                r#"{"workflow_id":"wf-lifecycle","run_id":"run-current","outcome":"terminated","reason":"forced stop","command_status":"accepted"}"#,
            ),
            "/api/workflows/wf-lifecycle/runs/run-current/cancel" => (
                "200 OK",
                r#"{"workflow_id":"wf-lifecycle","run_id":"run-current","outcome":"cancelled","command_status":"accepted"}"#,
            ),
            "/api/workflows/wf-lifecycle/runs/run-current/terminate" => (
                "200 OK",
                r#"{"workflow_id":"wf-lifecycle","run_id":"run-current","outcome":"terminated","command_status":"accepted"}"#,
            ),
            "/api/workflows/wf-lifecycle/runs/run-stale/cancel"
            | "/api/workflows/wf-lifecycle/runs/run-stale/terminate" => (
                "409 Conflict",
                r#"{"workflow_id":"wf-lifecycle","run_id":"run-stale","reason":"historical_run_command_rejected","target_scope":"run","message":"Commands cannot target historical runs."}"#,
            ),
            "/api/workflows/wf-failed" | "/api/workflows/wf-failed/runs/run-failed" => (
                "200 OK",
                r#"{"workflow_id":"wf-failed","run_id":"run-failed","status":"failed","closed_reason":"failed","error":"payment failed","failure":{"message":"payment failed","failure_category":"application","exception_type":"PaymentError","exception_class":"billing::PaymentError","non_retryable":true,"exception":{"type":"PaymentError","class":"billing::PaymentError","message":"payment failed"},"failures":[{"id":"failure-17","failure_category":"application"}]}}"#,
            ),
            "/api/workflows/wf-cancelled" => (
                "200 OK",
                r#"{"workflow_id":"wf-cancelled","run_id":"run-cancelled","status":"cancelled","closed_reason":"cancelled","reason":"cleanup requested"}"#,
            ),
            "/api/workflows/wf-terminated" => (
                "200 OK",
                r#"{"workflow_id":"wf-terminated","run_id":"run-terminated","status":"terminated","closed_reason":"terminated","reason":"forced stop"}"#,
            ),
            "/api/workflows/wf-timed-out" => (
                "200 OK",
                r#"{"workflow_id":"wf-timed-out","run_id":"run-timed-out","status":"failed","closed_reason":"timed_out","reason":"run_timeout"}"#,
            ),
            "/api/workflows/wf-waiting" | "/api/workflows/wf-waiting/runs/run-waiting" => (
                "200 OK",
                r#"{"workflow_id":"wf-waiting","run_id":"run-waiting","status":"waiting"}"#,
            ),
            "/api/workflows/wf-selected" => (
                "200 OK",
                r#"{"workflow_id":"wf-selected","run_id":"run-current","status":"completed","output":"current run output"}"#,
            ),
            "/api/workflows/wf-selected/runs/run-selected" => (
                "200 OK",
                r#"{"workflow_id":"wf-selected","run_id":"run-selected","status":"cancelled","closed_reason":"cancelled","reason":"selected run cancelled"}"#,
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
