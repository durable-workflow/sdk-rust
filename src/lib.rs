#![doc = include_str!("../README.md")]

use std::{
    any::{Any, TypeId},
    collections::{BTreeMap, HashMap},
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
pub use uuid::Uuid;

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
const WORKFLOW_TASK_WAITING_FOR_HISTORY_MESSAGE: &str =
    "Workflow task waiting for scheduled history.";
const WORKFLOW_TASK_WAITING_FOR_HISTORY_TYPE: &str = "WorkflowTaskWaitingForHistory";

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
    #[error(transparent)]
    NonDeterministicReplay(ReplayFailure),
    #[error(transparent)]
    ChildWorkflowFailed(ChildWorkflowFailure),
    #[error(transparent)]
    ActivityFailed(ActivityFailure),
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
    #[error("workflow future yielded without emitting a durable command")]
    WorkflowYieldedWithoutCommand,
    #[error("workflow state lock is poisoned")]
    WorkflowStatePoisoned,
    #[error("timer duration is too large for the worker protocol")]
    TimerDurationOverflow,
    #[error("operation timed out")]
    Timeout,
    #[error("worker loop error: {0}")]
    WorkerLoop(String),
    #[error("invalid child workflow options: {0}")]
    InvalidChildWorkflowOptions(String),
    #[error(transparent)]
    InvalidActivityOptions(ActivityOptionsError),
    #[error(transparent)]
    InvalidContinueAsNewOptions(#[from] ContinueAsNewOptionsError),
    #[doc(hidden)]
    #[error("workflow requested continue as new")]
    ContinueAsNew(ContinueAsNewRequest),
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
    arguments: Value,
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
        let input = serde_json::to_value(input)?;
        let input_envelope = encode_value_envelope(&normalize_arguments(input), DEFAULT_CODEC)?;
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
        let input = serde_json::to_value(input)?;
        let input_envelope = encode_value_envelope(&normalize_arguments(input), DEFAULT_CODEC)?;
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

    async fn query_workflow_target<T: Serialize>(
        &self,
        workflow_id: &str,
        run_id: Option<&str>,
        query_name: &str,
        input: T,
    ) -> Result<Value> {
        let input = serde_json::to_value(input)?;
        let input_envelope = encode_value_envelope(&normalize_arguments(input), DEFAULT_CODEC)?;
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
        let timeout_seconds = long_poll_timeout_seconds(timeout);
        let body = json!({
            "worker_id": worker_id,
            "task_queue": task_queue,
            "poll_request_id": unique_request_id("rust-query-poll"),
            "timeout_seconds": timeout_seconds,
        });
        worker_poll_response(
            self.request_json_with_timeout(
                reqwest::Method::POST,
                "/worker/query-tasks/poll",
                RequestProtocol::Worker(QUERY_TASK_MINIMUM_WORKER_PROTOCOL_VERSION),
                Some(&body),
                timeout + Duration::from_secs(5),
            )
            .await,
        )
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
        let mut data: PollWorkflowTaskResponse = worker_poll_response(
            self.request_json_with_timeout(
                reqwest::Method::POST,
                "/worker/workflow-tasks/poll",
                RequestProtocol::Worker(WORKER_PROTOCOL_VERSION),
                Some(&body),
                timeout + Duration::from_secs(5),
            )
            .await,
        )?;

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
        let body = json!({
            "worker_id": worker_id,
            "task_queue": task_queue,
            "timeout_seconds": long_poll_timeout_seconds(timeout),
        });
        let data: PollActivityTaskResponse = worker_poll_response(
            self.request_json_with_timeout(
                reqwest::Method::POST,
                "/worker/activity-tasks/poll",
                RequestProtocol::Worker(WORKER_PROTOCOL_VERSION),
                Some(&body),
                timeout + Duration::from_secs(5),
            )
            .await,
        )?;
        Ok(data)
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

    /// Await only the run identity originally selected by this handle.
    pub async fn result_selected_run(&self, options: WorkflowResultOptions) -> Result<Value> {
        let run_id = self.run_id.as_deref().ok_or_else(|| {
            Error::Codec("run_id is required for selected-run result".to_string())
        })?;
        self.result_target(options, Some(run_id)).await
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
        let response = self
            .retry_worker_operation(|| {
                self.client.poll_workflow_task_response(
                    &self.worker_id,
                    &self.task_queue,
                    self.poll_timeout,
                )
            })
            .await?;
        if response.outcome().should_stop() {
            return Ok(ManagedPollOutcome::Stop);
        }
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

        match self.execute_workflow_task(task) {
            Ok(commands) if commands.is_empty() => {
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
            Ok(commands) => {
                let completion = self
                    .client
                    .complete_workflow_task(&task_id, &lease_owner, attempt, commands)
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
        let response = self
            .retry_worker_operation(|| {
                self.client.poll_activity_task_response(
                    &self.worker_id,
                    &self.task_queue,
                    self.poll_timeout,
                )
            })
            .await?;
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
        let response = self
            .retry_worker_operation(|| {
                self.client.poll_query_task_response(
                    &self.worker_id,
                    &self.task_queue,
                    self.poll_timeout,
                )
            })
            .await?;
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
                        return Ok(ManagedPollOutcome::Handled);
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
                let mut invocation =
                    replay(workflow_context.clone(), context.workflow_input.clone());
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

    fn execute_workflow_task(&self, task: WorkflowTask) -> Result<Vec<Value>> {
        let workflow = self
            .workflows
            .get(&task.workflow_type)
            .ok_or_else(|| Error::WorkflowNotRegistered(task.workflow_type.clone()))?;
        let input = decode_task_arguments(task.arguments.as_ref(), &task.payload_codec)?;
        let resume_signal = decode_resume_signal(&task)?;
        let history_budget = WorkflowHistoryBudget {
            event_count: task
                .total_history_events
                .unwrap_or_else(|| u64::try_from(task.history_events.len()).unwrap_or(u64::MAX)),
            size_bytes: task.history_size_bytes,
            continue_as_new_recommended: task.continue_as_new_recommended.unwrap_or(false),
            pressure: task.history_budget_pressure.clone(),
        };
        let mut workflow_state = WorkflowState::new_with_identity(
            task.history_events,
            task.workflow_id,
            task.run_id,
            self.task_queue.clone(),
            task.payload_codec.clone(),
            resume_signal,
        )?;
        workflow_state.history_budget = history_budget;
        let state = Arc::new(Mutex::new(workflow_state));
        let ctx = WorkflowContext { state };
        let mut future = (workflow.execute)(ctx.clone(), input);
        let mut cx = TaskContext::from_waker(noop_waker_ref());

        match future.as_mut().poll(&mut cx) {
            Poll::Ready(Ok(result)) => {
                ctx.ensure_history_consumed()?;
                let result = encode_value_envelope(&result, &task.payload_codec)?;
                let mut commands = ctx.take_commands()?;
                commands.push(json!({
                    "type": "complete_workflow",
                    "result": result
                }));
                Ok(commands)
            }
            Poll::Ready(Err(error)) => {
                if let Error::ContinueAsNew(request) = error {
                    let mut commands = ctx.take_commands()?;
                    if let Some(command) = ctx.continue_as_new_command(request)? {
                        commands.push(command);
                    }
                    ctx.ensure_history_consumed()?;
                    return Ok(commands);
                }
                // A handler error must not hide a committed durable command that
                // upgraded workflow code no longer consumes.
                ctx.ensure_history_consumed()?;
                if workflow_task_integrity_error(&error) {
                    // Replay and protocol failures describe the workflow-task
                    // decision itself. Do not let commands queued earlier in
                    // this uncommitted decision escape alongside a terminal
                    // workflow failure.
                    return Err(error);
                }
                let mut commands = ctx.take_commands()?;
                commands.push(workflow_failure_command(&error));
                Ok(commands)
            }
            Poll::Pending => {
                let commands = ctx.take_commands()?;
                if commands.is_empty() && !ctx.matched_recorded_pending()? {
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
            arguments: normalize_arguments(serde_json::to_value(args)?),
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
            args: Some(serde_json::to_value(args).map_err(Error::from)),
            scheduled: false,
        }
    }

    pub fn wait_signal(&self, signal_name: impl Into<String>) -> SignalCall {
        SignalCall {
            ctx: self.clone(),
            signal_name: signal_name.into(),
            opened_wait: false,
            matched_pending: false,
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
                        serde_json::from_value(value).map_err(|error| {
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
        let json_value = serde_json::to_value(&value)?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::WorkflowStatePoisoned)?;
        let result = encode_value_envelope(&json_value, &state.payload_codec)?;
        state.commands.push(json!({
            "type": "record_side_effect",
            "result": result,
        }));
        Ok(value)
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
            args: Some(serde_json::to_value(args).map_err(Error::from)),
            scheduled: false,
            matched_pending: false,
        }
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

        let arguments = encode_value_envelope(&request.arguments, &state.payload_codec)?;
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

#[derive(Debug)]
struct WorkflowState {
    workflow_id: Option<String>,
    run_id: Option<String>,
    task_queue: String,
    payload_codec: String,
    history_budget: WorkflowHistoryBudget,
    resume_signal: Option<ResumeSignal>,
    recorded_commands: Vec<RecordedCommand>,
    recorded_continue_as_new_sequence: Option<u64>,
    continue_as_new_consumed: bool,
    command_cursor: usize,
    matched_recorded_pending: bool,
    version_markers: HashMap<String, (i32, u64)>,
    commands: Vec<Value>,
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
        let event_count = u64::try_from(history.len()).unwrap_or(u64::MAX);
        Ok(Self {
            workflow_id,
            run_id,
            task_queue,
            payload_codec,
            history_budget: WorkflowHistoryBudget {
                event_count,
                ..WorkflowHistoryBudget::default()
            },
            resume_signal,
            recorded_commands,
            recorded_continue_as_new_sequence,
            continue_as_new_consumed: false,
            command_cursor: 0,
            matched_recorded_pending: false,
            version_markers: HashMap::new(),
            commands: Vec::new(),
        })
    }
}

#[derive(Clone, Debug)]
enum RecordedCommand {
    Activity {
        sequence: u64,
        activity_type: Option<String>,
        options: Option<RecordedActivityOptions>,
        outcome: Option<ActivityOutcome>,
    },
    Timer {
        sequence: u64,
        delay_seconds: u64,
        fired: bool,
    },
    ChildWorkflow {
        sequence: u64,
        workflow_type: Option<String>,
        outcome: Option<ChildWorkflowOutcome>,
    },
    SignalWait {
        sequence: u64,
        signal_name: String,
        value: Option<Vec<Value>>,
    },
    SideEffect {
        sequence: u64,
        value: Value,
    },
    VersionMarker {
        sequence: u64,
        change_id: String,
        version: i32,
    },
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
            | Self::SideEffect { sequence, .. }
            | Self::VersionMarker { sequence, .. } => *sequence,
        }
    }

    fn shape(&self) -> &'static str {
        match self {
            Self::Activity { .. } => "activity",
            Self::Timer { .. } => "timer",
            Self::ChildWorkflow { .. } => "child workflow",
            Self::SignalWait { .. } => "signal wait",
            Self::SideEffect { .. } => "side effect",
            Self::VersionMarker { .. } => "version marker",
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
    arguments: Vec<Value>,
}

pub struct ActivityCall {
    ctx: WorkflowContext,
    activity_type: String,
    options: ActivityOptions,
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
                    ..
                } => {
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
            let args = match self.args.take().unwrap_or(Ok(Value::Null)) {
                Ok(args) => args,
                Err(error) => return Poll::Ready(Err(error)),
            };
            let arguments = normalize_arguments(args);
            let envelope = match encode_value_envelope(&arguments, &state.payload_codec) {
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
            state.commands.push(Value::Object(command));
            self.scheduled = true;
        }

        Poll::Pending
    }
}

/// Future returned by [`WorkflowContext::sleep`].
pub struct TimerCall {
    ctx: WorkflowContext,
    delay_seconds: Option<u64>,
    scheduled: bool,
    matched_pending: bool,
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
                    ..
                } => {
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
            state.commands.push(json!({
                "type": "start_timer",
                "delay_seconds": requested_delay,
            }));
            self.scheduled = true;
        }

        Poll::Pending
    }
}

/// Future returned by [`WorkflowContext::start_child_workflow`].
pub struct ChildWorkflowCall {
    ctx: WorkflowContext,
    workflow_type: String,
    options: ChildWorkflowOptions,
    args: Option<Result<Value>>,
    scheduled: bool,
    matched_pending: bool,
}

impl Future for ChildWorkflowCall {
    type Output = Result<ChildWorkflowResult>;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
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
                    ..
                } => {
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

            let args = match self.args.take().unwrap_or(Ok(Value::Null)) {
                Ok(args) => args,
                Err(error) => return Poll::Ready(Err(error)),
            };
            let arguments =
                match encode_value_envelope(&normalize_arguments(args), &state.payload_codec) {
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
            state.commands.push(command);
            self.scheduled = true;
        }

        Poll::Pending
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
    opened_wait: bool,
    matched_pending: bool,
}

impl Future for SignalCall {
    type Output = Result<Vec<Value>>;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
        if self.matched_pending {
            return Poll::Pending;
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
                } => {
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
            state.commands.push(json!({
                "type": "open_signal_wait",
                "signal_name": self.signal_name
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
        let is_side_effect = event.event_type == "SideEffectRecorded";
        let is_version_marker = event.event_type == "VersionMarkerRecorded";
        if !is_activity
            && !is_workflow_timer
            && !is_child_workflow
            && !is_signal_wait
            && !is_side_effect
            && !is_version_marker
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

            let command_kind_count = usize::from(!activity_events.is_empty())
                + usize::from(!timer_events.is_empty())
                + usize::from(!child_events.is_empty())
                + usize::from(!signal_wait_events.is_empty())
                + usize::from(!side_effect_events.is_empty())
                + usize::from(!version_marker_events.is_empty());
            if command_kind_count > 1 {
                let actual = [
                    (!activity_events.is_empty()).then_some("activity"),
                    (!timer_events.is_empty()).then_some("timer"),
                    (!child_events.is_empty()).then_some("child workflow"),
                    (!signal_wait_events.is_empty()).then_some("signal wait"),
                    (!side_effect_events.is_empty()).then_some("side effect"),
                    (!version_marker_events.is_empty()).then_some("version marker"),
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
                if terminal.len() > 1 {
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
                });
            }

            if !child_events.is_empty() {
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
                if outcomes.len() > 1 {
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
                let value = decode_wire_value(result, codec).map_err(|error| {
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

type ActivityOutcome = std::result::Result<Value, ActivityFailure>;

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
        return Ok(Ok(decode_wire_value(
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

type ChildWorkflowOutcome = std::result::Result<ChildWorkflowResult, ChildWorkflowFailure>;

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
        outcomes.push(Ok(ChildWorkflowResult {
            parent: parent.clone(),
            child: WorkflowIdentity {
                workflow_id: child_workflow_id,
                run_id: child_workflow_run_id,
            },
            child_workflow_type,
            result: decode_wire_value(result, codec)?,
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

fn workflow_task_integrity_error(error: &Error) -> bool {
    matches!(
        error,
        Error::NonDeterministicReplay(_) | Error::Protocol(_) | Error::WorkflowStatePoisoned
    )
}

fn decode_signal_event_arguments(event: &HistoryEvent, fallback_codec: &str) -> Result<Vec<Value>> {
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
        None => Value::Array(Vec::new()),
    };
    let Value::Array(arguments) = normalize_arguments(decoded) else {
        unreachable!("normalize_arguments always returns an array");
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        io::{Read, Write},
        net::{SocketAddr, TcpListener, TcpStream},
        sync::atomic::AtomicUsize,
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

    fn workflow_context(history: Vec<HistoryEvent>) -> WorkflowContext {
        workflow_context_with_codec(history, JSON_CODEC)
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

    fn workflow_task(
        workflow_type: &str,
        history_events: Vec<HistoryEvent>,
        payload_codec: &str,
    ) -> WorkflowTask {
        WorkflowTask {
            task_id: format!("wft-{workflow_type}"),
            workflow_id: Some(format!("wf-{workflow_type}")),
            run_id: Some(format!("run-{workflow_type}")),
            workflow_type: workflow_type.to_string(),
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
            decode_wire_value(&commands[0]["result"], JSON_CODEC).expect("JSON result"),
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
    fn ordered_side_effects_share_the_durable_command_stream() {
        let first = encode_value_envelope(&json!("first"), JSON_CODEC).expect("first");
        let second = encode_value_envelope(&json!(29), JSON_CODEC).expect("second");
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
                JSON_CODEC.to_string(),
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
    fn duplicate_side_effects_and_version_markers_are_rejected() {
        let duplicate_side_effect = WorkflowState::new(
            vec![
                history_event(
                    "SideEffectRecorded",
                    json!({"sequence": 1, "result": {"codec": "json", "blob": "1"}}),
                ),
                history_event(
                    "SideEffectRecorded",
                    json!({"sequence": 1, "result": {"codec": "json", "blob": "2"}}),
                ),
            ],
            "rust-workers".to_string(),
            JSON_CODEC.to_string(),
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
            JSON_CODEC.to_string(),
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
                workflow_id: Some("wf-side-effect-version".to_string()),
                run_id: Some("run-side-effect-version".to_string()),
                workflow_type: "rust.side-effect-version".to_string(),
                payload_codec: JSON_CODEC.to_string(),
                arguments: Some(encode_value_envelope(&json!([]), JSON_CODEC).expect("arguments")),
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
        let result = encode_value_envelope(&json!({"value": 42}), JSON_CODEC).expect("result");
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
                    "payload_codec": "json",
                    "result": {"codec": "json", "blob": "{\"status\":\"recovered\"}"}
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
    fn workflow_history_rejects_unpaired_or_mismatched_timer_events() {
        let lone_fire = WorkflowState::new(
            vec![history_event(
                "TimerFired",
                json!({"sequence": 1, "timer_id": "timer-1", "delay_seconds": 5}),
            )],
            "rust-workers".to_string(),
            JSON_CODEC.to_string(),
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
            JSON_CODEC.to_string(),
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
            JSON_CODEC.to_string(),
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
            JSON_CODEC.to_string(),
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
                    "payload_codec": "json",
                    "result": {"codec": "json", "blob": "\"done\""},
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
    fn condition_wait_history_does_not_resolve_a_typed_signal_wait() {
        let ctx = workflow_context(vec![
            history_event(
                "ConditionWaitOpened",
                json!({"sequence": 1, "condition_key": "signal:finish"}),
            ),
            history_event(
                "ConditionWaitSatisfied",
                json!({"sequence": 1, "condition_key": "signal:finish"}),
            ),
            history_event(
                "SignalReceived",
                json!({"signal_name": "finish", "arguments": []}),
            ),
        ]);
        let mut signal = Box::pin(ctx.wait_signal("finish"));
        let mut task_context = TaskContext::from_waker(noop_waker_ref());

        assert!(matches!(
            signal.as_mut().poll(&mut task_context),
            Poll::Pending
        ));
        assert_eq!(
            ctx.take_commands().expect("typed signal-wait command"),
            vec![json!({
                "type": "open_signal_wait",
                "signal_name": "finish",
            })]
        );
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
                    "value": {"codec": "json", "blob": "[\"now\"]"},
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
                    "value": {"codec": "json", "blob": "[]"},
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
            JSON_CODEC.to_string(),
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
            JSON_CODEC.to_string(),
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
            JSON_CODEC.to_string(),
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
        let result = encode_value_envelope(&json!({"captured": true}), JSON_CODEC)
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
            encode_value_envelope(&json!("captured"), JSON_CODEC).expect("side-effect result");
        let zero = WorkflowState::new(
            vec![history_event(
                "SideEffectRecorded",
                json!({"sequence": 0, "result": result.clone()}),
            )],
            "rust-workers".to_string(),
            JSON_CODEC.to_string(),
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
            JSON_CODEC.to_string(),
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

        let marker =
            encode_value_envelope(&json!("after-finish"), JSON_CODEC).expect("side-effect result");
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
                        "payload_codec": "json",
                        "arguments": {"codec": "json", "blob": "[3]"},
                    }),
                ),
                history_event(
                    "SignalReceived",
                    json!({
                        "signal_id": "increment-5",
                        "signal_name": "increment",
                        "workflow_sequence": 3,
                        "payload_codec": "json",
                        "arguments": {"codec": "json", "blob": "[5]"},
                    }),
                ),
                history_event(
                    "SignalReceived",
                    json!({
                        "signal_id": "finish",
                        "signal_name": "finish",
                        "workflow_sequence": 4,
                        "payload_codec": "json",
                        "arguments": {"codec": "json", "blob": "[]"},
                    }),
                ),
                history_event(
                    "SignalApplied",
                    json!({
                        "sequence": 1,
                        "signal_id": "finish",
                        "signal_name": "finish",
                        "payload_codec": "json",
                        "value": {"codec": "json", "blob": "[]"},
                    }),
                ),
                history_event(
                    "SideEffectRecorded",
                    json!({"sequence": 5, "result": marker}),
                ),
            ],
            JSON_CODEC,
        );

        for _original_or_cold_worker in 0..2 {
            let commands = worker()
                .execute_workflow_task(task.clone())
                .expect("signal gaps preserve deterministic replay");
            assert_eq!(commands.len(), 1, "replay emits only terminal completion");
            assert_eq!(commands[0]["type"], "complete_workflow");
            assert_eq!(
                decode_wire_value(&commands[0]["result"], JSON_CODEC).expect("workflow output"),
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
    fn workflow_task_replay_completes_without_rescheduling_recorded_commands() {
        let client = Client::new("http://127.0.0.1:8080").expect("client");
        let mut worker = Worker::new(client, "rust-workers");
        worker.register_workflow("rust.timer", |ctx, _input| async move {
            ctx.sleep(Duration::from_secs(5)).await?;
            ctx.activity("after-timer", json!([])).await
        });

        let task = |history_events| WorkflowTask {
            task_id: "wft-rust-timer-1".to_string(),
            workflow_id: Some("wf-rust-timer".to_string()),
            run_id: Some("run-rust-timer".to_string()),
            workflow_type: "rust.timer".to_string(),
            payload_codec: JSON_CODEC.to_string(),
            arguments: Some(json!({"codec": "json", "blob": "[]"})),
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
            lease_owner: Some("rust-worker".to_string()),
        };

        let initial = worker
            .execute_workflow_task(task(Vec::new()))
            .expect("initial timer task");
        assert_eq!(
            initial,
            vec![json!({"type": "start_timer", "delay_seconds": 5})]
        );

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
                        "payload_codec": "json",
                        "result": {"codec": "json", "blob": "\"done\""},
                    }),
                ),
            ]))
            .expect("replayed workflow task");
        assert_eq!(replayed.len(), 1);
        assert_eq!(replayed[0]["type"], "complete_workflow");
        assert_eq!(
            decode_wire_value(&replayed[0]["result"], JSON_CODEC).expect("result"),
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
            JSON_CODEC,
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
            "payload_codec": JSON_CODEC,
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
        let result = decode_wire_value(&commands[0]["result"], JSON_CODEC).expect("result");
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
            workflow_id: Some("wf-rust-failing".to_string()),
            run_id: Some("run-rust-failing".to_string()),
            workflow_type: "rust.failing".to_string(),
            payload_codec: JSON_CODEC.to_string(),
            arguments: Some(encode_value_envelope(&json!([]), JSON_CODEC).expect("input")),
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
                JSON_CODEC,
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
            encode_value_envelope(&json!("committed"), JSON_CODEC).expect("side-effect result");

        let error = worker
            .execute_workflow_task(workflow_task(
                "rust.removed-side-effect",
                vec![history_event(
                    "SideEffectRecorded",
                    json!({"sequence": 1, "result": result}),
                )],
                JSON_CODEC,
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
                JSON_CODEC,
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
            workflow_id: Some("wf-rust-timer".to_string()),
            run_id: Some("run-rust-timer".to_string()),
            workflow_type: "rust.timer.pending".to_string(),
            payload_codec: JSON_CODEC.to_string(),
            arguments: Some(json!({"codec": "json", "blob": "[]"})),
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
            workflow_id: Some("wf-rust-timer".to_string()),
            run_id: Some("run-rust-timer".to_string()),
            workflow_type: "rust.timer.removed".to_string(),
            payload_codec: JSON_CODEC.to_string(),
            arguments: Some(json!({"codec": "json", "blob": "[]"})),
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
                    JSON_CODEC.to_string(),
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
            decode_wire_value(&command["arguments"], JSON_CODEC).expect("child args"),
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
            workflow_id: Some("wf-parent".to_string()),
            run_id: Some("run-parent".to_string()),
            workflow_type: "rust.parent".to_string(),
            payload_codec: JSON_CODEC.to_string(),
            arguments: Some(encode_value_envelope(&json!([]), JSON_CODEC).expect("input")),
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
                "payload_codec": "json",
                "result": {"codec": "json", "blob": "{\"from\":\"python\",\"ok\":true}"},
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
                decode_wire_value(&commands[0]["result"], JSON_CODEC).expect("parent output");
            assert_eq!(output["parent_workflow_id"], "wf-parent");
            assert_eq!(output["parent_run_id"], "run-parent");
            assert_eq!(output["child_workflow_id"], "wf-child");
            assert_eq!(output["child_run_id"], "run-child");
            assert_eq!(output["result"], json!({"from": "python", "ok": true}));
        }
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
        let output = decode_wire_value(&commands[0]["result"], JSON_CODEC).expect("parent output");
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
            history_size_bytes: None,
            continue_as_new_recommended: None,
            history_budget_pressure: None,
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
            history_size_bytes: None,
            continue_as_new_recommended: None,
            history_budget_pressure: None,
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

        let signal = task
            .history_events
            .iter()
            .find(|event| event.event_type == "SignalReceived")
            .expect("signal event");
        assert_eq!(
            decode_signal_event_arguments(signal, DEFAULT_CODEC).expect("signal arguments"),
            vec![json!("Rust")]
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
                    "payload_codec": "json",
                    "arguments": {"codec": "json", "blob": "[3]"}
                }
            },
            {
                "type": "SignalApplied",
                "payload": {
                    "sequence": 3,
                    "signal_id": "signal-3",
                    "signal_name": "increment",
                    "payload_codec": "json",
                    "value": {"codec": "json", "blob": "[3]"}
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
                    "payload_codec": "json",
                    "result": {"codec": "json", "blob": "\"loaded\""}
                }],
                "signals": [
                    {
                        "id": "signal-3",
                        "name": "increment",
                        "workflow_sequence": 2,
                        "payload_codec": "json",
                        "arguments": "[3]"
                    },
                    {
                        "id": "signal-5",
                        "name": "increment",
                        "workflow_sequence": 4,
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
                json!({"stage":"cleanup"}),
            )
            .await
            .expect("cancellation heartbeat");
        assert!(heartbeat.cancel_requested);
        assert!(heartbeat.should_stop());
        assert_eq!(heartbeat.reason.as_deref(), Some("run_cancelled"));
        assert_eq!(heartbeat.run_closed_reason.as_deref(), Some("cancelled"));

        let error = client
            .complete_activity_task(
                "activity-cancel",
                "attempt-cancel",
                "rust-worker",
                json!({"late":true}),
                JSON_CODEC,
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
                vec![json!({"type": "complete_workflow", "result": null})],
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
            json!([QUERY_TASKS_CAPABILITY])
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
        complete_named_signal: bool,
        poll_failures_per_path: usize,
        heartbeat_failures: usize,
        heartbeat_failure_request: Option<usize>,
        delayed_heartbeat_request: Option<usize>,
        heartbeat_response_delay: Duration,
        concurrent_requests: bool,
        unauthorized_polls: bool,
        cancelled_activity: bool,
        draining_polls: bool,
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

        fn request_count(&self, path: &str) -> usize {
            self.requests
                .lock()
                .expect("captured requests")
                .iter()
                .filter(|request| request.path == path)
                .count()
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
                received_at: Instant::now(),
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
                r#"{"task":{"task_id":"workflow-timeout-task","workflow_id":"reused-workflow-id","run_id":"run-selected-timeout","workflow_type":"timeout.workflow","payload_codec":"json","arguments":{"codec":"json","blob":"[]"},"history_events":[],"workflow_task_attempt":3,"lease_owner":"timeout-worker"}}"#,
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

        let (status, body) = match path {
            "/api/workflows" => (
                "201 Created",
                r#"{"workflow_id":"wf-start-options","run_id":"run-start-options","workflow_type":"rust.timeout"}"#,
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
                    r#"{"task":{"task_id":"activity-cancel","activity_attempt_id":"attempt-cancel","activity_type":"cancel-aware","payload_codec":"json","arguments":{"codec":"json","blob":"[]"},"attempt_number":1,"lease_owner":"rust-cancel-worker"}}"#,
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
                    r#"{"task":{"query_task_id":"query-late","query_task_attempt":1,"lease_owner":"late-worker","workflow_id":"counter-late","run_id":"run-late","workflow_type":"counter","query_name":"current","payload_codec":"json","workflow_arguments":{"codec":"json","blob":"[]"},"query_arguments":{"codec":"json","blob":"[]"},"history_events":[],"run_status":"running"}}"#,
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
            "/api/workflows/counter-1/query/current" => (
                "200 OK",
                r#"{"workflow_id":"counter-1","query_name":"current","result":{"count":8},"result_envelope":{"codec":"json","blob":"{\"count\":8}"}}"#,
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
