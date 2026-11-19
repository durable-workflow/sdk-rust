//! Signal another workflow from workflow code as a durable command
//! (durable-workflow/.github#223, Server #1134).
//!
//! The command is recorded once in the sender's history as
//! `ExternalSignalRequested`, followed in the same transaction by exactly one
//! `ExternalSignalDelivered` or `ExternalSignalFailed` at the same sequence.
//! Replay resolves the call from that recorded outcome and never submits the
//! signal again.

use super::{
    command_mismatch, decode_wire_avro_value, encode_typed_envelope, invalid_recorded_history,
    normalize_avro_arguments, AvroValue, Error, HistoryEvent, RecordedCommand, ReplayFailure,
    Result, Value, WorkflowContext,
};
use serde::Serialize;
use std::{
    future::Future,
    pin::Pin,
    task::{Context as TaskContext, Poll},
};

pub(super) const COMMAND: &str = "signal_external_workflow";
pub(super) const CAPABILITY: &str = "signal_external_workflow";
pub(super) const REQUESTED: &str = "ExternalSignalRequested";
pub(super) const DELIVERED: &str = "ExternalSignalDelivered";
pub(super) const FAILED: &str = "ExternalSignalFailed";

const MAX_WORKFLOW_ID_CHARS: usize = 191;
const MAX_SIGNAL_NAME_CHARS: usize = 191;
const MAX_SIGNAL_NAME_BYTES: usize = 255;

/// The recorded outcome of a signal sent with
/// [`WorkflowContext::signal_external_workflow`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ExternalSignalOutcome {
    /// The target run admitted the signal (`SignalReceived` in its history).
    Delivered,
    /// No workflow instance has that id in the namespace, or the requested
    /// run is not a run of it.
    TargetNotFound,
    /// The target run has closed, or the requested run was replaced.
    TargetClosed,
    /// The target's signal admission, or the Server, refused the signal.
    Rejected,
}

impl ExternalSignalOutcome {
    /// The wire value recorded in history.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Delivered => "delivered",
            Self::TargetNotFound => "target_not_found",
            Self::TargetClosed => "target_closed",
            Self::Rejected => "rejected",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "delivered" => Self::Delivered,
            "target_not_found" => Self::TargetNotFound,
            "target_closed" => Self::TargetClosed,
            "rejected" => Self::Rejected,
            _ => return None,
        })
    }
}

impl std::fmt::Display for ExternalSignalOutcome {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// The outcome of an external signal as recorded in the sender's history.
///
/// A failed delivery is a recorded result, not an error: inspect
/// [`outcome`](Self::outcome) and [`reason`](Self::reason) (for example
/// `instance_not_found`, `run_not_active` or `unknown_signal`) and decide in
/// workflow code what to do. Replay returns the same value.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ExternalSignalResult {
    pub outcome: ExternalSignalOutcome,
    /// The stable reason for a failed outcome; `None` when delivered.
    pub reason: Option<String>,
    /// The Server's human-readable detail for a failed outcome.
    pub message: Option<String>,
    pub target_workflow_id: String,
    /// The run that received the signal, or for a failure the run that was
    /// resolved (`None` when no run was resolved).
    pub target_run_id: Option<String>,
    pub signal_name: String,
    /// The command key (`workflow-signal:<sender run id>:<sequence>`).
    pub request_id: Option<String>,
    /// The command recorded on the target, when the target recorded one.
    pub target_command_id: Option<String>,
    /// The target's signal record, when delivered.
    pub target_signal_id: Option<String>,
    /// True when the same command key had already been delivered.
    pub retained: bool,
}

impl ExternalSignalResult {
    pub fn is_delivered(&self) -> bool {
        self.outcome == ExternalSignalOutcome::Delivered
    }
}

/// The intent recorded by `ExternalSignalRequested`.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct RecordedRequest {
    target_workflow_id: String,
    target_run_id: Option<String>,
    signal_name: String,
    arguments: Option<AvroValue>,
}

/// Whether the connected Server advertises the `signal_external_workflow`
/// workflow-task command in its worker `server_capabilities`.
pub(super) fn runtime_supports(capabilities: Option<&Value>) -> bool {
    let Some(capabilities) = capabilities.and_then(Value::as_object) else {
        return false;
    };
    let flag = match capabilities.get(CAPABILITY) {
        Some(Value::Bool(supported)) => *supported,
        Some(Value::Object(capability)) => capability.get("supported") == Some(&Value::Bool(true)),
        _ => false,
    };
    let command_advertised = capabilities
        .get("supported_workflow_task_commands")
        .and_then(Value::as_array)
        .is_some_and(|commands| commands.iter().any(|command| command == COMMAND));
    flag && command_advertised
}

pub(super) fn is_event(event: &HistoryEvent) -> bool {
    matches!(event.event_type.as_str(), REQUESTED | DELIVERED | FAILED)
}

fn optional_string(event: &HistoryEvent, field: &str) -> Option<String> {
    event
        .payload
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn required_string(event: &HistoryEvent, field: &str, sequence: u64) -> Result<String> {
    optional_string(event, field).ok_or_else(|| {
        invalid_recorded_history(
            "external_signal_field_missing",
            sequence,
            &format!("non-empty {field}"),
            &format!("missing or invalid {field} on {}", event.event_type),
            "external-signal history is missing a required field",
        )
    })
}

/// Build the recorded command for one sequence from its external-signal
/// events.
pub(super) fn recorded(
    events: &[&HistoryEvent],
    sequence: u64,
    fallback_codec: &str,
) -> Result<RecordedCommand> {
    let requested: Vec<_> = events
        .iter()
        .copied()
        .filter(|event| event.event_type == REQUESTED)
        .collect();
    if requested.len() != 1 {
        return Err(invalid_recorded_history(
            "external_signal_request_count",
            sequence,
            &format!("one {REQUESTED} event"),
            &format!("{} {REQUESTED} events", requested.len()),
            "external-signal history must record exactly one request per workflow sequence",
        ));
    }
    let requested = requested[0];
    let resolutions: Vec<_> = events
        .iter()
        .copied()
        .filter(|event| event.event_type != REQUESTED)
        .collect();
    if resolutions.len() > 1 {
        return Err(invalid_recorded_history(
            "external_signal_resolution_count",
            sequence,
            "at most one ExternalSignalDelivered or ExternalSignalFailed event",
            &format!("{} resolution events", resolutions.len()),
            "external-signal history resolves one request more than once",
        ));
    }

    let target_workflow_id = required_string(requested, "target_workflow_instance_id", sequence)?;
    let signal_name = required_string(requested, "signal_name", sequence)?;
    let codec = requested
        .payload
        .get("payload_codec")
        .and_then(Value::as_str)
        .unwrap_or(fallback_codec);
    let arguments = match requested
        .payload
        .get("arguments")
        .filter(|value| !value.is_null())
    {
        Some(envelope) => Some(normalize_avro_arguments(
            decode_wire_avro_value(envelope, codec).map_err(|error| {
                if error.to_string().contains("unsupported_payload_codec") {
                    return error;
                }
                invalid_recorded_history(
                    "external_signal_arguments_incompatible",
                    sequence,
                    &format!("valid {codec} payload envelope"),
                    &error.to_string(),
                    "external-signal history arguments cannot be decoded with their recorded codec",
                )
            })?,
        )),
        None => None,
    };
    let request = RecordedRequest {
        target_workflow_id,
        target_run_id: optional_string(requested, "target_workflow_run_id"),
        signal_name,
        arguments,
    };

    let outcome = match resolutions.first() {
        None => None,
        Some(resolution) => Some(resolution_result(
            resolution, &request, requested, sequence,
        )?),
    };
    Ok(RecordedCommand::ExternalSignal {
        sequence,
        request,
        outcome,
    })
}

fn resolution_result(
    event: &HistoryEvent,
    request: &RecordedRequest,
    requested: &HistoryEvent,
    sequence: u64,
) -> Result<ExternalSignalResult> {
    for (field, expected) in [
        (
            "target_workflow_instance_id",
            Some(request.target_workflow_id.as_str()),
        ),
        ("signal_name", Some(request.signal_name.as_str())),
    ] {
        let actual = event.payload.get(field).and_then(Value::as_str);
        if actual.is_some() && actual != expected {
            return Err(invalid_recorded_history(
                "external_signal_identity_mismatch",
                sequence,
                expected.unwrap_or_default(),
                actual.unwrap_or_default(),
                "external-signal resolution disagrees with its recorded request",
            ));
        }
    }
    let request_id = optional_string(event, "request_id");
    if let (Some(resolved), Some(recorded)) = (
        request_id.as_deref(),
        optional_string(requested, "request_id"),
    ) {
        if resolved != recorded {
            return Err(invalid_recorded_history(
                "external_signal_identity_mismatch",
                sequence,
                &recorded,
                resolved,
                "external-signal resolution disagrees with its recorded command key",
            ));
        }
    }
    let raw_outcome = event
        .payload
        .get("outcome")
        .and_then(Value::as_str)
        .unwrap_or(if event.event_type == DELIVERED {
            "delivered"
        } else {
            ""
        });
    let outcome = ExternalSignalOutcome::parse(raw_outcome)
        .filter(|outcome| {
            (*outcome == ExternalSignalOutcome::Delivered) == (event.event_type == DELIVERED)
        })
        .ok_or_else(|| {
            invalid_recorded_history(
                "external_signal_outcome_invalid",
                sequence,
                if event.event_type == DELIVERED {
                    "delivered"
                } else {
                    "target_not_found, target_closed or rejected"
                },
                raw_outcome,
                "external-signal resolution records an unknown outcome",
            )
        })?;
    let reason = optional_string(event, "reason");
    if outcome != ExternalSignalOutcome::Delivered && reason.is_none() {
        return Err(invalid_recorded_history(
            "external_signal_field_missing",
            sequence,
            "non-empty reason",
            &format!("missing reason on {}", event.event_type),
            "a failed external signal must record its reason",
        ));
    }
    Ok(ExternalSignalResult {
        outcome,
        reason,
        message: optional_string(event, "message"),
        target_workflow_id: request.target_workflow_id.clone(),
        target_run_id: optional_string(event, "target_workflow_run_id"),
        signal_name: request.signal_name.clone(),
        request_id,
        target_command_id: optional_string(event, "target_command_id"),
        target_signal_id: optional_string(event, "target_signal_id"),
        retained: event.payload.get("retained").and_then(Value::as_bool) == Some(true),
    })
}

fn validate(workflow_id: &str, signal_name: &str, run_id: Option<&str>) -> Result<()> {
    if workflow_id.is_empty() || workflow_id.chars().count() > MAX_WORKFLOW_ID_CHARS {
        return Err(Error::InvalidExternalSignal(format!(
            "workflow_id must be 1 to {MAX_WORKFLOW_ID_CHARS} characters"
        )));
    }
    if signal_name.trim().is_empty()
        || signal_name.chars().count() > MAX_SIGNAL_NAME_CHARS
        || signal_name.len() > MAX_SIGNAL_NAME_BYTES
    {
        return Err(Error::InvalidExternalSignal(format!(
            "signal_name must not be blank and at most {MAX_SIGNAL_NAME_CHARS} characters and {MAX_SIGNAL_NAME_BYTES} bytes"
        )));
    }
    if run_id.is_some_and(str::is_empty) {
        return Err(Error::InvalidExternalSignal(
            "run_id must not be empty; omit it to signal the current run".to_string(),
        ));
    }
    Ok(())
}

impl WorkflowContext {
    /// Signal another workflow in this namespace as a durable workflow
    /// command, and await the recorded outcome.
    ///
    /// The signal is recorded once in this workflow's history and delivered
    /// by the Server in the same transaction; replay reads the recorded
    /// outcome and never sends it again. Without
    /// [`run_id`](ExternalSignalCall::run_id) the signal goes to the target
    /// instance's current run. `args` are encoded like child-workflow
    /// arguments (a value that is not an array is sent as one argument).
    ///
    /// A delivery that fails (missing target, closed target, rejected
    /// signal) resolves to `Ok` with that [`ExternalSignalOutcome`] and its
    /// reason, so workflow code decides what to do. Errors are reserved for
    /// invalid input ([`Error::InvalidExternalSignal`]), a Server that does
    /// not advertise the command ([`Error::SignalExternalWorkflowUnavailable`],
    /// returned before anything is submitted), and replay mismatches. A
    /// different target, name or arguments at a recorded sequence is a
    /// nondeterministic replay.
    ///
    /// ```no_run
    /// # use durable_workflow::{json, Client, ExternalSignalOutcome, Worker};
    /// # fn configure(client: Client) {
    /// let mut worker = Worker::new(client, "orders");
    /// worker.register_workflow("approver", |ctx, _input| async move {
    ///     let sent = ctx
    ///         .signal_external_workflow("order-42", "approve", json!(["manager"]))
    ///         .await?;
    ///     Ok(json!({
    ///         "outcome": sent.outcome.as_str(),
    ///         "reason": sent.reason,
    ///         "delivered": sent.outcome == ExternalSignalOutcome::Delivered,
    ///     }))
    /// });
    /// # }
    /// ```
    pub fn signal_external_workflow<T: Serialize>(
        &self,
        workflow_id: impl Into<String>,
        signal_name: impl Into<String>,
        args: T,
    ) -> ExternalSignalCall {
        ExternalSignalCall {
            ctx: self.clone(),
            workflow_id: workflow_id.into(),
            signal_name: signal_name.into(),
            run_id: None,
            args: Some(AvroValue::from_serialize(&args)),
            submitted: false,
            matched_pending: false,
        }
    }
}

/// Future returned by [`WorkflowContext::signal_external_workflow`].
pub struct ExternalSignalCall {
    ctx: WorkflowContext,
    workflow_id: String,
    signal_name: String,
    run_id: Option<String>,
    args: Option<Result<AvroValue>>,
    submitted: bool,
    matched_pending: bool,
}

impl ExternalSignalCall {
    /// Signal only this run of the target instance. A run the instance has
    /// since replaced resolves to [`ExternalSignalOutcome::TargetClosed`]
    /// with reason `historical_run`.
    pub fn run_id(mut self, run_id: impl Into<String>) -> Self {
        self.run_id = Some(run_id.into());
        self
    }

    fn mismatch(
        &self,
        sequence: u64,
        field: &str,
        recorded: String,
        current: String,
    ) -> Poll<Result<ExternalSignalResult>> {
        Poll::Ready(Err(Error::NonDeterministicReplay(ReplayFailure::new(
            "recorded_command_detail_mismatch",
            Some(sequence),
            Some(format!("external signal {field}:{recorded}")),
            Some(format!("external signal {field}:{current}")),
            format!("recorded external signal {field} differs from the current workflow command"),
        ))))
    }
}

impl Future for ExternalSignalCall {
    type Output = Result<ExternalSignalResult>;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
        if self.matched_pending || self.submitted {
            return Poll::Pending;
        }
        let args = match self.args.take() {
            Some(Ok(args)) => normalize_avro_arguments(args),
            Some(Err(error)) => return Poll::Ready(Err(error)),
            None => AvroValue::Array(Vec::new()),
        };
        let ctx = self.ctx.clone();
        let mut state = match ctx.state.lock() {
            Ok(state) => state,
            Err(_) => return Poll::Ready(Err(Error::WorkflowStatePoisoned)),
        };

        if let Some(recorded) = state.recorded_commands.get(state.command_cursor).cloned() {
            let RecordedCommand::ExternalSignal {
                sequence,
                request,
                outcome,
            } = recorded
            else {
                return Poll::Ready(Err(command_mismatch(
                    &recorded,
                    format!("external signal:{}", self.signal_name),
                )));
            };
            if request.target_workflow_id != self.workflow_id {
                return self.mismatch(
                    sequence,
                    "target",
                    request.target_workflow_id,
                    self.workflow_id.clone(),
                );
            }
            if request.target_run_id != self.run_id {
                return self.mismatch(
                    sequence,
                    "run",
                    request.target_run_id.unwrap_or_default(),
                    self.run_id.clone().unwrap_or_default(),
                );
            }
            if request.signal_name != self.signal_name {
                return self.mismatch(
                    sequence,
                    "name",
                    request.signal_name,
                    self.signal_name.clone(),
                );
            }
            if let Some(recorded_args) = request.arguments {
                if recorded_args != args {
                    return self.mismatch(
                        sequence,
                        "arguments",
                        format!("{recorded_args:?}"),
                        format!("{args:?}"),
                    );
                }
            }
            state.command_cursor += 1;
            state.replay_clock.unavailable();
            if let Some(outcome) = outcome {
                return Poll::Ready(Ok(outcome));
            }
            // The outcome is recorded in the request's own transaction, so a
            // request without one is only seen on a partial history page.
            state.matched_recorded_pending = true;
            state.observation_pending = true;
            self.matched_pending = true;
            return Poll::Pending;
        }

        if let Err(error) = validate(&self.workflow_id, &self.signal_name, self.run_id.as_deref()) {
            return Poll::Ready(Err(error));
        }
        if !state.signal_external_workflow_supported {
            return Poll::Ready(Err(Error::SignalExternalWorkflowUnavailable));
        }
        let arguments = match encode_typed_envelope(&args, &state.payload_codec) {
            Ok(arguments) => arguments,
            Err(error) => return Poll::Ready(Err(error)),
        };
        state.replay_clock.unavailable();
        state.commands.push(serde_json::json!({
            "type": COMMAND,
            "workflow_id": self.workflow_id,
            "run_id": self.run_id,
            "signal_name": self.signal_name,
            "arguments": arguments,
        }));
        drop(state);
        self.submitted = true;
        Poll::Pending
    }
}
