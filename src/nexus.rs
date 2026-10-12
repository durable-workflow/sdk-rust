use super::*;

pub(super) fn installed_cooperation(capabilities: &Value) -> bool {
    capabilities["cooperative_cancellation"] == true
        && capabilities["nexus_cooperative_cancellation"] == true
        && capabilities["nexus_cooperative_cancellation_binding_kinds"]
            .as_array()
            .is_some_and(|kinds| kinds.iter().any(|kind| kind == "workflow_run"))
}

/// The catalog operation's execution mode.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NexusOperationMode {
    #[default]
    Sync,
    Async,
}

/// Resume from durable admission or wait for the committed service result.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NexusWaitFor {
    Accepted,
    #[default]
    Completed,
}

/// Optional routing and waiting policy for an authored Nexus call.
#[derive(Clone, Debug, Default)]
pub struct NexusServiceOptions {
    pub target_namespace: Option<String>,
    pub idempotency_key: Option<String>,
    pub mode: NexusOperationMode,
    pub wait_for: NexusWaitFor,
}

impl NexusServiceOptions {
    pub fn new() -> Self {
        Self::default()
    }

    fn completion_wait(&self) -> bool {
        self.mode == NexusOperationMode::Sync && self.wait_for == NexusWaitFor::Completed
    }
}

/// The original durable service identity and its observed outcome.
#[derive(Clone, Debug, PartialEq)]
pub struct NexusOperationResult {
    pub service_call_id: String,
    pub status: String,
    pub target_namespace: Option<String>,
    pub linked_workflow_instance_id: Option<String>,
    pub linked_workflow_run_id: Option<String>,
    pub result: AvroValue,
}

/// A committed service failure delivered at the original authored call.
#[derive(Clone, Debug, PartialEq, Error)]
#[error("Nexus service call {service_call_id} {status}: {message}")]
pub struct NexusOperationFailure {
    pub service_call_id: String,
    pub status: String,
    pub exception_type: String,
    pub message: String,
    pub exception: Value,
}

#[derive(Clone, Debug)]
pub(super) struct RecordedServiceCall {
    pub endpoint_name: String,
    pub service_name: String,
    pub operation_name: String,
    pub mode: NexusOperationMode,
    pub wait_for: NexusWaitFor,
    pub idempotency_key: Option<String>,
    pub admission: NexusOperationResult,
    pub outcome: Option<std::result::Result<NexusOperationResult, NexusOperationFailure>>,
}

fn history_error(sequence: u64, detail: &str) -> Error {
    invalid_recorded_history(
        "nexus_service_history_invalid",
        sequence,
        "one original service admission and terminal delivery",
        "invalid service history",
        detail,
    )
}

fn service_text(payload: &Value, field: &str) -> Option<String> {
    payload_string(payload, field).or_else(|| payload_string(&payload["service_call"], field))
}

fn service_result(
    payload: &Value,
    codec: &str,
    sequence: u64,
    status: &str,
) -> Result<NexusOperationResult> {
    let service_call_id = service_text(payload, "service_call_id")
        .ok_or_else(|| history_error(sequence, "service_call_id is missing"))?;
    let result = if status == "completed" {
        let envelope = payload
            .get("response_payload")
            .filter(|value| !value.is_null())
            .or_else(|| payload["service_call"].get("result_envelope"))
            .unwrap_or(&Value::Null);
        decode_wire_avro_value(envelope, codec)?
    } else {
        AvroValue::Null
    };
    Ok(NexusOperationResult {
        service_call_id,
        status: status.to_string(),
        result,
        target_namespace: service_text(payload, "target_namespace"),
        linked_workflow_instance_id: service_text(payload, "linked_workflow_instance_id"),
        linked_workflow_run_id: service_text(payload, "linked_workflow_run_id"),
    })
}

pub(super) fn is_service_event(event: &HistoryEvent) -> bool {
    matches!(
        event.event_type.as_str(),
        "ServiceCallStarted"
            | "ServiceCallCompleted"
            | "ServiceCallFailed"
            | "ServiceCallCancelled"
    )
}

pub(super) fn recorded_call(
    events: &[&HistoryEvent],
    sequence: u64,
    codec: &str,
) -> Result<RecordedServiceCall> {
    let starts: Vec<_> = events
        .iter()
        .filter(|event| event.event_type == "ServiceCallStarted")
        .collect();
    if starts.len() != 1 {
        return Err(history_error(
            sequence,
            "service history must contain one original admission",
        ));
    }
    let start = starts[0];
    let required = |field| {
        service_text(&start.payload, field)
            .ok_or_else(|| history_error(sequence, &format!("original {field} is missing")))
    };
    let mode = serde_json::from_value(
        start
            .payload
            .get("operation_mode")
            .cloned()
            .or_else(|| start.payload["service_call"].get("operation_mode").cloned())
            .unwrap_or(json!("sync")),
    )
    .map_err(|_| history_error(sequence, "original operation mode is invalid"))?;
    let wait_for = serde_json::from_value(
        start
            .payload
            .get("wait_for")
            .cloned()
            .or_else(|| start.payload["service_call"].get("wait_for").cloned())
            .unwrap_or(json!("completed")),
    )
    .map_err(|_| history_error(sequence, "original waiting policy is invalid"))?;
    let admission = service_result(&start.payload, codec, sequence, "started")?;
    let terminals: Vec<_> = events
        .iter()
        .filter(|event| event.event_type != "ServiceCallStarted")
        .collect();
    if terminals.len() > 1 {
        return Err(history_error(
            sequence,
            "service history contains more than one terminal delivery",
        ));
    }
    let outcome = terminals
        .first()
        .map(|event| {
            if service_text(&event.payload, "service_call_id").as_deref()
                != Some(&admission.service_call_id)
            {
                return Err(history_error(
                    sequence,
                    "terminal delivery changes the original service identity",
                ));
            }
            if event.event_type == "ServiceCallCompleted" {
                return service_result(&event.payload, codec, sequence, "completed").map(Ok);
            }
            let status = if event.event_type == "ServiceCallCancelled" {
                "cancelled"
            } else {
                "failed"
            };
            let exception_type = service_text(&event.payload, "caller_observed_error_type")
                .or_else(|| service_text(&event.payload, "exception_type"))
                .unwrap_or_else(|| format!("service_call_{status}"));
            Ok(Err(NexusOperationFailure {
                service_call_id: admission.service_call_id.clone(),
                status: status.to_string(),
                exception_type,
                message: service_text(&event.payload, "message")
                    .unwrap_or_else(|| format!("service call {status}")),
                exception: event
                    .payload
                    .get("exception")
                    .cloned()
                    .unwrap_or(Value::Null),
            }))
        })
        .transpose()?;
    Ok(RecordedServiceCall {
        endpoint_name: required("endpoint_name")?,
        service_name: required("service_name")?,
        operation_name: required("operation_name")?,
        mode,
        wait_for,
        idempotency_key: service_text(&start.payload, "idempotency_key"),
        admission,
        outcome,
    })
}

/// Future returned by [`WorkflowContext::call_nexus_service`].
pub struct NexusServiceCall {
    ctx: WorkflowContext,
    endpoint_name: String,
    service_name: String,
    operation_name: String,
    options: NexusServiceOptions,
    arguments: Option<Result<AvroValue>>,
    suspended: bool,
}

impl WorkflowContext {
    /// Await a service result through durable workflow history.
    pub fn call_nexus_service<T: Serialize>(
        &self,
        endpoint_name: impl Into<String>,
        service_name: impl Into<String>,
        operation_name: impl Into<String>,
        arguments: T,
    ) -> NexusServiceCall {
        self.call_nexus_service_with_options(
            endpoint_name,
            service_name,
            operation_name,
            NexusServiceOptions::default(),
            arguments,
        )
    }

    pub fn call_nexus_service_with_options<T: Serialize>(
        &self,
        endpoint_name: impl Into<String>,
        service_name: impl Into<String>,
        operation_name: impl Into<String>,
        options: NexusServiceOptions,
        arguments: T,
    ) -> NexusServiceCall {
        NexusServiceCall {
            ctx: self.clone(),
            endpoint_name: endpoint_name.into(),
            service_name: service_name.into(),
            operation_name: operation_name.into(),
            options,
            arguments: Some(AvroValue::from_serialize(&arguments)),
            suspended: false,
        }
    }
}

impl Future for NexusServiceCall {
    type Output = Result<NexusOperationResult>;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
        if self.suspended {
            return Poll::Pending;
        }
        let ctx = self.ctx.clone();
        let mut state = match ctx.state.lock() {
            Ok(state) => state,
            Err(_) => return Poll::Ready(Err(Error::WorkflowStatePoisoned)),
        };
        if ctx.cancellation_scope_id != "root" {
            return Poll::Ready(Err(Error::CancellationScopeExecutionUnavailable));
        }
        let cursor = state.command_cursor;
        let recorded = match state
            .cancellation_replay_command(cursor, CancellationCallKind::ServiceOperation)
        {
            Ok(recorded) => recorded,
            Err(error) => return Poll::Ready(Err(error)),
        };
        if let Some(recorded) = recorded {
            let RecordedCommand::ServiceOperation { sequence, call } = recorded else {
                return Poll::Ready(Err(command_mismatch(&recorded, "service operation")));
            };
            for (field, expected, actual) in [
                (
                    "endpoint_name",
                    call.endpoint_name.as_str(),
                    self.endpoint_name.as_str(),
                ),
                (
                    "service_name",
                    call.service_name.as_str(),
                    self.service_name.as_str(),
                ),
                (
                    "operation_name",
                    call.operation_name.as_str(),
                    self.operation_name.as_str(),
                ),
            ] {
                if expected != actual {
                    return Poll::Ready(Err(invalid_recorded_history(
                        &format!("nexus_{field}_changed"),
                        sequence,
                        expected,
                        actual,
                        "original service intent changed",
                    )));
                }
            }
            if self
                .options
                .target_namespace
                .as_ref()
                .is_some_and(|target| call.admission.target_namespace.as_ref() != Some(target))
                || self
                    .options
                    .idempotency_key
                    .as_ref()
                    .is_some_and(|key| call.idempotency_key.as_ref() != Some(key))
                || self.options.mode != call.mode
                || self.options.wait_for != call.wait_for
            {
                return Poll::Ready(Err(history_error(
                    sequence,
                    "original service routing, idempotency or waiting policy changed",
                )));
            }
            if self.options.completion_wait() {
                if let Err(error) =
                    state.replay_cancellation_at(cursor, CancellationCallKind::ServiceOperation)
                {
                    return Poll::Ready(Err(error));
                }
                match state.prepare_scalar_cancellation(
                    cursor,
                    CancellationCallKind::ServiceOperation,
                    &[],
                ) {
                    Ok(true) => {
                        self.suspended = true;
                        return Poll::Pending;
                    }
                    Ok(false) => {}
                    Err(error) => return Poll::Ready(Err(error)),
                }
            }
            state.command_cursor += 1;
            if !self.options.completion_wait() {
                state.advance_cancellation_sequence(sequence, &[]);
                return Poll::Ready(Ok(call.admission));
            }
            if let Some(outcome) = call.outcome {
                state.advance_cancellation_sequence(sequence, &[]);
                return Poll::Ready(
                    outcome.map_err(|error| Error::NexusOperationFailed(Box::new(error))),
                );
            }
            state.matched_recorded_pending = true;
            self.suspended = true;
            return Poll::Pending;
        }
        if [
            self.endpoint_name.as_str(),
            self.service_name.as_str(),
            self.operation_name.as_str(),
        ]
        .iter()
        .any(|name| name.trim().is_empty())
            || self
                .options
                .target_namespace
                .as_ref()
                .is_some_and(|value| value.trim().is_empty())
            || self
                .options
                .idempotency_key
                .as_ref()
                .is_some_and(|value| value.trim().is_empty())
        {
            return Poll::Ready(Err(Error::WorkerLoop(
                "Nexus service identity must be non-empty".into(),
            )));
        }
        if self.options.completion_wait() {
            match state.prepare_scalar_cancellation(
                cursor,
                CancellationCallKind::ServiceOperation,
                &[],
            ) {
                Ok(true) => {
                    self.suspended = true;
                    return Poll::Pending;
                }
                Ok(false) => {}
                Err(error) => return Poll::Ready(Err(error)),
            }
        }
        let arguments = match self.arguments.take().unwrap_or(Ok(AvroValue::Null)) {
            Ok(arguments) => normalize_avro_arguments(arguments),
            Err(error) => return Poll::Ready(Err(error)),
        };
        let request_payload = match encode_typed_envelope(&arguments, &state.payload_codec) {
            Ok(payload) => payload,
            Err(error) => return Poll::Ready(Err(error)),
        };
        let payload_codec = state.payload_codec.clone();
        state.commands.push(json!({
            "type": "start_service_operation", "endpoint_name": self.endpoint_name,
            "service_name": self.service_name, "operation_name": self.operation_name,
            "namespace": self.options.target_namespace, "idempotency_key": self.options.idempotency_key,
            "mode_override": self.options.mode, "wait_for": self.options.wait_for,
            "payload_codec": payload_codec, "request_payload": request_payload,
            "metadata": {"caller_sdk_language": "sdk-rust"},
        }));
        self.suspended = true;
        Poll::Pending
    }
}
