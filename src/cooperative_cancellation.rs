use super::*;
use std::collections::BTreeSet;

const CONTROL_BUDGET: Duration = Duration::from_secs(5);
const MAX_REFRESH_PAGES: usize = 128;

/// A delivery acknowledgment bound to one workflow task and durable run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CancellationDeliveryReceipt {
    pub task_id: String,
    pub run_id: String,
    pub delivery: CancellationDelivery,
}

/// Successful renewal of the exact workflow-task claim with optional observation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkflowTaskHeartbeat {
    pub task_id: String,
    pub workflow_task_attempt: u64,
    pub lease_owner: String,
    pub lease_expires_at: String,
    pub run_status: String,
    pub cancellation_request: Option<CancellationRequest>,
}

/// One actual protocol 1.20 claim and its original pending observation.
///
/// The claim is read-only. Observing a request does not deliver cancellation to
/// workflow code. Canonical history and a committed delivery are still required.
#[derive(Clone, Debug)]
pub struct CooperativeWorkflowTask {
    task: WorkflowTask,
    cancellation_request: Option<CancellationRequest>,
}

impl CooperativeWorkflowTask {
    pub fn task(&self) -> &WorkflowTask {
        &self.task
    }

    pub fn cancellation_request(&self) -> Option<&CancellationRequest> {
        self.cancellation_request.as_ref()
    }

    /// Renew this exact claim and retain the first observation's identity/token.
    ///
    /// A refused or malformed renewal leaves the claim and observation intact.
    pub async fn heartbeat(&mut self, client: &Client) -> Result<WorkflowTaskHeartbeat> {
        let receipt = client
            .heartbeat_workflow_task(&self.task, self.cancellation_request.as_ref())
            .await?;
        self.cancellation_request = receipt.cancellation_request.clone();
        Ok(receipt)
    }
}

/// An explicit cooperative poll, including ordinary idle and stop outcomes.
#[derive(Clone, Debug)]
pub struct CooperativeWorkflowTaskPoll {
    pub task: Option<CooperativeWorkflowTask>,
    pub outcome: WorkerPollOutcome,
    pub protocol_version: Option<String>,
    pub server_capabilities: Option<Value>,
}

/// Optional reason and runtime-owned cleanup limit for a cooperative request.
#[derive(Clone, Debug, Default, Serialize)]
pub struct CooperativeCancellationOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cleanup_timeout_seconds: Option<u64>,
}

/// The Server's original request identity and immutable cleanup deadline.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CancellationRequest {
    pub request_id: String,
    pub requested_at: String,
    pub cleanup_deadline_at: String,
    pub history_refresh_page_token: Option<String>,
}

/// Acknowledgment of a request targeting the current durable run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkflowCancellationRequest {
    pub workflow_id: String,
    pub run_id: String,
    pub duplicate: bool,
    pub cancellation_request: CancellationRequest,
}

/// Cancellation delivered at its committed authored call, with original identity.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
#[error("workflow cancellation {request_id} was requested", request_id = .request.request_id)]
pub struct CooperativeCancellationRequested {
    pub request: CancellationRequest,
    pub delivery: CancellationDelivery,
}

/// A workflow-local cleanup scope. Dropping it restores cancellation checks.
///
/// This does not renew or extend the Server's original cleanup deadline.
#[derive(Debug)]
pub struct CancellationShield {
    state: Arc<Mutex<WorkflowState>>,
}

impl Drop for CancellationShield {
    fn drop(&mut self) {
        if let Ok(mut state) = self.state.lock() {
            state.cancellation_shield_depth = state.cancellation_shield_depth.saturating_sub(1);
        }
    }
}

fn invalid(detail: impl Into<String>) -> Error {
    Error::InvalidCooperativeCancellation(detail.into())
}

fn text<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|text| !text.trim().is_empty())
        .ok_or_else(|| invalid(format!("{field} must be a non-empty string")))
}

impl CancellationRequest {
    pub(super) fn preserve_observation(&self, current: &Self) -> Result<Self> {
        self.validate_observation()?;
        current.validate_observation()?;
        if self.request_id != current.request_id
            || DateTime::parse_from_rfc3339(&self.requested_at).unwrap()
                != DateTime::parse_from_rfc3339(&current.requested_at).unwrap()
            || DateTime::parse_from_rfc3339(&self.cleanup_deadline_at).unwrap()
                != DateTime::parse_from_rfc3339(&current.cleanup_deadline_at).unwrap()
        {
            return Err(invalid(
                "observation changed the original request or cleanup deadline",
            ));
        }
        Ok(self.clone())
    }

    pub(super) fn validate_observation(&self) -> Result<()> {
        Self::from_observation(&json!({
            "request_id":self.request_id, "requested_at":self.requested_at,
            "cleanup_deadline_at":self.cleanup_deadline_at,
            "history_refresh_page_token":self.history_refresh_page_token,
        }))
        .map(|_| ())
    }

    pub(crate) fn from_observation(value: &Value) -> Result<Self> {
        let requested_at = text(value, "requested_at")?;
        let cleanup_deadline_at = text(value, "cleanup_deadline_at")?;
        let requested = DateTime::parse_from_rfc3339(requested_at)
            .map_err(|_| invalid("requested_at must be a timestamp with a timezone"))?;
        let deadline = DateTime::parse_from_rfc3339(cleanup_deadline_at)
            .map_err(|_| invalid("cleanup_deadline_at must be a timestamp with a timezone"))?;
        if deadline <= requested {
            return Err(invalid("cleanup deadline must follow the original request"));
        }
        Ok(Self {
            request_id: text(value, "request_id")?.to_owned(),
            requested_at: requested_at.to_owned(),
            cleanup_deadline_at: cleanup_deadline_at.to_owned(),
            history_refresh_page_token: Some(text(value, "history_refresh_page_token")?.to_owned()),
        })
    }
}

const REQUEST_EVENT: &str = "CooperativeCancellationRequested";
const DELIVERY_EVENT: &str = "CooperativeCancellationDelivered";
const MAX_SEQUENCE: u64 = i64::MAX as u64;

/// The authored durable call where canonical cancellation is delivered.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CancellationCallKind {
    Activity,
    LocalActivity,
    Timer,
    Condition,
    Signal,
    Child,
    Parallel,
    SelectionHandle,
}

/// One committed delivery marker, bound to the original cancellation request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CancellationDelivery {
    pub request_id: String,
    pub sequence: u64,
    pub call_kind: CancellationCallKind,
    pub sequence_span: u64,
    pub operation_sequence: Option<u64>,
    pub operation_sequence_span: u64,
}

fn positive(value: &Value, field: &str, maximum: u64) -> Result<u64> {
    value
        .as_u64()
        .filter(|number| (1..=maximum).contains(number))
        .ok_or_else(|| {
            invalid(format!(
                "{field} must be a positive integer within {maximum}"
            ))
        })
}

impl CancellationDelivery {
    pub(crate) fn from_payload(value: &Value) -> Result<Self> {
        let sequence = positive(&value["sequence"], "sequence", MAX_SEQUENCE)?;
        let call_kind: CancellationCallKind = serde_json::from_value(value["call_kind"].clone())
            .map_err(|_| invalid("delivery must name a supported durable call kind"))?;
        let span = positive(
            value.get("sequence_span").unwrap_or(&json!(1)),
            "sequence_span",
            1000,
        )?;
        let operation_span = positive(
            value.get("operation_sequence_span").unwrap_or(&json!(1)),
            "operation_sequence_span",
            1000,
        )?;
        let operation = value
            .get("operation_sequence")
            .filter(|value| !value.is_null());
        if (call_kind != CancellationCallKind::Parallel && span != 1)
            || sequence > MAX_SEQUENCE - span
        {
            return Err(invalid(
                "delivery call span is invalid or overflows the portable sequence range",
            ));
        }
        let operation_sequence = if call_kind == CancellationCallKind::SelectionHandle {
            let base = positive(
                operation.unwrap_or(&Value::Null),
                "operation_sequence",
                MAX_SEQUENCE,
            )?;
            if base >= sequence || operation_span > sequence - base {
                return Err(invalid(
                    "selection handle must name a complete earlier operation range",
                ));
            }
            Some(base)
        } else {
            if operation.is_some() || operation_span != 1 {
                return Err(invalid(
                    "only selection handles may name an operation range",
                ));
            }
            None
        };
        Ok(Self {
            request_id: text(value, "workflow_command_id")?.to_owned(),
            sequence,
            call_kind,
            sequence_span: span,
            operation_sequence,
            operation_sequence_span: operation_span,
        })
    }

    fn range(&self) -> (u64, u64) {
        self.operation_sequence
            .map_or((self.sequence, self.sequence_span), |base| {
                (base, self.operation_sequence_span)
            })
    }

    /// Whether a sequence belongs to the canonical interrupted operation range.
    pub fn interrupts(&self, sequence: u64) -> bool {
        let (base, span) = self.range();
        sequence >= base && sequence - base < span
    }
}

/// Validated canonical cancellation state for one workflow run.
///
/// An observation identifies a request. Only its committed delivery marker
/// authorizes interruption at an authored call. Earlier results retain priority.
#[derive(Clone, Debug)]
pub struct CancellationHistory {
    pub request: Option<CancellationRequest>,
    pub delivery: Option<CancellationDelivery>,
    pub request_index: usize,
    pub delivery_index: Option<usize>,
    resolved_before_request: BTreeSet<u64>,
    failed_before_request: BTreeSet<u64>,
    selected_before_request: BTreeSet<(u64, u64)>,
}

impl CancellationHistory {
    pub(super) fn bind_commands(
        &self,
        mut commands: Vec<RecordedCommand>,
    ) -> Result<Vec<RecordedCommand>> {
        let Some(delivery) = &self.delivery else {
            return Ok(commands);
        };
        if delivery.call_kind == CancellationCallKind::Parallel {
            let index = commands.partition_point(|command| command.sequence() < delivery.sequence);
            let end = commands.partition_point(|command| {
                command.sequence() < delivery.sequence + delivery.sequence_span
            });
            if commands.get(index).map(RecordedCommand::sequence) != Some(delivery.sequence) {
                let previous = index
                    .checked_sub(1)
                    .map_or(0, |index| commands[index].sequence());
                if previous.checked_add(1) != Some(delivery.sequence) {
                    return Err(invalid_recorded_history(
                        "cooperative_cancellation_call_mismatch",
                        delivery.sequence,
                        "next authored parallel group",
                        "missing earlier call",
                        "parallel cancellation marker skips an unrecorded authored command",
                    ));
                }
            }
            let original = commands.drain(index..end).collect();
            commands.insert(
                index,
                RecordedCommand::CancellationGroup {
                    sequence: delivery.sequence,
                    span: delivery.sequence_span,
                    original,
                },
            );
            return Ok(commands);
        }
        if !matches!(
            delivery.call_kind,
            CancellationCallKind::Activity
                | CancellationCallKind::Timer
                | CancellationCallKind::Condition
                | CancellationCallKind::Signal
                | CancellationCallKind::Child
                | CancellationCallKind::SelectionHandle
        ) {
            return Ok(commands);
        }
        let index = commands.partition_point(|command| command.sequence() < delivery.sequence);
        let original = if commands
            .get(index)
            .is_some_and(|command| command.sequence() == delivery.sequence)
        {
            Some(Box::new(commands.remove(index)))
        } else {
            let previous = index
                .checked_sub(1)
                .map_or(0, |index| commands[index].sequence());
            if previous.checked_add(1) != Some(delivery.sequence) {
                return Err(invalid_recorded_history(
                    "cooperative_cancellation_call_mismatch",
                    delivery.sequence,
                    "next authored durable call",
                    "missing earlier call",
                    "cancellation marker skips an unrecorded authored command",
                ));
            }
            None
        };
        commands.insert(
            index,
            RecordedCommand::CancellationBoundary {
                sequence: delivery.sequence,
                call_kind: delivery.call_kind,
                original,
            },
        );
        Ok(commands)
    }

    /// Read canonical markers without replacing original request identity.
    pub fn from_events(
        events: &[HistoryEvent],
        run_id: &str,
        observation: Option<&CancellationRequest>,
    ) -> Result<Self> {
        Self::read(events, run_id, observation).map_err(|error| {
            invalid_recorded_history(
                "cooperative_cancellation_history_invalid",
                0,
                "one canonical request and matching authored delivery",
                "invalid cancellation history",
                &error.to_string(),
            )
        })
    }

    fn read(
        events: &[HistoryEvent],
        run_id: &str,
        observation: Option<&CancellationRequest>,
    ) -> Result<Self> {
        if let Some(observed) = observation {
            let requested = DateTime::parse_from_rfc3339(&observed.requested_at)
                .map_err(|_| invalid("observed request timestamp is invalid"))?;
            let deadline = DateTime::parse_from_rfc3339(&observed.cleanup_deadline_at)
                .map_err(|_| invalid("observed cleanup deadline is invalid"))?;
            if observed.request_id.trim().is_empty()
                || deadline <= requested
                || observed
                    .history_refresh_page_token
                    .as_ref()
                    .is_some_and(|token| token.trim().is_empty())
            {
                return Err(invalid(
                    "observed request identity, deadline or history token is invalid",
                ));
            }
        }
        let mut state = Self {
            request: observation.cloned(),
            delivery: None,
            request_index: events.len(),
            delivery_index: None,
            resolved_before_request: BTreeSet::new(),
            failed_before_request: BTreeSet::new(),
            selected_before_request: BTreeSet::new(),
        };
        let mut saw_request = false;
        for (index, event) in events.iter().enumerate() {
            if !matches!(event.event_type.as_str(), REQUEST_EVENT | DELIVERY_EVENT) {
                continue;
            }
            let request_id = text(&event.payload, "workflow_command_id")?;
            let event_run = text(&event.payload, "workflow_run_id")?;
            if event
                .raw
                .get("workflow_command_id")
                .is_some_and(|value| value.as_str() != Some(request_id))
                || (!run_id.is_empty() && event_run != run_id)
            {
                return Err(invalid(
                    "canonical event does not match its request or workflow run",
                ));
            }
            if event.event_type == REQUEST_EVENT {
                if saw_request || state.delivery.is_some() {
                    return Err(invalid("history must contain one request before delivery"));
                }
                let recorded_at = event
                    .raw
                    .get("recorded_at")
                    .or_else(|| event.raw.get("timestamp"))
                    .and_then(Value::as_str)
                    .ok_or_else(|| invalid("canonical request lacks its recorded timestamp"))?;
                let requested = DateTime::parse_from_rfc3339(recorded_at)
                    .map_err(|_| invalid("canonical request timestamp is invalid"))?;
                let deadline_text = text(&event.payload, "cleanup_deadline_at")?;
                let deadline = DateTime::parse_from_rfc3339(deadline_text)
                    .map_err(|_| invalid("canonical cleanup deadline is invalid"))?;
                if deadline <= requested {
                    return Err(invalid(
                        "canonical cleanup deadline must follow the request",
                    ));
                }
                if let Some(observed) = observation {
                    let observed_deadline =
                        DateTime::parse_from_rfc3339(&observed.cleanup_deadline_at)
                            .map_err(|_| invalid("observed cleanup deadline is invalid"))?;
                    if observed.request_id != request_id || observed_deadline != deadline {
                        return Err(invalid(
                            "observation changes the original request or cleanup deadline",
                        ));
                    }
                } else {
                    state.request = Some(CancellationRequest {
                        request_id: request_id.to_owned(),
                        requested_at: recorded_at.to_owned(),
                        cleanup_deadline_at: deadline_text.to_owned(),
                        history_refresh_page_token: None,
                    });
                }
                state.request_index = index;
                saw_request = true;
            } else {
                if !saw_request || state.delivery.is_some() {
                    return Err(invalid(
                        "delivery requires one earlier request and one marker",
                    ));
                }
                let delivery = CancellationDelivery::from_payload(&event.payload)?;
                if state
                    .request
                    .as_ref()
                    .map(|request| request.request_id.as_str())
                    != Some(delivery.request_id.as_str())
                {
                    return Err(invalid("delivery names a different original request"));
                }
                state.delivery = Some(delivery);
                state.delivery_index = Some(index);
            }
        }
        if state.request.is_none() {
            return Ok(state);
        }
        for event in &events[..state.request_index] {
            if matches!(
                event.event_type.as_str(),
                "SelectionResolved" | "SelectionOperationCancelled"
            ) {
                let (base_field, span_field) = if event.event_type == "SelectionResolved" {
                    ("selection_group_base_sequence", "selection_group_size")
                } else {
                    ("member_base_sequence", "member_size")
                };
                if let (Ok(base), Ok(span)) = (
                    positive(&event.payload[base_field], base_field, MAX_SEQUENCE),
                    positive(&event.payload[span_field], span_field, 1000),
                ) {
                    if base <= MAX_SEQUENCE - span {
                        if event.event_type == "SelectionResolved" {
                            state.selected_before_request.insert((base, span));
                        } else {
                            state.resolved_before_request.extend(base..base + span);
                        }
                    }
                }
            }
            let Ok(sequence) = positive(&event.payload["sequence"], "sequence", MAX_SEQUENCE)
            else {
                continue;
            };
            if matches!(
                event.event_type.as_str(),
                "ActivityCompleted"
                    | "ActivityFailed"
                    | "ActivityCancelled"
                    | "ActivityTimedOut"
                    | "TimerFired"
                    | "TimerCancelled"
                    | "ConditionWaitSatisfied"
                    | "ConditionWaitTimedOut"
                    | "SignalApplied"
                    | "ChildRunCompleted"
                    | "ChildRunFailed"
                    | "ChildRunCancelled"
                    | "ChildRunTerminated"
            ) {
                state.resolved_before_request.insert(sequence);
                if matches!(
                    event.event_type.as_str(),
                    "ActivityFailed"
                        | "ActivityCancelled"
                        | "ActivityTimedOut"
                        | "ChildRunFailed"
                        | "ChildRunCancelled"
                        | "ChildRunTerminated"
                ) {
                    state.failed_before_request.insert(sequence);
                }
            }
        }
        if let Some(delivery) = &state.delivery {
            let (base, span) = delivery.range();
            if !state.range_eligible(base, span) {
                return Err(invalid(
                    "delivery cannot replace an earlier committed result",
                ));
            }
        }
        Ok(state)
    }

    fn range_eligible(&self, sequence: u64, span: u64) -> bool {
        (1..=1000).contains(&span)
            && sequence > 0
            && sequence <= MAX_SEQUENCE - span
            && !(sequence..sequence + span)
                .all(|sequence| self.resolved_before_request.contains(&sequence))
            && !(sequence..sequence + span)
                .any(|sequence| self.failed_before_request.contains(&sequence))
            && !self.selected_before_request.contains(&(sequence, span))
    }

    /// Whether an unresolved authored call may deliver the pending request.
    pub fn eligible(&self, sequence: u64, span: u64) -> bool {
        self.request.is_some() && self.delivery.is_none() && self.range_eligible(sequence, span)
    }
}

impl WorkflowState {
    fn next_cancellation_sequence(&self) -> Result<u64> {
        self.recorded_commands.get(self.command_cursor).map_or_else(
            || {
                self.recorded_commands
                    .last()
                    .map_or(0, RecordedCommand::sequence)
                    .checked_add(self.commands.len() as u64)
                    .and_then(|sequence| sequence.checked_add(1))
                    .filter(|sequence| *sequence < MAX_SEQUENCE)
                    .ok_or_else(|| invalid("authored cancellation sequence overflowed"))
            },
            |command| Ok(command.sequence()),
        )
    }

    fn prepare_cancellation_delivery(&mut self, delivery: CancellationDelivery) -> Result<bool> {
        if !self.cancellation_delivery_enabled || self.cancellation_shield_depth > 0 {
            return Ok(false);
        }
        if self.cancellation_delivery_intent.is_some() {
            self.matched_recorded_pending = true;
            return Ok(true);
        }
        let (base, span) = delivery.range();
        if !self.cancellation_history.eligible(base, span) {
            return Ok(false);
        }
        let payload = json!({
            "workflow_command_id":delivery.request_id, "sequence":delivery.sequence,
            "call_kind":delivery.call_kind, "sequence_span":delivery.sequence_span,
            "operation_sequence":delivery.operation_sequence,
            "operation_sequence_span":delivery.operation_sequence_span,
        });
        CancellationDelivery::from_payload(&payload)?;
        self.cancellation_delivery_command_count = self.commands.len();
        self.cancellation_delivery_intent = Some(delivery);
        self.matched_recorded_pending = true;
        Ok(true)
    }

    pub(super) fn prepare_scalar_cancellation(
        &mut self,
        index: usize,
        kind: CancellationCallKind,
        group_path: &[ParallelGroupMetadata],
    ) -> Result<bool> {
        if !self.cancellation_delivery_enabled || self.cancellation_shield_depth > 0 {
            return Ok(false);
        }
        // Group leaves validate their recorded calls before the enclosing
        // parallel/select future suspends. They never choose scalar delivery.
        if !group_path.is_empty() && self.cancellation_delivery_intent.is_none() {
            return Ok(false);
        }
        let Some(request) = self.cancellation_history.request.as_ref() else {
            return Ok(false);
        };
        let sequence = self.recorded_commands.get(index).map_or_else(
            || self.next_cancellation_sequence(),
            |command| Ok(command.sequence()),
        )?;
        let pending = self.prepare_cancellation_delivery(CancellationDelivery {
            request_id: request.request_id.clone(),
            sequence,
            call_kind: kind,
            sequence_span: 1,
            operation_sequence: None,
            operation_sequence_span: 1,
        })?;
        if pending && index < self.recorded_commands.len() {
            self.command_cursor = index + 1;
        }
        Ok(pending)
    }

    pub(super) fn prepare_group_cancellation(
        &mut self,
        descriptors: &[ParallelDescriptor],
    ) -> Result<()> {
        let Some(request) = self.cancellation_history.request.as_ref() else {
            return Ok(());
        };
        let Some(group) = descriptors.first().and_then(|leaf| leaf.group_path.first()) else {
            return Ok(());
        };
        self.prepare_cancellation_delivery(CancellationDelivery {
            request_id: request.request_id.clone(),
            sequence: group.parallel_group_base_sequence,
            call_kind: CancellationCallKind::Parallel,
            sequence_span: descriptors.len() as u64,
            operation_sequence: None,
            operation_sequence_span: 1,
        })?;
        Ok(())
    }

    pub(super) fn prepare_selection_handle_cancellation(
        &mut self,
        handle: &DurableOperationHandle,
    ) -> Result<bool> {
        if !self.cancellation_delivery_enabled || self.cancellation_shield_depth > 0 {
            return Ok(false);
        }
        validate_selection_delivery_handle(self, handle)?;
        if !self
            .cancellation_history
            .eligible(handle.base_sequence, handle.size as u64)
        {
            return Ok(false);
        }
        let request_id = self
            .cancellation_history
            .request
            .as_ref()
            .expect("eligible request")
            .request_id
            .clone();
        self.prepare_cancellation_delivery(CancellationDelivery {
            request_id,
            sequence: self.next_cancellation_sequence()?,
            call_kind: CancellationCallKind::SelectionHandle,
            sequence_span: 1,
            operation_sequence: Some(handle.base_sequence),
            operation_sequence_span: handle.size as u64,
        })
    }

    pub(super) fn expand_cancellation_group(
        &mut self,
        descriptors: &[ParallelDescriptor],
    ) -> Result<()> {
        let Some(RecordedCommand::CancellationGroup {
            sequence,
            span,
            original,
        }) = self.recorded_commands.get(self.command_cursor).cloned()
        else {
            return Ok(());
        };
        self.validate_cancellation_call(
            sequence,
            CancellationCallKind::Parallel,
            CancellationCallKind::Parallel,
        )?;
        if descriptors.len() as u64 != span {
            return Err(invalid_recorded_history(
                "cooperative_cancellation_call_mismatch",
                sequence,
                &format!("parallel group with {span} durable leaves"),
                &format!("{} durable leaves", descriptors.len()),
                "authored parallel group span differs from its committed cancellation",
            ));
        }
        let mut original = original.into_iter().peekable();
        let mut leaves = Vec::with_capacity(descriptors.len());
        for descriptor in descriptors {
            let leaf_sequence = sequence + descriptor.offset as u64;
            let command = if original.peek().map(RecordedCommand::sequence) == Some(leaf_sequence) {
                original.next()
            } else {
                None
            };
            if self
                .cancellation_history
                .resolved_before_request
                .contains(&leaf_sequence)
            {
                let command = command.ok_or_else(|| {
                    invalid_recorded_history(
                        "cooperative_cancellation_call_mismatch",
                        leaf_sequence,
                        "recorded earlier result",
                        "missing durable call",
                        "parallel cancellation cannot discard an earlier committed result",
                    )
                })?;
                leaves.push(command);
                continue;
            }
            let kind = match &descriptor.operation {
                ParallelOperation::Activity { .. } => CancellationCallKind::Activity,
                ParallelOperation::ChildWorkflow { .. } => CancellationCallKind::Child,
                ParallelOperation::Timer(_) => CancellationCallKind::Timer,
                ParallelOperation::Signal(_) => CancellationCallKind::Signal,
                ParallelOperation::Condition { .. } => CancellationCallKind::Condition,
                ParallelOperation::Group(_) => unreachable!("descriptor is a durable leaf"),
            };
            leaves.push(RecordedCommand::CancellationBoundary {
                sequence: leaf_sequence,
                call_kind: kind,
                original: command.map(Box::new),
            });
        }
        self.recorded_commands
            .splice(self.command_cursor..=self.command_cursor, leaves);
        Ok(())
    }

    pub(super) fn cancellation_error(&self) -> Error {
        match (
            &self.cancellation_history.request,
            &self.cancellation_history.delivery,
        ) {
            (Some(request), Some(delivery)) => {
                Error::CooperativeCancellationRequested(CooperativeCancellationRequested {
                    request: request.clone(),
                    delivery: delivery.clone(),
                })
            }
            _ => Error::WorkflowCancellationRequested(WorkflowCancellationRequested),
        }
    }

    fn validate_cancellation_call(
        &self,
        sequence: u64,
        kind: CancellationCallKind,
        recorded_kind: CancellationCallKind,
    ) -> Result<()> {
        if kind != recorded_kind || self.cancellation_shield_depth > 0 {
            return Err(invalid_recorded_history(
                "cooperative_cancellation_call_mismatch",
                sequence,
                "matching unshielded authored call",
                &format!("{kind:?}"),
                "committed cancellation call kind or cleanup scope changed",
            ));
        }
        Ok(())
    }

    pub(super) fn cancellation_replay_command(
        &mut self,
        index: usize,
        kind: CancellationCallKind,
    ) -> Result<Option<RecordedCommand>> {
        match self.recorded_commands.get(index).cloned() {
            Some(RecordedCommand::CancellationBoundary {
                sequence,
                call_kind,
                original,
            }) => {
                if let Some(original) = original {
                    return Ok(Some(*original));
                }
                self.validate_cancellation_call(sequence, kind, call_kind)?;
                self.cancellation_consumed = true;
                self.cancel_requested = true;
                self.command_cursor = index + 1;
                Err(self.cancellation_error())
            }
            command => Ok(command),
        }
    }

    pub(super) fn replay_cancellation_at(
        &mut self,
        index: usize,
        kind: CancellationCallKind,
    ) -> Result<()> {
        if let Some(RecordedCommand::CancellationBoundary {
            sequence,
            call_kind,
            ..
        }) = self.recorded_commands.get(index)
        {
            self.validate_cancellation_call(*sequence, kind, *call_kind)?;
            self.cancellation_consumed = true;
            self.cancel_requested = true;
            self.command_cursor = index + 1;
            return Err(self.cancellation_error());
        }
        Ok(())
    }

    pub(super) fn replay_selection_handle_cancellation(
        &mut self,
        handle: &DurableOperationHandle,
    ) -> Result<()> {
        let Some(RecordedCommand::CancellationBoundary {
            sequence,
            call_kind,
            original,
        }) = self.recorded_commands.get(self.command_cursor)
        else {
            return Ok(());
        };
        self.validate_cancellation_call(
            *sequence,
            CancellationCallKind::SelectionHandle,
            *call_kind,
        )?;
        let delivery = self
            .cancellation_history
            .delivery
            .as_ref()
            .expect("bound canonical delivery");
        if original.is_some()
            || delivery.operation_sequence != Some(handle.base_sequence)
            || delivery.operation_sequence_span != handle.size as u64
        {
            return Err(invalid_recorded_history(
                "cooperative_cancellation_call_mismatch",
                *sequence,
                "selection handle for the committed operation range",
                &format!("{}:{}", handle.base_sequence, handle.size),
                "cancellation delivery targets a different authored selection member",
            ));
        }
        validate_selection_delivery_handle(self, handle)?;
        self.replay_cancellation_at(self.command_cursor, CancellationCallKind::SelectionHandle)
    }
}

impl WorkflowContext {
    /// Shield explicit cleanup from repeated cancellation checks.
    ///
    /// Keep the returned guard alive across cleanup awaits. Scopes can nest.
    /// The Server retains and enforces the original immutable cleanup deadline.
    pub fn cancellation_shield(&self) -> Result<CancellationShield> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::WorkflowStatePoisoned)?;
        state.cancellation_shield_depth = state
            .cancellation_shield_depth
            .checked_add(1)
            .ok_or_else(|| invalid("cancellation shield nesting overflowed"))?;
        Ok(CancellationShield {
            state: Arc::clone(&self.state),
        })
    }
}

fn supports_protocol(version: &str) -> bool {
    let Some((major, minor)) = version.split_once('.') else {
        return false;
    };
    major == "1"
        && !minor.is_empty()
        && minor.bytes().all(|byte| byte.is_ascii_digit())
        && minor.parse::<u64>().is_ok_and(|minor| minor >= 20)
}

fn require_discovery(info: &Value) -> Result<()> {
    let protocol = &info["worker_protocol"];
    if protocol["server_capabilities"]["cooperative_cancellation"].as_bool() != Some(true)
        || !protocol["version"].as_str().is_some_and(supports_protocol)
    {
        return Err(Error::CooperativeCancellationUnavailable(
            "runtime discovery must explicitly advertise support and compatible protocol 1.20"
                .to_string(),
        ));
    }
    Ok(())
}

fn acknowledgment(
    value: &Value,
    workflow_id: &str,
    selected_run: Option<&str>,
) -> Result<WorkflowCancellationRequest> {
    if value["accepted"].as_bool() != Some(true)
        || text(value, "workflow_id")? != workflow_id
        || selected_run.is_some_and(|run| value["run_id"].as_str() != Some(run))
    {
        return Err(invalid(
            "acknowledgment does not match the accepted workflow/run request",
        ));
    }
    Ok(WorkflowCancellationRequest {
        workflow_id: workflow_id.to_owned(),
        run_id: text(value, "run_id")?.to_owned(),
        duplicate: value["duplicate"]
            .as_bool()
            .ok_or_else(|| invalid("duplicate must be a boolean"))?,
        cancellation_request: CancellationRequest::from_observation(
            &value["cancellation_request"],
        )?,
    })
}

impl Client {
    /// Observe an exact activity attempt without renewing its lease or progress.
    ///
    /// This explicit worker-protocol 1.20 operation has one five-second budget.
    /// A pending workflow request alone does not cancel an activity. Its reply
    /// reports the runtime's current continuation authority after delivery.
    pub async fn activity_task_status(
        &self,
        task_id: &str,
        activity_attempt_id: &str,
        lease_owner: &str,
    ) -> Result<Value> {
        if [task_id, activity_attempt_id, lease_owner]
            .iter()
            .any(|value| value.trim().is_empty())
        {
            return Err(invalid("activity observation requires the actual claim"));
        }
        tokio::time::timeout(CONTROL_BUDGET, async {
            let value: Value = activity_task_response(
                self.request_json(
                    reqwest::Method::POST,
                    &format!(
                        "/worker/activity-tasks/{}/status",
                        percent_encode_path_segment(task_id)
                    ),
                    RequestProtocol::Worker("1.20"),
                    Some(&json!({
                        "activity_attempt_id":activity_attempt_id,"lease_owner":lease_owner
                    })),
                )
                .await,
                "status",
                task_id,
                activity_attempt_id,
            )?;
            if value["task_id"].as_str() != Some(task_id)
                || value["activity_attempt_id"].as_str() != Some(activity_attempt_id)
                || value["lease_owner"].as_str() != Some(lease_owner)
                || value["can_continue"].as_bool().is_none()
                || value["cancel_requested"].as_bool().is_none()
                || value["heartbeat_recorded"].as_bool() != Some(false)
            {
                return Err(invalid(
                    "activity observation did not acknowledge the exact claim",
                ));
            }
            Ok(value)
        })
        .await
        .map_err(|_| Error::Timeout)?
    }

    /// Acquire a claim with protocol 1.20 and retain its pending observation.
    ///
    /// The Server must have admitted a capable worker registration. This method
    /// registers no capability and does not change ordinary [`Worker`] polling.
    /// It never substitutes a worker ID for a missing lease owner or invents a
    /// task attempt. Poll retries reuse one request ID. Poll and history loading
    /// share one long-poll budget plus five seconds, with at most 128 pages.
    pub async fn poll_cooperative_workflow_task(
        &self,
        worker_id: &str,
        task_queue: &str,
        timeout: Duration,
    ) -> Result<CooperativeWorkflowTaskPoll> {
        self.poll_cooperative_workflow_task_with_request_id(
            worker_id,
            task_queue,
            timeout,
            &unique_request_id("rust-workflow-poll"),
            1,
        )
        .await
    }

    async fn poll_cooperative_workflow_task_with_request_id(
        &self,
        worker_id: &str,
        task_queue: &str,
        timeout: Duration,
        poll_request_id: &str,
        transport_retries: usize,
    ) -> Result<CooperativeWorkflowTaskPoll> {
        if worker_id.trim().is_empty()
            || task_queue.trim().is_empty()
            || poll_request_id.trim().is_empty()
        {
            return Err(invalid(
                "cooperative polling requires a worker and task queue",
            ));
        }
        let budget = timeout
            .checked_add(CONTROL_BUDGET)
            .ok_or_else(|| invalid("cooperative poll timeout exceeds its supported budget"))?;
        let body = json!({
            "worker_id":worker_id, "task_queue":task_queue,
            "poll_request_id":poll_request_id,
            "timeout_seconds":long_poll_timeout_seconds(timeout),
            "history_page_size":WORKFLOW_HISTORY_PAGE_SIZE,
        });
        tokio::time::timeout(budget, async {
            let value: Value = self
                .poll_request_json(
                    "/worker/workflow-tasks/poll",
                    RequestProtocol::Worker("1.20"),
                    &body,
                    budget,
                    transport_retries,
                )
                .await?;
            let mut response: PollWorkflowTaskResponse = serde_json::from_value(value.clone())
                .map_err(|_| invalid("cooperative poll returned a malformed envelope or task"))?;
            let outcome = response.outcome();
            let task = if let Some(task) = response.task.take() {
                let (owner, _) = cancellation_claim(&task)?;
                if owner != worker_id
                    || value["task"]["workflow_task_attempt"].as_u64()
                        != Some(task.workflow_task_attempt)
                {
                    return Err(invalid(
                        "cooperative poll did not return the caller's actual owner and attempt",
                    ));
                }
                let cancellation_request = match value["task"].get("cancellation_request") {
                    None | Some(Value::Null) => None,
                    Some(observation) => Some(CancellationRequest::from_observation(observation)?),
                };
                let mut claim = CooperativeWorkflowTask {
                    task,
                    cancellation_request,
                };
                self.load_cooperative_claim_history(&mut claim).await?;
                Some(claim)
            } else {
                None
            };
            Ok(CooperativeWorkflowTaskPoll {
                task,
                outcome,
                protocol_version: response.protocol_version,
                server_capabilities: response.server_capabilities,
            })
        })
        .await
        .map_err(|_| Error::Timeout)?
    }

    async fn load_cooperative_claim_history(
        &self,
        claim: &mut CooperativeWorkflowTask,
    ) -> Result<()> {
        let (owner, _) = cancellation_claim(&claim.task)?;
        let owner = owner.to_owned();
        let mut token = claim.task.next_history_page_token.clone();
        let mut seen = BTreeSet::new();
        while let Some(current) = token.take() {
            if current.trim().is_empty()
                || seen.len() >= MAX_REFRESH_PAGES
                || !seen.insert(current.clone())
            {
                return Err(invalid(
                    "claim history exceeded its page bound or returned an invalid/repeated token",
                ));
            }
            let body = json!({
                "lease_owner":owner,"workflow_task_attempt":claim.task.workflow_task_attempt,
                "next_history_page_token":current,"history_page_size":WORKFLOW_HISTORY_PAGE_SIZE,
            });
            let value: Value = self
                .request_json(
                    reqwest::Method::POST,
                    &format!(
                        "/worker/workflow-tasks/{}/history",
                        percent_encode_path_segment(&claim.task.task_id)
                    ),
                    RequestProtocol::Worker("1.20"),
                    Some(&body),
                )
                .await?;
            if value["task_id"].as_str() != Some(claim.task.task_id.as_str())
                || value["workflow_task_attempt"].as_u64() != Some(claim.task.workflow_task_attempt)
            {
                return Err(invalid(
                    "claim history changed the selected task or attempt",
                ));
            }
            let events = value["history_events"]
                .as_array()
                .ok_or_else(|| invalid("claim history page must contain an event array"))?;
            if events.len() > WORKFLOW_HISTORY_PAGE_SIZE as usize {
                return Err(invalid(
                    "claim history page exceeds its requested event limit",
                ));
            }
            token = match value.get("next_history_page_token") {
                Some(Value::Null) => None,
                Some(Value::String(next)) if !next.trim().is_empty() && !events.is_empty() => {
                    Some(next.clone())
                }
                _ => return Err(invalid("claim history page token or progress is invalid")),
            };
            let page: WorkflowTaskHistoryPage = serde_json::from_value(value)
                .map_err(|_| invalid("claim history page contains malformed fields or events"))?;
            if page
                .history_events
                .iter()
                .any(|event| event.event_type.trim().is_empty())
            {
                return Err(invalid("claim history event type must be non-empty"));
            }
            claim.task.append_history_page(page);
        }
        Ok(())
    }

    /// Renew the exact selected workflow-task lease using worker protocol 1.20.
    ///
    /// The reply must acknowledge the actual task, owner and attempt. Renewal
    /// neither commits cancellation delivery nor extends its cleanup deadline.
    pub async fn heartbeat_workflow_task(
        &self,
        task: &WorkflowTask,
        original: Option<&CancellationRequest>,
    ) -> Result<WorkflowTaskHeartbeat> {
        let (owner, _) = cancellation_claim(task)?;
        if let Some(original) = original {
            original.validate_observation()?;
        }
        tokio::time::timeout(CONTROL_BUDGET, async {
            let response: Value = self.request_json(
                reqwest::Method::POST,
                &format!("/worker/workflow-tasks/{}/heartbeat", percent_encode_path_segment(&task.task_id)),
                RequestProtocol::Worker("1.20"),
                Some(&json!({"lease_owner":owner,"workflow_task_attempt":task.workflow_task_attempt})),
            ).await?;
            if response["task_id"].as_str() != Some(task.task_id.as_str())
                || response["workflow_task_attempt"].as_u64() != Some(task.workflow_task_attempt)
                || response["lease_owner"].as_str() != Some(owner)
                || response["renewed"].as_bool() != Some(true)
                || response.get("reason") != Some(&Value::Null)
                || response["task_status"].as_str() != Some("leased")
            {
                return Err(invalid("workflow heartbeat did not acknowledge the exact leased claim"));
            }
            let expires = text(&response, "lease_expires_at")?;
            DateTime::parse_from_rfc3339(expires).map_err(|_| invalid("workflow heartbeat lease expiry must include a timezone"))?;
            let run_status = text(&response, "run_status")?;
            if !matches!(run_status, "pending" | "running" | "waiting") {
                return Err(invalid("workflow heartbeat did not acknowledge an active run"));
            }
            let cancellation_request = match response.get("cancellation_request") {
                None | Some(Value::Null) if original.is_none() => None,
                None | Some(Value::Null) => return Err(invalid("workflow heartbeat omitted its original pending request")),
                Some(observation) => {
                    let current = CancellationRequest::from_observation(observation)?;
                    Some(match original {
                        Some(original) => original.preserve_observation(&current)?,
                        None => current,
                    })
                },
            };
            Ok(WorkflowTaskHeartbeat {
                task_id:task.task_id.clone(), workflow_task_attempt:task.workflow_task_attempt,
                lease_owner:owner.to_owned(), lease_expires_at:expires.to_owned(),
                run_status:run_status.to_owned(), cancellation_request,
            })
        }).await.map_err(|_| Error::Timeout)?
    }

    /// Commit cooperative delivery on the exact selected workflow-task claim.
    ///
    /// This explicit worker-protocol 1.20 operation renews no lease and advertises
    /// no worker capability. The Server requires a previously admitted capable
    /// claim. Reload canonical history before exposing cancellation to workflow
    /// code, including when an acknowledgment is lost.
    pub async fn deliver_workflow_cancellation(
        &self,
        task: &WorkflowTask,
        delivery: &CancellationDelivery,
    ) -> Result<CancellationDeliveryReceipt> {
        let (owner, run_id) = cancellation_claim(task)?;
        let body = json!({
            "lease_owner":owner, "workflow_task_attempt":task.workflow_task_attempt,
            "request_id":delivery.request_id, "sequence":delivery.sequence,
            "call_kind":delivery.call_kind, "sequence_span":delivery.sequence_span,
            "operation_sequence":delivery.operation_sequence,
            "operation_sequence_span":delivery.operation_sequence_span
        });
        let mut payload = body.clone();
        payload["workflow_command_id"] = json!(delivery.request_id);
        if CancellationDelivery::from_payload(&payload)? != *delivery {
            return Err(invalid(
                "delivery must name a valid authored operation range",
            ));
        }
        tokio::time::timeout(CONTROL_BUDGET, async {
            let path = format!(
                "/worker/workflow-tasks/{}/deliver-cancellation",
                percent_encode_path_segment(&task.task_id)
            );
            let response: Value = self
                .request_json(
                    reqwest::Method::POST,
                    &path,
                    RequestProtocol::Worker("1.20"),
                    Some(&body),
                )
                .await?;
            if response["delivered"].as_bool() != Some(true)
                || response["task_id"].as_str() != Some(task.task_id.as_str())
                || response["workflow_run_id"].as_str() != Some(run_id)
                || response.get("reason") != Some(&Value::Null)
                || [
                    "sequence_span",
                    "operation_sequence",
                    "operation_sequence_span",
                ]
                .iter()
                .any(|field| response.get(*field).is_none())
            {
                return Err(invalid(
                    "delivery acknowledgment does not match the selected task/run",
                ));
            }
            let mut recorded = response.clone();
            recorded["workflow_command_id"] = response["request_id"].clone();
            if CancellationDelivery::from_payload(&recorded)? != *delivery {
                return Err(invalid(
                    "delivery acknowledgment changed its authored operation range",
                ));
            }
            Ok(CancellationDeliveryReceipt {
                task_id: task.task_id.clone(),
                run_id: run_id.to_owned(),
                delivery: delivery.clone(),
            })
        })
        .await
        .map_err(|_| Error::Timeout)?
    }

    /// Reload canonical history with the Server-issued token and exact claim.
    ///
    /// The refresh has one five-second budget, at most 128 pages, and the existing
    /// SDK page-size limit. Invalid, repeated or non-progressing tokens and pages
    /// fail closed. This returns fresh history without modifying the caller's
    /// previous task snapshot or its original request identity.
    pub async fn refresh_workflow_cancellation_history(
        &self,
        task: &WorkflowTask,
        observation: &CancellationRequest,
    ) -> Result<Vec<HistoryEvent>> {
        let (owner, run_id) = cancellation_claim(task)?;
        observation.validate_observation()?;
        let first_token = observation
            .history_refresh_page_token
            .clone()
            .expect("validated history token");
        tokio::time::timeout(CONTROL_BUDGET, async {
            let path = format!("/worker/workflow-tasks/{}/history", percent_encode_path_segment(&task.task_id));
            let mut token = Some(first_token);
            let mut seen = BTreeSet::new();
            let mut history = Vec::new();
            while let Some(current) = token.take() {
                if seen.len() >= MAX_REFRESH_PAGES || !seen.insert(current.clone()) {
                    return Err(invalid("canonical history refresh exceeded its page bound or repeated a token"));
                }
                let body = json!({
                    "lease_owner":owner, "workflow_task_attempt":task.workflow_task_attempt,
                    "next_history_page_token":current, "history_page_size":WORKFLOW_HISTORY_PAGE_SIZE,
                });
                let page: Value = self.request_json(
                    reqwest::Method::POST, &path, RequestProtocol::Worker("1.20"), Some(&body),
                ).await?;
                if page["task_id"].as_str() != Some(task.task_id.as_str())
                    || page["workflow_task_attempt"].as_u64() != Some(task.workflow_task_attempt)
                {
                    return Err(invalid("canonical history page changed the selected task/attempt"));
                }
                let events = page["history_events"].as_array().ok_or_else(|| invalid("canonical history page must contain an event array"))?;
                if events.len() > WORKFLOW_HISTORY_PAGE_SIZE as usize {
                    return Err(invalid("canonical history page exceeds its requested event limit"));
                }
                token = match page.get("next_history_page_token") {
                    Some(Value::Null) => None,
                    Some(Value::String(next)) if !next.is_empty() && !events.is_empty() => Some(next.clone()),
                    _ => return Err(invalid("canonical history page token or progress is invalid")),
                };
                for event in events {
                    let event: HistoryEvent = serde_json::from_value(event.clone()).map_err(|_| invalid("canonical history page contains a malformed event"))?;
                    if event.event_type.trim().is_empty() {
                        return Err(invalid("canonical history event type must be non-empty"));
                    }
                    history.push(event);
                }
            }
            let canonical = CancellationHistory::from_events(&history, run_id, Some(observation))?;
            if canonical.request_index >= history.len() {
                return Err(invalid("canonical refresh omitted the original request event"));
            }
            Ok(history)
        }).await.map_err(|_| Error::Timeout)?
    }

    /// Request bounded workflow-authored cleanup on a capable runtime.
    ///
    /// This is separate from terminal cancellation. Discovery must advertise
    /// cooperative support and compatible protocol 1.20. The Server also
    /// refuses an active workflow claim that cannot deliver cooperation.
    /// Repeated requests retain its original request identity and deadline.
    pub async fn request_workflow_cancellation(
        &self,
        workflow_id: &str,
        options: CooperativeCancellationOptions,
    ) -> Result<WorkflowCancellationRequest> {
        self.request_workflow_cancellation_target(workflow_id, None, options)
            .await
    }

    /// Request cleanup only if the selected run remains current.
    pub async fn request_workflow_run_cancellation(
        &self,
        workflow_id: &str,
        run_id: &str,
        options: CooperativeCancellationOptions,
    ) -> Result<WorkflowCancellationRequest> {
        self.request_workflow_cancellation_target(workflow_id, Some(run_id), options)
            .await
    }

    async fn request_workflow_cancellation_target(
        &self,
        workflow_id: &str,
        run_id: Option<&str>,
        options: CooperativeCancellationOptions,
    ) -> Result<WorkflowCancellationRequest> {
        if workflow_id.trim().is_empty() || run_id.is_some_and(|id| id.trim().is_empty()) {
            return Err(invalid(
                "workflow and selected run identities must be non-empty",
            ));
        }
        if options
            .cleanup_timeout_seconds
            .is_some_and(|seconds| !(1..=3600).contains(&seconds))
            || options
                .reason
                .as_ref()
                .is_some_and(|reason| reason.chars().count() > 1000)
        {
            return Err(invalid(
                "cleanup timeout must be 1..3600 seconds and reason at most 1000 characters",
            ));
        }

        // Bound discovery, mutation and any storage retry by one total budget.
        tokio::time::timeout(CONTROL_BUDGET, async {
            let info: Value = self
                .request_json(
                    reqwest::Method::GET,
                    "/cluster/info",
                    RequestProtocol::ControlPlane,
                    Option::<&Value>::None,
                )
                .await?;
            require_discovery(&info)?;
            let mut path = format!("/workflows/{}", percent_encode_path_segment(workflow_id));
            if let Some(run_id) = run_id {
                path.push_str(&format!("/runs/{}", percent_encode_path_segment(run_id)));
            }
            path.push_str("/request-cancellation");
            let response: Value = self
                .request_json(
                    reqwest::Method::POST,
                    &path,
                    RequestProtocol::ControlPlane,
                    Some(&options),
                )
                .await?;
            acknowledgment(&response, workflow_id, run_id)
        })
        .await
        .map_err(|_| Error::Timeout)?
    }
}

impl Worker {
    pub(super) async fn poll_cooperative_activity_once(&self) -> Result<ManagedPollOutcome> {
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
        let guard = ActivityClaimGuard::new(&self.client, &task, &self.worker_id)?;
        let _abandon_on_drop = AbandonActivityOnDrop(guard.clone());
        if guard.observe().await.is_err() {
            return Ok(ManagedPollOutcome::Handled);
        }
        let invocation = self.execute_cooperative_activity_task(&task, &guard);
        tokio::pin!(invocation);
        let result = loop {
            tokio::select! {
                biased;
                _ = guard.wait_for_shutdown() => {
                    guard.abandon();
                    return Ok(ManagedPollOutcome::Handled);
                }
                result = &mut invocation => break result,
                _ = tokio::time::sleep(Duration::from_secs(1)) => {
                    if guard.observe().await.is_err() {
                        return Ok(ManagedPollOutcome::Handled);
                    }
                }
            }
        };
        // Both genuine application failures and successful results need current
        // authority before result upload or completion/failure publication.
        if matches!(result, Err(Error::ActivityExecutionAbandoned(_)))
            || guard.observe().await.is_err()
        {
            return Ok(ManagedPollOutcome::Handled);
        }
        let settlement = match result {
            Ok(value) => {
                self.client
                    .complete_activity_task(
                        &guard.task_id,
                        &guard.attempt_id,
                        &guard.owner,
                        value,
                        &task.payload_codec,
                    )
                    .await
            }
            Err(error) if worker_storage_admission_body(&error).is_some() => return Err(error),
            Err(error) => {
                self.client
                    .fail_activity_task(
                        &guard.task_id,
                        &guard.attempt_id,
                        &guard.owner,
                        error.to_string(),
                        false,
                    )
                    .await
            }
        };
        if let Err(error) = settlement {
            if !activity_task_rejection_is_final(&error) {
                return Err(error);
            }
        }
        Ok(ManagedPollOutcome::Handled)
    }

    async fn execute_cooperative_activity_task(
        &self,
        task: &ActivityTask,
        guard: &ActivityClaimGuard,
    ) -> Result<AvroValue> {
        validate_activity_task_payloads(task)?;
        let handler = self
            .activities
            .get(&task.activity_type)
            .ok_or_else(|| Error::ActivityNotRegistered(task.activity_type.clone()))?;
        let args = decode_task_avro_arguments(task.arguments.as_ref(), &task.payload_codec)?;
        let context = ActivityContext {
            client: self.client.clone(),
            task_id: guard.task_id.clone(),
            activity_attempt_id: guard.attempt_id.clone(),
            lease_owner: guard.owner.clone(),
            activity_type: task.activity_type.clone(),
            attempt_number: task.attempt_number,
            task_queue: self.task_queue.clone(),
            worker_id: self.worker_id.clone(),
            claim_guard: Some(guard.clone()),
        };
        guard.boundary()?;
        handler(context, args).await
    }

    pub(super) async fn poll_cooperative_workflow_once(&self) -> Result<ManagedPollOutcome> {
        let poll_request_id = unique_request_id("rust-workflow-poll");
        let response = self
            .retry_worker_operation(|| {
                self.client.poll_cooperative_workflow_task_with_request_id(
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
        if response.outcome.should_stop() {
            return Ok(ManagedPollOutcome::Stop);
        }
        let memo_updates_supported =
            runtime_supports_workflow_memo_updates(response.server_capabilities.as_ref());
        let Some(claim) = response.task else {
            return Ok(ManagedPollOutcome::Idle);
        };
        // An uncertain observation, delivery or canonical refresh cannot become
        // an application failure or be published as a workflow-task decision.
        let decision = self.replay_cooperative_workflow_claim(&claim).await?;
        let (owner, _) = cancellation_claim(&claim.task)?;
        self.settle_workflow_task_decision(
            &claim.task.task_id,
            owner,
            claim.task.workflow_task_attempt,
            claim.task.run_id.as_deref(),
            Ok(decision),
            memo_updates_supported,
        )
        .await
    }

    async fn replay_cooperative_workflow_claim(
        &self,
        claim: &CooperativeWorkflowTask,
    ) -> Result<WorkflowTaskDecision> {
        let (owner, run_id) = cancellation_claim(&claim.task)?;
        if owner != self.worker_id {
            return Err(invalid(
                "cooperative replay requires this worker's actual claim",
            ));
        }
        let mut task = claim.task.clone();
        let Some(observation) = claim.cancellation_request.as_ref() else {
            let canonical = CancellationHistory::from_events(&task.history_events, run_id, None)?;
            if canonical.request.is_some() {
                return Err(invalid(
                    "cooperative replay omitted its original pending observation",
                ));
            }
            return self.execute_workflow_task_decision(task);
        };
        task.history_events = self
            .client
            .refresh_workflow_cancellation_history(&claim.task, observation)
            .await?;
        for _ in 0..3 {
            // Count the actual refreshed snapshot. Its byte budget was not
            // remeasured, so do not expose the old snapshot's size as current.
            task.total_history_events = None;
            task.history_size_bytes = None;
            let mut decision = self.execute_workflow_task_decision_with_cancellation(
                task.clone(),
                Some(observation),
            )?;
            let Some(intent) = decision.cancellation_delivery.as_ref() else {
                return Ok(decision);
            };
            if !decision.commands.is_empty() {
                // Native permits delivery only at/before its next durable call.
                // Commit earlier commands first. Completion releases this claim
                // and its successor replays their durable results before delivery.
                decision.cancellation_delivery = None;
                return Ok(decision);
            }
            let delivery_error = self
                .client
                .deliver_workflow_cancellation(&claim.task, intent)
                .await
                .err();
            task.history_events = self
                .client
                .refresh_workflow_cancellation_history(&claim.task, observation)
                .await?;
            let canonical =
                CancellationHistory::from_events(&task.history_events, run_id, Some(observation))?;
            if canonical.delivery.as_ref() != Some(intent) {
                return Err(delivery_error.unwrap_or_else(|| {
                    invalid(
                        "canonical history did not prove the exact intended cancellation delivery",
                    )
                }));
            }
        }
        Err(invalid(
            "workflow replay did not converge on its canonical cancellation delivery",
        ))
    }
}

#[derive(Clone, Debug)]
pub(super) struct ActivityClaimGuard {
    client: Client,
    task_id: String,
    attempt_id: String,
    owner: String,
    active: Arc<AtomicBool>,
    stop: Option<Arc<AtomicBool>>,
}

struct AbandonActivityOnDrop(ActivityClaimGuard);

impl Drop for AbandonActivityOnDrop {
    fn drop(&mut self) {
        self.0.abandon();
    }
}

impl ActivityClaimGuard {
    fn new(client: &Client, task: &ActivityTask, worker_id: &str) -> Result<Self> {
        let owner = task.lease_owner.as_deref().unwrap_or_default();
        let attempt = task
            .activity_attempt_id
            .as_deref()
            .or(task.attempt_id.as_deref())
            .unwrap_or_default();
        if task.task_id.trim().is_empty()
            || attempt.trim().is_empty()
            || owner.trim().is_empty()
            || owner != worker_id
            || matches!((&task.activity_attempt_id, &task.attempt_id), (Some(left), Some(right)) if left != right)
        {
            return Err(invalid(
                "activity execution requires the worker's actual immutable claim",
            ));
        }
        Ok(Self {
            client: client.clone(),
            task_id: task.task_id.clone(),
            attempt_id: attempt.to_owned(),
            owner: owner.to_owned(),
            active: Arc::new(AtomicBool::new(true)),
            stop: client
                .worker_storage_admission
                .as_ref()
                .map(|admission| Arc::clone(&admission.stop)),
        })
    }

    fn abandon(&self) {
        self.active.store(false, Ordering::SeqCst);
    }

    fn boundary(&self) -> Result<()> {
        if !self.active.load(Ordering::SeqCst)
            || self
                .stop
                .as_ref()
                .is_some_and(|stop| stop.load(Ordering::SeqCst))
        {
            self.abandon();
            return Err(Error::ActivityExecutionAbandoned(
                "callback completed, was abandoned, or its worker is stopping".into(),
            ));
        }
        Ok(())
    }

    async fn wait_for_shutdown(&self) {
        if let Some(stop) = &self.stop {
            wait_for_worker_stop(stop).await;
        } else {
            std::future::pending::<()>().await;
        }
    }

    async fn observe(&self) -> Result<()> {
        self.boundary()?;
        let result = self
            .client
            .activity_task_status(&self.task_id, &self.attempt_id, &self.owner)
            .await
            .and_then(|value| {
                if value["can_continue"].as_bool() != Some(true)
                    || value["cancel_requested"].as_bool() != Some(false)
                    || value.get("reason") != Some(&Value::Null)
                    || value["task_status"].as_str() != Some("leased")
                    || value["attempt_status"].as_str() != Some("running")
                    || value["activity_status"].as_str() != Some("running")
                {
                    return Err(invalid("activity observation refused continuation"));
                }
                let mut bounds = vec![text(&value, "lease_expires_at")?];
                if let Some(deadlines) = value.get("deadlines").filter(|v| !v.is_null()) {
                    if !deadlines.is_object() {
                        return Err(invalid("activity execution deadlines must be an object"));
                    }
                    for kind in ["heartbeat", "start_to_close", "schedule_to_close"] {
                        if let Some(deadline) = deadlines.get(kind).filter(|v| !v.is_null()) {
                            bounds.push(
                                deadline
                                    .as_str()
                                    .ok_or_else(|| invalid("invalid activity deadline"))?,
                            );
                        }
                    }
                }
                if let Some(session) = value.get("worker_session").filter(|v| !v.is_null()) {
                    if session["status"].as_str() != Some("active")
                        || session["lease_owner"].as_str() != Some(self.owner.as_str())
                    {
                        return Err(invalid(
                            "activity no longer owns its required worker session",
                        ));
                    }
                    bounds.push(text(session, "lease_expires_at")?);
                    bounds.push(text(session, "ttl_expires_at")?);
                }
                for bound in bounds {
                    let deadline = DateTime::parse_from_rfc3339(bound)
                        .map_err(|_| invalid("activity deadline must include a timezone"))?;
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map_err(|_| invalid("activity observation clock precedes the epoch"))?;
                    if !u128::try_from(deadline.timestamp_millis())
                        .is_ok_and(|millis| millis > now.as_millis())
                    {
                        return Err(invalid("activity ownership or execution deadline elapsed"));
                    }
                }
                self.boundary()
            });
        result.map_err(|error| {
            self.abandon();
            Error::ActivityExecutionAbandoned(error.to_string())
        })
    }

    pub(super) async fn heartbeat<T: Serialize>(
        &self,
        context: &ActivityContext,
        details: T,
    ) -> Result<ActivityHeartbeatResponse> {
        if context.task_id != self.task_id
            || context.activity_attempt_id != self.attempt_id
            || context.lease_owner != self.owner
            || context.worker_id != self.owner
        {
            self.abandon();
            return Err(Error::ActivityExecutionAbandoned(
                "activity context changed its original claim".into(),
            ));
        }
        self.observe().await?;
        let result = tokio::time::timeout(CONTROL_BUDGET, async {
            let details = encode_typed_envelope(&AvroValue::from_serialize(&details)?, DEFAULT_CODEC)?;
            self.boundary()?;
            let value: Value = self.client.request_json(
                reqwest::Method::POST,
                &format!("/worker/activity-tasks/{}/heartbeat", percent_encode_path_segment(&self.task_id)),
                RequestProtocol::Worker("1.20"),
                Some(&json!({"activity_attempt_id":self.attempt_id,"lease_owner":self.owner,"details":details})),
            ).await?;
            if value["task_id"].as_str() != Some(self.task_id.as_str())
                || value["activity_attempt_id"].as_str() != Some(self.attempt_id.as_str())
                || value["lease_owner"].as_str() != Some(self.owner.as_str())
                || value["can_continue"].as_bool() != Some(true)
                || value["cancel_requested"].as_bool() != Some(false)
                || value["heartbeat_recorded"].as_bool() != Some(true)
            {
                return Err(invalid("activity heartbeat lost its original claim"));
            }
            serde_json::from_value(value).map_err(Error::from)
        }).await.map_err(|_| Error::Timeout).and_then(|result| result);
        let response = result.map_err(|error| {
            self.abandon();
            Error::ActivityExecutionAbandoned(error.to_string())
        })?;
        self.observe().await?;
        Ok(response)
    }
}

pub(super) fn cancellation_claim(task: &WorkflowTask) -> Result<(&str, &str)> {
    let owner = task
        .lease_owner
        .as_deref()
        .filter(|owner| !owner.trim().is_empty());
    let run = task.run_id.as_deref().filter(|run| !run.trim().is_empty());
    if task.task_id.trim().is_empty()
        || task.workflow_task_attempt == 0
        || task.workflow_task_attempt > MAX_SEQUENCE
    {
        return Err(invalid(
            "cancellation transport requires a valid task and attempt",
        ));
    }
    Ok((
        owner.ok_or_else(|| invalid("cancellation transport requires the actual lease owner"))?,
        run.ok_or_else(|| invalid("cancellation transport requires the selected durable run"))?,
    ))
}

impl WorkflowHandle {
    /// Request bounded cleanup for whichever run is current.
    pub async fn request_cancellation(
        &self,
        options: CooperativeCancellationOptions,
    ) -> Result<WorkflowCancellationRequest> {
        self.client
            .request_workflow_cancellation(&self.workflow_id, options)
            .await
    }

    /// Request bounded cleanup only while this handle's selected run is current.
    pub async fn request_selected_run_cancellation(
        &self,
        options: CooperativeCancellationOptions,
    ) -> Result<WorkflowCancellationRequest> {
        let run_id = self
            .run_id
            .as_deref()
            .ok_or_else(|| invalid("selected run_id is required"))?;
        self.client
            .request_workflow_run_cancellation(&self.workflow_id, run_id, options)
            .await
    }
}
