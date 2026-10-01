use super::*;
use std::collections::BTreeSet;

const CONTROL_BUDGET: Duration = Duration::from_secs(5);

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
