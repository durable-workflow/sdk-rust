use super::*;

const OPEN_BUDGET: Duration = Duration::from_secs(5);
const MAX_SCOPE_SEQUENCE: u64 = i64::MAX as u64;
const MAX_HISTORY_PAGES: usize = 128;

fn invalid() -> Error {
    Error::InvalidCooperativeCancellation("invalid canonical cancellation scope opening".into())
}

fn identity(value: &str) -> bool {
    !value.trim().is_empty() && value.len() <= 255
}

fn text<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    value[field]
        .as_str()
        .filter(|value| identity(value))
        .ok_or_else(invalid)
}

#[derive(Clone, Debug)]
pub(super) struct CanonicalScopeOpening {
    pub scope_id: String,
    pub parent_scope_id: String,
    pub shield_parent: bool,
}

#[derive(Clone, Default, Debug)]
pub(super) struct CancellationScopeHistory {
    pub openings: BTreeMap<u64, CanonicalScopeOpening>,
    pub memberships: BTreeMap<u64, String>,
}

fn invalid_history(detail: &str) -> Error {
    invalid_recorded_history(
        "invalid_cancellation_scope_history",
        0,
        "canonical scope tree and original membership",
        "invalid history",
        detail,
    )
}

fn starts_with_workflow_start(events: &[HistoryEvent]) -> bool {
    events
        .first()
        .is_some_and(|event| event.event_type == "WorkflowStarted")
        || (events
            .first()
            .is_some_and(|event| event.event_type == "StartAccepted")
            && events
                .get(1)
                .is_some_and(|event| event.event_type == "WorkflowStarted"))
}

impl CancellationScopeHistory {
    pub fn read(events: &[HistoryEvent], run_id: &str) -> Result<Self> {
        let has_scopes = events
            .iter()
            .any(|event| event.event_type == "CancellationScopeOpened");
        if has_scopes && !starts_with_workflow_start(events) {
            return Err(invalid_history(
                "scope history lacks its original workflow start",
            ));
        }
        let mut history = Self::default();
        let mut event_ids = BTreeSet::new();
        let mut scopes = BTreeMap::new();
        let mut namespace: Option<&str> = None;
        let mut last_event_sequence = 0;
        let mut last_opening = 0;
        for event in events {
            if has_scopes {
                let event_id = event
                    .raw
                    .get("id")
                    .and_then(Value::as_str)
                    .filter(|value| identity(value))
                    .ok_or_else(|| invalid_history("missing canonical event identity"))?;
                let sequence = event
                    .raw
                    .get("sequence")
                    .and_then(Value::as_u64)
                    .filter(|value| *value > last_event_sequence && *value <= MAX_SCOPE_SEQUENCE)
                    .ok_or_else(|| invalid_history("canonical event order changed"))?;
                let incoming_namespace = event
                    .raw
                    .get("namespace")
                    .and_then(Value::as_str)
                    .filter(|value| identity(value))
                    .ok_or_else(|| invalid_history("missing canonical namespace"))?;
                if !event_ids.insert(event_id)
                    || namespace.is_some_and(|previous| previous != incoming_namespace)
                    || !event.payload.is_object()
                    || event.event_type.trim().is_empty()
                {
                    return Err(invalid_history(
                        "canonical event identity, namespace or payload changed",
                    ));
                }
                namespace = Some(incoming_namespace);
                last_event_sequence = sequence;
            }
            let payload = &event.payload;
            let sequence = payload["sequence"]
                .as_u64()
                .filter(|value| *value > 0 && *value <= MAX_SCOPE_SEQUENCE);
            if event.event_type == "CancellationScopeOpened" {
                let scope_id = text(payload, "scope_id")
                    .map_err(|_| invalid_history("invalid scope identity"))?;
                let parent = text(payload, "parent_scope_id")
                    .map_err(|_| invalid_history("invalid parent identity"))?;
                let sequence =
                    sequence.ok_or_else(|| invalid_history("invalid authored opening sequence"))?;
                let shield = payload["shield_parent"]
                    .as_bool()
                    .ok_or_else(|| invalid_history("invalid shielding"))?;
                if payload["schema"] != "durable-workflow.cancellation-scope/v1"
                    || run_id.is_empty()
                    || payload["workflow_run_id"].as_str() != Some(run_id)
                    || scope_id == "root"
                    || scopes.contains_key(scope_id)
                    || (parent != "root" && !scopes.contains_key(parent))
                    || sequence <= last_opening
                    || history.memberships.contains_key(&sequence)
                {
                    return Err(invalid_history("invalid canonical opening tree"));
                }
                scopes.insert(scope_id, sequence);
                last_opening = sequence;
                history.openings.insert(
                    sequence,
                    CanonicalScopeOpening {
                        scope_id: scope_id.into(),
                        parent_scope_id: parent.into(),
                        shield_parent: shield,
                    },
                );
                continue;
            }
            let admission = matches!(
                event.event_type.as_str(),
                "ActivityScheduled"
                    | "TimerScheduled"
                    | "ChildWorkflowScheduled"
                    | "ConditionWaitOpened"
                    | "SignalWaitOpened"
            );
            let operation = admission
                || matches!(
                    event.event_type.as_str(),
                    "ActivityStarted"
                        | "ActivityCompleted"
                        | "ActivityFailed"
                        | "ActivityTimedOut"
                        | "ActivityCancelled"
                        | "ActivityRetryScheduled"
                        | "TimerFired"
                        | "TimerCancelled"
                        | "ChildRunStarted"
                        | "ChildRunCompleted"
                        | "ChildRunFailed"
                        | "ChildRunCancelled"
                        | "ChildRunTerminated"
                        | "ConditionWaitSatisfied"
                        | "ConditionWaitTimedOut"
                        | "ConditionWaitCancelled"
                        | "SignalWaitReceived"
                        | "SignalWaitTimedOut"
                        | "SignalWaitCancelled"
                );
            if !operation {
                continue;
            }
            let mut membership: Option<&str> = None;
            for snapshot in std::iter::once(payload).chain(
                ["activity", "timer", "child_workflow"]
                    .iter()
                    .filter_map(|name| payload.get(name)),
            ) {
                let Some(value) = snapshot.get("cancellation_scope_id") else {
                    continue;
                };
                let incoming = value
                    .as_str()
                    .filter(|value| identity(value))
                    .ok_or_else(|| invalid_history("invalid operation scope membership"))?;
                if membership.is_some_and(|previous| previous != incoming) {
                    return Err(invalid_history("contradictory operation scope membership"));
                }
                membership = Some(incoming);
            }
            if membership.is_none() && !admission {
                continue;
            }
            let membership = membership.unwrap_or("root");
            if membership != "root"
                && !sequence.is_some_and(|sequence| {
                    scopes
                        .get(membership)
                        .is_some_and(|opening| *opening < sequence)
                })
            {
                return Err(invalid_history(
                    "operation scope was not opened before original admission",
                ));
            }
            let Some(sequence) = sequence else {
                continue;
            };
            if history.openings.contains_key(&sequence)
                || history
                    .memberships
                    .get(&sequence)
                    .is_some_and(|previous| previous != membership)
            {
                return Err(invalid_history(
                    "operation changed its original scope membership",
                ));
            }
            history.memberships.insert(sequence, membership.into());
        }
        Ok(history)
    }
}

#[derive(Clone, Debug)]
pub(super) struct CancellationScopeOpening {
    pub sequence: u64,
    pub parent_scope_id: String,
    pub shield_parent: bool,
    pub command_count: usize,
}

impl WorkflowContext {
    pub(super) fn validate_scope_membership(
        &self,
        state: &mut WorkflowState,
        cursor: usize,
    ) -> Result<()> {
        if !state.allow_cancellation_scope_authoring {
            return Ok(());
        }
        if let Some(replay) = &mut state.scope_delivery {
            replay.active_scope = self.cancellation_scope_id.clone();
        }
        if let Some(recorded) = state.recorded_commands.get(cursor) {
            let sequence = recorded.sequence();
            let original = state
                .cancellation_scope_memberships
                .get(&sequence)
                .map(String::as_str)
                .or_else(|| {
                    state
                        .scope_delivery
                        .as_ref()
                        .and_then(|replay| replay.canonical.deliveries.get(&sequence))
                        .map(|delivered| delivered.context.scope_id())
                })
                .unwrap_or("root");
            if original != self.cancellation_scope_id {
                return Err(invalid_recorded_history(
                    "cancellation_scope_membership_changed",
                    sequence,
                    original,
                    &self.cancellation_scope_id,
                    "operation changed the scope where it was created",
                ));
            }
        }
        Ok(())
    }

    pub(super) fn apply_scope_membership(&self, command: &mut serde_json::Map<String, Value>) {
        if self.cancellation_scope_id != "root" {
            command.insert(
                "cancellation_scope_id".into(),
                json!(self.cancellation_scope_id),
            );
        }
    }

    /// Await the original durable opening, then run a body with its own context.
    /// Operations created from that context retain their scope when awaited later.
    /// Candidate authoring remains disabled on ordinary workers.
    pub async fn cancellation_scope<F, Fut, T>(&self, shield_parent: bool, body: F) -> Result<T>
    where
        F: FnOnce(WorkflowContext) -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let scope_id = ScopeOpeningCall {
            ctx: self.clone(),
            shield_parent,
        }
        .await?;
        let mut scoped = self.clone();
        scoped.cancellation_scope_id = scope_id;
        body(scoped).await
    }
}

struct ScopeOpeningCall {
    ctx: WorkflowContext,
    shield_parent: bool,
}

impl Future for ScopeOpeningCall {
    type Output = Result<String>;

    fn poll(self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
        let mut state = match self.ctx.state.lock() {
            Ok(state) => state,
            Err(_) => return Poll::Ready(Err(Error::WorkflowStatePoisoned)),
        };
        if !state.allow_cancellation_scope_authoring {
            return Poll::Ready(Err(Error::CancellationScopeExecutionUnavailable));
        }
        if state.cancellation_consumed {
            return Poll::Ready(Err(Error::CancellationScopeExecutionUnavailable));
        }
        if state
            .cancellation_scope_opening
            .as_ref()
            .is_some_and(|opening| opening.command_count != state.commands.len())
        {
            return Poll::Ready(Err(invalid_history(
                "workflow authored commands after an uncommitted opening",
            )));
        }
        let sequence = (state.command_cursor as u64)
            .saturating_add(state.commands.len() as u64)
            .saturating_add(1);
        if let Some(recorded) = state.recorded_commands.get(state.command_cursor).cloned() {
            return match recorded {
                RecordedCommand::CancellationScope {
                    sequence: original,
                    scope_id,
                    parent_scope_id,
                    shield_parent,
                } if original == sequence
                    && parent_scope_id == self.ctx.cancellation_scope_id
                    && shield_parent == self.shield_parent =>
                {
                    state.command_cursor += 1;
                    Poll::Ready(Ok(scope_id))
                }
                recorded => Poll::Ready(Err(invalid_recorded_history(
                    "cancellation_scope_opening_changed",
                    sequence,
                    "original scope opening, parent and shielding",
                    recorded.shape(),
                    "authored cancellation scope differs from committed history",
                ))),
            };
        }
        if state.history_events.iter().any(|event| {
            matches!(
                event.event_type.as_str(),
                "WorkflowCompleted"
                    | "WorkflowFailed"
                    | "WorkflowCancelled"
                    | "WorkflowTerminated"
                    | "WorkflowContinuedAsNew"
            )
        }) {
            return Poll::Ready(Err(invalid_recorded_history(
                "cancellation_scope_opening_changed",
                sequence,
                "original scope opening",
                "closed history",
                "closed history cannot admit an unrecorded scope",
            )));
        }
        state.cancellation_scope_opening = Some(CancellationScopeOpening {
            sequence,
            parent_scope_id: self.ctx.cancellation_scope_id.clone(),
            shield_parent: self.shield_parent,
            command_count: state.commands.len(),
        });
        Poll::Pending
    }
}

/// A scope opening proved against complete history on its original claim.
/// This proof does not advertise scoped workflow execution support.
#[derive(Clone, Debug)]
pub struct CancellationScopeOpenReceipt {
    scope_id: String,
    history_event_id: String,
    sequence: u64,
    parent_scope_id: String,
    shield_parent: bool,
    duplicate: bool,
    history: Vec<HistoryEvent>,
}

impl CancellationScopeOpenReceipt {
    pub fn scope_id(&self) -> &str {
        &self.scope_id
    }
    pub fn history_event_id(&self) -> &str {
        &self.history_event_id
    }
    pub fn sequence(&self) -> u64 {
        self.sequence
    }
    pub fn parent_scope_id(&self) -> &str {
        &self.parent_scope_id
    }
    pub fn shield_parent(&self) -> bool {
        self.shield_parent
    }
    pub fn duplicate(&self) -> bool {
        self.duplicate
    }
    pub fn history(&self) -> &[HistoryEvent] {
        &self.history
    }

    fn acknowledge<'a>(receipt: &'a Value, expected: &Value) -> Result<&'a str> {
        if expected
            .as_object()
            .ok_or_else(invalid)?
            .iter()
            .any(|(key, value)| key != "namespace" && receipt.get(key) != Some(value))
            || receipt["opened"].as_bool() != Some(true)
            || receipt["duplicate"].as_bool().is_none()
            || receipt["claim_released"].as_bool() != Some(false)
            || receipt.get("created_task_ids") != Some(&json!([]))
            || receipt.get("reason") != Some(&Value::Null)
            || text(receipt, "scope_id")? == "root"
        {
            return Err(invalid());
        }
        text(receipt, "history_event_id")?;
        receipt["history_refresh_page_token"]
            .as_str()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(invalid)
    }

    fn from_history(receipt: &Value, events: &[Value], expected: &Value) -> Result<Self> {
        Self::acknowledge(receipt, expected)?;
        let kind = |index: usize| {
            events
                .get(index)
                .and_then(|event| event.get("event_type").or_else(|| event.get("type")))
                .and_then(Value::as_str)
        };
        if kind(0) != Some("WorkflowStarted")
            && !(kind(0) == Some("StartAccepted") && kind(1) == Some("WorkflowStarted"))
        {
            return Err(invalid());
        }
        let mut event_ids = BTreeSet::new();
        let mut scopes = BTreeSet::new();
        let mut last_event_sequence = 0;
        let mut last_scope_sequence = 0;
        let mut found = false;
        let mut history = Vec::with_capacity(events.len());
        for event in events {
            let event_id = text(event, "id")?;
            let event_sequence = event["sequence"].as_u64().ok_or_else(invalid)?;
            let payload = &event["payload"];
            let event_kind = event
                .get("event_type")
                .or_else(|| event.get("type"))
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(invalid)?;
            if !event_ids.insert(event_id)
                || event_sequence <= last_event_sequence
                || event_sequence > MAX_SCOPE_SEQUENCE
                || event.get("namespace") != expected.get("namespace")
                || !payload.is_object()
            {
                return Err(invalid());
            }
            last_event_sequence = event_sequence;
            if event_kind == "CancellationScopeOpened" {
                let scope_id = text(payload, "scope_id")?;
                let parent = text(payload, "parent_scope_id")?;
                let sequence = payload["sequence"].as_u64().ok_or_else(invalid)?;
                if payload["schema"] != "durable-workflow.cancellation-scope/v1"
                    || payload.get("workflow_run_id") != expected.get("workflow_run_id")
                    || scope_id == "root"
                    || scopes.contains(scope_id)
                    || (parent != "root" && !scopes.contains(parent))
                    || payload["shield_parent"].as_bool().is_none()
                    || sequence <= last_scope_sequence
                    || sequence > MAX_SCOPE_SEQUENCE
                {
                    return Err(invalid());
                }
                scopes.insert(scope_id);
                last_scope_sequence = sequence;
                if Some(event_id) == receipt["history_event_id"].as_str() {
                    if ["scope_id", "sequence", "parent_scope_id", "shield_parent"]
                        .iter()
                        .any(|field| payload.get(*field) != receipt.get(*field))
                    {
                        return Err(invalid());
                    }
                    found = true;
                }
            }
            history.push(serde_json::from_value(event.clone()).map_err(|_| invalid())?);
        }
        if !found {
            return Err(invalid());
        }
        CancellationScopeHistory::read(&history, text(expected, "workflow_run_id")?)
            .map_err(|_| invalid())?;
        Ok(Self {
            scope_id: text(receipt, "scope_id")?.into(),
            history_event_id: text(receipt, "history_event_id")?.into(),
            sequence: receipt["sequence"].as_u64().ok_or_else(invalid)?,
            parent_scope_id: text(receipt, "parent_scope_id")?.into(),
            shield_parent: receipt["shield_parent"].as_bool().ok_or_else(invalid)?,
            duplicate: receipt["duplicate"].as_bool().ok_or_else(invalid)?,
            history,
        })
    }
}

impl Client {
    /// Open a scope and prove its canonical identity on the original claim.
    ///
    /// This explicit protocol 1.20 operation keeps one five-second budget for
    /// lost acknowledgement recovery and all history pages. No lease, worker
    /// capability or cancellation execution authority is added by this proof.
    pub async fn open_cancellation_scope_on_claim(
        &self,
        task: &WorkflowTask,
        sequence: u64,
        parent_scope_id: &str,
        shield_parent: bool,
    ) -> Result<CancellationScopeOpenReceipt> {
        let (owner, run_id) = cooperative_cancellation::cancellation_claim(task)?;
        if [
            task.task_id.as_str(),
            owner,
            run_id,
            parent_scope_id,
            self.namespace.as_str(),
        ]
        .iter()
        .any(|value| !identity(value))
            || sequence == 0
            || sequence > MAX_SCOPE_SEQUENCE
        {
            return Err(invalid());
        }
        let expected = json!({"task_id":task.task_id, "workflow_run_id":run_id,
            "lease_owner":owner, "workflow_task_attempt":task.workflow_task_attempt,
            "sequence":sequence, "parent_scope_id":parent_scope_id,
            "shield_parent":shield_parent, "namespace":self.namespace});
        let path = format!(
            "/worker/workflow-tasks/{}",
            percent_encode_path_segment(&task.task_id)
        );
        let body = json!({"lease_owner":owner, "workflow_task_attempt":task.workflow_task_attempt,
            "sequence":sequence, "parent_scope_id":parent_scope_id,"shield_parent":shield_parent});
        tokio::time::timeout(OPEN_BUDGET, async {
            let open_path = format!("{path}/cancellation-scopes/open");
            let request = || self.request_json::<Value, _>(reqwest::Method::POST,
                &open_path, RequestProtocol::Worker("1.20"), Some(&body));
            let receipt = match request().await {
                Err(error) if worker_operation_is_retryable(&error) => request().await?,
                result => result?,
            };
            let mut token = Some(CancellationScopeOpenReceipt::acknowledge(&receipt, &expected)?.to_owned());
            let mut seen = BTreeSet::new();
            let mut events = Vec::new();
            while let Some(current) = token.take() {
                if seen.len() >= MAX_HISTORY_PAGES || !seen.insert(current.clone()) { return Err(invalid()); }
                let page: Value = self.request_json(reqwest::Method::POST, &format!("{path}/history"),
                    RequestProtocol::Worker("1.20"), Some(&json!({"lease_owner":owner,
                        "workflow_task_attempt":task.workflow_task_attempt, "next_history_page_token":current,
                        "history_page_size":WORKFLOW_HISTORY_PAGE_SIZE}))).await?;
                if page["task_id"].as_str() != Some(task.task_id.as_str())
                    || page["workflow_task_attempt"].as_u64() != Some(task.workflow_task_attempt)
                { return Err(invalid()); }
                let batch = page["history_events"].as_array().ok_or_else(invalid)?;
                if batch.len() > WORKFLOW_HISTORY_PAGE_SIZE as usize || batch.iter().any(|event| !event.is_object()) {
                    return Err(invalid());
                }
                token = match page.get("next_history_page_token") {
                    Some(Value::Null) => None,
                    Some(Value::String(next)) if !next.trim().is_empty() && !batch.is_empty() => Some(next.clone()),
                    _ => return Err(invalid()),
                };
                events.extend(batch.iter().cloned());
            }
            CancellationScopeOpenReceipt::from_history(&receipt, &events, &expected)
        }).await.map_err(|_| Error::Timeout)?
    }
}
