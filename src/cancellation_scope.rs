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
