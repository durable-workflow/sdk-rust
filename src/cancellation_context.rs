use super::*;
use chrono::{SecondsFormat, Utc};
use std::sync::Weak;

/// One immutable local request in the ordered cancellation lineage.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CancellationLineage {
    request_id: String,
    workflow_instance_id: String,
    workflow_run_id: String,
}

impl CancellationLineage {
    pub fn request_id(&self) -> &str {
        &self.request_id
    }

    pub fn workflow_instance_id(&self) -> &str {
        &self.workflow_instance_id
    }

    pub fn workflow_run_id(&self) -> &str {
        &self.workflow_run_id
    }

    fn to_value(&self) -> Value {
        json!({
            "request_id": self.request_id,
            "workflow_instance_id": self.workflow_instance_id,
            "workflow_run_id": self.workflow_run_id,
        })
    }
}

/// Original cancellation metadata restored from canonical request history.
///
/// Fields and nested metadata have read-only accessors. Descendants retain
/// the original root identity, request time and cleanup budget.
#[derive(Clone, Debug)]
pub struct CancellationContext {
    request_id: String,
    root_request_id: String,
    root_workflow_instance_id: String,
    root_workflow_run_id: String,
    parent_request_id: Option<String>,
    reason: Option<String>,
    requester: BTreeMap<String, String>,
    source: String,
    requested_at: DateTime<Utc>,
    cleanup_deadline_at: DateTime<Utc>,
    lineage: Vec<CancellationLineage>,
    scope_origin: Option<Box<ScopedCancellationContext>>,
    replay: Option<Weak<Mutex<WorkflowState>>>,
}

impl PartialEq for CancellationContext {
    fn eq(&self, other: &Self) -> bool {
        self.request_id == other.request_id
            && self.root_request_id == other.root_request_id
            && self.root_workflow_instance_id == other.root_workflow_instance_id
            && self.root_workflow_run_id == other.root_workflow_run_id
            && self.parent_request_id == other.parent_request_id
            && self.reason == other.reason
            && self.requester == other.requester
            && self.source == other.source
            && self.requested_at == other.requested_at
            && self.cleanup_deadline_at == other.cleanup_deadline_at
            && self.lineage == other.lineage
            && self.scope_origin == other.scope_origin
    }
}

impl Eq for CancellationContext {}

fn invalid_context(message: &str) -> Error {
    Error::InvalidCooperativeCancellation(message.to_owned())
}

fn context_text<'a>(value: &'a Value, key: &str) -> Result<&'a str> {
    value[key]
        .as_str()
        .filter(|text| !text.trim().is_empty())
        .ok_or_else(|| invalid_context("cancellation context identity must be a non-empty string"))
}

fn nullable_text(value: &Value, key: &str, allow_empty: bool) -> Result<Option<String>> {
    match value.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(text)) if allow_empty || !text.is_empty() => Ok(Some(text.clone())),
        _ => Err(invalid_context(
            "cancellation parent identity or reason is invalid",
        )),
    }
}

impl CancellationContext {
    /// Read legacy v1 and candidate scoped v2 without granting a new cleanup budget.
    pub fn from_value(value: &Value) -> Result<Self> {
        let scope_origin = match value["schema"].as_str() {
            Some("durable-workflow.cancellation-context/v2") => Some(Box::new(
                ScopedCancellationContext::from_value(&value["scope_origin"])?,
            )),
            Some("durable-workflow.cancellation-context/v1") => {
                if value.get("scope_origin").is_some()
                    || value.get("scope_authority_deadline_at").is_some()
                {
                    return Err(invalid_context(
                        "legacy cancellation context cannot discard a scoped origin",
                    ));
                }
                None
            }
            _ => return Err(invalid_context("unsupported cancellation context schema")),
        };
        let requester = value["requester"]
            .as_object()
            .filter(|requester| !requester.is_empty())
            .ok_or_else(|| invalid_context("cancellation requester must identify its caller"))?;
        let mut normalized_requester = BTreeMap::new();
        for (key, value) in requester {
            let text = value
                .as_str()
                .filter(|text| !text.is_empty())
                .ok_or_else(|| {
                    invalid_context("cancellation requester contains unsupported metadata")
                })?;
            if !matches!(key.as_str(), "type" | "id" | "label") {
                return Err(invalid_context(
                    "cancellation requester contains unsupported metadata",
                ));
            }
            normalized_requester.insert(key.clone(), text.to_owned());
        }
        let lineage = value["lineage"]
            .as_array()
            .filter(|lineage| !lineage.is_empty())
            .ok_or_else(|| invalid_context("cancellation lineage must contain the root request"))?;
        let mut normalized = Vec::new();
        for entry in lineage {
            normalized.push(CancellationLineage {
                request_id: context_text(entry, "request_id")?.to_owned(),
                workflow_instance_id: context_text(entry, "workflow_instance_id")?.to_owned(),
                workflow_run_id: context_text(entry, "workflow_run_id")?.to_owned(),
            });
        }
        let request_id = context_text(value, "request_id")?.to_owned();
        let root_request_id = context_text(value, "root_request_id")?.to_owned();
        let root_workflow_instance_id =
            context_text(value, "root_workflow_instance_id")?.to_owned();
        let root_workflow_run_id = context_text(value, "root_workflow_run_id")?.to_owned();
        let parent_request_id = nullable_text(value, "parent_request_id", false)?;
        let reason = nullable_text(value, "reason", true)?;
        let requests: BTreeSet<_> = normalized.iter().map(|entry| &entry.request_id).collect();
        let runs: BTreeSet<_> = normalized
            .iter()
            .map(|entry| &entry.workflow_run_id)
            .collect();
        if requests.len() != normalized.len() || runs.len() != normalized.len() {
            return Err(invalid_context(
                "cancellation lineage cannot contain a cycle",
            ));
        }
        let expected_parent = normalized
            .iter()
            .rev()
            .nth(1)
            .map(|entry| &entry.request_id);
        if normalized[0].request_id != root_request_id
            || normalized[0].workflow_instance_id != root_workflow_instance_id
            || normalized[0].workflow_run_id != root_workflow_run_id
            || normalized.last().unwrap().request_id != request_id
            || (scope_origin.is_none() && parent_request_id.as_ref() != expected_parent)
        {
            return Err(invalid_context(
                "cancellation lineage does not match its request identities",
            ));
        }
        let requested_at = DateTime::parse_from_rfc3339(context_text(value, "requested_at")?)
            .map_err(|_| invalid_context("cancellation request timestamp is invalid"))?
            .with_timezone(&Utc);
        let cleanup_deadline_at =
            DateTime::parse_from_rfc3339(context_text(value, "cleanup_deadline_at")?)
                .map_err(|_| invalid_context("cancellation deadline timestamp is invalid"))?
                .with_timezone(&Utc);
        if cleanup_deadline_at <= requested_at {
            return Err(invalid_context(
                "cancellation deadline must follow the original request",
            ));
        }
        let context = Self {
            request_id,
            root_request_id,
            root_workflow_instance_id,
            root_workflow_run_id,
            parent_request_id,
            reason,
            requester: normalized_requester,
            source: context_text(value, "source")?.to_owned(),
            requested_at,
            cleanup_deadline_at,
            lineage: normalized,
            scope_origin,
            replay: None,
        };
        context.assert_scope_origin(value)?;
        Ok(context)
    }

    fn assert_scope_origin(&self, value: &Value) -> Result<()> {
        let Some(origin) = self.scope_origin.as_deref() else {
            return Ok(());
        };
        let root = &origin.root_context;
        let last = self.lineage.last().unwrap();
        let authority =
            DateTime::parse_from_rfc3339(context_text(value, "scope_authority_deadline_at")?)
                .map_err(|_| invalid_context("scoped cancellation authority timestamp is invalid"))?
                .with_timezone(&Utc);
        if self.parent_request_id.as_deref() != Some(origin.request_id())
            || self.root_request_id != root.root_request_id
            || self.root_workflow_instance_id != root.root_workflow_instance_id
            || self.root_workflow_run_id != root.root_workflow_run_id
            || self.reason != root.reason
            || self.requester != root.requester
            || self.source != root.source
            || self.requested_at != root.requested_at
            || self.cleanup_deadline_at != authority
            || self.cleanup_deadline_at > origin.deadline()
            || origin.lineage.iter().any(|entry| {
                entry.request_id == last.request_id || entry.workflow_run_id == last.workflow_run_id
            })
        {
            return Err(invalid_context(
                "run cancellation does not preserve its original scope context",
            ));
        }
        let mut expected = vec![root.lineage[0].clone()];
        for entry in &origin.lineage {
            if entry.workflow_run_id == root.root_workflow_run_id {
                continue;
            }
            let address = CancellationLineage {
                request_id: entry.request_id.clone(),
                workflow_instance_id: entry.workflow_instance_id.clone(),
                workflow_run_id: entry.workflow_run_id.clone(),
            };
            if expected.last().unwrap().workflow_run_id == entry.workflow_run_id {
                *expected.last_mut().unwrap() = address;
            } else {
                expected.push(address);
            }
        }
        expected.push(last.clone());
        if self.lineage != expected {
            return Err(invalid_context(
                "run cancellation lineage discards or replaces its original scope ancestry",
            ));
        }
        Ok(())
    }

    pub fn request_id(&self) -> &str {
        &self.request_id
    }
    pub fn root_request_id(&self) -> &str {
        &self.root_request_id
    }
    pub fn root_workflow_instance_id(&self) -> &str {
        &self.root_workflow_instance_id
    }
    pub fn root_workflow_run_id(&self) -> &str {
        &self.root_workflow_run_id
    }
    pub fn parent_request_id(&self) -> Option<&str> {
        self.parent_request_id.as_deref()
    }
    pub fn reason(&self) -> Option<&str> {
        self.reason.as_deref()
    }
    pub fn requester(&self) -> &BTreeMap<String, String> {
        &self.requester
    }
    pub fn source(&self) -> &str {
        &self.source
    }
    pub fn requested_at(&self) -> DateTime<Utc> {
        self.requested_at
    }
    pub fn deadline(&self) -> DateTime<Utc> {
        self.cleanup_deadline_at
    }
    pub fn scope_origin(&self) -> Option<&ScopedCancellationContext> {
        self.scope_origin.as_deref()
    }

    /// Remaining cleanup budget at the last blocking result consumed by this replay.
    ///
    /// Never reads host time. Detached metadata, an ended replay, and a missing or
    /// invalid committed timestamp return an explicit error. Expiry returns zero.
    pub fn remaining(&self) -> Result<Duration> {
        let state = self
            .replay
            .as_ref()
            .and_then(Weak::upgrade)
            .filter(cancellation_replay_clock::is_active)
            .ok_or_else(|| {
                invalid_context("remaining() is available only in its active workflow replay")
            })?;
        let time = state
            .lock()
            .map_err(|_| Error::WorkflowStatePoisoned)?
            .cancellation_time()?;
        Ok((self.cleanup_deadline_at - time)
            .to_std()
            .unwrap_or(Duration::ZERO))
    }

    pub(super) fn with_replay(mut self, replay: Option<Weak<Mutex<WorkflowState>>>) -> Self {
        self.replay = replay;
        self
    }
    pub fn lineage(&self) -> &[CancellationLineage] {
        &self.lineage
    }

    /// Detached metadata in the portable context schema.
    pub fn to_value(&self) -> Value {
        let mut value = json!({
            "schema": if self.scope_origin.is_none() { "durable-workflow.cancellation-context/v1" }
                else { "durable-workflow.cancellation-context/v2" },
            "request_id": self.request_id, "root_request_id": self.root_request_id,
            "root_workflow_instance_id": self.root_workflow_instance_id,
            "root_workflow_run_id": self.root_workflow_run_id,
            "parent_request_id": self.parent_request_id, "reason": self.reason,
            "requester": self.requester, "source": self.source,
            "requested_at": self.requested_at.to_rfc3339_opts(SecondsFormat::Micros, true),
            "cleanup_deadline_at": self.cleanup_deadline_at.to_rfc3339_opts(SecondsFormat::Micros, true),
            "lineage": self.lineage.iter().map(CancellationLineage::to_value).collect::<Vec<_>>(),
        });
        if let Some(origin) = &self.scope_origin {
            value["scope_origin"] = origin.to_value();
            value["scope_authority_deadline_at"] = value["cleanup_deadline_at"].clone();
        }
        value
    }
}

/// One immutable scope address and its bounded cleanup deadline.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScopedCancellationLineage {
    request_id: String,
    workflow_instance_id: String,
    workflow_run_id: String,
    scope_id: String,
    cleanup_deadline_at: DateTime<Utc>,
}

impl ScopedCancellationLineage {
    pub fn request_id(&self) -> &str {
        &self.request_id
    }
    pub fn workflow_instance_id(&self) -> &str {
        &self.workflow_instance_id
    }
    pub fn workflow_run_id(&self) -> &str {
        &self.workflow_run_id
    }
    pub fn scope_id(&self) -> &str {
        &self.scope_id
    }
    pub fn deadline(&self) -> DateTime<Utc> {
        self.cleanup_deadline_at
    }

    fn to_value(&self) -> Value {
        json!({
            "request_id": self.request_id, "workflow_instance_id": self.workflow_instance_id,
            "workflow_run_id": self.workflow_run_id, "scope_id": self.scope_id,
            "cleanup_deadline_at": self.cleanup_deadline_at.to_rfc3339_opts(SecondsFormat::Micros, true),
        })
    }
}

/// Original scope ancestry carried by a candidate cooperative child request.
/// Reading this immutable metadata does not authorize entering a scope body.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScopedCancellationContext {
    root_context: CancellationContext,
    lineage: Vec<ScopedCancellationLineage>,
}

fn assert_context_keys(value: &Value, keys: &[&str]) -> Result<()> {
    let object = value
        .as_object()
        .ok_or_else(|| invalid_context("scoped context must be an object"))?;
    if object.len() != keys.len() || keys.iter().any(|key| !object.contains_key(*key)) {
        return Err(invalid_context(
            "scoped cancellation context has missing or unsupported fields",
        ));
    }
    Ok(())
}

impl ScopedCancellationContext {
    pub(super) fn from_run_context(context: &CancellationContext) -> Result<Self> {
        let mut root = context.to_value();
        let mut lineage = Vec::new();
        if let Some(origin) = context.scope_origin() {
            root = origin.root_context().to_value();
            lineage.extend(
                origin
                    .lineage()
                    .iter()
                    .map(ScopedCancellationLineage::to_value),
            );
            let local = context.lineage().last().unwrap();
            lineage.push(json!({"request_id":context.request_id(),
                "workflow_instance_id":local.workflow_instance_id(),
                "workflow_run_id":local.workflow_run_id(), "scope_id":"root",
                "cleanup_deadline_at":context.deadline().to_rfc3339_opts(SecondsFormat::Micros, true)}));
        } else {
            root["request_id"] = json!(context.root_request_id());
            root["parent_request_id"] = Value::Null;
            root["lineage"] = json!([context.lineage()[0].to_value()]);
            lineage.extend(context.lineage().iter().map(|entry| json!({
                "request_id":entry.request_id(), "workflow_instance_id":entry.workflow_instance_id(),
                "workflow_run_id":entry.workflow_run_id(), "scope_id":"root",
                "cleanup_deadline_at":context.deadline().to_rfc3339_opts(SecondsFormat::Micros, true)})));
        }
        Self::from_value(
            &json!({"schema":"durable-workflow.scoped-cancellation-context/v1",
            "root_context":root, "lineage":lineage}),
        )
    }

    pub fn from_value(value: &Value) -> Result<Self> {
        assert_context_keys(value, &["schema", "root_context", "lineage"])?;
        if value["schema"] != "durable-workflow.scoped-cancellation-context/v1"
            || value["root_context"]["schema"] != "durable-workflow.cancellation-context/v1"
        {
            return Err(invalid_context(
                "scoped cancellation requires its original root context",
            ));
        }
        let root = CancellationContext::from_value(&value["root_context"])?;
        let root_value = root.to_value();
        let root_keys: Vec<_> = root_value
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        assert_context_keys(&value["root_context"], &root_keys)?;
        if root.request_id != root.root_request_id
            || root.parent_request_id.is_some()
            || root.lineage.len() != 1
        {
            return Err(invalid_context(
                "scoped cancellation root must contain the original root request",
            ));
        }
        let lineage = value["lineage"]
            .as_array()
            .filter(|rows| !rows.is_empty())
            .ok_or_else(|| {
                invalid_context("scoped cancellation lineage must contain its root address")
            })?;
        let mut normalized = Vec::new();
        let mut requests = BTreeSet::new();
        let mut addresses = BTreeSet::new();
        let mut instances_by_run = BTreeMap::new();
        let mut last_run = String::new();
        let mut deadline = root.deadline();
        for (index, entry) in lineage.iter().enumerate() {
            assert_context_keys(
                entry,
                &[
                    "request_id",
                    "workflow_instance_id",
                    "workflow_run_id",
                    "scope_id",
                    "cleanup_deadline_at",
                ],
            )?;
            let address = ScopedCancellationLineage {
                request_id: context_text(entry, "request_id")?.to_owned(),
                workflow_instance_id: context_text(entry, "workflow_instance_id")?.to_owned(),
                workflow_run_id: context_text(entry, "workflow_run_id")?.to_owned(),
                scope_id: context_text(entry, "scope_id")?.to_owned(),
                cleanup_deadline_at: DateTime::parse_from_rfc3339(context_text(
                    entry,
                    "cleanup_deadline_at",
                )?)
                .map_err(|_| invalid_context("scoped cancellation deadline timestamp is invalid"))?
                .with_timezone(&Utc),
            };
            if index == 0
                && (address.request_id != root.request_id
                    || address.workflow_instance_id != root.root_workflow_instance_id
                    || address.workflow_run_id != root.root_workflow_run_id
                    || address.cleanup_deadline_at != root.deadline())
            {
                return Err(invalid_context(
                    "scoped cancellation root address does not match its request",
                ));
            }
            if !requests.insert(address.request_id.clone())
                || !addresses.insert((address.workflow_run_id.clone(), address.scope_id.clone()))
            {
                return Err(invalid_context(
                    "scoped cancellation cannot repeat a request or address",
                ));
            }
            if let Some(instance) = instances_by_run.get(&address.workflow_run_id) {
                if instance != &address.workflow_instance_id || last_run != address.workflow_run_id
                {
                    return Err(invalid_context(
                        "scoped cancellation cannot reenter or reassign an earlier run",
                    ));
                }
            }
            if address.cleanup_deadline_at <= root.requested_at
                || address.cleanup_deadline_at > deadline
            {
                return Err(invalid_context(
                    "scoped cancellation cannot extend a descendant budget",
                ));
            }
            instances_by_run.insert(
                address.workflow_run_id.clone(),
                address.workflow_instance_id.clone(),
            );
            last_run = address.workflow_run_id.clone();
            deadline = address.cleanup_deadline_at;
            normalized.push(address);
        }
        Ok(Self {
            root_context: root,
            lineage: normalized,
        })
    }

    pub fn root_context(&self) -> &CancellationContext {
        &self.root_context
    }
    pub fn lineage(&self) -> &[ScopedCancellationLineage] {
        &self.lineage
    }
    pub fn request_id(&self) -> &str {
        &self.lineage.last().unwrap().request_id
    }
    pub fn parent_request_id(&self) -> Option<&str> {
        self.lineage
            .iter()
            .rev()
            .nth(1)
            .map(|entry| entry.request_id.as_str())
    }
    pub fn workflow_instance_id(&self) -> &str {
        &self.lineage.last().unwrap().workflow_instance_id
    }
    pub fn workflow_run_id(&self) -> &str {
        &self.lineage.last().unwrap().workflow_run_id
    }
    pub fn scope_id(&self) -> &str {
        &self.lineage.last().unwrap().scope_id
    }
    pub fn root_scope_id(&self) -> &str {
        &self.lineage[0].scope_id
    }
    pub fn requested_at(&self) -> DateTime<Utc> {
        self.root_context.requested_at()
    }
    pub fn root_deadline(&self) -> DateTime<Utc> {
        self.root_context.deadline()
    }
    pub fn deadline(&self) -> DateTime<Utc> {
        self.lineage.last().unwrap().cleanup_deadline_at
    }
    pub fn to_value(&self) -> Value {
        json!({
            "schema": "durable-workflow.scoped-cancellation-context/v1",
            "root_context": self.root_context.to_value(),
            "lineage": self.lineage.iter().map(ScopedCancellationLineage::to_value).collect::<Vec<_>>(),
        })
    }
}
