use super::*;
use chrono::{SecondsFormat, Utc};

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
#[derive(Clone, Debug, PartialEq, Eq)]
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
}

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
    /// Validate the portable v1 snapshot without granting a new cleanup budget.
    pub fn from_value(value: &Value) -> Result<Self> {
        if value["schema"] != "durable-workflow.cancellation-context/v1" {
            return Err(invalid_context("unsupported cancellation context schema"));
        }
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
            || parent_request_id.as_ref() != expected_parent
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
        Ok(Self {
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
        })
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
    pub fn lineage(&self) -> &[CancellationLineage] {
        &self.lineage
    }

    /// Detached metadata in the portable context schema.
    pub fn to_value(&self) -> Value {
        json!({
            "schema": "durable-workflow.cancellation-context/v1",
            "request_id": self.request_id, "root_request_id": self.root_request_id,
            "root_workflow_instance_id": self.root_workflow_instance_id,
            "root_workflow_run_id": self.root_workflow_run_id,
            "parent_request_id": self.parent_request_id, "reason": self.reason,
            "requester": self.requester, "source": self.source,
            "requested_at": self.requested_at.to_rfc3339_opts(SecondsFormat::Micros, true),
            "cleanup_deadline_at": self.cleanup_deadline_at.to_rfc3339_opts(SecondsFormat::Micros, true),
            "lineage": self.lineage.iter().map(CancellationLineage::to_value).collect::<Vec<_>>(),
        })
    }
}
