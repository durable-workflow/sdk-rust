//! Original accepted scope requests and frozen v5 delivery projections.
//! These facts alone grant no callback, stop, or publication authority.

use super::*;
use chrono::{SecondsFormat, Utc};
use serde::ser::{SerializeMap, SerializeSeq};

const FIELDS: [&str; 4] = [
    "activity_members",
    "timer_members",
    "wait_members",
    "child_members",
];
const GROUP_KEYS: [&str; 11] = [
    "parallel_group_id",
    "parallel_group_kind",
    "parallel_group_mode",
    "parallel_group_base_sequence",
    "parallel_group_size",
    "parallel_group_index",
    "selection_member_key",
    "selection_member_index",
    "selection_member_base_sequence",
    "selection_member_size",
    "selection_member_kind",
];

fn invalid(detail: &str) -> Error {
    invalid_recorded_history(
        "invalid_cancellation_scope_history",
        0,
        "original accepted scope and frozen operation projection",
        "invalid history",
        detail,
    )
}

fn identity(value: &Value) -> bool {
    value
        .as_str()
        .is_some_and(|text| !text.trim().is_empty() && text.len() <= 255)
}

fn text<'a>(value: &'a Value, key: &str) -> Result<&'a str> {
    value
        .get(key)
        .filter(|value| identity(value))
        .and_then(Value::as_str)
        .ok_or_else(|| invalid("scope proof requires its original identity"))
}

fn positive(value: &Value) -> bool {
    value
        .as_u64()
        .is_some_and(|value| value > 0 && value <= i64::MAX as u64)
}

pub(super) fn timestamp(value: &Value) -> Result<DateTime<Utc>> {
    value
        .as_str()
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .map(|value| value.with_timezone(&Utc))
        .ok_or_else(|| invalid("scope authority requires an original timestamp with timezone"))
}

fn event_time(event: &HistoryEvent) -> Result<DateTime<Utc>> {
    timestamp(
        event
            .raw
            .get("timestamp")
            .or_else(|| event.raw.get("recorded_at"))
            .unwrap_or(&Value::Null),
    )
}

fn event_id(event: &HistoryEvent) -> Result<&str> {
    event
        .raw
        .get("id")
        .filter(|value| identity(value))
        .and_then(Value::as_str)
        .ok_or_else(|| invalid("scope proof lacks its original event identity"))
}
fn event_sequence(event: &HistoryEvent) -> Result<u64> {
    event
        .raw
        .get("sequence")
        .filter(|value| positive(value))
        .and_then(Value::as_u64)
        .ok_or_else(|| invalid("scope proof lacks canonical event order"))
}
fn canonical_time(time: DateTime<Utc>) -> String {
    time.to_rfc3339_opts(SecondsFormat::Micros, true)
}
fn policy(value: &Value) -> bool {
    matches!(
        value.as_str(),
        Some("try_cancel" | "wait_cancellation_completed" | "abandon")
    )
}

// Only descriptor group objects use this key order. Do not change serde_json's
// global map behavior or any payload codec to reproduce Native's PHP hashes.
struct Descriptor<'a>(&'a Value);
impl Serialize for Descriptor<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        match self.0 {
            Value::Array(values) => {
                let mut seq = serializer.serialize_seq(Some(values.len()))?;
                for value in values {
                    seq.serialize_element(&Descriptor(value))?;
                }
                seq.end()
            }
            Value::Object(object) => {
                let mut map = serializer.serialize_map(Some(object.len()))?;
                for key in GROUP_KEYS {
                    if let Some(value) = object.get(key) {
                        map.serialize_entry(key, &Descriptor(value))?;
                    }
                }
                if object.keys().any(|key| !GROUP_KEYS.contains(&key.as_str())) {
                    return Err(serde::ser::Error::custom(
                        "unexpected scope descriptor object",
                    ));
                }
                map.end()
            }
            value => value.serialize(serializer),
        }
    }
}

fn descriptor_hash(values: Value) -> Result<String> {
    let encoded = serde_json::to_string(&Descriptor(&values))?;
    let mut php = String::new();
    for character in encoded.chars() {
        if character == '/' {
            php.push_str("\\/");
        } else if character as u32 > 0x7f {
            for unit in character.encode_utf16(&mut [0; 2]) {
                use std::fmt::Write;
                write!(&mut php, "\\u{unit:04x}").unwrap();
            }
        } else {
            php.push(character);
        }
    }
    Ok(format!("{:x}", Sha256::digest(php.as_bytes())))
}

#[cfg(test)]
mod descriptor_interop {
    use super::*;

    #[test]
    fn scope_history_descriptor_hash_matches_php_unicode_slashes_del_and_ordered_groups() {
        assert_eq!(
            descriptor_hash(json!([
                "scope/é😀",
                "event/a",
                "\u{7f}",
                "x\u{2028}y",
                true,
                null
            ]))
            .unwrap(),
            "afde5922dded878e86ff3e376d9ebd20804265c8df877ac50bcd6d3ca0b22aa8"
        );
        assert_eq!(
            descriptor_hash(json!([{"parallel_group_id":"parallel-calls:scope/é😀",
            "parallel_group_kind":"mixed", "parallel_group_base_sequence":3,
            "parallel_group_size":2, "parallel_group_index":0}]))
            .unwrap(),
            "c31d38786755cb4d32824eb8b765e305757bb627eaa7433eb6f82926928a970a"
        );
    }
}

pub(super) fn normalize_members(field: &str, value: &Value) -> Result<Value> {
    let (keys, id_key): (&[&str], &str) = match field {
        "activity_members" => (
            &["sequence", "activity_execution_id", "descriptor_hash"],
            "activity_execution_id",
        ),
        "timer_members" => (&["sequence", "timer_id", "descriptor_hash"], "timer_id"),
        "wait_members" => (
            &["kind", "sequence", "wait_id", "timer_id", "descriptor_hash"],
            "wait_id",
        ),
        "child_members" => (
            &[
                "sequence",
                "child_call_id",
                "child_workflow_instance_id",
                "child_workflow_run_id",
                "cancellation_policy",
                "descriptor_hash",
            ],
            "child_call_id",
        ),
        _ => return Err(invalid("unknown scope projection")),
    };
    let members = value
        .as_array()
        .ok_or_else(|| invalid("scope projection must be a list"))?;
    let mut ids = BTreeSet::new();
    let mut sequences = BTreeSet::new();
    for member in members {
        let object = member
            .as_object()
            .ok_or_else(|| invalid("scope member must be an object"))?;
        let hash = member["descriptor_hash"].as_str().unwrap_or_default();
        if object.len() != keys.len()
            || keys.iter().any(|key| !object.contains_key(*key))
            || !positive(&member["sequence"])
            || !identity(&member[id_key])
            || !ids.insert(member[id_key].as_str().unwrap().to_owned())
            || hash.len() != 64
            || !hash
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            || (matches!(field, "activity_members" | "child_members")
                && !sequences.insert(member["sequence"].as_u64().unwrap()))
        {
            return Err(invalid(
                "scope projection changes its original member address",
            ));
        }
        if field == "wait_members"
            && (!matches!(member["kind"].as_str(), Some("signal" | "condition"))
                || (!member["timer_id"].is_null() && !identity(&member["timer_id"])))
        {
            return Err(invalid("scope wait changes its kind or timeout address"));
        }
        if field == "child_members"
            && (!identity(&member["child_workflow_instance_id"])
                || !identity(&member["child_workflow_run_id"])
                || !policy(&member["cancellation_policy"]))
        {
            return Err(invalid("scope child changes its target or policy"));
        }
    }
    Ok(value.clone())
}

fn address<'a>(payload: &'a Value, descriptor_key: &str) -> Result<&'a str> {
    let descriptor = payload.get(descriptor_key);
    if descriptor.is_some_and(|value| !value.is_object()) {
        return Err(invalid("scope operation lacks its original descriptor"));
    }
    let nested = descriptor.and_then(|value| value.get("cancellation_scope_id"));
    let address = payload.get("cancellation_scope_id").or(nested);
    if address.is_some_and(|value| !identity(value)) || (nested.is_some() && nested != address) {
        return Err(invalid("scope operation changes its immediate membership"));
    }
    Ok(address.and_then(Value::as_str).unwrap_or("root"))
}

fn group_entry(payload: &Value) -> Option<Value> {
    let id = payload["parallel_group_id"].as_str()?;
    let kind = payload["parallel_group_kind"].as_str().or_else(|| {
        [
            ("parallel-activities:", "activity"),
            ("parallel-calls:", "mixed"),
            ("select-calls:", "mixed"),
            ("parallel-timers:", "timer"),
            ("parallel-children:", "child"),
        ]
        .into_iter()
        .find_map(|(prefix, kind)| id.starts_with(prefix).then_some(kind))
    })?;
    let mode = payload["parallel_group_mode"]
        .as_str()
        .filter(|value| !value.is_empty())
        .or_else(|| id.starts_with("select-calls:").then_some("select"));
    let select = mode == Some("select");
    if [
        "parallel_group_base_sequence",
        "parallel_group_size",
        "parallel_group_index",
    ]
    .iter()
    .any(|key| payload[*key].as_i64().is_none())
        || payload["parallel_group_size"].as_i64()? < 1
        || (select
            && !(payload["selection_member_key"]
                .as_i64()
                .is_some_and(|key| key >= 0)
                || payload["selection_member_key"]
                    .as_str()
                    .is_some_and(|key| !key.is_empty())))
    {
        return None;
    }
    let mut result = json!({"parallel_group_id":id, "parallel_group_kind":kind,
        "parallel_group_base_sequence":payload["parallel_group_base_sequence"],
        "parallel_group_size":payload["parallel_group_size"], "parallel_group_index":payload["parallel_group_index"]});
    if select {
        result["parallel_group_mode"] = json!("select");
        result["selection_member_key"] = payload["selection_member_key"].clone();
    }
    for key in [
        "selection_member_index",
        "selection_member_base_sequence",
        "selection_member_size",
    ] {
        if payload[key].as_i64().is_some() {
            result[key] = payload[key].clone();
        }
    }
    if payload["selection_member_kind"]
        .as_str()
        .is_some_and(|value| !value.is_empty())
    {
        result["selection_member_kind"] = payload["selection_member_kind"].clone();
    }
    Some(result)
}

fn group_path(payload: &Value) -> Result<Value> {
    let raw = match payload.get("parallel_group_path") {
        None => &[][..],
        Some(Value::Array(path)) => path.as_slice(),
        _ => {
            return Err(invalid(
                "scope operation has a malformed original group path",
            ))
        }
    };
    let path: Vec<_> = raw.iter().filter_map(group_entry).collect();
    Ok(json!(if path.is_empty() {
        group_entry(payload).into_iter().collect::<Vec<_>>()
    } else {
        path
    }))
}

pub(super) fn members_from_prefix(
    field: &str,
    prefix: &[HistoryEvent],
    scope: &str,
    run: &str,
) -> Result<Value> {
    let mut members = Vec::new();
    let mut activity_ids = BTreeSet::new();
    let mut activity_sequences = BTreeSet::new();
    let mut timer_sequences = BTreeMap::new();
    for event in prefix {
        let p = &event.payload;
        let sequence = &p["sequence"];
        match (field, event.event_type.as_str()) {
            ("activity_members", "ActivityScheduled") => {
                let activity = &p["activity"];
                let id = &p["activity_execution_id"];
                if !activity.is_object()
                    || !identity(id)
                    || activity["id"] != *id
                    || !positive(sequence)
                    || !activity_ids.insert(id.as_str().unwrap().to_owned())
                    || !activity_sequences.insert(sequence.as_u64().unwrap())
                    || p.get("cancellation_scope_id").unwrap_or(
                        activity
                            .get("cancellation_scope_id")
                            .unwrap_or(&json!("root")),
                    ) != activity
                        .get("cancellation_scope_id")
                        .unwrap_or(&json!("root"))
                {
                    return Err(invalid(
                        "scope Activity admission changes identity or membership",
                    ));
                }
                if address(p, "activity")? != scope {
                    continue;
                }
                for key in ["local_preparation", "local_group_admission"] {
                    if p[key]["cancellation_cleanup"].get("scope_id").is_some() {
                        return Err(invalid("nested cleanup needs its original cleanup proof"));
                    }
                }
                let cancellation_policy = activity
                    .get("cancellation_policy")
                    .cloned()
                    .unwrap_or(json!("try_cancel"));
                if !policy(&cancellation_policy)
                    || p.get("local_activity")
                        .is_some_and(|value| !value.is_boolean())
                    || p.get("execution_mode")
                        .is_some_and(|value| !value.is_null() && !value.is_string())
                    || activity
                        .get("schedule_to_close_deadline_at")
                        .is_some_and(|value| !value.is_null() && !value.is_string())
                {
                    return Err(invalid(
                        "scope Activity changes its cancellation descriptor",
                    ));
                }
                members.push(json!({"sequence":sequence, "activity_execution_id":id,
                    "descriptor_hash":descriptor_hash(json!([scope, event_id(event)?, cancellation_policy,
                        p.get("local_activity").unwrap_or(&Value::Bool(false)), p["execution_mode"],
                        activity["schedule_to_close_deadline_at"]]))?}));
            }
            ("timer_members", "TimerScheduled") if address(p, "timer")? == scope => {
                let id = &p["timer_id"];
                let kind = &p["timer_kind"];
                if !positive(sequence)
                    || !identity(id)
                    || p["delay_seconds"].as_i64().is_none_or(|delay| delay < 0)
                    || timer_sequences
                        .get(&sequence.as_u64().unwrap_or_default())
                        .is_some_and(|previous| {
                            previous != kind
                                || !matches!(
                                    kind.as_str(),
                                    Some("signal_timeout" | "condition_timeout")
                                )
                        })
                    || p["fire_at"].as_str()
                        != Some(canonical_time(timestamp(&p["fire_at"])?).as_str())
                {
                    return Err(invalid("scope timer changes its original descriptor"));
                }
                timer_sequences.insert(sequence.as_u64().unwrap(), kind.clone());
                members.push(json!({"sequence":sequence, "timer_id":id,
                    "descriptor_hash":descriptor_hash(json!([scope, event_id(event)?, sequence, id,
                        p["delay_seconds"], p["fire_at"], kind, p["condition_wait_id"],
                        p["condition_wait_occurrence_id"], p["signal_wait_id"], group_path(p)?]))?}));
            }
            ("wait_members", "SignalWaitOpened" | "ConditionWaitOpened")
                if p["cancellation_scope_id"].as_str().unwrap_or("root") == scope =>
            {
                let kind = if event.event_type == "SignalWaitOpened" {
                    "signal"
                } else {
                    "condition"
                };
                let id = &p[format!("{kind}_wait_id")];
                if !positive(sequence) || !identity(id) {
                    return Err(invalid("scope wait changes its original address"));
                }
                let timers: Vec<_> = prefix
                    .iter()
                    .filter(|row| {
                        row.event_type == "TimerScheduled"
                            && row.payload[format!("{kind}_wait_id")] == *id
                    })
                    .collect();
                if timers.len() > 1 {
                    return Err(invalid("scope wait duplicates its timeout"));
                }
                let timer = timers.first().copied();
                let empty = Value::Null;
                let timeout = timer.map_or(&empty, |row| &row.payload);
                if let Some(timer) = timer {
                    if timeout["timer_kind"] != format!("{kind}_timeout")
                        || timeout["sequence"] != *sequence
                        || event_sequence(timer)? <= event_sequence(event)?
                        || timeout["cancellation_scope_id"].as_str().unwrap_or("root") != scope
                        || !identity(&timeout["timer_id"])
                    {
                        return Err(invalid("scope wait changes its original timeout"));
                    }
                }
                members.push(json!({"kind":kind, "sequence":sequence, "wait_id":id, "timer_id":timeout["timer_id"],
                    "descriptor_hash":descriptor_hash(json!([scope, event_id(event)?, kind, sequence, id,
                        timer.map(event_id).transpose()?, timeout["timer_id"], p["timeout_seconds"], p["signal_name"],
                        p["condition_wait_occurrence_id"], p["condition_key"], p["condition_definition_fingerprint"],
                        group_path(p)?]))?}));
            }
            ("child_members", "ChildWorkflowScheduled")
                if address(p, "child_workflow")? == scope =>
            {
                let id = &p["child_call_id"];
                let instance = &p["child_workflow_instance_id"];
                let mut target = p["child_workflow_run_id"].clone();
                let cancellation_policy = p
                    .get("cancellation_policy")
                    .cloned()
                    .unwrap_or(json!("abandon"));
                if !positive(sequence)
                    || !identity(id)
                    || !identity(instance)
                    || !identity(&target)
                    || target == run
                    || !policy(&cancellation_policy)
                {
                    return Err(invalid("scope child changes its original target or policy"));
                }
                let mut last_started = None;
                for started in prefix.iter().filter(|row| {
                    row.event_type == "ChildRunStarted" && row.payload["sequence"] == *sequence
                }) {
                    let next = &started.payload;
                    if event_sequence(started)? <= event_sequence(event)?
                        || next["child_call_id"] != *id
                        || next["child_workflow_instance_id"] != *instance
                        || !identity(&next["child_workflow_run_id"])
                        || next["child_workflow_run_id"] == run
                        || next
                            .get("cancellation_scope_id")
                            .is_some_and(|value| value != scope)
                        || next
                            .get("cancellation_policy")
                            .is_some_and(|value| value != &cancellation_policy)
                    {
                        return Err(invalid("scope child changes its original continuation"));
                    }
                    target = next["child_workflow_run_id"].clone();
                    last_started = Some(event_id(started)?);
                }
                members.push(json!({"sequence":sequence, "child_call_id":id, "child_workflow_instance_id":instance,
                    "child_workflow_run_id":target, "cancellation_policy":cancellation_policy,
                    "descriptor_hash":descriptor_hash(json!([scope, event_id(event)?, sequence, id, instance,
                        target, cancellation_policy, p["parent_close_policy"], p["child_workflow_type"],
                        last_started, group_path(p)?]))?}));
            }
            _ => {}
        }
    }
    normalize_members(field, &json!(members))
}

fn descendants_from_prefix(
    prefix: &[HistoryEvent],
    scope: &str,
    run: &str,
    authority: DateTime<Utc>,
) -> Result<Value> {
    let openings: BTreeMap<_, _> = prefix
        .iter()
        .filter(|row| row.event_type == "CancellationScopeOpened")
        .map(|row| Ok((text(&row.payload, "scope_id")?, row)))
        .collect::<Result<_>>()?;
    let requests: BTreeMap<_, _> = prefix
        .iter()
        .filter(|row| row.event_type == "CancellationScopeRequested")
        .map(|row| Ok((text(&row.payload, "scope_id")?, row)))
        .collect::<Result<_>>()?;
    let mut included = BTreeSet::from([scope]);
    let mut members = Vec::new();
    // Use canonical opening order, rather than a map's lexical identity order.
    for opening in prefix
        .iter()
        .filter(|row| row.event_type == "CancellationScopeOpened")
    {
        let id = text(&opening.payload, "scope_id")?;
        let parent_id = text(&opening.payload, "parent_scope_id")?;
        if id == scope || opening.payload["shield_parent"] == true || !included.contains(parent_id)
        {
            continue;
        }
        included.insert(id);
        let request = requests
            .get(id)
            .ok_or_else(|| invalid("descendant lacks its accepted propagation"))?;
        let parent_request = requests
            .get(parent_id)
            .ok_or_else(|| invalid("descendant lacks its accepted parent"))?;
        let context = ScopedCancellationContext::from_value(&request.payload["cancellation"])?;
        let parent =
            ScopedCancellationContext::from_value(&parent_request.payload["cancellation"])?;
        let propagation = if context.root_context() == parent.root_context() {
            if request.payload["parent_scope_id"] != parent_id {
                return Err(invalid("descendant changes its original parent"));
            }
            *request
        } else {
            let mut conflict = None;
            for event in prefix.iter().filter(|row| {
                row.event_type == "CancellationScopeRequestConflicted"
                    && row.payload["scope_id"] == id
                    && row.payload["parent_scope_id"] == parent_id
            }) {
                let p = &event.payload;
                if p["schema"] != "durable-workflow.cancellation-scope-request/v1"
                    || p["workflow_run_id"] != run
                    || p["reason"] != "cancellation_root_conflict"
                    || event_sequence(event)?
                        <= event_sequence(request)?.max(event_sequence(parent_request)?)
                {
                    return Err(invalid("descendant changes its original conflict boundary"));
                }
                let incoming = ScopedCancellationContext::from_value(&p["incoming_cancellation"])?;
                let accepted = ScopedCancellationContext::from_value(&p["accepted_cancellation"])?;
                if incoming.root_context() == parent.root_context()
                    && incoming.lineage()[..incoming.lineage().len() - 1] == *parent.lineage()
                    && incoming.deadline() == parent.deadline()
                    && incoming.scope_id() == id
                    && incoming.workflow_run_id() == run
                    && incoming.workflow_instance_id() == context.workflow_instance_id()
                    && accepted == context
                {
                    conflict = Some(event);
                    break;
                }
            }
            conflict
                .ok_or_else(|| invalid("descendant lacks its original competing-root conflict"))?
        };
        let mut deadline = authority;
        let mut ancestor = id;
        while ancestor != "root" {
            if let Some(request) = requests.get(ancestor) {
                deadline = deadline.min(
                    ScopedCancellationContext::from_value(&request.payload["cancellation"])?
                        .deadline(),
                );
            }
            ancestor = text(
                &openings
                    .get(ancestor)
                    .ok_or_else(|| invalid("descendant lost its original ancestry"))?
                    .payload,
                "parent_scope_id",
            )?;
        }
        let mut member = json!({"scope_id":id, "parent_scope_id":parent_id, "scope_history_event_id":event_id(opening)?,
            "request_history_event_id":event_id(request)?, "request_id":context.request_id(),
            "propagation_history_event_id":event_id(propagation)?, "authority_deadline_at":canonical_time(deadline),
            "cancellation":context.to_value()});
        for field in FIELDS {
            member[field] = members_from_prefix(field, prefix, id, run)?;
        }
        members.push(member);
    }
    Ok(json!(members))
}

#[derive(Clone, Debug)]
pub(super) struct ScopeRequest {
    pub context: ScopedCancellationContext,
    pub timestamp: DateTime<Utc>,
    pub history_index: usize,
}

#[derive(Clone, Debug)]
pub(super) struct ScopeBoundary {
    pub context: ScopedCancellationContext,
    pub boundary: CancellationDelivery,
    pub authority_deadline: DateTime<Utc>,
    pub event: HistoryEvent,
}

#[derive(Default, Debug)]
pub(super) struct CommittedCancellationScopeHistory {
    pub preparations: BTreeMap<String, ScopeBoundary>,
    pub deliveries: BTreeMap<u64, ScopeBoundary>,
    pub pending_requests: BTreeMap<String, ScopeRequest>,
}

fn admission(kind: &str) -> Option<(&str, CancellationCallKind)> {
    match kind {
        "ActivityScheduled" => Some(("activity_members", CancellationCallKind::Activity)),
        "TimerScheduled" => Some(("timer_members", CancellationCallKind::Timer)),
        "ChildWorkflowScheduled" => Some(("child_members", CancellationCallKind::Child)),
        "ConditionWaitOpened" => Some(("wait_members", CancellationCallKind::Condition)),
        "SignalWaitOpened" => Some(("wait_members", CancellationCallKind::Signal)),
        _ => None,
    }
}

fn assert_complete_group(
    boundary: &CancellationDelivery,
    scope: &str,
    prefix: &[HistoryEvent],
    scopes: &cancellation_scope::CancellationScopeHistory,
) -> Result<()> {
    let mut members = BTreeSet::new();
    for event in prefix {
        let p = &event.payload;
        let Some(sequence) = p["sequence"]
            .as_u64()
            .filter(|value| *value > 0 && boundary.interrupts(*value))
        else {
            continue;
        };
        if admission(&event.event_type).is_none()
            || (event.event_type == "TimerScheduled"
                && matches!(
                    p["timer_kind"].as_str(),
                    Some("condition_timeout" | "signal_timeout")
                ))
        {
            continue;
        }
        if scopes
            .memberships
            .get(&sequence)
            .map(String::as_str)
            .unwrap_or("root")
            != scope
            || !members.insert(sequence)
        {
            return Err(invalid("scope group changes its original member address"));
        }
        let fallback = vec![p.clone()];
        let path = match p.get("parallel_group_path") {
            None => &fallback,
            Some(Value::Array(path)) if !path.is_empty() && path.iter().all(Value::is_object) => {
                path
            }
            _ => return Err(invalid("scope group requires its original admitted path")),
        };
        if path.iter().any(|entry| {
            entry["parallel_group_mode"].as_str().unwrap_or("all") != "all"
                || entry["parallel_group_id"]
                    .as_str()
                    .unwrap_or_default()
                    .starts_with("select-calls:")
        }) {
            return Err(invalid(
                "scoped selection needs its original selection proof",
            ));
        }
        if path[0]["parallel_group_base_sequence"].as_u64() != Some(boundary.sequence)
            || path[0]["parallel_group_size"].as_u64() != Some(boundary.sequence_span)
            || path[0]["parallel_group_index"].as_u64() != Some(sequence - boundary.sequence)
        {
            return Err(invalid(
                "scope group changes its original range or member index",
            ));
        }
    }
    if members.len() as u64 != boundary.sequence_span {
        return Err(invalid("scope group omits an admitted member"));
    }
    Ok(())
}

impl CommittedCancellationScopeHistory {
    pub fn read(history: &[HistoryEvent], run: &str, workflow: &str) -> Result<Self> {
        let scopes = cancellation_scope::CancellationScopeHistory::read(history, run)?;
        let addresses: BTreeMap<_, _> = scopes
            .openings
            .iter()
            .map(|(sequence, opening)| (opening.scope_id.as_str(), (*sequence, opening)))
            .collect();
        let run_state = CancellationHistory::from_events(history, run, None)?;
        let run_root = run_state
            .request
            .as_ref()
            .and_then(|request| request.context.as_ref())
            .map(ScopedCancellationContext::from_run_context)
            .transpose()?;
        let mut requests: BTreeMap<String, ScopeRequest> = BTreeMap::new();
        let mut request_ids = BTreeSet::new();
        let mut committed = Self::default();
        let mut delivered = BTreeSet::new();
        let mut opened = BTreeSet::new();
        let mut admissions: BTreeMap<&str, BTreeMap<u64, &str>> = BTreeMap::new();
        for (index, event) in history.iter().enumerate() {
            let kind = event.event_type.as_str();
            let p = &event.payload;
            if kind == "CancellationScopeOpened" {
                opened.insert(text(p, "scope_id")?);
            }
            if admission(kind).is_some() && positive(&p["sequence"]) {
                let sequence = p["sequence"].as_u64().unwrap();
                admissions.entry(kind).or_default().insert(
                    sequence,
                    scopes
                        .memberships
                        .get(&sequence)
                        .map(String::as_str)
                        .unwrap_or("root"),
                );
            }
            if !matches!(
                kind,
                "CancellationScopeRequested"
                    | "CancellationScopeDeliveryPrepared"
                    | "CancellationScopeDelivered"
            ) {
                continue;
            }
            let context = ScopedCancellationContext::from_value(&p["cancellation"])?;
            let id = context.scope_id();
            let (opening_sequence, opening) = addresses
                .get(id)
                .ok_or_else(|| invalid("scope request lacks its original opening"))?;
            if !opened.contains(id)
                || run.is_empty()
                || workflow.is_empty()
                || context.workflow_run_id() != run
                || context.workflow_instance_id() != workflow
                || p["workflow_run_id"] != run
                || p["scope_id"] != id
                || p["request_id"] != context.request_id()
            {
                return Err(invalid(
                    "scope cancellation changes its original run, request or address",
                ));
            }
            let time = event_time(event)?;
            if time < context.requested_at() {
                return Err(invalid("scope boundary predates its original request"));
            }
            if kind == "CancellationScopeRequested" {
                if p["schema"] != "durable-workflow.cancellation-scope-request/v1"
                    || p.get("parent_scope_id").is_none()
                    || requests.contains_key(id)
                    || !request_ids.insert(context.request_id().to_owned())
                {
                    return Err(invalid(
                        "scope cancellation requires one accepted original request",
                    ));
                }
                if p["parent_scope_id"].is_null() {
                    if context.lineage().len() != 1
                        || context.request_id() != context.root_context().request_id()
                    {
                        return Err(invalid(
                            "direct scope request substitutes an inherited lineage",
                        ));
                    }
                } else {
                    let parent_id = text(p, "parent_scope_id")?;
                    let parent = if parent_id == "root" && run_state.request_index < index {
                        run_root.as_ref()
                    } else {
                        requests
                            .get(parent_id)
                            .filter(|request| request.history_index < index)
                            .map(|request| &request.context)
                    }
                    .ok_or_else(|| invalid("inherited scope request lacks its earlier parent"))?;
                    if parent.workflow_run_id() != run
                        || parent.workflow_instance_id() != workflow
                        || opening.shield_parent
                        || parent_id != opening.parent_scope_id
                        || context.root_context() != parent.root_context()
                        || context.lineage()[..context.lineage().len() - 1] != *parent.lineage()
                        || context.deadline() != parent.deadline()
                    {
                        return Err(invalid("inherited scope request changes its accepted parent or crosses a shield"));
                    }
                }
                requests.insert(
                    id.to_owned(),
                    ScopeRequest {
                        context,
                        timestamp: time,
                        history_index: index,
                    },
                );
                continue;
            }
            let request = requests
                .get(id)
                .ok_or_else(|| invalid("scope boundary lacks its accepted request"))?;
            if context != request.context || time < request.timestamp {
                return Err(invalid(
                    "scope boundary changes its immutable accepted request",
                ));
            }
            if [
                "sequence_span",
                "operation_sequence",
                "operation_sequence_span",
            ]
            .iter()
            .any(|key| p.get(*key).is_none())
            {
                return Err(invalid("scope boundary omits its original operation range"));
            }
            let mut payload = p.clone();
            payload["workflow_command_id"] = json!(context.request_id());
            let boundary = CancellationDelivery::from_payload(&payload)?;
            let deadline = timestamp(&p["authority_deadline_at"])?;
            if boundary.sequence <= *opening_sequence
                || deadline < context.requested_at()
                || deadline > context.deadline()
                || time > deadline
            {
                return Err(invalid(
                    "scope boundary changes or exceeds its original authority ceiling",
                ));
            }
            if kind == "CancellationScopeDeliveryPrepared" {
                if p["schema"] != "durable-workflow.cancellation-scope-preparation/v5"
                    || committed.preparations.contains_key(id)
                    || delivered.contains(id)
                {
                    return Err(invalid(
                        "scope delivery requires one original v5 preparation",
                    ));
                }
                let prefix = &history[..index];
                for field in FIELDS {
                    if normalize_members(field, &p[field])?
                        != members_from_prefix(field, prefix, id, run)?
                    {
                        return Err(invalid(
                            "scope preparation changes its original member projection",
                        ));
                    }
                }
                if p["descendant_members"] != descendants_from_prefix(prefix, id, run, deadline)? {
                    return Err(invalid(
                        "scope preparation changes its original descendant projection",
                    ));
                }
                let mut by_scope = BTreeMap::from([(id, p)]);
                for member in p["descendant_members"].as_array().unwrap() {
                    by_scope.insert(text(member, "scope_id")?, member);
                }
                let operation_scope = scopes
                    .memberships
                    .get(&boundary.sequence)
                    .map(String::as_str)
                    .unwrap_or(id);
                if !by_scope.contains_key(operation_scope) {
                    return Err(invalid(
                        "scope boundary consumes an operation outside its frozen subtree",
                    ));
                }
                if boundary.call_kind == CancellationCallKind::Parallel {
                    assert_complete_group(&boundary, operation_scope, prefix, &scopes)?;
                } else if !matches!(
                    boundary.call_kind,
                    CancellationCallKind::Activity
                        | CancellationCallKind::LocalActivity
                        | CancellationCallKind::Timer
                        | CancellationCallKind::Condition
                        | CancellationCallKind::Child
                ) {
                    return Err(invalid("scope boundary uses an unsupported operation kind"));
                }
                for (member_scope, projection) in by_scope {
                    for (kind, positions) in &admissions {
                        let (field, mut call_kind) = admission(kind).unwrap();
                        let sequences: BTreeSet<_> = projection[field]
                            .as_array()
                            .unwrap()
                            .iter()
                            .filter(|entry| {
                                !matches!(*kind, "ConditionWaitOpened" | "SignalWaitOpened")
                                    || entry["kind"]
                                        == if *kind == "ConditionWaitOpened" {
                                            "condition"
                                        } else {
                                            "signal"
                                        }
                            })
                            .map(|entry| entry["sequence"].as_u64().unwrap())
                            .collect();
                        if positions.iter().any(|(sequence, owner)| {
                            *owner == member_scope && !sequences.contains(sequence)
                        }) {
                            return Err(invalid("scope preparation omits an admitted operation"));
                        }
                        if positions.get(&boundary.sequence).copied() != Some(member_scope) {
                            continue;
                        }
                        if call_kind == CancellationCallKind::Activity
                            && prefix.iter().any(|row| {
                                row.event_type == *kind
                                    && row.payload["sequence"].as_u64() == Some(boundary.sequence)
                                    && row.payload["local_activity"] == true
                            })
                        {
                            call_kind = CancellationCallKind::LocalActivity;
                        }
                        let timeout =
                            *kind == "TimerScheduled"
                                && projection["wait_members"].as_array().unwrap().iter().any(
                                    |entry| entry["sequence"].as_u64() == Some(boundary.sequence),
                                );
                        if !sequences.contains(&boundary.sequence)
                            || (!timeout
                                && boundary.call_kind != CancellationCallKind::Parallel
                                && boundary.call_kind != call_kind)
                        {
                            return Err(invalid("scope boundary replaces an admitted operation"));
                        }
                    }
                }
                committed.preparations.insert(
                    id.to_owned(),
                    ScopeBoundary {
                        context,
                        boundary,
                        authority_deadline: deadline,
                        event: event.clone(),
                    },
                );
                continue;
            }
            let preparation = committed
                .preparations
                .get(id)
                .ok_or_else(|| invalid("scope delivery lacks its original preparation"))?;
            if p["schema"] != "durable-workflow.cancellation-scope-delivery/v1"
                || delivered.contains(id)
                || committed.deliveries.contains_key(&boundary.sequence)
                || p["preparation_history_event_id"].as_str() != Some(event_id(&preparation.event)?)
                || boundary != preparation.boundary
                || deadline != preparation.authority_deadline
                || time < event_time(&preparation.event)?
            {
                return Err(invalid(
                    "scope delivery changes its original prepared boundary",
                ));
            }
            delivered.insert(id.to_owned());
            committed.deliveries.insert(
                boundary.sequence,
                ScopeBoundary {
                    context,
                    boundary,
                    authority_deadline: deadline,
                    event: event.clone(),
                },
            );
        }
        let mut covered = delivered;
        for delivery in committed.deliveries.values() {
            for member in committed.preparations[delivery.context.scope_id()]
                .event
                .payload["descendant_members"]
                .as_array()
                .unwrap()
            {
                covered.insert(text(member, "scope_id")?.to_owned());
            }
        }
        committed.pending_requests = requests
            .into_iter()
            .filter(|(scope, _)| !covered.contains(scope))
            .collect();
        Ok(committed)
    }

    pub fn pending_request_for_scope<'a>(
        &'a self,
        scope: &str,
        scopes: &cancellation_scope::CancellationScopeHistory,
    ) -> Result<Option<&'a ScopeRequest>> {
        let addresses: BTreeMap<_, _> = scopes
            .openings
            .values()
            .map(|opening| (opening.scope_id.as_str(), opening))
            .collect();
        let mut id = scope;
        let active = self.pending_requests.get(scope);
        let mut request = active;
        while let Some(opening) = addresses.get(id).filter(|opening| !opening.shield_parent) {
            id = &opening.parent_scope_id;
            let Some(ancestor) = self.pending_requests.get(id) else {
                continue;
            };
            if active.is_none_or(|active| {
                ancestor.context.root_context() != active.context.root_context()
                    || !active
                        .context
                        .lineage()
                        .starts_with(ancestor.context.lineage())
            }) {
                return Err(invalid(
                    "pending scope selection changes its original ancestor lineage",
                ));
            }
            request = Some(ancestor);
        }
        Ok(request)
    }
}

/// One original monotonic budget shared by scope preparation, delivery and proof.
#[doc(hidden)]
pub struct CancellationScopeDeliveryBudget {
    expires_at: tokio::time::Instant,
}

impl Default for CancellationScopeDeliveryBudget {
    fn default() -> Self {
        Self::new()
    }
}

impl CancellationScopeDeliveryBudget {
    pub fn new() -> Self {
        Self {
            expires_at: tokio::time::Instant::now() + Duration::from_secs(5),
        }
    }

    /// Narrow the original budget, without granting a new request or lease.
    pub fn restrict(&mut self, authority_deadline: DateTime<Utc>) -> Result<()> {
        let unix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Error::Timeout)?;
        let now = DateTime::<Utc>::from_timestamp(
            i64::try_from(unix.as_secs()).map_err(|_| Error::Timeout)?,
            unix.subsec_nanos(),
        )
        .ok_or(Error::Timeout)?;
        let remaining = (authority_deadline - now)
            .to_std()
            .unwrap_or(Duration::ZERO);
        self.expires_at = self.expires_at.min(tokio::time::Instant::now() + remaining);
        self.remaining()
    }

    fn remaining(&self) -> Result<()> {
        if tokio::time::Instant::now() >= self.expires_at {
            Err(Error::Timeout)
        } else {
            Ok(())
        }
    }
}

/// Proof of the original scope preparation and optional committed delivery.
/// This object does not grant callback execution or result publication authority.
#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct CancellationScopeDeliveryReceipt {
    pub(super) preparation: ScopeBoundary,
    pub(super) delivery: Option<ScopeBoundary>,
    history: Vec<HistoryEvent>,
}

impl CancellationScopeDeliveryReceipt {
    pub fn context(&self) -> &ScopedCancellationContext {
        &self.preparation.context
    }
    pub fn boundary(&self) -> &CancellationDelivery {
        &self.preparation.boundary
    }
    pub fn authority_deadline(&self) -> DateTime<Utc> {
        self.preparation.authority_deadline
    }
    pub fn history(&self) -> &[HistoryEvent] {
        &self.history
    }
    pub fn preparation_history_event_id(&self) -> &str {
        event_id(&self.preparation.event).unwrap()
    }
    pub fn delivery_history_event_id(&self) -> Option<&str> {
        self.delivery
            .as_ref()
            .map(|delivery| event_id(&delivery.event).unwrap())
    }

    fn acknowledge<'a>(receipt: &'a Value, expected: &Value, delivering: bool) -> Result<&'a str> {
        if expected
            .as_object()
            .ok_or_else(|| invalid("missing original scope claim"))?
            .iter()
            .any(|(key, value)| {
                !matches!(key.as_str(), "namespace" | "workflow_instance_id")
                    && receipt.get(key) != Some(value)
            })
            || receipt["prepared"].as_bool() != Some(true)
            || receipt["delivered"].as_bool() != Some(delivering)
            || receipt["claim_released"].as_bool() != Some(false)
            || receipt.get("created_task_ids") != Some(&json!([]))
            || receipt.get("reason") != Some(&Value::Null)
            || (text(receipt, "history_event_id")?
                == text(receipt, "preparation_history_event_id")?)
                == delivering
        {
            return Err(invalid(
                "scope acknowledgement changes its original claim or retained preparation",
            ));
        }
        for field in FIELDS {
            normalize_members(field, &receipt[field])?;
        }
        let context = ScopedCancellationContext::from_value(&receipt["cancellation"])?;
        let authority = timestamp(&receipt["authority_deadline_at"])?;
        if context.workflow_run_id() != text(expected, "workflow_run_id")?
            || context.workflow_instance_id() != text(expected, "workflow_instance_id")?
            || context.scope_id() != text(expected, "scope_id")?
            || context.request_id() != text(expected, "request_id")?
            || authority < context.requested_at()
            || authority > context.deadline()
        {
            return Err(invalid(
                "scope acknowledgement changes its original cancellation authority",
            ));
        }
        receipt["history_refresh_page_token"]
            .as_str()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| {
                invalid("scope acknowledgement lacks an opaque canonical history cursor")
            })
    }

    fn from_history(
        receipt: &Value,
        history: Vec<HistoryEvent>,
        expected: &Value,
        delivering: bool,
    ) -> Result<Self> {
        Self::acknowledge(receipt, expected, delivering)?;
        let mut starts = BTreeSet::new();
        for event in &history {
            if event.raw.get("namespace") != expected.get("namespace") {
                return Err(invalid(
                    "scope receipt history changes its original namespace",
                ));
            }
            if matches!(
                event.event_type.as_str(),
                "StartAccepted" | "WorkflowStarted"
            ) && (!starts.insert(event.event_type.as_str())
                || event.payload.get("workflow_run_id") != expected.get("workflow_run_id")
                || event.payload.get("workflow_instance_id")
                    != expected.get("workflow_instance_id"))
            {
                return Err(invalid(
                    "scope receipt history changes its original workflow start",
                ));
            }
        }
        if !starts.contains("WorkflowStarted") {
            return Err(invalid("scope receipt history omits its workflow start"));
        }
        let committed = CommittedCancellationScopeHistory::read(
            &history,
            text(expected, "workflow_run_id")?,
            text(expected, "workflow_instance_id")?,
        )?;
        let preparation = committed
            .preparations
            .get(text(expected, "scope_id")?)
            .ok_or_else(|| invalid("scope receipt lacks its original committed preparation"))?
            .clone();
        let context = ScopedCancellationContext::from_value(&receipt["cancellation"])?;
        let mut payload = receipt.clone();
        payload["workflow_command_id"] = expected["request_id"].clone();
        let boundary = CancellationDelivery::from_payload(&payload)?;
        if event_id(&preparation.event)? != text(receipt, "preparation_history_event_id")?
            || preparation.context != context
            || preparation.boundary != boundary
            || preparation.authority_deadline != timestamp(&receipt["authority_deadline_at"])?
        {
            return Err(invalid(
                "scope receipt differs from its original committed preparation",
            ));
        }
        for field in FIELDS {
            if normalize_members(field, &preparation.event.payload[field])?
                != normalize_members(field, &receipt[field])?
            {
                return Err(invalid(
                    "scope receipt changes its original frozen projection",
                ));
            }
        }
        let delivery = if delivering {
            let delivery = committed
                .deliveries
                .get(&boundary.sequence)
                .ok_or_else(|| invalid("scope receipt lacks its original committed delivery"))?
                .clone();
            if event_id(&delivery.event)? != text(receipt, "history_event_id")?
                || delivery.context != context
                || delivery.boundary != boundary
            {
                return Err(invalid(
                    "scope receipt changes its original committed delivery",
                ));
            }
            Some(delivery)
        } else {
            None
        };
        Ok(Self {
            preparation,
            delivery,
            history,
        })
    }

    fn assert_original_preparation(&self, original: &Self) -> Result<()> {
        let before = &original.preparation;
        let after = &self.preparation;
        if event_id(&before.event)? != event_id(&after.event)?
            || before.context != after.context
            || before.boundary != after.boundary
            || before.authority_deadline != after.authority_deadline
            || FIELDS
                .into_iter()
                .chain(["descendant_members"])
                .any(|field| before.event.payload[field] != after.event.payload[field])
        {
            return Err(invalid(
                "scope delivery substitutes its previously proved original preparation",
            ));
        }
        Ok(())
    }
}

impl Client {
    /// Candidate scalar preparation proof. All operations share the supplied budget.
    #[doc(hidden)]
    pub async fn prepare_cancellation_scope_on_claim(
        &self,
        task: &WorkflowTask,
        context: &ScopedCancellationContext,
        boundary: &CancellationDelivery,
        budget: &CancellationScopeDeliveryBudget,
    ) -> Result<CancellationScopeDeliveryReceipt> {
        budget.remaining()?;
        let run = task.run_id.as_deref().unwrap_or_default();
        let committed = CommittedCancellationScopeHistory::read(
            &task.history_events,
            run,
            task.workflow_id.as_deref().unwrap_or_default(),
        )?;
        let scopes = cancellation_scope::CancellationScopeHistory::read(&task.history_events, run)?;
        let operation_scope = scopes
            .memberships
            .get(&boundary.sequence)
            .map(String::as_str)
            .unwrap_or(context.scope_id());
        if committed
            .pending_request_for_scope(operation_scope, &scopes)?
            .is_none_or(|request| &request.context != context)
        {
            return Err(invalid(
                "scope preparation lacks its original pending request on this claim",
            ));
        }
        self.cancellation_scope_boundary_on_claim(task, context, boundary, None, budget)
            .await
    }

    /// Candidate scalar delivery requires the earlier original preparation proof.
    #[doc(hidden)]
    pub async fn deliver_cancellation_scope_on_claim(
        &self,
        task: &WorkflowTask,
        original: &CancellationScopeDeliveryReceipt,
        budget: &CancellationScopeDeliveryBudget,
    ) -> Result<CancellationScopeDeliveryReceipt> {
        self.cancellation_scope_boundary_on_claim(
            task,
            original.context(),
            original.boundary(),
            Some(original),
            budget,
        )
        .await
    }

    async fn cancellation_scope_boundary_on_claim(
        &self,
        task: &WorkflowTask,
        context: &ScopedCancellationContext,
        boundary: &CancellationDelivery,
        original: Option<&CancellationScopeDeliveryReceipt>,
        budget: &CancellationScopeDeliveryBudget,
    ) -> Result<CancellationScopeDeliveryReceipt> {
        let (owner, run) = cooperative_cancellation::cancellation_claim(task)?;
        let workflow = task.workflow_id.as_deref().unwrap_or_default();
        if [
            task.task_id.as_str(),
            owner,
            run,
            workflow,
            context.scope_id(),
            context.request_id(),
            self.namespace.as_str(),
        ]
        .iter()
        .any(|value| !identity(&json!(value)))
            || context.scope_id() == "root"
            || context.workflow_run_id() != run
            || context.workflow_instance_id() != workflow
            || boundary.request_id != context.request_id()
            || boundary.sequence_span != 1
            || boundary.operation_sequence.is_some()
            || boundary.operation_sequence_span != 1
            || !matches!(
                boundary.call_kind,
                CancellationCallKind::Activity
                    | CancellationCallKind::LocalActivity
                    | CancellationCallKind::Timer
                    | CancellationCallKind::Condition
                    | CancellationCallKind::Child
            )
        {
            return Err(invalid(
                "scope boundary requires its original claim and scalar durable call",
            ));
        }
        budget.remaining()?;
        let body = json!({"scope_id":context.scope_id(), "request_id":context.request_id(),
            "sequence":boundary.sequence, "call_kind":boundary.call_kind, "sequence_span":boundary.sequence_span,
            "operation_sequence":boundary.operation_sequence, "operation_sequence_span":boundary.operation_sequence_span,
            "lease_owner":owner, "workflow_task_attempt":task.workflow_task_attempt});
        let mut checked = body.clone();
        checked["workflow_command_id"] = json!(boundary.request_id);
        CancellationDelivery::from_payload(&checked)?;
        let mut expected = body.clone();
        expected["task_id"] = json!(task.task_id);
        expected["workflow_run_id"] = json!(run);
        expected["workflow_instance_id"] = json!(workflow);
        expected["namespace"] = json!(self.namespace);
        let path = format!(
            "/worker/workflow-tasks/{}",
            percent_encode_path_segment(&task.task_id)
        );
        let delivering = original.is_some();
        tokio::time::timeout_at(budget.expires_at, async {
            let boundary_path = format!("{path}/cancellation-scopes/{}", if delivering { "deliver" } else { "prepare" });
            let request = || self.request_json::<Value, _>(reqwest::Method::POST, &boundary_path,
                RequestProtocol::Worker("1.20"), Some(&body));
            let receipt = match request().await {
                Err(error) if worker_operation_is_retryable(&error) => request().await?,
                result => result?,
            };
            let mut token = Some(CancellationScopeDeliveryReceipt::acknowledge(&receipt, &expected, delivering)?.to_owned());
            let mut seen = BTreeSet::new();
            let mut history = Vec::new();
            while let Some(current) = token.take() {
                budget.remaining()?;
                if seen.len() >= 128 || !seen.insert(current.clone()) { return Err(invalid("scope history repeated or exceeded its opaque cursors")); }
                let page: Value = self.request_json(reqwest::Method::POST, &format!("{path}/history"),
                    RequestProtocol::Worker("1.20"), Some(&json!({"lease_owner":owner,
                        "workflow_task_attempt":task.workflow_task_attempt, "next_history_page_token":current,
                        "history_page_size":WORKFLOW_HISTORY_PAGE_SIZE}))).await?;
                if page["task_id"].as_str() != Some(task.task_id.as_str())
                    || page["workflow_task_attempt"].as_u64() != Some(task.workflow_task_attempt) {
                    return Err(invalid("scope history page changes its original claim"));
                }
                let batch = page["history_events"].as_array().ok_or_else(|| invalid("scope history page lacks canonical events"))?;
                if batch.len() > WORKFLOW_HISTORY_PAGE_SIZE as usize || batch.iter().any(|event| !event.is_object()) {
                    return Err(invalid("scope history page exceeds its bounded complete shape"));
                }
                token = match page.get("next_history_page_token") {
                    Some(Value::Null) => None,
                    Some(Value::String(next)) if !next.trim().is_empty() && !batch.is_empty() => Some(next.clone()),
                    _ => return Err(invalid("scope history page omits its terminal cursor")),
                };
                for event in batch { history.push(serde_json::from_value(event.clone()).map_err(|_| invalid("scope history event is malformed"))?); }
            }
            let proved = CancellationScopeDeliveryReceipt::from_history(&receipt, history, &expected, delivering)?;
            if let Some(original) = original { proved.assert_original_preparation(original)?; }
            budget.remaining()?;
            Ok(proved)
        }).await.map_err(|_| Error::Timeout)?
    }
}
