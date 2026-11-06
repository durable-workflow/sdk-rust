use super::{HistoryEvent, Value};

pub(super) fn boundary(
    history: &[HistoryEvent],
    indices: &[usize],
    sequence: u64,
) -> Option<usize> {
    let [opened, resolved] = indices else {
        return None;
    };
    let open = &history[*opened];
    let resolution = &history[*resolved];
    if open.event_type != "ConditionWaitOpened"
        || !matches!(
            resolution.event_type.as_str(),
            "ConditionWaitSatisfied" | "ConditionWaitTimedOut"
        )
    {
        return None;
    }
    for event in [open, resolution] {
        if event.payload.get("sequence").and_then(Value::as_u64) != Some(sequence)
            || event
                .payload
                .get("workflow_sequence")
                .is_some_and(|v| v.as_u64() != Some(sequence))
        {
            return None;
        }
    }
    for key in [
        "condition_wait_id",
        "condition_wait_occurrence_id",
        "condition_key",
        "condition_definition_fingerprint",
    ] {
        let identity = open.payload.get(key)?.as_str().filter(|v| !v.is_empty())?;
        if resolution.payload.get(key).and_then(Value::as_str) != Some(identity) {
            return None;
        }
    }
    let timeout = |event: &HistoryEvent| match event.payload.get("timeout_seconds") {
        None | Some(Value::Null) => Some(None),
        Some(value) => value.as_u64().map(Some),
    };
    let seconds = timeout(open)?;
    if timeout(resolution)? != seconds
        || (resolution.event_type == "ConditionWaitTimedOut" && seconds.is_none())
    {
        return None;
    }
    // The legacy parser already validates timeout backing and results. Only
    // the consumed persisted resolution supplies time; a TimerFired alone
    // cannot stand in for Core's resumeConditionResolution event.
    Some(*resolved)
}
