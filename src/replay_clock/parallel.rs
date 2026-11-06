//! Portable v1 group time. Legacy Carbon observations remain separate evidence.
use super::{durable_event_sequence, parse, DateTime, HashMap, HistoryEvent, Utc, Value};
use crate::ParallelGroupMetadata;

pub(super) fn all_path(path: &[ParallelGroupMetadata], kind: &str) -> bool {
    !path.is_empty()
        && path.iter().all(|entry| {
            entry.parallel_group_kind == kind
                && entry
                    .parallel_group_mode
                    .as_deref()
                    .is_none_or(|mode| mode == "all")
                && entry.selection_member_key.is_none()
                && entry.selection_member_index.is_none()
                && entry.selection_member_base_sequence.is_none()
                && entry.selection_member_size.is_none()
                && entry.selection_member_kind.is_none()
        })
}

pub(super) fn round_microseconds(micros: i64) -> Option<i64> {
    let magnitude = micros.unsigned_abs().checked_add(500)? / 1000;
    let milliseconds = i64::try_from(magnitude).ok()?;
    if micros < 0 {
        milliseconds.checked_neg()
    } else {
        Some(milliseconds)
    }
}

pub(super) fn milliseconds(time: DateTime<Utc>) -> Option<i64> {
    if time.timestamp_subsec_nanos() % 1000 != 0 {
        return None;
    }
    let micros = time
        .timestamp()
        .checked_mul(1_000_000)?
        .checked_add(i64::from(time.timestamp_subsec_micros()))?;
    round_microseconds(micros)
}

pub(super) fn event_time(event: &HistoryEvent) -> Option<DateTime<Utc>> {
    match event.raw.get("recorded_at") {
        None | Some(Value::Null) => parse(event.raw.get("created_at")),
        present => parse(present),
    }
}

pub(super) fn event_indices(history: &[HistoryEvent], prefix: &str) -> HashMap<u64, Vec<usize>> {
    let mut events: HashMap<u64, Vec<usize>> = HashMap::new();
    for (index, event) in history.iter().enumerate() {
        if event.event_type.starts_with(prefix) {
            if let Some(sequence) = durable_event_sequence(event) {
                events.entry(sequence).or_default().push(index);
            }
        }
    }
    events
}
