//! Portable v1 group time. Legacy Carbon observations remain separate evidence.
use super::{durable_event_sequence, parse, DateTime, HashMap, HistoryEvent, Utc, Value};
use crate::{recorded_parallel_group_path, ParallelGroupMetadata};
use std::collections::HashSet;

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

pub(super) fn member_path(
    history: &[HistoryEvent],
    indices: &[usize],
    sequence: u64,
    path: &[ParallelGroupMetadata],
) -> bool {
    indices.iter().all(|&index| {
        let event = &history[index];
        event.payload.get("sequence").and_then(Value::as_u64) == Some(sequence)
            && event
                .payload
                .get("workflow_sequence")
                .is_none_or(|value| value.as_u64() == Some(sequence))
            && recorded_parallel_group_path(&[event], sequence)
                .ok()
                .flatten()
                .as_deref()
                == Some(path)
    })
}

#[derive(Debug)]
pub(super) struct Member {
    pub(super) path: Vec<ParallelGroupMetadata>,
    pub(super) identity: String,
    pub(super) terminal: Option<usize>,
    pub(super) failed: bool,
}

#[derive(Debug)]
pub(super) struct Members(pub(super) HashMap<u64, Member>);

impl Members {
    pub(super) fn consumed_time(
        &self,
        history: &[HistoryEvent],
        sequences: &[u64],
        failed: Option<u64>,
    ) -> Option<DateTime<Utc>> {
        let root = self.0.get(sequences.first()?)?.path.first()?;
        if root.parallel_group_base_sequence != *sequences.first()?
            || root.parallel_group_size != sequences.len()
        {
            return None;
        }
        let mut identities = HashSet::new();
        let mut success: Option<(i64, DateTime<Utc>)> = None;
        let mut failure: Option<(i64, u64)> = None;
        for (index, sequence) in sequences.iter().enumerate() {
            let member = self.0.get(sequence)?;
            let mut expected = root.clone();
            expected.parallel_group_index = index;
            if member.path.first()? != &expected
                || root
                    .parallel_group_base_sequence
                    .checked_add(index as u64)?
                    != *sequence
                || !identities.insert(&member.identity)
            {
                return None;
            }
            if member.failed {
                let ms = milliseconds(event_time(history.get(member.terminal?)?)?)?;
                // Descriptor iteration retains the first member on a tie.
                if failure.is_none_or(|(current, _)| ms < current) {
                    failure = Some((ms, *sequence));
                }
            } else if failed.is_none() {
                let time = event_time(history.get(member.terminal?)?)?;
                let ms = milliseconds(time)?;
                if success.is_none_or(|(current, _)| ms > current) {
                    success = Some((ms, time));
                }
            }
        }
        match failed {
            Some(selected) => {
                let (ms, canonical_selected) = failure?;
                // Keep the legacy exception even when v1 selects a different
                // failed member. That compatibility gap has no clock authority.
                (selected == canonical_selected)
                    .then(|| DateTime::from_timestamp_millis(ms))
                    .flatten()
            }
            None if failure.is_none() => success.map(|(_, time)| time),
            None => None,
        }
    }
}
