use super::{
    durable_event_sequence, parse, DateTime, HashMap, HistoryEvent, RecordedCommand, Utc, Value,
};
use crate::{recorded_parallel_group_path, ParallelGroupMetadata};
use std::collections::HashSet;

#[derive(Debug)]
struct Member {
    path: Vec<ParallelGroupMetadata>,
    execution: String,
    terminal: Option<usize>,
    failed: bool,
}

#[derive(Debug)]
pub(super) struct Members(HashMap<u64, Member>);

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

fn member(
    history: &[HistoryEvent],
    indices: &[usize],
    sequence: u64,
    path: &[ParallelGroupMetadata],
) -> Option<Member> {
    let events: Vec<_> = indices.iter().map(|&index| &history[index]).collect();
    let schedules: Vec<_> = events
        .iter()
        .filter(|event| event.event_type == "ActivityScheduled")
        .collect();
    let [schedule] = schedules.as_slice() else {
        return None;
    };
    let execution = schedule
        .payload
        .get("activity_execution_id")?
        .as_str()
        .filter(|s| !s.is_empty())?;
    let activity = schedule
        .payload
        .get("activity_type")?
        .as_str()
        .filter(|s| !s.is_empty())?;
    let mut terminal = None;
    for &index in indices {
        let event = &history[index];
        if event.payload.get("sequence").and_then(Value::as_u64) != Some(sequence)
            || event
                .payload
                .get("workflow_sequence")
                .is_some_and(|value| value.as_u64() != Some(sequence))
            || event
                .payload
                .get("activity_execution_id")
                .and_then(Value::as_str)
                != Some(execution)
            || event.payload.get("activity_type").and_then(Value::as_str) != Some(activity)
            || recorded_parallel_group_path(&[event], sequence)
                .ok()
                .flatten()
                .as_deref()
                != Some(path)
        {
            return None;
        }
        if matches!(
            event.event_type.as_str(),
            "ActivityCompleted" | "ActivityFailed" | "ActivityTimedOut" | "ActivityCancelled"
        ) {
            // Legacy identical delivery may retain the result, but two rows
            // cannot certify a unique consumed event's time.
            if terminal.replace(index).is_some() || index <= *indices.first()? {
                return None;
            }
        }
    }
    if history[*indices.first()?].event_type != "ActivityScheduled" {
        return None;
    }
    Some(Member {
        path: path.to_vec(),
        execution: execution.into(),
        terminal,
        failed: terminal.is_some_and(|index| history[index].event_type != "ActivityCompleted"),
    })
}

// Carbon 3.15.1 Timestamp::getTimestampMs calls round(microseconds / 1000),
// not truncation. Only accept the range where its float microsecond input is
// exact, and the persisted microsecond precision the Core carrier represents.
pub(super) fn milliseconds(time: DateTime<Utc>) -> Option<i64> {
    let micros = time.timestamp_micros();
    if micros.unsigned_abs() > (1_u64 << 53) || time.timestamp_subsec_nanos() % 1000 != 0 {
        return None;
    }
    Some((micros as f64 / 1000.0).round() as i64)
}

pub(super) fn event_time(event: &HistoryEvent) -> Option<DateTime<Utc>> {
    match event.raw.get("recorded_at") {
        None | Some(Value::Null) => parse(event.raw.get("created_at")),
        present => parse(present),
    }
}

impl Members {
    pub(super) fn new(history: &[HistoryEvent], commands: &[RecordedCommand]) -> Self {
        let mut events: HashMap<u64, Vec<usize>> = HashMap::new();
        for (index, event) in history.iter().enumerate() {
            if event.event_type.starts_with("Activity") {
                if let Some(sequence) = durable_event_sequence(event) {
                    events.entry(sequence).or_default().push(index);
                }
            }
        }
        Self(
            commands
                .iter()
                .filter_map(|command| {
                    let RecordedCommand::Activity {
                        sequence,
                        parallel_group_path: Some(path),
                        ..
                    } = command
                    else {
                        return None;
                    };
                    if !all_path(path, "activity") {
                        return None;
                    }
                    member(history, events.get(sequence)?, *sequence, path)
                        .map(|member| (*sequence, member))
                })
                .collect(),
        )
    }

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
        let mut executions = HashSet::new();
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
                || !executions.insert(&member.execution)
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
                let (ms, core_selected) = failure?;
                // Keep the legacy exception even when Core selects a different
                // failed member. That compatibility gap has no clock authority.
                (selected == core_selected)
                    .then(|| DateTime::from_timestamp_millis(ms))
                    .flatten()
            }
            None if failure.is_none() => success.map(|(_, time)| time),
            None => None,
        }
    }
}
