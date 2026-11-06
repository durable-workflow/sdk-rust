use super::parallel::{all_path, event_indices, member_path, Member, Members};
use super::{HistoryEvent, RecordedCommand, Value};
use crate::ParallelGroupMetadata;

fn member(
    history: &[HistoryEvent],
    indices: &[usize],
    sequence: u64,
    path: &[ParallelGroupMetadata],
) -> Option<Member> {
    if !member_path(history, indices, sequence, path) {
        return None;
    }
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
        if event
            .payload
            .get("activity_execution_id")
            .and_then(Value::as_str)
            != Some(execution)
            || event.payload.get("activity_type").and_then(Value::as_str) != Some(activity)
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
        identity: execution.into(),
        terminal,
        failed: terminal.is_some_and(|index| history[index].event_type != "ActivityCompleted"),
    })
}

pub(super) fn members(history: &[HistoryEvent], commands: &[RecordedCommand]) -> Members {
    let events = event_indices(history, "Activity");
    Members(
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
