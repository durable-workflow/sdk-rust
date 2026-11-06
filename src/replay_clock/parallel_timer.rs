use super::parallel::{all_path, event_indices, member_path, Member, Members};
use super::{HistoryEvent, RecordedCommand, Value};
use crate::ParallelGroupMetadata;

fn member(
    history: &[HistoryEvent],
    indices: &[usize],
    sequence: u64,
    delay: u64,
    path: &[ParallelGroupMetadata],
) -> Option<Member> {
    if !member_path(history, indices, sequence, path) {
        return None;
    }
    let [scheduled, fired] = indices else {
        return None;
    };
    if history[*scheduled].event_type != "TimerScheduled"
        || history[*fired].event_type != "TimerFired"
    {
        return None;
    }
    let timer = history[*scheduled]
        .payload
        .get("timer_id")?
        .as_str()
        .filter(|value| !value.is_empty())?;
    for &index in indices {
        let event = &history[index];
        if event.payload.get("timer_id").and_then(Value::as_str) != Some(timer)
            || event.payload.get("delay_seconds").and_then(Value::as_u64) != Some(delay)
            || event
                .payload
                .get("timer_kind")
                .is_some_and(|value| !value.is_null())
            || crate::is_internal_timer_event(event)
        {
            return None;
        }
    }
    Some(Member {
        path: path.to_vec(),
        identity: timer.into(),
        terminal: Some(*fired),
        failed: false,
    })
}

pub(super) fn members(history: &[HistoryEvent], commands: &[RecordedCommand]) -> Members {
    let events = event_indices(history, "Timer");
    Members(
        commands
            .iter()
            .filter_map(|command| {
                let RecordedCommand::Timer {
                    sequence,
                    delay_seconds,
                    fired: true,
                    parallel_group_path: Some(path),
                } = command
                else {
                    return None;
                };
                if !all_path(path, "timer") {
                    return None;
                }
                member(
                    history,
                    events.get(sequence)?,
                    *sequence,
                    *delay_seconds,
                    path,
                )
                .map(|member| (*sequence, member))
            })
            .collect(),
    )
}
