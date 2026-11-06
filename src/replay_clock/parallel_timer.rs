use super::parallel_activity::{all_path, event_time, milliseconds};
use super::{durable_event_sequence, DateTime, HashMap, HistoryEvent, RecordedCommand, Utc, Value};
use crate::{recorded_parallel_group_path, ParallelGroupMetadata};
use std::collections::HashSet;

#[derive(Debug)]
struct Member {
    path: Vec<ParallelGroupMetadata>,
    timer: String,
    fired: usize,
}

#[derive(Debug)]
pub(super) struct Members(HashMap<u64, Member>);

fn member(
    history: &[HistoryEvent],
    indices: &[usize],
    sequence: u64,
    delay: u64,
    path: &[ParallelGroupMetadata],
) -> Option<Member> {
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
        if event.payload.get("sequence").and_then(Value::as_u64) != Some(sequence)
            || event
                .payload
                .get("workflow_sequence")
                .is_some_and(|value| value.as_u64() != Some(sequence))
            || event.payload.get("timer_id").and_then(Value::as_str) != Some(timer)
            || event.payload.get("delay_seconds").and_then(Value::as_u64) != Some(delay)
            || event
                .payload
                .get("timer_kind")
                .is_some_and(|value| !value.is_null())
            || crate::is_internal_timer_event(event)
            || recorded_parallel_group_path(&[event], sequence)
                .ok()
                .flatten()
                .as_deref()
                != Some(path)
        {
            return None;
        }
    }
    Some(Member {
        path: path.to_vec(),
        timer: timer.into(),
        fired: *fired,
    })
}

impl Members {
    pub(super) fn new(history: &[HistoryEvent], commands: &[RecordedCommand]) -> Self {
        let mut events: HashMap<u64, Vec<usize>> = HashMap::new();
        for (index, event) in history.iter().enumerate() {
            if event.event_type.starts_with("Timer") {
                if let Some(sequence) = durable_event_sequence(event) {
                    events.entry(sequence).or_default().push(index);
                }
            }
        }
        Self(
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

    pub(super) fn consumed_time(
        &self,
        history: &[HistoryEvent],
        sequences: &[u64],
        failed: Option<u64>,
    ) -> Option<DateTime<Utc>> {
        if failed.is_some() {
            return None;
        }
        let root = self.0.get(sequences.first()?)?.path.first()?;
        if root.parallel_group_base_sequence != *sequences.first()?
            || root.parallel_group_size != sequences.len()
        {
            return None;
        }
        let mut timers = HashSet::new();
        let mut selected: Option<(i64, DateTime<Utc>)> = None;
        for (index, sequence) in sequences.iter().enumerate() {
            let member = self.0.get(sequence)?;
            let mut expected = root.clone();
            expected.parallel_group_index = index;
            if member.path.first()? != &expected
                || root
                    .parallel_group_base_sequence
                    .checked_add(index as u64)?
                    != *sequence
                || !timers.insert(&member.timer)
            {
                return None;
            }
            let time = event_time(history.get(member.fired)?)?;
            let ms = milliseconds(time)?;
            if selected.is_none_or(|(current, _)| ms > current) {
                selected = Some((ms, time));
            }
        }
        selected.map(|(_, time)| time)
    }
}
