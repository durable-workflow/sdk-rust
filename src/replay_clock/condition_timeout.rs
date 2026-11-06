use super::{HashMap, HistoryEvent, Value};
use crate::recorded_parallel_group_path;

fn counter(event: &HistoryEvent, sequence: u64) -> bool {
    event.payload.get("sequence").and_then(Value::as_u64) == Some(sequence)
        && event
            .payload
            .get("workflow_sequence")
            .is_none_or(|value| value.as_u64() == Some(sequence))
}

pub(super) struct Index<'a> {
    timers_by_wait: HashMap<&'a str, Vec<usize>>,
    timers_by_id: HashMap<&'a str, Vec<usize>>,
    conditions_by_wait: HashMap<&'a str, Vec<usize>>,
    opens_by_occurrence: HashMap<&'a str, Vec<usize>>,
}

fn link<'a>(
    map: &mut HashMap<&'a str, Vec<usize>>,
    event: &'a HistoryEvent,
    key: &str,
    position: usize,
) {
    if let Some(value) = event.payload.get(key).and_then(Value::as_str) {
        map.entry(value).or_default().push(position);
    }
}

impl<'a> Index<'a> {
    pub(super) fn new(history: &'a [HistoryEvent]) -> Self {
        let mut index = Self {
            timers_by_wait: HashMap::new(),
            timers_by_id: HashMap::new(),
            conditions_by_wait: HashMap::new(),
            opens_by_occurrence: HashMap::new(),
        };
        for (position, event) in history.iter().enumerate() {
            if event.event_type.starts_with("Timer") {
                link(
                    &mut index.timers_by_wait,
                    event,
                    "condition_wait_id",
                    position,
                );
                link(&mut index.timers_by_id, event, "timer_id", position);
            }
            if event.event_type.starts_with("ConditionWait") {
                link(
                    &mut index.conditions_by_wait,
                    event,
                    "condition_wait_id",
                    position,
                );
            }
            if event.event_type == "ConditionWaitOpened" {
                link(
                    &mut index.opens_by_occurrence,
                    event,
                    "condition_wait_occurrence_id",
                    position,
                );
            }
        }
        index
    }

    pub(super) fn boundary(
        &self,
        history: &[HistoryEvent],
        indices: &[usize],
        sequence: u64,
    ) -> Option<usize> {
        // A persisted explicit resolution has its own, different Core branch.
        let [opened] = indices else { return None };
        let open = &history[*opened];
        if open.event_type != "ConditionWaitOpened" || !counter(open, sequence) {
            return None;
        }
        let identity = [
            "condition_wait_id",
            "condition_wait_occurrence_id",
            "condition_key",
            "condition_definition_fingerprint",
        ];
        for key in identity {
            open.payload
                .get(key)?
                .as_str()
                .filter(|value| !value.is_empty())?;
        }
        let seconds = open.payload.get("timeout_seconds")?.as_u64()?;
        let wait = open.payload.get("condition_wait_id")?.as_str()?;
        if self.conditions_by_wait.get(wait)?.as_slice() != indices {
            return None;
        }
        let timers = self.timers_by_wait.get(wait)?;
        let [scheduled, fired] = timers.as_slice() else {
            return None;
        };
        if !(*opened < *scheduled && *scheduled < *fired)
            || history[*scheduled].event_type != "TimerScheduled"
            || history[*fired].event_type != "TimerFired"
        {
            return None;
        }
        let timer = history[*scheduled]
            .payload
            .get("timer_id")?
            .as_str()
            .filter(|value| !value.is_empty())?;
        for &index in timers {
            let event = &history[index];
            if !counter(event, sequence)
                || event.payload.get("timer_id").and_then(Value::as_str) != Some(timer)
                || event.payload.get("timer_kind").and_then(Value::as_str)
                    != Some("condition_timeout")
                || event.payload.get("delay_seconds").and_then(Value::as_u64) != Some(seconds)
                || recorded_parallel_group_path(&[event], sequence)
                    .ok()?
                    .is_some()
            {
                return None;
            }
            for key in identity {
                if event.payload.get(key) != open.payload.get(key) {
                    return None;
                }
            }
        }
        // A mislabeled duplicate of this timer must not disappear into another
        // wait's bucket. This is an identity refusal, never a future-time maximum.
        if self.timers_by_id.get(timer)? != timers {
            return None;
        }
        let occurrence = open.payload.get("condition_wait_occurrence_id")?.as_str()?;
        let opens = self.opens_by_occurrence.get(occurrence)?;
        let next = opens.partition_point(|&index| index <= *opened);
        if opens.get(next).is_some_and(|&index| index < *fired) {
            // A later physical open superseded this wait before its old timer
            // fired. The legacy reopen resolver decides the final call outcome.
            return None;
        }
        Some(*fired)
    }
}
