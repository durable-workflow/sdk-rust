//! Additive business time, independent of cancellation's monotonic budget clock.

use super::{
    durable_event_sequence, is_internal_timer_event, recorded_commands, DurableOperationHandle,
    Error, HistoryEvent, ParallelGroupMetadata, QueryContext, RecordedCommand, Result, Value,
    WorkflowContext, WorkflowIdentity, WorkflowState,
};
use chrono::{DateTime, Utc};
use std::{
    cell::RefCell,
    collections::HashMap,
    sync::{Arc, Mutex, Weak},
};

thread_local! {
    static ACTIVE: RefCell<Option<Weak<Mutex<WorkflowState>>>> = const { RefCell::new(None) };
}

pub(super) struct Guard(Option<Weak<Mutex<WorkflowState>>>);

impl Guard {
    pub(super) fn enter(context: &WorkflowContext) -> Self {
        Self(ACTIVE.with(|active| active.replace(Some(Arc::downgrade(&context.state)))))
    }

    pub(super) fn unavailable() -> Self {
        Self(ACTIVE.with(|active| active.replace(None)))
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        ACTIVE.with(|active| active.replace(self.0.take()));
    }
}

fn unavailable() -> Error {
    Error::ReplayClockUnavailable
}

fn parse(value: Option<&Value>) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value?.as_str()?)
        .ok()
        .map(|time| time.with_timezone(&Utc))
}

fn recorded_time(event: &HistoryEvent, reused: bool) -> Option<DateTime<Utc>> {
    if reused && event.payload.get("reused_recorded_at").is_some() {
        return parse(event.payload.get("reused_recorded_at"));
    }
    // A present but invalid field cannot borrow another event's time.
    parse(
        event
            .raw
            .get("timestamp")
            .or_else(|| event.raw.get("recorded_at")),
    )
}

fn start_time(history: &[HistoryEvent]) -> Option<DateTime<Utc>> {
    let mut starts = history
        .iter()
        .filter(|event| event.event_type == "WorkflowStarted");
    let start = starts.next()?;
    if starts.next().is_some() || start.payload["recovery_kind"] != "redrive" {
        // Ordinary Core entry uses run.started_at, not the history insertion
        // time. The current worker task has no authoritative carrier for it.
        return None;
    }
    parse(start.payload.get("replayed_started_at"))
}

#[derive(Debug)]
pub(super) struct Clock {
    time: Option<DateTime<Utc>>,
    boundaries: HashMap<u64, usize>,
}

impl Clock {
    pub(super) fn new(history: &[HistoryEvent], commands: &[RecordedCommand]) -> Self {
        let supported: HashMap<_, _> = commands
            .iter()
            .filter_map(|command| match command {
                RecordedCommand::Activity {
                    sequence,
                    outcome: Some(_),
                    parallel_group_path,
                    ..
                } if parallel_group_path.as_ref().is_none_or(Vec::is_empty) => {
                    Some((*sequence, true))
                }
                RecordedCommand::Timer {
                    sequence,
                    fired: true,
                    parallel_group_path,
                    ..
                } if parallel_group_path.as_ref().is_none_or(Vec::is_empty) => {
                    Some((*sequence, false))
                }
                _ => None,
            })
            .collect();
        let mut boundaries = HashMap::new();
        for (index, event) in history.iter().enumerate() {
            let Some(sequence) = durable_event_sequence(event) else {
                continue;
            };
            let matched = match supported.get(&sequence) {
                Some(true) => matches!(
                    event.event_type.as_str(),
                    "ActivityCompleted"
                        | "ActivityFailed"
                        | "ActivityTimedOut"
                        | "ActivityCancelled"
                ),
                Some(false) => event.event_type == "TimerFired" && !is_internal_timer_event(event),
                None => false,
            };
            if matched {
                boundaries.entry(sequence).or_insert(index);
            }
        }
        // Index identities once. Timestamps are read only when that boundary
        // is actually consumed, never by scanning ahead from the accessor.
        Self {
            time: start_time(history),
            boundaries,
        }
    }

    pub(super) fn unavailable(&mut self) {
        self.time = None;
    }
}

impl WorkflowState {
    pub(super) fn consume_replay_sequence(
        &mut self,
        sequence: u64,
        path: &[ParallelGroupMetadata],
    ) {
        if path.is_empty() {
            self.observe_replay_sequence(sequence);
        } else {
            self.replay_clock.unavailable();
        }
        self.advance_cancellation_sequence(sequence, path);
    }

    pub(super) fn consume_unproven_boundary(&mut self, path: &[ParallelGroupMetadata]) {
        self.replay_clock.unavailable();
        self.unavailable_cancellation_boundary(path);
    }

    pub(super) fn consume_replay_group(&mut self, sequences: &[u64], failed: Option<u64>) {
        self.replay_clock.unavailable();
        self.advance_cancellation_sequences(sequences, failed);
    }

    pub(super) fn consume_replay_selection(&mut self, group: &str) {
        self.replay_clock.unavailable();
        self.advance_cancellation_selection(group);
    }

    pub(super) fn consume_replay_receipt(&mut self, group: &str, sequence: u64) {
        self.replay_clock.unavailable();
        self.advance_cancellation_receipt(group, sequence);
    }

    pub(super) fn consume_replay_handle(&mut self, handle: &DurableOperationHandle, failed: bool) {
        self.replay_clock.unavailable();
        self.advance_cancellation_handle(handle, failed);
    }

    pub(super) fn replay_time(&self) -> Result<DateTime<Utc>> {
        // Cancellation keeps its existing separate semantics. This first
        // cohort does not claim business-clock correlation for its callbacks.
        if self.cancellation_clock.is_some() {
            return Err(unavailable());
        }
        self.replay_clock.time.ok_or_else(unavailable)
    }

    pub(super) fn observe_replay_sequence(&mut self, sequence: u64) {
        self.replay_clock.time = self
            .replay_clock
            .boundaries
            .get(&sequence)
            .and_then(|index| self.history_events.get(*index))
            .and_then(|event| {
                recorded_time(
                    event,
                    matches!(
                        event.event_type.as_str(),
                        "ActivityCompleted" | "TimerFired"
                    ),
                )
            });
    }
}

impl WorkflowContext {
    /// Recorded business time at the last supported consumed replay boundary.
    ///
    /// Repeated reads at an unchanged prefix are equal. This never reads wall
    /// time, creates a command, or selects an unconsumed future event. Missing
    /// or invalid evidence returns [`Error::ReplayClockUnavailable`]. The clock
    /// is available only while this workflow's handler/future is executing.
    ///
    /// This initial source cohort supports correlated scalar activity/timer
    /// results and the explicit redrive start anchor. Ordinary start authority,
    /// inline metadata/local completions and grouped/other operation families
    /// remain unavailable. Cancellation's existing budget clock is unchanged.
    /// Migrate business decisions through normal workflow versioning. Event
    /// timestamps do not provide a total order; retain stream/counter identity.
    pub fn replay_now(&self) -> Result<DateTime<Utc>> {
        let active = ACTIVE.with(|active| active.borrow().as_ref().and_then(Weak::upgrade));
        if !active.is_some_and(|active| Arc::ptr_eq(&active, &self.state)) {
            return Err(unavailable());
        }
        self.state
            .lock()
            .map_err(|_| Error::WorkflowStatePoisoned)?
            .replay_time()
    }
}

impl QueryContext {
    /// Recorded time of this immutable, engine-correlated callback prefix.
    ///
    /// Missing, invalid or unproved boundary evidence is explicitly unavailable.
    /// Reading this accessor does not change the snapshot or append history.
    pub fn replay_now(&self) -> Result<DateTime<Utc>> {
        self.replay_time.ok_or_else(unavailable)
    }
}

pub(super) fn snapshot_time(history: &[HistoryEvent], codec: &str) -> Option<DateTime<Utc>> {
    let event = history.last()?;
    if event.event_type == "WorkflowStarted" {
        return start_time(history);
    }
    if !matches!(
        event.event_type.as_str(),
        "ActivityCompleted"
            | "ActivityFailed"
            | "ActivityTimedOut"
            | "ActivityCancelled"
            | "TimerFired"
    ) {
        return None;
    }
    let commands = recorded_commands(
        history,
        codec,
        WorkflowIdentity {
            workflow_id: None,
            run_id: None,
        },
    )
    .ok()?;
    Clock::new(history, &commands)
        .boundaries
        .get(&durable_event_sequence(event)?)
        .and_then(|index| history.get(*index))
        .and_then(|event| {
            recorded_time(
                event,
                matches!(
                    event.event_type.as_str(),
                    "ActivityCompleted" | "TimerFired"
                ),
            )
        })
}

pub(super) fn update_time(history: &[HistoryEvent], update_id: &str) -> Option<DateTime<Utc>> {
    let mut events = history.iter().filter(|event| {
        event.event_type == "UpdateAccepted"
            && event.payload["update_id"].as_str() == Some(update_id)
    });
    let event = events.next()?;
    if events.next().is_some() {
        return None;
    }
    recorded_time(event, true)
}

#[cfg(test)]
mod tests;
