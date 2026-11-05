//! Additive business time, independent of cancellation's monotonic budget clock.

use super::*;
use chrono::Utc;
use std::{cell::RefCell, sync::Weak};

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
}

impl Clock {
    pub(super) fn new(history: &[HistoryEvent]) -> Self {
        Self {
            time: start_time(history),
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
        self.replay_clock.time =
            sequence_time(&self.history_events, &self.recorded_commands, sequence);
    }
}

fn sequence_time(
    history: &[HistoryEvent],
    commands: &[RecordedCommand],
    sequence: u64,
) -> Option<DateTime<Utc>> {
    let command = commands
        .iter()
        .find(|command| command.sequence() == sequence);
    let event = history.iter().find(|event| {
        if durable_event_sequence(event) != Some(sequence) {
            return false;
        }
        match command {
            Some(RecordedCommand::Activity {
                outcome: Some(_), ..
            }) => matches!(
                event.event_type.as_str(),
                "ActivityCompleted" | "ActivityFailed" | "ActivityTimedOut" | "ActivityCancelled"
            ),
            Some(RecordedCommand::Timer { fired: true, .. }) => {
                event.event_type == "TimerFired" && !is_internal_timer_event(event)
            }
            _ => false,
        }
    })?;
    recorded_time(event, event.event_type != "ActivityFailed")
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
    sequence_time(history, &commands, durable_event_sequence(event)?)
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
