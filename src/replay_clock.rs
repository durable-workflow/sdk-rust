//! Additive business time, independent of cancellation's monotonic budget clock.

use super::{
    durable_event_sequence, is_internal_timer_event, is_recorded_signal_wait_event,
    recorded_commands, DurableOperationHandle, Error, HistoryEvent, ParallelGroupMetadata,
    QueryContext, RecordedCommand, Result, Value, WorkflowContext, WorkflowIdentity, WorkflowState,
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

enum BoundaryKind {
    Activity,
    Timer,
    Child,
    Signal,
}

fn signal_boundary(history: &[HistoryEvent], indices: &[usize], sequence: u64) -> Option<usize> {
    let [opened, applied] = indices else {
        return None;
    };
    let open = &history[*opened];
    let apply = &history[*applied];
    if open.event_type != "SignalWaitOpened"
        || apply.event_type != "SignalApplied"
        || open
            .payload
            .get("timeout_seconds")
            .is_some_and(|value| !value.is_null())
        || apply.payload.get("value").is_none_or(Value::is_null)
    {
        return None;
    }
    for event in [open, apply] {
        if event.payload.get("sequence").and_then(Value::as_u64) != Some(sequence)
            || event
                .payload
                .get("workflow_sequence")
                .is_some_and(|value| value.as_u64() != Some(sequence))
        {
            return None;
        }
    }
    for key in ["signal_wait_id", "signal_name"] {
        let identity = open
            .payload
            .get(key)?
            .as_str()
            .filter(|value| !value.is_empty())?;
        if apply.payload.get(key).and_then(Value::as_str) != Some(identity) {
            return None;
        }
    }
    Some(*applied)
}

fn child_boundary(history: &[HistoryEvent], indices: &[usize]) -> Option<usize> {
    // A scalar call has one schedule, an optional start, then one resolution.
    // Legacy replay can accept duplicate payloads, but their time is ambiguous.
    let (&scheduled, &terminal) = match indices {
        [scheduled, terminal] => (scheduled, terminal),
        [scheduled, started, terminal] if history[*started].event_type == "ChildRunStarted" => {
            (scheduled, terminal)
        }
        _ => return None,
    };
    let schedule = &history[scheduled];
    if schedule.event_type != "ChildWorkflowScheduled"
        || !matches!(
            history[terminal].event_type.as_str(),
            "ChildRunCompleted" | "ChildRunFailed" | "ChildRunCancelled" | "ChildRunTerminated"
        )
    {
        return None;
    }
    for key in [
        "child_call_id",
        "child_workflow_instance_id",
        "child_workflow_run_id",
        "child_workflow_type",
    ] {
        let identity = schedule
            .payload
            .get(key)?
            .as_str()
            .filter(|id| !id.is_empty())?;
        if indices
            .iter()
            .any(|&index| history[index].payload.get(key).and_then(Value::as_str) != Some(identity))
        {
            // Changed-run retry/continuation needs its own lineage algorithm.
            // Missing identities keep legacy result handling, without time.
            return None;
        }
    }
    Some(terminal)
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
                    Some((*sequence, BoundaryKind::Activity))
                }
                RecordedCommand::Timer {
                    sequence,
                    fired: true,
                    parallel_group_path,
                    ..
                } if parallel_group_path.as_ref().is_none_or(Vec::is_empty) => {
                    Some((*sequence, BoundaryKind::Timer))
                }
                RecordedCommand::ChildWorkflow {
                    sequence,
                    outcome: Some(_),
                    parallel_group_path,
                    ..
                } if parallel_group_path.as_ref().is_none_or(Vec::is_empty) => {
                    Some((*sequence, BoundaryKind::Child))
                }
                RecordedCommand::SignalWait {
                    sequence,
                    value: Some(_),
                    parallel_group_path,
                    ..
                } if parallel_group_path.as_ref().is_none_or(Vec::is_empty) => {
                    Some((*sequence, BoundaryKind::Signal))
                }
                _ => None,
            })
            .collect();
        let mut boundaries = HashMap::new();
        let mut children: HashMap<u64, Vec<usize>> = HashMap::new();
        let mut signals: HashMap<u64, Vec<usize>> = HashMap::new();
        for (index, event) in history.iter().enumerate() {
            let Some(sequence) = durable_event_sequence(event) else {
                continue;
            };
            let matched = match supported.get(&sequence) {
                Some(BoundaryKind::Activity) => matches!(
                    event.event_type.as_str(),
                    "ActivityCompleted"
                        | "ActivityFailed"
                        | "ActivityTimedOut"
                        | "ActivityCancelled"
                ),
                Some(BoundaryKind::Timer) => {
                    event.event_type == "TimerFired" && !is_internal_timer_event(event)
                }
                Some(BoundaryKind::Child) => {
                    if event.event_type == "ChildWorkflowScheduled"
                        || event.event_type.starts_with("ChildRun")
                    {
                        children.entry(sequence).or_default().push(index);
                    }
                    false
                }
                Some(BoundaryKind::Signal) => {
                    if is_recorded_signal_wait_event(event)
                        || (event.event_type.starts_with("Timer") && is_internal_timer_event(event))
                    {
                        signals.entry(sequence).or_default().push(index);
                    }
                    false
                }
                None => false,
            };
            if matched {
                boundaries.entry(sequence).or_insert(index);
            }
        }
        for (sequence, indices) in children {
            if let Some(index) = child_boundary(history, &indices) {
                boundaries.insert(sequence, index);
            }
        }
        for (sequence, indices) in signals {
            if let Some(index) = signal_boundary(history, &indices, sequence) {
                boundaries.insert(sequence, index);
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
                        "ActivityCompleted" | "TimerFired" | "SignalApplied"
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
    /// This source cohort supports correlated scalar activity/timer/child/signal
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
            | "ChildRunCompleted"
            | "ChildRunFailed"
            | "ChildRunCancelled"
            | "ChildRunTerminated"
            | "SignalApplied"
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
                    "ActivityCompleted" | "TimerFired" | "SignalApplied"
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
