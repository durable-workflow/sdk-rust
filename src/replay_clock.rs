//! Additive business time, independent of cancellation's monotonic budget clock.

use super::{
    durable_event_sequence, is_internal_timer_event, is_recorded_signal_wait_event,
    recorded_commands, DurableOperationHandle, Error, HistoryEvent, ParallelGroupMetadata,
    QueryContext, RecordedCommand, Result, Value, WorkflowContext, WorkflowIdentity, WorkflowState,
};
use chrono::{DateTime, Utc};
use std::{
    cell::{Cell, RefCell},
    collections::HashMap,
    sync::{Arc, Mutex, Weak},
};

mod condition;
mod condition_timeout;
mod parallel;
mod parallel_activity;
mod parallel_child;
mod parallel_timer;

thread_local! {
    static ACTIVE: RefCell<Option<Weak<Mutex<WorkflowState>>>> = const { RefCell::new(None) };
    static QUERY_REPLAY: Cell<bool> = const { Cell::new(false) };
}

pub(super) struct Guard(Option<Weak<Mutex<WorkflowState>>>, bool);

impl Guard {
    pub(super) fn enter(context: &WorkflowContext) -> Self {
        Self::scope(Some(Arc::downgrade(&context.state)), false)
    }

    pub(super) fn query(context: &WorkflowContext) -> Self {
        Self::scope(Some(Arc::downgrade(&context.state)), true)
    }

    pub(super) fn unavailable() -> Self {
        Self::scope(None, false)
    }

    fn scope(context: Option<Weak<Mutex<WorkflowState>>>, query: bool) -> Self {
        Self(
            ACTIVE.with(|active| active.replace(context)),
            QUERY_REPLAY.with(|active| active.replace(query)),
        )
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        ACTIVE.with(|active| active.replace(self.0.take()));
        QUERY_REPLAY.with(|active| active.set(self.1));
    }
}

pub(super) fn observation_mode(context: &WorkflowContext) -> Option<bool> {
    let active = ACTIVE.with(|active| active.borrow().as_ref().and_then(Weak::upgrade))?;
    Arc::ptr_eq(&active, &context.state).then(|| QUERY_REPLAY.with(Cell::get))
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

fn boundary_time(event: &HistoryEvent, query: bool) -> Option<DateTime<Utc>> {
    let internal = event.event_type == "TimerFired" && is_internal_timer_event(event);
    if internal {
        // Core query replay requires the explicit condition resolution. Its
        // executor-only fired-time branch does not grant callback authority.
        return if query {
            None
        } else {
            parse(event.raw.get("recorded_at"))
        };
    }
    recorded_time(
        event,
        matches!(
            event.event_type.as_str(),
            "ActivityCompleted" | "TimerFired" | "SignalApplied"
        ),
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

/// `WorkerReplayClock` v1, the workflow task's entry instant (Server #1132/#1156).
const ENTRY_SCHEMA: &str = "durable-workflow.worker-replay-clock/v1";

/// A validated `replay_clock` carrier from one workflow task claim.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Entry {
    started_at: DateTime<Utc>,
    redrive: bool,
}

/// `YYYY-MM-DDTHH:MM:SS.ffffffZ`, exactly as the v1 schema pattern requires.
fn strict_entry_time(text: &str) -> Option<DateTime<Utc>> {
    let bytes = text.as_bytes();
    let shape = bytes.len() == 27
        && bytes.iter().enumerate().all(|(index, byte)| match index {
            4 | 7 => *byte == b'-',
            10 => *byte == b'T',
            13 | 16 => *byte == b':',
            19 => *byte == b'.',
            26 => *byte == b'Z',
            _ => byte.is_ascii_digit(),
        });
    let time = DateTime::parse_from_rfc3339(text).ok().filter(|_| shape)?;
    // chrono admits a leap second as nanoseconds >= 1s; the carrier cannot.
    (time.timestamp_subsec_nanos() < 1_000_000_000).then(|| time.with_timezone(&Utc))
}

/// Read the task's `replay_clock`. Anything other than a complete v1 object
/// for this task's run is absent: the SDK never substitutes another time.
pub(super) fn entry(task: &Value) -> Option<Entry> {
    entry_for(
        task.get("replay_clock"),
        task.get("run_id").and_then(Value::as_str),
    )
}

/// Validate one carrier against the run it is replayed for. The v1 schema
/// has `additionalProperties: false`, so an extra key also makes it absent.
pub(super) fn entry_for(carrier: Option<&Value>, run_id: Option<&str>) -> Option<Entry> {
    const KEYS: [&str; 4] = ["schema", "workflow_run_id", "started_at", "source"];
    let carrier = carrier?.as_object()?;
    let run_id = run_id.filter(|id| !id.is_empty())?;
    if carrier.len() != KEYS.len()
        || !KEYS.iter().all(|key| carrier.contains_key(*key))
        || carrier.get("schema").and_then(Value::as_str) != Some(ENTRY_SCHEMA)
        || carrier.get("workflow_run_id").and_then(Value::as_str) != Some(run_id)
    {
        return None;
    }
    let redrive = match carrier.get("source").and_then(Value::as_str)? {
        "run_started_at" => false,
        "redrive_replayed_started_at" => true,
        _ => return None,
    };
    Some(Entry {
        started_at: strict_entry_time(carrier.get("started_at")?.as_str()?)?,
        redrive,
    })
}

/// A JSON `null` field is absent.
fn present(value: Option<&Value>) -> bool {
    value.is_some_and(|value| !value.is_null())
}

/// How a query or update-validation task attests the run's entry instant.
/// Server #1338 always sends the `replay_clock` key on those tasks: the v1
/// object when the start is provable, explicit `null` when it is not. An
/// older Server omits the key, so the entry its workflow tasks carried is
/// unknown to the rebuild.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Attestation {
    /// No key: the rebuild cannot know the entry its workflow tasks had.
    Unattested,
    /// Explicit `null`, or an object this SDK refuses (as it refused the
    /// same object on the run's workflow tasks): no entry was carried.
    Unprovable,
    Entry(Entry),
}

pub(super) fn attestation(task: &Value) -> Attestation {
    match task.get("replay_clock") {
        None => Attestation::Unattested,
        Some(Value::Null) => Attestation::Unprovable,
        Some(_) => entry(task).map_or(Attestation::Unprovable, Attestation::Entry),
    }
}

/// Whether this history declares a redrive start, whether or not it is proved.
fn declares_redrive(history: &[HistoryEvent]) -> bool {
    history.iter().any(|event| match event.event_type.as_str() {
        "WorkflowStarted" => {
            event.payload["recovery_kind"] == "redrive"
                || present(event.payload.get("replayed_started_at"))
                || present(event.payload.get("resume_step_sequence"))
        }
        "StartAccepted" => event.payload["outcome"] == "redriven",
        _ => false,
    })
}

#[derive(Debug)]
pub(super) struct Clock {
    time: Option<DateTime<Utc>>,
    boundaries: HashMap<u64, usize>,
    parallel_activities: parallel::Members,
    parallel_timers: parallel::Members,
    parallel_children: parallel::Members,
    /// No boundary, inline or durable, has been consumed yet.
    at_entry: bool,
    /// A rebuild whose task did not attest the entry instant.
    entry_unattested: bool,
    /// Workflow code read the clock at an unattested entry, so the rebuilt
    /// state may differ from what the workflow task computed.
    unattested_entry_read: bool,
}

enum BoundaryKind {
    Activity,
    Timer,
    Child,
    Signal,
    Condition,
}

fn supported_boundary(command: &RecordedCommand) -> Option<(u64, BoundaryKind)> {
    match command {
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
        RecordedCommand::ConditionWait {
            sequence,
            result: Some(_),
            parallel_group_path,
            ..
        } if parallel_group_path.as_ref().is_none_or(Vec::is_empty) => {
            Some((*sequence, BoundaryKind::Condition))
        }
        _ => None,
    }
}

fn signal_boundary(history: &[HistoryEvent], indices: &[usize], sequence: u64) -> Option<usize> {
    let (opened, timers, applied) = match indices {
        [opened, applied] => (opened, &[][..], applied),
        [opened, _, _, applied] => (opened, &indices[1..3], applied),
        _ => return None,
    };
    let open = &history[*opened];
    let apply = &history[*applied];
    if open.event_type != "SignalWaitOpened"
        || apply.event_type != "SignalApplied"
        || apply.payload.get("value").is_none_or(Value::is_null)
    {
        return None;
    }
    let events: Vec<_> = indices.iter().map(|&index| &history[index]).collect();
    if super::recorded_parallel_group_path(&events, sequence)
        .ok()?
        .is_some_and(|path| !path.is_empty())
    {
        return None;
    }
    for event in &events {
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
        if events
            .iter()
            .any(|event| event.payload.get(key).and_then(Value::as_str) != Some(identity))
        {
            return None;
        }
    }
    signal_timeout(open, apply, history, timers)?;
    Some(*applied)
}

fn signal_timeout(
    open: &HistoryEvent,
    apply: &HistoryEvent,
    history: &[HistoryEvent],
    timers: &[usize],
) -> Option<()> {
    let timeout = open.payload.get("timeout_seconds").filter(|v| !v.is_null());
    let seconds = match timeout {
        Some(value) => Some(value.as_u64()?),
        None => None,
    };
    if apply
        .payload
        .get("timeout_seconds")
        .filter(|value| !value.is_null())
        .is_some_and(|value| Some(value) != timeout)
    {
        return None;
    }
    let [scheduled, cancelled] = timers else {
        // A signal already pending at the call can win even a zero timeout,
        // without ever scheduling a timer. The persisted apply is authority.
        return timers.is_empty().then_some(());
    };
    let schedule = &history[*scheduled];
    let cancel = &history[*cancelled];
    if seconds? == 0
        || schedule.event_type != "TimerScheduled"
        || cancel.event_type != "TimerCancelled"
        || parse(cancel.payload.get("cancelled_at")).is_none()
        || cancel
            .payload
            .get("cancellation_scope")
            .is_some_and(|v| !v.is_null())
    {
        return None;
    }
    for event in [schedule, cancel] {
        if event.payload["timer_kind"] != "signal_timeout"
            || event.payload.get("delay_seconds").and_then(Value::as_u64) != seconds
        {
            return None;
        }
    }
    let timer_id = schedule
        .payload
        .get("timer_id")?
        .as_str()
        .filter(|v| !v.is_empty())?;
    if cancel.payload.get("timer_id").and_then(Value::as_str) != Some(timer_id)
        || parse(schedule.payload.get("fire_at")).is_none()
        || cancel.payload.get("fire_at") != schedule.payload.get("fire_at")
    {
        return None;
    }
    Some(())
}

fn child_boundary(history: &[HistoryEvent], indices: &[usize]) -> Option<usize> {
    child_lifecycle(history, indices)?
}

fn child_lifecycle(history: &[HistoryEvent], indices: &[usize]) -> Option<Option<usize>> {
    // One call has one schedule, an optional start, then at most one resolution.
    // Legacy replay can accept duplicate payloads, but their time is ambiguous.
    let (&scheduled, terminal) = match indices {
        [scheduled] => (scheduled, None),
        [scheduled, started] if history[*started].event_type == "ChildRunStarted" => {
            (scheduled, None)
        }
        [scheduled, terminal] => (scheduled, Some(*terminal)),
        [scheduled, started, terminal] if history[*started].event_type == "ChildRunStarted" => {
            (scheduled, Some(*terminal))
        }
        _ => return None,
    };
    let schedule = &history[scheduled];
    if schedule.event_type != "ChildWorkflowScheduled"
        || terminal.is_some_and(|terminal| {
            !matches!(
                history[terminal].event_type.as_str(),
                "ChildRunCompleted" | "ChildRunFailed" | "ChildRunCancelled" | "ChildRunTerminated"
            )
        })
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
        let supported: HashMap<_, _> = commands.iter().filter_map(supported_boundary).collect();
        let mut boundaries = HashMap::new();
        let mut children: HashMap<u64, Vec<usize>> = HashMap::new();
        let mut signals: HashMap<u64, Vec<usize>> = HashMap::new();
        let mut conditions: HashMap<u64, Vec<usize>> = HashMap::new();
        let mut signal_timers: HashMap<&str, Vec<usize>> = HashMap::new();
        for (index, event) in history.iter().enumerate() {
            if event.event_type.starts_with("Timer") {
                if let Some(wait) = event.payload.get("signal_wait_id").and_then(Value::as_str) {
                    signal_timers.entry(wait).or_default().push(index);
                }
            }
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
                    if is_recorded_signal_wait_event(event) || event.event_type.starts_with("Timer")
                    {
                        signals.entry(sequence).or_default().push(index);
                    }
                    false
                }
                Some(BoundaryKind::Condition) => {
                    if super::is_recorded_condition_wait_event(event) {
                        conditions.entry(sequence).or_default().push(index);
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
        for (sequence, mut indices) in signals {
            // A timer with a conflicting/missing counter must not disappear
            // from its wait's evidence just because it uses another bucket.
            let related: Vec<_> = indices
                .iter()
                .filter_map(|&index| history[index].payload.get("signal_wait_id")?.as_str())
                .filter_map(|wait| signal_timers.get(wait))
                .flatten()
                .copied()
                .collect();
            indices.extend(related);
            indices.sort_unstable();
            indices.dedup();
            if let Some(index) = signal_boundary(history, &indices, sequence) {
                boundaries.insert(sequence, index);
            }
        }
        let condition_timeouts = condition_timeout::Index::new(history);
        boundaries.extend(conditions.into_iter().filter_map(|(sequence, indices)| {
            condition::boundary(history, &indices, sequence)
                .or_else(|| condition_timeouts.boundary(history, &indices, sequence))
                .map(|index| (sequence, index))
        }));
        // Index identities once. Timestamps are read only when that boundary
        // is actually consumed, never by scanning ahead from the accessor.
        Self {
            time: start_time(history),
            boundaries,
            parallel_activities: parallel_activity::members(history, commands),
            parallel_timers: parallel_timer::members(history, commands),
            parallel_children: parallel_child::members(history, commands),
            at_entry: true,
            entry_unattested: false,
            unattested_entry_read: false,
        }
    }

    pub(super) fn unavailable(&mut self) {
        self.time = None;
        self.at_entry = false;
    }

    /// Ordinary entry time from a validated task carrier, before any
    /// boundary is consumed. Its source must match the history's start kind.
    /// A redrive anchor already proved from history is kept as it was.
    fn adopt_entry(&mut self, history: &[HistoryEvent], entry: Entry) {
        // An anchor a start event records itself, even one this history
        // cannot prove, must equal the carrier's.
        // An unparsable recorded anchor also refuses the carrier.
        let disagrees = history.iter().any(|event| {
            event.event_type == "WorkflowStarted"
                && event
                    .payload
                    .get("replayed_started_at")
                    .filter(|value| !value.is_null())
                    .is_some_and(|recorded| parse(Some(recorded)) != Some(entry.started_at))
        });
        if self.time.is_none() && entry.redrive == declares_redrive(history) && !disagrees {
            self.time = Some(entry.started_at);
        }
    }
}

impl WorkflowState {
    /// Apply this claim's `replay_clock` carrier. Call it once, after
    /// construction and before the workflow body runs.
    pub(super) fn adopt_replay_entry(&mut self, entry: Option<Entry>) {
        if let Some(entry) = entry {
            self.replay_clock.adopt_entry(&self.history_events, entry);
        }
    }

    /// Apply a query rebuild's attestation. Call it once, after construction
    /// and before the workflow body runs.
    pub(super) fn attest_replay_entry(&mut self, attestation: Attestation) {
        match attestation {
            Attestation::Unattested => self.replay_clock.entry_unattested = true,
            Attestation::Unprovable => {}
            Attestation::Entry(entry) => self.adopt_replay_entry(Some(entry)),
        }
    }

    /// Whether the rebuilt body read the clock at an unattested entry. Its
    /// state is then not proved to match the workflow task's, whatever the
    /// body did with the unavailable result.
    pub(super) fn read_unattested_replay_entry(&self) -> bool {
        self.replay_clock.unattested_entry_read
    }

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
        if path.is_empty() {
            self.observation_pending = false;
        }
        self.replay_clock.unavailable();
        self.unavailable_cancellation_boundary(path);
    }

    pub(super) fn consume_replay_group(&mut self, sequences: &[u64], failed: Option<u64>) {
        self.observation_pending = false;
        // Core's query group-send branch supplies no aggregate time. Do not
        // transfer executor authority into reconstructed query execution.
        self.replay_clock.at_entry = false;
        self.replay_clock.time = if QUERY_REPLAY.with(Cell::get) {
            None
        } else {
            self.replay_clock
                .parallel_activities
                .consumed_time(&self.history_events, sequences, failed)
                .or_else(|| {
                    self.replay_clock.parallel_timers.consumed_time(
                        &self.history_events,
                        sequences,
                        failed,
                    )
                })
                .or_else(|| {
                    self.replay_clock.parallel_children.consumed_time(
                        &self.history_events,
                        sequences,
                        failed,
                    )
                })
        };
        self.advance_cancellation_sequences(sequences, failed);
    }

    pub(super) fn consume_replay_selection(&mut self, group: &str) {
        self.observation_pending = false;
        self.replay_clock.unavailable();
        self.advance_cancellation_selection(group);
    }

    pub(super) fn consume_replay_receipt(&mut self, group: &str, sequence: u64) {
        self.observation_pending = false;
        self.replay_clock.unavailable();
        self.advance_cancellation_receipt(group, sequence);
    }

    pub(super) fn consume_replay_handle(&mut self, handle: &DurableOperationHandle, failed: bool) {
        self.observation_pending = false;
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

    /// Workflow code's read, which records an unattested entry read.
    fn read_replay_time(&mut self) -> Result<DateTime<Utc>> {
        let time = self.replay_time();
        if time.is_err()
            && self.cancellation_clock.is_none()
            && self.replay_clock.at_entry
            && self.replay_clock.entry_unattested
        {
            self.replay_clock.unattested_entry_read = true;
        }
        time
    }

    pub(super) fn observe_replay_sequence(&mut self, sequence: u64) {
        self.observation_pending = false;
        self.replay_clock.at_entry = false;
        self.replay_clock.time = self
            .replay_clock
            .boundaries
            .get(&sequence)
            .and_then(|index| self.history_events.get(*index))
            .and_then(|event| boundary_time(event, QUERY_REPLAY.with(Cell::get)));
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
    /// Before any boundary is consumed, the clock is the workflow task's
    /// Server-provided `replay_clock` entry instant (`WorkerReplayClock` v1):
    /// the run's own start, or a redrive successor's original start. It is
    /// pinned to the run's stored rows, so every replay reads the same value.
    /// A task without a valid carrier keeps entry unavailable, except for the
    /// explicit redrive start anchor proved from history. Step times come
    /// from history; only the entry value comes from the task's carrier.
    ///
    /// A replayed-instance query or update validator rebuilds the body with its own task's
    /// `replay_clock` attestation. When the query task omits that key (a
    /// Server before #1338) and the body reads this clock at entry, the
    /// query fails with `query_workflow_state_unavailable`, even if the body
    /// tolerated the unavailable result: its rebuilt state is not proved to
    /// match the workflow task's. An explicit `null` keeps entry unavailable,
    /// as on the run's workflow tasks. Offline replay takes the carrier
    /// through [`crate::testing::WorkflowHistory::replay_clock`]. An
    /// unhandled [`Error::ReplayClockUnavailable`] fails the workflow like
    /// any error.
    ///
    /// Supported boundaries are correlated scalar activity, timer, child, signal
    /// and condition results, and persisted Activity, Timer and Child all
    /// groups. Inline metadata and local completions, and other grouped
    /// families remain unavailable.
    /// Cancellation's existing budget clock is unchanged.
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
            .read_replay_time()
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
    if super::settled_at_run_close(event) {
        return None;
    }
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
            | "ConditionWaitSatisfied"
            | "ConditionWaitTimedOut"
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
        .and_then(|event| boundary_time(event, true))
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
