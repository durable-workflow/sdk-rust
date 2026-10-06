use super::*;
use chrono::Utc;
use std::{cell::RefCell, sync::Weak};

thread_local! {
    static ACTIVE_REPLAY: RefCell<Option<Weak<Mutex<WorkflowState>>>> = const { RefCell::new(None) };
}

/// A binding is useful only while its original workflow future is being polled.
pub(super) struct ReplayGuard {
    previous: Option<Weak<Mutex<WorkflowState>>>,
}

impl ReplayGuard {
    pub(super) fn enter(ctx: &WorkflowContext) -> Result<Option<Self>> {
        let bound = ctx
            .state
            .lock()
            .map_err(|_| Error::WorkflowStatePoisoned)?
            .cancellation_clock
            .is_some();
        if !bound && ACTIVE_REPLAY.with(|active| active.borrow().is_none()) {
            return Ok(None);
        }
        let previous =
            ACTIVE_REPLAY.with(|active| active.replace(bound.then(|| Arc::downgrade(&ctx.state))));
        Ok(Some(Self { previous }))
    }
}

impl Drop for ReplayGuard {
    fn drop(&mut self) {
        ACTIVE_REPLAY.with(|active| active.replace(self.previous.take()));
    }
}

pub(super) fn active_binding() -> Option<Weak<Mutex<WorkflowState>>> {
    ACTIVE_REPLAY.with(|active| active.borrow().clone())
}

pub(super) fn is_active(state: &Arc<Mutex<WorkflowState>>) -> bool {
    ACTIVE_REPLAY.with(|active| {
        active
            .borrow()
            .as_ref()
            .and_then(Weak::upgrade)
            .is_some_and(|current| Arc::ptr_eq(&current, state))
    })
}

/// Index only committed blocking results. Synchronous metadata never enters this clock.
#[derive(Debug)]
pub(super) struct ReplayClock {
    boundaries: HashMap<u64, usize>,
    failures: BTreeSet<u64>,
    selections: HashMap<String, usize>,
    cancellations: HashMap<(String, u64), usize>,
    delivery_index: Option<usize>,
    time: Option<DateTime<Utc>>,
    available: bool,
    started: bool,
}

impl ReplayClock {
    pub(super) fn new(
        history: &[HistoryEvent],
        commands: &[RecordedCommand],
        delivery_index: Option<usize>,
    ) -> Self {
        let mut clock = Self {
            boundaries: HashMap::new(),
            failures: BTreeSet::new(),
            selections: HashMap::new(),
            cancellations: HashMap::new(),
            delivery_index,
            time: None,
            available: false,
            started: false,
        };
        for (index, event) in history.iter().enumerate() {
            if matches!(
                event.event_type.as_str(),
                "ActivityCompleted"
                    | "ActivityFailed"
                    | "ActivityCancelled"
                    | "ActivityTimedOut"
                    | "ChildRunCompleted"
                    | "ChildRunFailed"
                    | "ChildRunCancelled"
                    | "ChildRunTerminated"
                    | "TimerFired"
                    | "SignalApplied"
                    | "ConditionWaitSatisfied"
                    | "ConditionWaitTimedOut"
            ) {
                if let Some(sequence) = durable_event_sequence(event) {
                    if let std::collections::hash_map::Entry::Vacant(entry) =
                        clock.boundaries.entry(sequence)
                    {
                        entry.insert(index);
                        if matches!(
                            event.event_type.as_str(),
                            "ActivityFailed"
                                | "ActivityCancelled"
                                | "ActivityTimedOut"
                                | "ChildRunFailed"
                                | "ChildRunCancelled"
                                | "ChildRunTerminated"
                        ) {
                            clock.failures.insert(sequence);
                        }
                    }
                }
            } else if event.event_type == "SelectionResolved" {
                if let Some(group) = event.payload["selection_group_id"].as_str() {
                    clock.selections.entry(group.to_owned()).or_insert(index);
                }
            } else if event.event_type == "SelectionOperationCancelled" {
                if let (Some(group), Some(sequence)) = (
                    event.payload["selection_group_id"].as_str(),
                    event.payload["member_base_sequence"].as_u64(),
                ) {
                    clock
                        .cancellations
                        .entry((group.to_owned(), sequence))
                        .or_insert(index);
                }
            }
        }
        // One logical grouped condition may have several physical waits. The
        // group consumes its final resolved wait, as the operation replayer does.
        let mut conditions = HashMap::new();
        for command in commands {
            if let RecordedCommand::ConditionWait {
                sequence,
                occurrence_id,
                parallel_group_path: Some(_),
                ..
            } = command
            {
                let original = *conditions.entry(occurrence_id).or_insert(*sequence);
                if original != *sequence {
                    if let Some(index) = clock.boundaries.get(sequence).copied() {
                        clock.boundaries.insert(original, index);
                    } else {
                        clock.boundaries.remove(&original);
                    }
                }
            }
        }
        clock
    }

    fn observe(&mut self, event: Option<&HistoryEvent>) {
        let time = event.and_then(|event| {
            event
                .raw
                .get("timestamp")
                .filter(|value| !value.is_null())
                .or_else(|| event.raw.get("recorded_at"))
                .and_then(Value::as_str)
                .and_then(|time| DateTime::parse_from_rfc3339(time).ok())
                .map(|time| time.with_timezone(&Utc))
        });
        self.available = time.is_some();
        if let Some(time) = time {
            self.time = Some(self.time.map_or(time, |previous| previous.max(time)));
        }
    }
}

impl WorkflowState {
    pub(super) fn observe_scope_cancellation_delivery(&mut self, event: &HistoryEvent) {
        if let Some(clock) = &mut self.cancellation_clock {
            clock.started = true;
            clock.observe(Some(event));
        }
    }

    pub(super) fn start_cancellation_clock(&mut self) {
        if let Some(clock) = &mut self.cancellation_clock {
            if !clock.started {
                clock.started = true;
                clock.observe(
                    clock
                        .delivery_index
                        .and_then(|index| self.history_events.get(index)),
                );
            }
        }
    }

    pub(super) fn cancellation_time(&self) -> Result<DateTime<Utc>> {
        self.cancellation_clock
            .as_ref()
            .filter(|clock| clock.started && clock.available)
            .and_then(|clock| clock.time)
            .ok_or_else(|| {
                Error::InvalidCooperativeCancellation(
                    "remaining() requires a recorded consumed blocking timestamp".to_owned(),
                )
            })
    }

    fn advance_cancellation_indices(&mut self, mut indices: Vec<Option<usize>>) {
        let Some(clock) = &mut self.cancellation_clock else {
            return;
        };
        if !clock.started {
            return;
        }
        // A missing timestamp/result cannot borrow a different operation's clock.
        indices.sort_unstable();
        indices.dedup();
        let mut missing = false;
        for index in indices {
            clock.observe(index.and_then(|index| self.history_events.get(index)));
            missing |= !clock.available;
        }
        if missing {
            clock.available = false;
        }
    }

    pub(super) fn advance_cancellation_sequence(
        &mut self,
        sequence: u64,
        path: &[ParallelGroupMetadata],
    ) {
        if path.is_empty() {
            self.advance_cancellation_sequences(&[sequence], None);
        }
    }

    pub(super) fn unavailable_cancellation_boundary(&mut self, path: &[ParallelGroupMetadata]) {
        if path.is_empty() {
            self.advance_cancellation_indices(vec![None]);
        }
    }

    pub(super) fn advance_cancellation_sequences(
        &mut self,
        sequences: &[u64],
        failed_sequence: Option<u64>,
    ) {
        let Some(clock) = &self.cancellation_clock else {
            return;
        };
        let cutoff = failed_sequence.and_then(|sequence| clock.boundaries.get(&sequence).copied());
        let indices = sequences
            .iter()
            .filter_map(|sequence| {
                let index = clock.boundaries.get(sequence).copied();
                if cutoff.is_some_and(|cutoff| index.is_some_and(|index| index > cutoff)) {
                    None
                } else {
                    Some(index)
                }
            })
            .collect();
        self.advance_cancellation_indices(indices);
    }

    pub(super) fn advance_cancellation_selection(&mut self, group: &str) {
        let index = self
            .cancellation_clock
            .as_ref()
            .and_then(|clock| clock.selections.get(group).copied());
        self.advance_cancellation_indices(vec![index]);
    }

    pub(super) fn advance_cancellation_receipt(&mut self, group: &str, sequence: u64) {
        let index = self.cancellation_clock.as_ref().and_then(|clock| {
            clock
                .cancellations
                .get(&(group.to_owned(), sequence))
                .copied()
        });
        self.advance_cancellation_indices(vec![index]);
    }

    pub(super) fn advance_cancellation_handle(
        &mut self,
        handle: &DurableOperationHandle,
        failed: bool,
    ) {
        let sequences = (handle.base_sequence
            ..handle.base_sequence.saturating_add(handle.size as u64))
            .collect::<Vec<_>>();
        let failure = self
            .cancellation_clock
            .as_ref()
            .filter(|_| failed)
            .and_then(|clock| {
                sequences
                    .iter()
                    .filter(|sequence| clock.failures.contains(sequence))
                    .min_by_key(|sequence| clock.boundaries.get(sequence))
                    .copied()
            });
        self.advance_cancellation_sequences(&sequences, failure);
    }
}
