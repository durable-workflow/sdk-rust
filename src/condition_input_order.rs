//! Input evidence for a timed condition whose outcome is chosen by replay.
use crate::{invalid_recorded_history, HistoryEvent, Result, WorkflowState};
use serde_json::{json, Value};
use std::{
    collections::{BTreeSet, HashSet},
    sync::{Arc, Mutex},
};

pub(super) const CAPABILITY: &str = "condition_wait_input_order";
const AUTHORING: &str = "condition_wait_input_order_authoring";
pub(super) const ORDER: &str = "durable_receipt_order";

/// An omitted snapshot differs from a snapshot with no outstanding receipts.
pub(super) type PendingInput = Option<Option<u64>>;

/// Per-claim task evidence read from the poll's wire task, beside the public
/// `WorkflowTask` model (whose stable 4.x struct literals stay unchanged).
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct Claim {
    pub(super) authoring: bool,
    pub(super) pending_input: PendingInput,
    /// The validated `replay_clock` entry carrier (.github#215).
    pub(super) replay_entry: Option<crate::replay_clock::Entry>,
}

pub(super) fn read_claim(task: &Value) -> Result<Claim> {
    let pending_input = match task.get("condition_wait_pending_input_sequence") {
        None => Ok(None),
        Some(Value::Null) => Ok(Some(None)),
        Some(value) => value
            .as_u64()
            .filter(|sequence| *sequence > 0)
            .map(|sequence| Some(Some(sequence)))
            .ok_or_else(|| {
                crate::Error::Codec("invalid pending condition input history sequence".into())
            }),
    }?;
    Ok(Claim {
        authoring: task[AUTHORING] == true,
        pending_input,
        replay_entry: crate::replay_clock::entry(task),
    })
}

pub(super) fn marked(payload: &Value) -> bool {
    payload[CAPABILITY] == ORDER
}

fn history_sequence(event: &HistoryEvent) -> Option<u64> {
    event
        .raw
        .get("sequence")
        .and_then(Value::as_u64)
        .filter(|sequence| *sequence > 0)
}

pub(super) struct CompetingSelection {
    terminal_id: String,
    sequence: u64,
}

pub(super) fn competing_selection(
    history: &[HistoryEvent],
    group_id: &str,
) -> Result<Option<CompetingSelection>> {
    let mut candidates = history.iter().filter(|event| {
        event.event_type == "SelectionCandidateReady"
            && event.payload["selection_group_id"] == group_id
    });
    let Some(candidate) = candidates.next() else {
        return Ok(None);
    };
    let invalid = || crate::Error::Codec("invalid ordered selection candidate evidence".into());
    let terminal_id = candidate.payload["resolution_event_id"]
        .as_str()
        .filter(|id| !id.is_empty())
        .ok_or_else(invalid)?;
    let sequence = candidate.payload["resolution_event_sequence"]
        .as_u64()
        .filter(|sequence| *sequence > 0)
        .ok_or_else(invalid)?;
    if candidates.next().is_some()
        || history_sequence(candidate).is_none_or(|recorded| recorded <= sequence)
        || !history.iter().any(|event| {
            history_sequence(event) == Some(sequence)
                && event
                    .raw
                    .get("id")
                    .or_else(|| event.raw.get("event_id"))
                    .and_then(Value::as_str)
                    == Some(terminal_id)
                && candidate.payload["resolution_event_type"] == event.event_type
        })
    {
        return Err(invalid());
    }
    Ok(Some(CompetingSelection {
        terminal_id: terminal_id.to_owned(),
        sequence,
    }))
}

#[derive(Clone, Debug)]
pub(super) struct Wait {
    pub(super) id: String,
    opened: u64,
    fire: Option<u64>,
}

impl Wait {
    pub(super) fn read(
        opening: &HistoryEvent,
        schedule: Option<&HistoryEvent>,
        fire: Option<&HistoryEvent>,
        id: &str,
        sequence: u64,
    ) -> Result<Self> {
        let invalid = || {
            invalid_recorded_history(
                "invalid_ordered_condition_boundary",
                sequence,
                "canonical opening, timer schedule and any matching fire in history order",
                "missing or mismatched ordered condition evidence",
                "an ordered condition cannot substitute another timeout or history sequence",
            )
        };
        let opened = history_sequence(opening).ok_or_else(invalid)?;
        let schedule = schedule
            .filter(|event| marked(&event.payload))
            .ok_or_else(invalid)?;
        let scheduled = history_sequence(schedule)
            .filter(|scheduled| *scheduled > opened)
            .ok_or_else(invalid)?;
        let fire = fire
            .map(|event| {
                history_sequence(event)
                    .filter(|fired| *fired > scheduled && marked(&event.payload))
                    .ok_or_else(invalid)
            })
            .transpose()?;
        Ok(Self {
            id: id.to_owned(),
            opened,
            fire,
        })
    }

    pub(super) fn verify_terminal(&self, terminal: &HistoryEvent, sequence: u64) -> Result<()> {
        let satisfied = terminal.event_type == "ConditionWaitSatisfied";
        let receipt = terminal.payload["input_receipt_sequence"].as_u64();
        let fire = terminal.payload["timer_fired_sequence"].as_u64();
        let terminal_sequence = history_sequence(terminal);
        if !marked(&terminal.payload)
            || fire != self.fire
            || !terminal_sequence
                .is_some_and(|terminal| terminal > self.fire.unwrap_or(self.opened))
            || receipt.is_some_and(|receipt| {
                receipt == 0 || terminal_sequence.is_some_and(|terminal| receipt >= terminal)
            })
            || (satisfied
                && self
                    .fire
                    .is_some_and(|fire| receipt.is_none_or(|receipt| receipt >= fire)))
            || (!satisfied && (self.fire.is_none() || receipt.is_some()))
        {
            return Err(invalid_recorded_history(
                "invalid_ordered_condition_outcome",
                sequence,
                "a recorded outcome for the same wait and original fired timer",
                &terminal.event_type,
                "an ordered condition outcome must name its exact durable boundary",
            ));
        }
        Ok(())
    }

    pub(super) fn evaluate(
        &self,
        state: &Arc<Mutex<WorkflowState>>,
        predicate: &dyn Fn() -> Result<bool>,
        competing: Option<&CompetingSelection>,
    ) -> Result<Option<Value>> {
        let (inputs, pending) = {
            let state = state
                .lock()
                .map_err(|_| crate::Error::WorkflowStatePoisoned)?;
            let pending = state.condition_wait_pending_input_sequence.ok_or_else(|| {
                crate::Error::WorkerLoop(
                    "ordered condition replay is missing the input admission snapshot".into(),
                )
            })?;
            (Arc::new(applied_receipts(&state.history_events)), pending)
        };
        let competing_position = competing.map(|candidate| candidate.sequence);
        let terminal_boundary = [self.fire, competing_position].into_iter().flatten().min();
        let boundary = [terminal_boundary, pending]
            .into_iter()
            .flatten()
            .min()
            .unwrap_or(u64::MAX);
        let entry = self.opened.min(boundary.saturating_sub(1));
        // Preserve established input on entry, then expose one accepted input
        // at a time. A later withdrawal cannot erase the first true prefix.
        let entry_receipt = inputs.range(..=entry).next_back().copied();
        let prefixes = std::iter::once((entry, entry_receipt)).chain(
            inputs
                .range((
                    std::ops::Bound::Excluded(entry),
                    std::ops::Bound::Excluded(boundary),
                ))
                .map(|receipt| (*receipt, Some(*receipt))),
        );
        let mut final_error = None;
        for (through, receipt) in prefixes {
            let _input = PredicateScope::enter(
                state,
                View {
                    receipts: inputs.clone(),
                    through,
                },
            )?;
            let _clock = crate::replay_clock::Guard::unavailable();
            match predicate() {
                Ok(true) if receipt.is_some() || self.fire.is_none() => {
                    let mut command = json!({"type":"resolve_condition_wait","condition_wait_id":self.id,"satisfied":true});
                    if let Some(receipt) = receipt {
                        command["input_receipt_sequence"] = json!(receipt);
                    }
                    return Ok(Some(command));
                }
                Ok(_) => final_error = None,
                Err(error) => final_error = Some(error),
            }
        }
        if let Some(error) = final_error.filter(|_| {
            pending
                .is_none_or(|pending| terminal_boundary.is_some_and(|terminal| pending >= terminal))
        }) {
            return Err(error);
        }
        if let Some(terminal) =
            terminal_boundary.filter(|terminal| pending.is_none_or(|pending| pending >= *terminal))
        {
            let mut command = json!({"type":"resolve_condition_wait","condition_wait_id":self.id,"satisfied":false});
            if let Some(candidate) = competing.filter(|candidate| candidate.sequence == terminal) {
                command["competing_resolution_event_id"] = json!(candidate.terminal_id);
            }
            return Ok(Some(command));
        }
        Ok(None)
    }
}

#[derive(Debug)]
pub(super) struct View {
    receipts: Arc<BTreeSet<u64>>,
    through: u64,
}

impl View {
    pub(super) fn includes(&self, event: &HistoryEvent) -> bool {
        history_sequence(event)
            .is_some_and(|sequence| sequence <= self.through && self.receipts.contains(&sequence))
    }
}

struct PredicateScope<'a> {
    state: &'a Mutex<WorkflowState>,
    previous: Option<View>,
}

impl<'a> PredicateScope<'a> {
    fn enter(state: &'a Mutex<WorkflowState>, view: View) -> Result<Self> {
        let previous = state
            .lock()
            .map_err(|_| crate::Error::WorkflowStatePoisoned)?
            .condition_input_view
            .replace(view);
        Ok(Self { state, previous })
    }
}

impl Drop for PredicateScope<'_> {
    fn drop(&mut self) {
        if let Ok(mut state) = self.state.lock() {
            state.condition_input_view = self.previous.take();
        }
    }
}

fn applied_receipts(history: &[HistoryEvent]) -> BTreeSet<u64> {
    let mut applied = HashSet::new();
    for event in history {
        let receipt_kind = match event.event_type.as_str() {
            "SignalApplied" => "SignalReceived",
            "UpdateApplied" => "UpdateAccepted",
            _ => continue,
        };
        if let Some(command) = command_id(event) {
            applied.insert((receipt_kind, command));
        }
    }
    history
        .iter()
        .filter_map(|event| {
            let command = command_id(event)?;
            applied
                .contains(&(event.event_type.as_str(), command))
                .then(|| history_sequence(event))
                .flatten()
        })
        .collect()
}

fn command_id(event: &HistoryEvent) -> Option<&str> {
    event
        .raw
        .get("workflow_command_id")
        .and_then(Value::as_str)
        .or_else(|| {
            event
                .payload
                .get("workflow_command_id")
                .and_then(Value::as_str)
        })
        .filter(|id| !id.is_empty())
}
