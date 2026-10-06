//! Candidate scalar scope delivery. Admission remains private and disabled by default.

use super::*;
use crate::cancellation_scope_history::CommittedCancellationScopeHistory;
use chrono::Utc;

#[doc(hidden)]
#[derive(Clone, Debug, Error, PartialEq, Eq)]
#[error("scope cancellation {request_id} was requested", request_id = .context.request_id())]
pub struct CancellationScopeRequested {
    pub context: ScopedCancellationContext,
    pub delivery: CancellationDelivery,
}

#[derive(Clone, Debug)]
pub(super) struct ScopeDeliveryIntent {
    pub context: ScopedCancellationContext,
    pub boundary: CancellationDelivery,
    pub command_count: usize,
}

#[derive(Debug)]
pub(super) struct ScopeReplay {
    pub canonical: CommittedCancellationScopeHistory,
    pub tree: cancellation_scope::CancellationScopeHistory,
    pub active_scope: String,
    pub consumed: BTreeSet<u64>,
    pub contexts: BTreeMap<String, (ScopedCancellationContext, DateTime<Utc>)>,
    pub intent: Option<ScopeDeliveryIntent>,
}

fn invalid(sequence: u64, detail: &str) -> Error {
    invalid_recorded_history(
        "cancellation_scope_call_mismatch",
        sequence,
        "original scalar scoped call",
        "changed call or authority",
        detail,
    )
}

fn scalar(kind: CancellationCallKind) -> bool {
    matches!(
        kind,
        CancellationCallKind::Activity
            | CancellationCallKind::Timer
            | CancellationCallKind::Condition
            | CancellationCallKind::Child
    )
}

impl ScopeReplay {
    pub fn read(
        history: &[HistoryEvent],
        tree: &cancellation_scope::CancellationScopeHistory,
        run: &str,
        workflow: &str,
    ) -> Result<Self> {
        let canonical = CommittedCancellationScopeHistory::read(history, run, workflow)?;
        for preparation in canonical.preparations.values() {
            if !scalar(preparation.boundary.call_kind)
                || preparation.boundary.sequence_span != 1
                || preparation.boundary.operation_sequence.is_some()
                || preparation.event.payload["descendant_members"]
                    .as_array()
                    .is_none_or(|members| !members.is_empty())
            {
                return Err(Error::CancellationScopeExecutionUnavailable);
            }
        }
        Ok(Self {
            canonical,
            tree: tree.clone(),
            active_scope: "root".into(),
            consumed: BTreeSet::new(),
            contexts: BTreeMap::new(),
            intent: None,
        })
    }

    pub fn bind_commands(&self, commands: &mut Vec<RecordedCommand>) -> Result<()> {
        for (&sequence, delivered) in &self.canonical.deliveries {
            let index = commands.partition_point(|command| command.sequence() < sequence);
            let original = if commands
                .get(index)
                .is_some_and(|command| command.sequence() == sequence)
            {
                Some(Box::new(commands.remove(index)))
            } else {
                let prior = index
                    .checked_sub(1)
                    .map_or(0, |index| commands[index].sequence());
                if prior.checked_add(1) != Some(sequence) {
                    return Err(invalid(
                        sequence,
                        "scope delivery skips an earlier authored call",
                    ));
                }
                None
            };
            commands.insert(
                index,
                RecordedCommand::CancellationBoundary {
                    sequence,
                    call_kind: delivered.boundary.call_kind,
                    original,
                },
            );
        }
        Ok(())
    }
}

impl WorkflowState {
    pub(super) fn prepare_scalar_scope_cancellation(
        &mut self,
        index: usize,
        kind: CancellationCallKind,
        path: &[ParallelGroupMetadata],
    ) -> Result<bool> {
        let Some(replay) = &self.scope_delivery else {
            return Ok(false);
        };
        if self.cancellation_shield_depth > 0 {
            return Ok(false);
        }
        if replay.intent.is_some() {
            self.matched_recorded_pending = true;
            return Ok(true);
        }
        let Some(request) = replay
            .canonical
            .pending_request_for_scope(&replay.active_scope, &replay.tree)?
        else {
            return Ok(false);
        };
        if replay.contexts.contains_key(request.context.scope_id()) {
            return Ok(false);
        }
        if !scalar(kind) || !path.is_empty() || request.context.scope_id() != replay.active_scope {
            return Err(Error::CancellationScopeExecutionUnavailable);
        }
        let sequence = self.recorded_commands.get(index).map_or_else(
            || self.next_scope_cancellation_sequence(),
            |command| Ok(command.sequence()),
        )?;
        if self
            .history_events
            .iter()
            .take(request.history_index)
            .any(|event| {
                durable_event_sequence(event) == Some(sequence)
                    && matches!(
                        event.event_type.as_str(),
                        "ActivityCompleted"
                            | "ActivityFailed"
                            | "ActivityCancelled"
                            | "ActivityTimedOut"
                            | "TimerFired"
                            | "TimerCancelled"
                            | "ConditionWaitSatisfied"
                            | "ConditionWaitTimedOut"
                            | "ChildRunCompleted"
                            | "ChildRunFailed"
                            | "ChildRunCancelled"
                            | "ChildRunTerminated"
                    )
            })
        {
            return Ok(false);
        }
        let boundary = CancellationDelivery {
            request_id: request.context.request_id().into(),
            sequence,
            call_kind: kind,
            sequence_span: 1,
            operation_sequence: None,
            operation_sequence_span: 1,
        };
        if replay
            .canonical
            .preparations
            .get(request.context.scope_id())
            .is_some_and(|prepared| prepared.boundary != boundary)
        {
            return Err(invalid(
                sequence,
                "prepared scope changed its original authored boundary",
            ));
        }
        let intent = ScopeDeliveryIntent {
            context: request.context.clone(),
            boundary,
            command_count: self.commands.len(),
        };
        self.scope_delivery.as_mut().unwrap().intent = Some(intent);
        self.matched_recorded_pending = true;
        if index < self.recorded_commands.len() {
            self.command_cursor = index + 1;
        }
        Ok(true)
    }

    fn next_scope_cancellation_sequence(&self) -> Result<u64> {
        self.recorded_commands
            .last()
            .map_or(0, RecordedCommand::sequence)
            .checked_add(self.commands.len() as u64)
            .and_then(|sequence| sequence.checked_add(1))
            .filter(|sequence| *sequence < i64::MAX as u64)
            .ok_or_else(|| invalid(0, "scope cancellation authored sequence overflowed"))
    }

    pub(super) fn replay_scope_cancellation_at(
        &mut self,
        index: usize,
        kind: CancellationCallKind,
    ) -> Result<()> {
        let Some(sequence) = self
            .recorded_commands
            .get(index)
            .map(RecordedCommand::sequence)
        else {
            return Ok(());
        };
        let Some(replay) = &self.scope_delivery else {
            return Ok(());
        };
        let Some(delivered) = replay.canonical.deliveries.get(&sequence).cloned() else {
            return Ok(());
        };
        if replay.consumed.contains(&sequence)
            || delivered.boundary.call_kind != kind
            || replay.active_scope != delivered.context.scope_id()
            || self.cancellation_shield_depth > 0
        {
            return Err(invalid(
                sequence,
                "committed scope delivery changed its original call, membership or shielding",
            ));
        }
        self.observe_scope_cancellation_delivery(&delivered.event);
        let replay = self.scope_delivery.as_mut().unwrap();
        replay.consumed.insert(sequence);
        replay.contexts.insert(
            delivered.context.scope_id().into(),
            (delivered.context.clone(), delivered.authority_deadline),
        );
        self.command_cursor = index + 1;
        Err(Error::CancellationScopeRequested(
            CancellationScopeRequested {
                context: delivered
                    .context
                    .with_replay(cancellation_replay_clock::active_binding()),
                delivery: delivered.boundary,
            },
        ))
    }
}

impl WorkflowContext {
    /// Original metadata only after consuming this context's committed scope delivery.
    #[doc(hidden)]
    pub fn scoped_cancellation_context(&self) -> Result<Option<ScopedCancellationContext>> {
        let state = self
            .state
            .lock()
            .map_err(|_| Error::WorkflowStatePoisoned)?;
        Ok(state
            .scope_delivery
            .as_ref()
            .and_then(|replay| replay.contexts.get(&self.cancellation_scope_id))
            .map(|(context, _)| {
                context
                    .clone()
                    .with_replay(Some(Arc::downgrade(&self.state)))
            }))
    }
}
