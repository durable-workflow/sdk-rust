//! Candidate scope delivery. Admission remains private and disabled by default.

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
    pub cleanup_timers: BTreeMap<u64, Value>,
    pub intent: Option<ScopeDeliveryIntent>,
}

fn invalid(sequence: u64, detail: &str) -> Error {
    invalid_recorded_history(
        "cancellation_scope_call_mismatch",
        sequence,
        "original scoped call",
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

fn scope_operation_terminal(kind: &str) -> bool {
    matches!(
        kind,
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
}

fn validate_scoped_group_definition(
    state: &WorkflowState,
    descriptor: &ParallelDescriptor,
    recorded: &RecordedCommand,
) -> Result<()> {
    let sequence = recorded.sequence();
    let path = match (&descriptor.operation, recorded) {
        (
            ParallelOperation::Activity {
                activity_type,
                options,
                ..
            },
            RecordedCommand::Activity {
                activity_type: original_type,
                cancellation_policy,
                options: original_options,
                parallel_group_path,
                ..
            },
        ) => {
            let options = options.validate().map_err(Error::InvalidActivityOptions)?;
            if original_type
                .as_ref()
                .is_some_and(|original| original != activity_type)
                || cancellation_policy
                    != options
                        .cancellation_policy
                        .unwrap_or(CancellationPolicy::TryCancel)
                        .as_str()
            {
                return Err(invalid(
                    sequence,
                    "scoped activity changed its original type or cancellation policy",
                ));
            }
            if let Some(original) = original_options {
                let queue = RecordedSnapshotValue::Known(Some(
                    options
                        .task_queue
                        .clone()
                        .unwrap_or_else(|| state.task_queue.clone()),
                ));
                if !original.task_queue.matches_current(&queue)
                    || !original
                        .execution_mode
                        .matches_current(&RecordedSnapshotValue::Known(None))
                    || !original
                        .retry_policy
                        .matches_current(&current_activity_retry_snapshot(&options))
                {
                    return Err(invalid(sequence, "scoped activity changed its original queue, execution mode or retry policy"));
                }
            }
            parallel_group_path
        }
        (
            ParallelOperation::Timer(delay),
            RecordedCommand::Timer {
                delay_seconds,
                parallel_group_path,
                ..
            },
        ) => {
            if delay
                .as_secs()
                .checked_add(u64::from(delay.subsec_nanos() > 0))
                != Some(*delay_seconds)
            {
                return Err(invalid(sequence, "scoped timer changed its original delay"));
            }
            parallel_group_path
        }
        (
            ParallelOperation::ChildWorkflow {
                workflow_type,
                options,
                ..
            },
            RecordedCommand::ChildWorkflow {
                workflow_type: original_type,
                policies,
                parallel_group_path,
                ..
            },
        ) => {
            if original_type
                .as_ref()
                .is_some_and(|original| original != workflow_type)
            {
                return Err(invalid(
                    sequence,
                    "scoped child changed its original workflow type",
                ));
            }
            ensure_child_policies_match(sequence, policies, options)?;
            parallel_group_path
        }
        (
            ParallelOperation::Condition { options, .. },
            RecordedCommand::ConditionWait {
                condition_key,
                predicate_identity,
                timeout_seconds,
                parallel_group_path,
                ..
            },
        ) => {
            let options = options
                .validate()
                .map_err(Error::InvalidConditionWaitOptions)?;
            if condition_key.as_deref() != Some(options.condition_key.as_str())
                || predicate_identity != &options.predicate_identity
                || timeout_seconds != &options.timeout_seconds
            {
                return Err(invalid(
                    sequence,
                    "scoped condition changed its original key, predicate identity or timeout",
                ));
            }
            // Delivery interrupts the original occurrence without evaluating the predicate.
            parallel_group_path
        }
        _ => {
            return Err(invalid(
                sequence,
                "scoped group changed its original member kind",
            ))
        }
    };
    ensure_parallel_path_matches(sequence, path.as_deref(), &descriptor.group_path)
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
            let boundary = &preparation.boundary;
            let supported = if boundary.call_kind == CancellationCallKind::Parallel {
                history
                    .iter()
                    .filter(|event| {
                        durable_event_sequence(event)
                            .is_some_and(|sequence| boundary.interrupts(sequence))
                    })
                    .all(|event| match event.event_type.as_str() {
                        "SignalWaitOpened" => false,
                        "ActivityScheduled" => {
                            event.payload["local_activity"] != true
                                && event.payload["execution_mode"] != "local"
                                && event.payload["activity"]["local_activity"] != true
                                && event.payload["activity"]["execution_mode"] != "local"
                        }
                        _ => true,
                    })
            } else {
                scalar(boundary.call_kind) && boundary.sequence_span == 1
            };
            if !supported
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
            cleanup_timers: history.iter().filter(|row| row.event_type == "TimerScheduled")
                .filter_map(|row| row.payload.get("cancellation_cleanup").map(|snapshot|
                    (row.payload["sequence"].as_u64().unwrap(), json!({"scope_id":snapshot["scope_id"],
                     "request_id":snapshot["request_id"], "delivery_history_event_id":snapshot["delivery_history_event_id"]})))).collect(),
            intent: None,
        })
    }

    pub fn bind_commands(&self, commands: &mut Vec<RecordedCommand>) -> Result<()> {
        for (&sequence, delivered) in &self.canonical.deliveries {
            let index = commands.partition_point(|command| command.sequence() < sequence);
            if delivered.boundary.call_kind == CancellationCallKind::Parallel {
                let end = commands.partition_point(|command| {
                    delivered.boundary.interrupts(command.sequence())
                        || command.sequence() < sequence
                });
                let original: Vec<_> = commands.drain(index..end).collect();
                if original.len() as u64 != delivered.boundary.sequence_span
                    || original
                        .iter()
                        .enumerate()
                        .any(|(offset, command)| command.sequence() != sequence + offset as u64)
                {
                    return Err(invalid(
                        sequence,
                        "scope delivery omits an original group member",
                    ));
                }
                commands.insert(
                    index,
                    RecordedCommand::CancellationGroup {
                        sequence,
                        span: delivered.boundary.sequence_span,
                        original,
                    },
                );
                continue;
            }
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
    /// Validate the whole group before exposing cancellation to application code.
    pub(super) fn prepare_parallel_scope_cancellation(
        &mut self,
        descriptors: &[ParallelDescriptor],
    ) -> Result<bool> {
        let Some(replay) = &self.scope_delivery else {
            return Ok(false);
        };
        let index = self.command_cursor;
        let sequence = descriptors[0].group_path[0].parallel_group_base_sequence;
        if let Some(delivered) = replay.canonical.deliveries.get(&sequence) {
            let delivered = delivered.clone();
            if replay.active_scope != delivered.context.scope_id()
                || self.cancellation_shield_depth > 0
                || delivered.boundary.call_kind != CancellationCallKind::Parallel
            {
                return Err(invalid(
                    sequence,
                    "scoped group changed its original membership, kind or shielding",
                ));
            }
            let Some(RecordedCommand::CancellationGroup { span, original, .. }) =
                self.recorded_commands.get(index).cloned()
            else {
                return Err(invalid(
                    sequence,
                    "scoped group crossed its original delivery boundary",
                ));
            };
            if span != descriptors.len() as u64 {
                return Err(invalid(sequence, "scoped group changed its original span"));
            }
            self.validate_scoped_group_members(descriptors, &original)?;
            let conditions = descriptors
                .iter()
                .filter(|descriptor| {
                    matches!(descriptor.operation, ParallelOperation::Condition { .. })
                })
                .count() as u64;
            self.condition_wait_occurrence_counter = self
                .condition_wait_occurrence_counter
                .checked_add(conditions)
                .ok_or_else(|| {
                    invalid(sequence, "scoped condition occurrence counter overflowed")
                })?;
            self.recorded_commands.splice(index..=index, original);
            return self
                .replay_scope_cancellation_at(index, CancellationCallKind::Parallel)
                .map(|()| false);
        }
        if self.cancellation_shield_depth > 0 {
            return Ok(false);
        }
        if replay.intent.is_some() {
            return Ok(true);
        }
        let Some(request) = replay
            .canonical
            .pending_request_for_scope(&replay.active_scope, &replay.tree)?
            .cloned()
        else {
            return Ok(false);
        };
        if replay.contexts.contains_key(request.context.scope_id()) {
            return Ok(false);
        }
        if request.context.scope_id() != replay.active_scope
            || descriptors
                .iter()
                .any(|descriptor| matches!(descriptor.operation, ParallelOperation::Signal(_)))
        {
            return Err(Error::CancellationScopeExecutionUnavailable);
        }
        let span = descriptors.len() as u64;
        let resolved = |sequence| {
            self.history_events
                .iter()
                .take(request.history_index)
                .any(|event| {
                    durable_event_sequence(event) == Some(sequence)
                        && scope_operation_terminal(&event.event_type)
                })
        };
        if (sequence..sequence + span).all(resolved) {
            return Ok(false);
        }
        let original: Vec<_> = self
            .recorded_commands
            .iter()
            .skip(index)
            .take(descriptors.len())
            .cloned()
            .collect();
        self.validate_scoped_group_members(descriptors, &original)?;
        let boundary = CancellationDelivery {
            request_id: request.context.request_id().into(),
            sequence,
            call_kind: CancellationCallKind::Parallel,
            sequence_span: span,
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
                "prepared scope changed its original authored group",
            ));
        }
        self.scope_delivery.as_mut().unwrap().intent = Some(ScopeDeliveryIntent {
            context: request.context,
            boundary,
            command_count: self.commands.len(),
        });
        self.matched_recorded_pending = true;
        self.command_cursor = index + descriptors.len();
        Ok(true)
    }

    fn validate_scoped_group_members(
        &self,
        descriptors: &[ParallelDescriptor],
        original: &[RecordedCommand],
    ) -> Result<()> {
        if descriptors.len() != original.len() {
            return Err(invalid(
                descriptors[0].group_path[0].parallel_group_base_sequence,
                "scoped group omits an original admitted member",
            ));
        }
        let replay = self.scope_delivery.as_ref().unwrap();
        for (descriptor, recorded) in descriptors.iter().zip(original) {
            let sequence =
                descriptor.group_path[0].parallel_group_base_sequence + descriptor.offset as u64;
            if recorded.sequence() != sequence
                || replay.tree.memberships.get(&sequence).map(String::as_str)
                    != Some(replay.active_scope.as_str())
            {
                return Err(invalid(
                    sequence,
                    "scoped group changed its original member address",
                ));
            }
            validate_scoped_group_definition(self, descriptor, recorded)?;
        }
        Ok(())
    }

    pub(super) fn scope_cleanup_timer_proof(&self, scope: &str) -> Result<Option<Value>> {
        if self.cancellation_shield_depth == 0 {
            return Ok(None);
        }
        let Some(replay) = &self.scope_delivery else {
            return Ok(None);
        };
        let Some((context, _)) = replay.contexts.get(scope) else {
            return Ok(None);
        };
        let delivered = replay
            .canonical
            .deliveries
            .values()
            .find(|delivery| {
                delivery.context == *context
                    && replay.consumed.contains(&delivery.boundary.sequence)
            })
            .ok_or_else(|| {
                Error::InvalidCooperativeCancellation(
                    "scoped cleanup lacks its consumed original delivery".into(),
                )
            })?;
        Ok(Some(
            json!({"scope_id":scope, "request_id":context.request_id(),
            "delivery_history_event_id":delivered.event.raw["id"]}),
        ))
    }

    pub(super) fn validate_scope_cleanup_timer(&self, scope: &str, sequence: u64) -> Result<()> {
        let original = self
            .scope_delivery
            .as_ref()
            .and_then(|replay| replay.cleanup_timers.get(&sequence));
        if original != self.scope_cleanup_timer_proof(scope)?.as_ref() {
            return Err(invalid(
                sequence,
                "cleanup timer changed its original delivery or shielding",
            ));
        }
        Ok(())
    }

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
        if !scalar(kind) || !path.is_empty() || request.context.scope_id() != replay.active_scope {
            return Err(Error::CancellationScopeExecutionUnavailable);
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
        self.command_cursor = index
            + if kind == CancellationCallKind::Parallel {
                delivered.boundary.sequence_span as usize
            } else {
                1
            };
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
