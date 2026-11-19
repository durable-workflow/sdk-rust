use crate::{replay_clock, WorkflowContext, WorkflowState};
use std::{cell::Cell, panic::AssertUnwindSafe};

thread_local! {
    static RECONSTRUCTING: Cell<bool> = const { Cell::new(false) };
}

/// The live execution region for workflow observations.
///
/// This read-only view does not authorize different durable workflow commands.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum WorkflowReplayState {
    /// Reconstructing committed history or a query. Process logs are suppressed.
    Replaying,
    /// New execution beyond the committed prefix. A task retry can repeat logs.
    Executing,
    /// Outside this workflow invocation, or its state cannot be read safely.
    Unavailable,
}

pub(crate) struct ReconstructionGuard(bool);

impl ReconstructionGuard {
    pub(crate) fn enter() -> Self {
        Self(RECONSTRUCTING.with(|active| active.replace(true)))
    }
}

impl Drop for ReconstructionGuard {
    fn drop(&mut self) {
        RECONSTRUCTING.with(|active| active.set(self.0));
    }
}

fn replaying(state: &WorkflowState) -> bool {
    state.observation_terminal
        || state.command_cursor < state.recorded_commands.len()
        || state.selection_marker_cursor < state.selection_markers.len()
        || (state.observation_pending && state.commands.is_empty())
        || state.cancellation_delivery_intent.is_some()
        || state.cancellation_scope_opening.is_some()
        || (state.cancellation_history.delivery.is_some() && !state.cancellation_consumed)
        || (state.recorded_continue_as_new_sequence.is_some() && !state.continue_as_new_consumed)
        || state.scope_delivery.as_ref().is_some_and(|replay| {
            replay.intent.is_some()
                || replay
                    .canonical
                    .deliveries
                    .keys()
                    .any(|sequence| !replay.consumed.contains(sequence))
        })
}

impl WorkflowContext {
    /// Observe the current replay boundary without changing commands or history.
    ///
    /// Query/offline reconstruction remains `Replaying` after the last recorded
    /// command. A retained context outside its invocation is `Unavailable`.
    pub fn replay_state(&self) -> WorkflowReplayState {
        let Some(query) = replay_clock::observation_mode(self) else {
            return WorkflowReplayState::Unavailable;
        };
        let Ok(state) = self.state.lock() else {
            return WorkflowReplayState::Unavailable;
        };
        if query || RECONSTRUCTING.with(Cell::get) || replaying(&state) {
            WorkflowReplayState::Replaying
        } else {
            WorkflowReplayState::Executing
        }
    }

    /// Log new workflow execution through the application's `tracing` subscriber.
    ///
    /// Committed replay, queries and discarded invocations stay quiet. Handlers
    /// must be bounded/nonblocking. This process logger is not an exactly-once
    /// outbox: logs from an uncommitted task can repeat when the task retries.
    pub fn logger(&self) -> WorkflowLogger {
        WorkflowLogger {
            context: self.clone(),
        }
    }
}

/// Replay-aware host logging with SDK-bound namespace/workflow/run fields.
#[derive(Clone)]
pub struct WorkflowLogger {
    context: WorkflowContext,
}

impl std::fmt::Debug for WorkflowLogger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkflowLogger").finish_non_exhaustive()
    }
}

impl WorkflowLogger {
    /// Emit only in a new execution region with a complete bound run identity.
    /// Logger failures do not manufacture success, commands or workflow history.
    pub fn log(&self, level: tracing::Level, message: &str) {
        if self.context.replay_state() != WorkflowReplayState::Executing {
            return;
        }
        let identity = self.context.state.lock().ok().and_then(|state| {
            let namespace = state
                .observation_namespace
                .as_ref()
                .filter(|v| !v.is_empty())?;
            let workflow = state.workflow_id.as_ref().filter(|v| !v.is_empty())?;
            let run = state.run_id.as_ref().filter(|v| !v.is_empty())?;
            Some((namespace.clone(), workflow.clone(), run.clone()))
        });
        let Some((namespace, workflow, run)) = identity else {
            return;
        };
        // Invoke application code without holding workflow state. The temporary
        // observation scope also suppresses recursive logs from a subscriber.
        let _scope = replay_clock::Guard::unavailable();
        let _ = std::panic::catch_unwind(AssertUnwindSafe(|| {
            macro_rules! emit {
                ($level:expr) => {
                    tracing::event!(target: "durable_workflow::workflow", $level,
                        namespace = namespace.as_str(),
                        workflow_instance_id = workflow.as_str(),
                        workflow_run_id = run.as_str(), message)
                };
            }
            match level {
                tracing::Level::TRACE => emit!(tracing::Level::TRACE),
                tracing::Level::DEBUG => emit!(tracing::Level::DEBUG),
                tracing::Level::INFO => emit!(tracing::Level::INFO),
                tracing::Level::WARN => emit!(tracing::Level::WARN),
                tracing::Level::ERROR => emit!(tracing::Level::ERROR),
            }
        }));
    }

    /// Emit a trace observation in new workflow execution.
    pub fn trace(&self, message: &str) {
        self.log(tracing::Level::TRACE, message);
    }
    /// Emit a debug observation in new workflow execution.
    pub fn debug(&self, message: &str) {
        self.log(tracing::Level::DEBUG, message);
    }
    /// Emit an informational observation in new workflow execution.
    pub fn info(&self, message: &str) {
        self.log(tracing::Level::INFO, message);
    }
    /// Emit a warning observation in new workflow execution.
    pub fn warn(&self, message: &str) {
        self.log(tracing::Level::WARN, message);
    }
    /// Emit an error observation in new workflow execution.
    pub fn error(&self, message: &str) {
        self.log(tracing::Level::ERROR, message);
    }
}

#[cfg(test)]
mod tests;
