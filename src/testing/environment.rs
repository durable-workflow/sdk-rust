use super::WorkflowHistory;
use crate::{
    decode_wire_avro_value, encode_avro_value, ActivityContext, AvroValue, Client, Error,
    ReplayFailure, Result, Value, Worker, WorkflowContext, DEFAULT_CODEC,
};
use std::future::Future;

/// The outcome of replaying one recorded workflow snapshot.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ReplayReport {
    pub completed: bool,
    pub commands: Vec<Value>,
}

/// In-process workflow/activity registration and recorded-history replay.
pub struct WorkflowEnvironment {
    pub(super) worker: Worker,
    pub(super) last_history: Option<WorkflowHistory>,
    pub(super) max_steps: usize,
}

impl WorkflowEnvironment {
    /// Create an environment without contacting any Server.
    pub fn new() -> Result<Self> {
        Ok(Self::from_worker(Worker::new(
            Client::new("http://127.0.0.1:9")?,
            "test",
        )))
    }

    /// Reuse an application's complete registered workflow/activity set.
    /// Execution methods on this environment do not use the Worker's transport.
    pub fn from_worker(worker: Worker) -> Self {
        Self {
            worker,
            last_history: None,
            max_steps: 10_000,
        }
    }

    /// Access all existing typed, lossless and replayed-state registration APIs.
    pub fn worker_mut(&mut self) -> &mut Worker {
        &mut self.worker
    }

    pub fn register_workflow<F, Fut>(&mut self, name: impl Into<String>, handler: F)
    where
        F: Fn(WorkflowContext, Value) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Value>> + Send + 'static,
    {
        self.worker.register_workflow(name, handler);
    }

    pub fn register_workflow_avro_value<F, Fut>(&mut self, name: impl Into<String>, handler: F)
    where
        F: Fn(WorkflowContext, AvroValue) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<AvroValue>> + Send + 'static,
    {
        self.worker.register_workflow_avro_value(name, handler);
    }

    pub fn register_activity<F, Fut>(&mut self, name: impl Into<String>, handler: F)
    where
        F: Fn(ActivityContext, Value) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Value>> + Send + 'static,
    {
        self.worker.register_activity(name, handler);
    }

    pub fn register_activity_avro_value<F, Fut>(&mut self, name: impl Into<String>, handler: F)
    where
        F: Fn(ActivityContext, AvroValue) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<AvroValue>> + Send + 'static,
    {
        self.worker.register_activity_avro_value(name, handler);
    }

    /// Explicitly replace an activity callback for this environment, retaining
    /// the production Worker's duplicate-registration checks for real handlers.
    pub fn mock_activity<F, Fut>(&mut self, name: impl Into<String>, handler: F)
    where
        F: Fn(ActivityContext, Value) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Value>> + Send + 'static,
    {
        let name = name.into();
        let mut mock = Worker::new(self.worker.client.clone(), &self.worker.task_queue);
        mock.register_activity(name.clone(), handler);
        self.worker.activities.insert(
            name.clone(),
            mock.activities.remove(&name).expect("registered mock"),
        );
    }

    /// Lossless fixed-Avro counterpart to [`Self::mock_activity`].
    pub fn mock_activity_avro_value<F, Fut>(&mut self, name: impl Into<String>, handler: F)
    where
        F: Fn(ActivityContext, AvroValue) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<AvroValue>> + Send + 'static,
    {
        let name = name.into();
        let mut mock = Worker::new(self.worker.client.clone(), &self.worker.task_queue);
        mock.register_activity_avro_value(name.clone(), handler);
        self.worker.activities.insert(
            name.clone(),
            mock.activities.remove(&name).expect("registered mock"),
        );
    }

    pub fn max_steps(mut self, limit: usize) -> Self {
        self.max_steps = limit.max(1);
        self
    }
    pub fn last_history(&self) -> Option<&WorkflowHistory> {
        self.last_history.as_ref()
    }

    /// Replay through the worker's codec, durable-command and marker paths.
    /// Newly emitted durable commands are divergences, including commands added
    /// after the recorded history ended. No activity callbacks are invoked.
    pub fn replay(&self, history: &WorkflowHistory) -> Result<ReplayReport> {
        self.worker.validate_registration()?;
        let mut worker = self.worker.clone();
        if let Some(queue) = &history.task_queue {
            worker.task_queue = queue.clone();
        }
        let (decision, sequence) =
            match worker.prepare_workflow_task_execution(history.task.clone(), None)? {
                crate::PreparedWorkflowTask::Decision(decision) => (decision, 1),
                crate::PreparedWorkflowTask::Execution {
                    task,
                    context,
                    mut future,
                } => {
                    let sequence = {
                        let state = context
                            .state
                            .lock()
                            .map_err(|_| Error::WorkflowStatePoisoned)?;
                        let sequence = state
                            .recorded_commands
                            .last()
                            .map_or(1, |command| command.sequence().saturating_add(1));
                        sequence
                    };
                    let outcome = Worker::poll_workflow_future(&context, &mut future)?;
                    {
                        let state = context
                            .state
                            .lock()
                            .map_err(|_| Error::WorkflowStatePoisoned)?;
                        if !state.local_activity_requests.is_empty() {
                            validate_commands(&state.commands, sequence)?;
                        }
                    }
                    (
                        worker.finish_workflow_task_execution(&task, &context, outcome)?,
                        sequence,
                    )
                }
            };
        validate_commands(&decision.commands, sequence)?;
        validate_terminal(history, &decision.commands, sequence)?;
        Ok(ReplayReport {
            completed: decision.commands.iter().any(terminal_command),
            commands: decision.commands,
        })
    }
}

fn terminal_command(command: &Value) -> bool {
    matches!(
        command["type"].as_str(),
        Some("complete_workflow" | "fail_workflow" | "continue_as_new")
    )
}

fn validate_commands(commands: &[Value], sequence: u64) -> Result<()> {
    if let Some(command) = commands.iter().find(|command| !terminal_command(command)) {
        return Err(Error::NonDeterministicReplay(ReplayFailure::new(
            "unrecorded_command",
            Some(sequence),
            Some("end of recorded command stream".into()),
            Some(command.to_string()),
            "workflow produced a command absent from recorded history",
        )));
    }
    Ok(())
}

fn validate_terminal(history: &WorkflowHistory, commands: &[Value], sequence: u64) -> Result<()> {
    let Some((recorded, expected)) =
        history
            .events()
            .iter()
            .rev()
            .find_map(|event| match event.event_type.as_str() {
                "WorkflowCompleted" => Some((event, "complete_workflow")),
                "WorkflowFailed" => Some((event, "fail_workflow")),
                "WorkflowContinuedAsNew" => Some((event, "continue_as_new")),
                _ => None,
            })
    else {
        return Ok(());
    };
    let command = commands
        .iter()
        .find(|command| command["type"] == expected)
        .ok_or_else(|| {
            Error::NonDeterministicReplay(ReplayFailure::new(
                "workflow_terminal_mismatch",
                Some(sequence),
                Some(expected.into()),
                Some(format!("{commands:?}")),
                format!(
                    "workflow terminal decision differs from {}",
                    recorded.event_type
                ),
            ))
        })?;
    if expected == "complete_workflow" {
        let old = encode_avro_value(&decode_wire_avro_value(
            &recorded.payload["output"],
            DEFAULT_CODEC,
        )?)?;
        let new = encode_avro_value(&decode_wire_avro_value(&command["result"], DEFAULT_CODEC)?)?;
        if old != new {
            return Err(Error::NonDeterministicReplay(ReplayFailure::new(
                "workflow_result_mismatch",
                Some(sequence),
                Some(old.blob),
                Some(new.blob),
                "workflow completion differs from recorded output",
            )));
        }
    }
    Ok(())
}
