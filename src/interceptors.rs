//! Ordered worker interceptors around workflow tasks, activities and queries.
//!
//! Register interceptors with [`Worker::interceptor`](crate::Worker::interceptor)
//! or [`Worker::interceptors`](crate::Worker::interceptors). The first
//! registered interceptor is the outermost wrapper: it runs first before the
//! core and last after it. Each hook receives a context and a `next` handle;
//! running `next` continues the chain and the innermost `next` runs the
//! SDK's real handling. Every hook has a passthrough default, so an
//! interceptor implements only the hooks it needs.
//!
//! | Hook | Wraps | Context | `next` returns |
//! | --- | --- | --- | --- |
//! | [`WorkerInterceptor::execute_workflow_task`] | one workflow task (replay plus new execution) | [`WorkflowTaskInterceptorContext`] | [`WorkflowTaskOutcome`] |
//! | [`WorkerInterceptor::execute_activity`] | one activity or local activity attempt | [`ActivityInterceptorContext`] | the activity result |
//! | [`WorkerInterceptor::execute_query_task`] | one query task | [`QueryTaskInterceptorContext`] | the query result or [`QueryTaskFailure`] |
//!
//! Workflow-task interceptors cannot change what the workflow records. The
//! core always runs the task as received ([`WorkflowTaskNext::run`] takes no
//! input) and the worker submits exactly the commands the core produced. The
//! outcome an interceptor sees is a read-only view. If the core fails, the
//! worker reports that failure even when an interceptor returns `Ok`. If an
//! interceptor fails after the core succeeded, or returns without running
//! `next`, the worker fails the workflow task ([`Error::WorkflowTaskInterceptor`]);
//! nothing is recorded and the Server retries the task. Interceptors run on
//! every workflow task. [`WorkflowTaskInterceptorContext::replay_state`] says
//! whether the task first re-ran a committed history prefix; the worker never
//! resubmits replayed commands, so an outcome's commands are always new for
//! that task. A failed task's retry can repeat an outcome; use
//! [`WorkflowTaskInterceptorContext::attempt`] to detect retries.
//!
//! Built-in [`ExecutionTelemetry`](crate::telemetry::ExecutionTelemetry) stays
//! outermost: its spans wrap the whole interceptor chain.

use crate::{
    recorded_commands, ActivityContext, ActivityHandler, AvroValue, Error, QueryTask,
    QueryTaskExecutionFailure, Result, Value, Worker, WorkflowIdentity, WorkflowReplayState,
    WorkflowTask, WorkflowTaskDecision,
};
use std::{
    fmt,
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

/// The boxed `Send` future an interceptor hook returns.
pub type InterceptorFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Ordered hooks around worker task execution.
///
/// Every method defaults to calling `next` unchanged, so implement only the
/// hooks you need. Hooks return boxed futures, typically
/// `Box::pin(async move { ... })`.
///
/// ```
/// use durable_workflow::interceptors::{
///     InterceptorFuture, WorkerInterceptor, WorkflowTaskInterceptorContext, WorkflowTaskNext,
///     WorkflowTaskOutcome,
/// };
/// use durable_workflow::Result;
///
/// struct AuditLog;
///
/// impl WorkerInterceptor for AuditLog {
///     fn execute_workflow_task<'a>(
///         &'a self,
///         context: &'a WorkflowTaskInterceptorContext,
///         next: WorkflowTaskNext<'a>,
///     ) -> InterceptorFuture<'a, Result<WorkflowTaskOutcome>> {
///         Box::pin(async move {
///             let outcome = next.run().await?;
///             // The commands are always new for this task; a retry has attempt > 1.
///             println!(
///                 "{} attempt {} produced {} commands",
///                 context.task_id(),
///                 context.attempt(),
///                 outcome.commands().len()
///             );
///             Ok(outcome)
///         })
///     }
/// }
/// ```
pub trait WorkerInterceptor: Send + Sync + 'static {
    /// Wrap one workflow task. Run `next` exactly once.
    ///
    /// The returned outcome is ignored: the worker settles the core's own
    /// decision. Returning an error after the core succeeded fails the
    /// workflow task (not the workflow).
    fn execute_workflow_task<'a>(
        &'a self,
        context: &'a WorkflowTaskInterceptorContext,
        next: WorkflowTaskNext<'a>,
    ) -> InterceptorFuture<'a, Result<WorkflowTaskOutcome>> {
        let _ = context;
        next.run()
    }

    /// Wrap one activity attempt, remote or local.
    ///
    /// Pass the context to `next`, optionally with transformed arguments
    /// ([`ActivityInterceptorContext::with_args`]). The returned value is the
    /// activity result the worker reports, so an interceptor may transform it,
    /// handle an error, or skip `next` entirely.
    fn execute_activity(
        &self,
        context: ActivityInterceptorContext,
        next: ActivityNext,
    ) -> InterceptorFuture<'_, Result<AvroValue>> {
        next.run(context)
    }

    /// Wrap one query task. The returned value is what the worker reports.
    fn execute_query_task<'a>(
        &'a self,
        context: &'a QueryTaskInterceptorContext,
        next: QueryTaskNext<'a>,
    ) -> InterceptorFuture<'a, std::result::Result<AvroValue, QueryTaskFailure>> {
        let _ = context;
        next.run()
    }
}

impl<T: WorkerInterceptor + ?Sized> WorkerInterceptor for Arc<T> {
    fn execute_workflow_task<'a>(
        &'a self,
        context: &'a WorkflowTaskInterceptorContext,
        next: WorkflowTaskNext<'a>,
    ) -> InterceptorFuture<'a, Result<WorkflowTaskOutcome>> {
        (**self).execute_workflow_task(context, next)
    }

    fn execute_activity(
        &self,
        context: ActivityInterceptorContext,
        next: ActivityNext,
    ) -> InterceptorFuture<'_, Result<AvroValue>> {
        (**self).execute_activity(context, next)
    }

    fn execute_query_task<'a>(
        &'a self,
        context: &'a QueryTaskInterceptorContext,
        next: QueryTaskNext<'a>,
    ) -> InterceptorFuture<'a, std::result::Result<AvroValue, QueryTaskFailure>> {
        (**self).execute_query_task(context, next)
    }
}

/// An interceptor that only calls `next`; a base for partial interceptors.
#[derive(Clone, Copy, Debug, Default)]
pub struct PassthroughWorkerInterceptor;

impl WorkerInterceptor for PassthroughWorkerInterceptor {}

#[derive(Clone)]
pub(crate) struct RegisteredInterceptor {
    name: &'static str,
    interceptor: Arc<dyn WorkerInterceptor>,
}

impl RegisteredInterceptor {
    pub(crate) fn new<I: WorkerInterceptor>(interceptor: I) -> Self {
        Self {
            name: std::any::type_name::<I>(),
            interceptor: Arc::new(interceptor),
        }
    }
}

impl fmt::Debug for RegisteredInterceptor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name)
    }
}

// ---------------------------------------------------------------------------
// Workflow tasks
// ---------------------------------------------------------------------------

/// Read-only context of one workflow task.
#[derive(Clone, Debug)]
pub struct WorkflowTaskInterceptorContext {
    worker_id: String,
    task_queue: String,
    namespace: String,
    task_id: String,
    workflow_id: Option<String>,
    run_id: Option<String>,
    workflow_type: String,
    attempt: u64,
    history_event_count: usize,
    total_history_events: Option<u64>,
    history_size_bytes: Option<u64>,
    cancel_requested: bool,
    observability_context: Option<Value>,
    replay_state: WorkflowReplayState,
}

impl WorkflowTaskInterceptorContext {
    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }
    pub fn task_queue(&self) -> &str {
        &self.task_queue
    }
    pub fn namespace(&self) -> &str {
        &self.namespace
    }
    pub fn task_id(&self) -> &str {
        &self.task_id
    }
    pub fn workflow_id(&self) -> Option<&str> {
        self.workflow_id.as_deref()
    }
    pub fn run_id(&self) -> Option<&str> {
        self.run_id.as_deref()
    }
    pub fn workflow_type(&self) -> &str {
        &self.workflow_type
    }
    /// The workflow-task attempt; above 1 when the Server retries the task.
    pub fn attempt(&self) -> u64 {
        self.attempt
    }
    /// Number of history events delivered with the task.
    pub fn history_event_count(&self) -> usize {
        self.history_event_count
    }
    /// The Server's committed history length, when it reported one.
    pub fn total_history_events(&self) -> Option<u64> {
        self.total_history_events
    }
    /// The Server's committed history size, when it reported one.
    pub fn history_size_bytes(&self) -> Option<u64> {
        self.history_size_bytes
    }
    pub fn cancel_requested(&self) -> bool {
        self.cancel_requested
    }
    /// The task's optional, untrusted observability metadata (trace carrier).
    pub fn observability_context(&self) -> Option<&Value> {
        self.observability_context.as_ref()
    }
    /// Whether this task re-runs a committed history prefix before new execution.
    ///
    /// [`WorkflowReplayState::Replaying`]: the history holds commands that
    /// earlier workflow tasks committed, so workflow code re-runs them before
    /// any new execution. [`WorkflowReplayState::Executing`]: no committed
    /// prefix. [`WorkflowReplayState::Unavailable`]: the history could not be
    /// read (the core reports why).
    ///
    /// Replayed commands are never resubmitted, so the outcome's commands are
    /// new for this task in every state. Do not skip auditing a task because it
    /// replayed. A retry of a failed task (`attempt() > 1`) can repeat an
    /// outcome.
    pub fn replay_state(&self) -> WorkflowReplayState {
        self.replay_state
    }
}

/// A read-only view of what the workflow-task core produced.
///
/// It is only obtained from [`WorkflowTaskNext::run`]. The worker settles
/// the core's own decision, never a returned outcome.
#[derive(Clone, Debug)]
pub struct WorkflowTaskOutcome {
    commands: Option<Arc<Vec<Value>>>,
}

impl WorkflowTaskOutcome {
    /// The commands the worker will submit, in order. Empty when the lease
    /// was lost or the task only waits for more history.
    ///
    /// This is the view before built-in
    /// [`ExecutionTelemetry`](crate::telemetry::ExecutionTelemetry) stamps its
    /// observability fields: with telemetry enabled, the submitted commands
    /// also carry those fields. Their order and content are otherwise the
    /// same.
    pub fn commands(&self) -> &[Value] {
        self.commands.as_deref().map_or(&[], Vec::as_slice)
    }

    /// The worker lost or released the task's lease; nothing is reported.
    pub fn is_lease_lost(&self) -> bool {
        self.commands.is_none()
    }
}

/// A read-only description of the workflow-task core's own error.
///
/// [`WorkflowTaskNext::run`] returns it as [`Error::WorkflowTaskFailed`]. It
/// does not own the error: the worker keeps the original and reports it
/// whatever an interceptor returns or retains.
#[derive(Clone)]
pub struct WorkflowTaskFailure(Arc<FailureView>);

struct FailureView {
    kind: String,
    message: String,
    debug: String,
}

impl WorkflowTaskFailure {
    fn describe(error: &Error) -> Self {
        let debug = format!("{error:?}");
        let kind = debug
            .split(|c: char| !(c.is_alphanumeric() || c == '_'))
            .next()
            .unwrap_or_default()
            .to_owned();
        Self(Arc::new(FailureView {
            kind,
            message: error.to_string(),
            debug,
        }))
    }

    /// The [`Error`](enum@crate::Error) variant name, such as `"WorkflowNotRegistered"`.
    pub fn kind(&self) -> &str {
        &self.0.kind
    }

    /// The error's display text.
    pub fn message(&self) -> &str {
        &self.0.message
    }

    /// The error's debug text.
    pub fn debug(&self) -> &str {
        &self.0.debug
    }
}

impl fmt::Debug for WorkflowTaskFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0.debug)
    }
}

impl fmt::Display for WorkflowTaskFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0.message)
    }
}

impl std::error::Error for WorkflowTaskFailure {}

type WorkflowCoreResult = Result<Option<WorkflowTaskDecision>>;

enum CoreRecord {
    Completed {
        decision: WorkflowTaskDecision,
        commands: Arc<Vec<Value>>,
    },
    LeaseLost,
    Failed(Error),
}

struct WorkflowCore<'a> {
    future: InterceptorFuture<'a, WorkflowCoreResult>,
    record: &'a Mutex<Option<CoreRecord>>,
    started: &'a AtomicBool,
}

/// How far the workflow-task core got inside the interceptor chain.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CoreProgress {
    /// The core never began; nothing it does happened.
    NotStarted,
    /// The core began but an interceptor dropped it before it finished.
    Abandoned,
    /// The core finished and its result was recorded.
    Completed,
}

/// Continues a workflow-task chain; the innermost `next` runs the task.
pub struct WorkflowTaskNext<'a> {
    chain: &'a [RegisteredInterceptor],
    index: usize,
    reached: &'a AtomicUsize,
    context: &'a WorkflowTaskInterceptorContext,
    core: WorkflowCore<'a>,
}

impl<'a> WorkflowTaskNext<'a> {
    /// Run the rest of the chain and the core with the task as received.
    ///
    /// A core failure is returned as [`Error::WorkflowTaskFailed`].
    pub fn run(self) -> InterceptorFuture<'a, Result<WorkflowTaskOutcome>> {
        if let Some(registered) = self.chain.get(self.index) {
            // Record the deepest hook that actually began running, on its
            // first poll: creating `next.run()` and dropping it is not running it.
            let (reached, index) = (self.reached, self.index);
            let next = WorkflowTaskNext {
                index: self.index + 1,
                ..self
            };
            let hook = registered
                .interceptor
                .execute_workflow_task(next.context, next);
            return Box::pin(async move {
                reached.fetch_max(index, Ordering::SeqCst);
                hook.await
            });
        }
        let WorkflowCore {
            future,
            record,
            started,
        } = self.core;
        Box::pin(async move {
            started.store(true, Ordering::SeqCst);
            let (stored, outcome) = match future.await {
                Ok(Some(mut decision)) => {
                    let commands = Arc::new(std::mem::take(&mut decision.commands));
                    let outcome = WorkflowTaskOutcome {
                        commands: Some(commands.clone()),
                    };
                    (CoreRecord::Completed { decision, commands }, Ok(outcome))
                }
                Ok(None) => (
                    CoreRecord::LeaseLost,
                    Ok(WorkflowTaskOutcome { commands: None }),
                ),
                Err(error) => {
                    let failure = WorkflowTaskFailure::describe(&error);
                    (
                        CoreRecord::Failed(error),
                        Err(Error::WorkflowTaskFailed(failure)),
                    )
                }
            };
            if let Ok(mut slot) = record.lock() {
                *slot = Some(stored);
            }
            outcome
        })
    }
}

impl Worker {
    /// The interceptor context for a workflow task, or `None` without interceptors.
    pub(crate) fn workflow_task_interceptor_context(
        &self,
        task: &WorkflowTask,
    ) -> Option<WorkflowTaskInterceptorContext> {
        if self.interceptors.is_empty() {
            return None;
        }
        Some(WorkflowTaskInterceptorContext {
            worker_id: self.worker_id.clone(),
            task_queue: self.task_queue.clone(),
            namespace: self.client.namespace.clone(),
            task_id: task.task_id.clone(),
            workflow_id: task.workflow_id.clone(),
            run_id: task.run_id.clone(),
            workflow_type: task.workflow_type.clone(),
            attempt: task.workflow_task_attempt,
            history_event_count: task.history_events.len(),
            total_history_events: task.total_history_events,
            history_size_bytes: task.history_size_bytes,
            cancel_requested: task.cancel_requested,
            observability_context: task.observability_context.clone(),
            replay_state: task_replay_state(task),
        })
    }

    /// Run one workflow task's core inside the interceptor chain.
    ///
    /// Returns exactly the core's decision or error. An interceptor failure
    /// after core success, or a chain that never ran the core, becomes
    /// [`Error::WorkflowTaskInterceptor`].
    pub(crate) async fn intercept_workflow_task<F>(
        &self,
        context: Option<WorkflowTaskInterceptorContext>,
        core: F,
    ) -> WorkflowCoreResult
    where
        F: Future<Output = WorkflowCoreResult> + Send,
    {
        self.intercept_workflow_task_tracked(context, core).await.0
    }

    /// [`Self::intercept_workflow_task`], also reporting how far the core got.
    pub(crate) async fn intercept_workflow_task_tracked<F>(
        &self,
        context: Option<WorkflowTaskInterceptorContext>,
        core: F,
    ) -> (WorkflowCoreResult, CoreProgress)
    where
        F: Future<Output = WorkflowCoreResult> + Send,
    {
        let Some(context) = context else {
            return (core.await, CoreProgress::Completed);
        };
        let record = Mutex::new(None);
        let reached = AtomicUsize::new(0);
        let started = AtomicBool::new(false);
        let chain = WorkflowTaskNext {
            chain: &self.interceptors,
            index: 0,
            reached: &reached,
            context: &context,
            core: WorkflowCore {
                future: Box::pin(core),
                record: &record,
                started: &started,
            },
        }
        .run()
        .await
        .map(drop);
        let Some(record) = record.into_inner().ok().flatten() else {
            let error = match &chain {
                Ok(()) => String::new(),
                Err(error) => format!(": {error}"),
            };
            if started.load(Ordering::SeqCst) {
                return (
                    Err(Error::WorkflowTaskInterceptor(format!(
                        "a workflow task interceptor dropped the workflow task before it completed{error}"
                    ))),
                    CoreProgress::Abandoned,
                );
            }
            let skipped = &self.interceptors[reached
                .load(Ordering::SeqCst)
                .min(self.interceptors.len() - 1)];
            let message = match chain {
                Ok(()) => format!(
                    "workflow task interceptor {} returned without running next",
                    skipped.name
                ),
                Err(_) => format!(
                    "workflow task interceptor {} failed without running next{error}",
                    skipped.name
                ),
            };
            return (
                Err(Error::WorkflowTaskInterceptor(message)),
                CoreProgress::NotStarted,
            );
        };
        let result = match record {
            CoreRecord::Failed(error) => Err(error),
            CoreRecord::LeaseLost => Ok(None),
            CoreRecord::Completed {
                mut decision,
                commands,
            } => match chain {
                Ok(()) => {
                    decision.commands =
                        Arc::try_unwrap(commands).unwrap_or_else(|retained| (*retained).clone());
                    Ok(Some(decision))
                }
                Err(error) => Err(Error::WorkflowTaskInterceptor(format!(
                    "workflow task interceptor failed: {error}"
                ))),
            },
        };
        (result, CoreProgress::Completed)
    }
}

/// Whether the task's history holds commands committed by earlier tasks.
fn task_replay_state(task: &WorkflowTask) -> WorkflowReplayState {
    match recorded_commands(
        &task.history_events,
        &task.payload_codec,
        WorkflowIdentity {
            workflow_id: task.workflow_id.clone(),
            run_id: task.run_id.clone(),
        },
    ) {
        Ok(commands) if commands.is_empty() => WorkflowReplayState::Executing,
        Ok(_) => WorkflowReplayState::Replaying,
        Err(_) => WorkflowReplayState::Unavailable,
    }
}

// ---------------------------------------------------------------------------
// Activities
// ---------------------------------------------------------------------------

/// Context of one activity attempt; owns the arguments passed down the chain.
#[derive(Clone, Debug)]
pub struct ActivityInterceptorContext {
    activity: ActivityContext,
    args: AvroValue,
    local: bool,
}

impl ActivityInterceptorContext {
    pub fn worker_id(&self) -> &str {
        &self.activity.worker_id
    }
    pub fn task_queue(&self) -> &str {
        &self.activity.task_queue
    }
    pub fn namespace(&self) -> &str {
        &self.activity.client.namespace
    }
    pub fn task_id(&self) -> &str {
        &self.activity.task_id
    }
    pub fn activity_attempt_id(&self) -> &str {
        &self.activity.activity_attempt_id
    }
    pub fn activity_type(&self) -> &str {
        &self.activity.activity_type
    }
    pub fn attempt(&self) -> u64 {
        self.activity.attempt_number
    }
    pub fn workflow_id(&self) -> Option<&str> {
        self.activity.workflow_id()
    }
    pub fn run_id(&self) -> Option<&str> {
        self.activity.run_id()
    }
    /// Whether this is an inline local activity run by a workflow task.
    pub fn is_local(&self) -> bool {
        self.local
    }
    /// The decoded arguments the activity will receive.
    pub fn args(&self) -> &AvroValue {
        &self.args
    }
    /// The [`ActivityContext`] the activity handler will receive.
    pub fn activity_context(&self) -> &ActivityContext {
        &self.activity
    }
    /// Replace the arguments passed to the rest of the chain.
    pub fn with_args(mut self, args: AvroValue) -> Self {
        self.args = args;
        self
    }
}

/// Continues an activity chain; the innermost `next` runs the activity.
pub struct ActivityNext {
    chain: Arc<[RegisteredInterceptor]>,
    index: usize,
    handler: ActivityHandler,
}

impl ActivityNext {
    /// Run the rest of the chain and the activity handler.
    pub fn run(
        self,
        context: ActivityInterceptorContext,
    ) -> InterceptorFuture<'static, Result<AvroValue>> {
        let Some(registered) = self.chain.get(self.index) else {
            return (self.handler)(context.activity, context.args);
        };
        let interceptor = registered.interceptor.clone();
        let next = ActivityNext {
            index: self.index + 1,
            ..self
        };
        Box::pin(async move { interceptor.execute_activity(context, next).await })
    }
}

impl fmt::Debug for ActivityNext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ActivityNext")
            .field(
                "remaining",
                &&self.chain[self.index.min(self.chain.len())..],
            )
            .finish()
    }
}

/// Run an activity handler inside the interceptor chain.
pub(crate) fn intercept_activity(
    chain: &Arc<[RegisteredInterceptor]>,
    handler: &ActivityHandler,
    activity: ActivityContext,
    args: AvroValue,
    local: bool,
) -> InterceptorFuture<'static, Result<AvroValue>> {
    if chain.is_empty() {
        return handler(activity, args);
    }
    ActivityNext {
        chain: chain.clone(),
        index: 0,
        handler: handler.clone(),
    }
    .run(ActivityInterceptorContext {
        activity,
        args,
        local,
    })
}

// ---------------------------------------------------------------------------
// Queries
// ---------------------------------------------------------------------------

/// Read-only context of one query task.
#[derive(Clone, Debug)]
pub struct QueryTaskInterceptorContext {
    worker_id: String,
    task_queue: String,
    namespace: String,
    query_task_id: String,
    attempt: u64,
    workflow_id: Option<String>,
    run_id: Option<String>,
    workflow_type: String,
    query_name: String,
    observability_context: Option<Value>,
}

impl QueryTaskInterceptorContext {
    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }
    pub fn task_queue(&self) -> &str {
        &self.task_queue
    }
    pub fn namespace(&self) -> &str {
        &self.namespace
    }
    pub fn query_task_id(&self) -> &str {
        &self.query_task_id
    }
    pub fn attempt(&self) -> u64 {
        self.attempt
    }
    pub fn workflow_id(&self) -> Option<&str> {
        self.workflow_id.as_deref()
    }
    pub fn run_id(&self) -> Option<&str> {
        self.run_id.as_deref()
    }
    pub fn workflow_type(&self) -> &str {
        &self.workflow_type
    }
    pub fn query_name(&self) -> &str {
        &self.query_name
    }
    /// The task's optional, untrusted observability metadata (trace carrier).
    pub fn observability_context(&self) -> Option<&Value> {
        self.observability_context.as_ref()
    }
}

/// A query task failure the worker reports to the Server.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryTaskFailure {
    reason: String,
    message: String,
    failure_type: String,
}

impl QueryTaskFailure {
    /// A failure with a machine `reason`, human `message` and `failure_type`.
    pub fn new(
        reason: impl Into<String>,
        message: impl Into<String>,
        failure_type: impl Into<String>,
    ) -> Self {
        Self {
            reason: reason.into(),
            message: message.into(),
            failure_type: failure_type.into(),
        }
    }
    pub fn reason(&self) -> &str {
        &self.reason
    }
    pub fn message(&self) -> &str {
        &self.message
    }
    pub fn failure_type(&self) -> &str {
        &self.failure_type
    }
}

impl fmt::Display for QueryTaskFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.reason, self.message)
    }
}

impl std::error::Error for QueryTaskFailure {}

impl From<QueryTaskExecutionFailure> for QueryTaskFailure {
    fn from(failure: QueryTaskExecutionFailure) -> Self {
        Self {
            reason: failure.reason,
            message: failure.message,
            failure_type: failure.failure_type,
        }
    }
}

impl From<QueryTaskFailure> for QueryTaskExecutionFailure {
    fn from(failure: QueryTaskFailure) -> Self {
        Self {
            reason: failure.reason,
            message: failure.message,
            failure_type: failure.failure_type,
        }
    }
}

type QueryResult = std::result::Result<AvroValue, QueryTaskFailure>;

/// Continues a query chain; the innermost `next` answers the query.
pub struct QueryTaskNext<'a> {
    chain: &'a [RegisteredInterceptor],
    context: &'a QueryTaskInterceptorContext,
    core: InterceptorFuture<'a, QueryResult>,
}

impl<'a> QueryTaskNext<'a> {
    /// Run the rest of the chain and the query handler.
    pub fn run(self) -> InterceptorFuture<'a, QueryResult> {
        match self.chain.split_first() {
            Some((registered, rest)) => registered.interceptor.execute_query_task(
                self.context,
                QueryTaskNext {
                    chain: rest,
                    ..self
                },
            ),
            None => self.core,
        }
    }
}

impl Worker {
    /// The interceptor context for a query task, or `None` without interceptors.
    pub(crate) fn query_task_interceptor_context(
        &self,
        task: &QueryTask,
    ) -> Option<QueryTaskInterceptorContext> {
        if self.interceptors.is_empty() {
            return None;
        }
        Some(QueryTaskInterceptorContext {
            worker_id: self.worker_id.clone(),
            task_queue: self.task_queue.clone(),
            namespace: self.client.namespace.clone(),
            query_task_id: task.query_task_id.clone(),
            attempt: task.query_task_attempt,
            workflow_id: task.workflow_id.clone(),
            run_id: task.run_id.clone(),
            workflow_type: task.workflow_type.clone(),
            query_name: task.query_name.clone(),
            observability_context: task.observability_context.clone(),
        })
    }

    /// Run one query task's core inside the interceptor chain.
    pub(crate) async fn intercept_query_task<F>(
        &self,
        context: Option<QueryTaskInterceptorContext>,
        core: F,
    ) -> std::result::Result<AvroValue, QueryTaskExecutionFailure>
    where
        F: Future<Output = std::result::Result<AvroValue, QueryTaskExecutionFailure>> + Send,
    {
        let Some(context) = context else {
            return core.await;
        };
        QueryTaskNext {
            chain: &self.interceptors,
            context: &context,
            core: Box::pin(async move { core.await.map_err(QueryTaskFailure::from) }),
        }
        .run()
        .await
        .map_err(QueryTaskExecutionFailure::from)
    }
}
