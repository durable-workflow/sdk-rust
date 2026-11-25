//! Synchronous pre-accept update validation (durable-workflow/.github#234).
//!
//! A worker that registers an update validator advertises it in the
//! workflow command contract's `update_validators` list and the
//! `update_validation_tasks` capability. A Server that advertises
//! `synchronous_update_validation` then holds each such update until a
//! validator-capable worker approves it. The worker's workflow long poll asks
//! for `task_kinds: ["workflow", "update_validation"]`, so one capacity
//! reservation covers either kind. A validation task is answered by
//! reconstructing the run from its committed history, as a query does, and
//! invoking only the validator: the update handler never runs, and no
//! workflow command is ever completed for the task. An approval lets the
//! Server accept the update; a rejection is recorded by the Server as a
//! rejected update and is never delivered to workflow code.

use crate::{
    path_segment, AvroValue, Client, Error, ManagedPollOutcome, QueryContext, QueryTask,
    RegisteredQuery, RegistrationKind, RequestProtocol, Result, Worker,
};
use serde::Serialize;
use serde_json::{json, Value};
use std::{
    any::TypeId,
    collections::BTreeMap,
    fmt,
    sync::{atomic::Ordering, Arc, Mutex, PoisonError},
    time::Duration,
};

/// The Server's failure reason for a validator that refused the update.
pub const UPDATE_VALIDATOR_REJECTED: &str = "update_validator_rejected";
/// The worker's failure reason when it could not run the validator.
pub const UPDATE_VALIDATION_WORKER_FAILED: &str = "update_validation_worker_failed";

const WORKFLOW_POLL_PATH: &str = "/worker/workflow-tasks/poll";
const TASK_KINDS: [&str; 2] = ["workflow", "update_validation"];
const COMMAND_REFUSED_TYPE: &str = "UpdateValidatorCommandRefused";
const VALIDATOR_ERROR_TYPE: &str = "UpdateValidatorError";

/// Completion refusals that mean another attempt or worker owns the task now.
const FENCED_REASONS: [&str; 7] = [
    "update_validation_task_not_found",
    "duplicate_update_validation_completion",
    "update_validation_task_not_leased",
    "update_validation_lease_owner_mismatch",
    "stale_update_validation_completion",
    "update_validation_lease_expired",
    "update_validator_worker_lost",
];

/// Why an update validator refused an update.
///
/// Return it from a validator registered with
/// [`Worker::register_update_validator`] or
/// [`Worker::register_replayed_update_validator`]. The update is rejected
/// before acceptance: the Server records a rejected update with reason
/// `update_validator_rejected`, the update handler never runs and workflow
/// code never sees the update. The caller receives the message and validation
/// errors through [`Error::update_validator_rejection`].
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct UpdateRejection {
    /// The rejection message returned to the caller.
    pub message: String,
    /// An optional application failure type, recorded on the validation task.
    pub failure_type: Option<String>,
    /// Field names mapped to their validation messages.
    pub validation_errors: BTreeMap<String, Vec<String>>,
}

impl UpdateRejection {
    /// Reject an update with a message.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            failure_type: None,
            validation_errors: BTreeMap::new(),
        }
    }

    /// Name the application failure type.
    pub fn with_type(mut self, failure_type: impl Into<String>) -> Self {
        self.failure_type = Some(failure_type.into());
        self
    }

    /// Add one validation message for a field.
    pub fn with_validation_error(
        mut self,
        field: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        self.validation_errors
            .entry(field.into())
            .or_default()
            .push(message.into());
        self
    }

    fn wire(&self, reason: &str) -> Value {
        let mut failure = json!({"message": self.message, "reason": reason});
        if let Some(failure_type) = &self.failure_type {
            failure["type"] = json!(failure_type);
        }
        if !self.validation_errors.is_empty() {
            failure["validation_errors"] = json!(self.validation_errors);
        }
        failure
    }
}

impl fmt::Display for UpdateRejection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for UpdateRejection {}

impl From<Error> for UpdateRejection {
    fn from(error: Error) -> Self {
        match error {
            Error::UpdateValidatorRejected(rejection) => rejection,
            error => Self::new(error.to_string()).with_type(VALIDATOR_ERROR_TYPE),
        }
    }
}

impl From<serde_json::Error> for UpdateRejection {
    fn from(error: serde_json::Error) -> Self {
        Self::new(error.to_string()).with_type(VALIDATOR_ERROR_TYPE)
    }
}

/// The refusal for a validator that added a workflow command.
pub(crate) fn scheduled_work_refusal() -> UpdateRejection {
    UpdateRejection::new(
        "update validators are read-only: the validator tried to schedule durable workflow work",
    )
    .with_type(COMMAND_REFUSED_TYPE)
}

/// An update the Server rejected because the workflow's validator refused it.
///
/// Nothing was accepted: the update handler did not run and workflow code
/// never saw the update. Read it with [`Error::update_validator_rejection`].
/// `body` is the Server's full answer.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct UpdateValidatorRejected {
    pub status: u16,
    pub workflow_id: Option<String>,
    pub run_id: Option<String>,
    pub update_name: Option<String>,
    pub update_id: Option<String>,
    pub message: String,
    pub validation_errors: BTreeMap<String, Vec<String>>,
    pub body: Value,
}

impl Error {
    /// Whether the workflow's update validator rejected the update before
    /// acceptance (HTTP 422, `reason: "update_validator_rejected"`).
    pub fn is_update_validator_rejected(&self) -> bool {
        self.update_validator_rejection().is_some()
    }

    /// The details of an update the workflow's validator rejected.
    pub fn update_validator_rejection(&self) -> Option<UpdateValidatorRejected> {
        let Self::Http { status, body } = self else {
            return None;
        };
        let body: Value = serde_json::from_str(body).ok()?;
        if body.get("reason").and_then(Value::as_str) != Some(UPDATE_VALIDATOR_REJECTED) {
            return None;
        }
        let string = |name: &str| {
            body.get(name)
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
        };
        let validation_errors = body
            .get("validation_errors")
            .and_then(Value::as_object)
            .map(|fields| {
                fields
                    .iter()
                    .map(|(field, messages)| {
                        let messages = messages
                            .as_array()
                            .map(|messages| {
                                messages
                                    .iter()
                                    .filter_map(Value::as_str)
                                    .map(str::to_string)
                                    .collect()
                            })
                            .unwrap_or_default();
                        (field.clone(), messages)
                    })
                    .collect()
            })
            .unwrap_or_default();
        Some(UpdateValidatorRejected {
            status: status.as_u16(),
            workflow_id: string("workflow_id"),
            run_id: string("run_id"),
            update_name: string("update_name"),
            update_id: string("update_id"),
            message: string("message")
                .unwrap_or_else(|| "the update validator rejected the request".to_string()),
            validation_errors,
            body,
        })
    }

    /// Whether a validated update could not be validated, for example
    /// because no validator-capable worker answered before the Server's
    /// deadline or the worker could not run the validator
    /// (`outcome: "update_validation_failed"`). Nothing was accepted. The
    /// Server's `reason` and `retryable` fields are in the error body.
    pub fn is_update_validation_failed(&self) -> bool {
        let Self::Http { body, .. } = self else {
            return false;
        };
        serde_json::from_str::<Value>(body).is_ok_and(|body| {
            body.get("outcome").and_then(Value::as_str) == Some("update_validation_failed")
        })
    }
}

/// A validation task leased by a multiplexed workflow poll, parked until the
/// poller that holds the capacity reservation runs it.
#[derive(Debug, Default)]
pub(crate) struct ParkedTask(Mutex<Option<Value>>);

impl ParkedTask {
    pub(crate) fn take(&self) -> Option<Value> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner).take()
    }
}

pub(crate) type Slot = Arc<ParkedTask>;

impl Client {
    /// The multiplexed poll body for a validator worker's workflow poll.
    pub(crate) fn multiplexed_workflow_poll_body<B: Serialize + ?Sized>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<Option<Value>> {
        if self.update_validation_slot.is_none() || path != WORKFLOW_POLL_PATH {
            return Ok(None);
        }
        let mut body = serde_json::to_value(body)?;
        body["task_kinds"] = json!(TASK_KINDS);
        Ok(Some(body))
    }

    /// Move a leased update-validation task out of a workflow poll answer.
    pub(crate) fn park_update_validation_task(&self, answer: &mut Value) -> Result<()> {
        let Some(slot) = &self.update_validation_slot else {
            return Ok(());
        };
        match answer["task"].get("task_kind").and_then(Value::as_str) {
            None | Some("workflow") => Ok(()),
            Some("update_validation") => {
                let task = answer["task"].take();
                *slot.0.lock().unwrap_or_else(PoisonError::into_inner) = Some(task);
                Ok(())
            }
            Some(other) => Err(Error::Codec(format!(
                "multiplexed workflow poll returned unsupported task_kind {other:?}"
            ))),
        }
    }

    /// Approve one leased update-validation task so the Server accepts its
    /// update. `lease_owner` and `update_validation_attempt` fence the
    /// completion to the delivery that leased it.
    pub async fn approve_update_validation_task(
        &self,
        update_validation_task_id: &str,
        lease_owner: &str,
        update_validation_attempt: u64,
    ) -> Result<Value> {
        let path = format!(
            "/worker/update-validation-tasks/{}/approve",
            path_segment("update_validation_task_id", update_validation_task_id)?
        );
        self.request_json(
            reqwest::Method::POST,
            &path,
            RequestProtocol::DefaultWorker,
            Some(&json!({
                "lease_owner": lease_owner,
                "update_validation_attempt": update_validation_attempt,
            })),
        )
        .await
    }

    /// Reject one leased update-validation task. Use reason
    /// [`UPDATE_VALIDATOR_REJECTED`] for a validator's refusal and
    /// [`UPDATE_VALIDATION_WORKER_FAILED`] when the validator could not run.
    pub async fn reject_update_validation_task(
        &self,
        update_validation_task_id: &str,
        lease_owner: &str,
        update_validation_attempt: u64,
        reason: &str,
        rejection: &UpdateRejection,
    ) -> Result<Value> {
        let path = format!(
            "/worker/update-validation-tasks/{}/reject",
            path_segment("update_validation_task_id", update_validation_task_id)?
        );
        self.request_json(
            reqwest::Method::POST,
            &path,
            RequestProtocol::DefaultWorker,
            Some(&json!({
                "lease_owner": lease_owner,
                "update_validation_attempt": update_validation_attempt,
                "failure": rejection.wire(reason),
            })),
        )
        .await
    }
}

/// `_server_supports_update_validation_tasks` in the Python SDK: the Server
/// advertises synchronous validation with an atomic multiplexed workflow poll.
pub(crate) fn server_supports_update_validation_tasks(info: &Value) -> bool {
    let capabilities = &info["worker_protocol"]["server_capabilities"];
    if capabilities["update_validation_tasks"] != true {
        return false;
    }
    let semantics = &capabilities["synchronous_update_validation"];
    let poll = &semantics["task_poll"];
    semantics["supported"] == true
        && semantics["acceptance_boundary"] == "validator_approved"
        && semantics["worker_capability"] == crate::UPDATE_VALIDATION_TASKS_CAPABILITY
        && semantics["workflow_contract_field"] == "update_validators"
        && poll["strategy"] == "multiplexed"
        && poll["endpoint"] == WORKFLOW_POLL_PATH
        && poll["request_field"] == "task_kinds"
        && poll["response_discriminator"] == "task.task_kind"
        && poll["task_kinds"].as_array().is_some_and(|kinds| {
            TASK_KINDS
                .iter()
                .all(|kind| kinds.iter().any(|offered| offered == kind))
        })
}

fn fenced(error: &Error) -> Option<String> {
    let Error::Http { body, .. } = error else {
        return None;
    };
    let reason = serde_json::from_str::<Value>(body)
        .ok()?
        .get("reason")?
        .as_str()?
        .to_string();
    FENCED_REASONS.contains(&reason.as_str()).then_some(reason)
}

fn validator_name(update_name: &str) -> String {
    format!("{update_name}#validator")
}

impl Worker {
    /// Register a validator for a workflow update.
    ///
    /// The Server runs it before accepting the update. Returning an
    /// [`UpdateRejection`] rejects the update with reason
    /// `update_validator_rejected`: the update handler never runs and no
    /// acceptance is recorded. Returning `Ok(())` approves it, and the Server
    /// then accepts the update and the worker runs its handler as usual.
    ///
    /// The validator receives the same arguments as the update handler and a
    /// read-only [`QueryContext`] over the run's committed history, so it has
    /// no way to schedule durable work. It is not run again on replay. The
    /// update must also be registered with [`Worker::register_update`] (or
    /// the Avro variant) before [`Worker::register`]; registration refuses a
    /// Server that does not advertise synchronous update validation. A
    /// duplicate validator for the same update is reported as the update
    /// handler name followed by `#validator`.
    ///
    /// A validator cannot schedule durable work; this does not compile:
    ///
    /// ```compile_fail
    /// # fn configure(worker: &mut durable_workflow::Worker) {
    /// worker.register_update_validator("orders", "approve", |ctx, _args| {
    ///     let _timer = ctx.sleep(std::time::Duration::from_secs(1));
    ///     Ok(())
    /// });
    /// # }
    /// ```
    #[track_caller]
    pub fn register_update_validator<F>(
        &mut self,
        workflow_type: impl Into<String>,
        update_name: impl Into<String>,
        validator: F,
    ) where
        F: Fn(QueryContext, Value) -> std::result::Result<(), UpdateRejection>
            + Send
            + Sync
            + 'static,
    {
        let validator = Arc::new(validator);
        self.insert_update_validator::<F>(
            workflow_type.into(),
            update_name.into(),
            "register_update_validator",
            RegisteredQuery::Snapshot(Arc::new(move |ctx, args: AvroValue| {
                let validator = Arc::clone(&validator);
                Box::pin(async move {
                    let args = args.into_json()?;
                    approve(validator(ctx, args))
                })
            })),
        );
    }

    /// Register an update validator on the lossless fixed Avro Value surface.
    #[track_caller]
    pub fn register_update_validator_avro_value<F>(
        &mut self,
        workflow_type: impl Into<String>,
        update_name: impl Into<String>,
        validator: F,
    ) where
        F: Fn(QueryContext, AvroValue) -> std::result::Result<(), UpdateRejection>
            + Send
            + Sync
            + 'static,
    {
        let validator = Arc::new(validator);
        self.insert_update_validator::<F>(
            workflow_type.into(),
            update_name.into(),
            "register_update_validator_avro_value",
            RegisteredQuery::Snapshot(Arc::new(move |ctx, args| {
                let validator = Arc::clone(&validator);
                Box::pin(async move { approve(validator(ctx, args)) })
            })),
        );
    }

    /// Register an update validator that reads the workflow's replayed
    /// instance state.
    ///
    /// The workflow type must use [`Worker::register_replayed_workflow`] with
    /// the same state type `S`. The worker reconstructs the state from
    /// committed history, as for a replayed query, and passes the validator a
    /// detached snapshot, so state changes are discarded. If the validator
    /// adds a workflow command to the reconstructed run (for example through
    /// a [`crate::WorkflowContext`] kept in state), the update is rejected
    /// with failure type `UpdateValidatorCommandRefused` and nothing is
    /// scheduled. Otherwise this behaves like
    /// [`Worker::register_update_validator`].
    #[track_caller]
    pub fn register_replayed_update_validator<S, F>(
        &mut self,
        workflow_type: impl Into<String>,
        update_name: impl Into<String>,
        validator: F,
    ) where
        S: Clone + Send + Sync + 'static,
        F: Fn(QueryContext, Arc<S>, Value) -> std::result::Result<(), UpdateRejection>
            + Send
            + Sync
            + 'static,
    {
        let validator = Arc::new(validator);
        self.insert_update_validator::<F>(
            workflow_type.into(),
            update_name.into(),
            "register_replayed_update_validator",
            RegisteredQuery::Replayed {
                state_type: TypeId::of::<S>(),
                handler: Arc::new(move |ctx, state, args| {
                    let state = state.downcast::<S>().map_err(|_| {
                        "registered update validator state type does not match the replayed workflow state"
                            .to_string()
                    })?;
                    let validator = Arc::clone(&validator);
                    Ok(Box::pin(async move {
                        let args = args.into_json()?;
                        approve(validator(ctx, state, args))
                    }))
                }),
            },
        );
    }

    #[track_caller]
    fn insert_update_validator<F>(
        &mut self,
        workflow_type: String,
        update_name: String,
        method: &'static str,
        validator: RegisteredQuery,
    ) {
        if !self.admit_registration::<F>(
            RegistrationKind::Update,
            Some(&workflow_type),
            &validator_name(&update_name),
            method,
        ) {
            return;
        }
        self.update_validators
            .entry(workflow_type)
            .or_default()
            .insert(update_name, validator);
    }

    pub(crate) fn has_update_validators(&self) -> bool {
        self.update_validators
            .values()
            .any(|validators| !validators.is_empty())
    }

    /// A fresh parking slot for one workflow poll, or `None` without validators.
    pub(crate) fn update_validation_poll_slot(&self) -> Result<Option<Slot>> {
        if !self.has_update_validators() {
            return Ok(None);
        }
        if !self.update_validation_confirmed.load(Ordering::SeqCst) {
            return Err(Error::WorkerLoop(
                "update_validation_registration_unconfirmed: register this validator worker before polling workflow tasks".into(),
            ));
        }
        Ok(Some(Arc::default()))
    }

    /// Refuse registration unless every validator has an update handler and
    /// the Server advertises synchronous validation with multiplexed polling.
    pub(crate) async fn require_update_validation_support(&self) -> Result<()> {
        for (workflow_type, validators) in &self.update_validators {
            for update_name in validators.keys() {
                let handled = self.workflows.contains_key(workflow_type)
                    && self
                        .updates
                        .get(workflow_type)
                        .is_some_and(|updates| updates.contains_key(update_name));
                if !handled {
                    return Err(Error::UpdateValidationUnavailable(format!(
                        "update validator {workflow_type}.{update_name} has no registered workflow update handler"
                    )));
                }
            }
        }
        // Discovery uses the Worker credential, including worker-only Clients.
        let info: Value = self
            .client
            .retry_cluster_info(|| {
                self.client
                    .request_json_unresolved_with_timeout::<Value, Value>(
                        reqwest::Method::GET,
                        "/cluster/info",
                        RequestProtocol::DefaultWorker,
                        None,
                        Duration::from_secs(5),
                    )
            })
            .await?;
        if !server_supports_update_validation_tasks(&info) {
            return Err(Error::UpdateValidationUnavailable(
                "this worker declares update validators, but the Server does not advertise synchronous pre-accept update validation with atomic multiplexed workflow/update-validation polling; refusing registration so validated updates cannot be accepted without validator approval".into(),
            ));
        }
        Ok(())
    }

    /// Run one leased update-validation task and approve or reject it.
    ///
    /// Completion refusals that fence a stale delivery and other completion
    /// failures are logged; the Server's lease expiry owns redelivery.
    pub(crate) async fn execute_update_validation_task(&self, task: Value) -> ManagedPollOutcome {
        let task_id = task["update_validation_task_id"]
            .as_str()
            .filter(|id| !id.is_empty())
            .map(str::to_string);
        let attempt = task["update_validation_attempt"]
            .as_u64()
            .filter(|attempt| *attempt >= 1);
        let (Some(task_id), Some(attempt)) = (task_id, attempt) else {
            tracing::warn!(
                target: "durable_workflow::worker",
                "received a malformed update validation task"
            );
            return ManagedPollOutcome::Handled;
        };
        let decision = self.decide_update_validation(task).await;
        let lease_owner = self.worker_id.clone();
        let completed = self
            .retry_worker_operation(|| async {
                match &decision {
                    Ok(()) => {
                        self.client
                            .approve_update_validation_task(&task_id, &lease_owner, attempt)
                            .await
                    }
                    Err((reason, rejection)) => {
                        self.client
                            .reject_update_validation_task(
                                &task_id,
                                &lease_owner,
                                attempt,
                                reason,
                                rejection,
                            )
                            .await
                    }
                }
            })
            .await;
        match completed {
            Ok(_) => {}
            Err(error) => match fenced(&error) {
                Some(reason) => tracing::info!(
                    target: "durable_workflow::worker",
                    update_validation_task_id = task_id.as_str(),
                    reason = reason.as_str(),
                    "update validation completion was fenced"
                ),
                None => tracing::warn!(
                    target: "durable_workflow::worker",
                    update_validation_task_id = task_id.as_str(),
                    error = %error,
                    "failed to complete update validation task"
                ),
            },
        }
        ManagedPollOutcome::Handled
    }

    /// Approve, or the failure reason and rejection to report.
    pub(crate) async fn decide_update_validation(
        &self,
        task: Value,
    ) -> std::result::Result<(), (&'static str, UpdateRejection)> {
        let failed = |message: String| {
            (
                UPDATE_VALIDATION_WORKER_FAILED,
                UpdateRejection::new(message),
            )
        };
        let workflow_type = task["workflow_type"]
            .as_str()
            .filter(|value| !value.is_empty());
        let update_name = task["update_name"]
            .as_str()
            .filter(|value| !value.is_empty());
        let (Some(workflow_type), Some(update_name)) = (workflow_type, update_name) else {
            return Err(failed(
                "update validation task is missing workflow type or update name".into(),
            ));
        };
        let (workflow_type, update_name) = (workflow_type.to_string(), update_name.to_string());
        let mut envelope = json!({"task": task});
        self.client
            .resolve_runtime_payloads(
                &mut envelope,
                WORKFLOW_POLL_PATH,
                RequestProtocol::Worker(self.client.worker_protocol_version.as_str()),
            )
            .await
            .map_err(|error| failed(format!("cannot resolve validation payloads: {error}")))?;
        let task = envelope["task"].take();
        let query: QueryTask = serde_json::from_value(json!({
            "query_task_id": task["update_validation_task_id"],
            "query_task_attempt": task["update_validation_attempt"],
            "workflow_id": task["workflow_id"],
            "run_id": task["run_id"],
            "workflow_type": workflow_type,
            "query_name": update_name,
            "payload_codec": task["payload_codec"],
            "workflow_arguments": task["workflow_arguments"],
            "query_arguments": task["update_arguments"],
            "history_events": task.get("history_events").cloned().unwrap_or_else(|| json!([])),
            "history_export": task["history_export"],
            "run_status": task["run_status"],
        }))
        .map_err(|error| failed(format!("malformed update validation task: {error}")))?;
        crate::validate_query_task_payloads(&query)
            .map_err(|error| failed(format!("cannot decode validation payloads: {error}")))?;
        if !self.workflows.contains_key(&workflow_type) {
            return Err(failed(format!(
                "no workflow registered for type {workflow_type:?}"
            )));
        }
        if !self
            .updates
            .get(&workflow_type)
            .is_some_and(|updates| updates.contains_key(&update_name))
        {
            return Err(failed(format!("unknown update {update_name:?}")));
        }
        let Some(validator) = self
            .update_validators
            .get(&workflow_type)
            .and_then(|validators| validators.get(&update_name))
        else {
            return Err(failed(format!(
                "update validator {update_name:?} is not declared"
            )));
        };
        match self
            .run_registered_query(query, validator, None, true)
            .await
        {
            Err(failure) => Err((
                UPDATE_VALIDATION_WORKER_FAILED,
                UpdateRejection::new(failure.message).with_type(failure.failure_type),
            )),
            Ok(Ok(_)) => Ok(()),
            Ok(Err(error)) => Err((UPDATE_VALIDATOR_REJECTED, UpdateRejection::from(error))),
        }
    }
}

fn approve(outcome: std::result::Result<(), UpdateRejection>) -> Result<AvroValue> {
    outcome
        .map(|()| AvroValue::Null)
        .map_err(Error::UpdateValidatorRejected)
}
