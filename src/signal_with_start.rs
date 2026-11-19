//! Atomic signal-with-start intake (durable-workflow/.github#222, server#1133).
//!
//! One request starts the workflow, or reuses its open run, and records one
//! signal on that run in a single Server transaction. Unlike
//! [`Client::start_workflow`] followed by [`Client::signal_workflow`], the run
//! cannot close in between. Every refusal is atomic: nothing is started,
//! signalled or reserved.

use std::sync::atomic::Ordering;

use serde::Serialize;
use serde_json::{json, Value};

use crate::{
    encode_typed_envelope, generated_request_id, normalize_avro_arguments, path_segment,
    validate_request_id, validate_user_signal_name, with_request_id, AvroValue, Client, Error,
    RequestProtocol, Result, WorkflowStartOptions, DEFAULT_CODEC,
};

/// The cluster capability that advertises
/// `POST /workflows/{workflow_id}/signal-with-start/{signal_name}`.
pub(crate) const SIGNAL_WITH_START_CAPABILITY: &str = "signal_with_start";

/// What an open run means for the intake (the start's `duplicate_policy`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum OpenRunPolicy {
    /// Signal the open run (`use-existing`, the default). Its input,
    /// deadlines and history are unchanged.
    #[default]
    UseExisting,
    /// Refuse an open run with `instance_already_started` (`fail`).
    Fail,
}

impl OpenRunPolicy {
    /// The `duplicate_policy` value sent to the Server.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::UseExisting => "use-existing",
            Self::Fail => "fail",
        }
    }
}

/// What a closed current run means for the intake.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum ClosedRunPolicy {
    /// Refuse with `run_not_active` (the default). A closed run is never
    /// reopened.
    #[default]
    Reject,
    /// Start a fresh run after the closed one with this request's input and
    /// timeouts. The closed run keeps its outcome and history.
    StartNewRun,
}

impl ClosedRunPolicy {
    /// The `closed_run_policy` value sent to the Server.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Reject => "reject",
            Self::StartNewRun => "start_new_run",
        }
    }
}

/// Options for [`Client::signal_with_start`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct SignalWithStartOptions {
    /// Deadlines for a run the intake starts. A reused open run keeps its own.
    pub start: WorkflowStartOptions,
    pub open_run_policy: OpenRunPolicy,
    pub closed_run_policy: ClosedRunPolicy,
    /// The signal's caller command key (1 to 255 Unicode characters). When
    /// `None`, one `sdk-rust-signal-with-start-<uuid>` key is generated per call.
    pub request_id: Option<String>,
    pub business_key: Option<String>,
}

impl SignalWithStartOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn start(mut self, start: WorkflowStartOptions) -> Self {
        self.start = start;
        self
    }

    pub fn open_run_policy(mut self, policy: OpenRunPolicy) -> Self {
        self.open_run_policy = policy;
        self
    }

    pub fn closed_run_policy(mut self, policy: ClosedRunPolicy) -> Self {
        self.closed_run_policy = policy;
        self
    }

    pub fn request_id(mut self, request_id: impl Into<String>) -> Self {
        self.request_id = Some(request_id.into());
        self
    }

    pub fn business_key(mut self, business_key: impl Into<String>) -> Self {
        self.business_key = Some(business_key.into());
        self
    }
}

impl Client {
    /// Start the workflow or reuse its open run, and signal it, in one Server
    /// transaction (native Server `capabilities.signal_with_start`).
    ///
    /// - No run: a new run starts with `input` and `options.start`; the
    ///   receipt's `start_outcome` is `started_new`.
    /// - An open run, including the newest run after continue-as-new: the
    ///   signal joins it and `start_outcome` is `returned_existing_active`.
    ///   [`OpenRunPolicy::Fail`] refuses it with `instance_already_started`.
    /// - A closed run: refused with `run_not_active` unless
    ///   [`ClosedRunPolicy::StartNewRun`], which starts a fresh run after it
    ///   (`closed_run_id` names the closed run).
    ///
    /// `workflow_type` must be the instance's type and `task_queue` the open
    /// run's queue. The receipt carries the signal's `command_id`,
    /// `command_sequence`, `run_id` and `outcome`, the start's
    /// `start_command_id`, `start_outcome` and `start_command_status`,
    /// `intake_group_id` and `request_id`. A retry with the same key, signal
    /// name, arguments and workflow type returns the original receipt with
    /// `retained: true`, even after the run closed or continued, and starts
    /// nothing.
    ///
    /// Before sending, the client checks discovery and returns
    /// [`Error::SignalWithStartUnavailable`] when the Server does not
    /// advertise the capability. Every Server refusal is an [`Error::Http`]
    /// whose body `reason` names the cause (for example `run_not_active`,
    /// `workflow_type_conflict`, `task_queue_conflict`, `unknown_signal`); see
    /// [`Error::signal_with_start_refusal`]. Nothing was written for any of
    /// them. An admitted signal is admitted once; effects it triggers still
    /// need their own idempotency.
    #[allow(clippy::too_many_arguments)]
    pub async fn signal_with_start<I: Serialize, S: Serialize>(
        &self,
        workflow_type: &str,
        task_queue: &str,
        workflow_id: &str,
        signal_name: &str,
        input: I,
        signal_input: S,
        options: SignalWithStartOptions,
    ) -> Result<Value> {
        options.start.validate()?;
        validate_user_signal_name(signal_name)?;
        let request_id = match &options.request_id {
            Some(request_id) => request_id.clone(),
            None => Self::generate_signal_with_start_request_id(),
        };
        validate_request_id(&request_id)?;
        let path = format!(
            "/workflows/{}/signal-with-start/{}",
            path_segment("workflow_id", workflow_id)?,
            path_segment("signal_name", signal_name)?,
        );
        let input = normalize_avro_arguments(AvroValue::from_serialize(&input)?);
        let signal_input = normalize_avro_arguments(AvroValue::from_serialize(&signal_input)?);
        let mut body = json!({
            "workflow_type": workflow_type,
            "task_queue": task_queue,
            "input": encode_typed_envelope(&input, DEFAULT_CODEC)?,
            "signal_input": encode_typed_envelope(&signal_input, DEFAULT_CODEC)?,
            "execution_timeout_seconds": options.start.execution_timeout_seconds,
            "run_timeout_seconds": options.start.run_timeout_seconds,
            "duplicate_policy": options.open_run_policy.as_str(),
            "closed_run_policy": options.closed_run_policy.as_str(),
            "request_id": request_id,
        });
        if let Some(business_key) = &options.business_key {
            body["business_key"] = json!(business_key);
        }
        self.require_signal_with_start().await?;
        let body = self.with_observability_context(body);
        let receipt = self
            .request_json(
                reqwest::Method::POST,
                &path,
                RequestProtocol::ControlPlane,
                Some(&body),
            )
            .await
            .map_err(|error| error.with_sent_request_id(&request_id))?;
        Ok(with_request_id(receipt, &request_id))
    }

    /// A fresh caller command key for one logical signal-with-start
    /// (`sdk-rust-signal-with-start-<uuid>`). Obtain it up front and pass it
    /// in [`SignalWithStartOptions::request_id`] so a transport failure does
    /// not lose the key needed for a safe retry.
    pub fn generate_signal_with_start_request_id() -> String {
        generated_request_id("signal-with-start")
    }

    /// Refuse a Server that does not advertise `signal_with_start`. Discovery
    /// runs until a Server advertises it; after that the client remembers it.
    async fn require_signal_with_start(&self) -> Result<()> {
        if self.signal_with_start_supported.load(Ordering::SeqCst) {
            return Ok(());
        }
        let discovery = self.cluster_info().await?;
        if discovery["capabilities"][SIGNAL_WITH_START_CAPABILITY] != Value::Bool(true) {
            return Err(Error::SignalWithStartUnavailable);
        }
        self.signal_with_start_supported
            .store(true, Ordering::SeqCst);
        Ok(())
    }
}

impl Error {
    /// The Server's refusal reason when a signal-with-start intake was
    /// refused, such as `run_not_active`, `instance_already_started`,
    /// `workflow_type_conflict`, `task_queue_conflict`,
    /// `workflow_id_reserved_in_namespace`, `unknown_signal`,
    /// `invalid_signal_arguments` or `request_id_conflict`. Nothing was
    /// started, signalled or reserved. `None` for any other error.
    pub fn signal_with_start_refusal(&self) -> Option<String> {
        let body = self.refusal_body()?;
        if body["control_plane"]["operation"] != "signal_with_start" {
            return None;
        }
        body.get("reason")
            .and_then(Value::as_str)
            .filter(|reason| !reason.is_empty())
            .map(str::to_string)
    }
}

#[cfg(test)]
mod tests;
