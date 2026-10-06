use super::*;

const MAX_ATTEMPTS: u64 = 100;
const MAX_HEARTBEATS: usize = 1000;
const LEASE_RENEWAL_INTERVAL: Duration = Duration::from_secs(1);

/// Retry and timeout settings for an activity executed by the workflow worker.
///
/// Local activities bypass the activity queue. They do not accept routing,
/// schedule-to-start timeouts, or remote activity cancellation policies.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LocalActivityOptions {
    pub retry_policy: Option<ActivityRetryPolicy>,
    pub start_to_close_timeout: Option<Duration>,
    pub schedule_to_close_timeout: Option<Duration>,
    pub heartbeat_timeout: Option<Duration>,
}

impl LocalActivityOptions {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn retry_policy(mut self, policy: ActivityRetryPolicy) -> Self {
        self.retry_policy = Some(policy);
        self
    }
    pub fn start_to_close_timeout(mut self, timeout: Duration) -> Self {
        self.start_to_close_timeout = Some(timeout);
        self
    }
    pub fn schedule_to_close_timeout(mut self, timeout: Duration) -> Self {
        self.schedule_to_close_timeout = Some(timeout);
        self
    }
    pub fn heartbeat_timeout(mut self, timeout: Duration) -> Self {
        self.heartbeat_timeout = Some(timeout);
        self
    }
    fn into_activity_options(self) -> ActivityOptions {
        ActivityOptions {
            retry_policy: self.retry_policy,
            start_to_close_timeout: self.start_to_close_timeout,
            schedule_to_close_timeout: self.schedule_to_close_timeout,
            heartbeat_timeout: self.heartbeat_timeout,
            ..ActivityOptions::default()
        }
    }
}

pub(super) fn validate(options: &ValidatedActivityOptions) -> Result<()> {
    let max_attempts = options
        .retry_policy
        .as_ref()
        .and_then(|policy| policy["max_attempts"].as_u64())
        .unwrap_or(1);
    if max_attempts > MAX_ATTEMPTS {
        return Err(Error::InvalidActivityOptions(ActivityOptionsError::new(
            ActivityOptionsErrorKind::InvalidMaxAttempts,
            Some("max_attempts"),
            "local activities support at most 100 attempts",
        )));
    }
    for seconds in [
        options.start_to_close_timeout,
        options.schedule_to_close_timeout,
        options.heartbeat_timeout,
    ]
    .into_iter()
    .flatten()
    .chain(options.retry_policy.iter().flat_map(|p| {
        p["backoff_seconds"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_u64)
    })) {
        if Instant::now()
            .checked_add(Duration::from_secs(seconds))
            .is_none()
        {
            return Err(Error::InvalidActivityOptions(ActivityOptionsError::new(
                ActivityOptionsErrorKind::TimeoutOverflow,
                Some("local_activity"),
                "local timing exceeds the monotonic clock range",
            )));
        }
    }
    Ok(())
}

impl WorkflowContext {
    /// Execute a registered activity in this workflow worker and record its outcome.
    ///
    /// Committed results are replayed without executing the callback. A worker
    /// lost before acknowledgment can execute the callback again, so side effects
    /// must be idempotent. Callbacks must yield to Tokio and avoid blocking work.
    pub fn local_activity<T: Serialize>(
        &self,
        activity_type: impl Into<String>,
        args: T,
    ) -> ActivityCall {
        self.local_activity_with_options(activity_type, LocalActivityOptions::new(), args)
    }

    pub fn local_activity_with_options<T: Serialize>(
        &self,
        activity_type: impl Into<String>,
        options: LocalActivityOptions,
        args: T,
    ) -> ActivityCall {
        let mut call =
            self.activity_with_options(activity_type, options.into_activity_options(), args);
        call.local = true;
        call
    }

    /// Lossless Avro result, including bytes and large integers.
    pub async fn local_activity_avro_value<T: Serialize>(
        &self,
        activity_type: impl Into<String>,
        args: T,
    ) -> Result<AvroValue> {
        self.local_activity_avro_value_with_options(
            activity_type,
            LocalActivityOptions::new(),
            args,
        )
        .await
    }

    pub async fn local_activity_avro_value_with_options<T: Serialize>(
        &self,
        activity_type: impl Into<String>,
        options: LocalActivityOptions,
        args: T,
    ) -> Result<AvroValue> {
        let mut call = self.local_activity_with_options(activity_type, options, args);
        std::future::poll_fn(|cx| Pin::new(&mut call).poll_avro_value(cx)).await
    }

    pub async fn local_activity_typed<I: Serialize, O: DeserializeOwned>(
        &self,
        activity_type: impl Into<String>,
        args: I,
    ) -> Result<O> {
        self.local_activity_typed_with_options(activity_type, LocalActivityOptions::new(), args)
            .await
    }

    pub async fn local_activity_typed_with_options<I: Serialize, O: DeserializeOwned>(
        &self,
        activity_type: impl Into<String>,
        options: LocalActivityOptions,
        args: I,
    ) -> Result<O> {
        let activity_type = activity_type.into();
        let result = self
            .local_activity_avro_value_with_options(activity_type.clone(), options, args)
            .await?;
        decode_handler_result(result, HandlerKind::Activity, &activity_type)
    }
}

#[derive(Debug)]
pub(super) struct Request {
    pub command_index: usize,
    pub options: ValidatedActivityOptions,
    pub arguments: AvroValue,
    pub result: Arc<Mutex<Option<ActivityOutcome>>>,
}

#[derive(Debug)]
pub(super) struct Heartbeats {
    active: bool,
    started: Instant,
    last: Instant,
    reports: Vec<Value>,
    capacity: usize,
    overflowed: bool,
}

impl Heartbeats {
    pub fn record<T: Serialize>(&mut self, details: T) -> Result<ActivityHeartbeatResponse> {
        if !self.active {
            return Ok(heartbeat_response(false));
        }
        if self.reports.len() >= self.capacity {
            self.overflowed = true;
            return Err(Error::WorkerLoop(
                "local_activity_heartbeat_limit_exceeded: at most 1000 reports per local command"
                    .into(),
            ));
        }
        let details = encode_typed_envelope(&AvroValue::from_serialize(&details)?, DEFAULT_CODEC)?;
        let now = Instant::now();
        self.reports.push(
            json!({"elapsed_ms": millis(now.duration_since(self.started)), "details": details}),
        );
        self.last = now;
        Ok(heartbeat_response(true))
    }
}

fn heartbeat_response(active: bool) -> ActivityHeartbeatResponse {
    ActivityHeartbeatResponse {
        cancel_requested: false,
        heartbeat_recorded: active,
        can_continue: Some(active),
        reason: (!active).then(|| "local_activity_attempt_closed".into()),
        run_closed_reason: None,
        run_closed_at: None,
        lease_expires_at: None,
        last_heartbeat_at: None,
    }
}

struct CloseAttempt(Arc<Mutex<Heartbeats>>);
impl Drop for CloseAttempt {
    fn drop(&mut self) {
        if let Ok(mut heartbeats) = self.0.lock() {
            heartbeats.active = false;
        }
    }
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

impl Worker {
    pub(super) async fn execute_workflow_with_local_activities(
        &self,
        task: WorkflowTask,
    ) -> Result<Option<WorkflowTaskDecision>> {
        let (task, context, mut future) = match self.prepare_workflow_task_execution(task, None)? {
            PreparedWorkflowTask::Decision(decision) => return Ok(Some(decision)),
            PreparedWorkflowTask::Execution {
                task,
                context,
                future,
            } => (task, context, future),
        };
        loop {
            let outcome = Self::poll_workflow_future(&context, &mut future)?;
            let requests = {
                let mut state = context
                    .state
                    .lock()
                    .map_err(|_| Error::WorkflowStatePoisoned)?;
                std::mem::take(&mut state.local_activity_requests)
            };
            if requests.is_empty() {
                return self
                    .finish_workflow_task_execution(&task, &context, outcome)
                    .map(Some);
            }
            if !self.client.local_activities_enabled {
                return Err(Error::WorkerLoop("local_activities_not_enabled: opt in with Worker::local_activities(true) before executing inline local work".into()));
            }
            // A custom future cannot publish a terminal decision while ignoring
            // an unresolved local call. Resolve only genuinely suspended work.
            if !outcome.is_pending() {
                return Err(Error::WorkflowYieldedWithoutCommand);
            }
            for request in requests {
                let Some((wire, result)) = self
                    .execute_local_activity(&task, &context, &request)
                    .await?
                else {
                    // Do not fail, complete, or continue the workflow after a
                    // refused or uncertain lease. Reclaim owns subsequent work.
                    return Ok(None);
                };
                let mut state = context
                    .state
                    .lock()
                    .map_err(|_| Error::WorkflowStatePoisoned)?;
                state.commands[request.command_index] = wire;
                *request
                    .result
                    .lock()
                    .map_err(|_| Error::WorkflowStatePoisoned)? = Some(result);
            }
        }
    }

    async fn local_claim_is_active(&self, task: &WorkflowTask) -> bool {
        tokio::select! {
            biased;
            _ = self.local_worker_stopped() => false,
            receipt = self.client.heartbeat_workflow_task_with_protocol(task, None, WORKER_PROTOCOL_VERSION) => {
                receipt.is_ok_and(|receipt| receipt.cancellation_request.is_none())
            }
        }
    }

    async fn local_worker_stopped(&self) {
        match &self.client.worker_storage_admission {
            Some(admission) => wait_for_worker_stop(&admission.stop).await,
            None => std::future::pending::<()>().await,
        }
    }

    async fn externalize_local_command(
        &self,
        task: &WorkflowTask,
        context: &WorkflowContext,
        index: usize,
        wire: &mut Value,
    ) -> Result<()> {
        let mut commands = context
            .state
            .lock()
            .map_err(|_| Error::WorkflowStatePoisoned)?
            .commands
            .clone();
        commands[index] = wire.clone();
        let mut body = json!({"lease_owner": task.lease_owner, "workflow_task_attempt": task.workflow_task_attempt,
            "commands": commands});
        let path = format!("/worker/workflow-tasks/{}/complete", task.task_id);
        self.client
            .externalize_runtime_payloads(
                &mut body,
                &path,
                RequestProtocol::Worker(WORKER_PROTOCOL_VERSION),
            )
            .await?;
        *wire = body["commands"][index].take();
        Ok(())
    }

    async fn execute_local_activity(
        &self,
        task: &WorkflowTask,
        context: &WorkflowContext,
        request: &Request,
    ) -> Result<Option<(Value, ActivityOutcome)>> {
        let mut wire = context
            .state
            .lock()
            .map_err(|_| Error::WorkflowStatePoisoned)?
            .commands[request.command_index]
            .clone();
        // Serialize and admit input before the callback can produce side effects.
        if self
            .externalize_local_command(task, context, request.command_index, &mut wire)
            .await
            .is_err()
        {
            return Ok(None);
        }
        let activity_type = wire["activity_type"]
            .as_str()
            .unwrap_or_default()
            .to_owned();
        let policy = request.options.retry_policy.as_ref();
        let max_attempts = policy
            .and_then(|policy| policy["max_attempts"].as_u64())
            .unwrap_or(1);
        let started = Instant::now();
        let total_deadline = request
            .options
            .schedule_to_close_timeout
            .map(|seconds| started + Duration::from_secs(seconds));
        let mut attempts = Vec::new();
        let mut heartbeat_count = 0;
        for number in 1..=max_attempts {
            if !self.local_claim_is_active(task).await {
                return Ok(None);
            }
            let attempt_started = Instant::now();
            let reports = Arc::new(Mutex::new(Heartbeats {
                active: true,
                started: attempt_started,
                last: attempt_started,
                reports: Vec::new(),
                capacity: MAX_HEARTBEATS - heartbeat_count,
                overflowed: false,
            }));
            let close = CloseAttempt(reports.clone());
            let attempt_id = format!(
                "{:x}",
                Sha256::digest(
                    format!(
                        "{}\0{}\0{}\0{}",
                        task.task_id, task.workflow_task_attempt, request.command_index, number
                    )
                    .as_bytes()
                )
            );
            let ctx = ActivityContext {
                client: self.client.clone(),
                task_id: task.task_id.clone(),
                activity_attempt_id: attempt_id.clone(),
                lease_owner: task.lease_owner.clone().unwrap_or_default(),
                activity_type: activity_type.clone(),
                attempt_number: number,
                task_queue: self.task_queue.clone(),
                worker_id: self.worker_id.clone(),
                claim_guard: None,
                local_heartbeats: Some(reports.clone()),
                worker_session: None,
            };
            let callback = self
                .activities
                .get(&activity_type)
                .filter(|_| !total_deadline.is_some_and(|deadline| Instant::now() >= deadline));
            let mut future: ActivityFuture = match callback {
                Some(callback) => callback(ctx, request.arguments.clone()),
                None if total_deadline.is_some_and(|deadline| Instant::now() >= deadline) => {
                    Box::pin(std::future::pending())
                }
                None => {
                    let unknown = activity_type.clone();
                    Box::pin(async move { Err(Error::ActivityNotRegistered(unknown)) })
                }
            };
            let attempt_deadline = request
                .options
                .start_to_close_timeout
                .map(|seconds| attempt_started + Duration::from_secs(seconds));
            let outcome = loop {
                let heartbeat_deadline = request
                    .options
                    .heartbeat_timeout
                    .map(|seconds| {
                        reports
                            .lock()
                            .map(|reports| reports.last + Duration::from_secs(seconds))
                    })
                    .transpose()
                    .map_err(|_| Error::WorkflowStatePoisoned)?;
                let next_timeout = [
                    (total_deadline, "schedule_to_close"),
                    (attempt_deadline, "start_to_close"),
                    (heartbeat_deadline, "heartbeat"),
                ]
                .into_iter()
                .filter_map(|(deadline, kind)| deadline.map(|d| (d, kind)))
                .min_by_key(|(deadline, _)| *deadline);
                let timeout_kind = next_timeout
                    .map(|(_, kind)| kind)
                    .unwrap_or("start_to_close");
                let timeout = async {
                    match next_timeout {
                        Some((deadline, _)) => tokio::time::sleep_until(deadline.into()).await,
                        None => std::future::pending::<()>().await,
                    }
                };
                tokio::pin!(timeout);
                tokio::select! {
                    biased;
                    _ = self.local_worker_stopped() => return Ok(None),
                    _ = &mut timeout => {
                        if timeout_kind == "heartbeat" && heartbeat_extended(&reports, request.options.heartbeat_timeout)? { continue; }
                        break Err((format!("local activity exceeded {timeout_kind} timeout"), "LocalActivityTimeout".to_owned(), false, Some(timeout_kind)));
                    },
                    result = &mut future => break result.map_err(local_error),
                    _ = tokio::time::sleep(LEASE_RENEWAL_INTERVAL) => {
                        tokio::select! {
                            biased;
                            _ = &mut timeout => {
                                if timeout_kind == "heartbeat" && heartbeat_extended(&reports, request.options.heartbeat_timeout)? { continue; }
                                break Err((format!("local activity exceeded {timeout_kind} timeout"), "LocalActivityTimeout".to_owned(), false, Some(timeout_kind)));
                            },
                            active = self.local_claim_is_active(task) => { if !active { return Ok(None); } }
                        }
                    }
                }
            };
            drop(future);
            drop(close);
            let (heartbeats, overflowed) = {
                let mut reports = reports.lock().map_err(|_| Error::WorkflowStatePoisoned)?;
                (std::mem::take(&mut reports.reports), reports.overflowed)
            };
            heartbeat_count += heartbeats.len();
            let outcome = if overflowed {
                Err((
                    "local activity exceeded 1000 heartbeat reports".into(),
                    "LocalActivityHeartbeatLimit".into(),
                    true,
                    None,
                ))
            } else {
                outcome
            };
            let mut report = json!({"attempt_number": number, "attempt_id": attempt_id,
                "duration_ms": millis(attempt_started.elapsed()), "heartbeats": heartbeats});
            let (result, failure) = match outcome {
                Ok(result) => match encode_typed_envelope(&result, &task.payload_codec) {
                    Ok(envelope) => {
                        wire["result"] = envelope;
                        (Some(result), None)
                    }
                    Err(error) => (
                        None,
                        Some((
                            error.to_string(),
                            "LocalActivityResultCodecError".into(),
                            true,
                            None,
                        )),
                    ),
                },
                Err(failure) => (None, Some(failure)),
            };
            if let Some((message, exception_type, mut non_retryable, timeout_kind)) = failure {
                non_retryable |= policy
                    .and_then(|policy| policy["non_retryable_error_types"].as_array())
                    .is_some_and(|types| {
                        types
                            .iter()
                            .any(|t| t.as_str() == Some(exception_type.as_str()))
                    });
                let status = if timeout_kind.is_some() {
                    "timed_out"
                } else {
                    "failed"
                };
                report["outcome"] = json!(status);
                report["message"] = json!(message);
                report["exception_type"] = json!(exception_type);
                report["non_retryable"] = json!(non_retryable);
                if let Some(kind) = timeout_kind {
                    report["timeout_kind"] = json!(kind);
                }
                let retry = number < max_attempts
                    && !non_retryable
                    && timeout_kind != Some("schedule_to_close");
                if retry {
                    let backoff = policy
                        .and_then(|p| p["backoff_seconds"].get((number - 1) as usize))
                        .and_then(Value::as_u64)
                        .unwrap_or(0);
                    report["retry_reason"] = json!(if timeout_kind.is_some() {
                        "timeout"
                    } else {
                        "failure"
                    });
                    report["backoff_seconds"] = json!(backoff);
                    attempts.push(report);
                    let wake = Instant::now() + Duration::from_secs(backoff);
                    while Instant::now() < wake {
                        // An elapsed total deadline is reported as the next
                        // terminal attempt, with no further callback invocation.
                        if total_deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                            break;
                        }
                        if !self.local_claim_is_active(task).await {
                            return Ok(None);
                        }
                        let deadline = total_deadline.map_or(wake, |d| d.min(wake));
                        tokio::time::sleep(
                            deadline
                                .saturating_duration_since(Instant::now())
                                .min(LEASE_RENEWAL_INTERVAL),
                        )
                        .await;
                    }
                    continue;
                }
                attempts.push(report);
                for field in [
                    "outcome",
                    "message",
                    "exception_type",
                    "non_retryable",
                    "timeout_kind",
                ] {
                    if let Some(value) = attempts.last().unwrap().get(field) {
                        wire[field] = value.clone();
                    }
                }
                wire["attempts"] = json!(attempts);
                // Match the terminal fields Server persists. Database-generated
                // execution/failure IDs are only available on committed replay.
                let mut payload = wire.clone();
                payload["attempt_number"] = json!(number);
                payload["failure_category"] = json!(if timeout_kind.is_some() {
                    "timeout"
                } else {
                    "application"
                });
                if timeout_kind.is_some() {
                    payload["exception_class"] = payload["exception_type"].take();
                    payload.as_object_mut().unwrap().remove("exception_type");
                    payload.as_object_mut().unwrap().remove("non_retryable");
                }
                let event = HistoryEvent {
                    event_type: if timeout_kind.is_some() {
                        "ActivityTimedOut"
                    } else {
                        "ActivityFailed"
                    }
                    .into(),
                    payload,
                    raw: HashMap::new(),
                };
                let result =
                    activity_outcome(&event, &task.payload_codec, Some(activity_type.clone()))?;
                if self
                    .externalize_local_command(task, context, request.command_index, &mut wire)
                    .await
                    .is_err()
                    || !self.local_claim_is_active(task).await
                {
                    return Ok(None);
                }
                return Ok(Some((wire, result)));
            }
            report["outcome"] = json!("completed");
            attempts.push(report);
            wire["outcome"] = json!("completed");
            wire["attempts"] = json!(attempts);
            if self
                .externalize_local_command(task, context, request.command_index, &mut wire)
                .await
                .is_err()
                || !self.local_claim_is_active(task).await
            {
                return Ok(None);
            }
            return Ok(Some((wire, Ok(result.unwrap()))));
        }
        unreachable!("validated local retry budget includes at least one attempt")
    }
}

fn local_error(error: Error) -> (String, String, bool, Option<&'static str>) {
    let (kind, non_retryable) = match &error {
        Error::ActivityFailed(failure) => (
            failure
                .exception_type
                .clone()
                .unwrap_or_else(|| "RustActivityError".into()),
            failure.non_retryable,
        ),
        Error::ActivityNotRegistered(_) => ("ActivityNotRegistered".into(), true),
        Error::Codec(_) | Error::HandlerType { .. } => ("ActivityCodecError".into(), true),
        _ => ("RustActivityError".into(), false),
    };
    if kind.len() > 255 {
        return (
            "local activity exception type exceeded 255 bytes".into(),
            "LocalActivityFailureMetadataError".into(),
            true,
            None,
        );
    }
    (error.to_string(), kind, non_retryable, None)
}

fn heartbeat_extended(reports: &Arc<Mutex<Heartbeats>>, timeout: Option<u64>) -> Result<bool> {
    let reports = reports.lock().map_err(|_| Error::WorkflowStatePoisoned)?;
    Ok(timeout.is_some_and(|seconds| reports.last + Duration::from_secs(seconds) > Instant::now()))
}
