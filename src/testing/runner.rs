use super::{TestingError, WorkflowEnvironment, WorkflowHistory};
use crate::{
    decode_task_avro_arguments, encode_typed_envelope, json, local_activity,
    normalize_avro_arguments, ActivityContext, ActivityError, AvroValue, Error, HistoryEvent,
    PreparedWorkflowTask, Result, Value, Worker, DEFAULT_CODEC,
};
use serde::Serialize;
use std::task::Poll;

struct Turn {
    commands: Vec<Value>,
    result: Option<Result<AvroValue>>,
}

struct Budget {
    remaining: usize,
    limit: usize,
}

impl Budget {
    fn spend(&mut self) -> Result<()> {
        if self.remaining == 0 {
            return Err(TestingError::StepLimit(self.limit).into());
        }
        self.remaining -= 1;
        Ok(())
    }
}

impl WorkflowEnvironment {
    /// Run one registered activity attempt, including its normal argument/result
    /// conversion. Heartbeats are captured locally. This method does not retry;
    /// workflow execution applies the workflow's activity retry policy.
    pub async fn run_activity<T: Serialize>(&self, name: &str, input: T) -> Result<Value> {
        self.run_activity_avro_value(name, AvroValue::from_serialize(&input)?)
            .await?
            .into_json()
    }

    pub async fn run_activity_avro_value(&self, name: &str, input: AvroValue) -> Result<AvroValue> {
        self.worker.validate_registration()?;
        activity_attempt(
            &self.worker,
            name,
            input,
            1,
            "offline-activity",
            &self.worker.task_queue,
            false,
        )
        .await
    }

    /// Run a new workflow without a Server, returning its result or original
    /// typed failure. Each durable turn uses the worker's cold-replay engine.
    /// Activity retry delays and durable timers are skipped. Registered activity
    /// handlers run normally and may be replaced by test callbacks.
    ///
    /// External services, signals, child workflows, sessions, cancellation and
    /// activity timeout simulation require a live integration test; unsupported
    /// commands fail explicitly. Parallel activity handlers run in command order.
    pub async fn run_workflow<T: Serialize>(&mut self, name: &str, input: T) -> Result<Value> {
        self.last_history = None;
        self.run_workflow_avro_value(name, AvroValue::from_serialize(&input)?)
            .await?
            .into_json()
    }

    pub async fn run_workflow_avro_value(
        &mut self,
        name: &str,
        input: AvroValue,
    ) -> Result<AvroValue> {
        self.last_history = None;
        self.worker.validate_registration()?;
        let input = encode_typed_envelope(&normalize_avro_arguments(input), DEFAULT_CODEC)?;
        let mut history = WorkflowHistory::from_history(name, (), json!([]))?
            .task_queue(self.worker.task_queue.clone());
        history.task.arguments = Some(input.clone());
        history.task.workflow_id = Some("offline-workflow".into());
        history.task.run_id = Some("offline-run".into());
        append(
            &mut history,
            "WorkflowStarted",
            json!({
                "workflow_type":name, "arguments":input, "task_queue":self.worker.task_queue,
            }),
        )?;
        let mut budget = Budget {
            remaining: self.max_steps,
            limit: self.max_steps,
        };
        let result = self.drive(&mut history, &mut budget).await;
        self.last_history = Some(history);
        result
    }

    async fn drive(&self, history: &mut WorkflowHistory, budget: &mut Budget) -> Result<AvroValue> {
        let mut sequence = 1;
        loop {
            budget.spend()?;
            let turn = self.workflow_turn(history)?;
            if turn.commands.is_empty() && turn.result.is_none() {
                return Err(TestingError::WorkflowBlocked.into());
            }
            for command in turn.commands {
                if command["type"] == "complete_workflow" {
                    append(
                        history,
                        "WorkflowCompleted",
                        json!({"output":command["result"]}),
                    )?;
                } else {
                    self.record(history, command, sequence, budget).await?;
                    sequence += 1;
                }
            }
            if let Some(result) = turn.result {
                if let Err(error) = &result {
                    append(
                        history,
                        "WorkflowFailed",
                        json!({"message":error.to_string()}),
                    )?;
                }
                return result;
            }
        }
    }

    fn workflow_turn(&self, history: &WorkflowHistory) -> Result<Turn> {
        let prepared = self
            .worker
            .prepare_workflow_task_execution(history.task.clone(), None)?;
        let PreparedWorkflowTask::Execution {
            task,
            context,
            mut future,
        } = prepared
        else {
            return Err(TestingError::UnsupportedCommand("workflow update".into()).into());
        };
        let outcome = Worker::poll_workflow_future(&context, &mut future)?;
        // The live supervisor resolves local calls under its lease. Offline
        // execution records the outcome and cold replays without that transport.
        context
            .state
            .lock()
            .map_err(|_| Error::WorkflowStatePoisoned)?
            .local_activity_requests
            .clear();
        match outcome {
            Poll::Ready(Err(error)) => {
                if crate::workflow_task_integrity_error(&error) {
                    return Err(error);
                }
                if matches!(error, Error::ContinueAsNew(_)) {
                    return Err(TestingError::UnsupportedCommand("continue_as_new".into()).into());
                }
                context.ensure_history_consumed()?;
                Ok(Turn {
                    commands: context.take_commands()?,
                    result: Some(Err(error)),
                })
            }
            other => {
                let result = match &other {
                    Poll::Ready(Ok(value)) => Some(Ok(value.clone())),
                    _ => None,
                };
                let decision = self
                    .worker
                    .finish_workflow_task_execution(&task, &context, other)?;
                Ok(Turn {
                    commands: decision.commands,
                    result,
                })
            }
        }
    }

    async fn record(
        &self,
        history: &mut WorkflowHistory,
        command: Value,
        sequence: u64,
        budget: &mut Budget,
    ) -> Result<()> {
        budget.spend()?;
        let kind = command["type"].as_str().unwrap_or_default();
        let mut payload = command.clone();
        payload
            .as_object_mut()
            .expect("worker command object")
            .remove("type");
        payload["sequence"] = json!(sequence);
        match kind {
            "schedule_activity" | "record_local_activity" => {
                self.record_activity(history, &command, payload, sequence, budget)
                    .await
            }
            "start_timer" => {
                payload["timer_id"] = json!(format!("offline-timer-{sequence}"));
                append(history, "TimerScheduled", payload.clone())?;
                append(history, "TimerFired", payload)
            }
            "record_side_effect" => append(history, "SideEffectRecorded", payload),
            "record_version_marker" => append(history, "VersionMarkerRecorded", payload),
            "upsert_memo" => record_memo(history, payload),
            "upsert_search_attributes" => append(history, "SearchAttributesUpserted", payload),
            _ => Err(TestingError::UnsupportedCommand(kind.into()).into()),
        }
    }
    async fn record_activity(
        &self,
        history: &mut WorkflowHistory,
        command: &Value,
        mut payload: Value,
        sequence: u64,
        budget: &mut Budget,
    ) -> Result<()> {
        validate_activity_command(command)?;
        let local = command["type"] == "record_local_activity";
        let fallback = if local {
            "RustActivityError"
        } else {
            "RustActivityFailure"
        };
        let name = command["activity_type"].as_str().unwrap_or_default();
        let identity = format!("offline-activity-{sequence}");
        payload["activity_execution_id"] = json!(identity);
        payload["activity"] = json!({
            "queue":command.get("queue").cloned().unwrap_or_else(||json!(self.worker.task_queue)),
            "retry_policy":{
                "snapshot_version":1,"max_attempts":command["retry_policy"]["max_attempts"].as_u64().unwrap_or(1),
                "backoff_seconds":command["retry_policy"].get("backoff_seconds").cloned().unwrap_or_else(||json!([])),
                "non_retryable_error_types":command["retry_policy"].get("non_retryable_error_types").cloned().unwrap_or_else(||json!([])),
                "start_to_close_timeout":null,"schedule_to_start_timeout":null,
                "schedule_to_close_timeout":null,"heartbeat_timeout":null,
            },
            "execution_mode":if local {Some("local")} else {None},
        });
        append(history, "ActivityScheduled", payload.clone())?;
        let args = decode_task_avro_arguments(command.get("arguments"), DEFAULT_CODEC)?;
        let policy = &command["retry_policy"];
        let attempts = policy["max_attempts"].as_u64().unwrap_or(1);
        for attempt in 1..=attempts {
            budget.spend()?;
            let result = activity_attempt(
                &self.worker,
                name,
                args.clone(),
                attempt,
                &identity,
                command["queue"].as_str().unwrap_or(&self.worker.task_queue),
                local,
            )
            .await;
            payload["attempt_number"] = json!(attempt);
            payload["activity_attempt_id"] = json!(format!("{identity}-attempt-{attempt}"));
            let Err(error) = result else {
                payload["result"] = encode_typed_envelope(&result?, DEFAULT_CODEC)?;
                return append(history, "ActivityCompleted", payload);
            };
            let mut failure = ActivityError::from_error(&error, fallback);
            failure.non_retryable |= excluded_failure(policy, &failure.exception_type);
            if attempt < attempts && !failure.non_retryable {
                append(
                    history,
                    "ActivityRetryScheduled",
                    json!({
                        "sequence":sequence,"attempt_number":attempt,"activity_type":name,
                        "activity_execution_id":identity,
                    }),
                )?;
                continue;
            }
            payload["message"] = json!(failure.message);
            payload["exception_type"] = json!(failure.exception_type);
            payload["non_retryable"] = json!(failure.non_retryable);
            payload["failure_category"] = json!("activity");
            return append(history, "ActivityFailed", payload);
        }
        Err(TestingError::InvalidHistory("activity retry policy has zero attempts".into()).into())
    }
}

fn record_memo(history: &mut WorkflowHistory, mut payload: Value) -> Result<()> {
    let previous = history
        .events()
        .iter()
        .rev()
        .find(|event| event.event_type == "MemoUpserted");
    let mut merged = match previous {
        Some(event) => crate::decode_memo_history_map(&event.payload["merged"], false)?,
        None => AvroValue::Map(Default::default()),
    };
    let AvroValue::Map(values) = &mut merged else {
        unreachable!("validated memo map");
    };
    let AvroValue::Map(entries) = crate::decode_memo_history_map(&payload["entries"], true)? else {
        unreachable!("validated memo entries");
    };
    for (key, value) in entries {
        if value == AvroValue::Null {
            values.remove(&key);
        } else {
            values.insert(key, value);
        }
    }
    payload["merged"] = encode_typed_envelope(&merged, DEFAULT_CODEC)?;
    append(history, "MemoUpserted", payload)
}

fn excluded_failure(policy: &Value, exception_type: &str) -> bool {
    policy["non_retryable_error_types"]
        .as_array()
        .is_some_and(|types| {
            types
                .iter()
                .any(|value| value.as_str() == Some(exception_type))
        })
}

fn validate_activity_command(command: &Value) -> Result<()> {
    if command.get("worker_session").is_some() || command.get("cancellation_policy").is_some() {
        return Err(
            TestingError::UnsupportedCommand("activity session/cancellation".into()).into(),
        );
    }
    if [
        "start_to_close_timeout",
        "schedule_to_start_timeout",
        "schedule_to_close_timeout",
        "heartbeat_timeout",
    ]
    .iter()
    .any(|field| command.get(*field).is_some())
    {
        return Err(TestingError::UnsupportedCommand("activity timeout simulation".into()).into());
    }
    if command["parallel_group_mode"] == "select"
        || command["parallel_group_path"]
            .as_array()
            .is_some_and(|path| {
                path.iter()
                    .any(|entry| entry["parallel_group_mode"] == "select")
            })
    {
        return Err(TestingError::UnsupportedCommand(
            "select execution (use recorded replay)".into(),
        )
        .into());
    }
    Ok(())
}

async fn activity_attempt(
    worker: &Worker,
    name: &str,
    args: AvroValue,
    attempt: u64,
    identity: &str,
    queue: &str,
    local: bool,
) -> Result<AvroValue> {
    let handler = worker
        .activities
        .get(name)
        .ok_or_else(|| Error::ActivityNotRegistered(name.into()))?;
    let (heartbeats, _close) = local_activity::Heartbeats::captured_attempt();
    let context = ActivityContext {
        client: worker.client.clone(),
        task_id: identity.into(),
        activity_attempt_id: format!("{identity}-attempt-{attempt}"),
        lease_owner: "offline".into(),
        activity_type: name.into(),
        attempt_number: attempt,
        task_queue: queue.into(),
        worker_id: worker.worker_id.clone(),
        claim_guard: None,
        local_heartbeats: Some(heartbeats),
        worker_session: None,
    };
    worker
        .activity_callback(handler, context, normalize_avro_arguments(args), local)
        .await
}

fn append(history: &mut WorkflowHistory, kind: &str, payload: Value) -> Result<()> {
    let event: HistoryEvent = serde_json::from_value(json!({"event_type":kind,"payload":payload}))?;
    history.task.history_events.push(event);
    Ok(())
}
