//! Stable client/worker metrics, with application-owned recording and export.
//!
//! Recorders must be fast and handle exporter errors locally. Panics are isolated
//! from SDK operations. Durations use a monotonic clock and seconds; observations
//! describe this process, not durable execution counts or billing units.
use crate::{Client, Error, RequestProtocol, Result, WorkerPollOutcome};
use serde::{de::DeserializeOwned, Serialize};
use std::{
    collections::BTreeMap,
    fmt,
    panic::{catch_unwind, AssertUnwindSafe},
    sync::{
        atomic::{AtomicU16, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

#[cfg(feature = "metrics-prometheus")]
mod prometheus;
#[cfg(feature = "metrics-prometheus")]
pub use prometheus::PrometheusMetrics;

/// Exact core labels. Exporter resource labels are separate application policy.
pub type MetricTags = BTreeMap<String, String>;
pub const CLIENT_REQUESTS: &str = "durable_workflow_client_requests";
pub const CLIENT_REQUEST_DURATION_SECONDS: &str =
    "durable_workflow_client_request_duration_seconds";
pub const WORKER_POLLS: &str = "durable_workflow_worker_polls";
pub const WORKER_POLL_DURATION_SECONDS: &str = "durable_workflow_worker_poll_duration_seconds";
pub const WORKER_TASKS: &str = "durable_workflow_worker_tasks";
pub const WORKER_TASK_DURATION_SECONDS: &str = "durable_workflow_worker_task_duration_seconds";

/// Pluggable, thread-safe recorder. Counter increments are counts; records are seconds.
pub trait MetricsRecorder: Send + Sync + 'static {
    fn increment(&self, name: &str, value: u64, tags: &MetricTags);
    fn record(&self, name: &str, value: f64, tags: &MetricTags);
}

/// Explicit no-op recorder; omission has the same behavior.
#[derive(Debug, Default)]
pub struct NoopMetrics;
impl MetricsRecorder for NoopMetrics {
    fn increment(&self, _: &str, _: u64, _: &MetricTags) {}
    fn record(&self, _: &str, _: f64, _: &MetricTags) {}
}

#[derive(Clone, Default)]
pub(super) struct Metrics(Option<Arc<dyn MetricsRecorder>>);
impl fmt::Debug for Metrics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Metrics")
            .field("enabled", &self.0.is_some())
            .finish()
    }
}
impl Metrics {
    pub(super) fn new(recorder: Arc<dyn MetricsRecorder>) -> Self {
        Self(Some(recorder))
    }

    fn pair(&self, counter: &str, histogram: &str, tags: &MetricTags, elapsed: f64) {
        if let Some(recorder) = &self.0 {
            let _ = catch_unwind(AssertUnwindSafe(|| recorder.increment(counter, 1, tags)));
            let _ = catch_unwind(AssertUnwindSafe(|| {
                recorder.record(histogram, elapsed, tags)
            }));
        }
    }

    pub(super) fn worker(&self, task: bool, kind: &str, queue: &str) -> WorkerObservation {
        WorkerObservation {
            metrics: self.clone(),
            start: Instant::now(),
            task,
            tags: if self.0.is_some() {
                [
                    ("task_kind".into(), kind.into()),
                    ("task_queue".into(), queue.into()),
                    ("outcome".into(), "error".into()),
                ]
                .into()
            } else {
                MetricTags::new()
            },
        }
    }
}

pub(super) struct WorkerObservation {
    metrics: Metrics,
    start: Instant,
    task: bool,
    tags: MetricTags,
}
impl WorkerObservation {
    pub(super) fn outcome(&mut self, value: &str) {
        if self.metrics.0.is_some() {
            self.tags.insert("outcome".into(), value.into());
        }
    }
    pub(super) fn poll<T>(
        &mut self,
        result: &Result<Option<T>>,
        classify: impl FnOnce(&T) -> WorkerPollOutcome,
    ) {
        let outcome = match result {
            Ok(Some(value)) => match classify(value) {
                WorkerPollOutcome::Task => "task",
                WorkerPollOutcome::Idle {
                    poll_status,
                    reason,
                }
                | WorkerPollOutcome::Stop {
                    poll_status,
                    reason,
                } if poll_status.as_deref() == Some("backpressure")
                    || reason.as_deref().is_some_and(|v| {
                        matches!(
                            v,
                            "backpressure"
                                | "worker_capacity_exhausted"
                                | "storage_capacity_exhausted"
                        )
                    }) =>
                {
                    "backpressure"
                }
                _ => "empty",
            },
            Ok(None) => "empty",
            Err(error) if crate::worker_poll_capacity_retry_after(error).is_some() => {
                "backpressure"
            }
            Err(_) => "error",
        };
        self.outcome(outcome);
    }
}
impl Drop for WorkerObservation {
    fn drop(&mut self) {
        let (counter, histogram) = if self.task {
            (WORKER_TASKS, WORKER_TASK_DURATION_SECONDS)
        } else {
            (WORKER_POLLS, WORKER_POLL_DURATION_SECONDS)
        };
        self.metrics.pair(
            counter,
            histogram,
            &self.tags,
            self.start.elapsed().as_secs_f64(),
        );
    }
}

pub(super) struct ClientObservation {
    metrics: Metrics,
    start: Instant,
    status: AtomicU16,
    tags: MetricTags,
}
impl ClientObservation {
    fn new(
        client: &Client,
        method: &reqwest::Method,
        path: &str,
        protocol: RequestProtocol,
    ) -> Self {
        let plane = match protocol {
            RequestProtocol::ControlPlane => "control",
            _ => "worker",
        };
        Self {
            metrics: client.metrics.clone(),
            start: Instant::now(),
            status: AtomicU16::new(0),
            tags: if client.metrics.0.is_some() {
                [
                    ("method".into(), method.as_str().into()),
                    ("route".into(), route(path).into()),
                    ("plane".into(), plane.into()),
                    ("status_code".into(), "none".into()),
                    ("outcome".into(), "pending".into()),
                ]
                .into()
            } else {
                MetricTags::new()
            },
        }
    }
    pub(super) fn status(&self, status: reqwest::StatusCode) {
        self.status.store(status.as_u16(), Ordering::Relaxed);
    }
    fn finish<T>(&mut self, result: &Result<T>) {
        if self.metrics.0.is_none() {
            return;
        }
        let outcome = match result {
            Ok(_) if self.status.load(Ordering::Relaxed) >= 400 => "http_error",
            Ok(_) => "ok",
            Err(Error::Http { .. }) => "http_error",
            Err(Error::Protocol(_)) => "bridge_rejected",
            Err(Error::Transport(_)) => "TransportError",
            Err(Error::Timeout) => "TimeoutError",
            Err(_) => "SdkError",
        };
        self.tags.insert("outcome".into(), outcome.into());
    }
}
impl Drop for ClientObservation {
    fn drop(&mut self) {
        if self.metrics.0.is_none() {
            return;
        }
        let status = self.status.load(Ordering::Relaxed);
        if status != 0 {
            self.tags.insert("status_code".into(), status.to_string());
        }
        self.metrics.pair(
            CLIENT_REQUESTS,
            CLIENT_REQUEST_DURATION_SECONDS,
            &self.tags,
            self.start.elapsed().as_secs_f64(),
        );
    }
}

impl Client {
    /// Replace this client's recorder. Clones and new workers inherit it.
    pub fn with_metrics_recorder(mut self, recorder: Arc<dyn MetricsRecorder>) -> Self {
        self.metrics = Metrics::new(recorder);
        self
    }

    pub(super) async fn metrics_request<T: DeserializeOwned, B: Serialize + ?Sized>(
        &self,
        method: reqwest::Method,
        path: &str,
        protocol: RequestProtocol,
        body: Option<&B>,
        timeout: Duration,
        resolve: bool,
    ) -> Result<T> {
        let mut observation = ClientObservation::new(self, &method, path, protocol);
        let result = async {
            if !resolve {
                return Box::pin(self.send_json_with_timeout(
                    method,
                    path,
                    protocol,
                    body,
                    timeout,
                    &observation,
                ))
                .await;
            }
            let mut value: serde_json::Value = Box::pin(self.send_json_with_timeout(
                method,
                path,
                protocol,
                body,
                timeout,
                &observation,
            ))
            .await?;
            self.resolve_runtime_payloads(
                &mut value,
                path,
                self.worker_request_protocol(protocol)?,
            )
            .await?;
            Ok(serde_json::from_value(value)?)
        }
        .await;
        observation.finish(&result);
        result
    }

    pub(super) async fn metrics_poll_request<T: DeserializeOwned, B: Serialize + ?Sized>(
        &self,
        path: &str,
        protocol: RequestProtocol,
        body: &B,
        timeout: Duration,
        max_retries: usize,
        resolve: bool,
    ) -> Result<T> {
        let mut observation = ClientObservation::new(self, &reqwest::Method::POST, path, protocol);
        let result = async {
            let mut retries = 0;
            loop {
                let response = async {
                    let mut value: serde_json::Value = Box::pin(self.send_json_with_timeout(
                        reqwest::Method::POST,
                        path,
                        protocol,
                        Some(body),
                        timeout,
                        &observation,
                    ))
                    .await?;
                    if resolve {
                        self.resolve_runtime_payloads(
                            &mut value,
                            path,
                            self.worker_request_protocol(protocol)?,
                        )
                        .await?;
                    }
                    Ok(value)
                }
                .await;
                match response {
                    Err(Error::Transport(_)) if retries < max_retries => retries += 1,
                    response => {
                        let value = crate::worker_poll_response(response)?;
                        if value.get("task").is_some_and(|task| !task.is_null()) {
                            if let Some(slot) = &self.worker_task_slot {
                                slot.claimed();
                            }
                        }
                        return Ok(serde_json::from_value(value)?);
                    }
                }
            }
        }
        .await;
        observation.finish(&result);
        result
    }
}

/// Reviewed templates only; unknown operations use a single bounded value.
fn route(path: &str) -> &'static str {
    let clean = path.split(['?', '#']).next().unwrap_or_default();
    ROUTES
        .iter()
        .copied()
        .find(|template| {
            let mut actual = clean.trim_matches('/').split('/');
            for expected in template.trim_matches('/').split('/') {
                let Some(segment) = actual.next() else {
                    return false;
                };
                if expected != segment && !(expected.starts_with('{') && !segment.is_empty()) {
                    return false;
                }
            }
            actual.next().is_none()
        })
        .unwrap_or("unknown")
}

include!("metrics/routes.rs");

#[cfg(test)]
mod tests;

// A failed workflow command is a failed dispatch outcome, even when its failure receipt succeeds.
pub(super) fn workflow_task_outcome(
    decision: Option<&crate::WorkflowTaskDecision>,
) -> &'static str {
    match decision {
        Some(decision)
            if !decision.commands.iter().any(|command| {
                command.get("type").and_then(serde_json::Value::as_str) == Some("fail_workflow")
            }) =>
        {
            "completed"
        }
        _ => "failed",
    }
}
