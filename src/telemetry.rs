//! Optional execution spans and application log correlation.
//!
//! Applications own providers, exporters, sampling and shutdown. Enabling this
//! feature installs none of those components and does not change Server wire
//! messages. Each received task starts a fresh trace until portable carriers are
//! qualified with Server. Attempt spans are children of their executing task.

use std::{
    borrow::Cow, collections::BTreeMap, future::Future, panic::AssertUnwindSafe, sync::Arc,
    time::SystemTime,
};

use opentelemetry::{
    global::BoxedTracer,
    logs::{AnyValue, LogRecord, Logger, LoggerProvider, Severity},
    trace::{FutureExt, Status, TraceContextExt, Tracer},
    Context, InstrumentationScope, Key, KeyValue, SpanId, TraceFlags, TraceId,
};

use crate::{ActivityContext, ActivityTask, QueryTask, Worker, WorkflowTask};

/// Bounded execution identifiers. Values contain no arguments, results, headers,
/// exception messages or baggage. Trace IDs reflect the current application
/// span, including an application-created HTTP/database child span.
pub fn current_log_context() -> BTreeMap<String, String> {
    let context = Context::current();
    let Some(fields) = context.get::<ExecutionFields>() else {
        return BTreeMap::new();
    };
    let mut values = fields.0.clone();
    let span = context.span();
    let identity = span.span_context();
    if identity.is_valid() {
        values.insert("trace_id".into(), identity.trace_id().to_string());
        values.insert("span_id".into(), identity.span_id().to_string());
    }
    values
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ExecutionFields(BTreeMap<String, String>);

impl ExecutionFields {
    fn insert(mut self, name: &'static str, value: Option<&str>) -> Self {
        if let Some(value) = value.filter(|value| {
            !value.is_empty() && value.len() <= 256 && !value.chars().any(char::is_control)
        }) {
            self.0.insert(name.into(), value.into());
        }
        self
    }

    fn attempt(self, name: &'static str, value: u64) -> Self {
        self.insert(name, Some(&value.to_string()))
    }

    fn attributes(&self) -> Vec<KeyValue> {
        self.0
            .iter()
            .map(|(key, value)| KeyValue::new(format!("durable.{key}"), value.clone()))
            .collect()
    }
}

/// Opt-in worker telemetry using an application-owned tracer.
///
/// This value is cloneable; it never shuts down or replaces a global provider.
/// No SDK or exporter is required by the production dependency graph.
#[derive(Clone, Debug)]
pub struct ExecutionTelemetry {
    tracer: Arc<BoxedTracer>,
}

impl ExecutionTelemetry {
    /// Snapshot the application's current global tracer provider. Configure it
    /// before constructing this value. An unconfigured provider emits no spans.
    pub fn new() -> Self {
        let scope = InstrumentationScope::builder("durable-workflow/sdk-rust")
            .with_version(env!("CARGO_PKG_VERSION"))
            .build();
        use opentelemetry::trace::TracerProvider;
        Self::from_tracer(opentelemetry::global::tracer_provider().tracer_with_scope(scope))
    }

    /// Use a tracer without changing any application globals.
    pub fn from_tracer<T>(tracer: T) -> Self
    where
        T: Tracer + Send + Sync + 'static,
        T::Span: Send + Sync + 'static,
    {
        Self {
            tracer: Arc::new(BoxedTracer::new(Box::new(tracer))),
        }
    }

    fn scope(&self, name: &'static str, fields: ExecutionFields, nested: bool) -> ExecutionScope {
        // Only an existing SDK execution may parent another SDK scope. Poller
        // ambient spans and baggage must not leak into unrelated durable work.
        let current = Context::current();
        let parent = if nested && current.get::<ExecutionFields>().is_some() {
            current
        } else {
            Context::new()
        };
        let context = std::panic::catch_unwind(AssertUnwindSafe(|| {
            let span = self.tracer.build_with_context(
                self.tracer
                    .span_builder(name)
                    .with_attributes(fields.attributes()),
                &parent,
            );
            parent.with_span(span)
        }))
        .unwrap_or_else(|_| Context::new())
        .with_value(fields);
        ExecutionScope {
            context,
            finished: false,
        }
    }

    pub(crate) async fn run<T, E, F>(
        &self,
        name: &'static str,
        fields: ExecutionFields,
        nested: bool,
        future: F,
    ) -> Result<T, E>
    where
        F: Future<Output = Result<T, E>>,
    {
        let mut scope = self.scope(name, fields, nested);
        // Attaches only during each poll, never across an await on a Tokio thread.
        let result = future.with_context(scope.context.clone()).await;
        scope.finish(result.is_err());
        result
    }

    pub(crate) fn run_sync<T, E>(
        &self,
        name: &'static str,
        fields: ExecutionFields,
        callback: impl FnOnce() -> Result<T, E>,
    ) -> Result<T, E> {
        let mut scope = self.scope(name, fields, false);
        let result = {
            let _guard = scope.context.clone().attach();
            callback()
        };
        scope.finish(result.is_err());
        result
    }
}

impl Default for ExecutionTelemetry {
    fn default() -> Self {
        Self::new()
    }
}

struct ExecutionScope {
    context: Context,
    finished: bool,
}

impl ExecutionScope {
    fn finish(&mut self, failed: bool) {
        self.finished = true;
        let _ = std::panic::catch_unwind(AssertUnwindSafe(|| {
            if failed {
                self.context
                    .span()
                    .set_attribute(KeyValue::new("error.type", "durable.execution.failed"));
                self.context
                    .span()
                    .set_status(Status::error("durable execution failed"));
            }
            self.context.span().end();
        }));
    }
}

impl Drop for ExecutionScope {
    fn drop(&mut self) {
        if !self.finished {
            self.finish(true);
        }
    }
}

/// Decorate the application's OpenTelemetry logger provider with execution IDs.
/// Use this provider in an OpenTelemetry `log` or `tracing` bridge. Its lifecycle
/// remains owned by the application; ordinary log bodies and request IDs remain
/// application-owned. No logging bridge is installed automatically.
#[derive(Clone, Debug)]
pub struct ExecutionLoggerProvider<P>(pub P);

impl<P: LoggerProvider> LoggerProvider for ExecutionLoggerProvider<P> {
    type Logger = ExecutionLogger<P::Logger>;

    fn logger_with_scope(&self, scope: InstrumentationScope) -> Self::Logger {
        ExecutionLogger(self.0.logger_with_scope(scope))
    }
}

/// Logger returned by [`ExecutionLoggerProvider`].
#[derive(Debug)]
pub struct ExecutionLogger<L>(L);

impl<L: Logger> Logger for ExecutionLogger<L> {
    type LogRecord = ExecutionLogRecord<L::LogRecord>;

    fn create_log_record(&self) -> Self::LogRecord {
        ExecutionLogRecord {
            inner: self.0.create_log_record(),
            attributes: Vec::new(),
        }
    }

    fn emit(&self, mut record: Self::LogRecord) {
        let _ = std::panic::catch_unwind(AssertUnwindSafe(|| {
            let fields = current_log_context();
            for (key, value) in record.attributes {
                if !fields.contains_key(key.as_str()) {
                    record.inner.add_attribute(key, value);
                }
            }
            record.inner.add_attributes(fields);
            self.0.emit(record.inner);
        }));
    }

    fn event_enabled(&self, level: Severity, target: &str, name: Option<&str>) -> bool {
        std::panic::catch_unwind(AssertUnwindSafe(|| {
            self.0.event_enabled(level, target, name)
        }))
        .unwrap_or(false)
    }
}

/// Application log record whose execution IDs are resolved when it is emitted.
/// Buffering attributes permits replacing stale durable IDs without duplicate
/// keys; application attributes such as `request_id` keep their original value.
#[derive(Clone, Debug)]
pub struct ExecutionLogRecord<R> {
    inner: R,
    attributes: Vec<(Key, AnyValue)>,
}

impl<R: LogRecord> LogRecord for ExecutionLogRecord<R> {
    fn set_event_name(&mut self, name: &'static str) {
        self.inner.set_event_name(name);
    }
    fn set_target<T: Into<Cow<'static, str>>>(&mut self, target: T) {
        self.inner.set_target(target);
    }
    fn set_timestamp(&mut self, timestamp: SystemTime) {
        self.inner.set_timestamp(timestamp);
    }
    fn set_observed_timestamp(&mut self, timestamp: SystemTime) {
        self.inner.set_observed_timestamp(timestamp);
    }
    fn set_severity_text(&mut self, text: &'static str) {
        self.inner.set_severity_text(text);
    }
    fn set_severity_number(&mut self, number: Severity) {
        self.inner.set_severity_number(number);
    }
    fn set_body(&mut self, body: AnyValue) {
        self.inner.set_body(body);
    }
    fn add_attributes<I, K, V>(&mut self, attributes: I)
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<Key>,
        V: Into<AnyValue>,
    {
        self.attributes.extend(
            attributes
                .into_iter()
                .map(|(key, value)| (key.into(), value.into())),
        );
    }
    fn add_attribute<K: Into<Key>, V: Into<AnyValue>>(&mut self, key: K, value: V) {
        self.attributes.push((key.into(), value.into()));
    }
    fn set_trace_context(&mut self, trace_id: TraceId, span_id: SpanId, flags: Option<TraceFlags>) {
        self.inner.set_trace_context(trace_id, span_id, flags);
    }
}

impl Worker {
    fn telemetry_fields(&self) -> ExecutionFields {
        ExecutionFields::default()
            .insert("namespace", Some(&self.client.namespace))
            .insert("worker_id", Some(&self.worker_id))
            .insert("task_queue", Some(&self.task_queue))
    }

    pub(crate) fn workflow_telemetry_fields(&self, task: &WorkflowTask) -> ExecutionFields {
        self.telemetry_fields()
            .insert("workflow_instance_id", task.workflow_id.as_deref())
            .insert("workflow_run_id", task.run_id.as_deref())
            .insert("task_id", Some(&task.task_id))
            .insert("workflow_type", Some(&task.workflow_type))
            .insert("command_id", task.workflow_command_id.as_deref())
            .attempt("workflow_task_attempt", task.workflow_task_attempt)
    }

    pub(crate) fn query_telemetry_fields(&self, task: &QueryTask) -> ExecutionFields {
        self.telemetry_fields()
            .insert("workflow_instance_id", task.workflow_id.as_deref())
            .insert("workflow_run_id", task.run_id.as_deref())
            .insert("task_id", Some(&task.query_task_id))
            .insert("workflow_type", Some(&task.workflow_type))
            .attempt("query_task_attempt", task.query_task_attempt)
    }

    pub(crate) fn activity_telemetry_fields(&self, task: &ActivityTask) -> ExecutionFields {
        self.telemetry_fields()
            .insert("task_id", Some(&task.task_id))
            .insert("activity_type", Some(&task.activity_type))
            .insert(
                "activity_attempt_id",
                task.activity_attempt_id
                    .as_deref()
                    .or(task.attempt_id.as_deref()),
            )
            .attempt("activity_attempt_number", task.attempt_number)
    }

    pub(crate) fn callback_telemetry_fields(
        &self,
        context: &ActivityContext,
        local: bool,
    ) -> ExecutionFields {
        let fields = if local {
            Context::current()
                .get::<ExecutionFields>()
                .cloned()
                .unwrap_or_else(|| self.telemetry_fields())
        } else {
            self.telemetry_fields()
        };
        fields
            .insert("task_id", Some(&context.task_id))
            .insert("activity_type", Some(&context.activity_type))
            .insert("activity_attempt_id", Some(&context.activity_attempt_id))
            .attempt("activity_attempt_number", context.attempt_number)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{json, AvroValue, Client, Error, Value};
    use opentelemetry::trace::{Span, SpanId, TracerProvider};
    use opentelemetry_sdk::{
        logs::{InMemoryLogExporter, SdkLoggerProvider},
        trace::{InMemorySpanExporter, SdkTracerProvider, SpanData, SpanExporter},
    };

    fn telemetry() -> (ExecutionTelemetry, SdkTracerProvider, InMemorySpanExporter) {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        (
            ExecutionTelemetry::from_tracer(provider.tracer("test")),
            provider,
            exporter,
        )
    }

    fn worker() -> Worker {
        Worker::new(
            Client::builder("http://127.0.0.1:9")
                .namespace("shipping")
                .build()
                .unwrap(),
            "shipping",
        )
        .worker_id("test-worker")
    }

    fn activity_task(attempt: u64) -> ActivityTask {
        serde_json::from_value(json!({
            "task_id": "task-1", "activity_type": "ship", "activity_attempt_id": format!("attempt-{attempt}"),
            "attempt_number": attempt, "payload_codec": "avro",
            "arguments": crate::encode_value_envelope(&json!([]), "avro").unwrap(),
        })).unwrap()
    }

    #[tokio::test]
    async fn actual_activity_attempts_correlate_application_child_logs_and_preserve_result() {
        let (telemetry, provider, exporter) = telemetry();
        let log_exporter = InMemoryLogExporter::default();
        let logs = SdkLoggerProvider::builder()
            .with_simple_exporter(log_exporter.clone())
            .build();
        let logger = Arc::new(ExecutionLoggerProvider(logs.clone()).logger("application"));
        let tracer = Arc::new(provider.tracer("http-client"));
        let mut worker = worker().telemetry(telemetry);
        worker.register_activity("ship", move |_, _| {
            let logger = logger.clone();
            let tracer = tracer.clone();
            async move {
                assert_eq!(current_log_context()["task_id"], "task-1");
                assert_eq!(current_log_context()["namespace"], "shipping");
                let child = tracer.start("HTTP POST shipping");
                let child_identity = child.span_context().clone();
                let child_context = Context::current_with_span(child);
                async {
                    tokio::task::yield_now().await;
                    assert_eq!(
                        current_log_context()["span_id"],
                        child_identity.span_id().to_string()
                    );
                    let mut record = logger.create_log_record();
                    record.set_body("application message".into());
                    record.add_attribute("request_id", "application-request");
                    record.add_attribute("activity_attempt_id", "stale-application-id");
                    logger.emit(record);
                }
                .with_context(child_context.clone())
                .await;
                child_context.span().end();
                Ok(json!("shipped"))
            }
        });
        for attempt in 1..=2 {
            assert_eq!(
                worker
                    .execute_activity_task(activity_task(attempt))
                    .await
                    .unwrap(),
                AvroValue::String("shipped".into())
            );
            assert!(current_log_context().is_empty());
        }
        let spans = exporter.get_finished_spans().unwrap();
        assert_eq!(spans.len(), 6);
        let roots: Vec<_> = spans
            .iter()
            .filter(|span| span.name == "durable.activity.task")
            .collect();
        assert_eq!(roots.len(), 2);
        assert!(roots
            .iter()
            .all(|span| span.parent_span_id == SpanId::INVALID));
        assert_ne!(
            roots[0].span_context.trace_id(),
            roots[1].span_context.trace_id()
        );
        for attempt in spans
            .iter()
            .filter(|span| span.name == "durable.activity.attempt")
        {
            assert!(roots
                .iter()
                .any(|root| root.span_context.span_id() == attempt.parent_span_id));
        }
        let emitted = log_exporter.get_emitted_logs().unwrap();
        assert_eq!(emitted.len(), 2);
        for (index, log) in emitted.iter().enumerate() {
            assert_eq!(
                log.record
                    .attributes_iter()
                    .filter(|(key, _)| key.as_str() == "activity_attempt_id")
                    .count(),
                1
            );
            let attributes: BTreeMap<_, _> = log
                .record
                .attributes_iter()
                .map(|(key, value)| (key.as_str(), value))
                .collect();
            assert_eq!(attributes["request_id"], &"application-request".into());
            assert_eq!(
                attributes["activity_attempt_id"],
                &format!("attempt-{}", index + 1).into()
            );
            let child = spans
                .iter()
                .find(|span| {
                    span.span_context.span_id() == log.record.trace_context().unwrap().span_id
                })
                .unwrap();
            assert_eq!(child.name, "HTTP POST shipping");
        }
    }

    #[tokio::test]
    async fn concurrent_tokio_tasks_keep_identifiers_isolated() {
        let (telemetry, _provider, exporter) = telemetry();
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let mut worker = worker().telemetry(telemetry);
        worker.register_activity("ship", move |context, _| {
            let barrier = barrier.clone();
            async move {
                let before = current_log_context();
                barrier.wait().await;
                tokio::task::yield_now().await;
                assert_eq!(current_log_context(), before);
                assert_eq!(before["activity_attempt_id"], context.activity_attempt_id);
                Ok(Value::Null)
            }
        });
        let worker = Arc::new(worker);
        let handles: Vec<_> = (1..=2)
            .map(|attempt| {
                let worker = worker.clone();
                tokio::spawn(
                    async move { worker.execute_activity_task(activity_task(attempt)).await },
                )
            })
            .collect();
        for handle in handles {
            handle.await.unwrap().unwrap();
        }
        assert!(current_log_context().is_empty());
        assert_eq!(exporter.get_finished_spans().unwrap().len(), 4);
    }

    #[tokio::test]
    async fn workflow_yield_replay_and_disabled_controls_preserve_commands() {
        let (telemetry, _provider, exporter) = telemetry();
        let mut enabled = worker().telemetry(telemetry);
        enabled.register_workflow("ship-order", |context, _| async move {
            assert_eq!(current_log_context()["workflow_run_id"], "run-order");
            context.activity("ship", json!([])).await
        });
        let mut disabled = worker();
        disabled.register_workflow("ship-order", |context, _| async move {
            assert!(current_log_context().is_empty());
            context.activity("ship", json!([])).await
        });
        let task: WorkflowTask = serde_json::from_value(json!({
            "task_id": "workflow-task-1", "workflow_id": "order", "run_id": "run-order",
            "workflow_type": "ship-order", "payload_codec": "avro",
            "arguments": crate::encode_value_envelope(&json!([]), "avro").unwrap(),
        }))
        .unwrap();
        let expected = disabled.execute_workflow_task(task.clone()).unwrap();
        assert_eq!(
            enabled.execute_workflow_task(task.clone()).unwrap(),
            expected
        );
        let decision = enabled
            .execute_workflow_with_local_activities(task)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(decision.commands, expected);
        let spans = exporter.get_finished_spans().unwrap();
        assert_eq!(spans.len(), 2);
        assert!(spans
            .iter()
            .all(|span| span.name == "durable.workflow.task"));
        assert!(current_log_context().is_empty());
    }

    #[tokio::test]
    async fn genuine_failure_is_preserved_without_recording_payload_or_exception_text() {
        let (telemetry, _provider, exporter) = telemetry();
        let mut worker = worker().telemetry(telemetry);
        worker.register_activity("ship", |_, _| async {
            Err(Error::WorkerLoop("private-token-123".into()))
        });
        assert!(
            matches!(worker.execute_activity_task(activity_task(1)).await, Err(Error::WorkerLoop(message)) if message == "private-token-123")
        );
        let spans = exporter.get_finished_spans().unwrap();
        assert_eq!(spans.len(), 2);
        for span in spans {
            assert_eq!(span.status, Status::error("durable execution failed"));
            assert!(span.events.events.is_empty());
            assert!(!format!("{span:?}").contains("private-token-123"));
        }
        assert!(current_log_context().is_empty());
    }

    #[tokio::test]
    async fn dropped_future_and_panicking_callback_end_scopes_and_restore_caller() {
        let (telemetry, provider, exporter) = telemetry();
        let caller = provider.tracer("application").start("caller");
        let caller_id = caller.span_context().span_id();
        let _caller = Context::current_with_span(caller).attach();
        let mut pending = Box::pin(telemetry.run::<(), (), _>(
            "pending",
            ExecutionFields::default(),
            false,
            std::future::pending(),
        ));
        assert!(futures_util::poll!(pending.as_mut()).is_pending());
        assert!(current_log_context().is_empty());
        assert_eq!(
            Context::current().span().span_context().span_id(),
            caller_id
        );
        drop(pending);
        let panic = std::panic::catch_unwind(AssertUnwindSafe(|| {
            telemetry.run_sync::<(), ()>("panicking", ExecutionFields::default(), || {
                panic!("business panic")
            })
        }));
        assert!(panic.is_err());
        assert_eq!(
            Context::current().span().span_context().span_id(),
            caller_id
        );
        let spans = exporter.get_finished_spans().unwrap();
        assert_eq!(spans.len(), 2);
        assert!(spans
            .iter()
            .all(|span| span.parent_span_id == SpanId::INVALID));
    }

    #[derive(Debug)]
    struct RejectingExporter;
    impl SpanExporter for RejectingExporter {
        async fn export(&self, _: Vec<SpanData>) -> opentelemetry_sdk::error::OTelSdkResult {
            Err(opentelemetry_sdk::error::OTelSdkError::InternalFailure(
                "export unavailable".into(),
            ))
        }
    }

    #[tokio::test]
    async fn exporter_failure_and_invalid_identifiers_do_not_change_business_result() {
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(RejectingExporter)
            .build();
        let telemetry = ExecutionTelemetry::from_tracer(provider.tracer("test"));
        let fields = ExecutionFields::default()
            .insert("task_id", Some("invalid\nidentifier"))
            .insert("worker_id", Some(&"x".repeat(257)))
            .insert("workflow_run_id", Some("valid"));
        let result = telemetry
            .run("test", fields, false, async {
                assert_eq!(
                    current_log_context().get("workflow_run_id").unwrap(),
                    "valid"
                );
                assert!(!current_log_context().contains_key("task_id"));
                assert!(!current_log_context().contains_key("worker_id"));
                Ok::<_, ()>(42)
            })
            .await;
        assert_eq!(result, Ok(42));
        assert!(current_log_context().is_empty());
    }
}
