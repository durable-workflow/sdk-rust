use super::*;

// The official consumer also runs against the SDK before this API existed.
// Its fallback is ordinary host logging, which repeats the committed prefix.
#[allow(dead_code)]
trait LegacyWorkflowLogging {
    fn logger(&self) -> LegacyLogger;
}
impl LegacyWorkflowLogging for WorkflowContext {
    fn logger(&self) -> LegacyLogger {
        LegacyLogger
    }
}
#[allow(dead_code)]
struct LegacyLogger;
#[allow(dead_code)]
impl LegacyLogger {
    fn info(&self, message: &str) {
        tracing::event!(target:"durable_workflow::workflow",tracing::Level::INFO,message);
    }
}

pub(super) fn register(worker: &mut Worker, workflow_type: &str) {
    worker.register_workflow(workflow_type, |ctx, _| async move {
        ctx.logger().info("committed");
        ctx.sleep(Duration::from_secs(1)).await?;
        ctx.logger().info("fresh");
        Ok(json!("complete"))
    });
}

pub(super) fn is_fixture(workflow_type: &str) -> bool {
    matches!(
        workflow_type,
        "corpus.replay-aware-logging" | "corpus.replay-aware-nexus-pending"
    )
}

pub(super) fn register_nexus_pending(worker: &mut Worker, workflow_type: &str) {
    worker.register_workflow(workflow_type, |ctx, _| async move {
        ctx.logger().info("committed");
        let mut call = Box::pin(ctx.call_nexus_service("billing", "payments", "charge", json!([])));
        std::future::poll_fn(|cx| {
            assert!(std::future::Future::poll(call.as_mut(), cx).is_pending());
            ctx.logger().info("pending Nexus replay");
            std::task::Poll::Pending
        })
        .await
    });
}

#[derive(Default)]
pub(super) struct Capture(Mutex<Vec<String>>);
struct Message(Option<String>);
impl tracing::field::Visit for Message {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "message" {
            self.0 = Some(value.into());
        }
    }
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.0 = Some(format!("{value:?}"));
        }
    }
}
impl tracing::Subscriber for Capture {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        if event.metadata().target() != "durable_workflow::workflow" {
            return;
        }
        let mut message = Message(None);
        event.record(&mut message);
        if let Some(message) = message.0 {
            self.0.lock().unwrap().push(message);
        }
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}
impl Capture {
    pub(super) fn messages(&self) -> Value {
        json!(*self.0.lock().unwrap())
    }
}
