use super::*;
use chrono::{DateTime, SecondsFormat, Utc};

// The same official consumer must compile against the pre-accessor SDK.
// Inherent methods win over this explicit unavailable baseline shim.
trait LegacyReplayClock {
    fn replay_now(&self) -> Result<DateTime<Utc>, Error>;
}
impl LegacyReplayClock for WorkflowContext {
    fn replay_now(&self) -> Result<DateTime<Utc>, Error> {
        Err(Error::WorkerLoop(
            "baseline SDK has no replay_now accessor".into(),
        ))
    }
}

pub(super) fn register(worker: &mut Worker, workflow_type: &str) {
    worker.register_workflow(workflow_type, |ctx, _| async move {
        assert!(LegacyReplayClock::replay_now(&ctx).is_err());
        let start = ctx.replay_now().ok();
        ctx.sleep(Duration::from_secs(1)).await?;
        let timer = ctx.replay_now().ok();
        let result = ctx.activity("clock-activity", json!([])).await?;
        let activity = ctx.replay_now().ok();
        Ok(json!({
            "start":start.map(|time| time.to_rfc3339_opts(SecondsFormat::Micros,true)),
            "timer":timer.map(|time| time.to_rfc3339_opts(SecondsFormat::Micros,true)),
            "activity":activity.map(|time| time.to_rfc3339_opts(SecondsFormat::Micros,true)),
            "unchanged_prefix":activity.is_some() && activity==ctx.replay_now().ok(),
            "elapsed_seconds":start.zip(activity).map(|(start,end)| (end-start).num_seconds()),
            "result":result,
        }))
    });
}
