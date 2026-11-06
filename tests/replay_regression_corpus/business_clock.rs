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

pub(super) fn register_child(worker: &mut Worker, workflow_type: &str) {
    worker.register_workflow(workflow_type, |ctx, _| async move {
        let entry_unavailable = ctx.replay_now().is_err();
        let child = ctx
            .start_child_workflow(
                "clock-child",
                ChildWorkflowOptions::new("children"),
                json!([]),
            )
            .await?;
        let completed = ctx.replay_now().ok();
        let outcome = ctx
            .start_child_workflow(
                "clock-child",
                ChildWorkflowOptions::new("children"),
                json!([]),
            )
            .await;
        let Err(Error::ChildWorkflowFailed(failure)) = outcome else {
            return Err(Error::WorkerLoop(
                "expected the recorded child failure".into(),
            ));
        };
        let failed = ctx.replay_now().ok();
        Ok(json!({
            "entry_unavailable":entry_unavailable,
            "completed":completed.map(|time|time.to_rfc3339_opts(SecondsFormat::Micros,true)),
            "failed":failed.map(|time|time.to_rfc3339_opts(SecondsFormat::Micros,true)),
            "unchanged_prefix":failed.is_some() && failed==ctx.replay_now().ok(),
            "result":child.result,
            "failure_reason":failure.reason,
        }))
    });
}

pub(super) fn register_signal(worker: &mut Worker, workflow_type: &str) {
    worker.register_workflow(workflow_type, |ctx, _| async move {
        let entry_unavailable = ctx.replay_now().is_err();
        let first_arguments = ctx.wait_signal("ready").await?;
        let first_time = ctx.replay_now().ok();
        let second_arguments = ctx.wait_signal("ready").await?;
        let second_time = ctx.replay_now().ok();
        Ok(json!({
            "entry_unavailable":entry_unavailable,
            "first_time":first_time.map(|time|time.to_rfc3339_opts(SecondsFormat::Micros,true)),
            "second_time":second_time.map(|time|time.to_rfc3339_opts(SecondsFormat::Micros,true)),
            "unchanged_prefix":second_time.is_some() && second_time==ctx.replay_now().ok(),
            "arguments":[first_arguments,second_arguments],
        }))
    });
}

pub(super) fn register_condition(worker: &mut Worker, workflow_type: &str) {
    worker.register_workflow(workflow_type, |ctx, _| async move {
        let entry_unavailable = ctx.replay_now().is_err();
        let first = ctx
            .wait_condition(
                ConditionWaitOptions::new("ready", "sha256:ready-v1"),
                || Ok(false),
            )
            .await?;
        let first_time = ctx.replay_now().ok();
        let second = ctx
            .wait_condition(
                ConditionWaitOptions::new("ready", "sha256:ready-v1")
                    .timeout(Duration::from_secs(30)),
                || Ok(false),
            )
            .await?;
        let second_time = ctx.replay_now().ok();
        Ok(json!({
            "entry_unavailable":entry_unavailable,
            "results":[format!("{first:?}"),format!("{second:?}")],
            "first_time":first_time.map(|time|time.to_rfc3339_opts(SecondsFormat::Micros,true)),
            "second_time":second_time.map(|time|time.to_rfc3339_opts(SecondsFormat::Micros,true)),
            "unchanged_prefix":second_time.is_some() && second_time==ctx.replay_now().ok(),
        }))
    });
}

pub(super) fn register_parallel_activity(worker: &mut Worker, workflow_type: &str) {
    worker.register_workflow(workflow_type, |ctx, _| async move {
        let operations = || {
            vec![
                ParallelOperation::activity("clock-a", json!([])),
                ParallelOperation::activity("clock-b", json!([])),
            ]
        };
        let entry_unavailable = ctx.replay_now().is_err();
        ctx.parallel(operations()).await?;
        let success = ctx.replay_now().ok();
        let Err(Error::ParallelFailed(failure)) = ctx.parallel(operations()).await else {
            return Err(Error::WorkerLoop(
                "expected recorded parallel failure".into(),
            ));
        };
        let failed = ctx.replay_now().ok();
        Ok(json!({
            "entry_unavailable":entry_unavailable,
            "success_time":success.map(|time|time.to_rfc3339_opts(SecondsFormat::Micros,true)),
            "failure_time":failed.map(|time|time.to_rfc3339_opts(SecondsFormat::Micros,true)),
            "failure_member":failure.member_path,
            "unchanged_prefix":failed.is_some() && failed==ctx.replay_now().ok(),
        }))
    });
}
