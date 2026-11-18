//! Schedule origin: the schedule fire that started a run, read from history
//! (durable-workflow/.github#243).

use durable_workflow::{
    json,
    testing::{WorkflowEnvironment, WorkflowHistory},
    Error, Value,
};

const TICK: &str = "2026-11-18T00:00:03.500000+00:00";
const BACKFILL: &str = "2026-11-01T00:00:00.000000+00:00";

/// Reads the origin before its first command and after it, picks the activity
/// from it and returns it, so replay checks both the command and the result.
fn reader() -> WorkflowEnvironment {
    let mut env = WorkflowEnvironment::new().unwrap();
    env.register_workflow("report", |ctx, _| async move {
        let origin = ctx.schedule_origin()?;
        let kind = match &origin {
            Some(origin) if origin.occurrence_time.is_some() => "window.report",
            Some(_) => "manual.report",
            None => "adhoc.report",
        };
        ctx.activity(kind, json!([])).await?;
        assert_eq!(ctx.schedule_origin()?, origin);
        Ok(serde_json::to_value(origin).unwrap())
    });
    env
}

/// Records a history whose single activity and result are fixed.
async fn recorded(kind: &'static str, result: Value) -> Value {
    let mut env = WorkflowEnvironment::new().unwrap();
    env.register_workflow("report", move |ctx, _| {
        let result = result.clone();
        async move {
            ctx.activity(kind, json!([])).await?;
            Ok(result)
        }
    });
    env.register_activity(kind, |_, _| async { Ok(json!("done")) });
    env.run_workflow("report", ()).await.unwrap();
    env.last_history().unwrap().export()
}

/// The export with a ScheduleTriggered event recorded right after the start,
/// where the Server records it.
fn scheduled(mut export: Value, marker: Value) -> WorkflowHistory {
    let events = export["history_events"].as_array_mut().unwrap();
    assert_eq!(events[0]["event_type"], "WorkflowStarted");
    let mut event = events[0].clone();
    event["event_type"] = json!("ScheduleTriggered");
    event["payload"] = marker;
    events.insert(1, event);
    for (index, event) in events.iter_mut().enumerate() {
        event["sequence"] = json!(index + 1);
    }
    WorkflowHistory::from_export(export).unwrap()
}

fn marker(trigger: u64, occurrence: Option<&str>) -> Value {
    let mut marker = json!({"schedule_id":"daily-report","schedule_ulid":"01JSCHEDULE",
        "overlap_policy":"skip","cron_expression":"0 0 * * *","timezone":"UTC","trigger_number":trigger});
    if let Some(occurrence) = occurrence {
        marker["occurrence_time"] = json!(occurrence);
    }
    marker
}

/// Keeps only WorkflowStarted; `scheduled` then adds the marker, giving the
/// history of the run's first workflow task.
fn start_only(history: &mut Value) {
    history["history_events"]
        .as_array_mut()
        .unwrap()
        .truncate(1);
}

#[tokio::test]
async fn ticked_and_backfilled_fires_read_their_occurrence_on_cold_replay() {
    for (occurrence, trigger) in [(TICK, 12), (BACKFILL, 1)] {
        let expected = json!({"schedule_id":"daily-report","occurrence_time":occurrence,"trigger_number":trigger});
        let export = recorded("window.report", expected.clone()).await;
        let history = scheduled(export.clone(), marker(trigger, Some(occurrence)));
        let env = reader();
        assert!(env.replay(&history).unwrap().completed);
        // A cold replay of the same history reads the identical origin.
        assert!(reader().replay(&history).unwrap().completed);

        // A different recorded occurrence is a different result, not a guess.
        let other = scheduled(
            export.clone(),
            marker(trigger, Some("2026-10-01T00:00:00+00:00")),
        );
        assert!(matches!(env.replay(&other),
            Err(Error::NonDeterministicReplay(failure)) if failure.reason == "workflow_result_mismatch"));

        // Without the marker the run is not scheduled and picks another activity.
        let plain = WorkflowHistory::from_export(export).unwrap();
        assert!(matches!(env.replay(&plain),
            Err(Error::NonDeterministicReplay(failure)) if failure.actual.as_deref().is_some_and(|a| a.contains("adhoc.report"))));
    }
}

#[tokio::test]
async fn origin_is_available_on_the_first_workflow_task() {
    for (marker, kind) in [
        (Some(marker(12, Some(TICK))), "window.report"),
        (Some(marker(4, None)), "manual.report"),
        (None, "adhoc.report"),
    ] {
        let mut export = recorded("unused", json!(null)).await;
        start_only(&mut export);
        let history = match marker {
            Some(marker) => scheduled(export, marker),
            None => WorkflowHistory::from_export(export).unwrap(),
        };
        let Err(Error::NonDeterministicReplay(failure)) = reader().replay(&history) else {
            panic!("the first task must emit its first command");
        };
        assert_eq!(failure.reason, "unrecorded_command");
        assert!(failure.actual.unwrap().contains(kind));
    }
}

#[tokio::test]
async fn non_scheduled_and_manual_starts_read_absent_values() {
    let export = recorded("adhoc.report", json!(null)).await;
    assert!(
        reader()
            .replay(&WorkflowHistory::from_export(export).unwrap())
            .unwrap()
            .completed
    );

    let expected = json!({"schedule_id":"daily-report","occurrence_time":null,"trigger_number":4});
    let export = recorded("manual.report", expected).await;
    assert!(
        reader()
            .replay(&scheduled(export, marker(4, None)))
            .unwrap()
            .completed
    );
}

#[tokio::test]
async fn unusable_markers_are_absent_not_guessed() {
    let export = recorded("adhoc.report", json!(null)).await;
    for marker in [
        json!({}),
        json!({"schedule_id":""}),
        json!({"schedule_id":7}),
    ] {
        assert!(
            reader()
                .replay(&scheduled(export.clone(), marker))
                .unwrap()
                .completed
        );
    }
    let expected =
        json!({"schedule_id":"daily-report","occurrence_time":null,"trigger_number":null});
    let export = recorded("manual.report", expected).await;
    for (occurrence, trigger) in [
        (json!("2026-11-01T00:00:00"), json!(0)),
        (json!("yesterday"), json!(-1)),
    ] {
        let marker = json!({"schedule_id":"daily-report","occurrence_time":occurrence,"trigger_number":trigger});
        assert!(
            reader()
                .replay(&scheduled(export.clone(), marker))
                .unwrap()
                .completed
        );
    }
}
