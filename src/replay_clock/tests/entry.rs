//! The task's `replay_clock` entry carrier (`WorkerReplayClock` v1, .github#215).
use super::*;
use crate::condition_input_order;

const RUN: &str = "entry-run";
const ENTRY: &str = "2026-03-04T05:06:07.891234Z";

fn carrier(source: &str, started_at: &str) -> Value {
    json!({
        "schema":"durable-workflow.worker-replay-clock/v1",
        "workflow_run_id":RUN,
        "started_at":started_at,
        "source":source,
    })
}

fn task(clock: Value) -> Value {
    json!({"run_id":RUN,"replay_clock":clock})
}

fn ordinary_start() -> HistoryEvent {
    // The insertion time differs from the carrier: it is never the clock.
    event(
        "WorkflowStarted",
        json!({"workflow_run_id":RUN}),
        "2026-03-04T05:06:09Z",
    )
}

fn entry_context(history: Vec<HistoryEvent>, clock: Option<Value>) -> WorkflowContext {
    let ctx = context(history);
    let entry = clock.and_then(|clock| entry(&task(clock)));
    ctx.state.lock().unwrap().adopt_replay_entry(entry);
    ctx
}

fn unavailable(ctx: &WorkflowContext) -> bool {
    matches!(ctx.replay_now(), Err(Error::ReplayClockUnavailable))
}

#[test]
fn entry_carrier_accepts_only_a_complete_v1_object_for_this_run() {
    let parsed = entry(&task(carrier("run_started_at", ENTRY))).unwrap();
    assert_eq!(
        parsed
            .started_at
            .to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
        ENTRY
    );
    assert_eq!(parsed.started_at.timestamp_subsec_micros(), 891_234);
    assert!(!parsed.redrive);
    assert!(
        entry(&task(carrier("redrive_replayed_started_at", ENTRY)))
            .unwrap()
            .redrive
    );
    let whole = entry(&task(carrier(
        "run_started_at",
        "2026-03-04T05:06:07.000000Z",
    )))
    .unwrap();
    assert_eq!(whole.started_at.timestamp_subsec_nanos(), 0);

    assert_eq!(entry(&json!({"run_id":RUN})), None);
    assert_eq!(entry(&task(Value::Null)), None);
    assert_eq!(entry(&task(json!("2026-03-04T05:06:07.891234Z"))), None);
    let valid = carrier("run_started_at", ENTRY);
    for (field, value) in [
        ("schema", json!("durable-workflow.worker-replay-clock/v2")),
        ("schema", Value::Null),
        ("workflow_run_id", json!("another-run")),
        ("workflow_run_id", json!("")),
        ("source", json!("run_created_at")),
        ("source", Value::Null),
        ("started_at", json!(1_772_600_767)),
        ("started_at", json!("2026-03-04T05:06:07Z")),
        ("started_at", json!("2026-03-04T05:06:07.891Z")),
        ("started_at", json!("2026-03-04T05:06:07.8912345Z")),
        ("started_at", json!("2026-03-04T05:06:07.891234+00:00")),
        ("started_at", json!("2026-03-04T05:06:07.891234z")),
        ("started_at", json!("2026-03-04 05:06:07.891234Z")),
        ("started_at", json!(" 2026-03-04T05:06:07.891234Z")),
        ("started_at", json!("2026-13-04T05:06:07.891234Z")),
        ("started_at", json!("2026-02-30T05:06:07.891234Z")),
        ("started_at", json!("2026-12-31T23:59:60.000000Z")),
    ] {
        let mut changed = valid.clone();
        changed[field] = value.clone();
        assert_eq!(entry(&task(changed)), None, "{field}={value}");
        let mut missing = valid.clone();
        missing.as_object_mut().unwrap().remove(field);
        assert_eq!(entry(&task(missing)), None, "missing {field}");
    }
    // The v1 object has exactly four keys; a non-string source is absent.
    let mut extra = valid.clone();
    extra["note"] = json!("unexpected");
    assert_eq!(entry(&task(extra)), None);
    let mut numeric = valid.clone();
    numeric["source"] = json!(1);
    assert_eq!(entry(&task(numeric)), None);
    // The carrier must name the task's own run, which must be present.
    assert_eq!(entry(&json!({"replay_clock":valid})), None);
    assert_eq!(entry(&json!({"run_id":"","replay_clock":valid})), None);
    assert_eq!(entry(&json!({"run_id":"other","replay_clock":valid})), None);
}

#[test]
fn query_attestation_distinguishes_absent_null_and_object_keys() {
    assert_eq!(attestation(&json!({"run_id":RUN})), Attestation::Unattested);
    assert_eq!(
        attestation(&json!({"run_id":RUN,"replay_clock":null})),
        Attestation::Unprovable
    );
    assert_eq!(
        attestation(&json!({"run_id":RUN,"replay_clock":{}})),
        Attestation::Unprovable
    );
    assert!(matches!(
        attestation(&task(carrier("run_started_at", ENTRY))),
        Attestation::Entry(_)
    ));
}

#[test]
fn entry_read_returns_the_carrier_and_replays_identically() {
    let mut history = vec![ordinary_start()];
    history.extend(timer(1, "2026-03-04T05:06:12.5Z"));
    let execute = || {
        let ctx = entry_context(history.clone(), Some(carrier("run_started_at", ENTRY)));
        let inner = ctx.clone();
        ready(
            &ctx,
            Box::pin(async move {
                let entry = read(&inner);
                assert_eq!(read(&inner), entry, "repeated reads are stable");
                inner.sleep(Duration::from_secs(1)).await?;
                let timer = read(&inner);
                let elapsed = (inner.replay_now()?
                    - DateTime::parse_from_rfc3339(&entry)
                        .unwrap()
                        .with_timezone(&Utc))
                .num_milliseconds();
                AvroValue::from_serialize(
                    &json!({"entry":entry,"timer":timer,"elapsed_ms":elapsed}),
                )
            }),
        )
        .into_json()
        .unwrap()
    };
    let first = execute();
    assert_eq!(first, execute(), "a later lease carries the same value");
    assert_eq!(
        first,
        json!({"entry":ENTRY,"timer":"2026-03-04T05:06:12.500000Z","elapsed_ms":4608})
    );
}

#[test]
fn entry_absent_or_invalid_keeps_the_existing_unavailable_entry() {
    let mut wrong_schema = carrier("run_started_at", ENTRY);
    wrong_schema["schema"] = json!("other");
    for clock in [None, Some(wrong_schema)] {
        let ctx = entry_context(vec![ordinary_start()], clock);
        let _guard = Guard::enter(&ctx);
        assert!(unavailable(&ctx));
    }
    // No query, snapshot or update path reads the workflow task's carrier.
    assert_eq!(snapshot_time(&[ordinary_start()], DEFAULT_CODEC), None);
}

#[test]
fn entry_source_must_match_the_history_start_kind() {
    // An ordinary history cannot take a redrive anchor.
    let ctx = entry_context(
        vec![ordinary_start()],
        Some(carrier("redrive_replayed_started_at", ENTRY)),
    );
    let guard = Guard::enter(&ctx);
    assert!(unavailable(&ctx));
    drop(guard);

    // A declared but unproved redrive start cannot take an ordinary start.
    let unproved = event(
        "WorkflowStarted",
        json!({"recovery_kind":"redrive","workflow_run_id":RUN}),
        "2026-03-04T05:06:09Z",
    );
    let ctx = entry_context(
        vec![unproved.clone()],
        Some(carrier("run_started_at", ENTRY)),
    );
    let guard = Guard::enter(&ctx);
    assert!(unavailable(&ctx));
    drop(guard);

    // A StartAccepted redrive outcome also declares a redrive.
    let accepted = event("StartAccepted", json!({"outcome":"redriven"}), START);
    let ctx = entry_context(
        vec![accepted, ordinary_start()],
        Some(carrier("run_started_at", ENTRY)),
    );
    let guard = Guard::enter(&ctx);
    assert!(unavailable(&ctx));
    drop(guard);

    // Server proved the redrive anchor that this history could not.
    let ctx = entry_context(
        vec![unproved],
        Some(carrier("redrive_replayed_started_at", ENTRY)),
    );
    let _guard = Guard::enter(&ctx);
    assert_eq!(read(&ctx), ENTRY);
}

#[test]
fn entry_carrier_must_agree_with_an_unproved_recorded_anchor() {
    // A recorded anchor without `recovery_kind` declares a redrive that this
    // history cannot prove. The carrier must still match the anchor.
    let unproved = |anchor: &str| {
        event(
            "WorkflowStarted",
            json!({"workflow_run_id":RUN,"replayed_started_at":anchor}),
            "2026-03-04T05:06:09Z",
        )
    };
    let ctx = entry_context(
        vec![unproved("2026-01-01T00:00:00Z")],
        Some(carrier("redrive_replayed_started_at", ENTRY)),
    );
    let guard = Guard::enter(&ctx);
    assert!(unavailable(&ctx));
    drop(guard);
    // An unparsable recorded anchor refuses the carrier too.
    let ctx = entry_context(
        vec![unproved("not a time")],
        Some(carrier("redrive_replayed_started_at", ENTRY)),
    );
    let guard = Guard::enter(&ctx);
    assert!(unavailable(&ctx));
    drop(guard);
    // A null anchor or resume step is absent: the history is ordinary.
    let ctx = entry_context(
        vec![event(
            "WorkflowStarted",
            json!({"workflow_run_id":RUN,"replayed_started_at":null,"resume_step_sequence":null}),
            "2026-03-04T05:06:09Z",
        )],
        Some(carrier("run_started_at", ENTRY)),
    );
    let guard = Guard::enter(&ctx);
    assert_eq!(read(&ctx), ENTRY);
    drop(guard);
    let ctx = entry_context(
        vec![unproved("2026-03-04T05:06:07.891234+00:00")],
        Some(carrier("redrive_replayed_started_at", ENTRY)),
    );
    let _guard = Guard::enter(&ctx);
    assert_eq!(read(&ctx), ENTRY);
}

#[test]
fn entry_carrier_never_replaces_a_redrive_anchor_proved_from_history() {
    for source in ["redrive_replayed_started_at", "run_started_at"] {
        let ctx = entry_context(vec![start()], Some(carrier(source, ENTRY)));
        let _guard = Guard::enter(&ctx);
        assert_eq!(read(&ctx), START, "{source}");
    }
}

#[test]
fn entry_carrier_does_not_open_cancellation_callbacks() {
    let ctx = entry_context(
        vec![ordinary_start()],
        Some(carrier("run_started_at", ENTRY)),
    );
    let mut state = ctx.state.lock().unwrap();
    assert_eq!(
        state
            .replay_time()
            .unwrap()
            .to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
        ENTRY
    );
    state.cancellation_clock = Some(crate::cancellation_replay_clock::ReplayClock::new(
        &[],
        &[],
        None,
    ));
    assert!(matches!(
        state.replay_time(),
        Err(Error::ReplayClockUnavailable)
    ));
}

#[test]
fn entry_carrier_reaches_the_workflow_task_execution_path() {
    let mut worker = Worker::new(Client::new("http://127.0.0.1:8080").unwrap(), "clock-tests");
    worker.register_workflow("clock", |ctx, _args| async move {
        Ok(json!(ctx
            .replay_now()
            .map(|t| t.to_rfc3339_opts(chrono::SecondsFormat::Micros, true))
            .ok()))
    });
    let wire = json!({
        "task_id":"t1","workflow_id":"entry-workflow","run_id":RUN,"workflow_type":"clock",
        "payload_codec":DEFAULT_CODEC,"history_events":[{"event_type":"WorkflowStarted","payload":{"workflow_run_id":RUN},"recorded_at":"2026-03-04T05:06:09Z"}],
        "replay_clock":carrier("run_started_at", ENTRY),
    });
    let task: WorkflowTask = serde_json::from_value(wire.clone()).unwrap();
    let result = |worker: &Worker| {
        let commands = worker.execute_workflow_task(task.clone()).unwrap();
        assert_eq!(commands[0]["type"], "complete_workflow");
        decode_wire_value(&commands[0]["result"], DEFAULT_CODEC).unwrap()
    };
    // A public task built without the poll's claim evidence keeps today's value.
    assert_eq!(result(&worker), Value::Null);
    worker.ordered_condition_claim = condition_input_order::read_claim(&wire).unwrap();
    assert_eq!(result(&worker), json!(ENTRY));
    assert_eq!(result(&worker), json!(ENTRY));
}

fn entry_reading_worker() -> Worker {
    let mut worker = Worker::new(Client::new("http://127.0.0.1:8080").unwrap(), "clock-tests");
    worker.register_replayed_workflow("clock", String::new, |ctx, _args, state| async move {
        let entry = ctx
            .replay_now()?
            .to_rfc3339_opts(chrono::SecondsFormat::Micros, true);
        state.update(|value| *value = entry.clone())?;
        Ok(json!(entry))
    });
    worker.register_replayed_query::<String, _, _>(
        "clock",
        "entry",
        |_ctx, state, _args| async move { Ok(json!(state.as_str())) },
    );
    worker.register_update("clock", "inspect", |_ctx, _args| async {
        Ok(json!("updated"))
    });
    worker
}

/// A body that tolerates an unavailable entry read stores what it saw.
fn tolerant_worker() -> Worker {
    let mut worker = Worker::new(Client::new("http://127.0.0.1:8080").unwrap(), "clock-tests");
    worker.register_replayed_workflow("clock", String::new, |ctx, _args, state| async move {
        let entry = ctx.replay_now().map_or_else(
            |_| "none".to_string(),
            |time| time.to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
        );
        state.update(|value| *value = entry.clone())?;
        Ok(json!(entry))
    });
    worker.register_replayed_query::<String, _, _>(
        "clock",
        "entry",
        |_ctx, state, _args| async move { Ok(json!(state.as_str())) },
    );
    worker
}

#[tokio::test]
async fn query_rebuild_of_an_entry_read_follows_the_query_tasks_attestation() {
    let worker = tolerant_worker();
    let started = json!({"event_type":"WorkflowStarted","payload":{"workflow_run_id":RUN},"recorded_at":"2026-03-04T05:06:09Z"});
    let workflow = |clock: Option<Value>| {
        let mut wire = json!({"task_id":"t1","workflow_id":"entry-workflow","run_id":RUN,
            "workflow_type":"clock","payload_codec":DEFAULT_CODEC,"history_events":[started]});
        if let Some(clock) = clock {
            wire["replay_clock"] = clock;
        }
        let mut worker = tolerant_worker();
        worker.ordered_condition_claim = condition_input_order::read_claim(&wire).unwrap();
        let task: WorkflowTask = serde_json::from_value(wire).unwrap();
        let commands = worker.execute_workflow_task(task).unwrap();
        decode_wire_value(&commands[0]["result"], DEFAULT_CODEC).unwrap()
    };
    let query = |clock: Option<Value>| {
        let mut wire = json!({"query_task_id":"q1","workflow_type":"clock","query_name":"entry",
            "run_id":RUN,"payload_codec":DEFAULT_CODEC,"history_events":[started]});
        if let Some(clock) = clock {
            wire["replay_clock"] = clock;
        }
        let attestation = attestation(&wire);
        let task: QueryTask = serde_json::from_value(wire).unwrap();
        (task, attestation)
    };
    let ask = |(task, attestation): (QueryTask, Attestation)| {
        let worker = &worker;
        async move {
            worker
                .execute_query_task_with_authority(task, None, attestation)
                .await
                .map(|answer| answer.into_json().unwrap())
        }
    };

    // A Server before #1338 omits the key: the tolerant body's "none" is not
    // what the workflow task computed, so the query fails closed.
    let failure = ask(query(None)).await.unwrap_err();
    assert_eq!(failure.reason, "query_workflow_state_unavailable");
    assert!(
        failure.message.contains("does not attest"),
        "{}",
        failure.message
    );
    // An object this SDK refuses was refused on the workflow task too: no
    // entry was carried there, so the rebuild matches it.
    let mut invalid = carrier("run_started_at", ENTRY);
    invalid["schema"] = json!("other");
    assert_eq!(workflow(Some(invalid.clone())), json!("none"));
    assert_eq!(ask(query(Some(invalid))).await.unwrap(), json!("none"));
    // A Server attesting no provable start matches a workflow task without one.
    assert_eq!(workflow(None), json!("none"));
    assert_eq!(ask(query(Some(Value::Null))).await.unwrap(), json!("none"));
    // A carrier on both answers with the same entry.
    let clock = carrier("run_started_at", ENTRY);
    assert_eq!(workflow(Some(clock.clone())), json!(ENTRY));
    for _ in 0..2 {
        assert_eq!(ask(query(Some(clock.clone()))).await.unwrap(), json!(ENTRY));
    }
    // The public, wire-less path has no attestation.
    let (task, _) = query(Some(clock));
    assert_eq!(
        worker.execute_query_task(task).await.unwrap_err().reason,
        "query_workflow_state_unavailable"
    );
}

#[tokio::test]
async fn query_rebuild_without_an_entry_read_is_unaffected_by_attestation() {
    let mut worker = Worker::new(Client::new("http://127.0.0.1:8080").unwrap(), "clock-tests");
    worker.register_replayed_workflow(
        "clock",
        || 7_u64,
        |_ctx, _args, _state| async move { Ok(Value::Null) },
    );
    worker.register_replayed_query::<u64, _, _>(
        "clock",
        "value",
        |_ctx, state, _args| async move { Ok(json!(*state)) },
    );
    let task: QueryTask = serde_json::from_value(json!({"query_task_id":"q1","workflow_type":"clock",
        "query_name":"value","run_id":RUN,"payload_codec":DEFAULT_CODEC,
        "history_events":[{"event_type":"WorkflowStarted","payload":{"workflow_run_id":RUN},"recorded_at":"2026-03-04T05:06:09Z"}]}))
    .unwrap();
    assert_eq!(
        worker
            .execute_query_task(task)
            .await
            .unwrap()
            .into_json()
            .unwrap(),
        json!(7)
    );
}

#[test]
fn entry_reading_run_answers_an_update_task_carrying_the_carrier() {
    // Rust update tasks run the handler against history; they do not rebuild
    // the workflow body, so the body's entry read cannot diverge there.
    let worker = entry_reading_worker();
    let task: WorkflowTask = serde_json::from_value(json!({
        "task_id":"u1","workflow_type":"clock","run_id":RUN,"payload_codec":DEFAULT_CODEC,
        "workflow_update_id":"u1","update_name":"inspect",
        "history_events":[{"event_type":"WorkflowStarted","payload":{"workflow_run_id":RUN},"recorded_at":"2026-03-04T05:06:09Z"}, {"event_type":"UpdateAccepted","payload":{"update_id":"u1","update_name":"inspect"},"recorded_at":START}],
        "replay_clock":carrier("run_started_at", ENTRY),
    }))
    .unwrap();
    let commands = worker.execute_update_task(&task, "u1").unwrap();
    assert_eq!(commands[0]["type"], "complete_update");
}

#[test]
fn offline_replay_takes_the_carrier_as_an_optional_input() {
    use crate::testing::{WorkflowEnvironment, WorkflowHistory};
    let environment = WorkflowEnvironment::from_worker(entry_reading_worker());
    let output = encode_typed_envelope(&AvroValue::String(ENTRY.into()), DEFAULT_CODEC).unwrap();
    let history = WorkflowHistory::from_history(
        "clock",
        (),
        json!({"workflow_id":"entry-workflow","run_id":RUN,"events":[
            {"event_type":"WorkflowStarted","payload":{"workflow_run_id":RUN},"recorded_at":"2026-03-04T05:06:09Z"},
            {"event_type":"WorkflowCompleted","payload":{"output":output},"recorded_at":"2026-03-04T05:06:09Z"},
        ]}),
    )
    .unwrap();
    assert!(environment.replay(&history).is_err());
    let mut foreign = carrier("run_started_at", ENTRY);
    foreign["workflow_run_id"] = json!("another-run");
    assert!(history.clone().replay_clock(foreign).is_err());
    assert!(history
        .clone()
        .replay_clock(json!({"started_at":ENTRY}))
        .is_err());
    let report = environment
        .replay(
            &history
                .replay_clock(carrier("run_started_at", ENTRY))
                .unwrap(),
        )
        .unwrap();
    assert!(report.completed);
}

#[tokio::test]
async fn an_empty_group_keeps_the_entry_clock_and_its_attestation_rule() {
    // An empty root group consumes no history and keeps the entry value, so
    // a read after it is still an entry read and follows the attestation.
    let worker_for = || {
        let mut worker = Worker::new(Client::new("http://127.0.0.1:8080").unwrap(), "clock-tests");
        worker.register_replayed_workflow("clock", String::new, |ctx, _args, state| async move {
            ctx.parallel(Vec::new()).await?;
            let read = ctx.replay_now().map_or_else(
                |_| "none".to_string(),
                |time| time.to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
            );
            state.update(|value| *value = read.clone())?;
            Ok(json!(read))
        });
        worker.register_replayed_query::<String, _, _>(
            "clock",
            "entry",
            |_ctx, state, _args| async move { Ok(json!(state.as_str())) },
        );
        worker
    };
    let started = json!({"event_type":"WorkflowStarted","payload":{"workflow_run_id":RUN},"recorded_at":"2026-03-04T05:06:09Z"});
    let workflow = |clock: Option<Value>| {
        let mut wire = json!({"task_id":"t1","workflow_id":"entry-workflow","run_id":RUN,
            "workflow_type":"clock","payload_codec":DEFAULT_CODEC,"history_events":[started]});
        if let Some(clock) = clock {
            wire["replay_clock"] = clock;
        }
        let mut worker = worker_for();
        worker.ordered_condition_claim = condition_input_order::read_claim(&wire).unwrap();
        let commands = worker
            .execute_workflow_task(serde_json::from_value(wire).unwrap())
            .unwrap();
        decode_wire_value(&commands[0]["result"], DEFAULT_CODEC).unwrap()
    };
    let worker = worker_for();
    let query = |clock: Option<Value>| {
        let mut query = json!({"query_task_id":"q1","workflow_type":"clock","query_name":"entry",
            "run_id":RUN,"payload_codec":DEFAULT_CODEC,"history_events":[started]});
        if let Some(clock) = clock {
            query["replay_clock"] = clock;
        }
        let attestation = attestation(&query);
        let worker = &worker;
        async move {
            worker
                .execute_query_task_with_authority(
                    serde_json::from_value(query).unwrap(),
                    None,
                    attestation,
                )
                .await
                .map(|answer| answer.into_json().unwrap())
        }
    };
    let clock = carrier("run_started_at", ENTRY);
    assert_eq!(workflow(Some(clock.clone())), json!(ENTRY));
    assert_eq!(workflow(None), json!("none"));
    assert_eq!(
        query(None).await.unwrap_err().reason,
        "query_workflow_state_unavailable"
    );
    assert_eq!(query(Some(Value::Null)).await.unwrap(), json!("none"));
    assert_eq!(query(Some(clock)).await.unwrap(), json!(ENTRY));
}
