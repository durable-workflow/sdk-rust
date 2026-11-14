//! Exact-published-package Worker qualification for Server #980.
//! No Server or database is created here. The caller retains the immutable
//! producer/package/build receipt, read-only physical-row counts and cleanup.
use chrono::{DateTime, SecondsFormat, Utc};
use durable_workflow::{
    json, ActivityError, ActivityOptions, ActivityRetryPolicy, Client, Error, Value, Worker,
    WorkerRetryPolicy,
};
#[cfg(target_os = "linux")]
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    io::Read,
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

const CHILD: &str = "DURABLE_WORKFLOW_CLOCK_CHILD";
const QUEUE: &str = "DURABLE_WORKFLOW_CLOCK_QUEUE";
const TYPE: &str = "DURABLE_WORKFLOW_CLOCK_TYPE";
const PHASE: &str = "DURABLE_WORKFLOW_CLOCK_PHASE";
const RUN: &str = "DURABLE_WORKFLOW_CLOCK_RUN";
const OUTPUT: &str = "WORKFLOW_CLOCK_RESULT ";
const OUTPUT_LIMIT: u64 = 65_536;
const IMAGE: &str = "DURABLE_WORKFLOW_CLOCK_IMAGE";
const TIMER_SECONDS: u64 = 5;
#[cfg(target_os = "linux")]
const CONTROL: &str = "DURABLE_WORKFLOW_CLOCK_EXECUTABLE_CONTROL";
const CHILD_ARGUMENTS: [&str; 6] = [
    "--ignored",
    "--exact",
    "published_clock_worker_entrypoint",
    "--nocapture",
    "--test-threads=1",
    "--quiet",
];

fn client() -> Client {
    assert_eq!(
        std::env::var("DURABLE_WORKFLOW_LOCAL_ISOLATED").as_deref(),
        Ok("1")
    );
    let origin = std::env::var("DURABLE_WORKFLOW_SERVER_URL").unwrap();
    assert!(origin
        .strip_prefix("http://127.0.0.1:")
        .and_then(|port| port.parse::<u16>().ok())
        .is_some_and(|port| port > 0));
    Client::builder(origin)
        .namespace("default")
        .timeout(Duration::from_secs(3))
        .token(Some(std::env::var("DURABLE_WORKFLOW_AUTH_TOKEN").unwrap()))
        .build()
        .unwrap()
}

fn instant(value: &Value) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(value.as_str().expect("full UTC clock text"))
        .expect("valid full-precision timestamp")
        .with_timezone(&Utc)
}

fn clock(value: DateTime<Utc>) -> String {
    value.to_rfc3339_opts(SecondsFormat::Nanos, true)
}

#[derive(Default)]
struct Observations {
    prefix_calls: usize,
    boundary_calls: Vec<Value>,
}

fn worker(
    client: Client,
    queue: &str,
    kind: &str,
    phase: usize,
    observations: Arc<Mutex<Observations>>,
) -> Worker {
    let prefix = format!("{kind}.prefix");
    let boundary = format!("{kind}.boundary");
    let mut worker = Worker::new(client, queue)
        .worker_id(format!("{queue}-phase-{phase}"))
        .poll_timeout(Duration::from_millis(100))
        .retry_policy(WorkerRetryPolicy {
            max_retries: 1,
            initial_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_millis(10),
        });
    let saved = observations.clone();
    worker.register_activity(&prefix, move |_, _| {
        let saved = saved.clone();
        async move {
            saved.lock().unwrap().prefix_calls += 1;
            Ok(json!("original-prefix"))
        }
    });
    worker.register_activity(&boundary, move |ctx, arguments| {
        let saved = observations.clone();
        async move {
            assert_eq!(arguments.as_array().unwrap().len(), 1);
            saved.lock().unwrap().boundary_calls.push(json!({
                "clock":arguments[0], "workflow_id":ctx.workflow_id(),
                "run_id":ctx.run_id(), "activity_execution_id":ctx.activity_execution_id()
            }));
            if phase < 2 {
                return Err(
                    ActivityError::new("ClockFixture", "declared activity boundary")
                        .non_retryable(true)
                        .into(),
                );
            }
            Ok(json!("recovered"))
        }
    });
    // The workflow definition is identical in all processes and all three runs.
    // Only the activity fixture chooses its declared failure/success response.
    worker.register_workflow(kind, move |ctx, _| {
        let prefix = prefix.clone();
        let boundary = boundary.clone();
        async move {
            let entry = ctx.replay_now().ok().map(clock);
            let original = ctx.activity(&prefix, json!([])).await?;
            let after_activity = ctx.replay_now()?;
            let stable_activity = ctx.replay_now()? == after_activity;
            ctx.sleep(Duration::from_secs(TIMER_SECONDS)).await?;
            let after_timer = ctx.replay_now()?;
            let stable_timer = ctx.replay_now()? == after_timer;
            let observed = json!({"entry":entry, "activity":clock(after_activity),
                "timer":clock(after_timer), "stable_activity":stable_activity,
                "stable_timer":stable_timer});
            let result = ctx
                .activity_with_options(
                    &boundary,
                    ActivityOptions::new().retry_policy(ActivityRetryPolicy::new(1)),
                    json!([observed]),
                )
                .await?;
            Ok(json!({"prefix":original,"result":result}))
        }
    });
    worker
        .set_workflow_definition_sources(kind, &[include_str!("replay_clock_redrive_server.rs")])
        .unwrap();
    worker
}

#[tokio::test]
#[ignore = "child entrypoint: invoked only by the owned published qualification parent"]
async fn published_clock_worker_entrypoint() {
    #[cfg(target_os = "linux")]
    if let Ok(mode) = std::env::var(CONTROL) {
        executable_control(&mode).await;
        return;
    }
    assert_eq!(std::env::var(CHILD).as_deref(), Ok("1"));
    let image = executable_identity();
    assert_eq!(
        image,
        serde_json::from_str::<Value>(&std::env::var(IMAGE).unwrap()).unwrap()
    );
    let client = client();
    assert_eq!(client.cluster_info().await.unwrap()["version"], "3.11.0");
    let queue = std::env::var(QUEUE).unwrap();
    let kind = std::env::var(TYPE).unwrap();
    let run = std::env::var(RUN).unwrap();
    let phase: usize = std::env::var(PHASE).unwrap().parse().unwrap();
    assert!(phase <= 2);
    let observations = Arc::new(Mutex::new(Observations::default()));
    let worker = worker(client.clone(), &queue, &kind, phase, observations.clone());
    worker.register().await.unwrap();
    let selected = run.clone();
    tokio::time::timeout(
        Duration::from_secs(25),
        worker.run_until(async move {
            loop {
                let description = client
                    .describe_workflow_run(&queue, &selected)
                    .await
                    .unwrap();
                if description.is_terminal() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }),
    )
    .await
    .expect("cold worker must reach its selected terminal run")
    .unwrap();
    let observations = observations.lock().unwrap();
    println!(
        "{OUTPUT}{}",
        json!({"pid":std::process::id(),"executable":image,
        "run_id":run,"phase":phase,"prefix_calls":observations.prefix_calls,
        "boundary_calls":observations.boundary_calls})
    );
}

async fn activation(queue: &str, kind: &str, run: &str, phase: usize, image: &Value) -> Value {
    let mut command = image_command();
    command
        .env(CHILD, "1")
        .env(QUEUE, queue)
        .env(TYPE, kind)
        .env(RUN, run)
        .env(PHASE, phase.to_string())
        .env(IMAGE, image.to_string());
    for name in [
        "DURABLE_WORKFLOW_LOCAL_ISOLATED",
        "DURABLE_WORKFLOW_SERVER_URL",
        "DURABLE_WORKFLOW_AUTH_TOKEN",
    ] {
        command.env(name, std::env::var(name).unwrap());
    }
    let (pid, output) = run_child(command).await;
    let record = parse_report(&output);
    assert_eq!(record["pid"], pid);
    assert_eq!(record["executable"], *image);
    assert_eq!(record["run_id"], run);
    assert_eq!(record["phase"], phase);
    record
}

fn events<'a>(history: &'a Value, kind: &str) -> Vec<&'a Value> {
    history["history_events"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|event| event["type"] == kind)
        .collect()
}

fn one<'a>(history: &'a Value, kind: &str) -> &'a Value {
    let found = events(history, kind);
    assert_eq!(found.len(), 1, "one {kind} event");
    found[0]
}

fn validate_export(history: &Value, workflow: &str, run: &str, kind: &str) {
    assert_eq!(
        history["history_complete"], true,
        "terminal, retained and unpruned history"
    );
    assert_eq!(history["workflow"]["instance_id"], workflow);
    assert_eq!(history["workflow"]["run_id"], run);
    assert_eq!(history["workflow"]["workflow_type"], kind);
    assert_eq!(history["workflow"]["queue"], workflow);
    assert_eq!(history["workflow"]["details_state"], "retained");
    assert!(history["workflow"]["details_pruned_at"].is_null());
    assert_eq!(history["redaction"]["applied"], false);
    assert_eq!(
        history["selected_run"]["waits_projection_source"],
        "workflow_run_waits"
    );
    assert!(history["tasks"].is_array());
    assert!(history["waits"].is_array());
}

fn zero_timer_tasks_and_waits(history: &Value) {
    // All stored statuses are counted. export.timers is a history projection,
    // not physical timer inventory, and must not be used for the row proof.
    assert_eq!(
        history["tasks"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|row| row["type"] == "timer")
            .count(),
        0
    );
    assert_eq!(
        history["waits"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|row| row["kind"] == "timer")
            .count(),
        0
    );
}

fn assert_reused_timer_payload(original: &Value, successor: &Value) {
    let mut expected = original.as_object().unwrap().clone();
    for field in ["task", "command", "timer_id"] {
        expected.remove(field);
    }
    let mut actual = successor.as_object().unwrap().clone();
    for field in ["task", "command"] {
        assert!(
            !actual.contains_key(field),
            "reused timer must not carry {field}"
        );
    }
    for field in ["timer_id", "reused_from_run_id", "reused_recorded_at"] {
        actual.remove(field);
    }
    assert_eq!(actual, expected, "complete reused timer payload");
}

#[test]
fn reused_timer_payload_allows_only_producer_rewrites() {
    let original = json!({"sequence":2,"delay_seconds":5,"timer_id":"source",
        "fire_at":"2026-11-14T00:00:00.123456Z","task":{},"command":{}});
    let accepted = json!({"sequence":2,"delay_seconds":5,"timer_id":"successor",
        "fire_at":"2026-11-14T00:00:00.123456Z",
        "reused_from_run_id":"predecessor","reused_recorded_at":"2026-11-14T00:00:01.123456Z"});
    assert_reused_timer_payload(&original, &accepted);
    for (key, value) in [
        ("fire_at", json!("2026-11-14T00:00:00.123457Z")),
        ("unexpected", json!(true)),
        ("task", json!({})),
        ("command", json!({})),
    ] {
        let mut changed = accepted.clone();
        changed[key] = value;
        assert!(
            std::panic::catch_unwind(|| assert_reused_timer_payload(&original, &changed)).is_err()
        );
    }
}

#[test]
fn clock_comparison_preserves_fraction_and_normalizes_utc_offset() {
    let precise = instant(&json!("2026-11-14T00:00:00.123456Z"));
    assert_eq!(precise, instant(&json!("2026-11-14T01:00:00.123456+01:00")));
    assert_ne!(precise, instant(&json!("2026-11-14T00:00:00Z")));
    assert_eq!(precise.timestamp_subsec_nanos(), 123_456_000);
}

#[tokio::test]
#[ignore = "requires exact published package/Server3.11 provenance and separate physical-row collector"]
async fn published_timer_clock_survives_first_and_repeated_cold_redrive() {
    let image = executable_identity();
    let client = client();
    assert_eq!(client.cluster_info().await.unwrap()["version"], "3.11.0");
    let id = format!("rust-clock-{}", durable_workflow::Uuid::new_v4());
    let kind = format!("qualification.clock.{id}");
    // Registration is distinct from polling. Keep this process's exact compiled
    // definition available while each cold execution worker exits and deregisters.
    let admission_id = format!("{id}-admission");
    let admission = worker(client.clone(), &id, &kind, 0, Arc::default())
        .worker_id(admission_id.clone());
    let registered = admission.register().await.unwrap();
    assert!(registered.registered);
    assert_eq!(registered.worker_id, admission_id);
    let handle = client
        .start_workflow(&kind, &id, &id, json!([]))
        .await
        .unwrap();
    let mut run = handle.run_id.unwrap();
    let mut runs: Vec<Value> = Vec::new();
    let mut pids = BTreeSet::new();
    let mut timer_ids = BTreeSet::new();
    let mut original_timer = None;
    let mut original_activity = None;
    let mut original_start = None;
    let mut original_fingerprint = None;
    let mut registration_refusals = Vec::new();
    for phase in 0..=2 {
        let previous_run = run.clone();
        if phase > 0 {
            client
                .deregister_worker_registration(&admission_id)
                .await
                .unwrap();
            let before = client.export_workflow_history(&id, &run).await.unwrap();
            let rejected_request = format!("{id}-without-registration-{phase}");
            let rejected = client
                .redrive_workflow_run(&id, &run, Some(&rejected_request))
                .await
                .expect_err("redrive must refuse while no matching definition is registered");
            let Error::Http { status, body } = rejected else {
                panic!("expected the producer's definition-registration refusal");
            };
            assert_eq!(status.as_u16(), 409);
            let refusal: Value = serde_json::from_str(&body).unwrap();
            assert_eq!(refusal["reason"], "workflow_definition_unavailable_or_changed");
            assert_eq!(refusal["accepted"], false);
            assert_eq!(refusal["workflow_id"], id);
            assert_eq!(refusal["continued_from_run_id"], run);
            assert_eq!(refusal.get("workflow_run_id"), Some(&Value::Null));
            assert_eq!(refusal.get("task_id"), Some(&Value::Null));
            let after = client.export_workflow_history(&id, &run).await.unwrap();
            assert_eq!(before["history_events"], after["history_events"]);
            registration_refusals.push(json!({"phase":phase,"source_run_id":run,
                "reason":refusal["reason"],"accepted":false}));

            // This Worker only registers. Only the fresh child below polls or
            // executes the successor, using the same source-bound fingerprint.
            let registered = admission.register().await.unwrap();
            assert!(registered.registered);
            assert_eq!(registered.worker_id, admission_id);
            let request = format!("{id}-redrive-{phase}");
            let redrive = client
                .redrive_workflow_run(&id, &run, Some(&request))
                .await
                .unwrap();
            assert_eq!(redrive.source_run_id, run);
            assert_eq!(redrive.workflow_id, id);
            assert_eq!(redrive.resume_step_sequence, 3);
            run = redrive.run_id;
        }
        let started = Instant::now();
        let observed = activation(&id, &kind, &run, phase, &image).await;
        let elapsed = started.elapsed();
        assert!(
            pids.insert(observed["pid"].as_u64().unwrap()),
            "fresh process for every run"
        );
        assert_eq!(observed["prefix_calls"], usize::from(phase == 0));
        let callbacks = observed["boundary_calls"].as_array().unwrap();
        assert_eq!(
            callbacks.len(),
            1,
            "only the failed activity executes on each redrive"
        );
        assert_eq!(callbacks[0]["workflow_id"], id);
        assert_eq!(callbacks[0]["run_id"], run);
        assert!(callbacks[0]["activity_execution_id"]
            .as_str()
            .is_some_and(|id| !id.is_empty()));
        let callback = &callbacks[0]["clock"];
        assert_eq!(callback["stable_activity"], true);
        assert_eq!(callback["stable_timer"], true);
        let history = client.export_workflow_history(&id, &run).await.unwrap();
        validate_export(&history, &id, &run, &kind);
        let started = one(&history, "WorkflowStarted");
        let fingerprint = started["payload"]["workflow_definition_fingerprint"]
            .as_str()
            .filter(|value| !value.is_empty())
            .expect("recorded workflow definition fingerprint");
        assert_eq!(
            events(&history, "WorkflowFailed").len(),
            usize::from(phase < 2)
        );
        assert_eq!(
            events(&history, "WorkflowCompleted").len(),
            usize::from(phase == 2)
        );
        let scheduled = one(&history, "TimerScheduled");
        let fired = one(&history, "TimerFired");
        let timer_id = fired["payload"]["timer_id"].as_str().unwrap().to_owned();
        assert!(!timer_id.is_empty() && timer_ids.insert(timer_id.clone()));
        assert_eq!(scheduled["payload"]["timer_id"], timer_id);
        assert_eq!(scheduled["payload"]["delay_seconds"], TIMER_SECONDS);
        let first_activity = events(&history, "ActivityCompleted")
            .into_iter()
            .find(|event| event["payload"]["sequence"] == 1)
            .unwrap();
        if phase == 0 {
            original_fingerprint = Some(fingerprint.to_owned());
            let timer = instant(&fired["recorded_at"]);
            assert_ne!(
                timer.timestamp_subsec_nanos(),
                0,
                "real nonzero-fraction TimerFired required"
            );
            original_timer = Some(timer);
            original_activity = Some(instant(&first_activity["recorded_at"]));
            original_start = Some(instant(&history["workflow"]["started_at"]));
            assert!(
                callback["entry"].is_null(),
                "ordinary Rust entry clock is not advertised"
            );
        } else {
            assert_eq!(Some(fingerprint), original_fingerprint.as_deref());
            let previous = &runs[phase - 1]["history"];
            let start = one(&history, "WorkflowStarted");
            assert_eq!(start["payload"]["recovery_kind"], "redrive");
            assert_eq!(start["payload"]["continued_from_run_id"], previous_run);
            assert_eq!(start["payload"]["resume_step_sequence"], 3);
            assert_eq!(
                instant(&start["payload"]["replayed_started_at"]),
                original_start.unwrap()
            );
            assert_eq!(instant(&callback["entry"]), original_start.unwrap());
            assert_eq!(fired["payload"]["reused_from_run_id"], previous_run);
            assert_eq!(
                instant(&fired["payload"]["reused_recorded_at"]),
                original_timer.unwrap()
            );
            assert_eq!(
                first_activity["payload"]["reused_from_run_id"],
                previous_run
            );
            assert_eq!(
                instant(&first_activity["payload"]["reused_recorded_at"]),
                original_activity.unwrap()
            );
            let prior_activity = events(previous, "ActivityCompleted")
                .into_iter()
                .find(|event| event["payload"]["sequence"] == 1)
                .unwrap();
            assert_eq!(
                first_activity["payload"]["result"],
                prior_activity["payload"]["result"]
            );
            assert_eq!(
                first_activity["payload"]["reused_activity_execution_id"],
                prior_activity["payload"]["activity_execution_id"]
            );
            assert_ne!(
                first_activity["payload"]["activity_execution_id"],
                prior_activity["payload"]["activity_execution_id"]
            );
            let prior_fired = one(previous, "TimerFired");
            for key in ["delay_seconds", "fire_at", "fired_at"] {
                assert_eq!(fired["payload"][key], prior_fired["payload"][key]);
            }
            for (event_kind, current) in [("TimerScheduled", scheduled), ("TimerFired", fired)] {
                assert_reused_timer_payload(
                    &one(&runs[0]["history"], event_kind)["payload"],
                    &current["payload"],
                );
            }
            zero_timer_tasks_and_waits(&history);
            assert!(
                elapsed < Duration::from_secs(TIMER_SECONDS),
                "redrive must not repeat the authored timer delay"
            );
            // Earlier terminal histories must remain byte-for-byte unchanged.
            for earlier in &runs {
                let earlier_run = earlier["run_id"].as_str().unwrap();
                let reread = client
                    .export_workflow_history(&id, earlier_run)
                    .await
                    .unwrap();
                assert_eq!(
                    reread["history_events"],
                    earlier["history"]["history_events"]
                );
            }
        }
        assert_eq!(instant(&callback["activity"]), original_activity.unwrap());
        assert_eq!(instant(&callback["timer"]), original_timer.unwrap());
        runs.push(json!({"run_id":run,"phase":phase,"worker":observed,
            "elapsed_ms":elapsed.as_millis(),"history":history}));
    }
    assert_eq!(pids.len(), 3);
    assert_eq!(timer_ids.len(), 3);
    assert_eq!(registration_refusals.len(), 2);
    client
        .deregister_worker_registration(&admission_id)
        .await
        .unwrap();
    let final_run = client.describe_workflow_run(&id, &run).await.unwrap();
    assert_eq!(final_run.status.as_deref(), Some("completed"));
    assert_eq!(
        final_run.output,
        Some(json!({"prefix":"original-prefix","result":"recovered"}))
    );
    // The reviewed caller retains this exact bundle and performs its separate
    // consistent read-only physical timer-table counts before owned cleanup.
    println!(
        "CLOCK_COHORT {}",
        json!({"workflow_id":id,"workflow_type":kind,
        "executable":image,"runs":runs,"registration_refusals":registration_refusals,
        "physical_timer_counts":"caller-required"})
    );
}

fn image_command() -> Command {
    assert!(
        cfg!(target_os = "linux"),
        "the installed qualifier requires Linux parent-image execution"
    );
    // In the forked child this magic link still names the running parent image,
    // even if its former pathname has been renamed or replaced.
    let mut command = Command::new("/proc/self/exe");
    command.args(CHILD_ARGUMENTS).env_clear();
    command
}

fn executable_identity() -> Value {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::MetadataExt;
        let mut file = std::fs::File::open("/proc/self/exe").unwrap();
        let metadata = file.metadata().unwrap();
        let mut digest = Sha256::new();
        let mut buffer = [0_u8; 65_536];
        let mut length = 0_u64;
        loop {
            let read = file.read(&mut buffer).unwrap();
            if read == 0 {
                break;
            }
            length += read as u64;
            digest.update(&buffer[..read]);
        }
        assert_eq!(length, metadata.len());
        json!({"sha256":format!("{:x}",digest.finalize()),"length":length,"device":metadata.dev(),"inode":metadata.ino()})
    }
    #[cfg(not(target_os = "linux"))]
    {
        panic!("the installed qualifier requires Linux parent-image execution");
    }
}

// Own only the process returned by spawn. A failed assertion or timeout cannot
// leave that worker running after the acceptance parent returns.
struct OwnedChild(Child);
impl Drop for OwnedChild {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

async fn run_child(mut command: Command) -> (u32, String) {
    let mut child = OwnedChild(
        command
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let expected_pid = child.0.id();
    let deadline = Instant::now() + Duration::from_secs(30);
    let status = loop {
        if let Some(status) = child.0.try_wait().unwrap() {
            break status;
        }
        assert!(
            Instant::now() < deadline,
            "fresh clock worker exceeded its declared bound"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    let output = bounded_output(child.0.stdout.take().unwrap(), "stdout");
    let errors = bounded_output(child.0.stderr.take().unwrap(), "stderr");
    assert!(
        status.success(),
        "owned clock fixture process exited {status}; stderr: {}",
        String::from_utf8_lossy(&errors)
    );
    let output = String::from_utf8(output).unwrap();
    (expected_pid, output)
}

fn bounded_output(output: impl Read, stream: &str) -> Vec<u8> {
    let mut bytes = Vec::new();
    output
        .take(OUTPUT_LIMIT + 1)
        .read_to_end(&mut bytes)
        .unwrap();
    assert!(
        bytes.len() as u64 <= OUTPUT_LIMIT,
        "worker {stream} exceeded the fixture bound"
    );
    bytes
}

fn parse_report(output: &str) -> Value {
    // libtest may place its progress prefix before the captured result line.
    let records: Vec<Value> = output
        .lines()
        .filter_map(|line| line.find(OUTPUT).map(|at| &line[at + OUTPUT.len()..]))
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(records.len(), 1, "one process result is required");
    records.into_iter().next().unwrap()
}

#[cfg(target_os = "linux")]
async fn executable_control(mode: &str) {
    use std::os::unix::fs::PermissionsExt;
    if mode == "diagnostic-failure" {
        eprintln!("SYNTHETIC_CLOCK_CHILD_FAILURE");
        std::process::exit(23);
    }
    if mode == "identity" {
        let image = executable_identity();
        assert_eq!(
            image,
            serde_json::from_str::<Value>(&std::env::var(IMAGE).unwrap()).unwrap()
        );
        println!(
            "{OUTPUT}{}",
            json!({"pid":std::process::id(),"executable":image})
        );
        return;
    }
    assert_eq!(mode, "replace-private-copy");
    let path = std::env::current_exe().unwrap();
    assert_eq!(path.file_name().unwrap(), "private-copy");
    let root = path.parent().unwrap();
    assert!(root
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .starts_with("workflow-clock-executable-"));
    assert_eq!(root.metadata().unwrap().permissions().mode() & 0o777, 0o700);
    let image = executable_identity();
    // Capture both commands before replacing the name. The predecessor reopens
    // the captured pathname; the actual image command must execute this image.
    let mut predecessor = Command::new(&path);
    predecessor.env_clear();
    let mut candidate = image_command();
    candidate
        .env(CONTROL, "identity")
        .env(IMAGE, image.to_string());
    std::fs::rename(&path, root.join("running-image")).unwrap();
    std::fs::write(&path, b"#!/bin/sh\nprintf 'PATH_REPLACED\\n'\n").unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
    let (_, old_output) = run_child(predecessor).await;
    assert_eq!(old_output.trim(), "PATH_REPLACED");
    let (pid, output) = run_child(candidate).await;
    let record = parse_report(&output);
    assert_eq!(record["pid"], pid);
    assert_eq!(record["executable"], image);
    println!("EXECUTABLE_CONTROL_PASS");
}

#[cfg(target_os = "linux")]
#[tokio::test]
#[should_panic(expected = "SYNTHETIC_CLOCK_CHILD_FAILURE")]
async fn failed_child_retains_bounded_stderr_diagnostics() {
    let mut command = image_command();
    command.env(CONTROL, "diagnostic-failure");
    run_child(command).await;
}

#[test]
#[should_panic(expected = "worker stderr exceeded the fixture bound")]
fn oversized_worker_stderr_is_refused() {
    bounded_output(std::io::repeat(b'x'), "stderr");
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn replaced_executable_path_cannot_change_the_child_image() {
    use std::{
        io::Write,
        os::unix::fs::{DirBuilderExt, OpenOptionsExt},
    };
    struct Directory(std::path::PathBuf);
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let root = Directory(std::env::temp_dir().join(format!(
        "workflow-clock-executable-{}",
        durable_workflow::Uuid::new_v4()
    )));
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(&root.0)
        .unwrap();
    let path = root.0.join("private-copy");
    let mut source = std::fs::File::open("/proc/self/exe").unwrap();
    let mut snapshot = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o700)
        .open(&path)
        .unwrap();
    std::io::copy(&mut source, &mut snapshot).unwrap();
    snapshot.flush().unwrap();
    drop(snapshot);
    let mut control = Command::new(path);
    control
        .args(CHILD_ARGUMENTS)
        .env_clear()
        .env(CONTROL, "replace-private-copy");
    let (_, output) = run_child(control).await;
    assert!(output
        .lines()
        .any(|line| line.contains("EXECUTABLE_CONTROL_PASS")));
    std::fs::remove_dir_all(&root.0).unwrap();
    assert!(!root.0.exists());
}
