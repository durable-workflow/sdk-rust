//! Copy into an exact-package consumer for the published logging acceptance.
//! Each activation runs in a new process; this test never starts a Server.
use durable_workflow::{
    json, Client, Error, Value, Worker, WorkerRetryPolicy, WorkflowResultOptions,
};
#[cfg(target_os = "linux")]
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::Read,
    process::{Child, Command, Stdio},
    sync::Mutex,
    time::{Duration, Instant},
};
use tracing::instrument::WithSubscriber;

const CHILD: &str = "DURABLE_WORKFLOW_LOGGING_CHILD";
const QUEUE: &str = "DURABLE_WORKFLOW_LOGGING_QUEUE";
const TYPE: &str = "DURABLE_WORKFLOW_LOGGING_TYPE";
const PHASE: &str = "DURABLE_WORKFLOW_LOGGING_PHASE";
const OUTPUT: &str = "WORKFLOW_LOGGING_RESULT ";
const OUTPUT_LIMIT: u64 = 65_536;
const IMAGE: &str = "DURABLE_WORKFLOW_LOGGING_IMAGE";
#[cfg(target_os = "linux")]
const CONTROL: &str = "DURABLE_WORKFLOW_LOGGING_EXECUTABLE_CONTROL";

const CHILD_ARGUMENTS: [&str; 6] = [
    "--ignored",
    "--exact",
    "published_logging_worker_entrypoint",
    "--nocapture",
    "--test-threads=1",
    "--quiet",
];

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

fn worker(client: Client, queue: &str, kind: &str, phase: &str) -> Worker {
    let mut worker = Worker::new(client, queue)
        .worker_id(format!("{queue}-{phase}"))
        .poll_timeout(Duration::from_secs(1))
        .retry_policy(WorkerRetryPolicy {
            max_retries: 1,
            initial_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_millis(10),
        });
    worker.register_workflow(kind, |ctx, _| async move {
        for step in 0..3 {
            ctx.logger().info(&format!("step-{step}"));
            let signal = ctx.wait_signal("advance").await?;
            if signal != vec![json!(step + 1)] {
                return Err(Error::WorkerLoop(
                    "logging fixture received the wrong step".into(),
                ));
            }
        }
        ctx.logger().info("complete");
        Ok(json!({"steps":3}))
    });
    worker.declare_workflow_signals(kind, &["advance"]).unwrap();
    worker
}

#[derive(Default)]
struct Capture(Mutex<Vec<Value>>);
struct Fields(BTreeMap<String, String>);
impl tracing::field::Visit for Fields {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.0.insert(field.name().into(), value.into());
    }
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.0.insert(field.name().into(), format!("{value:?}"));
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
        let mut fields = Fields(BTreeMap::new());
        event.record(&mut fields);
        assert_eq!(fields.0.len(), 4);
        assert_eq!(*event.metadata().level(), tracing::Level::INFO);
        self.0.lock().unwrap().push(json!(fields.0));
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}

#[tokio::test]
#[ignore = "entrypoint invoked only by the isolated logging acceptance parent"]
async fn published_logging_worker_entrypoint() {
    #[cfg(target_os = "linux")]
    if let Ok(mode) = std::env::var(CONTROL) {
        executable_control(&mode).await;
        return;
    }
    assert_eq!(std::env::var(CHILD).as_deref(), Ok("1"));
    let image = executable_identity();
    assert_eq!(
        image,
        serde_json::from_str::<Value>(&std::env::var(IMAGE).unwrap()).unwrap(),
        "child image must equal the captured parent before constructing a client"
    );
    let queue = std::env::var(QUEUE).unwrap();
    let kind = std::env::var(TYPE).unwrap();
    let phase = std::env::var(PHASE).unwrap();
    let capture = std::sync::Arc::new(Capture::default());
    let worker = worker(client(), &queue, &kind, &phase);
    worker.register().await.unwrap();
    let handled = worker
        .run_once()
        .with_subscriber(capture.clone())
        .await
        .unwrap();
    assert_eq!(
        handled, 1,
        "each fresh worker must handle exactly one committed activation"
    );
    println!(
        "{OUTPUT}{}",
        json!({"pid":std::process::id(),"executable":image,"handled":handled,"logs":*capture.0.lock().unwrap()})
    );
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

async fn activation(queue: &str, kind: &str, phase: usize, image: &Value) -> Value {
    let mut command = image_command();
    command
        .env(CHILD, "1")
        .env(QUEUE, queue)
        .env(TYPE, kind)
        .env(PHASE, phase.to_string())
        .env(IMAGE, image.to_string());
    for name in [
        "DURABLE_WORKFLOW_LOCAL_ISOLATED",
        "DURABLE_WORKFLOW_SERVER_URL",
        "DURABLE_WORKFLOW_AUTH_TOKEN",
    ] {
        command.env(name, std::env::var(name).unwrap());
    }
    let (expected_pid, output) = run_child(command).await;
    let record = parse_report(&output);
    assert_eq!(record["pid"], expected_pid);
    assert_eq!(record["executable"], *image);
    assert_eq!(record["handled"], 1);
    record
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
            "fresh logging worker exceeded its declared bound"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    let output = bounded_output(child.0.stdout.take().unwrap(), "stdout");
    let errors = bounded_output(child.0.stderr.take().unwrap(), "stderr");
    assert!(
        status.success(),
        "owned logging fixture process exited {status}; stderr: {}",
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
        eprintln!("SYNTHETIC_LOGGING_CHILD_FAILURE");
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
        .starts_with("workflow-logging-executable-"));
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
#[should_panic(expected = "SYNTHETIC_LOGGING_CHILD_FAILURE")]
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
        "workflow-logging-executable-{}",
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

fn event_count(history: &Value, kind: &str) -> usize {
    history["history_events"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|event| event["type"] == kind)
        .count()
}

#[tokio::test]
#[ignore = "requires an isolated published Server and exact package provenance from the caller"]
async fn published_logging_emits_once_per_committed_step_across_cold_worker_processes() {
    let image = executable_identity();
    let client = client();
    let expected_version = std::env::var("DURABLE_WORKFLOW_EXPECT_SERVER_VERSION").unwrap();
    assert_eq!(
        client.cluster_info().await.unwrap()["version"],
        expected_version
    );
    let id = format!("logger-{}", durable_workflow::Uuid::new_v4());
    let kind = format!("qualification.logging.{id}");
    worker(client.clone(), &id, &kind, "register")
        .register()
        .await
        .unwrap();
    let handle = client
        .start_workflow(&kind, &id, &id, json!([]))
        .await
        .unwrap();
    let run = handle
        .run_id
        .as_deref()
        .expect("the fixture must retain its original run identity");
    let mut pids = BTreeSet::new();
    let mut messages = Vec::new();
    for phase in 0..=3 {
        if phase > 0 {
            handle.signal("advance", json!([phase])).await.unwrap();
        }
        let observation = activation(&id, &kind, phase, &image).await;
        assert!(
            pids.insert(observation["pid"].as_u64().unwrap()),
            "each activation must use a new process"
        );
        let logs = observation["logs"].as_array().unwrap();
        assert_eq!(
            logs.len(),
            1,
            "the new process must suppress the entire committed prefix"
        );
        assert_eq!(logs[0]["namespace"], "default");
        assert_eq!(logs[0]["workflow_instance_id"], id);
        assert_eq!(logs[0]["workflow_run_id"], run);
        let expected = if phase == 3 {
            "complete".to_string()
        } else {
            format!("step-{phase}")
        };
        assert_eq!(logs[0]["message"], expected);
        messages.push(expected);
        // Read the actual committed history before advancing the next step.
        let history = client.export_workflow_history(&id, run).await.unwrap();
        assert_eq!(
            event_count(&history, "SignalWaitOpened"),
            (phase + 1).min(3)
        );
        assert_eq!(event_count(&history, "SignalApplied"), phase);
        assert_eq!(
            event_count(&history, "WorkflowCompleted"),
            usize::from(phase == 3)
        );
        assert_eq!(event_count(&history, "WorkflowFailed"), 0);
    }
    let result = handle
        .result(WorkflowResultOptions {
            timeout: Duration::from_secs(5),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(result, json!({"steps":3}));
    assert_eq!(messages, ["step-0", "step-1", "step-2", "complete"]);
    assert_eq!(pids.len(), 4);
    println!("PASS: four cold worker processes; one observation per committed region; original run identity; three applied signals; exact result");
}
