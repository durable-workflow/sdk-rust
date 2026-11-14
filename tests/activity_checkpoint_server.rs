//! Source-only qualifier until an exact package, producer and caller are reviewed.
//! This test starts only its owned Worker children, never a Server.
use durable_workflow::{
    json, ActivityError, ActivityOptions, ActivityRetryPolicy, Client, Error, Value, Worker,
    WorkflowResultOptions,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{
    io::{BufRead, Read, Write},
    process::{Child, Command, Stdio},
    sync::{mpsc, Arc, Mutex},
    thread,
    time::{Duration, Instant},
};

const CHILD: &str = "DURABLE_WORKFLOW_CHECKPOINT_CHILD";
const QUEUE: &str = "DURABLE_WORKFLOW_CHECKPOINT_QUEUE";
const PHASE: &str = "DURABLE_WORKFLOW_CHECKPOINT_PHASE";
const CASE: &str = "DURABLE_WORKFLOW_CHECKPOINT_CASE";
const IMAGE: &str = "DURABLE_WORKFLOW_CHECKPOINT_IMAGE";
const REPORT: &str = "CHECKPOINT_WORKER ";
const STREAM_LIMIT: usize = 32 * 1024;

#[derive(Debug, Deserialize, PartialEq)]
struct Progress {
    cursor: u64,
    checked: bool,
    label: String,
    optional: Option<String>,
}

fn details() -> Value {
    json!({"cursor":7,"checked":true,"label":"acknowledged","optional":null})
}

fn executable_identity() -> Value {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::MetadataExt;
        let mut file = std::fs::File::open("/proc/self/exe").unwrap();
        let metadata = file.metadata().unwrap();
        let mut digest = Sha256::new();
        let mut length = 0_u64;
        let mut buffer = [0; 65536];
        loop {
            let count = file.read(&mut buffer).unwrap();
            if count == 0 {
                break;
            }
            length += count as u64;
            digest.update(&buffer[..count]);
        }
        assert_eq!(length, metadata.len());
        json!({"sha256":format!("{:x}",digest.finalize()),"length":length,
            "device":metadata.dev(),"inode":metadata.ino()})
    }
    #[cfg(not(target_os = "linux"))]
    panic!("the isolated qualifier requires Linux parent-image execution");
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

fn emit(value: Value) {
    let text = value.to_string();
    assert!(text.len() <= 8192);
    println!("{REPORT}{text}");
    std::io::stdout().flush().unwrap();
}

fn worker(
    client: Client,
    queue: &str,
    phase: u64,
    case: &str,
    observations: Arc<Mutex<Vec<Value>>>,
) -> Worker {
    let mut worker = Worker::new(client, queue)
        .worker_id(format!("{queue}-{phase}"))
        .poll_timeout(Duration::from_millis(250));
    worker.register_workflow(queue, |ctx, _| async move {
        ctx.activity_with_options(
            "resume-batch",
            ActivityOptions::new()
                .retry_policy(ActivityRetryPolicy::new(3).backoff_intervals([Duration::ZERO; 2]))
                .heartbeat_timeout(Duration::from_secs(5))
                .start_to_close_timeout(Duration::from_secs(30))
                .schedule_to_close_timeout(Duration::from_secs(90)),
            json!([]),
        )
        .await
    });
    let checkpoint = case == "acknowledged";
    worker.register_activity("resume-batch", move |ctx, _| {
        let observations = observations.clone();
        async move {
            assert_eq!(ctx.attempt_number, phase);
            assert_eq!(ctx.heartbeat_resume_version(), Some(1));
            let received = ctx.last_heartbeat_checkpoint()?.cloned();
            if phase == 1 || !checkpoint {
                assert!(received.is_none());
            } else {
                let received = received.as_ref().unwrap();
                assert_eq!(received.details(), &details());
                assert_eq!(received.contract_version(), 1);
                assert_eq!(received.details_as::<Progress>().unwrap(), Progress {
                    cursor:7,checked:true,label:"acknowledged".into(),optional:None,
                });
            }
            let mut observation = json!({"event":"claimed","pid":std::process::id(),
                "phase":phase,"execution":ctx.activity_execution_id(),"attempt":ctx.activity_attempt_id,
                "task":ctx.task_id,"owner":ctx.lease_owner,"workflow":ctx.workflow_id(),"run":ctx.run_id(),
                "details":received.as_ref().map(|v|v.details()),
                "at":received.as_ref().map(|v|v.acknowledged_at().to_rfc3339()),
                "retry_policy":ctx.retry_info().policy()});
            if phase == 1 && checkpoint {
                let ack = ctx.heartbeat(details()).await?;
                assert!(ack.heartbeat_recorded && !ack.should_stop());
                observation["acknowledged_at"] = json!(ack.last_heartbeat_at.unwrap());
                assert!(ctx.last_heartbeat_checkpoint()?.is_none());
            }
            observations.lock().unwrap().push(observation.clone());
            emit(observation);
            if phase == 1 {
                // The parent kills this process only after the flushed claim/ack.
                std::future::pending::<()>().await;
            }
            if phase == 2 {
                // Keep attempt 2 live while the parent tries attempt 1's stale write.
                let mut command = Vec::new();
                std::io::stdin().lock().take(16).read_until(b'\n', &mut command).unwrap();
                assert_eq!(command, b"retry\n");
                assert_eq!(ctx.last_heartbeat_checkpoint()?.cloned(), received);
                return Err(ActivityError::new("CheckpointProbe", "one declared retry").into());
            }
            assert_eq!(phase, 3);
            let ack = ctx.heartbeat(json!({"cursor":8})).await?;
            assert!(ack.heartbeat_recorded && !ack.should_stop());
            assert_eq!(ctx.last_heartbeat_checkpoint()?.cloned(), received);
            Ok(json!({"resumed":received.as_ref().map(|v|v.details()),"completed_through":8}))
        }
    });
    worker
}

#[tokio::test]
#[ignore = "entrypoint only for the isolated checkpoint qualifier parent"]
async fn checkpoint_worker_entrypoint() {
    assert_eq!(std::env::var(CHILD).as_deref(), Ok("1"));
    let image = executable_identity();
    assert_eq!(
        image,
        serde_json::from_str::<Value>(&std::env::var(IMAGE).unwrap()).unwrap()
    );
    // No client or credential access precedes the held running-image identity check.
    let queue = std::env::var(QUEUE).unwrap();
    let phase: u64 = std::env::var(PHASE).unwrap().parse().unwrap();
    let case = std::env::var(CASE).unwrap();
    assert!(matches!(phase, 1..=3) && matches!(case.as_str(), "acknowledged" | "empty"));
    let client = client();
    let observations = Arc::new(Mutex::new(Vec::new()));
    let worker = worker(client.clone(), &queue, phase, &case, observations.clone());
    worker.register().await.unwrap();
    emit(json!({"event":"registered","pid":std::process::id(),"image":image,"phase":phase}));
    tokio::time::timeout(Duration::from_secs(40), async {
        for _ in 0..100 {
            worker.run_once().await.unwrap();
            let callbacks = observations.lock().unwrap().len();
            if callbacks > 0 {
                if phase == 2
                    || client
                        .describe_workflow(&queue)
                        .await
                        .unwrap()
                        .is_terminal()
                {
                    emit(
                        json!({"event":"finished","pid":std::process::id(),"phase":phase,
                        "callbacks":callbacks}),
                    );
                    return;
                }
            }
        }
        panic!("bounded worker polling did not finish");
    })
    .await
    .expect("checkpoint child exceeded its original bound");
    client
        .deregister_worker_registration(&format!("{queue}-{phase}"))
        .await
        .unwrap();
}

struct OwnedChild {
    child: Child,
    lines: mpsc::Receiver<Result<Vec<u8>, &'static str>>,
    readers: Vec<thread::JoinHandle<()>>,
    registered: bool,
    image: Value,
}
impl Drop for OwnedChild {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
        // Reaping closes both pipes; readers never block on the channel.
        for reader in self.readers.drain(..) {
            let _ = reader.join();
        }
    }
}

fn read_stream(mut stream: impl Read, events: mpsc::Sender<Result<Vec<u8>, &'static str>>) {
    let mut total = 0;
    let mut line = Vec::new();
    let mut buffer = [0; 4096];
    loop {
        let count = match stream.read(&mut buffer) {
            Ok(0) => break,
            Ok(count) => count,
            Err(_) => {
                let _ = events.send(Err("owned child stream read failed"));
                return;
            }
        };
        total += count;
        if total > STREAM_LIMIT {
            let _ = events.send(Err("owned child stream exceeded its byte bound"));
            return;
        }
        for byte in &buffer[..count] {
            line.push(*byte);
            if *byte == b'\n' && events.send(Ok(std::mem::take(&mut line))).is_err() {
                return;
            }
        }
    }
    if !line.is_empty() {
        let _ = events.send(Ok(line));
    }
}

impl OwnedChild {
    fn spawn(queue: &str, phase: u64, case: &str, image: &Value) -> Self {
        assert!(cfg!(target_os = "linux"));
        let mut command = Command::new("/proc/self/exe");
        command
            .args([
                "--ignored",
                "--exact",
                "checkpoint_worker_entrypoint",
                "--nocapture",
                "--test-threads=1",
                "--quiet",
            ])
            .env_clear()
            .env(CHILD, "1")
            .env(QUEUE, queue)
            .env(PHASE, phase.to_string())
            .env(CASE, case)
            .env(IMAGE, image.to_string());
        for name in [
            "DURABLE_WORKFLOW_LOCAL_ISOLATED",
            "DURABLE_WORKFLOW_SERVER_URL",
            "DURABLE_WORKFLOW_AUTH_TOKEN",
        ] {
            command.env(name, std::env::var(name).unwrap());
        }
        let mut child = command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        // An unbounded channel stores at most the two readers' fixed byte caps.
        let (tx, lines) = mpsc::channel();
        let stdout = child.stdout.take().unwrap();
        let stderr = child.stderr.take().unwrap();
        let errors = tx.clone();
        let readers = vec![
            thread::spawn(move || read_stream(stdout, tx)),
            thread::spawn(move || read_stream(stderr, errors)),
        ];
        Self {
            child,
            lines,
            readers,
            registered: false,
            image: image.clone(),
        }
    }
    async fn event(&mut self, name: &str) -> Value {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            match self.lines.try_recv() {
                Ok(Ok(line)) => {
                    let text = String::from_utf8(line).unwrap();
                    if let Some(at) = text.find(REPORT) {
                        let record: Value =
                            serde_json::from_str(text[at + REPORT.len()..].trim()).unwrap();
                        assert_eq!(record["pid"], self.child.id());
                        if record["event"] == "registered" {
                            assert_eq!(record["image"], self.image);
                            self.registered = true;
                        }
                        assert!(self.registered, "claim before bound registration");
                        if record["event"] == name {
                            return record;
                        }
                    }
                }
                Ok(Err(error)) => panic!("{error}"),
                Err(mpsc::TryRecvError::Disconnected) => {
                    panic!("child exited before required event")
                }
                Err(mpsc::TryRecvError::Empty) => {
                    assert!(Instant::now() < deadline, "owned child event timed out");
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        }
    }
    async fn finish(&mut self) {
        let record = self.event("finished").await;
        assert_eq!(record["callbacks"], 1);
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                assert!(status.success());
                for reader in self.readers.drain(..) {
                    reader.join().unwrap();
                }
                for event in self.lines.try_iter() {
                    event.expect("bounded child output");
                }
                return;
            }
            assert!(Instant::now() < deadline, "owned child did not exit");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

fn instant(value: &Value) -> chrono::DateTime<chrono::FixedOffset> {
    chrono::DateTime::parse_from_rfc3339(value.as_str().unwrap()).unwrap()
}

fn assert_stale_refusal(
    result: durable_workflow::Result<durable_workflow::ActivityHeartbeatResponse>,
    original: &Value,
) {
    match result {
        Err(Error::ActivityTaskRejected(rejection)) => {
            assert_eq!(rejection.status, 409);
            assert!(matches!(
                rejection.reason.as_str(),
                "stale_attempt"
                    | "attempt_closed"
                    | "activity_not_running"
                    | "task_not_leased"
                    | "attempt_not_found"
            ));
            assert_eq!(rejection.task_id, original["task"].as_str().unwrap());
            assert_eq!(
                rejection.activity_attempt_id,
                original["attempt"].as_str().unwrap()
            );
        }
        Ok(response) => {
            assert!(!response.heartbeat_recorded && response.can_continue == Some(false));
            assert!(matches!(
                response.reason.as_deref(),
                Some("stale_attempt" | "attempt_closed" | "activity_not_running")
            ));
        }
        other => panic!("expected a fenced stale-write refusal, got {other:?}"),
    }
}

#[test]
fn child_stream_reader_refuses_oversized_output_during_reads() {
    let (send, received) = mpsc::channel();
    read_stream(std::io::Cursor::new(vec![b'x'; STREAM_LIMIT + 1]), send);
    assert_eq!(
        received.into_iter().collect::<Vec<_>>(),
        vec![Err("owned child stream exceeded its byte bound")]
    );
    let (send, received) = mpsc::channel();
    read_stream(std::io::Cursor::new(b"one\ntwo"), send);
    assert_eq!(
        received.into_iter().collect::<Vec<_>>(),
        vec![Ok(b"one\n".to_vec()), Ok(b"two".to_vec())]
    );
}

#[test]
fn stale_write_observer_refuses_transport_and_unrelated_errors() {
    let original = json!({"task":"owned-task","attempt":"old-attempt"});
    let accepted = || {
        serde_json::from_value(
            json!({"heartbeat_recorded":false,"can_continue":false,"reason":"stale_attempt"}),
        )
        .unwrap()
    };
    assert_stale_refusal(Ok(accepted()), &original);
    assert!(std::panic::catch_unwind(|| assert_stale_refusal(
        Err(Error::Codec("not a producer refusal".into())),
        &original
    ))
    .is_err());
    let wrong = serde_json::from_value(
        json!({"heartbeat_recorded":true,"can_continue":false,"reason":"stale_attempt"}),
    )
    .unwrap();
    assert!(std::panic::catch_unwind(|| assert_stale_refusal(Ok(wrong), &original)).is_err());
}

#[tokio::test]
#[ignore = "requires reviewed exact published SDK/Server/caller; starts no Server"]
async fn published_cold_replacement_preserves_acknowledged_activity_checkpoint() {
    let client = client();
    let cluster = client.cluster_info().await.unwrap();
    assert_eq!(cluster["implementation"], "rust");
    assert_eq!(
        cluster["version"],
        std::env::var("DURABLE_WORKFLOW_EXPECT_SERVER_VERSION").unwrap()
    );
    let image = executable_identity();
    for case in ["acknowledged", "empty"] {
        tokio::time::timeout(Duration::from_secs(100),async {
            let queue=format!("rust-checkpoint-{}",durable_workflow::Uuid::new_v4());
            let registration=worker(client.clone(),&queue,0,case,Arc::new(Mutex::new(Vec::new())));
            registration.register().await.unwrap();
            let handle=client.start_workflow(&queue,&queue,&queue,json!([])).await.unwrap();
            let run=handle.run_id.as_deref().unwrap().to_string();
            let mut first=OwnedChild::spawn(&queue,1,case,&image);
            let original=first.event("claimed").await;
            assert_eq!(original["workflow"],queue);assert_eq!(original["run"],run);
            tokio::task::spawn_blocking(move || {
                first.child.kill().unwrap();
                let killed=first.child.wait().unwrap();
                assert!(!killed.success());
            }).await.unwrap();
            let mut second=OwnedChild::spawn(&queue,2,case,&image);
            let replacement=second.event("claimed").await;
            assert_ne!(original["pid"],replacement["pid"]);
            for key in ["workflow","run","execution","retry_policy"] {assert_eq!(original[key],replacement[key]);}
            assert!(original["execution"].as_str().is_some_and(|s|!s.is_empty()));
            assert_ne!(original["attempt"],replacement["attempt"]);
            assert_eq!(original["retry_policy"]["max_attempts"],3);
            assert_eq!(original["retry_policy"]["backoff_seconds"],json!([0,0]));
            assert_eq!(original["retry_policy"]["heartbeat_timeout"],5);
            assert_eq!(original["retry_policy"]["start_to_close_timeout"],30);
            assert_eq!(original["retry_policy"]["schedule_to_close_timeout"],90);
            let prefix=client.export_workflow_history(&queue,&run).await.unwrap();
            // A dead attempt cannot replace the checkpoint while attempt 2 owns it.
            let stale=client.heartbeat_activity_task(original["task"].as_str().unwrap(),
                original["attempt"].as_str().unwrap(),original["owner"].as_str().unwrap(),json!({"cursor":99})).await;
            assert_stale_refusal(stale,&original);
            second.child.stdin.as_mut().unwrap().write_all(b"retry\n").unwrap();second.finish().await;
            let mut third=OwnedChild::spawn(&queue,3,case,&image);
            let retained=third.event("claimed").await;
            for key in ["workflow","run","execution","retry_policy","details","at"] {assert_eq!(retained[key],replacement[key]);}
            assert_ne!(retained["pid"],replacement["pid"]);assert_ne!(retained["pid"],original["pid"]);
            assert_ne!(retained["attempt"],replacement["attempt"]);assert_ne!(retained["attempt"],original["attempt"]);
            if case=="acknowledged" {
                assert_eq!(replacement["details"],details());
                assert_eq!(instant(&replacement["at"]),instant(&original["acknowledged_at"]));
            } else {assert!(replacement["details"].is_null() && replacement["at"].is_null());}
            third.finish().await;
            assert_eq!(handle.describe_selected_run().await.unwrap().status.as_deref(),Some("completed"));
            assert_eq!(handle.result_selected_run(WorkflowResultOptions::default()).await.unwrap(),
                json!({"resumed":if case=="acknowledged" {details()}else{Value::Null},"completed_through":8}));
            let history=client.export_workflow_history(&queue,&run).await.unwrap();
            assert_eq!(history["workflow"]["instance_id"],queue);assert_eq!(history["workflow"]["run_id"],run);
            assert_eq!(history["workflow"]["details_state"],"retained");
            let events=history["history_events"].as_array().unwrap();
            let prefix=prefix["history_events"].as_array().unwrap();assert_eq!(&events[..prefix.len()],prefix);
            let count=|kind:&str| events.iter().filter(|e|e["type"]==kind).count();
            assert_eq!(count("ActivityScheduled"),1);assert_eq!(count("ActivityStarted"),3);
            assert_eq!(count("ActivityRetryScheduled"),2);assert_eq!(count("ActivityCompleted"),1);
            assert_eq!(count("WorkflowCompleted"),1);
            let recorded:Vec<_>=events.iter().filter(|e|e["type"]=="ActivityHeartbeatRecorded").collect();
            assert!(recorded.iter().all(|e|e["payload"]["details"]["cursor"]!=99));
            if case=="acknowledged" {assert!(recorded.iter().any(|e|e["payload"]["details"]==details()));}
            client.deregister_worker_registration(&format!("{queue}-0")).await.unwrap();
            client.deregister_worker_registration(&format!("{queue}-1")).await.unwrap();
            let report=json!({"case":case,"original":original,"replacement":replacement,"retained":retained,
                "worker1_killed":true,"stale_write_refused":true,"history":history});
            let report=report.to_string();assert!(report.len()<=512*1024);println!("ACTIVITY_CHECKPOINT {report}");
        }).await.expect("checkpoint case exceeded its original budget");
    }
}
