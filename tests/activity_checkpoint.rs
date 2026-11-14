//! Poll-to-handler controls for acknowledged checkpoint snapshots, not a real Server.
use durable_workflow::{
    json, testing::WorkflowEnvironment, ActivityHeartbeatResumeUnavailable, Client, Value, Worker,
};
use serde::Deserialize;
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

const AT: &str = "2026-11-14T09:00:00.123456789Z";
fn claim(attempt: u64, execution: &str, details: Value, at: Value) -> Value {
    json!({"task":{"task_id":format!("task-{execution}-{attempt}"),"workflow_id":"workflow",
        "run_id":"run","namespace":"default","activity_execution_id":execution,
        "activity_attempt_id":format!("attempt-{execution}-{attempt}"),"attempt_number":attempt,
        "activity_type":"inspect","payload_codec":"avro","lease_owner":"checkpoint-worker",
        "retry_policy":{"max_attempts":3,"backoff_seconds":[1,2],"schedule_to_close_timeout":90},
        "last_heartbeat_details":details,"last_heartbeat_at":at},
        "server_capabilities":{"activity_heartbeat_resume":{"supported":true,"version":1}}})
}
struct Server {
    url: String,
    task: tokio::task::JoinHandle<()>,
    registrations: Arc<Mutex<Vec<Value>>>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Server {
    async fn start(envelopes: Vec<Value>, heartbeat_replies: Vec<(u16, Value)>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let registrations = Arc::new(Mutex::new(Vec::new()));
        let recorded = registrations.clone();
        let mut envelopes: VecDeque<_> = envelopes.into();
        let mut heartbeats: VecDeque<_> = heartbeat_replies.into();
        let task = tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                let (end, length) = loop {
                    let mut buffer = [0; 4096];
                    let count = stream.read(&mut buffer).await.unwrap();
                    assert_ne!(count, 0);
                    bytes.extend_from_slice(&buffer[..count]);
                    assert!(bytes.len() < 65536);
                    if let Some(index) = bytes.windows(4).position(|x| x == b"\r\n\r\n") {
                        let headers = String::from_utf8_lossy(&bytes[..index]);
                        let length = headers
                            .lines()
                            .find_map(|line| {
                                let (k, v) = line.split_once(':')?;
                                k.eq_ignore_ascii_case("content-length")
                                    .then(|| v.trim().parse::<usize>().unwrap())
                            })
                            .unwrap_or(0);
                        assert!(length < 32768);
                        break (index + 4, length);
                    }
                };
                while bytes.len() < end + length {
                    let mut buffer = [0; 4096];
                    let count = stream.read(&mut buffer).await.unwrap();
                    assert_ne!(count, 0);
                    bytes.extend_from_slice(&buffer[..count]);
                }
                let headers = String::from_utf8_lossy(&bytes[..end]);
                let path = headers.split_whitespace().nth(1).unwrap();
                let request: Value =
                    serde_json::from_slice(&bytes[end..end + length]).unwrap_or(Value::Null);
                let (status, body) = if path.ends_with("/cluster/info") {
                    (200, json!({}))
                } else if path.ends_with("/register") {
                    recorded.lock().unwrap().push(request.clone());
                    (
                        200,
                        json!({"registered":true,"worker_id":"checkpoint-worker","namespace":"default","task_queue":"checkpoint-queue","protocol_version":"1.20","server_capabilities":{"cooperative_cancellation":true,"activity_heartbeat_resume":{"supported":true,"version":1}},"capabilities":request["capabilities"]}),
                    )
                } else if path.ends_with("/workflow-tasks/poll") {
                    (200, json!({"task":null}))
                } else if path.ends_with("/activity-tasks/poll") {
                    (200, envelopes.pop_front().unwrap_or(json!({"task":null})))
                } else if path.ends_with("/status") {
                    (
                        200,
                        json!({"task_id":path.split('/').rev().nth(1).unwrap(),"activity_attempt_id":request["activity_attempt_id"],"lease_owner":"checkpoint-worker","can_continue":true,"cancel_requested":false,"heartbeat_recorded":false,"reason":null,"task_status":"leased","attempt_status":"running","activity_status":"running","lease_expires_at":"2099-01-01T00:00:00Z"}),
                    )
                } else if path.ends_with("/heartbeat") {
                    heartbeats.pop_front().expect("declared heartbeat reply")
                } else if path.ends_with("/complete") || path.ends_with("/fail") {
                    (200, json!({"recorded":true}))
                } else {
                    panic!("unexpected mock path {path}")
                };
                let body = body.to_string();
                let reason = if status == 200 { "OK" } else { "Refused" };
                stream.write_all(format!("HTTP/1.1 {status} {reason}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
                stream.shutdown().await.unwrap();
            }
        });
        Self {
            url,
            task,
            registrations,
        }
    }
    fn worker(&self, cooperative: bool) -> Worker {
        Worker::new(
            Client::builder(&self.url)
                .timeout(Duration::from_secs(2))
                .build()
                .unwrap(),
            "checkpoint-queue",
        )
        .worker_id("checkpoint-worker")
        .cooperative_cancellation(cooperative)
    }
}

#[derive(Debug, Deserialize, PartialEq)]
struct Progress {
    cursor: u64,
    flags: Vec<Option<bool>>,
}

#[tokio::test]
async fn unavailable_and_supported_empty_are_distinct() {
    let valid = claim(2, "execution", json!({"cursor":1}), json!(AT));
    let mut cases = Vec::new();
    for capability in [
        Value::Null,
        json!(false),
        json!({"supported":false,"version":1}),
        json!({"supported":true,"version":2}),
        json!({"supported":true,"version":"1"}),
    ] {
        let mut wire = valid.clone();
        wire["server_capabilities"]["activity_heartbeat_resume"] = capability;
        cases.push(wire);
    }
    let mut missing = valid;
    missing["task"]
        .as_object_mut()
        .unwrap()
        .remove("last_heartbeat_details");
    missing["task"]
        .as_object_mut()
        .unwrap()
        .remove("last_heartbeat_at");
    cases.push(missing);
    cases.push(claim(1, "empty", Value::Null, Value::Null));
    let server = Server::start(cases, vec![]).await;
    let mut worker = server.worker(false);
    let seen = Arc::new(Mutex::new(0));
    let captured = seen.clone();
    worker.register_activity("inspect", move |ctx, _| {
        let seen = captured.clone();
        async move {
            let index = *seen.lock().unwrap();
            if index < 6 {
                assert_eq!(ctx.heartbeat_resume_version(), None);
                assert_eq!(
                    ctx.last_heartbeat_details(),
                    Err(ActivityHeartbeatResumeUnavailable)
                );
                assert!(ctx.last_heartbeat_at().is_err());
            } else {
                assert_eq!(ctx.heartbeat_resume_version(), Some(1));
                assert_eq!(ctx.last_heartbeat_details().unwrap(), None);
                assert_eq!(ctx.last_heartbeat_at().unwrap(), None);
                assert!(ctx.last_heartbeat_checkpoint().unwrap().is_none());
            }
            *seen.lock().unwrap() += 1;
            Ok(Value::Null)
        }
    });
    worker.register().await.unwrap();
    for _ in 0..7 {
        assert_eq!(worker.run_once().await.unwrap(), 1);
    }
    assert_eq!(*seen.lock().unwrap(), 7);
    assert!(server.registrations.lock().unwrap()[0]["capabilities"]
        .as_array()
        .unwrap()
        .iter()
        .any(|v| v == "activity_heartbeat_resume"));
}

#[tokio::test]
async fn ordinary_and_cooperative_handlers_receive_typed_immutable_claims() {
    for cooperative in [false, true] {
        let details =
            json!({"cursor":u64::MAX,"flags":[true,null,false],"secret":"private-checkpoint"});
        let server = Server::start(
            vec![claim(2, "execution", details.clone(), json!(AT))],
            vec![],
        )
        .await;
        let mut worker = server.worker(cooperative);
        let expected = details.clone();
        let finished = Arc::new(Mutex::new(false));
        let completed = finished.clone();
        worker.register_activity("inspect",move |mut ctx,_|{let expected=expected.clone();let completed=completed.clone();async move {
            assert_eq!(ctx.activity_execution_id(),Some("execution"));assert_eq!(ctx.activity_attempt_id,"attempt-execution-2");
            assert_eq!(ctx.workflow_id(),Some("workflow"));assert_eq!(ctx.run_id(),Some("run"));
            let checkpoint=ctx.last_heartbeat_checkpoint()?.unwrap().clone();
            assert_eq!(checkpoint.details(),&expected);assert_eq!(checkpoint.contract_version(),1);
            assert_eq!(checkpoint.details_as::<Progress>().unwrap(),Progress{cursor:u64::MAX,flags:vec![Some(true),None,Some(false)]});
            assert_eq!(checkpoint.acknowledged_at().to_rfc3339_opts(chrono::SecondsFormat::Nanos,true),AT);
            assert_eq!(ctx.last_heartbeat_at()?.unwrap(),checkpoint.acknowledged_at());
            assert!(!format!("{ctx:?}").contains("private-checkpoint"));
            ctx.attempt_number=99;assert_eq!(ctx.retry_info().attempt_number(),2);
            assert_eq!(ctx.retry_info().policy(),Some(&json!({"max_attempts":3,"backoff_seconds":[1,2],"schedule_to_close_timeout":90})));
            let mut detached=ctx.last_heartbeat_details()?.unwrap().clone();detached["cursor"]=json!(0);
            assert_eq!(ctx.last_heartbeat_checkpoint()?.unwrap(),&checkpoint);*completed.lock().unwrap()=true;Ok(Value::Null)
        }});
        worker.register().await.unwrap();
        assert_eq!(worker.run_once().await.unwrap(), 1);
        assert!(*finished.lock().unwrap());
    }
}

#[tokio::test]
async fn later_claims_do_not_leak_checkpoints_between_executions_or_runs() {
    let mut last = claim(2, "other-execution", Value::Null, Value::Null);
    last["task"]["workflow_id"] = json!("other-workflow");
    last["task"]["run_id"] = json!("other-run");
    let server = Server::start(
        vec![
            claim(1, "same", Value::Null, Value::Null),
            claim(2, "same", json!({"cursor":7}), json!(AT)),
            claim(
                3,
                "same",
                json!({"cursor":8}),
                json!("2026-11-14T09:00:01Z"),
            ),
            last,
        ],
        vec![],
    )
    .await;
    let mut worker = server.worker(false);
    let seen = Arc::new(Mutex::new(Vec::new()));
    let captured = seen.clone();
    worker.register_activity("inspect", move |ctx, _| {
        let seen = captured.clone();
        async move {
            seen.lock().unwrap().push((
                ctx.activity_execution_id().unwrap().to_string(),
                ctx.activity_attempt_id.clone(),
                ctx.last_heartbeat_details()?.cloned(),
                ctx.run_id().unwrap().to_string(),
            ));
            Ok(Value::Null)
        }
    });
    worker.register().await.unwrap();
    for _ in 0..4 {
        assert_eq!(worker.run_once().await.unwrap(), 1);
    }
    let rows = seen.lock().unwrap();
    assert_eq!(rows[0].2, None);
    assert_eq!(rows[1].2, Some(json!({"cursor":7})));
    assert_eq!(rows[2].2, Some(json!({"cursor":8})));
    assert_ne!(rows[1].1, rows[2].1);
    assert_eq!(rows[1].0, rows[2].0);
    assert_eq!(rows[3].2, None);
    assert_eq!(rows[3].3, "other-run");
}

#[tokio::test]
async fn accepted_rejected_and_uncertain_writes_never_mutate_the_received_snapshot() {
    let server = Server::start(
        vec![claim(2, "execution", json!({"cursor":7}), json!(AT))],
        vec![
            (
                200,
                json!({"heartbeat_recorded":true,"last_heartbeat_at":"2026-11-14T09:01:00Z"}),
            ),
            (
                200,
                json!({"heartbeat_recorded":false,"can_continue":false,"reason":"stale_attempt"}),
            ),
            (409, json!({"message":"stale attempt","recorded":false})),
            (503, json!({"message":"response uncertain"})),
        ],
    )
    .await;
    let mut worker = server.worker(false);
    let finished = Arc::new(Mutex::new(false));
    let completed = finished.clone();
    worker.register_activity("inspect", move |ctx, _| {
        let completed = completed.clone();
        async move {
            let checkpoint = ctx.last_heartbeat_checkpoint()?.unwrap().clone();
            assert!(ctx.heartbeat(json!({"cursor":8})).await?.heartbeat_recorded);
            let rejected = ctx.heartbeat(json!({"cursor":9})).await?;
            assert!(!rejected.heartbeat_recorded && rejected.should_stop());
            assert!(ctx.heartbeat(json!({"cursor":10})).await.is_err());
            assert!(ctx.heartbeat(json!({"cursor":11})).await.is_err());
            assert_eq!(ctx.last_heartbeat_checkpoint()?.unwrap(), &checkpoint);
            *completed.lock().unwrap() = true;
            Ok(Value::Null)
        }
    });
    worker.register().await.unwrap();
    assert_eq!(worker.run_once().await.unwrap(), 1);
    assert!(*finished.lock().unwrap());
}

#[tokio::test]
async fn malformed_negotiated_checkpoints_refuse_before_the_handler() {
    let original = claim(2, "execution", json!({"cursor":7}), json!(AT));
    let mut cases = Vec::new();
    for (field, value) in [
        ("last_heartbeat_at", Value::Null),
        ("last_heartbeat_at", json!("no timestamp")),
        ("last_heartbeat_at", json!("2026-11-14T09:00:00")),
        ("last_heartbeat_details", Value::Null),
        ("activity_execution_id", json!("")),
        ("activity_attempt_id", json!("")),
        ("workflow_id", json!("")),
        ("run_id", json!("")),
        ("namespace", json!("foreign")),
        ("attempt_number", json!(1)),
    ] {
        let mut wire = original.clone();
        wire["task"][field] = value;
        cases.push(wire);
    }
    for field in ["last_heartbeat_at", "last_heartbeat_details"] {
        let mut wire = original.clone();
        wire["task"].as_object_mut().unwrap().remove(field);
        cases.push(wire);
    }
    let server = Server::start(cases, vec![]).await;
    let mut worker = server.worker(false);
    worker.register_activity("inspect", |_, _| async {
        panic!("malformed checkpoint reached handler");
        #[allow(unreachable_code)]
        Ok(Value::Null)
    });
    worker.register().await.unwrap();
    for _ in 0..12 {
        assert!(worker.run_once().await.is_err());
    }
}

#[tokio::test]
async fn server_free_activities_report_unavailable() {
    let mut env = WorkflowEnvironment::new().unwrap();
    env.register_activity("inspect", |ctx, _| async move {
        assert_eq!(ctx.heartbeat_resume_version(), None);
        assert!(ctx.last_heartbeat_checkpoint().is_err());
        Ok(Value::Null)
    });
    assert_eq!(env.run_activity("inspect", ()).await.unwrap(), Value::Null);
}
