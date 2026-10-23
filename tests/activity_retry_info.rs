//! Public poll-to-handler controls, also run unchanged against the installed crate.
use durable_workflow::{
    json, testing::WorkflowEnvironment, ActivityAttemptLimit, ActivityError, ActivityOptions,
    ActivityRetryInfo, ActivityRetryPolicy, Client, Value, Worker,
};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

struct Server {
    url: String,
    task: tokio::task::JoinHandle<()>,
    settlements: Arc<Mutex<Vec<Value>>>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Server {
    async fn start(cases: &[(u64, Option<Value>)]) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let mut tasks: VecDeque<_> = cases.iter().enumerate().map(|(i, (attempt, policy))| {
            let mut task = json!({"task_id":format!("task-{i}"),"activity_attempt_id":format!("attempt-{i}"),
                "activity_type":"inspect", "payload_codec":"avro", "attempt_number":attempt,
                "lease_owner":"retry-worker"});
            if let Some(policy) = policy { task["retry_policy"] = policy.clone(); }
            task
        }).collect();
        let settlements = Arc::new(Mutex::new(Vec::new()));
        let saved = settlements.clone();
        let task = tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                let (header_end, length) = loop {
                    let mut buffer = [0; 4096];
                    let n = stream.read(&mut buffer).await.unwrap();
                    assert_ne!(n, 0);
                    bytes.extend_from_slice(&buffer[..n]);
                    if let Some(index) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
                        let headers = String::from_utf8_lossy(&bytes[..index]);
                        let length = headers
                            .lines()
                            .find_map(|line| {
                                let (key, value) = line.split_once(':')?;
                                key.eq_ignore_ascii_case("content-length")
                                    .then(|| value.trim().parse::<usize>().unwrap())
                            })
                            .unwrap_or(0);
                        break (index + 4, length);
                    }
                };
                while bytes.len() < header_end + length {
                    let mut buffer = [0; 4096];
                    let n = stream.read(&mut buffer).await.unwrap();
                    assert_ne!(n, 0);
                    bytes.extend_from_slice(&buffer[..n]);
                }
                let headers = String::from_utf8_lossy(&bytes[..header_end]);
                let path = headers.split_whitespace().nth(1).unwrap();
                let request: Value =
                    serde_json::from_slice(&bytes[header_end..header_end + length])
                        .unwrap_or(Value::Null);
                let body = if path.ends_with("/register") {
                    json!({"registered":true,"worker_id":"retry-worker", "namespace":"default", "task_queue":"retry-queue", "protocol_version":"1.20", "server_capabilities":{"cooperative_cancellation":true}, "capabilities":request["capabilities"]})
                } else if path.ends_with("/workflow-tasks/poll") {
                    json!({"task":null})
                } else if path.ends_with("/activity-tasks/poll") {
                    json!({"task":tasks.pop_front()})
                } else if path.ends_with("/status") {
                    let id = path.split('/').rev().nth(1).unwrap();
                    json!({"task_id":id,"activity_attempt_id":request["activity_attempt_id"],
                        "lease_owner":"retry-worker","can_continue":true,"cancel_requested":false,
                        "heartbeat_recorded":false,"reason":null,"task_status":"leased",
                        "attempt_status":"running","activity_status":"running",
                        "lease_expires_at":"2099-01-01T00:00:00Z"})
                } else if path.ends_with("/complete") || path.ends_with("/fail") {
                    saved.lock().unwrap().push(request);
                    json!({"recorded":true})
                } else {
                    panic!("unexpected request: {path}");
                };
                let body = body.to_string();
                let response=format!("HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",body.len());
                stream.write_all(response.as_bytes()).await.unwrap();
                stream.shutdown().await.unwrap();
            }
        });
        Self {
            url,
            task,
            settlements,
        }
    }
}

async fn check_worker(cooperative: bool) {
    let cases = vec![
        (
            1,
            Some(json!({"snapshot_version":1,"max_attempts":3,"future":{"retained":[null,42]}})),
        ),
        (3, Some(json!({"max_attempts":3}))),
        (1, None),
        (8, Some(json!({"max_attempts":null}))),
        (1, Some(Value::Null)),
        (1, Some(json!({"snapshot_version":0}))),
        (1, Some(json!({"max_attempts":0}))),
        (1, Some(json!({"max_attempts":"3"}))),
        (1, Some(json!({"max_attempts":3.0}))),
        (1, Some(json!({"max_attempts":-1}))),
        (1, Some(json!([{"max_attempts":3}]))),
        (u64::MAX, Some(json!({"max_attempts":u64::MAX}))),
        (0, Some(json!({"max_attempts":3}))),
        (4, Some(json!({"max_attempts":3}))),
    ];
    let server = Server::start(&cases).await;
    let client = Client::builder(&server.url)
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    let mut worker = Worker::new(client, "retry-queue")
        .worker_id("retry-worker")
        .cooperative_cancellation(cooperative);
    let infos = Arc::new(Mutex::new(Vec::<ActivityRetryInfo>::new()));
    let saved = infos.clone();
    worker.register_activity("inspect", move |mut ctx, _| {
        let saved = saved.clone();
        async move {
            let original = ctx.retry_info().clone();
            ctx.attempt_number = 99;
            assert_eq!(ctx.retry_info(), &original);
            let mut detached = original.policy().cloned();
            if let Some(policy) = detached.as_mut().and_then(Value::as_object_mut) {
                policy.insert("max_attempts".into(), json!(999));
            }
            assert_eq!(ctx.retry_info(), &original);
            saved.lock().unwrap().push(original);
            // Remaining budget does not override a non-retryable failure.
            Err(ActivityError::new("Permanent", "planned")
                .non_retryable(true)
                .into())
        }
    });
    worker.register().await.unwrap();
    for _ in &cases {
        assert_eq!(worker.run_once().await.unwrap(), 1);
    }
    assert_eq!(worker.run_once().await.unwrap(), 0);
    let infos = infos.lock().unwrap();
    assert_eq!(infos.len(), cases.len());
    for (info, (attempt, policy)) in infos.iter().zip(&cases) {
        assert_eq!(info.attempt_number(), *attempt);
        assert_eq!(info.policy(), policy.as_ref());
    }
    assert_eq!(infos[0].attempt_limit(), ActivityAttemptLimit::Bounded(3));
    assert_eq!(infos[0].is_last_configured_attempt(), Some(false));
    assert_eq!(infos[1].is_last_configured_attempt(), Some(true));
    assert_eq!(infos[3].attempt_limit(), ActivityAttemptLimit::Unlimited);
    assert_eq!(infos[3].is_last_configured_attempt(), Some(false));
    for i in [2, 4, 5, 6, 7, 8, 9, 10] {
        assert_eq!(infos[i].attempt_limit(), ActivityAttemptLimit::Unknown);
        assert_eq!(infos[i].is_last_configured_attempt(), None);
    }
    assert_eq!(
        infos[11].attempt_limit(),
        ActivityAttemptLimit::Bounded(u64::MAX)
    );
    assert_eq!(infos[11].is_last_configured_attempt(), Some(true));
    assert_eq!(infos[12].is_last_configured_attempt(), None);
    assert_eq!(infos[13].is_last_configured_attempt(), Some(true));
    let settlements = server.settlements.lock().unwrap();
    assert_eq!(settlements.len(), cases.len());
    for (i, receipt) in settlements.iter().enumerate() {
        assert_eq!(receipt["activity_attempt_id"], format!("attempt-{i}"));
        assert_eq!(receipt["failure"]["type"], "Permanent");
        assert_eq!(receipt["failure"]["non_retryable"], true);
    }
}
#[tokio::test]
async fn normal_worker_retains_each_claim_snapshot_and_unknown_policy() {
    check_worker(false).await;
}
#[tokio::test]
async fn cooperative_worker_retains_each_claim_snapshot_and_unknown_policy() {
    check_worker(true).await;
}

#[tokio::test]
async fn fresh_workflow_tests_share_the_recorded_snapshot_and_direct_attempts_remain_unknown() {
    let mut env = WorkflowEnvironment::new().unwrap();
    env.register_activity("inspect",|ctx,_| async move {
        let info=ctx.retry_info();
        if info.is_last_configured_attempt()==Some(false) {
            return Err(ActivityError::new("Transient","try again").into());
        }
        Ok(json!({"attempt":info.attempt_number(),"policy":info.policy(),"last":info.is_last_configured_attempt()}))
    });
    assert_eq!(
        env.run_activity("inspect", ()).await.unwrap(),
        json!({"attempt":1,"policy":null,"last":null})
    );
    env.register_workflow("bounded", |ctx, _| async move {
        ctx.activity_with_options(
            "inspect",
            ActivityOptions::new().retry_policy(ActivityRetryPolicy::new(3)),
            json!([]),
        )
        .await
    });
    let result = env.run_workflow("bounded", ()).await.unwrap();
    assert_eq!(result["attempt"], 3);
    assert_eq!(result["last"], true);
    let scheduled = env
        .last_history()
        .unwrap()
        .events()
        .iter()
        .find(|event| event.event_type == "ActivityScheduled")
        .unwrap();
    assert_eq!(
        result["policy"],
        scheduled.payload["activity"]["retry_policy"]
    );
    env.register_workflow("default", |ctx, _| async move {
        ctx.activity("inspect", json!([])).await
    });
    let result = env.run_workflow("default", ()).await.unwrap();
    assert_eq!(result["attempt"], 1);
    assert_eq!(result["policy"]["max_attempts"], 1);
    assert_eq!(result["last"], true);
}
