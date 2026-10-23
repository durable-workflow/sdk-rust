use super::*;

fn capacity_server(path: &str, _: &str, request: usize) -> Option<(&'static str, String)> {
    if path == "/api/worker/register" {
        return Some((
            "200 OK",
            json!({"worker_id":"capacity-worker", "registered":true,
                   "heartbeat_interval_seconds":1})
            .to_string(),
        ));
    }
    for (family, task_type) in [("activity", "activity_type"), ("workflow", "workflow_type")] {
        if path == format!("/api/worker/{family}-tasks/poll") && request <= 5 {
            let mut task = json!({
                "task_id":format!("capacity-{family}-{request}"),
                "payload_codec":DEFAULT_CODEC,
                "arguments":encode_value_envelope(&json!([request]),DEFAULT_CODEC).unwrap(),
                "lease_owner":"capacity-worker",
            });
            task[task_type] = json!(format!("capacity.{family}"));
            if family == "activity" {
                task["activity_attempt_id"] = json!(format!("attempt-{request}"));
                task["attempt_number"] = json!(1);
            } else {
                task["workflow_task_attempt"] = json!(1);
            }
            return Some(("200 OK", json!({"task":task}).to_string()));
        }
    }
    if path.starts_with("/api/worker/workflow-tasks/capacity-workflow-")
        && path.ends_with("/complete")
    {
        // Hold real settlements open so workflow slots must remain occupied.
        thread::sleep(Duration::from_millis(1200));
        return Some(("200 OK", "{}".into()));
    }
    if path.starts_with("/api/worker/activity-tasks/capacity-activity-")
        && path.ends_with("/complete")
    {
        return Some(("200 OK", "{}".into()));
    }
    None
}

fn server_and_worker() -> (MockWorkerServer, Worker) {
    let server = MockWorkerServer::start_with_behavior(MockWorkerBehavior {
        request_override: Some(capacity_server),
        concurrent_requests: true,
        ..MockWorkerBehavior::default()
    });
    let worker = Worker::new(Client::new(server.base_url()).unwrap(), "capacity-queue")
        .worker_id("capacity-worker")
        .max_concurrent_workflow_tasks(3)
        .max_concurrent_activity_tasks(3)
        .poll_timeout(Duration::from_millis(10));
    (server, worker)
}

#[tokio::test]
async fn concurrent_run_once_calls_share_the_configured_activity_limit() {
    fn activity_only(path: &str, body: &str, request: usize) -> Option<(&'static str, String)> {
        if path == "/api/worker/workflow-tasks/poll" {
            return Some(("200 OK", json!({"task":null}).to_string()));
        }
        capacity_server(path, body, request)
    }
    let server = MockWorkerServer::start_with_behavior(MockWorkerBehavior {
        request_override: Some(activity_only),
        concurrent_requests: true,
        ..MockWorkerBehavior::default()
    });
    let active = Arc::new(AtomicUsize::new(0));
    let maximum = Arc::new(AtomicUsize::new(0));
    let mut worker = Worker::new(Client::new(server.base_url()).unwrap(), "capacity-queue")
        .worker_id("capacity-worker")
        .max_concurrent_activity_tasks(2);
    worker.register_activity("capacity.activity", {
        let (active, maximum) = (active.clone(), maximum.clone());
        move |_, _| {
            let (active, maximum) = (active.clone(), maximum.clone());
            async move {
                let count = active.fetch_add(1, Ordering::SeqCst) + 1;
                maximum.fetch_max(count, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(50)).await;
                active.fetch_sub(1, Ordering::SeqCst);
                Ok(Value::Null)
            }
        }
    });
    let mut calls = FuturesUnordered::new();
    for _ in 0..5 {
        let worker = worker.clone();
        calls.push(async move { worker.run_once().await });
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(result) = calls.next().await {
            assert_eq!(result.unwrap(), 1);
        }
    })
    .await
    .unwrap();
    assert_eq!(maximum.load(Ordering::SeqCst), 2);
    assert_eq!(worker.activity_capacity.available(), 2);
}

#[tokio::test]
async fn slow_activity_allows_other_work_and_heartbeats_report_occupied_slots() {
    let (server, mut worker) = server_and_worker();
    let slow = Arc::new(tokio::sync::Notify::new());
    let completed = Arc::new(AtomicUsize::new(0));
    let running = Arc::new(AtomicUsize::new(0));
    let maximum = Arc::new(AtomicUsize::new(0));
    worker.register_activity("capacity.activity", {
        let (slow, completed, running, maximum) = (
            slow.clone(),
            completed.clone(),
            running.clone(),
            maximum.clone(),
        );
        move |_, input| {
            let (slow, completed, running, maximum) = (
                slow.clone(),
                completed.clone(),
                running.clone(),
                maximum.clone(),
            );
            async move {
                let current = running.fetch_add(1, Ordering::SeqCst) + 1;
                maximum.fetch_max(current, Ordering::SeqCst);
                if input[0] == 1 {
                    slow.notified().await;
                } else {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                running.fetch_sub(1, Ordering::SeqCst);
                completed.fetch_add(1, Ordering::SeqCst);
                Ok(Value::Null)
            }
        }
    });
    let watchdog = async {
        loop {
            let heartbeat = server
                .request_bodies("/api/worker/heartbeat")
                .iter()
                .any(|body| body["task_slots"]["activity_available"] == 2);
            if completed.load(Ordering::SeqCst) == 4 && heartbeat {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(maximum.load(Ordering::SeqCst), 3);
        assert_eq!(worker.activity_capacity.available(), 2);
        slow.notify_one();
        while completed.load(Ordering::SeqCst) != 5 || worker.activity_capacity.available() != 3 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let before = server.request_count("/api/worker/heartbeat");
        while server.request_count("/api/worker/heartbeat") == before {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            server
                .request_bodies("/api/worker/heartbeat")
                .last()
                .unwrap()["task_slots"]["activity_available"],
            3
        );
    };
    tokio::time::timeout(Duration::from_secs(10), worker.run_until(watchdog))
        .await
        .expect("other activities and capacity heartbeats must progress while one activity is slow")
        .unwrap();
    assert_eq!(completed.load(Ordering::SeqCst), 5);
    assert_eq!(worker.activity_capacity.available(), 3);
}

#[tokio::test]
async fn workflow_lanes_hold_capacity_until_settlement_and_restore_it_afterward() {
    let (server, mut worker) = server_and_worker();
    let calls = Arc::new(AtomicUsize::new(0));
    worker.register_workflow("capacity.workflow", {
        let calls = calls.clone();
        move |_, _| {
            calls.fetch_add(1, Ordering::SeqCst);
            async { Ok(Value::Null) }
        }
    });
    let maximum = AtomicUsize::new(0);
    let watchdog = async {
        loop {
            let active = 3 - worker.workflow_capacity.available();
            maximum.fetch_max(active, Ordering::SeqCst);
            if calls.load(Ordering::SeqCst) == 5 && active == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(10), worker.run_until(watchdog))
        .await
        .expect("workflow tasks must use all three slots and settle")
        .unwrap();
    assert_eq!(maximum.load(Ordering::SeqCst), 3);
    assert!(server
        .request_bodies("/api/worker/heartbeat")
        .iter()
        .any(|body| { body["task_slots"]["workflow_available"] == 0 }));
    assert_eq!(worker.workflow_capacity.available(), 3);
}
