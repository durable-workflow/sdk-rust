use super::*;
use futures_util::{stream::FuturesUnordered, StreamExt};

async fn terminal_poll_does_not_refill(respond: RequestOverride, workflow: bool, stopped: bool) {
    let server = MockWorkerServer::start_with_behavior(MockWorkerBehavior {
        request_override: Some(respond),
        concurrent_requests: true,
        ..MockWorkerBehavior::default()
    });
    let stop = Arc::new(AtomicBool::new(false));
    let mut worker = Worker::new(
        Client::builder(server.base_url())
            .worker_protocol_version(WorkerProtocolVersion::V119)
            .build()
            .unwrap(),
        "capacity-queue",
    )
    .worker_id("capacity-worker")
    .max_concurrent_workflow_tasks(4)
    .max_concurrent_activity_tasks(4);
    worker.run_stop = Some(stop.clone());
    let mut calls = Vec::new();
    for _ in 0..4 {
        let worker = worker.clone();
        calls.push(tokio::spawn(async move {
            if workflow {
                worker.poll_workflow_once().await
            } else {
                worker.poll_activity_once().await
            }
        }));
    }
    let mut errors = 0;
    tokio::time::timeout(Duration::from_secs(3), async {
        for call in calls {
            match call.await.unwrap() {
                Err(_) => errors += 1,
                Ok(ManagedPollOutcome::Stop | ManagedPollOutcome::Idle) => {}
                Ok(ManagedPollOutcome::Handled) => {
                    panic!("invalid/stopped poll must not launch settlement")
                }
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(
        errors,
        usize::from(!stopped),
        "preserve the first fatal error"
    );
    assert!(stop.load(Ordering::SeqCst));
    let path = if workflow {
        "/api/worker/workflow-tasks/poll"
    } else {
        "/api/worker/activity-tasks/poll"
    };
    assert_eq!(
        server.request_count(path),
        1,
        "queued lanes must see stop before poll ownership wakes them"
    );
    assert_eq!(worker.workflow_capacity.available(), 4);
    assert_eq!(worker.activity_capacity.available(), 4);
}

#[tokio::test]
async fn malformed_poll_stops_queued_lanes_before_refill() {
    fn malformed(path: &str, _: &str, _: usize) -> Option<(&'static str, String)> {
        path.ends_with("/poll")
            .then(|| ("200 OK", json!({"task":{"task_id":7}}).to_string()))
    }
    for workflow in [true, false] {
        terminal_poll_does_not_refill(malformed, workflow, false).await;
    }
}

#[tokio::test]
async fn server_stop_is_visible_before_queued_pollers_wake() {
    fn stopped(path: &str, _: &str, _: usize) -> Option<(&'static str, String)> {
        path.ends_with("/poll").then(|| {
            (
                "200 OK",
                json!({"task":null,"poll_status":"stopped"}).to_string(),
            )
        })
    }
    for workflow in [true, false] {
        terminal_poll_does_not_refill(stopped, workflow, true).await;
    }
}

#[tokio::test]
async fn failed_activity_hydration_stops_queued_lanes_without_settlement() {
    fn broken_payload(path: &str, _: &str, _: usize) -> Option<(&'static str, String)> {
        path.ends_with("/poll").then(|| {
            ("200 OK", json!({"task":{
            "task_id":"hydrate-task","workflow_id":"workflow","run_id":"run",
            "activity_execution_id":"execution","activity_type":"activity",
            "activity_attempt_id":"attempt","attempt_number":1,
            "lease_owner":"capacity-worker","namespace":"default","task_queue":"capacity-queue",
            "lease_expires_at":"2099-01-01T00:00:00Z","deadlines":{},
            "payload_codec":"avro","arguments":{"codec":"avro","external_payload":{}}
        }}).to_string())
        })
    }
    terminal_poll_does_not_refill(broken_payload, false, false).await;
}

#[tokio::test]
async fn capacity_refusal_holds_the_family_poller_through_backoff() {
    static ARRIVALS: Mutex<Vec<Instant>> = Mutex::new(Vec::new());
    ARRIVALS.lock().unwrap().clear();
    fn refusal(path: &str, _: &str, request: usize) -> Option<(&'static str, String)> {
        if path != "/api/worker/workflow-tasks/poll" {
            return None;
        }
        ARRIVALS.lock().unwrap().push(Instant::now());
        Some(if request == 1 {
            ("429 Too Many Requests", json!({"reason":"long_poll_capacity_exhausted",
                "poll_status":"long_poll_capacity_exhausted","retryable":true,"retry_after_seconds":1}).to_string())
        } else {
            (
                "200 OK",
                json!({"task":null,"poll_status":"stopped"}).to_string(),
            )
        })
    }
    let server = MockWorkerServer::start_with_behavior(MockWorkerBehavior {
        request_override: Some(refusal),
        concurrent_requests: true,
        ..MockWorkerBehavior::default()
    });
    let mut worker = Worker::new(Client::new(server.base_url()).unwrap(), "backoff")
        .max_concurrent_workflow_tasks(4);
    worker.run_stop = Some(Arc::new(AtomicBool::new(false)));
    let mut calls = FuturesUnordered::new();
    for _ in 0..4 {
        let worker = worker.clone();
        calls.push(async move { worker.poll_workflow_once().await });
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(result) = calls.next().await {
            result.unwrap();
        }
    })
    .await
    .unwrap();
    let arrivals = ARRIVALS.lock().unwrap();
    assert_eq!(arrivals.len(), 2);
    assert!(
        arrivals[1].duration_since(arrivals[0]) >= Duration::from_secs(1),
        "no queued lane may bypass the advertised backoff"
    );
    assert_eq!(worker.workflow_capacity.available(), 4);
}

#[tokio::test]
async fn external_stop_interrupts_backoff_without_waking_queued_polls() {
    struct BackoffSeen(Arc<tokio::sync::Notify>);
    impl metrics::MetricsRecorder for BackoffSeen {
        fn increment(&self, name: &str, _: u64, tags: &metrics::MetricTags) {
            if name == metrics::WORKER_POLLS
                && tags.get("outcome").map(String::as_str) == Some("backpressure")
            {
                self.0.notify_one();
            }
        }
        fn record(&self, _: &str, _: f64, _: &metrics::MetricTags) {}
    }
    fn refusal(path: &str, _: &str, _: usize) -> Option<(&'static str, String)> {
        path.ends_with("/poll").then(|| {
            ("429 Too Many Requests", json!({
            "reason":"long_poll_capacity_exhausted","poll_status":"long_poll_capacity_exhausted",
            "retryable":true,"retry_after_seconds":3
        }).to_string())
        })
    }
    let server = MockWorkerServer::start_with_behavior(MockWorkerBehavior {
        request_override: Some(refusal),
        concurrent_requests: true,
        ..MockWorkerBehavior::default()
    });
    let seen = Arc::new(tokio::sync::Notify::new());
    let stop = Arc::new(AtomicBool::new(false));
    let mut worker = Worker::new(Client::new(server.base_url()).unwrap(), "backoff")
        .max_concurrent_workflow_tasks(4)
        .metrics_recorder(Arc::new(BackoffSeen(seen.clone())));
    worker.run_stop = Some(stop.clone());
    let mut calls = Vec::new();
    for _ in 0..4 {
        let worker = worker.clone();
        calls.push(tokio::spawn(
            async move { worker.poll_workflow_once().await },
        ));
    }
    tokio::time::timeout(Duration::from_secs(2), seen.notified())
        .await
        .unwrap();
    stop.store(true, Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(1), async {
        for call in calls {
            call.await.unwrap().unwrap();
        }
    })
    .await
    .unwrap();
    assert_eq!(server.request_count("/api/worker/workflow-tasks/poll"), 1);
    assert_eq!(worker.workflow_capacity.available(), 4);
}

#[tokio::test]
async fn shutdown_waits_for_claimed_report_before_deregistering() {
    static REPORT_STARTED: AtomicBool = AtomicBool::new(false);
    static REPORT_FINISHED: AtomicBool = AtomicBool::new(false);
    static ALLOW_REPORT: AtomicBool = AtomicBool::new(false);
    static EARLY_DEREGISTRATION: AtomicBool = AtomicBool::new(false);
    for value in [
        &REPORT_STARTED,
        &REPORT_FINISHED,
        &ALLOW_REPORT,
        &EARLY_DEREGISTRATION,
    ] {
        value.store(false, Ordering::SeqCst);
    }
    fn held_report(path: &str, body: &str, request: usize) -> Option<(&'static str, String)> {
        if path == "/api/worker/activity-tasks/poll" {
            return Some((
                "200 OK",
                json!({"task":null,"poll_status":"idle"}).to_string(),
            ));
        }
        if path.starts_with("/api/worker/workflow-tasks/capacity-workflow-")
            && path.ends_with("/complete")
        {
            REPORT_STARTED.store(true, Ordering::SeqCst);
            let deadline = Instant::now() + Duration::from_secs(5);
            while !ALLOW_REPORT.load(Ordering::SeqCst) && Instant::now() < deadline {
                thread::sleep(Duration::from_millis(1));
            }
            REPORT_FINISHED.store(true, Ordering::SeqCst);
            return Some(("200 OK", "{}".into()));
        }
        if path == "/api/worker/registrations/capacity-worker"
            && !REPORT_FINISHED.load(Ordering::SeqCst)
        {
            EARLY_DEREGISTRATION.store(true, Ordering::SeqCst);
        }
        capacity_server(path, body, request)
    }
    let server = MockWorkerServer::start_with_behavior(MockWorkerBehavior {
        request_override: Some(held_report),
        concurrent_requests: true,
        ..MockWorkerBehavior::default()
    });
    let mut worker = Worker::new(Client::new(server.base_url()).unwrap(), "capacity-queue")
        .worker_id("capacity-worker")
        .max_concurrent_workflow_tasks(1);
    worker.register_workflow("capacity.workflow", |_, _| async { Ok(Value::Null) });
    let capacity = worker.workflow_capacity.clone();
    let run = tokio::spawn(async move {
        worker
            .run_until(async {
                while !REPORT_STARTED.load(Ordering::SeqCst) {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await
    });
    tokio::time::timeout(Duration::from_secs(3), async {
        while !REPORT_STARTED.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!run.is_finished());
    assert_eq!(capacity.available(), 0);
    assert_eq!(
        server.request_count("/api/worker/registrations/capacity-worker"),
        0
    );
    ALLOW_REPORT.store(true, Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(3), run)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(REPORT_FINISHED.load(Ordering::SeqCst));
    assert!(!EARLY_DEREGISTRATION.load(Ordering::SeqCst));
    assert_eq!(
        server.request_count("/api/worker/registrations/capacity-worker"),
        1
    );
    assert_eq!(capacity.available(), 1);
}

#[tokio::test]
async fn one_poller_fills_sixteen_slots_and_holds_them_through_reporting() {
    static ACTIVE_POLLS: AtomicUsize = AtomicUsize::new(0);
    static MAX_POLLS: AtomicUsize = AtomicUsize::new(0);
    static REPORTS: AtomicUsize = AtomicUsize::new(0);
    static ALLOW_REPORTS: AtomicBool = AtomicBool::new(false);
    ACTIVE_POLLS.store(0, Ordering::SeqCst);
    MAX_POLLS.store(0, Ordering::SeqCst);
    REPORTS.store(0, Ordering::SeqCst);
    ALLOW_REPORTS.store(false, Ordering::SeqCst);
    fn burst(path: &str, _: &str, request: usize) -> Option<(&'static str, String)> {
        if path == "/api/worker/activity-tasks/poll" {
            let active = ACTIVE_POLLS.fetch_add(1, Ordering::SeqCst) + 1;
            MAX_POLLS.fetch_max(active, Ordering::SeqCst);
            thread::sleep(Duration::from_millis(10));
            let reply = if request <= 16 {
                json!({"task":{
                    "task_id":format!("burst-{request}"),"activity_type":"burst",
                    "activity_attempt_id":format!("attempt-{request}"),"attempt_number":1,
                    "lease_owner":"burst-worker","payload_codec":"avro",
                    "arguments":encode_value_envelope(&json!([request]),DEFAULT_CODEC).unwrap()
                }})
            } else {
                json!({"task":null,"poll_status":"idle"})
            };
            ACTIVE_POLLS.fetch_sub(1, Ordering::SeqCst);
            return Some(("200 OK", reply.to_string()));
        }
        if path.starts_with("/api/worker/activity-tasks/burst-") && path.ends_with("/complete") {
            REPORTS.fetch_add(1, Ordering::SeqCst);
            let deadline = Instant::now() + Duration::from_secs(5);
            while !ALLOW_REPORTS.load(Ordering::SeqCst) && Instant::now() < deadline {
                thread::sleep(Duration::from_millis(1));
            }
            return Some(("200 OK", "{}".into()));
        }
        None
    }
    let server = MockWorkerServer::start_with_behavior(MockWorkerBehavior {
        request_override: Some(burst),
        concurrent_requests: true,
        ..MockWorkerBehavior::default()
    });
    let barrier = Arc::new(tokio::sync::Barrier::new(17));
    let entered = Arc::new(AtomicUsize::new(0));
    let mut worker = Worker::new(Client::new(server.base_url()).unwrap(), "burst")
        .worker_id("burst-worker")
        .max_concurrent_activity_tasks(16);
    worker.register_activity("burst", {
        let (barrier, entered) = (barrier.clone(), entered.clone());
        move |_, _| {
            let (barrier, entered) = (barrier.clone(), entered.clone());
            async move {
                entered.fetch_add(1, Ordering::SeqCst);
                barrier.wait().await;
                Ok(Value::Null)
            }
        }
    });
    let mut calls = Vec::new();
    for _ in 0..16 {
        let worker = worker.clone();
        calls.push(tokio::spawn(
            async move { worker.poll_activity_once().await },
        ));
    }
    tokio::time::timeout(Duration::from_secs(3), async {
        while entered.load(Ordering::SeqCst) != 16 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(MAX_POLLS.load(Ordering::SeqCst), 1);
    assert_eq!(worker.activity_capacity.available(), 0);
    let extra = {
        let worker = worker.clone();
        tokio::spawn(async move { worker.poll_activity_once().await })
    };
    barrier.wait().await;
    tokio::time::timeout(Duration::from_secs(3), async {
        while REPORTS.load(Ordering::SeqCst) != 16 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        worker.activity_capacity.available(),
        0,
        "all reports still own their slots"
    );
    assert_eq!(server.request_count("/api/worker/activity-tasks/poll"), 16);
    ALLOW_REPORTS.store(true, Ordering::SeqCst);
    for call in calls {
        assert!(matches!(
            call.await.unwrap().unwrap(),
            ManagedPollOutcome::Handled
        ));
    }
    assert!(matches!(
        extra.await.unwrap().unwrap(),
        ManagedPollOutcome::Idle
    ));
    assert_eq!(MAX_POLLS.load(Ordering::SeqCst), 1);
    assert_eq!(worker.activity_capacity.available(), 16);
}

#[tokio::test]
async fn task_capacity_does_not_multiply_idle_polls() {
    fn delayed_idle(path: &str, _: &str, _: usize) -> Option<(&'static str, String)> {
        if matches!(
            path,
            "/api/worker/workflow-tasks/poll" | "/api/worker/activity-tasks/poll"
        ) {
            thread::sleep(Duration::from_millis(500));
            return Some((
                "200 OK",
                json!({"task":null,"poll_status":"idle"}).to_string(),
            ));
        }
        None
    }
    let server = MockWorkerServer::start_with_behavior(MockWorkerBehavior {
        request_override: Some(delayed_idle),
        concurrent_requests: true,
        ..MockWorkerBehavior::default()
    });
    let worker = Worker::new(Client::new(server.base_url()).unwrap(), "poll-capacity")
        .max_concurrent_workflow_tasks(4)
        .max_concurrent_activity_tasks(4);
    let mut calls = Vec::new();
    for workflow in [true, false] {
        for _ in 0..4 {
            let worker = worker.clone();
            calls.push(tokio::spawn(async move {
                if workflow {
                    worker.poll_workflow_once().await
                } else {
                    worker.poll_activity_once().await
                }
            }));
        }
    }
    let workflows = "/api/worker/workflow-tasks/poll";
    let activities = "/api/worker/activity-tasks/poll";
    tokio::time::timeout(Duration::from_secs(2), async {
        while server.request_count(workflows) == 0 || server.request_count(activities) == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        server.request_count(workflows),
        1,
        "execution slots must not fan out long polls"
    );
    assert_eq!(
        server.request_count(activities),
        1,
        "each task family has its own poller"
    );
    assert_eq!(worker.workflow_capacity.available(), 4);
    assert_eq!(worker.activity_capacity.available(), 4);
    for call in calls {
        assert!(matches!(
            call.await.unwrap().unwrap(),
            ManagedPollOutcome::Idle
        ));
    }
    assert_eq!(server.request_count(workflows), 4);
    assert_eq!(server.request_count(activities), 4);
    assert_eq!(worker.workflow_capacity.available(), 4);
    assert_eq!(worker.activity_capacity.available(), 4);
}

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
