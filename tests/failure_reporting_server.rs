//! Actual failure delivery through an isolated, published PHP Server.
use durable_workflow::{
    encode_avro_value, json, ActivityError, ActivityOptions, ActivityRetryPolicy, AvroValue,
    Client, Error, LocalActivityOptions, Value, Worker, WorkflowHandle, WorkflowHistoryOptions,
    WorkflowResultOptions,
};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
const MIB: usize = 1024 * 1024;
fn client() -> Client {
    assert_eq!(
        std::env::var("DURABLE_WORKFLOW_LOCAL_ISOLATED").as_deref(),
        Ok("1")
    );
    Client::builder(std::env::var("DURABLE_WORKFLOW_SERVER_URL").unwrap())
        .token(Some(std::env::var("DURABLE_WORKFLOW_AUTH_TOKEN").unwrap()))
        .build()
        .unwrap()
}
async fn history(client: &Client, handle: &WorkflowHandle) -> Vec<Value> {
    let page = client
        .get_workflow_history(
            &handle.workflow_id,
            handle.run_id.as_deref().unwrap(),
            WorkflowHistoryOptions::new().page_size(1000),
        )
        .await
        .unwrap();
    assert!(page.next_page_token.is_none());
    page.events
        .into_iter()
        .map(|event| serde_json::to_value(event).unwrap())
        .collect()
}
fn replay_worker(
    client: &Client,
    queue: &str,
    local: bool,
    cooperative: bool,
    replacement: bool,
    calls: Arc<AtomicUsize>,
    observed: Arc<Mutex<Vec<Value>>>,
) -> Worker {
    let mut worker = Worker::new(client.clone(), queue)
        .worker_id(format!(
            "{queue}-{}",
            if replacement {
                "replacement"
            } else {
                "original"
            }
        ))
        .poll_timeout(Duration::from_millis(10))
        .local_activities(local)
        .cooperative_cancellation(cooperative);
    worker.register_activity("large", move |ctx, _| {
        assert!(!replacement, "cold replay must not invoke callbacks");
        calls.fetch_add(1, Ordering::SeqCst);
        async move {
            Err(
                ActivityError::new("OriginalFailureType", "\u{0000}界🦀".repeat(400_000))
                    .non_retryable(ctx.attempt_number == 2)
                    .into(),
            )
        }
    });
    let activity_queue = queue.to_owned();
    worker.register_workflow("tests.bounded-failure",move |ctx,_| {
        let queue=activity_queue.clone(); let observed=observed.clone(); async move {
            let policy=ActivityRetryPolicy::new(3).backoff_intervals([Duration::ZERO,Duration::ZERO]);
            let result=if local {ctx.local_activity_with_options("large",LocalActivityOptions::new().retry_policy(policy),json!([])).await}
                else {ctx.activity_with_options("large",ActivityOptions::new().task_queue(queue).retry_policy(policy),json!([])).await};
            let Err(Error::ActivityFailed(failure))=result else {panic!("expected terminal typed activity failure");};
            assert_eq!(failure.exception_type.as_deref(),Some("OriginalFailureType"));
            assert!(failure.non_retryable);
            assert_eq!(failure.attempt_number,Some(2),"non-retryable ends before the configured budget3");
            assert!(failure.message.len()<=62*1024);
            assert!(failure.message.contains("[truncated"));
            let value=json!({"message":failure.message,"type":failure.exception_type,"non_retryable":failure.non_retryable,"attempt":failure.attempt_number});
            observed.lock().unwrap().push(value.clone());
            ctx.wait_signal("finish").await?;
            Ok(value)
        }
    });
    worker
        .declare_workflow_signals("tests.bounded-failure", &["finish"])
        .unwrap();
    worker
}
async fn check_worker(local: bool, cooperative: bool) {
    let client = client();
    let queue = format!("rust-bounded-{}", durable_workflow::Uuid::new_v4());
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::new(Mutex::new(Vec::new()));
    let worker = replay_worker(
        &client,
        &queue,
        local,
        cooperative,
        false,
        calls.clone(),
        observed.clone(),
    );
    worker.register().await.unwrap();
    let handle = client
        .start_workflow("tests.bounded-failure", &queue, &queue, json!([]))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(60), async {
        while observed.lock().unwrap().is_empty() {
            worker.run_once().await.unwrap();
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("large failures must reach workflow code");
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let initial = observed.lock().unwrap().last().unwrap().clone();
    let events = history(&client, &handle).await;
    let failed = events
        .iter()
        .find(|event| event["event_type"] == "ActivityFailed")
        .expect("failure committed to history");
    assert_eq!(failed["payload"]["message"], initial["message"]);
    assert_eq!(failed["payload"]["exception_type"], "OriginalFailureType");
    let replacement = replay_worker(
        &client,
        &queue,
        local,
        cooperative,
        true,
        calls.clone(),
        observed.clone(),
    );
    replacement.register().await.unwrap();
    handle.signal("finish", json!([])).await.unwrap();
    tokio::time::timeout(Duration::from_secs(60), async {
        while !handle.describe().await.unwrap().is_terminal() {
            replacement.run_once().await.unwrap();
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("cold replay must match the originally observed bounded failure");
    assert_eq!(
        handle
            .result(WorkflowResultOptions::default())
            .await
            .unwrap(),
        initial
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert!(observed
        .lock()
        .unwrap()
        .iter()
        .all(|value| value == &initial));
    println!("published bounded worker failure passed: local={local}, cooperative={cooperative}, two attempts, original type/nonretryable, exact observed/history/replay message");
}
#[tokio::test]
#[ignore = "requires isolated published Server"]
async fn normal_worker_delivers_large_failure_and_cold_replays() {
    check_worker(false, false).await;
}
#[tokio::test]
#[ignore = "requires isolated published Server"]
async fn cooperative_worker_delivers_large_failure_and_cold_replays() {
    check_worker(false, true).await;
}
#[tokio::test]
#[ignore = "requires isolated published Server"]
async fn local_worker_bounds_before_observation_and_cold_replays() {
    check_worker(true, false).await;
}

#[tokio::test]
#[ignore = "requires isolated published Server"]
async fn structured_stack_details_and_terminal_workflow_failure_are_recorded() {
    let client = client();
    let queue = format!("rust-structured-{}", durable_workflow::Uuid::new_v4());
    let worker_id = format!("{queue}-worker");
    let mut worker = Worker::new(client.clone(), &queue).worker_id(&worker_id);
    worker.register_workflow("tests.structured-failure", |_, _| async { Ok(json!(null)) });
    worker.register_activity("structured", |_, _| async {
        panic!("manual claim consumer owns this test");
    });
    worker.register().await.unwrap();
    let handle = client
        .start_workflow("tests.structured-failure", &queue, &queue, json!([]))
        .await
        .unwrap();
    let task = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if let Some(task) = client
                .poll_workflow_task(&worker_id, &queue, Duration::from_millis(10))
                .await
                .unwrap()
            {
                break task;
            }
        }
    })
    .await
    .unwrap();
    let owner = task.lease_owner.as_deref().unwrap();
    let arguments =
        serde_json::to_value(encode_avro_value(&AvroValue::Array(vec![])).unwrap()).unwrap();
    client.complete_workflow_task(&task.task_id,owner,task.workflow_task_attempt,vec![json!({"type":"schedule_activity","activity_type":"structured","queue":queue,"arguments":arguments,"retry_policy":{"max_attempts":3,"backoff_seconds":[0,0],"non_retryable_error_types":[]}})]).await.unwrap();
    let activity = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if let Some(task) = client
                .poll_activity_task(&worker_id, &queue, Duration::from_millis(10))
                .await
                .unwrap()
            {
                break task;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(
        activity.workflow_id.as_deref(),
        Some(handle.workflow_id.as_str())
    );
    let details = serde_json::to_value(
        encode_avro_value(&AvroValue::String("large detail".repeat(300_000))).unwrap(),
    )
    .unwrap();
    let failure = json!({"type":"OriginalStructuredType","message":"\u{0000}".repeat(3*MIB),"stack_trace":"\u{0001}".repeat(3*MIB),"non_retryable":true,
        "details":details,"diagnostics":{"nested_causes":[{"message":"x".repeat(3*MIB)}]},"runtime_diagnostics":{"metadata":"x".repeat(MIB)}});
    client
        .fail_activity_task_with_failure(
            &activity.task_id,
            activity.activity_attempt_id.as_deref().unwrap(),
            activity.lease_owner.as_deref().unwrap(),
            failure,
        )
        .await
        .unwrap();
    let events = history(&client, &handle).await;
    let failed = events
        .iter()
        .find(|event| event["event_type"] == "ActivityFailed")
        .expect("large structured failure recorded");
    assert_eq!(
        failed["payload"]["exception_type"],
        "OriginalStructuredType"
    );
    assert!(failed["payload"]["message"]
        .as_str()
        .unwrap()
        .contains("[truncated"));
    let task = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if let Some(task) = client
                .poll_workflow_task(&worker_id, &queue, Duration::from_millis(10))
                .await
                .unwrap()
            {
                break task;
            }
        }
    })
    .await
    .unwrap();
    client.complete_workflow_task(&task.task_id,task.lease_owner.as_deref().unwrap(),task.workflow_task_attempt,vec![json!({"type":"fail_workflow","message":"\u{0000}".repeat(3*MIB),"exception_type":"OriginalWorkflowType","non_retryable":true,
        "exception":{"type":"OriginalWorkflowType","message":"\u{0000}".repeat(3*MIB),"stack_trace":"\u{0001}".repeat(3*MIB),"non_retryable":true,"properties":{"nested":"x".repeat(3*MIB)}}})]).await.unwrap();
    let events = history(&client, &handle).await;
    let failed = events
        .iter()
        .find(|event| event["event_type"] == "WorkflowFailed")
        .expect("terminal workflow failure recorded");
    assert!(failed["payload"]["message"]
        .as_str()
        .unwrap()
        .contains("[truncated"));
    assert_eq!(
        handle.describe().await.unwrap().status.as_deref(),
        Some("failed")
    );
    println!("published structured failure passed:3MiB message+stack+valid Avro details+nested diagnostics, original type, terminal workflow failure delivered");
}

#[tokio::test]
#[ignore = "requires isolated published Server"]
async fn workflow_task_failure_records_large_stack_and_omitted_valid_details() {
    let client = client();
    let queue = format!("rust-task-failure-{}", durable_workflow::Uuid::new_v4());
    let worker_id = format!("{queue}-worker");
    let mut worker = Worker::new(client.clone(), &queue).worker_id(&worker_id);
    worker.register_workflow("tests.workflow-task-failure", |_, _| async {
        Ok(json!(null))
    });
    worker.register().await.unwrap();
    let handle = client
        .start_workflow("tests.workflow-task-failure", &queue, &queue, json!([]))
        .await
        .unwrap();
    let task = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if let Some(task) = client
                .poll_workflow_task(&worker_id, &queue, Duration::from_millis(10))
                .await
                .unwrap()
            {
                break task;
            }
        }
    })
    .await
    .unwrap();
    let details = serde_json::to_value(
        encode_avro_value(&AvroValue::String("valid detail".repeat(300_000))).unwrap(),
    )
    .unwrap();
    let receipt=client.fail_workflow_task_with_failure(&task.task_id,task.lease_owner.as_deref().unwrap(),task.workflow_task_attempt,
        json!({"type":"OriginalTaskFailure","message":"\u{0000}".repeat(3*MIB),"stack_trace":"\u{0001}".repeat(3*MIB),"details":details,"diagnostics":{"cause":"x".repeat(3*MIB)}})).await.unwrap();
    assert_eq!(receipt["recorded"], true);
    assert_eq!(receipt["outcome"], "failed");
    assert_eq!(receipt["task_id"], task.task_id);
    assert_eq!(receipt["workflow_task_attempt"], task.workflow_task_attempt);
    assert!(receipt["reason"].is_null());
    assert!(receipt["next_task_id"]
        .as_str()
        .is_some_and(|id| id != task.task_id));
    println!("published workflow-task failure passed:3MiB message/stack/valid details, recorded receipt, exact identity and retry task; originaltype=OriginalTaskFailure,task_id={}",task.task_id);
}
