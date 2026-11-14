//! Published-package observation of local retry delays for shared issue #219.
//! The caller owns the isolated Server, exact package/producer proof and cleanup.
use durable_workflow::{
    json, ActivityError, ActivityRetryPolicy, Client, LocalActivityOptions, Value, Worker,
    WorkflowResultOptions,
};
use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

fn retry_policy(intervals: &[u64]) -> ActivityRetryPolicy {
    ActivityRetryPolicy::new(5)
        .backoff_intervals(intervals.iter().copied().map(Duration::from_secs))
}

fn worker(
    client: &Client,
    queue: &str,
    kind: &str,
    intervals: Vec<u64>,
    observations: Arc<Mutex<Vec<(u64, Instant)>>>,
    replacement: bool,
) -> Worker {
    let mut worker = Worker::new(client.clone(), queue)
        .worker_id(format!("{queue}-{replacement}"))
        .local_activities(true)
        .poll_timeout(Duration::from_millis(100));
    worker.register_workflow(kind, move |ctx, _| {
        let policy = retry_policy(&intervals);
        async move {
            let value = ctx
                .local_activity_with_options(
                    "retry-probe",
                    LocalActivityOptions::new()
                        .retry_policy(policy)
                        .start_to_close_timeout(Duration::from_secs(3))
                        .schedule_to_close_timeout(Duration::from_secs(30)),
                    json!([]),
                )
                .await?;
            ctx.wait_signal("finish").await?;
            Ok(value)
        }
    });
    worker.register_activity("retry-probe", move |ctx, _| {
        observations
            .lock()
            .unwrap()
            .push((ctx.attempt_number, Instant::now()));
        assert!(
            !replacement,
            "committed local results must not call the handler"
        );
        async move {
            if ctx.attempt_number < 5 {
                return Err(ActivityError::new("RetryProbe", "declared retry").into());
            }
            Ok(json!([5]))
        }
    });
    worker
}

fn activity_events(history: &Value) -> Vec<&Value> {
    history["history_events"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|event| {
            matches!(
                event["type"].as_str(),
                Some(
                    "ActivityScheduled"
                        | "ActivityStarted"
                        | "ActivityRetryScheduled"
                        | "ActivityCompleted"
                )
            )
        })
        .collect()
}

fn verify_reports(
    history: &Value,
    workflow: &str,
    run: &str,
    expected: &[u64],
    terminal: bool,
) -> Vec<Value> {
    assert_eq!(history["workflow"]["instance_id"], workflow);
    assert_eq!(history["workflow"]["run_id"], run);
    assert_eq!(history["workflow"]["details_state"], "retained");
    assert_eq!(history["history_complete"], terminal);
    let events = activity_events(history);
    let scheduled: Vec<_> = events
        .iter()
        .filter(|event| event["type"] == "ActivityScheduled")
        .collect();
    let completed: Vec<_> = events
        .iter()
        .filter(|event| event["type"] == "ActivityCompleted")
        .collect();
    assert_eq!(scheduled.len(), 1);
    assert_eq!(completed.len(), 1);
    let execution = scheduled[0]["payload"]["activity_execution_id"]
        .as_str()
        .unwrap();
    assert!(!execution.is_empty());
    for event in &events {
        let payload = &event["payload"];
        assert_eq!(payload["sequence"], 1);
        assert_eq!(payload["activity_execution_id"], execution);
        assert_eq!(payload["execution_mode"], "local");
        assert_eq!(payload["local_activity"], true);
    }
    let starts: Vec<_> = events
        .iter()
        .filter(|event| event["type"] == "ActivityStarted")
        .collect();
    assert_eq!(starts.len(), 5);
    let mut attempts = BTreeSet::new();
    for (index, event) in starts.iter().enumerate() {
        assert_eq!(event["payload"]["attempt_number"], index + 1);
        let id = event["payload"]["activity_attempt_id"].as_str().unwrap();
        assert!(!id.is_empty() && attempts.insert(id));
    }
    let retries: Vec<_> = events
        .iter()
        .filter(|event| event["type"] == "ActivityRetryScheduled")
        .collect();
    assert_eq!(retries.len(), expected.len());
    assert_eq!(expected.len(), 4);
    for (index, (event, delay)) in retries.iter().zip(expected).enumerate() {
        let payload = &event["payload"];
        assert_eq!(payload["retry_after_attempt"], index + 1);
        assert_eq!(
            payload["retry_after_attempt_id"],
            starts[index]["payload"]["activity_attempt_id"]
        );
        assert_eq!(payload["retry_backoff_seconds"].as_u64(), Some(*delay));
    }
    assert_eq!(completed[0]["payload"]["attempt_number"], 5);
    retries
        .into_iter()
        .map(|event| event["payload"].clone())
        .collect()
}

fn verify_gaps(observations: &[(u64, Instant)], expected: &[u64]) -> Vec<u128> {
    assert_eq!(observations.len(), 5);
    assert_eq!(expected.len(), 4);
    for (index, (attempt, _)) in observations.iter().enumerate() {
        assert_eq!(*attempt, (index + 1) as u64);
    }
    observations
        .windows(2)
        .zip(expected)
        .map(|(pair, seconds)| {
            let gap = pair[1].1.duration_since(pair[0].1);
            assert!(
                gap >= Duration::from_secs(*seconds),
                "retry ran before its declared delay"
            );
            gap.as_millis()
        })
        .collect()
}

#[tokio::test]
#[ignore = "requires an exact published SDK and isolated qualified Server tuple"]
async fn published_local_retries_repeat_last_and_replay_committed_results() {
    assert_eq!(
        std::env::var("DURABLE_WORKFLOW_LOCAL_ISOLATED").as_deref(),
        Ok("1")
    );
    let origin = std::env::var("DURABLE_WORKFLOW_SERVER_URL").unwrap();
    assert!(origin
        .strip_prefix("http://127.0.0.1:")
        .and_then(|port| port.parse::<u16>().ok())
        .is_some_and(|port| port > 0));
    let expected_version = std::env::var("DURABLE_WORKFLOW_EXPECT_SERVER_VERSION").unwrap();
    assert!(!expected_version.is_empty());
    let client = Client::builder(origin)
        .namespace("default")
        .timeout(Duration::from_secs(3))
        .token(Some(std::env::var("DURABLE_WORKFLOW_AUTH_TOKEN").unwrap()))
        .build()
        .unwrap();
    let cluster = client.cluster_info().await.unwrap();
    assert_eq!(cluster["implementation"], "rust");
    assert_eq!(cluster["version"], expected_version);
    for (name, intervals, expected) in [
        ("short", vec![1, 2], vec![1, 2, 2, 2]),
        ("empty", vec![], vec![0, 0, 0, 0]),
    ] {
        tokio::time::timeout(Duration::from_secs(35), async {
            let id = format!("rust-retry-{name}-{}", durable_workflow::Uuid::new_v4());
            let kind = format!("qualification.local-retry.{id}");
            let observations = Arc::new(Mutex::new(Vec::new()));
            let original = worker(
                &client,
                &id,
                &kind,
                intervals.clone(),
                observations.clone(),
                false,
            );
            original.register().await.unwrap();
            let handle = client
                .start_workflow(&kind, &id, &id, json!([]))
                .await
                .unwrap();
            let run = handle.run_id.as_deref().unwrap();
            assert!(original.run_once().await.unwrap() > 0);
            let committed = client.export_workflow_history(&id, run).await.unwrap();
            let reports = verify_reports(&committed, &id, run, &expected, false);
            let gaps = verify_gaps(&observations.lock().unwrap(), &expected);
            client
                .deregister_worker_registration(&format!("{id}-false"))
                .await
                .unwrap();
            let replacement = worker(&client, &id, &kind, intervals, observations.clone(), true);
            replacement.register().await.unwrap();
            handle.signal_selected_run("finish", json!([])).await.unwrap();
            let mut terminal = false;
            for _ in 0..20 {
                replacement.run_once().await.unwrap();
                let description = handle.describe_selected_run().await.unwrap();
                if description.is_terminal() {
                    assert_eq!(description.status.as_deref(), Some("completed"));
                    terminal = true;
                    break;
                }
            }
            assert!(
                terminal,
                "replacement did not complete within its polling bound"
            );
            assert_eq!(
                handle
                    .result_selected_run(WorkflowResultOptions::default())
                    .await
                    .unwrap(),
                json!([5])
            );
            assert_eq!(observations.lock().unwrap().len(), 5);
            let final_history = client.export_workflow_history(&id, run).await.unwrap();
            assert_eq!(
                verify_reports(&final_history, &id, run, &expected, true),
                reports
            );
            let prefix = committed["history_events"].as_array().unwrap();
            assert_eq!(
                &final_history["history_events"].as_array().unwrap()[..prefix.len()],
                prefix
            );
            client
                .deregister_worker_registration(&format!("{id}-true"))
                .await
                .unwrap();
            let report = json!({"case":name,"workflow_id":id,"run_id":run,
                "expected_backoff_seconds":expected,"callback_gap_ms":gaps,"retry_reports":reports,
                "callback_attempts":5,"callbacks_after_replay":0,"history":final_history});
            let text = report.to_string();
            assert!(text.len() <= 512 * 1024);
            println!("LOCAL_RETRY_TIMING {text}");
        })
        .await
        .expect("one local retry case exceeded its fixed bound");
    }
}

#[test]
fn callback_observer_rejects_zero_tail_and_accepts_repeat_last() {
    let start = Instant::now();
    let observed = |times: &[u64]| {
        times
            .iter()
            .enumerate()
            .map(|(i, seconds)| ((i + 1) as u64, start + Duration::from_secs(*seconds)))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        verify_gaps(&observed(&[0, 1, 3, 5, 7]), &[1, 2, 2, 2]),
        vec![1000, 2000, 2000, 2000]
    );
    assert!(
        std::panic::catch_unwind(|| verify_gaps(&observed(&[0, 1, 3, 3, 3]), &[1, 2, 2, 2]))
            .is_err()
    );
    assert_eq!(
        verify_gaps(&observed(&[0, 0, 0, 0, 0]), &[0; 4]),
        vec![0; 4]
    );
}

#[test]
fn committed_reports_reject_zero_tail_and_remote_activity_mixups() {
    let mut events = vec![json!({"type":"ActivityScheduled","payload":{
        "sequence":1,"activity_execution_id":"local-id","execution_mode":"local","local_activity":true}})];
    for number in 1..=5 {
        let payload = json!({"sequence":1,"activity_execution_id":"local-id","execution_mode":"local",
            "local_activity":true,"attempt_number":number,"activity_attempt_id":format!("attempt-{number}")});
        events.push(json!({"type":"ActivityStarted","payload":payload}));
        if number < 5 {
            let mut retry = events.last().unwrap()["payload"].clone();
            retry["retry_after_attempt"] = json!(number);
            retry["retry_after_attempt_id"] = json!(format!("attempt-{number}"));
            retry["retry_backoff_seconds"] = json!(if number == 1 { 1 } else { 2 });
            events.push(json!({"type":"ActivityRetryScheduled","payload":retry}));
        }
    }
    let last = events.last().unwrap()["payload"].clone();
    events.push(json!({"type":"ActivityCompleted","payload":last}));
    let accepted = json!({"workflow":{"instance_id":"workflow","run_id":"run","details_state":"retained"},
        "history_complete":true,"history_events":events});
    assert_eq!(
        verify_reports(&accepted, "workflow", "run", &[1, 2, 2, 2], true).len(),
        4
    );
    let mut zero_tail = accepted.clone();
    for event in zero_tail["history_events"].as_array_mut().unwrap() {
        if event["type"] == "ActivityRetryScheduled"
            && event["payload"]["retry_after_attempt"].as_u64().unwrap() >= 3
        {
            event["payload"]["retry_backoff_seconds"] = json!(0);
        }
    }
    assert!(std::panic::catch_unwind(|| verify_reports(
        &zero_tail,
        "workflow",
        "run",
        &[1, 2, 2, 2],
        true
    ))
    .is_err());
    let mut remote = accepted;
    remote["history_events"][0]["payload"]["execution_mode"] = json!("remote");
    assert!(std::panic::catch_unwind(|| verify_reports(
        &remote,
        "workflow",
        "run",
        &[1, 2, 2, 2],
        true
    ))
    .is_err());
}
