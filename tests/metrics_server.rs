//! Installed-package-compatible acceptance of actual emitted client/worker metrics.
#![cfg(feature = "metrics-prometheus")]
use durable_workflow::{
    json,
    metrics::{MetricTags, MetricsRecorder, PrometheusMetrics},
    ActivityError, Client, Error, Worker, WorkflowResultOptions,
};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};

struct Recorder {
    samples: Mutex<Vec<(String, &'static str, f64, MetricTags)>>,
    prometheus: PrometheusMetrics,
}
impl MetricsRecorder for Recorder {
    fn increment(&self, name: &str, value: u64, tags: &MetricTags) {
        self.samples
            .lock()
            .unwrap()
            .push((name.into(), "increment", value as f64, tags.clone()));
        self.prometheus.increment(name, value, tags);
    }
    fn record(&self, name: &str, value: f64, tags: &MetricTags) {
        self.samples
            .lock()
            .unwrap()
            .push((name.into(), "record", value, tags.clone()));
        self.prometheus.record(name, value, tags);
    }
}

#[tokio::test]
#[ignore = "requires isolated published Server"]
async fn published_worker_emits_the_pinned_contract_and_prometheus_exposition() {
    assert_eq!(
        std::env::var("DURABLE_WORKFLOW_LOCAL_ISOLATED").as_deref(),
        Ok("1")
    );
    let recorder = Arc::new(Recorder {
        samples: Mutex::new(Vec::new()),
        prometheus: PrometheusMetrics::new().unwrap(),
    });
    let client = Client::builder(std::env::var("DURABLE_WORKFLOW_SERVER_URL").unwrap())
        .token(Some(std::env::var("DURABLE_WORKFLOW_AUTH_TOKEN").unwrap()))
        .metrics_recorder(recorder.clone())
        .build()
        .unwrap();
    client.health().await.unwrap();
    assert!(matches!(
        client.describe_workflow("metrics-missing-synthetic").await,
        Err(Error::Http { .. })
    ));
    let queue = "rust-metrics-acceptance".to_string();
    let workflow_prefix = format!("rust-metrics-{}", durable_workflow::Uuid::new_v4());
    let mut worker = Worker::new(client.clone(), &queue).poll_timeout(Duration::from_millis(10));
    worker.register_activity("metrics.activity", |_, args| async move {
        if args[0] == json!(true) {
            Err(ActivityError::new("MetricsFixture", "synthetic failure")
                .non_retryable(true)
                .into())
        } else {
            Ok(json!("ok"))
        }
    });
    worker.register_workflow("tests.metrics", |ctx, args| async move {
        match ctx.activity("metrics.activity", args).await {
            Ok(value) => Ok(value),
            Err(Error::ActivityFailed(_)) => Ok(json!("handled")),
            Err(error) => Err(error),
        }
    });
    worker.register().await.unwrap();
    let mut handles = Vec::new();
    for fail in [false, true] {
        handles.push((
            fail,
            client
                .start_workflow(
                    "tests.metrics",
                    &queue,
                    &format!("{workflow_prefix}-{fail}"),
                    json!([fail]),
                )
                .await
                .unwrap(),
        ));
    }
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let running = tokio::spawn(async move {
        worker
            .run_until(async {
                let _ = stopped.await;
            })
            .await
    });
    for (fail, handle) in handles {
        assert_eq!(
            handle
                .result(WorkflowResultOptions {
                    timeout: Duration::from_secs(45),
                    ..Default::default()
                })
                .await
                .unwrap(),
            if fail { json!("handled") } else { json!("ok") }
        );
    }
    stop.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(15), running)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let samples = recorder.samples.lock().unwrap();
    let contract: serde_json::Value =
        serde_json::from_slice(include_bytes!("fixtures/sdk-metrics-contract.json")).unwrap();
    let mut observed = BTreeMap::<String, usize>::new();
    let mut paired = BTreeMap::<(String, MetricTags), (usize, usize)>::new();
    for (name, method, value, tags) in samples.iter() {
        let entry = contract["metrics"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["name"] == *name)
            .unwrap();
        assert_eq!(entry["recorder_method"], *method);
        let expected: std::collections::BTreeSet<_> = entry["tags"]
            .as_array()
            .unwrap()
            .iter()
            .map(|tag| tag.as_str().unwrap())
            .collect();
        assert_eq!(
            tags.keys()
                .map(String::as_str)
                .collect::<std::collections::BTreeSet<_>>(),
            expected
        );
        assert!(value.is_finite() && *value >= 0.0);
        let counts = paired
            .entry((entry["scope"].as_str().unwrap().into(), tags.clone()))
            .or_default();
        if *method == "increment" {
            assert_eq!(*value, 1.0);
            counts.0 += 1;
        } else {
            counts.1 += 1;
        }
        *observed.entry(name.clone()).or_default() += 1;
        for (key, value) in tags {
            if key == "task_queue" {
                assert_eq!(value, &queue);
            } else {
                assert!(!value.contains("synthetic") && !value.contains("rust-metrics-"));
            }
        }
    }
    assert_eq!(observed.len(), 6, "all shared metrics must actually emit");
    assert!(paired
        .values()
        .all(|(counter, histogram)| counter == histogram));
    for outcome in ["completed", "failed"] {
        assert!(samples
            .iter()
            .any(|(name, _, _, tags)| name == "durable_workflow_worker_tasks"
                && tags.get("task_kind").map(String::as_str) == Some("activity")
                && tags.get("outcome").map(String::as_str) == Some(outcome)));
    }
    assert!(samples.iter().any(
        |(name, _, _, tags)| name == "durable_workflow_client_requests"
            && tags.get("outcome").map(String::as_str) == Some("http_error")
    ));
    let text = String::from_utf8(recorder.prometheus.encode().unwrap()).unwrap();
    for metric in contract["metrics"].as_array().unwrap() {
        let name = metric["name"].as_str().unwrap();
        assert!(text.contains(&format!(
            "# TYPE {name}{} {}",
            if metric["type"] == "counter" {
                "_total"
            } else {
                ""
            },
            metric["type"].as_str().unwrap()
        )));
    }
    assert!(!text.contains("synthetic failure") && !text.contains("metrics-missing-synthetic"));
    println!("actual published Server metrics passed: six shared names, paired counts/durations, activity success/failure, HTTP error, official Prometheus exposition");
}
