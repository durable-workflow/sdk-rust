use super::*;
use crate::{json, Value, Worker};
use std::sync::{atomic::AtomicUsize, Mutex};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

#[derive(Clone, Debug)]
struct Sample {
    name: String,
    method: &'static str,
    value: f64,
    tags: MetricTags,
}
#[derive(Default)]
struct Recorder(Mutex<Vec<Sample>>);
impl MetricsRecorder for Recorder {
    fn increment(&self, name: &str, value: u64, tags: &MetricTags) {
        self.0.lock().unwrap().push(Sample {
            name: name.into(),
            method: "increment",
            value: value as f64,
            tags: tags.clone(),
        });
    }
    fn record(&self, name: &str, value: f64, tags: &MetricTags) {
        self.0.lock().unwrap().push(Sample {
            name: name.into(),
            method: "record",
            value,
            tags: tags.clone(),
        });
    }
}
impl Recorder {
    fn samples(&self) -> Vec<Sample> {
        self.0.lock().unwrap().clone()
    }
    fn clear(&self) {
        self.0.lock().unwrap().clear();
    }
    fn assert_contract(&self) {
        use sha2::{Digest, Sha256};
        let bytes = include_bytes!("../../tests/fixtures/sdk-metrics-contract.json");
        assert_eq!(
            format!("{:x}", Sha256::digest(bytes)),
            "c456f4f434c601fa26f0caba07ef0fd29f548ca8f5fc29ba2f6430f01e32c0f0"
        );
        let contract: Value = serde_json::from_slice(bytes).unwrap();
        let samples = self.samples();
        assert!(!samples.is_empty());
        assert_eq!(samples.len() % 2, 0);
        for pair in samples.chunks_exact(2) {
            assert_eq!(pair[0].method, "increment");
            assert_eq!(pair[0].value, 1.0);
            assert_eq!(pair[1].method, "record");
            assert!(pair[1].value.is_finite() && pair[1].value >= 0.0);
            assert_eq!(pair[0].tags, pair[1].tags);
            let mut scopes = Vec::new();
            for sample in pair {
                let entry = contract["metrics"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|e| e["name"] == sample.name)
                    .unwrap();
                assert_eq!(entry["recorder_method"], sample.method);
                assert_eq!(
                    entry["type"],
                    if sample.method == "increment" {
                        "counter"
                    } else {
                        "histogram"
                    }
                );
                assert_eq!(
                    entry["unit"],
                    if sample.method == "increment" {
                        "1"
                    } else {
                        "s"
                    }
                );
                let expected: std::collections::BTreeSet<_> = entry["tags"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|v| v.as_str().unwrap())
                    .collect();
                assert_eq!(
                    sample
                        .tags
                        .keys()
                        .map(String::as_str)
                        .collect::<std::collections::BTreeSet<_>>(),
                    expected
                );
                scopes.push(entry["scope"].clone());
            }
            assert_eq!(scopes[0], scopes[1]);
        }
    }
}

struct Server {
    url: String,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Server {
    async fn start(handler: impl Fn(&str) -> Option<(u16, Value)> + Send + Sync + 'static) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                while !bytes.windows(4).any(|b| b == b"\r\n\r\n") {
                    let mut buffer = [0; 4096];
                    let n = stream.read(&mut buffer).await.unwrap();
                    if n == 0 {
                        break;
                    }
                    bytes.extend_from_slice(&buffer[..n]);
                }
                let text = String::from_utf8_lossy(&bytes);
                let path = text.split_whitespace().nth(1).unwrap();
                if let Some((status, body)) = handler(path) {
                    let body = body.to_string();
                    let response=format!("HTTP/1.1 {status} fixture\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",body.len());
                    stream.write_all(response.as_bytes()).await.unwrap();
                    stream.shutdown().await.unwrap();
                }
            }
        });
        Self { url, task }
    }
    fn client(&self, recorder: Arc<dyn MetricsRecorder>) -> Client {
        Client::builder(&self.url)
            .metrics_recorder(recorder)
            .token(Some("synthetic-secret".into()))
            .build()
            .unwrap()
    }
}

#[tokio::test]
async fn requests_match_pinned_contract_and_hide_ids_queries_and_unknown_routes() {
    let server = Server::start(|path| {
        Some((
            if path.contains("fail-secret") {
                403
            } else {
                200
            },
            json!({}),
        ))
    })
    .await;
    let recorder = Arc::new(Recorder::default());
    let client = server.client(recorder.clone());
    for path in [
        "/workflows/id-one/runs/run-one/history?query=synthetic-secret",
        "/workflows/id-two/runs/run-two/history?token=other-secret",
        "/worker/update-validation-tasks/task-one/approve",
        "/worker/update-validation-tasks/task-two/approve",
        "/worker/sessions/session-one/heartbeat",
        "/worker/sessions/session-two/heartbeat",
        "/workers/worker-one",
        "/workers/worker-two",
        "/unreviewed/secret-one",
        "/unreviewed/secret-two",
        "/workflows/fail-secret",
    ] {
        let _: Result<Value> = client
            .request_json_unresolved_with_timeout(
                reqwest::Method::GET,
                path,
                RequestProtocol::ControlPlane,
                None::<&Value>,
                Duration::from_secs(2),
            )
            .await;
    }
    recorder.assert_contract();
    let samples = recorder.samples();
    for pair in samples[..20].chunks_exact(4) {
        assert_eq!(pair[0].tags["route"], pair[2].tags["route"]);
    }
    for sample in &samples {
        let tags = format!("{:?}", sample.tags);
        assert!(!tags.contains("secret") && !tags.contains("-one") && !tags.contains("-two"));
        assert_eq!(sample.tags["plane"], "control");
    }
    assert_eq!(samples.last().unwrap().tags["outcome"], "http_error");
    assert_eq!(samples.last().unwrap().tags["status_code"], "403");
    assert_eq!(samples[16].tags["route"], "unknown");
}

#[tokio::test]
async fn a_transport_retry_is_one_logical_request_and_one_duration() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let seen = attempts.clone();
    let server = Server::start(move |_| {
        if seen.fetch_add(1, Ordering::SeqCst) == 0 {
            None
        } else {
            Some((200, json!({"task":null})))
        }
    })
    .await;
    let recorder = Arc::new(Recorder::default());
    let response = server
        .client(recorder.clone())
        .poll_workflow_task_response("worker-secret", "queue", Duration::from_millis(10))
        .await
        .unwrap();
    assert!(response.task.is_none());
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    recorder.assert_contract();
    let samples = recorder.samples();
    assert_eq!(samples.len(), 2);
    assert_eq!(samples[0].tags["plane"], "worker");
    assert_eq!(samples[0].tags["status_code"], "200");
}

#[tokio::test]
async fn worker_empty_error_and_backpressure_polls_use_inherited_or_overridden_recorder() {
    for (status, body, outcome) in [
        (200, json!({"task":null}), "empty"),
        (503, json!({"error":"fixture"}), "error"),
        (
            200,
            json!({"task":null,"poll_status":"backpressure"}),
            "backpressure",
        ),
    ] {
        let server = Server::start(move |_| Some((status, body.clone()))).await;
        let inherited = Arc::new(Recorder::default());
        let override_recorder = Arc::new(Recorder::default());
        let worker = Worker::new(server.client(inherited.clone()), "bounded-queue").retry_policy(
            crate::WorkerRetryPolicy {
                max_retries: 0,
                ..Default::default()
            },
        );
        let _ = worker.run_once().await;
        inherited.assert_contract();
        assert!(inherited
            .samples()
            .iter()
            .any(|s| s.name == WORKER_POLLS && s.tags["outcome"] == outcome));
        inherited.clear();
        let _ = worker
            .metrics_recorder(override_recorder.clone())
            .run_once()
            .await;
        override_recorder.assert_contract();
        assert!(override_recorder
            .samples()
            .iter()
            .all(|s| s.name.starts_with("durable_workflow_worker_")));
        inherited.assert_contract();
        assert!(inherited
            .samples()
            .iter()
            .all(|s| s.name.starts_with("durable_workflow_client_")));
    }
}

#[tokio::test]
async fn successful_and_failed_claims_emit_separate_task_outcomes() {
    for fail in [false, true] {
        let args =
            serde_json::to_value(crate::encode_payload(&json!([]), "avro").unwrap()).unwrap();
        let server=Server::start(move |path|Some((200,if path.ends_with("workflow-tasks/poll") {
            json!({"task":{"task_id":"workflow-task-secret","workflow_type":"sample","payload_codec":"avro","arguments":args,"history_events":[]}})
        }else if path.ends_with("activity-tasks/poll") {
            json!({"task":{"task_id":"activity-task-secret","activity_type":"sample","activity_attempt_id":"attempt-secret","payload_codec":"avro","arguments":args}})
        }else{json!({"recorded":true})}))).await;
        let recorder = Arc::new(Recorder::default());
        let mut worker = Worker::new(server.client(recorder.clone()), "bounded-queue");
        worker.register_workflow("sample", move |_, _| async move {
            if fail {
                Err(Error::WorkerLoop("synthetic-secret".into()))
            } else {
                Ok(json!("ok"))
            }
        });
        worker.register_activity("sample", move |_, _| async move {
            if fail {
                Err(Error::WorkerLoop("synthetic-secret".into()))
            } else {
                Ok(json!("ok"))
            }
        });
        assert_eq!(worker.run_once().await.unwrap(), 2);
        recorder.assert_contract();
        let tasks: Vec<_> = recorder
            .samples()
            .into_iter()
            .filter(|s| s.name == WORKER_TASKS)
            .collect();
        assert_eq!(tasks.len(), 2);
        for task in tasks {
            assert_eq!(
                task.tags["outcome"],
                if fail { "failed" } else { "completed" }
            );
            assert_eq!(task.tags["task_queue"], "bounded-queue");
        }
        assert!(!format!("{:?}", recorder.samples()).contains("synthetic-secret"));
    }
}

#[tokio::test]
async fn no_op_and_panicking_exporter_do_not_change_results() {
    struct Panics;
    impl MetricsRecorder for Panics {
        fn increment(&self, _: &str, _: u64, _: &MetricTags) {
            panic!("exporter fixture")
        }
        fn record(&self, _: &str, _: f64, _: &MetricTags) {
            panic!("exporter fixture")
        }
    }
    let server = Server::start(|_| Some((200, json!({"ok":true})))).await;
    assert_eq!(
        Client::new(&server.url).unwrap().health().await.unwrap(),
        json!({"ok":true})
    );
    assert_eq!(
        server.client(Arc::new(NoopMetrics)).health().await.unwrap(),
        json!({"ok":true})
    );
    assert_eq!(
        server.client(Arc::new(Panics)).health().await.unwrap(),
        json!({"ok":true})
    );
}

#[tokio::test]
async fn failed_transport_has_no_status_and_pagination_counts_each_page() {
    let server = Server::start(|_| Some((200, json!({})))).await;
    let url = server.url.clone();
    drop(server);
    let recorder = Arc::new(Recorder::default());
    assert!(Client::builder(url)
        .metrics_recorder(recorder.clone())
        .build()
        .unwrap()
        .health()
        .await
        .is_err());
    recorder.assert_contract();
    assert_eq!(recorder.samples()[0].tags["status_code"], "none");
    assert_eq!(recorder.samples()[0].tags["outcome"], "TransportError");
    recorder.clear();
    let pages = Arc::new(AtomicUsize::new(0));
    let seen = pages.clone();
    let server=Server::start(move |_|Some((200,json!({"workflows":[],"next_page_token":if seen.fetch_add(1,Ordering::SeqCst)==0 {Some("synthetic-token")}else{None}})))).await;
    let client = server.client(recorder.clone());
    let mut iterator =
        client.iter_workflows(crate::WorkflowListOptions::new().query("synthetic-secret"));
    assert!(recorder.samples().is_empty());
    assert!(iterator.next().await.unwrap().is_none());
    recorder.assert_contract();
    assert_eq!(recorder.samples().len(), 4);
    assert_eq!(pages.load(Ordering::SeqCst), 2);
    assert!(recorder
        .samples()
        .iter()
        .all(|s| s.tags["route"] == "/workflows"));
}

#[tokio::test]
async fn query_dispatch_success_and_failure_have_the_same_worker_contract() {
    for fail in [false, true] {
        let args =
            serde_json::to_value(crate::encode_payload(&json!([]), "avro").unwrap()).unwrap();
        let server=Server::start(move |path|Some((200,if path.ends_with("query-tasks/poll") {
            json!({"task":{"query_task_id":"query-secret","workflow_type":"sample","query_name":"state","payload_codec":"avro","workflow_arguments":args,"query_arguments":args,"history_events":[]}})
        }else{json!({"task":null,"recorded":true})}))).await;
        let recorder = Arc::new(Recorder::default());
        let mut worker = Worker::new(server.client(recorder.clone()), "bounded-queue");
        worker.register_workflow("sample", |_, _| async { Ok(json!("workflow")) });
        worker.register_query("sample", "state", move |_, _| async move {
            if fail {
                Err(Error::WorkerLoop("synthetic-secret".into()))
            } else {
                Ok(json!("ok"))
            }
        });
        assert_eq!(worker.run_once().await.unwrap(), 1);
        recorder.assert_contract();
        assert!(recorder.samples().iter().any(|s| s.name == WORKER_TASKS
            && s.tags["task_kind"] == "query"
            && s.tags["outcome"] == if fail { "failed" } else { "completed" }));
    }
}

#[cfg(feature = "metrics-prometheus")]
#[tokio::test]
async fn prometheus_adapter_exports_actual_request_counter_and_histogram() {
    let server = Server::start(|_| Some((200, json!({"ok":true})))).await;
    let recorder = Arc::new(PrometheusMetrics::new().unwrap());
    server.client(recorder.clone()).health().await.unwrap();
    let text = String::from_utf8(recorder.encode().unwrap()).unwrap();
    assert!(text.contains("# TYPE durable_workflow_client_requests_total counter"));
    assert!(text.contains("# TYPE durable_workflow_client_request_duration_seconds histogram"));
    assert!(text.contains("route=\"/health\""));
    assert!(text.contains("status_code=\"200\""));
    assert!(!text.contains("synthetic-secret"));
}
