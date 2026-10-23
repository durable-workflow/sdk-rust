//! Public APIs exercised unchanged by normal installed consumers.
#![cfg(feature = "metrics-prometheus")]
use durable_workflow::{
    json, metrics::PrometheusMetrics, Client, Worker, WorkerRetryPolicy, WorkflowHistoryOptions,
    WorkflowListOptions,
};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

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
    async fn start(
        handler: impl Fn(&str) -> Option<(u16, serde_json::Value)> + Send + 'static,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                while !bytes.windows(4).any(|v| v == b"\r\n\r\n") {
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
    fn client(&self, recorder: Arc<PrometheusMetrics>) -> Client {
        Client::builder(&self.url)
            .metrics_recorder(recorder)
            .build()
            .unwrap()
    }
}
fn text(recorder: &PrometheusMetrics) -> String {
    String::from_utf8(recorder.encode().unwrap()).unwrap()
}
fn values(text: &str, name: &str) -> Vec<f64> {
    text.lines()
        .filter(|line| line.starts_with(&format!("{name}{{")))
        .map(|line| line.rsplit_once(' ').unwrap().1.parse().unwrap())
        .collect()
}

#[tokio::test]
async fn installed_transport_retry_emits_one_counter_and_histogram_observation() {
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = calls.clone();
    let server = Server::start(move |_| {
        if seen.fetch_add(1, Ordering::SeqCst) == 0 {
            None
        } else {
            Some((200, json!({"task":null})))
        }
    })
    .await;
    let recorder = Arc::new(PrometheusMetrics::new().unwrap());
    assert!(server
        .client(recorder.clone())
        .poll_workflow_task_response(
            "synthetic-worker",
            "bounded-queue",
            Duration::from_millis(10)
        )
        .await
        .unwrap()
        .task
        .is_none());
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let text = text(&recorder);
    assert_eq!(
        values(&text, "durable_workflow_client_requests_total"),
        vec![1.0]
    );
    assert_eq!(
        values(
            &text,
            "durable_workflow_client_request_duration_seconds_count"
        ),
        vec![1.0]
    );
    assert!(text.contains("plane=\"worker\""));
    assert!(!text.contains("synthetic-worker"));
}

#[tokio::test]
async fn installed_backpressure_keeps_http_error_and_worker_outcome_distinct() {
    let server=Server::start(|_|Some((429,json!({"poll_status":"long_poll_capacity_exhausted","retryable":true,"retry_after_seconds":0})))).await;
    let recorder = Arc::new(PrometheusMetrics::new().unwrap());
    let worker = Worker::new(server.client(recorder.clone()), "bounded-queue").retry_policy(
        WorkerRetryPolicy {
            max_retries: 0,
            initial_backoff: Duration::from_millis(1),
            ..Default::default()
        },
    );
    assert_eq!(worker.run_once().await.unwrap(), 0);
    let text = text(&recorder);
    assert!(text.contains("outcome=\"backpressure\""));
    assert!(text.contains("outcome=\"http_error\""));
    assert!(text.contains("status_code=\"429\""));
    assert_eq!(
        values(&text, "durable_workflow_worker_polls_total")
            .iter()
            .sum::<f64>(),
        2.0
    );
    assert_eq!(
        values(&text, "durable_workflow_worker_poll_duration_seconds_count")
            .iter()
            .sum::<f64>(),
        2.0
    );
    assert_eq!(
        values(&text, "durable_workflow_client_requests_total")
            .iter()
            .sum::<f64>(),
        2.0
    );
}

#[tokio::test]
async fn installed_accepted_stop_retains_original_http_status_and_error_outcome() {
    let server = Server::start(|_| {
        Some((
            409,
            json!({"task":null,"poll_status":"stopped","reason":"worker_stopped"}),
        ))
    })
    .await;
    let recorder = Arc::new(PrometheusMetrics::new().unwrap());
    assert!(server
        .client(recorder.clone())
        .poll_workflow_task_response("worker", "bounded-queue", Duration::from_millis(10))
        .await
        .unwrap()
        .outcome()
        .should_stop());
    let text = text(&recorder);
    assert!(text.contains("status_code=\"409\""));
    assert!(text.contains("outcome=\"http_error\""));
}

#[tokio::test]
async fn installed_iterator_counts_pages_and_history_labels_hide_identity_and_query() {
    let pages = Arc::new(AtomicUsize::new(0));
    let seen = pages.clone();
    let server=Server::start(move |path|Some((200,if path.contains("/history") {
        let parts:Vec<_>=path.split('?').next().unwrap().split('/').collect();
        json!({"workflow_id":parts[3],"run_id":parts[5],"events":[],"next_page_token":null})
    }else{json!({"workflows":[],"next_page_token":if seen.fetch_add(1,Ordering::SeqCst)==0 {Some("synthetic-secret-token")}else{None}})}))).await;
    let recorder = Arc::new(PrometheusMetrics::new().unwrap());
    let client = server.client(recorder.clone());
    let mut iter =
        client.iter_workflows(WorkflowListOptions::new().query("OrderRef = 'synthetic-secret'"));
    assert!(recorder.encode().unwrap().is_empty());
    assert!(iter.next().await.unwrap().is_none());
    assert_eq!(pages.load(Ordering::SeqCst), 2);
    for id in ["one", "two"] {
        client
            .get_workflow_history(
                &format!("workflow-secret-{id}"),
                &format!("run-secret-{id}"),
                WorkflowHistoryOptions::new().next_page_token("synthetic-secret-token"),
            )
            .await
            .unwrap();
    }
    let text = text(&recorder);
    assert!(!text.contains("secret"));
    assert!(text.contains("/workflows/{workflow_id}/runs/{run_id}/history"));
    assert_eq!(
        values(&text, "durable_workflow_client_requests_total"),
        vec![2.0, 2.0]
    );
    assert_eq!(
        values(
            &text,
            "durable_workflow_client_request_duration_seconds_count"
        ),
        vec![2.0, 2.0]
    );
}
