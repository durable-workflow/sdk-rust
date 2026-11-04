use durable_workflow::{
    json, Client, Error, Value, WorkflowActivity, WorkflowActivityAttempt, WorkflowActivityList,
};
use std::sync::{Arc, Mutex};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

#[derive(Clone, Debug)]
struct Request {
    method: String,
    path: String,
    headers: std::collections::HashMap<String, String>,
}

struct Server {
    url: String,
    requests: Arc<Mutex<Vec<Request>>>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Server {
    async fn start(status: u16, document: Value) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured = requests.clone();
        let task = tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                while !bytes.windows(4).any(|window| window == b"\r\n\r\n") {
                    let mut buffer = [0; 4096];
                    let count = stream.read(&mut buffer).await.unwrap();
                    assert_ne!(count, 0);
                    bytes.extend_from_slice(&buffer[..count]);
                }
                let header = String::from_utf8(bytes).unwrap();
                let mut first = header.lines().next().unwrap().split_whitespace();
                let request = Request {
                    method: first.next().unwrap().into(),
                    path: first.next().unwrap().into(),
                    headers: header
                        .lines()
                        .skip(1)
                        .filter_map(|line| line.split_once(':'))
                        .map(|(name, value)| (name.to_ascii_lowercase(), value.trim().into()))
                        .collect(),
                };
                let content_length = request
                    .headers
                    .get("content-length")
                    .map(|value| value.parse::<usize>().unwrap())
                    .unwrap_or(0);
                let header_end = header.find("\r\n\r\n").unwrap() + 4;
                let mut received = header.len() - header_end;
                while received < content_length {
                    let mut buffer = [0; 4096];
                    let count = stream.read(&mut buffer).await.unwrap();
                    assert_ne!(count, 0);
                    received += count;
                }
                captured.lock().unwrap().push(request);
                let body = document.to_string();
                let reply = format!("HTTP/1.1 {status} fixture\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}", body.len());
                stream.write_all(reply.as_bytes()).await.unwrap();
                stream.shutdown().await.unwrap();
            }
        });
        Self {
            url,
            requests,
            task,
        }
    }

    fn client(&self) -> Client {
        Client::builder(format!("{}/proxy", self.url))
            .namespace("tenant-a")
            .control_token(Some("synthetic-control".into()))
            .worker_token(Some("synthetic-worker".into()))
            .build()
            .unwrap()
    }

    async fn read(&self) -> durable_workflow::Result<WorkflowActivityList> {
        self.client()
            .get_run_activities("workflow/one", "run?one")
            .await
    }
}

fn response(items: Value) -> Value {
    json!({"workflow_id":"workflow/one", "run_id":"run?one", "activities":items})
}

fn full_document() -> Value {
    serde_json::from_str(include_str!("fixtures/run-activities.json")).unwrap()
}

#[tokio::test]
async fn exact_run_route_returns_typed_activity_attempt_and_authority() {
    let server = Server::start(200, full_document()).await;
    let view = server.read().await.unwrap();
    assert_eq!(view.workflow_id, "workflow/one");
    assert_eq!(view.run_id, "run?one");
    let item: &WorkflowActivity = &view.activities[0];
    assert_eq!(item.id.as_deref(), Some("execution-1"));
    assert_eq!(item.activity_type.as_deref(), Some("invoice"));
    assert_eq!(item.activity_class.as_deref(), Some("InvoiceActivity"));
    assert_eq!(item.sequence, Some(4));
    assert_eq!(item.status.as_deref(), Some("running"));
    assert_eq!(item.attempt_count, Some(2));
    assert_eq!(item.local_activity, Some(false));
    assert_eq!(item.diagnostic_only, Some(false));
    assert_eq!(item.history_authority.as_deref(), Some("typed_history"));
    assert_eq!(
        item.last_heartbeat_progress,
        Some(json!({"details":{"part":3}}))
    );
    assert_eq!(item.parallel_group_path.as_ref().unwrap()[0]["index"], 1);
    assert_eq!(
        item.retry_policy,
        Some(json!({"snapshot_version":1,"backoff_seconds":[0]}))
    );
    let attempt: &WorkflowActivityAttempt = &item.attempts.as_ref().unwrap()[0];
    assert_eq!(attempt.id.as_deref(), Some("attempt-2"));
    assert_eq!(attempt.attempt_number, Some(2));
    assert_eq!(attempt.can_continue, Some(true));
    assert_eq!(attempt.cancel_requested, Some(false));
    assert_eq!(
        attempt.last_heartbeat_at.as_deref(),
        Some("2026-11-04T12:00:05Z")
    );
    assert_eq!(
        attempt.last_heartbeat_progress,
        item.last_heartbeat_progress
    );
    assert_eq!(item.raw["future_metadata"], json!({"preserve":[true,null]}));
    // All presented fields retain their names and JSON types on the typed view.
    let serialized = serde_json::to_value(item).unwrap();
    for (key, expected) in full_document()["activities"][0].as_object().unwrap() {
        assert_eq!(&serialized[key], expected, "presented field {key}");
    }
    let requests = server.requests.lock().unwrap();
    assert_eq!(
        requests.len(),
        1,
        "no discovery or standalone-activity request"
    );
    let request = &requests[0];
    assert_eq!(request.method, "GET");
    assert_eq!(
        request.path,
        "/proxy/api/workflows/workflow%2Fone/runs/run%3Fone/activities"
    );
    assert_eq!(request.headers["x-namespace"], "tenant-a");
    assert_eq!(request.headers["authorization"], "Bearer synthetic-control");
    assert_eq!(
        request.headers["x-durable-workflow-control-plane-version"],
        "2"
    );
    assert!(!request
        .headers
        .contains_key("x-durable-workflow-protocol-version"));
}

#[tokio::test]
async fn empty_unavailable_and_explicit_zero_or_false_remain_distinct() {
    let server = Server::start(200, response(json!([]))).await;
    assert!(server.read().await.unwrap().activities.is_empty());
    let server = Server::start(
        200,
        response(json!([
            {}, {"attempts":[]}, {"attempts":[{}]},
            {"attempt_count":0,"local_activity":false,"diagnostic_only":false,
             "attempts":[{"attempt_number":0,"can_continue":false,"cancel_requested":false}]}
        ])),
    )
    .await;
    let items = server.read().await.unwrap().activities;
    assert_eq!(items[0].id, None);
    assert_eq!(items[0].status, None);
    assert_eq!(items[0].attempt_count, None);
    assert_eq!(items[0].diagnostic_only, None);
    assert_eq!(items[0].last_heartbeat_at, None);
    assert!(items[0].attempts.is_none());
    assert!(items[1].attempts.as_ref().unwrap().is_empty());
    let unknown = &items[2].attempts.as_ref().unwrap()[0];
    assert_eq!(unknown.can_continue, None);
    assert_eq!(unknown.cancel_requested, None);
    assert_eq!(items[3].attempt_count, Some(0));
    assert_eq!(items[3].local_activity, Some(false));
    assert_eq!(
        items[3].attempts.as_ref().unwrap()[0].can_continue,
        Some(false)
    );
}

#[tokio::test]
async fn diagnostic_unsupported_and_future_states_are_not_reinterpreted() {
    let server = Server::start(200, response(json!([
        {"status":"unsupported","diagnostic_only":true,
         "history_authority":"unsupported_terminal_without_history",
         "history_unsupported_reason":"terminal_activity_row_without_typed_history"},
        {"status":"future-paused","history_authority":"future-authority", "future_field":[1,false,null]}
    ]))).await;
    let items = server.read().await.unwrap().activities;
    assert_eq!(items[0].status.as_deref(), Some("unsupported"));
    assert_eq!(items[0].diagnostic_only, Some(true));
    assert_eq!(
        items[0].history_unsupported_reason.as_deref(),
        Some("terminal_activity_row_without_typed_history")
    );
    assert_eq!(items[1].status.as_deref(), Some("future-paused"));
    assert_eq!(
        items[1].history_authority.as_deref(),
        Some("future-authority")
    );
    assert_eq!(items[1].raw["future_field"], json!([1, false, null]));
}

#[tokio::test]
async fn required_root_fields_and_selected_run_identity_cannot_be_fabricated() {
    let mut documents = vec![Value::Null, json!([])];
    for field in ["workflow_id", "run_id", "activities"] {
        let mut missing = response(json!([]));
        missing.as_object_mut().unwrap().remove(field);
        documents.push(missing);
        let mut null = response(json!([]));
        null[field] = Value::Null;
        documents.push(null);
    }
    for field in ["workflow_id", "run_id"] {
        let mut wrong = response(json!([]));
        wrong[field] = json!("private-marker");
        documents.push(wrong);
    }
    for document in documents {
        let server = Server::start(200, document).await;
        let error = server.read().await.unwrap_err();
        assert!(matches!(error, Error::WorkflowActivityMetadata(_)));
        assert!(!error.to_string().contains("private-marker"));
        assert_eq!(server.requests.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn malformed_known_types_refuse_without_copying_values_into_errors() {
    let items = [
        json!("private-marker"),
        json!({"id":1}),
        json!({"status":1}),
        json!({"attempt_count":true}),
        json!({"sequence":"private-marker"}),
        json!({"local_activity":"false"}),
        json!({"diagnostic_only":0}),
        json!({"attempts":{}}),
        json!({"attempts":["private-marker"]}),
        json!({"attempts":[{"can_continue":"true"}]}),
        json!({"attempts":[{"attempt_number":false}]}),
        json!({"history_event_types":[1]}),
        json!({"parallel_group_path":[false]}),
        json!({"last_heartbeat_progress":"private-marker"}),
        json!({"attempts":[{"last_heartbeat_progress":3}]}),
    ];
    for item in items {
        let server = Server::start(200, response(json!([item]))).await;
        let error = server.read().await.unwrap_err();
        assert!(matches!(error, Error::WorkflowActivityMetadata(_)));
        assert!(!error.to_string().contains("private-marker"));
    }
    for items in [Value::Null, json!({}), json!(true)] {
        let server = Server::start(200, response(items)).await;
        assert!(matches!(
            server.read().await,
            Err(Error::WorkflowActivityMetadata(_))
        ));
    }
}

#[tokio::test]
async fn structured_progress_and_unrecognized_inspection_values_keep_json_types() {
    let server = Server::start(200, response(json!([
        {"last_heartbeat_progress":[], "retry_policy":{"future_version":2,"disabled":false},
         "arguments":{"typed":"future"},"result":null,
         "attempts":[{"last_heartbeat_progress":{"details":{"zero":0,"false":false,"null":null}}}]}
    ]))).await;
    let item = server.read().await.unwrap().activities.remove(0);
    assert_eq!(item.last_heartbeat_progress, Some(json!([])));
    assert_eq!(item.raw["arguments"], json!({"typed":"future"}));
    assert_eq!(item.raw.get("result"), Some(&Value::Null));
    assert_eq!(
        item.attempts.as_ref().unwrap()[0].last_heartbeat_progress,
        Some(json!({"details":{"zero":0,"false":false,"null":null}}))
    );
}

#[tokio::test]
async fn route_http_errors_keep_the_existing_client_error_boundary() {
    for status in [401, 403, 404, 422, 503] {
        let server = Server::start(status, json!({"error":"fixture_refusal"})).await;
        match server.read().await.unwrap_err() {
            Error::Http { status: actual, .. } => assert_eq!(actual.as_u16(), status),
            error => panic!("expected HTTP refusal, got {error:?}"),
        }
        assert_eq!(server.requests.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn worker_credentials_cannot_authorize_a_control_plane_read() {
    let server = Server::start(200, response(json!([]))).await;
    let client = Client::builder(&server.url)
        .worker_token(Some("synthetic-worker".into()))
        .build()
        .unwrap();
    assert!(matches!(
        client.get_run_activities("workflow/one", "run?one").await,
        Err(Error::MissingRoleCredentials {
            role: "control",
            ..
        })
    ));
    assert!(server.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn handle_reads_its_selected_run_without_resolving_the_current_run() {
    let server = Server::start(200, full_document()).await;
    let handle = server
        .client()
        .start_workflow("fixture", "queue", "fixture-id", json!([]))
        .await
        .unwrap();
    let before = server.requests.lock().unwrap().len();
    let view = handle.get_activities().await.unwrap();
    assert_eq!(view.run_id, "run?one");
    let requests = server.requests.lock().unwrap();
    assert_eq!(
        requests.len(),
        before + 1,
        "the selected-run read makes exactly one additional request"
    );
    let request = requests.last().unwrap();
    assert_eq!(request.method, "GET");
    assert_eq!(
        request.path,
        "/proxy/api/workflows/workflow%2Fone/runs/run%3Fone/activities"
    );
}

#[tokio::test]
async fn handle_without_run_identity_refuses_before_activity_request() {
    let server = Server::start(200, full_document()).await;
    let mut handle = server
        .client()
        .start_workflow("fixture", "queue", "fixture-id", json!([]))
        .await
        .unwrap();
    handle.run_id = None;
    let before = server.requests.lock().unwrap().len();
    match handle.get_activities().await.unwrap_err() {
        Error::WorkflowActivityMetadata(error) => assert_eq!(error.reason, "missing_run_id"),
        other => panic!("expected selected-run refusal, got {other:?}"),
    }
    assert_eq!(
        server.requests.lock().unwrap().len(),
        before,
        "no implicit current-run lookup"
    );
}
