use durable_workflow::{
    json, worker_protocol_compatible, Client, Error, RawMethod, RawRequest, Value,
    CONTROL_PLANE_VERSION, WORKER_PROTOCOL_VERSION,
};
use std::sync::{Arc, Mutex};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

/// One captured HTTP request: request line, lowercase headers and body.
#[derive(Clone, Debug)]
struct Captured {
    line: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Captured {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }
    fn target(&self) -> &str {
        self.line.split_whitespace().nth(1).unwrap_or_default()
    }
}

struct Reply {
    status: &'static str,
    headers: Vec<(&'static str, String)>,
    body: String,
}

fn reply(status: &'static str, headers: &[(&'static str, &str)], body: &str) -> Reply {
    Reply {
        status,
        headers: headers.iter().map(|(k, v)| (*k, v.to_string())).collect(),
        body: body.to_string(),
    }
}

struct Server {
    url: String,
    requests: Arc<Mutex<Vec<Captured>>>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Serve the scripted replies in order, one per connection.
async fn serve(replies: Vec<Reply>) -> Server {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let requests = Arc::new(Mutex::new(Vec::new()));
    let observed = requests.clone();
    let task = tokio::spawn(async move {
        for reply in replies {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut raw = Vec::new();
            let header_end = loop {
                let mut buffer = [0; 4096];
                let count = stream.read(&mut buffer).await.unwrap();
                assert!(count > 0, "client closed before sending headers");
                raw.extend_from_slice(&buffer[..count]);
                if let Some(end) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
                    break end + 4;
                }
            };
            let head = String::from_utf8_lossy(&raw[..header_end]).to_string();
            let mut lines = head.lines();
            let line = lines.next().unwrap().to_string();
            let headers: Vec<(String, String)> = lines
                .filter_map(|l| l.split_once(':'))
                .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_string()))
                .collect();
            let length = headers
                .iter()
                .find(|(k, _)| k == "content-length")
                .map(|(_, v)| v.parse::<usize>().unwrap())
                .unwrap_or(0);
            while raw.len() < header_end + length {
                let mut buffer = [0; 4096];
                let count = stream.read(&mut buffer).await.unwrap();
                assert!(count > 0, "client closed before sending its body");
                raw.extend_from_slice(&buffer[..count]);
            }
            observed.lock().unwrap().push(Captured {
                line,
                headers,
                body: raw[header_end..header_end + length].to_vec(),
            });
            let mut response = format!(
                "HTTP/1.1 {}\r\ncontent-length: {}\r\nconnection: close\r\n",
                reply.status,
                reply.body.len()
            );
            for (name, value) in &reply.headers {
                response.push_str(&format!("{name}: {value}\r\n"));
            }
            response.push_str("\r\n");
            response.push_str(&reply.body);
            stream.write_all(response.as_bytes()).await.unwrap();
            stream.shutdown().await.unwrap();
        }
    });
    Server {
        url,
        requests,
        task,
    }
}

fn client(url: &str) -> Client {
    Client::builder(url)
        .control_token(Some("control-secret".into()))
        .worker_token(Some("worker-secret".into()))
        .namespace("orders")
        .build()
        .unwrap()
}

#[test]
fn path_segments_are_rawurlencoded_and_traversal_is_refused() {
    let request = RawRequest::get(["workflows", "a/b c%é?#", "runs", "run-1.2_~"]);
    assert_eq!(request.method(), RawMethod::Get);
    assert_eq!(
        request.path().unwrap(),
        "/workflows/a%2Fb%20c%25%C3%A9%3F%23/runs/run-1.2_~"
    );
    for segments in [vec![], vec![""], vec!["workflows", "."], vec!["..", "x"]] {
        assert!(matches!(
            RawRequest::get(segments).path(),
            Err(Error::InvalidRawRequest(_))
        ));
    }
}

#[tokio::test]
async fn invalid_paths_are_refused_before_any_connection() {
    let server = serve(Vec::new()).await;
    let error = client(&server.url)
        .control_plane_request(RawRequest::delete(["workflows", ".."]))
        .await
        .unwrap_err();
    assert!(matches!(error, Error::InvalidRawRequest(_)));
    assert!(server.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn control_plane_request_sends_identity_query_and_body_and_returns_bytes_unchanged() {
    // Unknown fields, key order, whitespace and float digits must survive.
    let body = "{\"zeta\":1, \"unknown\":{\"nested\":[true,null]},\"score\":1.4820142857142857}";
    let server = serve(vec![reply(
        "200 OK",
        &[
            ("content-type", "application/json"),
            ("x-durable-workflow-control-plane-version", "2"),
        ],
        body,
    )])
    .await;
    // A managed-namespace base URL keeps its path prefix.
    let base = format!("{}/api/runtime/v1/namespaces/ns-1", server.url);
    let response = client(&base)
        .control_plane_request(
            RawRequest::post(["workflows", "order/42", "signal", "approve"])
                .query("b", "2")
                .query("a", "x y&z")
                .json(json!({"input": [1, "two"], "extra": {"kept": true}})),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    assert!(response.is_success());
    assert_eq!(response.body_bytes(), body.as_bytes());
    assert_eq!(response.control_plane_version(), Some("2"));
    assert_eq!(response.header("content-type"), Some("application/json"));
    assert!(response
        .headers()
        .any(|(name, value)| name == "content-type" && value == b"application/json"));
    let decoded = response.json().unwrap();
    assert_eq!(decoded["unknown"], json!({"nested": [true, null]}));
    assert_eq!(decoded["score"].as_f64(), Some(1.482_014_285_714_285_7));
    assert!(response.refusal().is_none());

    let captured = server.requests.lock().unwrap()[0].clone();
    assert_eq!(
        captured.line,
        "POST /api/runtime/v1/namespaces/ns-1/api/workflows/order%2F42/signal/approve?b=2&a=x+y%26z HTTP/1.1"
    );
    assert_eq!(
        captured.header("authorization"),
        Some("Bearer control-secret")
    );
    assert_eq!(captured.header("x-namespace"), Some("orders"));
    assert_eq!(captured.header("accept"), Some("application/json"));
    assert_eq!(captured.header("content-type"), Some("application/json"));
    assert_eq!(
        captured.header("x-durable-workflow-control-plane-version"),
        Some(CONTROL_PLANE_VERSION)
    );
    assert_eq!(
        captured.header("x-durable-workflow-protocol-version"),
        Some(WORKER_PROTOCOL_VERSION)
    );
    let sent: Value = serde_json::from_slice(&captured.body).unwrap();
    assert_eq!(sent, json!({"input": [1, "two"], "extra": {"kept": true}}));
}

#[tokio::test]
async fn per_request_version_overrides_replace_both_headers() {
    let server = serve(vec![reply("204 No Content", &[], "")]).await;
    let response = client(&server.url)
        .control_plane_request(
            RawRequest::get(["namespaces"])
                .protocol_version("1.17")
                .control_plane_version("3"),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 204);
    assert_eq!(response.json().unwrap(), Value::Null);
    let captured = server.requests.lock().unwrap()[0].clone();
    assert_eq!(captured.target(), "/api/namespaces");
    assert!(captured.body.is_empty());
    assert_eq!(
        captured.header("x-durable-workflow-protocol-version"),
        Some("1.17")
    );
    assert_eq!(
        captured.header("x-durable-workflow-control-plane-version"),
        Some("3")
    );
}

#[tokio::test]
async fn refusals_keep_headers_and_decode_every_message_field() {
    let refused = json!({
        "reason": "historical_run_command_rejected",
        "rejection_reason": "run is not current",
        "message": "Command rejected.",
        "error": "conflict",
        "validation_errors": {"run_id": ["must be current"]},
        "unknown": 7
    })
    .to_string();
    let protocol = json!({
        "reason": "unsupported_control_plane_version",
        "message": "use 2",
        "supported_version": "2"
    })
    .to_string();
    let server = serve(vec![
        reply(
            "409 Conflict",
            &[("x-durable-workflow-control-plane-version", "2")],
            &refused,
        ),
        reply("400 Bad Request", &[], &protocol),
        reply("502 Bad Gateway", &[], "<html>upstream</html>"),
    ])
    .await;
    let client = client(&server.url);

    let response = client
        .control_plane_request(RawRequest::post(["workflows", "w", "cancel"]))
        .await
        .unwrap();
    assert_eq!(response.status(), 409);
    assert!(!response.is_success());
    assert_eq!(response.control_plane_version(), Some("2"));
    let refusal = response.refusal().unwrap();
    assert_eq!(refusal.status, 409);
    assert_eq!(
        refusal.reason.as_deref(),
        Some("historical_run_command_rejected")
    );
    assert_eq!(
        refusal.rejection_reason.as_deref(),
        Some("run is not current")
    );
    assert_eq!(refusal.message.as_deref(), Some("Command rejected."));
    assert_eq!(refusal.error.as_deref(), Some("conflict"));
    assert_eq!(
        refusal.validation_errors,
        Some(json!({"run_id": ["must be current"]}))
    );
    assert_eq!(refusal.body["unknown"], 7);
    let error = response.error_for_status().unwrap_err();
    match &error {
        Error::Http { status, body } => {
            assert_eq!(status.as_u16(), 409);
            assert_eq!(body, &refused);
        }
        other => panic!("expected Error::Http, got {other:?}"),
    }
    assert_eq!(
        error.refusal().unwrap().reason.as_deref(),
        Some("historical_run_command_rejected")
    );

    let response = client
        .control_plane_request(RawRequest::get(["workflows"]))
        .await
        .unwrap();
    let error = response.error_for_status().unwrap_err();
    let Error::Protocol(failure) = &error else {
        panic!("protocol refusal must stay Error::Protocol, got {error:?}");
    };
    assert_eq!(failure.reason, "unsupported_control_plane_version");
    assert_eq!(
        error.refusal().unwrap().reason.as_deref(),
        Some("unsupported_control_plane_version")
    );

    let response = client
        .control_plane_request(RawRequest::get(["health"]))
        .await
        .unwrap();
    assert_eq!(response.body_bytes(), b"<html>upstream</html>");
    assert!(response.refusal().is_none());
    let error = response.error_for_status().unwrap_err();
    assert!(matches!(&error, Error::Http { body, .. } if body == "<html>upstream</html>"));
    assert!(error.refusal().is_none());
}

#[tokio::test]
async fn worker_plane_accepts_compatible_versions_and_refuses_invalid_echoes() {
    let ok = |version: &str| {
        reply(
            "200 OK",
            &[("x-durable-workflow-protocol-version", version)],
            &json!({"task": null, "protocol_version": version}).to_string(),
        )
    };
    let server = serve(vec![
        ok("1.20"),
        ok("1.21"),
        ok("2.0"),
        ok("1.19"),
        reply(
            "200 OK",
            &[],
            &json!({"protocol_version": "1.20"}).to_string(),
        ),
        reply(
            "200 OK",
            &[("x-durable-workflow-protocol-version", "1.20")],
            &json!({"task": null}).to_string(),
        ),
        reply(
            "200 OK",
            &[("x-durable-workflow-protocol-version", "1.21")],
            &json!({"protocol_version": "1.20"}).to_string(),
        ),
        reply(
            "409 Conflict",
            &[],
            &json!({"reason": "task_not_leased"}).to_string(),
        ),
    ])
    .await;
    let client = client(&server.url);
    let poll = || RawRequest::post(["worker", "workflow-tasks", "poll"]).json(json!({}));

    for _ in 0..2 {
        let response = client.worker_plane_request(poll()).await.unwrap();
        assert!(response.is_success());
    }
    for expected in [
        "not same-major compatible",
        "not same-major compatible",
        "is not same-major compatible with 1.20",
        "must equal the protocol header",
        "must equal the protocol header",
    ] {
        let error = client.worker_plane_request(poll()).await.unwrap_err();
        let Error::Protocol(failure) = &error else {
            panic!("expected Error::Protocol, got {error:?}");
        };
        assert_eq!(failure.reason, "invalid_worker_protocol_response");
        assert_eq!(failure.requested_version.as_deref(), Some("1.20"));
        assert!(failure.message.contains(expected), "{}", failure.message);
    }
    // Non-2xx responses are returned unchecked.
    let response = client.worker_plane_request(poll()).await.unwrap();
    assert_eq!(response.status(), 409);
    assert_eq!(
        response.refusal().unwrap().reason.as_deref(),
        Some("task_not_leased")
    );

    let captured = server.requests.lock().unwrap().clone();
    assert_eq!(captured.len(), 8);
    for request in &captured {
        assert_eq!(request.target(), "/api/worker/workflow-tasks/poll");
        assert_eq!(
            request.header("authorization"),
            Some("Bearer worker-secret")
        );
        assert_eq!(
            request.header("x-durable-workflow-protocol-version"),
            Some(WORKER_PROTOCOL_VERSION)
        );
        assert_eq!(
            request.header("x-durable-workflow-control-plane-version"),
            Some(CONTROL_PLANE_VERSION)
        );
    }
}

#[test]
fn worker_protocol_compatibility_is_same_major_with_a_newer_or_equal_minor() {
    for (client, server, compatible) in [
        ("1.20", "1.20", true),
        ("1.20", "1.21", true),
        ("1.20", "1.100", true),
        ("1.20", "1.19", false),
        ("1.20", "2.20", false),
        ("1.20", "1.2x", false),
        ("1.20", "", false),
        ("v1", "v1", true),
        ("1.20", " 1.20", false),
    ] {
        assert_eq!(
            worker_protocol_compatible(client, server),
            compatible,
            "{client} vs {server}"
        );
    }
}

#[tokio::test]
async fn raw_requests_use_role_tokens_and_refuse_a_missing_role_credential() {
    let server = serve(Vec::new()).await;
    let worker_only = Client::builder(&server.url)
        .worker_token(Some("worker-secret".into()))
        .build()
        .unwrap();
    assert!(matches!(
        worker_only
            .control_plane_request(RawRequest::get(["namespaces"]))
            .await,
        Err(Error::MissingRoleCredentials {
            role: "control",
            ..
        })
    ));
    assert!(server.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn url_shaped_segments_stay_on_the_configured_origin_with_client_identity() {
    let server = serve(vec![
        reply("200 OK", &[], "{}"),
        reply("200 OK", &[], "{}"),
        reply("200 OK", &[], "{}"),
    ])
    .await;
    let client = client(&server.url);
    for segments in [
        vec!["https://evil.example/steal"],
        vec!["//evil.example", "x"],
        vec!["workflows", "@evil.example"],
    ] {
        let response = client
            .control_plane_request(RawRequest::get(segments))
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
    }
    let captured = server.requests.lock().unwrap().clone();
    let targets: Vec<&str> = captured.iter().map(Captured::target).collect();
    assert_eq!(
        targets,
        [
            "/api/https%3A%2F%2Fevil.example%2Fsteal",
            "/api/%2F%2Fevil.example/x",
            "/api/workflows/%40evil.example",
        ]
    );
    for request in &captured {
        assert_eq!(
            request.header("authorization"),
            Some("Bearer control-secret")
        );
        assert_eq!(request.header("x-namespace"), Some("orders"));
    }
}
