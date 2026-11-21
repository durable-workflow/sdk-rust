//! Worker egress: gzip responses and the server capabilities digest
//! (server#1261, server#1262), through the public Client API.

use durable_workflow::{Client, Error};
use serde_json::{json, Value};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

/// `{"workflow_id":"wf-gzip","run_id":"run-1","status":"running","padding":"x"*1500}`,
/// gzip-compressed (91 bytes for a 1574-byte body).
const GZIP_DESCRIPTION: &[u8] = &[
    31, 139, 8, 0, 0, 0, 0, 0, 2, 255, 171, 86, 42, 207, 47, 202, 78, 203, 201, 47, 143, 207, 76,
    81, 178, 82, 42, 79, 211, 77, 175, 202, 44, 80, 210, 81, 42, 42, 205, 131, 8, 1, 25, 186, 134,
    64, 129, 226, 146, 196, 146, 210, 98, 136, 64, 94, 102, 94, 58, 80, 168, 32, 49, 37, 5, 196,
    178, 82, 170, 24, 5, 163, 96, 20, 140, 130, 81, 48, 236, 128, 82, 45, 0, 209, 178, 131, 153,
    38, 6, 0, 0,
];

struct Reply {
    status: u16,
    headers: Vec<(&'static str, &'static str)>,
    body: Vec<u8>,
}

fn json_reply(status: u16, body: Value) -> Reply {
    Reply {
        status,
        headers: vec![("Content-Type", "application/json")],
        body: body.to_string().into_bytes(),
    }
}

/// Answer each connection with the next scripted reply; return the
/// lower-cased request heads in order.
async fn server(replies: Vec<Reply>) -> (String, JoinHandle<Vec<String>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let handle = tokio::spawn(async move {
        let mut heads = Vec::new();
        for reply in replies {
            let (mut stream, _) = tokio::time::timeout(Duration::from_secs(5), listener.accept())
                .await
                .unwrap()
                .unwrap();
            let mut bytes = Vec::new();
            let (head, offset) = loop {
                let mut buf = [0; 8192];
                let count = stream.read(&mut buf).await.unwrap();
                assert!(count > 0);
                bytes.extend_from_slice(&buf[..count]);
                if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                    break (
                        String::from_utf8_lossy(&bytes[..end]).to_ascii_lowercase(),
                        end + 4,
                    );
                }
            };
            let length: usize = head
                .lines()
                .find_map(|line| line.strip_prefix("content-length: "))
                .map_or(0, |value| value.trim().parse().unwrap());
            while bytes.len() < offset + length {
                let mut buf = [0; 8192];
                let count = stream.read(&mut buf).await.unwrap();
                assert!(count > 0);
                bytes.extend_from_slice(&buf[..count]);
            }
            heads.push(head);
            if reply.status == 0 {
                // Close without any reply: the client sees a transport error.
                drop(stream);
                continue;
            }
            let mut response = format!("HTTP/1.1 {} Reply\r\n", reply.status);
            for (name, value) in &reply.headers {
                response.push_str(&format!("{name}: {value}\r\n"));
            }
            response.push_str(&format!(
                "Content-Length: {}\r\nConnection: close\r\n\r\n",
                reply.body.len()
            ));
            stream.write_all(response.as_bytes()).await.unwrap();
            stream.write_all(&reply.body).await.unwrap();
        }
        heads
    });
    (url, handle)
}

fn client(url: &str) -> Client {
    Client::builder(url)
        .namespace("default")
        .token(Some("token".into()))
        .build()
        .unwrap()
}

fn header<'a>(head: &'a str, name: &str) -> Option<&'a str> {
    let prefix = format!("{name}: ");
    head.lines()
        .find_map(|line| line.strip_prefix(prefix.as_str()))
}

#[tokio::test]
async fn requests_accept_gzip_and_compressed_answers_are_decoded() {
    let (url, requests) = server(vec![Reply {
        status: 200,
        headers: vec![
            ("Content-Type", "application/json"),
            ("Content-Encoding", "gzip"),
        ],
        body: GZIP_DESCRIPTION.to_vec(),
    }])
    .await;
    let description = client(&url).describe_workflow("wf-gzip").await.unwrap();
    assert_eq!(description.workflow_id.as_deref(), Some("wf-gzip"));
    assert_eq!(description.status.as_deref(), Some("running"));
    let heads = requests.await.unwrap();
    assert!(header(&heads[0], "accept-encoding").is_some_and(|value| value.contains("gzip")));
    // Control-plane requests do not opt in to the worker digest.
    assert_eq!(
        header(&heads[0], "x-durable-workflow-server-capabilities-digest"),
        None
    );
}

fn capabilities() -> Value {
    json!({"activity_lease_renewal": true, "sticky_execution": {"supported": true}})
}

fn tagged(digest: &str) -> Value {
    let mut value = capabilities();
    value["digest"] = json!(digest);
    value
}

#[tokio::test]
async fn worker_answers_are_restored_from_the_digest_each_request_sent() {
    let d1 = "sha256:d1";
    let (url, requests) = server(vec![
        json_reply(
            200,
            json!({"task": null, "server_capabilities": tagged(d1)}),
        ),
        json_reply(200, json!({"task": null, "server_capabilities": d1})),
        // A garbled answer: the poll is resent once without the held digest.
        json_reply(
            200,
            json!({"task": null, "server_capabilities": "sha256:unknown"}),
        ),
        json_reply(
            200,
            json!({"task": null, "server_capabilities": tagged(d1)}),
        ),
        // Unknown on the resend too: then it is a protocol failure.
        json_reply(
            200,
            json!({"task": null, "server_capabilities": "sha256:unknown"}),
        ),
        json_reply(
            200,
            json!({"task": null, "server_capabilities": "sha256:other"}),
        ),
    ])
    .await;
    let client = client(&url);
    let worker = client.clone();
    let poll = || worker.poll_workflow_task_response("worker", "queue", Duration::from_secs(1));

    let first = poll().await.unwrap();
    assert_eq!(
        first.server_capabilities,
        Some(capabilities()),
        "digest member stripped"
    );
    let second = poll().await.unwrap();
    assert_eq!(
        second.server_capabilities,
        Some(capabilities()),
        "digest string restored"
    );
    let third = poll().await.unwrap();
    assert_eq!(
        third.server_capabilities,
        Some(capabilities()),
        "recovered by resending"
    );
    let Err(Error::Protocol(failure)) = poll().await else {
        panic!("an unknown digest on the resend must be a protocol failure");
    };
    assert_eq!(failure.reason, "server_capabilities_digest_unknown");

    let heads = requests.await.unwrap();
    let sent: Vec<_> = heads
        .iter()
        .map(|head| header(head, "x-durable-workflow-server-capabilities-digest"))
        .collect();
    assert_eq!(
        sent,
        [
            Some("none"),
            Some(d1),
            Some(d1),
            Some("none"),
            Some(d1),
            Some("none")
        ]
    );
}

#[tokio::test]
async fn a_garbled_completion_answer_is_kept_and_capabilities_come_from_cluster_info() {
    let d1 = "sha256:d1";
    let (url, requests) = server(vec![
        json_reply(
            200,
            json!({"task": null, "server_capabilities": tagged(d1)}),
        ),
        // The first completion discovers the inline payload policy.
        json_reply(200, json!({})),
        // The Server committed the completion; its answer carries a result
        // that must not be lost and an unknown digest.
        json_reply(
            200,
            json!({"recorded": true, "next_task": {"task_id": "next-1"},
                "server_capabilities": "sha256:unknown"}),
        ),
        // Read-only capability recovery.
        json_reply(
            200,
            json!({"worker_protocol": {"server_capabilities": tagged("sha256:d2")}}),
        ),
        json_reply(
            200,
            json!({"task": null, "server_capabilities": tagged("sha256:d2")}),
        ),
    ])
    .await;
    let client = client(&url);
    client
        .poll_workflow_task_response("worker", "queue", Duration::from_secs(1))
        .await
        .unwrap();
    let answer = client
        .complete_activity_task("task-1", "attempt-1", "worker", json!("done"), "avro")
        .await
        .expect("the completion's own answer is used");
    assert_eq!(answer["recorded"], true);
    assert_eq!(answer["next_task"]["task_id"], "next-1");
    assert_eq!(answer["server_capabilities"], capabilities());
    client
        .poll_workflow_task_response("worker", "queue", Duration::from_secs(1))
        .await
        .unwrap();

    let heads = requests.await.unwrap();
    let completes = heads
        .iter()
        .filter(|head| head.starts_with("post /api/worker/activity-tasks/task-1/complete"))
        .count();
    assert_eq!(completes, 1, "a completion is never resent");
    assert!(
        heads[3].starts_with("get /api/cluster/info"),
        "{}",
        heads[3]
    );
    let sent: Vec<_> = heads
        .iter()
        .map(|head| header(head, "x-durable-workflow-server-capabilities-digest"))
        .collect();
    // The held pair is forgotten: cluster info and the next poll send none.
    assert_eq!(
        sent,
        [Some("none"), None, Some(d1), Some("none"), Some("none")]
    );
}

#[tokio::test]
async fn decoded_response_bodies_are_capped() {
    let (url, requests) = server(vec![Reply {
        status: 200,
        headers: vec![
            ("Content-Type", "application/json"),
            ("Content-Encoding", "gzip"),
        ],
        body: GZIP_DESCRIPTION.to_vec(),
    }])
    .await;
    let client = Client::builder(&url)
        .namespace("default")
        .token(Some("token".into()))
        .max_response_bytes(1_000)
        .build()
        .unwrap();
    // 91 compressed bytes decode to 1574, over the 1000-byte cap.
    let error = client.describe_workflow("wf-gzip").await.unwrap_err();
    assert!(
        matches!(error, Error::ResponseBodyTooLarge { limit_bytes: 1_000 }),
        "{error:?}"
    );
    requests.await.unwrap();
}

#[tokio::test]
async fn a_garbled_answer_survives_unavailable_cluster_info() {
    for (case, failed_read) in [
        (
            "500",
            json_reply(500, json!({"message": "upstream exploded"})),
        ),
        (
            "connection dropped",
            Reply {
                status: 0,
                headers: vec![],
                body: vec![],
            },
        ),
    ] {
        let d1 = "sha256:d1";
        let (url, requests) = server(vec![
            json_reply(
                200,
                json!({"task": null, "server_capabilities": tagged(d1)}),
            ),
            json_reply(200, json!({})),
            json_reply(
                200,
                json!({"recorded": true, "next_task": {"task_id": "next-1"},
                    "server_capabilities": "sha256:unknown"}),
            ),
            failed_read,
            // Later requests ask with `none` and learn the capabilities again.
            json_reply(
                200,
                json!({"task": null, "server_capabilities": tagged("sha256:d2")}),
            ),
        ])
        .await;
        let client = client(&url);
        client
            .poll_workflow_task_response("worker", "queue", Duration::from_secs(1))
            .await
            .unwrap();
        let answer = client
            .complete_activity_task("task-1", "attempt-1", "worker", json!("done"), "avro")
            .await
            .unwrap_or_else(|error| panic!("{case}: the received answer must be used: {error:?}"));
        assert_eq!(answer["recorded"], true, "{case}");
        assert_eq!(answer["next_task"]["task_id"], "next-1", "{case}");
        assert!(
            answer.get("server_capabilities").is_none(),
            "{case}: dropped, not garbled"
        );
        let next = client
            .poll_workflow_task_response("worker", "queue", Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(
            next.server_capabilities,
            Some(capabilities()),
            "{case}: learned again"
        );

        let heads = requests.await.unwrap();
        let sent: Vec<_> = heads
            .iter()
            .map(|head| header(head, "x-durable-workflow-server-capabilities-digest"))
            .collect();
        assert_eq!(
            sent,
            [Some("none"), None, Some(d1), Some("none"), Some("none")],
            "{case}"
        );
    }
}
