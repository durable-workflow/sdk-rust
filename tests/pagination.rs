use durable_workflow::{json, Client, Error, Value, WorkflowHistoryOptions, WorkflowListOptions};
use std::sync::{Arc, Mutex};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

#[derive(Clone, Debug)]
struct Request {
    path: String,
    namespace: String,
    control_plane: bool,
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
    async fn start(
        handler: impl Fn(usize, &Request) -> (u16, Value) + Send + Sync + 'static,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let observed = requests.clone();
        let task = tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                while !bytes.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
                    let mut buffer = [0; 4096];
                    let n = stream.read(&mut buffer).await.unwrap();
                    if n == 0 {
                        break;
                    }
                    bytes.extend_from_slice(&buffer[..n]);
                }
                let header = String::from_utf8_lossy(&bytes);
                let headers = header
                    .lines()
                    .filter_map(|line| line.split_once(':'))
                    .map(|(key, value)| (key.to_ascii_lowercase(), value.trim().to_string()))
                    .collect::<std::collections::HashMap<_, _>>();
                let request = Request {
                    path: header.split_whitespace().nth(1).unwrap().into(),
                    namespace: headers.get("x-namespace").cloned().unwrap_or_default(),
                    control_plane: headers
                        .get("x-durable-workflow-control-plane-version")
                        .is_some_and(|v| v == "2"),
                };
                let index = {
                    let mut requests = observed.lock().unwrap();
                    let index = requests.len();
                    requests.push(request.clone());
                    index
                };
                let (status, body) = handler(index, &request);
                let body = body.to_string();
                let response=format!("HTTP/1.1 {status} fixture\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",body.len());
                stream.write_all(response.as_bytes()).await.unwrap();
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
        Client::builder(&self.url)
            .namespace("orders")
            .build()
            .unwrap()
    }
    fn count(&self) -> usize {
        self.requests.lock().unwrap().len()
    }
}

fn summary(id: &str) -> Value {
    json!({"workflow_id":id,"run_id":format!("run-{id}"),"workflow_type":"payments","status":"running","search_attributes":{"OrderRef":"order-42"}})
}
fn list(items: Vec<Value>, token: Option<&str>) -> Value {
    json!({"workflow_count":items.len(),"workflows":items,"next_page_token":token})
}
fn event(sequence: u64) -> Value {
    json!({"sequence":sequence,"event_type":"WorkflowStarted","payload":{},"timestamp":"2026-10-23T00:00:00Z"})
}
fn history(events: Vec<Value>, token: Option<&str>) -> Value {
    json!({"workflow_id":"order/one","run_id":"run/one","events":events,"next_page_token":token})
}
fn query(request: &Request) -> std::collections::BTreeMap<String, String> {
    reqwest::Url::parse(&format!("http://capture.invalid{}", request.path))
        .unwrap()
        .query_pairs()
        .into_owned()
        .collect()
}

#[tokio::test]
async fn list_iteration_is_lazy_and_pins_namespace_filters_and_opaque_tokens() {
    let server = Server::start(|index, _| {
        (
            200,
            match index {
                0 => list(vec![summary("one"), summary("two")], Some("next+ /%opaque")),
                1 => list(vec![summary("three")], None),
                _ => panic!("iterator must stop without another request"),
            },
        )
    })
    .await;
    let filter = "OrderRef = 'synthetic-secret&next_page_token=fake+100%雪'";
    let options = WorkflowListOptions::new()
        .workflow_type("payments & more")
        .status("running")
        .query(filter)
        .page_size(2)
        .next_page_token("initial+ /%opaque");
    let mut results = server.client().iter_workflows(options);
    assert_eq!(
        server.count(),
        0,
        "constructing the iterator must do no I/O"
    );
    assert_eq!(results.next().await.unwrap().unwrap().workflow_id, "one");
    assert_eq!(server.count(), 1);
    assert_eq!(results.next().await.unwrap().unwrap().workflow_id, "two");
    assert_eq!(
        server.count(),
        1,
        "do not prefetch while a page has buffered rows"
    );
    assert_eq!(results.next().await.unwrap().unwrap().workflow_id, "three");
    assert!(results.next().await.unwrap().is_none());
    assert!(results.next().await.unwrap().is_none());
    assert_eq!(server.count(), 2);
    for (index, request) in server.requests.lock().unwrap().iter().enumerate() {
        assert_eq!(request.namespace, "orders");
        assert!(request.control_plane);
        let pairs = query(request);
        assert_eq!(pairs.len(), 5);
        assert_eq!(pairs["workflow_type"], "payments & more");
        assert_eq!(pairs["status"], "running");
        assert_eq!(pairs["query"], filter);
        assert_eq!(pairs["page_size"], "2");
        assert_eq!(
            pairs["next_page_token"],
            if index == 0 {
                "initial+ /%opaque"
            } else {
                "next+ /%opaque"
            }
        );
    }
}

#[tokio::test]
async fn history_iteration_preserves_run_and_wait_options_across_empty_pages() {
    let server = Server::start(|index, request| {
        assert!(request
            .path
            .starts_with("/api/workflows/order%2Fone/runs/run%2Fone/history?"));
        (
            200,
            match index {
                0 => history(vec![event(1)], Some("a+token")),
                1 => history(vec![], Some("b/token")),
                2 => history(vec![event(2), event(3)], None),
                _ => panic!("unexpected history page"),
            },
        )
    })
    .await;
    let mut events = server.client().iter_workflow_history(
        "order/one",
        "run/one",
        WorkflowHistoryOptions::new()
            .page_size(2)
            .wait_new_event(true),
    );
    assert_eq!(server.count(), 0);
    let mut sequences = Vec::new();
    while let Some(event) = events.next().await.unwrap() {
        sequences.push(event.raw["sequence"].as_u64().unwrap());
    }
    assert_eq!(sequences, [1, 2, 3]);
    assert_eq!(server.count(), 3);
    for (index, request) in server.requests.lock().unwrap().iter().enumerate() {
        assert_eq!(request.namespace, "orders");
        assert!(request.control_plane);
        let pairs = query(request);
        assert_eq!(pairs["page_size"], "2");
        assert_eq!(pairs["wait_new_event"], "1");
        assert_eq!(
            pairs.get("next_page_token").map(String::as_str),
            match index {
                0 => None,
                1 => Some("a+token"),
                _ => Some("b/token"),
            }
        );
    }
}

#[tokio::test]
async fn middle_page_errors_remain_typed_and_are_never_a_successful_end() {
    let server = Server::start(|index, _| {
        if index == 0 {
            (200, list(vec![summary("one")], Some("a")))
        } else {
            (403, json!({"error":"synthetic middle-page refusal"}))
        }
    })
    .await;
    let mut results = server
        .client()
        .iter_workflows(WorkflowListOptions::default());
    assert_eq!(results.next().await.unwrap().unwrap().workflow_id, "one");
    assert!(matches!(results.next().await,Err(Error::Http {status,..}) if status.as_u16()==403));
    assert!(results.next().await.unwrap().is_none());
    assert_eq!(server.count(), 2);
}

#[tokio::test]
async fn repeated_tokens_and_longer_cycles_fail_without_growing_token_history() {
    for cycle in [vec!["a"], vec!["a", "b", "c"]] {
        let server =
            Server::start(move |index, _| (200, list(vec![], Some(cycle[index % cycle.len()]))))
                .await;
        let mut results = server
            .client()
            .iter_workflows(WorkflowListOptions::default());
        let result = tokio::time::timeout(std::time::Duration::from_secs(2), results.next())
            .await
            .unwrap();
        assert!(
            matches!(result,Err(Error::Pagination(error)) if error.reason=="repeated_page_token")
        );
        assert!(server.count() <= 10);
        assert!(results.next().await.unwrap().is_none());
    }
}

#[tokio::test]
async fn empty_token_ends_and_missing_token_refuses_partial_results() {
    let server = Server::start(|_, _| (200, list(vec![summary("one")], Some("")))).await;
    let mut results = server
        .client()
        .iter_workflows(WorkflowListOptions::default());
    assert_eq!(results.next().await.unwrap().unwrap().workflow_id, "one");
    assert!(results.next().await.unwrap().is_none());
    assert_eq!(server.count(), 1);
    let missing = Server::start(|_, _| (200, json!({"workflows":[summary("one")]}))).await;
    let mut results = missing
        .client()
        .iter_workflows(WorkflowListOptions::default());
    assert!(
        matches!(results.next().await,Err(Error::Pagination(error)) if error.reason=="missing_continuation")
    );
    assert!(results.next().await.unwrap().is_none());
}

#[tokio::test]
async fn history_response_identity_cannot_change_at_a_page_boundary() {
    let server = Server::start(|index, _| {
        let mut page = history(vec![event(index as u64 + 1)], Some("a"));
        if index > 0 {
            page["run_id"] = json!("different-run");
            page["next_page_token"] = Value::Null;
        }
        (200, page)
    })
    .await;
    let mut events = server.client().iter_workflow_history(
        "order/one",
        "run/one",
        WorkflowHistoryOptions::default(),
    );
    assert!(events.next().await.unwrap().is_some());
    assert!(
        matches!(events.next().await,Err(Error::Pagination(error)) if error.reason=="response_identity_mismatch")
    );
    assert!(events.next().await.unwrap().is_none());
    assert_eq!(server.count(), 2);
}

#[tokio::test]
async fn one_page_metadata_and_server_page_size_validation_are_preserved() {
    let server = Server::start(|index, request| {
        if index == 0 {
            assert_eq!(query(request)["page_size"], "0");
            return (422, json!({"message":"page size rejected by Server"}));
        }
        let mut row = summary("one");
        row["search_attributes"] = json!([]);
        row["history_event_count"] = json!(17);
        (200, list(vec![row], Some("a")))
    })
    .await;
    let client = server.client();
    assert!(
        matches!(client.list_workflows(WorkflowListOptions::new().page_size(0)).await,Err(Error::Http {status,..}) if status.as_u16()==422)
    );
    let page = client
        .list_workflows(WorkflowListOptions::default())
        .await
        .unwrap();
    assert_eq!(page.workflow_count, Some(1));
    assert_eq!(page.next_page_token.as_deref(), Some("a"));
    assert_eq!(page.workflows[0].run_id, "run-one");
    assert!(page.workflows[0].search_attributes.is_empty());
    assert_eq!(page.workflows[0].raw["history_event_count"], 17);
}

#[tokio::test]
async fn stream_adapter_is_lazy_and_propagates_a_middle_page_failure() {
    use futures_util::TryStreamExt;
    let server = Server::start(|index, _| {
        if index == 0 {
            (200, list(vec![summary("one")], Some("next")))
        } else {
            (403, json!({"error":"synthetic refusal"}))
        }
    })
    .await;
    let stream = server
        .client()
        .iter_workflows(WorkflowListOptions::default())
        .into_stream();
    assert_eq!(server.count(), 0);
    futures_util::pin_mut!(stream);
    assert_eq!(stream.try_next().await.unwrap().unwrap().workflow_id, "one");
    assert!(matches!(stream.try_next().await,Err(Error::Http {status,..}) if status.as_u16()==403));
    assert!(stream.try_next().await.unwrap().is_none());
    assert_eq!(server.count(), 2);
}
