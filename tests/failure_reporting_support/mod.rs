use durable_workflow::{json, Value};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

pub const MIB: usize = 1024 * 1024;
pub struct Server {
    pub url: String,
    pub sent: Arc<Mutex<Vec<(String, Vec<u8>)>>>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Server {
    pub async fn start(limit: usize, activities: Vec<Value>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let sent = Arc::new(Mutex::new(Vec::new()));
        let saved = sent.clone();
        let mut activities = VecDeque::from(activities);
        let task = tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                let (start, length) = loop {
                    let mut buffer = [0; 8192];
                    let n = stream.read(&mut buffer).await.unwrap();
                    assert_ne!(n, 0);
                    bytes.extend_from_slice(&buffer[..n]);
                    if let Some(index) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
                        let header = String::from_utf8_lossy(&bytes[..index]);
                        let length = header
                            .lines()
                            .find_map(|line| {
                                let (name, value) = line.split_once(':')?;
                                name.eq_ignore_ascii_case("content-length")
                                    .then(|| value.trim().parse::<usize>().unwrap())
                            })
                            .unwrap_or(0);
                        assert!(length < 64 * MIB, "test fixture bounds its raw intake");
                        break (index + 4, length);
                    }
                };
                while bytes.len() < start + length {
                    let mut buffer = [0; 8192];
                    let n = stream.read(&mut buffer).await.unwrap();
                    assert_ne!(n, 0);
                    bytes.extend_from_slice(&buffer[..n]);
                }
                let header = String::from_utf8_lossy(&bytes[..start]);
                let path = header.split_whitespace().nth(1).unwrap();
                let raw = bytes[start..start + length].to_vec();
                let request: Value = serde_json::from_slice(&raw).unwrap_or(Value::Null);
                let response = if path.ends_with("/cluster/info") {
                    json!({"limits":{"max_payload_bytes":limit}})
                } else if path.ends_with("/register") {
                    json!({"registered":true,"worker_id":"failure-worker","namespace":"default","task_queue":"failures",
                    "protocol_version":"1.20","server_capabilities":{"cooperative_cancellation":true},"capabilities":request["capabilities"]})
                } else if path.ends_with("/workflow-tasks/poll") {
                    json!({"task":null})
                } else if path.ends_with("/activity-tasks/poll") {
                    json!({"task":activities.pop_front()})
                } else if path.ends_with("/status") {
                    json!({"task_id":path.split('/').rev().nth(1).unwrap(),"activity_attempt_id":request["activity_attempt_id"],
                    "lease_owner":"failure-worker","can_continue":true,"cancel_requested":false,"heartbeat_recorded":false,
                    "reason":null,"run_closed_reason":null,"run_closed_at":null,"last_heartbeat_at":null,
                    "task_status":"leased","attempt_status":"running","activity_status":"running","lease_expires_at":"2099-01-01T00:00:00Z"})
                } else if path.ends_with("/fail") || path.ends_with("/complete") {
                    saved.lock().unwrap().push((path.to_owned(), raw));
                    json!({"recorded":true})
                } else {
                    panic!("unexpected request: {path}");
                };
                let response = response.to_string();
                let header=format!("HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",response.len());
                stream.write_all(header.as_bytes()).await.unwrap();
                stream.write_all(response.as_bytes()).await.unwrap();
                stream.shutdown().await.unwrap();
            }
        });
        Self { url, sent, task }
    }
    pub fn reports(&self) -> Vec<(String, Vec<u8>, Value)> {
        self.sent
            .lock()
            .unwrap()
            .iter()
            .map(|(path, bytes)| {
                (
                    path.clone(),
                    bytes.clone(),
                    serde_json::from_slice(bytes).unwrap(),
                )
            })
            .collect()
    }
}
