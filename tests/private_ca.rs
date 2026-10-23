use durable_workflow::{encode_avro_value, json, AvroValue, Client, Error};
use rustls::{
    pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer},
    ServerConfig,
};
use sha2::{Digest, Sha256};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use tokio_rustls::TlsAcceptor;

const CA: &[u8] = include_bytes!("fixtures/tls/ca.pem");
const REFERENCE_ID: &str = "ep_00000000000000000000000001";

struct TestServer {
    port: u16,
    requests: Arc<Mutex<Vec<String>>>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl TestServer {
    async fn start() -> Self {
        let config =
            ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_safe_default_protocol_versions()
                .unwrap()
                .with_no_client_auth()
                .with_single_cert(
                    vec![CertificateDer::from(
                        include_bytes!("fixtures/tls/server.der").to_vec(),
                    )],
                    PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
                        include_bytes!("fixtures/tls/server-key.der").to_vec(),
                    )),
                )
                .unwrap();
        let acceptor = TlsAcceptor::from(Arc::new(config));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let observed = requests.clone();
        let task = tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let Ok(mut stream) = acceptor.accept(stream).await else {
                    continue;
                };
                let mut request = Vec::new();
                while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                    let mut buffer = [0; 4096];
                    let count = stream.read(&mut buffer).await.unwrap();
                    if count == 0 {
                        break;
                    }
                    request.extend_from_slice(&buffer[..count]);
                }
                let header_end = request
                    .windows(4)
                    .position(|window| window == b"\r\n\r\n")
                    .unwrap()
                    + 4;
                let content_length = String::from_utf8_lossy(&request[..header_end])
                    .lines()
                    .filter_map(|line| line.split_once(':'))
                    .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                    .map(|(_, value)| value.trim().parse::<usize>().unwrap())
                    .unwrap_or(0);
                while request.len() < header_end + content_length {
                    let mut buffer = [0; 4096];
                    let count = stream.read(&mut buffer).await.unwrap();
                    assert!(count > 0, "client must send the complete request body");
                    request.extend_from_slice(&buffer[..count]);
                }
                let request = String::from_utf8_lossy(&request);
                let path = request.split_whitespace().nth(1).unwrap_or("");
                observed.lock().unwrap().push(path.to_string());
                let blob = encode_avro_value(&AvroValue::String("private CA payload".into()))
                    .unwrap()
                    .blob;
                let body = match path {
                    "/api/health" => json!({"status":"ok"}).to_string(),
                    "/api/cluster/info" => json!({"limits":{"max_payload_bytes":2097152}}).to_string(),
                    "/api/worker/activity-tasks/poll" => json!({"task":null}).to_string(),
                    "/api/workflows/test/query/value" => json!({"result_envelope":{
                        "codec":"avro", "external_payload": {
                            "schema":"durable-workflow.v2.runtime-external-payload-reference.v1",
                            "codec":"avro", "reference_id":REFERENCE_ID,
                            "size_bytes":blob.len(), "sha256":format!("{:x}",Sha256::digest(blob.as_bytes())),
                        }
                    }}).to_string(),
                    _ if path == format!("/api/external-payloads/v1/{REFERENCE_ID}") => blob,
                    _ => panic!("unexpected fixture route {path}"),
                };
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                stream.write_all(response.as_bytes()).await.unwrap();
                stream.shutdown().await.unwrap();
            }
        });
        Self {
            port,
            requests,
            task,
        }
    }

    fn url(&self, host: &str) -> String {
        format!("https://{host}:{}", self.port)
    }
}

#[tokio::test]
async fn added_private_ca_preserves_verification_and_is_used_for_every_request_family() {
    let server = TestServer::start().await;
    let untrusted = Client::builder(server.url("localhost")).build().unwrap();
    assert!(matches!(untrusted.health().await, Err(Error::Transport(_))));
    assert!(
        server.requests.lock().unwrap().is_empty(),
        "untrusted TLS must fail before HTTP"
    );

    let bundle = [CA, CA].concat();
    let client = Client::builder(server.url("localhost"))
        .additional_root_certificates_pem(bundle)
        .unwrap()
        .additional_root_certificates_pem(CA)
        .unwrap()
        .build()
        .unwrap();
    assert_eq!(client.health().await.unwrap()["status"], "ok");
    assert!(client
        .poll_activity_task("worker", "queue", Duration::ZERO)
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        client
            .query_workflow_avro_value("test", "value", json!([]))
            .await
            .unwrap(),
        AvroValue::String("private CA payload".into())
    );
    let mismatch = Client::builder(server.url("127.0.0.1"))
        .additional_root_certificates_pem(CA)
        .unwrap()
        .build()
        .unwrap();
    assert!(matches!(mismatch.health().await, Err(Error::Transport(_))));
    assert_eq!(
        *server.requests.lock().unwrap(),
        [
            "/api/health",
            "/api/worker/activity-tasks/poll",
            "/api/cluster/info",
            "/api/workflows/test/query/value",
            "/api/external-payloads/v1/ep_00000000000000000000000001",
        ]
    );
}

#[test]
fn invalid_or_empty_pem_bundles_are_rejected() {
    for pem in [
        b"".as_slice(),
        b"not a PEM certificate",
        b"-----BEGIN CERTIFICATE-----\n!invalid!\n-----END CERTIFICATE-----",
    ] {
        assert!(matches!(
            Client::builder("https://localhost").additional_root_certificates_pem(pem),
            Err(Error::InvalidRootCertificates(_))
        ));
    }
    assert!(Client::builder("https://localhost")
        .additional_root_certificates_pem(
            b"-----BEGIN CERTIFICATE-----\nAA==\n-----END CERTIFICATE-----"
        )
        .unwrap()
        .build()
        .is_err());
}

#[tokio::test]
#[ignore = "requires the public Rust documentation HTTPS endpoint"]
async fn adding_a_private_ca_keeps_bundled_public_roots_trusted() {
    let client = Client::builder("https://rust.durable-workflow.com")
        .additional_root_certificates_pem(CA)
        .unwrap()
        .timeout(Duration::from_secs(20))
        .build()
        .unwrap();
    // The documentation site is not a runtime. Reaching its HTTP response
    // proves that adding a private root preserves the public TLS trust path.
    assert!(matches!(
        client.health().await,
        Ok(_) | Err(Error::Http { .. }) | Err(Error::Json(_))
    ));
}
