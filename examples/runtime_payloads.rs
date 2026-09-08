//! Native external-payload qualification against an isolated Server.
//! See tests/runtime-payloads.md. Never point this fixture at a customer runtime.
use durable_workflow::{
    decode_avro_value, encode_payload, json, wait_condition, ActivityOptions, AvroValue, Client,
    PayloadEnvelope, Value, Worker, WorkflowDescription,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, time::Duration};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
const NS: &str = "external-proof";
const WORKFLOW: &str = "external-payload.rust";
const ACTIVITY: &str = "external-payload.rust.echo";
const MAXIMUM: usize = 50_331_633;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
struct Payload {
    text: String,
    long: i64,
    double: f64,
    negative_zero: f64,
    #[serde(with = "serde_bytes")]
    binary: Vec<u8>,
    nested: BTreeMap<String, BTreeMap<String, i64>>,
}

#[derive(Deserialize, Serialize)]
struct Request {
    value: Payload,
    wait: bool,
}

fn payload() -> Payload {
    Payload {
        text: "durable-external-value-".repeat(131072),
        long: 7,
        double: 7.0,
        negative_zero: -0.0,
        binary: vec![0, 255, 128, 1],
        nested: BTreeMap::from([(
            "nested".into(),
            BTreeMap::from([("z".into(), 2), ("a".into(), 1)]),
        )]),
    }
}

fn assert_payload(value: AvroValue) -> Result<()> {
    // Compare on the wire before Serde conversion: 7 and 7.0 are not the same value.
    let expected = decode_avro_value(&encode_payload(&payload(), "avro")?)?;
    assert_eq!(value, expected, "lossless Avro value changed");
    let actual: Payload = value.deserialize()?;
    assert_eq!(actual, payload());
    assert!(actual.negative_zero.is_sign_negative());
    Ok(())
}

fn token(role: &str) -> String {
    format!("{:x}", Sha256::digest(format!("external-proof-{role}")))
}

fn client(url: &str, worker: bool) -> Result<Client> {
    let builder = Client::builder(url)
        .namespace(NS)
        .timeout(Duration::from_secs(120));
    Ok(if worker {
        builder.worker_token(Some(token("worker")))
    } else {
        builder.control_token(Some(token("operator")))
    }
    .build()?)
}

async fn api(url: &str, method: &str, path: &str, body: Value) -> Result<Value> {
    let response = reqwest::Client::new()
        .request(method.parse()?, format!("{url}/api{path}"))
        .bearer_auth("external-payload-fixture")
        .header("X-Namespace", NS)
        .header("X-Durable-Workflow-Control-Plane-Version", "2")
        .json(&body)
        .send()
        .await?;
    let status = response.status();
    let value: Value = response.json().await?;
    if !status.is_success() {
        return Err(format!("fixture API {path}: HTTP {status}: {value}").into());
    }
    Ok(value)
}

async fn wait_for(client: &Client, id: &str, status: &str) -> Result<WorkflowDescription> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    loop {
        let description = client.describe_workflow(id).await?;
        if description.status.as_deref() == Some(status) {
            return Ok(description);
        }
        if description.is_terminal() || tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "{id}: expected {status}, got {:?}: {:?}",
                description.status, description.failure
            )
            .into());
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn worker(url: &str) -> Result<()> {
    let mut worker = Worker::new(client(url, true)?, NS)
        .worker_id("external-rust-worker")
        .poll_timeout(Duration::from_secs(1));
    worker.register_typed_activity(ACTIVITY, |ctx, input: Payload| async move {
        ctx.heartbeat(json!({"bytes": input.text.len()})).await?;
        Ok(input)
    });
    worker.register_typed_workflow(WORKFLOW, |ctx, input: Request| async move {
        let result: Payload = ctx
            .activity_typed_with_options(
                ACTIVITY,
                ActivityOptions::new().start_to_close_timeout(Duration::from_secs(60)),
                input.value,
            )
            .await?;
        if input.wait {
            let predicate_ctx = ctx.clone();
            wait_condition!(ctx, "released", move || Ok(!predicate_ctx
                .signals("release")?
                .is_empty()))
            .await?;
            assert_eq!(
                ctx.signals("release")?[0],
                vec![json!(format!("{:x}", Sha256::digest(&result.text)))]
            );
        }
        Ok(result)
    });
    worker.register_query_avro_value(WORKFLOW, "value", |ctx, _| async move {
        let result = ctx
            .history_events()
            .iter()
            .find(|event| event.event_type == "ActivityCompleted")
            .ok_or_else(|| {
                durable_workflow::Error::WorkerLoop("missing completed activity history".into())
            })?;
        let envelope: PayloadEnvelope = serde_json::from_value(result.payload["result"].clone())?;
        decode_avro_value(&envelope)
    });
    worker.register_typed_workflow("external-payload.rust.maximum", |_, _: ()| async {
        Ok("m".repeat(MAXIMUM))
    });
    worker.run().await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let url = std::env::var("RUNTIME_URL").unwrap_or_else(|_| "http://server:8080".into());
    assert!(
        matches!(
            reqwest::Url::parse(&url)?.host_str(),
            Some("server" | "localhost" | "127.0.0.1")
        ),
        "local fixture only"
    );
    let phase = std::env::args()
        .nth(1)
        .ok_or("choose prepare, worker, start, verify, maximum or verify-maximum")?;
    if phase == "worker" {
        return worker(&url).await;
    }
    let client = client(&url, false)?;
    match phase.as_str() {
        "prepare" => {
            api(
                &url,
                "POST",
                "/namespaces",
                json!({"name":NS,"retention_days":30}),
            )
            .await?;
            api(&url, "PUT", &format!("/namespaces/{NS}/external-storage"), json!({
                "enabled":true,"driver":"local","threshold_bytes":64,"config":{"uri":"file:///payloads"}
            })).await?;
            for role in ["operator", "worker"] {
                api(&url, "PUT", &format!("/runtime-credentials/external-proof-{role}"), json!({
                    "token":token(role),"subject":format!("external-proof-{role}"),"roles":[role],"tenant":NS
                })).await?;
            }
            let info = client.cluster_info().await?;
            assert_eq!(info["limits"]["max_payload_bytes"], 2097152);
            assert_eq!(
                info["namespace"]["external_payload_storage"]["transport"]["limits"]
                    ["max_payload_bytes"],
                67108864
            );
        }
        "start" => {
            for (kind, wait) in [("completed", false), ("waiting", true)] {
                let id = format!("external-rust-{kind}");
                let handle = client
                    .start_workflow(
                        WORKFLOW,
                        NS,
                        &id,
                        Request {
                            value: payload(),
                            wait,
                        },
                    )
                    .await?;
                let description =
                    wait_for(&client, &id, if wait { "waiting" } else { "completed" }).await?;
                if !wait {
                    assert_payload(
                        description
                            .output_avro_value
                            .ok_or("missing typed output")?,
                    )?;
                } else {
                    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
                    loop {
                        let history = api(
                            &url,
                            "GET",
                            &format!(
                                "/workflows/{id}/runs/{}/history",
                                handle.run_id.as_deref().unwrap()
                            ),
                            Value::Null,
                        )
                        .await?;
                        let events = history["events"].as_array().ok_or("missing history")?;
                        if events
                            .iter()
                            .any(|event| event["event_type"] == "ConditionWaitOpened")
                        {
                            assert_eq!(
                                events
                                    .iter()
                                    .filter(|event| event["event_type"] == "ActivityCompleted")
                                    .count(),
                                1
                            );
                            break;
                        }
                        if tokio::time::Instant::now() >= deadline {
                            return Err("workflow never opened its durable condition".into());
                        }
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                }
                println!("{kind}: workflow_id={id} run_id={}", handle.run_id.unwrap());
            }
        }
        "verify" => {
            assert_payload(
                client
                    .query_workflow_avro_value("external-rust-waiting", "value", json!([]))
                    .await?,
            )?;
            client
                .signal_workflow(
                    "external-rust-waiting",
                    "release",
                    [format!("{:x}", Sha256::digest(payload().text))],
                )
                .await?;
            for kind in ["completed", "waiting"] {
                let description =
                    wait_for(&client, &format!("external-rust-{kind}"), "completed").await?;
                assert_payload(
                    description
                        .output_avro_value
                        .ok_or("missing typed output")?,
                )?;
            }
        }
        "maximum" | "verify-maximum" => {
            if phase == "maximum" {
                client
                    .start_workflow(
                        "external-payload.rust.maximum",
                        NS,
                        "external-rust-maximum",
                        (),
                    )
                    .await?;
            }
            let description = wait_for(&client, "external-rust-maximum", "completed").await?;
            let actual: String = description
                .output_avro_value
                .ok_or("missing maximum output")?
                .deserialize()?;
            assert_eq!(actual.len(), MAXIMUM);
            assert!(actual.bytes().all(|byte| byte == b'm'));
            assert_eq!(encode_payload(&actual, "avro")?.blob.len(), 67_108_864);
        }
        _ => return Err("unknown phase".into()),
    }
    println!("{phase}: passed");
    Ok(())
}
