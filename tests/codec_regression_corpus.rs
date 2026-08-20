use std::{collections::BTreeMap, env, fs, path::Path, time::Duration};

use durable_workflow::{
    decode_avro_value, encode_avro_value, encode_payload, ActivityTask, AvroValue, Client,
    PayloadEnvelope, QueryTask, WorkflowTask, AVRO_VALUE_SCHEMA_FINGERPRINT_HEX, DEFAULT_CODEC,
};
use serde_json::Value;

const FIXTURE_DIRECTORY: &str = "tests/fixtures/codec-regressions";
const FIXTURE_MANIFEST: &str = include_str!("fixtures/codec-regressions/manifest.txt");
const IDENTITY_SCHEMA: &str = "durable-workflow.codec-value-identity/v1";
const IDENTITY_REQUEST_ENV: &str = "DURABLE_WORKFLOW_CODEC_VALUE_IDENTITY_REQUEST";
const IDENTITY_RESPONSE_ENV: &str = "DURABLE_WORKFLOW_CODEC_VALUE_IDENTITY_RESPONSE";

fn tagged_value(value: &Value) -> AvroValue {
    match value["type"].as_str().expect("tagged value type") {
        "null" => AvroValue::Null,
        "boolean" => AvroValue::Boolean(value["value"].as_bool().expect("boolean value")),
        "long" => AvroValue::Long(
            value["value"]
                .as_str()
                .expect("long string")
                .parse()
                .expect("i64 value"),
        ),
        "double" => AvroValue::Double(
            value["value"]
                .as_str()
                .expect("double string")
                .parse()
                .expect("f64 value"),
        ),
        "bytes" => AvroValue::Bytes(
            base64::Engine::decode(
                &base64::engine::general_purpose::STANDARD,
                value["base64"].as_str().expect("base64 bytes"),
            )
            .expect("decode base64 bytes"),
        ),
        "string" => AvroValue::String(value["value"].as_str().expect("string value").to_string()),
        "array" => AvroValue::Array(
            value["items"]
                .as_array()
                .expect("array items")
                .iter()
                .map(tagged_value)
                .collect(),
        ),
        "map" => AvroValue::Map(
            value["entries"]
                .as_array()
                .expect("map entries")
                .iter()
                .map(|entry| {
                    (
                        entry["key"].as_str().expect("map key").to_string(),
                        tagged_value(&entry["value"]),
                    )
                })
                .collect::<BTreeMap<_, _>>(),
        ),
        kind => panic!("unsupported tagged corpus value {kind}"),
    }
}

fn canonical_avro_value(value: AvroValue) -> Value {
    match value {
        AvroValue::Null => serde_json::json!({"type": "null"}),
        AvroValue::Boolean(value) => {
            serde_json::json!({"type": "boolean", "value": value})
        }
        AvroValue::Long(value) => serde_json::json!({"type": "long", "value": value}),
        AvroValue::Double(value) => serde_json::json!({
            "type": "double",
            "bits": format!("{:016x}", value.to_bits()),
        }),
        AvroValue::Bytes(value) => serde_json::json!({
            "type": "bytes",
            "base64": base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                value,
            ),
        }),
        AvroValue::String(value) => serde_json::json!({"type": "string", "value": value}),
        AvroValue::Array(values) => serde_json::json!({
            "type": "array",
            "items": values
                .into_iter()
                .map(canonical_avro_value)
                .collect::<Vec<_>>(),
        }),
        AvroValue::Map(values) => serde_json::json!({
            "type": "map",
            "entries": values
                .into_iter()
                .map(|(key, value)| serde_json::json!({
                    "key": key,
                    "value": canonical_avro_value(value),
                }))
                .collect::<Vec<_>>(),
        }),
    }
}

fn process_identity_request(request_path: &Path, response_path: &Path) -> Result<(), String> {
    let request: Value = serde_json::from_str(
        &fs::read_to_string(request_path)
            .map_err(|error| format!("read {}: {error}", request_path.display()))?,
    )
    .map_err(|error| format!("parse {}: {error}", request_path.display()))?;
    if request["schema"] != IDENTITY_SCHEMA {
        return Err("codec value identity request has an unsupported schema".to_string());
    }
    let request_id = request["request_id"]
        .as_str()
        .ok_or_else(|| "codec value identity request has no request_id".to_string())?;
    let value = request
        .get("value")
        .ok_or_else(|| "codec value identity request has no value".to_string())?;
    let response = serde_json::json!({
        "schema": IDENTITY_SCHEMA,
        "request_id": request_id,
        "value": canonical_avro_value(tagged_value(value)),
    });
    fs::write(
        response_path,
        serde_json::to_vec(&response)
            .map_err(|error| format!("encode codec value identity response: {error}"))?,
    )
    .map_err(|error| format!("write {}: {error}", response_path.display()))
}

fn check_task_boundary(fixture: &Value) {
    let Some(boundary) = fixture.get("task_boundary") else {
        return;
    };
    let expected_error = boundary["error"]
        .as_str()
        .expect("task-boundary stable error");

    match boundary["operation"].as_str() {
        Some("complete_workflow_task") => {
            let command = boundary
                .get("command")
                .cloned()
                .expect("task-boundary command");
            let client = Client::builder("http://127.0.0.1:9")
                .timeout(Duration::from_millis(100))
                .build()
                .expect("task-boundary client");
            let error = tokio::runtime::Runtime::new()
                .expect("task-boundary runtime")
                .block_on(client.complete_workflow_task(
                    "codec-regression",
                    "codec-regression-worker",
                    1,
                    vec![command],
                ))
                .expect_err("invalid command must fail before transport");
            assert!(error.to_string().contains(expected_error), "{error}");
        }
        Some("deserialize_worker_tasks") => {
            for case in boundary["cases"].as_array().expect("task-boundary cases") {
                let task = case.get("task").cloned().expect("task-boundary task");
                let codec = match case["family"].as_str() {
                    Some("workflow") => {
                        serde_json::from_value::<WorkflowTask>(task)
                            .expect("workflow task must remain settleable")
                            .payload_codec
                    }
                    Some("activity") => {
                        serde_json::from_value::<ActivityTask>(task)
                            .expect("activity task must remain settleable")
                            .payload_codec
                    }
                    Some("query") => {
                        serde_json::from_value::<QueryTask>(task)
                            .expect("query task must remain settleable")
                            .payload_codec
                    }
                    family => panic!("unsupported task family {family:?}"),
                };
                let error = encode_payload(&Value::Null, &codec)
                    .expect_err("invalid task codec must reach the stable codec validator");
                assert!(
                    error.to_string().contains(expected_error),
                    "{} returned an unrelated diagnostic: {error}",
                    case["id"]
                );
            }
        }
        operation => panic!("unsupported task-boundary corpus operation {operation:?}"),
    }
}

fn check_corpus() {
    let directory = Path::new(env!("CARGO_MANIFEST_DIR")).join(FIXTURE_DIRECTORY);
    let mut manifest = FIXTURE_MANIFEST
        .lines()
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect::<Vec<_>>();
    manifest.sort();
    let mut observed = fs::read_dir(&directory)
        .expect("read codec fixture directory")
        .map(|entry| {
            entry
                .expect("fixture entry")
                .file_name()
                .to_string_lossy()
                .to_string()
        })
        .filter(|name| name.ends_with(".json"))
        .collect::<Vec<_>>();
    observed.sort();
    assert_eq!(
        observed, manifest,
        "fixture manifest must name every JSON fixture"
    );

    for name in manifest {
        let fixture: Value = serde_json::from_str(
            &fs::read_to_string(directory.join(&name)).expect("read codec regression fixture"),
        )
        .expect("parse codec regression fixture");
        assert_eq!(
            fixture["fixture_schema"],
            "durable-workflow.codec-regression/v1"
        );
        assert!(fixture["bindings"]
            .as_array()
            .expect("bindings")
            .contains(&Value::String("rust".to_string())));
        assert_eq!(
            fixture["protocol"]["fingerprint"],
            AVRO_VALUE_SCHEMA_FINGERPRINT_HEX
        );

        let value = tagged_value(&fixture["value"]);
        let wire = fixture["framing"]["wire_base64"].as_str();
        let operation = fixture["failure_policy"]["operation"]
            .as_str()
            .expect("failure operation");
        let expected_error = fixture["failure_policy"]["error"].as_str();

        match operation {
            "round_trip" => {
                let wire = wire.expect("round-trip wire");
                let encoded = encode_avro_value(&value).expect("encode fixture");
                assert_eq!(encoded.blob, wire, "{name}");
                let decoded = decode_avro_value(&PayloadEnvelope {
                    codec: DEFAULT_CODEC.to_string(),
                    blob: wire.to_string(),
                })
                .expect("decode fixture");
                assert_eq!(decoded, value, "{name}");
                assert_eq!(
                    encode_avro_value(&decoded).expect("reencode fixture").blob,
                    wire,
                    "{name}"
                );
            }
            "decode_reject" => {
                let error = decode_avro_value(&PayloadEnvelope {
                    codec: DEFAULT_CODEC.to_string(),
                    blob: wire.expect("rejection wire").to_string(),
                })
                .expect_err("decode must reject")
                .to_string();
                assert!(error.contains(expected_error.expect("stable decode error")));
            }
            "encode_reject" => {
                let error = encode_avro_value(&value)
                    .expect_err("encode must reject")
                    .to_string();
                assert!(error.contains(expected_error.expect("stable encode error")));
            }
            other => panic!("unsupported failure policy {other}"),
        }

        check_task_boundary(&fixture);
    }
}

#[test]
fn checked_in_codec_regression_corpus_uses_apache_avro() {
    match (
        env::var_os(IDENTITY_REQUEST_ENV),
        env::var_os(IDENTITY_RESPONSE_ENV),
    ) {
        (Some(request), Some(response)) => {
            process_identity_request(Path::new(&request), Path::new(&response))
                .unwrap_or_else(|error| panic!("{error}"));
        }
        (None, None) => check_corpus(),
        _ => {
            panic!("{IDENTITY_REQUEST_ENV} and {IDENTITY_RESPONSE_ENV} must be configured together")
        }
    }
}

#[test]
fn ignored_tagged_value_members_do_not_change_encode_rejection() {
    let value = serde_json::json!({"type": "double", "value": "NaN"});
    let decorated = serde_json::json!({"type": "double", "value": "NaN", "consumer_ignored": true});

    let value = tagged_value(&value);
    let decorated = tagged_value(&decorated);

    assert_eq!(
        canonical_avro_value(value.clone()),
        canonical_avro_value(decorated.clone()),
    );
    assert_eq!(
        encode_avro_value(&value)
            .expect_err("non-finite double must be rejected")
            .to_string(),
        encode_avro_value(&decorated)
            .expect_err("decorated non-finite double must be rejected")
            .to_string(),
    );
}
