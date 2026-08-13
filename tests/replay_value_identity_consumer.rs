use std::{env, fs, path::Path};

use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use durable_workflow::{
    decode_avro_value, encode_avro_value, json, AvroValue, PayloadEnvelope, Value, DEFAULT_CODEC,
};

const IDENTITY_SCHEMA: &str = "durable-workflow.replay-value-identity/v1";
const REQUEST_ENV: &str = "DURABLE_WORKFLOW_REPLAY_VALUE_IDENTITY_REQUEST";
const RESPONSE_ENV: &str = "DURABLE_WORKFLOW_REPLAY_VALUE_IDENTITY_RESPONSE";

fn canonical_avro_value(value: AvroValue) -> Value {
    match value {
        AvroValue::Null => json!({"type": "null"}),
        AvroValue::Boolean(value) => json!({"type": "boolean", "value": value}),
        AvroValue::Long(value) => json!({"type": "long", "value": value}),
        AvroValue::Double(value) => json!({"type": "double", "value": value}),
        AvroValue::Bytes(value) => json!({"type": "bytes", "value_base64": BASE64.encode(value)}),
        AvroValue::String(value) => json!({"type": "string", "value": value}),
        AvroValue::Array(values) => json!({
            "type": "array",
            "value": values.into_iter().map(canonical_avro_value).collect::<Vec<_>>()
        }),
        AvroValue::Map(values) => json!({
            "type": "map",
            "value": values.into_iter().map(|(key, value)| (key, canonical_avro_value(value))).collect::<serde_json::Map<_, _>>()
        }),
    }
}

fn canonical_replay_value(value: &Value, fallback_codec: &str) -> Result<Value, String> {
    let (codec, blob) = value
        .as_object()
        .and_then(|envelope| {
            Some((
                envelope.get("codec")?.as_str()?,
                envelope.get("blob")?.as_str()?,
            ))
        })
        .or_else(|| value.as_str().map(|blob| (fallback_codec, blob)))
        .ok_or_else(|| {
            "replay value is not a payload blob or published payload envelope".to_string()
        })?;

    if codec != DEFAULT_CODEC {
        return Err(format!("unsupported_payload_codec: workflow payload codec {codec:?} is not supported; use codec=\"avro\""));
    }

    decode_avro_value(&PayloadEnvelope {
        codec: codec.to_string(),
        blob: blob.to_string(),
    })
    .map(canonical_avro_value)
    .map_err(|error| format!("decode Avro replay value through official consumer: {error}"))
}

fn process_request(request_path: &Path, response_path: &Path) -> Result<(), String> {
    let request: Value = serde_json::from_str(
        &fs::read_to_string(request_path)
            .map_err(|error| format!("read {}: {error}", request_path.display()))?,
    )
    .map_err(|error| format!("parse {}: {error}", request_path.display()))?;
    if request["schema"] != IDENTITY_SCHEMA {
        return Err("replay value identity request has an unsupported schema".to_string());
    }
    let request_id = request["request_id"]
        .as_str()
        .ok_or_else(|| "replay value identity request has no request_id".to_string())?;
    let fallback_codec = request["fallback_codec"]
        .as_str()
        .ok_or_else(|| "replay value identity request has no fallback_codec".to_string())?;
    let response = json!({
        "schema": IDENTITY_SCHEMA,
        "request_id": request_id,
        "value": canonical_replay_value(request.get("value").ok_or_else(|| "replay value identity request has no value".to_string())?, fallback_codec)?,
    });
    fs::write(
        response_path,
        serde_json::to_vec(&response)
            .map_err(|error| format!("encode replay value identity response: {error}"))?,
    )
    .map_err(|error| format!("write {}: {error}", response_path.display()))
}

#[test]
fn canonical_replay_value_uses_only_the_official_avro_consumer() {
    match (env::var_os(REQUEST_ENV), env::var_os(RESPONSE_ENV)) {
        (Some(request), Some(response)) => {
            process_request(Path::new(&request), Path::new(&response))
                .unwrap_or_else(|error| panic!("{error}"))
        }
        (None, None) => {
            let avro = serde_json::to_value(
                encode_avro_value(&AvroValue::String("captured-once".to_string()))
                    .expect("encode Avro replay value"),
            )
            .expect("serialize Avro envelope");
            assert_eq!(
                canonical_replay_value(&avro, DEFAULT_CODEC).expect("decode Avro"),
                json!({"type": "string", "value": "captured-once"})
            );
            assert!(canonical_replay_value(
                &json!({"codec": "json", "blob": "\"captured-once\""}),
                DEFAULT_CODEC
            )
            .expect_err("JSON must fail")
            .contains("unsupported_payload_codec"));
        }
        _ => panic!("{REQUEST_ENV} and {RESPONSE_ENV} must be configured together"),
    }
}
