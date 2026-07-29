use std::{collections::BTreeMap, fs, path::Path};

use durable_workflow::{
    decode_avro_value, encode_avro_value, AvroValue, PayloadEnvelope,
    AVRO_VALUE_SCHEMA_FINGERPRINT_HEX, DEFAULT_CODEC,
};
use serde_json::Value;

const FIXTURE_DIRECTORY: &str = "tests/fixtures/codec-regressions";
const FIXTURE_MANIFEST: &str = include_str!("fixtures/codec-regressions/manifest.txt");

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

#[test]
fn checked_in_codec_regression_corpus_uses_apache_avro() {
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
    }
}
