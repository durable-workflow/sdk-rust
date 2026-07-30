use std::collections::BTreeMap;

use apache_avro::{
    from_avro_datum, rabin::Rabin, to_avro_datum, types::Value as AvroDatum, Schema,
};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use durable_workflow::{
    decode_avro_value, encode_avro_value, AvroValue, PayloadEnvelope,
    AVRO_VALUE_SCHEMA_FINGERPRINT, AVRO_VALUE_SCHEMA_FINGERPRINT_HEX, AVRO_VALUE_SCHEMA_JSON,
    DEFAULT_CODEC,
};

const PACKAGED_AVRO_VALUE_SCHEMA_JSON: &str =
    include_str!("../schema/durable_workflow.protocol.Value.v1.avsc");
const GOLDEN_FIXTURE_JSON: &str = include_str!("../schema/avro-value-v1-golden.json");

fn cases() -> Vec<(&'static str, AvroValue, &'static str)> {
    vec![
        ("null", AvroValue::Null, "wwHioz3/VYAiNwA="),
        (
            "boolean_false",
            AvroValue::Boolean(false),
            "wwHioz3/VYAiNwIA",
        ),
        ("boolean_true", AvroValue::Boolean(true), "wwHioz3/VYAiNwIB"),
        (
            "long_min",
            AvroValue::Long(i64::MIN),
            "wwHioz3/VYAiNwT///////////8B",
        ),
        (
            "long_max",
            AvroValue::Long(i64::MAX),
            "wwHioz3/VYAiNwT+//////////8B",
        ),
        ("long_7", AvroValue::Long(7), "wwHioz3/VYAiNwQO"),
        (
            "double_7",
            AvroValue::Double(7.0),
            "wwHioz3/VYAiNwYAAAAAAAAcQA==",
        ),
        (
            "negative_zero",
            AvroValue::Double(-0.0),
            "wwHioz3/VYAiNwYAAAAAAAAAgA==",
        ),
        (
            "bytes_00ff",
            AvroValue::Bytes(vec![0, 255]),
            "wwHioz3/VYAiNwgEAP8=",
        ),
        (
            "string_utf8",
            AvroValue::String("héllo".to_string()),
            "wwHioz3/VYAiNwoMaMOpbGxv",
        ),
        (
            "array",
            AvroValue::Array(vec![
                AvroValue::Null,
                AvroValue::Boolean(true),
                AvroValue::Long(7),
                AvroValue::Double(7.0),
                AvroValue::Bytes(vec![0, 255]),
                AvroValue::String("text".to_string()),
            ]),
            "wwHioz3/VYAiNwwMAAIBBA4GAAAAAAAAHEAIBAD/Cgh0ZXh0AA==",
        ),
        (
            "map",
            AvroValue::Map(BTreeMap::from([
                ("a".to_string(), AvroValue::Long(1)),
                (
                    "b".to_string(),
                    AvroValue::Array(vec![AvroValue::Boolean(false)]),
                ),
            ])),
            "wwHioz3/VYAiNw4EAmEEAgJiDAICAAAA",
        ),
        (
            "map_empty",
            AvroValue::Map(BTreeMap::new()),
            "wwHioz3/VYAiNw4A",
        ),
        (
            "map_key_0",
            AvroValue::Map(BTreeMap::from([(
                "0".to_string(),
                AvroValue::String("zero".to_string()),
            )])),
            "wwHioz3/VYAiNw4CAjAKCHplcm8A",
        ),
        (
            "map_keys_0_1",
            AvroValue::Map(BTreeMap::from([
                ("0".to_string(), AvroValue::String("zero".to_string())),
                ("1".to_string(), AvroValue::String("one".to_string())),
            ])),
            "wwHioz3/VYAiNw4EAjAKCHplcm8CMQoGb25lAA==",
        ),
        (
            "nested",
            AvroValue::Map(BTreeMap::from([(
                "items".to_string(),
                AvroValue::Array(vec![
                    AvroValue::Map(BTreeMap::from([(
                        "enabled".to_string(),
                        AvroValue::Boolean(true),
                    )])),
                    AvroValue::Bytes(b"bytes".to_vec()),
                    AvroValue::Double(-2.5),
                ]),
            )])),
            "wwHioz3/VYAiNw4CCml0ZW1zDAYOAg5lbmFibGVkAgEACApieXRlcwYAAAAAAAAEwAAA",
        ),
    ]
}

#[test]
fn packaged_runtime_schema_fingerprints_match_golden_fixture() {
    assert_eq!(AVRO_VALUE_SCHEMA_JSON, PACKAGED_AVRO_VALUE_SCHEMA_JSON);

    let schema = Schema::parse_str(PACKAGED_AVRO_VALUE_SCHEMA_JSON).expect("parse Value schema");
    let fingerprint = schema.fingerprint::<Rabin>();
    assert_eq!(fingerprint.bytes.as_slice(), AVRO_VALUE_SCHEMA_FINGERPRINT);
    let fingerprint_hex = fingerprint
        .bytes
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    assert_eq!(fingerprint_hex, AVRO_VALUE_SCHEMA_FINGERPRINT_HEX);

    let fixture: serde_json::Value =
        serde_json::from_str(GOLDEN_FIXTURE_JSON).expect("parse checked-in golden fixture");
    assert_eq!(
        fixture["fingerprint"]
            .as_str()
            .expect("fixture fingerprint"),
        fingerprint_hex
    );

    for case in fixture["cases"].as_array().expect("golden cases") {
        let frame = BASE64
            .decode(case["wire_base64"].as_str().expect("golden wire bytes"))
            .expect("base64 golden");
        assert_eq!(
            &frame[2..10],
            fingerprint.bytes.as_slice(),
            "{}",
            case["name"]
        );
    }
}

#[test]
fn rust_matches_cross_language_golden_single_object_bytes() {
    let fixture: serde_json::Value =
        serde_json::from_str(GOLDEN_FIXTURE_JSON).expect("parse checked-in golden fixture");
    let fixture_cases = fixture["cases"].as_array().expect("golden cases");
    assert_eq!(fixture_cases.len(), cases().len());

    for (name, value, expected) in cases() {
        let fixture_case = fixture_cases
            .iter()
            .find(|case| case["name"] == name)
            .unwrap_or_else(|| panic!("missing shared fixture case {name}"));
        assert_eq!(fixture_case["wire_base64"], expected, "{name}");
        assert_eq!(
            decode_avro_value(&PayloadEnvelope {
                codec: DEFAULT_CODEC.to_string(),
                blob: expected.to_string(),
            })
            .unwrap_or_else(|error| panic!("{name}: {error}")),
            value,
            "{name}"
        );
        let encoded = encode_avro_value(&value).unwrap_or_else(|error| panic!("{name}: {error}"));
        if !matches!(value, AvroValue::Map(_)) {
            assert_eq!(encoded.blob, expected, "{name}");
        }
        let decoded = decode_avro_value(&encoded).unwrap_or_else(|error| panic!("{name}: {error}"));
        assert_eq!(decoded, value, "{name}");
        let reencoded =
            encode_avro_value(&decoded).unwrap_or_else(|error| panic!("{name}: {error}"));
        assert_eq!(
            decode_avro_value(&reencoded).unwrap_or_else(|error| panic!("{name}: {error}")),
            value,
            "{name}"
        );
        assert_eq!(
            &BASE64.decode(expected).expect("base64 golden")[..10],
            &[&[0xc3, 0x01], AVRO_VALUE_SCHEMA_FINGERPRINT.as_slice()].concat(),
            "{name}"
        );
    }
}

#[test]
fn shared_malformed_frames_are_rejected() {
    let fixture: serde_json::Value =
        serde_json::from_str(GOLDEN_FIXTURE_JSON).expect("parse checked-in golden fixture");
    for case in fixture["malformed_frames"]
        .as_array()
        .expect("malformed frames")
    {
        let error = decode_avro_value(&PayloadEnvelope {
            codec: DEFAULT_CODEC.to_string(),
            blob: case["wire_base64"]
                .as_str()
                .expect("wire bytes")
                .to_string(),
        })
        .expect_err("malformed frame must fail")
        .to_string();
        assert!(
            error.contains(case["error"].as_str().expect("error contract")),
            "{}: {error}",
            case["name"]
        );
    }
}

#[test]
fn invalid_base64_and_decoded_non_magic_bytes_use_distinct_framing_branches() {
    let invalid_base64_error = decode_avro_value(&PayloadEnvelope {
        codec: DEFAULT_CODEC.to_string(),
        blob: "%%%".to_string(),
    })
    .expect_err("invalid base64 payload envelope must fail")
    .to_string();
    assert!(invalid_base64_error.contains("invalid_payload_framing"));
    assert!(invalid_base64_error.contains("expected strict base64 Avro single-object bytes"));

    let fixture: serde_json::Value =
        serde_json::from_str(GOLDEN_FIXTURE_JSON).expect("parse checked-in golden fixture");
    let decoded_non_magic_bytes = fixture["malformed_frames"]
        .as_array()
        .expect("malformed frames")
        .iter()
        .find(|case| case["name"] == "decoded_non_magic_bytes")
        .expect("decoded non-magic bytes case");
    let canonical_blob = decoded_non_magic_bytes["wire_base64"]
        .as_str()
        .expect("wire bytes");
    assert_eq!(
        BASE64.decode(canonical_blob).expect("canonical base64"),
        b"%%%"
    );

    let non_magic_error = decode_avro_value(&PayloadEnvelope {
        codec: DEFAULT_CODEC.to_string(),
        blob: canonical_blob.to_string(),
    })
    .expect_err("decoded non-magic bytes must fail")
    .to_string();
    assert!(non_magic_error.contains("invalid_payload_framing"));
    assert!(non_magic_error.contains("expected Avro single-object magic c301"));
    assert_ne!(invalid_base64_error, non_magic_error);
}

#[test]
fn shared_alternate_map_orders_decode_to_the_same_nested_value() {
    let fixture: serde_json::Value =
        serde_json::from_str(GOLDEN_FIXTURE_JSON).expect("parse checked-in golden fixture");
    let expected = AvroValue::Map(BTreeMap::from([
        (
            "outer".to_string(),
            AvroValue::Array(vec![AvroValue::Map(BTreeMap::from([
                ("left".to_string(), AvroValue::Long(1)),
                ("right".to_string(), AvroValue::Bytes(b"x".to_vec())),
            ]))]),
        ),
        ("tail".to_string(), AvroValue::String("done".to_string())),
    ]));

    for blob in fixture["alternate_map_orders"][0]["wire_base64"]
        .as_array()
        .expect("alternate map frames")
    {
        let decoded = decode_avro_value(&PayloadEnvelope {
            codec: DEFAULT_CODEC.to_string(),
            blob: blob.as_str().expect("alternate map frame").to_string(),
        })
        .expect("decode alternate map order");
        assert_eq!(decoded, expected);
        let reencoded = encode_avro_value(&decoded).expect("reencode alternate map value");
        assert_eq!(
            decode_avro_value(&reencoded).expect("decode reencoded alternate map value"),
            expected
        );
    }
}

#[test]
fn rust_decodes_the_shared_nested_cross_language_golden() {
    let fixture: serde_json::Value =
        serde_json::from_str(GOLDEN_FIXTURE_JSON).expect("parse checked-in golden fixture");
    let nested = fixture["cases"]
        .as_array()
        .expect("golden cases")
        .iter()
        .find(|case| case["name"] == "nested")
        .expect("nested golden");
    let envelope = PayloadEnvelope {
        codec: DEFAULT_CODEC.to_string(),
        blob: nested["wire_base64"]
            .as_str()
            .expect("nested wire bytes")
            .to_string(),
    };
    let expected = AvroValue::Map(BTreeMap::from([(
        "items".to_string(),
        AvroValue::Array(vec![
            AvroValue::Map(BTreeMap::from([(
                "enabled".to_string(),
                AvroValue::Boolean(true),
            )])),
            AvroValue::Bytes(b"bytes".to_vec()),
            AvroValue::Double(-2.5),
        ]),
    )]));

    assert_eq!(
        decode_avro_value(&envelope).expect("decode nested golden"),
        expected
    );
}

#[test]
fn rejects_non_finite_values_and_unknown_fingerprints() {
    let error = encode_avro_value(&AvroValue::Double(f64::NAN))
        .expect_err("NaN must fail")
        .to_string();
    assert!(error.contains("non_finite_float"));

    let mut bytes = BASE64.decode(cases()[0].2).expect("base64 golden");
    bytes[2] ^= 0xff;
    let error = decode_avro_value(&PayloadEnvelope {
        codec: DEFAULT_CODEC.to_string(),
        blob: BASE64.encode(bytes),
    })
    .expect_err("unknown fingerprint must fail")
    .to_string();
    assert!(error.contains("unsupported_payload_schema"));
}

#[test]
fn appended_named_branch_resolves_old_data_and_old_reader_rejects_new_branch() {
    let v1_json: serde_json::Value =
        serde_json::from_str(AVRO_VALUE_SCHEMA_JSON).expect("parse schema json");
    let mut v2_json = v1_json.clone();
    v2_json["fields"][0]["type"]
        .as_array_mut()
        .expect("Value union")
        .push(serde_json::json!({
            "type": "record",
            "name": "TimestampValue",
            "fields": [{"name": "timestamp", "type": "string"}]
        }));
    let v1 = Schema::parse_str(&v1_json.to_string()).expect("v1 schema");
    let v2 = Schema::parse_str(&v2_json.to_string()).expect("v2 schema");

    let old = AvroDatum::Record(vec![(
        "value".to_string(),
        AvroDatum::Union(
            2,
            Box::new(AvroDatum::Record(vec![(
                "long".to_string(),
                AvroDatum::Long(7),
            )])),
        ),
    )]);
    let old_bytes = to_avro_datum(&v1, old).expect("encode old datum");
    let mut old_slice = old_bytes.as_slice();
    let resolved = from_avro_datum(&v1, &mut old_slice, Some(&v2)).expect("resolve old datum");
    assert!(matches!(resolved, AvroDatum::Record(_)));

    let new = AvroDatum::Record(vec![(
        "value".to_string(),
        AvroDatum::Union(
            8,
            Box::new(AvroDatum::Record(vec![(
                "timestamp".to_string(),
                AvroDatum::String("2026-07-28T00:00:00Z".to_string()),
            )])),
        ),
    )]);
    let new_bytes = to_avro_datum(&v2, new).expect("encode new datum");
    let mut new_slice = new_bytes.as_slice();
    assert!(from_avro_datum(&v2, &mut new_slice, Some(&v1)).is_err());
}
