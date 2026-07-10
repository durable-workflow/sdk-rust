use apache_avro::{from_avro_datum, from_value, Schema};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use durable_workflow::{decode_payload, encode_payload, PayloadEnvelope, DEFAULT_CODEC};
use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Deserialize)]
struct CompatibilityFixture {
    schema: String,
    json: String,
    value: Value,
    producers: Vec<ProducerFixture>,
}

#[derive(Debug, Deserialize)]
struct ProducerFixture {
    runtime: String,
    library: String,
    blob: String,
}

#[derive(Debug, Deserialize)]
struct GenericWrapper {
    json: String,
    version: i32,
}

fn fixture() -> CompatibilityFixture {
    serde_json::from_str(include_str!("fixtures/avro_generic_wrapper.json"))
        .expect("valid Avro compatibility fixture")
}

#[test]
fn decodes_python_and_php_official_avro_datums() {
    let fixture = fixture();
    assert_eq!(fixture.producers.len(), 2);

    for producer in fixture.producers {
        assert!(matches!(producer.runtime.as_str(), "Python" | "PHP"));
        assert!(producer.library.contains("avro"));

        let envelope = PayloadEnvelope {
            codec: DEFAULT_CODEC.to_string(),
            blob: producer.blob,
        };
        let decoded: Value = decode_payload(&envelope).unwrap_or_else(|error| {
            panic!(
                "could not decode {} {} fixture: {error}",
                producer.runtime, producer.library
            )
        });
        assert_eq!(decoded, fixture.value);
    }
}

#[test]
fn rust_datum_matches_official_python_and_php_output() {
    let fixture = fixture();
    let envelope = encode_payload(&fixture.value, DEFAULT_CODEC).expect("encode Rust payload");

    for producer in &fixture.producers {
        assert_eq!(
            envelope.blob, producer.blob,
            "Rust output differs from {} {}",
            producer.runtime, producer.library
        );
    }

    let bytes = BASE64
        .decode(&envelope.blob)
        .expect("Rust output is valid base64");
    assert_eq!(bytes.first(), Some(&0x00));

    let schema = Schema::parse_str(&fixture.schema).expect("parse wrapper schema");
    let mut encoded_datum = &bytes[1..];
    let datum = from_avro_datum(&schema, &mut encoded_datum, None)
        .expect("official Apache Avro decodes the Rust datum");
    let wrapper: GenericWrapper = from_value(&datum).expect("decode wrapper record");

    assert_eq!(wrapper.json, fixture.json);
    assert_eq!(wrapper.version, 1);
}
