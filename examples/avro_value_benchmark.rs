use std::{collections::BTreeMap, env, hint::black_box, process::ExitCode, time::Instant};

use apache_avro::{from_avro_datum, to_avro_datum, types::Value as Datum, Schema};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use durable_workflow::{
    decode_avro_value, encode_avro_value, AvroValue, PayloadEnvelope, DEFAULT_CODEC, JSON_CODEC,
};
use serde_json::{json, Value};

const OLD_SCHEMA: &str = r#"{"type":"record","name":"Payload","namespace":"durable_workflow","fields":[{"name":"json","type":"string"},{"name":"version","type":"int","default":1}]}"#;
const CORPUS_JSON: &str = include_str!("../schema/avro-value-benchmark-v1.json");
const CORPUS_SHA256: &str = "588771404977f2a95fe7d8969c24a15e1c7dd78fe498af9aa2406f82be54b666";

fn corpus() -> Value {
    serde_json::from_str(CORPUS_JSON).expect("shared benchmark corpus must parse")
}

fn adapt_typed(value: &Value) -> AvroValue {
    match value {
        Value::Null => AvroValue::Null,
        Value::Bool(value) => AvroValue::Boolean(*value),
        Value::Number(value) if value.is_i64() => {
            AvroValue::Long(value.as_i64().expect("signed benchmark integer"))
        }
        Value::Number(value) => AvroValue::Double(value.as_f64().expect("benchmark double")),
        Value::String(value) => AvroValue::String(value.clone()),
        Value::Array(values) => AvroValue::Array(values.iter().map(adapt_typed).collect()),
        Value::Object(values) if values.len() == 1 && values.contains_key("$avro_bytes") => {
            AvroValue::Bytes(
                BASE64
                    .decode(
                        values["$avro_bytes"]
                            .as_str()
                            .expect("benchmark bytes adapter must be base64 text"),
                    )
                    .expect("benchmark bytes adapter must contain valid base64"),
            )
        }
        Value::Object(values) => AvroValue::Map(
            values
                .iter()
                .map(|(key, value)| (key.clone(), adapt_typed(value)))
                .collect::<BTreeMap<_, _>>(),
        ),
    }
}

fn old_encode(schema: &Schema, value: &Value) -> Vec<u8> {
    let json = serde_json::to_string(value).expect("sample JSON must encode");
    let mut frame = vec![0];
    frame.extend(
        to_avro_datum(
            schema,
            Datum::Record(vec![
                ("json".into(), Datum::String(json)),
                ("version".into(), Datum::Int(1)),
            ]),
        )
        .expect("old wrapper must encode"),
    );
    frame
}

fn old_decode(schema: &Schema, frame: &[u8]) -> Datum {
    from_avro_datum(schema, &mut &frame[1..], None).expect("old wrapper must decode")
}

fn measure(mut operation: impl FnMut(), iterations: u32) -> f64 {
    let mut samples = Vec::with_capacity(5);
    for _ in 0..5 {
        let started = Instant::now();
        for _ in 0..iterations {
            operation();
        }
        samples.push(started.elapsed().as_secs_f64() * 1_000_000.0 / f64::from(iterations));
    }
    samples.sort_by(f64::total_cmp);
    samples[2]
}

fn envelope_size(codec: &str, blob: String) -> usize {
    serde_json::to_vec(&PayloadEnvelope {
        codec: codec.into(),
        blob,
    })
    .expect("envelope must encode")
    .len()
}

fn main() -> ExitCode {
    let arguments: Vec<_> = env::args().collect();
    let enforce = arguments.iter().any(|argument| argument == "--enforce");
    let iterations = arguments
        .windows(2)
        .find(|window| window[0] == "--iterations")
        .and_then(|window| window[1].parse().ok())
        .unwrap_or(2_000_u32);

    let corpus = corpus();
    let json_value = corpus["value"].clone();
    let typed_value = adapt_typed(&json_value);
    let json_bytes = serde_json::to_vec(&json_value).expect("sample JSON must encode");
    let old_schema = Schema::parse_str(OLD_SCHEMA).expect("old wrapper schema must parse");
    let old_payload = old_encode(&old_schema, &json_value);
    let typed_envelope = encode_avro_value(&typed_value).expect("typed sample must encode");
    let typed_payload = BASE64
        .decode(&typed_envelope.blob)
        .expect("typed payload must be base64");

    let json_encode = measure(
        || {
            black_box(serde_json::to_vec(black_box(&json_value)).unwrap());
        },
        iterations,
    );
    let json_decode = measure(
        || {
            black_box(serde_json::from_slice::<Value>(black_box(&json_bytes)).unwrap());
        },
        iterations,
    );
    let old_encode_us = measure(
        || {
            black_box(old_encode(black_box(&old_schema), black_box(&json_value)));
        },
        iterations,
    );
    let old_decode_us = measure(
        || {
            black_box(old_decode(black_box(&old_schema), black_box(&old_payload)));
        },
        iterations,
    );
    let typed_encode = measure(
        || {
            black_box(encode_avro_value(black_box(&typed_value)).unwrap());
        },
        iterations,
    );
    let typed_decode = measure(
        || {
            black_box(decode_avro_value(black_box(&typed_envelope)).unwrap());
        },
        iterations,
    );

    let results = json!({
        "implementation": "apache-avro",
        "corpus": {
            "schema": corpus["schema"],
            "case": corpus["case"],
            "sha256": CORPUS_SHA256,
        },
        "iterations": iterations,
        "sizes_bytes": {
            "plain_json": {
                "raw": json_bytes.len(),
                "http_envelope": envelope_size(JSON_CODEC, String::from_utf8(json_bytes.clone()).unwrap()),
            },
            "old_json_wrapper": {
                "raw_datum": old_payload.len() - 1,
                "framed": old_payload.len(),
                "http_envelope": envelope_size(DEFAULT_CODEC, BASE64.encode(&old_payload)),
            },
            "fixed_typed_value": {
                "raw_datum": typed_payload.len() - 10,
                "single_object": typed_payload.len(),
                "http_envelope": envelope_size(DEFAULT_CODEC, typed_envelope.blob.clone()),
            },
        },
        "latency_us": {
            "plain_json_encode": json_encode,
            "plain_json_decode": json_decode,
            "old_json_wrapper_encode": old_encode_us,
            "old_json_wrapper_decode": old_decode_us,
            "fixed_typed_value_encode": typed_encode,
            "fixed_typed_value_decode": typed_decode,
        },
    });
    println!(
        "{}",
        serde_json::to_string_pretty(&results).expect("benchmark output must encode")
    );

    if !enforce {
        return ExitCode::SUCCESS;
    }

    let encode_budget = env::var("AVRO_VALUE_ENCODE_BUDGET_US")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(75.0);
    let decode_budget = env::var("AVRO_VALUE_DECODE_BUDGET_US")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(60.0);
    if typed_encode > encode_budget || typed_decode > decode_budget {
        eprintln!(
            "Avro Value production-path regression budget exceeded: encode <= {encode_budget} us, decode <= {decode_budget} us."
        );
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}
