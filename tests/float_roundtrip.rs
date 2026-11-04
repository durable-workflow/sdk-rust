//! Floats in Server JSON documents must decode to the exact double that was
//! sent. Without serde_json's `float_roundtrip` feature some 16-17 digit
//! values decode one ULP off, which changes re-emitted digits and makes replay
//! refuse its own recorded search attributes (sdk-rust#181).

use durable_workflow::{
    decode_payload, encode_payload, json,
    testing::{WorkflowEnvironment, WorkflowHistory},
    SearchAttributeUpdate, Value, DEFAULT_CODEC,
};

// Shortest-form doubles that the default serde_json parser rounds incorrectly.
const CASES: [(&str, f64); 3] = [
    ("1.4820142857142857", 1.482_014_285_714_285_7),
    ("124435.71428571429", 124_435.714_285_714_29),
    ("0.7071067811865476", 0.707_106_781_186_547_6),
];

#[test]
fn server_json_floats_decode_exactly_and_reencode_with_the_same_digits() {
    for (text, expected) in CASES {
        let decoded: f64 = serde_json::from_str(text).unwrap();
        assert_eq!(decoded.to_bits(), expected.to_bits(), "decode {text}");
        assert_eq!(serde_json::to_string(&decoded).unwrap(), text);
    }

    let document = format!("[{}]", CASES.map(|(text, _)| text).join(","));
    let value: Value = serde_json::from_str(&document).unwrap();
    assert_eq!(serde_json::to_string(&value).unwrap(), document);
}

#[test]
fn json_document_floats_keep_their_bits_through_the_avro_payload_codec() {
    for (text, expected) in CASES {
        let value: Value = serde_json::from_str(&format!("{{\"amount\":{text}}}")).unwrap();
        let envelope = encode_payload(&value, DEFAULT_CODEC).unwrap();
        let decoded: Value = decode_payload(&envelope).unwrap();
        let amount = decoded["amount"].as_f64().unwrap();
        assert_eq!(amount.to_bits(), expected.to_bits(), "payload {text}");
    }
}

#[tokio::test]
async fn exported_history_text_replays_float_search_attributes_it_recorded() {
    let mut env = WorkflowEnvironment::new().unwrap();
    env.register_workflow("scores", |ctx, _| async move {
        let mut update = SearchAttributeUpdate::new();
        for (index, (_, value)) in CASES.into_iter().enumerate() {
            update = update.float(format!("Score{index}"), value)?;
        }
        ctx.upsert_search_attributes(update)?;
        Ok(json!("done"))
    });
    env.run_workflow("scores", ()).await.unwrap();

    // A `dw workflow:history-export` file is JSON text; replay it from text.
    let text = serde_json::to_string(&env.last_history().unwrap().export()).unwrap();
    for (_, value) in CASES {
        assert!(
            text.contains(&serde_json::to_string(&value).unwrap()),
            "export must carry the shortest digits for {value}"
        );
    }
    let export: Value = serde_json::from_str(&text).unwrap();
    let history = WorkflowHistory::from_export(export).unwrap();
    let report = env.replay(&history).unwrap();
    assert!(report.completed);
}

#[test]
fn shortest_form_doubles_parse_to_the_same_bits() {
    // A fixed xorshift sweep over finite doubles. Rust's `{:?}` prints the
    // shortest form that round-trips, as PHP, Python and the Server emit.
    let mut state: u64 = 0x9e37_79b9_7f4a_7c15;
    let mut checked = 0;
    while checked < 20_000 {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let expected = f64::from_bits(state);
        if !expected.is_finite() {
            continue;
        }
        let text = format!("{expected:?}");
        let decoded: f64 = serde_json::from_str(&text).unwrap();
        assert_eq!(
            expected.to_bits(),
            decoded.to_bits(),
            "{text} decoded as {decoded:?}"
        );
        checked += 1;
    }
}
