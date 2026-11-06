use super::*;

#[test]
fn replay_clock_canonical_v1_uses_exact_signed_microseconds_across_epoch_boundaries_and_offsets() {
    let table: Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/replay-clock-canonical-v1.json"
    ))
    .unwrap();
    for row in table["vectors"].as_array().unwrap() {
        let time = DateTime::parse_from_rfc3339(row["input"].as_str().unwrap())
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(
            time.timestamp_micros().to_string(),
            row["utc_microseconds"].as_str().unwrap()
        );
        let ms = super::super::parallel::milliseconds(time).unwrap();
        assert_eq!(
            ms.to_string(),
            row["milliseconds"].as_str().unwrap(),
            "{}",
            row["id"]
        );
        assert_eq!(
            DateTime::<Utc>::from_timestamp_millis(ms)
                .unwrap()
                .to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
            row["reconstructed"].as_str().unwrap()
        );
    }
}

#[test]
fn replay_clock_canonical_v1_handles_signed_integer_limits_without_float_or_abs_overflow() {
    use super::super::parallel::round_microseconds;
    for (micro, ms) in [
        (-501, -1),
        (-500, -1),
        (-499, 0),
        (499, 0),
        (500, 1),
        (501, 1),
        (i64::MAX, 9_223_372_036_854_776),
        (i64::MIN, -9_223_372_036_854_776),
    ] {
        assert_eq!(round_microseconds(micro), Some(ms));
    }
    let too_precise = DateTime::parse_from_rfc3339("2026-01-01T00:00:00.123456789Z")
        .unwrap()
        .with_timezone(&Utc);
    assert!(super::super::parallel::milliseconds(too_precise).is_none());
}
