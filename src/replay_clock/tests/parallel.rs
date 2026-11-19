use super::*;

#[test]
fn replay_clock_canonical_v1_uses_exact_signed_microseconds_across_epoch_boundaries_and_offsets() {
    let table: Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/replay-clock-canonical-v1.json"
    ))
    .unwrap();
    for row in table["vectors"].as_array().unwrap() {
        let time = crate::wire_time::parse_instant(row["input"].as_str().unwrap()).unwrap();
        assert_eq!(
            time.as_microsecond().to_string(),
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
            crate::wire_time::format_micros(
                crate::wire_time::Timestamp::from_millisecond(ms).unwrap()
            ),
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
    let too_precise = crate::wire_time::parse_instant("2026-01-01T00:00:00.123456789Z").unwrap();
    assert!(super::super::parallel::milliseconds(too_precise).is_none());
}
