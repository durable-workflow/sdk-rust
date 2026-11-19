use super::*;
use serde_json::Value;

/// Wire text, Unix seconds, nanoseconds and written offset.
const VECTORS: &[(&str, i64, i32, i32)] = &[
    ("2026-11-18T09:15:42Z", 1_794_993_342, 0, 0),
    ("2026-11-18T09:15:42.1Z", 1_794_993_342, 100_000_000, 0),
    (
        "2026-11-18T09:15:42.12+05:30",
        1_794_973_542,
        120_000_000,
        19_800,
    ),
    (
        "2026-11-18T09:15:42.123-08:00",
        1_795_022_142,
        123_000_000,
        -28_800,
    ),
    (
        "2026-11-18T09:15:42.1234+00:01",
        1_794_993_282,
        123_400_000,
        60,
    ),
    (
        "2026-11-18T09:15:42.12345-00:30",
        1_794_995_142,
        123_450_000,
        -1_800,
    ),
    ("2026-11-18T09:15:42.123456Z", 1_794_993_342, 123_456_000, 0),
    (
        "2026-11-18T09:15:42.1234567+23:59",
        1_794_907_002,
        123_456_700,
        86_340,
    ),
    (
        "2026-11-18T09:15:42.12345678-23:59",
        1_795_079_682,
        123_456_780,
        -86_340,
    ),
    (
        "2026-11-18T09:15:42.123456789+01:00",
        1_794_989_742,
        123_456_789,
        3_600,
    ),
    ("2026-11-18T09:15:42.000000000Z", 1_794_993_342, 0, 0),
    (
        "2026-11-18T09:15:42.999999999+14:00",
        1_794_942_942,
        999_999_999,
        50_400,
    ),
    ("1970-01-01T00:00:00Z", 0, 0, 0),
    ("1969-12-31T23:59:59.5Z", -1, 500_000_000, 0),
    ("0000-01-01T00:00:00+01:00", -62_167_222_800, 0, 3_600),
    ("2024-02-29T23:59:59.000001Z", 1_709_251_199, 1_000, 0),
    (
        "9999-12-30T21:59:59.999999999Z",
        253_402_207_199,
        999_999_999,
        0,
    ),
];

fn digits(text: &str) -> usize {
    text.split_once('.').map_or(0, |(_, rest)| {
        rest.bytes().take_while(u8::is_ascii_digit).count()
    })
}

fn parts(timestamp: Timestamp) -> (i64, i32) {
    (timestamp.as_second(), timestamp.subsec_nanosecond())
}

#[test]
fn wire_vectors_parse_to_their_instant_and_offset() {
    for (text, seconds, nanoseconds, offset) in VECTORS {
        let parsed = parse(text).unwrap_or_else(|| panic!("{text} must parse"));
        let expected = Timestamp::new(*seconds, *nanoseconds).unwrap();
        assert_eq!(parsed.timestamp, expected, "{text}");
        assert_eq!(parsed.offset_seconds, *offset, "{text}");
        assert!(is_valid(text), "{text}");
    }
}

#[test]
fn offsets_and_up_to_nine_fraction_digits_round_trip_byte_identically() {
    for (text, ..) in VECTORS {
        let parsed = parse(text).unwrap();
        assert_eq!(
            format(parsed.timestamp, parsed.offset_seconds, digits(text)),
            *text,
            "{text}"
        );
    }
}

#[test]
fn canonical_utc_micros_round_trip_byte_identically() {
    for text in [
        "2026-11-18T09:15:42.000000Z",
        "2026-11-18T09:15:42.123456Z",
        "1969-12-31T23:59:59.999999Z",
        "0001-01-01T00:00:00.000001Z",
        "9999-12-30T21:59:59.999999Z",
    ] {
        assert_eq!(format_micros(parse_instant(text).unwrap()), text);
    }
    // Canonicalisation truncates exactly as chrono's Micros did.
    let parsed = parse("2026-11-18T14:45:42.123456999+05:30").unwrap();
    assert_eq!(
        format_micros(parsed.timestamp),
        "2026-11-18T09:15:42.123456Z"
    );
    assert_eq!(
        format(parsed.timestamp, parsed.offset_seconds, 6),
        "2026-11-18T14:45:42.123456+05:30"
    );
}

/// What chrono 0.4.45 parsed and printed, recorded before it was removed.
fn reference() -> Value {
    serde_json::from_str(include_str!(
        "../../tests/fixtures/wire-time-chrono-reference.json"
    ))
    .unwrap()
}

fn texts(value: &Value) -> impl Iterator<Item = &str> {
    value
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
}

#[test]
fn reference_fixture_covers_the_wire_vectors() {
    let reference = reference();
    let corpus = reference["corpus"].as_array().unwrap();
    assert_eq!(corpus.len(), VECTORS.len() + 512);
    for ((text, seconds, nanoseconds, offset), row) in VECTORS.iter().zip(corpus) {
        assert_eq!(row[0], *text);
        assert_eq!(row[1], *seconds);
        assert_eq!(row[2], *nanoseconds);
        assert_eq!(row[3], *offset);
    }
}

#[test]
fn rejects_text_chrono_rejected() {
    let reference = reference();
    let rejected: Vec<&str> = texts(&reference["rejected"]).collect();
    assert!(rejected.len() >= 29);
    for text in rejected {
        assert!(parse(text).is_none(), "{text:?} must not parse");
        assert!(!is_valid(text), "{text:?} must not validate");
    }
}

#[test]
fn accepts_the_lenient_forms_chrono_accepted() {
    let reference = reference();
    for row in reference["lenient"].as_array().unwrap() {
        let text = row[0].as_str().unwrap();
        assert!(is_valid(text), "{text}");
        assert_eq!(
            format_micros(parse_instant(text).unwrap()),
            row[1],
            "{text}"
        );
    }
}

#[test]
fn matches_chrono_on_instant_offset_and_printed_bytes() {
    let reference = reference();
    let rows = reference["corpus"].as_array().unwrap();
    for row in rows {
        let text = row[0].as_str().unwrap();
        let ours = parse(text).unwrap_or_else(|| panic!("{text} must parse"));
        assert!(is_valid(text), "{text}");
        // chrono split an instant into floor seconds and non-negative nanoseconds.
        assert_eq!(
            ours.timestamp.as_nanosecond(),
            i128::from(row[1].as_i64().unwrap()) * 1_000_000_000
                + i128::from(row[2].as_i64().unwrap()),
            "{text}"
        );
        assert_eq!(
            i64::from(ours.offset_seconds),
            row[3].as_i64().unwrap(),
            "{text}"
        );
        assert_eq!(
            format(ours.timestamp, ours.offset_seconds, digits(text)),
            text,
            "{text}"
        );
        for (column, width) in [0, 3, 6, 9].into_iter().enumerate() {
            assert_eq!(
                format(ours.timestamp, ours.offset_seconds, width),
                row[4 + column],
                "{text} at {width} digits"
            );
            assert_eq!(
                format(ours.timestamp, 0, width),
                row[8 + column],
                "{text} at {width} UTC digits"
            );
        }
        assert_eq!(format_micros(ours.timestamp), row[10], "{text}");
        assert_eq!(format_utc_auto_si(ours.timestamp), row[12], "{text}");
    }
}

#[test]
fn documented_limits_differ_only_outside_server_time() {
    let reference = reference();
    // A leap second is the instant chrono's Unix time gave it, one second on.
    let leap = &reference["leap_second"];
    let ours = parse_instant(leap["text"].as_str().unwrap()).unwrap();
    assert_eq!(
        parts(ours),
        (
            leap["chrono_unix_seconds"].as_i64().unwrap() + 1,
            leap["chrono_nanoseconds"].as_i64().unwrap() as i32 - 1_000_000_000
        )
    );
    assert_eq!(format_micros(ours), "2017-01-01T00:00:00.500000Z");
    // Syntax validation keeps chrono's range; instants stop at jiff's.
    let end = &reference["end_of_range"];
    let text = end["text"].as_str().unwrap();
    assert_eq!(end["chrono_accepts"], true);
    assert!(is_valid(text));
    assert!(parse(text).is_none());
}

/// Every RFC 3339 string in a committed fixture.
fn fixture_timestamps(value: &Value, found: &mut Vec<String>) {
    match value {
        Value::String(text) if text.len() >= 20 && is_valid(text) => found.push(text.clone()),
        Value::Array(items) => items
            .iter()
            .for_each(|item| fixture_timestamps(item, found)),
        Value::Object(fields) => fields
            .values()
            .for_each(|item| fixture_timestamps(item, found)),
        _ => {}
    }
}

#[test]
fn every_fixture_timestamp_round_trips_byte_identically() {
    fn walk(directory: &std::path::Path, files: &mut Vec<std::path::PathBuf>) {
        for entry in std::fs::read_dir(directory).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                walk(&path, files);
            } else if path
                .extension()
                .is_some_and(|extension| extension == "json")
            {
                files.push(path);
            }
        }
    }
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut files = Vec::new();
    walk(&root.join("tests/fixtures"), &mut files);
    walk(&root.join("schema"), &mut files);
    files.sort();
    let mut checked = 0;
    for file in &files {
        // The chrono reference is checked above, including its out-of-range text.
        if file.ends_with("wire-time-chrono-reference.json") {
            continue;
        }
        let Ok(document) = serde_json::from_slice::<Value>(&std::fs::read(file).unwrap()) else {
            continue;
        };
        let mut found = Vec::new();
        fixture_timestamps(&document, &mut found);
        for text in found {
            // A recorded wire value in the form the SDK prints (`T`, `Z` or a
            // non-zero `±HH:MM`, at most nine digits, no leap second) prints
            // back to its own bytes at its own precision and offset.
            let canonical = text.as_bytes()[10] == b'T'
                && !text.contains(['z', '\u{2212}'])
                && !text.ends_with("+00:00")
                && !text.ends_with("-00:00")
                && digits(&text) <= 9
                && &text[17..19] != "60";
            if !canonical {
                continue;
            }
            let parsed = parse(&text).unwrap_or_else(|| panic!("{}: {text}", file.display()));
            assert_eq!(
                format(parsed.timestamp, parsed.offset_seconds, digits(&text)),
                text,
                "{}",
                file.display()
            );
            checked += 1;
        }
    }
    assert!(checked > 1000, "only {checked} fixture timestamps checked");
}

#[test]
fn until_is_non_negative_std_duration() {
    let earlier = parse_instant("2026-11-18T09:15:42Z").unwrap();
    let later = parse_instant("2026-11-18T09:15:43.25Z").unwrap();
    assert_eq!(
        until(later, earlier),
        Some(std::time::Duration::from_millis(1250))
    );
    assert_eq!(until(earlier, later), None);
    assert_eq!(until(earlier, earlier), Some(std::time::Duration::ZERO));
    assert!(now().is_some_and(|now| now > earlier));
}
