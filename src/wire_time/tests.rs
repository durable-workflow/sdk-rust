use super::*;
use chrono::{DateTime, SecondsFormat, Utc};

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

#[test]
fn rejects_text_chrono_rejected() {
    for text in [
        "",
        "2026-11-18",
        "2026-11-18T09:15:42",
        "2026-11-18T09:15Z",
        "2026-11-18T09:15:42.Z",
        "2026-11-18T09:15:42,5Z",
        "2026-11-18T09:15:42+0530",
        "2026-11-18T09:15:42+05",
        "2026-11-18T09:15:42+05:3",
        "2026-11-18T09:15:42+05:60",
        "2026-11-18T09:15:42+24:00",
        "2026-11-18T09:15:42Z ",
        "2026-11-18T09:15:42ZZ",
        " 2026-11-18T09:15:42Z",
        "2026-11-18T09:15:42z[UTC]",
        "2026-11-18T24:00:00Z",
        "2026-11-18T09:60:00Z",
        "2026-11-18T09:15:61Z",
        "2026-02-29T09:15:42Z",
        "2026-13-01T09:15:42Z",
        "2026-00-01T09:15:42Z",
        "2026-11-00T09:15:42Z",
        "+2026-11-18T09:15:42Z",
        "20261118T091542Z",
        "2026-11-18_09:15:42Z",
        "2026-11-18T09:15:42.123GMT",
        "2026-11-18T09:15:42\u{2212}0500",
        "2026-11-18T09:15:42.１Z",
        "２026-11-18T09:15:42Z",
    ] {
        assert!(parse(text).is_none(), "{text:?} must not parse");
        assert!(!is_valid(text), "{text:?} must not validate");
        assert!(
            DateTime::parse_from_rfc3339(text).is_err(),
            "{text:?} must stay rejected by chrono too"
        );
    }
}

#[test]
fn accepts_the_lenient_forms_chrono_accepted() {
    for (text, canonical) in [
        ("2026-11-18t09:15:42z", "2026-11-18T09:15:42.000000Z"),
        ("2026-11-18 09:15:42Z", "2026-11-18T09:15:42.000000Z"),
        ("2026-11-18T09:15:42-00:00", "2026-11-18T09:15:42.000000Z"),
        ("2026-11-18T09:15:42+00:00", "2026-11-18T09:15:42.000000Z"),
        (
            "2026-11-18T04:15:42\u{2212}05:00",
            "2026-11-18T09:15:42.000000Z",
        ),
        (
            "2026-11-18T09:15:42.1234567891234Z",
            "2026-11-18T09:15:42.123456Z",
        ),
    ] {
        assert_eq!(format_micros(parse_instant(text).unwrap()), canonical);
        assert!(DateTime::parse_from_rfc3339(text).is_ok(), "{text}");
    }
}

#[test]
fn matches_chrono_on_instant_offset_and_printed_bytes() {
    let mut corpus: Vec<String> = VECTORS.iter().map(|(text, ..)| text.to_string()).collect();
    // A deterministic spread of instants, offsets and fraction widths.
    let mut state = 0x9E37_79B9_7F4A_7C15_u64;
    let mut next = || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    for _ in 0..4096 {
        // Years 0001 through 9999, kept inside jiff's range at either offset.
        let seconds = (next() % (315_537_811_200 - 3 * 86_400)) as i64 - 62_135_596_800 + 86_400;
        let nanoseconds = (next() % 1_000_000_000) as u32;
        let offset = ((next() % 2879) as i32 - 1439) * 60;
        let width = (next() % 10) as usize;
        let time = chrono::DateTime::from_timestamp(seconds, nanoseconds)
            .unwrap()
            .with_timezone(&chrono::FixedOffset::east_opt(offset).unwrap());
        let text = time.to_rfc3339_opts(SecondsFormat::Nanos, true);
        let text = match width {
            0 => text.replacen(&text[19..29], "", 1),
            width => text.replacen(&text[19..29], &text[19..20 + width], 1),
        };
        corpus.push(text);
    }
    for text in &corpus {
        let ours = parse(text).unwrap_or_else(|| panic!("{text} must parse"));
        let theirs = DateTime::parse_from_rfc3339(text).unwrap();
        assert_eq!(
            to_chrono(ours.timestamp),
            theirs.with_timezone(&Utc),
            "{text}"
        );
        assert_eq!(
            ours.offset_seconds,
            theirs.offset().local_minus_utc(),
            "{text}"
        );
        assert_eq!(
            format(ours.timestamp, ours.offset_seconds, digits(text)),
            *text,
            "{text}"
        );
        for (width, form) in [
            (0, SecondsFormat::Secs),
            (3, SecondsFormat::Millis),
            (6, SecondsFormat::Micros),
            (9, SecondsFormat::Nanos),
        ] {
            assert_eq!(
                format(ours.timestamp, ours.offset_seconds, width),
                theirs.to_rfc3339_opts(form, true),
                "{text} at {width} digits"
            );
            assert_eq!(
                format(ours.timestamp, 0, width),
                theirs.with_timezone(&Utc).to_rfc3339_opts(form, true),
                "{text} at {width} UTC digits"
            );
        }
        assert_eq!(
            format_utc_auto_si(ours.timestamp),
            theirs.with_timezone(&Utc).to_rfc3339()
        );
        assert_eq!(
            format_micros(ours.timestamp),
            theirs
                .with_timezone(&Utc)
                .format("%Y-%m-%dT%H:%M:%S%.6fZ")
                .to_string()
        );
    }
}

#[test]
fn public_chrono_boundary_is_lossless() {
    for (text, ..) in VECTORS {
        let instant = parse_instant(text).unwrap();
        let public = to_chrono(instant);
        assert_eq!(from_chrono(public), instant, "{text}");
        assert_eq!(
            public.to_rfc3339_opts(SecondsFormat::Nanos, true),
            format(instant, 0, 9),
            "{text}"
        );
    }
    let far = DateTime::<Utc>::from_timestamp(300_000_000_000, 0).unwrap();
    assert_eq!(from_chrono(far), Timestamp::MAX);
    assert_eq!(from_chrono(DateTime::<Utc>::MIN_UTC), Timestamp::MIN);
}

#[test]
fn documented_limits_differ_only_outside_server_time() {
    // A leap second is the instant chrono's Unix time gave it.
    let leap = "2016-12-31T23:59:60.5Z";
    let chrono = DateTime::parse_from_rfc3339(leap).unwrap();
    let ours = parse_instant(leap).unwrap();
    assert_eq!(
        parts(ours),
        (
            chrono.timestamp() + 1,
            chrono.timestamp_subsec_nanos() as i32 - 1_000_000_000
        )
    );
    assert_eq!(format_micros(ours), "2017-01-01T00:00:00.500000Z");
    // Syntax validation keeps chrono's range; instants stop at jiff's.
    let end = "9999-12-31T23:59:59.999999999Z";
    assert!(is_valid(end) && DateTime::parse_from_rfc3339(end).is_ok());
    assert!(parse(end).is_none());
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
