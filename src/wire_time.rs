//! RFC 3339 timestamps exchanged with the Server, parsed and printed with jiff.
//!
//! The SDK replaced chrono for wire time (#283, RUSTSEC-2026-0543). [`parse`]
//! accepts exactly the text chrono 0.4's `DateTime::parse_from_rfc3339` did:
//! a four-digit year, `T`, `t` or a space between date and time, 1 to 9
//! significant fractional digits (further digits are ignored), a `Z`/`z` or
//! `±HH:MM` offset below 24 hours (U+2212 is accepted as a minus sign), and
//! second 60 as a leap second. [`format`] prints what chrono's
//! `to_rfc3339_opts(.., true)` printed, so canonical timestamps the SDK sends
//! or checks keep identical bytes.
//!
//! Two representational limits differ from chrono. A leap second `:60.f` is
//! the instant `f` after the next whole second, as the PHP Server reads it;
//! chrono kept a distinct `:60` value and printed it back. An instant must
//! also lie within jiff's range, which ends at `9999-12-30T22:00:00Z`. Syntax
//! validation through [`is_valid`] keeps the full chrono range.
//!
//! chrono's accepted text and printed bytes are pinned by
//! `tests/fixtures/wire-time-chrono-reference.json`, recorded from chrono
//! 0.4.45 before it was removed. Public accessors return [`jiff::Timestamp`].

use jiff::{civil, tz::Offset, SignedDuration};
use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) use jiff::Timestamp;

/// One parsed RFC 3339 value: its instant and the offset it was written with.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Rfc3339 {
    pub(crate) timestamp: Timestamp,
    pub(crate) offset_seconds: i32,
}

#[derive(Clone, Copy, Debug)]
struct Fields {
    date: civil::Date,
    hour: i8,
    minute: i8,
    second: i8,
    nanosecond: i32,
    leap: bool,
    offset_seconds: i32,
}

fn two(bytes: &[u8], at: usize) -> Option<i32> {
    match (bytes.get(at)?, bytes.get(at + 1)?) {
        (high @ b'0'..=b'9', low @ b'0'..=b'9') => {
            Some(i32::from(high - b'0') * 10 + i32::from(low - b'0'))
        }
        _ => None,
    }
}

fn offset(rest: &[u8]) -> Option<i32> {
    let (negative, rest) = match rest {
        [b'Z' | b'z'] => return Some(0),
        [b'+', rest @ ..] => (false, rest),
        [b'-', rest @ ..] => (true, rest),
        // U+2212 MINUS SIGN, which chrono accepts for RFC 3339 offsets.
        [0xE2, 0x88, 0x92, rest @ ..] => (true, rest),
        _ => return None,
    };
    if rest.len() != 5 || rest[2] != b':' {
        return None;
    }
    let hours = two(rest, 0)?;
    let minutes = two(rest, 3)?;
    if minutes > 59 || hours > 23 {
        return None;
    }
    let seconds = hours * 3600 + minutes * 60;
    Some(if negative { -seconds } else { seconds })
}

fn fields(text: &str) -> Option<Fields> {
    let bytes = text.as_bytes();
    if bytes.len() < 19
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || !matches!(bytes[10], b'T' | b't' | b' ')
        || bytes[13] != b':'
        || bytes[16] != b':'
    {
        return None;
    }
    let year = two(bytes, 0)? * 100 + two(bytes, 2)?;
    let date = civil::Date::new(
        i16::try_from(year).ok()?,
        i8::try_from(two(bytes, 5)?).ok()?,
        i8::try_from(two(bytes, 8)?).ok()?,
    )
    .ok()?;
    let (hour, minute, second) = (two(bytes, 11)?, two(bytes, 14)?, two(bytes, 17)?);
    if hour > 23 || minute > 59 || second > 60 {
        return None;
    }
    let mut rest = &bytes[19..];
    let mut nanosecond = 0;
    if let [b'.', fraction @ ..] = rest {
        let digits = fraction
            .iter()
            .take_while(|byte| byte.is_ascii_digit())
            .count();
        if digits == 0 {
            return None;
        }
        // Digits beyond nanoseconds are ignored, as chrono ignored them.
        for (index, digit) in fraction.iter().take(9).take(digits).enumerate() {
            nanosecond += i32::from(digit - b'0') * 10_i32.pow(8 - index as u32);
        }
        rest = &fraction[digits..];
    }
    Some(Fields {
        date,
        hour: hour as i8,
        minute: minute as i8,
        second: second.min(59) as i8,
        nanosecond,
        leap: second == 60,
        offset_seconds: offset(rest)?,
    })
}

/// Whether `text` is an RFC 3339 timestamp chrono 0.4 would have accepted.
pub(crate) fn is_valid(text: &str) -> bool {
    fields(text).is_some()
}

/// Parse an RFC 3339 timestamp, keeping the offset it was written with.
pub(crate) fn parse(text: &str) -> Option<Rfc3339> {
    let fields = fields(text)?;
    let local = fields.date.to_datetime(
        civil::Time::new(fields.hour, fields.minute, fields.second, fields.nanosecond).ok()?,
    );
    let mut timestamp = Offset::from_seconds(fields.offset_seconds)
        .ok()?
        .to_timestamp(local)
        .ok()?;
    if fields.leap {
        timestamp = timestamp.checked_add(SignedDuration::from_secs(1)).ok()?;
    }
    Some(Rfc3339 {
        timestamp,
        offset_seconds: fields.offset_seconds,
    })
}

/// Parse an RFC 3339 timestamp as an instant, discarding its offset.
pub(crate) fn parse_instant(text: &str) -> Option<Timestamp> {
    parse(text).map(|parsed| parsed.timestamp)
}

/// Print `timestamp` at `offset_seconds` with exactly `digits` (0 to 9)
/// truncated fractional digits, and `Z` for a zero offset.
pub(crate) fn format(timestamp: Timestamp, offset_seconds: i32, digits: usize) -> String {
    write(timestamp, offset_seconds, digits, true)
}

/// chrono's `DateTime<Utc>::to_rfc3339()`: 0, 3, 6 or 9 digits and `+00:00`.
pub(crate) fn format_utc_auto_si(timestamp: Timestamp) -> String {
    let nanosecond = Offset::UTC.to_datetime(timestamp).subsec_nanosecond();
    let digits = match nanosecond {
        0 => 0,
        _ if nanosecond % 1_000_000 == 0 => 3,
        _ if nanosecond % 1_000 == 0 => 6,
        _ => 9,
    };
    write(timestamp, 0, digits, false)
}

fn write(timestamp: Timestamp, offset_seconds: i32, digits: usize, zulu: bool) -> String {
    use std::fmt::Write as _;

    let digits = digits.min(9);
    let offset = Offset::from_seconds(offset_seconds).unwrap_or(Offset::UTC);
    let local = offset.to_datetime(timestamp);
    let mut text = String::with_capacity(35);
    let year = local.year();
    if (0..=9999).contains(&year) {
        let _ = write!(text, "{year:04}");
    } else {
        let _ = write!(text, "{year:+05}");
    }
    let _ = write!(
        text,
        "-{:02}-{:02}T{:02}:{:02}:{:02}",
        local.month(),
        local.day(),
        local.hour(),
        local.minute(),
        local.second()
    );
    if digits > 0 {
        let fraction = local.subsec_nanosecond() / 10_i32.pow(9 - digits as u32);
        let _ = write!(text, ".{fraction:0digits$}");
    }
    let seconds = offset.seconds();
    if seconds == 0 && zulu {
        text.push('Z');
    } else {
        let sign = if seconds < 0 { '-' } else { '+' };
        let minutes = seconds.unsigned_abs() / 60;
        let _ = write!(text, "{sign}{:02}:{:02}", minutes / 60, minutes % 60);
    }
    text
}

/// The SDK's canonical UTC wire form: six fractional digits and `Z`.
pub(crate) fn format_micros(timestamp: Timestamp) -> String {
    format(timestamp, 0, 6)
}

/// The current wall clock, or `None` before the Unix epoch.
pub(crate) fn now() -> Option<Timestamp> {
    let elapsed = SystemTime::now().duration_since(UNIX_EPOCH).ok()?;
    Timestamp::new(
        i64::try_from(elapsed.as_secs()).ok()?,
        i32::try_from(elapsed.subsec_nanos()).ok()?,
    )
    .ok()
}

/// The non-negative std duration from `earlier` to `later`, if any.
pub(crate) fn until(later: Timestamp, earlier: Timestamp) -> Option<std::time::Duration> {
    std::time::Duration::try_from(later.duration_since(earlier)).ok()
}

/// Test shorthand for the canonical micros wire form.
#[cfg(test)]
pub(crate) trait Rfc3339Micros {
    fn to_rfc3339_micros(&self) -> String;
}

#[cfg(test)]
impl Rfc3339Micros for Timestamp {
    fn to_rfc3339_micros(&self) -> String {
        format_micros(*self)
    }
}

#[cfg(test)]
mod tests;
