//! The Server's `Retry-After` advice for worker retries.
//!
//! A 429 or 5xx answer may carry `Retry-After` as delta-seconds or an
//! HTTP-date. A worker that retries after such an answer waits at least that
//! long, capped at [`CAP`], and otherwise keeps its own jittered backoff. The
//! advice is shared by a Client's clones, so every lane of a worker backs off
//! together, and any successful answer clears it.

use std::sync::Mutex;
use std::time::{Duration, SystemTime};

use chrono::{DateTime, Utc};

/// The longest `Retry-After` a worker honors.
pub(crate) const CAP: Duration = Duration::from_secs(30);

/// Parse a `Retry-After` value at `now`: delta-seconds or an HTTP-date. A
/// date in the past is no wait; anything unparseable is no advice.
pub(crate) fn parse(value: &str, now: SystemTime) -> Option<Duration> {
    let value = value.trim();
    if !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()) {
        // An overlong number is still a valid, very long delay.
        return Some(value.parse().map_or(CAP, Duration::from_secs).min(CAP));
    }
    let at = DateTime::parse_from_rfc2822(value)
        .ok()?
        .with_timezone(&Utc);
    let now = now.duration_since(SystemTime::UNIX_EPOCH).ok()?;
    let now =
        DateTime::<Utc>::from_timestamp(i64::try_from(now.as_secs()).ok()?, now.subsec_nanos())?;
    Some((at - now).to_std().unwrap_or(Duration::ZERO).min(CAP))
}

/// The newest `Retry-After` deadline the Server advised.
#[derive(Debug, Default)]
pub(crate) struct Advice(Mutex<Option<tokio::time::Instant>>);

impl Advice {
    /// Record one answer: a success clears the advice; a 429 or 5xx with a
    /// `Retry-After` extends it; any other answer leaves it as it was.
    pub(crate) fn observe(
        &self,
        status: reqwest::StatusCode,
        retry_after: Option<&reqwest::header::HeaderValue>,
    ) {
        let Ok(mut deadline) = self.0.lock() else {
            return;
        };
        if status.is_success() {
            *deadline = None;
            return;
        }
        if !(status.is_server_error() || status == reqwest::StatusCode::TOO_MANY_REQUESTS) {
            return;
        }
        let Some(delay) = retry_after
            .and_then(|value| value.to_str().ok())
            .and_then(|value| parse(value, SystemTime::now()))
        else {
            return;
        };
        let advised = tokio::time::Instant::now() + delay;
        *deadline = Some(deadline.map_or(advised, |current| current.max(advised)));
    }

    /// How long the Server still asks retries to wait.
    pub(crate) fn remaining(&self) -> Duration {
        self.0
            .lock()
            .ok()
            .and_then(|deadline| *deadline)
            .map_or(Duration::ZERO, |deadline| {
                deadline.saturating_duration_since(tokio::time::Instant::now())
            })
            .min(CAP)
    }

    /// `delay`, lengthened to the Server's remaining advice.
    pub(crate) fn honor(&self, delay: Duration) -> Duration {
        delay.max(self.remaining())
    }
}

#[cfg(test)]
thread_local! {
    /// Set by a test to record every worker retry wait on this thread (a
    /// `#[tokio::test]` runtime runs all its tasks on one thread). Recorded
    /// waits last a millisecond, so a test observes the schedule quickly.
    pub(crate) static RECORDED: std::cell::RefCell<Option<Vec<Duration>>> =
        const { std::cell::RefCell::new(None) };
}

/// Wait `delay` before a worker retry.
pub(crate) async fn sleep(delay: Duration) {
    #[cfg(test)]
    if RECORDED.with(|recorded| {
        recorded
            .borrow_mut()
            .as_mut()
            .map(|recorded| recorded.push(delay))
            .is_some()
    }) {
        tokio::time::sleep(Duration::from_millis(1)).await;
        return;
    }
    tokio::time::sleep(delay).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delta_seconds_and_http_dates_are_parsed_and_capped() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_445_412_480);
        // 2015-10-21T07:28:00Z
        assert_eq!(parse("3", now), Some(Duration::from_secs(3)));
        assert_eq!(parse(" 0 ", now), Some(Duration::ZERO));
        assert_eq!(parse("120", now), Some(CAP));
        assert_eq!(parse("99999999999999999999999", now), Some(CAP));
        assert_eq!(
            parse("Wed, 21 Oct 2015 07:28:05 GMT", now),
            Some(Duration::from_secs(5))
        );
        assert_eq!(
            parse("Wed, 21 Oct 2015 08:28:00 GMT", now),
            Some(CAP),
            "a far date is capped"
        );
        assert_eq!(
            parse("Wed, 21 Oct 2015 07:27:00 GMT", now),
            Some(Duration::ZERO),
            "a past date is no wait"
        );
        for invalid in ["", "-1", "1.5", "soon", "Wed, 32 Oct 2015 07:28:00 GMT"] {
            assert_eq!(parse(invalid, now), None, "{invalid:?}");
        }
    }

    #[tokio::test]
    async fn advice_is_extended_by_failures_and_cleared_by_success() {
        let advice = Advice::default();
        let four = reqwest::header::HeaderValue::from_static("4");
        let four = Some(&four);
        assert_eq!(
            advice.honor(Duration::from_millis(5)),
            Duration::from_millis(5)
        );
        // A definitive refusal is not retry advice.
        advice.observe(reqwest::StatusCode::CONFLICT, four);
        assert_eq!(advice.remaining(), Duration::ZERO);
        advice.observe(reqwest::StatusCode::SERVICE_UNAVAILABLE, four);
        let honored = advice.honor(Duration::from_millis(5));
        assert!(honored > Duration::from_secs(3) && honored <= Duration::from_secs(4));
        // A failure without advice keeps it; a longer wait is not shortened.
        advice.observe(reqwest::StatusCode::INTERNAL_SERVER_ERROR, None);
        assert!(advice.remaining() > Duration::from_secs(3));
        assert_eq!(
            advice.honor(Duration::from_secs(10)),
            Duration::from_secs(10)
        );
        advice.observe(reqwest::StatusCode::OK, None);
        assert_eq!(advice.remaining(), Duration::ZERO);
    }
}
