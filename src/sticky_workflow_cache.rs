//! Bounded durable wire history. Replay always gets a fresh decoded snapshot.

use serde_json::Value;
use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CacheKey {
    pub workflow_id: String,
    pub run_id: String,
    pub build_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ResumeCursor {
    pub token: String,
    pub offset: usize,
}

/// Replay counters and retained encoded history. Decoding/replay memory is additional.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct StickyCacheMetrics {
    pub hit: u64,
    pub miss: u64,
    pub eviction: u64,
    pub forced_cold_replay: u64,
    pub entries: usize,
    pub history_bytes: usize,
}

#[derive(Debug)]
struct Entry {
    key: CacheKey,
    encoded: Box<[u8]>,
    expires_at: Instant,
    resume: Option<ResumeCursor>,
}

#[derive(Debug)]
pub(crate) struct StickyWorkflowCache {
    capacity: usize,
    max_bytes: usize,
    ttl: Duration,
    entries: VecDeque<Entry>,
    history_bytes: usize,
    metrics: StickyCacheMetrics,
}

/// Opt-in bounds for a worker's durable-history cache.
#[derive(Clone, Debug)]
pub struct StickyCacheOptions {
    pub(crate) capacity: usize,
    pub(crate) max_bytes: usize,
    pub(crate) ttl: Duration,
}

impl StickyCacheOptions {
    /// Bound the retained run count. Zero disables caching. Defaults to 16 MiB and 300 seconds.
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            max_bytes: 16 * 1024 * 1024,
            ttl: Duration::from_secs(300),
        }
    }
    pub fn max_history_bytes(mut self, bytes: usize) -> Self {
        self.max_bytes = bytes;
        self
    }
    /// Whole seconds, from 1 to 3600. Reads do not extend the original expiry.
    pub fn ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }
}

pub(crate) fn complete_history(history: &[Value]) -> bool {
    let Some(first) = history.first() else {
        return false;
    };
    let starts_workflow = first["event_type"] == "WorkflowStarted"
        || (first["event_type"] == "StartAccepted"
            && history
                .get(1)
                .is_some_and(|event| event["event_type"] == "WorkflowStarted"));
    starts_workflow
        && history.iter().enumerate().all(|(index, event)| {
            event.is_object() && event["sequence"].as_u64() == u64::try_from(index + 1).ok()
        })
}

impl StickyWorkflowCache {
    pub fn new(capacity: usize, max_bytes: usize, ttl: Duration) -> Result<Self, &'static str> {
        if max_bytes == 0 {
            return Err("sticky cache byte limit must be positive");
        }
        if ttl < Duration::from_secs(1) || ttl > Duration::from_secs(3600) {
            return Err("sticky cache TTL must be between 1 and 3600 seconds");
        }
        Ok(Self {
            capacity,
            max_bytes,
            ttl,
            entries: VecDeque::new(),
            history_bytes: 0,
            metrics: StickyCacheMetrics::default(),
        })
    }

    pub fn enabled(&self) -> bool {
        self.capacity > 0
    }

    pub fn empty(&self) -> Self {
        Self::new(self.capacity, self.max_bytes, self.ttl).expect("validated cache options")
    }
    pub fn ttl_seconds(&self) -> u64 {
        self.ttl.as_secs()
    }

    fn expire(&mut self, now: Instant) {
        self.entries.retain(|entry| entry.expires_at > now);
        self.history_bytes = self.entries.iter().map(|entry| entry.encoded.len()).sum();
    }

    pub fn discard(&mut self, key: &CacheKey) {
        if let Some(index) = self.entries.iter().position(|entry| &entry.key == key) {
            let entry = self.entries.remove(index).expect("known cache entry");
            self.history_bytes -= entry.encoded.len();
        }
    }

    pub fn lookup(
        &mut self,
        key: &CacheKey,
        now: Instant,
    ) -> Option<(Vec<Value>, Option<ResumeCursor>)> {
        self.expire(now);
        let index = self.entries.iter().position(|entry| &entry.key == key)?;
        let entry = self.entries.remove(index)?;
        let history =
            serde_json::from_slice(&entry.encoded).expect("cache encodes its own history");
        let resume = entry.resume.clone();
        self.entries.push_back(entry);
        Some((history, resume))
    }

    pub fn remember(
        &mut self,
        key: CacheKey,
        history: &[Value],
        resume: Option<ResumeCursor>,
        now: Instant,
    ) -> bool {
        self.expire(now);
        self.discard(&key);
        if !self.enabled()
            || key.workflow_id.is_empty()
            || key.run_id.is_empty()
            || key.build_id.is_empty()
            || !complete_history(history)
            || resume
                .as_ref()
                .is_some_and(|cursor| cursor.token.is_empty() || cursor.offset > history.len())
        {
            return false;
        }
        let Ok(encoded) = serde_json::to_vec(history) else {
            return false;
        };
        if encoded.len() > self.max_bytes {
            return false;
        }
        while self.entries.len() >= self.capacity
            || self.history_bytes > self.max_bytes - encoded.len()
        {
            let Some(oldest) = self.entries.pop_front() else {
                return false;
            };
            self.history_bytes -= oldest.encoded.len();
            self.metrics.eviction = self.metrics.eviction.saturating_add(1);
        }
        self.history_bytes += encoded.len();
        self.entries.push_back(Entry {
            key,
            encoded: encoded.into_boxed_slice(),
            expires_at: now + self.ttl,
            resume,
        });
        true
    }

    pub fn record_replay(&mut self, hit: bool, forced: bool) {
        if hit {
            self.metrics.hit = self.metrics.hit.saturating_add(1);
        } else {
            self.metrics.miss = self.metrics.miss.saturating_add(1);
        }
        if forced {
            self.metrics.forced_cold_replay = self.metrics.forced_cold_replay.saturating_add(1);
        }
    }

    pub fn metrics(&mut self, now: Instant) -> StickyCacheMetrics {
        self.expire(now);
        StickyCacheMetrics {
            entries: self.entries.len(),
            history_bytes: self.history_bytes,
            ..self.metrics
        }
    }

    pub fn clear(&mut self) {
        self.entries.clear();
        self.history_bytes = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn key(run: &str) -> CacheKey {
        CacheKey {
            workflow_id: "workflow".into(),
            run_id: run.into(),
            build_id: "build".into(),
        }
    }
    fn history() -> Vec<Value> {
        vec![
            json!({"event_type":"StartAccepted", "sequence":1}),
            json!({"event_type":"WorkflowStarted", "sequence":2, "payload":{"value":[1,2]}}),
        ]
    }
    fn cache(capacity: usize, max_bytes: usize) -> StickyWorkflowCache {
        StickyWorkflowCache::new(capacity, max_bytes, Duration::from_secs(1)).unwrap()
    }

    #[test]
    fn canonical_start_and_contiguous_sequences_are_required() {
        assert!(complete_history(&history()));
        assert!(complete_history(&[
            json!({"event_type":"WorkflowStarted","sequence":1})
        ]));
        assert!(!complete_history(&[]));
        assert!(!complete_history(&history()[..1]));
        for bad in [json!(true), json!(-2), json!(2.0), json!(3), Value::Null] {
            let mut events = history();
            events[1]["sequence"] = bad;
            assert!(!complete_history(&events));
        }
        let mut events = history();
        events[1]["event_type"] = json!("ActivityCompleted");
        assert!(!complete_history(&events));
        assert!(!complete_history(&[json!(false)]));
    }

    #[test]
    fn replay_mutations_cannot_change_retained_history_or_cursor() {
        let now = Instant::now();
        let mut cache = cache(2, 10_000);
        let cursor = Some(ResumeCursor {
            token: "opaque/lease-token".into(),
            offset: 1,
        });
        assert!(cache.remember(key("run"), &history(), cursor.clone(), now));
        let (mut replay, mut replay_cursor) = cache.lookup(&key("run"), now).unwrap();
        replay[1]["payload"]["value"][0] = json!(999);
        replay_cursor.as_mut().unwrap().token.clear();
        assert_eq!(cache.lookup(&key("run"), now), Some((history(), cursor)));
    }

    #[test]
    fn lru_lookup_changes_entry_eviction_order() {
        let now = Instant::now();
        let mut cache = cache(2, 10_000);
        for run in ["a", "b"] {
            assert!(cache.remember(key(run), &history(), None, now));
        }
        assert!(cache.lookup(&key("a"), now).is_some());
        assert!(cache.remember(key("c"), &history(), None, now));
        assert!(cache.lookup(&key("b"), now).is_none());
        assert!(cache.lookup(&key("a"), now).is_some());
        assert!(cache.lookup(&key("c"), now).is_some());
        assert_eq!(cache.metrics(now).eviction, 1);
    }

    #[test]
    fn encoded_byte_budget_evicts_even_below_entry_capacity() {
        let now = Instant::now();
        let bytes = serde_json::to_vec(&history()).unwrap().len();
        let mut cache = cache(10, bytes);
        assert!(cache.remember(key("a"), &history(), None, now));
        assert!(cache.remember(key("b"), &history(), None, now));
        assert!(cache.lookup(&key("a"), now).is_none());
        assert_eq!(cache.metrics(now).history_bytes, bytes);
        assert_eq!(cache.metrics(now).entries, 1);
        assert_eq!(cache.metrics(now).eviction, 1);
    }

    #[test]
    fn oversized_or_invalid_replacement_discards_only_its_own_old_entry() {
        let now = Instant::now();
        let mut cache = cache(2, 1000);
        for run in ["a", "b"] {
            assert!(cache.remember(key(run), &history(), None, now));
        }
        let mut oversized = history();
        oversized[1]["payload"] = json!("x".repeat(1000));
        assert!(!cache.remember(key("a"), &oversized, None, now));
        assert!(cache.lookup(&key("a"), now).is_none());
        assert!(cache.lookup(&key("b"), now).is_some());
        assert!(!cache.remember(key("b"), &[], None, now));
        assert_eq!(cache.metrics(now).history_bytes, 0);
        assert_eq!(cache.metrics(now).eviction, 0);
    }

    #[test]
    fn expiry_uses_original_admission_time_and_releases_bytes() {
        let now = Instant::now();
        let mut cache = cache(1, 10_000);
        assert!(cache.remember(key("a"), &history(), None, now));
        assert!(cache
            .lookup(&key("a"), now + Duration::from_millis(999))
            .is_some());
        assert!(cache
            .lookup(&key("a"), now + Duration::from_secs(1))
            .is_none());
        assert_eq!(cache.metrics(now + Duration::from_secs(1)).history_bytes, 0);
        assert_eq!(cache.metrics(now + Duration::from_secs(1)).eviction, 0);
    }

    #[test]
    fn workflow_run_and_build_identity_are_all_part_of_the_key() {
        let now = Instant::now();
        let mut cache = cache(1, 10_000);
        assert!(cache.remember(key("a"), &history(), None, now));
        for different in [
            CacheKey {
                workflow_id: "other".into(),
                ..key("a")
            },
            key("other"),
            CacheKey {
                build_id: "other".into(),
                ..key("a")
            },
        ] {
            assert!(cache.lookup(&different, now).is_none());
        }
        assert!(cache.lookup(&key("a"), now).is_some());
    }

    #[test]
    fn disabled_profile_never_retains_or_claims_history() {
        let now = Instant::now();
        let mut cache = cache(0, 10_000);
        assert!(!cache.enabled());
        assert!(!cache.remember(key("a"), &history(), None, now));
        assert!(cache.lookup(&key("a"), now).is_none());
        assert_eq!(cache.metrics(now).entries, 0);
    }

    #[test]
    fn invalid_limits_identity_and_cursors_are_refused() {
        assert!(StickyWorkflowCache::new(1, 0, Duration::from_secs(1)).is_err());
        assert!(StickyWorkflowCache::new(1, 1, Duration::from_millis(999)).is_err());
        assert!(StickyWorkflowCache::new(1, 1, Duration::from_secs(3601)).is_err());
        let now = Instant::now();
        let mut cache = cache(1, 10_000);
        for invalid in [
            CacheKey {
                workflow_id: "".into(),
                ..key("a")
            },
            CacheKey {
                run_id: "".into(),
                ..key("a")
            },
            CacheKey {
                build_id: "".into(),
                ..key("a")
            },
        ] {
            assert!(!cache.remember(invalid, &history(), None, now));
        }
        for cursor in [
            ResumeCursor {
                token: "".into(),
                offset: 0,
            },
            ResumeCursor {
                token: "opaque".into(),
                offset: 3,
            },
        ] {
            assert!(!cache.remember(key("a"), &history(), Some(cursor), now));
        }
    }

    #[test]
    fn terminal_discard_and_shutdown_clear_preserve_replay_counters() {
        let now = Instant::now();
        let mut cache = cache(2, 10_000);
        for run in ["a", "b"] {
            assert!(cache.remember(key(run), &history(), None, now));
        }
        cache.record_replay(true, false);
        cache.record_replay(false, true);
        cache.discard(&key("a"));
        cache.discard(&key("missing"));
        assert_eq!(cache.metrics(now).entries, 1);
        cache.clear();
        assert_eq!(
            cache.metrics(now),
            StickyCacheMetrics {
                hit: 1,
                miss: 1,
                forced_cold_replay: 1,
                ..StickyCacheMetrics::default()
            }
        );
    }
}
