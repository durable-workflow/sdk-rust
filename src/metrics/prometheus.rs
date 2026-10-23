use super::{
    MetricTags, MetricsRecorder, CLIENT_REQUESTS, CLIENT_REQUEST_DURATION_SECONDS, WORKER_POLLS,
    WORKER_POLL_DURATION_SECONDS, WORKER_TASKS, WORKER_TASK_DURATION_SECONDS,
};
use ::prometheus::{
    Encoder, HistogramOpts, HistogramVec, IntCounterVec, Opts, Registry, TextEncoder,
};
use std::collections::BTreeMap;

/// Prometheus adapter backed by the official `prometheus` crate.
///
/// Each instance owns a dedicated registry, with no global registration or
/// listener. Serve [`Self::encode`] from an application-owned metrics endpoint.
/// Counters use the exporter's `_total` suffix; recorder names stay unchanged.
#[derive(Debug)]
pub struct PrometheusMetrics {
    registry: Registry,
    counters: BTreeMap<&'static str, IntCounterVec>,
    histograms: BTreeMap<&'static str, HistogramVec>,
}

const CLIENT_LABELS: &[&str] = &["method", "route", "plane", "status_code", "outcome"];
const WORKER_LABELS: &[&str] = &["task_kind", "task_queue", "outcome"];

impl PrometheusMetrics {
    pub fn new() -> ::prometheus::Result<Self> {
        let registry = Registry::new();
        let mut counters = BTreeMap::new();
        let mut histograms = BTreeMap::new();
        for (counter, histogram, labels) in [
            (
                CLIENT_REQUESTS,
                CLIENT_REQUEST_DURATION_SECONDS,
                CLIENT_LABELS,
            ),
            (WORKER_POLLS, WORKER_POLL_DURATION_SECONDS, WORKER_LABELS),
            (WORKER_TASKS, WORKER_TASK_DURATION_SECONDS, WORKER_LABELS),
        ] {
            let metric =
                IntCounterVec::new(Opts::new(format!("{counter}_total"), counter), labels)?;
            registry.register(Box::new(metric.clone()))?;
            counters.insert(counter, metric);
            let metric = HistogramVec::new(HistogramOpts::new(histogram, histogram), labels)?;
            registry.register(Box::new(metric.clone()))?;
            histograms.insert(histogram, metric);
        }
        Ok(Self {
            registry,
            counters,
            histograms,
        })
    }

    /// Application-owned registry; use it to gather or add application metrics.
    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    /// Prometheus text exposition, suitable for `text/plain; version=0.0.4`.
    pub fn encode(&self) -> ::prometheus::Result<Vec<u8>> {
        let mut buffer = Vec::new();
        TextEncoder::new().encode(&self.registry.gather(), &mut buffer)?;
        Ok(buffer)
    }
}

fn values<'a>(name: &str, tags: &'a MetricTags) -> Option<Vec<&'a str>> {
    let labels = if matches!(name, CLIENT_REQUESTS | CLIENT_REQUEST_DURATION_SECONDS) {
        CLIENT_LABELS
    } else {
        WORKER_LABELS
    };
    if tags.len() != labels.len() {
        return None;
    }
    labels
        .iter()
        .map(|key| tags.get(*key).map(String::as_str))
        .collect()
}

impl MetricsRecorder for PrometheusMetrics {
    fn increment(&self, name: &str, value: u64, tags: &MetricTags) {
        if let (Some(metric), Some(values)) = (self.counters.get(name), values(name, tags)) {
            if let Ok(metric) = metric.get_metric_with_label_values(&values) {
                metric.inc_by(value);
            }
        }
    }
    fn record(&self, name: &str, value: f64, tags: &MetricTags) {
        if !value.is_finite() || value < 0.0 {
            return;
        }
        if let (Some(metric), Some(values)) = (self.histograms.get(name), values(name, tags)) {
            if let Ok(metric) = metric.get_metric_with_label_values(&values) {
                metric.observe(value);
            }
        }
    }
}
