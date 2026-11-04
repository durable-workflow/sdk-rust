//! Opt-in one-activity worker benchmark. Run only against a disposable Server.
//! Use the same source, release profile, Server and driver for compared SDKs.

use durable_workflow::{
    json,
    metrics::{MetricTags, MetricsRecorder, CLIENT_REQUEST_DURATION_SECONDS},
    Client, Error, Uuid, Value, Worker, WorkflowResultOptions,
};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

#[derive(Default)]
struct Requests(Mutex<BTreeMap<String, Vec<f64>>>);

impl MetricsRecorder for Requests {
    fn increment(&self, _: &str, _: u64, _: &MetricTags) {}
    fn record(&self, name: &str, seconds: f64, tags: &MetricTags) {
        if name == CLIENT_REQUEST_DURATION_SECONDS {
            let route = tags.get("route").cloned().unwrap_or_default();
            self.0
                .lock()
                .unwrap()
                .entry(route)
                .or_default()
                .push(seconds * 1000.0);
        }
    }
}

fn distribution(mut values: Vec<f64>) -> Value {
    values.sort_by(f64::total_cmp);
    if values.is_empty() {
        return Value::Null;
    }
    let percentile =
        |percent: usize| values[(values.len() * percent).div_ceil(100).saturating_sub(1)];
    json!({"count":values.len(), "p50_ms":percentile(50), "p95_ms":percentile(95)})
}

impl Requests {
    fn summary(&self) -> Value {
        self.0
            .lock()
            .unwrap()
            .iter()
            .map(|(route, values)| (route.clone(), distribution(values.clone())))
            .collect::<serde_json::Map<String, Value>>()
            .into()
    }
}

#[tokio::main(worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mode = std::env::args().nth(1).ok_or("expected worker or drive")?;
    let url = std::env::var("DURABLE_WORKFLOW_SERVER_URL")?;
    let queue = std::env::var("TASK_QUEUE")?;
    let requests = Arc::new(Requests::default());
    let client = Client::builder(url)
        .token(std::env::var("DURABLE_WORKFLOW_TOKEN").ok())
        .metrics_recorder(requests.clone())
        .build()?;
    match mode.as_str() {
        "worker" => {
            let capacity: usize = std::env::var("SDK_BENCH_CAPACITY")
                .unwrap_or_else(|_| "16".into())
                .parse()?;
            if !(1..=16).contains(&capacity) {
                return Err("benchmark capacity must be 1..16".into());
            }
            let mut worker = Worker::new(client, queue)
                .worker_id(format!("latency-{}", Uuid::new_v4()))
                .max_concurrent_workflow_tasks(capacity)
                .max_concurrent_activity_tasks(capacity);
            worker.register_typed_activity("bench.echo", |_ctx, payload: String| async move {
                Ok::<String, Error>(payload)
            });
            worker.register_typed_workflow(
                "bench.one-activity",
                |ctx, payload: String| async move {
                    ctx.activity_typed::<_, String>("bench.echo", payload).await
                },
            );
            // An abandoned benchmark must stop its worker without its driver.
            worker
                .run_until(async {
                    tokio::select! {
                        _ = tokio::signal::ctrl_c() => {},
                        _ = tokio::time::sleep(Duration::from_secs(180)) => {},
                    }
                })
                .await?;
            println!(
                "{}",
                json!({"mode":"worker", "capacity":capacity, "requests":requests.summary()})
            );
        }
        "drive" => {
            let mut samples = Vec::new();
            let mut workflows = Vec::new();
            let payload = "x".repeat(1024);
            for index in 0..25 {
                let id = format!("latency-{}", Uuid::new_v4());
                let started = Instant::now();
                let handle = client
                    .start_workflow("bench.one-activity", &queue, &id, vec![payload.clone()])
                    .await?;
                let result: String = handle
                    .result_typed(WorkflowResultOptions {
                        poll_interval: Duration::from_millis(5),
                        timeout: Duration::from_secs(10),
                    })
                    .await?;
                if result != payload {
                    return Err("echo result mismatch".into());
                }
                if index >= 5 {
                    samples.push(started.elapsed().as_secs_f64() * 1000.0);
                    workflows.push(id);
                }
            }
            println!(
                "{}",
                json!({"mode":"drive", "warmups":5, "latency":distribution(samples.clone()),
                "samples_ms":samples, "workflows":workflows, "requests":requests.summary()})
            );
        }
        _ => return Err("expected worker or drive".into()),
    }
    Ok(())
}
