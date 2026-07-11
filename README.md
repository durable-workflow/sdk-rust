# Durable Workflow Rust SDK

`durable-workflow` is the first-party Rust SDK for Durable Workflow workers
and clients. It can register workflow and activity handlers, long-poll the
worker protocol, start, signal, and query workflow executions, expose named
read-only query handlers, heartbeat workers and activities, and exchange
JSON-native payloads through the platform's generic Avro wrapper.

## Install

Add the exact crates.io release with Cargo:

```sh
cargo add durable-workflow@0.1.3 --exact
```

Or add the same exact requirement directly to `Cargo.toml`:

```toml
[dependencies]
durable-workflow = "=0.1.3"
```

Version `0.1.3` requires Rust `1.86` or newer. Snapshot inspection queries were
introduced in `0.1.1`; replayed workflow-instance state queries are available
from `0.1.2`.

## Compatibility

| SDK releases | Durable Workflow server | Worker protocol | Control plane |
| --- | --- | --- | --- |
| `0.1.0` | `>=0.2,<0.3` | `1.2` | `2` |
| `0.1.1` | `>=0.2,<0.3` | `1.2` (snapshot queries require `1.8`) | `2` |
| `0.1.2+` | `>=0.2,<0.3` | `1.2` (replayed queries require `1.8`) | `2` |

The machine-readable values live in `[package.metadata.durable-workflow]` in
`Cargo.toml` as `supported-server-versions`, `worker-protocol-version`, and
`control-plane-version`. Query-capable releases also publish `query-tasks`,
`query-task-minimum-worker-protocol-version`, `replayed-instance-state-queries`,
`query-state-model`, `snapshot-inspection-queries`, and `payload-codecs`. Existing
worker operations retain the `1.2` baseline; only query-task poll, complete,
and fail requests use the additive `1.8` feature floor. The server's advertised
protocol manifests remain authoritative when checking compatibility during
deployment.

## Worker

```rust
use durable_workflow::{json, Client, Result, Worker};

#[derive(Clone, Default)]
struct HelloState {
    started_by: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let client = Client::builder("http://127.0.0.1:8080")
        .token(std::env::var("DURABLE_WORKFLOW_TOKEN").ok())
        .namespace("default")
        .build()?;

    let mut worker = Worker::new(client.clone(), "rust-workers");

    worker.register_activity("hello.activity", |ctx, args| async move {
        ctx.heartbeat(json!({"stage": "started"})).await?;
        let name = args.first().and_then(|value| value.as_str()).unwrap_or("world");
        Ok(json!(format!("hello, {name}")))
    });

    worker.register_replayed_workflow("hello.workflow", HelloState::default, |ctx, _input, state| async move {
        let signal = ctx.wait_signal("start").await?;
        let name = signal.first().and_then(|value| value.as_str()).unwrap_or("world");
        state.update(|current| current.started_by = Some(name.to_string()))?;
        let greeting = ctx.activity("hello.activity", json!([name])).await?;
        Ok(json!({"greeting": greeting}))
    });

    worker.register_replayed_query::<HelloState, _, _>("hello.workflow", "started-by", |_ctx, state, _args| async move {
        Ok(json!(state.started_by))
    });

    worker.run().await
}
```

## Client

```rust
# use durable_workflow::{json, Client, Result};
# async fn example(client: Client) -> Result<()> {
let handle = client
    .start_workflow("hello.workflow", "rust-workers", "hello-rust-1", json!([]))
    .await?;

client
    .signal_workflow(&handle.workflow_id, "start", json!(["Rust"]))
    .await?;

let started_by = handle.query("started-by", json!([])).await?;
assert_eq!(started_by, json!("Rust"));

let output = handle.result(Default::default()).await?;
# println!("{output}");
# Ok(())
# }
```

## Queries

`Worker::register_replayed_workflow` gives ordinary workflow execution a typed
`WorkflowInstance<S>`. Put transitions after activity and signal resolution in
that workflow closure. `Worker::register_replayed_query` re-runs the same
closure over committed durable history, then invokes the named query with an
immutable, detached `Arc<S>`. This is the recommended workflow-instance query
API: query code does not parse history or duplicate transition logic.

Replay-generated commands are discarded. A query handler has no command API,
and its detached state is never retained, so successful and failed queries do
not append history, advance execution, or change a later query. The same query
serves running, restarted, and successfully completed workflows:

```rust
# use durable_workflow::{json, Client, Worker};
# #[derive(Clone, Default)]
# struct CounterState { count: i64 }
# fn configure(client: Client) {
let mut worker = Worker::new(client, "counter-workers");
worker.register_replayed_workflow("counter", CounterState::default, |ctx, _input, state| async move {
    let signal = ctx.wait_signal("increment").await?;
    let amount = signal.first().and_then(|value| value.as_i64()).unwrap_or_default();
    state.update(|current| current.count += amount)?;
    state.read(|current| Ok(json!(current.count)))?
});
worker.register_replayed_query::<CounterState, _, _>("counter", "current", |_ctx, state, _args| async move {
    Ok(json!(state.count))
});
# }
```

`Worker::register_query` remains the lower-level snapshot-inspection API. Its
`QueryContext` exposes normalized workflow input, raw committed history, and
decoded signals. Use it for transport-level inspection when replayed typed
state is not appropriate; snapshot handlers must reduce history themselves and
are not workflow-instance query parity.

Client-side rejections are `Error::QueryFailed(QueryFailure)`. Match the
public `reason` and `status` fields for automation; the original response is
retained in `body`. Stable reasons include `rejected_unknown_query`,
`invalid_query_arguments`, `query_handler_unavailable`,
`query_payload_decode_failed`, `query_workflow_state_unavailable`, and
`query_worker_unavailable`. Protocol negotiation failures use
`Error::Protocol(ProtocolFailure)` and retain supported/requested versions.

## Heartbeats

`Worker::run` and `Worker::run_until` register the worker and then send worker
heartbeats automatically. The registration response supplies the preferred
cadence; `Worker::heartbeat_interval` is the fallback when the server does not
advertise one. Use `Worker::on_worker_heartbeat` to observe successful server
acknowledgements for metrics or structured logging.

Activity handlers report progress with `ActivityContext::heartbeat`. The
returned `ActivityHeartbeatResponse` exposes `heartbeat_recorded` and
`cancel_requested` so long-running work can respond to server state:

```rust
# use durable_workflow::{json, Client, Result, Worker};
# fn configure(client: Client) {
let mut worker = Worker::new(client, "rust-workers")
    .on_worker_heartbeat(|observation| {
        println!("worker heartbeat acknowledged at {}", observation.acknowledged_at_unix_millis);
    });

worker.register_activity("batch.process", |ctx, _args| async move {
    let acknowledgement = ctx.heartbeat(json!({"completed": 25})).await?;
    if acknowledgement.cancel_requested {
        return Ok(json!({"cancelled": true}));
    }
    Ok(json!({"completed": 100}))
});
# }
```

Lower-level integrations can call `Client::heartbeat_worker` and
`Client::heartbeat_activity_task` directly.

## Worker liveness and errors

Workflow, activity, and query polls advertise the configured poll timeout to
the server. An empty response at that boundary is normal: `Worker::run` and
`Worker::run_until` keep every poller and worker heartbeats running, so the same
worker can accept work after an idle period.

Poll acquisition and worker-heartbeat transport failures, HTTP 408/429
responses, and server errors use capped exponential backoff. Configure the
bound with `Worker::retry_policy`; the default retries five times from 100 ms
up to 5 seconds. Retries wrap only acquisition and heartbeat requests, never a
leased task's handler or settlement request, so an ambiguous completion is not
re-executed by the retry loop. Once the retry bound is exhausted, the transport
or HTTP error is returned.

Authentication failures remain `Error::Http` with their status and response
body, and protocol incompatibilities remain
`Error::Protocol(ProtocolFailure)` with stable reason and version fields.
Codec, handler, and other non-retryable failures are returned immediately and
are never retried indefinitely.

## Example

`examples/hello_world.rs` contains a complete round trip: it registers a Rust
worker, starts a workflow, sends a signal, runs an activity, heartbeats that
activity, exposes a named query, and waits for the completed result.

With a Durable Workflow server running locally:

```sh
DURABLE_WORKFLOW_SERVER_URL=http://127.0.0.1:8080 \
DURABLE_WORKFLOW_TOKEN=your-token \
cargo run --example hello_world
```

`TASK_QUEUE` optionally overrides the default `rust-workers` task queue.

## API documentation

The complete API reference is published at
[rust.durable-workflow.com](https://rust.durable-workflow.com/). Documentation
for `main` is rebuilt and deployed automatically.

## Ownership and versioning

The Durable Workflow project owns and maintains the crate. This repository is
the authoritative source for the `durable-workflow` crate and its Rust API
documentation.

Crate releases follow semantic versioning and are tagged with the exact crate
version, such as `0.1.1`. Rust SDK versions are independent from Durable
Workflow server image versions. A compatible server range is declared in
package metadata instead of coupling crate publication to a server release.
