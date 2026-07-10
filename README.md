# Durable Workflow Rust SDK

`durable-workflow` is the first-party Rust SDK for Durable Workflow workers
and clients. It can register workflow and activity handlers, long-poll the
worker protocol, start, signal, and query workflow executions, expose named
read-only query handlers, heartbeat workers and activities, and exchange
JSON-native payloads through the platform's generic Avro wrapper.

## Install

Add the exact crates.io release with Cargo:

```sh
cargo add durable-workflow@0.1.0 --exact
```

Or add the same exact requirement directly to `Cargo.toml`:

```toml
[dependencies]
durable-workflow = "=0.1.0"
```

Version `0.1.0` requires Rust `1.86` or newer. Query APIs described below are
available from the direct-successor `0.1.1` release.

## Compatibility

| SDK releases | Durable Workflow server | Worker protocol | Control plane |
| --- | --- | --- | --- |
| `0.1.0` | `>=0.2,<0.3` | `1.2` | `2` |
| `0.1.1+` | `>=0.2,<0.3` | `1.2` (queries require `1.8`) | `2` |

The machine-readable values live in `[package.metadata.durable-workflow]` in
`Cargo.toml` as `supported-server-versions`, `worker-protocol-version`, and
`control-plane-version`. Query-capable releases also publish `query-tasks`,
`query-task-minimum-worker-protocol-version`, and `payload-codecs`. Existing
worker operations retain the `1.2` baseline; only query-task poll, complete,
and fail requests use the additive `1.8` feature floor. The server's advertised
protocol manifests remain authoritative when checking compatibility during
deployment.

## Worker

```rust
use durable_workflow::{json, Client, Result, Worker};

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

    worker.register_workflow("hello.workflow", |ctx, _input| async move {
        let signal = ctx.wait_signal("start").await?;
        let name = signal.first().and_then(|value| value.as_str()).unwrap_or("world");
        let greeting = ctx.activity("hello.activity", json!([name])).await?;
        Ok(json!({"greeting": greeting}))
    });

    worker.register_query("hello.workflow", "started-by", |ctx, _args| async move {
        let name = ctx
            .signals("start")
            .last()
            .and_then(|args| args.first())
            .cloned()
            .unwrap_or(json!(null));
        Ok(name)
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

`Worker::register_query` associates a public query name with a workflow type.
The handler receives an immutable `QueryContext` containing the normalized
workflow input, committed history, and decoded signals in workflow order. It
has no command-emission API, and completing or failing a query task does not
append history or advance workflow execution.

Query handlers must remain read-only: do not mutate captured application state
or perform side effects. Rebuild the answer from the supplied snapshot. The
same handler serves running workflows and successfully completed workflows:

```rust
# use durable_workflow::{json, Client, Worker};
# fn configure(client: Client) {
let mut worker = Worker::new(client, "counter-workers");
worker.register_workflow("counter", |ctx, _input| async move {
    let _ = ctx.wait_signal("increment").await?;
    Ok(json!(null))
});
worker.register_query("counter", "current", |ctx, _args| async move {
    let count: i64 = ctx
        .signals("increment")
        .iter()
        .filter_map(|args| args.first().and_then(|value| value.as_i64()))
        .sum();
    Ok(json!(count))
});
# }
```

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
