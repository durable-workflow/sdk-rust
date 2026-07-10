# Durable Workflow Rust SDK

`durable-workflow` is the first-party Rust SDK for Durable Workflow workers
and clients. It can register workflow and activity handlers, long-poll the
worker protocol, start and signal workflow executions, heartbeat workers and
activities, and exchange JSON-native payloads through the platform's generic
Avro wrapper.

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

Version `0.1.0` requires Rust `1.86` or newer.

## Compatibility

| SDK releases | Durable Workflow server | Worker protocol | Control plane |
| --- | --- | --- | --- |
| `0.1.x` | `>=0.2,<0.3` | `1.2` | `2` |

The machine-readable values live in `[package.metadata.durable-workflow]` in
`Cargo.toml` as `supported-server-versions`, `worker-protocol-version`, and
`control-plane-version`. The server's advertised protocol manifests remain
authoritative when checking compatibility during deployment.

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

let output = handle.result(Default::default()).await?;
# println!("{output}");
# Ok(())
# }
```

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
activity, and waits for the completed result.

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
version, such as `0.1.0`. Rust SDK versions are independent from Durable
Workflow server image versions. A compatible server range is declared in
package metadata instead of coupling crate publication to a server release.
