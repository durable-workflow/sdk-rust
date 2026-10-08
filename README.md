# Durable Workflow Rust SDK

[![CI](https://github.com/durable-workflow/sdk-rust/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/durable-workflow/sdk-rust/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/durable-workflow.svg)](https://crates.io/crates/durable-workflow)
[![API docs](https://img.shields.io/badge/docs-rust.durable--workflow.com-0f766e.svg)](https://rust.durable-workflow.com/)
[![Rust 1.86+](https://img.shields.io/badge/rust-1.86%2B-orange.svg)](https://github.com/durable-workflow/sdk-rust/blob/main/Cargo.toml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/durable-workflow/sdk-rust/blob/main/LICENSE)

`durable-workflow` is the first-party Rust client and worker SDK for
[Durable Workflow](https://durable-workflow.com/). Rust applications can start
and inspect workflows, run workflow and activity handlers, and exchange typed
Avro values with PHP, Python, and Rust workers through one durable runtime.

Use the SDK with a self-hosted Durable Workflow Server or a managed Durable
Workflow Cloud namespace. Your workers remain ordinary Rust processes and
scale independently from the runtime.

## Install

Rust `1.86` or newer is required.

```sh
cargo add durable-workflow
```

Applications using Tokio entry points should also enable the Tokio features
they need:

```sh
cargo add tokio --features macros,rt-multi-thread
```

## Run the example

[`examples/hello_world.rs`](https://github.com/durable-workflow/sdk-rust/blob/main/examples/hello_world.rs) is a complete typed
workflow and activity example. It starts a workflow, runs two activities,
demonstrates retry and failure handling, waits for completion, and prints the
result.

Start a bootstrapped [self-hosted Server](https://durable-workflow.com/docs/2.0/polyglot/server/),
then run the example from this repository:

```sh
DURABLE_WORKFLOW_SERVER_URL=http://127.0.0.1:8080 \
DURABLE_WORKFLOW_TOKEN=dev-token \
cargo run --example hello_world
```

Pass the Server origin without a trailing `/api`. `TASK_QUEUE` changes the
default `rust-workers` queue, and `GREETING_NAME` changes the example input.

For a provisioned Cloud namespace, use the exact runtime URL and the separate
client and worker credentials shown in Cloud:

```sh
DURABLE_WORKFLOW_RUNTIME_URL=https://cloud.durable-workflow.com/api/runtime/v1/namespaces/your-namespace-id \
DURABLE_WORKFLOW_RUNTIME_NAMESPACE=your.namespace \
DURABLE_WORKFLOW_CLIENT_TOKEN=your-client-token \
DURABLE_WORKFLOW_WORKER_TOKEN=your-worker-token \
cargo run --example hello_world
```

The namespace runtime URL is already complete. Do not append another `/api`.

## Core API

- `Client` starts, signals, queries, updates, cancels, terminates, describes,
  and awaits workflow executions.
- `Worker` registers workflow, activity, query and update handlers, declares
  workflow signals, and long-polls task queues.
- `WorkflowContext` provides durable activities, timers, conditions, child
  workflows, side effects, version markers, parallel operations, selection,
  sagas, message streams, memo, search attributes, and continue-as-new.
- Typed registration and result helpers preserve Serde request and result
  types over the fixed Avro Value protocol.
- Activity options cover retries, start-to-close, schedule-to-start,
  schedule-to-close, heartbeat timeouts, cancellation, and heartbeats.

After registering a workflow, declare every signal name it reads through
`wait_signal` or `signals` before starting the worker:

```rust
worker.declare_workflow_signals("orders", &["finish", "changed"])?;
```

Server records those names and positional argument contracts when starting a
run. Changing worker declarations does not change existing runs.

The SDK writes Avro payloads only. The fixed recursive Value schema preserves
nulls, booleans, signed 64-bit integers, finite doubles, bytes, UTF-8 strings,
lists, and string-keyed maps across official SDKs without customer-managed
schemas or a registry.

Large payloads are uploaded automatically when the namespace advertises runtime
storage. The SDK follows its inline threshold and upload limit, including batches
that exceed the ordinary request limit. Uploads and downloads use the same
runtime URL, namespace, and credential role, with size and SHA-256 verification.
`Client::builder(...).max_external_payload_bytes(...)` limits unique downloaded
bytes per response (64 MiB by default). Provider credentials are not needed;
payload fetches never follow redirects.

## Examples

### Local activities

Enable inline local execution with `Worker::new(client, queue).local_activities(true)`.
Register the callback with the ordinary activity registration methods, then call
`ctx.local_activity(...)`, `local_activity_typed(...)` or
`local_activity_avro_value(...)` from workflow code. `LocalActivityOptions` sets
bounded retries and start-to-close, schedule-to-close and heartbeat timeouts.

Local activities run in the workflow worker and bypass the activity queue.
Server records their attempts, heartbeat details and terminal result. Committed
results replay without running the callback. A worker lost before completion is
acknowledged can execute the callback again, so external effects must be idempotent.

Callbacks must yield to Tokio. Timeout or lost workflow lease drops the async
callback without requiring application heartbeats. Blocking work needs separate
process supervision. Inline local execution and cooperative prepared local
supervision use separate worker profiles. Registration rejects combining them.
Database-generated execution and failure IDs become available after Server
commits history. Use the failure kind, timeout kind and attempt number to branch
during fresh execution, rather than testing whether an ID has been assigned.

Available since SDK 3.1.0. Default workers keep local execution disabled.
Opted-in workers negotiate their local capability during registration.

### Worker sessions

Enable `Worker::worker_sessions(true)` and declare the resource requirements that
the worker actually satisfies with `capabilities(...)`. Route a remote call with
`ctx.activity(...).in_worker_session(options)` and use `.typed::<Output>()` or
`.avro_value()` for its result. A parallel group's `in_worker_session(...)` applies
the same routing to each activity leaf.

Server creates a session on the first admitted activity. After registration,
`worker.worker_session(options)` also provides an explicit shared handle for
`create()`, `renew()` and `close(reason)`. `ActivityContext::worker_session()` exposes
the current handle. Heartbeats renew the holder lease without extending the
absolute TTL. Graceful worker shutdown drains activities and closes held sessions
before deregistering.

`lease_seconds(...)` bounds holder authority and `ttl_seconds(...)` bounds the
session's total lifetime. Reacquisition retains the original TTL deadline. Renew
the handle explicitly while keeping an idle resource alive. Activity heartbeats
also renew the lease. An async callback is dropped when its locally observed lease
or TTL ends, even without application heartbeats. Callbacks must yield to Tokio.

Set `max_concurrent_worker_sessions(...)` to bound the worker's session registry
and `max_concurrent_activities(...)` on the options to bound each session's activity
concurrency. Uncreated or failed handles release their local slot when dropped.
An admitted session keeps its slot while its holder lease is active.

Session memory is process-local. A replacement holder must rebuild its resources,
and an interrupted activity can execute again. Use idempotency and attempt fencing
for external side effects. Committed results replay from history without rebuilding
resources or rerunning callbacks. Changing recorded session options fails replay.
Local activities cannot use session routing. Sticky execution remains unsupported.

The runnable session example uses a real process-local cache and prints its resource
generation. Use Server 2.5.1 / Native 2.4.1 for session history and original TTL
preservation. Session support starts with SDK 3.2.0.

### Recovering from Server outages

For a long-running service worker, enable `.recover_transient_outages(true)`
on `Worker`. Retryable poll and worker-heartbeat failures then keep retrying
with capped exponential backoff. A retried poll keeps its original request ID.
`run_until` interrupts retry waits when shutdown arrives and settles any poll
response already in flight.

The default preserves bounded retries. `run_once` remains bounded in either
mode, and `WorkerRetryPolicy.max_retries = 0` disables ordinary retries.
Authentication, protocol, codec, handler and task settlement failures still
return an error. Registration and deregistration are outside this recovery
option. Keep a process supervisor for startup failures and process crashes.

### Runnable examples

| Example | Demonstrates |
| --- | --- |
| [`hello_world.rs`](https://github.com/durable-workflow/sdk-rust/blob/main/examples/hello_world.rs) | Typed worker, workflow, activities, retries, and completion |
| [`activity_options.rs`](https://github.com/durable-workflow/sdk-rust/blob/main/examples/activity_options.rs) | Activity retry and timeout policies |
| [`local_activities.rs`](https://github.com/durable-workflow/sdk-rust/blob/main/examples/local_activities.rs) | Explicit local execution, retries, durable timers and remote work |
| [`worker_sessions.rs`](https://github.com/durable-workflow/sdk-rust/blob/main/examples/worker_sessions.rs) | Typed activity routing, holder-local resources and graceful session close |
| [`condition_search_attributes.rs`](https://github.com/durable-workflow/sdk-rust/blob/main/examples/condition_search_attributes.rs) | Durable conditions and typed search attributes |
| [`continue_as_new.rs`](https://github.com/durable-workflow/sdk-rust/blob/main/examples/continue_as_new.rs) | Bounded histories and continue-as-new |
| [`parallel_saga.rs`](https://github.com/durable-workflow/sdk-rust/blob/main/examples/parallel_saga.rs) | Deterministic parallel work and saga compensation |

## Documentation

- [Rust SDK landing page](https://rust.durable-workflow.com/)
- [Generated API reference](https://rust.durable-workflow.com/durable_workflow/)
- [Rust SDK guide](https://durable-workflow.com/docs/2.0/polyglot/rust/)
- [Self-hosted Server guide](https://durable-workflow.com/docs/2.0/polyglot/server/)
- [Cloud early access](https://cloud.durable-workflow.com/early-access)

## Compatibility

The SDK supports cooperative requests and bounded,
replayable cleanup. Enable `Worker::cooperative_cancellation(true)` against a
Server that advertises protocol 1.20 and the required capabilities. This profile
supervises async Activity futures independently of application heartbeats.
Independently cancellable scopes remain disabled. See the
[cancellation guide](https://github.com/durable-workflow/sdk-rust/blob/main/docs/cooperative-cancellation-design.md)
and [v3 migration guide](https://github.com/durable-workflow/sdk-rust/blob/main/docs/migrating-to-v3.md).

The crate publishes its supported Server and worker-protocol ranges in
`[package.metadata.durable-workflow]` in [`Cargo.toml`](https://github.com/durable-workflow/sdk-rust/blob/main/Cargo.toml). Runtime
capability manifests, not matching package version strings, determine protocol
compatibility. Stable releases follow semantic versioning.

## Development

```sh
cargo fmt --all --check
cargo test --all-targets --all-features
cargo doc --all-features --no-deps
cargo package
```

Replay and codec defects require a minimal regression fixture. See
[`CONTRIBUTING.md`](https://github.com/durable-workflow/sdk-rust/blob/main/CONTRIBUTING.md) for the corpus rules.

## License

Durable Workflow Rust SDK is released under the [MIT License](https://github.com/durable-workflow/sdk-rust/blob/main/LICENSE).
