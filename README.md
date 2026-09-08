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
- `Worker` registers workflow, activity, signal, query, and update handlers and
  long-polls task queues.
- `WorkflowContext` provides durable activities, timers, conditions, child
  workflows, side effects, version markers, parallel operations, selection,
  sagas, message streams, memo, search attributes, and continue-as-new.
- Typed registration and result helpers preserve Serde request and result
  types over the fixed Avro Value protocol.
- Activity options cover retries, start-to-close, schedule-to-start,
  schedule-to-close, heartbeat timeouts, cancellation, and heartbeats.

The SDK writes Avro payloads only. The fixed recursive Value schema preserves
nulls, booleans, signed 64-bit integers, finite doubles, bytes, UTF-8 strings,
lists, and string-keyed maps across official SDKs without customer-managed
schemas or a registry.

Server-managed external payloads are fetched automatically through the same
runtime URL, namespace, and credential role, with size and SHA-256 verification.
`Client::builder(...).max_external_payload_bytes(...)` limits unique downloaded
bytes per response (64 MiB by default). Provider credentials are not needed;
payload fetches never follow redirects.

## Examples

| Example | Demonstrates |
| --- | --- |
| [`hello_world.rs`](https://github.com/durable-workflow/sdk-rust/blob/main/examples/hello_world.rs) | Typed worker, workflow, activities, retries, and completion |
| [`activity_options.rs`](https://github.com/durable-workflow/sdk-rust/blob/main/examples/activity_options.rs) | Activity retry and timeout policies |
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
