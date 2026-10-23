# Durable Workflow Rust SDK

[![Quality Gate](https://sonarcloud.io/api/project_badges/measure?project=durable-workflow_sdk-rust&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=durable-workflow_sdk-rust)
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

## Migrate from Rust SDK 3

Nexus calls add `Error::NexusOperationFailed` and
`CancellationCallKind::ServiceOperation`. Both enums are now `non_exhaustive`.
Include a fallback arm when matching either enum outside this crate so future
error or cancellation kinds can be handled without another exhaustive-match
migration. Handle a Nexus failure at its original durable call; a committed
admission is separate from a synchronous completion.

`WorkflowTask`, `ActivityTask` and `QueryTask` retain optional, untrusted
observability metadata. `ActivityTask` also exposes optional workflow, run and
activity execution identities supplied by the runtime. When constructing task
structs directly, initialize the new optional fields to `None`. Receiving these
fields does not enable tracing, authorize work or invent missing correlation.
Task structs remain directly constructible. Future additions to their public
fields require another major release; extensible metadata inside the existing
optional JSON fields does not require new task literal fields.

Existing workflow histories are unchanged. Preserve the workflow's recorded
command sequence and verify representative histories before deployment. Nexus
authoring requires explicit worker opt-in and an installed Server capability;
ordinary worker registration does not enable it.

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

### Private certificate authorities

The SDK trusts bundled Mozilla public roots by default. It does not load the
operating system trust store or `SSL_CERT_FILE`. For a self-hosted Server or a
proxy using a private certificate authority, add its PEM root certificates:

```rust,no_run
use durable_workflow::Client;
# fn client() -> Result<Client, Box<dyn std::error::Error>> {
let client = Client::builder("https://workflow.internal")
    .additional_root_certificates_pem(std::fs::read("company-ca.pem")?)?
    .build()?;
# Ok(client)
# }
```

The bundle can contain multiple certificates, and repeated calls add more
roots. Bundled public roots remain enabled. The same configured HTTP client
handles control requests, worker polling and external payload transfers.
Certificate-chain and hostname verification remain enabled. A private CA does
not make a certificate for the wrong hostname valid. Client certificates
(mTLS) are not configured by this option.

### Worker concurrency

`Worker::max_concurrent_workflow_tasks(...)` and
`Worker::max_concurrent_activity_tasks(...)` each default to 10. `run` and
`run_until` run up to those limits in independent workflow and activity lanes.
Use a value of 1 for serial execution within a task family. Clones share their
configured capacity, including concurrent `run_once` calls.

Heartbeats report unused slots from actual claimed tasks, including input and
history loading, callback execution and settlement. An empty long poll keeps
its capacity available. Success, failure, refusal and cancellation release the
local slot. Inline local activities occupy their workflow-task slot and bypass
the remote activity pool. Queries retain their separate polling lane.

Handlers run as async futures on Tokio and must yield. Use
`tokio::task::spawn_blocking` or a separate process for blocking work, with its own
cancellation boundary. A concurrency setting does not provide CPU isolation or
make blocking code cancellable.

### Query execution authority

On a runtime advertising `query_task_authority` with worker protocol 1.20,
`Worker` captures the original query claim before loading external inputs. It
checks ownership without renewing the lease during loading and execution, before
calling the handler, and before submitting its outcome. The original caller
deadline and lease bound the entire operation. Expiry, shutdown or an unavailable
observation drops the work without sending a completion or failure for it.

Use `Worker::require_query_task_authority(true)` to refuse registration unless
worker-authenticated discovery advertises this support. Without this requirement,
older runtimes and explicit protocol 1.19 keep compatibility: disclosed original
timing supplies a local bound, while deliveries without timing retain legacy
behavior. This path cannot observe live revocation and never calls the 1.20
query-status endpoint.

Async handlers must yield to Tokio to be cancelled promptly. Synchronous or
CPU-bound callbacks cannot be interrupted while they block, but their late result
is fenced before submission. Query handlers should avoid side effects. Direct
`Client` completion and failure methods remain available for inspecting an
original committed receipt.

### Handler registration

Register each workflow and activity wire name once per Worker. Named query and
update handlers are also scoped to their workflow type. Dynamic, typed, Avro and
replayed adapters share these namespaces. Repeating a registration, including
the same callback, is an error. Different Workers or handler kinds may reuse a
name, and queries or updates on different workflow types may reuse a name.

Registration methods keep their existing signatures. A duplicate preserves the
first handler and invalidates that Worker. Call `worker.validate_registration()?`
after configuring it for a local check. `register`, `run`, `run_until` and
`run_once` also reject it before contacting Server with `Error::WorkerLoop`
and a `duplicate_registration` diagnostic. Local validation returns the typed
`DuplicateRegistrationError`, exposing the handler kind, wire name, optional
workflow scope, and both source locations and authoring adapters. It converts
into the existing worker-error variant when used with `?`, preserving exhaustive
matches on the SDK's existing `Error` enum.
Construct a new Worker to correct an invalid configuration. Choose the desired
handler before registration instead of relying on replacement order.

After registering a workflow, declare every signal name it reads through
`wait_signal` or `signals` before starting the worker:

```rust,no_run
# fn declare_signals(worker: &mut durable_workflow::Worker) -> durable_workflow::Result<()> {
worker.declare_workflow_signals("orders", &["finish", "changed"])?;
# Ok(())
# }
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

Workers confirm and renew the exact workflow task claim while loading external
inputs and paged history, then confirm it again before replay. Loading stops on
lost authority or Worker shutdown without executing workflow code. Pending
cooperative cancellation retains its original identity and cleanup deadline.

External activity inputs retain their original lease and execution deadline
bounds while loading. Without the optional lease-renewal capability, protocol 1.20 observes the exact activity attempt before,
during and after the transfer, and immediately before callback entry. Unconfirmed
authority stops loading without invoking the callback. These observations do not
renew the lease or record application heartbeats. Explicit protocol 1.19 uses
the original local lease/deadline bounds and completion fencing; it cannot
observe live revocation or ownership. Live authority supervision requires 1.20.
Shutdown and dropping a poll stop its loading and observation together.

The optional `Client::renew_activity_task_lease` request is available only when
actual runtime discovery advertises `activity_lease_renewal=true`. Pass the
original task ID, attempt ID, lease owner and a positive request window of at
most five seconds. Discovery and renewal share that window and use worker
credentials. This request records no application progress. Validate the full
original identity, granted lease and business deadlines before using its reply
to extend local authority. Direct Client users manage their own renewal lifecycle.

When registration, cluster discovery or the actual poll advertises that boolean
capability, `Worker` confirms and automatically maintains the original activity
claim throughout input loading, the yielding callback and terminal publication.
Missing or changed task, attempt, holder or run identity prevents callback entry.
Lost authority, exhausted original timeouts or managed shutdown drop the callback
future and fence retained callback Clients against late completion, failure,
heartbeat or result upload. Retryable renewal errors grant no additional time.
Older runtimes keep their existing path.

Runtime lease pings do not record application progress or satisfy a configured
heartbeat timeout. Continue calling `ctx.heartbeat(...)` for real progress; only
acknowledged same-attempt progress advances that recorded interval. Session-bound
activities also require an acknowledged same-holder session grant and retain
their original absolute TTL. The maintainer starts before callback entry, so
a spare executor thread can keep it running if a callback temporarily blocks.
Callbacks must still yield to Tokio: maintenance cannot progress if every
executor thread is blocked. Use `tokio::task::spawn_blocking` for blocking I/O
and CPU work, and give that work its own cancellation or process boundary.
Dropping an async callback cannot stop detached threads or reverse external
side effects.

Completion still uses the runtime's attempt fence. Server 2.6.1 refuses
superseded attempts but can accept an expired, unreclaimed attempt; see
[Workflow #769](https://github.com/durable-workflow/workflow/issues/769).

The Client's default 60-second HTTP timeout includes connection and the complete
response body. `Client::builder(...).timeout(Duration::from_secs(120))` selects a
larger total budget before starting a Worker. Claimed input and history loading
share that budget across all pages. Lease renewals do not restart it. Low-level
cooperative polls retain their long-poll timeout plus five seconds, and explicit
canonical cancellation refreshes retain their five-second budget.

## Testing without a Server

`testing::WorkflowEnvironment` runs registered workflows and activities in the
current process. Use `from_worker(worker)` to reuse your application's complete
registration set, or register test callbacks directly. `worker_mut()` exposes
the existing typed and replayed-state registration APIs.

```rust
use durable_workflow::{json, testing::WorkflowEnvironment};
# async fn test() -> durable_workflow::Result<()> {
let mut env = WorkflowEnvironment::new()?;
env.register_activity("greet", |_, args| async move {
    Ok(json!(format!("Hello, {}!", args[0].as_str().unwrap())))
});
env.register_workflow("hello", |ctx, args| async move {
    ctx.activity("greet", args).await
});
assert_eq!(env.run_activity("greet", "Ada").await?, json!("Hello, Ada!"));
env.mock_activity("greet", |_, _| async { Ok(json!("mock greeting")) });
assert_eq!(env.run_workflow("hello", "Ada").await?, json!("mock greeting"));
# Ok(())
# }
```

Workflow execution uses the worker's cold-replay engine, codec and activity
adapters. Real or mocked remote/local activity failures become the same typed
`Error::ActivityFailed` inside workflow code, including exception type, attempt
number and non-retryable marker. Retry policies apply before workflow code
receives a terminal failure. Timer and retry delays are skipped; activity
callbacks still execute normally and must yield. An uncaught workflow error is
returned with its original Rust error variant. Direct `run_activity` executes
one attempt. Heartbeats are captured locally and closed when the attempt ends.
The `*_avro_value` methods preserve fixed-Avro bytes, integers and other values
that have no lossless JSON representation.

The executor handles activities, local activities, timers, side effects,
version markers, memo/search-attribute upserts and ordered parallel commands.
Parallel callbacks run in command order. Activity timeout simulation, new select
races, signals, child workflows, services, sessions, continue-as-new and cancellation require
integration tests; unsupported commands return `Error::Testing` explicitly.
This environment does not simulate queues, leases or deployment behavior.
`max_steps(n)` bounds durable turns, commands and attempts (default 10,000);
use `tokio::time::timeout` to bound asynchronous callbacks as well.

### Replay before deploying changed workflow code

Load a self-contained JSON bundle from `dw workflow:history-export` or
`Client::export_workflow_history(workflow_id, run_id).await?`, register the
candidate workflow code, then replay without a Server:

```rust,no_run
use durable_workflow::testing::{WorkflowEnvironment, WorkflowHistory};
# fn replay(env: WorkflowEnvironment) -> std::result::Result<(), Box<dyn std::error::Error>> {
let export = serde_json::from_slice(&std::fs::read("workflow-history.json")?)?;
let history = WorkflowHistory::from_export(export)?;
let report = env.replay(&history)?;
assert!(report.completed); // A running snapshot can remain durably pending.
# Ok(())
# }
```

For the raw history API, combine **all** pages and call
`WorkflowHistory::from_history("hello", original_input, response)?`.
Set `.task_queue("original-queue")` if that context is absent. Exports must
include original input and required recorded results. Redacted/pruned payloads,
unresolved external references and remaining page tokens are refused. The SDK
export helper resolves payload references while downloading; replay itself
never contacts a Server. `env.last_history()` captures a new in-process run and
can be exported for regression tests, including after an execution error.

Replay uses the worker's decoding, side-effect, version-marker and persisted
select-resolution paths. Activity callbacks do not run. Reordered or changed
commands return `Error::NonDeterministicReplay(ReplayFailure)` with `sequence`,
`expected` and `actual`. New commands beyond the snapshot also fail; use a
fresh in-process run to test new logic. A report contains the reconstructed
terminal commands or an empty decision for a recorded pending operation.

Keep workflow command order deterministic:

| Pattern | Durable replacement |
| --- | --- |
| Iterating a `HashMap` while scheduling | Sort keys first or use a `BTreeMap` |
| Reading the system clock, randomness or a UUID | `ctx.side_effect(...)` for a recorded snapshot, or an activity |
| `tokio::time::sleep`, `std::thread::sleep` | `ctx.sleep(...)` / `ctx.start_timer(...)` |
| `tokio::select!`, racing ordinary futures | `ctx.select(...)` / `ctx.select_keyed(...)` |
| Untracked concurrent durable calls | SDK `ctx.parallel(...)` with stable input ordering |
| Network, filesystem or other external I/O | A registered activity |
| Changing existing command order on deployment | A stable `ctx.get_version(...)` branch, checked against recorded history |

See the runnable [`workflow_testing`](examples/workflow_testing.rs) example and
[`workflow_testing` tests](tests/workflow_testing.rs).

## Workflow lists and complete run history

`Client::list_workflows(WorkflowListOptions)` and
`Client::get_workflow_history(workflow_id, run_id, WorkflowHistoryOptions)`
each return **one page**. Inspect `next_page_token` to resume manually, or use
the lazy iterators for complete reads. These methods wrap the published Server
APIs and preserve their filters, validation and namespace scope; they add no
Server query fields or grammar.

For example, find an open run by a registered keyword search attribute:

```rust,no_run
use durable_workflow::WorkflowListOptions;
# async fn find(client: durable_workflow::Client) -> durable_workflow::Result<()> {
let mut matches = client.iter_workflows(
    WorkflowListOptions::new()
        .workflow_type("payments")
        .status("running")
        .query("OrderRef = 'order-42'")
        .page_size(2),
);
match (matches.next().await?, matches.next().await?) {
    (Some(run), None) => println!("open run: {} / {}", run.workflow_id, run.run_id),
    (None, _) => println!("no open run"),
    _ => println!("multiple open runs; refine the query"),
}
# Ok(())
# }
```

The Server's `status=running` filter selects the active **status bucket**,
including runs whose raw status is `pending` or `waiting`. `WorkflowSummary`
preserves `status`, `status_bucket`, `is_terminal`, search attributes and unknown
metadata in `raw`. Search attributes do not impose a uniqueness constraint.
Create the client with the intended namespace before constructing the iterator;
the same namespace and filters are retained across every opaque token.

Read an entire selected run without loading every page at once:

```rust,no_run
use durable_workflow::WorkflowHistoryOptions;
# async fn audit(client: durable_workflow::Client) -> durable_workflow::Result<()> {
let mut events = client.iter_workflow_history(
    "payment-42", "selected-run-id", WorkflowHistoryOptions::new().page_size(100),
);
while let Some(event) = events.next().await? {
    println!("{}", event.event_type);
}
# Ok(())
# }
```

Similarly, `iter_workflows(options)` yields each `WorkflowSummary` through
`next().await`. Both iterators also provide `into_stream()` for standard
`futures_util::TryStreamExt` consumers; pin the returned stream before polling.
Constructing an iterator performs no I/O. It fetches only when
the current page is exhausted, holds one page, and ends on a null or empty
continuation token. Empty pages with a continuation are followed. Repeated or
cyclic tokens produce `Error::Pagination` using bounded token state. A page
failure is returned with its original typed error; that iterator then ends.
Handle the error rather than treating a partial read as a complete result.

Iterating history keeps the original workflow/run identity and does not follow
continue-as-new into another run. It enumerates available history rather than
subscribing indefinitely. History payloads retain their wire envelopes and
lossless event metadata. For offline replay, use the existing
`Client::export_workflow_history` and `testing::WorkflowHistory::from_export`
instead of passing a partial page to the replay API.

Published Server 2.11 defaults to 50 list rows (maximum 200) and 100 history
events (maximum 1,000) per page. Page sizes are sent unchanged for Server
validation. Stop the iterator when you have enough results, choose an appropriate
page size, and bound work with your application's timeout. List pagination
inherits the Server's ordering/consistency contract; it does not add snapshot
isolation while runs change concurrently. See the read-only
[`workflow_pagination` example](examples/workflow_pagination.rs).

## Examples

### Activity retry policies

`ActivityRetryPolicy::new(max_attempts).backoff_intervals(...)` preserves the
complete explicit list, including entries that the attempt budget will not use.
One attempt uses no retry delay. The server repeats the last entry when further
retries are allowed, while the attempt budget still limits execution. Replay
compares the complete stored list and the other known policy fields exactly;
changing an unused entry is also a policy change. The default remains one
attempt with an empty backoff list.

Activity handlers can return an application-defined failure type with
`ActivityError`. The same type, message and retry marker are reported for remote
and local activities, including cooperative workers:

```rust
use durable_workflow::{ActivityError, Worker};
# fn register(worker: &mut Worker) {
worker.register_activity("charge", |_, _| async {
    Err(ActivityError::new("PaymentDeclined", "card declined")
        .non_retryable(true)
        .into())
});
# }
```

`ActivityError::new(...)` is retryable by default. An activity's
`ActivityRetryPolicy::non_retryable_error_types(["PaymentDeclined"])` also stops
retries when the returned type matches exactly. Setting `.non_retryable(true)`
stops retries regardless of that list. Workflow code receives the terminal
`Error::ActivityFailed` with its `exception_type`, `non_retryable` and attempt
number. Application types must contain 1 to 255 bytes and cannot be blank;
invalid types produce a non-retryable `ActivityFailureMetadataError`.

Other errors retain the generic `RustActivityFailure` type for remote execution
and `RustActivityError` for local execution. Use `ActivityError` when a retry
policy must match the same application failure in both modes.

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
Local activities cannot use session routing.

The runnable session example uses a real process-local cache and prints its resource
generation. Use Server 2.5.1 / Native 2.4.1 for session history and original TTL
preservation. Session support starts with SDK 3.2.0.

### Bounded sticky execution

Enable a durable-history cache explicitly on a Worker:

```rust,no_run
use durable_workflow::{Client, Result, StickyCacheOptions, Worker};
use std::time::Duration;

fn configure(client: Client) -> Result<Worker> {
    Worker::new(client, "rust-workers")
        .build_id("orders-v1")
        .sticky_cache(StickyCacheOptions::new(32)
            .max_history_bytes(8 * 1024 * 1024)
            .ttl(Duration::from_secs(60)))
}
```

Caching is disabled by default. A positive capacity opts in, and zero disables it.
The defaults are 16 MiB of retained encoded history and a 300-second TTL. Set
whole-second TTLs between 1 and 3600 seconds. Registration must confirm the worker,
queue, namespace, build and sticky capability before it can poll with caching.
Use Server 2.5.10 or newer. SDK support starts with 3.4.0.

The cache retains immutable wire histories, never live workflow instances or
session resources. Replays decode a fresh snapshot. Entry and encoded-byte bounds
use LRU eviction, and reads do not extend expiry. Decoding, replay, keys and cursor
metadata use additional memory. Size the cache within your worker's memory budget.

A warm replay validates the inline prefix and fetches the retained tail cursor
under the current task's lease and attempt. Missing, expired, changed or invalid
entries fall back to complete durable history. Worker replacement needs no cache
transfer. Committed side effects replay, and cancellation delivery remains governed
by canonical Server history. Terminal completion and worker shutdown clear entries.

`worker.sticky_cache_metrics()` reports hits, misses, evictions, forced cold
replays, retained entries and encoded history bytes. This reduces history fetching
for warm runs. Measure CPU, memory and completion rate with your own workloads.

An explicit `build_id(...)` pins new runs to that deployment build. A different
build cannot take an existing pinned run. Omit it to retain unversioned routing.
The runnable example below prints its result and cache counters.

### Deploying workflow patches

Use a stable change ID with `ctx.patched(...)` or `ctx.get_version(...)` to
preserve deployment decisions through replay. An old unmarked history selects
the legacy version `-1` if the next durable operation was already recorded.
New executions record one decision per change ID, even when code asks again.

Some older workers recorded a marker for every repeated call. Rust replays
consistent markers at their original positions, retaining the recorded history
and timestamps. Keep those call boundaries, or replace them with
`ctx.deprecate_patch(...)`, while such histories remain active. Replay consumes
one matching historical marker per call and never skips an intervening activity,
timer or side effect. Conflicting recorded versions and unsupported current
ranges remain replay errors.

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
| [`sticky_execution.rs`](https://github.com/durable-workflow/sdk-rust/blob/main/examples/sticky_execution.rs) | Opt-in bounded history, signal replay and cache counters |
| [`condition_search_attributes.rs`](https://github.com/durable-workflow/sdk-rust/blob/main/examples/condition_search_attributes.rs) | Durable conditions and typed search attributes |
| [`continue_as_new.rs`](https://github.com/durable-workflow/sdk-rust/blob/main/examples/continue_as_new.rs) | Bounded histories and continue-as-new |
| [`parallel_saga.rs`](https://github.com/durable-workflow/sdk-rust/blob/main/examples/parallel_saga.rs) | Deterministic parallel work and saga compensation |

### Optional execution telemetry

Enable the `telemetry` Cargo feature and configure your application's
OpenTelemetry provider, exporter, sampling and bounded export timeouts before
constructing the worker. Configure the application's global W3C propagator
before making admission calls or starting workers:

```rust,no_run
# #[cfg(feature = "telemetry")]
# fn main() -> Result<(), Box<dyn std::error::Error>> {
use durable_workflow::{Client, Worker};
use durable_workflow::telemetry::ExecutionTelemetry;
opentelemetry::global::set_text_map_propagator(
    opentelemetry_sdk::propagation::TraceContextPropagator::new(),
);
let client = Client::builder("http://localhost:8080").build()?;
let worker = Worker::new(client, "orders").telemetry(ExecutionTelemetry::new());
# Ok(())
# }
# #[cfg(not(feature = "telemetry"))]
# fn main() {}
```

The default build has no OpenTelemetry dependency. The optional feature needs
the API and official W3C propagators supplied by the OpenTelemetry SDK crate;
the application owns provider/exporter configuration and shutdown.
If the application already uses a composite propagator, retain its other
propagators and include W3C trace context. A missing propagator leaves task
spans independent even when the Server supports carriers. The SDK does not
replace the application's global configuration.
Servers without the exact optional capability use independent task traces.
When available, a valid run origin parents task spans; task-bound dispatch and
validated command/outcome contexts become links. The original poll identity,
generation and existing hydration, query and cooperative fences precede their
use. Missing or insufficient authority omits propagation without changing
business execution. Actual remote/local activity attempts have separate child
spans. Use the configured Server/SDK tuple qualified in
[Workflow #773](https://github.com/durable-workflow/workflow/issues/773).
Replay emits workflow work spans without pretending completed activities ran
again. No span stays open during durable waiting.

Configure admission/command clients with `client.with_telemetry(telemetry)` and
use existing `client.cluster_info().await` discovery during setup. Worker startup
can perform one bounded optional discovery; its failure does not refuse business
registration or add business retries. Only the exact supported
`worker_protocol.server_capabilities.observability_context` object enables
carriers. No mandatory protocol floor or Server version inference is added.
Caller capture precedes HTTP transport. Worker child/continue commands capture
their actual workflow scope before settlement and preserve business arguments
and request keys.

Official W3C propagation validates the agreed byte limits. Baggage defaults empty;
`with_baggage_allowlist` retains explicit keys only on run parents. Links never
carry baggage. Envelopes are bounded to 4096 bytes and at most 32 links. Malformed
metadata is ignored with fixed `discarded`/`filtered` span diagnostics. Use
`with_enabled(false)` to disable spans and propagation explicitly.

Use `telemetry::current_log_context()` in a logging adapter, or wrap an existing
OpenTelemetry logger provider in `telemetry::ExecutionLoggerProvider` before
passing it to your application's logging bridge. It adds bounded namespace,
worker, queue, task and available workflow/attempt IDs; trace/span IDs follow
the current application child span. Request IDs and log messages remain owned
by the application. The SDK installs no global logging bridge or provider.
Tokio contexts attach during each poll and restore on yield, completion, panic
and cancellation. Spawned application tasks must explicitly propagate their
context when that is intended. Export failures do not change business results;
applications must bound exporter I/O and drain their providers during shutdown.
Abrupt process death may lose spans; telemetry is not exactly-once delivery.

## Documentation

- [Rust SDK landing page](https://rust.durable-workflow.com/)
- [Generated API reference](https://rust.durable-workflow.com/durable_workflow/)
- [Rust SDK guide](https://durable-workflow.com/docs/2.0/polyglot/rust/)
- [Self-hosted Server guide](https://durable-workflow.com/docs/2.0/polyglot/server/)
- [Cloud early access](https://cloud.durable-workflow.com/early-access)

## Nexus service calls

Await a catalog operation from workflow code with Avro arguments:

```rust,ignore
let reply = ctx.call_nexus_service("billing", "invoices", "create", invoice).await?;
let receipt: Receipt = reply.result.deserialize()?;
```

The default synchronous call waits for its committed result. Cold replay resumes
the original call without admitting it again. `NexusServiceOptions` selects a
target namespace, idempotency key, async mode or acceptance-only waiting. Async
and acceptance-only calls return the original admission, even when a terminal
result appears later. Committed failures use `Error::NexusOperationFailed`.
Calls currently run in the root workflow scope.

Enable `Worker::nexus_cooperative_cancellation(true)` for cooperative service
waits. It enables ordinary cooperation and requires the installed Server to
advertise protocol 1.20, both cancellation capabilities and a `workflow_run`
binding. Cancellation reaches the original synchronous completion wait after
owned target cleanup, preserving the original request and cleanup deadline.
Use a cancellation shield for durable cleanup. See
[`examples/nexus_cooperation.rs`](examples/nexus_cooperation.rs) for caller and
target workers. Ordinary cooperative workers keep their existing registration
behavior when Nexus cooperation is disabled.

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
cargo test --locked --all-targets --all-features
cargo doc --locked --all-features --no-deps
cargo package --locked
```

The committed lockfile records the dependency graph used by CI and packaging.
Fresh-consumer checks resolve dependencies independently to verify the published
crate also builds for new consumers. Release verification runs tooling pinned to
the workflow commit against a separate immutable source tag. For older tags that
ignored the lockfile, it restores only the missing lock from the checksum-verified
published crate, then requires the complete regenerated archive to match.

Replay and codec defects require a minimal regression fixture. See
[`CONTRIBUTING.md`](https://github.com/durable-workflow/sdk-rust/blob/main/CONTRIBUTING.md) for the corpus rules.

## License

Durable Workflow Rust SDK is released under the [MIT License](https://github.com/durable-workflow/sdk-rust/blob/main/LICENSE).
