# Durable Workflow Rust SDK

`durable-workflow` is the first-party Rust SDK for Durable Workflow workers
and clients. It can register workflow and activity handlers, long-poll the
worker protocol, start, signal, query, cancel, terminate, and await workflow
executions, start and await durable child workflows, expose named read-only
query handlers, heartbeat
workers and activities, and exchange JSON-native payloads through the
platform's generic Avro wrapper. Workflow code can also wait on server-backed
durable time, capture non-deterministic values exactly once, and evolve across
deployments with durable version markers.

## Install

Add the exact crates.io release with Cargo:

```sh
cargo add durable-workflow@=2.0.0-beta.4
```

Or add the same exact requirement directly to `Cargo.toml`:

```toml
[dependencies]
durable-workflow = "=2.0.0-beta.4"
```

Version `2.0.0-beta.4` requires Rust `1.86` or newer and includes the complete
Durable Workflow 2.0 beta baseline described below.

## Compatibility

Rust SDK `2.0.0-beta.4` is supported with server `2.0.0-beta.4`, control plane
`2`, and the server's additive worker-protocol negotiation window. Earlier
crate versions remain historical and are not separate supported feature
levels. No compatibility shim connects earlier 2.0 prereleases to this train.

The machine-readable values live in `[package.metadata.durable-workflow]` in
`Cargo.toml` as `supported-server-versions`, `worker-protocol-version`, and
`control-plane-version`. Query-capable releases also publish `query-tasks`,
`query-task-minimum-worker-protocol-version`, `replayed-instance-state-queries`,
`query-state-model`, `snapshot-inspection-queries`, and `payload-codecs`.
Timer-capable releases additionally publish `durable-timers`, `timer-command`,
and `timer-replay-validation`. Child-capable releases additionally publish
`child-workflows`, `child-workflow-command`, and
`child-workflow-failure-reasons`. Activity-options releases publish
`activity-options`, `activity-retry-policy`, `activity-timeouts`, and
`activity-failure-reasons`. Lifecycle releases publish
`workflow-lifecycle-commands`, `workflow-lifecycle-run-targeting`, and
`workflow-terminal-outcomes`; releases with start deadline support also publish
`workflow-start-timeouts`. Existing worker operations retain the `1.2`
baseline; only query-task poll, complete, and fail requests use the additive
`1.8` feature floor. The server's advertised protocol manifests remain
authoritative when checking compatibility during deployment.

Side-effect releases additionally publish `deterministic-side-effects`,
`side-effect-command`, and `side-effect-history-event`. Version-marker releases
publish `version-markers`, `version-marker-command`,
`version-marker-history-event`, and `version-marker-helpers`.
Continue-as-new releases publish `continue-as-new`,
`continue-as-new-command`, `continue-as-new-routing-overrides`,
`workflow-history-budget`, and `workflow-result-routing`.

## Deterministic side effects and UUIDs

`WorkflowContext::side_effect` evaluates its callback only when no matching
`SideEffectRecorded` value exists at the current durable command sequence. It
emits exactly one `record_side_effect` command with the workflow task's selected
Avro or JSON envelope. Cold replay decodes the recorded value into the requested
Rust type without invoking the callback.

```rust
# use durable_workflow::{json, Client, Result, Worker};
# fn read_external_exchange_rate() -> String { "1.25".to_string() }
# fn configure(client: Client) {
let mut worker = Worker::new(client, "billing-workers");
worker.register_workflow("price-invoice", |ctx, _input| async move {
    let request_id = ctx.uuid_v4()?.to_string();
    let exchange_rate: String = ctx.side_effect(read_external_exchange_rate)?;
    Ok(json!({
        "request_id": request_id,
        "exchange_rate": exchange_rate,
    }))
});
# }
```

Use side effects only for small value capture. Calls that need retries,
timeouts, cancellation, or observable external work remain activities. A
missing, duplicate, malformed, reordered, codec-incompatible, or Rust
type-incompatible recorded value returns `Error::NonDeterministicReplay` with
stable `ReplayFailure` fields.

## Version markers and staged code evolution

`get_version(change_id, min_supported, max_supported)` chooses
`max_supported` for a new run. After the server commits the marker, every
worker restart and later code deployment reads that exact version. Repeating
the same change ID in one execution returns the cached choice and never emits a
second marker.

Start a rollout by preserving both branches:

```rust
# use durable_workflow::{json, Result, WorkflowContext, Value};
# async fn rollout(ctx: WorkflowContext) -> Result<Value> {
let version = ctx.get_version("invoice-tax-v2", 1, 2)?;
if version == 1 {
    Ok(json!({"calculation": "legacy"}))
} else {
    Ok(json!({"calculation": "v2"}))
}
# }
```

Once all version `1` runs have drained, a later deployment can require
`get_version("invoice-tax-v2", 2, 2)`. A still-running version `1` history is
then rejected deterministically instead of silently taking new code. For a
boolean rollout, `patched(change_id)` uses the standard `-1` legacy / `1`
patched range. After removing the old branch, call `deprecate_patch(change_id)`
at the same location to keep existing histories replay-compatible.

## Durable timers

`WorkflowContext::sleep` waits on server-backed durable wall time. It emits a
`start_timer` workflow command and yields the workflow future; it never calls
`tokio::time::sleep` or keeps a process-local deadline. Durations are rounded
up to whole seconds, so a timer cannot be scheduled earlier than requested.

`WorkflowContext::start_timer` is an alias for `sleep`. On every workflow task,
the SDK reconstructs one shared sequence-ordered command stream for activities,
timers, and signal waits. A replayed timer must have a matching
`TimerScheduled` event, and it resolves only when the same sequence and timer
identity has one `TimerFired` event. The recorded delay must equal the current
workflow call. Worker and server restarts therefore preserve the original
deadline, while replay and repeated polling do not append or consume the timer
twice. Changed command order, changed delay, unpaired events, duplicate durable
sequences, and duplicate fires return `Error::NonDeterministicReplay`; its
`ReplayFailure` exposes stable `reason`, `sequence`, `expected`, and `actual`
fields. Durations too large to round up without shortening the requested wait
return `Error::TimerDurationOverflow` without emitting a command.

```rust
# use std::time::Duration;
# use durable_workflow::{json, Client, Worker};
# fn configure(client: Client) {
let mut worker = Worker::new(client, "reminder-workers");
worker.register_workflow("send-reminder", |ctx, _input| async move {
    ctx.sleep(Duration::from_secs(30)).await?;
    let receipt = ctx.activity("deliver-reminder", json!([])).await?;
    Ok(receipt)
});
# }
```

Use `WorkflowContext::sleep` only inside workflow code. Ordinary
`tokio::time::sleep` remains appropriate for worker-process concerns such as
local polling or shutdown coordination, but it is not durable workflow state.

Running and completed executions remain available through `WorkflowHandle`'s
`describe`, `query`, and `result` methods. Server protocol incompatibilities
remain `Error::Protocol(ProtocolFailure)`, including stable `reason`, `status`,
and requested/supported version fields. Activity settlement rejections are
typed `Error::ActivityTaskRejected`; other rejected worker requests remain
`Error::Http` values with the response status and body.

## Bounded workflows with continue-as-new

A workflow ID identifies the stable public workflow instance. Each
continue-as-new transition closes one run and creates exactly one successor
with a new run ID and fresh run-timeout budget. The server carries namespace,
execution timeout, memo, search attributes, and other instance-owned metadata.
Workflow code supplies only successor arguments and optional workflow-type or
task-queue overrides.

```rust
# use durable_workflow::{json, Client, ContinueAsNewOptions, Value, Worker};
# fn configure(client: Client) {
let mut worker = Worker::new(client, "invoice-workers");
worker.register_workflow("invoice-sweep", |ctx, input| async move {
    let next = input.get(0).and_then(Value::as_u64).unwrap_or(0);
    let stop = input.get(1).and_then(Value::as_u64).unwrap_or(10_000);
    let chunk_end = next.saturating_add(100).min(stop);

    for invoice in next..chunk_end {
        ctx.activity("invoice-one", json!([invoice])).await?;
    }

    if chunk_end < stop {
        let budget = ctx.history_budget()?;
        // Fixed chunks bound history; an adaptive workflow can transition
        // earlier whenever the server recommendation becomes true.
        if budget.continue_as_new_recommended || chunk_end - next >= 100 {
            return ctx.continue_as_new_with_options(
                ContinueAsNewOptions::new().task_queue("invoice-workers"),
                json!([chunk_end, stop]),
            );
        }
    }

    Ok(json!({"processed": stop}))
});
# }
```

`WorkflowHandle::describe`, `signal`, `query`, and `result` resolve the current
run, so a handle created for the first run continues to operate across the
chain. `result` returns the final successful value or a typed terminal error
from the final run. Use `describe_selected_run`, `signal_selected_run`,
`query_selected_run`, or `result_selected_run` when the handle's original run
identity is intentional. Selected-run commands are rejected once that run is
historical; selected description and result remain available for inspection.

## Workflow cancellation, termination, and outcomes

Cancellation is cooperative. Use it when workflow and activity code should run
cleanup; use termination only as a forced operator stop. Instance-targeted
commands resolve the server's current run at command time:

```rust
# use durable_workflow::{Client, Result, WorkflowCommandOptions};
# async fn stop(client: &Client) -> Result<()> {
client.cancel_workflow(
    "order-42",
    WorkflowCommandOptions::new()
        .reason("customer withdrew the order")
        .request_id("cancel-order-42"),
).await?;
# Ok(())
# }
```

A `WorkflowHandle` offers the same `cancel` and `terminate` methods. When the
handle's original run identity matters, use `cancel_selected_run` or
`terminate_selected_run`. These call the run-targeted endpoint and fail with
`Error::WorkflowCommandRejected(WorkflowCommandRejection)` if that run is no
longer current. Automation can match `reason ==
"historical_run_command_rejected"`; the rejection also retains `workflow_id`,
`run_id`, `target_scope`, HTTP status, and the original response body.

`WorkflowHandle::result` returns the decoded `Value` on success. Other terminal
states are distinct typed errors. Handles returned by `start_workflow` carry
the initial run ID for explicit historical inspection; `result` follows the
instance's current run and `result_selected_run` awaits only that initial run:

```rust
# use durable_workflow::{Error, Result, WorkflowHandle, WorkflowResultOptions};
# async fn wait(handle: WorkflowHandle) -> Result<()> {
match handle.result(WorkflowResultOptions::default()).await {
    Ok(value) => println!("completed: {value}"),
    Err(Error::WorkflowCancelled(outcome)) => {
        println!("cancelled {} / {:?}: {}", outcome.workflow_id, outcome.run_id, outcome.reason);
    }
    Err(Error::WorkflowTerminated(outcome)) => {
        println!("terminated: {}", outcome.reason);
    }
    Err(Error::WorkflowFailed(outcome)) => {
        println!("failure {:?}: {:?}", outcome.failure_id, outcome.exception_class);
    }
    Err(Error::WorkflowTimedOut(outcome)) => {
        println!("timeout category: {:?}", outcome.failure_category);
    }
    Err(error) => return Err(error),
}
# Ok(())
# }
```

Every `WorkflowTerminalOutcome` carries workflow/run identity and a stable
kind and reason. It also exposes failure category and identity, exception type
and class, non-retryable state, message, exception payload, and the raw
description whenever the server supplies them. A local result-wait deadline
uses the same typed timeout with reason `result_wait_timeout` and category
`client_timeout`; it is distinguishable from a server-terminal `timed_out`
run without parsing display text.

Use `Client::start_workflow_with_options` when the server, rather than the
caller, must enforce a workflow deadline:

```rust
# use durable_workflow::{json, Client, Result, WorkflowStartOptions};
# async fn start(client: Client) -> Result<()> {
let handle = client.start_workflow_with_options(
    "orders.await-payment",
    "orders",
    "order-42",
    WorkflowStartOptions::new()
        .execution_timeout_seconds(300)
        .run_timeout_seconds(30),
    json!([]),
).await?;
# let _ = handle;
# Ok(())
# }
```

Both values must be positive and the run timeout cannot exceed the execution
timeout. The existing `start_workflow` method retains its 3600-second execution
and 600-second run defaults.

## Durable activity options

`WorkflowContext::activity_with_options` adds routing, durable retries, and
server-enforced timeouts while the existing `activity` and `activity_on_queue`
convenience methods remain unchanged. `ActivityRetryPolicy` accepts explicit
backoff intervals or generates integer exponential intervals. All options are
recorded on the single `schedule_activity` command; transport retry settings
on `Worker` and `Client` are separate.

```rust
# use durable_workflow::{json, ActivityOptions, ActivityRetryPolicy, Error, Result, WorkflowContext};
# use std::time::Duration;
# async fn charge(ctx: WorkflowContext) -> Result<durable_workflow::Value> {
let options = ActivityOptions::new()
    .task_queue("payments")
    .retry_policy(
        ActivityRetryPolicy::new(4)
            .exponential_backoff(Duration::from_secs(1), 2, Some(Duration::from_secs(30)))
            .non_retryable_error_type("PaymentDeclined"),
    )
    .start_to_close_timeout(Duration::from_secs(60))
    .schedule_to_start_timeout(Duration::from_secs(10))
    .schedule_to_close_timeout(Duration::from_secs(180))
    .heartbeat_timeout(Duration::from_secs(15));

match ctx
    .activity_with_options("charge-card", options, json!([{"order_id": "order-42"}]))
    .await
{
    Ok(receipt) => Ok(receipt),
    Err(Error::ActivityFailed(failure)) => Ok(json!({
        "kind": format!("{:?}", failure.kind),
        "reason": failure.reason,
        "category": failure.failure_category,
        "activity_execution_id": failure.activity_execution_id,
        "timeout_kind": failure.timeout_kind,
    })),
    Err(error) => Err(error),
}
# }
```

Timeouts use `Duration` and round up to whole protocol seconds.
`start_to_close_timeout` limits one attempt, `schedule_to_start_timeout` limits
queue wait, `schedule_to_close_timeout` includes all attempts and retry
backoff, and `heartbeat_timeout` limits the gap between heartbeats. Invalid
positive values, ordering, retry bounds, blank error types, and empty policies
return `Error::InvalidActivityOptions(ActivityOptionsError)` before any command
is emitted.

Completed activities still return their decoded value. Terminal
`ActivityFailed`, `ActivityCancelled`, and `ActivityTimedOut` history returns
`Error::ActivityFailed(ActivityFailure)`. Match `ActivityFailureKind` and the
stable `reason`, `failure_category`, and `timeout_kind` fields; durable activity
and attempt identities are retained when the server provides them. A retry
history with no terminal event remains pending during replay and does not emit
a second activity schedule after a worker restart.

## Child workflows

`WorkflowContext::start_child_workflow` records a named child on a mandatory,
explicit task queue and waits for a terminal `ChildRun*` history event. The
same history is replayed after a Rust worker restart: a committed result is
decoded using its recorded payload codec and no second child command is
emitted. A recorded child that has not settled remains pending without another
start command on redelivery. This also permits a Rust parent to call a PHP or
Python child (or the reverse) without changing payload shape.

```rust
# use durable_workflow::{json, ChildWorkflowOptions, ParentClosePolicy, Result, WorkflowContext};
# async fn parent(ctx: WorkflowContext) -> Result<durable_workflow::Value> {
let child = ctx
    .start_child_workflow(
        "python.fulfil-order",
        ChildWorkflowOptions::new("python-workers")
            .parent_close_policy(ParentClosePolicy::RequestCancel)
            .execution_timeout_seconds(600)
            .run_timeout_seconds(120),
        json!([{"order_id": "order-42"}]),
    )
    .await?;

assert_eq!(child.parent, ctx.workflow_identity()?);
println!("child workflow={:?} run={:?}", child.child.workflow_id, child.child.run_id);
Ok(child.result)
# }
```

Success returns `ChildWorkflowResult`, including parent and child workflow/run
identities. Failure, cancellation, and termination return
`Error::ChildWorkflowFailed(ChildWorkflowFailure)` inside workflow code. Match
its `kind` or stable `reason` (`child_workflow`, `cancelled`, or `terminated`),
not its message. An uncaught error becomes a durable `fail_workflow` command
whose structured exception retains those fields.

`ParentClosePolicy::Abandon` leaves an open child running when the parent
closes. `RequestCancel` requests child cancellation, and `Terminate` closes it
immediately. Retry and timeout options are recorded server-side with the child
call; they are not SDK HTTP retry limits.

## Worker

```rust,no_run
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
        let name = args
            .as_array()
            .and_then(|arguments| arguments.first())
            .and_then(|value| value.as_str())
            .unwrap_or("world");
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
acknowledgements for metrics or structured logging. After each heartbeat
attempt and its bounded retries finish, the worker waits for a complete
advertised interval before sending the next heartbeat. Delayed responses
therefore do not queue catch-up requests.

Activity handlers report progress with `ActivityContext::heartbeat`. The
returned `ActivityHeartbeatResponse` exposes `heartbeat_recorded` and
`cancel_requested`, plus `can_continue`, `reason`, and the run close state, so
long-running work can stop and clean up without treating cancellation as a
transport or codec failure:

```rust
# use durable_workflow::{json, Client, Result, Worker};
# fn configure(client: Client) {
let mut worker = Worker::new(client, "rust-workers")
    .on_worker_heartbeat(|observation| {
        println!("worker heartbeat acknowledged at {}", observation.acknowledged_at_unix_millis);
    });

worker.register_activity("batch.process", |ctx, _args| async move {
    let acknowledgement = ctx.heartbeat(json!({"completed": 25})).await?;
    if acknowledgement.should_stop() {
        cleanup_temporary_files();
        return Ok(json!({"cleanup": "complete"}));
    }
    Ok(json!({"completed": 100}))
});
# }
# fn cleanup_temporary_files() {}
```

The server remains authoritative after cancellation: if the handler finishes
late, its completion is refused and cannot turn the run into success. Direct
client settlement calls return `Error::ActivityTaskRejected` with a stable
reason such as `run_cancelled`, `run_terminated`, or `stale_attempt`. The
managed worker treats these definitive late-settlement responses as terminal
for that leased attempt and continues polling, including after a worker
restart during cancellation. `Client::poll_activity_task_response` and
`Client::poll_workflow_task_response` preserve drain and poll-stop metadata for
lower-level worker integrations; `Client::poll_query_task_response` does the
same for query tasks. Call `outcome()` on any full poll response and match
`WorkerPollOutcome::Stop` instead of parsing a display string. In particular,
the server's HTTP `409` / `worker_draining` response decodes as a normal stop
outcome. Managed workers honor it by ceasing new polls and draining cleanly.

Lower-level integrations can call `Client::heartbeat_worker` and
`Client::heartbeat_activity_task` directly.

## Worker liveness and errors

Workflow, activity, and query polls advertise the configured poll timeout to
the server. An empty response at that boundary is normal: `Worker::run` and
`Worker::run_until` keep every poller and worker heartbeats running, so the same
worker can accept work after an idle period.

Replaying a workflow that is still blocked on a recorded activity, timer,
child workflow, or signal wait can also produce no new commands. The worker
acknowledges that task as waiting for scheduled history instead of submitting
an invalid empty completion. Workflow and query pollers therefore remain live,
and worker heartbeats continue while unrelated signals are recorded.

A run deadline can expire while a worker holds a workflow task. If completion
returns the authoritative conflict for that exact task, attempt, and selected
run -- `recorded=false`, `reason=run_timed_out`, and terminal
`run_status=failed` -- the managed worker considers the tick settled and keeps
polling. The rejected commands were not recorded and cannot overwrite the
terminal run. A bare HTTP 409, a different run identity, or any nearby lease,
ownership, protocol, validation, or nonterminal conflict remains an error.
`Client::complete_workflow_task` is unchanged for lower-level integrations: it
returns the HTTP status and machine-readable response body directly.

This settlement race is separate from `WorkflowResultOptions::timeout`. The
former acknowledges server-terminal state for one selected run; the latter is
only a client-side wait bound and produces `result_wait_timeout` if the run is
still nonterminal.

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
`examples/activity_options.rs` is an executable retry scenario with activity
heartbeats, a heartbeat timeout, and typed terminal failure handling.

With a Durable Workflow server running locally:

```sh
DURABLE_WORKFLOW_SERVER_URL=http://127.0.0.1:8080 \
DURABLE_WORKFLOW_TOKEN=your-token \
cargo run --example hello_world
```

Run the activity-options scenario with the same environment using
`cargo run --example activity_options`.

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
version. During the 2.0 beta, the crate advances with the synchronized product
train. After stable 2.0, fixes, additive capabilities, and breaking changes use
ordinary patch, minor, and major progression respectively.
