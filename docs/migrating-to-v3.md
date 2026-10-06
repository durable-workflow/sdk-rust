# Rust SDK 3.0 migration and release proposal

Rust SDK 3.0.0 is the approved major version for cooperative cancellation. This
document describes the source candidate. Publish it after the reviewed portable
contract is frozen and the release qualifications below pass.

## Source changes

The minimum supported Rust version remains 1.86. Existing workflow payloads use
the same Avro encoding. Protocol 1.19 remains the default, and cooperative
execution requires explicit protocol 1.20 support on the Server and worker.

### Activity options

`ActivityOptions` adds `cancellation_policy: Option<CancellationPolicy>`.
Applications using a complete struct literal must add `cancellation_policy:
None` or use `ActivityOptions::new()` and the existing builders.

`None` preserves historical TryCancel behavior and the old command shape.
Explicit TryCancel or WaitCancellationCompleted requires cooperative support.
Abandon also requires a finite positive `schedule_to_close_timeout`.

```rust
use durable_workflow::{ActivityOptions, CancellationPolicy};
use std::time::Duration;

let historical = ActivityOptions::new();
let wait_for_stop = ActivityOptions::new()
    .cancellation_policy(CancellationPolicy::WaitCancellationCompleted);
let independently_bounded = ActivityOptions::new()
    .schedule_to_close_timeout(Duration::from_secs(60))
    .cancellation_policy(CancellationPolicy::Abandon);
```

### Child options and parent closure

`ChildWorkflowOptions` adds `cancellation_policy: CancellationPolicy`. Complete
struct literals must add `CancellationPolicy::Abandon`, the historical default,
or use `ChildWorkflowOptions::new(task_queue)` and its builders.

`ParentClosePolicy` adds `RequestCancellation`. Update exhaustive matches to
handle it. `RequestCancel` keeps its existing terminal semantics.

```rust
use durable_workflow::{CancellationPolicy, ChildWorkflowOptions, ParentClosePolicy};

let historical = ChildWorkflowOptions::new("child-workers");
let cooperative = ChildWorkflowOptions::new("child-workers")
    .cancellation_policy(CancellationPolicy::WaitCancellationCompleted)
    .parent_close_policy(ParentClosePolicy::RequestCancellation);
```

Operation cancellation and parent closure are separate decisions. A child
using WaitCancellationCompleted participates in cooperative cancellation at the
awaiting operation. RequestCancellation propagates a cooperative request when
the parent closes. Both retain the original root lineage and budget.

### Errors and request APIs

Update exhaustive matches on `ActivityOptionsErrorKind` for
`MissingTotalTimeout`. Update exhaustive matches on `Error` for
`CooperativeCancellationRequested`, `CooperativeCancellationUnavailable`,
`InvalidCooperativeCancellation` and `ActivityExecutionAbandoned`.

Propagate cooperative workflow cancellation through the SDK's authored cleanup
path. Do not turn it into an ordinary workflow failure or publish an abandoned
Activity result. Capability and malformed-authority errors remain errors.

Use `WorkflowHandle::request_cancellation()` for bounded cooperative cleanup, or
`request_selected_run_cancellation()` for an explicit run. The corresponding
Client methods are `request_workflow_cancellation()` and
`request_workflow_run_cancellation()`. Existing `cancel()` and
`cancel_selected_run()` remain terminal cancellation.

`Worker::cooperative_cancellation(true)` opts into managed cooperation. Runtime
discovery and the immutable issued claim must support it. Unsupported capability
is an explicit diagnostic, never a silent conversion to terminal cancellation.

## Existing runs and deployment order

Omitted historical policies retain Activity TryCancel and child Abandon. Keep
those authored defaults while replaying existing runs. Changing a policy on an
already recorded operation is a replay mismatch. Introduce changed policies
through a new workflow type or the product's documented workflow versioning.

1. Retain the old worker artifact and a tested recovery path. Qualify the exact
   Server/Native upgrade with retained ordinary histories before deployment.
2. Upgrade workers without changing their authored historical defaults. Verify
   ordinary published workflow execution and replay on the selected tuple.
3. Enable cooperation only after Server/Native discovery, protocol 1.20 and the
   installed capability checks agree. Route new cooperative workflows to workers
   that can replay their cancellation and operation-policy histories.
4. Inspect the cascade API/UI and durable cleanup outcomes before expanding use.

Downgrading to 2.x is safe only for histories it can replay. Older workers cannot
take over runs requiring the new cooperative contract. Keep a compatible worker
available to finish those runs or use the documented recovery procedure before
rolling back a deployment. Do not delete history to make a downgrade appear safe.

## Qualification and publication

Release 3.0.0 only after current-source Rust 1.86/stable, replay corpus, official
Avro, package and documentation gates pass, nested-scope work is complete and the
portable contract is frozen. Qualify supported/default lease and repair settings.

Publish an exact compatible tuple, then run the complete PHP parent, Python child,
Rust remote Activity and PHP local Activity scenario. Verify one root identity,
the original +30-second deadline, cooperative child delivery, both managed
callbacks stopped without application heartbeats, stale publication fenced,
actual workflow-process SIGKILL during cleanup, replacement replay of the same
boundary and consumed budget, duplicate identity/deadline, both runs Cancelled
before the original deadline and one coherent API/UI cascade.

Verify crates.io availability, source provenance, archive checksum and a fresh
Rust 1.86 consumer. Update the published examples and documentation to that exact
qualified artifact. Shared cancellation issue 136 stays open until its complete
customer outcome and competitive qualification are demonstrated.

Managed async callback supervision does not undo external effects or stop
detached threads and processes. Cooperating downstream systems still need
idempotency and fencing. Rust local Activities and worker affinity remain outside
this release's supported cancellation surfaces.
