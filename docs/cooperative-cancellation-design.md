# Cooperative cancellation source design

This document describes the unfinished source candidate for shared cancellation
issue 136. Default worker protocol remains 1.19. Cooperation requires explicit
protocol 1.20 opt-in and a compatible Server and Native backend. The shared
specification and published mixed-language qualification remain pending.

## Recorded context

`WorkflowContext::cancellation_context()` returns the original immutable metadata
after committed authored cancellation delivery. Earlier workflow code receives
`None`. The existing `Error::CooperativeCancellationRequested` carries the same
snapshot in `request.context`. Poll and heartbeat observations do not supply
workflow-facing context, even when their transport object contains extra fields.

`CancellationContext::remaining()` returns a `Result<Duration>` from the accepted
cleanup deadline, bounded by the original global deadline, and the recorded
blocking boundary consumed by this workflow replay. A scoped child can have an
earlier authority ceiling.
Committed delivery starts the clock. Activity/child results, timer fires, signal
and condition resolutions advance it. Parallel groups use their consumed
members and exclude results later than the failure returned to workflow code.
Selection uses its committed winner marker, then the result or first cancellation
receipt of a handle that workflow code actually awaits. Synchronous side effects,
version markers, memo and search attributes preserve the same budget before and
after persistence. Recorded clock skew cannot increase the budget, microseconds
are preserved, and expiry returns zero.

The helper reads no host clock and cannot renew a deadline. Contexts restored
from portable metadata, ended or unrelated workflow replays, and missing or
invalid committed timestamps return an explicit error. A weak replay binding
does not keep workflow history alive. The Server independently enforces the
original deadline while a worker is suspended or absent.

`CancellationContext` and `CancellationLineage` expose read-only accessors.
Metadata includes local and root request IDs, root workflow instance and run IDs,
the immediate parent request ID, original reason, requester/source, root request
time, original deadline and ordered lineage. Requester fields are limited to
caller type, ID and label. Timestamp helpers return immutable UTC dates.
`to_value()` produces a detached portable snapshot.

A scoped child context preserves its complete immutable `scope_origin` alongside
the local run lineage. `ScopedCancellationContext::root_context()` retains the
original root deadline. Its scoped lineage records each run, instance, scope,
request identity and accepted authority ceiling, including intermediate scopes
in the same run. The child deadline can narrow that ceiling and never extend it.
Legacy version 1 snapshots remain readable. Version 2 parsing rejects altered
root metadata, substituted ancestry and widened budgets. This metadata support
does not advertise scoped execution while that source contract is unfinished.

A child keeps the root request time and cleanup budget even if its local request
is accepted later. Cold replay restores the object from canonical request history.
Explicit checks and shielded cleanup retain it. The parser validates local
request/run binding, cycles, budget and delivery snapshot equality. Older
cancellation histories without rich context keep their existing delivery behavior
with `context == None`.

## Child policies

`ChildWorkflowOptions` accepts `CancellationPolicy` through its builder for
ordinary, parallel and selected child calls.

```rust
ChildWorkflowOptions::new("python-workers")
    .cancellation_policy(CancellationPolicy::WaitCancellationCompleted)
    .parent_close_policy(ParentClosePolicy::RequestCancellation)
```

`TryCancel` requests child cleanup and delivers parent cancellation without
waiting. `WaitCancellationCompleted` parks the parent until the child reaches a
recorded terminal outcome, releasing its task claim for other work. `Abandon`
leaves the child independent and preserves the historical default. Parent
closure remains separate. `RequestCancellation` requests genuine cooperative
cleanup with the original lineage and budget. `RequestCancel` retains its
legacy terminal behavior.

Cold replay compares both policies, including cancellation delivery and groups.
Omitted historical fields preserve the `Abandon` defaults. Later events with
missing fields retain the scheduled snapshot. Invalid or conflicting policy
history fails explicitly. A worker without the cooperation opt-in refuses the
commands before completion with its identity and required protocol. Server also
checks the immutable task claim and installed backend.

## Remote Activity policies

`ActivityOptions::cancellation_policy()` accepts the same `CancellationPolicy`
enum for ordinary, parallel and selection Activity calls. Omission retains the
historical Try behavior and wire shape.

```rust
ActivityOptions::new()
    .cancellation_policy(CancellationPolicy::WaitCancellationCompleted)
```

Try requests cancellation and continues without waiting for the stop receipt.
Wait delays workflow delivery until the original remote attempt's callback drop
is durably acknowledged. Abandon leaves work independent after parent
cancellation. It requires a finite positive `schedule_to_close_timeout` and
keeps that original deadline as its total lifetime. It does not extend the
parent's cleanup budget.

The opted-in managed worker runs at most `max_concurrent_activity_tasks`
callbacks concurrently and joins all slots before deregistration. Reserve an
available activity slot for remote cleanup when independent Abandon work uses
the same worker. Workflow polling continues independently. Default protocol
1.19 workers retain their existing serial activity loop.

Explicit policies require cooperative worker opt-in, protocol 1.20 and a
compatible installed backend. Unsupported workers identify their operation,
identity and required protocol before submission. Server also checks the
original immutable claim. Replay compares the policy against canonical history
before ordinary/group/selection settlement or cancellation delivery. Later
events with missing fields retain the original policy. Invalid, conflicting
and changed history fails explicitly. Historical omission retains Try.

Connected Source cases include explicit Try/Wait callback drop without app
heartbeats, receipt ordering for Wait and stale-result refusal. Bounded Abandon
checks that the callback future survives parent closure, commits independent
completion under its original total timeout and cannot publish a second outcome
or reopen the parent. These tests supervise async callback futures. Detached
threads, processes and downstream effects need their own cooperation and fencing.

## Remote callback-stop receipts

The managed worker drops its activity callback future before reporting a stop.
Cloned `ActivityContext` values remain fenced. Only a canonical cancellation
observation on the original task, attempt and owner supplies the local request ID
for `Client::acknowledge_activity_cancellation()`. A lost lease, transport error,
shutdown or malformed observation does not supply a cooperative stop receipt.
Callbacks that never started are not reported as dropped.

The explicit protocol 1.20 receipt request has a five-second budget. It validates
the Server's original claim, request and history receipt. A failed acknowledgment
remains an error and cannot become a completion or a new cleanup budget. A
duplicate returns the original event. The report covers the managed callback
future. Detached threads, processes and downstream effects need their own
cooperating cancellation and fencing.

Connected source qualification accepts an optional exact `native_commit` in
addition to `server_commit`. The Native checkout is mounted read-only while the
image retains its published Composer authority. The source lane verifies the
durable receipt for callbacks with and without application heartbeats,
duplicates, the original deadline and stale result refusal. Actions retains all
three source identities and scenario history. This does not qualify a published
Native image or package.

## Rust API release boundary

The candidate adds a variant to the existing exhaustive `ParentClosePolicy`
enum and fields to the public `ChildWorkflowOptions` and `ActivityOptions`
structs. It also adds a total-lifetime error category to the exhaustive public
`ActivityOptionsErrorKind` enum. Existing struct
literals and exhaustive matches need updates. The cooperative error variants
also extend an existing exhaustive public enum. The maintainer approved Rust
SDK 3.0.0 in the [October 3 release decision](https://github.com/durable-workflow/sdk-rust/pull/55#issuecomment-5966048960).
The [migration and release proposal](migrating-to-v3.md) covers these source
changes, historical defaults, upgrade order and rollback. Approval resolves the
major-version decision. Source qualification, protocol freeze and the published
mixed-language cancellation acceptance scenario remain release gates. The
candidate does not change a published artifact.

## Candidate scoped delivery

The private scope opt-ins consume scalar calls and flat or nested `parallel`
groups of remote activities, timers, children and conditions. The complete group
is checked against its original definitions, policies, member addresses and paths
before workflow code receives `CancellationScopeRequested`. Earlier completed
members do not hide pending siblings. A committed condition is interrupted
without evaluating its predicate. Replacement replay retains the original group
span, cleanup sequence, context and deadline.

Shielded cleanup timers carry only the original scope, request and delivery
references. Server derives their immutable authority and rejects a timer that
would reach its ceiling. Unsupported local, signal, selection, incomplete or
mixed-scope groups are refused before the workflow factory runs. Descendant
delivery and root/scope composition still require qualification. These opt-ins
remain disabled by default and do not advertise general scoped execution.

## Remaining qualification

Complete the remaining scoped consumers, competitive qualification and exact
published artifacts. Do not subtract the host clock from the deadline in workflow code.
The runtime continues enforcing the original deadline and fencing task and
activity ownership. Rust local activities and worker affinity remain unsupported.

Connected qualification must cover the PHP parent, Python child, Rust remote
activity and PHP local activity together. Both callbacks must stop without
application heartbeats. A replacement after SIGKILL during cleanup must replay
the same boundary and finish before the original 30-second deadline. Record
supported workflow lease, heartbeat and repair settings with that scenario.
Exact published artifacts and one cascade inspection view remain required before
release claims.
