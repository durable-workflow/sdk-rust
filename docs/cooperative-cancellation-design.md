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

`CancellationContext` and `CancellationLineage` expose read-only accessors.
Metadata includes local and root request IDs, root workflow instance and run IDs,
the immediate parent request ID, original reason, requester/source, root request
time, original deadline and ordered lineage. Requester fields are limited to
caller type, ID and label. Timestamp helpers return immutable UTC dates.
`to_value()` produces a detached portable snapshot.

A child keeps the root request time and cleanup budget even if its local request
is accepted later. Cold replay restores the object from canonical request history.
Explicit checks and shielded cleanup retain it. The parser validates local
request/run binding, cycles, budget and delivery snapshot equality. Older
cancellation histories without rich context keep their existing delivery behavior
with `context == None`.

## Remaining qualification

Portable operation policies, nested scopes and deterministic remaining-time
helpers still need completion. Remaining time must use the replayed workflow
clock. Do not subtract the host clock from the deadline in workflow code. The
runtime continues enforcing the original deadline and fencing task and activity
ownership. Rust local activities and worker affinity remain unsupported.

Connected qualification must cover the PHP parent, Python child, Rust remote
activity and PHP local activity together. Both callbacks must stop without
application heartbeats. A replacement after SIGKILL during cleanup must replay
the same boundary and finish before the original 30-second deadline. Record
supported workflow lease, heartbeat and repair settings with that scenario.
Exact published artifacts and one cascade inspection view remain required before
release claims.
