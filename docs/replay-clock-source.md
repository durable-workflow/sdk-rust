# Additive replay clock: first Rust source cohort

This source checkpoint implements `WorkflowContext::replay_now()` and
`QueryContext::replay_now()` toward [the cross-SDK replay-clock feature](https://github.com/durable-workflow/.github/issues/215).
It does not qualify a published package or close that feature. Existing APIs,
UUID decisions, command/history shapes and the cancellation budget clock are
unchanged. Business decisions adopt the new accessor through ordinary workflow
versioning.

The accessor returns a recorded UTC `chrono::DateTime`, or
`Error::ReplayClockUnavailable` when the current invocation has no supported,
validated time authority. It never substitutes wall time, a future history row,
a maximum over all events, a task ID or an observation timestamp. Sequential
consumption selects the matched event's time, including its precision; it does
not enforce monotonically increasing timestamps. Use stream/outbox/workflow
counter identity to order emitted events.

| Boundary | Authority in this cohort | Availability |
| --- | --- | --- |
| Redrive entry | Unique `WorkflowStarted` with `recovery_kind=redrive` and valid `replayed_started_at` | Recorded original start |
| Ordinary entry | Core uses original `WorkflowRun.started_at`; current Rust worker task has no such carrier | Unavailable, even if the start history row has an insertion timestamp |
| Scalar remote activity | Existing validated command sequence/type/result correlation, then the selected terminal event | Selected recorded time; successful redrive reuse retains `reused_recorded_at`; the existing Core failure branch uses its event time |
| Scalar plain timer | Existing validated `TimerScheduled`/`TimerFired` pair, exact sequence/ID/delay | Matched fire time, with recorded reuse provenance |
| Snapshot query | Immutable inspected prefix whose final boundary has the same validated activity/timer correlation, or explicit redrive start | Repeated reads of that prefix are equal; unknown/invalid final boundary is unavailable |
| Replayed-instance query | Clock of the actual reconstructed workflow prefix, rather than a later visible row | Same supported boundary rules |
| Direct update handler | Unique committed `UpdateAccepted` correlated to the handler's update ID | That event's validated time; missing/ambiguous acceptance is unavailable |
| Executor condition predicate | Current executor exposes a full signal/update view without retaining the exact triggering consumed prefix | Unavailable inside the predicate; enclosing workflow scope is restored on return/error |
| Inline side effect/version/memo/search/local result | User code can resume before its event is persisted | Unavailable on first execution and the corresponding cold path in this cohort; no implicit marker/yield/fence |
| Child/signal/condition result, grouped/selection/Nexus or cancellation callback | Their dedicated clock algorithms are outside this initial cohort | Explicitly unavailable; existing execution/cancellation behavior remains intact |

The mutable workflow clock is bound only during handler construction and each
future poll. A separate execution-local guard restores its predecessor on
return/error; one workflow cannot read another workflow's active clock. Query
and update contexts own an immutable engine-correlated value, with no shared
ambient mutable state. Clock reads append no commands/history.

The ordinary entry gap is [confirmed by Server](https://github.com/durable-workflow/server/issues/325#issuecomment-6671799265):
worker claims omit `run.started_at`; native start and history insertion sample
time separately. The existing describe endpoint needs exact namespace/workflow/run
binding; export summary time does not establish a worker anchor. Server owns a
later additive authoritative task carrier and its precision/missing contract.
This source does not add a wire field or extra describe/export RPC.

An advancing time immediately after a newly authored inline marker/local result
also needs an explicitly selected opt-in persistence/resume boundary. It cannot
be manufactured from the existing same-pass result. Core/Server own that
producer boundary; SDK work remains for the other already-persisted operation
families and exact predicate-prefix retention. These are distinct from the
private operating-clock work in Rust #191/#192.

Focused source controls exercise the actual Rust context, future, query/update
and predicate paths; advancing deadline/elapsed calculations; unchanged-prefix
reads; cold replay; backward/equal/precise times; redrive provenance; future-row
isolation; concurrent/nested/error scope restoration; unavailable inputs and
first-pass/cold marker handling. A retained published PHP activity history is
also consumed as source evidence, without inventing its ordinary start anchor.
Normal current-head checks remain required. Published cross-SDK conformance,
producer implementation/adoption, release and guide/localized delivery remain
separate work on the owning issue.
