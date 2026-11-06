# Additive replay clock: Rust source cohorts

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
| Scalar child result | One schedule, optional start and unique parent `ChildRunCompleted` / `ChildRunFailed` / `ChildRunCancelled` / `ChildRunTerminated`, with matching nonempty child call, instance, run and type identities | Parent resolution event time for both send and throw; child `closed_at` and `reused_recorded_at` are not this branch's clock authority |
| Scalar persisted signal delivery | One `SignalWaitOpened` followed by a unique `SignalApplied`, matching nonempty wait ID/name and explicit command sequence, with the persisted value consumed. A configured timeout requires valid numeric metadata and either no scheduled timer or the exact ordered schedule/cancel pair | Applied event time, preserving valid `reused_recorded_at` exactly as Core signal resume does |
| Snapshot query | Immutable inspected prefix whose final boundary has the same validated activity/timer/child/signal correlation, or explicit redrive start | Repeated reads of that prefix are equal; unknown/invalid final boundary is unavailable |
| Replayed-instance query | Clock of the actual reconstructed workflow prefix, rather than a later visible row | Same supported boundary rules |
| Direct update handler | Unique committed `UpdateAccepted` correlated to the handler's update ID | That event's validated time; missing/ambiguous acceptance is unavailable |
| Executor condition predicate | Current executor exposes a full signal/update view without retaining the exact triggering consumed prefix | Unavailable inside the predicate; enclosing workflow scope is restored on return/error |
| Inline side effect/version/memo/search/local result | User code can resume before its event is persisted | Unavailable on first execution and the corresponding cold path in this cohort; no implicit marker/yield/fence |
| Condition result, signal timeout result, grouped/selection/Nexus or cancellation callback | Their dedicated clock algorithms are outside this initial cohort | Explicitly unavailable; existing execution/cancellation behavior remains intact |

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

The scalar child cohort follows Core's persisted branches at
[`4b2cdc55` `WorkflowExecutor::resumeChildResolution`](https://github.com/durable-workflow/workflow/blob/4b2cdc5507120d9c213b0b02a85738bec96c574a/src/V2/Support/WorkflowExecutor.php#L1638)
and the corresponding `QueryStateReplayer` branch. Both completion and failure
resume at the selected parent resolution event's own `recorded_at`. Unlike the
successful activity/timer reuse path, they do not select `reused_recorded_at`.
The child row's `closed_at` is not a substitute. Rust consumes the existing
validated scalar command before advancing this clock. Identical duplicate
terminal payloads, missing/conflicting lifecycle identities, changing-run
retry/continuation lineage, out-of-order lifecycle or invalid/missing time keep
business time unavailable without changing legacy result/failure behavior.
A schedule without a resolution stays pending and does not advance time.

Focused child controls exercise actual calls and caught terminal failures,
unchanged-prefix and cold replay, backward/precise times, future-row isolation,
identity/time refusals, snapshot/reconstructed query prefixes and interleaved
poll/error scope restoration. The separate child corpus consumer runs unchanged
against base and candidate production code, retaining an exact behavioral
negative rather than a compilation failure.

The scalar persisted-signal cohort follows Core
[`4b2cdc55` `WorkflowExecutor::resumeSignal`](https://github.com/durable-workflow/workflow/blob/4b2cdc5507120d9c213b0b02a85738bec96c574a/src/V2/Support/WorkflowExecutor.php#L1434),
which supplies `RedriveReplayClock::recordedAt` for the matched `SignalApplied`.
Rust consumes that recorded wait by command sequence and signal name. Business
time additionally requires matching nonempty persisted wait IDs and explicit
numeric command counters, the canonical persisted value, one ordered open/apply
pair. Missing/conflicting identity, duplicate/order
ambiguity or invalid/missing time leaves time unavailable while
preserving existing signal results, commands and cancellation behavior.

A visible `SignalReceived` is not application. Neither an unconsumed future row
nor task-resume arguments without the committed applied carrier advance this
clock. Core query reconstruction has a separate committed received-event legacy
fallback; this cohort does not invent that authority for Rust or read a mutable
projection as persisted time. No new observation envelope, event, wire field,
marker, RPC or inline persistence fence is introduced. Reconstructed/snapshot
queries retain the existing actually consumed/inspected prefix rules.

Successful scalar timed waits follow the same consumed applied boundary. At
[Core `ce24207e`](https://github.com/durable-workflow/workflow/blob/ce24207ef86856e64f97b939c9aeef9d1fb387c8/src/V2/Support/WorkflowExecutor.php#L1341),
a signal already pending at the call may win even a zero timeout without a
timer. A parked wait instead records open, schedule, cancel, then apply.
[`TimerCancellation::record`](https://github.com/durable-workflow/workflow/blob/ce24207ef86856e64f97b939c9aeef9d1fb387c8/src/V2/Support/TimerCancellation.php#L55)
preserves the scheduled timeout's ID, kind, wait/name, sequence, delay and fire
time. Rust requires that complete ordered identity, consistent numeric timeout
and valid cancellation metadata. Timer timestamps never replace the selected
applied event's time. Missing, mismatched, duplicate, fired, grouped or otherwise
ambiguous timeout evidence leaves business time unavailable while preserving
legacy signal results. A timeout-only history remains outside this cohort, as do
task-only and received-event resume paths. No timeout API or wire field is added.
