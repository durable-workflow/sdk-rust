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
| Scalar persisted condition outcome | One ordered canonical wait open/resolution, matching physical wait, logical occurrence, explicit command counter, key/fingerprint and timeout definition | Consumed satisfied/timed-out resolution event time, preserving its precision; timer and scalar redrive helpers are not substituted |
| Persisted remote Activity all group | Exact authored group/path and each consumed member lifecycle/type/sequence/execution identity | Core millisecond aggregate on success, full chosen precision; exact agreed failure at Core millisecond precision |
| Persisted scalar condition timeout without explicit resolution | One canonical wait and its exact ordered schedule/fire pair, with complete identity/definition/counters and unique physical timer | Executor uses the fired event’s own `recorded_at`; Core query has no corresponding resolution authority |
| Snapshot query | Immutable inspected prefix whose final boundary has the same validated activity/timer/child/signal/condition correlation, or explicit redrive start | Repeated reads of that prefix are equal; unknown/invalid final boundary is unavailable |
| Replayed-instance query | Clock of the actual reconstructed workflow prefix, rather than a later visible row | Same scalar boundary rules; grouped executor aggregation is not query authority |
| Direct update handler | Unique committed `UpdateAccepted` correlated to the handler's update ID | That event's validated time; missing/ambiguous acceptance is unavailable |
| Executor condition predicate | Current executor exposes a full signal/update view without retaining the exact triggering consumed prefix | Unavailable inside the predicate; enclosing workflow scope is restored on return/error |
| Inline side effect/version/memo/search/local result | User code can resume before its event is persisted | Unavailable on first execution and the corresponding cold path in this cohort; no implicit marker/yield/fence |
| Signal timeout result, other grouped/selection/Nexus or cancellation callback | Their dedicated clock algorithms are outside this initial cohort | Explicitly unavailable; existing execution/cancellation behavior remains intact |

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

Canonical persisted scalar condition outcomes follow Core
[`resumeConditionResolution`](https://github.com/durable-workflow/workflow/blob/ce24207ef86856e64f97b939c9aeef9d1fb387c8/src/V2/Support/WorkflowExecutor.php#L875).
The actual selected `ConditionWaitSatisfied` or `ConditionWaitTimedOut` supplies
its own recorded time. Rust's existing canonical parser must first validate the
wait and occurrence identities, predicate definition, timeout backing and result.
The clock additionally requires one ordered open/resolution pair with explicit
numeric counters and complete matching definitions; legacy fallback parsing
may still return a result when that time authority is missing. Each actual
physical resolution in a logical reopen chain advances only when consumed.

The explicit resolution branch does not borrow a timeout fire's time. Core's live timeout branch has its own timer/fired-row time
input; it is not inferred for Rust from an unrelated timer. Predicate callbacks
still have explicit unavailability inside the invocation. Synchronous local
true/zero-timeout paths preserve the already accepted previous-prefix behavior,
without inventing a new committed time. Unknown/malformed persisted resolution
time invalidates the consumed boundary instead of borrowing the previous one.
Grouped conditions and their dedicated prefix algorithms remain outside this
scalar cohort. Canonical Rust occurrence fields are existing parser authority;
this source adds no occurrence field or producer representation to Core.

Persisted parallel Activity `all` follows [Core's descriptor aggregation](https://github.com/durable-workflow/workflow/blob/ce24207ef86856e64f97b939c9aeef9d1fb387c8/src/V2/Support/WorkflowExecutor.php#L1916)
and [the accepted cross-SDK decision](https://github.com/durable-workflow/.github/issues/215#issuecomment-6677422213).
Only consumed members of that authored group participate. Each needs a unique
ordered schedule/terminal lifecycle, explicit numeric counter, exact type,
execution identity and complete matching group path. Nested all-activity paths
retain authored descriptor order. Duplicate or ambiguous evidence preserves
legacy results but cannot supply time.

Success folds members in that order, comparing Carbon milliseconds and replacing
only on a strictly greater value. The first descriptor wins an equal-millisecond
tie, retaining its full recorded microsecond precision. Each consumed success
must have represented `recorded_at` or Core's actual `created_at` fallback.
Scalar redrive reuse timestamps, a later stream row, unconsumed groups and an
unrepresented activity row never supply authority. Empty groups add no boundary;
pending groups and other operation families have no aggregate clock here.

Failure preserves Rust's existing lowest-member exception. Time is available
only when that member also wins Core's earliest-millisecond/member-index rule;
it is reconstructed with Core's millisecond precision. Conflicting multifailure
selection remains the [OPEN compatibility/versioning proposal](https://github.com/durable-workflow/.github/issues/215#issuecomment-6677866585),
without rewriting legacy outcomes. Grouped snapshot and reconstructed-query time
remain unavailable: Core's successful query group-send supplies no aggregate.
A later consumed supported scalar boundary can establish query time again.

The measured reference in `tests/fixtures/replay-clock-carbon-reference.json`
uses PHP 8.4.27 and Carbon 3.15.1 at `ce0e5636`. Its
`Timestamp.php` SHA-256 is `57b2a235656f8edad7c333087a64e45491adaaf1b01ac9c40e53185b73db356d`.
`getTimestampMs()` rounds to nearest milliseconds, with halfway values away from
zero, including negative epochs. `createFromTimestampMs()` reconstructs those
milliseconds. The clock accepts persisted microseconds within the exact float
microsecond range used by that comparator; finer or out-of-range evidence stays
unavailable. Reference vectors cover positive/negative epochs, 499/500/501
microsecond edges, equal-millisecond candidates and timezone-equivalent instants.

The persisted fired-only condition-timeout cohort follows Core
[`resumeTimedOutCondition`](https://github.com/durable-workflow/workflow/blob/ce24207ef86856e64f97b939c9aeef9d1fb387c8/src/V2/Support/WorkflowExecutor.php#L895).
It consumes the selected `TimerFired.recorded_at`, including its precision.
The native producer already carries the matching physical/logical wait,
key/fingerprint, timer ID/kind, command sequence and delay through
[`wait_fields`](https://github.com/durable-workflow/server/blob/fadf8729265e077f280c8f71a87c06d8f73a682e/rust/src/runtime/wait_timeouts.rs#L38)
and the scheduled/fired history. It intentionally need not append a
`ConditionWaitTimedOut` event. This source change adds no field or producer.

One ordered open/schedule/fire and complete matching metadata are required.
A cancelled, aliased, duplicate, reordered, superseded or otherwise ambiguous
physical wait cannot provide time. A logical reopen uses the final actually
consumed physical wait; an old orphan timeout cannot supply a pending reopen's
clock. Indexing reads identities once, and accessors only read the consumed
boundary. Payload `fired_at`, deadline, reuse, insertion aliases and an
unrepresented timer-row fallback do not replace the selected `recorded_at`.
Missing or invalid time is unavailable while legacy results remain unchanged.

This fired-only authority is executor-specific. Current Core query replay stops
without an explicit condition resolution, so snapshot and reconstructed-query
clocks remain unavailable on this shape, including inside the reconstructed
workflow. Existing explicit persisted condition-resolution query behavior is
preserved. Grouped conditions, predicate-trigger retention and the separate PHP
occurrence-carrier gap remain open, as do published/native conformance and
release adoption. The corpus fixture is synthetic source evidence shaped from
the immutable writer, not a claim that a native experiment ran.
