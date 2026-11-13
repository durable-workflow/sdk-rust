# Changelog

## Unreleased

- Worker metrics emit the revision-2 schedule-to-start histogram in seconds
  for workflow and activity claims when the Server advertises
  `claimed_task_ready_age`. Missing or invalid producer values are omitted.
  Prometheus export uses only the task kind and task queue labels.

## 4.15.0

- All workflow and run routes encode each identifier or name as one path
  segment. Empty identifiers and `.` or `..` segments return
  `Error::InvalidPathSegment` before any HTTP request, including capability
  discovery for cooperative cancellation.

- Cooperative cancellation now reads the optional top-level `cause` of the
  portable cancellation context (workflow#863, workflow#1129). A
  timeout-cleanup request (`cause: "timeout"`: the run passed its work
  deadline and is now in cleanup) arrives through the existing
  `Error::CooperativeCancellationRequested`, so existing
  `Error::CooperativeCancellationRequested(_)` arms, such as saga
  compensation, run for it unchanged. There is no new `Error` variant. Tell
  the two apart with the new `#[non_exhaustive]` `CancellationCause`:
  `Requested` for an ordinary request (`cause` absent or `"cancellation"`) or
  `Timeout`. The request's cleanup deadline is its existing
  `cleanup_deadline_at`. `CancellationRequest::cause()`, `CancellationContext::cause()` and `is_timeout()`
  read it from the context. A context with any other `cause` is refused as
  `Error::InvalidCooperativeCancellation`. The cause is never inherited: a
  descendant cancelled under the request, scoped or not, carries no `cause`
  and is an ordinary cancellation, even when its scope origin's root context
  carries `cause: "timeout"`. `CancellationContext::to_value()` writes `cause`
  back exactly as read, including an explicit `"cancellation"`, and an
  ordinary context's output is unchanged.

  `CancellationRequest::cause()` derives the discriminator from the existing
  context, preserving stable 4.x struct literals and exhaustive destructuring.
  This parser support is experimental. Service-mode timeout-cleanup admission
  and execution require the upstream Core/native implementation and separate
  published-artifact qualification.

- A workflow-task completion that native Server 3.6.0 refuses as too large
  (HTTP 413, `limit_kind: workflow_task_completion_bytes`, server#1152,
  durable-workflow/.github#228) is now a typed, non-retryable outcome.
  `Client::complete_workflow_task` returns the new
  `Error::WorkflowTaskCompletionTooLarge` with the Server's limit, the encoded
  request size and the Server's message, and the request is never resent. A
  `Worker` no longer leaves the task to be redelivered into the same refusal:
  it completes the task once with a non-retryable `fail_workflow` command whose
  message starts with `Structural limit exceeded:`, so the run fails with
  failure category `structural_limit` and guidance to split the fan-out, use
  child workflows or continue as new. Other 413 and 5xx responses are
  unchanged. The README documents the completion size limit.

- An update whose wait ends before its handler finishes now returns
  `Error::UpdateStillPending` instead of `Ok(Null)` (durable-workflow/.github#212).
  `update_workflow` used to return the Server's `result: null` for a 202
  `wait_timed_out` receipt, so a pending update looked like a handler that
  returned null. The error carries the `update_id`, `request_id`, current
  `run_id`, `update_status` and full receipt; a real null result is still
  `Ok(Null)`. `update_workflow_avro_value` returns the same error instead of a
  `missing_payload_envelope` codec error.

- `Client::update_outcome`, `Client::update_outcome_avro_value` and
  `Client::update_outcome_receipt` (and `WorkflowHandle::update_outcome`) read an
  accepted update's outcome later by its update id, without submitting it
  again (server#1131). They need a Server that advertises
  `update_outcome_reads` (native 3.6.0 and later) and return
  `Error::UpdateOutcomeReadsUnavailable` otherwise, before reading anything. A
  still-running update returns `Error::UpdateStillPending` with a hint to read
  again by update id. A failed update is `Error::Http` 422, and
  `Error::is_update_not_found()` detects 404 `update_not_found`. An empty, `.`
  or `..` workflow id or update id is refused before any request, so a read
  can't collapse onto another route. A successful answer that isn't a receipt
  for the requested update (matching `update_id` and an `update_status`), or a
  non-completed receipt where an outcome was expected, returns
  `Error::UnrecognizedUpdateOutcome` instead of a value.

- `describe_workflow` and `update_workflow` (and `update_workflow_avro_value`)
  percent-encode the workflow id, and the update name, as one path segment.
  Before, they were put into the path raw, so an id containing `/`, `?` or `#`
  addressed a different route. An empty, `.` or `..` id or update name now
  returns `Error::InvalidPathSegment` before any request; `..` would otherwise
  have described or updated another route. Found in the security review of
  sdk-rust#243 (durable-workflow/.github#212).

- A `request_id_conflict` refusal from native Server 3.9.0 (server#934,
  server#1191) now names the key that was sent. Native doesn't echo the key, so
  `Error::request_id()` returned `None` and `RequestIdConflict::request_id` was
  empty. The SDK now adds the sent key to the refusal body when the Server
  doesn't echo it. The error is still `Error::Http` with status 409, it is sent
  once and never resent, and `RequestIdConflict` documents native's `conflict`
  parts (`name`, `arguments`, `target_or_arguments`). The README describes
  request IDs and the `command_request_keys` capability. Older Servers are
  unaffected.

- Replay ignores the activity cancellations a run's own close records
  (durable-workflow/server#1193, durable-workflow/workflow#1117). When a run
  completes, fails or continues as new, Workflow 2.28.0 (and Server with
  server#1193) records `ActivityCancelled` with `cancellation_reason: run_closed`
  for each activity it never awaited, such as a `select()` loser. Replay now
  leaves that activity open instead of treating the cancellation as an outcome:
  a handled `all()` failure is no longer replaced by a lower-index member
  cancelled at close. The new `abandoned_activities` key on `WorkflowCompleted`,
  `WorkflowFailed` and `WorkflowContinuedAsNew` is accepted. Activity workers
  already treat a `run_closed` heartbeat or late outcome (`recorded: false`) as
  final; new tests cover it, including an abandon-policy activity that keeps
  running after close.

## 4.14.0

- Workers with a sticky cache advertise `sticky_history_suffix` (#234,
  server#1017). On Server 3.5.0 or later, a sticky hit receives only the history
  after the sequence the worker cached, instead of the full history on every
  workflow task. A suffix is used only when it continues the cached history
  exactly; otherwise the worker drops the entry and refetches full history.

## 4.13.0

- Local activities no longer retry with zero delay once their backoff list runs out.
  Each later retry repeats the final backoff entry, as Server-scheduled activities
  do (durable-workflow/.github#219).
- Reject malformed supplied shared, control and worker static tokens during
  client construction, including unused or masked options. Diagnostics name
  only the option. Empty tokens and valid token bytes retain their precedence.

- Add `Client::get_run_activities` with typed run activity and attempt metadata,
  preserving unavailable fields, diagnostic authority and heartbeat progress.

## 4.12.0

- Keep one in-flight poll per task family while filling the configured workflow
  and activity execution slots (#199). A received task finishes hydration and
  initial lease confirmation before polling refills a spare slot. Its execution
  slot stays occupied through completion reporting, so slow callbacks can run
  concurrently without multiplying idle long polls.

- Reuse worker-authenticated activity lease capability observations instead of
  fetching `/cluster/info` for every activity (#199). Client clones share one
  discovery when needed and refresh only after an explicit capability refusal. Original claim,
  ownership and deadline validation still apply to every renewal.

- Add a public raw request API for tools that must reach every Server route
  and report its response unchanged (#180). `Client::control_plane_request`
  and `Client::worker_plane_request` take a `RawRequest` (path segments the
  SDK percent-encodes, query pairs, a verbatim JSON body, a timeout and
  optional version-header overrides) and return a `RawResponse` for every
  HTTP status, with the exact body bytes and all headers.
  `RawResponse::error_for_status` gives the same `Error::Http` or
  `Error::Protocol` a typed call would, and `refusal()` decodes `reason`,
  `rejection_reason`, `message`, `error` and `validation_errors`. Raw
  requests make one attempt and never rewrite bodies. Worker-plane 2xx
  responses must echo a same-major, equal-or-newer-minor protocol version in
  both the header and the body. Typed methods remain the API for
  applications.
- Add `ClientBuilder::danger_accept_invalid_certs` for self-hosted Servers
  whose certificates cannot be verified. It defaults to `false`; prefer
  `additional_root_certificates_pem` for a private CA.
- Replay condition-wait histories in which the Server recorded a second open
  of a wait that was still open (#191, #192). A wait is now linked to the
  same logical wait by its state when the next open was recorded: if it was
  still open, the next open is a repeated open. So an orphaned duplicate whose
  timer fires later no longer fails replay with
  `condition_wait_reopened_after_timeout` (the sdk-php#283 shape). On PHP
  Server 2.x, condition members of one `parallel` or `select` group woken by
  one signal no longer fail the next task with
  `condition_wait_group_reopen_mismatch`, and each member consumes its own
  reopen, including when another member such as a timer sits between them
  (#194). A pending scope cancellation over such a group selects its original
  members by sequence. A wait that timed out before the next open still ends
  it. Both histories were refused by 4.11.1 as well.

## 4.11.2

- A `wait_condition` timeout now runs from the first time the wait opens
  (#187, parity with sdk-php#269). The Server wakes an open condition wait on
  any signal and records it satisfied. When the predicate was still false, the
  worker reopened the wait with the full `timeout_seconds`, so each
  non-matching signal pushed the deadline back. The worker now reopens with
  the time remaining since the first `ConditionWaitOpened`, rounded up to whole
  seconds and never below zero; a reopen after the deadline opens with a zero
  timeout so the Server resolves it as timed out. A condition wait inside a
  `parallel` or `select` group reopens with its authored timeout, as sdk-php
  does, because the PHP Server accepts a grouped reopen only when its timeout
  equals the original open's (#189). Histories whose reopens
  carry the full timeout still replay, and a reopen longer than the authored
  timeout is still refused. A reopen links only to the same logical wait
  (`condition_wait_occurrence_id`), so a separate wait that follows a timed-out
  one measures from its own open. This needs no Server change. Rolling deploys
  and rollback: workers on 4.11.1 or earlier refuse a history whose reopen
  records less than the authored timeout (`condition_wait_timeout_mismatch`),
  so keep those runs on fixed workers.
- Parse JSON floats with correct rounding by enabling serde_json's
  `float_roundtrip` feature (#181). Without it, some 16 to 17
  significant-digit doubles in Server JSON decoded one ULP off, for example
  `1.4820142857142857` became `1.4820142857142855`. Replay compared those
  values with the workflow's own floats, so a recorded float search attribute
  could fail as `search_attribute_value_mismatch`. Other JSON numbers read
  from Server responses and history had the same drift. Avro payloads
  (arguments, results, memos, query and update values) carry binary doubles
  and were not affected. Cargo unifies features, so this applies to every
  serde_json user in a worker binary; float parsing is slightly slower.
- Qualify replay of Workflow 2.19 redrive successors that reuse
  `MemoUpserted`, `SearchAttributesUpserted` or an applied signal wait
  (`SignalWaitOpened`, `SignalReceived`, `SignalApplied`). Values replay
  unchanged and nothing is re-emitted. A reused prefix always precedes any
  cancellation delivered on the successor, so these events never change
  `remaining()`.
- Keep managed workers polling when a workflow-task completion or failure
  report receives a non-recorded `run_closed` receipt for the same task,
  attempt and run, for example after the run was cancelled while the report
  was in flight. The receipt must carry `recorded=false`, the exact task,
  attempt and `run_id`, a terminal `run_status`, the `stop_reason` it implies
  and `can_continue=false`, as native Server 3.0.5 sends (server#994).
  Native 3.0.4's `run_closed` body lacks `recorded` and `run_id` and still
  surfaces, as do recorded, mismatched and other conflicts. Direct client
  calls keep the original HTTP error.
- Correction to 4.11.1: its cancellation-clock fixture was not a valid redrive
  successor history. It had no redrive `WorkflowStarted` and placed the
  cancellation before the reused prefix. Reused times are not observable through
  `remaining()` in valid redrive histories: the reused prefix always precedes
  the successor's cancellation delivery, so `remaining()` is the cleanup
  deadline less that delivery. Preferring `reused_recorded_at` changes no
  observable result, and 4.11.1 does not restore any cleanup budget. Reused
  activity completions, fired timers, side effects and version markers are now
  qualified with valid Workflow 2.19 successor histories. The 4.11.1 markers and
  timer fixtures also lacked the redrive envelope. Each redrive fixture must
  either pass the successor check or be one of the named 4.11.1 exceptions. The
  check binds the full redrive `WorkflowStarted` envelope, each event's run and
  instance, and the successor's cancellation request and delivery.

## 4.11.1

- Advance the cooperative cancellation clock to a redrive-reused step's
  original `reused_recorded_at` time instead of the time the successor run
  copied it, matching Core, so `remaining()` after reused activity completions
  or fired timers no longer loses the redrive delay from its cleanup budget.

## 4.11.0

- Keep managed workers polling when a workflow-task completion or failure
  report receives a non-recorded `task_not_leased` receipt for the same task,
  attempt and run, for example after the run was cancelled. Recorded,
  mismatched and other conflicts still surface, and direct client calls keep
  the original HTTP error.
- Fill `WorkflowFailed` and `WorkflowTimedOut` results from the run's terminal
  history event when a terminal description carries no failure detail or timeout
  reason, as native Server 3.0.1 does. The error type, message, retry flag and
  `run_timeout`/`execution_timeout` reason are restored. Complete descriptions
  and cancelled or terminated runs never read history, and an unreadable history
  keeps the description-only outcome.

## 4.10.3

- Declare minimum stable Server releases per major, including native Server 3.x
  from 3.0.2, without changing the existing 2.x fields, worker protocol or
  capabilities. Native 3.0.0 and 3.0.1 are not declared because their run
  descriptions omit terminal failure detail (server#967).
  Validate the complete additive map before publication and retain it in release
  evidence alongside the published crate.

## 4.10.2

- Keep managed workers polling when a history-wait or workflow-task failure
  report receives the original task, attempt and run's terminal timeout receipt.
  Apply the same strict HTTP 409 identity check as command completions, discard
  provisional sticky state, and preserve other refusals and direct client errors.

## 4.10.1

- Preserve valid outer workflow-failure messages when a nested exception message
  is absent or blank, matching published Server fallback semantics. Keep supplied
  small report fields unchanged, while reducing redundant outer copies when a
  lower encoded-body budget requires it. Preserve type, retry flags and claim IDs.

## 4.10.0

- Bound outgoing activity, workflow-task and terminal workflow failure messages,
  stacks and combined diagnostic data, with visible truncation and original-size
  diagnostics. Enforce a 1 MiB complete encoded report cap or a lower known
  transport limit after JSON escaping. Preserve claim identity, original type and
  retry semantics; refuse unfit required data as a typed reporting error.
- Expose structured activity and workflow-task reporting over existing Server
  failure fields. Keep valid Avro details whole or omit them with a marker.
  Bound local messages before workflow observation so committed replay matches.
- Keep composed one-tick workers on the normal thread stack with optional
  telemetry and cooperative polling. Reporting refusals discard provisional
  sticky state and remain distinct from application failures.

## 4.9.0

- Expose optional immutable activity execution, workflow instance and run IDs
  from the original claim, including worker-session activities. Preserve absence
  on older claims without deriving stable identity from task or attempt IDs.
  Inline local callbacks expose their received parent workflow/run IDs and no
  Server activity ID; Server-free tests report no Server identities.

## 4.8.0

- Expose immutable `ActivityContext::retry_info()` with the original attempt
  number and received retry policy, including unknown fields. Distinguish bounded,
  explicit unlimited and unknown attempt budgets and check the final configured
  attempt without promising retry admission. Preserve public task struct literals,
  existing worker execution semantics and Rust 1.86 compatibility.

## 4.7.0

- Add pluggable client/worker metrics recorders with a no-op default and the six
  stable shared SDK counter/histogram names, count/second units and exact labels.
  Observe logical requests, polls and workflow/activity/query dispatches without
  exposing IDs, payloads, credentials or query strings in core tags. Unknown
  routes use a bounded fallback. Workers inherit or explicitly override recording;
  recorder panics cannot change SDK results.
- Add the optional `metrics-prometheus` adapter using a dedicated official
  Prometheus registry, plus a runnable export recipe and pinned shared-contract
  acceptance. New Rust client outcomes are ok/http_error/bridge_rejected,
  TransportError/TimeoutError/SdkError/pending; poll outcomes are
  task/empty/backpressure/error, and task outcomes completed/failed/expired.

## 4.6.0

- Add typed one-page workflow listing and selected-run history reads over the
  published APIs, including search attributes and unchanged Server validation.
  Add lazy async iterators that preserve namespace/run/filter identity and
  hold one page at a time. Propagate page errors and detect repeated/cyclic
  tokens with bounded state; retain the existing complete-history export helper.
- Document single-page versus complete enumeration, active status-bucket
  filtering, query identity, termination and consistency limits. Add a read-only
  pagination example and real filtered multi-page list/history acceptance.

## 4.5.0

- Add Server-free `testing::WorkflowEnvironment` and `WorkflowHistory` for
  deployment replay and in-process workflow/activity tests. Reuse the worker's
  codec, marker and persisted-selection replay paths; report typed divergence
  sequences and recorded/produced identities. Import CLI exports or complete
  history API responses, and export resolved payloads with the Client helper.
- Execute real/mocked activities with authored retry policies and typed terminal
  failures; skip durable timers and retry delays. Unsupported external runtime
  operations and activity timeout simulation fail explicitly. Document the
  supported execution model, determinism rules and runnable test examples.

## 4.4.0

- Add `ClientBuilder::additional_root_certificates_pem` for self-hosted Servers
  and proxies using private certificate authorities. Additional roots apply to
  every request while preserving bundled public roots and hostname checks.

## 4.3.1

- Enforce the configured workflow and activity concurrency limits in ordinary
  workers and report heartbeat availability from in-flight tasks. Defaults stay
  at 10 for each task family; workers now use those slots instead of executing
  workflows and ordinary activities serially. Set either limit to 1 for serial
  execution. Claimed tasks retain their slot through loading and settlement.

## 4.3.0

- Add `ActivityError` for application-defined activity failure types and explicit
  non-retryable outcomes. Ordinary, cooperative and local activity workers
  preserve these fields so activity retry policies can match the authored type.
  Low-level callers can use `Client::fail_activity_task_with_error`.

## 4.2.2

- Require patched TLS certificate verification and root bundles that reject
  the compromised emSign intermediates. Apply the minimum versions to library
  consumers as well as this repository's lockfile, preserving normal hostname
  validation and configured private certificate authorities.

## 4.2.1

- Start the runtime activity lease maintainer before callback entry. A temporary
  blocking callback no longer strands the maintainer in the polling task's
  scheduler slot when a spare Tokio executor thread is available. Original
  claim, timeout, shutdown and late-publication fences remain unchanged.
- Clarify that async callbacks must still yield to Tokio. Blocking I/O and CPU
  work need `spawn_blocking` and their own cancellation or process boundary;
  dropping a future cannot stop a detached thread or reverse an external effect.

## 4.2.0

- Let a select-member cancellation request complete within its original workflow
  task so subsequent workflow commands can commit. The unit result does not
  report that cancellation won a race: recorded history still governs the
  member outcome, replay validation and `await_result()`.
- Add a bounded activity lease-renewal client request behind affirmative
  runtime discovery. It uses worker credentials and preserves original claim
  identity without recording application progress.
- Automatically maintain activity claims when the runtime advertises the lease
  renewal capability. Preserve original business and heartbeat deadlines, session
  identity and TTL, callback shutdown and late-publication fences through payload
  loading, callbacks and settlement. Application progress still requires genuine
  heartbeats. Published long-activity and killed-worker acceptance remain pending.

## 4.1.0

- Add optional execution spans and application log correlation for workflow,
  query and actual activity attempts. The default build has no OpenTelemetry
  dependency. Applications own providers, propagators, exporters and shutdown.
- Propagate bounded W3C contexts only when actual Server discovery advertises
  the optional observability capability. Run origins parent tasks; validated
  dispatch, command and outcome contexts become links after original authority
  fences. Baggage defaults empty and is excluded from links.
- Restore Tokio context on yield, completion, cancellation and panic. Cold
  replay does not emit attempts for completed activities. Existing request IDs,
  task identities, business values and retry budgets retain their meaning.

## 4.0.0

- Author Nexus service calls with deterministic intent, typed Avro outcomes,
  original call identities and deadlines. Explicit acceptance-only calls keep
  their admission snapshot across cold replay. Synchronous calls wait for the
  committed result or typed failure.
- Deliver cooperative cancellation at the original service call after confirming
  the installed runtime capability. Preserve canonical cancellation identity,
  cleanup deadlines and matching-claim ownership through cold recovery.
- Add `Error::NexusOperationFailed` and `CancellationCallKind::ServiceOperation`.
  Mark both enums `non_exhaustive`; downstream matches must include a fallback
  arm. This is the Rust 4 migration boundary.
- Retain optional observability metadata on public workflow, activity and query
  task models, and optional activity workflow/run/execution identities. Direct
  task struct literals must initialize the new fields. The metadata remains
  untrusted and does not enable tracing or affect execution authority.

## 3.5.6

- Refuse incompatible typed workflow entry arguments before invoking workflow
  code, including cold replay with recorded history. Preserve the original task,
  input and history for diagnosis and compatible-code recovery; exceptions from
  an invoked workflow handler remain ordinary workflow failures.
- Advertise pre-entry argument validation so supporting Servers can safely
  select a compatible worker after a definition changes. Server 2.9.9 or later
  provides the corresponding replay-blocked classification and recovery path;
  older Servers retain their existing failure handling.

## 3.5.5

- Fence managed queries with their original claim and caller deadline before
  external loading, replay, handler entry and outcome submission. Protocol 1.20
  observes authority without renewing it. Expiry and unavailable authority drop
  yielding work without sending a fabricated failure acknowledgement.
- Add `Worker::require_query_task_authority(true)` for pre-registration refusal
  when strong query authority is unavailable. Explicit 1.19 and older Servers
  retain their documented weaker timing behavior. Public `QueryTask` construction
  and direct Client completion/receipt operations stay compatible.
- Synchronous or CPU-bound handlers cannot be interrupted while they block, but
  their results are fenced after the callback returns.

## 3.5.4

- Retry explicitly deferred workflow completions after backend lock pressure when
  the response proves the same task, attempt and lease owner and `recorded=false`.
  Preserve the serialized decision and honor the Server's delay without running
  the handler again. Shutdown and the original cooperative cleanup deadline bound
  recovery, and stale lease refusals remain errors.

## 3.5.3

- Preserve all explicit activity backoff entries independently of the finite
  attempt budget, allowing matching authoring of published PHP retry snapshots.
  Defaults and exact replay comparison remain unchanged, including unused
  entries, attempt budgets, timeouts and non-retryable error types.

## 3.5.2

- Supervise external activity input loading within the original lease and
  execution deadlines. Protocol 1.20 confirms the exact task, attempt, owner
  and authenticated namespace before, during and after loading, and before
  callback entry. Unconfirmed authority, expiry, shutdown and dropped polls
  stop loading without invoking application code. Observation does not renew
  the lease or record application progress.
- Preserve explicit protocol 1.19 ordinary support using immutable original
  local bounds, without requesting an unsupported status endpoint. This path
  cannot observe live revocation or ownership. Keep the Client's total transfer
  budget separate from claim deadlines and preserve runtime outcome fencing.

## 3.5.1

- Confirm and renew the exact workflow task claim while loading external inputs
  and paged history. Abort loading on lost authority or Worker shutdown before
  workflow code executes. Preserve the selected Client timeout across the whole
  claimed transfer, without restarting it on renewal.
- Reuse bounded, verified payload bytes within a cooperative claim's canonical
  cancellation refresh. Keep the original cancellation identity, cleanup deadline
  and committed delivery boundary. Dropped polls retain no renewal task, and
  ordinary Worker shutdown finishes normally during input loading.

## 3.5.0

- Default worker requests and metadata to protocol 1.20. Select
  `WorkerProtocolVersion::V119` with `ClientBuilder::worker_protocol_version`
  for an older Server. Keep optional capabilities explicit and refuse
  cooperative registration from an explicit 1.19 client before sending it.
  Preserve control-plane protocol 2 and the existing command and feature floors.

## 3.4.4

- Reject duplicate workflow, activity, query and update handler names before
  contacting Server. Dynamic, typed, Avro and replayed adapters share their
  handler namespaces. Preserve the original registration and report both source
  locations and adapters through `DuplicateRegistrationError` from local
  validation. Existing registration method signatures and the `Error` enum
  remain unchanged. Startup returns its existing `WorkerLoop` error with the
  duplicate diagnostic. `validate_registration` provides an explicit local
  check, also enforced by every worker startup and polling entry point.
  Query/update names remain scoped to their workflow type.

## 3.4.3

- Replay consistent historical patch markers written by older workers at their
  original positions. Repeated calls consume matching aliases without skipping
  intervening operations or changing recorded history and timestamps. Fresh
  decisions remain deduplicated. Reject conflicting selected versions, malformed
  ranges and duplicate records of one physical command. A failed supported-range
  check leaves replay at its original boundary.

## 3.4.2

- Select the original branch when a patch is added before an unmarked recorded
  operation or to an already completed workflow. Preserve the original command
  and its sequence, reuse the decision for repeated calls, and reject supported
  ranges that exclude the recorded decision.

## 3.4.1

- Send remote activity heartbeat details as the bounded JSON progress map
  accepted by Server, matching PHP and Python. Preserve integer, fractional,
  boolean, null and Unicode metadata in both ordinary and cooperative workers.
  Valid progress no longer becomes an oversized encoded envelope that Server
  rejects while the activity's heartbeat deadline expires.

## 3.4.0

- Add explicit `Worker::sticky_cache(StickyCacheOptions)` with entry and encoded
  history byte limits, LRU eviction, TTL, build identity and cache counters.
  Cache only immutable durable wire history and decode fresh replay snapshots.
  Validate current-lease tail cursors and fall back to complete cold history
  after expiry, eviction, invalid cursors or process loss. Clear terminal and
  stopped-worker entries. Default workers continue to replay complete history.
- Keep canonical cancellation delivery authoritative when caching is enabled.
  Require confirmed sticky registration before polling.
- Recognize buffered control-plane signal application markers separately from
  authored signal waits. Preserve their outer history sequence without treating
  it as a workflow command sequence during cold or warm replay.
- Add a runnable sticky execution example. Use Server 2.5.10 or newer.

## 3.3.3

- Preserve the original signal argument vector when matching its application
  marker. Empty signals apply as `true`, single arguments apply as their value,
  and multiple arguments apply as a list. Keep nested arrays, bytes and nulls
  intact while checking immutable identity and conflicting data.

## 3.3.2

- Expose one committed signal per immutable signal or command identity in
  query and update snapshots. Receipt and application markers describe the
  same signal. Preserve distinct signals, even with equal names and arguments,
  and refuse conflicting payloads for the same identity.

## 3.3.1

- Populate update handlers' `QueryContext` with the original workflow input
  and committed signals, matching the query snapshot. Preserve the separate
  update arguments and lossless Avro values. Reject malformed snapshot payloads
  before invoking the handler.

## 3.3.0

- Add `Worker::declare_workflow_signals` for names consumed through `wait_signal`
  and `signals`. Register their positional argument contracts alongside queries
  and updates so Server can admit those signals for new workflow runs.
- Sort and deduplicate declarations, reject invalid names and unknown workflow
  types, and allow an explicit empty declaration. Existing runs retain the
  declarations captured when they started.

## 3.2.2

- Register complete query and update argument contracts for JSON and lossless
  Avro handlers. Server can admit commands for new Rust workflow runs using
  the handlers' positional argument vectors.
- Keep unsupported update validators refused and preserve explicitly supplied
  low-level registration metadata. Existing runs retain their original declarations.

## 3.2.1

- Add `Worker::recover_transient_outages(true)` for long-running service workers.
  Retryable poll and worker-heartbeat failures keep retrying with capped backoff
  and the original poll request identity. Shutdown interrupts retry waits.
- Preserve bounded retries by default and for `run_once`. Disabling ordinary
  retries still takes precedence. Authentication, protocol, codec, handler and
  task settlement failures remain errors.

## 3.2.0

- Add typed worker-session options and shared create, renew and close handles.
  Enable sessions explicitly with `Worker::worker_sessions(true)` and route
  remote activities through a session. Declare resource requirements and bound
  worker/session activity capacity.
- Supervise async callbacks against their holder lease and original absolute
  TTL without requiring application heartbeats. Rebuild process-local resources
  after holder loss and reject changed session routing during cold replay.
- Close held sessions before worker deregistration. Dropped failed or uncreated
  handles release their local registry slot. Committed results replay without
  rebuilding resources or rerunning activities. Sticky execution stays unsupported.
- Use Server 2.5.1 / Native 2.4.1 for original TTL preservation and session history.

## 3.1.0

- Add explicit inline local activity execution with typed and lossless Avro
  results, workflow-task lease renewal, bounded retry and heartbeat reports,
  timeouts, external payloads and cold replay. Enable it explicitly on an
  ordinary Worker. Qualified against published Server 2.5.0 / Native 2.4.0.
- Stop a pending local callback on worker shutdown and reclaim unfinished work
  on a replacement. Keep fresh and replayed failure categories and attempts aligned.
- Reject local activity history replayed as remote, including top-level and
  legacy markers without an option snapshot. Reject conflicting execution modes.

## 3.0.0

- Add cooperative whole-run cancellation with immutable request metadata,
  lineage, deterministic cleanup time helpers and the original bounded deadline.
- Add explicit Activity and Child TryCancel, WaitCancellationCompleted and
  Abandon policies, and cooperative RequestCancellation on parent closure.
- Supervise opted-in async Activity callbacks independently of application
  heartbeats, acknowledge their stop and reject stale publication. Resume
  shielded durable cleanup from the original delivery boundary after worker loss.
- Keep cooperation opt-in and ordinary worker protocol 1.19. Independently
  cancellable scopes remain a disabled source preview. Portable local activities,
  worker sessions and sticky execution remain unsupported by this Rust profile.
- This major release adds public cancellation fields and enum variants. Follow
  the [v3 migration guide](https://github.com/durable-workflow/sdk-rust/blob/main/docs/migrating-to-v3.md)
  when updating struct literals and exhaustive matches. The wire protocol remains
  compatible with older same-major workers on a supporting Server.

## 2.1.5

- Allow the ordinary Worker to append and close workflow streams when Server
  supplies a durable task ID without a separate workflow command ID. Prefer an
  explicit command ID when available. Recorded stream effects still replay
  without producing duplicate output.

## 2.1.4

- Keep fresh installations compatible with Rust 1.86 by selecting uuid 1.26.1.
  The newer uuid 1.27 requires Rust 1.89.

## 2.1.3

- Replay condition reopens inside parallel and keyed selection groups using
  their recorded authored identity. Keep changed definitions, paths and
  unproven predecessors invalid.
- Wait for a selected condition's canonical winner without reopening a true
  predicate. Resolve the winner and operation handles through the latest
  physical wait, including typed timeout results after acknowledgement.
- Keep ordinary worker protocol 1.19. The grouped reopen Server correction is
  provided by Workflow 2.3.2. Cooperative cancellation remains separately gated.

## 2.1.2

- Request bounded workflow history pages in Rust worker polls. The client
  follows page tokens and returns complete histories without requiring an
  unbounded Server poll response.

## 2.1.1

- Keep managed activity and workflow workers retrying the same fenced completion
  after an identity-matching, retryable Server `backend_unavailable` response.
  Preserve the serialized result without re-executing the handler, and stop the
  wait promptly on shutdown. Mismatched responses and terminal rejections stay
  authoritative.

## 2.1.0

- Add failed-run redrive to the Rust client and workflow handle. A worker can
  register source identity so a successor run can reuse the completed prefix
  and retry the failed boundary when the Server supports redrive.

## 2.0.7

- Keep managed workers polling and heartbeating through Server's explicit
  retryable `backend_unavailable` response, even after the ordinary retry
  budget. Preserve the original poll request, bound backoff, and interrupt the
  wait on shutdown. Authentication, malformed responses, and unrelated worker
  errors remain terminal.

## 2.0.6

- Keep fresh installs compatible with Rust 1.86 by constraining the transitive
  yoke-derive dependency to its compatible release. Consumer builds do not
  inherit the SDK repository's Cargo.lock.

## 2.0.5

- On supporting Servers, retry a draining refusal for a completion payload
  with its exact activity, workflow, or query lease and immutable payload slot.
  Client uploads, unknown capabilities, and hard storage fences remain blocked.
- Preserve prepared completion bytes during late upload pressure without
  re-executing handlers. Existing retries remain interruptible; Server upload
  allowances, namespace quotas, and stale-lease errors remain authoritative.

## 2.0.4

- Upload large encoded payloads through the authenticated namespace runtime
  before sending client requests or worker commands. Discover runtime limits,
  externalize aggregate requests when necessary, and preserve exact Avro types.
- Verify upload references, keep discovery and responses bounded, and reuse
  existing worker storage-admission waits without re-executing handlers.
- Include a native runtime qualification for large typed input, activity and
  workflow results, query/signal recovery, maximum size and cold restart.

## 2.0.3

- Resolve Server-managed external payload references before decoding worker
  tasks, history, query/update results, and client workflow results. Large
  activity results can resume workflows without re-executing their activities.
- Fetch only from the authenticated runtime with the original namespace and
  credential role, verify size and SHA-256, and reject redirects or malformed
  references. Keep downloads bounded by a configurable per-response byte limit
  and leave application metadata untouched.

## 2.0.2

- Retry retryable storage-admission refusals in worker registration, polling,
  heartbeats, and acknowledgements while preserving the prepared request and
  already-computed handler result. Retries do not re-execute activities or
  queries or change task identities.
- Keep storage retries interruptible during shutdown. Client requests retain
  their existing error behavior; authentication and stale-lease failures remain
  terminal.

## 2.0.1

- Keep workflow, activity, and query pollers alive when Server applies typed
  long-poll capacity backpressure, honoring its bounded retry delay before the
  worker polls again.
- Accept full valid UTF-8 text values for typed string, keyword, and keyword
  list search attributes while retaining structural and encoded-size limits.

## 2.0.0

- Publish the stable Rust SDK qualified against the immutable Server `2.0.0`
  artifact.

## 2.0.0-rc.39

- Qualify this prerelease against the immutable Server `2.0.0-rc.68` artifact
  exactly.

## 2.0.0-rc.38

- Qualify this prerelease against the immutable Server `2.0.0-rc.57` artifact
  exactly.

## 2.0.0-rc.37

- Qualify this prerelease against the immutable Server `2.0.0-rc.56` artifact
  exactly.

## 2.0.0-rc.36

- Qualify this prerelease against the final Server `2.0.0-rc.55` artifact
  exactly.

## 2.0.0-rc.35

- Explicitly refuse local activities, worker sessions, and sticky execution in
  the worker capability manifest until the Rust runtime implements those
  protocol 1.18 contracts.
- Add persisted first-completion selection across activities, child workflows,
  durable timers, signal waits, condition waits, and nested parallel groups.
  Server commits one stable keyed winner while non-winning operations continue
  and remain available for later awaiting or explicit cancellation.
- Replay the committed winner independently of later terminal-history order and
  preserve typed results and failures across cold worker replacement.
- Advance the worker protocol to `1.19` and advertise durable selection during
  high-level worker registration.
- Qualify this prerelease against the synchronized Server `2.0.0-rc.51`
  artifact exactly.

## 2.0.0-rc.34

- Require the current release entry to be present in the source commit before
  tagging or generated-reference deployment can authorize it.
- Verify the packaged release entry and VCS identity before publication, then
  verify the downloaded registry archive against the authorized package.
- Restore `apache-avro` as the authoritative outbound datum encoder while
  retaining canonical string-map order, the fixed single-object frame, typed
  value identity, and the existing cross-language protocol bytes.
- Preserve one-to-one authored condition-wait occurrences with an explicit
  deterministic identity. Adjacent waits at the same call site now replay
  independently, while signal- and update-driven physical re-evaluations keep
  the identity of their open wait and continue to replay as one occurrence.
- Require Server `>=2.0.0-rc.50,<2.0.0`, the first release line that preserves
  condition-wait occurrence identity throughout open, terminal, and timeout
  history.
- Advance the worker protocol to `1.17` and advertise condition-wait occurrence
  identity, memo upserts, and typed search attributes explicitly from
  high-level worker registration.
- Add `WorkflowContext::message_stream` for ordered bounded consumption of
  named repeated input. Runtime-owned cursor and wait metadata survives replay,
  worker replacement, server restart, duplicates, and continue-as-new while
  preserving exact Avro payload values.

## 2.0.0-rc.33

- Added `WorkflowContext::upsert_memo` with bounded canonical Avro map patches,
  `MemoUpserted` replay identity, opaque payload-envelope transport, and a
  fail-closed runtime capability check before worker-task completion. Replay
  identity compares double bit patterns, including the sign of zero.
- Add Serde-typed workflow, replayed-workflow, and activity handler adapters
  over the fixed Avro Value protocol, with typed activity calls and workflow
  results.
- Add typed run-scoped Workflow Stream list, describe, resumable subscribe,
  append, close, and errored lifecycle operations, including replay-stable
  workflow-command authoring identity and opaque external payload references.
- Add deterministic `WorkflowContext::parallel` / `join` composition for
  nested activity, child-workflow, timer, and mixed groups. Results retain the
  input shape and order; typed partial failures retain member paths, shared
  protocol metadata, causes, and completed siblings across restart and replay.
- Add `WorkflowContext::saga` with deterministic reverse-order activity
  compensation after failure or cooperative cancellation. Compensation
  failures preserve both the initiating and compensation errors.
- Preserve canonical declared search-attribute types in worker commands and
  replay identity, including deterministic same-value type mismatch detection
  and value-only compatibility for legacy history without type metadata.
  Require Server `>=2.0.0-rc.47,<2.0.0`, the first release line that supports
  worker protocol `1.16`.
- Add deterministic durable condition waits with typed satisfied/timed-out
  results, signal/update re-evaluation, timeout preservation, and definition
  drift detection across replay and restarts.
- Add validated typed workflow search-attribute updates on the public worker
  protocol and consume their committed mutations during replay.
- Ship a standalone, task-oriented condition and operator-metadata example in
  the generated Rust guidance.

## 2.0.0-rc.32

- Require an explicit string `payload_codec="avro"` on polled workflow,
  activity, and query tasks. Missing, null, and non-string declarations now
  produce a controlled `unsupported_payload_codec` task failure before handler
  execution.

## 2.0.0-rc.31

- Reject non-Avro and malformed payload envelopes across workflow, activity,
  query, replay, and outbound command boundaries before handlers or transport
  shortcuts can produce unrelated outcomes.
- Require centrally approved immutable GitHub Action commits before target
  branch qualification can pass.
- Keep dependency resolution compatible with the declared Rust 1.86 minimum.
- Align the crate README and generated API reference with the general-first
  install, local Server, API reference, and SDK-guide journey. Keep Cloud
  discoverable as a secondary limited early-access deployment path.
- Qualify the documentation hierarchy in source, generated rustdoc, and the
  packaged crate without binding the checks to Markdown prose or layout.

## 2.0.0-rc.30

- Remove the JSON payload-codecs feature and make the fixed typed Avro Value
  schema with single-object framing the only public payload codec.
- Reject JSON-tagged and unknown payload envelopes without transcoding or
  codec inference, qualified against Server `2.0.0-rc.32`.
- Replace exact prerelease pins and Cloud-first documentation with account-free
  Rust SDK onboarding through the public qualified versionless installer;
  Cloud remains available as a clearly labeled limited early-access path.

## 2.0.0-rc.12

- Add the initial task-oriented Rust documentation landing page and generated
  API reference navigation. The current general-first hierarchy is described
  in the `2.0.0-rc.31` entry.
- Let the released `hello_world` example accept separate client and worker
  credentials for one namespace-scoped Cloud runtime URL.
- Retain the qualified Server baseline at `2.0.0-rc.17` under the additive
  `>=2.0.0-rc.17,<2.0.0` compatibility range.

## 2.0.0-rc.11

- Reject unsupported update-validator declarations from the public low-level
  worker registration API before transport while preserving query and update
  handler contracts.
- Retain the qualified Server baseline at `2.0.0-rc.17` under the additive
  `>=2.0.0-rc.17,<2.0.0` compatibility range.

## 2.0.0-rc.10

- Declare the absence of an update-validator authoring surface in registered
  workflow contracts so Server discovery does not infer validator parity.
- Preserve the additive `>=2.0.0-rc.17,<2.0.0` protocol compatibility range.

## 2.0.0-rc.9

- Correct every shipped example to pass a Server origin or path-prefixed Cloud
  runtime URL without the SDK-owned `/api` suffix.
- Qualify the base-URL contract across all example sources and their rendered
  Rust documentation before publication.

## 2.0.0-rc.8

- Gracefully deregister successful worker registrations after all managed
  pollers have joined, while preserving both work-processing and cleanup errors.
- Add the typed worker-plane deregistration client operation and keep it
  separate from operator worker management.
- Enforce least-privilege token selection between worker, control, and shared
  credentials.
- Support Server `>=2.0.0-rc.17,<2.0.0` under the advertised protocol contract.
