# Changelog

## Unreleased

- Worker interceptors also wrap update-validation tasks through
  `WorkerInterceptor::execute_update_validation_task`, with an
  `UpdateValidationInterceptorContext` (validation task id, attempt, workflow
  and run ids, workflow type, update name, trace carrier). The hook has a
  passthrough default. Interceptors see a read-only `UpdateValidationOutcome`
  and cannot change the validator's decision; an interceptor error, a skipped
  `next` or a dropped validation reports `update_validation_worker_failed`, so
  the update is not accepted.
  ([Durable Workflow #234](https://github.com/durable-workflow/.github/issues/234),
  [#226](https://github.com/durable-workflow/.github/issues/226))
- A running worker no longer exits when the Server answers HTTP 500 during a
  database failover ([cloud#752](https://github.com/durable-workflow/cloud/issues/752)).
  An untyped 500 (for example `{"message":"Server Error"}` or an empty or HTML
  body) and a 500 or 503 with reason `server_error` (the Server's answer when
  its database is unavailable to authenticate a request) are now transient,
  like an untyped 502/503/504. Previously a 500 on a poll or the worker
  heartbeat was returned from `run`/`run_until` once `WorkerRetryPolicy.max_retries`
  was spent, and a 500 on an activity report was not resent. A typed 5xx
  refusal (another `reason`), `retryable: false`, 401, 403 and other 4xx stay
  fatal as before.
  - Worker retries now honor the Server's `Retry-After` header on a 429 or 5xx,
    as delta-seconds or an HTTP-date, capped at 30 s; the jittered backoff
    applies when it is absent.
  - A workflow-task completion or failure that hits a transient failure is
    resent unchanged (same task, lease owner and attempt) within
    `WorkerRetryPolicy.max_retries`, instead of being dropped until the task's
    lease expired. A definitive refusal of a resend ends no worse than not
    resending: the worker keeps running and the Server redelivers the task.

- `WorkflowContext::replay_now()` is available at ordinary workflow entry,
  before any durable step. It returns the `replay_clock` instant (`WorkerReplayClock`
  v1) that Server 3.6.0+ sends on workflow tasks: the run's own start, or a
  redrive successor's original start. The value is pinned to the run's stored
  rows, so every replay reads the same instant. A task without a valid carrier,
  or whose carrier names another run or disagrees with the history's start kind,
  keeps entry unavailable as before. A replayed-instance query or replayed update
  validator rebuilds the body with its own task's `replay_clock` attestation:
  the v1 object is used as on a workflow task, and an explicit `null` or an
  object refused for its own shape, schema, time or source keeps entry
  unavailable. An object refused only because it names another run than the
  task, or because the task has no run, is treated as an absent key and fails
  closed. When the query task omits the key (a Server before #1338), or its
  object is treated as absent, and the rebuilt body reads `replay_now()` at
  entry, the query fails with `query_workflow_state_unavailable` (a validation
  task reports a worker failure), even if the body tolerated the unavailable
  result. `WorkflowHistory::replay_clock(carrier)` validates and
  supplies the carrier to offline replay. Update handler reads and the
  cancellation clock are unchanged.
  ([Durable Workflow #215](https://github.com/durable-workflow/.github/issues/215))

  Upgrade note: `replay_now()` shipped in 4.24.0 and returned
  `Error::ReplayClockUnavailable` at ordinary entry. A running workflow whose
  code branches on that error at entry (for example with `.ok()` or
  `unwrap_or`) now takes the other branch on replay once its tasks carry the
  instant. Put such a change behind workflow versioning before upgrading.
  Until Server #1338 (in the next Server release) sends `replay_clock` on
  query and update-validation tasks, a query or validation of a workflow that
  tolerates the unavailable result at entry now fails where it used to answer,
  against every current Server release and against PHP Servers that send no
  carrier. The public `execute_query_task`, which has no wire task, counts as
  key-absent in the same way.

- Workers with cooperative cancellation enabled advertise the
  `timeout_cleanup` capability, version 1. A native Server can then admit
  starts that reserve time for deadline-triggered timeout cleanup on their
  queue. When workflow code lets the delivered timeout
  `Error::CooperativeCancellationRequested` escape, uncaught or returned
  unchanged, the `fail_workflow` command carries
  `failure.interruption: "timeout_cleanup"`, and the Server records the cleanup
  as completed. Other failures, including wrapped interruptions and saga
  compensation failures, still record failed cleanup. Workers without
  cooperative cancellation are unchanged. `timeout_cleanup` is now reserved
  and cannot be added as a resource capability.
  ([Server #1201](https://github.com/durable-workflow/server/issues/1201))

- Workers accept ordered interceptors around workflow tasks, activities and
  queries with `Worker::interceptor` and `Worker::interceptors`. The first
  interceptor registered is the outermost. Contexts carry the worker, task
  queue, namespace and task identity; workflow-task contexts also expose
  replay state. Workflow-task interceptors see a read-only view of the
  commands: the worker always submits the commands the workflow produced and
  reports its own failure, which interceptors see as a read-only
  `WorkflowTaskFailure` (kind, message and debug text). An interceptor error
  after a successful task, or skipping `next`, fails the workflow task without
  recording anything. With cooperative cancellation, a replay an interceptor
  drops partway is not reported: the poll returns the error and the task's
  lease expires. Activity interceptors may transform arguments and results.
  Built-in execution telemetry stays outermost. See
  `examples/audit_interceptor.rs`.
  ([Durable Workflow #226](https://github.com/durable-workflow/.github/issues/226))

- Workers can register update validators with
  `Worker::register_update_validator` (or the Avro and replayed-state
  variants). The Server runs a validator before accepting the update; an
  `UpdateRejection` rejects it with reason `update_validator_rejected`, so the
  handler never runs and no acceptance is recorded. Validators are read-only:
  one that adds a workflow command is refused. A worker with validators
  advertises `update_validators` and `update_validation_tasks`, refuses a
  Server without synchronous update validation, and leases validation tasks
  through its multiplexed workflow poll, recorded as worker task metrics with
  `task_kind` `update_validation`. Inside the worker a refusal is
  `Error::UpdateRejection`. `Error::update_validator_rejection()`
  and `Error::is_update_validation_failed()` tell a rejection from a failed
  update. Low-level registration now accepts a list of validator names.
  ([Durable Workflow #234](https://github.com/durable-workflow/.github/issues/234))

## 4.24.0

- When a compatible Native Server enables ordered condition authoring, timed
  waits evaluate signal and successful update effects in durable receipt order
  before the original timer. In a selection, the first qualifying prefix
  competes with the original terminal event from a child or child-only group.
  Replay preserves the committed result.
  ([Durable Workflow #230](https://github.com/durable-workflow/.github/issues/230))

## 4.23.0

- Workers can request `ContinueAsNewTarget::Promoted` after Server confirms
  support for the current registration. Plain continue-as-new keeps the
  predecessor's build. If the queue has no eligible promoted build, the worker
  reports the refusal on the current attempt so the workflow task can retry.
  (durable-workflow/workflow#880)

## 4.22.1

- Replay mismatches from direct activity, local activity and timer calls report
  the authored filename and line where the future was created. The existing
  replay reason and sequence remain available.
- Require `url` 2.5.9 or later when resolving the SDK's HTTP dependencies
  ([RUSTSEC-2026-0592](https://rustsec.org/advisories/RUSTSEC-2026-0592.html)).

## 4.22.0

- A failed cluster-info read no longer stops a running worker. Startup
  discovery (query-task authority, Nexus cooperation) and the inline-payload
  policy read before a report retry a connection failure, timeout, 5xx or 429
  with the shared jittered backoff, uniform in `[0, min(4 s, 0.25 s *
  2^(n-1))]`, logging a `tracing` ERROR once the outage has lasted 60 s and
  every 5 minutes after, until it succeeds or the worker stops. An explicit
  `retryable: false`, 401, 403 and other refusals are still returned.
  `Client::cluster_info` and Clients outside a running worker make one attempt.
  When the cached payload policy (refreshed every 60 s) expires during such an
  outage, reports keep using the last-known policy and discovery is tried
  again 10 s later; only a worker that has no policy yet waits.
- An activity report the Server refuses as too large (413 `payload_too_large`)
  because it lowered its inline limit since the policy was cached is no longer
  lost. The worker forgets the policy, rediscovers it, encodes the same report
  again (externalizing payloads the new limit requires) and resends it once.
  A result that is still too large (refused again, above the upload limit, or
  with no payload storage) no longer stops the worker: the attempt is failed
  as a non-retryable `ActivityResultTooLarge` (`failure.type` and
  `failure.class`), as in the PHP SDK, and the worker keeps running.

- **BEHAVIOUR CHANGE: `Worker::recover_transient_outages` now defaults to
  `true`.** `run` and `run_until` keep running through transient Server
  outages instead of returning the error once `WorkerRetryPolicy.max_retries`
  is spent. Call `.recover_transient_outages(false)` to restore the previous
  bounded behavior.
- A running worker no longer exits on a transient Server outage (the Rust
  side of sdk-php#448). Native Server answers any storage failure, including
  warm-up, with `503 {"reason":"storage_unavailable"}`; after the default five
  retries `run` and `run_until` used to return that error. Transient failures
  are now a connection refused, reset or timed out; an upstream 502/503/504
  without a protocol body; a 503 with reason `storage_unavailable` or
  `backend_unavailable`; or any 5xx or 429 with `retryable: true`.
  `Worker::recover_transient_outages` is now on by default: poll and heartbeat
  retries continue past the retry budget for transients (a poll keeps its
  request identity) with the same fully jittered schedule rather than the
  fixed `max_backoff` delay, so workers do not retry in step; an outage that
  lasts 60 s is logged as a `tracing` error, then every 5 minutes, and a transient that escapes a poll lane, a task
  settlement or registration is logged with `tracing` and retried after a
  jittered wait, uniform in `[0, min(4 s, 0.25 s * 2^(n-1))]`. With the option
  on, only transients retry past the budget; an untyped 500 or a 429 without
  `retryable: true` is now returned once the budget is spent. An explicit
  `retryable: false`, 401, 403 and other refusals stop the worker with a
  `tracing` error event. `.recover_transient_outages(false)` restores the
  previous bounded behavior. 401 and 403 stop the worker, as in the PHP SDK;
  the Python SDK's pollers instead retry them so credentials can rotate.
- An activity completion, failure or `worker_lost` release that hits a
  transient failure is no longer lost. The worker resends the identical report
  with the same jittered backoff while the lease lasts, then logs a `tracing`
  error and leaves the attempt to the Server. A resend after the Server had
  already committed the first copy is answered by native Server 3.17 with
  `409 {"reason":"stale_attempt","recorded":false}` for this task, attempt and
  owner with every status `completed` (or `attempt_status` `failed`); the
  worker counts that as its own recorded outcome. Verified against the
  published `durableworkflow/server:3.17.0` image: the run completed with
  exactly one `ActivityCompleted`.

- A timeout, transport error, 429 or 5xx on the activity lease renewal made
  before the callback starts no longer drops the claimed task (#302, the Rust
  side of sdk-php#445). Previously the Worker gave up on the claim without
  completing, failing or releasing it, so the Server held the lease until it
  expired and, with one attempt, failed the workflow. The callback now starts
  under the poll's original grant, and lease maintenance retries the renewal
  after a fully jittered wait, uniform in `[0, min(4 s, 0.25 s * 2^n)]` after
  the n-th consecutive failure, the same schedule as the other SDKs. Each
  attempt spends at most half of the remaining grant. A delivered lease is now
  measured from the poll's start rather than its reply. A refusal or a changed
  claim identity still ends the claim before the callback starts. Recorded
  history and replay are unchanged.
- A claim whose renewal does not recover is now released explicitly instead of
  held silently until the lease expires. The Worker keeps a reserve at the end
  of the lease (at most 10 s and at most a fifth of the grant). Once renewal
  has failed and only that reserve is left, at start or later, it drops the
  callback future. Only after the callback has confirmed stopped, and only
  while more than a margin of the lease (at most 1 s) is left, it fails the
  attempt with `failure.type` `worker_lost`, `failure.class`
  `ActivityWorkerLost`, `non_retryable: false` and a message naming the local
  cause. A callback that blocks past that point is not released; the Server
  records `lease_expired`, so one attempt never runs twice. The Server's retry
  policy then acts at once, and `worker_lost` can be listed in
  `non_retryable_error_types`. A Server refusal such as `lease_expired`, as
  well as expiry and Worker shutdown, still ends the claim without a report.
  The default retry policy is unchanged.
- `ActivityFailure::lease_loss_reason()` returns a `LeaseLossReason`
  (`LeaseExpired`, `WorkerDeregistered` or `WorkerLost`, with `as_str()` giving
  `lease_expired`, `worker_deregistered` or `worker_lost`; the enum is
  `#[non_exhaustive]`) when an attempt lost its worker, and `None` otherwise.
  `WorkerLost` is a Worker release (`exception_type` `worker_lost`). For a
  lease the Server settled (`ActivityLeaseExpiredException`) it is the
  Server's recorded `failure_reason`, `LeaseExpired` by default. The Server
  records that reason beside the exception, so replay now copies it into
  `ActivityFailure::exception` as `failure_reason`, creating a minimal
  `{class, failure_reason}` descriptor when the event recorded no exception.
  No existing type changes.

- Worker egress is much smaller against a Server that supports it (#302).
  Requests now send `Accept-Encoding: gzip` and decode compressed answers
  (reqwest's `gzip` feature; native Server compresses JSON answers of 1 KiB or
  more, server#1261). Worker-plane requests also send
  `X-Durable-Workflow-Server-Capabilities-Digest` with the digest of the
  capabilities this Client holds, or `none` (server#1262). The Server then
  replaces each repeated ~17 KB `server_capabilities` copy with that digest,
  or tags a full copy with a `digest` member when the digest changed. The SDK
  captures the digest and object when it sends each request and restores that
  request's answer, success or error, at the top level and up to two objects
  deep, before anything reads it. A tagged copy is stripped of `digest` and
  used for later requests, so overlapping requests across a rolling deploy each
  restore their own capabilities. A digest string that matches neither the
  request's digest nor a tagged copy in the same answer makes the Client
  forget its held digest. Only a task poll, idempotent by its poll request ID,
  is resent once with `none`. Any other request (a completion, failure,
  heartbeat, start or signal) is never resent: its answer is kept as received,
  because a completion's answer can carry the next leased task, and the full
  capabilities are read with `GET /api/cluster/info`
  (`worker_protocol.server_capabilities`) and put in place of the unknown
  digest. If that read fails, the answer is still used without the field and
  later requests learn the capabilities again; the request never fails. A
  poll whose resend names an unknown digest again is `Error::Protocol` with
  reason `server_capabilities_digest_unknown`.
  Decoded response bodies are capped at 128 MiB by default
  (`ClientBuilder::max_response_bytes`); a larger body stops being read and
  fails with the new `Error::ResponseBodyTooLarge`, so a small gzip answer
  cannot expand without bound. Older
  Servers ignore both headers and answer in full. In a lab run of one
  start, one 1 KiB activity and one result per workflow against native Server
  main (43ad8d8f), response bytes per workflow fell from about 159 KB to 8.8 KB
  for the worker and from 19 KB to 4.7 KB for the caller.

## 4.21.0

- `Client::signal_with_start` starts a workflow or reuses its open run and
  records one signal on it in a single Server transaction, so the run cannot
  close between a separate start and signal (durable-workflow/.github#222,
  server#1133). It needs native Server 3.15.0 or later, the first release
  that advertises `capabilities.signal_with_start`, and returns
  `Error::SignalWithStartUnavailable` before sending otherwise.
  `SignalWithStartOptions` carries the start deadlines, `OpenRunPolicy`
  (`UseExisting` by default, or `Fail`), `ClosedRunPolicy` (`Reject` by
  default, or `StartNewRun` to start a fresh run after a closed one), the
  signal's command key (generated as `sdk-rust-signal-with-start-<uuid>` when
  omitted) and a business key. A retry with the same key returns the original
  receipt, even after the run closed. `Error::signal_with_start_refusal()`
  names the Server's reason for every refusal; nothing was written for any of
  them.
- Workflow code can signal another workflow durably with
  `WorkflowContext::signal_external_workflow(workflow_id, signal_name, args)`
  (add `.run_id(..)` to target one run), the Rust slice of
  durable-workflow/.github#223. It emits the `signal_external_workflow` command
  and resolves to an `ExternalSignalResult` read from the sender's own history
  (`ExternalSignalRequested`, then `ExternalSignalDelivered` or
  `ExternalSignalFailed` at the same sequence, Server #1134). A failed delivery
  is a returned value with `outcome` `TargetNotFound`, `TargetClosed` or
  `Rejected` and a stable `reason`, not an error. Replay never sends the signal
  again; a different target, run, signal name or arguments at the recorded
  sequence is `Error::NonDeterministicReplay`. The Worker reads
  `signal_external_workflow` from each workflow poll's `server_capabilities`,
  which native Server 3.15.0 and later advertise; without it the call returns
  `Error::SignalExternalWorkflowUnavailable` before anything is submitted, and
  a workflow that returns that error fails its workflow task, not its run. An
  invalid target or signal name (a blank name, a control character, more than
  191 characters or 255 bytes, as the Server refuses) returns
  `Error::InvalidExternalSignal` in workflow code before any command is
  emitted.

## 4.20.0

- A signal or update refused with HTTP 409 `request_id_ambiguous` is now typed:
  `Error::is_request_id_ambiguous()` and `Error::request_id_ambiguous()` return
  a `RequestIdAmbiguous` with the sent key, even when the Server does not echo
  it (durable-workflow/.github#216). The Server answers this when a key
  retained before it compared intent matches more than one command. Nothing
  was written and no receipt was selected. It is never resent and is not a
  `request_id_conflict`. `Error::Http` matching is unchanged.
- `Client::update_workflow_receipt` and `WorkflowHandle::update_receipt` send
  the same update as `update_workflow` and return the full receipt, including
  `retained` and `intent_verified`. A legacy readback (`retained: true`,
  `intent_verified: false`) is not a verified match, and the result-only
  methods cannot show that. A pending update is returned as its receipt.
- A workflow-task completion the Server refuses because recording it would
  exceed its history-transaction budget (HTTP 409 with reason
  `history_transaction_limit_exceeded` or `completion_budget_exceeded`) now
  returns `Error::WorkflowTaskHistoryLimitExceeded`, carrying the Server's
  `limit_kind`, measured `current_value` and `configured_limit` (native Server
  3.13.0 names all three). A `Worker` never resends that completion: it
  completes the same task once with a non-retryable `fail_workflow` command
  whose message starts with `Structural limit exceeded: <limit kind>:`, so the
  run fails with failure category `structural_limit`. Before, the worker loop
  surfaced the HTTP 409 and the run never progressed. This happens only when a
  Server's `DW_V2_LIMIT_HISTORY_TRANSACTION_BYTES` (or its event or row budget)
  is below what a completion records; native Server 3.13.0's default budget is
  four times the completion size limit (durable-workflow/.github#228).
- `testing::WorkflowEnvironment::replay` accepts a history that closed with
  the structural-limit `WorkflowFailed` a Worker records for a refused
  completion (413 or 409). The refused commands were never recorded, so replay
  reports them with `completed: true` instead of an `unrecorded_command`
  divergence. Any other recorded failure is checked as before.

## 4.19.0

- `WorkflowContext::schedule_origin()` returns the schedule fire that started
  the run as a `ScheduleOrigin` (`schedule_id`, `occurrence_time`,
  `trigger_number`), or `None` for a run no schedule started
  (durable-workflow/.github#243). It reads the run's `ScheduleTriggered`
  history event, so it is available on the first workflow task and identical
  on replay. `occurrence_time` is the time the scheduler evaluated (the
  jittered fire time for a tick, the enumerated time for a backfill), not the
  run's start time, and is `None` for a manual trigger. Histories without the
  event read `None`, so existing runs replay unchanged.

## 4.18.0

- The default `msrv-1-86` feature retains the existing compiler-compatibility
  dependency pin. Modern hosts can disable it to compose the SDK with native
  Server's newer official Avro dependency. Rust 1.86 consumers that disable
  default features must explicitly enable `msrv-1-86`; an unpinned fresh graph
  can require a newer compiler.

- Service activity contexts expose negotiated, immutable acknowledged-heartbeat
  checkpoint details and a typed acknowledgement time. Unsupported claims stay
  unavailable, separately from a supported claim with no checkpoint. Heartbeat
  writes do not optimistically advance the received snapshot.

## 4.17.0

- Workflow code can use `ctx.logger()` to send new-execution observations to
  the application's `tracing` subscriber with namespace/workflow/run identity.
  Committed replay, query reconstruction and discarded invocations stay quiet.
  `ctx.replay_state()` exposes the live consumed-history boundary. Logs from
  uncommitted task attempts may repeat on retry; handlers must be nonblocking.

## 4.16.0

- Completion payload planning reads the optional advertised
  `limits.max_workflow_task_completion_bytes` separately from ordinary request
  and per-value limits. Individually valid inline batches no longer demand
  unavailable external storage when the Server advertises this allowance and
  storage is explicitly unconfigured, disabled or unsupported. Missing fields
  retain the conservative ordinary cap; malformed fields refuse completion
  planning before uploads. Server #1136 publication and installed fan-out and
  over-limit qualification remain required for delivery of SDK #254.

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

  **Known transport limitation:** this typed failure requires the Server's
  HTTP 413 to reach the worker. An aggregate over-limit request can currently
  lose that response and return a transport error instead. Automatic
  structural failure for that path is not yet qualified. The Server response
  correction and published-artifact controls remain open in
  [the fan-out issue](https://github.com/durable-workflow/.github/issues/228).

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
