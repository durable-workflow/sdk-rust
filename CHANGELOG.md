# Changelog

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
