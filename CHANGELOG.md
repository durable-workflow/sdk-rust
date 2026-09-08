# Changelog

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
