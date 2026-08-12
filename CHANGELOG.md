# Changelog

## 2.0.0-rc.12

- Add a task-oriented Rust documentation landing page that leads with the
  supported Cloud quickstart before the generated API reference.
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
