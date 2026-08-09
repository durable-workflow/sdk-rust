# Changelog

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
