# Grouped condition Worker consumer

This consumer exercises the SDK source in this checkout through the public
Client and Worker APIs against an isolated Server. It uses ordinary protocol
1.19 with cooperative cancellation disabled. It is source qualification, not a
published-artifact conformance claim.

The original published baseline consumer and lockfile are retained in
Workflow PR #602. This extended consumer keeps the historical package name and
adds final completion and timeout checks.

Set `DURABLE_WORKFLOW_SERVER_URL` to the isolated Server, configured with the
disposable token `test-token` and namespace `default`. Build and execute in a
Rust 1.86 container as UID/GID 1000:1000:

```sh
cargo build --locked --manifest-path tests/fixtures/grouped-condition-worker-consumer/Cargo.toml --target-dir target
target/debug/workflow-601-published-repro
```

Each workflow claim is executed by a new subprocess. The five cases are a
scalar condition, nested parallel condition, keyed selection condition,
selection deadline and nested parallel deadline. A first insufficient vote
must reopen the same authored occurrence. Finishing other nested members must
leave its condition pending. A second vote or the condition deadline must then
produce `WorkflowCompleted`, with no extra physical reopen after a true
predicate. Timeout cases require the durable condition timer fire.

The program prints one result per case and exits nonzero on failure. Run only
against a disposable stack and remove its data after retaining the result in
the owning issue or PR.
