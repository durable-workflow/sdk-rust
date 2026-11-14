# Activity checkpoint replacement acceptance

`activity_checkpoint_server.rs` is an ignored integration qualifier for the
`activity_heartbeat_resume` v1 accessor. Compilation and its inert observer
tests do not establish an installed result. Run the ignored parent only after
the exact test, package, producer, compiled executable and isolated caller have
passed their required reviews.

Use an ordinary registry-installed SDK containing the accessor, without a path,
Git or patch override. Copy this test unchanged into that consumer. Retain its
lockfile, downloaded crate checksum, test hash and compiled parent hash. The
qualifier executes children from the running Linux parent image and verifies
their SHA-256, length, device and inode before constructing a client.

The reviewed caller supplies `DURABLE_WORKFLOW_LOCAL_ISOLATED=1`, a numeric
loopback `DURABLE_WORKFLOW_SERVER_URL`, a synthetic fixture token in
`DURABLE_WORKFLOW_AUTH_TOKEN`, and the selected published native version in
`DURABLE_WORKFLOW_EXPECT_SERVER_VERSION`. It owns the exact immutable runtime,
disposable database, bounded resources, evidence and cleanup. This test does not
start a Server or select an image.

```bash
cargo test --locked --test activity_checkpoint_server \
  published_cold_replacement_preserves_acknowledged_activity_checkpoint \
  -- --ignored --exact --nocapture --test-threads=1
```

The parent runs acknowledged and empty-checkpoint cases in separate workflow
runs. For each it creates three cold Worker processes with the same compiled
definition. Attempt 1 reports its claim, acknowledges typed progress when that
case calls for it, then stays pending until the parent kills and reaps it.
Attempt 2 receives the retry claim. While it remains live, the parent tries the
dead attempt's heartbeat and requires a producer refusal rather than accepting
a transport error as proof. Attempt 2 then reports one retryable failure.
Attempt 3 must receive the same original checkpoint, proving the stale write
did not replace it, and completes the activity and workflow.

Assertions bind workflow/run/execution identity, distinct attempt IDs and PIDs,
typed details, exact parsed acknowledgement time, the complete received retry
policy and its original timeout values, one authored activity, three starts,
two retries, one completion and the retained committed-history prefix. The
empty case also checks that the earlier run's checkpoint does not leak into a
new execution. Child output is bounded while being read. Timeouts and failed
assertions kill and reap only children returned by this parent's `spawn`.

Retain the structured `ACTIVITY_CHECKPOINT` observations and exact package,
runtime, configuration, process and independent cleanup evidence. A failure or
runner refusal remains NONPASS. This qualifier does not test unavailable older
producers, a committed write with a lost response, checkpoint pruning, embedded
PHP parity or external business-effect reconciliation. Those claims need
their own evidence under shared #218. A checkpoint never atomically commits an
external business effect.
