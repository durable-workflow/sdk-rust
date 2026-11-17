# Resume a bounded batch

`checkpointed_batch.rs` uses the new acknowledged-checkpoint accessor in the
current source. It needs a native Server advertising `activity_heartbeat_resume`
v1. Source compilation and the tests below do not establish a published-package
or worker-loss result. Use the separately reviewed replacement qualifier for
that evidence.

The example processes three items, with a maximum of sixteen. Its activity has
two attempts, one one-second retry delay, a ten-second heartbeat timeout, a
thirty-second start-to-close timeout and a sixty-second schedule-to-close timeout.
Checkpoint recovery does not replace or extend those budgets.

Run against your isolated development Server on Linux. Supply a **new** directory
for each invocation. The program leaves its synthetic provider receipts there
for inspection. It refuses an existing directory.

```bash
export DURABLE_WORKFLOW_SERVER_URL=http://127.0.0.1:8080
export DURABLE_WORKFLOW_TOKEN=your-development-token
cargo run --example checkpointed_batch -- ./receipts-before before-commit
cargo run --example checkpointed_batch -- ./receipts-commit after-commit
cargo run --example checkpointed_batch -- ./receipts-checkpoint after-checkpoint
```

The first attempt fails once at the selected boundary. The replacement uses
`last_heartbeat_checkpoint()` and typed details to obtain its next item. Missing
resume support fails explicitly. A supported empty checkpoint starts at zero.
The checkpoint also binds the complete business input, so a cursor from another
batch or outside this batch is refused.

| Failure boundary | Provider state | Replacement action |
| --- | --- | --- |
| Before the first commit | No receipt | Commit item zero, then acknowledge progress |
| After commit, before acknowledgement | Item zero committed, no checkpoint | Reconcile its stable business key, then acknowledge progress |
| After acknowledgement | Item zero committed and checkpoint says next item one | Continue at item one |
| Heartbeat response lost | Checkpoint may or may not have committed | Use the next claim's checkpoint and reconcile any repeated operation |

The receipt directory models a provider's durable operation lookup. Each item
uses the same application business ID and item index across retries, rather
than the attempt ID. Every receipt binds the complete batch fingerprint as well as the item. A repeated
key must contain the identical receipt, even when no checkpoint was received.
Appending, truncating or reordering input under that business ID refuses. Changed
input or a partial receipt refuses and requires reconciliation; the example
never overwrites it or assumes an uncertain effect failed. Files and directory
entries are synchronized before a new receipt is considered committed. This is
a synthetic provider, intended for a private local directory, not an integration
with a payment or other real external service.

The application acknowledges only after the provider reports commit or a matching
receipt. It stops if heartbeat acknowledgement is missing or the attempt must
stop. The received SDK snapshot stays immutable. A lost heartbeat response can
follow a durable Server commit, so a later attempt must trust its received
checkpoint and still reconcile repeated business keys.

In a real integration, replace `ReceiptStore` with the provider's idempotency,
operation lookup and fencing contract. A checkpoint does not atomically commit
an external effect, and this example does not promise exactly-once effects.
Retain the business ID when restarting the same business operation in another
workflow. Separate new business operations need new IDs.

The following tests run without a Server. They cover the three failure boundaries,
both outcomes of a lost acknowledgement response, conflicting/partial receipts
and invalid checkpoint/input bounds. They simulate activity failures rather than
proving real process loss or Server checkpoint persistence.

```bash
cargo test --locked --example checkpointed_batch
```
