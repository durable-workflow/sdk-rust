# One-activity worker latency

`worker_latency.rs` runs an echo activity in a workflow with a 1 KiB string.
It uses 16 workflow slots and 16 activity slots. The driver warms up with five
sequential workflows, then measures twenty, checking every typed result and
polling for results every 5 ms. It prints samples, p50/p95 and SDK route metrics.
`SDK_BENCH_CAPACITY` can select 1..16 slots for a separate poll-topology
diagnostic. Report that capacity explicitly; the default comparison uses 16.

Use a disposable Server and database. Set `DURABLE_WORKFLOW_SERVER_URL`,
`DURABLE_WORKFLOW_TOKEN` to fixture authentication, and a unique `TASK_QUEUE`.
Build with `cargo build --release --example worker_latency`, start
`target/release/examples/worker_latency worker`, then run the same binary with
`drive`. Stop the worker with SIGINT. It also stops itself after three minutes.
Clean up the Server, database and fixture authentication afterward.

To compare published and candidate SDKs, copy this exact source into two small
binary crates. Both manifests use the same exact registry dependency, Tokio
and serde_json. Only the candidate adds a crates.io patch pointing to the clean
candidate checkout. Use the same release profile, toolchain, dependency lock,
host and disposable runtime. Verify the baseline's registry checksum and the
candidate's source commit. Interleave samples and record the exact Server image,
database settings, resource limits and cleanup with the results. Source builds
do not replace packaged or published release acceptance.

`tests/worker_latency/python_worker.py` supplies the same workflow and activity
through an independently installed, exact published Python SDK. It uses the same
environment variables and Rust driver. Record that wheel's checksum and verify
installed package files before using it as a reference.

Route metrics include startup and idle long polls. Distinguish those from
measured workflow durations, and separate the initial payload-policy discovery
from activity lease capability discovery. A lower request count alone does not
establish the cause of an end-to-end latency difference.
