# Installed Rust command-key controls

`tests/command_keys_server.rs` exercises the Rust part of
[shared #216](https://github.com/durable-workflow/.github/issues/216). Copy the
file unchanged into a registry-only consumer. It uses the public SDK, official
Avro encoding and a real Worker. No local SDK patch is allowed for published
acceptance.

Use a disposable, bootstrapped Server with synthetic data and credentials,
`capabilities.command_request_keys`, and a short update wait limit
(`DW_V2_UPDATE_WAIT_COMPLETION_TIMEOUT_SECONDS=1`). Bind it to a numeric
`127.0.0.1` port. The test creates workflows and a namespace; never point it at
a shared or production runtime. The runtime owner removes its container and
data afterward. The Worker uses a 10 ms poll timeout and a bounded shutdown.

From the repository root, set `SDK_VERSION` to the exact published version
being qualified, `COMMAND_KEY_CONSUMER` to an empty owned directory, and the
three runtime variables below. Then create the consumer:

```sh
cargo init --lib --name command-key-consumer "$COMMAND_KEY_CONSUMER"
cargo add --manifest-path "$COMMAND_KEY_CONSUMER/Cargo.toml" \
  "durable-workflow@=$SDK_VERSION" serde_json tokio \
  --features tokio/macros,tokio/rt-multi-thread,tokio/sync,tokio/time
mkdir -p "$COMMAND_KEY_CONSUMER/tests"
cp tests/command_keys_server.rs "$COMMAND_KEY_CONSUMER/tests/"
export DURABLE_WORKFLOW_LOCAL_ISOLATED=1
export DURABLE_WORKFLOW_SERVER_URL=http://127.0.0.1:8080
export DURABLE_WORKFLOW_AUTH_TOKEN=synthetic-fixture-token
CARGO_BUILD_JOBS=2 cargo test --manifest-path "$COMMAND_KEY_CONSUMER/Cargo.toml" \
  --locked --test command_keys_server \
  retained_command_keys_preserve_identity_and_execution_fences -- --ignored --exact
```

Verify the consumer lock uses the official registry and its expected archive
checksum. Record the package/source, Server index and executed platform digest,
runner commit and file hash, timestamps, terminal result and cleanup in #216.

The direct control covers four concurrent explicit signal/update retries,
four racing first submissions on one key, generated-key recovery, changed
name and run-target conflicts,
typed integer-to-boolean intent conflicts with unchanged history, typed pending
receipts, closed-run receipt recovery, continue-as-new retained identity and
historical-run refusal, and foreign-namespace refusal with distinct local
workflow identities. It does not assert a fresh receipt's optional `retained`
marker. During a running workflow, the fence assertion compares durable command
events separately from normal worker bookkeeping.

The separate `unicode_255_character_signal_key_is_accepted` test retains a
255-character key (2-byte code points) across four concurrent retries, refuses
changed input under it, and refuses 256 characters before sending. Native
3.9.1 failed it; 3.9.2 and later pass.

## One installed cohort

`run_cohort.sh` runs the whole installed Rust cohort against the exact
published Server and crate it names: the two direct tests above in a fresh
registry-only consumer (with `run_direct.py` owning one capped Server), then
the bound adapter cells in `run_open.py`: postcommit response loss through the
accepted shared proxy, synthetic legacy readback for open, completed and
continued runs, the `two-rows`, `legacy-token` and `modern-raw` ambiguity
shapes, and case-only keys. Run it as uid 1000 (the Server image user) with
`COHORT_ROOT`, `RUST_IMAGE`, `LOSS_FIXTURE` and an owned `CONTAINER_PREFIX`.
`.github/workflows/published-command-keys.yml` runs it on a hosted runner.

The physical 3.8 upgrade cell (`--physical-*`) is not part of it: it needs the
producer's frozen backup and seed receipts.

Passing the direct test alone does not complete #216 or prove exactly-once
activity side effects.
