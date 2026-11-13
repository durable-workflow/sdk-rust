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
typed integer-to-boolean intent conflicts with unchanged history, typed pending
receipts, closed-run receipt recovery, continue-as-new retained identity and
historical-run refusal, and foreign-namespace refusal with distinct local
workflow identities. It does not assert a fresh receipt's optional `retained`
marker. During a running workflow, the fence assertion compares durable command
events separately from normal worker bookkeeping.

The separate `unicode_255_character_signal_key_is_accepted` test expresses the
accepted character-count contract. It is expected to fail on native 3.9.1
under [Server #1207](https://github.com/durable-workflow/server/pull/1207).
Run it separately and record that outcome as `product-fail`, never as a passed
command-key cohort:

```sh
CARGO_BUILD_JOBS=2 cargo test --manifest-path "$COMMAND_KEY_CONSUMER/Cargo.toml" \
  --locked --test command_keys_server \
  unicode_255_character_signal_key_is_accepted -- --ignored --exact
```

Transport-loss coverage waits for the reviewed shared response-loss fixture.
Legacy upgrade coverage waits for the producer-owned canonical seed. Passing
the direct test alone does not complete #216 or prove exactly-once activity
side effects.
