# Stopped-copy synthetic legacy helper

`sqlite_legacy.py` is a portable stdlib-only helper for the accepted
[producer manifest](https://github.com/durable-workflow/server/blob/d66e33bfb5c76f94c5b202f8c9f7a9e8cd75cf1c/resources/platform-protocol-specs/command-key-legacy-v1-vectors.json).
The adjacent JSON is byte-identical to that source, bound by SHA-256. The helper
executes only its two transformation statements on selected current-producer API
admissions. It does not create an old-image seed or perform a physical upgrade.

The outer runner owns an isolated synthetic Server and a private disk directory.
It must stop its exact Server/process, check the terminal stopped state and
checkpointed SQLite files, then call `transform_copy` with `stopped=True`.
Source and destination must be distinct sibling paths in that owned directory
with mode 0700. The destination must not exist. The source must be an owned
regular single-link file; nonempty WAL/journal files and sidecar symlinks refuse.
The source is opened read-only and immutable only after those preconditions.
The original database is never a mutation target.

Each selection contains exactly these nonempty string fields, from the real API
admission and the producer manifest:

```python
selection = {
    "command_id": original_command_id,
    "command_type": "signal",  # or "update"
    "workflow_instance_id": original_workflow_id,
    "workflow_run_id": original_run_id,
    "caller_key": original_caller_key,
    "legacy_request_id": manifest_selected_raw_key,
}
receipt = sqlite_legacy.transform_copy(
    stopped_source, disposable_copy, [selection], stopped=True,
)
```

The copy uses SQLite's backup API with a five-second bound. Every selection's
command/type/workflow/run identity, modern stored token and exactly one intent
row must match before either statement executes. One transaction applies the
manifest DELETE and UPDATE, requiring one affected row from each. A complete
typed logical snapshot of every table row and schema proves that only the
selected request key and intent row changed. An unexpected trigger side effect
or failed precondition rolls back and removes only the newly created copy.
An existing destination is never overwritten. The retained original's file
identity and logical contents must stay unchanged during copying.

The limits are 64 MiB source files, 20,000 total rows, 32 MiB encoded snapshots
and at most 32 distinct selections. The default receipt contains the synthetic
cell label, manifest digest, transformation count and before/after snapshot
digests. `snapshot` returns complete private logical values for the outer
runner's zero-write checks; publish only sanitized identities and outcomes.

Run the inert controls without a Server, Docker or a toolchain:

```sh
python3 -B -m unittest discover -s scripts/acceptance/command-keys -p 'test_*.py' -v
```

These minimal SQLite unit fixtures verify the helper boundary. They are not an
installed SDK result, a physical old-image rehearsal or evidence of response
loss. An actual synthetic cell still needs real current-producer API admissions,
the exact published SDK/Server tuple, restart/readback/zero-write evidence and
owned cleanup. Physical upgrade and post-commit loss remain separate cells.

## Reproduce an installed state cell

`run_open.py` is a one-shot Linux amd64/SQLite control. Prepare the
[registry consumer](consumer/README.md) using the explicit builder below, from
a committed reviewed tree. The builder copies only the committed Cargo manifest,
lock and adapter into a private directory, checks their hashes before and after
building, and downloads the official SDK archive into a fresh Cargo home. It
verifies that archive's lock checksum and VCS source identity. No host Cargo or
Docker configuration, registry credentials or dependency cache is mounted.

Check machine capacity and select an already present official Linux amd64 Rust
image by immutable local image ID. The single build container uses two CPUs,
3 GiB, 512 PIDs, a read-only root and source, private writable Cargo/target
directories, and a fifteen-minute timeout. Cargo runs with two jobs.

```sh
python3 scripts/acceptance/command-keys/build_consumer.py \
  --work-dir "$CONSUMER_BUILD_DIR" --rust-image "$RUST_IMAGE_ID"
```

This writes `consumer` and a private `build-receipt.json`. The receipt binds
the exact Git input tree, build recipe, image, toolchain, lock, SDK archive/source
and executable hash plus filesystem identity. The runner requires that receipt,
checks its input hashes against the committed source, opens the owned single-link
executable once, and executes its held descriptor. It rechecks the descriptor and
original path before and after every call and before admitting the final receipt.
Replacing even an identically hashed executable inode refuses qualification.

Select an already qualified exact Server index/source/version and ensure the
image is present locally. Check current machine capacity and announce the run
before launching it. The runner owns at most one Server at a time, capped at
1 CPU, 1 GiB and 256 PIDs, with an init process, read-only root, private SQLite
bind, tmpfs for image-declared volumes, loopback ephemeral HTTP port and fresh
synthetic credentials. It verifies image labels, architecture, live version
and the command-key capability. It performs no build or package installation.

Supply the selected exact identities and an absolute, nonexistent owned output
directory:

```sh
python3 scripts/acceptance/command-keys/run_open.py \
  --work-dir "$LEGACY_WORK_DIR" \
  --state "$LEGACY_STATE" \
  --consumer-binary "$CONSUMER_BINARY" \
  --consumer-lock "$CONSUMER_LOCK" \
  --build-receipt "$CONSUMER_BUILD_RECEIPT" \
  --sdk-version "$SDK_VERSION" --sdk-sha256 "$SDK_SHA256" --sdk-source "$SDK_SOURCE" \
  --server-index "$SERVER_INDEX" --server-version "$SERVER_VERSION" \
  --server-source "$SERVER_SOURCE"
```

Use `$CONSUMER_BUILD_DIR/consumer` and
`$CONSUMER_BUILD_DIR/build-receipt.json` for the binary and receipt, and the
committed `consumer/Cargo.lock` for the lock. These are local absolute paths.
Publish their hashes and the build command, without publishing private paths or
the complete build receipt. Readiness and cluster probes connect directly to the
exact numeric loopback peer, ignore proxy variables, refuse redirects and require
HTTP 200 with a bounded complete JSON object. The inert controls cover proxy,
redirect, peer, framing, executable replacement and source/lock substitutions.

The control admits real signal/update commands through the installed SDK,
stops its exact owned Server, reads the actual `command_type` catalog, copies
and transforms only the selected admissions, then restarts the same image on
the copy. Four concurrent retries retain unverified legacy identities;
explicit-original-run readback and name/run conflicts preserve them. Both raw
receipt status and high-level SDK pending/signal behavior are checked. Complete
logical database rows/schema must match before and after those calls.
Before the first readback, the restarted baseline fingerprint must equal the
copy helper's transformed fingerprint. A startup rewrite refuses qualification.

The original and copied databases, runtime and generated environment file are
removed in cleanup. The terminal sanitized receipt, data-free catalog DDL and
private complete before/after logical snapshots (mode 0600) remain in the output
directory. Keep this output for at most seven days and publish the lasting
sanitized receipt in #216. Run one state at a time: `open` (the default),
`completed`, or `continued`. Every state uses a fresh owned runtime/database and
has its own named receipt and fingerprints.

The completed and continued setups run the same installed worker through the
held executable descriptor, bounded to eight seconds. The admitted update must
complete with null before the finish signal closes or continues the original
run. The worker must stop cleanly before the Server is stopped and copied.
After restart, closed update readbacks remain completed null with the original
receipt identities. Continuation records a distinct nonempty successor run ID,
while retries and explicit original-run reads retain the original command/run.
The complete database comparison includes the successor, so these readbacks
cannot add executions or durable rows there.

For a separate open-state ambiguity or case cell, add `--scenario` with one
of these accepted manifest shapes. Other states refuse these scenarios before
any runtime effect.

| Scenario | Setup and required result |
| --- | --- |
| `two-rows` | Admit both original and second keys through the API. Transform only the second command to the original raw key. Both command types refuse four concurrent retries and the high-level call with409 `request_id_ambiguous`, retained/unverified/nonretryable markers and no selected receipt. |
| `legacy-token` | Transform the original command to the manifest's literal stored-token string and remove its intent. The original caller key has the same ambiguity refusal. |
| `modern-raw` | Keep the modern original admission unchanged, stop/restart its owned database without a transform, and retry the literal stored-token string. The receipt explicitly reports zero transforms. |
| `case-only` | Transform the exact case-sensitive key, prove exact-byte readbacks and conflicts are zero-write, then submit the differently cased key. SQLite admits a distinct new command for each type. Its admissions are separately recorded and explicitly excluded from the zero-write scope. Other backends are not inferred. |

Ambiguity cells compare all logical rows/schema across the raw and high-level retries.
The case-only cell retains a third private snapshot after distinct admissions
and requires every preexisting command row to remain present unchanged.
Actual physical upgrade and post-commit loss still require their own named
results. Python
optimization is refused because it would disable conformance assertions.

## Run the separate committed-response-loss cell

Obtain the accepted shared
[`committed_response_loss.py`](https://github.com/durable-workflow/.github/blob/117118c83093822469b3824ba9587ff4068ed29f/conformance/command-keys/committed_response_loss.py)
at commit `117118c83093822469b3824ba9587ff4068ed29f`, SHA-256
`bd1e8ccbbf76f63fc2eafa278a023eb181d405b68d9c30016392e395a1eff725`.
Use the same build and runtime arguments above with
`--response-loss-fixture "$ACCEPTED_LOSS_FIXTURE"`, keeping state `open`.
The runner refuses changed source before runtime effects and executes only
the verified source buffer. No proxy is reimplemented or modified here.

This creates a separate fresh runtime and real signal/update admissions through
the registry SDK. The accepted proxy must consume a complete successful upstream
response before closing the client socket. The client must observe a transport
error. Before the first retry, the runner captures the actual committed command
and related update identity from a complete read-only logical snapshot and
checks one corresponding durable history event. Four explicit same-key retries
must retain those identities with verified intent. Integer-to-boolean changed
intent must fail with the typed409 conflict and exact caller key. All rows/schema
and history must remain unchanged across retries/conflict. The fixture retains
only bounded metadata and body hashes.

The terminal `postcommit-response-loss-only` receipt binds the accepted fixture,
public adapter and recorded executable/build identities, original/returned
identities, proxy success/drop metadata and per-type zero-write digests. Private
before/after snapshots remain mode0600 with the same seven-day retention. The
cell performs no SQL transform, physical upgrade or worker execution. It does
not qualify the synthetic legacy cells or the complete #216 cohort.
