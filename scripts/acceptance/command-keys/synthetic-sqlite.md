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
