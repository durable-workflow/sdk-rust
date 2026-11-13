# Registry-installed command-key adapter

This fixture reads one JSON operation from stdin, calls the official SDK and
writes one JSON result. Its dependency is pinned in `Cargo.toml`; there is no
local SDK patch. The adjacent lock pins the registry dependency graph. Use the
explicit [bounded builder](../synthetic-sqlite.md#reproduce-an-installed-state-cell)
to bind the executed binary to the reviewed inputs and build recipe. Record the
resolved registry checksum, unchanged adapter hash and executed binary hash in
the owning [#216 receipt](https://github.com/durable-workflow/.github/issues/216).

The adapter requires `DURABLE_WORKFLOW_LOCAL_ISOLATED=1`, a numeric
`http://127.0.0.1:<port>` Server origin in `DURABLE_WORKFLOW_SERVER_URL`, and a
synthetic `DURABLE_WORKFLOW_AUTH_TOKEN`. The optional namespace variable is
`DURABLE_WORKFLOW_NAMESPACE`. Inputs are bounded to 64 KiB. The outer runner
owns process timeouts, disposable data, Server stop/restart and cleanup.

Operations use `op` plus the applicable `workflow_id`, `run_id`, `name`,
`request_id`, `queue`, `workflow_type` and `input` fields:

- `register` and `worker` install the same `keys.wait` and `keys.continue`
  handlers. Both wait for `finish`; `keys.continue` continues from `[0]` to
  `[1]`. `touch` and `other` are also declared signals. `mutate` and `other`
  updates return their arguments, or null when `null_update: true` is supplied
  consistently to registration and execution. The worker's `lifetime` is
  bounded to 1–300 seconds, and polling uses 10 ms.
- `start`, `signal`, `signal_run`, `update`, `outcome`, `describe` and `history`
  use the corresponding public high-level SDK methods. Typed pending updates
  preserve their complete receipt; completed null remains an ordinary result.
- `signal_raw` and `update_raw` read the full admission receipt through the public
  `Client::control_plane_request` and `RawRequest` APIs, using the official
  `PayloadEnvelope::avro` encoder. An optional `run_id` explicitly pins the
  original run through its `/runs/{run_id}/...` route. `update_run_raw` is an
  alias. These receipt checks complement the high-level signal/update checks.
  The raw surface preserves non-success HTTP statuses in `value.status` and
  `value.body`; a 409 there is not a `kind: http` high-level error.
- `create_namespace` creates a synthetic namespace through the public raw
  control-plane surface for the namespace fence control.

Output has `kind: ok` and `value`, or a classified `pending`, `http`,
`transport` or `error` result. These are diagnostic fixture records, not an
automatic conformance PASS. Keep raw receipts and history private to the
synthetic run; publish only the required sanitized identities and outcomes.

## Legacy authority cells

Use the producer-owned [legacy manifest at accepted Server commit
d66e33bf](https://github.com/durable-workflow/server/blob/d66e33bfb5c76f94c5b202f8c9f7a9e8cd75cf1c/resources/platform-protocol-specs/command-key-legacy-v1-vectors.json),
Git blob `8a3619209ec758cf01bf6499f221ea6e88c1cad4`, SHA-256
`da1990000c611d7377690c7526487d6415fa4f9cb42d299f59585a7c2eebf769`.
Report each cell separately:

| Cell | Required proof | Current boundary |
| --- | --- | --- |
| Synthetic legacy shape | Exact manifest transaction on a stopped disposable copy of real current-producer admissions; open/completed/continued signal and update readback, original identities, unverified legacy marker, zero-write snapshots, name/run conflicts and ambiguity/case controls | This adapter supplies API calls only; no source or compilation result qualifies the cell. |
| Physical upgrade | Real baseline-image admissions, stopped database/backup identities, one exact schema upgrade, preserved raw keys/history, empty new intent table and installed readbacks | Producer owns the canonical baseline seed/upgrade handoff. Synthetic transforms are not a substitute. |
| Post-commit response loss | Reviewed shared proxy proves a complete committed response before dropping it, followed by explicit same-key retries | Bind the final reviewed proxy source and hash independently. Legacy transforms prove no response loss. |

The full cohort also retains the published producer's Unicode-key limitation
until its separately qualified correction. The direct API control in
`tests/command_keys_server.rs` and this adapter do not close #216 by themselves.

The common offline [SQLite copy helper](../synthetic-sqlite.md) is independent
of this Rust API adapter and can be reused by the other language consumers.
