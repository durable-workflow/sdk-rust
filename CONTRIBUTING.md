# Contributing

Run `cargo fmt --check`, focused tests, and Clippy or the repository's normal
warning-free build for changed code.

Replay and payload-codec fixes also follow the organization
[regression-corpus contract](https://github.com/durable-workflow/.github/tree/main/regression-corpus).
Replay fixes add one minimal history or command-sequence fixture under
`tests/fixtures/replay-regressions/`. Codec fixes add the same wire fixture
under `tests/fixtures/codec-regressions/` in every applicable official binding;
also list Rust fixtures in that directory's `manifest.txt`.

Fixtures preserve the value and type, framing, and stable failure policy.
Existing evidence is append-only; protocol evolution adds a new fixture with a
`supersedes` identity. For guarded Rust changes, validation runs each new
fixture through the base revision's official consumer against both production
revisions. The fixture must fail with the base implementation and pass with the
candidate implementation before the candidate's complete official corpus is
run. Run:

```bash
python scripts/ci/validate-regression-corpus.py --base-ref <target>
```
