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
`supersedes` identity. For guarded codec changes, validation runs each new
fixture through the base revision's official consumer against both production
revisions. Replay validation first proves that the base corpus passes with its
original consumer, then installs the candidate replay consumer and support
modules under `tests/replay_regression_corpus/` byte-for-byte over both
production revisions. This lets the executable workflow set grow while keeping
scenario semantics identical between controls. Each new fixture must fail with
the base implementation and pass with the candidate implementation before the
candidate's complete official corpus is run. Run:

```bash
python scripts/ci/validate-regression-corpus.py --base-ref <target>
```
