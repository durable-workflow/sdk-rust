# Source qualification

Forgejo pull requests use one bounded structural route. On the warm local
runner, `scripts/ci/run-forgejo-fast-path.py` must complete within the
machine-readable budget in `forgejo-fast-path.json`, while the enclosing job
has a two-minute timeout that also covers checkout and the cached Rust 1.86
toolchain setup. The route parses the Cargo manifest, checks Rust formatting
and changed-file whitespace, scans the public boundary, and runs
`cargo check --all-targets`. It does not link or execute the test suite.

GitHub Actions remains the source-qualification authority. Its independently
runnable CI workflow tests all targets on Rust 1.86 and stable, enforces the
typed Avro compatibility and performance contracts, builds warning-free API
documentation, verifies the publishable package contents, and validates the
release tooling. The separate GitHub public-boundary workflow remains enabled
for pull requests and target-branch pushes.

GitHub pull requests continue to use the unprivileged `pull_request` event with
read-only repository contents. Publication and recovery stay in their
protected-ref workflows and are not part of the local structural route.
