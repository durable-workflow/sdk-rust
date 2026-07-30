# Source qualification

Source qualification defaults to the complete route. It tests all targets on
Rust 1.86 and current stable, enforces the typed Avro compatibility and
performance contracts, builds warning-free API documentation, verifies the
publishable package contents, and validates the release tooling.

A CI control plane may set the repository variable
`SOURCE_QUALIFICATION_MODE=bounded` to select the bounded structural route.
`scripts/ci/run-bounded-qualification.py` must complete within the
machine-readable budget in `bounded-qualification.json`, while the enclosing
job allows three minutes for setup and the checks. The route parses the Cargo
manifest, checks Rust formatting and changed-file whitespace, scans the public
boundary, and runs `cargo check --all-targets`. It does not link or execute the
test suite.

Pull requests use the unprivileged `pull_request` event with read-only
repository contents. Publication and recovery stay in their protected-ref
workflows and are not part of the bounded structural route.
