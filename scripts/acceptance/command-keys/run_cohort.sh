#!/usr/bin/env bash
# Installed Rust command-key cohort for durable-workflow/.github#216.
#
# Runs, against one exact published native Server image and the published
# durable-workflow crate pinned in consumer/Cargo.toml:
#   1. the two direct tests in tests/command_keys_server.rs, copied unchanged
#      into a fresh registry-only consumer (run_direct.py owns the Server);
#   2. the bound adapter's cells in run_open.py: postcommit response loss,
#      synthetic legacy readback (open, completed, continued), the three
#      ambiguity shapes and case-only keys.
#
# Required environment:
#   COHORT_ROOT        new absolute directory owned by the current user
#   RUST_IMAGE         local official Rust image ID (sha256:...)
#   LOSS_FIXTURE       the accepted shared committed_response_loss.py, owned,
#                      single-link, not group/other writable
#   CONTAINER_PREFIX   owned container name prefix (default sdk-rust-216)
# The current user must be uid 1000, the Server image's user, so the Server
# can write the SQLite directory run_open.py bind-mounts.
set -euo pipefail

SERVER_INDEX=sha256:30ccaf2cea6810a92915a9eb389f1a63ea870818a74c9cc1a321379b07b2d25c
SERVER_VERSION=3.13.0
SERVER_SOURCE=2d1244be4611e1bc4aa4f0d856c708bed17937b4
SDK_VERSION=4.19.0
SDK_SHA256=5ddefff4eecee6744b070d5f894426c4c33033a872afe059a3914edaea4bca06
SDK_SOURCE=0351e407c4fdfd4db863f7dd4df57ac3a24c8a86

here="$(cd "$(dirname "$0")" && pwd)"
repo="$(git -C "$here" rev-parse --show-toplevel)"
prefix="${CONTAINER_PREFIX:-sdk-rust-216}"
: "${COHORT_ROOT:?}" "${RUST_IMAGE:?}" "${LOSS_FIXTURE:?}"
[[ "$COHORT_ROOT" = /* && ! -e "$COHORT_ROOT" ]] || { echo "COHORT_ROOT must be a new absolute path" >&2; exit 2; }
[[ "$(id -u)" = 1000 ]] || { echo "run as uid 1000, the Server image user" >&2; exit 2; }
grep -q "\"=$SDK_VERSION\"" "$here/consumer/Cargo.toml" || { echo "consumer does not pin $SDK_VERSION" >&2; exit 2; }
mkdir -m 700 "$COHORT_ROOT"
results="$COHORT_ROOT/results"; mkdir -m 700 "$results"

# 1. Direct tests in a fresh registry-only consumer.
direct="$COHORT_ROOT/direct-consumer"; mkdir -m 700 "$direct" "$COHORT_ROOT/cargo-home"
mkdir "$direct/tests"; cp "$repo/tests/command_keys_server.rs" "$direct/tests/"
docker run --rm --name "$prefix-direct-build" --cpus 2 --memory 3g --pids-limit 1024 \
  --user "$(id -u):$(id -g)" -e HOME=/tmp -e CARGO_HOME=/cargo-home -e CARGO_BUILD_JOBS=2 \
  -v "$COHORT_ROOT/cargo-home:/cargo-home" -v "$direct:/c" -w /c "$RUST_IMAGE" bash -ec "
    cargo init --lib --name command-key-consumer . >/dev/null 2>&1
    cargo add 'durable-workflow@=$SDK_VERSION' serde_json tokio \
      --features tokio/macros,tokio/rt-multi-thread,tokio/sync,tokio/time >/dev/null 2>&1
    cargo test --locked --test command_keys_server --no-run"
python3 -I -B - "$direct/Cargo.lock" "$SDK_VERSION" "$SDK_SHA256" <<'PY'
import sys, tomllib
lock = tomllib.load(open(sys.argv[1], 'rb'))
[sdk] = [p for p in lock['package'] if p['name'] == 'durable-workflow']
assert sdk['version'] == sys.argv[2] and sdk['checksum'] == sys.argv[3], sdk
assert sdk['source'] == 'registry+https://github.com/rust-lang/crates.io-index', sdk
PY
direct_start="$(date -u +%FT%TZ)"
python3 -B "$here/run_direct.py" --server-index "$SERVER_INDEX" --server-version "$SERVER_VERSION" \
  --server-source "$SERVER_SOURCE" --container-prefix "$prefix-direct" --work-dir "$COHORT_ROOT/direct-run" -- \
  docker run --rm --network host --name "$prefix-direct-tests" --cpus 2 --memory 3g --pids-limit 1024 \
    --user "$(id -u):$(id -g)" -e CARGO_HOME=/cargo-home -v "$COHORT_ROOT/cargo-home:/cargo-home" \
    -v "$direct:/c" -w /c -e DURABLE_WORKFLOW_LOCAL_ISOLATED -e DURABLE_WORKFLOW_SERVER_URL \
    -e DURABLE_WORKFLOW_AUTH_TOKEN "$RUST_IMAGE" \
    cargo test --locked --offline --test command_keys_server -- --ignored --test-threads 1 \
  | tee "$results/direct.log"
echo "direct start=$direct_start end=$(date -u +%FT%TZ)" | tee -a "$results/cells.txt"

# 2. Bound adapter cells.
build="$COHORT_ROOT/consumer-build"
python3 -B "$here/build_consumer.py" --work-dir "$build" --rust-image "$RUST_IMAGE" \
  --container-prefix "$prefix-build" | tee "$results/build.json"
common=(--consumer-binary "$build/consumer" --consumer-lock "$build/source/Cargo.lock"
  --build-receipt "$build/build-receipt.json" --sdk-version "$SDK_VERSION" --sdk-sha256 "$SDK_SHA256"
  --sdk-source "$SDK_SOURCE" --server-index "$SERVER_INDEX" --server-version "$SERVER_VERSION"
  --server-source "$SERVER_SOURCE" --container-prefix "$prefix")
cell() {
  local label=$1; shift
  local start; start="$(date -u +%FT%TZ)"
  timeout 900 python3 -B "$here/run_open.py" --work-dir "$COHORT_ROOT/work-$label" "${common[@]}" "$@" \
    > "$results/$label.jsonl"
  python3 -I -B - "$results/$label.jsonl" <<'PY'
import json, sys
rows = [json.loads(line) for line in open(sys.argv[1])]
assert any(row.get('outcome') == 'pass' and 'cell' in row for row in rows), rows[-3:]
assert rows[-1] == {**rows[-1], 'cleanup': 'pass'}, rows[-1]
PY
  echo "$label start=$start end=$(date -u +%FT%TZ) outcome=pass" | tee -a "$results/cells.txt"
}
cell response-loss --response-loss-fixture "$LOSS_FIXTURE"
for state in open completed continued; do cell "legacy-$state" --state "$state"; done
for scenario in two-rows legacy-token modern-raw case-only; do cell "$scenario" --scenario "$scenario"; done
echo "cohort complete: direct + 8 adapter cells passed" | tee -a "$results/cells.txt"
