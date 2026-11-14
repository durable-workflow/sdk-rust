#!/usr/bin/env bash
set -euo pipefail

composition_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
composition_manifest="$composition_root/tests/native-composition/Cargo.toml"
composition_log="$(mktemp)"
trap 'rm -f -- "$composition_log"' EXIT

# The modern host opts out; Cargo must compile both official Avro versions.
cargo check --manifest-path "$composition_manifest"

# Re-enabling the legacy compiler pin must retain the real resolver refusal.
if cargo metadata --format-version 1 --manifest-path "$composition_manifest" \
    --features sdk-msrv-pin >"$composition_log" 2>&1; then
    printf 'native composition unexpectedly accepted the incompatible MSRV pin\n' >&2
    exit 1
fi
python3 -I -S -B - "$composition_log" <<'PY'
import pathlib
import sys

error = pathlib.Path(sys.argv[1]).read_text()
if not all(value in error for value in ("bon", "3.8.1", "3.9.1")):
    raise SystemExit("native composition failed without the expected bon constraint conflict")
print("modern composition builds; the explicit MSRV pin retains its resolver refusal")
PY
