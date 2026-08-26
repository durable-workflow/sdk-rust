#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 1 ] || [ -z "$1" ]; then
    printf 'usage: %s ABSOLUTE_VENV_PATH\n' "${0##*/}" >&2
    exit 2
fi
if [[ "$1" != /* ]]; then
    printf 'release tooling virtual environment path must be absolute\n' >&2
    exit 2
fi
if [ -z "${GITHUB_PATH:-}" ]; then
    printf 'GITHUB_PATH is required to expose the isolated release tooling\n' >&2
    exit 2
fi

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
release_tooling_venv="$1"

python3 -m venv "$release_tooling_venv"
"$release_tooling_venv/bin/python" -m pip install --disable-pip-version-check \
    --require-hashes --only-binary=:all: \
    --requirement "$script_dir/release-tooling-requirements.txt"
printf '%s\n' "$release_tooling_venv/bin" >> "$GITHUB_PATH"
