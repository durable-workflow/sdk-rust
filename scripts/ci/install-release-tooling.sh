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

# -I protects the interpreters started here, but pip runs the target venv
# interpreter as a child that would still read PYTHONPATH.
unset PYTHONPATH

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
release_tooling_venv="$1"

# Isolated mode (-I): with -m, a plain interpreter puts the working directory
# first on sys.path, so a venv/ package or stdlib-shadowing module there would
# run before the release tooling exists.
python3 -I -m venv --without-pip "$release_tooling_venv"
# Keep dependency requests on the bootstrapped client, outside ensurepip's bundle.
python3 -I -m pip --python "$release_tooling_venv/bin/python" install --disable-pip-version-check \
    --require-hashes --only-binary=:all: \
    --requirement "$script_dir/release-tooling-requirements.txt"
printf '%s\n' "$release_tooling_venv/bin" >> "$GITHUB_PATH"
