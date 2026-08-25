#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
consumer_root="$root/scripts/ci/official-avro-consumers"
python="${OFFICIAL_AVRO_PYTHON:-python3}"
autoload="${OFFICIAL_AVRO_PHP_AUTOLOAD:-$consumer_root/vendor/autoload.php}"
temporary="$(mktemp -d)"
payload="$temporary/rust-official-avro-nested-boundaries.bin"
trap 'find "$temporary" -depth -delete' EXIT

if [[ ! -f "$autoload" ]]; then
    echo "official PHP apache/avro dependencies are unavailable at $autoload" >&2
    exit 2
fi

cd "$root"
DURABLE_WORKFLOW_CROSS_LANGUAGE_AVRO_OUTPUT="$payload" \
    cargo test --quiet --test codec_regression_corpus \
    checked_in_codec_regression_corpus_uses_apache_avro -- --exact

test -s "$payload"
"$python" "$consumer_root/consume.py" \
    "$payload" "$root/schema/durable_workflow.protocol.Value.v1.avsc"
php "$consumer_root/consume.php" "$autoload" \
    "$payload" "$root/schema/durable_workflow.protocol.Value.v1.avsc"
