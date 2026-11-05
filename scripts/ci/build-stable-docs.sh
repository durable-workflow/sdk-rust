#!/usr/bin/env bash
set -euo pipefail

python3 scripts/ci/test-resolve-docs-release.py
identity_output="$(mktemp)"
trap 'rm -f "$identity_output"' EXIT
python3 scripts/ci/resolve-docs-release.py > "$identity_output"
cat "$identity_output"
stable_source="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["source_directory"])' "$identity_output")"
stable_identity="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["identity_file"])' "$identity_output")"

if [ "${GITHUB_REF:-}" = refs/heads/main ]; then
    if ! printf '%s' "${CLOUDFLARE_WEB_ANALYTICS_TOKEN:-}" | grep -Eq '^[a-f0-9]{32}$'; then
        echo 'CLOUDFLARE_WEB_ANALYTICS_TOKEN must be a 32-character site token' >&2
        exit 1
    fi
    sed -i "s/__CLOUDFLARE_WEB_ANALYTICS_TOKEN__/$CLOUDFLARE_WEB_ANALYTICS_TOKEN/" docs/analytics/analytics.js
fi

export CARGO_BUILD_JOBS=2
export CARGO_TARGET_DIR="${RUNNER_TEMP:?}/stable-docs-target"
export RUSTDOCFLAGS="-D warnings --html-in-header $PWD/docs/analytics-head.html"
cargo doc --manifest-path "$stable_source/Cargo.toml" --locked --all-features --no-deps
cargo doc --manifest-path "$stable_source/Cargo.toml" --locked --all-features --no-deps --examples
mkdir -p target
test ! -e target/stable-docs
mv "$CARGO_TARGET_DIR/doc" target/stable-docs
printf '%s\n' 'rust.durable-workflow.com' > target/stable-docs/CNAME
touch target/stable-docs/.nojekyll
cp docs/index.html docs/layout.css docs/navigation.js docs/status.js target/stable-docs/
mkdir -p target/stable-docs/analytics target/stable-docs/assets
cp docs/analytics/analytics.js target/stable-docs/analytics/
cp -R docs/assets/. target/stable-docs/assets/
cp "$stable_identity" target/stable-docs/__build.json
python3 - <<'PY'
import json
from pathlib import Path
import re
identity = json.loads(Path('target/stable-docs/__build.json').read_text())
landing = Path('target/stable-docs/index.html')
html = landing.read_text().replace('Stable release</strong>', f"Stable release {identity['version']}</strong>")
html = re.sub(r'(https://github.com/durable-workflow/sdk-rust/blob/)[^/]+(/examples/)', rf"\g<1>{identity['source_revision']}\2", html)
landing.write_text(html)
PY
python3 scripts/ci/test-docs-identity.py
python3 scripts/ci/prepare-docs-identity.py --stable
python3 scripts/ci/test-rustdoc-controls.py
python3 scripts/ci/prepare-rustdoc-controls.py --stable
python3 scripts/check-docs-analytics.py target/stable-docs
python3 scripts/check-docs-site.py target/stable-docs
python3 scripts/ci/validate-example-base-urls.py --root "$stable_source" --rendered-docs target/stable-docs
node scripts/ci/test-docs-analytics.mjs
node scripts/ci/test-docs-navigation.mjs
node scripts/ci/test-docs-status.mjs
