# Rust documentation source

The landing and API shell use `header.html`, `footer.html`, `layout.css` and
`navigation.js`. They implement the shared One Identity design; the canonical
flowchart logo is unchanged. Fonts are self-hosted with their source hashes and
OFL licenses in `assets/fonts/`. The shared status interpreter is copied from
the main documentation's `static/identity/status.mjs`; `status.js` only connects
its summary states to the footer.

The protected `scripts/ci/build-stable-docs.sh` builds API content and examples
from the approved published crate, then applies the current documentation shell.
`__build.json` records both `source_revision` (published SDK) and
`tooling_revision` (documentation source). The version badge and example links
use the published identity. A normal source CI build of main is separate from
that published API build.

Run the focused controls from the repository root:

```sh
python3 scripts/ci/test-docs-identity.py
node scripts/ci/test-docs-navigation.mjs
node --test scripts/ci/test-docs-status.mjs
```

The published documentation check retains `rust-docs-preview` for one day so a
reviewer can inspect the actual checked output. For browser acceptance, serve a
built directory on loopback and use a separately installed Playwright client
and Chrome/Chromium. This check performs no browser download or installation:

```sh
python3 -m http.server 8000 --bind 127.0.0.1 --directory target/stable-docs
# In another shell, with Playwright available to Node:
CHROMIUM_PATH=/path/to/chromium node scripts/ci/check-docs-browser.cjs \
  http://127.0.0.1:8000 target/docs-browser
```

`PLAYWRIGHT_MODULE` can select an existing public Playwright package. The check
uses a new disposable profile, at most two renderer processes and a 210-second
bound. It captures desktop/tablet/narrow-mobile landing and API pages in both
themes and checks theme persistence, copy, keyboard switching, API navigation,
search and synthetic status/freshness cases. Screenshots and `summary.json` go
to the output directory. Close the separate static server when finished.
Synthetic status fixtures never establish live service health; production
acceptance also verifies the existing public feed from the deployed origin.
