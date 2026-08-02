import re
import sys
from pathlib import Path

build_directory = Path(sys.argv[1] if len(sys.argv) > 1 else "target/doc")
runtime = Path("docs/analytics/analytics.js").read_text(encoding="utf-8")

if (build_directory / "analytics/analytics.js").read_text(encoding="utf-8") != runtime:
    raise SystemExit("Rendered rustdoc analytics runtime is stale")

for required in (
    "G-HD1YHT442Y",
    "rust.durable-workflow.com",
    "analytics_storage: 'granted'",
    "send_page_view: true",
    "cookie_domain: SITE_HOSTNAME",
    "PARENT_COOKIE_DOMAIN = 'durable-workflow.com'",
    "new Set([SITE_HOSTNAME, PARENT_COOKIE_DOMAIN])",
):
    if required not in runtime:
        raise SystemExit(f"Analytics runtime is missing required configuration: {required}")

if "gtag('event', 'page_view'" in runtime:
    raise SystemExit("Analytics runtime must not duplicate automatic navigation page views")

html_files = list(build_directory.rglob("*.html"))
if not html_files:
    raise SystemExit("rustdoc did not render HTML pages")

for html_file in html_files:
    html = html_file.read_text(encoding="utf-8")
    if re.search(r'<meta[^>]+http-equiv=["\']refresh["\']', html, re.IGNORECASE):
        if "/analytics/analytics.js" in html:
            raise SystemExit(f"{html_file} redirect must not create a duplicate page view")
        continue
    if len(re.findall(r'src="/analytics/analytics\.js"', html)) != 1:
        raise SystemExit(f"{html_file} must load one local analytics runtime")
    if len(re.findall(r'href="/analytics/analytics\.css"', html)) != 1:
        raise SystemExit(f"{html_file} must load one local analytics stylesheet")
    if "googletagmanager.com" in html:
        raise SystemExit(f"{html_file} must not load Google before consent")

print(f"Validated consent-gated analytics in {len(html_files)} rendered pages.")
