import re
import sys
from pathlib import Path

build_directory = Path(sys.argv[1] if len(sys.argv) > 1 else "target/doc")
runtime = Path("docs/analytics/analytics.js").read_text(encoding="utf-8")
stylesheet = Path("docs/analytics/analytics.css").read_text(encoding="utf-8")

if (build_directory / "analytics/analytics.js").read_text(encoding="utf-8") != runtime:
    raise SystemExit("Rendered rustdoc analytics runtime is stale")
if (build_directory / "analytics/analytics.css").read_text(
    encoding="utf-8"
) != stylesheet:
    raise SystemExit("Rendered rustdoc analytics stylesheet is stale")
flow_selectors = {
    ".dw-analytics-controls",
    ".dw-analytics-consent",
    ".dw-analytics-preferences",
}
observed_flow_selectors = set()
for selector_list, declarations in re.findall(r"([^{}]+)\{([^{}]*)\}", stylesheet):
    for selector in flow_selectors:
        if selector not in selector_list:
            continue
        observed_flow_selectors.add(selector)
        if re.search(r"position\s*:\s*fixed", declarations):
            raise SystemExit(
                f"{selector} must participate in the rustdoc document flow"
            )
if observed_flow_selectors != flow_selectors:
    raise SystemExit("rustdoc analytics flow styles are incomplete")

for required in (
    "G-HD1YHT442Y",
    "rust.durable-workflow.com",
    "analytics_storage: 'granted'",
    "send_page_view: true",
    "cookie_domain: SITE_HOSTNAME",
    "PARENT_COOKIE_DOMAIN = 'durable-workflow.com'",
    "new Set([SITE_HOSTNAME, PARENT_COOKIE_DOMAIN])",
    "document.querySelector('main .width-limiter')",
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
