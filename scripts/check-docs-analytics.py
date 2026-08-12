import re
import sys
from pathlib import Path

build_directory = Path(sys.argv[1] if len(sys.argv) > 1 else "target/doc")
runtime = Path("docs/analytics/analytics.js").read_text(encoding="utf-8")
styles = Path("docs/layout.css").read_text(encoding="utf-8")
landing_styles = styles.split(".sidebar .sidebar-elems", maxsplit=1)[0]

if (build_directory / "analytics/analytics.js").read_text(encoding="utf-8") != runtime:
    raise SystemExit("Rendered rustdoc analytics runtime is stale")
if not re.search(r"\.dw-cloud-promotion__eyebrow\s*\{[^}]*letter-spacing:\s*0;", styles, re.DOTALL):
    raise SystemExit("Promotion eyebrow letter spacing must remain zero")
if re.search(r"gradient|clamp\(|letter-spacing:\s*-|\b\d+(?:\.\d+)?vw\b", landing_styles):
    raise SystemExit("Rust documentation landing must use fixed type and flat surfaces")

for required in (
    "https://static.cloudflareinsights.com/beacon.min.js",
    "document.querySelector(BEACON_SELECTOR)",
    "loader.type = 'module'",
    "loader.dataset.cfBeacon = JSON.stringify({token: TOKEN})",
    "'rust.durable-workflow.com'",
    "'cloud.durable-workflow.com': new Set(['/', '/early-access', '/early-access/'])",
    "'status.durable-workflow.com': new Set(['/'])",
):
    if required not in runtime:
        raise SystemExit(
            f"Analytics runtime is missing required configuration: {required}"
        )

if re.search(r"\bspa\s*:", runtime):
    raise SystemExit("Analytics runtime overrides Cloudflare's supported navigation semantics")

forbidden = re.compile(
    r"localStorage|sessionStorage|document\.cookie|googletagmanager|google-analytics|"
    r"G-HD1YHT442Y|durable-workflow\.analytics-consent|_ga(?:\b|_)",
    re.IGNORECASE,
)
if forbidden.search(runtime):
    raise SystemExit(
        "Analytics runtime contains retired Google or browser-storage behavior"
    )

html_files = list(build_directory.rglob("*.html"))
if not html_files:
    raise SystemExit("rustdoc did not render HTML pages")

for html_file in html_files:
    html = html_file.read_text(encoding="utf-8")
    if '<meta http-equiv="refresh"' in html:
        if "/analytics/analytics.js" in html:
            raise SystemExit(
                f"{html_file} redirect must not emit an analytics page view"
            )
        continue
    if len(re.findall(r'src="/analytics/analytics\.js"', html)) != 1:
        raise SystemExit(f"{html_file} must load one cookie-free analytics runtime")
    if not re.search(
        r'<script(?=[^>]*\bsrc="/analytics/analytics\.js")(?=[^>]*\btype="module")[^>]*>',
        html,
    ):
        raise SystemExit(f"{html_file} must use module semantics for analytics")
    if "/analytics/analytics.css" in html:
        raise SystemExit(f"{html_file} still loads retired analytics UI styles")
    if forbidden.search(html):
        raise SystemExit(
            f"{html_file} contains retired Google analytics or consent state"
        )

landing = (build_directory / "index.html").read_text(encoding="utf-8")
if '<meta http-equiv="refresh"' in landing:
    raise SystemExit("Rust documentation root must be a task-oriented landing page")
if len(re.findall(r"<h1(?:\s|>)", landing)) != 1:
    raise SystemExit("Rust documentation landing must expose one primary heading")

crate_home = (build_directory / "durable_workflow/index.html").read_text(
    encoding="utf-8"
)
if crate_home.count('data-promotion-source="sdk-rust-reference"') != 1:
    raise SystemExit("Rust reference home must render one bounded Cloud promotion")
if (
    'href="https://cloud.durable-workflow.com/early-access#source=sdk-rust-reference"'
    not in crate_home
):
    raise SystemExit("Rust reference promotion must resolve to the public early-access form")

for promotion_boundary in (
    "PROMOTION_SOURCE = 'sdk-rust-reference'",
    "credentials: 'omit'",
    "referrerPolicy: 'no-referrer'",
    "JSON.stringify({source: PROMOTION_SOURCE, event})",
):
    if promotion_boundary not in runtime:
        raise SystemExit(
            f"Promotion analytics is missing its bounded contract: {promotion_boundary}"
        )

print(
    f"Validated cookie-free Cloudflare Web Analytics in {len(html_files)} rendered pages."
)
