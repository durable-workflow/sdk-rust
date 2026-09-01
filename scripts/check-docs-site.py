#!/usr/bin/env python3
"""Check the generated Rust documentation's durable public contract."""

from __future__ import annotations

from html.parser import HTMLParser
from pathlib import Path
import re
import struct
import sys
from urllib.parse import urlsplit


class AssetParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.assets: list[str] = []
        self.h1_count = 0

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]]
    ) -> None:
        values = dict(attrs)
        if tag == "h1":
            self.h1_count += 1
        attribute = "href" if tag in {"a", "link"} else "src" if tag in {"img", "script"} else None
        if attribute and values.get(attribute):
            self.assets.append(values[attribute] or "")


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(message)


def local_path(build: Path, source: Path, value: str) -> Path | None:
    parsed = urlsplit(value)
    if parsed.scheme or parsed.netloc or value.startswith(("#", "mailto:")):
        return None
    raw = parsed.path
    if not raw:
        return None
    candidate = build / raw.lstrip("/") if raw.startswith("/") else source.parent / raw
    if raw.endswith("/"):
        candidate /= "index.html"
    return candidate


def main() -> None:
    build = Path(sys.argv[1] if len(sys.argv) > 1 else "target/doc").resolve()
    landing = build / "index.html"
    crate_home = build / "durable_workflow/index.html"

    for path in (
        landing,
        crate_home,
        build / "layout.css",
        build / "navigation.js",
        build / "analytics/analytics.js",
        build / "assets/favicon.svg",
        build / "assets/social-card.png",
    ):
        require(path.is_file() and path.stat().st_size > 0, f"missing documentation asset: {path}")

    html = landing.read_text(encoding="utf-8")
    parser = AssetParser()
    parser.feed(html)
    require(parser.h1_count == 1, "documentation landing must contain one h1")
    require('<meta http-equiv="refresh"' not in html, "documentation root must not redirect")
    require("cargo add durable-workflow" in html, "landing must show the stable Cargo install command")
    require("install-sdk.sh" not in html, "landing must not depend on a release resolver")
    require("quickstart-execution-contract" not in html, "landing must not depend on a retired quickstart contract")

    for marker in (
        '<link rel="canonical" href="https://rust.durable-workflow.com/">',
        '<link rel="icon" type="image/svg+xml" href="/assets/favicon.svg">',
        '<meta property="og:image" content="https://rust.durable-workflow.com/assets/social-card.png">',
        '<meta name="twitter:card" content="summary_large_image">',
        'href="durable_workflow/"',
        'href="https://durable-workflow.com/docs/2.0/polyglot/rust/"',
        'href="https://github.com/durable-workflow/sdk-rust"',
    ):
        require(marker in html, f"documentation landing is missing: {marker}")

    for value in parser.assets:
        path = local_path(build, landing, value)
        if path is not None:
            require(path.exists(), f"landing references missing local asset: {value}")

    with (build / "assets/social-card.png").open("rb") as image:
        require(image.read(8) == b"\x89PNG\r\n\x1a\n", "social card must be a PNG")
        length = struct.unpack(">I", image.read(4))[0]
        require(image.read(4) == b"IHDR" and length == 13, "social card has an invalid PNG header")
        width, height = struct.unpack(">II", image.read(8))
        require((width, height) == (1200, 630), "social card must be 1200x630")

    rendered = list(build.rglob("*.html"))
    require(rendered, "rustdoc did not render HTML")
    prerelease = re.compile(r"\b2\.0\.0-(?:alpha|beta|rc)\.\d+\b")
    for path in rendered:
        require(not prerelease.search(path.read_text(encoding="utf-8")), f"stale prerelease version in {path}")

    require((build / "CNAME").read_text(encoding="utf-8").strip() == "rust.durable-workflow.com", "CNAME is incorrect")
    print(f"Checked Rust documentation landing and {len(rendered)} rendered HTML pages.")


if __name__ == "__main__":
    main()
