#!/usr/bin/env python3
"""Apply the same static documentation chrome to the landing and rustdoc pages."""
from html import escape
import json
from pathlib import Path
import re
import sys

DOCS = Path(__file__).resolve().parents[2] / "docs"


def region(name: str, content: str) -> str:
    return f"<!-- dw:{name}:start -->\n{content.strip()}\n<!-- dw:{name}:end -->"


def prepare(root: Path) -> int:
    if root.is_symlink() or not root.is_dir():
        raise ValueError("documentation output must be a regular directory")
    root = root.resolve()
    header = region("header", (DOCS / "header.html").read_text())
    footer = region("footer", (DOCS / "footer.html").read_text())
    identity_path = root / "__build.json"
    identity = json.loads(identity_path.read_text()) if identity_path.exists() else None
    updates = []
    for path in root.rglob("*.html"):
        if path.is_symlink() or not path.is_file():
            raise ValueError("documentation page must be a regular file")
        html = path.read_text()
        if '<meta http-equiv="refresh"' in html:
            continue
        landing = path == root / "index.html"
        if not landing and '<meta name="generator" content="rustdoc">' not in html:
            continue
        for name, value in (("header", header), ("footer", footer)):
            marker = f"<!-- dw:{name} -->"
            pattern = rf"<!-- dw:{name}:start -->.*?<!-- dw:{name}:end -->"
            if re.search(pattern, html, re.DOTALL):
                html, count = re.subn(pattern, lambda _: value, html, flags=re.DOTALL)
            elif marker in html:
                count = html.count(marker)
                html = html.replace(marker, value)
            elif not landing:
                anchor = r"(<body\b[^>]*>)" if name == "header" else r"(</body>)"
                html, count = re.subn(anchor, lambda match: match[0] + "\n" + value if name == "header" else value + "\n" + match[0], html)
            else:
                raise ValueError(f"missing {name} source marker")
            if count != 1:
                raise ValueError(f"ambiguous {name} in {path.name}")
        if not landing:
            html = re.sub(r'(<body\b[^>]*\bclass=")([^"]*)(")',
                          lambda match: match[1] + " ".join(dict.fromkeys([*match[2].split(), "dw-reference"])) + match[3], html, count=1)
        elif identity:
            version = identity["version"]
            revision = identity["source_revision"]
            if not re.fullmatch(r"\d+\.\d+\.\d+", version) or not re.fullmatch(r"[0-9a-f]{40}", revision):
                raise ValueError("invalid published documentation identity")
            html = html.replace("Stable release</strong>", f"Stable release {escape(version)}</strong>")
            html = re.sub(r"(https://github.com/durable-workflow/sdk-rust/blob/)[^/]+(/examples/)",
                          lambda match: match[1] + revision + match[2], html)
        updates.append((path, html))
    if not any(path == root / "index.html" for path, _ in updates):
        raise ValueError("missing landing page")
    for path, html in updates:
        path.write_text(html)
    return len(updates)


if __name__ == "__main__":
    directory = Path(sys.argv[1] if len(sys.argv) > 1 else "target/doc")
    print(f"Applied shared identity to {prepare(directory)} documentation pages.")
