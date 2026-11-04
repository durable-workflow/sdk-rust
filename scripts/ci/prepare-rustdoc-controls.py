#!/usr/bin/env python3
"""Move the pinned rustdoc back controls onto the external navigation runtime."""

from pathlib import Path
import sys


OLD_CONTROL = '<a id="back" href="javascript:void(0)" onclick="history.back();">'
SAFE_CONTROL = '<a id="back" href="./durable_workflow/index.html" data-rustdoc-history-back>'
NAVIGATION = '<script type="module" src="/navigation.js"></script>'


def prepare(root: Path) -> int:
    if not (root / "durable_workflow/index.html").is_file():
        raise ValueError("rustdoc back-control fallback is missing")
    runtime = root / "navigation.js"
    if not runtime.is_file() or "data-rustdoc-history-back" not in runtime.read_text(encoding="utf-8"):
        raise ValueError("external rustdoc history-back listener is missing")

    updates: list[tuple[Path, bytes]] = []
    for name in ("help.html", "settings.html"):
        target = root / name
        html = target.read_bytes().decode("utf-8")
        if html.count(NAVIGATION) != 1:
            raise ValueError(f"{name} must load the external navigation runtime once")
        if html.count(SAFE_CONTROL) == 1 and OLD_CONTROL not in html:
            continue
        if html.count(OLD_CONTROL) != 1 or SAFE_CONTROL in html:
            raise ValueError(f"unexpected pinned rustdoc back-control markup in {name}")
        updates.append((target, html.replace(OLD_CONTROL, SAFE_CONTROL).encode("utf-8")))

    for target, content in updates:
        target.write_bytes(content)
    return len(updates)


if __name__ == "__main__":
    changed = prepare(Path(sys.argv[1] if len(sys.argv) > 1 else "target/doc"))
    print(f"Prepared CSP-safe rustdoc back controls in {changed} pages.")
