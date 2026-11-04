#!/usr/bin/env python3
"""Move the pinned rustdoc back controls onto the external navigation runtime."""

from pathlib import Path
import sys


OLD_CONTROL = '<a id="back" href="javascript:void(0)" onclick="history.back();">'
SAFE_CONTROL = '<a id="back" href="./durable_workflow/index.html" data-rustdoc-history-back>'
NAVIGATION = '<script type="module" src="/navigation.js"></script>'


def regular_input(root: Path, relative: str) -> Path:
    candidate = root / relative
    if candidate.is_symlink() or not candidate.is_file():
        raise ValueError(f"rustdoc input must be a regular file: {relative}")
    resolved = candidate.resolve(strict=True)
    if not resolved.is_relative_to(root):
        raise ValueError(f"rustdoc input escapes the output directory: {relative}")
    return resolved


def prepare(root: Path) -> int:
    if root.is_symlink():
        raise ValueError("rustdoc output directory must not be a symlink")
    root = root.resolve(strict=True)
    regular_input(root, "durable_workflow/index.html")
    runtime = regular_input(root, "navigation.js")
    if "data-rustdoc-history-back" not in runtime.read_text(encoding="utf-8"):
        raise ValueError("external rustdoc history-back listener is missing")

    updates: list[tuple[Path, bytes]] = []
    for name in ("help.html", "settings.html"):
        target = regular_input(root, name)
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
    if not sys.argv[1:]:
        output = Path("target/doc")
    elif sys.argv[1:] == ["--stable"]:
        output = Path("target/stable-docs")
    else:
        raise SystemExit("Usage: prepare-rustdoc-controls.py [--stable]")
    if not output.resolve(strict=True).is_relative_to(Path.cwd().resolve() / "target"):
        raise SystemExit("rustdoc output must stay inside the checkout's target directory")
    changed = prepare(output)
    print(f"Prepared CSP-safe rustdoc back controls in {changed} pages.")
