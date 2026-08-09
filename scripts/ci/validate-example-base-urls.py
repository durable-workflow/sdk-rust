#!/usr/bin/env python3
"""Qualify every shipped Rust example against the Client base-URL contract."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
from pathlib import Path
import re
import subprocess
import sys
from typing import Any
from urllib.parse import urlsplit


ROOT = Path(__file__).resolve().parents[2]
HTTP_URL = re.compile(r"https?://[^\s\"'<>]+")
TRAILING_PUNCTUATION = ",.;:!?)]}"


class QualificationError(RuntimeError):
    """The shipped-example base-URL contract is not satisfied."""


@dataclass(frozen=True)
class ExampleTarget:
    name: str
    source: Path


@dataclass(frozen=True)
class InvalidEndpoint:
    url: str
    line: int


def load_metadata(root: Path) -> dict[str, Any]:
    result = subprocess.run(
        ["cargo", "metadata", "--no-deps", "--format-version", "1"],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise QualificationError(f"cargo metadata failed: {detail}")
    try:
        metadata = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise QualificationError(
            f"cargo metadata returned invalid JSON: {error}"
        ) from error
    if not isinstance(metadata, dict):
        raise QualificationError("cargo metadata must return an object")
    return metadata


def example_targets(metadata: dict[str, Any], root: Path) -> tuple[ExampleTarget, ...]:
    manifest = (root / "Cargo.toml").resolve()
    packages = metadata.get("packages")
    if not isinstance(packages, list):
        raise QualificationError("cargo metadata packages must be an array")

    package = next(
        (
            candidate
            for candidate in packages
            if isinstance(candidate, dict)
            and Path(str(candidate.get("manifest_path", ""))).resolve() == manifest
        ),
        None,
    )
    if package is None:
        raise QualificationError(f"cargo metadata does not contain {manifest}")

    targets: list[ExampleTarget] = []
    for target in package.get("targets", []):
        if not isinstance(target, dict) or "example" not in target.get("kind", []):
            continue
        name = target.get("name")
        source_value = target.get("src_path")
        if not isinstance(name, str) or not name or not isinstance(source_value, str):
            raise QualificationError("example targets require names and source paths")
        source = Path(source_value).resolve()
        try:
            source.relative_to(root.resolve())
        except ValueError as error:
            raise QualificationError(
                f"example target {name!r} is outside the package root: {source}"
            ) from error
        targets.append(ExampleTarget(name=name, source=source))

    if not targets:
        raise QualificationError("cargo metadata did not discover any example targets")
    if len({target.name for target in targets}) != len(targets):
        raise QualificationError(
            "cargo metadata contains duplicate example target names"
        )
    return tuple(sorted(targets, key=lambda target: target.name))


def invalid_endpoints(text: str) -> tuple[InvalidEndpoint, ...]:
    invalid: list[InvalidEndpoint] = []
    for match in HTTP_URL.finditer(text):
        url = match.group(0).rstrip(TRAILING_PUNCTUATION)
        path = urlsplit(url).path.rstrip("/")
        if path.endswith("/api"):
            invalid.append(
                InvalidEndpoint(url=url, line=text.count("\n", 0, match.start()) + 1)
            )
    return tuple(invalid)


def rendered_source_path(docs: Path, target: ExampleTarget) -> Path:
    crate_name = target.name.replace("-", "_")
    return docs / "src" / crate_name / f"{target.source.name}.html"


def qualify(
    targets: tuple[ExampleTarget, ...], rendered_docs: Path | None = None
) -> None:
    failures: list[str] = []
    for target in targets:
        try:
            source = target.source.read_text(encoding="utf-8")
        except OSError as error:
            failures.append(f"{target.source}: could not read example source: {error}")
            continue
        for invalid in invalid_endpoints(source):
            failures.append(
                f"{target.source}:{invalid.line}: {invalid.url!r} includes the "
                "SDK-owned /api suffix; pass the Server origin or Cloud runtime URL"
            )

        if rendered_docs is None:
            continue
        rendered = rendered_source_path(rendered_docs, target)
        if not rendered.is_file():
            failures.append(
                f"{rendered}: rendered source is missing for example target {target.name!r}"
            )
            continue
        try:
            html = rendered.read_text(encoding="utf-8")
        except OSError as error:
            failures.append(f"{rendered}: could not read rendered source: {error}")
            continue
        for invalid in invalid_endpoints(html):
            failures.append(
                f"{rendered}:{invalid.line}: rendered example contains rejected endpoint "
                f"{invalid.url!r}"
            )

    if failures:
        raise QualificationError("\n".join(failures))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument(
        "--rendered-docs",
        type=Path,
        help="also require and inspect rustdoc source HTML for every example target",
    )
    return parser.parse_args()


def main() -> int:
    arguments = parse_args()
    root = arguments.root.resolve()
    rendered_docs = (
        arguments.rendered_docs.resolve()
        if arguments.rendered_docs is not None
        else None
    )
    try:
        targets = example_targets(load_metadata(root), root)
        qualify(targets, rendered_docs)
    except (OSError, QualificationError) as error:
        print(f"example base-URL qualification failed:\n{error}", file=sys.stderr)
        return 1

    scope = "source and rendered rustdoc" if rendered_docs is not None else "source"
    print(f"qualified {len(targets)} shipped example target(s): {scope}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
