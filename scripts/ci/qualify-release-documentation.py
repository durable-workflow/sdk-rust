#!/usr/bin/env python3
"""Qualify the Rust release-documentation hierarchy across shipped surfaces."""

from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from html.parser import HTMLParser
import os
from pathlib import Path
import sys
import tarfile
import time
import tomllib
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[2]
VOID_ELEMENTS = frozenset(
    {
        "area",
        "base",
        "br",
        "col",
        "embed",
        "hr",
        "img",
        "input",
        "link",
        "meta",
        "param",
        "source",
        "track",
        "wbr",
    }
)
EXPECTED_PATH_ORDER = ("general", "cloud")
EXPECTED_AVAILABILITY = {
    "general": "generally-available",
    "cloud": "limited-early-access",
}
EXPECTED_DESTINATIONS = {
    "general": frozenset(
        {"crate-install", "local-self-hosted", "api-reference", "sdk-guide"}
    ),
    "cloud": frozenset({"cloud-access", "cloud-guide"}),
}


class QualificationError(RuntimeError):
    """A release-documentation surface violates the structured contract."""


@dataclass
class Node:
    tag: str
    attrs: dict[str, str]
    order: int
    children: list[Node | str] = field(default_factory=list)


class DocumentParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.root = Node("document", {}, 0)
        self.stack = [self.root]
        self.order = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self.order += 1
        node = Node(
            tag,
            {name: value or "" for name, value in attrs},
            self.order,
        )
        self.stack[-1].children.append(node)
        if tag not in VOID_ELEMENTS:
            self.stack.append(node)

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self.handle_starttag(tag, attrs)
        if tag not in VOID_ELEMENTS:
            self.stack.pop()

    def handle_endtag(self, tag: str) -> None:
        for index in range(len(self.stack) - 1, 0, -1):
            if self.stack[index].tag == tag:
                del self.stack[index:]
                return

    def handle_data(self, data: str) -> None:
        self.stack[-1].children.append(data)


def walk(node: Node):
    yield node
    for child in node.children:
        if isinstance(child, Node):
            yield from walk(child)


def visible_text(node: Node) -> str:
    return " ".join(
        child if isinstance(child, str) else visible_text(child)
        for child in node.children
    ).strip()


def parse_document(markup: str) -> Node:
    parser = DocumentParser()
    parser.feed(markup)
    parser.close()
    return parser.root


def release_contract(manifest: Path) -> tuple[dict[str, object], dict[str, object]]:
    parsed = tomllib.loads(manifest.read_text(encoding="utf-8"))
    package = parsed.get("package")
    if not isinstance(package, dict):
        raise QualificationError("Cargo manifest is missing package metadata")
    durable = package.get("metadata", {}).get("durable-workflow", {})
    documentation = durable.get("documentation") if isinstance(durable, dict) else None
    if not isinstance(documentation, dict):
        raise QualificationError("Cargo metadata is missing the documentation contract")

    if tuple(documentation.get("path-order", ())) != EXPECTED_PATH_ORDER:
        raise QualificationError("documentation paths must remain general-first")
    if documentation.get("availability") != EXPECTED_AVAILABILITY:
        raise QualificationError("documentation availability metadata is inconsistent")
    for path in EXPECTED_PATH_ORDER:
        destinations = documentation.get(f"{path}-destinations")
        if (
            not isinstance(destinations, list)
            or frozenset(destinations) != EXPECTED_DESTINATIONS[path]
        ):
            raise QualificationError(
                f"documentation metadata has inconsistent {path} destinations"
            )
    return package, documentation


def qualify_markup(markup: str, documentation: dict[str, object], name: str) -> None:
    root = parse_document(markup)
    nodes = tuple(walk(root))
    path_nodes = tuple(
        node for node in nodes if "data-documentation-path" in node.attrs
    )
    path_order = tuple(node.attrs["data-documentation-path"] for node in path_nodes)
    expected_order = tuple(documentation["path-order"])
    if path_order != expected_order:
        raise QualificationError(
            f"{name} documentation paths are not in the declared general-first order"
        )

    markers = dict(zip(path_order, path_nodes, strict=True))
    for path in expected_order:
        expected_access = EXPECTED_AVAILABILITY[path]
        if markers[path].attrs.get("data-access") != expected_access:
            raise QualificationError(
                f"{name} {path} path has inconsistent availability"
            )

        destinations = tuple(
            node
            for node in nodes
            if node.attrs.get("data-docs-destination") in EXPECTED_DESTINATIONS[path]
        )
        found = frozenset(node.attrs["data-docs-destination"] for node in destinations)
        if found != EXPECTED_DESTINATIONS[path] or len(destinations) != len(found):
            raise QualificationError(f"{name} {path} destinations are incomplete")

        start = markers[path].order
        next_orders = [node.order for node in path_nodes if node.order > start]
        end = min(next_orders) if next_orders else sys.maxsize
        for destination in destinations:
            if not start <= destination.order < end:
                raise QualificationError(
                    f"{name} {path} destination is outside its declared journey"
                )
            if destination.attrs.get("data-access") != expected_access:
                raise QualificationError(
                    f"{name} {path} destination has inconsistent availability"
                )
            if not visible_text(destination):
                raise QualificationError(
                    f"{name} {path} destination must have a visible label"
                )

    access_labels = tuple(
        node
        for node in nodes
        if node.attrs.get("data-access-label") == EXPECTED_AVAILABILITY["cloud"]
    )
    if (
        len(access_labels) != 1
        or access_labels[0].order <= markers["cloud"].order
        or not visible_text(access_labels[0])
    ):
        raise QualificationError(
            f"{name} Cloud path must expose one visible limited-access label"
        )


def qualify_package(
    archive: Path,
    source_readme: bytes,
    package: dict[str, object],
) -> None:
    expected_member = f"{package['name']}-{package['version']}/{package['readme']}"
    try:
        with tarfile.open(archive, "r:*") as crate:
            member = crate.extractfile(expected_member)
            if member is None:
                raise QualificationError(
                    "packaged crate is missing its declared README"
                )
            packaged_readme = member.read()
    except (KeyError, OSError, tarfile.TarError) as error:
        raise QualificationError(
            f"could not inspect packaged crate: {error}"
        ) from error
    if packaged_readme != source_readme:
        raise QualificationError(
            "packaged crate README differs from the qualified source"
        )


def read_url(url: str, attempts: int, retry_delay: float) -> str:
    for attempt in range(1, attempts + 1):
        try:
            request = Request(
                url, headers={"User-Agent": "sdk-rust-docs-qualification"}
            )
            with urlopen(request, timeout=20) as response:
                if 200 <= response.status < 300:
                    return response.read().decode("utf-8")
                reason = f"HTTP {response.status}"
        except (HTTPError, URLError, OSError, UnicodeError) as error:
            reason = str(error)
        if attempt < attempts:
            time.sleep(retry_delay)
    raise QualificationError(f"could not read generated API reference: {reason}")


def package_archive(package: dict[str, object]) -> Path:
    target = Path(os.environ.get("CARGO_TARGET_DIR", ROOT / "target"))
    if not target.is_absolute():
        target = ROOT / target
    return target / "package" / f"{package['name']}-{package['version']}.crate"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=ROOT / "Cargo.toml")
    parser.add_argument("--readme", type=Path, default=ROOT / "README.md")
    parser.add_argument("--rustdoc", type=Path)
    parser.add_argument("--rustdoc-url")
    parser.add_argument("--check-package", action="store_true")
    parser.add_argument("--attempts", type=int, default=1)
    parser.add_argument("--retry-delay", type=float, default=0)
    arguments = parser.parse_args()

    if arguments.rustdoc is not None and arguments.rustdoc_url is not None:
        parser.error("choose either --rustdoc or --rustdoc-url")
    if arguments.attempts < 1 or arguments.retry_delay < 0:
        parser.error("attempts must be positive and retry delay non-negative")

    try:
        package, documentation = release_contract(arguments.manifest)
        source_readme = arguments.readme.read_bytes()
        qualify_markup(source_readme.decode("utf-8"), documentation, "source README")
        qualified = ["source README"]

        if arguments.rustdoc is not None:
            qualify_markup(
                arguments.rustdoc.read_text(encoding="utf-8"),
                documentation,
                "generated API reference",
            )
            qualified.append("generated API reference")
        elif arguments.rustdoc_url is not None:
            qualify_markup(
                read_url(
                    arguments.rustdoc_url,
                    arguments.attempts,
                    arguments.retry_delay,
                ),
                documentation,
                "live generated API reference",
            )
            qualified.append("live generated API reference")

        if arguments.check_package:
            qualify_package(package_archive(package), source_readme, package)
            qualified.append("packaged crate README")
    except (KeyError, OSError, UnicodeError, ValueError, QualificationError) as error:
        print(f"release documentation qualification failed: {error}", file=sys.stderr)
        return 1

    print(f"Qualified the {'; '.join(qualified)} general-first hierarchy.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
