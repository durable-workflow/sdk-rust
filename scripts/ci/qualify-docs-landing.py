#!/usr/bin/env python3
"""Qualify the Rust documentation landing contract and its destinations."""

from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from html.parser import HTMLParser
from pathlib import Path
import re
import sys
import time
from typing import Callable, Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import unquote, urljoin, urlsplit, urlunsplit
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[2]
FIRST_PARTY_HOSTS = frozenset(
    {
        "rust.durable-workflow.com",
        "durable-workflow.com",
        "cloud.durable-workflow.com",
    }
)
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


class QualificationError(RuntimeError):
    """The published landing contract is not satisfied."""


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


def parse_document(html: str) -> Node:
    parser = DocumentParser()
    parser.feed(html)
    parser.close()
    return parser.root


def walk(node: Node) -> Iterable[Node]:
    yield node
    for child in node.children:
        if isinstance(child, Node):
            yield from walk(child)


def visible_text(node: Node) -> str:
    parts: list[str] = []
    for child in node.children:
        if isinstance(child, Node):
            parts.append(visible_text(child))
        else:
            parts.append(child)
    return " ".join(" ".join(parts).split())


def select(root: Node, **attrs: str) -> list[Node]:
    return [
        node
        for node in walk(root)
        if all(node.attrs.get(name) == value for name, value in attrs.items())
    ]


def one(nodes: list[Node], description: str) -> Node:
    if len(nodes) != 1:
        raise QualificationError(
            f"landing must contain exactly one {description}; found {len(nodes)}"
        )
    return nodes[0]


def manifest_identity(manifest: Path) -> tuple[str, str]:
    text = manifest.read_text(encoding="utf-8")
    version = re.search(r'^version = "([^"]+)"$', text, re.MULTILINE)
    rust_version = re.search(r'^rust-version = "([^"]+)"$', text, re.MULTILINE)
    if version is None or rust_version is None:
        raise QualificationError(
            "Cargo.toml must declare package version and rust-version"
        )
    return version.group(1), rust_version.group(1)


def validate_structure(root: Node, crate_version: str, rust_version: str) -> None:
    body = one([node for node in walk(root) if node.tag == "body"], "body")
    if body.attrs.get("data-landing-contract") != "general-rust-first":
        raise QualificationError("landing is missing the general-first contract marker")
    if body.attrs.get("data-crate-version") != crate_version:
        raise QualificationError("landing crate identity is stale")
    if body.attrs.get("data-rust-version") != rust_version:
        raise QualificationError("landing Rust requirement is stale")

    descriptions = [
        node.attrs.get("content", "")
        for node in walk(root)
        if node.tag == "meta" and node.attrs.get("name") == "description"
    ]
    description = one(
        [Node("description", {}, 0, [value]) for value in descriptions],
        "metadata description",
    )
    if "cloud" in visible_text(description).casefold():
        raise QualificationError("metadata description must lead with the general SDK")

    headings = [node for node in walk(root) if node.tag == "h1"]
    if len(headings) != 1 or "Rust SDK" not in visible_text(headings[0]):
        raise QualificationError("landing must identify the Rust SDK in one h1")

    navigation = one(
        select(root, **{"data-landing-region": "primary-navigation"}),
        "primary navigation region",
    )
    lead = one(select(root, **{"data-landing-lead": ""}), "lead paragraph")
    first_task = one(
        select(root, **{"data-landing-section": "first-task"}),
        "first task section",
    )
    for region, description_name in (
        (navigation, "primary navigation"),
        (lead, "lead paragraph"),
        (first_task, "first task section"),
    ):
        if "cloud" in visible_text(region).casefold():
            raise QualificationError(
                f"{description_name} must lead with the generally available Rust path"
            )

    first_task_text = visible_text(first_task)
    if f"cargo add durable-workflow@={crate_version}" not in first_task_text:
        raise QualificationError("first task must install the current crate identity")
    if "rustc --version" not in first_task_text:
        raise QualificationError("first task must expose the Rust requirement check")
    if "DURABLE_WORKFLOW_" in first_task_text:
        raise QualificationError("first task must not require runtime credentials")

    links = [node for node in walk(root) if node.tag == "a"]
    for link in links:
        if not link.attrs.get("href") or not visible_text(link):
            raise QualificationError(
                "every landing link must have a destination and label"
            )
        if "rust-cloud-quickstart" in link.attrs["href"]:
            raise QualificationError(
                "landing contains the retired Cloud quickstart route"
            )

    destinations: dict[str, list[Node]] = {}
    for link in links:
        destination = link.attrs.get("data-docs-destination")
        if destination:
            destinations.setdefault(destination, []).append(link)
    for destination in ("api-reference", "sdk-guide", "cloud-access", "cloud-guide"):
        if destination not in destinations:
            raise QualificationError(
                f"landing is missing the structured {destination} destination"
            )

    nav_destinations = {
        link.attrs.get("data-docs-destination")
        for link in walk(navigation)
        if link.tag == "a"
    }
    if not {"api-reference", "sdk-guide"}.issubset(nav_destinations):
        raise QualificationError(
            "primary navigation must expose both API reference and SDK guide"
        )

    primary = one(
        select(root, **{"data-docs-priority": "primary"}),
        "primary documentation action",
    )
    if primary.attrs.get("data-docs-destination") != "api-reference":
        raise QualificationError("primary action must lead to the API reference")
    if primary.attrs.get("data-access") != "general":
        raise QualificationError("primary action must be generally available")
    if "cloud" in (primary.attrs.get("href", "") + visible_text(primary)).casefold():
        raise QualificationError("Cloud must not be the primary action")

    cloud = one(select(root, **{"data-journey": "cloud"}), "Cloud section")
    if cloud.attrs.get("data-access") != "limited-early-access":
        raise QualificationError("Cloud section must declare limited early access")
    cloud_text = visible_text(cloud).casefold()
    if "limited" not in cloud_text or "early access" not in cloud_text:
        raise QualificationError(
            "Cloud section must visibly label limited early access"
        )

    general_sections = select(root, **{"data-journey": "general"})
    if not general_sections or cloud.order <= max(
        node.order for node in general_sections
    ):
        raise QualificationError("Cloud section must follow the general Rust path")

    cloud_access = one(destinations["cloud-access"], "Cloud access action")
    access_label = visible_text(cloud_access).casefold()
    if (
        cloud_access.attrs.get("data-access") != "limited-early-access"
        or "request" not in access_label
        or "access" not in access_label
    ):
        raise QualificationError("Cloud action must honestly request early access")
    if (
        destinations["cloud-guide"][0].attrs.get("data-access")
        != "limited-early-access"
    ):
        raise QualificationError(
            "Cloud guide must remain part of the secondary journey"
        )


def landing_links(root: Node) -> tuple[str, ...]:
    return tuple(
        node.attrs["href"]
        for node in walk(root)
        if node.tag == "a" and node.attrs.get("href")
    )


def first_party_http_links(links: Iterable[str], base_url: str) -> tuple[str, ...]:
    destinations: set[str] = set()
    for href in links:
        parsed = urlsplit(href)
        if parsed.scheme in {"mailto", "tel", "javascript"}:
            continue
        destination = urljoin(base_url, href)
        destination_parts = urlsplit(destination)
        if destination_parts.hostname not in FIRST_PARTY_HOSTS:
            continue
        destinations.add(
            urlunsplit(
                (
                    destination_parts.scheme,
                    destination_parts.netloc,
                    destination_parts.path,
                    destination_parts.query,
                    "",
                )
            )
        )
    return tuple(sorted(destinations))


def qualify_local_links(root: Node, build_directory: Path) -> None:
    build_root = build_directory.resolve()
    for href in landing_links(root):
        parsed = urlsplit(href)
        if parsed.scheme or parsed.netloc or href.startswith("#"):
            continue
        path = unquote(parsed.path)
        candidate = build_root / path.lstrip("/")
        if path.endswith("/") or not candidate.suffix:
            candidate /= "index.html"
        candidate = candidate.resolve()
        try:
            candidate.relative_to(build_root)
        except ValueError as error:
            raise QualificationError(
                f"local landing destination escapes the Pages artifact: {href}"
            ) from error
        if not candidate.is_file():
            raise QualificationError(
                f"local landing destination is missing from the Pages artifact: {href}"
            )


def read_2xx(
    url: str,
    timeout: float,
    opener: Callable[..., object] = urlopen,
) -> str:
    request = Request(
        url,
        headers={
            "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
            "User-Agent": "durable-workflow-docs-qualification/1",
        },
    )
    with opener(request, timeout=timeout) as response:  # type: ignore[attr-defined]
        status = response.getcode()  # type: ignore[attr-defined]
        if status is None or not 200 <= status < 300:
            raise QualificationError(f"{url} returned HTTP {status}")
        return response.read().decode("utf-8", errors="replace")  # type: ignore[attr-defined]


def retry(
    description: str,
    operation: Callable[[], str],
    attempts: int,
    delay: float,
) -> str:
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            return operation()
        except (HTTPError, URLError, OSError, QualificationError) as error:
            last_error = error
            if attempt < attempts:
                time.sleep(delay)
    raise QualificationError(
        f"{description} did not return qualified HTTP 2xx content after "
        f"{attempts} attempt(s): {last_error}"
    ) from last_error


def qualify_http_links(
    root: Node,
    base_url: str,
    attempts: int,
    delay: float,
    timeout: float,
    reader: Callable[[str, float], str] = read_2xx,
) -> tuple[str, ...]:
    destinations = first_party_http_links(landing_links(root), base_url)
    for destination in destinations:
        retry(
            destination,
            lambda destination=destination: reader(destination, timeout),
            attempts,
            delay,
        )
    return destinations


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--build-directory", type=Path)
    source.add_argument("--landing-url")
    parser.add_argument("--manifest", type=Path, default=ROOT / "Cargo.toml")
    parser.add_argument("--check-external", action="store_true")
    parser.add_argument("--attempts", type=int, default=3)
    parser.add_argument("--link-attempts", type=int, default=3)
    parser.add_argument("--retry-delay", type=float, default=2)
    parser.add_argument("--timeout", type=float, default=20)
    return parser.parse_args()


def main() -> int:
    arguments = parse_args()
    if arguments.attempts < 1 or arguments.link_attempts < 1:
        print(
            "documentation landing qualification requires positive attempts",
            file=sys.stderr,
        )
        return 2

    try:
        crate_version, rust_version = manifest_identity(arguments.manifest)
        if arguments.build_directory is not None:
            landing_path = arguments.build_directory / "index.html"
            root = parse_document(landing_path.read_text(encoding="utf-8"))
            validate_structure(root, crate_version, rust_version)
            qualify_local_links(root, arguments.build_directory)
            destinations: tuple[str, ...] = ()
            if arguments.check_external:
                destinations = qualify_http_links(
                    root,
                    "https://rust.durable-workflow.com/",
                    arguments.link_attempts,
                    arguments.retry_delay,
                    arguments.timeout,
                )
        else:
            landing_url = arguments.landing_url
            assert landing_url is not None

            def fetch_qualified_landing() -> str:
                html = read_2xx(landing_url, arguments.timeout)
                validate_structure(parse_document(html), crate_version, rust_version)
                return html

            html = retry(
                landing_url,
                fetch_qualified_landing,
                arguments.attempts,
                arguments.retry_delay,
            )
            root = parse_document(html)
            destinations = qualify_http_links(
                root,
                landing_url,
                arguments.link_attempts,
                arguments.retry_delay,
                arguments.timeout,
            )
    except (OSError, QualificationError) as error:
        print(f"documentation landing qualification failed:\n{error}", file=sys.stderr)
        return 1

    suffix = (
        f" and {len(destinations)} live first-party destination(s)"
        if destinations
        else ""
    )
    print(f"Qualified the general-first Rust SDK landing{suffix}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
