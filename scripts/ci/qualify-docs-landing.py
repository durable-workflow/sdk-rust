#!/usr/bin/env python3
"""Qualify the Rust documentation landing contract and its destinations."""

from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from html.parser import HTMLParser
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import time
import tomllib
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
VERSIONLESS_INSTALLER = (
    "curl -fsSL https://durable-workflow.com/install-sdk.sh | sh -s -- rust"
)
INSTALLER_URL = "https://durable-workflow.com/install-sdk.sh"
QUICKSTART_CONTRACT_URL = (
    "https://durable-workflow.com/quickstart-execution-contract.json"
)
QUICKSTART_CONTRACT_SCHEMA = (
    "durable-workflow.docs.v2.quickstart-execution-contract"
)
EXACT_PRERELEASE_VERSION = re.compile(
    r"\b\d+\.\d+\.\d+-(?:alpha|beta|rc)\.\d+\b"
)
VISIBLE_CARGO_PATH = re.compile(
    r"\bcargo\s+add\s+durable-workflow\b|\bdurable-workflow\s*="
)
MARKDOWN_CODE_BLOCK = re.compile(r"```[^\n]*\n(?P<body>.*?)```", re.DOTALL)
CARGO_ADD_REQUIREMENT = re.compile(
    r"\bcargo\s+add\s+durable-workflow@(?P<requirement>[^\s`]+)"
)
CARGO_TOML_REQUIREMENT = re.compile(
    r"\bdurable-workflow\s*=\s*[\"'](?P<requirement>[^\"']+)[\"']"
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
    if VERSIONLESS_INSTALLER not in first_task_text:
        raise QualificationError("first task must use the versionless SDK installer")
    if VISIBLE_CARGO_PATH.search(visible_text(body)):
        raise QualificationError(
            "visible Cargo installation must use the qualified SDK installer"
        )
    if EXACT_PRERELEASE_VERSION.search(visible_text(body)):
        raise QualificationError(
            "visible onboarding must not contain an exact prerelease version"
        )
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


def qualified_rust_version(contract_text: str) -> str:
    try:
        contract = json.loads(contract_text)
    except json.JSONDecodeError as error:
        raise QualificationError(
            "public quickstart authority is not valid JSON"
        ) from error
    if not isinstance(contract, dict) or contract.get("schema") != QUICKSTART_CONTRACT_SCHEMA:
        raise QualificationError("public quickstart authority has an unsupported schema")
    artifacts = contract.get("artifacts")
    rust = artifacts.get("sdk-rust") if isinstance(artifacts, dict) else None
    version = rust.get("version") if isinstance(rust, dict) else None
    if not isinstance(version, str) or EXACT_PRERELEASE_VERSION.fullmatch(version) is None:
        raise QualificationError(
            "public quickstart authority is missing a qualified Rust prerelease"
        )
    return version


def markdown_code_samples(markdown: str) -> tuple[str, ...]:
    return tuple(match.group("body") for match in MARKDOWN_CODE_BLOCK.finditer(markdown))


def html_code_samples(root: Node) -> tuple[str, ...]:
    return tuple(visible_text(node) for node in walk(root) if node.tag == "code")


def cargo_requirements(samples: Iterable[str]) -> tuple[str, ...]:
    requirements: list[str] = []
    for sample in samples:
        requirements.extend(
            match.group("requirement").rstrip(",;")
            for match in CARGO_ADD_REQUIREMENT.finditer(sample)
        )
        requirements.extend(
            match.group("requirement")
            for match in CARGO_TOML_REQUIREMENT.finditer(sample)
        )
    return tuple(requirements)


def validate_visible_cargo_paths(
    root: Node,
    readme: str,
    qualified_version: str,
) -> int:
    surfaces = (
        ("README", readme, markdown_code_samples(readme)),
        ("Rust landing", visible_text(root), html_code_samples(root)),
    )
    direct_paths = 0
    for name, text, samples in surfaces:
        if VERSIONLESS_INSTALLER not in text:
            raise QualificationError(
                f"{name} must expose the qualified versionless Rust installer"
            )
        for requirement in cargo_requirements(samples):
            direct_paths += 1
            if requirement != f"={qualified_version}":
                raise QualificationError(
                    f"{name} Cargo path resolves {requirement}, but the public "
                    f"authority qualifies ={qualified_version}"
                )
    return direct_paths


def resolved_dependency_version(lock_text: str) -> str:
    try:
        packages = tomllib.loads(lock_text).get("package", [])
    except (ValueError, TypeError) as error:
        raise QualificationError("clean Cargo.lock is not valid TOML") from error
    matches = [
        package.get("version")
        for package in packages
        if isinstance(package, dict) and package.get("name") == "durable-workflow"
    ]
    if len(matches) != 1 or not isinstance(matches[0], str):
        raise QualificationError(
            "clean Cargo.lock must contain exactly one durable-workflow package"
        )
    return matches[0]


def qualify_cargo_resolution(
    root: Node,
    readme: str,
    installer_text: str,
    contract_text: str,
    cargo: str,
    timeout: float,
) -> tuple[str, int]:
    qualified_version = qualified_rust_version(contract_text)
    direct_paths = validate_visible_cargo_paths(root, readme, qualified_version)

    with tempfile.TemporaryDirectory(prefix="dw-rust-onboarding-") as directory:
        temporary = Path(directory)
        project = temporary / "consumer"
        project.mkdir()
        manifest = project / "Cargo.toml"
        manifest.write_text(
            "[package]\n"
            'name = "qualified-rust-onboarding"\n'
            'version = "0.0.0"\n'
            'edition = "2021"\n\n'
            "[dependencies]\n",
            encoding="utf-8",
        )
        (project / "src").mkdir()
        (project / "src/lib.rs").write_text("", encoding="utf-8")

        installer = temporary / "install-sdk.sh"
        installer.write_text(installer_text, encoding="utf-8")
        contract = temporary / "quickstart-execution-contract.json"
        contract.write_text(contract_text, encoding="utf-8")

        environment = os.environ.copy()
        environment.update(
            {
                "CARGO_BIN": cargo,
                "CARGO_HOME": str(temporary / "cargo-home"),
                "CARGO_TARGET_DIR": str(temporary / "cargo-target"),
                "CARGO_NET_RETRY": "3",
                "DURABLE_WORKFLOW_QUICKSTART_CONTRACT_URL": contract.as_uri(),
            }
        )
        try:
            result = subprocess.run(
                ["sh", str(installer), "rust"],
                cwd=project,
                env=environment,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=timeout,
                check=False,
            )
        except (OSError, subprocess.SubprocessError) as error:
            raise QualificationError(
                f"qualified Rust installer could not run in a clean project: {error}"
            ) from error
        if result.returncode != 0:
            detail = result.stderr.strip() or result.stdout.strip() or "no output"
            raise QualificationError(
                "qualified Rust installer failed in a clean project: " + detail
            )

        try:
            manifest_data = tomllib.loads(manifest.read_text(encoding="utf-8"))
        except (OSError, ValueError, TypeError) as error:
            raise QualificationError(
                "qualified Rust installer did not emit a valid Cargo.toml"
            ) from error
        dependency = manifest_data.get("dependencies", {}).get("durable-workflow")
        requirement = (
            dependency.get("version") if isinstance(dependency, dict) else dependency
        )
        if requirement != f"={qualified_version}":
            raise QualificationError(
                "qualified Rust installer emitted Cargo requirement "
                f"{requirement!r}, expected '={qualified_version}'"
            )

        lock_path = project / "Cargo.lock"
        if not lock_path.is_file():
            raise QualificationError(
                "qualified Rust installer did not emit a clean Cargo.lock"
            )
        resolved_version = resolved_dependency_version(
            lock_path.read_text(encoding="utf-8")
        )
        if resolved_version != qualified_version:
            raise QualificationError(
                f"clean Cargo.lock resolved {resolved_version}, but the public "
                f"authority qualifies {qualified_version}"
            )

    return qualified_version, direct_paths


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--build-directory", type=Path)
    source.add_argument("--landing-url")
    parser.add_argument("--manifest", type=Path, default=ROOT / "Cargo.toml")
    parser.add_argument("--check-external", action="store_true")
    parser.add_argument("--check-cargo-resolution", action="store_true")
    parser.add_argument("--attempts", type=int, default=3)
    parser.add_argument("--link-attempts", type=int, default=3)
    parser.add_argument("--retry-delay", type=float, default=2)
    parser.add_argument("--timeout", type=float, default=20)
    parser.add_argument("--cargo-timeout", type=float, default=180)
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
            html = landing_path.read_text(encoding="utf-8")
            root = parse_document(html)
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
        cargo_resolution: tuple[str, int] | None = None
        if arguments.check_cargo_resolution:
            installer_text = retry(
                INSTALLER_URL,
                lambda: read_2xx(INSTALLER_URL, arguments.timeout),
                arguments.attempts,
                arguments.retry_delay,
            )
            contract_text = retry(
                QUICKSTART_CONTRACT_URL,
                lambda: read_2xx(QUICKSTART_CONTRACT_URL, arguments.timeout),
                arguments.attempts,
                arguments.retry_delay,
            )
            cargo = shutil.which("cargo")
            if cargo is None:
                raise QualificationError(
                    "clean onboarding qualification requires Cargo"
                )
            cargo_resolution = qualify_cargo_resolution(
                root,
                (ROOT / "README.md").read_text(encoding="utf-8"),
                installer_text,
                contract_text,
                cargo,
                arguments.cargo_timeout,
            )
    except (OSError, QualificationError) as error:
        print(f"documentation landing qualification failed:\n{error}", file=sys.stderr)
        return 1

    suffix = (
        f" and {len(destinations)} live first-party destination(s)"
        if destinations
        else ""
    )
    if cargo_resolution is not None:
        version, direct_paths = cargo_resolution
        suffix += (
            f"; clean Cargo resolution selected {version} with "
            f"{direct_paths} direct alternative path(s)"
        )
    print(f"Qualified the general-first Rust SDK landing{suffix}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
