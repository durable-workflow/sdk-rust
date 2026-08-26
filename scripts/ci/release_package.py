#!/usr/bin/env python3
"""Verify source release notes and their packaged crate identity."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import re
import sys
import tarfile
import tomllib
from typing import Any

from markdown_it import MarkdownIt
from markdown_it.token import Token


COMMIT_PATTERN = re.compile(r"^[0-9a-f]{40}$")
MARKDOWN = MarkdownIt("commonmark")
SCHEMA = "durable-workflow.rust-release-package/v1"


class ReleasePackageError(RuntimeError):
    """Release source or crate contents are not safe to authorize."""


@dataclass(frozen=True)
class ParsedHeading:
    """One semantic top-level CommonMark heading and its source range."""

    token_index: int
    start_line: int
    end_line: int
    rank: int
    title: str


def sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def load_package(manifest: Path) -> dict[str, Any]:
    try:
        parsed = tomllib.loads(manifest.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, tomllib.TOMLDecodeError) as error:
        raise ReleasePackageError(f"could not read Cargo manifest: {error}") from error
    package = parsed.get("package")
    if not isinstance(package, dict):
        raise ReleasePackageError("Cargo manifest is missing package metadata")
    name = package.get("name")
    version = package.get("version")
    if not isinstance(name, str) or not name:
        raise ReleasePackageError("Cargo manifest has no package name")
    if not isinstance(version, str) or not version:
        raise ReleasePackageError("Cargo manifest has no package version")
    return package


def _heading_names_version(title: str, version: str) -> bool:
    return re.fullmatch(
        rf"\[?{re.escape(version)}\]?(?:[ \t]+-[ \t]+\d{{4}}-\d{{2}}-\d{{2}})?",
        title.strip(),
    ) is not None


def _normalized_inline_text(token: Token) -> str:
    children = token.children or []
    if any(child.type == "html_inline" for child in children):
        return ""
    parts = [
        " " if child.type in {"softbreak", "hardbreak"} else child.content
        for child in children
        if child.type in {"text", "code_inline", "image", "softbreak", "hardbreak"}
    ]
    return " ".join("".join(parts).split())


def _top_level_headings(tokens: list[Token]) -> list[ParsedHeading]:
    headings: list[ParsedHeading] = []
    for index, token in enumerate(tokens):
        if token.type != "heading_open" or token.level != 0:
            continue
        if (
            token.map is None
            or len(token.map) != 2
            or not re.fullmatch(r"h[1-6]", token.tag)
            or index + 1 >= len(tokens)
            or tokens[index + 1].type != "inline"
        ):
            raise ReleasePackageError(
                "CommonMark parser returned an invalid top-level heading"
            )
        headings.append(
            ParsedHeading(
                token_index=index,
                start_line=token.map[0],
                end_line=token.map[1],
                rank=int(token.tag[1]),
                title=_normalized_inline_text(tokens[index + 1]),
            )
        )
    return headings


def _visible_content_lines(
    tokens: list[Token], *, start_line: int, end_line: int
) -> int:
    heading_inline_tokens = {
        index + 1
        for index, token in enumerate(tokens[:-1])
        if token.type == "heading_open" and tokens[index + 1].type == "inline"
    }
    visible_lines: set[int] = set()
    for index, token in enumerate(tokens):
        if token.map is None or index in heading_inline_tokens:
            continue
        token_start = max(start_line, token.map[0])
        token_end = min(end_line, token.map[1])
        if token_start >= token_end:
            continue

        visible = ""
        if token.type == "inline":
            visible = _normalized_inline_text(token)
        elif token.type in {"code_block", "fence"}:
            visible = token.content.strip()
        if visible:
            visible_lines.update(range(token_start, token_end))
    return len(visible_lines)


def _line_starts(value: bytes) -> list[int]:
    starts = [0]
    index = 0
    while index < len(value):
        if value[index] == 0x0D:
            index += 1
            if index < len(value) and value[index] == 0x0A:
                index += 1
            starts.append(index)
        elif value[index] == 0x0A:
            index += 1
            starts.append(index)
        else:
            index += 1
    return starts


def release_entry(changelog: bytes, version: str) -> dict[str, Any]:
    try:
        text = changelog.decode("utf-8")
    except UnicodeError as error:
        raise ReleasePackageError("CHANGELOG.md is not valid UTF-8") from error

    tokens = MARKDOWN.parse(text)
    headings = _top_level_headings(tokens)
    matches = [
        heading
        for heading in headings
        if _heading_names_version(heading.title, version)
    ]
    if len(matches) != 1:
        raise ReleasePackageError(
            f"CHANGELOG.md must contain one current-version entry for {version}"
        )

    current = matches[0]
    boundary: ParsedHeading | None = None
    for heading in headings:
        if (
            heading.token_index > current.token_index
            and heading.rank <= current.rank
        ):
            boundary = heading
            break

    line_starts = _line_starts(changelog)
    end_line = boundary.start_line if boundary is not None else len(line_starts)
    content_lines = _visible_content_lines(
        tokens, start_line=current.end_line, end_line=end_line
    )
    if not content_lines:
        raise ReleasePackageError(
            f"CHANGELOG.md current-version entry for {version} has no release notes"
        )

    if current.start_line >= len(line_starts):
        raise ReleasePackageError("CommonMark heading source map is out of range")
    start_offset = line_starts[current.start_line]
    if boundary is None:
        end_offset = len(changelog)
    elif boundary.start_line < len(line_starts):
        end_offset = line_starts[boundary.start_line]
    else:
        raise ReleasePackageError("CommonMark section source map is out of range")
    entry = changelog[start_offset:end_offset]
    return {
        "version": version,
        "entry_sha256": sha256(entry),
        "content_lines": content_lines,
    }


def verify_source(manifest: Path, changelog: Path) -> dict[str, Any]:
    package = load_package(manifest)
    try:
        changelog_bytes = changelog.read_bytes()
    except OSError as error:
        raise ReleasePackageError(f"could not read CHANGELOG.md: {error}") from error
    entry = release_entry(changelog_bytes, str(package["version"]))
    return {
        "package": str(package["name"]),
        "package_version": str(package["version"]),
        "changelog": {
            "path": changelog.name,
            "sha256": sha256(changelog_bytes),
            **entry,
        },
    }


def _regular_member(archive: tarfile.TarFile, name: str) -> tarfile.TarInfo:
    matches = [member for member in archive.getmembers() if member.name == name]
    if len(matches) != 1 or not matches[0].isfile():
        raise ReleasePackageError(f"crate archive must contain one regular {name}")
    return matches[0]


def _member_bytes(archive: tarfile.TarFile, member: tarfile.TarInfo) -> bytes:
    extracted = archive.extractfile(member)
    if extracted is None:
        raise ReleasePackageError(f"could not read crate member {member.name}")
    return extracted.read()


def verify_archive(
    archive_path: Path,
    manifest: Path,
    changelog: Path,
    expected_vcs_commit: str | None = None,
) -> dict[str, Any]:
    source = verify_source(manifest, changelog)
    if expected_vcs_commit is not None and not COMMIT_PATTERN.fullmatch(
        expected_vcs_commit
    ):
        raise ReleasePackageError("expected VCS commit must be a full lowercase SHA-1")

    try:
        archive_bytes = archive_path.read_bytes()
        with tarfile.open(archive_path, "r:*") as archive:
            root = f"{source['package']}-{source['package_version']}"
            packaged_changelog = _member_bytes(
                archive, _regular_member(archive, f"{root}/CHANGELOG.md")
            )
            vcs_raw = _member_bytes(
                archive, _regular_member(archive, f"{root}/.cargo_vcs_info.json")
            )
    except (OSError, tarfile.TarError) as error:
        raise ReleasePackageError(f"could not inspect crate archive: {error}") from error

    try:
        source_changelog = changelog.read_bytes()
    except OSError as error:
        raise ReleasePackageError(f"could not read CHANGELOG.md: {error}") from error
    if packaged_changelog != source_changelog:
        raise ReleasePackageError(
            "packaged CHANGELOG.md differs from the authorized source changelog"
        )
    packaged_entry = release_entry(packaged_changelog, source["package_version"])
    if packaged_entry["entry_sha256"] != source["changelog"]["entry_sha256"]:
        raise ReleasePackageError(
            "packaged current-version release entry differs from authorized source"
        )

    try:
        vcs = json.loads(vcs_raw)
    except (UnicodeError, json.JSONDecodeError) as error:
        raise ReleasePackageError("crate VCS metadata is not valid JSON") from error
    git = vcs.get("git") if isinstance(vcs, dict) else None
    vcs_commit = git.get("sha1") if isinstance(git, dict) else None
    vcs_dirty = git.get("dirty", False) if isinstance(git, dict) else None
    if not isinstance(vcs_commit, str) or not COMMIT_PATTERN.fullmatch(vcs_commit):
        raise ReleasePackageError("crate VCS metadata has no exact source commit")
    if vcs_dirty is not False:
        raise ReleasePackageError("crate VCS metadata does not identify a clean source")
    if expected_vcs_commit is not None and vcs_commit != expected_vcs_commit:
        raise ReleasePackageError(
            "crate VCS metadata differs from the authorized release commit"
        )

    return {
        "schema": SCHEMA,
        "version": 1,
        **source,
        "archive": {
            "path": archive_path.name,
            "sha256": sha256(archive_bytes),
            "vcs_commit": vcs_commit,
            "vcs_dirty": vcs_dirty,
            "changelog_sha256": sha256(packaged_changelog),
            "release_entry_sha256": packaged_entry["entry_sha256"],
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=Path("Cargo.toml"))
    parser.add_argument("--changelog", type=Path, default=Path("CHANGELOG.md"))
    parser.add_argument("--archive", type=Path)
    parser.add_argument("--expected-vcs-commit")
    arguments = parser.parse_args()

    try:
        if arguments.archive is None:
            if arguments.expected_vcs_commit is not None:
                parser.error("--expected-vcs-commit requires --archive")
            evidence = {
                "schema": SCHEMA,
                "version": 1,
                **verify_source(arguments.manifest, arguments.changelog),
            }
        else:
            evidence = verify_archive(
                arguments.archive,
                arguments.manifest,
                arguments.changelog,
                arguments.expected_vcs_commit,
            )
    except ReleasePackageError as error:
        print(f"release package verification failed: {error}", file=sys.stderr)
        return 1

    print(json.dumps(evidence, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
