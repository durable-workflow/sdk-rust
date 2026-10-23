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
SOURCE_MANIFEST = "Cargo.toml"
SOURCE_CHANGELOG = "CHANGELOG.md"


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


def _release_entry_content(changelog: bytes, version: str) -> tuple[bytes, list[str]]:
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
    return entry, content_lines


def release_entry(changelog: bytes, version: str) -> dict[str, Any]:
    entry, content_lines = _release_entry_content(changelog, version)
    return {
        "version": version,
        "entry_sha256": sha256(entry),
        "content_lines": content_lines,
    }


def render_release_notes(changelog: bytes, version: str) -> str:
    """Render the exact validated source entry, rather than merged-PR ancestry."""
    entry, _ = _release_entry_content(changelog, version)
    return entry.decode("utf-8").rstrip("\r\n") + "\n"


def source_release_notes(version: str) -> str:
    root = Path.cwd()
    manifest = root / SOURCE_MANIFEST
    changelog = root / SOURCE_CHANGELOG
    for path in (manifest, changelog):
        if path.is_symlink():
            raise ReleasePackageError(f"release notes require regular checkout files: {path.name}")
    package = load_package(manifest)
    if package["version"] != version:
        raise ReleasePackageError("release notes version differs from Cargo package version")
    try:
        changelog_bytes = changelog.read_bytes()
    except OSError as error:
        raise ReleasePackageError(f"could not read CHANGELOG.md: {error}") from error
    return render_release_notes(changelog_bytes, version)


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


def restore_missing_lockfile(
    archive_path: Path,
    manifest: Path,
    changelog: Path,
    expected_vcs_commit: str,
    expected_archive_sha256: str,
) -> dict[str, Any]:
    identity = verify_archive(archive_path, manifest, changelog, expected_vcs_commit)
    if (
        not re.fullmatch(r"[0-9a-f]{64}", expected_archive_sha256)
        or identity["archive"]["sha256"] != expected_archive_sha256
    ):
        raise ReleasePackageError("registry archive checksum differs from expected SHA-256")
    try:
        with tarfile.open(archive_path, "r:*") as archive:
            member = _regular_member(
                archive, f"{identity['package']}-{identity['package_version']}/Cargo.lock"
            )
            if not 0 < member.size <= 1024 * 1024:
                raise ReleasePackageError("packaged Cargo.lock exceeds the 1 MiB limit")
            lockfile = _member_bytes(archive, member)
        parsed = tomllib.loads(lockfile.decode("utf-8"))
        version = parsed.get("version")
        if not isinstance(version, int) or version not in (3, 4) or not isinstance(parsed.get("package"), list):
            raise ReleasePackageError("packaged Cargo.lock has no supported dependency graph")
        with (manifest.parent / "Cargo.lock").open("xb") as output:
            output.write(lockfile)
    except (OSError, UnicodeError, tomllib.TOMLDecodeError, tarfile.TarError) as error:
        raise ReleasePackageError(f"could not restore missing Cargo.lock: {error}") from error
    identity["lockfile"] = {"sha256": sha256(lockfile), "source": "verified_registry_archive"}
    return identity


def _validate_notes_arguments(
    arguments: argparse.Namespace, parser: argparse.ArgumentParser,
) -> None:
    if any((arguments.archive, arguments.expected_vcs_commit,
            arguments.expected_archive_sha256, arguments.restore_missing_lockfile)):
        parser.error("--release-notes cannot be combined with archive verification")
    if (arguments.manifest != Path(SOURCE_MANIFEST)
            or arguments.changelog != Path(SOURCE_CHANGELOG)):
        parser.error("--release-notes uses the current checkout files; path overrides are refused")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=Path(SOURCE_MANIFEST))
    parser.add_argument("--changelog", type=Path, default=Path(SOURCE_CHANGELOG))
    parser.add_argument("--archive", type=Path)
    parser.add_argument("--expected-vcs-commit")
    parser.add_argument("--expected-archive-sha256")
    parser.add_argument("--restore-missing-lockfile", action="store_true")
    parser.add_argument(
        "--release-notes", metavar="VERSION",
        help="print the exact current-version Markdown after manifest validation",
    )
    arguments = parser.parse_args()
    if arguments.expected_archive_sha256 is not None and not arguments.restore_missing_lockfile:
        parser.error("--expected-archive-sha256 requires --restore-missing-lockfile")

    try:
        if arguments.release_notes is not None:
            _validate_notes_arguments(arguments, parser)
            sys.stdout.write(source_release_notes(arguments.release_notes))
            return 0
        elif arguments.restore_missing_lockfile:
            if not all((arguments.archive, arguments.expected_vcs_commit, arguments.expected_archive_sha256)):
                parser.error("lockfile recovery requires archive, source commit and registry checksum")
            evidence = restore_missing_lockfile(
                arguments.archive, arguments.manifest, arguments.changelog,
                arguments.expected_vcs_commit, arguments.expected_archive_sha256,
            )
        elif arguments.archive is None:
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
