#!/usr/bin/env python3
"""Verify source release notes and their packaged crate identity."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime
import hashlib
import io
import json
import os
from pathlib import Path
import re
import stat
import subprocess
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

    _archive_files(archive_path, f"{source['package']}-{source['package_version']}")
    # One no-follow read: the checksum and the inspected members come from the
    # same bytes.
    archive_bytes = _read_regular_no_follow(archive_path, "crate archive")
    try:
        with tarfile.open(fileobj=io.BytesIO(archive_bytes), mode="r:*") as archive:
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
    archive_bytes = _read_regular_no_follow(archive_path, "crate archive")
    try:
        with tarfile.open(fileobj=io.BytesIO(archive_bytes), mode="r:*") as archive:
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


APPROVAL_SCHEMA = "durable-workflow.rust-sdk.approved-package/v1"
# Semantic Versioning 2.0.0, ASCII only. A version is validated with this before
# it names a file (release/approved-packages/<version>.json); no identifier can
# be empty, so it can never contain "/" or a ".." component.
STRICT_SEMVER = re.compile(
    r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)"
    r"(?:-(?:0|[1-9]\d*|\d*[A-Za-z-][0-9A-Za-z-]*)(?:\.(?:0|[1-9]\d*|\d*[A-Za-z-][0-9A-Za-z-]*))*)?"
    r"(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$",
    re.ASCII,
)


def _refuse_unsafe_text(value: str, label: str) -> None:
    if not value or "\0" in value:
        raise ReleasePackageError(f"{label} is empty or contains a NUL byte")
    if ".." in Path(value).parts:
        raise ReleasePackageError(f"{label} contains a '..' component")


def confined_root(value: str | os.PathLike[str]) -> str:
    """Canonicalize an explicit trust root: an existing directory, not a link."""
    text = os.fspath(value)
    _refuse_unsafe_text(text, "path root")
    if os.path.islink(text):
        raise ReleasePackageError("path root must not be a symbolic link")
    root = os.path.realpath(text)
    if not os.path.isdir(root):
        raise ReleasePackageError("path root is not an existing directory")
    return root


def confined_file(
    root: str, relative: str | os.PathLike[str], *, must_exist: bool = True,
) -> Path:
    """Resolve `relative` strictly inside canonical `root`.

    Refuses NUL bytes, absolute paths, '..' components and a symbolic link at
    any component below the root, then requires the canonical result to stay
    inside the root (os.path.commonpath) and, if `must_exist`, to be a regular
    file.
    """
    text = os.fspath(relative)
    _refuse_unsafe_text(text, "path")
    if os.path.isabs(text):
        raise ReleasePackageError("path must be relative to its root, not absolute")
    current = root
    for part in Path(text).parts:
        current = os.path.join(current, part)
        if os.path.islink(current):
            raise ReleasePackageError("path must not pass through a symbolic link")
    resolved = os.path.realpath(os.path.join(root, text))
    if os.path.commonpath([root, resolved]) != root:
        raise ReleasePackageError("path escapes its root")
    if must_exist and not os.path.isfile(resolved):
        raise ReleasePackageError("path is not an existing regular file")
    if not must_exist and os.path.lexists(resolved):
        raise ReleasePackageError("path already exists")
    return Path(resolved)


def approval_record_relative(version: str) -> str:
    """release/approved-packages/<version>.json, only for a strict SemVer version."""
    if not isinstance(version, str) or not STRICT_SEMVER.fullmatch(version):
        raise ReleasePackageError("approval version is not a strict SemVer version")
    return f"{APPROVAL_DIRECTORY}/{version}.json"


def approval_summary_relative(version: str) -> str:
    """release/approved-packages/<version>.summary.json, the run's exact summary.json."""
    return approval_record_relative(version).removesuffix(".json") + ".summary.json"


def _safe_member_name(name: object) -> bool:
    if not isinstance(name, str) or not name or "\0" in name or "\\" in name:
        return False
    if name.startswith("/") or os.path.isabs(name):
        return False
    return all(part not in ("", ".", "..") for part in name.split("/"))
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
CARGO_VERSION_PATTERN = re.compile(r"^\d+\.\d+\.\d+$", re.ASCII)
APPROVAL_FIELDS = {
    "schema", "package", "version", "commit", "crate_sha256", "cargo_version",
    "files", "file_list_sha256", "acceptance",
}


def file_list_sha256(files: list[str]) -> str:
    return sha256("".join(f"{name}\n" for name in sorted(files)).encode())


ACCEPTANCE_FIELDS = {
    "summary_sha256", "harness_commit", "server_index", "executed_manifest", "platform",
    "expected_server_version", "expected_revision", "expected_release_tag",
    "cluster_versions", "cells", "finished_at", "outcome",
}
ACCEPTANCE_CELLS = ["deadline", "lifecycle"]
ACCEPTANCE_PLATFORMS = {"linux/amd64", "linux/arm64"}
# <registry>/<repository>@sha256:<digest>, lowercase, as `docker manifest` names it.
IMAGE_DIGEST_PATTERN = re.compile(
    r"^((?:[a-z0-9]+(?:[.-][a-z0-9]+)*(?::[0-9]+)?/)?"
    r"[a-z0-9]+(?:(?:[._]|__|-+)[a-z0-9]+)*(?:/[a-z0-9]+(?:(?:[._]|__|-+)[a-z0-9]+)*)*)"
    r"@sha256:[0-9a-f]{64}$",
    re.ASCII,
)
APPROVAL_DIRECTORY = "release/approved-packages"


def _is_text(value: object, pattern: re.Pattern[str] | None = None) -> bool:
    return isinstance(value, str) and (pattern is None or pattern.fullmatch(value) is not None)


def _valid_timestamp(value: object) -> bool:
    if not isinstance(value, str):
        return False
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return False
    return parsed.tzinfo is not None and parsed.utcoffset() is not None


def validate_acceptance(acceptance: object) -> None:
    """The receipt of one passing exact-Server lifecycle and deadline run.

    The inner schema is exact (no missing or extra keys, strict types) and its
    relationships are bound: one image repository for index and executed
    manifest, tag equal to the expected Server version, every cell reporting
    that version, both cells, a supported platform and a valid completion time.
    """
    if not isinstance(acceptance, dict) or set(acceptance) != ACCEPTANCE_FIELDS:
        raise ReleasePackageError("approval acceptance receipt has missing or unexpected fields")
    index = IMAGE_DIGEST_PATTERN.fullmatch(acceptance["server_index"]) if _is_text(acceptance["server_index"]) else None
    executed = (IMAGE_DIGEST_PATTERN.fullmatch(acceptance["executed_manifest"])
                if _is_text(acceptance["executed_manifest"]) else None)
    version = acceptance["expected_server_version"]
    checks = {
        "summary_sha256": _is_text(acceptance["summary_sha256"], SHA256_PATTERN),
        "harness_commit": _is_text(acceptance["harness_commit"], COMMIT_PATTERN),
        "expected_revision": _is_text(acceptance["expected_revision"], COMMIT_PATTERN),
        "server_index": index is not None,
        "executed_manifest": executed is not None and index is not None and executed.group(1) == index.group(1),
        "platform": _is_text(acceptance["platform"]) and acceptance["platform"] in ACCEPTANCE_PLATFORMS,
        "expected_server_version": _is_text(version, STRICT_SEMVER),
        "expected_release_tag": _is_text(acceptance["expected_release_tag"]) and acceptance["expected_release_tag"] == version,
        "cluster_versions": isinstance(acceptance["cluster_versions"], list) and acceptance["cluster_versions"] == [version],
        "cells": isinstance(acceptance["cells"], list) and acceptance["cells"] == ACCEPTANCE_CELLS,
        "finished_at": _valid_timestamp(acceptance["finished_at"]),
        "outcome": _is_text(acceptance["outcome"]) and acceptance["outcome"] == "pass",
    }
    failed = sorted(field for field, ok in checks.items() if not ok)
    if failed:
        raise ReleasePackageError(f"approval acceptance receipt is invalid: {', '.join(failed)}")


def parse_approval(raw: bytes) -> tuple[dict[str, Any], str]:
    """Parse one reviewed approved-package record and check its exact shape."""
    try:
        approval = json.loads(raw)
    except (UnicodeError, json.JSONDecodeError) as error:
        raise ReleasePackageError(f"could not read approved package record: {error}") from error
    if not isinstance(approval, dict) or set(approval) != APPROVAL_FIELDS:
        raise ReleasePackageError("approved package record has unexpected fields")
    files = approval["files"]
    if (
        approval["schema"] != APPROVAL_SCHEMA
        or not _is_text(approval["package"])
        or not _is_text(approval["version"], STRICT_SEMVER)
        or not _is_text(approval["commit"], COMMIT_PATTERN)
        or not _is_text(approval["crate_sha256"], SHA256_PATTERN)
        or not _is_text(approval["cargo_version"], CARGO_VERSION_PATTERN)
        or not isinstance(files, list)
        or not files
        or not all(_safe_member_name(name) for name in files)
        or files != sorted(set(files))
        or approval["file_list_sha256"] != file_list_sha256(files)
    ):
        raise ReleasePackageError("approved package record is malformed")
    validate_acceptance(approval["acceptance"])
    return approval, sha256(raw)


def _read_regular_no_follow(path: Path, what: str = "approved package record") -> bytes:
    try:
        if not stat.S_ISREG(os.lstat(path).st_mode):
            raise ReleasePackageError(f"{what} is not a regular file")
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    except OSError as error:
        raise ReleasePackageError(f"could not open {what}: {error.strerror}") from error
    with os.fdopen(descriptor, "rb") as handle:
        if not stat.S_ISREG(os.fstat(handle.fileno()).st_mode):
            raise ReleasePackageError(f"{what} is not a regular file")
        return handle.read()


def load_approval(path: Path) -> tuple[dict[str, Any], str]:
    """Read (without following links) and parse one approved-package record."""
    return parse_approval(_read_regular_no_follow(path))


GIT_OBJECT_PATTERN = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")


def _git(repository: str, *arguments: str) -> bytes:
    """Run a fixed git query in the confined repository.

    The repository is the working directory, never an argument, and every
    argument is a constant, the SemVer-derived record path after "--", or a
    validated object id, so no caller value can become a git option.
    """
    result = subprocess.run(
        ["git", *arguments], cwd=repository, capture_output=True, check=False, timeout=60,
    )
    if result.returncode:
        raise ReleasePackageError(f"git {arguments[0]} failed in the approvals repository")
    return result.stdout


def read_tracked_record(repository: str, relative: str) -> bytes:
    """One reviewed file from the maintained-tooling checkout.

    The worktree file must be a regular file reached without any symbolic link
    (lstat + O_NOFOLLOW + fstat), confined to the repository, and
    byte-identical to a regular (mode 100644) blob tracked at exactly
    `relative` in the checked-out commit.
    """
    path = confined_file(repository, relative)
    content = _read_regular_no_follow(path)
    listing = _git(repository, "ls-tree", "-z", "--full-tree", "HEAD", "--", relative)
    entries = [entry for entry in listing.split(b"\0") if entry]
    if len(entries) != 1:
        raise ReleasePackageError(f"approved package file {relative} is not tracked at its exact path")
    meta, _, tracked_path = entries[0].partition(b"\t")
    fields = meta.split()
    if (tracked_path != relative.encode() or len(fields) != 3
            or fields[0] != b"100644" or fields[1] != b"blob"):
        raise ReleasePackageError(f"approved package file {relative} is not a tracked regular file at its exact path")
    object_id = fields[2].decode("ascii")
    if not GIT_OBJECT_PATTERN.fullmatch(object_id):
        raise ReleasePackageError(f"approved package file {relative} has an invalid tracked object id")
    if _git(repository, "cat-file", "blob", object_id) != content:
        raise ReleasePackageError(f"approved package file {relative} differs from its tracked content")
    return content


def read_approval_record(repository: str, version: str) -> tuple[bytes, bytes, str]:
    """The reviewed record and its acceptance summary for `version`.

    Both live under release/approved-packages/, named only after `version`
    passes strict SemVer: <version>.json and <version>.summary.json (the
    harness's exact summary.json bytes). Each is read with read_tracked_record.
    """
    relative = approval_record_relative(version)
    record = read_tracked_record(repository, relative)
    summary = read_tracked_record(repository, approval_summary_relative(version))
    return record, summary, relative


def _summary_mismatches(approval: dict[str, Any], summary: Any) -> list[str]:
    acceptance = approval["acceptance"]
    if not isinstance(summary, dict):
        return ["summary"]
    package = summary.get("package") if isinstance(summary.get("package"), dict) else {}
    crate = summary.get("crate") if isinstance(summary.get("crate"), dict) else {}
    server = summary.get("server") if isinstance(summary.get("server"), dict) else {}
    cells = summary.get("cells") if isinstance(summary.get("cells"), dict) else {}
    cargo = package.get("cargo_version")
    version = acceptance["expected_server_version"]
    checks = {
        "mode": summary.get("mode") == "pre-publication",
        "outcome": summary.get("outcome") == "pass" and summary.get("checks_outcome") == "pass",
        "error": "error" not in summary,
        "cleanup": summary.get("cleanup") == {
            "pass": True, "errors": [], "local_leftovers": [],
            "leftover_containers": [], "leftover_networks": [],
        },
        "harness_commit": summary.get("harness_commit") == acceptance["harness_commit"],
        "finished_at": summary.get("finished_at") == acceptance["finished_at"],
        "expected_server": summary.get("expected_server") == {
            "version": version, "revision": acceptance["expected_revision"],
            "release_tag": acceptance["expected_release_tag"],
        },
        "server": (server.get("index") == acceptance["server_index"]
                   and server.get("executed_manifest") == acceptance["executed_manifest"]
                   and server.get("platform") == acceptance["platform"]),
        "package.identity": (package.get("package") == approval["package"]
                             and package.get("version") == approval["version"]
                             and package.get("commit") == approval["commit"]
                             and package.get("vcs_info") == {"git": {"sha1": approval["commit"]}, "path_in_vcs": ""}),
        "package.crate_sha256": (package.get("crate_sha256") == approval["crate_sha256"]
                                 and package.get("crate_sha256_after_cells") == approval["crate_sha256"]),
        "package.files": (package.get("files") == approval["files"]
                          and package.get("file_list_sha256") == approval["file_list_sha256"]),
        "package.cargo_version": (isinstance(cargo, str) and len(cargo.split()) >= 2
                                  and cargo.split()[0] == "cargo" and cargo.split()[1] == approval["cargo_version"]),
        "crate": (crate.get("sha256") == approval["crate_sha256"]
                  and crate.get("lock_checksum") == approval["crate_sha256"]
                  and crate.get("source_commit") == approval["commit"]
                  and crate.get("registry_published") is False
                  and isinstance(crate.get("baselines"), dict) and crate["baselines"].get("pass") is True),
        "cells": (sorted(cells) == acceptance["cells"]
                  and all(isinstance(cell, dict) and cell.get("outcome") == "pass"
                          and isinstance(cell.get("cluster_info"), dict)
                          and cell["cluster_info"].get("version") == version
                          for cell in cells.values())),
    }
    return sorted(field for field, ok in checks.items() if not ok)


def verify_acceptance_summary(approval: dict[str, Any], summary_record: bytes) -> None:
    """The committed summary.json is the run the approval names, and it passed."""
    if sha256(summary_record) != approval["acceptance"]["summary_sha256"]:
        raise ReleasePackageError("acceptance summary SHA-256 differs from the approval's summary_sha256")
    try:
        summary = json.loads(summary_record)
    except (UnicodeError, json.JSONDecodeError) as error:
        raise ReleasePackageError("acceptance summary is not valid JSON") from error
    mismatches = _summary_mismatches(approval, summary)
    if mismatches:
        raise ReleasePackageError(f"acceptance summary does not match the approval: {', '.join(mismatches)}")


def _archive_files(archive_path: Path, root: str) -> list[str]:
    prefix = f"{root}/"
    # Read the confined crate as a regular file without following links, then
    # list its members from memory; nothing is ever extracted.
    data = _read_regular_no_follow(archive_path, "crate archive")
    try:
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as archive:
            members = archive.getmembers()
    except (OSError, tarfile.TarError, EOFError) as error:
        raise ReleasePackageError(f"could not inspect crate archive: {error}") from error
    names = []
    for member in members:
        # Each member independently: a regular file (no symlink or hardlink)
        # under the exact <name>-<version>/ prefix, with a canonical relative
        # POSIX path (no absolute form, empty, '.', '..', backslash or NUL).
        relative = member.name[len(prefix):] if member.name.startswith(prefix) else None
        if not member.isfile() or relative is None or not _safe_member_name(relative):
            raise ReleasePackageError(f"crate member {member.name!r} is not a regular package file")
        names.append(relative)
    if len(names) != len(set(names)):
        raise ReleasePackageError("crate archive repeats a member")
    return sorted(names)


def verify_approved_archive(
    archive_path: Path,
    manifest: Path,
    changelog: Path,
    expected_vcs_commit: str,
    approval_record: bytes,
    summary_record: bytes,
    cargo_version: str | None = None,
) -> dict[str, Any]:
    """Require an archive to be exactly the approved pre-publication package."""
    identity = verify_archive(archive_path, manifest, changelog, expected_vcs_commit)
    approval, approval_sha256 = parse_approval(approval_record)
    if approval["package"] != identity["package"]:
        raise ReleasePackageError("approved package name differs from the release manifest")
    if approval["version"] != identity["package_version"]:
        raise ReleasePackageError("approved package version differs from the release manifest")
    if approval["commit"] != expected_vcs_commit or approval["commit"] != identity["archive"]["vcs_commit"]:
        raise ReleasePackageError("approved package commit differs from the release commit")
    if cargo_version is not None and approval["cargo_version"] != cargo_version:
        raise ReleasePackageError(
            f"release Cargo {cargo_version} differs from approved Cargo {approval['cargo_version']}"
        )
    files = _archive_files(archive_path, f"{identity['package']}-{identity['package_version']}")
    if files != approval["files"]:
        missing = sorted(set(approval["files"]) - set(files))
        extra = sorted(set(files) - set(approval["files"]))
        raise ReleasePackageError(
            f"archive file set differs from approved package: missing {missing[:10]}, extra {extra[:10]}"
        )
    if identity["archive"]["sha256"] != approval["crate_sha256"]:
        raise ReleasePackageError(
            "archive SHA-256 differs from the approved pre-publication package"
        )
    # The record matches the archive; now its committed acceptance run must too.
    verify_acceptance_summary(approval, summary_record)
    identity["approval"] = {
        "record_sha256": approval_sha256,
        "crate_sha256": approval["crate_sha256"],
        "commit": approval["commit"],
        "version": approval["version"],
        "cargo_version": approval["cargo_version"],
        "file_count": len(approval["files"]),
        "file_list_sha256": approval["file_list_sha256"],
        "acceptance_summary_sha256": approval["acceptance"]["summary_sha256"],
        "server_index": approval["acceptance"]["server_index"],
        "server_version": approval["acceptance"]["expected_server_version"],
        "summary_verified": True,
    }
    return identity


def _validate_notes_arguments(
    arguments: argparse.Namespace, parser: argparse.ArgumentParser,
) -> None:
    if any((arguments.archive, arguments.expected_vcs_commit,
            arguments.expected_archive_sha256, arguments.restore_missing_lockfile,
            arguments.approval_repository, arguments.archive_root, arguments.cargo_version,
            arguments.check_paths)):
        parser.error("--release-notes cannot be combined with archive verification")
    if (arguments.manifest != Path(SOURCE_MANIFEST)
            or arguments.changelog != Path(SOURCE_CHANGELOG)
            or arguments.source_root != "."):
        parser.error("--release-notes uses the current checkout files; path overrides are refused")


def _validate_mode_arguments(
    arguments: argparse.Namespace, parser: argparse.ArgumentParser,
) -> None:
    if arguments.expected_archive_sha256 is not None and not arguments.restore_missing_lockfile:
        parser.error("--expected-archive-sha256 requires --restore-missing-lockfile")
    if arguments.check_paths and any((
        arguments.archive, arguments.archive_root, arguments.approval_repository,
        arguments.expected_vcs_commit, arguments.restore_missing_lockfile, arguments.cargo_version,
    )):
        parser.error("--check-paths takes only --source-root, --manifest and --changelog")
    if (arguments.archive is None) != (arguments.archive_root is None):
        parser.error("--archive and --archive-root must be given together")
    if arguments.cargo_version is not None and arguments.approval_repository is None:
        parser.error("--cargo-version requires --approval-repository")
    if arguments.approval_repository is not None and (
        arguments.archive is None or arguments.expected_vcs_commit is None
        or arguments.restore_missing_lockfile
    ):
        parser.error("--approval-repository requires --archive and --expected-vcs-commit only")


def _run(arguments: argparse.Namespace, parser: argparse.ArgumentParser) -> dict[str, Any]:
    # Every file named on the command line is confined to an explicit root
    # before it is opened: the checkout (--source-root), the package directory
    # (--archive-root) or the reviewed approvals directory (--approval-repository).
    source_root = confined_root(arguments.source_root)
    manifest = confined_file(source_root, arguments.manifest)
    changelog = confined_file(source_root, arguments.changelog)
    if arguments.check_paths:
        return {"manifest": str(manifest), "changelog": str(changelog)}
    archive = None
    if arguments.archive is not None:
        archive = confined_file(confined_root(arguments.archive_root), arguments.archive)
    if arguments.restore_missing_lockfile:
        if not all((archive, arguments.expected_vcs_commit, arguments.expected_archive_sha256)):
            parser.error("lockfile recovery requires archive, source commit and registry checksum")
        # The lockfile is written beside the confined manifest, never through a link.
        confined_file(source_root, manifest.parent.relative_to(source_root) / "Cargo.lock", must_exist=False)
        return restore_missing_lockfile(
            archive, manifest, changelog,
            arguments.expected_vcs_commit, arguments.expected_archive_sha256,
        )
    if archive is None:
        if arguments.expected_vcs_commit is not None:
            parser.error("--expected-vcs-commit requires --archive")
        return {"schema": SCHEMA, "version": 1, **verify_source(manifest, changelog)}
    if arguments.approval_repository is not None:
        record, summary, relative = read_approval_record(
            confined_root(arguments.approval_repository), str(load_package(manifest)["version"])
        )
        identity = verify_approved_archive(
            archive, manifest, changelog, arguments.expected_vcs_commit,
            record, summary, arguments.cargo_version,
        )
        identity["approval"]["record_path"] = relative
        return identity
    return verify_archive(archive, manifest, changelog, arguments.expected_vcs_commit)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", default=".",
                        help="release checkout; --manifest and --changelog are relative to it")
    parser.add_argument("--manifest", type=Path, default=Path(SOURCE_MANIFEST))
    parser.add_argument("--changelog", type=Path, default=Path(SOURCE_CHANGELOG))
    parser.add_argument("--archive-root", help="directory holding --archive")
    parser.add_argument("--archive", type=Path, help="crate file name relative to --archive-root")
    parser.add_argument("--expected-vcs-commit")
    parser.add_argument("--expected-archive-sha256")
    parser.add_argument("--restore-missing-lockfile", action="store_true")
    parser.add_argument("--check-paths", action="store_true",
                        help="only confine --manifest and --changelog to --source-root")
    parser.add_argument(
        "--approval-repository",
        help="maintained-tooling checkout; requires --archive to equal its tracked "
             "release/approved-packages/<version>.json",
    )
    parser.add_argument(
        "--cargo-version", help="with --approval-repository: the packaging Cargo release (X.Y.Z)",
    )
    parser.add_argument(
        "--release-notes", metavar="VERSION",
        help="print the exact current-version Markdown after manifest validation",
    )
    arguments = parser.parse_args()
    if arguments.release_notes is not None:
        _validate_notes_arguments(arguments, parser)
    _validate_mode_arguments(arguments, parser)

    try:
        if arguments.release_notes is not None:
            sys.stdout.write(source_release_notes(arguments.release_notes))
            return 0
        evidence = _run(arguments, parser)
    except ReleasePackageError as error:
        print(f"release package verification failed: {error}", file=sys.stderr)
        return 1

    print(json.dumps(evidence, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
