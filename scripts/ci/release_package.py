#!/usr/bin/env python3
"""Verify source release notes and their packaged crate identity."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import hashlib
import json
import os
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


def approval_record_path(root: str, version: str) -> Path:
    """The reviewed record for `version`: <root>/<version>.json, strictly confined."""
    if not isinstance(version, str) or not STRICT_SEMVER.fullmatch(version):
        raise ReleasePackageError("approval version is not a strict SemVer version")
    return confined_file(root, f"{version}.json")


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


def load_approval(path: Path) -> tuple[dict[str, Any], str]:
    """Read one reviewed approved-package record and check its shape."""
    try:
        raw = path.read_bytes()
        approval = json.loads(raw)
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise ReleasePackageError(f"could not read approved package record: {error}") from error
    if not isinstance(approval, dict) or set(approval) != APPROVAL_FIELDS:
        raise ReleasePackageError("approved package record has unexpected fields")
    files = approval["files"]
    acceptance = approval["acceptance"]
    if (
        approval["schema"] != APPROVAL_SCHEMA
        or not isinstance(approval["package"], str)
        or not isinstance(approval["version"], str)
        or not STRICT_SEMVER.fullmatch(approval["version"])
        or not isinstance(approval["commit"], str)
        or not COMMIT_PATTERN.fullmatch(approval["commit"])
        or not isinstance(approval["crate_sha256"], str)
        or not SHA256_PATTERN.fullmatch(approval["crate_sha256"])
        or not isinstance(approval["cargo_version"], str)
        or not CARGO_VERSION_PATTERN.fullmatch(approval["cargo_version"])
        or not isinstance(files, list)
        or not files
        or not all(_safe_member_name(name) for name in files)
        or files != sorted(set(files))
        or approval["file_list_sha256"] != file_list_sha256(files)
        or not isinstance(acceptance, dict)
        or not isinstance(acceptance.get("summary_sha256"), str)
        or not SHA256_PATTERN.fullmatch(acceptance["summary_sha256"])
        or not isinstance(acceptance.get("server_index"), str)
        or not acceptance["server_index"]
    ):
        raise ReleasePackageError("approved package record is malformed")
    return approval, sha256(raw)


def _archive_files(archive_path: Path, root: str) -> list[str]:
    prefix = f"{root}/"
    try:
        with tarfile.open(archive_path, "r:gz") as archive:
            members = archive.getmembers()
    except (OSError, tarfile.TarError, EOFError) as error:
        raise ReleasePackageError(f"could not inspect crate archive: {error}") from error
    names = []
    for member in members:
        if not member.isfile() or not member.name.startswith(prefix):
            raise ReleasePackageError(f"crate member {member.name!r} is not a regular package file")
        names.append(member.name[len(prefix):])
    if len(names) != len(set(names)):
        raise ReleasePackageError("crate archive repeats a member")
    return sorted(names)


def verify_approved_archive(
    archive_path: Path,
    manifest: Path,
    changelog: Path,
    expected_vcs_commit: str,
    approval_path: Path,
    cargo_version: str | None = None,
) -> dict[str, Any]:
    """Require an archive to be exactly the approved pre-publication package."""
    identity = verify_archive(archive_path, manifest, changelog, expected_vcs_commit)
    approval, approval_sha256 = load_approval(approval_path)
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
    }
    return identity


def _validate_notes_arguments(
    arguments: argparse.Namespace, parser: argparse.ArgumentParser,
) -> None:
    if any((arguments.archive, arguments.expected_vcs_commit,
            arguments.expected_archive_sha256, arguments.restore_missing_lockfile,
            arguments.approval_root, arguments.archive_root, arguments.cargo_version,
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
        arguments.archive, arguments.archive_root, arguments.approval_root,
        arguments.expected_vcs_commit, arguments.restore_missing_lockfile, arguments.cargo_version,
    )):
        parser.error("--check-paths takes only --source-root, --manifest and --changelog")
    if (arguments.archive is None) != (arguments.archive_root is None):
        parser.error("--archive and --archive-root must be given together")
    if arguments.cargo_version is not None and arguments.approval_root is None:
        parser.error("--cargo-version requires --approval-root")
    if arguments.approval_root is not None and (
        arguments.archive is None or arguments.expected_vcs_commit is None
        or arguments.restore_missing_lockfile
    ):
        parser.error("--approval-root requires --archive and --expected-vcs-commit only")


def _run(arguments: argparse.Namespace, parser: argparse.ArgumentParser) -> dict[str, Any]:
    # Every file named on the command line is confined to an explicit root
    # before it is opened: the checkout (--source-root), the package directory
    # (--archive-root) or the reviewed approvals directory (--approval-root).
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
    if arguments.approval_root is not None:
        approval = approval_record_path(
            confined_root(arguments.approval_root), str(load_package(manifest)["version"])
        )
        return verify_approved_archive(
            archive, manifest, changelog, arguments.expected_vcs_commit,
            approval, arguments.cargo_version,
        )
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
        "--approval-root",
        help="reviewed approvals directory; requires --archive to equal <version>.json in it",
    )
    parser.add_argument(
        "--cargo-version", help="with --approval-root: the packaging Cargo release (X.Y.Z)",
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
