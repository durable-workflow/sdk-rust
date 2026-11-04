#!/usr/bin/env python3
"""Resolve and unpack the approved public stable crate for API documentation."""

from __future__ import annotations

import argparse
import hashlib
import io
import json
from pathlib import Path, PurePosixPath
import re
import subprocess
import sys
import tarfile
import tomllib
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

PACKAGE = "durable-workflow"
REPOSITORY = "https://github.com/durable-workflow/sdk-rust.git"
VERSION = re.compile(r"(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)")
COMMIT = re.compile(r"[0-9a-f]{40}")
SHA256 = re.compile(r"[0-9a-f]{64}")
MAX_ARCHIVE = 32 * 1024 * 1024
MAX_UNPACKED = 128 * 1024 * 1024
MAX_FILES = 10000


class DocsReleaseError(ValueError):
    """Stable documentation cannot be bound to an approved published crate."""


def require(condition: bool, message: str) -> None:
    if not condition:
        raise DocsReleaseError(message)


def download(url: str, maximum: int) -> bytes:
    request = Request(url, headers={"User-Agent": "durable-workflow-stable-docs (+https://github.com/durable-workflow/sdk-rust)"})
    with urlopen(request, timeout=30) as response:
        final = urlsplit(response.geturl())
        require(final.scheme == "https" and final.hostname in {"crates.io", "static.crates.io"}, "unexpected registry download origin")
        body = response.read(maximum + 1)
    require(len(body) <= maximum, "registry response exceeds its byte limit")
    return body


def select_release(metadata: dict) -> tuple[str, str]:
    crate = metadata.get("crate", {})
    require(crate.get("name") == PACKAGE, "registry package identity differs")
    version = crate.get("max_stable_version", "")
    require(isinstance(version, str) and VERSION.fullmatch(version) is not None, "registry has no exact stable version")
    versions = [item for item in metadata.get("versions", []) if item.get("num") == version]
    require(len(versions) == 1, "stable registry version is absent or ambiguous")
    item = versions[0]
    require(item.get("yanked") is False, "stable registry version is yanked")
    checksum = item.get("checksum", "")
    require(isinstance(checksum, str) and SHA256.fullmatch(checksum) is not None, "registry checksum is malformed")
    return version, checksum


def approved_release(root: Path, version: str, checksum: str) -> dict:
    approval = json.loads((root / "release" / "approved-packages" / f"{version}.json").read_bytes())
    require(approval.get("schema") == "durable-workflow.rust-sdk.approved-package/v1", "approval schema differs")
    require(approval.get("package") == PACKAGE and approval.get("version") == version, "approval package/version differs")
    require(approval.get("crate_sha256") == checksum, "approval and public registry checksums differ")
    commit = approval.get("commit", "")
    require(isinstance(commit, str) and COMMIT.fullmatch(commit) is not None, "approval source commit is malformed")
    files = approval.get("files")
    require(isinstance(files, list) and 0 < len(files) <= MAX_FILES and all(isinstance(name, str) for name in files), "approval file inventory is malformed")
    require(files == sorted(set(files)), "approval file inventory must be unique and sorted")
    digest = hashlib.sha256("".join(f"{name}\n" for name in files).encode()).hexdigest()
    require(digest == approval.get("file_list_sha256"), "approval file inventory digest differs")
    return approval


def tag_commit(refs: str, version: str) -> str:
    entries = dict(line.split()[::-1] for line in refs.splitlines() if line.strip())
    ref = f"refs/tags/{version}"
    commit = entries.get(ref + "^{}", entries.get(ref, ""))
    require(COMMIT.fullmatch(commit) is not None, "published source tag is absent or malformed")
    return commit


def unpack_crate(raw: bytes, destination: Path, approval: dict, source_commit: str) -> None:
    require(len(raw) <= MAX_ARCHIVE, "crate archive exceeds its byte limit")
    require(hashlib.sha256(raw).hexdigest() == approval["crate_sha256"], "public crate checksum differs")
    require(source_commit == approval["commit"], "source tag and approval commit differ")
    prefix = f"{PACKAGE}-{approval['version']}"
    files: dict[str, bytes] = {}
    seen: set[str] = set()
    total = 0
    with tarfile.open(fileobj=io.BytesIO(raw), mode="r:gz") as archive:
        for count, member in enumerate(archive, 1):
            require(count <= MAX_FILES, "crate archive has too many entries")
            path = PurePosixPath(member.name)
            require(not path.is_absolute() and ".." not in path.parts and "\\" not in member.name, "unsafe crate archive path")
            require(member.name.rstrip("/") == path.as_posix(), "noncanonical crate archive path")
            require(path.parts and path.parts[0] == prefix and member.name not in seen, "unexpected or duplicate crate archive path")
            seen.add(member.name)
            require(member.isfile() or member.isdir(), "crate archive contains a link or special file")
            if member.isdir():
                continue
            name = path.relative_to(prefix).as_posix()
            require(name != ".", "crate archive root must be a directory")
            total += member.size
            require(0 <= member.size <= MAX_UNPACKED and total <= MAX_UNPACKED, "crate unpacked bytes exceed their limit")
            stream = archive.extractfile(member)
            require(stream is not None, "crate archive member is unreadable")
            files[name] = stream.read(member.size + 1)
            require(len(files[name]) == member.size, "crate archive member is truncated")
    require(sorted(files) == approval["files"], "public crate file inventory differs from approval")
    vcs = json.loads(files[".cargo_vcs_info.json"])
    require(vcs.get("git", {}).get("sha1") == source_commit and vcs.get("git", {}).get("dirty", False) is False and vcs.get("path_in_vcs") == "", "crate VCS identity differs from the clean published source")
    manifest = tomllib.loads(files["Cargo.toml"].decode())
    require(manifest.get("package", {}).get("name") == PACKAGE and manifest.get("package", {}).get("version") == approval["version"], "crate manifest identity differs")
    require("Cargo.lock" in files and "src/lib.rs" in files, "crate has no locked library source")
    # Verify everything before creating a buildable source tree. Never overlay main.
    destination.mkdir(parents=True, exist_ok=False)
    for name, contents in files.items():
        target = destination / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(contents)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[2])
    parser.add_argument("--destination", type=Path, required=True)
    parser.add_argument("--identity", type=Path, required=True)
    args = parser.parse_args()
    try:
        metadata = json.loads(download(f"https://crates.io/api/v1/crates/{PACKAGE}", 4 * 1024 * 1024))
        version, checksum = select_release(metadata)
        approval = approved_release(args.root, version, checksum)
        refs = subprocess.run(["git", "ls-remote", REPOSITORY, f"refs/tags/{version}", f"refs/tags/{version}^{{}}"], check=True, capture_output=True, text=True, timeout=30).stdout
        source = tag_commit(refs, version)
        raw = download(f"https://static.crates.io/crates/{PACKAGE}/{PACKAGE}-{version}.crate", MAX_ARCHIVE)
        unpack_crate(raw, args.destination, approval, source)
        tooling = subprocess.run(["git", "rev-parse", "HEAD"], cwd=args.root, check=True, capture_output=True, text=True).stdout.strip()
        identity = {"schema": "durable-workflow.rust-sdk.stable-docs/v1", "package": PACKAGE, "version": version, "crate_sha256": checksum, "source_revision": source, "tooling_revision": tooling}
        args.identity.write_text(json.dumps(identity, indent=2) + "\n")
        print(json.dumps(identity))
    except (OSError, ValueError, KeyError, TypeError, tarfile.TarError, subprocess.SubprocessError) as error:
        print(f"stable documentation source refused: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
