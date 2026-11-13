#!/usr/bin/env python3
"""Fail when a committed Cargo lock resolves a crate with a known advisory.

Every crates.io package in the root lock and the fixture locks is checked
against OSV, which includes GitHub-reviewed and RustSec advisories for crates.io.
Any other package source, and any OSV response that is not a clear answer,
fails the check instead of passing unchecked.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import sys
import tomllib
import urllib.request
from typing import Callable

ROOT = pathlib.Path(__file__).resolve().parents[2]
LOCKS = (
    "Cargo.lock",
    "scripts/acceptance/command-keys/consumer/Cargo.lock",
    "tests/fixtures/grouped-condition-worker-consumer/Cargo.lock",
    "tests/nexus/Cargo.lock",
)
# The only source OSV's "crates.io" ecosystem describes. Cargo records crates.io
# with this source in lock files for both the git and sparse index protocols.
CRATES_IO = "registry+https://github.com/rust-lang/crates.io-index"
OSV = "https://api.osv.dev/v1/querybatch"
BATCH = 500

Package = tuple[str, str]
Post = Callable[[dict], object]


class LockError(Exception):
    pass


class OsvError(Exception):
    pass


def crates_io_packages(text: str) -> list[Package]:
    """Return crates.io packages, refusing sources OSV cannot identify."""
    packages = []
    for package in tomllib.loads(text).get("package", []):
        source = package.get("source")
        if source is None:
            continue  # Local path or workspace package: this repository's source.
        if source != CRATES_IO:
            raise LockError(
                f"{package['name']} {package['version']} uses unsupported source {source}"
            )
        packages.append((package["name"], package["version"]))
    if not packages:
        raise LockError("no crates.io packages found")
    return packages


def osv_lookup(packages: list[Package], post: Post) -> dict[Package, list[str]]:
    """Return advisory IDs per affected package, failing on any unclear answer."""
    found: dict[Package, list[str]] = {}
    for start in range(0, len(packages), BATCH):
        chunk = packages[start : start + BATCH]
        response = post(
            {
                "queries": [
                    {"package": {"ecosystem": "crates.io", "name": name}, "version": version}
                    for name, version in chunk
                ]
            }
        )
        # A batch response has exactly one top-level field, "results".
        if not isinstance(response, dict) or set(response) != {"results"}:
            raise OsvError(f"unexpected batch response: {json.dumps(response)[:200]}")
        results = response["results"]
        if not isinstance(results, list) or len(results) != len(chunk):
            raise OsvError("unexpected batch response shape")
        for (name, version), result in zip(chunk, results):
            ids = advisory_ids(result, f"{name} {version}")
            if ids:
                found[(name, version)] = ids
    return found


def advisory_ids(result: object, package: str) -> list[str]:
    # A clean result is an empty object. Anything else must be a non-empty,
    # unpaginated list of advisories with string IDs.
    if not isinstance(result, dict) or set(result) - {"vulns"}:
        raise OsvError(f"unexpected result for {package}: {json.dumps(result)[:200]}")
    vulns = result.get("vulns", [])
    if not isinstance(vulns, list) or ("vulns" in result and not vulns):
        raise OsvError(f"malformed advisories for {package}")
    ids = [vuln.get("id") if isinstance(vuln, dict) else None for vuln in vulns]
    if not all(isinstance(vuln_id, str) and vuln_id for vuln_id in ids):
        raise OsvError(f"malformed advisory ID for {package}")
    return ids


def post_osv(body: dict) -> object:
    request = urllib.request.Request(
        OSV, json.dumps(body).encode(), {"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        return json.load(response)


def check(locks: dict[str, str], post: Post) -> list[str]:
    """Return one failure line per affected package or unusable lock."""
    failures = []
    for path, text in locks.items():
        try:
            affected = osv_lookup(crates_io_packages(text), post)
        except LockError as error:
            failures.append(f"{path}: {error}")
            continue
        for (name, version), ids in sorted(affected.items()):
            failures.append(f"{path}: {name} {version} is affected by {', '.join(ids)}")
    return failures


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("locks", nargs="*", default=list(LOCKS))
    arguments = parser.parse_args()

    locks = {path: (ROOT / path).read_text() for path in arguments.locks}
    # HTTP and OSV response errors propagate and fail the job.
    failures = check(locks, post_osv)
    for failure in failures:
        print(f"::error::{failure}")
    if failures:
        return 1
    print(f"No known advisories in {len(locks)} Cargo locks.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
