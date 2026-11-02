#!/usr/bin/env python3
"""Fail when a committed Cargo lock resolves a crate with a known advisory.

Every registry package in the root lock and the fixture locks is checked
against OSV, which includes GitHub-reviewed and RustSec advisories for crates.io.
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
    "tests/fixtures/grouped-condition-worker-consumer/Cargo.lock",
    "tests/nexus/Cargo.lock",
)
OSV = "https://api.osv.dev/v1/querybatch"
BATCH = 500

Package = tuple[str, str]
Lookup = Callable[[list[Package]], dict[Package, list[str]]]


def registry_packages(text: str) -> list[Package]:
    return [
        (package["name"], package["version"])
        for package in tomllib.loads(text).get("package", [])
        if package.get("source", "").startswith("registry+")
    ]


def osv_lookup(packages: list[Package]) -> dict[Package, list[str]]:
    """Return advisory IDs for each package that has one."""
    found: dict[Package, list[str]] = {}
    for start in range(0, len(packages), BATCH):
        chunk = packages[start : start + BATCH]
        body = json.dumps(
            {
                "queries": [
                    {"package": {"ecosystem": "crates.io", "name": name}, "version": version}
                    for name, version in chunk
                ]
            }
        ).encode()
        request = urllib.request.Request(OSV, body, {"Content-Type": "application/json"})
        with urllib.request.urlopen(request, timeout=60) as response:
            results = json.load(response).get("results")
        if not isinstance(results, list) or len(results) != len(chunk):
            raise RuntimeError("OSV returned an unexpected batch response")
        for package, result in zip(chunk, results):
            ids = [vuln["id"] for vuln in result.get("vulns", [])]
            if ids or result.get("next_page_token"):
                found[package] = ids or ["(paginated OSV result)"]
    return found


def check(locks: dict[str, str], lookup: Lookup) -> list[str]:
    """Return one failure line per affected package in each lock."""
    failures = []
    for path, text in locks.items():
        packages = registry_packages(text)
        if not packages:
            failures.append(f"{path}: no registry packages found")
            continue
        for (name, version), ids in sorted(lookup(packages).items()):
            failures.append(f"{path}: {name} {version} is affected by {', '.join(ids)}")
    return failures


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("locks", nargs="*", default=list(LOCKS))
    arguments = parser.parse_args()

    locks = {path: (ROOT / path).read_text() for path in arguments.locks}
    failures = check(locks, osv_lookup)
    for failure in failures:
        print(f"::error::{failure}")
    if failures:
        return 1
    print(f"No known advisories in {len(locks)} Cargo locks.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
