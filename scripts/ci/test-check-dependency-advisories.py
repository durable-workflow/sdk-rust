#!/usr/bin/env python3
"""Offline tests for the Cargo lock advisory check."""

from __future__ import annotations

import importlib.util
import sys
import unittest
import urllib.error
from pathlib import Path
from types import ModuleType


def _load_checker() -> ModuleType:
    path = Path(__file__).with_name("check-dependency-advisories.py")
    spec = importlib.util.spec_from_file_location("check_dependency_advisories", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


CHECKER = _load_checker()
ADVISORY = "GHSA-qwgh-2vcv-g2f7"  # Affects block-buffer >= 0.11.0, < 0.12.1.


def lock(*packages: tuple[str, str], source: str = CHECKER.CRATES_IO) -> str:
    sections = ['[[package]]\nname = "durable-workflow"\nversion = "0.0.0"\n']
    for name, version in packages:
        sections.append(
            f'[[package]]\nname = "{name}"\nversion = "{version}"\nsource = "{source}"\n'
        )
    return "\n".join(sections)


def osv(affected: dict[tuple[str, str], list[str]]):
    """Answer like OSV's batch API: {} when clean, otherwise advisory IDs."""

    def post(body):
        results = []
        for query in body["queries"]:
            package = (query["package"]["name"], query["version"])
            assert query["package"]["ecosystem"] == "crates.io"
            ids = affected.get(package)
            results.append({"vulns": [{"id": i, "modified": "x"} for i in ids]} if ids else {})
        return {"results": results}

    return post


def respond(*results: object):
    return lambda body: {"results": list(results)}


CLEAN_LOCK = lock(("sha2", "0.10.9"))


class AdvisoryCheckTest(unittest.TestCase):
    def test_reports_an_affected_resolution_in_any_lock(self) -> None:
        locks = {
            "Cargo.lock": lock(("sha2", "0.11.0"), ("block-buffer", "0.12.1")),
            "fixture/Cargo.lock": lock(("sha2", "0.11.0"), ("block-buffer", "0.12.0")),
        }
        self.assertEqual(
            [f"fixture/Cargo.lock: block-buffer 0.12.0 is affected by {ADVISORY}"],
            CHECKER.check(locks, osv({("block-buffer", "0.12.0"): [ADVISORY]})),
        )

    def test_accepts_unaffected_resolutions(self) -> None:
        locks = {"Cargo.lock": lock(("sha2", "0.10.9"), ("block-buffer", "0.10.4"))}
        self.assertEqual([], CHECKER.check(locks, osv({("block-buffer", "0.12.0"): [ADVISORY]})))

    def test_queries_only_crates_io_packages(self) -> None:
        queried = []

        def post(body):
            queried.extend(query["package"]["name"] for query in body["queries"])
            return {"results": [{} for _ in body["queries"]]}

        CHECKER.check({"Cargo.lock": lock(("serde", "1.0.0"))}, post)
        self.assertEqual(["serde"], queried)

    def test_rejects_sources_osv_cannot_identify(self) -> None:
        for source in (
            "registry+https://example.com/private-index",
            "sparse+https://index.crates.io/",
            "sparse+https://example.com/private-index/",
            "git+https://github.com/example/serde?rev=0#0",
        ):
            with self.subTest(source=source):
                self.assertEqual(
                    [f"Cargo.lock: serde 1.0.0 uses unsupported source {source}"],
                    CHECKER.check({"Cargo.lock": lock(("serde", "1.0.0"), source=source)}, osv({})),
                )

    def test_rejects_a_lock_without_crates_io_packages(self) -> None:
        self.assertEqual(
            ["Cargo.lock: no crates.io packages found"],
            CHECKER.check({"Cargo.lock": lock()}, osv({})),
        )

    def test_fails_on_unclear_osv_results(self) -> None:
        for result in (
            {"error": {"code": 500, "message": "unavailable"}},
            {"vulns": {}},
            {"vulns": []},
            {"vulns": None},
            {"vulns": [{"id": ""}]},
            {"vulns": ["GHSA-x"]},
            {"vulns": [{"id": ADVISORY}], "next_page_token": "next"},
            {"next_page_token": "next"},
            [],
            None,
        ):
            with self.subTest(result=result):
                with self.assertRaises(CHECKER.OsvError):
                    CHECKER.check({"Cargo.lock": CLEAN_LOCK}, respond(result))

    def test_fails_on_unclear_osv_batches(self) -> None:
        for response in (
            {},
            {"results": {}},
            {"results": [{}, {}]},
            {"results": []},
            {"results": [{}], "error": {"code": 500, "message": "unavailable"}},
            {"results": [{}], "next_page_token": "next"},
            [],
            None,
        ):
            with self.subTest(response=response):
                with self.assertRaises(CHECKER.OsvError):
                    CHECKER.check({"Cargo.lock": CLEAN_LOCK}, lambda body: response)

    def test_propagates_http_failures(self) -> None:
        def post(body):
            raise urllib.error.URLError("unreachable")

        with self.assertRaises(urllib.error.URLError):
            CHECKER.check({"Cargo.lock": CLEAN_LOCK}, post)

    def test_checks_every_committed_lock(self) -> None:
        root = Path(__file__).resolve().parents[2]
        committed = {
            str(path.relative_to(root))
            for path in root.rglob("Cargo.lock")
            if "target" not in path.relative_to(root).parts
        }
        self.assertEqual(committed, set(CHECKER.LOCKS))
        for path in CHECKER.LOCKS:
            CHECKER.crates_io_packages((root / path).read_text())


if __name__ == "__main__":
    unittest.main()
