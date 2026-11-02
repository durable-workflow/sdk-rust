#!/usr/bin/env python3
"""Offline tests for the Cargo lock advisory check."""

from __future__ import annotations

import importlib.util
import sys
import unittest
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
REGISTRY = 'source = "registry+https://github.com/rust-lang/crates.io-index"'


def lock(*packages: tuple[str, str]) -> str:
    sections = ['[[package]]\nname = "durable-workflow"\nversion = "0.0.0"\n']
    for name, version in packages:
        sections.append(f'[[package]]\nname = "{name}"\nversion = "{version}"\n{REGISTRY}\n')
    return "\n".join(sections)


def advisories(records: dict) -> object:
    def lookup(packages):
        return {package: records[package] for package in packages if package in records}

    return lookup


# GHSA-qwgh-2vcv-g2f7 affects block-buffer >= 0.11.0, < 0.12.1.
AFFECTED = advisories({("block-buffer", "0.12.0"): ["GHSA-qwgh-2vcv-g2f7"]})


class AdvisoryCheckTest(unittest.TestCase):
    def test_reports_an_affected_resolution_in_any_lock(self) -> None:
        locks = {
            "Cargo.lock": lock(("sha2", "0.11.0"), ("block-buffer", "0.12.1")),
            "fixture/Cargo.lock": lock(("sha2", "0.11.0"), ("block-buffer", "0.12.0")),
        }
        self.assertEqual(
            ["fixture/Cargo.lock: block-buffer 0.12.0 is affected by GHSA-qwgh-2vcv-g2f7"],
            CHECKER.check(locks, AFFECTED),
        )

    def test_accepts_unaffected_resolutions(self) -> None:
        locks = {"Cargo.lock": lock(("sha2", "0.10.9"), ("block-buffer", "0.10.4"))}
        self.assertEqual([], CHECKER.check(locks, AFFECTED))

    def test_queries_only_registry_packages(self) -> None:
        queried = []

        def lookup(packages):
            queried.extend(packages)
            return {}

        CHECKER.check({"Cargo.lock": lock(("serde", "1.0.0"))}, lookup)
        self.assertEqual([("serde", "1.0.0")], queried)

    def test_rejects_a_lock_without_registry_packages(self) -> None:
        self.assertEqual(
            ["Cargo.lock: no registry packages found"],
            CHECKER.check({"Cargo.lock": lock()}, AFFECTED),
        )

    def test_checks_every_committed_lock(self) -> None:
        root = Path(__file__).resolve().parents[2]
        committed = {
            str(path.relative_to(root))
            for path in root.rglob("Cargo.lock")
            if "target" not in path.relative_to(root).parts
        }
        self.assertEqual(committed, set(CHECKER.LOCKS))


if __name__ == "__main__":
    unittest.main()
