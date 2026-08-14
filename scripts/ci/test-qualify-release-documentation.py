#!/usr/bin/env python3
"""Focused tests for the Rust release-documentation qualifier."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
import tarfile
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
QUALIFIER_PATH = Path(__file__).with_name("qualify-release-documentation.py")
SPEC = importlib.util.spec_from_file_location(
    "qualify_release_documentation", QUALIFIER_PATH
)
assert SPEC is not None and SPEC.loader is not None
QUALIFIER = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = QUALIFIER
SPEC.loader.exec_module(QUALIFIER)


class ReleaseDocumentationQualificationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.package, self.contract = QUALIFIER.release_contract(ROOT / "Cargo.toml")
        self.readme = (ROOT / "README.md").read_text(encoding="utf-8")

    def test_current_source_uses_the_structured_general_first_contract(self) -> None:
        QUALIFIER.qualify_markup(self.readme, self.contract, "source README")

    def test_rejects_reordered_deployment_paths(self) -> None:
        swapped = (
            self.readme.replace(
                'data-documentation-path="general"',
                'data-documentation-path="temporary"',
                1,
            )
            .replace(
                'data-documentation-path="cloud"',
                'data-documentation-path="general"',
                1,
            )
            .replace(
                'data-documentation-path="temporary"',
                'data-documentation-path="cloud"',
                1,
            )
        )
        with self.assertRaisesRegex(QUALIFIER.QualificationError, "general-first"):
            QUALIFIER.qualify_markup(swapped, self.contract, "candidate")

    def test_rejects_an_unqualified_general_destination(self) -> None:
        missing = self.readme.replace(
            'data-docs-destination="local-self-hosted"',
            'data-docs-destination="unqualified"',
            1,
        )
        with self.assertRaisesRegex(QUALIFIER.QualificationError, "incomplete"):
            QUALIFIER.qualify_markup(missing, self.contract, "candidate")

    def test_rejects_a_missing_visible_cloud_access_label(self) -> None:
        missing = self.readme.replace(
            'data-access-label="limited-early-access"',
            'data-access-label="unspecified"',
            1,
        )
        with self.assertRaisesRegex(QUALIFIER.QualificationError, "visible"):
            QUALIFIER.qualify_markup(missing, self.contract, "candidate")

    def test_packaged_readme_must_match_the_qualified_source(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            archive = Path(directory) / "candidate.crate"
            member = (
                f"{self.package['name']}-{self.package['version']}/"
                f"{self.package['readme']}"
            )
            payload = Path(directory) / "README.md"
            payload.write_bytes(b"different package readme\n")
            with tarfile.open(archive, "w:gz") as crate:
                crate.add(payload, arcname=member)
            with self.assertRaisesRegex(QUALIFIER.QualificationError, "differs"):
                QUALIFIER.qualify_package(
                    archive,
                    self.readme.encode("utf-8"),
                    self.package,
                )


if __name__ == "__main__":
    unittest.main()
