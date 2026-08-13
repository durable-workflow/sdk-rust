#!/usr/bin/env python3
"""Regression tests for Rust documentation landing qualification."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]


def _load_qualifier():
    path = Path(__file__).with_name("qualify-docs-landing.py")
    spec = importlib.util.spec_from_file_location("qualify_docs_landing", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


QUALIFIER = _load_qualifier()


class DocsLandingQualificationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.html = (ROOT / "docs/index.html").read_text(encoding="utf-8")
        self.crate_version, self.rust_version = QUALIFIER.manifest_identity(
            ROOT / "Cargo.toml"
        )

    def test_current_landing_satisfies_the_general_first_contract(self) -> None:
        QUALIFIER.validate_structure(
            QUALIFIER.parse_document(self.html),
            self.crate_version,
            self.rust_version,
        )

    def test_rejects_a_stale_machine_owned_crate_identity(self) -> None:
        stale_identity = self.html.replace(
            f'data-crate-version="{self.crate_version}"',
            'data-crate-version="2.0.0-rc.1"',
            1,
        )

        with self.assertRaisesRegex(
            QUALIFIER.QualificationError,
            "landing crate identity is stale",
        ):
            QUALIFIER.validate_structure(
                QUALIFIER.parse_document(stale_identity),
                self.crate_version,
                self.rust_version,
            )

    def test_rejects_an_exact_prerelease_version_in_visible_onboarding(self) -> None:
        pinned_onboarding = self.html.replace(
            'durable-workflow = "2.0.0-rc"',
            f'durable-workflow = "={self.crate_version}"',
            1,
        )

        with self.assertRaisesRegex(
            QUALIFIER.QualificationError,
            "visible onboarding must not contain an exact prerelease version",
        ):
            QUALIFIER.validate_structure(
                QUALIFIER.parse_document(pinned_onboarding),
                self.crate_version,
                self.rust_version,
            )

    def test_rejects_a_missing_versionless_installer(self) -> None:
        pinned_installer = self.html.replace(
            QUALIFIER.VERSIONLESS_INSTALLER,
            f"cargo add durable-workflow@={self.crate_version}",
            1,
        )

        with self.assertRaisesRegex(
            QUALIFIER.QualificationError,
            "first task must use the versionless SDK installer",
        ):
            QUALIFIER.validate_structure(
                QUALIFIER.parse_document(pinned_installer),
                self.crate_version,
                self.rust_version,
            )

    def test_rejects_a_cloud_primary_action(self) -> None:
        cloud_primary = self.html.replace(
            'data-docs-priority="primary" data-access="general" href="durable_workflow/"',
            'data-docs-priority="primary" data-access="limited-early-access" '
            'href="https://durable-workflow.com/docs/2.0/polyglot/rust-cloud-quickstart/"',
            1,
        )

        with self.assertRaisesRegex(
            QUALIFIER.QualificationError,
            "retired Cloud quickstart|generally available",
        ):
            QUALIFIER.validate_structure(
                QUALIFIER.parse_document(cloud_primary),
                self.crate_version,
                self.rust_version,
            )

    def test_rejects_a_missing_generated_reference_target(self) -> None:
        root = QUALIFIER.parse_document(self.html)
        with tempfile.TemporaryDirectory() as directory:
            build = Path(directory)
            (build / "index.html").write_text(self.html, encoding="utf-8")

            with self.assertRaisesRegex(
                QUALIFIER.QualificationError,
                "durable_workflow/",
            ):
                QUALIFIER.qualify_local_links(root, build)

    def test_accepts_built_root_and_generated_reference_targets(self) -> None:
        root = QUALIFIER.parse_document(self.html)
        with tempfile.TemporaryDirectory() as directory:
            build = Path(directory)
            (build / "index.html").write_text(self.html, encoding="utf-8")
            reference = build / "durable_workflow/index.html"
            reference.parent.mkdir()
            reference.write_text("reference", encoding="utf-8")

            QUALIFIER.qualify_local_links(root, build)

    def test_first_party_http_failure_blocks_qualification(self) -> None:
        root = QUALIFIER.parse_document(self.html)

        def fail(destination: str, timeout: float) -> str:
            del timeout
            raise QUALIFIER.QualificationError(f"{destination} returned HTTP 404")

        with self.assertRaisesRegex(QUALIFIER.QualificationError, "HTTP 404"):
            QUALIFIER.qualify_http_links(
                root,
                "https://rust.durable-workflow.com/",
                attempts=1,
                delay=0,
                timeout=1,
                reader=fail,
            )

    def test_discovers_every_visible_first_party_destination(self) -> None:
        root = QUALIFIER.parse_document(self.html)

        self.assertEqual(
            (
                "https://cloud.durable-workflow.com/early-access",
                "https://durable-workflow.com/docs/2.0/polyglot/cloud-control-plane/",
                "https://durable-workflow.com/docs/2.0/polyglot/rust/",
                "https://rust.durable-workflow.com/",
                "https://rust.durable-workflow.com/durable_workflow/",
            ),
            QUALIFIER.first_party_http_links(
                QUALIFIER.landing_links(root),
                "https://rust.durable-workflow.com/",
            ),
        )


if __name__ == "__main__":
    unittest.main()
