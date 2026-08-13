#!/usr/bin/env python3
"""Regression tests for Rust documentation landing qualification."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import textwrap
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

    def test_rejects_a_floating_cargo_requirement_in_visible_onboarding(self) -> None:
        floating_onboarding = self.html.replace(
            '<p class="dw-version">',
            '<p class="dw-version"><span>durable-workflow = "2.0.0-rc"</span>',
            1,
        )

        with self.assertRaisesRegex(
            QUALIFIER.QualificationError,
            "visible Cargo installation",
        ):
            QUALIFIER.validate_structure(
                QUALIFIER.parse_document(floating_onboarding),
                self.crate_version,
                self.rust_version,
            )

    def test_rejects_an_exact_prerelease_version_in_visible_onboarding(self) -> None:
        pinned_onboarding = self.html.replace(
            '<p class="dw-version">',
            f'<p class="dw-version"><span>Qualified crate {self.crate_version}</span>',
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

    def test_visible_cargo_paths_are_bound_to_the_public_authority(self) -> None:
        direct_paths = QUALIFIER.validate_visible_cargo_paths(
            QUALIFIER.parse_document(self.html),
            (ROOT / "README.md").read_text(encoding="utf-8"),
            "2.0.0-rc.7",
        )

        self.assertEqual(0, direct_paths)

    def test_rejects_a_readme_path_that_floats_past_the_authority(self) -> None:
        readme = (ROOT / "README.md").read_text(encoding="utf-8")
        readme += '\n```toml\ndurable-workflow = "2.0.0-rc"\n```\n'

        with self.assertRaisesRegex(
            QUALIFIER.QualificationError,
            "Cargo path resolves 2.0.0-rc.*qualifies =2.0.0-rc.7",
        ):
            QUALIFIER.validate_visible_cargo_paths(
                QUALIFIER.parse_document(self.html),
                readme,
                "2.0.0-rc.7",
            )

    def test_clean_installer_resolution_uses_the_authority_version(self) -> None:
        contract = {
            "schema": QUALIFIER.QUICKSTART_CONTRACT_SCHEMA,
            "artifacts": {"sdk-rust": {"version": "2.0.0-rc.7"}},
        }
        installer = textwrap.dedent(
            """\
            #!/bin/sh
            set -eu
            exec "$CARGO_BIN" add durable-workflow@=2.0.0-rc.7
            """
        )

        with tempfile.TemporaryDirectory() as directory:
            cargo = Path(directory) / "cargo"
            cargo.write_text(
                textwrap.dedent(
                    """\
                    #!/usr/bin/env python3
                    from pathlib import Path
                    import sys

                    requirement = sys.argv[2].split("@", 1)[1]
                    manifest = Path("Cargo.toml")
                    manifest.write_text(
                        manifest.read_text(encoding="utf-8")
                        + f'durable-workflow = "{requirement}"\\n',
                        encoding="utf-8",
                    )
                    Path("Cargo.lock").write_text(
                        'version = 4\\n\\n'
                        '[[package]]\\n'
                        'name = "durable-workflow"\\n'
                        f'version = "{requirement.removeprefix("=")}"\\n',
                        encoding="utf-8",
                    )
                    """
                ),
                encoding="utf-8",
            )
            cargo.chmod(0o755)

            version, direct_paths = QUALIFIER.qualify_cargo_resolution(
                QUALIFIER.parse_document(self.html),
                (ROOT / "README.md").read_text(encoding="utf-8"),
                installer,
                json.dumps(contract),
                str(cargo),
                5,
            )

        self.assertEqual("2.0.0-rc.7", version)
        self.assertEqual(0, direct_paths)


if __name__ == "__main__":
    unittest.main()
