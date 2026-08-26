#!/usr/bin/env python3
"""Contract tests for provider-neutral Rust SDK source qualification."""

from __future__ import annotations

import json
from pathlib import Path
import re
import unittest


ROOT = Path(__file__).resolve().parents[2]
CI_WORKFLOW = ROOT / ".github/workflows/ci.yml"
BOUNDARY_WORKFLOW = ROOT / ".github/workflows/public-boundary.yml"
CONTRACT = ROOT / "scripts/ci/bounded-qualification.json"
RELEASE_TOOLING_INSTALLER = ROOT / "scripts/ci/install-release-tooling.sh"


def job(workflow: str, name: str) -> str:
    match = re.search(
        rf"^  {re.escape(name)}:\n(?P<body>.*?)(?=^  [a-z][a-z0-9-]*:\n|\Z)",
        workflow,
        re.MULTILINE | re.DOTALL,
    )
    if match is None:
        raise AssertionError(f"workflow job {name!r} is missing")
    return match.group("body")


class SourceQualificationContractTest(unittest.TestCase):
    def setUp(self) -> None:
        self.workflow = CI_WORKFLOW.read_text(encoding="utf-8")
        self.boundary_workflow = BOUNDARY_WORKFLOW.read_text(encoding="utf-8")
        self.contract = json.loads(CONTRACT.read_text(encoding="utf-8"))
        self.release_tooling_installer = RELEASE_TOOLING_INSTALLER.read_text(
            encoding="utf-8"
        )

    def test_bounded_route_has_a_cold_compile_budget(self) -> None:
        self.assertEqual(
            "durable-workflow.sdk-rust.bounded-qualification/v1",
            self.contract["schema"],
        )
        self.assertEqual(150, self.contract["budget_seconds"])
        self.assertEqual(3, self.contract["job_timeout_minutes"])

        expected_checks = {
            "manifest": ["cargo", "metadata", "--no-deps", "--format-version", "1"],
            "rustfmt": ["cargo", "fmt", "--all", "--check"],
            "diff-boundary": ["git", "diff", "--check", "{candidate_range}"],
            "public-boundary": ["scripts/check-public-boundary.sh"],
            "official-avro-encoder": [
                "python3",
                "scripts/ci/verify-official-avro-encoder.py",
            ],
            "example-base-urls": [
                "python3",
                "scripts/ci/validate-example-base-urls.py",
            ],
            "release-documentation": [
                "python3",
                "scripts/ci/qualify-release-documentation.py",
            ],
            "release-notes": ["python3", "scripts/ci/release_package.py"],
            "compile": ["cargo", "check", "--all-targets"],
        }
        self.assertEqual(
            expected_checks,
            {check["id"]: check["command"] for check in self.contract["checks"]},
        )

    def test_control_plane_can_select_the_bounded_structural_route(self) -> None:
        verify = job(self.workflow, "verify")
        bounded = job(self.workflow, "bounded-qualification")
        qualification = job(self.workflow, "target-branch-qualification")
        boundary = job(self.boundary_workflow, "scan")

        self.assertIn(
            "vars.SOURCE_QUALIFICATION_MODE == '' || "
            "vars.SOURCE_QUALIFICATION_MODE == 'complete'",
            verify,
        )
        self.assertIn("vars.SOURCE_QUALIFICATION_MODE == 'bounded'", bounded)
        self.assertIn("timeout-minutes: 3", bounded)
        self.assertIn("toolchain: 1.86.0", bounded)
        self.assertIn(
            "python3 scripts/ci/run-bounded-qualification.py", bounded
        )
        self.assertIn("persist-credentials: false", bounded)
        self.assertIn("QUALIFICATION_MODE:", qualification)
        self.assertIn("BOUNDED_RESULT:", qualification)
        self.assertIn('case "${QUALIFICATION_MODE:-complete}" in', qualification)
        self.assertIn('test "$BOUNDED_RESULT" = success', qualification)
        self.assertIn("unsupported source qualification mode", qualification)
        action_policy = job(self.workflow, "action-policy")
        self.assertIn("github.server_url == 'https://github.com'", action_policy)
        self.assertEqual(
            2,
            self.workflow.count("github.server_url == 'https://github.com'"),
        )
        self.assertNotIn("server_url", verify)
        self.assertNotIn("server_url", bounded)
        self.assertEqual(1, qualification.count("server_url"))
        self.assertNotIn("server_url", boundary)

    def test_default_route_keeps_the_complete_matrix(self) -> None:
        verify = job(self.workflow, "verify")
        qualification = job(self.workflow, "target-branch-qualification")

        self.assertIn("rust: ['1.86.0', 'stable']", verify)
        self.assertIn("cargo test --all-targets", verify)
        self.assertIn(
            "scripts/ci/qualify-cross-language-avro-consumers.sh", verify
        )
        self.assertIn("if: matrix.rust == 'stable'", verify)
        self.assertIn(
            "cargo run --release --example avro_value_benchmark -- --enforce", verify
        )
        self.assertIn("cargo doc --all-features --no-deps", verify)
        self.assertIn("cargo doc --all-features --no-deps --examples", verify)
        self.assertIn(
            "python3 scripts/ci/validate-example-base-urls.py --rendered-docs target/doc",
            verify,
        )
        self.assertIn("cargo package", verify)
        self.assertIn("scripts/ci/release_package.py", verify)
        self.assertIn("--check-package", verify)
        self.assertIn("--rustdoc target/doc/durable_workflow/index.html", verify)
        self.assertIn("python3 scripts/ci/verify-fresh-consumer.py package", verify)
        self.assertIn("if: matrix.rust == '1.86.0'", verify)
        self.assertIn("Validate release tooling", verify)
        self.assertIn('test "$COMPLETE_RESULT" = success', qualification)
        self.assertEqual(
            {
                "msrv-all-target-tests",
                "stable-all-target-tests",
                "typed-avro-compatibility",
                "official-avro-encoder-authority",
                "official-php-python-avro-consumers",
                "typed-avro-regression-budget",
                "shipped-example-base-url-contract",
                "warning-free-rustdoc",
                "publishable-package-content",
                "release-documentation-surfaces",
                "fresh-msrv-consumer",
                "release-tooling",
            },
            set(self.contract["complete_checks"]),
        )

    def test_release_parser_is_installed_for_both_qualification_routes(self) -> None:
        installer = self.release_tooling_installer
        venv_creation = installer.index('python3 -m venv "$release_tooling_venv"')
        package_install = installer.index(
            '"$release_tooling_venv/bin/python" -m pip install'
        )
        path_export = installer.index('>> "$GITHUB_PATH"')
        self.assertLess(
            venv_creation,
            package_install,
        )
        self.assertLess(package_install, path_export)
        self.assertIn("--require-hashes --only-binary=:all:", installer)
        self.assertIn(
            '--requirement "$script_dir/release-tooling-requirements.txt"',
            installer,
        )
        self.assertNotIn("--break-system-packages", installer)
        self.assertNotIn("--user", installer)

        for route in ("verify", "bounded-qualification"):
            candidate = job(self.workflow, route)
            with self.subTest(route=route):
                self.assertIn("Install hash-locked release tooling", candidate)
                self.assertIn(
                    'scripts/ci/install-release-tooling.sh "${RUNNER_TEMP}/release-tooling"',
                    candidate,
                )
                self.assertLess(
                    candidate.index("Install hash-locked release tooling"),
                    candidate.index("scripts/ci/release_package.py")
                    if route == "verify"
                    else candidate.index("scripts/ci/run-bounded-qualification.py"),
                )

    def test_untrusted_pull_requests_remain_read_only_with_pinned_actions(self) -> None:
        self.assertIn("pull_request:", self.workflow)
        self.assertNotIn("pull_request_target:", self.workflow)
        self.assertRegex(self.workflow, r"permissions:\n  contents: read")
        self.assertRegex(self.boundary_workflow, r"permissions:\n  contents: read")
        action_references = re.findall(
            r"uses:\s+[^@\s]+@([^\s#]+)",
            self.workflow + self.boundary_workflow,
        )
        self.assertTrue(action_references)
        self.assertTrue(
            all(re.fullmatch(r"[0-9a-f]{40}", ref) for ref in action_references)
        )


if __name__ == "__main__":
    unittest.main()
