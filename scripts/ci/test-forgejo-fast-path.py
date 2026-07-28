#!/usr/bin/env python3
"""Contract tests for local and GitHub Rust SDK qualification routing."""

from __future__ import annotations

import json
from pathlib import Path
import re
import unittest


ROOT = Path(__file__).resolve().parents[2]
CI_WORKFLOW = ROOT / ".github/workflows/ci.yml"
BOUNDARY_WORKFLOW = ROOT / ".github/workflows/public-boundary.yml"
CONTRACT = ROOT / "scripts/ci/forgejo-fast-path.json"


def job(workflow: str, name: str) -> str:
    match = re.search(
        rf"^  {re.escape(name)}:\n(?P<body>.*?)(?=^  [a-z][a-z0-9-]*:\n|\Z)",
        workflow,
        re.MULTILINE | re.DOTALL,
    )
    if match is None:
        raise AssertionError(f"workflow job {name!r} is missing")
    return match.group("body")


class ForgejoFastPathContractTest(unittest.TestCase):
    def setUp(self) -> None:
        self.workflow = CI_WORKFLOW.read_text(encoding="utf-8")
        self.boundary_workflow = BOUNDARY_WORKFLOW.read_text(encoding="utf-8")
        self.contract = json.loads(CONTRACT.read_text(encoding="utf-8"))

    def test_fast_path_has_one_sub_two_minute_machine_readable_budget(self) -> None:
        self.assertEqual(
            "durable-workflow.sdk-rust.forgejo-fast-path/v1",
            self.contract["schema"],
        )
        self.assertEqual("warm-local", self.contract["runner_profile"])
        self.assertGreater(self.contract["budget_seconds"], 0)
        self.assertLess(self.contract["budget_seconds"], 120)
        self.assertEqual(2, self.contract["job_timeout_minutes"])

        expected_checks = {
            "manifest": ["cargo", "metadata", "--no-deps", "--format-version", "1"],
            "rustfmt": ["cargo", "fmt", "--all", "--check"],
            "diff-boundary": ["git", "diff", "--check", "{candidate_range}"],
            "public-boundary": ["scripts/check-public-boundary.sh"],
            "compile": ["cargo", "check", "--all-targets"],
        }
        self.assertEqual(
            expected_checks,
            {check["id"]: check["command"] for check in self.contract["checks"]},
        )

    def test_forgejo_runs_only_the_bounded_structural_route(self) -> None:
        verify = job(self.workflow, "verify")
        qualification = job(self.workflow, "target-branch-qualification")
        boundary = job(self.boundary_workflow, "scan")

        self.assertIn("github.server_url == 'https://github.com'", verify)
        self.assertIn("github.server_url != 'https://github.com'", qualification)
        self.assertIn("timeout-minutes: 2", qualification)
        self.assertIn("toolchain: 1.86.0", qualification)
        self.assertIn("python3 scripts/ci/run-forgejo-fast-path.py", qualification)
        self.assertIn("persist-credentials: false", qualification)
        self.assertIn("github.server_url == 'https://github.com'", boundary)

    def test_github_keeps_the_complete_authoritative_matrix(self) -> None:
        verify = job(self.workflow, "verify")
        qualification = job(self.workflow, "target-branch-qualification")

        self.assertIn("rust: ['1.86.0', 'stable']", verify)
        self.assertIn("cargo test --all-targets", verify)
        self.assertIn(
            "cargo run --release --example avro_value_benchmark -- --enforce", verify
        )
        self.assertIn("cargo doc --all-features --no-deps", verify)
        self.assertIn("cargo package", verify)
        self.assertIn("Validate release tooling", verify)
        self.assertIn('test "$VERIFY_RESULT" = success', qualification)
        self.assertEqual(
            {
                "msrv-all-target-tests",
                "stable-all-target-tests",
                "typed-avro-compatibility",
                "typed-avro-regression-budget",
                "warning-free-rustdoc",
                "publishable-package-content",
                "release-tooling",
            },
            set(self.contract["github_authoritative_checks"]),
        )

    def test_untrusted_github_pull_requests_remain_read_only(self) -> None:
        self.assertIn("pull_request:", self.workflow)
        self.assertNotIn("pull_request_target:", self.workflow)
        self.assertRegex(self.workflow, r"permissions:\n  contents: read")


if __name__ == "__main__":
    unittest.main()
