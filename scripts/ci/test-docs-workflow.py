#!/usr/bin/env python3
"""Structural contract tests for portable API documentation qualification."""

from __future__ import annotations

from pathlib import Path
import re
import unittest


ROOT = Path(__file__).resolve().parents[2]
DOCS_WORKFLOW = ROOT / ".github/workflows/docs.yml"
PAGES_CONDITION = (
    r"if: >-\n\s+github\.api_url == 'https://api\.github\.com' &&\n"
    r"\s+github\.ref == 'refs/heads/main'"
)


def job(workflow: str, name: str) -> str:
    match = re.search(
        rf"^  {re.escape(name)}:\n(?P<body>.*?)(?=^  [a-z][a-z0-9-]*:\n|\Z)",
        workflow,
        re.MULTILINE | re.DOTALL,
    )
    if match is None:
        raise AssertionError(f"workflow job {name!r} is missing")
    return match.group("body")


def step(workflow: str, name: str) -> str:
    match = re.search(
        rf"^      - name: {re.escape(name)}\n"
        rf"(?P<body>.*?)(?=^      - (?:name:|uses:)|\Z)",
        workflow,
        re.MULTILINE | re.DOTALL,
    )
    if match is None:
        raise AssertionError(f"workflow step {name!r} is missing")
    return match.group("body")


class DocsWorkflowContractTest(unittest.TestCase):
    def setUp(self) -> None:
        self.workflow = DOCS_WORKFLOW.read_text(encoding="utf-8")

    def test_docs_qualify_pull_requests_and_target_branch_pushes(self) -> None:
        triggers = self.workflow.split("\njobs:\n", maxsplit=1)[0]
        self.assertRegex(
            triggers,
            r"on:\n  pull_request:\n    branches: \[main\]\n"
            r"  push:\n    branches: \[main\]",
        )

    def test_build_and_analytics_validation_are_provider_neutral(self) -> None:
        build = step(self.workflow, "Build complete API documentation")
        prepare = step(self.workflow, "Prepare Pages artifact")

        self.assertIn("cargo doc --all-features --no-deps", build)
        self.assertNotIn("if:", build)
        self.assertIn("python3 scripts/check-docs-analytics.py target/doc", prepare)
        self.assertNotIn("if:", prepare)

    def test_pages_service_actions_only_run_for_github_main(self) -> None:
        configure = step(self.workflow, "Configure GitHub Pages")
        upload = step(self.workflow, "Upload Pages artifact")
        deploy = job(self.workflow, "deploy")

        self.assertRegex(configure, PAGES_CONDITION)
        self.assertIn("uses: actions/configure-pages@", configure)
        self.assertRegex(upload, PAGES_CONDITION)
        self.assertIn("uses: actions/upload-pages-artifact@", upload)
        self.assertRegex(deploy, PAGES_CONDITION)
        self.assertIn("uses: actions/deploy-pages@", deploy)
        self.assertNotIn("github.repository", self.workflow)


if __name__ == "__main__":
    unittest.main()
