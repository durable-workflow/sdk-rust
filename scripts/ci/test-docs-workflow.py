#!/usr/bin/env python3
"""Structural contract tests for portable API documentation qualification."""

from __future__ import annotations

from pathlib import Path
import re
import unittest


ROOT = Path(__file__).resolve().parents[2]
DOCS_WORKFLOW = ROOT / ".github/workflows/docs.yml"
PAGES_WORKFLOW = ROOT / ".github/workflows/pages.yml"
PAGES_CONDITION = (
    r"if: >-\n\s+github\.api_url == 'https://api\.github\.com' &&\n"
    r"\s+github\.event_name == 'push' &&\n"
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
        self.qualification = DOCS_WORKFLOW.read_text(encoding="utf-8")
        self.publication = PAGES_WORKFLOW.read_text(encoding="utf-8")

    def test_docs_qualify_pull_requests_and_target_branch_pushes(self) -> None:
        triggers = self.qualification.split("\njobs:\n", maxsplit=1)[0]
        self.assertRegex(
            triggers,
            r"on:\n  pull_request:\n    branches: \[main\]\n"
            r"  push:\n    branches: \[main\]\n"
            r"  workflow_dispatch:\n\npermissions:",
        )
        self.assertRegex(triggers, r"permissions:\n  contents: read")

    def test_pull_request_qualification_is_complete_and_unprivileged(self) -> None:
        qualify = job(self.qualification, "build")
        build = step(qualify, "Build complete API documentation")
        validate = step(qualify, "Prepare Pages artifact")

        self.assertIn("cargo doc --all-features --no-deps", build)
        self.assertNotIn("if:", build)
        self.assertIn("python3 scripts/check-docs-analytics.py target/doc", validate)
        self.assertNotIn("if:", validate)
        self.assertNotIn("permissions:", qualify)
        self.assertNotIn("environment:", qualify)
        self.assertNotIn("pages: write", self.qualification)
        self.assertNotIn("id-token: write", self.qualification)
        self.assertNotIn("actions/configure-pages@", self.qualification)
        self.assertNotIn("actions/upload-pages-artifact@", self.qualification)
        self.assertNotIn("actions/deploy-pages@", self.qualification)

    def test_visual_changes_use_the_source_bound_organization_capture_gate(self) -> None:
        visual = job(self.qualification, "visual-evidence")
        checkout = step(visual, "Check out candidate source")
        admission = step(visual, "Admit the structurally qualified candidate")
        classify = step(visual, "Classify candidate changes")
        capture = step(visual, "Capture candidate state matrix")
        validate = step(visual, "Validate source-bound candidate evidence")
        retain = step(visual, "Retain candidate visual evidence")

        self.assertIn("permissions:\n      contents: read", visual)
        self.assertIn("repository: durable-workflow/.github", visual)
        self.assertIn("github.api_url != 'https://api.github.com'", visual)
        self.assertIn("ref: ${{ github.sha }}", checkout)
        self.assertNotIn("if:", checkout)
        self.assertIn("working-directory: candidate", admission)
        self.assertIn("python3 scripts/ci/test-docs-workflow.py", admission)
        self.assertIn("--profile rust-sdk-reference", classify)
        self.assertIn("github.event.pull_request.base.sha", classify)
        self.assertIn("github.event.before", classify)
        self.assertIn("1440x900 800x900 390x844", capture)
        self.assertIn("capture initial", capture)
        self.assertIn("capture granted", capture)
        self.assertIn("capture denied", capture)
        self.assertIn("capture preferences-open", capture)
        self.assertIn("--source-revision", capture)
        self.assertIn("--expected-revision", validate)
        self.assertIn("if-no-files-found: error", retain)

    def test_complete_visual_capture_is_reserved_for_github_qualification(self) -> None:
        visual = job(self.qualification, "visual-evidence")
        github_steps = (
            "Check out visual evidence controller",
            "Classify candidate changes",
            "Install Rust 1.86",
            "Install pinned visual capture runtime",
            "Build candidate API reference",
            "Capture candidate state matrix",
            "Validate source-bound candidate evidence",
            "Retain candidate visual evidence",
        )

        for name in github_steps:
            with self.subTest(step=name):
                self.assertIn(
                    "github.api_url == 'https://api.github.com'",
                    step(visual, name),
                )

    def test_pages_publication_only_accepts_trusted_main_pushes(self) -> None:
        triggers = self.publication.split("\njobs:\n", maxsplit=1)[0]
        build = job(self.publication, "build")
        deploy = job(self.publication, "deploy")

        self.assertRegex(
            triggers,
            r"on:\n  push:\n    branches: \[main\]\n\npermissions:",
        )
        self.assertNotIn("pull_request:", triggers)
        self.assertNotIn("workflow_dispatch:", triggers)
        self.assertRegex(build, PAGES_CONDITION)
        self.assertRegex(build, r"permissions:\n\s+contents: read")
        self.assertNotIn("id-token: write", build)
        self.assertNotIn("pages: write", build)
        self.assertNotIn("environment:", build)
        self.assertRegex(deploy, PAGES_CONDITION)
        self.assertRegex(
            deploy,
            r"permissions:\n\s+id-token: write\n\s+pages: write",
        )
        self.assertRegex(
            deploy,
            r"environment:\n\s+name: github-pages\n"
            r"\s+url: \$\{\{ steps\.deployment\.outputs\.page_url \}\}",
        )

    def test_deployment_artifact_is_rebuilt_from_the_trusted_push(self) -> None:
        producer = job(self.publication, "build")
        consumer = job(self.publication, "deploy")
        checkout = step(producer, "Check out exact trusted source")
        build = step(producer, "Build complete API documentation")
        prepare = step(producer, "Prepare Pages artifact")
        upload = step(producer, "Upload Pages artifact")
        deploy = step(consumer, "Deploy GitHub Pages")

        self.assertIn("ref: ${{ github.sha }}", checkout)
        self.assertIn("persist-credentials: false", checkout)
        self.assertIn("cargo doc --all-features --no-deps", build)
        self.assertIn("python3 scripts/check-docs-analytics.py target/doc", prepare)
        self.assertIn("uses: actions/upload-pages-artifact@", upload)
        self.assertIn("uses: actions/deploy-pages@", deploy)
        self.assertIn("needs: build", consumer)
        self.assertNotIn("actions/download-artifact@", self.publication)
        self.assertNotIn("actions/deploy-pages@", producer)
        self.assertNotIn("actions/upload-pages-artifact@", consumer)


if __name__ == "__main__":
    unittest.main()
