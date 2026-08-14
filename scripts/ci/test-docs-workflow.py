"""Structural contract tests for portable API documentation qualification."""

from __future__ import annotations

import json
import re
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DOCS_WORKFLOW = ROOT / ".github/workflows/docs.yml"
PAGES_WORKFLOW = ROOT / ".github/workflows/pages.yml"
DOCS_LANDING = ROOT / "docs/index.html"
NAVIGATION_EVIDENCE_VALIDATOR = (
    ROOT / "scripts/ci/validate-rustdoc-navigation-evidence.py"
)
NAVIGATION_EVIDENCE_BINDER_TEST = (
    ROOT / "scripts/ci/test-rustdoc-navigation-evidence.mjs"
)
VISUAL_CONTROLLER_REVISION = "0421c2e3a78ba4ca2adfe118e57db88d2264a62b"
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

    def test_root_is_a_general_first_landing(self) -> None:
        landing = DOCS_LANDING.read_text(encoding="utf-8")

        self.assertNotIn('<meta http-equiv="refresh"', landing)
        self.assertIn('data-landing-contract="general-rust-first"', landing)
        self.assertIn('data-docs-priority="primary"', landing)
        self.assertNotIn("rust-cloud-quickstart", landing)

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
        build_job = job(self.qualification, "build")
        build = step(build_job, "Build complete API documentation")
        prepare = step(build_job, "Prepare Pages artifact")

        self.assertIn("cargo doc --all-features --no-deps", build)
        self.assertNotIn("if:", build)
        self.assertIn("CLOUDFLARE_WEB_ANALYTICS_TOKEN", build)
        self.assertIn("cp docs/analytics/analytics.js", prepare)
        self.assertIn("cp docs/navigation.js", prepare)
        self.assertNotIn("analytics.css", prepare)
        self.assertIn("python3 scripts/check-docs-analytics.py target/doc", prepare)
        self.assertIn("node scripts/ci/test-docs-analytics.mjs", prepare)
        self.assertIn("node scripts/ci/test-docs-navigation.mjs", prepare)
        self.assertIn("python3 scripts/ci/qualify-docs-landing.py", prepare)
        self.assertIn("--build-directory target/doc --check-external", prepare)
        self.assertNotIn("--check-cargo-resolution", prepare)
        self.assertNotIn("if:", prepare)
        self.assertNotIn("permissions:", build_job)
        self.assertNotIn("environment:", build_job)
        self.assertNotIn("pages: write", self.qualification)
        self.assertNotIn("id-token: write", self.qualification)
        self.assertNotIn("environment:", self.qualification)
        self.assertNotIn("actions/configure-pages@", self.qualification)
        self.assertNotIn("actions/upload-pages-artifact@", self.qualification)
        self.assertNotIn("actions/deploy-pages@", self.qualification)

    def test_visual_changes_use_the_source_bound_controller(self) -> None:
        visual = job(self.qualification, "visual-evidence")
        checkout = step(visual, "Check out candidate source")
        controller_checkout = step(visual, "Check out visual evidence controller")
        admission = step(visual, "Admit the structurally qualified candidate")
        classify = step(visual, "Classify candidate changes")
        capture = step(visual, "Capture candidate state matrix")
        validate = step(visual, "Validate source-bound candidate evidence")
        retain = step(visual, "Retain candidate visual evidence")

        self.assertIn("permissions:\n      contents: read", visual)
        self.assertIn("ref: ${{ github.sha }}", checkout)
        self.assertNotIn("if:", checkout)
        self.assertIn("repository: durable-workflow/.github", controller_checkout)
        self.assertIn(f"ref: {VISUAL_CONTROLLER_REVISION}", controller_checkout)
        self.assertNotIn("ref: main", controller_checkout)
        self.assertIn("github.api_url != 'https://api.github.com'", visual)
        self.assertIn("working-directory: candidate", admission)
        self.assertIn("python3 scripts/ci/test-docs-workflow.py", admission)
        self.assertIn("--profile rust-sdk-reference", classify)
        self.assertIn("github.event.pull_request.base.sha", classify)
        self.assertIn("github.event.before", classify)
        self.assertRegex(
            classify,
            r'if ! git -C candidate diff --quiet \\\n'
            r'\s+"\$SOURCE_BASE_SHA\.\.\.HEAD" -- \.github/workflows/docs\.yml; then\n'
            r"\s+classification_args\+=\(--changed-file docs/analytics/analytics\.js\)",
        )
        self.assertIn("1440x900 800x900 390x844 640x360", capture)
        self.assertEqual(1, capture.count("capture analytics-ui-removed"))
        self.assertEqual(2, capture.count("capture navigation-open"))
        self.assertEqual(
            2, capture.count("--state-scope responsive --click '.sidebar-menu-toggle'")
        )
        self.assertIn(
            "candidate/scripts/ci/record-rustdoc-navigation-isolation.mjs",
            capture,
        )
        self.assertIn("--manifest visual-review/manifest.json", capture)
        self.assertIn('if [ "$state" = navigation-open ]; then', capture)
        self.assertIn("capture_args+=(--full-page)", capture)
        self.assertIn('if [ "$state" = analytics-ui-removed ]', capture)
        self.assertIn("--url http://127.0.0.1:4173/durable_workflow/", capture)
        self.assertNotRegex(
            capture, r"data-consent|analytics-preferences|googletagmanager"
        )
        self.assertIn("--source-revision", capture)
        self.assertIn("--expected-revision", validate)
        self.assertRegex(
            validate,
            r'if ! git -C candidate diff --quiet \\\n'
            r'\s+"\$SOURCE_BASE_SHA\.\.\.HEAD" -- \.github/workflows/docs\.yml; then\n'
            r"\s+validation_args\+=\(--changed-file docs/analytics/analytics\.js\)",
        )
        self.assertIn(
            "candidate/scripts/ci/validate-rustdoc-navigation-evidence.py",
            validate,
        )
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

    def test_visual_capture_loads_the_generated_reference_route(self) -> None:
        visual = job(self.qualification, "visual-evidence")
        build = step(visual, "Build candidate API reference")
        capture = step(visual, "Capture candidate state matrix")

        self.assertIn("cp docs/index.html target/doc/index.html", build)
        self.assertIn("python3 scripts/ci/qualify-docs-landing.py", build)
        self.assertIn("--build-directory target/doc --check-external", build)
        self.assertEqual(
            ["candidate/target/doc"],
            re.findall(
                r"(?m)^\s*python3 -m http\.server\b[^\n]*"
                r"--directory\s+([^\s\\]+)",
                capture,
            ),
        )
        self.assertEqual(
            [
                "http://127.0.0.1:4173/",
                "http://127.0.0.1:4173/durable_workflow/",
                "http://127.0.0.1:4173/durable_workflow/",
            ],
            re.findall(r'http://127\.0\.0\.1:4173/[^\s"\\]*', capture),
        )

    def test_pages_publication_only_accepts_qualified_trusted_main_pushes(self) -> None:
        triggers = self.publication.split("\njobs:\n", maxsplit=1)[0]
        build = job(self.publication, "build")
        visual = job(self.publication, "visual-evidence")
        deploy = job(self.publication, "deploy")
        verify = job(self.publication, "verify-deployment")

        self.assertRegex(
            triggers,
            r"on:\n  push:\n    branches: \[main\]\n\npermissions:",
        )
        self.assertNotIn("pull_request:", triggers)
        self.assertNotIn("workflow_dispatch:", triggers)
        self.assertRegex(build, PAGES_CONDITION)
        self.assertRegex(visual, PAGES_CONDITION)
        self.assertNotIn("id-token: write", build)
        self.assertNotIn("pages: write", build)
        self.assertNotIn("environment:", build)
        self.assertNotIn("id-token: write", visual)
        self.assertNotIn("pages: write", visual)
        self.assertIn("needs: [build, visual-evidence]", deploy)
        self.assertRegex(deploy, PAGES_CONDITION)
        self.assertIn("needs: [deploy]", verify)
        self.assertRegex(verify, PAGES_CONDITION)
        self.assertRegex(
            deploy,
            r"permissions:\n\s+id-token: write\n\s+pages: write",
        )
        self.assertRegex(
            deploy,
            r"environment:\n\s+name: github-pages\n"
            r"\s+url: \$\{\{ steps\.deployment\.outputs\.page_url \}\}",
        )

    def test_publication_independently_qualifies_the_trusted_source(self) -> None:
        visual = job(self.publication, "visual-evidence")
        checkout = step(visual, "Check out exact trusted source")
        controller_checkout = step(visual, "Check out visual evidence controller")
        build = step(visual, "Build trusted API reference")
        capture = step(visual, "Capture trusted state matrix")
        validate = step(visual, "Validate trusted source-bound evidence")
        retain = step(visual, "Retain trusted visual evidence")

        self.assertIn("ref: ${{ github.sha }}", checkout)
        self.assertIn("persist-credentials: false", checkout)
        self.assertIn("repository: ${{ github.repository }}", checkout)
        self.assertIn("repository: durable-workflow/.github", controller_checkout)
        self.assertIn(f"ref: {VISUAL_CONTROLLER_REVISION}", controller_checkout)
        self.assertNotIn("ref: main", controller_checkout)
        self.assertIn("CLOUDFLARE_WEB_ANALYTICS_TOKEN", build)
        self.assertIn("cargo doc --all-features --no-deps", build)
        self.assertIn("cp docs/index.html target/doc/index.html", build)
        self.assertIn("cp docs/navigation.js target/doc/navigation.js", build)
        self.assertIn("node scripts/ci/test-docs-analytics.mjs", build)
        self.assertIn("node scripts/ci/test-docs-navigation.mjs", build)
        self.assertIn("1440x900 800x900 390x844 640x360", capture)
        self.assertEqual(1, capture.count("capture analytics-ui-removed"))
        self.assertEqual(2, capture.count("capture navigation-open"))
        self.assertEqual(
            2, capture.count("--state-scope responsive --click '.sidebar-menu-toggle'")
        )
        self.assertIn(
            "candidate/scripts/ci/record-rustdoc-navigation-isolation.mjs",
            capture,
        )
        self.assertIn("--manifest visual-review/manifest.json", capture)
        self.assertIn('if [ "$state" = navigation-open ]; then', capture)
        self.assertIn("capture_args+=(--full-page)", capture)
        self.assertIn('if [ "$state" = analytics-ui-removed ]', capture)
        self.assertIn("--url http://127.0.0.1:4173/durable_workflow/", capture)
        self.assertIn("--source-revision", capture)
        self.assertIn("--expected-revision", validate)
        self.assertIn("--changed-file docs/analytics/analytics.js", validate)
        self.assertIn(
            "candidate/scripts/ci/validate-rustdoc-navigation-evidence.py",
            validate,
        )
        self.assertIn("if-no-files-found: error", retain)

    def test_publication_verifies_the_deployed_reference_after_deploy(self) -> None:
        verify = job(self.publication, "verify-deployment")
        checkout = step(verify, "Check out exact trusted source")
        controller_checkout = step(verify, "Check out visual evidence controller")
        qualify = step(verify, "Verify deployed landing and first-party destinations")
        capture = step(verify, "Capture deployed reference matrix")
        validate = step(verify, "Validate deployed reference reports")
        retain = step(verify, "Retain deployed reference evidence")

        self.assertIn("needs: [deploy]", verify)
        self.assertIn("ref: ${{ github.sha }}", checkout)
        self.assertIn("persist-credentials: false", checkout)
        self.assertIn("repository: durable-workflow/.github", controller_checkout)
        self.assertIn(f"ref: {VISUAL_CONTROLLER_REVISION}", controller_checkout)
        self.assertIn(
            "--landing-url https://rust.durable-workflow.com/", qualify
        )
        self.assertIn("--attempts 30", qualify)
        self.assertIn("--link-attempts 3", qualify)
        self.assertIn(
            "--url https://rust.durable-workflow.com/durable_workflow/", capture
        )
        self.assertIn("1440x900 800x900 390x844 640x360", capture)
        self.assertEqual(1, capture.count("capture analytics-ui-removed"))
        self.assertEqual(2, capture.count("capture navigation-open"))
        self.assertEqual(
            2, capture.count("--state-scope responsive --click '.sidebar-menu-toggle'")
        )
        self.assertIn(
            "scripts/ci/record-rustdoc-navigation-isolation.mjs",
            capture,
        )
        self.assertIn("--manifest deployed-visual-review/manifest.json", capture)
        self.assertIn('if [ "$state" = navigation-open ]; then', capture)
        self.assertIn('if [ "$state" = analytics-ui-removed ]', capture)
        self.assertIn("validate-rustdoc-navigation-evidence.py", validate)
        self.assertIn("if-no-files-found: error", retain)

    def test_navigation_evidence_rejects_a_noop_open_capture(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            captures = []
            matrix = (
                ("analytics-ui-removed", 1440, 900, False),
                ("analytics-ui-removed", 800, 900, False),
                ("analytics-ui-removed", 390, 844, False),
                ("analytics-ui-removed", 640, 360, True),
                ("navigation-open", 390, 844, False),
                ("navigation-open", 640, 360, False),
            )
            open_reports = []
            for state, width, height, full_page in matrix:
                stem = f"{state}-{width}x{height}"
                report_name = f"{stem}.json"
                screenshot_name = f"{stem}.png"
                interactions = (
                    [{"type": "click", "selector": ".sidebar-menu-toggle"}]
                    if state == "navigation-open"
                    else []
                )
                capture = {
                    "surface": "rust-sdk-reference",
                    "state": state,
                    "viewport": {"width": width, "height": height},
                    "full_page": full_page,
                    "interactions": interactions,
                    "report": report_name,
                    "screenshot": screenshot_name,
                }
                if state == "navigation-open":
                    capture["state_scope"] = "responsive"
                captures.append(capture)

                overlay = {
                    "tag": "nav",
                    "id": "dw-rustdoc-navigation",
                    "position": "fixed",
                    "intentional_overlay": True,
                    "isolated_background_count": 1,
                    "overlaps": [{"tag": "main"}],
                }
                report = {
                    "title": "durable_workflow - Rust",
                    "page_status": 200,
                    "geometry": {
                        "horizontal_overflow": False,
                        "clipped_text": [],
                        "clipped_control_text": [],
                        "unreachable_controls": [],
                        "overlapping_floating_elements": [],
                        "displaced_primary_content": [],
                        "orphaned_body_controls": [],
                        "intentional_overlays": (
                            [overlay] if state == "navigation-open" else []
                        ),
                    },
                    "console_errors": [],
                    "console_warnings": [],
                    "page_errors": [],
                    "request_failures": [],
                    "http_errors": [],
                }
                report_path = root / report_name
                report_path.write_text(json.dumps(report), encoding="utf-8")
                (root / screenshot_name).write_bytes(stem.encode("utf-8"))
                if state == "navigation-open":
                    open_reports.append(report_path)

            manifest = {
                "schema": "durable-workflow.pipeline.visual-review/v1",
                "captures": captures,
            }
            manifest_path = root / "manifest.json"
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
            valid = subprocess.run(
                [sys.executable, str(NAVIGATION_EVIDENCE_VALIDATOR), str(manifest_path)],
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertEqual(0, valid.returncode, valid.stderr)

            for report_path in open_reports:
                report = json.loads(report_path.read_text(encoding="utf-8"))
                report["geometry"]["intentional_overlays"] = []
                report_path.write_text(json.dumps(report), encoding="utf-8")
            noop = subprocess.run(
                [sys.executable, str(NAVIGATION_EVIDENCE_VALIDATOR), str(manifest_path)],
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(0, noop.returncode)
            self.assertEqual(
                2,
                noop.stderr.count(
                    "did not prove an isolated dw-rustdoc-navigation overlay"
                ),
            )

    def test_navigation_evidence_binds_legacy_controller_metadata(self) -> None:
        result = subprocess.run(
            ["node", str(NAVIGATION_EVIDENCE_BINDER_TEST)],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(0, result.returncode, result.stderr)

    def test_deployment_artifact_is_rebuilt_from_the_trusted_push(self) -> None:
        producer = job(self.publication, "build")
        consumer = job(self.publication, "deploy")
        checkout = step(producer, "Check out exact trusted source")
        build = step(producer, "Build complete API documentation")
        prepare = step(producer, "Prepare Pages artifact")
        upload = step(producer, "Upload qualified Pages artifact")
        deploy = step(consumer, "Deploy qualified GitHub Pages artifact")

        self.assertIn("ref: ${{ github.sha }}", checkout)
        self.assertIn("persist-credentials: false", checkout)
        self.assertIn("cargo doc --all-features --no-deps", build)
        self.assertIn("CLOUDFLARE_WEB_ANALYTICS_TOKEN", build)
        self.assertIn("python3 scripts/check-docs-analytics.py target/doc", prepare)
        self.assertIn("node scripts/ci/test-docs-analytics.mjs", prepare)
        self.assertIn("node scripts/ci/test-docs-navigation.mjs", prepare)
        self.assertIn("cp docs/navigation.js target/doc/navigation.js", prepare)
        self.assertIn("python3 scripts/ci/qualify-docs-landing.py", prepare)
        self.assertIn("--build-directory target/doc --check-external", prepare)
        self.assertIn("--check-cargo-resolution", prepare)
        self.assertNotIn("analytics.css", prepare)
        self.assertIn("uses: actions/upload-pages-artifact@", upload)
        self.assertIn("uses: actions/deploy-pages@", deploy)
        self.assertIn("needs: [build, visual-evidence]", consumer)
        self.assertNotIn("actions/download-artifact@", self.publication)
        self.assertNotIn("actions/deploy-pages@", producer)
        self.assertNotIn("actions/upload-pages-artifact@", consumer)


if __name__ == "__main__":
    unittest.main()
