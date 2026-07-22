#!/usr/bin/env python3
"""Executable coverage for immutable release tag publication."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).with_name("publish-planned-tag.py")
RECOVERY_SCRIPT = Path(__file__).with_name("component-release-recovery.py")
REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
PLAN_TAG = "release-plan/continuity-test"
RELEASE_TAG = "0.1.16"


def git(*arguments: str, cwd: Path | None = None, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *arguments],
        cwd=cwd,
        check=check,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


class PlannedTagPublicationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="planned-tag-test-")
        self.root = Path(self.temporary.name)
        self.source = self.root / "source"
        self.remote = self.root / "remote.git"
        git("init", "--quiet", "--initial-branch=main", str(self.source))
        git("init", "--quiet", "--bare", str(self.remote))
        git("config", "user.name", "Release Test", cwd=self.source)
        git("config", "user.email", "release-test@example.invalid", cwd=self.source)
        self.first = self.commit("first", RELEASE_TAG)
        self.second = self.commit("second", RELEASE_TAG)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def commit(self, value: str, version: str) -> str:
        (self.source / "value.txt").write_text(f"{value}\n", encoding="utf-8")
        (self.source / "Cargo.toml").write_text(
            "[package]\n"
            'name = "durable-workflow"\n'
            f'version = "{version}"\n',
            encoding="utf-8",
        )
        git("add", "Cargo.toml", "value.txt", cwd=self.source)
        git("commit", "--quiet", "-m", value, cwd=self.source)
        return git("rev-parse", "HEAD", cwd=self.source).stdout.strip()

    def publish(self, commit: str, evidence_name: str = "evidence.json") -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                sys.executable,
                str(SCRIPT),
                "--remote",
                str(self.remote),
                "--tag",
                RELEASE_TAG,
                "--commit",
                commit,
                "--plan-tag",
                PLAN_TAG,
                "--evidence",
                str(self.root / evidence_name),
            ],
            cwd=self.source,
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

    def evidence(self, name: str = "evidence.json") -> dict[str, object]:
        return json.loads((self.root / name).read_text(encoding="utf-8"))

    def remote_tag(self) -> str:
        return git("--git-dir", str(self.remote), "rev-parse", f"refs/tags/{RELEASE_TAG}").stdout.strip()

    def test_creates_exact_tag_and_identical_rerun_is_idempotent(self) -> None:
        created = self.publish(self.first)
        self.assertEqual(created.returncode, 0, created.stderr)
        self.assertEqual(self.remote_tag(), self.first)
        self.assertEqual(self.evidence()["action"], "created")

        verified = self.publish(self.first, "rerun.json")
        self.assertEqual(verified.returncode, 0, verified.stderr)
        self.assertEqual(self.remote_tag(), self.first)
        self.assertEqual(self.evidence("rerun.json")["action"], "verified")

    def test_rejects_moved_tag_with_actionable_non_secret_evidence(self) -> None:
        git(
            "push",
            "--force",
            str(self.remote),
            f"{self.second}:refs/tags/{RELEASE_TAG}",
            cwd=self.source,
        )

        rejected = self.publish(self.first)
        self.assertEqual(rejected.returncode, 1)
        self.assertEqual(self.remote_tag(), self.second)
        evidence = self.evidence()
        self.assertEqual(evidence["attempted_ref"], f"refs/tags/{RELEASE_TAG}")
        self.assertEqual(evidence["planned_commit"], self.first)
        self.assertEqual(evidence["outcome"], "failed")
        self.assertIn("release-plan-publication", str(evidence["effective_permission_boundary"]))
        self.assertIn("rerun Release plan recovery", str(evidence["safe_recovery_action"]))
        self.assertNotIn("PRIVATE KEY", json.dumps(evidence))

    def test_rejects_manifest_version_conflict_without_creating_tag(self) -> None:
        conflicting_commit = self.commit("conflicting source identity", "0.1.15")

        rejected = self.publish(conflicting_commit)

        self.assertEqual(rejected.returncode, 1)
        evidence = self.evidence()
        self.assertEqual(evidence["phase"], "source-identity")
        self.assertEqual(evidence["classification"], "terminal-source-identity-conflict")
        self.assertEqual(evidence["declared_version"], "0.1.15")
        self.assertEqual(evidence["planned_version"], RELEASE_TAG)
        self.assertEqual(evidence["attempted_ref"], f"refs/tags/{RELEASE_TAG}")
        self.assertEqual(evidence["planned_commit"], conflicting_commit)
        self.assertIn("protected successor plan", str(evidence["safe_recovery_action"]))
        self.assertIn("do not tag or publish", str(evidence["safe_recovery_action"]))
        self.assertIn("terminal-source-identity-conflict", json.dumps(evidence))
        absent = git("ls-remote", str(self.remote), f"refs/tags/{RELEASE_TAG}")
        self.assertEqual(absent.stdout, "")

    def test_records_repository_authority_push_rejection(self) -> None:
        leaked_token = "ghp_" + "abcdefghijklmnopqrstuvwxyz" + "1234567890"
        hook = self.remote / "hooks" / "pre-receive"
        hook.write_text(
            "#!/usr/bin/env bash\n"
            "printf '%s\\n' 'release policy refused the exact planned tag: older-ancestor tag creation denied' >&2\n"
            f"printf '%s\\n' 'Authorization: Bearer {leaked_token}' >&2\n"
            "printf '%02048d' 0 >&2\n"
            "exit 1\n",
            encoding="utf-8",
        )
        os.chmod(hook, 0o755)

        rejected = self.publish(self.first)
        self.assertEqual(rejected.returncode, 1)
        evidence = self.evidence()
        self.assertEqual(evidence["phase"], "repository-authority")
        self.assertEqual(evidence["attempted_ref"], f"refs/tags/{RELEASE_TAG}")
        self.assertEqual(evidence["planned_commit"], self.first)
        self.assertIn("git push", str(evidence["git_operation"]))
        self.assertIn("write deploy key", str(evidence["effective_permission_boundary"]))
        self.assertIn("do not move the tag", str(evidence["safe_recovery_action"]))
        diagnostic = str(evidence["remote_diagnostic"])
        self.assertIn("release policy refused the exact planned tag", diagnostic)
        self.assertIn("release policy refused the exact planned tag", rejected.stderr)
        self.assertIn("[REDACTED]", diagnostic)
        self.assertIn("[REDACTED]", rejected.stderr)
        self.assertIn("[diagnostic truncated]", diagnostic)
        self.assertLessEqual(len(diagnostic), 2048)
        self.assertNotIn(leaked_token, json.dumps(evidence))
        self.assertNotIn(leaked_token, rejected.stderr)

    def test_recovery_workflow_requires_protected_tag_publication(self) -> None:
        spec = importlib.util.spec_from_file_location("component_release_recovery", RECOVERY_SCRIPT)
        assert spec is not None and spec.loader is not None
        recovery = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = recovery
        spec.loader.exec_module(recovery)
        source = (REPOSITORY_ROOT / ".github/workflows/release-plan-recovery.yml").read_text(encoding="utf-8")

        expected_sha256 = hashlib.sha256(source.encode("utf-8")).hexdigest()
        recovery.verify_recovery_workflow_source("sdk-rust", source, expected_sha256)
        without_environment = source.replace("environment: release-plan-publication", "environment: unprotected")
        with self.assertRaises(recovery.RecoveryError):
            recovery.verify_recovery_workflow_source("sdk-rust", without_environment, expected_sha256)


if __name__ == "__main__":
    unittest.main()
