#!/usr/bin/env python3
"""Focused contract tests for crates.io retirement and live auditing."""

from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts/ci/maintain-crates-io.py"
PLAN = ROOT / "scripts/ci/crates-io-retirement-plan.json"
MANIFEST = ROOT / "Cargo.toml"
WORKFLOW = ROOT / ".github/workflows/crates-io-maintenance.yml"

SPEC = importlib.util.spec_from_file_location("maintain_crates_io", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
maintenance = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = maintenance
SPEC.loader.exec_module(maintenance)


class FakeRegistry:
    def __init__(self, plan: dict[str, object], current: str) -> None:
        self.package = str(plan["package"])
        self.current = current
        self.default = current
        self.missing: set[str] = set()
        self.versions = {
            version: self._version(version, yanked=True)
            for version in plan["retired_versions"]
        }
        self.versions.update(
            {
                "2.0.0-rc.1": self._version("2.0.0-rc.1", yanked=False),
                current: self._version(current, yanked=False),
            }
        )

    @staticmethod
    def _version(version: str, *, yanked: bool) -> dict[str, object]:
        return {
            "checksum": f"checksum-{version}",
            "created_at": "2026-01-01T00:00:00Z",
            "dl_path": f"/api/v1/crates/durable-workflow/{version}/download",
            "num": version,
            "updated_at": "2026-08-09T00:00:00Z",
            "yanked": yanked,
        }

    def get(self, suffix: str = "") -> object:
        if not suffix:
            visible = [
                item
                for version, item in self.versions.items()
                if version not in self.missing
            ]
            return maintenance.RegistryResponse(
                200,
                {
                    "crate": {
                        "default_version": self.default,
                        "newest_version": self.current,
                    },
                    "versions": visible,
                },
            )
        if suffix in self.missing or suffix not in self.versions:
            return maintenance.RegistryResponse(404, None)
        return maintenance.RegistryResponse(200, {"version": self.versions[suffix]})

    def yank(self, _package: str, version: str) -> int:
        self.versions[version]["yanked"] = True
        if all(
            item["yanked"] is True
            for name, item in self.versions.items()
            if maintenance.is_pre_2_final(name)
        ):
            self.default = self.current
        return 0


class RegistryMaintenanceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.plan, self.current, self.digest = maintenance.load_authority(
            PLAN, MANIFEST
        )
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)

    def evidence(self, name: str) -> Path:
        return Path(self.temp.name) / name

    def test_plan_names_every_historical_final_and_no_prerelease(self) -> None:
        self.assertEqual(
            [f"0.1.{patch}" for patch in range(23)],
            self.plan["retired_versions"],
        )
        self.assertTrue(
            all(
                maintenance.is_pre_2_final(version)
                for version in self.plan["retired_versions"]
            )
        )
        self.assertTrue(maintenance.is_2_0_release_candidate(self.current))

    def test_retirement_yanks_only_active_reviewed_versions(self) -> None:
        registry = FakeRegistry(self.plan, self.current)
        registry.default = "0.1.22"
        active = {"0.1.0", "0.1.22"}
        for version in active:
            registry.versions[version]["yanked"] = False
        evidence_path = self.evidence("retirement.json")

        with mock.patch.dict(os.environ, {"CARGO_REGISTRY_TOKEN": "not-recorded"}):
            with mock.patch.object(
                maintenance, "cargo_yank", side_effect=registry.yank
            ) as cargo_yank:
                result = maintenance.retire(
                    self.plan,
                    self.current,
                    self.digest,
                    evidence_path,
                    1,
                    0,
                    registry,
                )

        self.assertEqual(0, result)
        self.assertEqual(active, {call.args[1] for call in cargo_yank.call_args_list})
        self.assertFalse(registry.versions[self.current]["yanked"])
        evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
        self.assertEqual("passed", evidence["outcome"])
        self.assertFalse(evidence["credential_handling"]["value_recorded"])
        self.assertNotIn("not-recorded", evidence_path.read_text(encoding="utf-8"))
        self.assertEqual(
            self.current,
            evidence["registry_after"]["crate_root"]["default_version"],
        )

    def test_retirement_fails_before_mutation_when_history_is_missing(self) -> None:
        registry = FakeRegistry(self.plan, self.current)
        registry.missing.add("0.1.5")
        evidence_path = self.evidence("missing.json")

        with mock.patch.dict(os.environ, {"CARGO_REGISTRY_TOKEN": "not-recorded"}):
            with mock.patch.object(maintenance, "cargo_yank") as cargo_yank:
                result = maintenance.retire(
                    self.plan,
                    self.current,
                    self.digest,
                    evidence_path,
                    1,
                    0,
                    registry,
                )

        self.assertEqual(1, result)
        cargo_yank.assert_not_called()
        evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
        missing = next(
            item
            for item in evidence["registry_before"]["historical_versions"]
            if item["version"] == "0.1.5"
        )
        self.assertEqual("missing_artifact", missing["state"])
        self.assertEqual(404, missing["http_status"])

    def test_live_audit_rejects_stale_default_discovery(self) -> None:
        registry = FakeRegistry(self.plan, self.current)
        registry.default = "0.1.22"
        evidence_path = self.evidence("audit.json")
        with mock.patch.object(maintenance, "verify_exact_install") as install:
            result = maintenance.audit(
                self.plan,
                self.current,
                self.digest,
                evidence_path,
                1,
                0,
                registry,
            )
        self.assertEqual(1, result)
        install.assert_not_called()
        evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
        self.assertIn(
            "crate_root_default_version_is_not_current_release_candidate",
            evidence["violations"],
        )

    def test_live_audit_checks_exact_install_without_registry_credentials(self) -> None:
        registry = FakeRegistry(self.plan, self.current)
        evidence_path = self.evidence("audit.json")
        install_evidence = {
            "credential_available": False,
            "lock_checksum": f"checksum-{self.current}",
            "requirement": f"={self.current}",
            "resolved_version": self.current,
            "state": "installable",
        }
        with mock.patch.object(
            maintenance, "verify_exact_install", return_value=install_evidence
        ) as install:
            result = maintenance.audit(
                self.plan,
                self.current,
                self.digest,
                evidence_path,
                1,
                0,
                registry,
            )
        self.assertEqual(0, result)
        install.assert_called_once_with(
            self.plan["package"],
            self.current,
            f"checksum-{self.current}",
        )
        evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
        self.assertEqual("installable", evidence["exact_installation"]["state"])

    def test_exact_install_matches_cargo_lock_checksum_and_removes_token(self) -> None:
        seen_environment: dict[str, str] = {}

        def cargo_fetch(
            command: list[str], **kwargs: object
        ) -> subprocess.CompletedProcess[str]:
            nonlocal seen_environment
            seen_environment = dict(kwargs["env"])
            cwd = Path(kwargs["cwd"])
            (cwd / "Cargo.lock").write_text(
                "version = 4\n\n"
                "[[package]]\n"
                f'name = "{self.plan["package"]}"\n'
                f'version = "{self.current}"\n'
                f'checksum = "checksum-{self.current}"\n',
                encoding="utf-8",
            )
            self.assertEqual(["cargo", "fetch", "--manifest-path"], command[:3])
            return subprocess.CompletedProcess(command, 0)

        with mock.patch.dict(os.environ, {"CARGO_REGISTRY_TOKEN": "not-recorded"}):
            with mock.patch.object(
                maintenance.subprocess, "run", side_effect=cargo_fetch
            ):
                result = maintenance.verify_exact_install(
                    self.plan["package"],
                    self.current,
                    f"checksum-{self.current}",
                )

        self.assertNotIn("CARGO_REGISTRY_TOKEN", seen_environment)
        self.assertEqual(f"={self.current}", result["requirement"])
        self.assertEqual("installable", result["state"])

    def test_cargo_yank_uses_the_environment_token_without_an_argument(self) -> None:
        completed = subprocess.CompletedProcess([], 0)
        with mock.patch.object(
            maintenance.subprocess, "run", return_value=completed
        ) as run:
            result = maintenance.cargo_yank(self.plan["package"], "0.1.22")

        self.assertEqual(0, result)
        command = run.call_args.args[0]
        self.assertEqual("cargo", command[0])
        self.assertIn("yank", command)
        self.assertIn("0.1.22", command)
        self.assertNotIn("--token", command)


class RegistryWorkflowContractTest(unittest.TestCase):
    def test_registry_credentials_are_confined_to_trusted_manual_main(self) -> None:
        workflow = WORKFLOW.read_text(encoding="utf-8")
        retire = workflow.split("  retire:\n", 1)[1].split("  audit:\n", 1)[0]
        audit = workflow.split("  audit:\n", 1)[1]

        self.assertIn("workflow_dispatch:", workflow)
        self.assertIn("schedule:", workflow)
        self.assertNotIn("inputs:", workflow)
        self.assertNotIn("pull_request", workflow)
        self.assertIn("permissions:\n  contents: read", workflow)
        self.assertIn("github.event_name == 'workflow_dispatch'", retire)
        self.assertIn("github.repository == 'durable-workflow/sdk-rust'", retire)
        self.assertIn("github.ref == 'refs/heads/main'", retire)
        self.assertIn("environment: crates-io", retire)
        self.assertIn("secrets.CARGO_REGISTRY_TOKEN", retire)
        self.assertEqual(1, workflow.count("secrets.CARGO_REGISTRY_TOKEN"))
        self.assertNotIn("CARGO_REGISTRY_TOKEN", audit)
        self.assertIn("maintain-crates-io.py audit", audit)
        self.assertIn("if: ${{ always() }}", workflow)
        self.assertEqual(2, workflow.count("persist-credentials: false"))
        action_references = re.findall(r"uses:\s+[^@\s]+@([^\s#]+)", workflow)
        self.assertTrue(action_references)
        self.assertTrue(
            all(re.fullmatch(r"[0-9a-f]{40}", ref) for ref in action_references)
        )


if __name__ == "__main__":
    unittest.main()
