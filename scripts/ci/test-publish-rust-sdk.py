#!/usr/bin/env python3
"""Focused contract tests for the Rust SDK release entrypoint."""

from __future__ import annotations

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import re
from pathlib import Path
import hashlib
import ssl
import shutil
import subprocess
import sys
import tarfile
import tempfile
import textwrap
import threading
import tomllib
import unittest


ROOT = Path(__file__).resolve().parents[2]
MANIFEST = ROOT / "Cargo.toml"
PUBLISH = ROOT / "scripts" / "ci" / "publish-rust-sdk.sh"
RELEASE_WORKFLOW = ROOT / ".github" / "workflows" / "release.yml"
RELEASE_TOOLING_INSTALLER = ROOT / "scripts" / "ci" / "install-release-tooling.sh"
SOURCE_PACKAGE = tomllib.loads(MANIFEST.read_text(encoding="utf-8"))["package"]
PACKAGE_VERSION = SOURCE_PACKAGE["version"]
PRODUCT_TRAIN = PACKAGE_VERSION
SERVER_VERSIONS = "2.0.0"
QUALIFIED_SERVER_VERSION = SOURCE_PACKAGE["metadata"]["durable-workflow"]["qualified-server-version"]
WORKER_PROTOCOL_VERSION = "1.20"
SERVER_WORKER_PROTOCOLS = ">=1.19,<2.0"
RELEASE_COMMIT = "0123456789abcdef0123456789abcdef01234567"
TOOLING_COMMIT = "fedcba9876543210fedcba9876543210fedcba98"
CHECKSUM = "a" * 64
REAL_GIT = shutil.which("git")
APPROVAL_RELATIVE = f"release/approved-packages/{PACKAGE_VERSION}.json"
SUMMARY_RELATIVE = f"release/approved-packages/{PACKAGE_VERSION}.summary.json"
# The exact receipt approval_candidate() emits for a passing 3.0.5 run.
ACCEPTANCE = {
    "summary_sha256": "c" * 64,
    "harness_commit": "e" * 40,
    "server_index": "ghcr.io/durable-workflow/server@sha256:" + "d" * 64,
    "executed_manifest": "ghcr.io/durable-workflow/server@sha256:" + "f" * 64,
    "platform": "linux/arm64",
    "expected_server_version": "3.0.5",
    "expected_revision": "b" * 40,
    "expected_release_tag": "3.0.5",
    "cluster_versions": ["3.0.5"],
    "cells": ["deadline", "lifecycle"],
    "finished_at": "2026-11-03T23:00:00+00:00",
    "outcome": "pass",
}


class PublishRustSdkContractTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.temp = Path(self.temp_dir.name)
        self.bin_dir = self.temp / "bin"
        self.bin_dir.mkdir()
        self.evidence = self.temp / "evidence.json"
        self.cargo_log = self.temp / "cargo-calls.jsonl"
        self.tooling = self.temp / "tooling"
        self.tooling.mkdir()
        self._tooling_git("init", "--quiet")
        self._write_mock_commands()

    def _write_executable(self, name: str, source: str) -> None:
        path = self.bin_dir / name
        path.write_text(textwrap.dedent(source).lstrip(), encoding="utf-8")
        path.chmod(0o755)

    def _write_mock_commands(self) -> None:
        self._write_executable(
            "cargo",
            r"""
            #!/usr/bin/env python3
            import json
            import io
            import gzip
            import os
            from pathlib import Path
            import sys
            import tarfile
            import tomllib

            command = sys.argv[1]
            if os.environ.get("MOCK_CARGO_LOG"):
                with open(os.environ["MOCK_CARGO_LOG"], "a", encoding="utf-8") as log:
                    log.write(json.dumps({
                        "argv": sys.argv[1:],
                        "phase": os.environ.get("MOCK_PHASE", ""),
                        "credential": bool(os.environ.get("CARGO_REGISTRY_TOKEN")),
                    }) + "\n")
            if command == "-V":
                print(f'cargo {os.environ.get("MOCK_CARGO_VERSION", "1.86.0")} (adf9b6ad1 2025-02-28)')
                raise SystemExit(0)
            manifest = Path(sys.argv[sys.argv.index("--manifest-path") + 1])
            package = tomllib.loads(manifest.read_text(encoding="utf-8"))["package"]
            target = Path(os.environ["CARGO_TARGET_DIR"])
            if command == "metadata":
                print(json.dumps({
                    "packages": [{
                        "name": package["name"],
                        "version": package["version"],
                        "rust_version": package["rust-version"],
                        "repository": package["repository"],
                        "documentation": package["documentation"],
                        "metadata": package["metadata"],
                    }],
                    "target_directory": str(target),
                }))
            elif command == "package":
                if not os.environ.get("MOCK_FIXTURE_CREATION") and "--locked" not in sys.argv:
                    raise SystemExit("release packaging must use --locked")
                archive = target / "package" / f'{package["name"]}-{package["version"]}.crate'
                archive.parent.mkdir(parents=True, exist_ok=True)
                root = f'{package["name"]}-{package["version"]}'
                changelog = Path(
                    os.environ.get("RUST_SDK_CHANGELOG_PATH", "CHANGELOG.md")
                ).read_bytes()
                entries = {
                    f"{root}/CHANGELOG.md": changelog,
                    f"{root}/.cargo_vcs_info.json": json.dumps({
                        "git": {
                            "sha1": os.environ["MOCK_RELEASE_COMMIT"],
                            "dirty": False,
                        }
                    }).encode(),
                    f"{root}/Cargo.lock": (manifest.parent / "Cargo.lock").read_bytes(),
                }
                with archive.open("wb") as output:
                    with gzip.GzipFile(fileobj=output, mode="wb", mtime=0, filename="") as compressed:
                        with tarfile.open(fileobj=compressed, mode="w") as crate:
                            for name, content in entries.items():
                                member = tarfile.TarInfo(name)
                                member.size = len(content)
                                crate.addfile(member, io.BytesIO(content))
            elif command == "publish":
                if "--locked" not in sys.argv or "--no-verify" not in sys.argv:
                    raise SystemExit("publication must use --locked --no-verify")
                Path(os.environ["MOCK_PUBLISH_MARKER"]).write_text("published\n")
            elif command == "build":
                if os.environ.get("MOCK_CONSUMER_BUILD_OUTCOME") == "fail":
                    raise SystemExit("mock fresh consumer build failed")
                dependency = tomllib.loads(manifest.read_text(encoding="utf-8"))["dependencies"]
                exact_version = dependency["durable-workflow"]
                if exact_version != f'={os.environ["MOCK_PACKAGE_VERSION"]}':
                    raise SystemExit(f"consumer dependency is not exact: {exact_version}")
                (manifest.parent / "Cargo.lock").write_text(
                    'version = 4\n\n'
                    '[[package]]\n'
                    'name = "durable-workflow"\n'
                    f'version = "{os.environ["MOCK_PACKAGE_VERSION"]}"\n'
                    'source = "registry+https://github.com/rust-lang/crates.io-index"\n'
                    'checksum = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"\n\n'
                    '[[package]]\n'
                    'name = "durable-workflow-msrv-consumer"\n'
                    'version = "0.0.0"\n'
                    'dependencies = [\n "durable-workflow",\n]\n',
                    encoding="utf-8",
                )
            else:
                raise SystemExit(f"unexpected cargo command: {command}")
            """,
        )
        self._write_executable(
            "rustc",
            r"""
            #!/usr/bin/env python3
            print("rustc 1.86.0 (05f9846f8 2025-03-31)")
            """,
        )
        self._write_executable(
            "git",
            r"""
            #!/usr/bin/env python3
            import os
            import sys

            command = sys.argv[1]
            approvals = os.environ.get("MOCK_APPROVAL_REPOSITORY")
            in_approvals = approvals and os.path.realpath(os.getcwd()) == os.path.realpath(approvals)
            if command in {"ls-tree", "cat-file"} and in_approvals:
                os.execv(os.environ["MOCK_REAL_GIT"], ["git", *sys.argv[1:]])
            if command == "-C" and approvals and os.path.realpath(sys.argv[2]) == os.path.realpath(approvals):
                # The approvals checkout is a real repository.
                os.execv(os.environ["MOCK_REAL_GIT"], ["git", *sys.argv[1:]])
            elif command == "-C":
                print(os.environ["MOCK_TOOLING_COMMIT"])
            elif command == "check-ignore":
                raise SystemExit(0 if os.environ.get("MOCK_IGNORED_LOCKFILE") else 1)
            elif command == "status":
                pass
            elif command in {"rev-parse", "rev-list"}:
                print(os.environ["MOCK_RELEASE_COMMIT"])
            elif command == "ls-remote":
                if sys.argv[2] != "origin":
                    raise SystemExit("tag readback must use origin")
                tag = sys.argv[3]
                peeled = os.environ.get("MOCK_REMOTE_TAG_COMMIT", os.environ["MOCK_RELEASE_COMMIT"])
                print("1111111111111111111111111111111111111111\t" + tag)
                if peeled:
                    print(peeled + "\t" + tag + "^{}")
            else:
                raise SystemExit(f"unexpected git command: {command}")
            """,
        )
        self._write_executable(
            "curl",
            f'''
            #!/usr/bin/env python3
            import hashlib
            import json
            import os
            from pathlib import Path
            import sys

            args = sys.argv[1:]
            output = Path(args[args.index("--output") + 1])
            url = args[-1]
            archive = Path(os.environ["MOCK_REGISTRY_ARCHIVE"])
            checksum = os.environ.get("MOCK_REGISTRY_CHECKSUM", hashlib.sha256(archive.read_bytes()).hexdigest())
            if url.endswith("/download"):
                output.write_bytes(archive.read_bytes())
            elif url.endswith("/{PACKAGE_VERSION}"):
                output.write_text(json.dumps({{"version": {{
                    "num": "{PACKAGE_VERSION}",
                    "checksum": checksum,
                    "created_at": "2026-07-22T00:00:00Z",
                }}}}), encoding="utf-8")
            elif url.endswith("/durable-workflow"):
                output.write_text(json.dumps({{"crate": {{
                    "repository": "https://github.com/durable-workflow/sdk-rust",
                }}}}), encoding="utf-8")
            else:
                raise SystemExit(f"unexpected curl URL: {{url}}")
            if "--write-out" in args:
                published = Path(os.environ["MOCK_PUBLISH_MARKER"]).exists()
                print("200" if published else os.environ.get("MOCK_LOOKUP_STATUS", "200"), end="")
            ''',
        )
    def _publish(
        self,
        manifest: Path = MANIFEST,
        environment: dict[str, str] | None = None,
        cwd: Path = ROOT,
        approval: dict[str, object] | None = None,
        registry_changelog: bytes | None = None,
        between_jobs=None,
        preflight_environment: dict[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        if cwd == ROOT and not manifest.resolve().is_relative_to(ROOT):
            # A modified manifest is its own release checkout: the publisher only
            # reads a manifest and changelog confined to its working directory.
            cwd = manifest.parent
            for name in ("CHANGELOG.md", "Cargo.lock"):
                if not (cwd / name).exists():
                    shutil.copyfile(ROOT / name, cwd / name)
        env = os.environ.copy()
        env.update(
            {
                "PATH": f"{self.bin_dir}{os.pathsep}{env['PATH']}",
                "CARGO_TARGET_DIR": str(self.temp / "target"),
                "MOCK_RELEASE_COMMIT": RELEASE_COMMIT,
                "MOCK_TOOLING_COMMIT": TOOLING_COMMIT,
                "MOCK_PACKAGE_VERSION": PACKAGE_VERSION,
                "MOCK_REGISTRY_ARCHIVE": str(self.temp / "registry" / "package" / f"durable-workflow-{PACKAGE_VERSION}.crate"),
                "RELEASE_TAG": PACKAGE_VERSION,
                "RUST_SDK_MANIFEST_PATH": os.path.relpath(manifest, cwd),
                "RUST_SDK_RELEASE_EVIDENCE_PATH": str(self.evidence),
                "RUST_SDK_APPROVED_PACKAGE_REPOSITORY": str(self.tooling),
                "MOCK_APPROVAL_REPOSITORY": str(self.tooling),
                "MOCK_REAL_GIT": REAL_GIT,
                "MOCK_PUBLISH_MARKER": str(self.temp / "published-marker"),
                "MOCK_CARGO_LOG": str(self.cargo_log),
            }
        )
        env.update(environment or {})
        # The simulated registry's own packaging is not part of the release run.
        fixture_env = {
            **{key: value for key, value in env.items() if key not in {"CARGO_REGISTRY_TOKEN", "MOCK_CARGO_LOG"}},
            "CARGO_TARGET_DIR": str(self.temp / "registry"),
            "RUST_SDK_CHANGELOG_PATH": str(ROOT / "CHANGELOG.md"),
            "MOCK_FIXTURE_CREATION": "1",
        }
        subprocess.run(
            [str(self.bin_dir / "cargo"), "package", "--manifest-path", str(MANIFEST)],
            env=fixture_env, check=True,
        )
        registry_archive = Path(env["MOCK_REGISTRY_ARCHIVE"])
        if approval is not None:
            record, summary = self._approval(registry_archive)
            record.update(approval)
            self._commit_approval(json.dumps(record).encode(), summary=summary)
        if registry_changelog is not None:
            # crates.io serves different (self-consistent) bytes than were approved.
            changed = self.temp / "changed-registry"
            changelog = self.temp / "registry-CHANGELOG.md"
            changelog.write_bytes(registry_changelog)
            subprocess.run(
                [str(self.bin_dir / "cargo"), "package", "--manifest-path", str(MANIFEST)],
                env={**fixture_env, "CARGO_TARGET_DIR": str(changed),
                     "RUST_SDK_CHANGELOG_PATH": str(changelog)}, check=True,
            )
            env["MOCK_REGISTRY_ARCHIVE"] = str(changed / "package" / registry_archive.name)
        return self._run_release(env, cwd, between_jobs, preflight_environment or {})

    def _phase(self, phase: str, env: dict[str, str], cwd: Path) -> subprocess.CompletedProcess[str]:
        if getattr(self, "_fresh_lockfile", False):
            (cwd / "Cargo.lock").unlink(missing_ok=True)
        result = subprocess.run(
            ["bash", str(PUBLISH), phase], cwd=cwd, env={**env, "MOCK_PHASE": phase},
            text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False,
        )
        self.phases.append(phase)
        return result

    def _run_release(self, env, cwd, between_jobs, preflight_environment) -> subprocess.CompletedProcess[str]:
        """Run the four release jobs as release.yml does, stopping at the first failure.

        Only the publish phase receives CARGO_REGISTRY_TOKEN; every other phase
        runs without it, as their jobs have no credential.
        """
        self.phases = []
        token = env.pop("CARGO_REGISTRY_TOKEN", None)
        # Each job starts from a fresh checkout: a lockfile a legacy phase
        # restored is not visible to the next job.
        self._fresh_lockfile = not (cwd / "Cargo.lock").exists()
        jobs = self.temp / "jobs"
        shutil.rmtree(jobs, ignore_errors=True)
        jobs.mkdir()
        outputs = jobs / "gate-output"
        gated = jobs / "gated-package.crate"
        result = self._phase("gate", {**env, "GITHUB_OUTPUT": str(outputs), "RUST_SDK_GATED_PACKAGE_FILE": str(gated)}, cwd)
        if result.returncode:
            return result
        gate = dict(line.split("=", 1) for line in outputs.read_text().splitlines())
        published_in_run = "false"
        if gate.get("publish_required") == "true":
            # actions/upload-artifact + download-artifact + the central validator:
            # job 2 receives the same single file in an isolated directory.
            isolated = jobs / "isolated-gated-package"
            isolated.mkdir()
            shutil.copyfile(gated, isolated / gated.name)
            if between_jobs is not None:
                between_jobs(isolated / gated.name)
            handoff = {
                "RUST_SDK_GATED_PACKAGE_FILE": str(isolated / gated.name),
                "RUST_SDK_GATED_SHA256": hashlib.sha256(gated.read_bytes()).hexdigest(),
                "RUST_SDK_GATED_CRATE_SHA256": gate.get("crate_sha256", ""),
                **preflight_environment,
            }
            result = self._phase("preflight", {**env, **handoff}, cwd)
            if result.returncode:
                return result
            publish_env = dict(env)
            if token is not None:
                publish_env["CARGO_REGISTRY_TOKEN"] = token
            result = self._phase("publish", publish_env, cwd)
            if result.returncode:
                return result
            published_in_run = "true"
        return self._phase("verify", {**env, "RUST_SDK_PUBLISHED_IN_RUN": published_in_run}, cwd)

    def _cargo_calls(self) -> list[dict[str, object]]:
        if not self.cargo_log.exists():
            return []
        return [json.loads(line) for line in self.cargo_log.read_text(encoding="utf-8").splitlines()]

    def _tooling_git(self, *arguments: str) -> None:
        subprocess.run(
            [REAL_GIT, "-C", str(self.tooling), "-c", "user.name=t", "-c", "user.email=t@example.invalid",
             "-c", "commit.gpgsign=false", *arguments],
            check=True, capture_output=True,
            env={**os.environ, "GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_NOSYSTEM": "1"},
        )

    def _commit_approval(self, content: bytes, relative: str = APPROVAL_RELATIVE,
                         summary: bytes | None = None) -> Path:
        files = {relative: content}
        if summary is not None:
            files[SUMMARY_RELATIVE] = summary
        for name, data in files.items():
            path = self.tooling / name
            path.parent.mkdir(parents=True, exist_ok=True)
            if path.is_symlink():
                path.unlink()
            path.write_bytes(data)
            self._tooling_git("add", "--", name)
        self._tooling_git("commit", "--quiet", "--allow-empty", "-m", "approve")
        return self.tooling / relative

    @staticmethod
    def _approval(archive: Path) -> tuple[dict[str, object], bytes]:
        """A record and the exact harness summary.json it binds."""
        root = f"durable-workflow-{PACKAGE_VERSION}/"
        with tarfile.open(archive) as crate:
            files = sorted(member.name.removeprefix(root) for member in crate.getmembers())
        digest = hashlib.sha256(archive.read_bytes()).hexdigest()
        file_list = hashlib.sha256("".join(f"{name}\n" for name in files).encode()).hexdigest()
        summary = (json.dumps({
            "outcome": "pass", "checks_outcome": "pass", "mode": "pre-publication",
            "expected_server": {"version": "3.0.5", "revision": ACCEPTANCE["expected_revision"], "release_tag": "3.0.5"},
            "harness_commit": ACCEPTANCE["harness_commit"],
            "server": {"index": ACCEPTANCE["server_index"], "executed_manifest": ACCEPTANCE["executed_manifest"],
                       "platform": ACCEPTANCE["platform"]},
            "package": {"package": "durable-workflow", "version": PACKAGE_VERSION, "commit": RELEASE_COMMIT,
                        "crate_sha256": digest, "crate_sha256_after_cells": digest, "files": files,
                        "file_list_sha256": file_list, "vcs_info": {"git": {"sha1": RELEASE_COMMIT}, "path_in_vcs": ""},
                        "cargo_version": "cargo 1.86.0 (adf9b6ad1 2025-02-28)"},
            "crate": {"sha256": digest, "lock_checksum": digest, "source_commit": RELEASE_COMMIT,
                      "registry_published": False, "baselines": {"pass": True}},
            "cells": {name: {"outcome": "pass", "cluster_info": {"version": "3.0.5"}} for name in ("lifecycle", "deadline")},
            "cleanup": {"pass": True, "errors": [], "local_leftovers": [], "leftover_containers": [], "leftover_networks": []},
            "finished_at": ACCEPTANCE["finished_at"],
        }, indent=2) + "\n").encode()
        return {
            "schema": "durable-workflow.rust-sdk.approved-package/v1",
            "package": "durable-workflow",
            "version": PACKAGE_VERSION,
            "commit": RELEASE_COMMIT,
            "crate_sha256": hashlib.sha256(archive.read_bytes()).hexdigest(),
            "cargo_version": "1.86.0",
            "files": files,
            "file_list_sha256": file_list,
            "acceptance": {**ACCEPTANCE, "summary_sha256": hashlib.sha256(summary).hexdigest()},
        }, summary

    def _evidence(self) -> dict[str, object]:
        return json.loads(self.evidence.read_text(encoding="utf-8"))

    def _published(self) -> bool:
        return (self.temp / "published-marker").exists()

    # Pre-publication approval binding (sdk-rust#170 Security ruling).

    def test_new_version_publishes_only_the_approved_reproduced_package(self) -> None:
        result = self._publish(environment={"MOCK_LOOKUP_STATUS": "404", "CARGO_REGISTRY_TOKEN": "test-token"},
                               approval={})
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertTrue(self._published())
        evidence = self._evidence()
        self.assertEqual("published", evidence["outcome"])
        self.assertEqual("reproduced", evidence["approved_package"]["status"])
        self.assertTrue(evidence["approved_package"]["registry_matches_approved_package"])
        self.assertEqual(evidence["registry_checksum_sha256"], evidence["approved_package"]["crate_sha256"])
        self.assertEqual(evidence["local_package_checksum_sha256"], evidence["approved_package"]["crate_sha256"])
        self.assertEqual(RELEASE_COMMIT, evidence["source_tag_readback"]["commit"])
        self.assertNotIn("test-token", self.evidence.read_text(encoding="utf-8"))

    def test_new_version_without_an_approval_record_is_never_published(self) -> None:
        result = self._publish(environment={"MOCK_LOOKUP_STATUS": "404", "CARGO_REGISTRY_TOKEN": "test-token"})
        self.assertNotEqual(0, result.returncode)
        self.assertFalse(self._published())
        self.assertEqual("approved_package_required", self._evidence()["reason"])
        result = self._publish(environment={"MOCK_LOOKUP_STATUS": "404", "CARGO_REGISTRY_TOKEN": "test-token",
                                            "RUST_SDK_APPROVED_PACKAGE_REPOSITORY": ""})
        self.assertNotEqual(0, result.returncode)
        self.assertFalse(self._published())

    def test_publish_time_repackage_mismatch_refuses_to_publish(self) -> None:
        cases = {
            "one-byte checksum drift": {"crate_sha256": "0" * 64},
            "wrong commit": {"commit": "f" * 40},
            "wrong version": {"version": "4.11.9"},
            "wrong cargo": {"cargo_version": "1.99.0"},
            "malformed": {"crate_sha256": "not-a-digest"},
            "unexpected field": {"approved_by": "nobody"},
        }
        for label, change in cases.items():
            with self.subTest(label=label):
                result = self._publish(
                    environment={"MOCK_LOOKUP_STATUS": "404", "CARGO_REGISTRY_TOKEN": "test-token"},
                    approval=change,
                )
                self.assertNotEqual(0, result.returncode)
                self.assertFalse(self._published())
                evidence = self._evidence()
                self.assertEqual("approved_package_mismatch", evidence["reason"])
                self.assertEqual("reproduction_mismatch", evidence["approved_package"]["status"])
                self.assertIn("refusing to publish", result.stderr)

    def test_extra_or_missing_approved_file_refuses_to_publish(self) -> None:
        for label, mutate in (
            ("extra", lambda files: sorted(files + ["src/extra.rs"])),
            ("missing", lambda files: files[1:]),
        ):
            with self.subTest(label=label):
                record = self._approval_files_override(mutate)
                result = self._publish(
                    environment={"MOCK_LOOKUP_STATUS": "404", "CARGO_REGISTRY_TOKEN": "test-token"},
                    approval=record,
                )
                self.assertNotEqual(0, result.returncode)
                self.assertFalse(self._published())
                self.assertEqual("approved_package_mismatch", self._evidence()["reason"])
                self.assertIn("file set differs", result.stderr)

    @staticmethod
    def _approval_files_override(mutate) -> dict[str, object]:
        # The mock package holds exactly these members.
        files = mutate([".cargo_vcs_info.json", "CHANGELOG.md", "Cargo.lock"])
        return {"files": files,
                "file_list_sha256": hashlib.sha256("".join(f"{n}\n" for n in files).encode()).hexdigest()}

    def test_post_publish_registry_readback_mismatch_fails(self) -> None:
        result = self._publish(
            environment={"MOCK_LOOKUP_STATUS": "404", "CARGO_REGISTRY_TOKEN": "test-token"},
            approval={},
            registry_changelog=(ROOT / "CHANGELOG.md").read_bytes() + b"\n",
        )
        self.assertNotEqual(0, result.returncode)
        self.assertTrue(self._published())
        evidence = self._evidence()
        self.assertEqual("failed", evidence["outcome"])
        self.assertEqual("published_checksum_differs_from_approved_package", evidence["reason"])
        self.assertFalse(evidence["approved_package"]["registry_matches_approved_package"])

    def test_registry_checksum_claim_differing_from_approval_fails(self) -> None:
        result = self._publish(
            environment={"MOCK_LOOKUP_STATUS": "404", "CARGO_REGISTRY_TOKEN": "test-token",
                         "MOCK_REGISTRY_CHECKSUM": "0" * 64},
            approval={},
        )
        self.assertNotEqual(0, result.returncode)
        self.assertNotEqual("published", self._evidence()["outcome"])
        self.assertFalse(self._evidence()["approved_package"]["registry_matches_approved_package"])

    def test_already_published_version_must_still_equal_its_approval(self) -> None:
        result = self._publish(approval={}, registry_changelog=(ROOT / "CHANGELOG.md").read_bytes() + b"\n")
        self.assertNotEqual(0, result.returncode)
        self.assertEqual("published_checksum_differs_from_approved_package", self._evidence()["reason"])

    def test_post_publish_source_tag_readback_mismatch_fails(self) -> None:
        for label, commit in (("moved", "e" * 40), ("missing", "")):
            with self.subTest(label=label):
                result = self._publish(
                    environment={"MOCK_LOOKUP_STATUS": "404", "CARGO_REGISTRY_TOKEN": "test-token",
                                 "MOCK_REMOTE_TAG_COMMIT": commit},
                    approval={},
                )
                self.assertNotEqual(0, result.returncode)
                evidence = self._evidence()
                self.assertEqual("source_tag_readback_mismatch", evidence["reason"])
                self.assertEqual("failed", evidence["outcome"])
                (self.temp / "published-marker").unlink(missing_ok=True)

    # Path confinement of every file the publisher reads or writes.

    def test_source_paths_outside_the_release_checkout_are_refused(self) -> None:
        source = self._source_checkout()
        outside = self.temp / "outside"
        outside.mkdir()
        shutil.copyfile(ROOT / "CHANGELOG.md", outside / "CHANGELOG.md")
        linked = source / "linked"
        linked.mkdir()
        (linked / "CHANGELOG.md").symlink_to(outside / "CHANGELOG.md")
        cases = {
            "absolute manifest": {"RUST_SDK_MANIFEST_PATH": str(source / "Cargo.toml")},
            "dot-dot manifest": {"RUST_SDK_MANIFEST_PATH": "../source/Cargo.toml"},
            "dot-dot changelog": {"RUST_SDK_CHANGELOG_PATH": "../outside/CHANGELOG.md"},
            "absolute changelog": {"RUST_SDK_CHANGELOG_PATH": str(outside / "CHANGELOG.md")},
            "symlinked changelog": {"RUST_SDK_CHANGELOG_PATH": "linked/CHANGELOG.md"},
        }
        for label, environment in cases.items():
            with self.subTest(label=label):
                result = self._publish(source / "Cargo.toml", environment, cwd=source)
                self.assertNotEqual(0, result.returncode)
                self.assertIn("inside the release checkout", result.stderr)
                self.assertFalse(self._published())
        # Positive control: the same checkout with plain relative paths publishes.
        result = self._publish(source / "Cargo.toml", {"MOCK_LOOKUP_STATUS": "404",
                                                       "CARGO_REGISTRY_TOKEN": "test-token"},
                               cwd=source, approval={})
        self.assertEqual(0, result.returncode, result.stderr)

    def _approved_record(self) -> bytes:
        """Publish once with a valid record, then reset the registry state."""
        result = self._publish(environment={"MOCK_LOOKUP_STATUS": "404", "CARGO_REGISTRY_TOKEN": "test-token"},
                               approval={})
        self.assertEqual(0, result.returncode, result.stderr)
        (self.temp / "published-marker").unlink()
        return (self.tooling / APPROVAL_RELATIVE).read_bytes()

    def _publish_new(self) -> subprocess.CompletedProcess[str]:
        return self._publish(environment={"MOCK_LOOKUP_STATUS": "404", "CARGO_REGISTRY_TOKEN": "test-token"})

    def test_approval_record_must_be_the_tracked_regular_file(self) -> None:
        record = self._approved_record()
        path = self.tooling / APPROVAL_RELATIVE
        outside = self.temp / "forged.json"
        outside.write_bytes(record)

        def external_symlink() -> None:
            path.unlink()
            path.symlink_to(outside)

        def tracked_symlink() -> None:
            path.unlink()
            path.symlink_to(outside)
            self._tooling_git("add", "--", APPROVAL_RELATIVE)
            self._tooling_git("commit", "--quiet", "-m", "symlink")

        def symlinked_directory() -> None:
            directory = self.tooling / "release" / "approved-packages"
            shutil.move(directory, self.temp / "elsewhere")
            directory.symlink_to(self.temp / "elsewhere", target_is_directory=True)

        def untracked_copy() -> None:
            self._tooling_git("rm", "--quiet", "--cached", "--", APPROVAL_RELATIVE)
            self._tooling_git("commit", "--quiet", "-m", "untrack")

        def edited_worktree() -> None:
            path.write_bytes(record.replace(b'"outcome": "pass"', b'"outcome": "pass" '))

        for label, mutate, message in (
            ("external symlink", external_symlink, "symbolic link"),
            ("tracked symlink", tracked_symlink, "symbolic link"),
            ("symlinked directory", symlinked_directory, "symbolic link"),
            ("untracked record", untracked_copy, "not tracked at its exact path"),
            ("worktree differs from tracked blob", edited_worktree, "differs from its tracked content"),
        ):
            with self.subTest(label=label):
                mutate()
                result = self._publish_new()
                self.assertNotEqual(0, result.returncode)
                self.assertFalse(self._published())
                self.assertEqual("approved_package_mismatch", self._evidence()["reason"])
                self.assertIn(message, result.stderr)
                # Restore the tracked regular record for the next case.
                if (self.tooling / "release" / "approved-packages").is_symlink():
                    (self.tooling / "release" / "approved-packages").unlink()
                    shutil.move(self.temp / "elsewhere", self.tooling / "release" / "approved-packages")
                self._commit_approval(record)

    def test_record_tracked_at_a_wrong_path_is_not_an_approval(self) -> None:
        record = self._approved_record()
        self._tooling_git("rm", "--quiet", "--", APPROVAL_RELATIVE)
        for relative in ("release/approved-packages/4.11.0.json", f"release/{PACKAGE_VERSION}.json",
                         f"release/approved-packages/nested/{PACKAGE_VERSION}.json",
                         f"approved-packages/{PACKAGE_VERSION}.json"):
            self._commit_approval(record, relative)
        result = self._publish_new()
        self.assertNotEqual(0, result.returncode)
        self.assertFalse(self._published())
        self.assertEqual("approved_package_required", self._evidence()["reason"])

    def test_truncated_or_fabricated_acceptance_receipt_refuses_to_publish(self) -> None:
        cases = {"empty receipt": {}, "receipt without outcome": {k: v for k, v in ACCEPTANCE.items() if k != "outcome"},
                 "failed receipt": {**ACCEPTANCE, "outcome": "fail"},
                 "other Server": {**ACCEPTANCE, "cluster_versions": ["3.0.4"]}}
        for label, acceptance in cases.items():
            with self.subTest(label=label):
                result = self._publish(
                    environment={"MOCK_LOOKUP_STATUS": "404", "CARGO_REGISTRY_TOKEN": "test-token"},
                    approval={"acceptance": acceptance},
                )
                self.assertNotEqual(0, result.returncode)
                self.assertFalse(self._published())
                self.assertEqual("approved_package_mismatch", self._evidence()["reason"])
                self.assertIn("acceptance receipt", result.stderr)

    def test_symlinked_evidence_path_is_refused(self) -> None:
        target = self.temp / "elsewhere.json"
        target.write_text("untouched", encoding="utf-8")
        link = self.temp / "linked-evidence.json"
        link.symlink_to(target)
        for value in (str(link), str(self.temp / "sub" / ".." / "evidence.json")):
            with self.subTest(value=value):
                (self.temp / "sub").mkdir(exist_ok=True)
                result = self._publish(environment={"RUST_SDK_RELEASE_EVIDENCE_PATH": value})
                self.assertNotEqual(0, result.returncode)
                self.assertIn("release evidence path", result.stderr)
        self.assertEqual("untouched", target.read_text(encoding="utf-8"))

    # F1: the publication credential never shares a job with crate code.

    def test_credential_reaches_only_the_no_verify_upload(self) -> None:
        result = self._publish(environment={"MOCK_LOOKUP_STATUS": "404", "CARGO_REGISTRY_TOKEN": "test-token"},
                               approval={})
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual(["gate", "preflight", "publish", "verify"], self.phases)
        calls = self._cargo_calls()
        with_credential = [call for call in calls if call["credential"]]
        self.assertEqual({"publish"}, {call["phase"] for call in with_credential})
        self.assertEqual({"metadata", "publish"}, {call["argv"][0] for call in with_credential})
        [upload] = [call for call in calls if call["argv"][0] == "publish"]
        self.assertLessEqual({"--locked", "--no-verify"}, set(upload["argv"]))
        packages = {call["phase"]: call["argv"] for call in calls if call["argv"][0] == "package"}
        self.assertNotIn("--no-verify", packages["gate"])  # the verify build runs credential-free
        self.assertIn("--no-verify", packages["preflight"])
        self.assertIn("--no-verify", packages["verify"])
        for call in calls:
            if call["argv"][0] in {"build", "test", "run", "check"}:
                self.assertFalse(call["credential"], call)
                self.assertNotEqual("publish", call["phase"])
        self.assertEqual(set(), {call["argv"][0] for call in calls if call["phase"] == "publish"} - {"metadata", "publish"})

    def test_phases_refuse_a_misplaced_credential(self) -> None:
        def run(phase: str, **environment: str) -> subprocess.CompletedProcess[str]:
            env = {key: value for key, value in os.environ.items()
                   if key not in {"CARGO_REGISTRY_TOKEN", "CARGO_REGISTRIES_CRATES_IO_TOKEN"}}
            return subprocess.run(["bash", str(PUBLISH), *([phase] if phase else [])], cwd=ROOT,
                                  env={**env, **environment}, text=True, capture_output=True, check=False)
        for phase in ("gate", "preflight", "verify"):
            for variable in ("CARGO_REGISTRY_TOKEN", "CARGO_REGISTRIES_CRATES_IO_TOKEN"):
                with self.subTest(phase=phase, variable=variable):
                    result = run(phase, **{variable: "test-token"})
                    self.assertEqual(1, result.returncode)
                    self.assertIn("must not see a crates.io publication credential", result.stderr)
                    self.assertNotIn("test-token", result.stderr + result.stdout)
        result = run("publish")
        self.assertEqual(1, result.returncode)
        self.assertIn("requires CARGO_REGISTRY_TOKEN", result.stderr)
        for phase in ("", "all", "--help"):
            with self.subTest(phase=phase):
                self.assertEqual(2, run(phase).returncode)

    def test_gated_handoff_must_equal_the_reproduction_and_approval(self) -> None:
        def flip_byte(path: Path) -> None:
            data = bytearray(path.read_bytes())
            data[-1] ^= 1
            path.write_bytes(bytes(data))

        def extra_file(path: Path) -> None:
            (path.parent / "extra.crate").write_bytes(b"x")

        def symlink(path: Path) -> None:
            copy = path.parent.parent / "elsewhere.crate"
            path.replace(copy)
            path.symlink_to(copy)

        def missing(path: Path) -> None:
            path.unlink()
        cases = {
            "one-byte artifact drift": (flip_byte, {}),
            "extra file in the handoff": (extra_file, {}),
            "symlinked artifact": (symlink, {}),
            "missing artifact": (missing, {}),
            "artifact digest output differs": (None, {"RUST_SDK_GATED_SHA256": "0" * 64}),
            "gate output differs": (None, {"RUST_SDK_GATED_CRATE_SHA256": "0" * 64}),
            "malformed digest": (None, {"RUST_SDK_GATED_SHA256": "sha256:" + "0" * 64}),
        }
        for label, (tamper, override) in cases.items():
            with self.subTest(label=label):
                self.cargo_log.unlink(missing_ok=True)
                result = self._publish(
                    environment={"MOCK_LOOKUP_STATUS": "404", "CARGO_REGISTRY_TOKEN": "test-token"},
                    approval={}, between_jobs=tamper, preflight_environment=override,
                )
                self.assertNotEqual(0, result.returncode)
                self.assertEqual(["gate", "preflight"], self.phases)
                self.assertEqual("gated_package_mismatch", self._evidence()["reason"])
                self.assertFalse(self._published())
                self.assertNotIn("publish", [call["argv"][0] for call in self._cargo_calls()])

    def test_version_published_between_jobs_is_not_uploaded_again(self) -> None:
        result = self._publish(
            environment={"MOCK_LOOKUP_STATUS": "404", "CARGO_REGISTRY_TOKEN": "test-token"}, approval={},
            between_jobs=lambda _path: (self.temp / "published-marker").write_text("elsewhere\n"),
        )
        self.assertNotEqual(0, result.returncode)
        self.assertEqual("registry_version_already_exists", self._evidence()["reason"])
        self.assertNotIn("publish", [call["argv"][0] for call in self._cargo_calls()])

    def test_tagged_source_cannot_configure_cargo_or_the_toolchain(self) -> None:
        for override in (".cargo/config.toml", ".cargo/config", "rust-toolchain.toml", "rust-toolchain"):
            with self.subTest(override=override):
                source = self.temp / "source"
                shutil.rmtree(source, ignore_errors=True)
                source = self._source_checkout()
                (source / override).parent.mkdir(parents=True, exist_ok=True)
                (source / override).write_text("[toolchain]\npath = \"/tmp/x\"\n", encoding="utf-8")
                result = self._publish(source / "Cargo.toml", {"MOCK_LOOKUP_STATUS": "404",
                                                               "CARGO_REGISTRY_TOKEN": "test-token"},
                                       cwd=source, approval={})
                self.assertNotEqual(0, result.returncode)
                self.assertIn("must not contain " + override, result.stderr)
                self.assertFalse(self._published())

    # F4: a record tracked at HEAD but absent from the worktree is still an
    # approval, so it can never silently fall back to legacy verification.

    def test_tracked_record_missing_from_the_worktree_still_binds(self) -> None:
        self._approved_record()
        (self.tooling / APPROVAL_RELATIVE).unlink()
        for status in ("200", "404"):
            with self.subTest(registry=status):
                result = self._publish(environment={"MOCK_LOOKUP_STATUS": status, "CARGO_REGISTRY_TOKEN": "test-token"})
                self.assertNotEqual(0, result.returncode)
                self.assertEqual("approved_package_mismatch", self._evidence()["reason"])
                self.assertEqual("reproduction_mismatch", self._evidence()["approved_package"]["status"])
                self.assertIn("not an existing regular file", result.stderr)
                self.assertFalse(self._published())

    def test_release_workflow_supplies_the_reviewed_approval_record(self) -> None:
        workflow = RELEASE_WORKFLOW.read_text(encoding="utf-8")
        self.assertIn(
            "RUST_SDK_APPROVED_PACKAGE_REPOSITORY: ${{ github.workspace }}/release-tools",
            workflow,
        )
        # The record comes from maintained tooling (main), never the tagged source.
        self.assertIn("ref: ${{ github.workflow_sha }}", workflow)
        self.assertLess(workflow.index("Resolve exact release identity"), workflow.index("RUST_SDK_APPROVED_PACKAGE_REPOSITORY"))
        self.assertNotIn("/release/", MANIFEST.read_text(encoding="utf-8").split("include = ", 1)[1].split("\n", 1)[0])

    def _manifest_with(self, old: str, new: str) -> Path:
        manifest = self.temp / "Cargo.toml"
        source = MANIFEST.read_text(encoding="utf-8")
        self.assertIn(old, source)
        manifest.write_text(source.replace(old, new, 1), encoding="utf-8")
        return manifest

    def _source_checkout(self, *, legacy: bool = False) -> Path:
        source = self.temp / "source"
        source.mkdir()
        for name in ("Cargo.toml", "CHANGELOG.md"):
            shutil.copyfile(ROOT / name, source / name)
        if not legacy:
            shutil.copyfile(ROOT / "Cargo.lock", source / "Cargo.lock")
        return source

    def test_maintained_tooling_keeps_separate_source_identity(self) -> None:
        source = self._source_checkout()
        old_script = source / "scripts" / "ci" / "publish-rust-sdk.sh"
        old_script.parent.mkdir(parents=True)
        old_script.write_text("exit 99\n", encoding="utf-8")
        result = self._publish(source / "Cargo.toml", cwd=source)
        self.assertEqual(0, result.returncode, result.stderr)
        evidence = json.loads(self.evidence.read_text(encoding="utf-8"))
        self.assertEqual(RELEASE_COMMIT, evidence["release"]["commit"])
        self.assertEqual(TOOLING_COMMIT, evidence["release_tooling"]["commit"])
        self.assertEqual("source_checkout", evidence["release_tooling"]["lockfile_source"])

    def test_legacy_release_restores_only_verified_missing_ignored_lockfile(self) -> None:
        source = self._source_checkout(legacy=True)
        result = self._publish(source / "Cargo.toml", {"MOCK_IGNORED_LOCKFILE": "1"}, cwd=source)
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual((ROOT / "Cargo.lock").read_bytes(), (source / "Cargo.lock").read_bytes())
        evidence = json.loads(self.evidence.read_text(encoding="utf-8"))
        self.assertTrue(evidence["exact_registry_archive_matches_release_checkout"])
        self.assertEqual("verified_registry_archive", evidence["release_tooling"]["lockfile_source"])

    def test_legacy_lock_rejects_wrong_registry_checksum_before_writing(self) -> None:
        source = self._source_checkout(legacy=True)
        result = self._publish(source / "Cargo.toml", {
            "MOCK_IGNORED_LOCKFILE": "1", "MOCK_REGISTRY_CHECKSUM": "0" * 64,
        }, cwd=source)
        self.assertNotEqual(0, result.returncode)
        self.assertFalse((source / "Cargo.lock").exists())
        self.assertIn("checksum differs", result.stderr)

    def test_missing_unignored_lock_cannot_authorize_recovery(self) -> None:
        source = self._source_checkout(legacy=True)
        result = self._publish(source / "Cargo.toml", cwd=source)
        self.assertNotEqual(0, result.returncode)
        self.assertFalse((source / "Cargo.lock").exists())
        self.assertEqual("release_lockfile_missing_or_invalid", json.loads(self.evidence.read_text())["reason"])

    def test_existing_lock_difference_cannot_be_repaired_from_registry(self) -> None:
        source = self._source_checkout()
        lockfile = source / "Cargo.lock"
        lockfile.write_bytes(lockfile.read_bytes() + b"\n# different authorized source\n")
        before = lockfile.read_bytes()
        result = self._publish(source / "Cargo.toml", cwd=source)
        self.assertNotEqual(0, result.returncode)
        self.assertEqual(before, lockfile.read_bytes())
        self.assertEqual("published_source_archive_mismatch", json.loads(self.evidence.read_text())["reason"])

    def test_manifest_declares_component_and_qualified_baseline(self) -> None:
        package = tomllib.loads(MANIFEST.read_text(encoding="utf-8"))["package"]
        metadata = package["metadata"]["durable-workflow"]
        self.assertEqual(PACKAGE_VERSION, package["version"])
        self.assertEqual(PRODUCT_TRAIN, metadata["product-train"])
        self.assertEqual("protocol-manifests", metadata["compatibility-authority"])
        self.assertEqual(SERVER_VERSIONS, metadata["supported-server-versions"])
        self.assertEqual({"2": SERVER_VERSIONS, "3": "3.0.2"}, metadata["supported-server-release-baselines"])
        self.assertEqual(QUALIFIED_SERVER_VERSION, metadata["qualified-server-version"])
        self.assertEqual(WORKER_PROTOCOL_VERSION, metadata["worker-protocol-version"])
        self.assertEqual(
            SERVER_WORKER_PROTOCOLS, metadata["server-worker-protocol-versions"]
        )
        self.assertTrue(metadata["durable-condition-waits"])
        self.assertEqual("open_condition_wait", metadata["condition-wait-command"])
        self.assertEqual(
            "1.9", metadata["condition-wait-minimum-worker-protocol-version"]
        )
        self.assertEqual(
            "condition_wait_occurrence_identity",
            metadata["condition-wait-occurrence-capability"],
        )
        self.assertEqual(
            "explicit-deterministic-authored-ordinal",
            metadata["condition-wait-occurrence-identity"],
        )
        self.assertEqual(
            "1.17",
            metadata["condition-wait-occurrence-minimum-worker-protocol-version"],
        )
        self.assertTrue(metadata["durable-selection"])
        self.assertEqual("select+select_keyed", metadata["durable-selection-authoring"])
        self.assertEqual(
            "1.19", metadata["durable-selection-minimum-worker-protocol-version"]
        )
        self.assertTrue(metadata["workflow-search-attribute-updates"])
        self.assertEqual(
            "upsert_search_attributes", metadata["search-attribute-update-command"]
        )
        self.assertEqual(
            "1.8",
            metadata["search-attribute-update-minimum-worker-protocol-version"],
        )
        self.assertTrue(metadata["typed-search-attributes"])
        self.assertEqual(
            "1.16",
            metadata["typed-search-attributes-minimum-worker-protocol-version"],
        )
        self.assertEqual(
            "json-value+canonical-declared-type",
            metadata["search-attribute-replay-identity"],
        )
        self.assertEqual(
            "value-only+unknown-type-identity",
            metadata["legacy-search-attribute-history"],
        )
        self.assertTrue(metadata["message-streams"])
        self.assertEqual(
            "1.15", metadata["message-streams-minimum-worker-protocol-version"]
        )

    def test_release_path_accepts_component_advance_and_emits_baseline(self) -> None:
        result = self._publish()
        self.assertEqual(0, result.returncode, result.stderr)
        evidence = json.loads(self.evidence.read_text(encoding="utf-8"))
        self.assertEqual(PACKAGE_VERSION, evidence["package_version"])
        self.assertEqual(PRODUCT_TRAIN, evidence["product_train"])
        self.assertEqual("protocol-manifests", evidence["compatibility_authority"])
        self.assertEqual(SERVER_VERSIONS, evidence["supported_server_versions"])
        self.assertEqual({"2": SERVER_VERSIONS, "3": "3.0.2"}, evidence["supported_server_release_baselines"])
        self.assertEqual(QUALIFIED_SERVER_VERSION, evidence["qualified_server_version"])
        self.assertEqual(
            WORKER_PROTOCOL_VERSION,
            evidence["protocol_compatibility"]["worker_protocol"],
        )
        self.assertEqual(
            SERVER_WORKER_PROTOCOLS,
            evidence["protocol_compatibility"]["server_worker_protocol_versions"],
        )
        protocol = evidence["protocol_compatibility"]
        self.assertTrue(protocol["durable_condition_waits"])
        self.assertEqual("open_condition_wait", protocol["condition_wait_command"])
        self.assertEqual("1.9", protocol["condition_wait_minimum_worker_protocol"])
        self.assertEqual(["satisfied", "timed_out"], protocol["condition_wait_result"])
        self.assertEqual(
            "condition_wait_occurrence_identity",
            protocol["condition_wait_occurrence_capability"],
        )
        self.assertEqual(
            "explicit-deterministic-authored-ordinal",
            protocol["condition_wait_occurrence_identity"],
        )
        self.assertEqual(
            "1.17",
            protocol["condition_wait_occurrence_minimum_worker_protocol"],
        )
        self.assertTrue(protocol["durable_selection"])
        self.assertEqual("select+select_keyed", protocol["durable_selection_authoring"])
        self.assertEqual(
            "1.19", protocol["durable_selection_minimum_worker_protocol"]
        )
        self.assertEqual(
            "SelectionResolved", protocol["durable_selection_winner_history_event"]
        )
        self.assertEqual(
            ["continue", "await", "cancel"],
            protocol["durable_selection_non_winner_lifecycle"],
        )
        self.assertTrue(protocol["workflow_search_attribute_updates"])
        self.assertEqual(
            "upsert_search_attributes", protocol["search_attribute_update_command"]
        )
        self.assertEqual(
            "1.8", protocol["search_attribute_update_minimum_worker_protocol"]
        )
        self.assertEqual(
            [
                "string",
                "keyword",
                "keyword_list",
                "int",
                "float",
                "bool",
                "datetime",
            ],
            protocol["search_attribute_types"],
        )
        self.assertTrue(protocol["typed_search_attributes"])
        self.assertEqual(
            "1.16",
            protocol["typed_search_attributes_minimum_worker_protocol"],
        )
        self.assertEqual(
            "json-value+canonical-declared-type",
            protocol["search_attribute_replay_identity"],
        )
        self.assertEqual(
            "value-only+unknown-type-identity",
            protocol["legacy_search_attribute_history"],
        )
        self.assertTrue(evidence["registry_verified"])
        self.assertEqual("1.86", evidence["fresh_consumer"]["rust_version"])
        self.assertEqual(
            f'durable-workflow = "={PACKAGE_VERSION}"',
            evidence["fresh_consumer"]["exact_dependency"],
        )
        self.assertTrue(evidence["fresh_consumer"]["fresh_lockfile"])
        self.assertTrue(evidence["fresh_consumer"]["build_verified"])
        self.assertEqual(
            evidence["release_notes"]["source_sha256"],
            evidence["release_notes"]["downloaded_archive_sha256"],
        )
        self.assertEqual(
            evidence["release_notes"]["current_entry_sha256"],
            evidence["release_notes"]["downloaded_current_entry_sha256"],
        )
        self.assertEqual(
            RELEASE_COMMIT,
            evidence["repository_provenance"]["downloaded_archive_vcs_commit"],
        )

    def test_release_path_fails_closed_when_fresh_consumer_does_not_build(self) -> None:
        result = self._publish(environment={"MOCK_CONSUMER_BUILD_OUTCOME": "fail"})
        self.assertNotEqual(0, result.returncode)
        evidence = json.loads(self.evidence.read_text(encoding="utf-8"))
        self.assertEqual("failed", evidence["outcome"])
        self.assertEqual(
            "published_fresh_consumer_msrv_build_failed", evidence["reason"]
        )
        self.assertTrue(evidence["registry_verified"])
        self.assertFalse(evidence["fresh_consumer"]["fresh_lockfile"])
        self.assertFalse(evidence["fresh_consumer"]["build_verified"])

    def test_release_path_rejects_a_divergent_product_train(self) -> None:
        manifest = self._manifest_with(
            f'product-train = "{PRODUCT_TRAIN}"',
            'product-train = "2.0.0-beta.3"',
        )
        result = self._publish(manifest)
        self.assertNotEqual(0, result.returncode)
        self.assertIn("must match its product train", result.stderr)

    def test_release_path_rejects_a_component_version_mismatch(self) -> None:
        manifest = self._manifest_with(
            f'version = "{PACKAGE_VERSION}"',
            'version = "2.0.0-rc.4"',
        )
        result = self._publish(manifest)
        self.assertNotEqual(0, result.returncode)
        self.assertIn("must match its product train", result.stderr)

    def test_release_path_refuses_a_new_pre_2_package(self) -> None:
        manifest = self.temp / "Cargo.toml"
        source = MANIFEST.read_text(encoding="utf-8")
        source = source.replace(
            f'version = "{PACKAGE_VERSION}"', 'version = "0.1.23"', 1
        ).replace(
            f'product-train = "{PRODUCT_TRAIN}"',
            'product-train = "0.1.23"',
            1,
        )
        manifest.write_text(source, encoding="utf-8")
        result = self._publish(manifest)
        self.assertNotEqual(0, result.returncode)
        self.assertIn("refuses unsupported pre-2.0", result.stderr)

    def test_release_path_rejects_a_divergent_server_version(self) -> None:
        manifest = self._manifest_with(
            f'supported-server-versions = "{SERVER_VERSIONS}"',
            'supported-server-versions = ">=0.2,<0.3"',
        )
        result = self._publish(manifest)
        self.assertNotEqual(0, result.returncode)
        self.assertIn("supported Server contract", result.stderr)

    def test_release_path_rejects_missing_condition_capability(self) -> None:
        manifest = self._manifest_with(
            "durable-condition-waits = true",
            "durable-condition-waits = false",
        )
        result = self._publish(manifest)
        self.assertNotEqual(0, result.returncode)

    def test_release_path_rejects_invalid_server_baselines(self) -> None:
        declaration = 'supported-server-release-baselines = { "2" = "2.0.0", "3" = "3.0.2" }'
        for replacement in (
            '',
            'supported-server-release-baselines = {}',
            'supported-server-release-baselines = ["3.0.2"]',
            'supported-server-release-baselines = { "02" = "2.0.0", "3" = "3.0.2" }',
            'supported-server-release-baselines = { "2" = "2.1.0", "3" = "3.0.2" }',
            'supported-server-release-baselines = { "3" = "3.0.2" }',
            'supported-server-release-baselines = { "2" = "2.0.0", "3" = 3 }',
            'supported-server-release-baselines = { "2" = "2.0.0", "3" = "3.0.2-rc.1" }',
            'supported-server-release-baselines = { "2" = "2.0.0", "3" = "3.00.2" }',
            'supported-server-release-baselines = { "2" = "2.0.0", "3" = "3.0.2", "4" = "3.0.2" }',
            # Native 3.0.0 and 3.0.1 omit terminal failure detail from describe (server#967).
            'supported-server-release-baselines = { "2" = "2.0.0", "3" = "3.0.0" }',
            'supported-server-release-baselines = { "2" = "2.0.0", "3" = "3.0.1" }',
        ):
            with self.subTest(replacement=replacement):
                result = self._publish(self._manifest_with(declaration, replacement))
                self.assertNotEqual(0, result.returncode)
                self.assertIn("Server release baselines", result.stderr)

    def test_release_path_rejects_an_invalid_qualified_server_identity(self) -> None:
        for version in ("3.0.0", "2.5", "2.5.0-rc.0", "2.5.0-preview.1"):
            with self.subTest(version=version):
                manifest = self._manifest_with(
                    f'qualified-server-version = "{QUALIFIED_SERVER_VERSION}"',
                    f'qualified-server-version = "{version}"',
                )
                result = self._publish(manifest)
                self.assertNotEqual(0, result.returncode)
                self.assertIn("supported Server contract", result.stderr)

    def test_release_path_rejects_package_without_current_release_notes(self) -> None:
        source = self._source_checkout()
        (source / "CHANGELOG.md").write_text(
            "# Changelog\n\n## 2.0.0-rc.31\n\n- Previous release.\n",
            encoding="utf-8",
        )
        result = self._publish(source / "Cargo.toml", cwd=source)
        self.assertNotEqual(0, result.returncode)
        evidence = json.loads(self.evidence.read_text(encoding="utf-8"))
        self.assertEqual("local_package_release_identity_mismatch", evidence["reason"])
        self.assertIn("current-version entry", result.stderr)

    @staticmethod
    def _workflow_jobs() -> dict[str, str]:
        """release.yml job name -> that job's source text."""
        workflow = RELEASE_WORKFLOW.read_text(encoding="utf-8")
        body = workflow.split("\njobs:\n", maxsplit=1)[1]
        parts = re.split(r"^  ([a-z][a-z0-9-]*):\n", body, flags=re.MULTILINE)
        return {parts[index]: parts[index + 1] for index in range(1, len(parts), 2)}

    @staticmethod
    def _steps(job: str) -> list[str]:
        return ["      - " + step for step in job.split("\n      - ")[1:]]

    def test_release_workflow_only_publishes_planned_dispatches(self) -> None:
        workflow = RELEASE_WORKFLOW.read_text(encoding="utf-8")
        triggers = workflow.split("\njobs:\n", maxsplit=1)[0]
        self.assertIn("workflow_dispatch:", triggers)
        self.assertNotIn("push:", triggers)
        self.assertNotIn("secrets.", workflow)
        self.assertEqual(["gate", "publish", "verify", "release"], list(self._workflow_jobs()))
        for name, job in self._workflow_jobs().items():
            with self.subTest(job=name):
                self.assertIn("github.ref == 'refs/heads/main'", job)
                self.assertIn('scripts/ci/install-release-tooling.sh "${RUNNER_TEMP}/release-tooling"', job)
                self.assertLess(job.index("Bootstrap patched pip trust"), job.index("Install hash-locked release tooling"))
                self.assertIn("persist-credentials: false", job.replace("'false'", "false"))

    def test_publication_credential_exists_only_in_the_publish_job(self) -> None:
        jobs = self._workflow_jobs()
        for marker in ("CARGO_REGISTRY_TOKEN", "id-token", "crates-io-auth-action", "crates-auth",
                       "environment: crates-io"):
            holders = [name for name, job in jobs.items() if marker in job]
            with self.subTest(marker=marker):
                self.assertEqual(["publish"], holders)
        publish = jobs["publish"]
        self.assertEqual(1, publish.count("id-token: write"))
        self.assertEqual(1, publish.count("CARGO_REGISTRY_TOKEN"))
        # The credential is minted after the credential-free preflight, and only
        # the single upload step receives it.
        steps = self._steps(publish)
        [token_step] = [step for step in steps if "CARGO_REGISTRY_TOKEN" in step]
        self.assertIn("CARGO_REGISTRY_TOKEN: ${{ steps.crates-auth.outputs.token }}", token_step)
        self.assertTrue(token_step.rstrip().endswith("run: ../scripts/ci/publish-rust-sdk.sh publish"))
        names = [step.split("\n", 1)[0] for step in steps]
        self.assertLess(names.index("      - name: Preflight the gated package without the publication credential"),
                        names.index("      - name: Obtain a short-lived crates.io publication token"))
        self.assertLess(names.index("      - name: Obtain a short-lived crates.io publication token"),
                        names.index("      - name: Publish the gated package"))
        for name in ("gate", "verify", "release"):
            with self.subTest(job=name):
                self.assertNotIn("id-token", jobs[name])
                self.assertNotIn("environment:", jobs[name])

    def test_publish_job_runs_no_crate_build_or_test(self) -> None:
        publish = self._workflow_jobs()["publish"]
        for forbidden in ("cargo build", "cargo test", "cargo run", "cargo check", "verify-fresh-consumer",
                          "publish-rust-sdk.sh gate", "publish-rust-sdk.sh verify", "cargo package",
                          "cargo publish"):
            with self.subTest(forbidden=forbidden):
                self.assertNotIn(forbidden, publish)
        phases = re.findall(r"publish-rust-sdk\.sh (\w+)", publish)
        self.assertEqual(["preflight", "publish"], phases)
        # The tagged source is data for --no-verify packaging: Cargo cannot be
        # redirected by an in-repo toolchain file.
        for step in self._steps(publish):
            if "publish-rust-sdk.sh" in step:
                self.assertIn("RUSTUP_TOOLCHAIN: 1.86.0", step)
        # The handed-off .crate is digest-bound before any other use.
        steps = self._steps(publish)
        self.assertIn("uses: actions/download-artifact@", steps[1])
        self.assertIn("artifact-ids: ${{ needs.gate.outputs.gated_artifact_id }}", steps[1])
        self.assertIn("digest-mismatch: error", steps[1])
        self.assertIn("EXPECTED_ARTIFACT_DIGEST: ${{ needs.gate.outputs.gated_artifact_digest }}", steps[2])
        self.assertIn("RUST_SDK_GATED_SHA256: ${{ needs.gate.outputs.gated_artifact_digest }}", publish)
        self.assertIn("RUST_SDK_GATED_CRATE_SHA256: ${{ needs.gate.outputs.crate_sha256 }}", publish)

    def test_gate_and_verify_jobs_build_without_the_credential(self) -> None:
        jobs = self._workflow_jobs()
        self.assertIn("run: ../release-tools/scripts/ci/publish-rust-sdk.sh gate", jobs["gate"])
        self.assertIn("path: gated-package.crate", jobs["gate"])
        self.assertIn("run: ../release-tools/scripts/ci/publish-rust-sdk.sh verify", jobs["verify"])
        self.assertIn("RUST_SDK_PUBLISHED_IN_RUN: ${{ needs.publish.result == 'success' }}", jobs["verify"])
        self.assertIn("needs: [gate, publish]", jobs["verify"])
        self.assertIn("needs.publish.result == 'success' || needs.publish.result == 'skipped'", jobs["verify"])
        self.assertIn("needs: verify", jobs["release"])
        self.assertIn("contents: write", jobs["release"])
        for name in ("gate", "publish", "verify"):
            self.assertNotIn("contents: write", jobs[name])

    def test_github_notes_use_validated_source_after_verification(self) -> None:
        release = self._workflow_jobs()["release"]
        self.assertIn('--release-notes "$RELEASE_TAG"', release)
        self.assertIn('--notes-file "${RUNNER_TEMP}/rust-sdk-release-notes.md"', release)
        self.assertNotIn('--generate-notes', release)
        self.assertLess(release.index("- name: Prepare exact source release notes"),
                        release.index("- name: Create the source GitHub Release"))
        self.assertIsNone(re.search(r"\bcargo (build|test|run|check|package|publish)\b", release))

    @staticmethod
    def _step_run(step: str) -> str:
        """A step's run command, with block lines and continuations joined."""
        match = re.search(r"^        run: (.*)$", step, flags=re.MULTILINE)
        if match is None:
            return ""
        if match.group(1).strip() != "|":
            return match.group(1).strip()
        lines = []
        for line in step[match.end():].splitlines()[1:]:
            if line.strip() and not line.startswith("          "):
                break
            lines.append(line.strip())
        return re.sub(r"\s+", " ", " ".join(lines).replace("\\ ", " ")).strip()

    # F5: the tagged tree is data. Only the maintained entrypoints run with it
    # as the working directory; the installer and other steps never do.
    TAGGED_TREE_ENTRYPOINTS = {
        "gate": {"../release-tools/scripts/ci/publish-rust-sdk.sh gate"},
        "publish": {"../scripts/ci/publish-rust-sdk.sh preflight", "../scripts/ci/publish-rust-sdk.sh publish"},
        "verify": {"../release-tools/scripts/ci/publish-rust-sdk.sh verify"},
        "release": {'python3 -I ../release-tools/scripts/ci/release_package.py --release-notes "$RELEASE_TAG" '
                    '> "${RUNNER_TEMP}/rust-sdk-release-notes.md"'},
    }
    MAINTAINED_INSTALLER = {
        "gate": "release-tools/scripts/ci/install-release-tooling.sh",
        "publish": "scripts/ci/install-release-tooling.sh",
        "verify": "release-tools/scripts/ci/install-release-tooling.sh",
        "release": "release-tools/scripts/ci/install-release-tooling.sh",
    }

    def test_tagged_tree_working_directory_runs_only_maintained_entrypoints(self) -> None:
        workflow = RELEASE_WORKFLOW.read_text(encoding="utf-8")
        # A defaults block would set a working directory no step declares.
        self.assertNotIn("defaults:", workflow)
        jobs = self._workflow_jobs()
        self.assertEqual(set(self.TAGGED_TREE_ENTRYPOINTS), set(jobs))
        for name, job in jobs.items():
            observed = set()
            for step in self._steps(job):
                directories = re.findall(r"^        working-directory: (.*)$", step, flags=re.MULTILINE)
                if not directories:
                    continue
                with self.subTest(job=name, step=step.split("\n", 1)[0]):
                    self.assertEqual(["release-source"], directories)
                    self.assertNotIn("\n        uses: ", step)
                    command = self._step_run(step)
                    self.assertIn(command, self.TAGGED_TREE_ENTRYPOINTS[name])
                    observed.add(command)
            with self.subTest(job=name):
                # Every allowlisted entrypoint is still present: the list matches the workflow.
                self.assertEqual(self.TAGGED_TREE_ENTRYPOINTS[name], observed)
                [installer] = [step for step in self._steps(job) if "install-release-tooling.sh" in step]
                self.assertNotIn("working-directory:", installer)
                self.assertEqual(f'{self.MAINTAINED_INSTALLER[name]} "${{RUNNER_TEMP}}/release-tooling"',
                                 self._step_run(installer))

    def test_tagged_tree_python_runs_isolated(self) -> None:
        publish = PUBLISH.read_text(encoding="utf-8")
        invocations = re.findall(r"\bpython3\b[^\n]*", publish)
        interpreters = [line for line in invocations if not line.startswith("python3 rustc")]
        self.assertGreater(len(interpreters), 5)
        for line in interpreters:
            with self.subTest(line=line):
                self.assertTrue(line.startswith("python3 -I "), line)
        installer = RELEASE_TOOLING_INSTALLER.read_text(encoding="utf-8")
        for line in re.findall(r"^python3\b[^\n]*", installer, flags=re.MULTILINE):
            with self.subTest(line=line):
                self.assertTrue(line.startswith("python3 -I -m "), line)


class ReleaseToolingInstallerTrustTest(unittest.TestCase):
    def test_isolated_tools_use_the_bootstrapped_pip_client(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            command_directory = root / "bin"
            command_directory.mkdir()
            calls_file = root / "calls.jsonl"
            github_path = root / "github-path"
            venv = root / "tools"
            python = command_directory / "python3"
            python.write_text(textwrap.dedent(f"""\
                #!{sys.executable}
                import json, os, pathlib, sys
                arguments = sys.argv[1:]
                with pathlib.Path(os.environ["INSTALLER_CALLS"]).open("a") as handle:
                    handle.write(json.dumps(arguments) + "\\n")
                if arguments[:3] == ["-I", "-m", "venv"]:
                    binary_directory = pathlib.Path(arguments[-1]) / "bin"
                    binary_directory.mkdir(parents=True)
                    bundled = binary_directory / "python"
                    bundled.write_text("#!/bin/sh\\necho unverified-venv-pip >&2\\nexit 42\\n")
                    bundled.chmod(0o755)
                elif arguments[:3] != ["-I", "-m", "pip"]:
                    raise SystemExit("unexpected installer client")
                """), encoding="utf-8")
            python.chmod(0o755)
            result = subprocess.run(
                ["bash", str(RELEASE_TOOLING_INSTALLER), str(venv)],
                env={**os.environ, "PATH": f"{command_directory}:{os.environ['PATH']}",
                     "INSTALLER_CALLS": str(calls_file), "GITHUB_PATH": str(github_path)},
                capture_output=True, text=True, check=False, timeout=10,
            )
            self.assertEqual(0, result.returncode, result.stderr)
            calls = [json.loads(line) for line in calls_file.read_text().splitlines()]
            self.assertEqual(["-I", "-m", "venv", "--without-pip", str(venv)], calls[0])
            self.assertEqual(["-I", "-m", "pip", "--python", str(venv / "bin/python")], calls[1][:5])
            self.assertIn("--require-hashes", calls[1])
            self.assertIn("--only-binary=:all:", calls[1])
            self.assertEqual(str(venv / "bin") + "\n", github_path.read_text())

    def test_working_directory_cannot_shadow_the_venv_module(self) -> None:
        """A venv/ package or stdlib-shadowing module in the caller's working
        directory (the tagged tree, before F5) never runs in the installer."""
        planted = {
            "venv-package": {"venv/__init__.py"},
            "stdlib-shadow": {"shutil.py"},
        }
        for case, files in planted.items():
            with self.subTest(case=case), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                marker = root / "planted-code-ran"
                hostile = root / "tagged-tree"
                for relative in files:
                    path = hostile / relative
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_text(f"open({str(marker)!r}, 'w').write('ran')\n", encoding="utf-8")
                command_directory = root / "bin"
                command_directory.mkdir()
                # The real interpreter creates the venv exactly as invoked; only
                # the networked, hash-locked dependency install is stubbed.
                wrapper = command_directory / "python3"
                wrapper.write_text(textwrap.dedent("""\
                    #!/bin/sh
                    case " $* " in
                        *" -m pip "*) exit 0 ;;
                    esac
                    exec "$INSTALLER_REAL_PYTHON" "$@"
                    """), encoding="utf-8")
                wrapper.chmod(0o755)
                venv = root / "tools"
                github_path = root / "github-path"
                result = subprocess.run(
                    ["bash", str(RELEASE_TOOLING_INSTALLER), str(venv)], cwd=hostile,
                    env={**os.environ, "PATH": f"{command_directory}:{os.environ['PATH']}",
                         "INSTALLER_REAL_PYTHON": sys.executable, "GITHUB_PATH": str(github_path)},
                    capture_output=True, text=True, check=False, timeout=60,
                )
                self.assertFalse(marker.exists(), "working-directory code ran in the installer")
                self.assertEqual(0, result.returncode, result.stderr)
                self.assertTrue((venv / "pyvenv.cfg").is_file(), result.stderr)
                self.assertTrue(os.access(venv / "bin" / "python", os.X_OK))
                self.assertEqual(str(venv / "bin") + "\n", github_path.read_text())


class RegistryHttpsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.temp = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.temp.cleanup)
        cls.certificate = Path(cls.temp.name) / "certificate.pem"
        private_key = Path(cls.temp.name) / "key.pem"
        subprocess.run(
            [
                "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
                "-keyout", str(private_key), "-out", str(cls.certificate),
                "-days", "1", "-subj", "/CN=localhost",
                "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1",
            ],
            check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        cls.http_requests: list[str] = []
        cls.https_requests: list[str] = []

        class Handler(BaseHTTPRequestHandler):
            def handle(self) -> None:
                try:
                    super().handle()
                except ConnectionResetError:
                    # A rejected TLS certificate can close the peer before HTTP.
                    if self.server is not cls.https:
                        raise

            def do_GET(self) -> None:
                requests = cls.http_requests if self.server is cls.http else cls.https_requests
                requests.append(self.path)
                if self.path in {"/redirect-https", "/redirect-http"}:
                    destination = cls.https_url if self.path == "/redirect-https" else cls.http_url
                    self.send_response(302)
                    self.send_header("Location", destination + "/version")
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                elif self.path == "/version":
                    body = b'{"version":"test"}'
                    self.send_response(200)
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                else:
                    self.send_error(404)

            def log_message(self, *_args: object) -> None:
                pass

        cls.http = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.addClassCleanup(cls.http.server_close)
        cls.https = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.addClassCleanup(cls.https.server_close)
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        context.load_cert_chain(cls.certificate, private_key)
        cls.https.socket = context.wrap_socket(cls.https.socket, server_side=True)
        cls.http_url = f"http://127.0.0.1:{cls.http.server_port}"
        cls.https_url = f"https://127.0.0.1:{cls.https.server_port}"
        for server in (cls.http, cls.https):
            threading.Thread(target=server.serve_forever, daemon=True).start()
            cls.addClassCleanup(server.shutdown)

    def setUp(self) -> None:
        self.http_requests.clear()
        self.https_requests.clear()

    def _lookup(
        self, url: str, *, trust_certificate: bool = True,
    ) -> tuple[subprocess.CompletedProcess[str], Path]:
        response = Path(self.temp.name) / f"{self.id()}.json"
        env = os.environ.copy()
        if trust_certificate:
            env["CURL_CA_BUNDLE"] = str(self.certificate)
        else:
            env.pop("CURL_CA_BUNDLE", None)
        env["NO_PROXY"] = "127.0.0.1,localhost"
        result = subprocess.run(
            [
                "bash", "-c",
                'source "$1"; sleep() { :; }; crates_io_version_lookup "$2" "$3" test-agent',
                "bash", str(PUBLISH.with_name("crates-io-publish-lib.sh")), url, str(response),
            ],
            env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=5,
        )
        return result, response

    def test_https_redirect_is_accepted(self) -> None:
        result, response = self._lookup(self.https_url + "/redirect-https")
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual("200", result.stdout.strip())
        self.assertEqual({"version": "test"}, json.loads(response.read_text()))
        self.assertEqual(["/redirect-https", "/version"], self.https_requests)
        self.assertEqual([], self.http_requests)

    def test_untrusted_certificate_is_refused(self) -> None:
        result, _ = self._lookup(self.https_url + "/version", trust_certificate=False)
        self.assertNotEqual(0, result.returncode)
        self.assertIn("certificate", result.stderr.lower())
        self.assertEqual([], self.https_requests)

    def test_initial_http_is_refused_before_any_request(self) -> None:
        result, _ = self._lookup(self.http_url + "/version")
        self.assertNotEqual(0, result.returncode)
        self.assertEqual([], self.http_requests)

    def test_https_redirect_cannot_downgrade_to_http(self) -> None:
        result, _ = self._lookup(self.https_url + "/redirect-http")
        self.assertNotEqual(0, result.returncode)
        self.assertEqual(["/redirect-http"] * 5, self.https_requests)
        self.assertEqual([], self.http_requests)


if __name__ == "__main__":
    unittest.main()
