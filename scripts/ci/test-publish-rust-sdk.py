#!/usr/bin/env python3
"""Focused contract tests for the Rust SDK release entrypoint."""

from __future__ import annotations

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import ssl
import shutil
import subprocess
import sys
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


class PublishRustSdkContractTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.temp = Path(self.temp_dir.name)
        self.bin_dir = self.temp / "bin"
        self.bin_dir.mkdir()
        self.evidence = self.temp / "evidence.json"
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
            if command == "-C":
                print(os.environ["MOCK_TOOLING_COMMIT"])
            elif command == "check-ignore":
                raise SystemExit(0 if os.environ.get("MOCK_IGNORED_LOCKFILE") else 1)
            elif command == "status":
                pass
            elif command in {"rev-parse", "rev-list"}:
                print(os.environ["MOCK_RELEASE_COMMIT"])
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
                print(os.environ.get("MOCK_LOOKUP_STATUS", "200"), end="")
            ''',
        )
    def _publish(
        self,
        manifest: Path = MANIFEST,
        environment: dict[str, str] | None = None,
        cwd: Path = ROOT,
    ) -> subprocess.CompletedProcess[str]:
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
                "RUST_SDK_MANIFEST_PATH": str(manifest),
                "RUST_SDK_RELEASE_EVIDENCE_PATH": str(self.evidence),
            }
        )
        env.update(environment or {})
        fixture_env = {
            **env,
            "CARGO_TARGET_DIR": str(self.temp / "registry"),
            "RUST_SDK_CHANGELOG_PATH": str(ROOT / "CHANGELOG.md"),
            "MOCK_FIXTURE_CREATION": "1",
        }
        subprocess.run(
            [str(self.bin_dir / "cargo"), "package", "--manifest-path", str(MANIFEST)],
            env=fixture_env, check=True,
        )
        return subprocess.run(
            ["bash", str(PUBLISH)],
            cwd=cwd,
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )

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
        changelog = self.temp / "CHANGELOG.md"
        changelog.write_text(
            "# Changelog\n\n## 2.0.0-rc.31\n\n- Previous release.\n",
            encoding="utf-8",
        )
        result = self._publish(
            environment={"RUST_SDK_CHANGELOG_PATH": str(changelog)}
        )
        self.assertNotEqual(0, result.returncode)
        evidence = json.loads(self.evidence.read_text(encoding="utf-8"))
        self.assertEqual("local_package_release_identity_mismatch", evidence["reason"])
        self.assertIn("current-version entry", result.stderr)

    def test_release_workflow_only_publishes_planned_dispatches(self) -> None:
        workflow = RELEASE_WORKFLOW.read_text(encoding="utf-8")
        triggers = workflow.split("\njobs:\n", maxsplit=1)[0]
        self.assertIn("workflow_dispatch:", triggers)
        self.assertNotIn("push:", triggers)
        self.assertIn("scripts/ci/publish-rust-sdk.sh", workflow)
        self.assertIn(
            'scripts/ci/install-release-tooling.sh "${RUNNER_TEMP}/release-tooling"',
            workflow,
        )
        self.assertLess(
            workflow.index("Install hash-locked release tooling"),
            workflow.index("CARGO_REGISTRY_TOKEN"),
        )
        self.assertNotIn("secrets.CARGO_REGISTRY_TOKEN", workflow)
        self.assertIn("environment: crates-io", workflow)
        self.assertEqual(1, workflow.count("id-token: write"))
        self.assertIn(
            "uses: rust-lang/crates-io-auth-action@c6f97d42243bad5fab37ca0427f495c86d5b1a18",
            workflow,
        )
        self.assertIn("CARGO_REGISTRY_TOKEN: ${{ steps.crates-auth.outputs.token }}", workflow)
        self.assertLess(workflow.index("id: crates-auth"), workflow.index("CARGO_REGISTRY_TOKEN"))
        self.assertLess(workflow.index("Bootstrap patched pip trust"), workflow.index("Install hash-locked release tooling"))
        self.assertIn("ref: ${{ github.workflow_sha }}", workflow)
        self.assertIn("working-directory: release-source", workflow)
        self.assertIn("run: ../release-tools/scripts/ci/publish-rust-sdk.sh", workflow)
        self.assertIn("path: release-source/rust-sdk-release-evidence.json", workflow)
        self.assertEqual(2, workflow.count("persist-credentials: false"))


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
                if arguments[:2] == ["-m", "venv"]:
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
            self.assertEqual(["-m", "venv", "--without-pip", str(venv)], calls[0])
            self.assertEqual(["-I", "-m", "pip", "--python", str(venv / "bin/python")], calls[1][:5])
            self.assertIn("--require-hashes", calls[1])
            self.assertIn("--only-binary=:all:", calls[1])
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
