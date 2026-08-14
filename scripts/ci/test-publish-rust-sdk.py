#!/usr/bin/env python3
"""Focused contract tests for the Rust SDK release entrypoint."""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import tempfile
import textwrap
import tomllib
import unittest


ROOT = Path(__file__).resolve().parents[2]
MANIFEST = ROOT / "Cargo.toml"
PUBLISH = ROOT / "scripts" / "ci" / "publish-rust-sdk.sh"
PACKAGE_VERSION = "2.0.0-rc.31"
PRODUCT_TRAIN = PACKAGE_VERSION
SERVER_VERSIONS = ">=2.0.0-rc.32,<2.0.0"
QUALIFIED_SERVER_VERSION = "2.0.0-rc.32"
SERVER_WORKER_PROTOCOLS = ">=1.2,<2.0"
RELEASE_COMMIT = "0123456789abcdef0123456789abcdef01234567"
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
            import os
            from pathlib import Path
            import sys
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
                archive = target / "package" / f'{package["name"]}-{package["version"]}.crate'
                archive.parent.mkdir(parents=True, exist_ok=True)
                archive.write_bytes(b"local crate")
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
            if command == "status":
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
            import json
            from pathlib import Path
            import sys

            args = sys.argv[1:]
            output = Path(args[args.index("--output") + 1])
            url = args[-1]
            if url.endswith("/download"):
                output.write_bytes(b"published crate")
            elif url.endswith("/{PACKAGE_VERSION}"):
                output.write_text(json.dumps({{"version": {{
                    "num": "{PACKAGE_VERSION}",
                    "checksum": "{CHECKSUM}",
                    "created_at": "2026-07-22T00:00:00Z",
                }}}}), encoding="utf-8")
            elif url.endswith("/durable-workflow"):
                output.write_text(json.dumps({{"crate": {{
                    "repository": "https://github.com/durable-workflow/sdk-rust",
                }}}}), encoding="utf-8")
            else:
                raise SystemExit(f"unexpected curl URL: {{url}}")
            if "--write-out" in args:
                print("200", end="")
            ''',
        )
        self._write_executable(
            "sha256sum",
            f'''
            #!/usr/bin/env python3
            import sys
            print("{CHECKSUM}  " + sys.argv[1])
            ''',
        )
        self._write_executable(
            "tar",
            r"""
            #!/usr/bin/env python3
            import json
            import os
            print(json.dumps({"git": {"sha1": os.environ["MOCK_RELEASE_COMMIT"], "dirty": False}}))
            """,
        )

    def _publish(
        self,
        manifest: Path = MANIFEST,
        environment: dict[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        env = os.environ.copy()
        env.update(
            {
                "PATH": f"{self.bin_dir}{os.pathsep}{env['PATH']}",
                "CARGO_TARGET_DIR": str(self.temp / "target"),
                "MOCK_RELEASE_COMMIT": RELEASE_COMMIT,
                "MOCK_PACKAGE_VERSION": PACKAGE_VERSION,
                "RELEASE_TAG": PACKAGE_VERSION,
                "RUST_SDK_MANIFEST_PATH": str(manifest),
                "RUST_SDK_RELEASE_EVIDENCE_PATH": str(self.evidence),
            }
        )
        env.update(environment or {})
        return subprocess.run(
            ["bash", str(PUBLISH)],
            cwd=ROOT,
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

    def test_manifest_declares_component_and_qualified_baseline(self) -> None:
        package = tomllib.loads(MANIFEST.read_text(encoding="utf-8"))["package"]
        metadata = package["metadata"]["durable-workflow"]
        self.assertEqual(PACKAGE_VERSION, package["version"])
        self.assertEqual(PRODUCT_TRAIN, metadata["product-train"])
        self.assertEqual("protocol-manifests", metadata["compatibility-authority"])
        self.assertEqual(SERVER_VERSIONS, metadata["supported-server-versions"])
        self.assertEqual(QUALIFIED_SERVER_VERSION, metadata["qualified-server-version"])
        self.assertEqual(SERVER_WORKER_PROTOCOLS, metadata["server-worker-protocol-versions"])

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
            SERVER_WORKER_PROTOCOLS,
            evidence["protocol_compatibility"]["server_worker_protocol_versions"],
        )
        self.assertTrue(evidence["registry_verified"])
        self.assertEqual("1.86", evidence["fresh_consumer"]["rust_version"])
        self.assertEqual(
            f'durable-workflow = "={PACKAGE_VERSION}"',
            evidence["fresh_consumer"]["exact_dependency"],
        )
        self.assertTrue(evidence["fresh_consumer"]["fresh_lockfile"])
        self.assertTrue(evidence["fresh_consumer"]["build_verified"])

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


if __name__ == "__main__":
    unittest.main()
