#!/usr/bin/env python3
"""Regression tests for shipped-example base-URL qualification."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
import tempfile
import unittest


def _load_validator():
    path = Path(__file__).with_name("validate-example-base-urls.py")
    spec = importlib.util.spec_from_file_location("validate_example_base_urls", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


VALIDATOR = _load_validator()


class ExampleBaseUrlQualificationTest(unittest.TestCase):
    def test_rejects_sdk_owned_api_suffixes_in_all_endpoint_forms(self) -> None:
        source = "\n".join(
            (
                'Client::new("http://127.0.0.1:8080/api")?;',
                'Client::new("https://runtime.example.test/account/api/")?;',
                'Client::new(r#"https://runtime.example.test/api?region=eu"#)?;',
            )
        )

        self.assertEqual(
            (1, 2, 3),
            tuple(endpoint.line for endpoint in VALIDATOR.invalid_endpoints(source)),
        )

    def test_accepts_server_origins_and_path_prefixed_cloud_runtime_urls(self) -> None:
        source = "\n".join(
            (
                'Client::new("http://127.0.0.1:8080")?;',
                'Client::new("https://cloud.example.test/accounts/acme/runtime")?;',
                'Client::new("https://cloud.example.test/namespaces/api-workers")?;',
            )
        )

        self.assertEqual((), VALIDATOR.invalid_endpoints(source))

    def test_discovers_the_manifest_package_example_targets(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = root / "Cargo.toml"
            first = root / "examples" / "first.rs"
            second = root / "examples" / "second.rs"
            first.parent.mkdir()
            manifest.write_text('[package]\nname = "fixture"\n', encoding="utf-8")
            first.write_text("fn main() {}\n", encoding="utf-8")
            second.write_text("fn main() {}\n", encoding="utf-8")
            metadata = {
                "packages": [
                    {
                        "manifest_path": str(manifest),
                        "targets": [
                            {"name": "fixture", "kind": ["lib"], "src_path": "unused"},
                            {
                                "name": "second",
                                "kind": ["example"],
                                "src_path": str(second),
                            },
                            {
                                "name": "first",
                                "kind": ["example"],
                                "src_path": str(first),
                            },
                        ],
                    }
                ]
            }

            targets = VALIDATOR.example_targets(metadata, root)

        self.assertEqual(("first", "second"), tuple(target.name for target in targets))

    def test_requires_clean_rendered_source_for_every_example_target(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "examples" / "cloud-example.rs"
            source.parent.mkdir()
            source.write_text(
                'fn main() { Client::new("https://cloud.example.test/runtime"); }\n',
                encoding="utf-8",
            )
            target = VALIDATOR.ExampleTarget(name="cloud-example", source=source)
            docs = root / "target" / "doc"
            rendered = docs / "src" / "cloud_example" / "cloud-example.rs.html"
            rendered.parent.mkdir(parents=True)
            rendered.write_text(
                '<span class="string">"https://cloud.example.test/runtime/api"</span>',
                encoding="utf-8",
            )

            with self.assertRaisesRegex(
                VALIDATOR.QualificationError,
                "rendered example contains rejected endpoint",
            ):
                VALIDATOR.qualify((target,), docs)


if __name__ == "__main__":
    unittest.main()
