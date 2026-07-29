#!/usr/bin/env python3
"""Regression tests for replay and codec change classification."""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import ModuleType


def _load_validator() -> ModuleType:
    path = Path(__file__).with_name("validate-regression-corpus.py")
    spec = importlib.util.spec_from_file_location("validate_regression_corpus", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


VALIDATOR = _load_validator()
CODEC_GUARD = {
    "glob": "src/lib.rs",
    "content_patterns": ["Avro", "avro", "Codec", "codec", "framing"],
}
REPLAY_GUARD = {
    "glob": "src/lib.rs",
    "content_patterns": ["Replay", "replay", "History", "history"],
}
POLICY = {
    "$schema": "https://example.invalid/regression-corpus-policy.json",
    "schema": "durable-workflow.regression-corpus-policy/v1",
    "repository": "sdk-rust",
    "binding": "rust",
    "categories": {
        "codec": {
            "fixtures": [
                {
                    "glob": "schema/avro-value-v1-golden.json",
                    "format": "avro-value-golden-v1",
                },
                {
                    "glob": "tests/fixtures/codec-regressions/*.json",
                    "format": "codec-regression-v1",
                },
            ],
            "guards": [CODEC_GUARD],
        }
    },
}
CODEC_FIXTURE = {
    "$schema": "https://example.invalid/regression-corpus-evidence.json",
    "fixture_schema": "durable-workflow.codec-regression/v1",
    "id": "avro-value-v1-long-zero",
    "protocol": {
        "codec": "avro",
        "schema": "durable_workflow.protocol.Value",
        "version": "1",
        "fingerprint": "e2a33dff55802237",
    },
    "bindings": ["php", "python", "rust"],
    "value": {"type": "long", "value": "0"},
    "framing": {
        "encoding": "avro-single-object",
        "wire_base64": "wwHioz3/VYAiNwQA",
    },
    "failure_policy": {"operation": "round_trip", "error": None},
}
GOLDEN_FIXTURE = {
    "schema": "durable_workflow.protocol.Value",
    "fingerprint": "e2a33dff55802237",
    "cases": [
        {
            "name": "null",
            "kind": "null",
            "wire_base64": "wwHioz3/VYAiNwA=",
        },
        {
            "name": "long_7",
            "kind": "long",
            "value": "7",
            "wire_base64": "wwHioz3/VYAiNwQO",
        },
    ],
    "alternate_map_orders": [
        {
            "name": "alternate",
            "wire_base64": ["AA==", "AQ=="],
        }
    ],
    "malformed_frames": [
        {
            "name": "invalid_base64",
            "error": "invalid_payload_framing",
            "wire_base64": "%%%",
        }
    ],
}
REPLAY_POLICY = {
    "$schema": "https://example.invalid/regression-corpus-policy.json",
    "schema": "durable-workflow.regression-corpus-policy/v1",
    "repository": "sdk-rust",
    "binding": "rust",
    "categories": {
        "replay": {
            "fixtures": [
                {
                    "glob": "tests/fixtures/replay-regressions/*.json",
                    "format": "replay-regression-v1",
                }
            ],
            "guards": [REPLAY_GUARD],
        }
    },
}
REPLAY_FIXTURE = json.loads(
    (
        Path(__file__).resolve().parents[2]
        / "tests/fixtures/replay-regressions/side-effect-version-cold-replay.json"
    ).read_text(encoding="utf-8")
)


class GuardClassificationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="regression-corpus-guard-")
        self.root = Path(self.temporary.name)
        (self.root / "src").mkdir()
        (self.root / "src/lib.rs").write_text(
            """\
fn decode_avro_value_blob(bytes: &[u8]) -> bool {
    bytes.len() < 10
}

fn replay_commands(history: &[u8]) -> bool {
    history.len() < 10
}

fn health_check(enabled: bool) -> bool {
    enabled
}
""",
            encoding="utf-8",
        )
        self._git("init", "--quiet")
        self._git("add", "src/lib.rs")
        self._git(
            "-c",
            "user.name=Corpus Guard Test",
            "-c",
            "user.email=corpus-guard@example.invalid",
            "commit",
            "--quiet",
            "-m",
            "base",
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _git(self, *arguments: str) -> None:
        subprocess.run(
            ["git", *arguments],
            cwd=self.root,
            check=True,
            capture_output=True,
            text=True,
        )

    def _guard_matches(self, guard: dict[str, object] = CODEC_GUARD) -> bool:
        return VALIDATOR._guard_matches(
            self.root,
            "HEAD",
            {"src/lib.rs"},
            guard,
        )

    def test_neutral_edit_inside_codec_function_is_related(self) -> None:
        source = self.root / "src/lib.rs"
        source.write_text(
            source.read_text(encoding="utf-8").replace("bytes.len() < 10", "bytes.len() < 9"),
            encoding="utf-8",
        )

        self.assertTrue(self._guard_matches())

    def test_neutral_edit_inside_replay_function_is_related(self) -> None:
        source = self.root / "src/lib.rs"
        source.write_text(
            source.read_text(encoding="utf-8").replace("history.len() < 10", "history.len() < 9"),
            encoding="utf-8",
        )

        self.assertTrue(self._guard_matches(REPLAY_GUARD))

    def test_edit_inside_unrelated_function_is_not_related(self) -> None:
        source = self.root / "src/lib.rs"
        source.write_text(
            source.read_text(encoding="utf-8").replace(
                "fn health_check(enabled: bool) -> bool {\n    enabled",
                "fn health_check(enabled: bool) -> bool {\n    !enabled",
            ),
            encoding="utf-8",
        )

        self.assertFalse(self._guard_matches())


class ReplaySemanticIdentityTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="regression-corpus-replay-")
        self.root = Path(self.temporary.name)
        self.fixture = (
            self.root
            / "tests/fixtures/replay-regressions/side-effect-version-cold-replay.json"
        )
        self.fixture.parent.mkdir(parents=True)
        self.fixture.write_text(json.dumps(REPLAY_FIXTURE), encoding="utf-8")
        (self.root / "src").mkdir()
        self.source = self.root / "src/lib.rs"
        self.source.write_text(
            "fn replay_commands(history: &[u8]) -> bool { !history.is_empty() }\n",
            encoding="utf-8",
        )
        (self.root / "regression-corpus-policy.json").write_text(
            json.dumps(REPLAY_POLICY),
            encoding="utf-8",
        )
        self._git("init", "--quiet")
        self._git("add", ".")
        self._git(
            "-c",
            "user.name=Replay Corpus Test",
            "-c",
            "user.email=replay-corpus@example.invalid",
            "commit",
            "--quiet",
            "-m",
            "base",
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _git(self, *arguments: str) -> None:
        subprocess.run(
            ["git", *arguments],
            cwd=self.root,
            check=True,
            capture_output=True,
            text=True,
        )

    def test_binding_metadata_cannot_satisfy_guarded_replay_growth(self) -> None:
        duplicate = json.loads(json.dumps(REPLAY_FIXTURE))
        duplicate["id"] = "rust-side-effect-version-cold-replay-with-php-metadata"
        duplicate["bindings"].append("php")
        (self.fixture.parent / "binding-metadata-rewrap.json").write_text(
            json.dumps(duplicate),
            encoding="utf-8",
        )
        self.source.write_text(
            self.source.read_text(encoding="utf-8").replace(
                "!history.is_empty()", "history.len() > 1"
            ),
            encoding="utf-8",
        )

        with self.assertRaisesRegex(
            VALIDATOR.CorpusError,
            "duplicate semantic fixtures",
        ):
            VALIDATOR.validate(
                self.root,
                Path("regression-corpus-policy.json"),
                "HEAD",
            )


class PolicyImmutabilityTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="regression-corpus-policy-")
        self.root = Path(self.temporary.name)
        self.policy = json.loads(json.dumps(POLICY))
        self.fixture = (
            self.root / "tests/fixtures/codec-regressions/avro-value-v1-long-zero.json"
        )
        self.fixture.parent.mkdir(parents=True)
        self.fixture.write_text(json.dumps(CODEC_FIXTURE), encoding="utf-8")
        self.manifest = self.fixture.parent / "manifest.txt"
        self.manifest.write_text(f"{self.fixture.name}\n", encoding="utf-8")
        self.golden_fixture = self.root / "schema/avro-value-v1-golden.json"
        self.golden_fixture.parent.mkdir()
        self.golden_fixture.write_text(json.dumps(GOLDEN_FIXTURE), encoding="utf-8")
        preexisting_fixture = json.loads(json.dumps(CODEC_FIXTURE))
        preexisting_fixture["id"] = "preexisting-unselected-codec-evidence"
        preexisting_fixture["framing"]["wire_base64"] = "AQ=="
        self.preexisting_fixture = (
            self.root / "tests/fixtures/preexisting-codec-regressions/evidence.json"
        )
        self.preexisting_fixture.parent.mkdir(parents=True)
        self.preexisting_fixture.write_text(
            json.dumps(preexisting_fixture),
            encoding="utf-8",
        )
        (self.root / "src").mkdir()
        self.source = self.root / "src/lib.rs"
        self.source.write_text(
            "fn decode_avro_value_blob(bytes: &[u8]) -> bool { !bytes.is_empty() }\n",
            encoding="utf-8",
        )
        self._write_policy()
        self._git("init", "--quiet")
        self._git("add", ".")
        self._git(
            "-c",
            "user.name=Corpus Policy Test",
            "-c",
            "user.email=corpus-policy@example.invalid",
            "commit",
            "--quiet",
            "-m",
            "base",
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _git(self, *arguments: str) -> None:
        subprocess.run(
            ["git", *arguments],
            cwd=self.root,
            check=True,
            capture_output=True,
            text=True,
        )

    def _write_policy(self) -> None:
        (self.root / "regression-corpus-policy.json").write_text(
            json.dumps(self.policy),
            encoding="utf-8",
        )

    def _validate(self) -> dict[str, object]:
        return VALIDATOR.validate(
            self.root,
            Path("regression-corpus-policy.json"),
            "HEAD",
        )

    def test_policy_cannot_hide_deleted_fixture(self) -> None:
        self.policy["categories"]["codec"]["fixtures"].pop()
        self._write_policy()
        self.fixture.unlink()
        self.manifest.write_text("", encoding="utf-8")

        with self.assertRaisesRegex(
            VALIDATOR.CorpusError, "cannot remove or weaken base selectors"
        ):
            self._validate()

    def test_policy_cannot_weaken_guard_patterns(self) -> None:
        self.policy["categories"]["codec"]["guards"][0]["content_patterns"].remove(
            "framing"
        )
        self._write_policy()

        with self.assertRaisesRegex(
            VALIDATOR.CorpusError, "cannot remove or weaken base guard"
        ):
            self._validate()

    def test_policy_can_extend_guard_patterns(self) -> None:
        self.policy["categories"]["codec"]["guards"][0]["content_patterns"].append(
            "wire"
        )
        self._write_policy()

        result = self._validate()

        self.assertEqual(result["status"], "pass")

    def test_unconsumed_codec_fixture_cannot_satisfy_guarded_growth(self) -> None:
        self.policy["categories"]["codec"]["fixtures"].append(
            {
                "glob": "tests/fixtures/additional-codec-regressions/*.json",
                "format": "codec-regression-v1",
            }
        )
        new_fixture = json.loads(json.dumps(CODEC_FIXTURE))
        new_fixture["id"] = "unconsumed-codec-evidence"
        new_fixture["framing"]["wire_base64"] = "Ag=="
        path = self.root / "tests/fixtures/additional-codec-regressions/evidence.json"
        path.parent.mkdir(parents=True)
        path.write_text(json.dumps(new_fixture), encoding="utf-8")
        self.source.write_text(
            self.source.read_text(encoding="utf-8").replace(
                "!bytes.is_empty()", "bytes.len() > 1"
            ),
            encoding="utf-8",
        )
        self._write_policy()

        with self.assertRaisesRegex(
            VALIDATOR.CorpusError,
            "not discovered by an official Rust codec corpus consumer",
        ):
            self._validate()

    def test_selector_expansion_cannot_manufacture_guarded_growth(self) -> None:
        self.policy["categories"]["codec"]["fixtures"].append(
            {
                "glob": "tests/fixtures/preexisting-codec-regressions/*.json",
                "format": "codec-regression-v1",
            }
        )
        self._write_policy()
        self.source.write_text(
            self.source.read_text(encoding="utf-8").replace(
                "!bytes.is_empty()", "bytes.len() > 1"
            ),
            encoding="utf-8",
        )

        with self.assertRaisesRegex(
            VALIDATOR.CorpusError,
            "not discovered by an official Rust codec corpus consumer",
        ):
            self._validate()

    def test_cross_format_rewrap_cannot_satisfy_guarded_growth(self) -> None:
        duplicate = json.loads(json.dumps(CODEC_FIXTURE))
        duplicate["id"] = "rewrapped-golden-long"
        duplicate["value"] = {"type": "long", "value": "7"}
        duplicate["framing"]["wire_base64"] = "wwHioz3/VYAiNwQO"
        (self.fixture.parent / "rewrapped-golden-long.json").write_text(
            json.dumps(duplicate),
            encoding="utf-8",
        )
        self.source.write_text(
            self.source.read_text(encoding="utf-8").replace(
                "!bytes.is_empty()", "bytes.len() > 1"
            ),
            encoding="utf-8",
        )

        with self.assertRaisesRegex(
            VALIDATOR.CorpusError,
            "duplicate semantic fixtures",
        ):
            self._validate()

    def test_equivalent_base64_bytes_cannot_satisfy_guarded_growth(self) -> None:
        duplicate = json.loads(json.dumps(CODEC_FIXTURE))
        duplicate["id"] = "rewrapped-golden-null"
        duplicate["value"] = {"type": "null"}
        duplicate["framing"]["wire_base64"] = "wwHioz3/VYAiNwB="
        (self.fixture.parent / "rewrapped-golden-null.json").write_text(
            json.dumps(duplicate),
            encoding="utf-8",
        )
        self.source.write_text(
            self.source.read_text(encoding="utf-8").replace(
                "!bytes.is_empty()", "bytes.len() > 1"
            ),
            encoding="utf-8",
        )

        with self.assertRaisesRegex(
            VALIDATOR.CorpusError,
            "duplicate semantic fixtures",
        ):
            self._validate()

    def test_guarded_growth_accepts_new_fixture(self) -> None:
        new_fixture = json.loads(json.dumps(CODEC_FIXTURE))
        new_fixture["id"] = "new-codec-evidence"
        new_fixture["framing"]["wire_base64"] = "Ag=="
        (self.fixture.parent / "new-codec-evidence.json").write_text(
            json.dumps(new_fixture),
            encoding="utf-8",
        )
        self.source.write_text(
            self.source.read_text(encoding="utf-8").replace(
                "!bytes.is_empty()", "bytes.len() > 1"
            ),
            encoding="utf-8",
        )

        result = self._validate()

        self.assertTrue(result["counts"]["codec"]["related_change"])
        self.assertEqual(result["counts"]["codec"]["current"], 6)
        self.assertEqual(result["counts"]["codec"]["new_fixture_evidence"], 1)


if __name__ == "__main__":
    unittest.main()
