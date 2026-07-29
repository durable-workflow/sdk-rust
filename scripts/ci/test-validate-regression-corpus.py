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
from unittest import mock


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
REPOSITORY_POLICY = json.loads(
    (Path(__file__).resolve().parents[2] / "regression-corpus-policy.json").read_text(
        encoding="utf-8"
    )
)
CODEC_GUARD = {
    "glob": "src/lib.rs",
    "content_patterns": ["Avro", "avro", "Codec", "codec", "framing"],
}
REPLAY_GUARD = REPOSITORY_POLICY["categories"]["replay"]["guards"][0]
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
            "wire_base64": [
                "wwHioz3/VYAiNw4ECm91dGVyDAIOBAhsZWZ0BAIKcmlnaHQIAngAAAh0YWlsCghkb25lAA==",
                "wwHioz3/VYAiNw4ECHRhaWwKCGRvbmUKb3V0ZXIMAg4ECnJpZ2h0CAJ4CGxlZnQEAgAAAA==",
            ],
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


class ConsumerIsolationTest(unittest.TestCase):
    def test_base_and_candidate_cargo_outputs_are_isolated(self) -> None:
        completed = mock.Mock(returncode=0, stdout="", stderr="")
        with mock.patch.object(
            VALIDATOR.subprocess,
            "run",
            return_value=completed,
        ) as run:
            VALIDATOR._consumer_result(Path("/tmp/corpus/base"), "codec")
            VALIDATOR._consumer_result(Path("/tmp/corpus/candidate"), "codec")

        targets = [call.kwargs["env"]["CARGO_TARGET_DIR"] for call in run.call_args_list]
        self.assertEqual(
            targets,
            ["/tmp/corpus/target-base", "/tmp/corpus/target-candidate"],
        )
        self.assertEqual(len(targets), len(set(targets)))


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

struct WorkflowContext;

impl WorkflowContext {
    fn continue_as_new_command(&self, cursor: usize) -> bool {
        cursor == 1
    }
}

enum RecordedSnapshotValue<T> {
    Unknown,
    Known(T),
}

impl<T: PartialEq> RecordedSnapshotValue<T> {
    fn matches_current(&self, current: &Self) -> bool {
        match self {
            Self::Unknown => true,
            Self::Known(recorded) => matches!(current, Self::Known(value) if value == recorded),
        }
    }
}

struct ActivityRetrySnapshot {
    max_attempts: u64,
}

impl ActivityRetrySnapshot {
    fn matches_current(&self, current: &Self) -> bool {
        self.max_attempts == current.max_attempts
    }
}

fn recorded_activity_retry_snapshot(max_attempts: u64) -> ActivityRetrySnapshot {
    ActivityRetrySnapshot { max_attempts }
}

struct CacheSnapshot {
    generation: u64,
}

impl CacheSnapshot {
    fn matches_current(&self, current: &Self) -> bool {
        self.generation == current.generation
    }
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

    def test_continue_as_new_consumption_without_keyword_is_related(self) -> None:
        source = self.root / "src/lib.rs"
        source.write_text(
            source.read_text(encoding="utf-8").replace("cursor == 1", "cursor > 1"),
            encoding="utf-8",
        )

        self.assertTrue(self._guard_matches(REPLAY_GUARD))

    def test_activity_snapshot_comparison_without_keyword_is_related(self) -> None:
        source = self.root / "src/lib.rs"
        source.write_text(
            source.read_text(encoding="utf-8").replace(
                "self.max_attempts == current.max_attempts",
                "self.max_attempts <= current.max_attempts",
            ),
            encoding="utf-8",
        )

        self.assertTrue(self._guard_matches(REPLAY_GUARD))

    def test_recorded_snapshot_comparison_without_keyword_is_related(self) -> None:
        source = self.root / "src/lib.rs"
        source.write_text(
            source.read_text(encoding="utf-8").replace(
                "Self::Unknown => true",
                "Self::Unknown => false",
            ),
            encoding="utf-8",
        )

        self.assertTrue(self._guard_matches(REPLAY_GUARD))

    def test_recorded_snapshot_decoding_without_keyword_is_related(self) -> None:
        source = self.root / "src/lib.rs"
        source.write_text(
            source.read_text(encoding="utf-8").replace(
                "ActivityRetrySnapshot { max_attempts }",
                "ActivityRetrySnapshot { max_attempts: max_attempts.max(1) }",
            ),
            encoding="utf-8",
        )

        self.assertTrue(self._guard_matches(REPLAY_GUARD))

    def test_same_named_unrelated_comparison_is_not_related(self) -> None:
        source = self.root / "src/lib.rs"
        source.write_text(
            source.read_text(encoding="utf-8").replace(
                "self.generation == current.generation",
                "self.generation <= current.generation",
            ),
            encoding="utf-8",
        )

        self.assertFalse(self._guard_matches(REPLAY_GUARD))

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
        self.consumer = self.root / "tests/replay_regression_corpus.rs"
        self.consumer.write_text(
            "// trusted base replay consumer\n",
            encoding="utf-8",
        )
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

    def test_protocol_version_cannot_satisfy_guarded_replay_growth(self) -> None:
        duplicate = json.loads(json.dumps(REPLAY_FIXTURE))
        duplicate["id"] = "rust-side-effect-version-cold-replay-new-protocol"
        duplicate["protocol_version"] = "1.3"
        (self.fixture.parent / "protocol-version-rewrap.json").write_text(
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

    def test_workflow_input_default_cannot_satisfy_guarded_replay_growth(self) -> None:
        duplicate = json.loads(json.dumps(REPLAY_FIXTURE))
        duplicate["id"] = "rust-side-effect-version-cold-replay-default-input"
        duplicate["workflow"].pop("input")
        (self.fixture.parent / "default-input-rewrap.json").write_text(
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

    def test_ignored_workflow_key_cannot_satisfy_guarded_replay_growth(self) -> None:
        duplicate = json.loads(json.dumps(REPLAY_FIXTURE))
        duplicate["id"] = "rust-side-effect-version-cold-replay-ignored-workflow-key"
        duplicate["workflow"]["consumer_ignored"] = "new evidence"
        (self.fixture.parent / "ignored-workflow-key-rewrap.json").write_text(
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

    def test_already_passing_replay_fixture_rejects_candidate_consumer_trick(self) -> None:
        fixture = json.loads(json.dumps(REPLAY_FIXTURE))
        fixture["id"] = "already-passing-replay-evidence"
        fixture["expected"]["side_effect_callback_calls"] = 42
        (self.fixture.parent / "already-passing.json").write_text(
            json.dumps(fixture),
            encoding="utf-8",
        )
        self.source.write_text(
            "fn replay_commands(history: &[u8]) -> bool { "
            "history.iter().next().is_some() }\n",
            encoding="utf-8",
        )
        self.consumer.write_text(
            "// candidate-only consumer manufactures a failure\n",
            encoding="utf-8",
        )

        def already_passes(
            checkout: Path,
            category: str,
        ) -> VALIDATOR.ConsumerResult:
            self.assertEqual(category, "replay")
            self.assertEqual(
                (checkout / "tests/replay_regression_corpus.rs").read_text(
                    encoding="utf-8"
                ),
                "// trusted base replay consumer\n",
            )
            return VALIDATOR.ConsumerResult(0, "fixture passes")

        with self.assertRaisesRegex(
            VALIDATOR.CorpusError,
            "already passes against the base production implementation",
        ):
            VALIDATOR.validate(
                self.root,
                Path("regression-corpus-policy.json"),
                "HEAD",
                consumer_runner=already_passes,
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
        self.consumer = self.root / "tests/codec_regression_corpus.rs"
        self.consumer.write_text(
            "// trusted base codec consumer\n",
            encoding="utf-8",
        )
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
            consumer_runner=self._consumer_runner,
        )

    @staticmethod
    def _consumer_runner(
        checkout: Path,
        category: str,
    ) -> VALIDATOR.ConsumerResult:
        if category != "codec":
            return VALIDATOR.ConsumerResult(1, f"unexpected category {category}")
        fixtures = list(
            (checkout / "tests/fixtures/codec-regressions").glob("*.json")
        )
        fixed = "bytes.len() > 1" in (checkout / "src/lib.rs").read_text(
            encoding="utf-8"
        )
        return VALIDATOR.ConsumerResult(
            1 if len(fixtures) > 1 and not fixed else 0,
            "simulated official Apache Avro consumer",
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

    def test_consumer_ignored_metadata_cannot_satisfy_guarded_growth(self) -> None:
        duplicate = json.loads(json.dumps(CODEC_FIXTURE))
        duplicate["id"] = "metadata-only-relabel"
        duplicate["protocol"]["codec"] = "renamed-codec"
        duplicate["protocol"]["schema"] = "renamed-schema"
        duplicate["protocol"]["version"] = "999"
        duplicate["framing"]["encoding"] = "renamed-encoding"
        (self.fixture.parent / "metadata-only-relabel.json").write_text(
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

    def test_each_alternate_map_wire_cannot_be_rewrapped_as_new_evidence(self) -> None:
        self.source.write_text(
            self.source.read_text(encoding="utf-8").replace(
                "!bytes.is_empty()", "bytes.len() > 1"
            ),
            encoding="utf-8",
        )
        duplicate = json.loads(json.dumps(CODEC_FIXTURE))
        duplicate["value"] = {
            "type": "map",
            "entries": [
                {
                    "key": "outer",
                    "value": {
                        "type": "array",
                        "items": [
                            {
                                "type": "map",
                                "entries": [
                                    {
                                        "key": "left",
                                        "value": {"type": "long", "value": "1"},
                                    },
                                    {
                                        "key": "right",
                                        "value": {"type": "bytes", "base64": "eA=="},
                                    },
                                ],
                            }
                        ],
                    },
                },
                {
                    "key": "tail",
                    "value": {"type": "string", "value": "done"},
                },
            ],
        }
        path = self.fixture.parent / "rewrapped-alternate-map-wire.json"
        for index, wire in enumerate(
            GOLDEN_FIXTURE["alternate_map_orders"][0]["wire_base64"]
        ):
            with self.subTest(index=index):
                duplicate["id"] = f"rewrapped-alternate-map-wire-{index}"
                duplicate["framing"]["wire_base64"] = wire
                path.write_text(json.dumps(duplicate), encoding="utf-8")
                try:
                    with self.assertRaisesRegex(
                        VALIDATOR.CorpusError,
                        "duplicate semantic fixtures",
                    ):
                        self._validate()
                finally:
                    path.unlink()

    def test_encode_rejection_values_remain_part_of_semantic_identity(self) -> None:
        first = json.loads(json.dumps(CODEC_FIXTURE))
        first["failure_policy"] = {
            "operation": "encode_reject",
            "error": "first stable error",
        }
        first["framing"]["wire_base64"] = None
        second = json.loads(json.dumps(first))
        second["value"]["value"] = "1"

        first_evidence = VALIDATOR._codec_fixture(first, "first.json", "rust")[0]
        second_evidence = VALIDATOR._codec_fixture(second, "second.json", "rust")[0]

        self.assertNotEqual(
            first_evidence.semantic_digest,
            second_evidence.semantic_digest,
        )

    def test_encode_rejection_ignores_unconsumed_wire_metadata(self) -> None:
        first = json.loads(json.dumps(CODEC_FIXTURE))
        first["failure_policy"] = {
            "operation": "encode_reject",
            "error": "stable encode error",
        }
        first["framing"]["wire_base64"] = None
        second = json.loads(json.dumps(first))
        second["framing"]["wire_base64"] = "AQ=="

        first_evidence = VALIDATOR._codec_fixture(first, "first.json", "rust")[0]
        second_evidence = VALIDATOR._codec_fixture(second, "second.json", "rust")[0]

        self.assertEqual(
            first_evidence.semantic_digest,
            second_evidence.semantic_digest,
        )

    def test_stable_rejection_policy_remains_part_of_semantic_identity(self) -> None:
        first = json.loads(json.dumps(CODEC_FIXTURE))
        first["failure_policy"] = {
            "operation": "decode_reject",
            "error": "first stable error",
        }
        second = json.loads(json.dumps(first))
        second["failure_policy"]["error"] = "second stable error"

        first_evidence = VALIDATOR._codec_fixture(first, "first.json", "rust")[0]
        second_evidence = VALIDATOR._codec_fixture(second, "second.json", "rust")[0]

        self.assertNotEqual(
            first_evidence.semantic_digest,
            second_evidence.semantic_digest,
        )

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
        self.manifest.write_text(
            f"{self.fixture.name}\nnew-codec-evidence.json\n",
            encoding="utf-8",
        )
        self.source.write_text(
            self.source.read_text(encoding="utf-8").replace(
                "!bytes.is_empty()", "bytes.len() > 1"
            ),
            encoding="utf-8",
        )
        self.consumer.write_text(
            "// candidate official codec consumer\n",
            encoding="utf-8",
        )
        observed_consumers: list[str] = []

        def observe_consumer(
            checkout: Path,
            category: str,
        ) -> VALIDATOR.ConsumerResult:
            observed_consumers.append(
                (checkout / "tests/codec_regression_corpus.rs").read_text(
                    encoding="utf-8"
                )
            )
            return self._consumer_runner(checkout, category)

        result = VALIDATOR.validate(
            self.root,
            Path("regression-corpus-policy.json"),
            "HEAD",
            consumer_runner=observe_consumer,
        )

        self.assertTrue(result["counts"]["codec"]["related_change"])
        self.assertEqual(result["counts"]["codec"]["current"], 7)
        self.assertEqual(result["counts"]["codec"]["new_fixture_evidence"], 1)
        self.assertEqual(result["negative_controls"]["codec"], 1)
        self.assertEqual(
            observed_consumers,
            ["// trusted base codec consumer\n"] * 4
            + ["// candidate official codec consumer\n"],
        )

    def test_already_passing_codec_fixture_rejects_candidate_consumer_trick(self) -> None:
        fixture = json.loads(json.dumps(CODEC_FIXTURE))
        fixture["id"] = "already-passing-codec-evidence"
        fixture["value"] = {"type": "long", "value": "1"}
        fixture["framing"]["wire_base64"] = "Aw=="
        (self.fixture.parent / "already-passing.json").write_text(
            json.dumps(fixture),
            encoding="utf-8",
        )
        self.source.write_text(
            "fn decode_avro_value_blob(bytes: &[u8]) -> bool { "
            "bytes.first().is_some() }\n",
            encoding="utf-8",
        )
        self.consumer.write_text(
            "// candidate-only consumer manufactures a failure\n",
            encoding="utf-8",
        )

        def already_passes(
            checkout: Path,
            category: str,
        ) -> VALIDATOR.ConsumerResult:
            self.assertEqual(category, "codec")
            self.assertEqual(
                (checkout / "tests/codec_regression_corpus.rs").read_text(
                    encoding="utf-8"
                ),
                "// trusted base codec consumer\n",
            )
            return VALIDATOR.ConsumerResult(0, "fixture passes")

        with self.assertRaisesRegex(
            VALIDATOR.CorpusError,
            "already passes against the base production implementation",
        ):
            VALIDATOR.validate(
                self.root,
                Path("regression-corpus-policy.json"),
                "HEAD",
                consumer_runner=already_passes,
            )


if __name__ == "__main__":
    unittest.main()
