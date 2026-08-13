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
            "name": "decoded_non_magic_bytes",
            "error": "invalid_payload_framing",
            "wire_base64": "JSUl",
        },
        {
            "name": "empty_blob",
            "error": "invalid_payload_framing",
            "wire_base64": "",
        },
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
        / "tests/fixtures/replay-regressions/side-effect-version-cold-replay-avro.json"
    ).read_text(encoding="utf-8")
)
CAPTURED_ONCE_AVRO = "wwHioz3/VYAiNwoaY2FwdHVyZWQtb25jZQ=="
CAPTURED_TWICE_AVRO = "wwHioz3/VYAiNwocY2FwdHVyZWQtdHdpY2U="


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


class ReplayValueIdentityConsumerTest(unittest.TestCase):
    @staticmethod
    def _responding_consumer(
        *,
        request_id: str | None = None,
        value: object | None = None,
    ):
        def run(*_arguments, **kwargs):
            request = json.loads(
                Path(
                    kwargs["env"][
                        "DURABLE_WORKFLOW_REPLAY_VALUE_IDENTITY_REQUEST"
                    ]
                ).read_text(encoding="utf-8")
            )
            response = {
                "schema": VALIDATOR.REPLAY_VALUE_IDENTITY_SCHEMA,
                "request_id": request["request_id"] if request_id is None else request_id,
                "value": (
                    {"type": "string", "value": "captured-once"}
                    if value is None
                    else value
                ),
            }
            Path(
                kwargs["env"]["DURABLE_WORKFLOW_REPLAY_VALUE_IDENTITY_RESPONSE"]
            ).write_text(json.dumps(response), encoding="utf-8")
            return mock.Mock(returncode=0, stdout="", stderr="")

        return run

    def test_replay_identity_is_obtained_from_the_rust_consumer(self) -> None:
        with mock.patch.object(
            VALIDATOR.subprocess,
            "run",
            side_effect=self._responding_consumer(),
        ) as run:
            identity = VALIDATOR._official_replay_value_identity(
                {"codec": "avro", "blob": CAPTURED_ONCE_AVRO},
                "avro",
                "fixture.result",
            )

        self.assertEqual(
            identity,
            {"type": "string", "value": "captured-once"},
        )
        command = run.call_args.args[0]
        self.assertEqual(
            command[1:6],
            [
                "test",
                "--quiet",
                "--test",
                "replay_value_identity_consumer",
                "canonical_replay_value_uses_only_the_official_avro_consumer",
            ],
        )

    def test_missing_rust_consumer_fails_closed(self) -> None:
        with mock.patch.object(
            VALIDATOR.subprocess,
            "run",
            side_effect=FileNotFoundError("cargo is unavailable"),
        ):
            with self.assertRaisesRegex(
                VALIDATOR.CorpusError,
                "official Rust replay value consumer is unavailable",
            ):
                VALIDATOR._official_replay_value_identity(
                    {"codec": "avro", "blob": CAPTURED_ONCE_AVRO},
                    "avro",
                    "fixture.result",
                )

    def test_rust_consumer_disagreement_fails_closed(self) -> None:
        with mock.patch.object(
            VALIDATOR.subprocess,
            "run",
            side_effect=self._responding_consumer(request_id="another-request"),
        ):
            with self.assertRaisesRegex(
                VALIDATOR.CorpusError,
                "official Rust replay value consumer disagreed with the request",
            ):
                VALIDATOR._official_replay_value_identity(
                    {"codec": "avro", "blob": CAPTURED_ONCE_AVRO},
                    "avro",
                    "fixture.result",
                )

    def test_non_envelope_side_effect_value_fails_closed(self) -> None:
        with mock.patch.object(
            VALIDATOR.subprocess,
            "run",
            side_effect=self._responding_consumer(),
        ) as run:
            with self.assertRaisesRegex(
                VALIDATOR.CorpusError,
                "published payload envelope",
            ):
                VALIDATOR._consumer_replay_value(
                    {"captured": "once"},
                    "avro",
                    "fixture.result",
                )
        run.assert_not_called()

    def test_json_tagged_replay_value_fails_closed(self) -> None:
        with self.assertRaisesRegex(
            VALIDATOR.CorpusError,
            "declares unsupported payload codec 'json'; expected 'avro'",
        ):
            VALIDATOR._consumer_replay_value(
                {"codec": "json", "blob": '"captured-once"'},
                "avro",
                "fixture.result",
            )


class CodecValueIdentityConsumerTest(unittest.TestCase):
    @staticmethod
    def _responding_consumer(
        *,
        request_id: str | None = None,
        value: object | None = None,
    ):
        def run(*_arguments, **kwargs):
            request = json.loads(
                Path(
                    kwargs["env"]["DURABLE_WORKFLOW_CODEC_VALUE_IDENTITY_REQUEST"]
                ).read_text(encoding="utf-8")
            )
            response = {
                "schema": VALIDATOR.CODEC_VALUE_IDENTITY_SCHEMA,
                "request_id": request["request_id"] if request_id is None else request_id,
                "value": (
                    {"type": "long", "value": 0}
                    if value is None
                    else value
                ),
            }
            Path(
                kwargs["env"]["DURABLE_WORKFLOW_CODEC_VALUE_IDENTITY_RESPONSE"]
            ).write_text(json.dumps(response), encoding="utf-8")
            return mock.Mock(returncode=0, stdout="", stderr="")

        return run

    def test_codec_identity_is_obtained_from_the_rust_consumer(self) -> None:
        with mock.patch.object(
            VALIDATOR.subprocess,
            "run",
            side_effect=self._responding_consumer(),
        ) as run:
            identity = VALIDATOR._official_codec_value_identity(
                {"type": "long", "value": "0", "consumer_ignored": True},
                "fixture.value",
            )

        self.assertEqual(identity, {"type": "long", "value": 0})
        command = run.call_args.args[0]
        self.assertEqual(
            command[1:6],
            [
                "test",
                "--quiet",
                "--test",
                "codec_regression_corpus",
                "checked_in_codec_regression_corpus_uses_apache_avro",
            ],
        )

    def test_missing_rust_codec_consumer_fails_closed(self) -> None:
        with mock.patch.object(
            VALIDATOR.subprocess,
            "run",
            side_effect=FileNotFoundError("cargo is unavailable"),
        ):
            with self.assertRaisesRegex(
                VALIDATOR.CorpusError,
                "official Rust codec value consumer is unavailable",
            ):
                VALIDATOR._official_codec_value_identity(
                    {"type": "long", "value": "0"},
                    "fixture.value",
                )

    def test_rust_codec_consumer_disagreement_fails_closed(self) -> None:
        with mock.patch.object(
            VALIDATOR.subprocess,
            "run",
            side_effect=self._responding_consumer(request_id="another-request"),
        ):
            with self.assertRaisesRegex(
                VALIDATOR.CorpusError,
                "official Rust codec value consumer disagreed with the request",
            ):
                VALIDATOR._official_codec_value_identity(
                    {"type": "long", "value": "0"},
                    "fixture.value",
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

    def test_non_codec_test_before_avro_helper_is_not_related(self) -> None:
        source = self.root / "src/lib.rs"
        source.write_text(
            source.read_text(encoding="utf-8")
            + """\

#[cfg(test)]
mod tests {
    fn typed_fidelity_probe() -> AvroValue {
        todo!()
    }
}
""",
            encoding="utf-8",
        )
        self._git("add", "src/lib.rs")
        self._git(
            "-c",
            "user.name=Corpus Guard Test",
            "-c",
            "user.email=corpus-guard@example.invalid",
            "commit",
            "--quiet",
            "-m",
            "add-test-module",
        )
        source.write_text(
            source.read_text(encoding="utf-8").replace(
                "mod tests {\n    fn typed_fidelity_probe() -> AvroValue",
                """\
mod tests {
    #[test]
    fn client_builder_accepts_runtime_prefix() {
        assert!(true);
    }

    fn typed_fidelity_probe() -> AvroValue""",
            ),
            encoding="utf-8",
        )

        self.assertFalse(self._guard_matches())

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

    def test_history_event_type_aliases_are_duplicate_evidence(self) -> None:
        duplicate = json.loads(json.dumps(REPLAY_FIXTURE))
        duplicate["id"] = "rust-side-effect-version-cold-replay-type-alias"
        for event in duplicate["history"]:
            event["type"] = event.pop("event_type")
        (self.fixture.parent / "event-type-alias-rewrap.json").write_text(
            json.dumps(duplicate),
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

    def test_history_sequence_aliases_are_duplicate_evidence(self) -> None:
        duplicate = json.loads(json.dumps(REPLAY_FIXTURE))
        duplicate["id"] = "rust-side-effect-version-cold-replay-sequence-aliases"
        first, second = duplicate["history"]
        first["payload"]["workflow_sequence"] = "01"
        first["payload"].pop("sequence")
        second["workflow_sequence"] = "+2"
        second["payload"].pop("sequence")
        (self.fixture.parent / "sequence-alias-rewrap.json").write_text(
            json.dumps(duplicate),
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

    def test_changed_replay_result_remains_distinct_evidence(self) -> None:
        changed = json.loads(json.dumps(REPLAY_FIXTURE))
        changed["id"] = "rust-side-effect-version-cold-replay-version-three"
        changed["history"][1]["payload"]["version"] = 3
        changed["history"][1]["payload"]["max_supported"] = 3
        changed["command_sequence"][0]["result"]["version"] = 3
        changed["expected"]["result"]["version"] = 3

        original_evidence = VALIDATOR._replay_fixture(
            REPLAY_FIXTURE,
            "original.json",
            "rust",
        )[0]
        changed_evidence = VALIDATOR._replay_fixture(
            changed,
            "changed.json",
            "rust",
        )[0]

        self.assertNotEqual(
            original_evidence.semantic_digest,
            changed_evidence.semantic_digest,
        )

    def test_explicit_avro_side_effect_rewrap_is_duplicate_evidence(self) -> None:
        duplicate = json.loads(json.dumps(REPLAY_FIXTURE))
        duplicate["id"] = "rust-side-effect-version-cold-replay-avro-rewrap"
        duplicate["history"][0]["payload"]["result"] = {
            "codec": "avro",
            "blob": CAPTURED_ONCE_AVRO,
        }
        (self.fixture.parent / "avro-side-effect-rewrap.json").write_text(
            json.dumps(duplicate),
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

    def test_non_envelope_side_effect_value_is_rejected(self) -> None:
        malformed = json.loads(json.dumps(REPLAY_FIXTURE))
        malformed["history"][0]["payload"]["result"] = {"captured": "once"}

        with self.assertRaisesRegex(
            VALIDATOR.CorpusError,
            "published payload envelope",
        ):
            VALIDATOR._replay_fixture(
                malformed,
                "malformed.json",
                "rust",
            )

    def test_changed_avro_side_effect_value_remains_distinct_evidence(self) -> None:
        changed = json.loads(json.dumps(REPLAY_FIXTURE))
        changed["id"] = "rust-side-effect-version-cold-replay-avro-changed"
        changed["history"][0]["payload"]["result"] = {
            "codec": "avro",
            "blob": CAPTURED_TWICE_AVRO,
        }
        changed["command_sequence"][0]["result"]["captured"] = "captured-twice"
        changed["expected"]["result"]["captured"] = "captured-twice"

        original_evidence = VALIDATOR._replay_fixture(
            REPLAY_FIXTURE,
            "original.json",
            "rust",
        )[0]
        changed_evidence = VALIDATOR._replay_fixture(
            changed,
            "changed.json",
            "rust",
        )[0]

        self.assertNotEqual(
            original_evidence.semantic_digest,
            changed_evidence.semantic_digest,
        )

    def test_ignored_history_metadata_is_duplicate_evidence(self) -> None:
        duplicate = json.loads(json.dumps(REPLAY_FIXTURE))
        duplicate["id"] = "rust-side-effect-version-cold-replay-history-metadata"
        for event in duplicate["history"]:
            event["recorded_at"] = "2030-01-01T00:00:00Z"
            event["payload"]["corpus_note"] = "representation-only"
        (self.fixture.parent / "history-metadata-rewrap.json").write_text(
            json.dumps(duplicate),
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

    def test_skipped_history_event_is_duplicate_evidence(self) -> None:
        duplicate = json.loads(json.dumps(REPLAY_FIXTURE))
        duplicate["id"] = "rust-side-effect-version-cold-replay-task-metadata"
        duplicate["history"].insert(
            1,
            {
                "event_type": "WorkflowTaskStarted",
                "payload": {
                    "sequence": 99,
                    "worker_id": "representation-only",
                },
                "recorded_at": "2030-01-01T00:00:00Z",
            },
        )
        (self.fixture.parent / "skipped-history-event-rewrap.json").write_text(
            json.dumps(duplicate),
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

    def test_explicit_fallback_payload_codec_is_duplicate_evidence(self) -> None:
        duplicate = json.loads(json.dumps(REPLAY_FIXTURE))
        duplicate["id"] = "rust-side-effect-version-cold-replay-explicit-codec"
        duplicate["history"][0]["payload"]["payload_codec"] = "avro"
        (self.fixture.parent / "explicit-payload-codec-rewrap.json").write_text(
            json.dumps(duplicate),
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

    def test_side_effect_avro_blob_is_duplicate_evidence(self) -> None:
        duplicate = json.loads(json.dumps(REPLAY_FIXTURE))
        duplicate["id"] = "rust-side-effect-version-cold-replay-legacy-result"
        result = duplicate["history"][0]["payload"]["result"]
        duplicate["history"][0]["payload"]["result"] = result["blob"]
        (self.fixture.parent / "legacy-side-effect-result-rewrap.json").write_text(
            json.dumps(duplicate),
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

    def test_redundant_replay_command_assertions_are_duplicate_evidence(self) -> None:
        duplicate = json.loads(json.dumps(REPLAY_FIXTURE))
        duplicate["id"] = "rust-side-effect-version-cold-replay-nested-commands"
        duplicate.pop("command_sequence")
        (self.fixture.parent / "nested-command-rewrap.json").write_text(
            json.dumps(duplicate),
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

    def test_guarded_growth_accepts_new_replay_scenario(self) -> None:
        fixture = json.loads(json.dumps(REPLAY_FIXTURE))
        fixture["id"] = "rust-continue-as-new-replay"
        fixture["workflow"]["type"] = "corpus.continue-as-new"
        fixture["command_sequence"] = [{"type": "continue_as_new"}]
        fixture["expected"] = {"type": "continue_as_new"}
        new_fixture = self.fixture.parent / "continue-as-new.json"
        new_fixture.write_text(json.dumps(fixture), encoding="utf-8")
        self.source.write_text(
            "fn replay_commands(history: &[u8]) -> bool { history.len() > 1 }\n",
            encoding="utf-8",
        )
        self.consumer.write_text(
            "// candidate consumer registers corpus.continue-as-new\n",
            encoding="utf-8",
        )
        support = self.root / "tests/replay_regression_corpus/continue_as_new.rs"
        support.parent.mkdir()
        support.write_text(
            "// candidate continue-as-new workflow harness\n",
            encoding="utf-8",
        )
        observed_consumers: list[tuple[str, str]] = []

        def run_consumer(
            checkout: Path,
            category: str,
        ) -> VALIDATOR.ConsumerResult:
            self.assertEqual(category, "replay")
            consumer = (checkout / "tests/replay_regression_corpus.rs").read_text(
                encoding="utf-8"
            )
            observed_consumers.append((checkout.name, consumer))
            if not (
                checkout
                / "tests/fixtures/replay-regressions/continue-as-new.json"
            ).is_file():
                return VALIDATOR.ConsumerResult(0, "baseline corpus passes")
            if "registers corpus.continue-as-new" not in consumer:
                return VALIDATOR.ConsumerResult(1, "workflow is not registered")
            if not (
                checkout
                / "tests/replay_regression_corpus/continue_as_new.rs"
            ).is_file():
                return VALIDATOR.ConsumerResult(1, "workflow harness is missing")
            fixed = "history.len() > 1" in (checkout / "src/lib.rs").read_text(
                encoding="utf-8"
            )
            return VALIDATOR.ConsumerResult(
                0 if fixed else 1,
                "simulated continue-as-new replay",
            )

        result = VALIDATOR.validate(
            self.root,
            Path("regression-corpus-policy.json"),
            "HEAD",
            consumer_runner=run_consumer,
        )

        self.assertTrue(result["counts"]["replay"]["related_change"])
        self.assertEqual(result["counts"]["replay"]["current"], 2)
        self.assertEqual(result["counts"]["replay"]["new_fixture_evidence"], 1)
        self.assertEqual(result["negative_controls"]["replay"], 1)
        self.assertEqual(
            observed_consumers,
            [
                ("base", "// trusted base replay consumer\n"),
                (
                    "candidate",
                    "// candidate consumer registers corpus.continue-as-new\n",
                ),
                (
                    "candidate",
                    "// candidate consumer registers corpus.continue-as-new\n",
                ),
                (
                    "base",
                    "// candidate consumer registers corpus.continue-as-new\n",
                ),
                (
                    "candidate",
                    "// candidate consumer registers corpus.continue-as-new\n",
                ),
            ],
        )

    def test_candidate_consumer_cannot_hide_already_passing_replay_fixture(self) -> None:
        fixture = json.loads(json.dumps(REPLAY_FIXTURE))
        fixture["id"] = "already-passing-replay-evidence"
        fixture["expected"]["side_effect_callback_calls"] = 42
        new_fixture = self.fixture.parent / "already-passing.json"
        new_fixture.write_text(
            json.dumps(fixture),
            encoding="utf-8",
        )
        self.source.write_text(
            "fn replay_commands(history: &[u8]) -> bool { "
            "history.iter().next().is_some() }\n",
            encoding="utf-8",
        )
        self.consumer.write_text(
            "// candidate consumer claims base production fails\n",
            encoding="utf-8",
        )
        observed_consumers: list[str] = []

        def already_passes(
            checkout: Path,
            category: str,
        ) -> VALIDATOR.ConsumerResult:
            self.assertEqual(category, "replay")
            consumer = (checkout / "tests/replay_regression_corpus.rs").read_text(
                encoding="utf-8"
            )
            observed_consumers.append(consumer)
            expected_consumer = (
                "// candidate consumer claims base production fails\n"
                if checkout.name == "candidate"
                or (checkout / new_fixture.relative_to(self.root)).is_file()
                else "// trusted base replay consumer\n"
            )
            self.assertEqual(consumer, expected_consumer)
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
        self.assertEqual(
            observed_consumers,
            [
                "// trusted base replay consumer\n",
                "// candidate consumer claims base production fails\n",
                "// candidate consumer claims base production fails\n",
                "// candidate consumer claims base production fails\n",
            ],
        )

    def test_candidate_consumer_failure_cannot_bypass_candidate_pass(self) -> None:
        fixture = json.loads(json.dumps(REPLAY_FIXTURE))
        fixture["id"] = "consumer-forced-replay-failure"
        fixture["expected"]["side_effect_callback_calls"] = 43
        new_fixture = self.fixture.parent / "consumer-forced-failure.json"
        new_fixture.write_text(json.dumps(fixture), encoding="utf-8")
        self.source.write_text(
            "fn replay_commands(history: &[u8]) -> bool { history.len() > 1 }\n",
            encoding="utf-8",
        )
        self.consumer.write_text(
            "// candidate consumer forces every new scenario to fail\n",
            encoding="utf-8",
        )
        observed_consumers: list[str] = []

        def forced_failure(
            checkout: Path,
            category: str,
        ) -> VALIDATOR.ConsumerResult:
            self.assertEqual(category, "replay")
            consumer = (checkout / "tests/replay_regression_corpus.rs").read_text(
                encoding="utf-8"
            )
            observed_consumers.append(consumer)
            if not (checkout / new_fixture.relative_to(self.root)).is_file():
                return VALIDATOR.ConsumerResult(0, "baseline corpus passes")
            if "forces every new scenario to fail" in consumer:
                return VALIDATOR.ConsumerResult(1, "consumer-forced failure")
            return VALIDATOR.ConsumerResult(0, "fixture passes")

        with self.assertRaisesRegex(
            VALIDATOR.CorpusError,
            "did not pass against candidate production",
        ):
            VALIDATOR.validate(
                self.root,
                Path("regression-corpus-policy.json"),
                "HEAD",
                consumer_runner=forced_failure,
            )
        self.assertEqual(
            observed_consumers,
            [
                "// trusted base replay consumer\n",
                "// candidate consumer forces every new scenario to fail\n",
                "// candidate consumer forces every new scenario to fail\n",
            ],
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

    def _set_malformed_wire_base(self, wire: str) -> None:
        fixture = json.loads(json.dumps(GOLDEN_FIXTURE))
        fixture["malformed_frames"][0]["wire_base64"] = wire
        self.golden_fixture.write_text(json.dumps(fixture), encoding="utf-8")
        self._git("add", ".")
        self._git(
            "-c",
            "user.name=Corpus Policy Test",
            "-c",
            "user.email=corpus-policy@example.invalid",
            "commit",
            "--quiet",
            "-m",
            "legacy-malformed-wire",
        )

    def _write_malformed_wire(self, wire: str) -> None:
        fixture = json.loads(json.dumps(GOLDEN_FIXTURE))
        fixture["malformed_frames"][0]["wire_base64"] = wire
        self.golden_fixture.write_text(json.dumps(fixture), encoding="utf-8")

    def _set_malformed_name_base(self, name: str) -> None:
        fixture = json.loads(json.dumps(GOLDEN_FIXTURE))
        fixture["malformed_frames"][0]["name"] = name
        self.golden_fixture.write_text(json.dumps(fixture), encoding="utf-8")
        self._git("add", ".")
        self._git(
            "-c",
            "user.name=Corpus Policy Test",
            "-c",
            "user.email=corpus-policy@example.invalid",
            "commit",
            "--quiet",
            "-m",
            "legacy-malformed-name",
        )

    def _write_malformed_name(self, name: str) -> None:
        fixture = json.loads(json.dumps(GOLDEN_FIXTURE))
        fixture["malformed_frames"][0]["name"] = name
        self.golden_fixture.write_text(json.dumps(fixture), encoding="utf-8")

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

    def test_non_codec_change_in_monolithic_source_does_not_require_growth(self) -> None:
        self.source.write_text(
            self.source.read_text(encoding="utf-8")
            + """\

fn value_as_u64(value: u64) -> u64 {
    value
}

#[cfg(test)]
mod tests {
    fn typed_fidelity_probe() -> AvroValue {
        todo!()
    }
}
""",
            encoding="utf-8",
        )
        self._git("add", "src/lib.rs")
        self._git(
            "-c",
            "user.name=Corpus Policy Test",
            "-c",
            "user.email=corpus-policy@example.invalid",
            "commit",
            "--quiet",
            "-m",
            "monolithic-source-base",
        )
        self.source.write_text(
            self.source.read_text(encoding="utf-8").replace(
                "mod tests {\n    fn typed_fidelity_probe() -> AvroValue",
                """\
mod tests {
    #[test]
    fn client_builder_accepts_runtime_prefix() {
        assert!(true);
    }

    fn typed_fidelity_probe() -> AvroValue""",
            ),
            encoding="utf-8",
        )

        result = self._validate()

        self.assertFalse(result["counts"]["codec"]["related_change"])
        self.assertEqual(
            result["counts"]["codec"]["base"],
            result["counts"]["codec"]["current"],
        )

    def test_codec_implementation_hunk_still_requires_growth(self) -> None:
        self.source.write_text(
            self.source.read_text(encoding="utf-8").replace(
                "!bytes.is_empty()", "bytes.len() > 0"
            ),
            encoding="utf-8",
        )

        with self.assertRaisesRegex(
            VALIDATOR.CorpusError,
            "codec implementation changed but its corpus did not grow",
        ):
            self._validate()

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

    def test_empty_golden_wire_rewrap_cannot_satisfy_guarded_growth(self) -> None:
        duplicate = json.loads(json.dumps(CODEC_FIXTURE))
        duplicate["id"] = "rewrapped-golden-empty-blob"
        duplicate["value"] = {"type": "bytes", "base64": ""}
        duplicate["framing"]["wire_base64"] = ""
        duplicate["failure_policy"] = {
            "operation": "decode_reject",
            "error": "invalid_payload_framing",
        }
        (self.fixture.parent / "rewrapped-golden-empty-blob.json").write_text(
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

    def test_decode_operations_accept_only_canonical_string_wires(self) -> None:
        for operation in ("round_trip", "decode_reject"):
            with self.subTest(operation=operation, wire="empty"):
                fixture = json.loads(json.dumps(CODEC_FIXTURE))
                fixture["framing"]["wire_base64"] = ""
                fixture["failure_policy"] = {
                    "operation": operation,
                    "error": None if operation == "round_trip" else "stable error",
                }

                evidence = VALIDATOR._codec_fixture(
                    fixture,
                    f"{operation}-empty.json",
                    "rust",
                )[0]

                self.assertIsInstance(evidence.semantic_digest, str)

            invalid_wires = {
                "null": None,
                "non-string": 0,
                "invalid-base64": "%%%",
                "noncanonical-base64": "AR==",
            }
            for label, wire in invalid_wires.items():
                with self.subTest(operation=operation, wire=label):
                    fixture = json.loads(json.dumps(CODEC_FIXTURE))
                    fixture["framing"]["wire_base64"] = wire
                    fixture["failure_policy"] = {
                        "operation": operation,
                        "error": None if operation == "round_trip" else "stable error",
                    }

                    with self.assertRaises(VALIDATOR.CorpusError):
                        VALIDATOR._codec_fixture(
                            fixture,
                            f"{operation}-{label}.json",
                            "rust",
                        )

            with self.subTest(operation=operation, wire="omitted"):
                fixture = json.loads(json.dumps(CODEC_FIXTURE))
                fixture["framing"].pop("wire_base64")
                fixture["failure_policy"] = {
                    "operation": operation,
                    "error": None if operation == "round_trip" else "stable error",
                }

                with self.assertRaisesRegex(
                    VALIDATOR.CorpusError,
                    "must include wire_base64",
                ):
                    VALIDATOR._codec_fixture(
                        fixture,
                        f"{operation}-omitted.json",
                        "rust",
                    )

    def test_empty_wire_remains_distinct_from_non_empty_wire(self) -> None:
        empty = json.loads(json.dumps(CODEC_FIXTURE))
        empty["framing"]["wire_base64"] = ""
        empty["failure_policy"] = {
            "operation": "decode_reject",
            "error": "stable error",
        }
        non_empty = json.loads(json.dumps(empty))
        non_empty["framing"]["wire_base64"] = "AQ=="

        empty_evidence = VALIDATOR._codec_fixture(empty, "empty.json", "rust")[0]
        non_empty_evidence = VALIDATOR._codec_fixture(
            non_empty,
            "non-empty.json",
            "rust",
        )[0]

        self.assertNotEqual(
            empty_evidence.semantic_digest,
            non_empty_evidence.semantic_digest,
        )

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
        changed_type = json.loads(json.dumps(first))
        changed_type["value"] = {"type": "boolean", "value": False}

        first_evidence = VALIDATOR._codec_fixture(first, "first.json", "rust")[0]
        second_evidence = VALIDATOR._codec_fixture(second, "second.json", "rust")[0]
        changed_type_evidence = VALIDATOR._codec_fixture(
            changed_type,
            "changed-type.json",
            "rust",
        )[0]

        self.assertNotEqual(
            first_evidence.semantic_digest,
            second_evidence.semantic_digest,
        )
        self.assertNotEqual(
            first_evidence.semantic_digest,
            changed_type_evidence.semantic_digest,
        )

    def test_malformed_encode_rejection_value_fails_closed(self) -> None:
        fixture = json.loads(json.dumps(CODEC_FIXTURE))
        fixture["failure_policy"] = {
            "operation": "encode_reject",
            "error": "stable encode error",
        }
        fixture["framing"]["wire_base64"] = None
        fixture["value"] = {
            "type": "array",
            "items": [{"type": "long"}],
        }

        with self.assertRaisesRegex(
            VALIDATOR.CorpusError,
            "official Rust codec value consumer rejected the value",
        ):
            VALIDATOR._codec_fixture(fixture, "malformed.json", "rust")

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

    def test_ignored_encode_rejection_value_member_is_duplicate_evidence(self) -> None:
        first = json.loads(json.dumps(CODEC_FIXTURE))
        first["id"] = "non-finite-double"
        first["failure_policy"] = {
            "operation": "encode_reject",
            "error": "non_finite_float",
        }
        first["framing"]["wire_base64"] = None
        first["value"] = {"type": "double", "value": "NaN"}
        decorated = json.loads(json.dumps(first))
        decorated["id"] = "decorated-non-finite-double"
        decorated["value"]["consumer_ignored"] = {"note": "metadata only"}
        (self.fixture.parent / "non-finite-double.json").write_text(
            json.dumps(first),
            encoding="utf-8",
        )
        (self.fixture.parent / "decorated-non-finite-double.json").write_text(
            json.dumps(decorated),
            encoding="utf-8",
        )

        with self.assertRaisesRegex(
            VALIDATOR.CorpusError,
            "duplicate semantic fixtures",
        ):
            self._validate()

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
            "is not canonical base64",
        ):
            self._validate()

    def test_malformed_golden_wire_must_be_canonical_base64(self) -> None:
        fixture = json.loads(json.dumps(GOLDEN_FIXTURE))
        fixture["malformed_frames"][0]["wire_base64"] = "%%%"

        with self.assertRaisesRegex(VALIDATOR.CorpusError, "base64"):
            VALIDATOR._avro_golden_fixture(fixture, "golden.json")

    def test_malformed_wire_migration_rejects_different_decoded_bytes(self) -> None:
        self._set_malformed_wire_base("AR==")
        self._write_malformed_wire("Ag==")

        with self.assertRaisesRegex(VALIDATOR.CorpusError, "immutable fixture file"):
            self._validate()

    def test_malformed_wire_migration_accepts_same_decoded_bytes(self) -> None:
        self._set_malformed_wire_base("AR==")
        self._write_malformed_wire("AQ==")

        result = self._validate()

        self.assertEqual(result["counts"]["codec"]["base"], result["counts"]["codec"]["current"])

    def test_malformed_wire_migration_rejects_invalid_base64_repair(self) -> None:
        self._set_malformed_wire_base("%%%")
        self._write_malformed_wire("JSUl")

        with self.assertRaisesRegex(VALIDATOR.CorpusError, "immutable fixture file"):
            self._validate()

    def test_malformed_name_migration_accepts_decoded_behavior_reclassification(self) -> None:
        self._set_malformed_name_base("invalid_base64")
        self._write_malformed_name("decoded_non_magic_bytes")

        result = self._validate()

        self.assertEqual(result["counts"]["codec"]["base"], result["counts"]["codec"]["current"])

    def test_malformed_name_migration_rejects_unrelated_reclassification(self) -> None:
        self._set_malformed_name_base("invalid_base64")
        self._write_malformed_name("unrelated_name")

        with self.assertRaisesRegex(VALIDATOR.CorpusError, "immutable fixture file"):
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
        self.assertEqual(result["counts"]["codec"]["current"], 8)
        self.assertEqual(result["counts"]["codec"]["new_fixture_evidence"], 1)
        self.assertEqual(result["negative_controls"]["codec"], 1)
        self.assertEqual(
            observed_consumers,
            ["// trusted base codec consumer\n"]
            + ["// candidate official codec consumer\n"] * 4,
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
            consumer = (checkout / "tests/codec_regression_corpus.rs").read_text(
                encoding="utf-8"
            )
            expected_consumer = (
                "// candidate-only consumer manufactures a failure\n"
                if checkout.name == "candidate"
                or (checkout / "tests/fixtures/codec-regressions/already-passing.json").is_file()
                else "// trusted base codec consumer\n"
            )
            self.assertEqual(
                consumer,
                expected_consumer,
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
