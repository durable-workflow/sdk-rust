#!/usr/bin/env python3
"""Validate immutable replay and payload-codec regression evidence."""

from __future__ import annotations

import argparse
import base64
import binascii
import fnmatch
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

POLICY_SCHEMA = "durable-workflow.regression-corpus-policy/v1"
CODEC_SCHEMA = "durable-workflow.codec-regression/v1"
REPLAY_SCHEMA = "durable-workflow.replay-regression/v1"
GOLDEN_HISTORY_SCHEMA = "durable-workflow.golden-history.v1"
SUPPORTED_FORMATS = {
    "avro-value-golden-v1",
    "codec-regression-v1",
    "golden-history-v1",
    "replay-regression-v1",
}
SUPPORTED_CATEGORIES = {"codec", "replay"}
SUPPORTED_BINDINGS = {"php", "python", "rust"}
RUST_OFFICIAL_CONSUMER_SELECTORS = {
    "codec": {
        ("schema/avro-value-v1-golden.json", "avro-value-golden-v1"),
        ("tests/fixtures/codec-regressions/*.json", "codec-regression-v1"),
    },
    "replay": {
        ("tests/fixtures/replay-regressions/*.json", "replay-regression-v1"),
    },
}
RUST_OFFICIAL_CONSUMERS = {
    "codec": (
        "tests/codec_regression_corpus.rs",
        "codec_regression_corpus",
        "checked_in_codec_regression_corpus_uses_apache_avro",
    ),
    "replay": (
        "tests/replay_regression_corpus.rs",
        "replay_regression_corpus",
        "checked_in_replay_regression_corpus_uses_official_worker_replay",
    ),
}
RUST_REPLAY_CONSUMER_SUPPORT = "tests/replay_regression_corpus/"
CODEC_FIXTURE_MANIFEST = "tests/fixtures/codec-regressions/manifest.txt"
ZERO_COMMIT = re.compile(r"^0+$")
REPLAY_VALUE_IDENTITY_SCHEMA = "durable-workflow.replay-value-identity/v1"
REPLAY_VALUE_IDENTITY_CONSUMER = (
    "replay_value_identity_consumer",
    "canonical_replay_value_uses_official_avro_consumer",
)
RUST_REPOSITORY_ROOT = Path(__file__).resolve().parents[2]


class CorpusError(RuntimeError):
    """The regression-corpus contract is not satisfied."""


@dataclass(frozen=True)
class Evidence:
    category: str
    identity: str
    path: str
    protocol_version: str
    semantic_digest: str
    supersedes: tuple[str, ...] = ()


@dataclass(frozen=True)
class ConsumerResult:
    returncode: int
    output: str


ConsumerRunner = Callable[[Path, str], ConsumerResult]


def _canonical_digest(value: Any) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _object(value: Any, context: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise CorpusError(f"{context} must be an object")
    return value


def _list(value: Any, context: str, *, nonempty: bool = False) -> Sequence[Any]:
    if not isinstance(value, Sequence) or isinstance(value, str | bytes):
        raise CorpusError(f"{context} must be an array")
    if nonempty and not value:
        raise CorpusError(f"{context} must not be empty")
    return value


def _string(value: Any, context: str) -> str:
    if not isinstance(value, str) or not value:
        raise CorpusError(f"{context} must be a non-empty string")
    return value


def _nullable_string(value: Any, context: str) -> str | None:
    if value is None:
        return None
    return _string(value, context)


def _nullable_wire_string(value: Any, context: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise CorpusError(f"{context} must be a string or null")
    return value


def _unique_strings(value: Any, context: str, *, allowed: set[str] | None = None) -> tuple[str, ...]:
    values = tuple(_string(item, f"{context}[]") for item in _list(value, context, nonempty=True))
    if len(values) != len(set(values)):
        raise CorpusError(f"{context} contains duplicates")
    if allowed is not None and not set(values) <= allowed:
        raise CorpusError(f"{context} contains unsupported values: {sorted(set(values) - allowed)}")
    return values


def _json(content: bytes, path: str) -> Mapping[str, Any]:
    try:
        value = json.loads(content)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise CorpusError(f"{path} is not valid UTF-8 JSON: {error}") from error
    return _object(value, path)


def _canonical_wire_bytes(value: str, context: str) -> bytes:
    try:
        decoded = base64.b64decode(value, validate=True)
    except (binascii.Error, ValueError) as error:
        raise CorpusError(f"{context} is not valid base64") from error
    canonical = base64.b64encode(decoded).decode("ascii")
    if value != canonical:
        raise CorpusError(f"{context} is not canonical base64")
    return decoded


def _wire_semantics(
    value: str,
    context: str,
) -> Mapping[str, str]:
    decoded = _canonical_wire_bytes(value, context)
    canonical = base64.b64encode(decoded).decode("ascii")
    return {"bytes_base64": canonical}


def _canonical_wire_replacement(value: str) -> str | None:
    """Return the only permitted canonical replacement for a legacy wire."""

    try:
        decoded = base64.b64decode(value, validate=True)
    except (binascii.Error, ValueError):
        return None

    canonical = base64.b64encode(decoded).decode("ascii")
    return canonical if canonical != value else None


def _official_replay_value_identity(
    value: Any,
    fallback_codec: str,
    context: str,
) -> Mapping[str, Any]:
    """Ask the Rust consumer to project a replay value onto its typed identity."""

    request_id = _canonical_digest(
        {
            "fallback_codec": fallback_codec,
            "value": value,
        }
    )
    request = {
        "schema": REPLAY_VALUE_IDENTITY_SCHEMA,
        "request_id": request_id,
        "fallback_codec": fallback_codec,
        "value": value,
    }
    test_target, test_name = REPLAY_VALUE_IDENTITY_CONSUMER
    environment = os.environ.copy()
    command = [
        environment.get("CARGO", "cargo"),
        "test",
        "--quiet",
        "--test",
        test_target,
        test_name,
        "--",
        "--exact",
    ]
    with tempfile.TemporaryDirectory(
        prefix="durable-workflow-replay-value-identity-"
    ) as temporary:
        request_path = Path(temporary) / "request.json"
        response_path = Path(temporary) / "response.json"
        request_path.write_text(
            json.dumps(request, ensure_ascii=False, separators=(",", ":")),
            encoding="utf-8",
        )
        environment["DURABLE_WORKFLOW_REPLAY_VALUE_IDENTITY_REQUEST"] = str(
            request_path
        )
        environment["DURABLE_WORKFLOW_REPLAY_VALUE_IDENTITY_RESPONSE"] = str(
            response_path
        )
        try:
            result = subprocess.run(
                command,
                cwd=RUST_REPOSITORY_ROOT,
                env=environment,
                check=False,
                capture_output=True,
                text=True,
                timeout=300,
            )
        except (OSError, subprocess.TimeoutExpired) as error:
            raise CorpusError(
                f"{context} official Rust replay value consumer is unavailable: {error}"
            ) from error
        if result.returncode != 0:
            detail = (result.stderr.strip() or result.stdout.strip())[-4000:]
            suffix = f": {detail}" if detail else ""
            raise CorpusError(
                f"{context} official Rust replay value consumer rejected the value"
                f"{suffix}"
            )
        if not response_path.is_file():
            raise CorpusError(
                f"{context} official Rust replay value consumer is unavailable: "
                "no response was produced"
            )
        try:
            response = json.loads(response_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
            raise CorpusError(
                f"{context} official Rust replay value consumer disagreed: "
                "its response is not valid JSON"
            ) from error

    if (
        not isinstance(response, Mapping)
        or response.get("schema") != REPLAY_VALUE_IDENTITY_SCHEMA
        or response.get("request_id") != request_id
        or not isinstance(response.get("value"), Mapping)
        or not isinstance(response["value"].get("type"), str)
    ):
        raise CorpusError(
            f"{context} official Rust replay value consumer disagreed with the request"
        )
    return response["value"]


def _avro_golden_migration(base_content: bytes, current_content: bytes) -> bool:
    """Allow one-way repairs of legacy malformed-frame wire metadata."""

    try:
        base_document = json.loads(base_content)
        current_document = json.loads(current_content)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return False
    if not isinstance(base_document, dict) or not isinstance(current_document, dict):
        return False
    base_frames = base_document.get("malformed_frames")
    current_frames = current_document.get("malformed_frames")
    if not isinstance(base_frames, list) or not isinstance(current_frames, list):
        return False
    if len(base_frames) != len(current_frames):
        return False

    migrated = False
    for index, (base_frame, current_frame) in enumerate(
        zip(base_frames, current_frames, strict=True)
    ):
        if not isinstance(base_frame, dict) or not isinstance(current_frame, dict):
            return False
        base_wire = base_frame.get("wire_base64")
        current_wire = current_frame.get("wire_base64")
        if base_wire != current_wire:
            if not isinstance(base_wire, str) or not isinstance(current_wire, str):
                return False
            if current_wire != _canonical_wire_replacement(base_wire):
                return False
            try:
                _wire_semantics(
                    current_wire,
                    f"current.malformed_frames[{index}].wire_base64",
                )
            except CorpusError:
                return False
            base_frame["wire_base64"] = current_wire
            migrated = True

        base_name = base_frame.get("name")
        current_name = current_frame.get("name")
        if base_name != current_name:
            if (
                base_name != "invalid_base64"
                or current_name != "decoded_non_magic_bytes"
                or current_wire != "JSUl"
                or base_frame.get("error") != "invalid_payload_framing"
                or current_frame.get("error") != "invalid_payload_framing"
            ):
                return False
            base_frame["name"] = current_name
            migrated = True

    return migrated and base_document == current_document


def _canonical_command_type(value: str) -> str:
    """Normalize runtime command class names to their wire discriminator."""

    words = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", value)
    return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", words).lower()


def _canonical_replay_command(value: Any) -> Any:
    """Normalize the command forms accepted by replay consumers."""

    if not isinstance(value, Mapping):
        return value

    command = dict(value)
    command_type = command.get("command_type")
    if not isinstance(command_type, str) or not command_type:
        return command

    wire_type = _canonical_command_type(command_type)
    declared_type = command.get("type")
    if declared_type is None or declared_type == wire_type:
        command.pop("command_type")
        command["type"] = wire_type
    return command


def _canonical_replay_commands(value: Any) -> Any:
    if not isinstance(value, Sequence) or isinstance(value, str | bytes):
        return value
    return [_canonical_replay_command(command) for command in value]


RUST_RECORDED_EVENT_TYPES = {
    "ActivityScheduled",
    "ActivityStarted",
    "ActivityHeartbeatRecorded",
    "ActivityRetryScheduled",
    "ActivityCompleted",
    "ActivityFailed",
    "ActivityCancelled",
    "ActivityTimedOut",
    "TimerScheduled",
    "TimerCancelled",
    "TimerFired",
    "ChildWorkflowScheduled",
    "ChildRunCompleted",
    "ChildRunFailed",
    "ChildRunCancelled",
    "ChildRunTerminated",
    "SignalWaitOpened",
    "SignalApplied",
    "SideEffectRecorded",
    "VersionMarkerRecorded",
    "WorkflowContinuedAsNew",
}


def _consumer_u64(value: Any) -> int | None:
    """Resolve the integer forms accepted by Rust's value_as_u64 helper."""

    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if 0 <= value <= (1 << 64) - 1 else None
    if not isinstance(value, str) or re.fullmatch(r"\+?[0-9]+", value) is None:
        return None
    try:
        parsed = int(value)
    except ValueError:
        return None
    return parsed if parsed <= (1 << 64) - 1 else None


def _consumer_i32(value: Any, context: str) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or value < -(1 << 31)
        or value > (1 << 31) - 1
    ):
        raise CorpusError(f"{context} must be a signed 32-bit integer")
    return value


def _consumer_replay_value(value: Any, fallback_codec: str, context: str) -> Any:
    """Decode a published payload envelope as the Rust replay consumer does."""

    if isinstance(value, Mapping):
        codec = value.get("codec")
        blob = value.get("blob")
        if not isinstance(codec, str) or not isinstance(blob, str):
            raise CorpusError(
                f"{context} must be a payload blob or published payload envelope"
            )
    elif isinstance(value, str):
        codec = fallback_codec
        blob = value
    else:
        raise CorpusError(
            f"{context} must be a payload blob or published payload envelope"
        )
    if codec == "avro":
        _canonical_wire_bytes(blob, context)
    return _official_replay_value_identity(value, fallback_codec, context)


def _replay_event_sequence(
    event: Mapping[str, Any],
    payload: Mapping[str, Any],
    context: str,
) -> int:
    for source, field in (
        (payload, "sequence"),
        (payload, "workflow_sequence"),
        (event, "sequence"),
        (event, "workflow_sequence"),
    ):
        if field not in source:
            continue
        sequence = _consumer_u64(source[field])
        if sequence is not None and sequence > 0:
            return sequence
        break
    raise CorpusError(f"{context} has no positive workflow sequence")


def _canonical_replay_history(
    value: Any,
    *,
    fallback_codec: str,
) -> Any:
    """Project history onto the recorded commands resolved by Rust replay."""

    if not isinstance(value, Sequence) or isinstance(value, str | bytes):
        return value

    history: list[tuple[int, Mapping[str, Any]]] = []
    for index, raw_event in enumerate(value):
        context = f"replay history[{index}]"
        event = _object(raw_event, context)
        has_event_type = "event_type" in event
        has_type_alias = "type" in event
        if has_event_type and has_type_alias:
            raise CorpusError(f"{context} cannot contain both event_type and type")
        event_type = _string(
            event.get("event_type") if has_event_type else event.get("type"),
            f"{context}.event_type",
        )
        raw_payload = event.get("payload")
        payload = dict(raw_payload) if isinstance(raw_payload, Mapping) else {}

        if event_type not in RUST_RECORDED_EVENT_TYPES:
            continue
        if event_type.startswith("Timer") and (
            payload.get("timer_kind", event.get("timer_kind"))
            in {"condition_timeout", "signal_timeout"}
        ):
            continue

        sequence = _replay_event_sequence(event, payload, context)
        if event_type == "SideEffectRecorded":
            result = payload.get("result")
            if result is None:
                raise CorpusError(f"{context}.payload.result is required")
            codec = payload.get("payload_codec")
            if not isinstance(codec, str):
                codec = fallback_codec
            canonical_event: Mapping[str, Any] = {
                "kind": "side_effect",
                "sequence": sequence,
                "value": _consumer_replay_value(
                    result,
                    codec,
                    f"{context}.payload.result",
                ),
            }
        elif event_type == "VersionMarkerRecorded":
            change_id = _string(
                payload.get("change_id"),
                f"{context}.payload.change_id",
            )
            version = _consumer_i32(
                payload.get("version"),
                f"{context}.payload.version",
            )
            min_supported = _consumer_i32(
                payload.get("min_supported"),
                f"{context}.payload.min_supported",
            )
            max_supported = _consumer_i32(
                payload.get("max_supported"),
                f"{context}.payload.max_supported",
            )
            if not min_supported <= version <= max_supported:
                raise CorpusError(
                    f"{context}.payload must satisfy "
                    "min_supported <= version <= max_supported"
                )
            canonical_event = {
                "kind": "version_marker",
                "sequence": sequence,
                "change_id": change_id,
                "version": version,
            }
        elif event_type == "WorkflowContinuedAsNew":
            canonical_event = {
                "kind": "continue_as_new",
                "sequence": sequence,
            }
        else:
            payload.pop("sequence", None)
            payload.pop("workflow_sequence", None)
            canonical_event = {
                "kind": event_type,
                "sequence": sequence,
                "payload": payload,
            }
        history.append((sequence, canonical_event))
    return [event for _, event in sorted(history, key=lambda item: item[0])]


def _merge_replay_assertions(left: Any, right: Any, context: str) -> Any:
    """Merge two compatible partial assertions over the same replay output."""

    if isinstance(left, Mapping) and isinstance(right, Mapping):
        merged = dict(left)
        for key, value in right.items():
            if key in merged:
                merged[key] = _merge_replay_assertions(
                    merged[key],
                    value,
                    f"{context}.{key}",
                )
            else:
                merged[key] = value
        return merged

    if (
        isinstance(left, Sequence)
        and not isinstance(left, str | bytes)
        and isinstance(right, Sequence)
        and not isinstance(right, str | bytes)
    ):
        if len(left) != len(right):
            raise CorpusError(f"replay command assertions conflict at {context}")
        return [
            _merge_replay_assertions(left_item, right_item, f"{context}[{index}]")
            for index, (left_item, right_item) in enumerate(
                zip(left, right, strict=True)
            )
        ]

    if left != right:
        raise CorpusError(f"replay command assertions conflict at {context}")
    return left


def _canonical_executed_commands(
    command_sequence: Any,
    expected: Mapping[str, Any],
) -> Any:
    """Collapse every consumer-supported command assertion onto one output."""

    executed_commands = (
        _canonical_replay_commands(command_sequence)
        if command_sequence is not None
        else None
    )
    expected_sequence = expected.get("command_sequence")
    if expected_sequence is not None:
        canonical_expected = _canonical_replay_commands(expected_sequence)
        executed_commands = (
            canonical_expected
            if executed_commands is None
            else _merge_replay_assertions(
                executed_commands,
                canonical_expected,
                "command_sequence",
            )
        )

    first_command = {
        key: value for key, value in expected.items() if key != "command_sequence"
    }
    if first_command:
        canonical_first = _canonical_replay_command(first_command)
        if executed_commands is None:
            executed_commands = [canonical_first]
        elif (
            not isinstance(executed_commands, Sequence)
            or isinstance(executed_commands, str | bytes)
            or len(executed_commands) != 1
        ):
            raise CorpusError(
                "flattened expected command requires exactly one executed command"
            )
        else:
            executed_commands = [
                _merge_replay_assertions(
                    executed_commands[0],
                    canonical_first,
                    "command_sequence[0]",
                )
            ]

    return executed_commands


def _replay_semantic(
    *,
    workflow_type: str,
    workflow_input: Any,
    history: Any,
    command_sequence: Any,
    expected: Mapping[str, Any],
    fallback_codec: str,
) -> Mapping[str, Any]:
    """Project every replay representation onto consumer-executed values."""

    return {
        "workflow": {"type": workflow_type, "input": workflow_input},
        "history": _canonical_replay_history(
            history,
            fallback_codec=fallback_codec,
        ),
        "executed_commands": _canonical_executed_commands(
            command_sequence,
            expected,
        ),
    }


def _fixture_evidence(
    *,
    category: str,
    identity: str,
    path: str,
    protocol_version: str,
    semantic_value: Any,
    supersedes: tuple[str, ...] = (),
) -> Evidence:
    return Evidence(
        category=category,
        identity=identity,
        path=path,
        protocol_version=protocol_version,
        semantic_digest=_canonical_digest(semantic_value),
        supersedes=supersedes,
    )


def _codec_semantics(
    *,
    value: Mapping[str, Any],
    wire: Mapping[str, str] | None,
    operation: str,
    error: str | None,
) -> Mapping[str, Any]:
    """Project a fixture onto behavior asserted by the official Rust consumers."""
    return {
        "value": value if operation == "encode_reject" else None,
        "framing": {
            "wire": wire if operation in {"round_trip", "decode_reject"} else None,
        },
        "failure_policy": {"operation": operation, "error": error},
    }


def _codec_fixture(document: Mapping[str, Any], path: str, binding: str | None) -> list[Evidence]:
    _string(document.get("$schema"), f"{path}.$schema")
    if document.get("fixture_schema") != CODEC_SCHEMA:
        raise CorpusError(f"{path} must declare fixture_schema={CODEC_SCHEMA}")
    identity = _string(document.get("id"), f"{path}.id")
    protocol = _object(document.get("protocol"), f"{path}.protocol")
    _string(protocol.get("codec"), f"{path}.protocol.codec")
    _string(protocol.get("schema"), f"{path}.protocol.schema")
    version = _string(protocol.get("version"), f"{path}.protocol.version")
    _nullable_string(protocol.get("fingerprint"), f"{path}.protocol.fingerprint")
    bindings = _unique_strings(
        document.get("bindings"),
        f"{path}.bindings",
        allowed=SUPPORTED_BINDINGS,
    )
    if binding is not None and binding not in bindings:
        raise CorpusError(f"{path} does not name this repository's {binding} binding")

    value = _object(document.get("value"), f"{path}.value")
    _string(value.get("type"), f"{path}.value.type")
    framing = _object(document.get("framing"), f"{path}.framing")
    _string(framing.get("encoding"), f"{path}.framing.encoding")
    wire = _nullable_wire_string(
        framing.get("wire_base64"),
        f"{path}.framing.wire_base64",
    )
    policy = _object(document.get("failure_policy"), f"{path}.failure_policy")
    operation = _string(policy.get("operation"), f"{path}.failure_policy.operation")
    if operation not in {"round_trip", "decode_reject", "encode_reject"}:
        raise CorpusError(f"{path}.failure_policy.operation is unsupported")
    error = _nullable_string(policy.get("error"), f"{path}.failure_policy.error")
    if operation in {"round_trip", "decode_reject"} and wire is None:
        raise CorpusError(f"{path} must include wire_base64 for {operation}")
    if operation == "round_trip" and error is not None:
        raise CorpusError(f"{path} round-trip evidence cannot declare an error")
    if operation != "round_trip" and error is None:
        raise CorpusError(f"{path} rejection evidence must declare its stable error policy")
    semantic_wire = (
        _wire_semantics(wire, f"{path}.framing.wire_base64")
        if wire is not None
        else None
    )

    supersedes = tuple(
        _string(item, f"{path}.supersedes[]")
        for item in _list(document.get("supersedes", []), f"{path}.supersedes")
    )
    if len(supersedes) != len(set(supersedes)) or identity in supersedes:
        raise CorpusError(f"{path}.supersedes is invalid")
    semantic = _codec_semantics(
        value=value,
        wire=semantic_wire,
        operation=operation,
        error=error,
    )
    return [
        _fixture_evidence(
            category="codec",
            identity=identity,
            path=path,
            protocol_version=version,
            semantic_value=semantic,
            supersedes=supersedes,
        )
    ]


def _replay_fixture(document: Mapping[str, Any], path: str, binding: str | None) -> list[Evidence]:
    _string(document.get("$schema"), f"{path}.$schema")
    if document.get("fixture_schema") != REPLAY_SCHEMA:
        raise CorpusError(f"{path} must declare fixture_schema={REPLAY_SCHEMA}")
    identity = _string(document.get("id"), f"{path}.id")
    protocol_version = _string(document.get("protocol_version"), f"{path}.protocol_version")
    bindings = _unique_strings(
        document.get("bindings"),
        f"{path}.bindings",
        allowed=SUPPORTED_BINDINGS,
    )
    if binding is not None and binding not in bindings:
        raise CorpusError(f"{path} does not name this repository's {binding} binding")
    workflow = _object(document.get("workflow"), f"{path}.workflow")
    workflow_type = _string(workflow.get("type"), f"{path}.workflow.type")
    workflow_input = workflow.get("input", [])
    _list(workflow_input, f"{path}.workflow.input")
    declared_history = document.get("history")
    commands = document.get("command_sequence")
    if declared_history is None and commands is None:
        raise CorpusError(f"{path} must include history or command_sequence")
    if declared_history is not None:
        _list(declared_history, f"{path}.history", nonempty=True)
    if commands is not None:
        _list(commands, f"{path}.command_sequence", nonempty=True)
    history = declared_history if declared_history is not None else []
    expected = _object(document.get("expected"), f"{path}.expected")
    if not expected:
        raise CorpusError(f"{path}.expected must not be empty")
    supersedes = tuple(
        _string(item, f"{path}.supersedes[]")
        for item in _list(document.get("supersedes", []), f"{path}.supersedes")
    )
    if len(supersedes) != len(set(supersedes)) or identity in supersedes:
        raise CorpusError(f"{path}.supersedes is invalid")
    # Keep identity aligned with effective values that execute_fixture consumes
    # or asserts. Protocol version, bindings, and extra workflow declarations
    # are validated metadata or ignored by the official replay path.
    semantic = _replay_semantic(
        workflow_type=workflow_type,
        workflow_input=workflow_input,
        history=history,
        command_sequence=commands,
        expected=expected,
        fallback_codec="json",
    )
    return [
        _fixture_evidence(
            category="replay",
            identity=identity,
            path=path,
            protocol_version=protocol_version,
            semantic_value=semantic,
            supersedes=supersedes,
        )
    ]


def _avro_golden_fixture(document: Mapping[str, Any], path: str) -> list[Evidence]:
    _string(document.get("schema"), f"{path}.schema")
    _string(document.get("fingerprint"), f"{path}.fingerprint")
    identity_version = "avro-value-v1"
    protocol_version = "1"
    evidence: list[Evidence] = []
    sections = {
        "case": _list(document.get("cases"), f"{path}.cases", nonempty=True),
        "malformed": _list(document.get("malformed_frames"), f"{path}.malformed_frames", nonempty=True),
        "alternate": _list(document.get("alternate_map_orders"), f"{path}.alternate_map_orders", nonempty=True),
    }
    for section, entries in sections.items():
        for index, raw_entry in enumerate(entries):
            entry = _object(raw_entry, f"{path}.{section}[{index}]")
            name = _string(entry.get("name"), f"{path}.{section}[{index}].name")
            wire = entry.get("wire_base64")
            if section == "alternate":
                semantic_wires = [
                    _wire_semantics(
                        wire_value,
                        f"{path}.{section}[{index}].wire_base64[]",
                    )
                    for wire_value in _unique_strings(
                        wire,
                        f"{path}.{section}[{index}].wire_base64",
                    )
                ]
            elif section == "case":
                wire_value = _string(wire, f"{path}.{section}[{index}].wire_base64")
                semantic_wires = [
                    _wire_semantics(
                        wire_value,
                        f"{path}.{section}[{index}].wire_base64",
                    )
                ]
            elif not isinstance(wire, str):
                raise CorpusError(f"{path}.{section}[{index}].wire_base64 must be a string")
            else:
                semantic_wires = [
                    _wire_semantics(
                        wire,
                        f"{path}.{section}[{index}].wire_base64",
                    )
                ]
            operation = "decode_reject" if section == "malformed" else "round_trip"
            error = entry.get("error") if section == "malformed" else None
            for wire_index, semantic_wire in enumerate(semantic_wires):
                identity = f"{identity_version}:{section}:{name}"
                if section == "alternate":
                    identity = f"{identity}:{wire_index}"
                evidence.append(
                    _fixture_evidence(
                        category="codec",
                        identity=identity,
                        path=path,
                        protocol_version=protocol_version,
                        semantic_value=_codec_semantics(
                            value={},
                            wire=semantic_wire,
                            operation=operation,
                            error=error,
                        ),
                    )
                )
    return evidence


def _golden_history_fixture(
    document: Mapping[str, Any],
    path: str,
    *,
    require_single_case: bool,
) -> list[Evidence]:
    if document.get("fixture_schema") != GOLDEN_HISTORY_SCHEMA:
        raise CorpusError(f"{path} must declare fixture_schema={GOLDEN_HISTORY_SCHEMA}")
    source = _object(document.get("source"), f"{path}.source")
    runtime = _string(source.get("runtime"), f"{path}.source.runtime")
    version = _string(source.get("version"), f"{path}.source.version")
    protocol_version = _string(
        source.get("worker_protocol_version"),
        f"{path}.source.worker_protocol_version",
    )
    cases = _list(document.get("cases"), f"{path}.cases", nonempty=True)
    if require_single_case and len(cases) != 1:
        raise CorpusError(f"new golden-history fixture {path} must contain exactly one minimal case")
    evidence: list[Evidence] = []
    for index, raw_case in enumerate(cases):
        case = _object(raw_case, f"{path}.cases[{index}]")
        name = _string(case.get("name"), f"{path}.cases[{index}].name")
        history = _list(case.get("history"), f"{path}.cases[{index}].history", nonempty=True)
        expected = case.get("expected", case.get("expected_state"))
        _object(expected, f"{path}.cases[{index}].expected")
        workflow_type = case.get("workflow_type", case.get("scenario"))
        _string(workflow_type, f"{path}.cases[{index}].workflow identity")
        semantic = _replay_semantic(
            workflow_type=workflow_type,
            workflow_input=case.get("start_input", []),
            history=history,
            command_sequence=case.get("command_sequence"),
            expected=expected,
            fallback_codec="json",
        )
        evidence.append(
            _fixture_evidence(
                category="replay",
                identity=f"{runtime}@{version}:{name}",
                path=path,
                protocol_version=protocol_version,
                semantic_value=semantic,
            )
        )
    return evidence


def _run(command: Sequence[str], root: Path, *, check: bool = True) -> str:
    result = subprocess.run(
        command,
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
    )
    if check and result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise CorpusError(f"{' '.join(command)} failed: {detail}")
    return result.stdout


def _policy(document: Mapping[str, Any], path: str) -> Mapping[str, Any]:
    _string(document.get("$schema"), f"{path}.$schema")
    if document.get("schema") != POLICY_SCHEMA:
        raise CorpusError(f"{path} must declare schema={POLICY_SCHEMA}")
    _string(document.get("repository"), f"{path}.repository")
    binding = document.get("binding")
    if binding is not None and binding not in SUPPORTED_BINDINGS:
        raise CorpusError(f"{path}.binding is unsupported")
    categories = _object(document.get("categories"), f"{path}.categories")
    if not categories or not set(categories) <= SUPPORTED_CATEGORIES:
        raise CorpusError(f"{path}.categories must contain only replay and/or codec")
    for name, raw_category in categories.items():
        category = _object(raw_category, f"{path}.categories.{name}")
        fixtures = _list(category.get("fixtures"), f"{path}.categories.{name}.fixtures", nonempty=True)
        for index, raw_fixture in enumerate(fixtures):
            fixture = _object(raw_fixture, f"{path}.categories.{name}.fixtures[{index}]")
            fixture_glob = _string(
                fixture.get("glob"),
                f"{path}.categories.{name}.fixtures[{index}].glob",
            )
            fixture_format = _string(
                fixture.get("format"),
                f"{path}.categories.{name}.fixtures[{index}].format",
            )
            if fixture_format not in SUPPORTED_FORMATS:
                raise CorpusError(f"{path}.categories.{name}.fixtures[{index}].format is unsupported")
            if not fixture_format.startswith(name) and not (
                name == "codec" and fixture_format == "avro-value-golden-v1"
            ) and not (name == "replay" and fixture_format == "golden-history-v1"):
                raise CorpusError(f"{path}.categories.{name} contains a fixture for another category")
            if binding == "rust" and (
                fixture_glob,
                fixture_format,
            ) not in RUST_OFFICIAL_CONSUMER_SELECTORS[name]:
                raise CorpusError(
                    f"{path}.categories.{name}.fixtures[{index}] is not discovered "
                    f"by an official Rust {name} corpus consumer"
                )
        guards = _list(category.get("guards"), f"{path}.categories.{name}.guards", nonempty=True)
        for index, raw_guard in enumerate(guards):
            guard = _object(raw_guard, f"{path}.categories.{name}.guards[{index}]")
            _string(guard.get("glob"), f"{path}.categories.{name}.guards[{index}].glob")
            patterns = guard.get("content_patterns")
            if patterns is not None:
                for pattern in _unique_strings(
                    patterns,
                    f"{path}.categories.{name}.guards[{index}].content_patterns",
                ):
                    try:
                        re.compile(pattern)
                    except re.error as error:
                        raise CorpusError(f"invalid guard regex {pattern!r}: {error}") from error
    return document


def _fixture_selector(raw_fixture: Any) -> tuple[str, str]:
    fixture = _object(raw_fixture, "fixture")
    return (
        _string(fixture.get("glob"), "fixture.glob"),
        _string(fixture.get("format"), "fixture.format"),
    )


def _guard_selector(raw_guard: Any) -> tuple[str, tuple[str, ...] | None]:
    guard = _object(raw_guard, "guard")
    patterns = guard.get("content_patterns")
    return (
        _string(guard.get("glob"), "guard.glob"),
        (
            tuple(sorted(_unique_strings(patterns, "guard.content_patterns")))
            if patterns is not None
            else None
        ),
    )


def _preserve_base_policy(
    base_policy: Mapping[str, Any],
    current_policy: Mapping[str, Any],
    path: str,
) -> None:
    if current_policy.get("repository") != base_policy.get("repository"):
        raise CorpusError(f"{path}.repository cannot change")
    base_binding = base_policy.get("binding")
    if base_binding is not None and current_policy.get("binding") != base_binding:
        raise CorpusError(f"{path}.binding cannot be weakened or changed")

    base_categories = _object(base_policy["categories"], "base categories")
    current_categories = _object(current_policy["categories"], "current categories")
    for name, raw_base_category in base_categories.items():
        if name not in current_categories:
            raise CorpusError(f"{path}.categories.{name} cannot be removed")
        base_category = _object(raw_base_category, f"base categories.{name}")
        current_category = _object(
            current_categories[name], f"current categories.{name}"
        )

        base_fixtures = {
            _fixture_selector(fixture)
            for fixture in _list(
                base_category["fixtures"], f"base categories.{name}.fixtures"
            )
        }
        current_fixtures = {
            _fixture_selector(fixture)
            for fixture in _list(
                current_category["fixtures"], f"current categories.{name}.fixtures"
            )
        }
        removed_fixtures = sorted(base_fixtures - current_fixtures)
        if removed_fixtures:
            raise CorpusError(
                f"{path}.categories.{name}.fixtures cannot remove or weaken base selectors: "
                f"{removed_fixtures}"
            )

        current_guards = [
            _guard_selector(guard)
            for guard in _list(
                current_category["guards"], f"current categories.{name}.guards"
            )
        ]
        for base_guard in (
            _guard_selector(guard)
            for guard in _list(
                base_category["guards"], f"base categories.{name}.guards"
            )
        ):
            base_glob, base_patterns = base_guard
            preserved = any(
                current_glob == base_glob
                and (
                    current_patterns is None
                    or (
                        base_patterns is not None
                        and set(base_patterns) <= set(current_patterns)
                    )
                )
                for current_glob, current_patterns in current_guards
            )
            if not preserved:
                raise CorpusError(
                    f"{path}.categories.{name}.guards cannot remove or weaken base guard "
                    f"{base_guard}"
                )


def _tracked_worktree_files(root: Path) -> dict[str, bytes]:
    paths = _run(
        ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
        root,
    ).split("\0")
    return {
        path: (root / path).read_bytes()
        for path in paths
        if path and (root / path).is_file()
    }


def _ref_files(root: Path, ref: str) -> dict[str, bytes]:
    paths = _run(["git", "ls-tree", "-r", "--name-only", "-z", ref], root).split("\0")
    return {
        path: _run(["git", "show", f"{ref}:{path}"], root).encode()
        for path in paths
        if path
    }


def _matches(path: str, pattern: str) -> bool:
    return fnmatch.fnmatchcase(path, pattern)


def _inventory(
    policy: Mapping[str, Any],
    files: Mapping[str, bytes],
    *,
    new_paths: set[str] | None = None,
) -> list[Evidence]:
    binding = policy.get("binding")
    evidence: list[Evidence] = []
    selected_paths: set[str] = set()
    for category_name, raw_category in _object(policy["categories"], "categories").items():
        category = _object(raw_category, f"categories.{category_name}")
        for raw_fixture in _list(category["fixtures"], f"categories.{category_name}.fixtures"):
            fixture = _object(raw_fixture, f"categories.{category_name}.fixtures[]")
            pattern = _string(fixture["glob"], "fixture.glob")
            fixture_format = _string(fixture["format"], "fixture.format")
            for path in sorted(candidate for candidate in files if _matches(candidate, pattern)):
                if path in selected_paths:
                    raise CorpusError(f"fixture path {path} is selected more than once")
                selected_paths.add(path)
                document = _json(files[path], path)
                if fixture_format == "codec-regression-v1":
                    parsed = _codec_fixture(document, path, binding if isinstance(binding, str) else None)
                elif fixture_format == "replay-regression-v1":
                    parsed = _replay_fixture(document, path, binding if isinstance(binding, str) else None)
                elif fixture_format == "avro-value-golden-v1":
                    parsed = _avro_golden_fixture(document, path)
                else:
                    parsed = _golden_history_fixture(
                        document,
                        path,
                        require_single_case=new_paths is not None and path in new_paths,
                    )
                if any(item.category != category_name for item in parsed):
                    raise CorpusError(f"{path} produced evidence for the wrong category")
                evidence.extend(parsed)

    identities = Counter(item.identity for item in evidence)
    repeated_identities = sorted(identity for identity, count in identities.items() if count > 1)
    if repeated_identities:
        raise CorpusError(f"duplicate fixture identities: {repeated_identities}")
    semantics = Counter((item.category, item.semantic_digest) for item in evidence)
    duplicate_semantics = sorted(key for key, count in semantics.items() if count > 1)
    if duplicate_semantics:
        paths = {
            key: sorted(item.path for item in evidence if (item.category, item.semantic_digest) == key)
            for key in duplicate_semantics
        }
        raise CorpusError(f"duplicate semantic fixtures: {paths}")
    return evidence


def _fixture_paths(policy: Mapping[str, Any], files: Mapping[str, bytes]) -> set[str]:
    return {
        path
        for raw_category in _object(policy["categories"], "categories").values()
        for raw_fixture in _list(
            _object(raw_category, "category")["fixtures"],
            "category.fixtures",
        )
        for path in files
        if _matches(path, _string(_object(raw_fixture, "fixture")["glob"], "fixture.glob"))
    }


def _category_fixture_paths(
    policy: Mapping[str, Any],
    files: Mapping[str, bytes],
    category_name: str,
) -> set[str]:
    category = _object(
        _object(policy["categories"], "categories")[category_name],
        f"categories.{category_name}",
    )
    return {
        path
        for raw_fixture in _list(
            category["fixtures"],
            f"categories.{category_name}.fixtures",
        )
        for path in files
        if _matches(
            path,
            _string(_object(raw_fixture, "fixture")["glob"], "fixture.glob"),
        )
    }


def _write_files(root: Path, files: Mapping[str, bytes]) -> None:
    for path, content in files.items():
        destination = root / path
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(content)


def _consumer_source_paths(
    files: Mapping[str, bytes],
    category: str,
) -> set[str]:
    consumer_path = RUST_OFFICIAL_CONSUMERS[category][0]
    paths = {consumer_path} if consumer_path in files else set()
    if category == "replay":
        paths.update(
            path
            for path in files
            if path.startswith(RUST_REPLAY_CONSUMER_SUPPORT)
        )
    return paths


def _replace_consumer_sources(
    *,
    checkouts: Sequence[Path],
    category: str,
    known_paths: set[str],
    source_files: Mapping[str, bytes],
) -> None:
    sources = _consumer_source_paths(source_files, category)
    for checkout in checkouts:
        for path in known_paths:
            candidate = checkout / path
            if candidate.is_file() or candidate.is_symlink():
                candidate.unlink()
        _write_files(
            checkout,
            {
                path: source_files[path]
                for path in sources
            },
        )


def _consumer_result(checkout: Path, category: str) -> ConsumerResult:
    _, test_target, test_name = RUST_OFFICIAL_CONSUMERS[category]
    environment = os.environ.copy()
    environment["CARGO_TARGET_DIR"] = str(
        checkout.parent / f"target-{checkout.name}"
    )
    command = [
        environment.get("CARGO", "cargo"),
        "test",
        "--quiet",
        "--test",
        test_target,
        test_name,
        "--",
        "--exact",
    ]
    try:
        result = subprocess.run(
            command,
            cwd=checkout,
            env=environment,
            check=False,
            capture_output=True,
            text=True,
            timeout=300,
        )
    except subprocess.TimeoutExpired as error:
        output = (error.stderr or "") + (error.stdout or "")
        return ConsumerResult(124, f"consumer timed out after 300 seconds\n{output}")
    return ConsumerResult(
        result.returncode,
        f"{result.stdout}\n{result.stderr}".strip(),
    )


def _consumer_failure(
    *,
    category: str,
    fixture: str,
    revision: str,
    result: ConsumerResult,
) -> CorpusError:
    detail = result.output[-4000:].strip()
    suffix = f":\n{detail}" if detail else ""
    return CorpusError(
        f"{category} fixture {fixture} did not pass against {revision} production "
        f"through the controlled official Rust consumer{suffix}"
    )


def _configure_consumer_fixture(
    *,
    checkouts: Sequence[Path],
    selected_paths: set[str],
    base_files: Mapping[str, bytes],
    current_files: Mapping[str, bytes],
    fixture_path: str | None,
    category: str,
) -> None:
    for checkout in checkouts:
        for path in selected_paths:
            candidate = checkout / path
            if candidate.is_file() or candidate.is_symlink():
                candidate.unlink()
        _write_files(
            checkout,
            {
                path: base_files[path]
                for path in selected_paths
                if path in base_files
            },
        )
        if fixture_path is not None:
            _write_files(checkout, {fixture_path: current_files[fixture_path]})

        if category == "codec":
            directory = checkout / Path(CODEC_FIXTURE_MANIFEST).parent
            names = sorted(
                path.name
                for path in directory.glob("*.json")
                if path.is_file()
            )
            manifest = checkout / CODEC_FIXTURE_MANIFEST
            manifest.parent.mkdir(parents=True, exist_ok=True)
            manifest.write_text(
                "".join(f"{name}\n" for name in names),
                encoding="utf-8",
            )


def _rust_negative_controls(
    *,
    policy_repository_path: str,
    policy: Mapping[str, Any],
    base_files: Mapping[str, bytes],
    current_files: Mapping[str, bytes],
    fixtures: Mapping[str, Sequence[str]],
    consumer_runner: ConsumerRunner,
) -> dict[str, int]:
    if policy.get("binding") != "rust":
        return {}
    controlled_categories = {
        category: tuple(sorted(paths))
        for category, paths in fixtures.items()
        if paths
    }
    if not controlled_categories:
        return {}
    if policy_repository_path not in base_files:
        raise CorpusError(
            "the base revision must contain the regression corpus policy "
            "for Rust negative controls"
        )

    base_policy = _policy(
        _json(
            base_files[policy_repository_path],
            f"base:{policy_repository_path}",
        ),
        f"base:{policy_repository_path}",
    )
    with tempfile.TemporaryDirectory(
        prefix="durable-workflow-rust-negative-control-"
    ) as temporary:
        temporary_root = Path(temporary)
        base_checkout = temporary_root / "base"
        candidate_checkout = temporary_root / "candidate"
        base_checkout.mkdir()
        candidate_checkout.mkdir()
        _write_files(base_checkout, base_files)
        _write_files(candidate_checkout, current_files)

        trusted_files = {policy_repository_path: base_files[policy_repository_path]}
        consumer_source_paths: dict[str, set[str]] = {}
        for category in controlled_categories:
            consumer_path = RUST_OFFICIAL_CONSUMERS[category][0]
            if consumer_path not in base_files:
                raise CorpusError(
                    f"base revision has no trusted official Rust {category} consumer "
                    f"at {consumer_path}"
                )
            if consumer_path not in current_files:
                raise CorpusError(
                    f"candidate removed the official Rust {category} consumer "
                    f"at {consumer_path}"
                )
            base_consumer_sources = _consumer_source_paths(base_files, category)
            current_consumer_sources = _consumer_source_paths(current_files, category)
            consumer_source_paths[category] = (
                base_consumer_sources | current_consumer_sources
            )
        _write_files(base_checkout, trusted_files)
        _write_files(candidate_checkout, trusted_files)
        for category in controlled_categories:
            _replace_consumer_sources(
                checkouts=(base_checkout, candidate_checkout),
                category=category,
                known_paths=consumer_source_paths[category],
                source_files=base_files,
            )

        results: dict[str, int] = {}
        for category, new_paths in controlled_categories.items():
            if category not in _object(base_policy["categories"], "base categories"):
                raise CorpusError(
                    f"base policy has no trusted Rust {category} consumer category"
                )
            selected_paths = _category_fixture_paths(
                policy,
                current_files,
                category,
            )
            selected_paths.update(
                _category_fixture_paths(
                    policy,
                    base_files,
                    category,
                )
            )
            _configure_consumer_fixture(
                checkouts=(base_checkout, candidate_checkout),
                selected_paths=selected_paths,
                base_files=base_files,
                current_files=current_files,
                fixture_path=None,
                category=category,
            )
            for revision, checkout in (
                ("base", base_checkout),
                ("candidate", candidate_checkout),
            ):
                baseline = consumer_runner(checkout, category)
                if baseline.returncode != 0:
                    raise _consumer_failure(
                        category=category,
                        fixture="<baseline corpus>",
                        revision=revision,
                        result=baseline,
                    )

            if category == "replay":
                # Replay scenarios are executable consumer code, so a newly
                # registered workflow must accompany its fixture. Install the
                # exact candidate consumer surface into both checkouts: only
                # the production implementation differs between the controls.
                _replace_consumer_sources(
                    checkouts=(base_checkout, candidate_checkout),
                    category=category,
                    known_paths=consumer_source_paths[category],
                    source_files=current_files,
                )

            for fixture_path in new_paths:
                _configure_consumer_fixture(
                    checkouts=(base_checkout, candidate_checkout),
                    selected_paths=selected_paths,
                    base_files=base_files,
                    current_files=current_files,
                    fixture_path=fixture_path,
                    category=category,
                )
                candidate_result = consumer_runner(candidate_checkout, category)
                if candidate_result.returncode != 0:
                    raise _consumer_failure(
                        category=category,
                        fixture=fixture_path,
                        revision="candidate",
                        result=candidate_result,
                    )
                base_result = consumer_runner(base_checkout, category)
                if base_result.returncode == 0:
                    raise CorpusError(
                        f"{category} fixture {fixture_path} already passes against "
                        "the base production implementation; guarded corpus growth "
                        "requires fail-before/pass-after evidence"
                    )

            for path in selected_paths:
                candidate = candidate_checkout / path
                if candidate.is_file() or candidate.is_symlink():
                    candidate.unlink()
            _write_files(
                candidate_checkout,
                {
                    path: current_files[path]
                    for path in selected_paths
                    if path in current_files
                },
            )
            _write_files(
                candidate_checkout,
                {
                    policy_repository_path: current_files[policy_repository_path],
                },
            )
            _replace_consumer_sources(
                checkouts=(candidate_checkout,),
                category=category,
                known_paths=consumer_source_paths[category],
                source_files=current_files,
            )
            if category == "codec":
                manifest = candidate_checkout / CODEC_FIXTURE_MANIFEST
                if manifest.is_file() or manifest.is_symlink():
                    manifest.unlink()
                if CODEC_FIXTURE_MANIFEST in current_files:
                    _write_files(
                        candidate_checkout,
                        {
                            CODEC_FIXTURE_MANIFEST: current_files[
                                CODEC_FIXTURE_MANIFEST
                            ]
                        },
                    )
            official_result = consumer_runner(candidate_checkout, category)
            if official_result.returncode != 0:
                raise _consumer_failure(
                    category=category,
                    fixture="<candidate corpus>",
                    revision="candidate official",
                    result=official_result,
                )
            results[category] = len(new_paths)
        return results


def _changed_paths(root: Path, base_ref: str) -> tuple[set[str], set[str]]:
    output = _run(["git", "diff", "--name-status", "--find-renames", base_ref, "--"], root)
    changed: set[str] = set()
    added: set[str] = set()
    for line in output.splitlines():
        parts = line.split("\t")
        status = parts[0]
        paths = parts[1:]
        if not paths:
            continue
        changed.update(paths)
        if status.startswith("A"):
            added.add(paths[-1])
    untracked = {
        path
        for path in _run(
            ["git", "ls-files", "--others", "--exclude-standard"],
            root,
        ).splitlines()
        if path
    }
    return changed | untracked, added | untracked


def _guard_matches(
    root: Path,
    base_ref: str,
    changed: set[str],
    raw_guard: Any,
) -> bool:
    guard = _object(raw_guard, "guard")
    matching = sorted(path for path in changed if _matches(path, _string(guard["glob"], "guard.glob")))
    if not matching:
        return False
    patterns = guard.get("content_patterns")
    if patterns is None:
        return True
    diff = _run(
        [
            "git",
            "diff",
            "--function-context",
            "--no-ext-diff",
            "--no-color",
            base_ref,
            "--",
            *matching,
        ],
        root,
    )
    untracked = set(
        _run(["git", "ls-files", "--others", "--exclude-standard"], root).splitlines()
    )
    context_lines: list[str] = []
    inside_hunk = False
    for line in diff.splitlines():
        if line.startswith("diff --git "):
            inside_hunk = False
        elif line.startswith("@@"):
            inside_hunk = True
        elif inside_hunk and line.startswith((" ", "+", "-")):
            context_lines.append(line[1:])
    for path in matching:
        if path in untracked and (root / path).is_file():
            context_lines.append((root / path).read_text(encoding="utf-8", errors="replace"))
    changed_context = "\n".join(context_lines)
    return any(re.search(pattern, changed_context) for pattern in patterns)


def validate(
    root: Path,
    policy_path: Path,
    base_ref: str | None,
    *,
    consumer_runner: ConsumerRunner = _consumer_result,
) -> dict[str, Any]:
    root = root.resolve()
    policy_file = policy_path if policy_path.is_absolute() else root / policy_path
    try:
        policy_repository_path = policy_file.resolve().relative_to(root).as_posix()
    except ValueError as error:
        raise CorpusError(f"{policy_path} must be inside the repository") from error
    policy = _policy(_json(policy_file.read_bytes(), str(policy_path)), str(policy_path))
    current_files = _tracked_worktree_files(root)
    changed: set[str] = set()
    added_paths: set[str] = set()
    base_evidence: list[Evidence] = []
    base_files: dict[str, bytes] = {}
    if base_ref and not ZERO_COMMIT.fullmatch(base_ref):
        _run(["git", "rev-parse", "--verify", f"{base_ref}^{{commit}}"], root)
        changed, added_paths = _changed_paths(root, base_ref)
        base_files = _ref_files(root, base_ref)
        base_policy = policy
        if policy_repository_path in base_files:
            base_policy = _policy(
                _json(
                    base_files[policy_repository_path],
                    f"{base_ref}:{policy_repository_path}",
                ),
                f"{base_ref}:{policy_repository_path}",
            )
            _preserve_base_policy(base_policy, policy, str(policy_path))
        # Apply current coverage to both revisions so selector expansion cannot
        # manufacture growth from pre-existing fixture evidence.
        for path in _fixture_paths(policy, base_files):
            current_content = current_files.get(path)
            if current_content != base_files[path] and current_content is not None:
                if _avro_golden_migration(base_files[path], current_content):
                    base_files[path] = current_content
                    continue
            if current_content != base_files[path]:
                raise CorpusError(f"immutable fixture file {path} was changed, moved, or removed")
        base_evidence = _inventory(policy, base_files)
    current_evidence = _inventory(policy, current_files, new_paths=added_paths)

    current_by_id = {item.identity: item for item in current_evidence}
    base_by_id = {item.identity: item for item in base_evidence}
    for identity, previous in base_by_id.items():
        current = current_by_id.get(identity)
        if current is None:
            raise CorpusError(f"immutable fixture {identity} was removed")
        if current.path != previous.path or current.semantic_digest != previous.semantic_digest:
            raise CorpusError(f"immutable fixture {identity} was changed; append a superseding fixture instead")
    for item in current_evidence:
        for superseded in item.supersedes:
            previous = current_by_id.get(superseded)
            if previous is None:
                raise CorpusError(f"{item.identity} supersedes unknown fixture {superseded}")
            if previous.category != item.category or previous.protocol_version == item.protocol_version:
                raise CorpusError(
                    f"{item.identity} must supersede evidence in the same category at an older protocol version"
                )

    counts: dict[str, dict[str, int | bool]] = {}
    negative_control_fixtures: dict[str, list[str]] = {}
    for category_name, raw_category in _object(policy["categories"], "categories").items():
        current_count = sum(item.category == category_name for item in current_evidence)
        base_count = sum(item.category == category_name for item in base_evidence)
        new_fixture_evidence = sum(
            item.category == category_name and item.path in added_paths
            for item in current_evidence
        )
        related = False
        if base_ref and not ZERO_COMMIT.fullmatch(base_ref):
            category = _object(raw_category, f"categories.{category_name}")
            related = any(
                _guard_matches(root, base_ref, changed, guard)
                for guard in _list(category["guards"], f"categories.{category_name}.guards")
            )
            if related and current_count <= base_count:
                raise CorpusError(
                    f"{category_name} implementation changed but its corpus did not grow "
                    f"(base={base_count}, current={current_count})"
                )
            if related and new_fixture_evidence == 0:
                raise CorpusError(
                    f"{category_name} implementation changed but its corpus growth "
                    "does not include a newly added fixture"
                )
            if related:
                negative_control_fixtures[category_name] = sorted(
                    {
                        item.path
                        for item in current_evidence
                        if item.category == category_name and item.path in added_paths
                    }
                )
        counts[category_name] = {
            "base": base_count,
            "current": current_count,
            "new_fixture_evidence": new_fixture_evidence,
            "related_change": related,
        }
    negative_controls = _rust_negative_controls(
        policy_repository_path=policy_repository_path,
        policy=policy,
        base_files=base_files,
        current_files=current_files,
        fixtures=negative_control_fixtures,
        consumer_runner=consumer_runner,
    )
    return {
        "schema": POLICY_SCHEMA,
        "repository": policy["repository"],
        "base_ref": base_ref,
        "changed_paths": len(changed),
        "counts": counts,
        "negative_controls": negative_controls,
        "status": "pass",
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--policy", type=Path, default=Path("regression-corpus-policy.json"))
    parser.add_argument("--base-ref")
    args = parser.parse_args(argv)
    try:
        result = validate(args.root.resolve(), args.policy, args.base_ref)
    except (CorpusError, OSError) as error:
        print(f"regression corpus validation failed: {error}", file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
