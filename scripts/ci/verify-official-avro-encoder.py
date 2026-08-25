#!/usr/bin/env python3
"""Require the maintained Apache Avro encoder on the outbound payload path."""

from __future__ import annotations

import argparse
from pathlib import Path
import re


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SOURCE = ROOT / "src/lib.rs"
HANDWRITTEN_ENCODERS = re.compile(
    r"(?m)^fn encode_avro_(?:long|size|bytes|string|value_datum)\b"
)


class QualificationError(ValueError):
    """The outbound Avro implementation crossed the maintained-codec boundary."""


def function_body(source: str, signature: str) -> str:
    start = source.find(signature)
    if start < 0:
        raise QualificationError(f"missing {signature}")
    opening = source.find("{", start + len(signature))
    if opening < 0:
        raise QualificationError(f"missing body for {signature}")

    depth = 0
    for index in range(opening, len(source)):
        character = source[index]
        if character == "{":
            depth += 1
        elif character == "}":
            depth -= 1
            if depth == 0:
                return source[opening + 1 : index]
    raise QualificationError(f"unterminated body for {signature}")


def qualify(source: str) -> None:
    if HANDWRITTEN_ENCODERS.search(source):
        raise QualificationError(
            "handwritten Avro binary encoder helpers are not allowed in the product source"
        )

    body = function_body(source, "pub fn encode_avro_value")
    required_fragments = (
        "let datum = avro_value_to_datum(value)?;",
        "to_avro_datum(",
        "bytes.extend_from_slice(&AVRO_SINGLE_OBJECT_MAGIC);",
        "bytes.extend_from_slice(&AVRO_VALUE_SCHEMA_FINGERPRINT);",
        "bytes.extend_from_slice(&datum);",
    )
    missing = [fragment for fragment in required_fragments if fragment not in body]
    if missing:
        raise QualificationError(
            "encode_avro_value does not route its framed datum through apache_avro::"
            f"to_avro_datum: missing {missing!r}"
        )
    if ".push(" in body or "to_le_bytes(" in body:
        raise QualificationError(
            "encode_avro_value contains primitive byte-writing operations"
        )
    if body.count("bytes.extend_from_slice(") != 3:
        raise QualificationError(
            "encode_avro_value must append only magic, fingerprint, and the official datum"
        )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    arguments = parser.parse_args()
    qualify(arguments.source.read_text(encoding="utf-8"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
