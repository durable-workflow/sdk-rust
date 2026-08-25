#!/usr/bin/env python3
"""Decode a Rust-produced Value datum with Apache's official Python library."""

from __future__ import annotations

import argparse
import io
from pathlib import Path
import struct

import avro
from avro import io as avro_io
from avro import schema as avro_schema


MAGIC_AND_FINGERPRINT = bytes.fromhex("c301e2a33dff55802237")
EXPECTED_AVRO_VERSION = "1.12.2"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("payload", type=Path)
    parser.add_argument("schema", type=Path)
    arguments = parser.parse_args()

    if avro.__version__ != EXPECTED_AVRO_VERSION:
        raise AssertionError(
            f"expected official Python avro {EXPECTED_AVRO_VERSION}, got {avro.__version__}"
        )

    frame = arguments.payload.read_bytes()
    if frame[:10] != MAGIC_AND_FINGERPRINT:
        raise AssertionError("Rust payload has the wrong single-object header")

    stream = io.BytesIO(frame[10:])
    schema = avro_schema.parse(arguments.schema.read_text(encoding="utf-8"))
    datum = avro_io.DatumReader(schema).read(avro_io.BinaryDecoder(stream))
    if stream.tell() != len(frame) - 10:
        raise AssertionError("official Python consumer left trailing datum bytes")

    entries = datum["value"]["entries"]
    if entries["empty_array"]["value"]["items"] != []:
        raise AssertionError("official Python consumer lost the nested empty array")
    if entries["empty_map"]["value"]["entries"] != {}:
        raise AssertionError("official Python consumer lost the nested empty map")

    nested = entries["nested"]["value"]["items"]
    boundaries = nested[0]["value"]["entries"]
    if boundaries["minimum"]["value"]["long"] != -(2**63):
        raise AssertionError("official Python consumer lost i64::MIN")
    if boundaries["maximum"]["value"]["long"] != 2**63 - 1:
        raise AssertionError("official Python consumer lost i64::MAX")
    scalar_array = nested[1]["value"]["items"]
    if scalar_array[0]["value"]["bytes"] != b"\x00\xff":
        raise AssertionError("official Python consumer lost non-UTF-8 bytes")
    negative_zero = scalar_array[1]["value"]["double"]
    if struct.pack(">d", negative_zero) != bytes.fromhex("8000000000000000"):
        raise AssertionError("official Python consumer lost negative-zero bits")

    print(f"official Python avro {avro.__version__} decoded Rust payload")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
