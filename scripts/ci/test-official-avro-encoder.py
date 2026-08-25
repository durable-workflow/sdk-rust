#!/usr/bin/env python3
"""Counterfactual tests for the outbound Apache Avro source qualification."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[2]
VERIFIER_PATH = Path(__file__).with_name("verify-official-avro-encoder.py")
SPEC = importlib.util.spec_from_file_location("official_avro_verifier", VERIFIER_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError("could not load official Avro verifier")
VERIFIER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(VERIFIER)


class OfficialAvroEncoderQualificationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.source = (ROOT / "src/lib.rs").read_text(encoding="utf-8")

    def test_product_source_uses_the_official_encoder(self) -> None:
        VERIFIER.qualify(self.source)

    def test_handwritten_fallback_cannot_replace_the_official_datum(self) -> None:
        counterfactual = self.source.replace(
            "let datum = to_avro_datum(avro_value_ordered_map_encoding_schema()?, datum)",
            "let datum = encode_avro_value_datum(datum)",
            1,
        ) + "\nfn encode_avro_value_datum(_: apache_avro::types::Value) -> Vec<u8> { vec![] }\n"
        with self.assertRaisesRegex(
            VERIFIER.QualificationError, "handwritten Avro binary encoder"
        ):
            VERIFIER.qualify(counterfactual)

    def test_decoy_official_call_cannot_hide_primitive_byte_writes(self) -> None:
        counterfactual = self.source.replace(
            "bytes.extend_from_slice(&datum);",
            "bytes.push(0);\n    bytes.extend_from_slice(&datum);",
            1,
        )
        with self.assertRaisesRegex(
            VERIFIER.QualificationError, "primitive byte-writing"
        ):
            VERIFIER.qualify(counterfactual)


if __name__ == "__main__":
    unittest.main()
