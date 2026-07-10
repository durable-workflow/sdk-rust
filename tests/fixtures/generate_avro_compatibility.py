"""Regenerate the Python official-Avro generic-wrapper fixture blob."""

import base64
import io
import json
from pathlib import Path

import avro.io
import avro.schema


fixture = json.loads(Path(__file__).with_name("avro_generic_wrapper.json").read_text())
schema = avro.schema.parse(fixture["schema"])
encoded_json = json.dumps(fixture["value"], separators=(",", ":"), ensure_ascii=False)
assert encoded_json == fixture["json"]
buffer = io.BytesIO()
writer = avro.io.DatumWriter(schema)
writer.write(
    {"json": encoded_json, "version": 1},
    avro.io.BinaryEncoder(buffer),
)
datum = buffer.getvalue()
decoded = avro.io.DatumReader(schema).read(avro.io.BinaryDecoder(io.BytesIO(datum)))
assert decoded == {"json": encoded_json, "version": 1}
print(base64.b64encode(b"\x00" + datum).decode("ascii"))
