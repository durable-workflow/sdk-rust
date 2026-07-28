# Avro Value benchmark

`cargo run --release --example avro_value_benchmark -- --enforce` loads the
checked-in medium corpus at `schema/avro-value-benchmark-v1.json` (SHA-256
`588771404977f2a95fe7d8969c24a15e1c7dd78fe498af9aa2406f82be54b666`).
The bytes sentinel is adapted only for the fixed typed path; compact JSON and
the removed wrapper use the corpus's documented JSON representation.

A release qualification run in the Linux Rust worker measured about 21 µs to
encode and 14 µs to decode. The enforced 75/60 µs defaults allow compiler and
shared-runner variance while rejecting the hundreds-of-microseconds recursive
same-schema resolution path. Set `AVRO_VALUE_ENCODE_BUDGET_US` and
`AVRO_VALUE_DECODE_BUDGET_US` to calibrate a different qualification runner.
