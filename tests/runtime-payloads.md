# Native Runtime Payload Qualification

This is a destructive, local-only experiment. Do not use customer namespaces or
production credentials. The fixture creates its own namespace and role-specific
test credentials. It uses the native published Server, MySQL and Redis; it does
not replace the HTTP server or lift ordinary request limits.

Set `SERVER_IMAGE` to an exact published image compatible with runtime external
payload transport. Record its digest and this checkout SHA in the PR or release
issue. For publication qualification, run the same example in a clean Cargo
consumer with an exact registry dependency instead of a path override.

From the repository root (Rust 1.86+, Docker Compose):

```sh
cargo build --example runtime_payloads
mkdir -p target/runtime-payloads
docker compose -f tests/runtime-payloads.compose.yml up -d --wait --wait-timeout 180 server
docker compose -f tests/runtime-payloads.compose.yml run --rm sdk prepare
docker compose -f tests/runtime-payloads.compose.yml run -d --name rust-payload-worker sdk worker
docker compose -f tests/runtime-payloads.compose.yml run --rm sdk start
docker compose -f tests/runtime-payloads.compose.yml run --rm sdk maximum
```

`start` must complete one workflow and persist a second workflow after its
activity reaches a condition wait. The payload contains 3,014,656 text bytes,
binary bytes including invalid UTF-8, an integer, an integral double, signed
zero and a nested map. `maximum` verifies an exactly 64 MiB encoded result.
No payload contents are printed.

Stop the worker, Server and MySQL; keep the database volume and payload files.
The worker uses abrupt termination deliberately to exercise process loss.

```sh
docker stop --timeout 2 rust-payload-worker
docker compose -f tests/runtime-payloads.compose.yml stop server mysql
docker compose -f tests/runtime-payloads.compose.yml up -d --wait --wait-timeout 180 server
docker start rust-payload-worker
docker compose -f tests/runtime-payloads.compose.yml run --rm sdk verify
docker compose -f tests/runtime-payloads.compose.yml run --rm sdk verify-maximum
```

`verify` fetches the committed activity through a cold query snapshot, sends a
digest signal, completes the waiting workflow and compares both results on the
lossless Avro surface before decoding into Serde types. `verify-maximum` fetches
the maximum result through a fresh client after the database/runtime restart.
Report failures as failures, including OOM or timeouts. Record memory limits,
container memory peaks and OOM flags alongside the outcome. This experiment
does not qualify a production storage provider or multi-tenant capacity.

Cleanup only these fixture resources:

```sh
docker rm -f rust-payload-worker
docker compose -f tests/runtime-payloads.compose.yml down --volumes --remove-orphans
rm -r target/runtime-payloads
```
