# Nexus HTTP source qualification

These controls use real SDK Workers and the public PHP Server API. They cover
Rust/PHP, PHP/Rust, Rust/Rust, Rust/Python and Python/Rust cancellation, plus a
Rust target killed after its cleanup timer commits and replaced by a cold worker.
They retain one original service admission, the original 30-second cleanup and
separate 180-second service deadline, canonical context/lineage, typed cold
results, duplicate identity, worker shutdown and nine actual-record reader refusals.
The same caller Worker must complete an unrelated workflow while its original
Nexus target is still cleaning up. The original caller cannot receive canonical
cancellation delivery before the target finishes.

The Rust crate under test is source. A passing run does not add Nexus to published
3.5.5 or authorize a Rust major release. PHP Server 2.9.4, PHP SDK 2.5.1 and Python
SDK 2.6.0 are separate published artifacts.

## Run

Use a disposable state directory, a PHP CLI with `pcntl` and `posix`, Python 3.13
and Rust 1.86 or later. Build this checkout's `nexus_cooperation` example into
`STATE/target`, install `composer.json` into `STATE/php` and `requirements.txt`
into `STATE/python`. The state directory is an absolute path outside the checkout.
The fixture's Cargo lock records the resolved input used for its actor build.
Copy it to the checkout root and retain the source/binary identities during the
immediate run review.

Create a Compose environment file declaring `COMPOSE_PROJECT_NAME`, `NEXUS_UID`,
`NEXUS_GID`, `NEXUS_STATE_DIR` and `NEXUS_PHP_IMAGE`. Optional `NEXUS_RUST_IMAGE`,
`NEXUS_PYTHON_IMAGE`, `NEXUS_MYSQL_IMAGE` and `NEXUS_REDIS_IMAGE` select prepared
runtime images. Preserve the published Server digest unless qualifying a
separately named cohort.

Prepare dependencies using the declared language runtimes:

```sh
mkdir -p "$NEXUS_STATE_DIR/php" "$NEXUS_STATE_DIR/python"
cp tests/nexus/composer.json tests/nexus/composer.lock "$NEXUS_STATE_DIR/php/"
composer install --working-dir="$NEXUS_STATE_DIR/php" --no-dev --prefer-dist
python -m pip install --target "$NEXUS_STATE_DIR/python" -r tests/nexus/requirements.txt -c tests/nexus/requirements.lock
cp tests/nexus/Cargo.lock Cargo.lock
CARGO_TARGET_DIR="$NEXUS_STATE_DIR/target" cargo build --locked --example nexus_cooperation
```

The recurring repair role follows the existing cooperative fixture's 10-second
cadence. Each command reports its actual result, including transient database
failures, and the next scheduled pass still runs. The control checks every
backend role before and after each original; a dead maintenance loop cannot be
reported as a complete profile.

```sh
export NEXUS_ENV_FILE=/absolute/path/to/fixture.env
export NEXUS_STATE_DIR=/absolute/path/to/state
docker compose --env-file "$NEXUS_ENV_FILE" -f tests/nexus/docker-compose.yml \
  up -d --no-build --wait server queue repair
bash tests/nexus/run.sh
docker compose --env-file "$NEXUS_ENV_FILE" -f tests/nexus/docker-compose.yml \
  down --volumes --remove-orphans
```

For successful typed results, target failures, missing-operation failures and
3 MiB external arguments/results across the five directions and default/named
namespaces, run `bash tests/nexus/run.sh outcomes` against a fresh backend.
This enables local namespace payload storage under the disposable provider mount.
Create writable `STATE/provider/default` and `STATE/provider/nexus-source`
directories before starting the backend. Each original retains its 30-second
observation and 180-second service policy. Exact types/bytes, cold selected-run
results, immutable histories and successful final Worker shutdown are required.

`bash tests/nexus/run.sh cross-outcomes` runs that same synchronous-result profile
with the catalog and target in the other namespace. Successful and external
results must reach the original caller; target failures and missing operations
keep their typed refusals. Cold target reads use its own namespace. Wrong-scope
caller and target histories must refuse independently. The published runtime
must translate external result references into the caller's payload boundary.

`bash tests/nexus/run.sh admission` exercises sync/accepted, async/completed and
async/accepted across the same directions/namespaces. Ordinary callers continue
before their eight-second target finishes. Cold controls kill the caller after
its three-second continuation timer commits, retire that registration, wait for
the target and start a replacement. The original admission snapshot and explicit
idempotency key must survive replay and an independent cold read. Accepted calls
do not gain a terminal service delivery in caller history. The configured service
policy remains 180 seconds and each original has a 30-second observation bound.

`bash tests/nexus/run.sh cross-namespace` uses the same admission and cold-replay
controls with the service catalog and target Worker in the other namespace.
Both default-to-named and named-to-default calls retain the original caller
authority. Each history must refuse reads from the other namespace. Target
selection uses the SDK's explicit service namespace option.

`bash tests/nexus/run.sh acknowledgement` runs twenty fresh synchronous calls
across the five directions and all four caller/target namespace pairs. A
disposable proxy forwards the original service command, receives an actual
`recorded=true` completion and closes that one response before the caller sees
it. It preserves ordinary traffic and sends no replacement commands. Retire the
original caller, wait for the target and start a cold caller. Its retained
history must contain one original admission and terminal result, with unchanged
caller/target authority, idempotency and budgets. The proxy's task/body hash is
correlated with the physical admission. Final Workers and independent cold
results must pass; the original faulted/killed process is recorded separately.
Run `node scripts/ci/test-completion-ack-proxy.mjs` to check the fault selector
and unchanged local-activity proxy default without any SDK or Server fixture.

## Published Waterline inspection

`bash tests/nexus/run.sh catalog` extends the cross-namespace result profile
with real published Waterline inspection. The same forty originals cover five
SDK directions, both namespace directions and success, typed failure, external
payload and missing-operation outcomes. Original workflow budgets stay unchanged.
Browser inspection occurs after completion with bounded request/navigation timeouts.

Prepare the locked browser dependency in `STATE/browser`: copy
`browser-package.json` to `package.json` and `browser-package-lock.json` to
`package-lock.json`, then run `npm ci --ignore-scripts` there. The pinned browser
image supplies matching Playwright Chromium. Also create a writable
`STATE/provider/nexus-unrelated` directory for the unrelated observer namespace.

The profile starts three read-only Waterline instances on the isolated Docker
network, with no published ports. Owner and caller views must preserve original
identities, typed values, exact deadlines and opaque foreign references. The
unrelated namespace and wrong selectors must refuse. Desktop/dark and
mobile/light pages must fit, expose their actual terminal result or failure,
and leave both original histories unchanged. No API mocks or observer writes
are permitted. Reports and representative screenshots go to `STATE/catalog`.
`node --test tests/nexus/test-catalog.mjs` checks the identity/type/redaction guards.

Use fresh case identities after a failure. Keep the original outcome separate;
do not retry a failed workflow by replacing its recorded state or widening a
deadline. Review bounded summaries and retire generated state after recording
the result on the owning PR. Registry attestations and committed fixtures remain.
