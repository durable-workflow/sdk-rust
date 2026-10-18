# Nexus HTTP source qualification

These controls use real SDK Workers and the public PHP Server API. They cover
Rust/PHP, PHP/Rust, Rust/Rust, Rust/Python and Python/Rust cancellation, plus a
Rust target killed after its cleanup timer commits and replaced by a cold worker.
They retain one original service admission, the original 30-second cleanup and
separate 180-second service deadline, canonical context/lineage, typed cold
results, duplicate identity, worker shutdown and six actual-record reader refusals.

The Rust crate under test is source. A passing run does not add Nexus to published
3.5.5 or authorize a Rust major release. PHP Server 2.9.4, PHP SDK 2.5.1 and Python
SDK 2.6.0 are separate published artifacts.

## Run

Use a disposable state directory, a PHP CLI with `pcntl` and `posix`, Python 3.13
and Rust 1.86 or later. Build this checkout's `nexus_cooperation` example into
`STATE/target`, install `composer.json` into `STATE/php` and `requirements.txt`
into `STATE/python`. The state directory is an absolute path outside the checkout.
The Rust library normally resolves its own Cargo lock; keep the generated lock
and source/binary identities until the immediate run review finishes.

Create a Compose environment file declaring `COMPOSE_PROJECT_NAME`, `NEXUS_UID`,
`NEXUS_GID`, `NEXUS_STATE_DIR` and `NEXUS_PHP_IMAGE`. Optional `NEXUS_RUST_IMAGE`,
`NEXUS_PYTHON_IMAGE`, `NEXUS_MYSQL_IMAGE` and `NEXUS_REDIS_IMAGE` select prepared
runtime images. Preserve the published Server digest unless qualifying a
separately named cohort.

```sh
export NEXUS_ENV_FILE=/absolute/path/to/fixture.env
export NEXUS_STATE_DIR=/absolute/path/to/state
docker compose --env-file "$NEXUS_ENV_FILE" -f tests/nexus/docker-compose.yml \
  up -d --no-build --wait server queue repair
bash tests/nexus/run.sh
docker compose --env-file "$NEXUS_ENV_FILE" -f tests/nexus/docker-compose.yml \
  down --volumes --remove-orphans
```

Use fresh case identities after a failure. Keep the original outcome separate;
do not retry a failed workflow by replacing its recorded state or widening a
deadline. Review bounded summaries and retire generated state after recording
the result on the owning PR. Registry attestations and committed fixtures remain.
