# Installed-package acceptance against native Server

This harness checks one exact published `durable-workflow` crate against one
exact native Server image. It runs in disposable local containers on an
internal Docker network and never contacts Cloud.

It does the following:

1. **Server identity.** Resolves the image index digest, pulls it and the
   requested platform manifest, and confirms both give the same image. It can
   require the revision and release-tag labels, and the `/api/cluster/info`
   version.
2. **Fresh build.** Builds `fixture/` with a freshly resolved lockfile against
   `durable-workflow = "=<version>"`, using `--jobs` cargo jobs (default 2).
3. **Crate provenance.**
   - The lockfile, crates.io and downloaded `.crate` SHA-256 values must agree.
   - The crate's VCS commit must equal the clean source tag.
   - Every packaged `src/` and `schema/` file must match that tag.
   - The packaged per-major Server baseline map
     (`supported-server-release-baselines`) is checked as `required` (equal to
     `--expected-baselines`, default `{"2": "2.0.0", "3": "3.0.2"}`, with the
     legacy scalar equal to its `2` entry) or `absent`.
   - `--baselines-control` runs the required-map check on another `Cargo.toml`
     as a positive control.
4. **Lifecycle cell.** One managed worker process, using the published crate,
   runs these scenarios:
   - a regular workflow: activity, query, signal and an exact typed result;
   - a non-retryable typed activity failure, which must surface as
     `WorkflowFailed`/`ActivityFailed`;
   - continue-as-new with exact typed values;
   - a real run timeout (`WorkflowTimedOut`, `run_timeout`);
   - cancellation;
   - another continue-as-new afterward.
5. **Deadline cell.** For run and execution deadlines, the proxy holds the
   worker's history-wait report for 3 seconds. The deadline then passes, and
   the runtime must answer with a matching terminal 409 receipt (`recorded=false`,
   `reason=run_timed_out`). History must hold exactly one `WorkflowTimedOut` of
   the right kind, and the same worker process must then complete another
   workflow. The execution case continues once after two seconds, so its
   second run reaches the 5-second execution deadline before its own 4-second
   run deadline.

`check_history.py` verifies each cell's durable history through the runtime's
public HTTP API, independently of the SDK under test.

## Run

Fetch tags first (`git fetch --tags origin`), then run:

```sh
python3 scripts/acceptance/installed-native/run.py \
  --crate-version 4.10.3 \
  --server-index sha256:<index digest> \
  --expect-server-version 3.0.1 \
  --expect-release-tag native-3.0.1 --expect-revision <40-hex commit> \
  --baselines required \
  --work-dir <new empty directory>
```

For a crate published before the map existed (≤ 4.10.2), pass
`--baselines absent --baselines-control Cargo.toml`.

The run writes `<work-dir>/summary.json` with every identity, receipt and
history result, plus per-container logs under `cells/*/logs`. The exit code is
0 only when every check passes and cleanup is complete (see below).

### Containers and cleanup

`--container-prefix` (default `sdkrust`, must match `^[a-z][a-z0-9-]{0,30}$`)
names the owner, for example `general` or `security`. Every container and
network is named `<prefix>-accept-<utc stamp>-<16 hex token>-…`, where the
token is fresh per invocation. Each carries the labels `task`,
`dev.durable-workflow.acceptance.run=<token>` and
`dev.durable-workflow.acceptance.owner=<prefix>`.

While the client is alive, it removes its containers, its network and the
fixture credentials, even when the run fails. Each cell writes its credential
file and creates its network and containers inside the cleanup-protected
region. It records each container name before running `docker run`, because
the daemon can create a container (or network) even when the client then times
out or errors. Cleanup inspects each recorded name, then logs and removes the
object by ID only when its run label equals this invocation's token. A name
collision or a failed create never touches anyone else's object.

Each cell creates `state/`, `database/`, `storage/` and its credential file
inside the cleanup-protected region, so a partial setup failure is cleaned up
too. The credential file is created owner-only (`0600`). Its readers (the
fixture worker, driver and history check) run as the host UID, and the server
roles get the token from their environment.

Each Docker inspect, log and remove step is best-effort: a failure or timeout
is recorded and the remaining steps still run. The credential file,
`database/` and `storage/` are removed separately, in an outer `finally`,
whatever Docker did. Afterwards each one is checked for absence, and any
survivor is listed in `local_leftovers`. A failed step never replaces the
run's own error.

The final JSON line and `summary.json` (`cleanup`) report:
- `cleanup_errors`, each with only the step, a bounded object label (a
  container or network name, or `<cell>/<path>`) and the error type;
- `local_leftovers`;
- `leftover_containers` and `leftover_networks` that still carry this run's
  token. A list is `null` when its query failed.

A container whose inspect failed is never removed, because its ownership could
not be confirmed.

**Cleanup fails closed.** The run fails and exits nonzero on any cleanup error,
a failed leftover query, a nonempty leftover list or a surviving local path,
even when every check passed. `summary.json` keeps the checks' own result in
`checks_outcome`, and any run error in `error`.

The fixture build container uses `--rm`, `--init`, `--cpus` equal to `--jobs`,
`--memory 3g`, `--pids-limit 1024`, `no-new-privileges` and `--cap-drop ALL`.
It uses the default network for the crates.io fetch and mounts only the fixture
directory. Its build runs under `timeout --kill-after=60s 3300s` inside the
container, so the container exits and the daemon removes it even if the client
is killed. The client's own timeout is 3600 seconds.

Cell containers all get `--init`, `--cpus 1`, `--memory 1g`,
`no-new-privileges` and `--cap-drop ALL`. Their `--pids-limit` is 256, except
MySQL, which gets 512. The native cell containers also run with a read-only
root filesystem. The PHP baseline (`--runtime php`) keeps writable roots, as in
its compose file. It adds back only the capabilities each image's entrypoint
uses:

| Role | Added capabilities | Why |
| --- | --- | --- |
| `mysql` (8.4) | `CHOWN`, `DAC_OVERRIDE`, `SETGID`, `SETUID` | The root entrypoint walks and chowns its data, socket and mysql-owned secure-file directories, then drops to `mysql` with gosu. |
| `redis` (7.4) | `CHOWN`, `SETGID`, `SETUID` | The root entrypoint chowns `/data`, then drops to `redis` with gosu. |
| `bootstrap` | `DAC_OVERRIDE` | Root writes Laravel caches under directories owned by www-data. |
| `runtime` | `CHOWN`, `DAC_OVERRIDE`, `KILL`, `SETGID`, `SETUID` | The Apache root master chowns its mutex, switches its children to www-data and signals them. |
| `queue`, `repair` | none | They run as uid 1000. |

Cell containers are detached and have no client-independent lifetime bound. If
the client is killed mid-cell, its containers, its network and the cell's
`state/` (credential file, database and storage) stay behind until removed by
hand. Removing the containers labelled with the run token alone is not a
complete cleanup. Issue #171 tracks an independent cell lifetime and reaper.

The cell runtime uses SQLite, normal native maintenance, and one shared
fixture token for all roles. It does not cover role-only authentication, other
databases, other platforms, or Cloud deployment.
