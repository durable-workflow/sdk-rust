# Installed-package acceptance against native Server

This harness checks one exact published `durable-workflow` crate, or (in
pre-publication mode, below) the exact `cargo package` bytes of one clean
release commit, against one exact native Server image. It runs in disposable local containers on an
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

### Pre-publication mode

A new version must pass this acceptance **before** it is tagged or published,
so crates.io never holds an unqualified immutable version. `--package-from`
replaces `--crate-version`: the harness packages one exact clean commit and
installs only those packaged bytes.

```sh
python3 scripts/acceptance/installed-native/run.py \
  --package-from <clean checkout whose HEAD is the release commit> \
  --package-commit <40-hex release commit> \
  --expect-version 4.11.2 \
  --server-index sha256:3cb6e5bf8ec766ac2abd933369b40a77af62794b6d7b921ce21d3fb263e88b72 \
  --platform linux/arm64 \
  --expect-server-version 3.0.5 \
  --expect-release-tag 3.0.5 --expect-revision bc24589db02161a21febbb30211309537fd1e24e \
  --baselines required --container-prefix general \
  --work-dir <new empty directory>
```

Run the harness from a clean checkout. Pre-publication mode refuses a
harness tree with changed, staged or untracked files, because the approval
records the harness commit.

Use the commit that will be tagged. Cargo records the commit in
`.cargo_vcs_info.json`, so a different commit (for example a later merge
commit with the same tree) produces a different package and will not match.

1. **Exact source.** The checkout's HEAD must equal `--package-commit` and
   its tree must be fully clean (no changed or untracked paths). The harness
   copies that commit into `<work-dir>/package-source`, a self-contained
   repository, and checks it the same way.
2. **Package.** A bounded `--rm` container (same limits as the fixture build)
   runs `cargo package --locked --no-verify --list` and
   `cargo package --locked --no-verify`, with the source mounted read-only. The
   container must report Cargo `--package-cargo-version` (default `1.86.0`,
   the release workflow's), because a different Cargo writes different bytes.
   `--no-verify` skips only Cargo's own test build. The archive is already
   written by then, and the offline fixture build compiles it.
3. **Bind.** The harness refuses the package unless all of these hold:
   - exactly `durable-workflow-<version>.crate` was produced, and the source and
     packaged manifests both declare `--expect-version`;
   - every member is a regular file under `durable-workflow-<version>/`, with
     no links, traversal or duplicates;
   - the member set equals the `--list` output exactly;
   - `.cargo_vcs_info.json` names the clean commit, with `path_in_vcs` empty;
   - every member except the generated `Cargo.toml` and `.cargo_vcs_info.json`
     is byte-identical to the commit's blob (`Cargo.toml.orig` against
     `Cargo.toml`).

   `--expect-package-sha256` also pins the archive SHA-256, for example when
   re-running an earlier result.
4. **Install only those bytes.** One container (default network) patches the
   dependency to the unpacked package, only to resolve and `cargo vendor`
   third-party crates. The harness then adds the verified members as a
   directory-source entry whose package checksum is the `.crate` SHA-256 and
   replaces crates.io with that directory. A second container, with
   `--network none`, regenerates the lockfile and builds offline. Cargo checks
   every vendored file against the checksums. The fixture lock must name
   `durable-workflow <version>` from the crates.io source with checksum equal
   to the `.crate` SHA-256. The installed files must equal the archive
   members exactly. There is no path dependency on checkout bytes.
5. **Same cells.** The lifecycle and deadline cells, Server identity and
   same-worker assertions are unchanged. After the cells, the saved `.crate`
   must still hash to the bound SHA-256.

`summary.json` records `package` (commit, version, SHA-256, size, sorted file
list and its digest, VCS record, Cargo version, Rust image identity) and
`crate` (lock checksum and source, installed file count, baselines). Only a run
whose checks and cleanup both pass writes `<work-dir>/approved-package.json`:

```json
{
  "schema": "durable-workflow.rust-sdk.approved-package/v1",
  "package": "durable-workflow", "version": "4.11.2", "commit": "<40 hex>",
  "crate_sha256": "<64 hex>", "cargo_version": "1.86.0",
  "files": ["..."], "file_list_sha256": "<64 hex>",
  "acceptance": {
    "summary_sha256": "<64 hex>", "harness_commit": "<40 hex>",
    "server_index": "ghcr.io/durable-workflow/server@sha256:<64 hex>",
    "executed_manifest": "ghcr.io/durable-workflow/server@sha256:<64 hex>",
    "platform": "linux/arm64", "expected_server_version": "3.0.5",
    "expected_revision": "<40 hex>", "expected_release_tag": "3.0.5",
    "cluster_versions": ["3.0.5"], "cells": ["deadline", "lifecycle"],
    "finished_at": "<ISO 8601 with offset>", "outcome": "pass"
  }
}
```

Publication accepts only this exact shape: no missing or extra keys at
either level, and strict types. It also checks how the receipt's fields relate:
- the index and executed manifest are canonical digests in one repository;
- the tag equals the expected Server version, and so does every cell's cluster
  version;
- the cells are exactly lifecycle and deadline;
- the platform is `linux/amd64` or `linux/arm64`;
- the completion time is a valid timestamp with an offset;
- the outcome is `pass`.

To match, pre-publication mode requires `--server-index sha256:<64 hex>`, a
supported `--platform`, both cells, a 40-hex `--expect-revision`, and
`--expect-release-tag` equal to `--expect-server-version`.

A reviewer commits that record unchanged to `main` as
`release/approved-packages/<version>.json`. The same change commits the run's
exact `summary.json` bytes as `release/approved-packages/<version>.summary.json`.
`release/` is outside the crate's `include` list, so neither file changes
package bytes. Both files are read from the maintained tooling checkout
(`main`), never from the tagged source, with the tracked-blob and no-follow
rules below. Publication requires all of the following:
- the summary's SHA-256 equals `acceptance.summary_sha256`;
- the summary is a passing pre-publication run, with checks, outcome and
  cleanup all passing and no error;
- its harness commit, Server tuple, finish time and cells match the receipt;
- its package commit, `.crate` SHA-256 (before and after the cells), file list
  and digest, VCS record and Cargo version, and its locked crate checksum, all
  equal the record.

The Release workflow (`release.yml`) runs as four jobs. Hosted runners keep
every secret a job references in memory, grant passwordless sudo, and let any
step of an `id-token` job mint OIDC tokens. The credential is therefore
isolated by job, not by step:

1. **`gate`** has no credential (`contents: read`).
   - It reproduces `cargo package --locked` from the tag with Cargo's full
     verify build. Every build script and proc-macro runs here, without the
     credential.
   - It refuses (`approved_package_mismatch`) unless the archive's SHA-256,
     file set, version, VCS commit, Cargo release and committed summary all
     equal the record.
   - It never publishes a missing version without a record
     (`approved_package_required`).
   - It uploads the `.crate` unarchived as a digest-bound artifact.
2. **`publish`** is the only job with `id-token: write`, the `crates-io`
   environment and the minted `CARGO_REGISTRY_TOKEN`.
   - It runs no build, test or other crate code.
   - The central handoff validator checks the artifact's ID, run and SHA-256.
   - Before minting, it reproduces the package with `--no-verify`, which
     neither builds nor runs build scripts. It requires that package to equal
     the handed-off file, the artifact digest, the gate output and the approval
     (`gated_package_mismatch`), and requires the version to still be absent.
   - Only then is the token minted, and only `cargo publish --locked
     --no-verify` sees it.
   - The tagged source is package data only. Cargo is pinned with
     `RUSTUP_TOOLCHAIN=1.86.0`, and the publisher refuses an in-repo
     `.cargo/config*` or `rust-toolchain*`.
3. **`verify`** has no credential. The crates.io checksum and the downloaded
   archive must equal the approved SHA-256, and the downloaded archive must
   match the record again. The tag must read back from `origin` as the release
   commit. Then the fresh-consumer build runs. Any difference fails the release
   (`published_checksum_differs_from_approved_package`,
   `published_package_differs_from_approved_package`,
   `source_tag_readback_mismatch`), and nothing may claim the version is
   qualified.
4. **`release`** creates the GitHub Release from the validated changelog
   entry.

Cargo cannot upload a prebuilt `.crate`, so `cargo publish --no-verify`
packages again from the same clean tag with the same Cargo. The preflight
equality before upload and the readback afterwards are what prove that the
published bytes are the accepted ones. CI's package job also checks every
change: the `--no-verify` repackage must equal the verify-built package.

#### Path confinement

Every file named by a command-line argument, an environment variable or an
approval record is resolved inside an explicit root before it is opened or
written.

| Input | Root | Rule |
| --- | --- | --- |
| `--package-from`, `--work-dir`, `--fixture-from`, `--baselines-control` | Operator-supplied root | The path is canonicalized. The harness refuses a NUL byte, a `..` component, or a path that is itself a symbolic link. |
| Paths the harness writes or reads below the work dir, such as unpacked and vendored members | `--work-dir` | Each is a `/`-separated relative path with no empty, `.`, `..`, NUL or backslash parts and no link at any component. Its canonical result must stay inside the root (`os.path.commonpath`). Files are created with `O_EXCL`/`O_NOFOLLOW`. |
| `release_package.py --manifest`, `--changelog` | `--source-root` (the release checkout) | Relative, confined |
| `release_package.py --archive` | `--archive-root` (Cargo's `target/package`, or the publisher's temporary directory) | Relative, confined |
| Approval record | `--approval-repository`, set by the Release workflow as `RUST_SDK_APPROVED_PACKAGE_REPOSITORY=$GITHUB_WORKSPACE/release-tools` (the maintained-tooling checkout of `main`) | Always `release/approved-packages/<version>.json`. The version must match strict Semantic Versioning 2.0.0 before it names the path. The record is read with `lstat` and `O_NOFOLLOW` and must be a regular file with no link at any component. It must also be byte-identical to a regular (`100644`) blob tracked at exactly that path in the checked-out commit. Its own version must match, and its file names must be canonical relative paths. |

Versions used in harness paths (`--expect-version`) also follow strict
Semantic Versioning, so a value such as `4.11.2/../../x` is refused.

The publisher also refuses a manifest or changelog outside its working
directory, and an evidence path that is a link or contains a `..` component.

Every `.crate` member is checked independently, in acceptance and again at
publication. Each must be a regular file, never a symlink, hardlink or
directory. Its name must sit under the exact `<name>-<version>/` prefix, as a
canonical relative POSIX path with no absolute form, empty, `.` or `..`
component, backslash or NUL. No name may appear twice.

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
