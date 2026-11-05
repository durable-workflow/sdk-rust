# Published condition-history acceptance

This packet prepares the selected acceptance for [#191](https://github.com/durable-workflow/sdk-rust/issues/191)
and [#192](https://github.com/durable-workflow/sdk-rust/issues/192).
It is source under review. It has not built its consumer or run a container.
Server history review and Security executable review precede a separate General
admission of the exact runner commit, command, fresh capacity and absolute deadline.

The package is crates.io `durable-workflow =4.12.0`, archive SHA-256
`620a8287af73c363dac61f0a91f20971a39f3b8cdc9513fbea85cdbd95a74ba4`,
source `4b2440a5618ccc1c98960ea2e1e62127042c3590` (H). Its published tag,
archive and approval records are inputs and remain unchanged.

`run.py` exports H's official replay consumer and four existing history fixtures
byte for byte, with fixed SHA-256 checks. `fixture/tests/conditions.rs` includes
that consumer and adds the public Worker port of H's four 300/180-second
orphan/tail variants and genuine-timeout refusal. Each case runs with fresh
workers. The typed replay error is observed in the handler before returning the
same error to the Worker; the ordinary published task-failure report is also
retained. The consumer then runs one new live two-member false-wake/reopen case
against unchanged PHP Server 2.11.8, requiring typed `both` completion.

The original raw proxy artifact for #191 is unavailable. These existing fixtures
are retained evidence, without a claim that they reconstruct that missing full
capture. The live case produces a new capture. #192's original duplicate producer
was the PHP Laravel bridge on native Server 3.0.5. This packet proves installed
Rust replay and does not claim Rust duplicate emission or Server admission behavior.

All executable image references select immutable `linux/amd64` manifests:

| Role | Image digest |
| --- | --- |
| Rust 1.86.0 build/consumer | `rust@sha256:300ec56abce8cc9448ddea2172747d048ed902a3090e6b57babb2bf19f754081` |
| PHP Server 2.11.8 | `ghcr.io/durable-workflow/server@sha256:980fe0a052089ced34ec3fcd2be2ea4e77436861d8fb537fb6654cb652a53e07` |
| MySQL | `mysql@sha256:80f4933e3835f9dc4d35a28ec500d7986cb4414e6c6821c5461239cb7beb8995` |
| Redis | `redis@sha256:ca0acbb137c1dc3339c8b147a58fd6f42775d4599327b50e7b116c23de501af2` |

Provenance is limited to those executed child manifests and their effective
`linux/amd64` content readbacks. The Server source label must equal
`fa3fc2f2f6b8935ea4a6eea47f6a9221e2adafff`. This packet makes no registry-index
membership claim.

The closed subject manifest binds Git R, H and every executable member's Git
blob, SHA-256, mode and size. `run.py` loads the subject directly from Git R.
The subject captures the engine, comparator, Rust fixture, inherited helper and
supervisor, new role supervisor and recovery program from allowlisted Git blobs.
It materializes a private read-only snapshot and refuses extra, missing, linked
or nonregular members. Imports, fixture exports and recovery launch use the
captured bytes. Python runs with `-I -S`; no checkout file is imported or reopened
as executable code after preflight. The inherited launch methods are overridden
by the literal role launcher in this packet.

The build has two sequential phases with two Cargo jobs, 2 CPU, 3 GiB memory,
equal total memory-swap, 1024 PIDs, non-root user, read-only root, all capabilities
dropped and no-new-privileges. Cargo uses a fresh home, lock, public archive and
binary. The public sparse index, lock, archive and VCS commit must agree. Fetch
alone has the default network. Compilation is offline, with source mounted
read-only and only Cargo cache and target output writable. No prior acceptance
binary, lock or registry cache is reused.

Runtime starts after all build containers are absent. Loopback replay runs alone.
The PHP case has at most five roles:

| Role | CPU | Memory and equal total swap | PIDs | User |
| --- | ---: | ---: | ---: | --- |
| MySQL | 1 | 1280 MiB | 512 | 0:0, image drops to service user |
| Redis | 0.25 | 128 MiB | 256 | 0:0, image drops to service user |
| HTTP | 0.75 | 768 MiB | 256 | 0:0, image starts its service processes |
| Queue | 0.5 | 512 MiB | 256 | 1000:1000 |
| Worker and driver | 0.5 | 768 MiB | 256 | Non-root owner UID:GID |

Bootstrap uses the HTTP CPU/memory/PID profile and is removed before HTTP and
queue startup. Total runtime caps are 3 CPU/3.375 GiB. `resources.PROFILES`
literally defines capability additions for each service; all roles drop ALL
before those additions. Worker has a read-only root and read-only fixture/binary
mounts, with only task state writable. Database/cache data and PHP cached
configuration use bounded tmpfs. Image volume declarations must be fully covered.

Each role is created and started while its inline captured supervisor waits for
a private ready marker. Before releasing that gate, the owner independently reads
back image/content/platform, resource and swap/PID caps, user, root mode,
capabilities, init/security settings, exact command/deadline, environment,
network/internal ownership and mount destinations/types/access. Tokens and the
Server key arrive through private stdin into role tmpfs only after readback.
No host credential, Docker socket, published port or host network is admitted.
The owner uses a fixed anonymous Docker configuration and an explicit local Unix
daemon. Before work creation or registry requests, read-only preflight refuses
any existing owner/run container, network or volume and every colliding exact name.
An uncertain query is a refusal.

One 30-minute absolute owner window includes setup, fetch, compilation, execution
and cleanup. Active work ends 60 seconds before the final ceiling. The Cell's
roles share one maximum 600-second deadline plus a 10-second TERM/KILL grace.
`run.json` is an exclusive owner-only regular single-link record binding R, H,
manifest and recovery digests, token, local daemon, root, UID/GID, isolated owner
PID/group/session, Linux boot ID and kernel start ticks, and all three deadlines.
Execution requires Linux proc authority. Identity observations must agree across
two kernel-stat reads and boot reads; recovery revalidates that tuple immediately
before each TERM or KILL. A missing or changed authority refuses the signal.
Its digest is captured outside
the work root in the private launch handoff.

Before any pull, the owner starts a detached recovery process from captured
inline bytes and requires its exact readiness receipt. At the active cutoff it
uses its captured complete authority to stop the exact owner and remove only
matching immutable container/network IDs. A later source/record drift fails the
result but cannot replace the already-captured cleanup authority. Foreign process
epochs, labels or names are refused. Interrupted Cell evidence is purged because
credential values are deliberately absent from the recovery record. Cleanup is
bounded by the final ceiling. No edit, retry, fallback or extension is included.

After source review and a separate exact execution admission, use the reviewed
inline collector below. `R`, `M`, `CHECKOUT`, fresh `W`, token `T`, local daemon
`D`, and captured `S`/`ACTIVE`/`CLEANUP` belong in the private proposal. `ACTIVE`
is `S + 1740` and `CLEANUP` is `S + 1800`. Do not substitute a mutable script path
for the captured entry point.

```sh
python3 -I -S -c '
import pathlib, re, subprocess, sys
checkout, revision = sys.argv[1:3]
if not re.fullmatch("[0-9a-f]{40}", revision):
    raise SystemExit("invalid reviewed runner")
entry = subprocess.run(["git", "--no-replace-objects", "-C", checkout, "show",
    revision + ":scripts/acceptance/condition-history/run.py"],
    check=True, capture_output=True, timeout=20).stdout.decode()
raise SystemExit(subprocess.run([sys.executable, "-I", "-S", "-c", entry,
    "run", "--checkout", checkout, "--runner-commit", revision, *sys.argv[3:]],
    start_new_session=True).returncode)
' CHECKOUT R --manifest-sha256 M \
  --package-commit 4b2440a5618ccc1c98960ea2e1e62127042c3590 \
  --work-dir W --run-token T --docker-endpoint D \
  --started-unix S --active-unix ACTIVE --cleanup-unix CLEANUP
```

The collector preserves the exact Git blob's trailing newline and starts a new
owner session. A `manifest` invocation of the same captured entry reads Git only
and prints the deterministic manifest and its digest. It does not contact Docker
or registries. The run refuses wrong H, dirty/wrong R, wrong M, existing work,
stale deadlines or a changed entry before importing the engine or creating roles.
Host aliases, live capacity samples and private paths stay out of public records.

For immediate recovery or the mandatory independent final audit, capture
`recover.py` directly from the reviewed Git R and invoke those bytes with
`python3 -I -S -c`. Supply `recover` or `audit` and all of `--work-dir`,
`--record-sha256`, `--manifest-sha256`, `--runner-commit`, `--package-commit`,
`--run-token`. Audit also requires the externally captured `--summary-sha256`.
Standalone recovery does not depend on a mutable checkout entry point.

The owner writes `summary.json` with `O_EXCL|O_NOFOLLOW`, mode 0600, and returns
its immutable SHA-256. It records package/source/lock/binary inputs, exact role
readbacks and fresh typed replay/live results. The fetch transport is the public
SDK raw control-plane method, not an independent second HTTP client. Owner checks
remain `pending_independent_audit`. Only after owner exit can a separately invoked
captured audit bind that summary digest, verify the original record/snapshot and
recovery-process receipt, and confirm every matching container, network, volume,
owner and recovery process group absent. It writes its own exclusive handoff.
Sanitize and retain both immutable results before any acceptance or closure.

The task network has an explicit local bridge/IPv4-only IPAM profile and exact
masquerade, inter-container and loopback-binding options. Driver, scope, ingress,
attachable/configuration flags, labels, IPAM and endpoints are read back before
releasing each role. This does not claim isolation from the daemon host itself.

The current shared-daemon execution proposal is held by Security's storage
finding on #210. A subsequent reviewed proposal must select a disposable
task-owned daemon/work store with a hard byte ceiling and externally owned
teardown. Capacity samples do not establish that ceiling. Neither the process
nor network source correction admits a pull, build or runtime from this packet.

The comparator scans every opened wait, condition outcome and timer row, binding
the authored group, member key/fingerprint/occurrence, wait ID and integer timeout.
Additional reopens must keep that member lineage and cannot extend its recorded
timeout. Present timer terminals require a matching earlier schedule; a superseded
orphan's timer is distinct from the current tail. Current-tail timeouts contradict
this case's required satisfied result. H's minimal two-member fixture has no timer
rows, and its direct condition outcomes do not imply an invented timer schedule.
The live consumer also checks both public parallel results are `Satisfied` before
returning `both`. These are source assertions for review, not observed live results.

Inert source checks, which do not invoke Docker or Cargo:

```sh
python3 -m unittest discover -s scripts/acceptance/condition-history -p 'test_*.py'
rustfmt --edition 2021 --check scripts/acceptance/condition-history/fixture/tests/conditions.rs
```

These check source guards, fixed fixture export and the history comparator. They
do not prove that the new Rust consumer compiles or that either live acceptance
criterion passes. Those remain subject to the reviewed bounded execution.
