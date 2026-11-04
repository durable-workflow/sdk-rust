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

The Server index is `sha256:3848d5d5cd1b23bd2881e3fdecea64cc44b72e6fb812e577c2db6d0872d065dd`,
source `fa3fc2f2f6b8935ea4a6eea47f6a9221e2adafff`. The MySQL and Redis manifests
were resolved from the existing installed-native pins using digest-verified public
registry index reads. Their indices remain
`sha256:6ea90827b1100f8f2ae306a539f86d2c264a26ed435a2a9f75551dd5c3aeb242`
and `sha256:858f009f9709ce576febc734aa78b8f6d624b82571f9ddb6bda4377c833b3499`.

The build uses two jobs and 3 GiB, with a fresh Cargo home, lock, downloaded crate
and test binary. The public sparse index, lock, archive and VCS commit must agree.
Only dependency resolution/fetch has network access. Compilation is offline.
There is no SDK source replacement or reuse of a prior acceptance binary/cache.

Runtime starts after all build containers are absent. Loopback replay runs alone.
The PHP case has at most five roles: MySQL 1 CPU/1.25 GiB, Redis 0.25 CPU/128 MiB,
HTTP 0.75 CPU/768 MiB, queue 0.5 CPU/512 MiB, and a combined Worker/driver
0.5 CPU/768 MiB. Bootstrap is removed before HTTP and queue startup. Total runtime
caps are 3 CPU/3.375 GiB. There is no repair or proxy role. Each role retains the
accepted installed-native supervisor, private stdin-to-tmpfs credential delivery,
dual ownership labels, evidence fence and cleanup. Database/cache data is tmpfs;
image volume declarations must be fully covered before any runtime starts.
Docker uses an empty registry configuration and the existing local Unix daemon.
No host credential, Docker socket or published port is mounted into a container.

One 30-minute absolute owner window includes setup, fetch, compilation, execution
and cleanup. Active work ends 60 seconds before the final ceiling. Each role in
the one Cell shares a maximum 600-second deadline, followed by TERM/KILL.
A detached owner recovery process is armed before pulls or build. It removes only
the exact owner/run labels and purges the interrupted Cell tree, since credential
values intentionally never enter `run.json`. It is canceled only after direct
zero-survivor readbacks. A failed attempt is retained as failed; no edit, retry,
fallback or extension is part of this command.

After the exact packet is reviewed and admitted, substitute the reviewed runner
commit R, a fresh absolute start S, an unused private work directory W and a
16-hex owner token T in this one command:

```sh
python3 scripts/acceptance/condition-history/run.py run \
  --package-commit 4b2440a5618ccc1c98960ea2e1e62127042c3590 \
  --runner-commit R --work-dir W --run-token T \
  --started-unix S --active-unix ACTIVE --cleanup-unix CLEANUP
```

`ACTIVE` must equal `S + 1740`; `CLEANUP` must equal `S + 1800`. The runner refuses
wrong H, a dirty/wrong R, an existing work directory or stale/malformed deadlines
before harness command construction or launch. Host/capacity facts and actual
private paths belong in task evidence, not public GitHub prose.

If the client dies, the same reviewed source supports immediate standalone cleanup:

```sh
python3 scripts/acceptance/condition-history/run.py recover --work-dir W --run-token T
```

The summary retains source/input/lock/package/binary identities, actual toolchain,
role ownership/resource readbacks, typed replay outputs, fresh raw live history and
an offline lineage comparator. The comparator has adversarial lineage/order/timer
controls. Its fetch transport is the public SDK raw control-plane method; it is
not an independent second HTTP client. After execution, a separate read-only owner
audit must confirm all matching containers, networks, volumes and processes absent.
Publish a sanitized immutable result before acceptance or closure.

Inert source checks, which do not invoke Docker or Cargo:

```sh
python3 -m unittest discover -s scripts/acceptance/condition-history -p test_packet.py
rustfmt --edition 2021 --check scripts/acceptance/condition-history/fixture/tests/conditions.rs
```

These check source guards, fixed fixture export and the history comparator. They
do not prove that the new Rust consumer compiles or that either live acceptance
criterion passes. Those remain subject to the reviewed bounded execution.
