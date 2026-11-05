# Disposable guest source proposal

This is a source proposal for #210, not an execution admission. No VM, disk
image, pull or consumer build has been started. Security must review the exact
source and General must admit a private invocation with fresh capacity before
any of the commands below can run. The shared-daemon proposal is superseded.

The selected backend is the already installed Lima 2.2.1 native Virtualization
framework backend on an Intel macOS 15.8.2 host. It avoids a host installation,
provider account or paid resource. An existing writable shared VM is unsuitable
because it shares the host home and owns unrelated images and caches. The two
Linux hosts have neither an installed VM launcher nor current-user KVM access.
The other native VZ host is arm64, while this acceptance pins amd64 manifests.
Private host names, cache paths, scope and capacity receipts belong in the
execution proposal rather than this repository.

## Immutable inputs and cost

| Input | Exact identity |
| --- | --- |
| `limactl` | Lima 2.2.1, 35,175,712 bytes, SHA-256 `216450fc8e3eeb7f50e3030de216c32efeef505b111ed3fb24912a36feebea49` |
| Linux amd64 guest agent gzip | 8,257,812 bytes, SHA-256 `4aa75eacc151cbf2b7aec1b9ebe3886a0fc3268977c108308c802ce267c76959` |
| Uncompressed guest agent | SHA-256 `d799765623d5fa3b52ac30dd437b8f867968aeac594933c8ddc3bf1ac78cb26a` |
| Cached guest image | [colima-core v0.10.4 Ubuntu 24.04 Docker amd64 raw gzip](https://github.com/abiosoft/colima-core/releases/download/v0.10.4/ubuntu-24.04-minimal-cloudimg-amd64-docker.raw.gz), 358,298,593 bytes, SHA-256 `4cd967d2c58aa6971621343d372255b14ffbfe28a185cda0e3c34ac8a1eb15e6` |
| Native tools | macOS 15.8.2 build 24H232 `hdiutil`, `diskutil`, `sysctl`, public `libproc` process information |
| Host guardian | Captured `guest-host.py` Git blob at admitted R, SHA-256 in the R manifest and private authority |
| Guest profile | Complete `guest-profile.json` at R, with its one image path replaced by the task-local path |

The cache was hashed read-only and matches the public release asset digest and
size. No download was needed. The release source builds Docker 29.5.2, but that
is an expectation to verify inside the fresh guest, not an observed engine
receipt. Python 3, Git, Docker, a block-backed ext4 root and an empty daemon/cache
are required. Missing dependencies cause refusal without package installation.

The bounded alternative costs **zero provider dollars**, one 4-vCPU / 6-GiB
guest and one fixed **32-GiB** read/write UDIF containing the complete fresh Lima
home, inputs, VM disk, generated SSH keys, temporary files and logs. The guest
disk is **20 GiB**, including its OS, Cargo cache, work, Docker/containerd stores,
images and logs. Its block-device and filesystem ceilings are checked before
registry requests and again before daemon mutations. Space exhaustion fails the
single attempt; there is no enlargement, retry, shared-cache reuse or pruning.
Only two sanitized guest results, each at most 8 MiB, plus bounded private host
authority/receipt files are exported outside the UDIF. Native control output is
held in memory with a combined 2-MiB ceiling and never streamed to unbounded logs.

Admission requires at least **10 GiB available RAM** and **48 GiB free host disk**,
fresh metrics, and agreement from the owners of any existing shared VM. The
observed host had about 6.5 GiB available, so it did not pass. A coordinated idle
window could release the existing 6-GiB shared VM allocation; this proposal does
not authorize stopping it. The fixed disk is additional to that shared VM, which
is never an acceptance input. No global daemon setting or host package changes.

The parent invocation starts at S, active work ends at S+1740 seconds, and all
parent cleanup ends by S+1800. Those ceilings never move. Setup captures their
monotonic counterparts once; the schema-v2 host authority persists them across
setup/guard and later readback rather than recomputing a lease from wall time.
Setup consumes the same window and must finish within 300 seconds. Host guardian timers use monotonic time once the externally captured
absolute cutoffs are bound. The existing guest Cell keeps its 600-second bound,
two Cargo jobs, sequential build/runtime phases and per-role caps.

## Launch and custody stages

`guest-host.py plan --plan-input PRIVATE_JSON` is inert. Its closed template check
produces the exact native argument vectors, environment, rendered profile digest
and resource/deadline budgets. It accepts private canonical parent P named
`sr-T`, where T is a fresh 16-hex token. Its Lima home is `P/v/l`, guest name
`sdk-rust-T`, gzip `P/v/a/guest.raw.gz` and fixed image `P/store.dmg`. The short
private path must satisfy Lima's Unix socket limit. No shell interpolation is
used for native controls.

1. Before the first mutation, the external owner captures R/M, S and both
   cutoffs, input/tool hashes, architecture, OS, native Python version, free space
   and current capacity. Write an exclusive 0600 setup intent outside the fresh P,
   schema `durable-workflow.condition-host-setup/v1`, with exactly `INTENT_KEYS`
   from the captured source. It binds the guardian SHA, R/M, host boot UUID,
   token, S/cutoffs, scope, launcher/agent and already cached public image paths.
   Capture its digest externally. Start the captured guardian in a detached new
   session with native Python `-I -S -c`, mode `prepare`, `--record INTENT`,
   `--record-sha256 INTENT_SHA` and `--profile CAPTURED_PROFILE`. No mutable
   checkout executable is loaded. This resident process is the setup watchdog;
   it owns the first native disk mutation and remains the guardian afterward.
2. The watchdog verifies the closed intent, pinned template and all three public
   input hashes, native Intel architecture, exact OS and fresh RAM/disk gates.
   Insufficient capacity refuses before even creating P. It creates P exclusively
   at 0700 and writes `setup-ready.json` with its kernel epoch and capacity.
   Only its plan's `hdiutil create -sectors 67108864 -type UDIF -layout
   GPTSPUD -fs APFS -nospotlight` and `attach -nobrowse -owners on -mountpoint P/v
   -plist` commands run. Native command output has the hard in-memory ceiling;
   create/attach are each additionally bounded by the remaining 300-second setup
   cutoff. Setup temporary files stay in P until the fixed filesystem is mounted,
   then all VM/input/cache/tmp/log writes use that filesystem. Capture image
   device/inode, the whole `/dev/diskN`, APFS volume
   UUID and the 32-GiB whole-device readback. Refuse an existing image, attachment,
   mount, Lima home or instance. Copy the already verified public gzip into the
   store without networking, reopen/hash the destination through a no-follow
   single-link owner file descriptor, and bind its device/inode/size/SHA. Recheck
   destination identity and bytes at consumption. Write the
   rendered JSON-as-YAML profile exclusively. No private host source or credential
   tree is copied. The installed public guest agent remains a pinned read-only
   input. Local image input and disabled containerd downloads avoid Lima's global
   downloader cache. Native readbacks must identify exactly one 32-GiB device,
   one matching image attachment and an APFS volume at the exact mount point.
3. The resident watchdog writes an exclusive 0600 authority record with schema
   `durable-workflow.condition-host/v1`. Its exact fields are `AUTHORITY_KEYS` in
   `guest-host.py`: R/M, guardian digest, token, host boot UUID, S/cutoffs, private
   scope and launcher/agent paths, image device/inode, whole device, volume UUID
   rendered profile/digest, copied image identity and uncompressed guest-agent
   digest. Its SHA-256 is captured outside the store.
4. The same process verifies its new record and becomes the active guardian
   without a process or custody gap. Standalone `guard` mode can only consume an
   already admitted complete authority record. The guardian verifies
   launcher/agent digests, host boot, authority, image inode/geometry, attachment
   and profile. Its exclusive `host-ready.json` binds authority/source and the
   guardian's kernel process epoch. Only after independent readiness verification
   may the external owner write an exclusive private `launch.json` with exactly
   `record_sha256`, `manifest_sha256` and `action: "create-and-start"`. This request
   is part of the separately admitted invocation. The resident guardian owns
   both create and start. It revalidates the copied image and selected agent,
   then passes the captured profile bytes on stdin to `limactl create --name NAME
   -`; no mutable YAML pathname supplies executable settings. It records and
   checks `limactl list --json --all-fields NAME` before start, including image,
   resource, network, mount, SSH, forwarding, environment and provision settings.
   Immediately before its own start call it repeats the input/config checks.
   Setup/template file edits cannot replace the captured create input.
5. Lima receives a fresh `LIMA_HOME`, a minimal environment, and no SSH agent,
   Docker context, proxy or cloud environment. Its host HOME remains the actual
   account home, with host SSH-key loading disabled. The complete profile has no
   base template, host mount, copied CA, copied host file, package install, service
   forward or nested virtualization. VZ NAT permits public package/image fetches.
   Only Lima's generated guest-control SSH connection is permitted. It does not
   forward host credentials. `hostResolver` and proxy propagation are disabled.
6. The guardian captures the task instance's `ha.pid` only after binding its
   scoped hostagent command and native kernel process epoch. Public
   `proc_pidinfo(PROC_PIDTBSDINFO)` provides start seconds **and microseconds**,
   UID, PID and group; consistent double reads plus host boot UUID are required.
   There is no `ps lstart` signal authority. Revalidate immediately before scoped
   `limactl stop --force`. The guardian never sends a numeric host kill signal to
   a recovered PID. Missing, changed or foreign authority is a failed result
   requiring the external owner, not permission to stop another instance.
   The supported Lima 2.2.1 selection is causal: the canonical launcher resolves
   `<PREFIX>/share/lima/lima-guestagent.Linux-x86_64.gz` through
   `usrlocal.GuestAgentBinary`, and `instance.Start` passes it as `--guestagent`
   to hostagent. The source refuses another correctly hashed path, and the
   independent hostagent command readback must name this exact selected path.
   Primary source: [agent selection](https://github.com/lima-vm/lima/blob/v2.2.1/pkg/usrlocal/usrlocal.go)
   and [start argument construction](https://github.com/lima-vm/lima/blob/v2.2.1/pkg/instance/start.go).
7. After start, the guardian repeats effective configuration readback and runs
   a captured read-only probe over Lima's generated SSH control connection. In
   the disposable guest only, passwordless sudo lets the probe hash the unique
   running guest agent's `/proc/PID/exe`, with consistent Linux boot/start ticks.
   That kernel executable hash must match the pinned uncompressed agent above.
   A path declaration alone cannot authorize the receipt. Exclusive
   `vm-started.json` binds the actual guest boot, agent hash, hostagent epoch and
   effective configuration; no SDK consumer is released before this receipt.
   These probes are source under review and have not run in a guest.
   Independently obtain B over the generated control path and match that receipt.
   Transfer a clean public R checkout with the immutable annotated H tag. Then
   write exclusive `guest-clock-request.json` with exactly `record_sha256` and
   `action: "capture-guest-clock"`. The resident guardian alone samples its
   original monotonic remaining budget around a captured read-only guest
   Unix/monotonic/boot probe. Both observation intervals must be at most five
   seconds. Its exclusive `guest-clock.json` binds those domains and the host
   authority digest. The guardian retains that receipt's digest in memory and
   refuses a finish handoff if the file changes. Its final cleanup receipt
   carries the same digest for independent host audit. A setup/launch with less than 600 seconds of work plus
   180 seconds of cleanup/audit reserve refuses before allocation or start.
   Capture fresh guest G and invoke the README entry with B and this exact
   parent clock JSON/digest. Guest active is `min(G+1740, mapped_parent_active-180)`
   and guest cleanup is active+60. The guest requires at least 600 active seconds
   before its work allocation. Independent audit/export keeps at least 120 seconds
   before parent active. Delaying setup or choosing fresh G never grants a new
   parent envelope. The run, recovery, summary and audits bind the same clock.
   Command/recovery cutoffs use its conservative guest monotonic mapping. No host source
   mount or credential is used. The guest requires an empty daemon/build cache
   and ext4 root/work/Docker data on its fixed block device before pulling.
   Existing Linux boot/start-tick recovery and full internal-network readbacks
   are preserved. Fetch is public/anonymous; credential tests use synthetic values.
8. After guest owner and recovery exit, independently run the captured guest
   audit, require the sanitized summary and its exact SHA, and export only the
   two bounded JSON results to P. An external exclusive `exports.json` binds the
   host record digest, independently observed `guest_boot_id` and both file
   digests. An exclusive `finish.json` binds the host record and exact exports
   digest. A missing, invalid or changed handoff cannot produce acceptance.
9. At a valid finish request or the unchanged active cutoff, the guardian stops
   only the bound scoped instance, confirms the hostagent absent, deletes the
   scoped instance, rechecks the enclosing image, detaches its bound whole device,
   confirms the mount absent and unlinks only the original image inode. Images,
   caches, work, logs and ephemeral keys disappear together. Cleanup never prunes
   a shared daemon. It writes exclusive `host-cleanup.json` outside the disk.
10. After guardian exit, run the same captured source in independent `audit`
    mode. Require image/mount/instance absence, guardian and VM hostagent absence,
    exact host custody/source receipts, R/M/B agreement, guest audit and summary
    digest agreement and equality with the resident clock receipt, including
    the original parent ceilings and both boot identities. Its exclusive
    `host-audit.json` may then record pass. A
    guest pass alone remains `pending_external_guest_teardown`.

Failure at any stage is terminal for this attempt. The resident setup watchdog
handles partial create/attach/copy failures within the unchanged cleanup cutoff.
After a lost attach response, it attempts a read-only recovery of the complete
attachment identity before authorizing detach. A failed create or uncertain
identity retains the task image explicitly; `setup-failure.json` records the
captured inode/device/volume authority it actually obtained. It never guesses
ownership or declares absence after an uncertain native result. The guardian
attempts cleanup after malformed exports or failed process observation. If a
device, inode, PID epoch or mount changed, it refuses destructive guesses and
records retained custody for the external owner, who must resolve it before any
capacity release. Exhausted time, a failed detach or a surviving process/store
is failure, even when product checks passed. A hard host reboot is an external
recovery case, not an unattended-success claim.

## Remaining review gates

The packet requires both separate execution admission and the exact private
launch request before it can create/start a guest. Setup watchdog, input
consumption and teardown source are included in the same sealed guardian. Read-only native
inspection confirmed the supported `diskutil` property names and Darwin epoch
structure. Native CLI help also confirms stdin create and effective Config
readback support; a read-only public-agent probe confirms executable-relative
selection and its uncompressed digest. Fake native controls bind
physical/synthesized devices, APFS geometry and mount/UUID authority. Actual held
source countercontrols expose destination corruption, post-ready YAML
edit/restore and an unrelated hashed agent path; candidate controls refuse them.
No new attachment, VM or guest-kernel readback is claimed.
Before execution, supply the exact private intent/command/handoff and fresh
sufficient capacity. The guest dependency assertions and all effective native
readbacks must pass on the sole admitted attempt; a missing tool or unexpected
schema fails rather than authorizing installation or a fallback. Security reviews
the full executable/storage packet;
General admits its exact R/M/command/window. Any admitted source change requires
new exact review. The old missing #191 raw capture and PHP-only #192 producer
provenance remain explicit; this guest does not alter either acceptance boundary.
