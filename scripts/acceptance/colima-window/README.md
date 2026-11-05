# One-shot default-Colima handoff proposal

This source accompanies #210. It is not admitted for execution. It preserves the
separately reviewed task controller at `df617fb681958801182e67ca63e886041bec6de0`
and its 13-member manifest
`9ce029dcfb64220ff233e9f42605400ea4a73aceb598272a4034b3e8a65d6e38`.
The outer source is a separate review subject. It does not change the Linux
process epochs, guest/network controls, fixed task store or acceptance rules.

The selected existing Intel macOS/VZ host cannot currently meet the task's
10-GiB available-memory gate while its default Colima profile runs. General owns
the coordinated idle handoff and final execution decision. Security reviews the
source and exact private invocation. No service stop, launchd bootstrap, VM,
disk allocation, pull or consumer build follows from these source tests.

Run the controls from the repository root:

```sh
python3 -B -m unittest discover -s scripts/acceptance/colima-window -p 'test_*.py'
```

The tests use synthetic native responses and disposable ordinary files. They
exercise readiness, original-state drift, owner death/PID reuse, partial stop,
restoration faults, survivors and exclusive independent auditing. They neither
contact Docker nor load/unload a service or create a VM.

## Contingent invocation and custody

A private proposal supplies the exact source digest, host boot/UID, original
LaunchAgent bytes/effective properties and arguments, six launcher hashes,
original profile/configuration hashes, original disk device/inode identities,
local Docker endpoint and idle engine ID/version/architecture/CPU/image/volume
inventory. The supported original service invocation is its existing foreground
`start -f --cpu 4 --memory 6 --disk 60 --vm-type vz --mount-type virtiofs`.
Its existing `KeepAlive={SuccessfulExit:false}` policy is preserved. Files are
never edited, disks never removed, and images/volumes never pruned. Disk contents
can change during an ordinary boot; disk identity, configurations and the engine
inventory must remain unchanged. Usable Linux memory can vary across boots and
must stay within the captured 6-GiB allocation rather than match one page count.

The proposal leaves the owner epoch null. The foreground native `owner`
invocation fills its own kernel PID/start-microsecond/boot epoch, validates the
proposal, and installs a one-shot user LaunchAgent. That controller has launchd
as its parent, `KeepAlive=false`, `LaunchOnlyOnce=true`, no inherited credentials,
and no output files beyond bounded exclusive JSON receipts. Its source, frozen
SDK source, intent and guest profile are captured arguments. It does not reopen
mutable source or watch-input files. The owner releases `go.json` only after the
actual launchd PID and kernel epoch match the controller's ready receipt.

Both initial controller bootstrap and later original-service restoration use a
held, unlinked plist descriptor. Read-only native `plutil -lint /dev/fd/N` works
on the selected host. **Actual launchd descriptor consumption is still unproven.**
The initial bootstrap must establish the independent controller before any
original-service stop. Failure has no pathname fallback and authorizes no stop.
The controller rechecks the bound tools, original service/configuration, idle
engine, VM epochs and its own ready receipt immediately before bootout/stop.

Let `S` be the final admitted outer Unix epoch. No deadline is extended:

| Stage | Absolute bound |
| --- | --- |
| Owner/watch startup | `S` through `S+30` |
| Original service stop | before `S+120` |
| Frozen task start | `S+120`, at most five seconds late |
| Frozen task active / cleanup | `S+1860` / `S+1920` |
| Original restoration | before `S+2220` |
| Controller removal / final audit | before `S+2280` |

The frozen task still owns its 300-second setup, 4-vCPU/6-GiB guest, fixed
32-GiB host store, 20-GiB guest store, fresh capacity refusal and unchanged inner
deadlines. This outer controller starts the frozen preparation as its own child,
then consumes its exact ready authority before issuing the create/start request.
No extra VM or dependency operation is added.

If the foreground owner is hard-stopped, the independent controller retains
custody. Before VM launch it may terminate only its own child and use the
complete frozen cleanup authority. After launch it lets the frozen child reach
its unchanged cleanup cutoff. Restoration requires the child, task VM, image,
mount and instance to be absent; malformed or missing authority cannot substitute
for absence. Any uncertain task cleanup leaves the original service stopped
and calls the existing host `dm` only to report that real fault. Restoration
failure is terminal and also reports the fault. There is no retry or provisioning
fallback. Host reboot or killing the independent controller is outside this
same-boot guarantee and requires operator recovery; no unconditional boot-time
restoration service is installed.

A normal restoration bootstraps the captured original plist, starts that same
service and independently reads back its effective arguments/properties, all
original file/disk bindings, idle engine fingerprint and inventories. A receipt
is written before the controller removes its own launchd registration. A
separate `audit` invocation must observe the controller process/job and task
absent and independently repeat the original-engine/file checks. Summary and
audit files are exclusive, and a self-removal failure marker refuses acceptance.
Operational window success never substitutes for the frozen SDK acceptance
summary and independent host/guest audit.

Private invocation is captured from Git, without executing checkout files:
select the reviewed outer commit's `window.py` bytes, the exact frozen commit's
`guest-host.py` bytes and `guest-profile.json`, verify their reviewed hashes,
then exec the pinned native Python with `-I -S -c OUTER_SOURCE owner --record
PRIVATE_PROPOSAL --record-sha256 PROPOSAL_SHA --sdk-code FROZEN_SOURCE --profile
PRIVATE_PROFILE`. Keep the owner foreground TTY open. General receives the exact
argument vector and private contingent record separately. Final host boot,
launcher/cache/configuration/engine identities, owner epoch, capacity and `S`
remain unbound until a coordinated admission. A stale record refuses execution.
