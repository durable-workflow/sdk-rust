"""Captured, stdlib-only cleanup and independent final audit. No checkout import."""
import argparse
import hashlib
import json
import os
import pathlib
import re
import signal
import stat
import subprocess
import sys
import time

H = '4b2440a5618ccc1c98960ea2e1e62127042c3590'
PREFIX = 'sdk-rust-condition'
CELL = 'condition-history'
RUN = 'dev.durable-workflow.acceptance.run'
OWNER = 'dev.durable-workflow.acceptance.owner'
SUBJECT = 'dev.durable-workflow.acceptance.subject'
ROLE = 'dev.durable-workflow.acceptance.role'
RECORD_KEYS = {'schema', 'owner', 'token', 'cells', 'runner_commit', 'package_commit', 'manifest_sha256',
               'recovery_sha256', 'started_unix', 'active_unix', 'cleanup_unix', 'work_dir',
               'docker_endpoint', 'uid', 'gid', 'owner_process', 'storage', 'parent_clock',
               'active_monotonic_ns', 'cleanup_monotonic_ns'}
NS = 1_000_000_000
MIN_WORK_SECONDS = 600
CLOCK_KEYS = {'schema', 'host_record_sha256', 'host_boot_id', 'guest_boot_id',
              'host_started_unix', 'host_active_unix', 'host_cleanup_unix',
              'host_active_monotonic_ns', 'host_cleanup_monotonic_ns',
              'host_before_monotonic_ns', 'host_after_monotonic_ns',
              'guest_before_monotonic_ns', 'guest_after_monotonic_ns', 'guest_unix_ns'}
STORAGE_KEYS = {'schema', 'guest_boot_id', 'device', 'block_bytes', 'filesystem_bytes', 'docker_root', 'daemon_id'}
GUEST_DISK_BYTES = 20 * 1024**3
ROLES = ('fetch', 'build', 'mysql', 'redis', 'bootstrap', 'runtime', 'queue', 'consumer')
PUBLIC_CONFIG = {'auths': {'https://index.docker.io/v1/': {}, 'ghcr.io': {}}}


def require(ok, label):
    if not ok:
        raise SystemExit(label)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def canonical(value):
    return (json.dumps(value, sort_keys=True, separators=(',', ':')) + '\n').encode()


def clock_envelope(clock, started, guest_boot):
    """Map the resident parent's remaining monotonic lease conservatively."""
    require(set(clock) == CLOCK_KEYS and clock['schema'] == 'durable-workflow.condition-parent-clock/v1',
            'wrong parent clock schema')
    integers = CLOCK_KEYS - {'schema', 'host_record_sha256', 'host_boot_id', 'guest_boot_id'}
    require(all(type(clock[k]) is int and clock[k] > 0 for k in integers)
            and type(started) is int, 'wrong clock integer authority')
    require(isinstance(clock['host_record_sha256'], str) and re.fullmatch('[0-9a-f]{64}', clock['host_record_sha256'])
            and all(isinstance(clock[k], str) and re.fullmatch('[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}', clock[k])
                    for k in ('host_boot_id', 'guest_boot_id'))
            and clock['guest_boot_id'] == guest_boot, 'wrong clock source/boot authority')
    require(clock['host_active_unix'] == clock['host_started_unix'] + 1740
            and clock['host_cleanup_unix'] == clock['host_started_unix'] + 1800
            and clock['host_cleanup_monotonic_ns'] == clock['host_active_monotonic_ns'] + 60 * NS,
            'parent clock ceilings changed')
    require(0 <= clock['host_after_monotonic_ns'] - clock['host_before_monotonic_ns'] <= 5 * NS
            and 0 <= clock['guest_after_monotonic_ns'] - clock['guest_before_monotonic_ns'] <= 5 * NS
            and clock['host_after_monotonic_ns'] < clock['host_active_monotonic_ns'],
            'clock observation is late or unbounded')
    sample = clock['guest_unix_ns']
    require(sample // NS <= started < sample // NS + 30, 'stale guest clock mapping')
    # The guest sample precedes the host response. Using the parent's remaining
    # lease at that later response can only shorten the mapped parent deadline.
    parent_active = (sample + clock['host_active_monotonic_ns'] - clock['host_after_monotonic_ns']) // NS
    active = min(started + 1740, parent_active - 180)
    cleanup = active + 60
    require(active - started >= MIN_WORK_SECONDS and cleanup <= parent_active - 120,
            'minimum guest work/audit reserve consumed')
    active_mono = clock['guest_before_monotonic_ns'] + active * NS - sample
    return {'active_unix': active, 'cleanup_unix': cleanup,
            'active_monotonic_ns': active_mono, 'cleanup_monotonic_ns': active_mono + 60 * NS}


def launch_clock(args):
    require(isinstance(args.parent_clock_json, str) and len(args.parent_clock_json.encode()) <= 8192,
            'oversize parent clock input')
    clock = json.loads(args.parent_clock_json)
    require(digest(canonical(clock)) == args.parent_clock_sha256, 'parent clock digest mismatch')
    envelope = clock_envelope(clock, args.started_unix, args.guest_boot_id)
    require(all(getattr(args, k) == envelope[k] for k in ('active_unix', 'cleanup_unix')),
            'guest deadlines differ from parent-constrained envelope')
    require(args.started_unix <= time.time() < args.started_unix + 30, 'stale launch timestamp')
    require(envelope['active_monotonic_ns'] - time.monotonic_ns() >= MIN_WORK_SECONDS * NS,
            'minimum guest work/audit reserve consumed before launch')
    args.parent_clock = clock
    for key in ('active_monotonic_ns', 'cleanup_monotonic_ns'):setattr(args, key, envelope[key])


def read(path, mode=0o600, limit=8 * 1024 * 1024):
    before = path.lstat()
    require(stat.S_ISREG(before.st_mode) and before.st_nlink == 1
            and stat.S_IMODE(before.st_mode) == mode and before.st_uid == os.getuid(), 'unsafe authority/artifact file')
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, 'rb') as stream:
        info = os.fstat(stream.fileno())
        require((info.st_dev, info.st_ino) == (before.st_dev, before.st_ino) and info.st_nlink == 1, 'authority file replaced')
        data = stream.read(limit + 1)
        after = os.fstat(stream.fileno())
        require(len(data) <= limit and after.st_nlink == 1
                and (info.st_size, info.st_mtime_ns, info.st_ctime_ns)
                == (after.st_size, after.st_mtime_ns, after.st_ctime_ns), 'authority file changed')
    return data


def exclusive(path, data):
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'wb') as stream:
        os.fchmod(stream.fileno(), 0o600)
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
        info = os.fstat(stream.fileno())
        require(info.st_nlink == 1 and stat.S_ISREG(info.st_mode) and info.st_size == len(data), 'unsafe final handoff')
    require(read(path) == data, 'handoff changed')
    return digest(data)


def process_identity(pid):
    require(sys.platform == 'linux' and type(pid) is int and pid > 1, 'Linux proc process authority required')
    boot = boot_identity()
    try:
        first = process_stat(pid)
        second = process_stat(pid)
    except FileNotFoundError:
        return None
    require(first == second and boot == boot_identity(), 'process epoch changed during observation')
    return dict(first, boot_id=boot)


def boot_identity():
    value = pathlib.Path('/proc/sys/kernel/random/boot_id').read_text().strip()
    require(re.fullmatch('[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}', value), 'invalid Linux boot authority')
    return value


def process_stat(pid):
    fd = os.open('/proc/%d/stat' % pid, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, 'rb') as stream:
        raw = stream.read(4097)
    require(len(raw) <= 4096 and raw.startswith(('%d (' % pid).encode()), 'invalid proc process authority')
    # comm may contain spaces or parentheses. Fields after its final ')' start
    # at state (field 3); starttime is field 22 in the kernel's proc ABI.
    head, separator, tail = raw.rpartition(b') ')
    fields = tail.split()
    require(separator and head and len(fields) >= 20, 'incomplete proc process authority')
    return {'pid': pid, 'pgid': int(fields[2]), 'sid': int(fields[3]), 'start_ticks': int(fields[19])}


def validate_process(process, label):
    require(set(process) == {'pid', 'pgid', 'sid', 'boot_id', 'start_ticks'}
            and all(type(process[k]) is int for k in ('pid', 'pgid', 'sid', 'start_ticks'))
            and process['pid'] > 1 and process['pid'] == process['pgid'] == process['sid']
            and process['start_ticks'] > 0
            and isinstance(process['boot_id'], str)
            and re.fullmatch('[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}', process['boot_id']), label)


def filesystem_storage(work_dir, docker_root, expected_boot):
    require(sys.platform == 'linux' and boot_identity() == expected_boot, 'wrong disposable guest boot identity')
    root = pathlib.Path('/')
    device = root.stat().st_dev
    require(os.major(device) != 0, 'block-backed disposable guest root required')
    for name in (pathlib.Path(work_dir).parent, pathlib.Path(docker_root)):
        require(name.is_absolute() and name.resolve(strict=True) == name and name.stat().st_dev == device,
                'work/daemon storage escapes guest root device')
    block_path = pathlib.Path('/sys/dev/block/%d:%d/size' % (os.major(device), os.minor(device)))
    block_bytes = int(block_path.read_text().strip()) * 512
    fs = os.statvfs(root)
    filesystem_bytes = fs.f_blocks * fs.f_frsize
    require(0 < filesystem_bytes <= block_bytes <= GUEST_DISK_BYTES, 'guest storage exceeds hard block ceiling')
    mounts = pathlib.Path('/proc/self/mountinfo').read_text().splitlines()
    root_rows = []
    for line in mounts:
        before, separator, after = line.partition(' - ')
        require(separator, 'invalid guest mount authority')
        fields, tail = before.split(), after.split()
        require(len(fields) >= 6 and len(tail) >= 3, 'invalid guest mount authority')
        require(tail[0] not in ('9p', 'virtiofs', 'nfs', 'nfs4', 'cifs', 'fuse.sshfs'), 'host/network filesystem share refused')
        if fields[4] == '/':
            root_rows.append((fields, tail))
    require(len(root_rows) == 1 and root_rows[0][1][0] == 'ext4'
            and root_rows[0][0][2] == '%d:%d' % (os.major(device), os.minor(device)), 'wrong guest root filesystem')
    return {'guest_boot_id': expected_boot, 'device': '%d:%d' % (os.major(device), os.minor(device)),
            'block_bytes': block_bytes, 'filesystem_bytes': filesystem_bytes, 'docker_root': docker_root}


def validate_storage(value, work_dir, observe=True):
    require(set(value) == STORAGE_KEYS and value['schema'] == 'durable-workflow.condition-guest-storage/v1'
            and re.fullmatch('[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}', value['guest_boot_id'])
            and re.fullmatch('[0-9]+:[0-9]+', value['device'])
            and type(value['block_bytes']) is int and type(value['filesystem_bytes']) is int
            and 0 < value['filesystem_bytes'] <= value['block_bytes'] <= GUEST_DISK_BYTES
            and isinstance(value['docker_root'], str) and value['docker_root'].startswith('/')
            and isinstance(value['daemon_id'], str) and re.fullmatch('[A-Za-z0-9:-]{12,128}', value['daemon_id']),
            'wrong bounded guest storage authority')
    if observe:
        current = filesystem_storage(work_dir, value['docker_root'], value['guest_boot_id'])
        require(all(value[k] == v for k, v in current.items()), 'guest storage authority changed')


def storage_preflight(args, command):
    info = json.loads(command(['docker', 'info', '--format', '{{json .}}']).stdout)
    frame = filesystem_storage(args.work_dir, info['DockerRootDir'], args.guest_boot_id)
    value = dict(frame, schema='durable-workflow.condition-guest-storage/v1', daemon_id=info['ID'])
    validate_storage(value, args.work_dir)
    # The public base image contains tools only. Nothing in this daemon may be
    # inherited from a previous SDK experiment or another owner.
    for line in (['docker', 'ps', '-aq'], ['docker', 'image', 'ls', '-q', '--no-trunc'], ['docker', 'volume', 'ls', '-q']):
        require(not command(line).stdout.split(), 'disposable guest daemon is not empty')
    rows = [json.loads(line) for line in command(['docker', 'system', 'df', '--format', '{{json .}}']).stdout.splitlines() if line]
    require(len(rows) == 4 and {row['Type'] for row in rows} == {'Images', 'Containers', 'Local Volumes', 'Build Cache'}
            and all((type(row['TotalCount']) is int and row['TotalCount'] == 0)
                    or (type(row['TotalCount']) is str and row['TotalCount'] == '0') for row in rows),
            'guest image/cache baseline is not empty')
    return value


def group_members(pgid):
    result = subprocess.run(['ps', '-axo', 'pid=,pgid='], capture_output=True, text=True, check=True, timeout=5)
    return [int(fields[0]) for line in result.stdout.splitlines()
            if len(fields := line.split()) == 2 and int(fields[1]) == pgid]


def make_record(args, manifest_sha, recovery_sha):
    owner = process_identity(os.getpid())
    require(owner and owner['pid'] == owner['pgid'] == owner['sid'], 'isolated owner session required')
    root = pathlib.Path(args.work_dir)
    require(root.is_absolute() and root.resolve() == root, 'canonical work root required')
    return {'schema': 'durable-workflow.condition-run/v5', 'owner': PREFIX, 'token': args.run_token,
            'cells': [CELL], 'runner_commit': args.runner_commit, 'package_commit': H,
            'manifest_sha256': manifest_sha, 'recovery_sha256': recovery_sha,
            'started_unix': args.started_unix, 'active_unix': args.active_unix,
            'cleanup_unix': args.cleanup_unix, 'work_dir': str(root),
            'docker_endpoint': args.docker_endpoint, 'uid': os.getuid(), 'gid': os.getgid(),
            'owner_process': owner, 'storage': args.guest_storage, 'parent_clock': args.parent_clock,
            'active_monotonic_ns': args.active_monotonic_ns, 'cleanup_monotonic_ns': args.cleanup_monotonic_ns}


def validate_record(root, expected_record_sha, runner, manifest_sha, recovery_sha, token):
    root = pathlib.Path(root)
    require(root.is_absolute() and root.resolve() == root and not root.is_symlink(), 'redirected work root')
    info = root.lstat()
    require(stat.S_ISDIR(info.st_mode) and info.st_uid == os.getuid()
            and stat.S_IMODE(info.st_mode) == 0o700, 'wrong work root authority')
    raw = read(root / 'run.json')
    require(digest(raw) == expected_record_sha, 'run record digest mismatch')
    record = json.loads(raw)
    require(set(record) == RECORD_KEYS and record['schema'] == 'durable-workflow.condition-run/v5', 'wrong run record schema')
    require(record['owner'] == PREFIX and record['token'] == token and re.fullmatch('[0-9a-f]{16}', token)
            and record['cells'] == [CELL], 'wrong recovery ownership')
    require(record['runner_commit'] == runner and re.fullmatch('[0-9a-f]{40}', runner)
            and record['package_commit'] == H and record['manifest_sha256'] == manifest_sha
            and record['recovery_sha256'] == recovery_sha, 'wrong recovery source subject')
    require(record['work_dir'] == str(root) and record['uid'] == os.getuid() and record['gid'] == os.getgid()
            and record['docker_endpoint'].startswith('unix:///'), 'wrong recovery placement')
    envelope = clock_envelope(record['parent_clock'], record['started_unix'], record['storage']['guest_boot_id'])
    require(all(type(record[k]) is int and record[k] == value for k, value in envelope.items()),
            'wrong recovery deadlines')
    process = record['owner_process']
    validate_process(process, 'wrong captured owner process')
    validate_storage(record['storage'], root, observe=False)
    require(process['boot_id'] == record['storage']['guest_boot_id'], 'owner/storage guest identity mismatch')
    snapshot = root / 'snapshot'
    require(not snapshot.is_symlink() and stat.S_ISDIR(snapshot.lstat().st_mode), 'unsafe recovery snapshot')
    raw_manifest = read(snapshot / 'manifest.json', mode=0o400)
    require(digest(raw_manifest) == manifest_sha, 'recovery manifest changed')
    value = json.loads(raw_manifest)
    require(set(value) == {'schema', 'runner_commit', 'package_commit', 'files'}
            and value['schema'] == 'durable-workflow.condition-subject/v1'
            and value['runner_commit'] == runner and value['package_commit'] == H, 'wrong sealed manifest authority')
    expected = value['files']
    require(expected['scripts/acceptance/condition-history/recover.py']['sha256'] == recovery_sha, 'recovery source drift')
    seen = set()
    allowed_dirs = {p.as_posix() for name in expected for p in pathlib.PurePosixPath(name).parents if str(p) != '.'}
    def unreadable(error):
        raise SystemExit('unreadable recovery snapshot')
    for parent, dirs, files in os.walk(snapshot, followlinks=False, onerror=unreadable):
        for name in dirs:
            path = pathlib.Path(parent) / name
            require(not path.is_symlink() and stat.S_ISDIR(path.lstat().st_mode)
                    and path.relative_to(snapshot).as_posix() in allowed_dirs, 'redirected or extra snapshot directory')
        for name in files:
            path = pathlib.Path(parent) / name
            relative = path.relative_to(snapshot).as_posix()
            seen.add(relative)
            if relative == 'manifest.json':
                continue
            require(relative in expected, 'unexpected recovery source member')
            data = read(path, mode=0o400)
            require(len(data) == expected[relative]['bytes'] and digest(data) == expected[relative]['sha256'], 'sealed recovery source changed')
    require(seen == set(expected) | {'manifest.json'}, 'missing recovery source member')
    return record


def docker(record, command, check=True):
    validate_storage(record['storage'], record['work_dir'])
    root = pathlib.Path(record['work_dir'])
    require(json.loads(read(root / 'recovery-docker/config.json')) == PUBLIC_CONFIG, 'anonymous Docker configuration changed')
    env = {'PATH': os.environ.get('PATH', '/usr/bin:/bin'), 'HOME': str(root / 'public-home'),
           'DOCKER_CONFIG': str(root / 'recovery-docker'), 'DOCKER_HOST': record['docker_endpoint']}
    remaining = (record['cleanup_monotonic_ns'] - time.monotonic_ns()) / NS
    require(remaining > 0, 'cleanup deadline exhausted')
    result = subprocess.run(['docker', *command], capture_output=True, text=True, timeout=min(5, remaining), env=env)
    require(not check or result.returncode == 0, 'recovery Docker query/removal failed')
    return result


def object_names(record):
    base = PREFIX + '-' + record['token']
    task = base + '-' + CELL
    # The only names this invocation can own, including partial build creates.
    return task, {r: base + '-' + r if r in ('fetch', 'build') else task + '-' + r for r in ROLES}


def objects(record, kind):
    command = {'container': ['ps', '-aq'], 'network': ['network', 'ls', '-q'], 'volume': ['volume', 'ls', '-q']}[kind]
    return docker(record, command + ['--filter', 'label=' + OWNER + '=' + PREFIX,
                                    '--filter', 'label=' + RUN + '=' + record['token']]).stdout.split()


def owned(record, kind, identifier):
    labels = '.Config.Labels' if kind == 'container' else '.Labels'
    template = '{"labels":{{json %s}},"name":{{json .Name}},"id":{{json .Id}}}' % labels
    result = docker(record, [kind, 'inspect', '--format', template, identifier], check=False)
    if result.returncode:
        require(any(x in result.stderr.lower() for x in ('no such object', 'no such container', 'no such network')), 'uncertain object ownership')
        return None
    frame = json.loads(result.stdout)
    labels, name = frame['labels'], frame['name'].removeprefix('/')
    task, names_ = object_names(record)
    role = labels.get(ROLE)
    require(labels.get(OWNER) == PREFIX and labels.get(RUN) == record['token']
            and labels.get(SUBJECT) == record['manifest_sha256'], 'foreign object authority')
    require((kind == 'network' and role == 'network' and name == task)
            or (kind == 'container' and role in names_ and name == names_[role]), 'unknown owned object name')
    require(isinstance(frame['id'], str) and re.fullmatch('[0-9a-f]{64}', frame['id']), 'invalid immutable object ID')
    return frame['id']


def cleanup(record):
    errors = []
    for kind in ('container', 'network'):
        try:
            identifiers = objects(record, kind)
        except BaseException:
            errors.append('query-' + kind)
            continue
        for identifier in identifiers:
            try:
                object_id = owned(record, kind, identifier)
                if object_id:
                    docker(record, ['rm', '-fv', object_id] if kind == 'container' else ['network', 'rm', object_id])
            except BaseException:
                errors.append('remove-' + kind)
    remaining = {}
    for kind in ('container', 'network', 'volume'):
        try:
            remaining[kind] = objects(record, kind)
        except BaseException:
            remaining[kind] = None
            errors.append('verify-' + kind)
    return {'pass': not errors and all(v == [] for v in remaining.values()), 'errors': errors, 'remaining': remaining}


def purge(root):
    import shutil
    parent = pathlib.Path(root) / 'cells'
    require(not parent.is_symlink(), 'redirected Cell parent')
    target = parent / CELL
    if target.is_symlink():
        target.unlink()
    elif target.exists():
        shutil.rmtree(target)
    require(not os.path.lexists(target), 'Cell purge survivor')


def stop_owner(record):
    expected = record['owner_process']
    current = process_identity(expected['pid'])
    if current is not None:
        require(current == expected, 'owner PID epoch changed; refusing foreign process')
        signal_owner(expected, signal.SIGTERM)
        until = time.monotonic() + 5
        while group_members(expected['pgid']) and time.monotonic() < until:
            time.sleep(0.1)
        if group_members(expected['pgid']):
            signal_owner(expected, signal.SIGKILL)
    require(not group_members(expected['pgid']), 'owner group survived or cannot be attributed')


def signal_owner(expected, signum):
    require(process_identity(expected['pid']) == expected, 'owner process epoch changed before signal')
    os.killpg(expected['pgid'], signum)


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=('recover', 'audit'))
    for key in ('work-dir', 'record-sha256', 'manifest-sha256', 'runner-commit', 'package-commit', 'run-token'):
        parser.add_argument('--' + key, required=True)
    parser.add_argument('--summary-sha256')
    parser.add_argument('--at', type=int)
    args = parser.parse_args(argv)
    require(args.package_commit == H, 'wrong recovery package source')
    original = sys.orig_argv
    require('-I' in original and '-S' in original and '-c' in original, 'captured recovery program required')
    source_sha = digest(original[original.index('-c') + 1].encode())
    record = validate_record(args.work_dir, args.record_sha256, args.runner_commit,
                             args.manifest_sha256, source_sha, args.run_token)
    config_dir = pathlib.Path(args.work_dir) / 'recovery-docker'
    if not config_dir.exists():
        config_dir.mkdir(mode=0o700)
        exclusive(config_dir / 'config.json', canonical(PUBLIC_CONFIG))
    require(not config_dir.is_symlink() and json.loads(read(config_dir / 'config.json')) == PUBLIC_CONFIG,
            'wrong captured recovery configuration')
    if args.mode == 'recover':
        if args.at is not None:
            exclusive(pathlib.Path(args.work_dir) / 'recovery-ready.json', canonical({
                'record_sha256': args.record_sha256, 'manifest_sha256': args.manifest_sha256,
                'recovery_sha256': source_sha, 'process': process_identity(os.getpid())}))
        root_fd = os.open(args.work_dir, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        errors = []
        try:
            if args.at is not None:
                require(args.at == record['active_unix'], 'wrong recovery wake deadline')
                time.sleep(max(0, (record['active_monotonic_ns'] - time.monotonic_ns()) / NS))
            # A later file drift fails evidence but cannot replace the captured
            # authority/code or prevent its already-authorized exact cleanup.
            try:
                validate_record(args.work_dir, args.record_sha256, args.runner_commit,
                                args.manifest_sha256, source_sha, args.run_token)
            except BaseException:
                errors.append('source-or-record-drift')
            try:
                stop_owner(record)
            except BaseException:
                errors.append('owner-process-uncertain')
            result = cleanup(record)
            try:
                import shutil
                parent = os.open('cells', os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=root_fd)
                try:
                    info = os.stat(CELL, dir_fd=parent, follow_symlinks=False)
                    if stat.S_ISLNK(info.st_mode):os.unlink(CELL, dir_fd=parent)
                    else:shutil.rmtree(CELL, dir_fd=parent)
                except FileNotFoundError:
                    pass
                finally:
                    os.close(parent)
            except FileNotFoundError:
                pass
            except BaseException:
                errors.append('cell-purge-uncertain')
            result['errors'] += errors
            result['pass'] = result['pass'] and not errors
            exclusive(pathlib.Path(args.work_dir) / 'recovery-audit.json', canonical(result))
            require(result['pass'], 'recovery incomplete')
        finally:
            os.close(root_fd)
    else:
        require(args.summary_sha256 and re.fullmatch('[0-9a-f]{64}', args.summary_sha256), 'expected immutable summary digest required')
        raw = read(pathlib.Path(args.work_dir) / 'summary.json')
        require(digest(raw) == args.summary_sha256, 'summary handoff digest mismatch')
        summary = json.loads(raw)
        require(summary.get('record_sha256') == args.record_sha256
                and summary.get('runner_commit') == args.runner_commit and summary.get('package_commit') == H
                and summary.get('manifest_sha256') == args.manifest_sha256
                and summary.get('parent_clock') == record['parent_clock'], 'summary subject mismatch')
        remaining = {kind: objects(record, kind) for kind in ('container', 'network', 'volume')}
        require(all(v == [] for v in remaining.values()), 'independent object survivor')
        require(not group_members(record['owner_process']['pgid']), 'independent owner process survivor')
        watcher_raw = read(pathlib.Path(args.work_dir) / 'recovery-process.json')
        require(digest(watcher_raw) == summary.get('recovery_process_sha256'), 'recovery-process handoff changed')
        watcher = json.loads(watcher_raw)
        require(set(watcher) == {'source_sha256', 'process'} and watcher['source_sha256'] == source_sha,
                'wrong captured recovery-process receipt')
        process = watcher['process']
        validate_process(process, 'wrong recovery-process identity')
        require(not group_members(watcher['process']['pgid']), 'independent recovery process survivor')
        require(summary.get('checks_outcome') == 'pass'
                and summary.get('outcome') == 'pending_independent_audit'
                and summary.get('cleanup') == {'pass': True, 'errors': [],
                    'remaining': {'container': [], 'network': [], 'volume': []}}
                and summary.get('cleanup_errors') == [], 'owner checks or cleanup did not pass')
        result = {'outcome': 'pending_external_guest_teardown', 'record_sha256': args.record_sha256,
                  'manifest_sha256': args.manifest_sha256, 'summary_sha256': args.summary_sha256,
                  'remaining': remaining, 'owner_process_absent': True, 'recovery_process_absent': True,
                  'storage': record['storage'], 'parent_clock': record['parent_clock']}
        exclusive(pathlib.Path(args.work_dir) / 'independent-audit.json', canonical(result))
        print(json.dumps(result))


if __name__ == '__main__':
    main()
