"""Source proposal for an external, task-scoped Lima/VZ storage guardian.

The plan is inert. Prepare/guard may allocate storage and consume an exact
private guest-launch request only after separate admission; audit is read-only.
"""
import argparse
import ctypes
import errno
import gzip
import hashlib
import json
import os
import pathlib
import plistlib
import re
import selectors
import shlex
import stat
import subprocess
import sys
import time

LIMA_SHA = '216450fc8e3eeb7f50e3030de216c32efeef505b111ed3fb24912a36feebea49'
AGENT_SHA = '4aa75eacc151cbf2b7aec1b9ebe3886a0fc3268977c108308c802ce267c76959'
AGENT_BINARY_SHA = 'd799765623d5fa3b52ac30dd437b8f867968aeac594933c8ddc3bf1ac78cb26a'
IMAGE_SHA = '4cd967d2c58aa6971621343d372255b14ffbfe28a185cda0e3c34ac8a1eb15e6'
IMAGE_BYTES = 358_298_593
STORE_BYTES = 32 * 1024**3
GUEST_BYTES = 20 * 1024**3
MEMORY_BYTES = 6 * 1024**3
PROFILE_SHA = 'fd12240c0a04fbe0614901038586a9cc411f154e11a56d6d1fcd3f4f045f0516'
AUTHORITY_KEYS = {'schema', 'token', 'runner_commit', 'manifest_sha256', 'guardian_sha256',
                  'started_unix', 'active_unix', 'cleanup_unix', 'parent', 'limactl',
                  'guest_agent', 'host_boot_id', 'image_dev', 'image_ino', 'whole_device',
                  'volume_uuid', 'profile_sha256', 'profile', 'guest_image', 'guest_agent_binary_sha256'}
INTENT_KEYS = (AUTHORITY_KEYS - {'image_dev', 'image_ino', 'whole_device', 'volume_uuid',
    'profile_sha256', 'profile', 'guest_image', 'guest_agent_binary_sha256'}) | {'cached_image'}


def require(ok, label):
    if not ok:
        raise SystemExit(label)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def canonical(value):
    return (json.dumps(value, sort_keys=True, separators=(',', ':')) + '\n').encode()


def regular(path, limit=8 * 1024**2):
    before = path.lstat()
    require(stat.S_ISREG(before.st_mode) and before.st_nlink == 1 and before.st_uid == os.getuid(), 'unsafe host handoff')
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, 'rb') as stream:
        opened = os.fstat(stream.fileno())
        require((opened.st_dev, opened.st_ino) == (before.st_dev, before.st_ino), 'host handoff replaced')
        data = stream.read(limit + 1)
        after = os.fstat(stream.fileno())
        require(len(data) <= limit and after.st_nlink == 1
                and (opened.st_size, opened.st_mtime_ns, opened.st_ctime_ns)
                == (after.st_size, after.st_mtime_ns, after.st_ctime_ns), 'host handoff changed')
    return data


def exclusive(path, value):
    data = canonical(value)
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_WRONLY, 0o600)
    with os.fdopen(fd, 'wb') as stream:
        os.fchmod(stream.fileno(), 0o600)
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    require(regular(path) == data, 'host handoff changed after write')
    return digest(data)


def paths(parent, token):
    root = pathlib.Path(parent)
    require(root.is_absolute() and root.resolve() == root and re.fullmatch('[0-9a-f]{16}', token), 'invalid host scope')
    require(root.name == 'sr-' + token and not any(ch.isspace() for ch in str(root)), 'wrong host scope name')
    home = root / 'v/l'
    name = 'sdk-rust-' + token
    require(len(str(home / name / 'ssh.sock.1234567890123456').encode()) < 104, 'Lima socket path is too long')
    return {'parent': root, 'mount': root / 'v', 'image': root / 'store.dmg',
            'home': home, 'instance': home / name, 'name': name}


def environment(record):
    scoped = paths(record['parent'], record['token'])
    return {'PATH': '/usr/bin:/bin:/usr/sbin:/sbin', 'HOME': str(pathlib.Path.home()),
            'LIMA_HOME': str(scoped['home']),
            'LIMA_SHELLENV_BLOCK': '*', 'TMPDIR': record.get('_setup_tmp', str(scoped['mount'] / 'tmp'))}


def plan(parent, token, limactl, image_path, profile):
    scoped = paths(parent, token)
    image = pathlib.Path(image_path)
    require(image == scoped['mount'] / 'a/guest.raw.gz', 'guest image is outside bounded store')
    value = json.loads(json.dumps(profile))
    # Pin the entire JSON document, including provision scripts and networking.
    expected = json.dumps(value, sort_keys=True, indent=2) + '\n'
    require(digest(expected.encode()) == PROFILE_SHA, 'unreviewed guest template')
    require(pathlib.Path(limactl).is_absolute(), 'absolute VM launcher required')
    require(value['images'] == [{'location': '__TASK_IMAGE__', 'arch': 'x86_64', 'digest': 'sha256:' + IMAGE_SHA}], 'wrong guest image profile')
    require(value['cpus'] == 4 and value['memory'] == '6GiB' and value['disk'] == '20GiB'
            and value['vmType'] == 'vz' and value['arch'] == 'x86_64', 'wrong guest resource profile')
    require(value['base'] == [] and value['mounts'] == [] and value['additionalDisks'] == []
            and value['copyToHost'] == [] and value['env'] == {} and value['propagateProxyEnv'] is False,
            'inherited inputs or host shares refused')
    require(all(value['ssh'][key] is False for key in ('loadDotSSHPubKeys', 'forwardAgent', 'forwardX11', 'forwardX11Trusted'))
            and value['containerd'] == {'system': False, 'user': False}, 'forwarded credentials or implicit runtime refused')
    require(value['portForwards'] == [{'guestIP': '0.0.0.0', 'guestIPMustBeZero': False,
        'guestPortRange': [1, 65535], 'proto': 'any', 'ignore': True}], 'guest service forwarding refused')
    value['images'][0]['location'] = str(image)
    return {'schema': 'durable-workflow.condition-guest-plan/v1', 'profile': value,
            'profile_sha256': digest(canonical(value)), 'guest_disk_bytes': GUEST_BYTES,
            'host_store_bytes': STORE_BYTES, 'guest_memory_bytes': MEMORY_BYTES, 'vcpus': 4,
            'minimum_host_available_bytes': 10 * 1024**3, 'minimum_host_free_disk_bytes': 48 * 1024**3,
            'setup_seconds': 300, 'active_seconds': 1740, 'cleanup_seconds': 1800,
            'create_store': ['/usr/bin/hdiutil', 'create', '-sectors', str(STORE_BYTES // 512),
                '-type', 'UDIF', '-layout', 'GPTSPUD', '-fs', 'APFS', '-nospotlight',
                '-volname', 'sr-' + token, str(scoped['image'])],
            'attach_store': ['/usr/bin/hdiutil', 'attach', '-nobrowse', '-owners', 'on',
                '-mountpoint', str(scoped['mount']), '-plist', str(scoped['image'])],
            'create_vm': [limactl, '--tty=false', 'create', '--name', scoped['name'], '-'],
            'start_vm': [limactl, '--tty=false', 'start', '--timeout', '5m', scoped['name']],
            'stop_vm': [limactl, '--tty=false', 'stop', '--force', scoped['name']],
            'delete_vm': [limactl, '--tty=false', 'delete', '--force', scoped['name']],
            'environment': environment({'parent': parent, 'token': token})}


def call(record, argv, input_data=None):
    remaining = min(record['_cleanup_mono'], record.get('_stage_mono', float('inf'))) - time.monotonic()
    require(remaining > 0, 'external cleanup deadline exhausted')
    # Read with a hard in-memory ceiling; native commands cannot fill host logs.
    require(input_data is None or len(input_data) <= 8192, 'oversize native input')
    child = subprocess.Popen(argv, stdin=subprocess.PIPE if input_data is not None else subprocess.DEVNULL,
                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                             env=environment(record), close_fds=True)
    data = bytearray()
    cutoff = time.monotonic() + min(record.get('_command_seconds', 20), remaining)
    try:
        if input_data is not None:
            child.stdin.write(input_data)
            child.stdin.close()
        with selectors.DefaultSelector() as selector:
            selector.register(child.stdout, selectors.EVENT_READ)
            while selector.get_map():
                require(time.monotonic() < cutoff, 'native scoped control timed out')
                for key, _ in selector.select(min(0.1, max(0, cutoff - time.monotonic()))):
                    block = os.read(key.fileobj.fileno(), 65536)
                    if not block:
                        selector.unregister(key.fileobj)
                        continue
                    require(len(data) + len(block) <= 2 * 1024**2, 'oversize native control response')
                    data.extend(block)
            child.wait(timeout=max(0.001, cutoff - time.monotonic()))
        require(child.returncode == 0, 'native scoped control failed')
        return bytes(data)
    finally:
        if child.poll() is None:
            child.kill()
            child.wait(timeout=2)
        child.stdout.close()


def boot_identity():
    value = subprocess.run(['/usr/sbin/sysctl', '-n', 'kern.bootsessionuuid'], check=True,
        capture_output=True, text=True, timeout=5).stdout.strip().lower()
    require(re.fullmatch('[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}', value), 'missing Darwin boot authority')
    return value


class BsdInfo(ctypes.Structure):
    _fields_ = [(name, ctypes.c_uint32) for name in (
        'flags', 'status', 'xstatus', 'pid', 'ppid', 'uid', 'gid', 'ruid', 'rgid', 'svuid', 'svgid', 'reserved')]
    _fields_ += [('comm', ctypes.c_char * 16), ('name', ctypes.c_char * 32)]
    _fields_ += [(name, ctypes.c_uint32) for name in ('nfiles', 'pgid', 'pjobc', 'tdev', 'tpgid')]
    _fields_ += [('nice', ctypes.c_int32), ('start_seconds', ctypes.c_uint64), ('start_microseconds', ctypes.c_uint64)]


def bsd_identity(pid):
    require(sys.platform == 'darwin', 'Darwin host guardian required')
    library = ctypes.CDLL('/usr/lib/libproc.dylib', use_errno=True)
    library.proc_pidinfo.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_uint64, ctypes.c_void_p, ctypes.c_int]
    library.proc_pidinfo.restype = ctypes.c_int
    info = BsdInfo()
    count = library.proc_pidinfo(pid, 3, 0, ctypes.byref(info), ctypes.sizeof(info))
    if count == 0 and ctypes.get_errno() == errno.ESRCH:
        return None
    require(count == ctypes.sizeof(info) and info.pid == pid and info.uid == os.getuid()
            and info.start_seconds > 0 and info.start_microseconds < 1_000_000, 'unavailable host kernel authority')
    return {'pid': pid, 'pgid': info.pgid, 'uid': info.uid,
            'start_seconds': info.start_seconds, 'start_microseconds': info.start_microseconds}


def process_identity(pid):
    boot = boot_identity()
    first, second = bsd_identity(pid), bsd_identity(pid)
    require(first == second and boot == boot_identity(), 'host process epoch changed during observation')
    return dict(first, boot_id=boot) if first else None


def authority(path, expected_sha, source_sha):
    raw = regular(path, 64 * 1024)
    require(digest(raw) == expected_sha, 'host authority digest mismatch')
    value = json.loads(raw)
    require(set(value) == AUTHORITY_KEYS and value['schema'] == 'durable-workflow.condition-host/v1', 'wrong host authority schema')
    require(value['guardian_sha256'] == source_sha and re.fullmatch('[0-9a-f]{40}', value['runner_commit'])
            and re.fullmatch('[0-9a-f]{64}', value['manifest_sha256']), 'wrong captured host source')
    require(all(type(value[k]) is int for k in ('started_unix', 'active_unix', 'cleanup_unix', 'image_dev', 'image_ino'))
            and value['active_unix'] == value['started_unix'] + 1740
            and value['cleanup_unix'] == value['started_unix'] + 1800, 'wrong host deadlines or inode authority')
    require(value['host_boot_id'] == boot_identity(), 'host boot changed')
    scoped = paths(value['parent'], value['token'])
    info = scoped['parent'].lstat()
    require(stat.S_ISDIR(info.st_mode) and info.st_uid == os.getuid() and stat.S_IMODE(info.st_mode) == 0o700,
            'wrong host parent authority')
    binary = pathlib.Path(value['limactl']).resolve(strict=True)
    require(digest(binary.read_bytes()) == LIMA_SHA, 'reviewed VM launcher changed')
    agent = pathlib.Path(value['guest_agent']).resolve(strict=True)
    require(digest(agent.read_bytes()) == AGENT_SHA, 'reviewed guest agent changed')
    selected_agent(value)
    template = json.loads(json.dumps(value['profile']))
    template['images'][0]['location'] = '__TASK_IMAGE__'
    expected_profile = plan(value['parent'], value['token'], value['limactl'],
                            str(scoped['mount'] / 'a/guest.raw.gz'), template)['profile']
    require(value['profile'] == expected_profile and digest(canonical(value['profile'])) == value['profile_sha256']
            and value['guest_agent_binary_sha256'] == AGENT_BINARY_SHA, 'captured profile or agent authority mismatch')
    require(re.fullmatch('/dev/disk[0-9]+', value['whole_device'])
            and re.fullmatch('[0-9A-Fa-f-]{36}', value['volume_uuid'])
            and re.fullmatch('[0-9a-f]{64}', value['profile_sha256']), 'invalid native store authority')
    value['_cleanup_mono'] = time.monotonic() + max(0, value['cleanup_unix'] - time.time())
    value['_active_mono'] = time.monotonic() + max(0, value['active_unix'] - time.time())
    return value


def verify_store(record, profile_required=True):
    scoped = paths(record['parent'], record['token'])
    info = scoped['image'].lstat()
    require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and info.st_uid == os.getuid()
            and (info.st_dev, info.st_ino) == (record['image_dev'], record['image_ino'])
            and 0 < info.st_size <= STORE_BYTES + 2 * 1024**2, 'fixed host image changed')
    entries = plistlib.loads(call(record, ['/usr/bin/hdiutil', 'info', '-plist']))['images']
    matches = [entry for entry in entries if pathlib.Path(entry['image-path']).resolve() == scoped['image']]
    require(len(matches) == 1, 'host image attachment is missing or ambiguous')
    entities = matches[0]['system-entities']
    require(any(entry.get('dev-entry') == record['whole_device'] for entry in entities)
            and any(entry.get('mount-point') == str(scoped['mount']) for entry in entities), 'foreign host image attachment')
    whole = plistlib.loads(call(record, ['/usr/sbin/diskutil', 'info', '-plist', record['whole_device']]))
    volume = plistlib.loads(call(record, ['/usr/sbin/diskutil', 'info', '-plist', str(scoped['mount'])]))
    require(whole['TotalSize'] == STORE_BYTES and volume['VolumeUUID'].lower() == record['volume_uuid'].lower()
            and volume['TotalSize'] <= STORE_BYTES and volume['MountPoint'] == str(scoped['mount'])
            and volume['FilesystemType'] == 'apfs',
            'host store does not have its fixed byte boundary')
    if profile_required:
        require(scoped['home'].stat().st_dev == scoped['mount'].stat().st_dev
                and digest(regular(scoped['mount'] / 'guest.yaml')) == record['profile_sha256'],
                'reviewed guest profile changed')


def input_bytes(path, expected_size, expected_sha, destination=None, deadline=None):
    """Hash/copy only the pinned public inputs, streaming within the setup bound."""
    path = pathlib.Path(path).resolve(strict=True)
    before = path.lstat()
    require(stat.S_ISREG(before.st_mode) and before.st_nlink == 1 and before.st_size == expected_size,
            'wrong public input size or type')
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    stream = os.fdopen(fd, 'rb')
    target = None
    state, count = hashlib.sha256(), 0
    try:
        if destination:
            target = os.fdopen(os.open(destination, os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_WRONLY, 0o600), 'wb')
        with stream:
            opened = os.fstat(stream.fileno())
            require((opened.st_dev, opened.st_ino) == (before.st_dev, before.st_ino), 'public input replaced')
            while True:
                if deadline is not None:require(time.monotonic() < deadline, 'public input setup cutoff')
                block = stream.read(1024**2)
                if not block:break
                count += len(block)
                require(count <= expected_size, 'public input grew')
                state.update(block)
                if target:target.write(block)
            after = os.fstat(stream.fileno())
            require((opened.st_size, opened.st_mtime_ns, opened.st_ctime_ns)
                    == (after.st_size, after.st_mtime_ns, after.st_ctime_ns) and after.st_nlink == 1,
                    'public input changed')
        require(count == expected_size and state.hexdigest() == expected_sha, 'public input digest mismatch')
        if target:
            target.flush()
            os.fsync(target.fileno())
    finally:
        if not stream.closed:stream.close()
        if target:target.close()


def bound_input(path, expected_size, expected_sha, deadline=None):
    before = path.lstat()
    require(stat.S_ISREG(before.st_mode) and before.st_nlink == 1 and before.st_uid == os.getuid(),
            'unsafe consumed guest input')
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    state, count = hashlib.sha256(), 0
    with os.fdopen(fd, 'rb') as stream:
        opened = os.fstat(stream.fileno())
        require((opened.st_dev, opened.st_ino) == (before.st_dev, before.st_ino), 'consumed guest input replaced on open')
        while True:
            if deadline is not None:require(time.monotonic() < deadline, 'consumed input cutoff')
            block = stream.read(1024**2)
            if not block:break
            count += len(block)
            require(count <= expected_size, 'consumed guest input grew')
            state.update(block)
        after = os.fstat(stream.fileno())
        current = path.lstat()
        identity = lambda info: (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns, info.st_nlink)
        require(identity(before) == identity(opened) == identity(after) == identity(current), 'consumed guest input changed')
        require(count == expected_size and state.hexdigest() == expected_sha, 'consumed guest input digest mismatch')
    return {'device': opened.st_dev, 'inode': opened.st_ino, 'bytes': expected_size, 'sha256': expected_sha}


def selected_agent(record):
    # Lima 2.2.1 usrlocal.ShareLima/GuestAgentBinary uses this executable-relative
    # compressed path first; instance.Start passes it as --guestagent to hostagent.
    launcher = pathlib.Path(record['limactl']).resolve()
    expected = launcher.parent.parent / 'share/lima/lima-guestagent.Linux-x86_64.gz'
    require(pathlib.Path(record['limactl']) == launcher and pathlib.Path(record['guest_agent']) == expected
            and expected.resolve() == expected, 'pinned agent is not the executable-selected Lima agent')
    return expected


def agent_binary_sha(path, deadline=None):
    state, count = hashlib.sha256(), 0
    with gzip.open(path, 'rb') as stream:
        while True:
            if deadline is not None:require(time.monotonic() < deadline, 'guest agent input cutoff')
            data = stream.read(1024**2)
            if not data:break
            count += len(data)
            require(count <= 64 * 1024**2, 'oversize uncompressed guest agent')
            state.update(data)
    require(count > 0, 'empty guest agent')
    require(state.hexdigest() == AGENT_BINARY_SHA, 'uncompressed pinned guest agent changed')
    input_bytes(path, 8_257_812, AGENT_SHA, deadline=deadline)
    return state.hexdigest()


def verify_guest_inputs(record):
    scoped = paths(record['parent'], record['token'])
    identity = bound_input(scoped['mount'] / 'a/guest.raw.gz', IMAGE_BYTES, IMAGE_SHA, record['_active_mono'])
    require(identity == record['guest_image'], 'consumed image identity differs from copied destination')
    selected_agent(record)
    input_bytes(record['guest_agent'], 8_257_812, AGENT_SHA, deadline=record['_active_mono'])
    require(agent_binary_sha(record['guest_agent'], record['_active_mono']) == record['guest_agent_binary_sha256'], 'consumed guest agent changed')


def setup_intent(path, expected_sha, source_sha):
    require(stat.S_IMODE(path.lstat().st_mode) == 0o600, 'private setup intent required')
    raw = regular(path, 64 * 1024)
    require(digest(raw) == expected_sha, 'setup intent digest mismatch')
    value = json.loads(raw)
    require(set(value) == INTENT_KEYS and value['schema'] == 'durable-workflow.condition-host-setup/v1',
            'wrong setup intent schema')
    require(value['guardian_sha256'] == source_sha and re.fullmatch('[0-9a-f]{40}', value['runner_commit'])
            and re.fullmatch('[0-9a-f]{64}', value['manifest_sha256']), 'wrong setup source authority')
    require(all(type(value[k]) is int for k in ('started_unix', 'active_unix', 'cleanup_unix'))
            and value['active_unix'] == value['started_unix'] + 1740
            and value['cleanup_unix'] == value['started_unix'] + 1800
            and value['started_unix'] <= time.time() < value['started_unix'] + 300, 'wrong setup deadlines')
    require(value['host_boot_id'] == boot_identity(), 'setup host boot changed')
    scoped = paths(value['parent'], value['token'])
    require(not os.path.lexists(scoped['parent']), 'setup scope already exists')
    for key in ('limactl', 'guest_agent', 'cached_image'):
        require(pathlib.Path(value[key]).is_absolute(), 'absolute pinned input required')
    now, monotonic = time.time(), time.monotonic()
    value['_cleanup_mono'] = monotonic + max(0, value['cleanup_unix'] - now)
    value['_active_mono'] = monotonic + max(0, value['active_unix'] - now)
    value['_stage_mono'] = monotonic + max(0, value['started_unix'] + 300 - now)
    return value


def capacity(record):
    require(sys.platform == 'darwin' and os.uname().machine == 'x86_64', 'native Intel VZ host required')
    version = call(record, ['/usr/bin/sw_vers', '-productVersion']).decode().strip()
    build = call(record, ['/usr/bin/sw_vers', '-buildVersion']).decode().strip()
    require((version, build) == ('15.8.2', '24H232'), 'reviewed native OS changed')
    memory = call(record, ['/usr/bin/vm_stat']).decode()
    page = re.search(r'page size of (\d+) bytes', memory)
    require(page, 'missing host page geometry')
    available = 0
    for label in ('Pages free', 'Pages inactive', 'Pages speculative'):
        match = re.search(r'^' + label + r':\s+(\d+)\.', memory, re.MULTILINE)
        require(match, 'missing host memory readback')
        available += int(match[1]) * int(page[1])
    filesystem = os.statvfs(paths(record['parent'], record['token'])['parent'].parent)
    free = filesystem.f_bavail * filesystem.f_frsize
    require(available >= 10 * 1024**3 and free >= 48 * 1024**3, 'fresh host capacity gate refused')
    return {'available_bytes': available, 'free_disk_bytes': free, 'os': version, 'build': build}


def observe_attachment(record):
    scoped = paths(record['parent'], record['token'])
    entries = plistlib.loads(call(record, ['/usr/bin/hdiutil', 'info', '-plist']))['images']
    matches = [entry for entry in entries if pathlib.Path(entry['image-path']).resolve() == scoped['image']]
    require(len(matches) == 1, 'setup image attachment missing or ambiguous')
    entities = matches[0]['system-entities']
    require(any(e.get('mount-point') == str(scoped['mount']) for e in entities), 'setup image mount missing')
    devices = []
    for entity in entities:
        device = entity.get('dev-entry', '')
        if re.fullmatch('/dev/disk[0-9]+', device):
            info = plistlib.loads(call(record, ['/usr/sbin/diskutil', 'info', '-plist', device]))
            if info['TotalSize'] == STORE_BYTES:devices.append(device)
    require(len(devices) == 1, 'fixed setup device is ambiguous')
    volume = plistlib.loads(call(record, ['/usr/sbin/diskutil', 'info', '-plist', str(scoped['mount'])]))
    require(volume['FilesystemType'] == 'apfs' and volume['MountPoint'] == str(scoped['mount'])
            and 0 < volume['TotalSize'] <= STORE_BYTES
            and re.fullmatch('[0-9A-Fa-f-]{36}', volume['VolumeUUID']), 'wrong setup volume readback')
    return {'whole_device': devices[0], 'volume_uuid': volume['VolumeUUID']}


def remove_store(record):
    scoped = paths(record['parent'], record['token'])
    call(record, ['/usr/bin/hdiutil', 'detach', record['whole_device']])
    require(not os.path.ismount(scoped['mount']), 'owned host store remains mounted')
    info = scoped['image'].lstat()
    require((info.st_dev, info.st_ino) == (record['image_dev'], record['image_ino'])
            and info.st_nlink == 1 and stat.S_ISREG(info.st_mode), 'host image changed before deletion')
    scoped['image'].unlink()


def prepare(record, template):
    """Resident watchdog owns setup before the first disk mutation. No VM start."""
    scoped = paths(record['parent'], record['token'])
    proposal = plan(record['parent'], record['token'], record['limactl'],
                    str(scoped['mount'] / 'a/guest.raw.gz'), template)
    observed = capacity(record)
    selected_agent(record)
    for key, size, sha in (('limactl', 35_175_712, LIMA_SHA), ('guest_agent', 8_257_812, AGENT_SHA),
                           ('cached_image', IMAGE_BYTES, IMAGE_SHA)):
        input_bytes(record[key], size, sha, deadline=record['_stage_mono'])
    scoped['parent'].mkdir(mode=0o700)
    record['_setup_tmp'] = str(scoped['parent'])
    exclusive(scoped['parent'] / 'setup-ready.json', {'guardian_sha256': record['guardian_sha256'],
        'runner_commit': record['runner_commit'], 'manifest_sha256': record['manifest_sha256'],
        'process': process_identity(os.getpid()), 'capacity': observed})
    attached = False
    try:
        record['_command_seconds'] = 180
        call(record, proposal['create_store'])
        info = scoped['image'].lstat()
        require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and info.st_uid == os.getuid()
                and 0 < info.st_size <= STORE_BYTES + 2 * 1024**2, 'wrong newly created store')
        record.update(image_dev=info.st_dev, image_ino=info.st_ino)
        scoped['mount'].mkdir(mode=0o700)
        call(record, proposal['attach_store'])
        record.update(observe_attachment(record))
        attached = True
        verify_store(record, profile_required=False)
        for directory in ('a', 'l', 'tmp'):(scoped['mount'] / directory).mkdir(mode=0o700)
        record.pop('_setup_tmp')
        input_bytes(record['cached_image'], IMAGE_BYTES, IMAGE_SHA, scoped['mount'] / 'a/guest.raw.gz', record['_stage_mono'])
        record['guest_image'] = bound_input(scoped['mount'] / 'a/guest.raw.gz', IMAGE_BYTES, IMAGE_SHA, record['_stage_mono'])
        record['guest_agent_binary_sha256'] = agent_binary_sha(record['guest_agent'], record['_stage_mono'])
        record['profile'] = proposal['profile']
        record['profile_sha256'] = exclusive(scoped['mount'] / 'guest.yaml', proposal['profile'])
        require(record['profile_sha256'] == proposal['profile_sha256'], 'rendered setup template changed')
        value = {key: record[key] for key in AUTHORITY_KEYS if key != 'schema'}
        value['schema'] = 'durable-workflow.condition-host/v1'
        record_sha = exclusive(scoped['parent'] / 'host-record.json', value)
        verify_store(record)
        require(time.monotonic() < record['_stage_mono'], 'setup window exhausted')
        for key in ('_stage_mono', '_command_seconds'):record.pop(key, None)
        return value, record_sha
    except BaseException as error:
        # Failed native create/attach may not return enough identity to authorize
        # destructive recovery. Uncertain objects remain explicit external custody.
        record.pop('_stage_mono', None)
        record['_command_seconds'] = 20
        disposition = 'retained_uncertain_task_store'
        if not attached and 'image_ino' in record:
            try:
                record.update(observe_attachment(record))
                attached = True
            except BaseException:
                pass
        if attached:
            try:
                verify_store(record, profile_required=False)
                remove_store(record)
                disposition = 'owned_store_removed'
            except BaseException:
                pass
        exclusive(scoped['parent'] / 'setup-failure.json', {'outcome': 'fail',
            'error_type': type(error).__name__, 'custody': disposition,
            'runner_commit': record['runner_commit'], 'manifest_sha256': record['manifest_sha256'],
            'store_authority': {key: record[key] for key in ('image_dev', 'image_ino', 'whole_device', 'volume_uuid') if key in record}})
        raise


def capture_agent(record):
    scoped = paths(record['parent'], record['token'])
    pid_path = scoped['instance'] / 'ha.pid'
    if not pid_path.exists():
        return None
    pid_text = regular(pid_path, 64).strip()
    require(pid_text.isdigit() and int(pid_text) > 1, 'invalid owned VM PID file')
    process = process_identity(int(pid_text))
    require(process and process['start_seconds'] >= record['started_unix'], 'stale VM process epoch')
    command = call(record, ['/bin/ps', '-ww', '-p', pid_text.decode(), '-o', 'command=']).decode()
    argv = shlex.split(command)
    binding = '--pidfile=' + str(pid_path)
    pairs = list(zip(argv, argv[1:]))
    require(argv and pathlib.Path(argv[0]).resolve() == pathlib.Path(record['limactl']).resolve()
            and 'hostagent' in argv and (binding in argv or ('--pidfile', str(pid_path)) in pairs)
            and ('--guestagent', str(selected_agent(record))) in pairs
            and scoped['name'] in argv and process_identity(process['pid']) == process, 'foreign VM host agent')
    return process


def export_handoff(record, record_sha):
    scoped = paths(record['parent'], record['token'])
    raw = regular(scoped['parent'] / 'exports.json', 64 * 1024)
    value = json.loads(raw)
    require(set(value) == {'record_sha256', 'guest_boot_id', 'guest-summary.json', 'guest-audit.json'}
            and value['record_sha256'] == record_sha
            and re.fullmatch('[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}', value['guest_boot_id']),
            'wrong external export authority')
    for name in ('guest-summary.json', 'guest-audit.json'):
        require(digest(regular(scoped['parent'] / name)) == value[name], 'exported guest proof changed')
    return value, digest(raw)


def finish_requested(record, record_sha):
    scoped = paths(record['parent'], record['token'])
    marker = scoped['parent'] / 'finish.json'
    if not os.path.lexists(marker):
        return False
    value = json.loads(regular(marker, 64 * 1024))
    _, export_sha = export_handoff(record, record_sha)
    require(value == {'record_sha256': record_sha, 'exports_sha256': export_sha}, 'wrong cleanup request authority')
    return True


def effective_config(record, status):
    scoped = paths(record['parent'], record['token'])
    value = json.loads(call(record, [record['limactl'], 'list', '--json', '--all-fields', scoped['name']]))
    require(value['name'] == scoped['name'] and value['dir'] == str(scoped['instance'])
            and value['status'] == status and not value.get('errors')
            and value['vmType'] == 'vz' and value['arch'] == 'x86_64' and value['cpus'] == 4
            and value['memory'] == MEMORY_BYTES and value['disk'] == GUEST_BYTES,
            'effective VM identity or resources changed')
    def contains(actual, expected):
        if expected == [] or expected == {}:
            return actual is None or actual == expected
        if isinstance(expected, dict):
            return isinstance(actual, dict) and all(k in actual and contains(actual[k], v) for k, v in expected.items())
        if isinstance(expected, list):
            return isinstance(actual, list) and len(actual) == len(expected) and all(contains(a, e) for a, e in zip(actual, expected))
        return type(actual) is type(expected) and actual == expected
    # base/minimum version are template loader directives; every effective
    # supplied field is checked, including mounts, images, ssh and provisioning.
    expected = {k: v for k, v in record['profile'].items() if k not in ('base', 'minimumLimaVersion')}
    config = dict(value['config'])
    config.setdefault('env', {})
    require(contains(config, expected), 'effective VM profile differs from captured configuration')
    return value


GUEST_AGENT_PROBE = '''import hashlib,json,pathlib
root=pathlib.Path('/proc');boot=(root/'sys/kernel/random/boot_id').read_text().strip();found=[]
for entry in root.iterdir():
    if not entry.name.isdigit():continue
    try:comm=(entry/'comm').read_text().strip()
    except FileNotFoundError:continue
    if comm!='lima-guestagent':continue
    try:
        first=(entry/'stat').read_text();state=hashlib.sha256();count=0
        with (entry/'exe').open('rb') as stream:
            while True:
                data=stream.read(1048576)
                if not data:break
                count+=len(data)
                if count>67108864:raise SystemExit('oversize running guest agent')
                state.update(data)
        second=(entry/'stat').read_text()
        start=lambda value:int(value[value.rfind(') ')+2:].split()[19])
        if start(first)!=start(second):raise SystemExit('guest agent epoch changed')
        found.append({'pid':int(entry.name),'start_ticks':start(second),'guest_boot_id':boot,'sha256':state.hexdigest()})
    except FileNotFoundError:raise SystemExit('guest process changed during probe')
if len(found)!=1 or (root/'sys/kernel/random/boot_id').read_text().strip()!=boot:raise SystemExit('ambiguous guest agent authority')
print(json.dumps(found[0],sort_keys=True))
'''


def launch_guest(record, record_sha):
    scoped = paths(record['parent'], record['token'])
    request = json.loads(regular(scoped['parent'] / 'launch.json', 64 * 1024))
    require(request == {'record_sha256': record_sha, 'manifest_sha256': record['manifest_sha256'], 'action': 'create-and-start'},
            'wrong exact guest launch request')
    require(not scoped['instance'].exists(), 'guest instance already exists')
    verify_store(record)
    verify_guest_inputs(record)
    # The bytes consumed by create come from captured authority, never the
    # mutable guest.yaml pathname. No external process owns create/start.
    profile = canonical(record['profile'])
    require(digest(profile) == record['profile_sha256'], 'captured consumption profile changed')
    record['_stage_mono'] = record['_active_mono']
    record['_command_seconds'] = 180
    try:
        call(record, [record['limactl'], '--tty=false', 'create', '--name', scoped['name'], '-'], profile)
        before = effective_config(record, 'Stopped')
        exclusive(scoped['parent'] / 'vm-created.json', {'record_sha256': record_sha,
            'profile_sha256': digest(profile), 'guest_image': record['guest_image'], 'effective': before})
        verify_guest_inputs(record)
        require(effective_config(record, 'Stopped') == before, 'VM config changed before guardian start')
        record['_command_seconds'] = 300
        call(record, [record['limactl'], '--tty=false', 'start', '--timeout', '5m', scoped['name']])
        after = effective_config(record, 'Running')
        process = capture_agent(record)
        require(process, 'missing consumed VM hostagent authority')
        proof = json.loads(call(record, [record['limactl'], 'shell', scoped['name'], '--',
            'sudo', '-n', 'python3', '-I', '-S', '-c', GUEST_AGENT_PROBE]))
        require(set(proof) == {'pid', 'start_ticks', 'guest_boot_id', 'sha256'}
                and type(proof['pid']) is int and proof['pid'] > 1
                and type(proof['start_ticks']) is int and proof['start_ticks'] > 0
                and proof['sha256'] == record['guest_agent_binary_sha256']
                and re.fullmatch('[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}', proof['guest_boot_id']),
                'running guest agent does not match selected pinned agent')
        exclusive(scoped['parent'] / 'vm-started.json', {'record_sha256': record_sha,
            'effective': after, 'host_process': process, 'agent': proof})
    finally:
        for key in ('_stage_mono', '_command_seconds'):record.pop(key, None)


def cleanup(record):
    scoped = paths(record['parent'], record['token'])
    verify_store(record)
    captured_path = scoped['parent'] / 'vm-process.json'
    process = json.loads(regular(captured_path)) if captured_path.exists() else capture_agent(record)
    if process:
        current = process_identity(process['pid'])
        if current:
            require(current == process and capture_agent(record) == process,
                    'VM process epoch changed before scoped stop')
            require(process_identity(process['pid']) == process, 'VM process epoch changed at scoped stop')
            call(record, [record['limactl'], '--tty=false', 'stop', '--force', scoped['name']])
            require(process_identity(process['pid']) is None, 'owned VM process survived')
    require(not (scoped['instance'] / 'ha.pid').exists(), 'VM process custody is uncertain')
    if scoped['instance'].exists():
        call(record, [record['limactl'], '--tty=false', 'delete', '--force', scoped['name']])
    require(not scoped['instance'].exists(), 'VM instance storage survived')
    verify_store(record)
    remove_store(record)
    return {'guest_absent': True, 'store_unmounted': True, 'image_absent': True,
            'image_cache_disposition': 'whole_task_guest_and_store_removed'}


def audit(record, record_sha, source_sha):
    scoped = paths(record['parent'], record['token'])
    require(not os.path.lexists(scoped['image']) and not os.path.ismount(scoped['mount'])
            and not os.path.lexists(scoped['instance']), 'external VM/store survivor')
    ready = json.loads(regular(scoped['parent'] / 'host-ready.json'))
    require(ready['record_sha256'] == record_sha and ready['source_sha256'] == source_sha
            and process_identity(ready['process']['pid']) is None, 'host guardian still active or changed')
    vm = json.loads(regular(scoped['parent'] / 'vm-process.json'))
    require(process_identity(vm['pid']) is None, 'VM host agent survived')
    result = json.loads(regular(scoped['parent'] / 'host-cleanup.json'))
    require(result['record_sha256'] == record_sha and result['guardian_sha256'] == source_sha
            and result['outcome'] == 'pending_independent_host_audit'
            and result['guest_absent'] is True and result['store_unmounted'] is True and result['image_absent'] is True,
            'external cleanup did not pass')
    exports, export_sha = export_handoff(record, record_sha)
    require(finish_requested(record, record_sha), 'completed guest export request required')
    guest = json.loads(regular(scoped['parent'] / 'guest-summary.json'))
    guest_audit = json.loads(regular(scoped['parent'] / 'guest-audit.json'))
    started = json.loads(regular(scoped['parent'] / 'vm-started.json'))
    require(guest['runner_commit'] == record['runner_commit'] and guest['manifest_sha256'] == record['manifest_sha256']
            and guest['checks_outcome'] == 'pass' and guest_audit['outcome'] == 'pending_external_guest_teardown'
            and guest['storage']['guest_boot_id'] == exports['guest_boot_id']
            and guest_audit['storage'] == guest['storage']
            and guest_audit['manifest_sha256'] == record['manifest_sha256']
            and started['record_sha256'] == record_sha
            and started['agent']['guest_boot_id'] == exports['guest_boot_id']
            and started['agent']['sha256'] == record['guest_agent_binary_sha256']
            and guest_audit['summary_sha256'] == exports['guest-summary.json'], 'guest evidence does not authorize acceptance')
    result.update(outcome='pass', exports_sha256=export_sha)
    exclusive(scoped['parent'] / 'host-audit.json', result)
    return result


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=('plan', 'prepare', 'guard', 'audit'))
    parser.add_argument('--record')
    parser.add_argument('--record-sha256')
    parser.add_argument('--plan-input')
    parser.add_argument('--profile')
    args = parser.parse_args(argv)
    if args.mode == 'plan':
        value = json.loads(pathlib.Path(args.plan_input).read_bytes())
        print(json.dumps(plan(**value), sort_keys=True))
        return
    require('-I' in sys.orig_argv and '-S' in sys.orig_argv and '-c' in sys.orig_argv, 'captured host guardian required')
    source_sha = digest(sys.orig_argv[sys.orig_argv.index('-c') + 1].encode())
    mode = args.mode
    if mode == 'prepare':
        initial = setup_intent(pathlib.Path(args.record), args.record_sha256, source_sha)
        template = json.loads(regular(pathlib.Path(args.profile), 64 * 1024))
        _, record_sha = prepare(initial, template)
        args.record = str(paths(initial['parent'], initial['token'])['parent'] / 'host-record.json')
        args.record_sha256 = record_sha
        mode = 'guard'
    record = authority(pathlib.Path(args.record), args.record_sha256, source_sha)
    scoped = paths(record['parent'], record['token'])
    if mode == 'guard':
        require(record['started_unix'] <= time.time() < record['started_unix'] + 300,
                'host setup cutoff exceeded')
        verify_store(record)
        exclusive(scoped['parent'] / 'host-ready.json', {'record_sha256': args.record_sha256,
            'source_sha256': source_sha, 'process': process_identity(os.getpid())})
        control_error = None
        launched = False
        try:
            while time.monotonic() < record['_active_mono']:
                if not launched and os.path.lexists(scoped['parent'] / 'launch.json'):
                    launch_guest(record, args.record_sha256)
                    launched = True
                process = capture_agent(record)
                receipt = scoped['parent'] / 'vm-process.json'
                if process and not receipt.exists():
                    exclusive(receipt, process)
                if finish_requested(record, args.record_sha256):
                    break
                time.sleep(0.25)
        except BaseException as error:
            # Bad handoffs or observations never bypass the cleanup attempt.
            control_error = type(error).__name__
        try:
            result = dict(cleanup(record), outcome='pending_independent_host_audit')
        except BaseException as error:
            result = {'outcome': 'fail', 'error_type': type(error).__name__,
                      'custody': 'external_owner_must_resolve_retained_task_objects'}
        if control_error:
            result.update(outcome='fail', control_error_type=control_error)
        result.update(record_sha256=args.record_sha256, guardian_sha256=source_sha)
        exclusive(scoped['parent'] / 'host-cleanup.json', result)
        require(result['outcome'] != 'fail', 'external cleanup incomplete')
    else:
        print(json.dumps(audit(record, args.record_sha256, source_sha)))


if __name__ == '__main__':
    main()
