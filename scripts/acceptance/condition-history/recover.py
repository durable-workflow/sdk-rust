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
               'docker_endpoint', 'uid', 'gid', 'owner_process'}
ROLES = ('fetch', 'build', 'mysql', 'redis', 'bootstrap', 'runtime', 'queue', 'consumer')
PUBLIC_CONFIG = {'auths': {'https://index.docker.io/v1/': {}, 'ghcr.io': {}}}


def require(ok, label):
    if not ok:
        raise SystemExit(label)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def canonical(value):
    return (json.dumps(value, sort_keys=True, separators=(',', ':')) + '\n').encode()


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
    result = subprocess.run(['ps', '-p', str(pid), '-o', 'lstart='], capture_output=True, text=True, timeout=5)
    if result.returncode or not result.stdout.strip():
        return None
    return {'pid': pid, 'pgid': os.getpgid(pid), 'sid': os.getsid(pid), 'started': ' '.join(result.stdout.split())}


def group_members(pgid):
    result = subprocess.run(['ps', '-axo', 'pid=,pgid='], capture_output=True, text=True, check=True, timeout=5)
    return [int(fields[0]) for line in result.stdout.splitlines()
            if len(fields := line.split()) == 2 and int(fields[1]) == pgid]


def make_record(args, manifest_sha, recovery_sha):
    owner = process_identity(os.getpid())
    require(owner and owner['pid'] == owner['pgid'] == owner['sid'], 'isolated owner session required')
    root = pathlib.Path(args.work_dir)
    require(root.is_absolute() and root.resolve() == root, 'canonical work root required')
    return {'schema': 'durable-workflow.condition-run/v2', 'owner': PREFIX, 'token': args.run_token,
            'cells': [CELL], 'runner_commit': args.runner_commit, 'package_commit': H,
            'manifest_sha256': manifest_sha, 'recovery_sha256': recovery_sha,
            'started_unix': args.started_unix, 'active_unix': args.active_unix,
            'cleanup_unix': args.cleanup_unix, 'work_dir': str(root),
            'docker_endpoint': args.docker_endpoint, 'uid': os.getuid(), 'gid': os.getgid(),
            'owner_process': owner}


def validate_record(root, expected_record_sha, runner, manifest_sha, recovery_sha, token):
    root = pathlib.Path(root)
    require(root.is_absolute() and root.resolve() == root and not root.is_symlink(), 'redirected work root')
    info = root.lstat()
    require(stat.S_ISDIR(info.st_mode) and info.st_uid == os.getuid()
            and stat.S_IMODE(info.st_mode) == 0o700, 'wrong work root authority')
    raw = read(root / 'run.json')
    require(digest(raw) == expected_record_sha, 'run record digest mismatch')
    record = json.loads(raw)
    require(set(record) == RECORD_KEYS and record['schema'] == 'durable-workflow.condition-run/v2', 'wrong run record schema')
    require(record['owner'] == PREFIX and record['token'] == token and re.fullmatch('[0-9a-f]{16}', token)
            and record['cells'] == [CELL], 'wrong recovery ownership')
    require(record['runner_commit'] == runner and re.fullmatch('[0-9a-f]{40}', runner)
            and record['package_commit'] == H and record['manifest_sha256'] == manifest_sha
            and record['recovery_sha256'] == recovery_sha, 'wrong recovery source subject')
    require(record['work_dir'] == str(root) and record['uid'] == os.getuid() and record['gid'] == os.getgid()
            and record['docker_endpoint'].startswith('unix:///'), 'wrong recovery placement')
    require(all(type(record[k]) is int for k in ('started_unix', 'active_unix', 'cleanup_unix'))
            and record['cleanup_unix'] == record['started_unix'] + 1800
            and record['active_unix'] == record['cleanup_unix'] - 60, 'wrong recovery deadlines')
    process = record['owner_process']
    require(set(process) == {'pid', 'pgid', 'sid', 'started'} and type(process['pid']) is int
            and process['pid'] > 1 and process['pid'] == process['pgid'] == process['sid']
            and isinstance(process['started'], str) and process['started'], 'wrong captured owner process')
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
    root = pathlib.Path(record['work_dir'])
    require(json.loads(read(root / 'recovery-docker/config.json')) == PUBLIC_CONFIG, 'anonymous Docker configuration changed')
    env = {'PATH': os.environ.get('PATH', '/usr/bin:/bin'), 'HOME': str(root / 'public-home'),
           'DOCKER_CONFIG': str(root / 'recovery-docker'), 'DOCKER_HOST': record['docker_endpoint']}
    remaining = record['cleanup_unix'] - time.time()
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
        os.killpg(expected['pgid'], signal.SIGTERM)
        until = time.monotonic() + 5
        while group_members(expected['pgid']) and time.monotonic() < until:
            time.sleep(0.1)
        if group_members(expected['pgid']):
            os.killpg(expected['pgid'], signal.SIGKILL)
    require(not group_members(expected['pgid']), 'owner group survived or cannot be attributed')


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
                time.sleep(max(0, args.at - time.time()))
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
                and summary.get('manifest_sha256') == args.manifest_sha256, 'summary subject mismatch')
        remaining = {kind: objects(record, kind) for kind in ('container', 'network', 'volume')}
        require(all(v == [] for v in remaining.values()), 'independent object survivor')
        require(not group_members(record['owner_process']['pgid']), 'independent owner process survivor')
        watcher_raw = read(pathlib.Path(args.work_dir) / 'recovery-process.json')
        require(digest(watcher_raw) == summary.get('recovery_process_sha256'), 'recovery-process handoff changed')
        watcher = json.loads(watcher_raw)
        require(set(watcher) == {'source_sha256', 'process'} and watcher['source_sha256'] == source_sha,
                'wrong captured recovery-process receipt')
        process = watcher['process']
        require(set(process) == {'pid', 'pgid', 'sid', 'started'} and type(process['pid']) is int
                and process['pid'] > 1 and process['pid'] == process['pgid'] == process['sid']
                and isinstance(process['started'], str) and process['started'], 'wrong recovery-process identity')
        require(not group_members(watcher['process']['pgid']), 'independent recovery process survivor')
        require(summary.get('checks_outcome') == 'pass', 'source checks did not pass')
        result = {'outcome': 'pass', 'record_sha256': args.record_sha256,
                  'manifest_sha256': args.manifest_sha256, 'summary_sha256': args.summary_sha256,
                  'remaining': remaining, 'owner_process_absent': True, 'recovery_process_absent': True}
        exclusive(pathlib.Path(args.work_dir) / 'independent-audit.json', canonical(result))
        print(json.dumps(result))


if __name__ == '__main__':
    main()
