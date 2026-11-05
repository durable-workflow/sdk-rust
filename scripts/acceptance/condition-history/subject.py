"""Capture one closed Git-object subject; never import executable checkout files."""
import argparse
import hashlib
import json
import os
import pathlib
import re
import stat
import subprocess
import sys
import time
import types

PREFIX = 'scripts/acceptance/condition-history/'
H = '4b2440a5618ccc1c98960ea2e1e62127042c3590'
RUNTIME_FILES = tuple(PREFIX + p for p in (
    'run.py', 'subject.py', 'engine.py', 'recover.py', 'resources.py',
    'role-supervisor.sh', 'check_history.py', 'fixture/Cargo.toml', 'fixture/tests/conditions.rs')) + (
    'scripts/acceptance/installed-native/run.py',
    'scripts/acceptance/installed-native/cell-supervisor.sh')
PACKET_FILES = {p for p in RUNTIME_FILES if p.startswith(PREFIX)} | {
    PREFIX + p for p in ('README.md', 'test_packet.py', 'test_subject.py', 'test_resources.py', 'test_audit.py')}
MODULES = {'engine': PREFIX + 'engine.py', 'resources': PREFIX + 'resources.py',
           'native': 'scripts/acceptance/installed-native/run.py', 'comparator': PREFIX + 'check_history.py',
           'recovery': PREFIX + 'recover.py'}


def require(ok, label):
    if not ok:
        raise SystemExit(label)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def canonical(value):
    return (json.dumps(value, sort_keys=True, separators=(',', ':')) + '\n').encode()


def git(checkout, *args, binary=False):
    result = subprocess.run(['git', '--no-replace-objects', '--literal-pathspecs', '-C', str(checkout), *args],
                            check=True, capture_output=True, timeout=20)
    return result.stdout if binary else result.stdout.decode().strip()


def manifest(checkout, revision):
    require(re.fullmatch('[0-9a-f]{40}', revision), 'invalid runner commit')
    listing = git(checkout, 'ls-tree', '-r', '-z', revision, '--', PREFIX, binary=True)
    found = {entry.split(b'\t', 1)[1].decode() for entry in listing.split(b'\0') if entry}
    require(found == PACKET_FILES, 'extra or missing packet Git member')
    files, captured = {}, {}
    for path in RUNTIME_FILES:
        entry = git(checkout, 'ls-tree', revision, '--', path).split()
        require(len(entry) == 4 and entry[0] in ('100644', '100755') and entry[1] == 'blob'
                and entry[3] == path, 'nonregular Git member')
        data = git(checkout, 'cat-file', 'blob', revision + ':' + path, binary=True)
        require(len(data) <= 2 * 1024 * 1024, 'oversize sealed member')
        captured[path] = data
        files[path] = {'git_blob': entry[2], 'sha256': digest(data), 'bytes': len(data), 'mode': entry[0]}
    value = {'schema': 'durable-workflow.condition-subject/v1', 'runner_commit': revision,
             'package_commit': H, 'files': files}
    return value, captured


def regular(path, limit=2 * 1024 * 1024, mode=None):
    before = path.lstat()
    require(stat.S_ISREG(before.st_mode) and before.st_nlink == 1 and before.st_uid == os.getuid(), 'nonregular, linked or foreign source/record')
    if mode is not None:
        require(stat.S_IMODE(before.st_mode) == mode, 'wrong private file mode')
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, 'rb') as stream:
        opened = os.fstat(stream.fileno())
        require((opened.st_dev, opened.st_ino) == (before.st_dev, before.st_ino)
                and stat.S_ISREG(opened.st_mode) and opened.st_nlink == 1, 'source/record changed on open')
        data = stream.read(limit + 1)
        after = os.fstat(stream.fileno())
        require(len(data) <= limit and (opened.st_size, opened.st_mtime_ns, opened.st_ctime_ns)
                == (after.st_size, after.st_mtime_ns, after.st_ctime_ns)
                and after.st_nlink == 1, 'source/record changed on read')
    return data


def exclusive(path, data, mode=0o600):
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, mode)
    with os.fdopen(fd, 'wb') as stream:
        os.fchmod(stream.fileno(), mode)
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
        info = os.fstat(stream.fileno())
        require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and info.st_size == len(data), 'unsafe exclusive handoff')
    require(regular(path, max(len(data), 1), mode) == data, 'exclusive handoff changed')
    return digest(data)


class Subject:
    def __init__(self, value, captured, root):
        self.value, self.captured, self.root = value, dict(captured), pathlib.Path(root)
        self.sha256 = digest(canonical(value))
        self.loaded = {}

    def materialize(self):
        self.root.mkdir(mode=0o700)
        for path, data in self.captured.items():
            target = self.root / path
            target.parent.mkdir(parents=True, exist_ok=True)
            exclusive(target, data, 0o400)
        exclusive(self.root / 'manifest.json', canonical(self.value), 0o400)
        for parent, directories, _ in os.walk(self.root, topdown=False):
            for directory in directories:
                (pathlib.Path(parent) / directory).chmod(0o500)
        self.root.chmod(0o500)
        self.verify()

    def verify(self):
        require(not self.root.is_symlink() and stat.S_ISDIR(self.root.lstat().st_mode), 'unsafe snapshot root')
        found = set()
        allowed_dirs = {p.as_posix() for name in self.captured for p in pathlib.PurePosixPath(name).parents if str(p) != '.'}
        def unreadable(error):
            raise SystemExit('unreadable snapshot directory')
        for parent, directories, files in os.walk(self.root, followlinks=False, onerror=unreadable):
            for name in directories:
                path = pathlib.Path(parent) / name
                require(not path.is_symlink() and stat.S_ISDIR(path.lstat().st_mode)
                        and path.relative_to(self.root).as_posix() in allowed_dirs, 'unsafe or extra snapshot directory')
            for name in files:
                path = pathlib.Path(parent) / name
                relative = path.relative_to(self.root).as_posix()
                found.add(relative)
                data = regular(path, mode=0o400)
                expected = canonical(self.value) if relative == 'manifest.json' else self.captured.get(relative)
                require(expected is not None and data == expected, 'snapshot member changed or unexpected')
        require(found == set(self.captured) | {'manifest.json'}, 'missing snapshot member')

    def read(self, path):
        self.verify()
        require(path in self.captured, 'member outside sealed subject')
        return self.captured[path]

    def module(self, name):
        require(name in MODULES, 'module outside sealed import set')
        self.verify()
        if name not in self.loaded:
            path = MODULES[name]
            module = types.ModuleType('sealed_' + name)
            module.__file__ = str(self.root / path)
            sys.modules[module.__name__] = module
            exec(compile(self.captured[path], module.__file__, 'exec'), module.__dict__)
            self.loaded[name] = module
        return self.loaded[name]

    def export(self, prefix, destination):
        self.verify()
        for name, data in self.captured.items():
            if name.startswith(prefix):
                target = destination / name.removeprefix(prefix)
                target.parent.mkdir(parents=True, exist_ok=True)
                exclusive(target, data)


def arguments(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=('run', 'recover', 'audit', 'manifest'))
    for key in ('checkout', 'runner-commit', 'package-commit'):
        parser.add_argument('--' + key, required=True)
    parser.add_argument('--manifest-sha256')
    parser.add_argument('--work-dir')
    parser.add_argument('--run-token')
    parser.add_argument('--docker-endpoint')
    parser.add_argument('--guest-boot-id')
    for key in ('started-unix', 'active-unix', 'cleanup-unix'):
        parser.add_argument('--' + key, type=int)
    parser.add_argument('--record-sha256')
    parser.add_argument('--summary-sha256')
    return parser.parse_args(argv)


def validate(args):
    require(args.package_commit == H, 'wrong fixed package source')
    require(git(args.checkout, 'rev-parse', 'HEAD') == args.runner_commit, 'wrong runner head')
    require(not git(args.checkout, 'status', '--porcelain', '--untracked-files=all'), 'runner checkout is not clean')
    require(git(args.checkout, 'rev-parse', 'refs/tags/4.12.0^{commit}') == H, 'wrong immutable package tag')
    if args.mode != 'run':
        return
    require(args.work_dir and not os.path.lexists(args.work_dir), 'work directory is not fresh')
    require(args.run_token and re.fullmatch('[0-9a-f]{16}', args.run_token), 'invalid run token')
    require(args.docker_endpoint and args.docker_endpoint.startswith('unix:///'), 'local daemon endpoint required')
    require(args.guest_boot_id and re.fullmatch('[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}', args.guest_boot_id),
            'exact disposable guest boot identity required')
    require(type(args.started_unix) is int and type(args.active_unix) is int and type(args.cleanup_unix) is int
            and args.cleanup_unix == args.started_unix + 1800
            and args.active_unix == args.cleanup_unix - 60, 'wrong absolute deadline envelope')
    require(args.started_unix <= time.time() < args.started_unix + 30, 'stale launch timestamp')


def bootstrap():
    global CURRENT, ARGS, RECORD, RECORD_SHA
    args = arguments()
    validate(args)
    value, captured = manifest(args.checkout, args.runner_commit)
    actual = digest(canonical(value))
    if args.mode == 'manifest':
        print(json.dumps({'manifest_sha256': actual, 'manifest': value}, sort_keys=True))
        return
    require(actual == args.manifest_sha256, 'wrong reviewed manifest')
    original = sys.orig_argv
    require('-I' in original and '-S' in original and '-c' in original, 'captured isolated entry point required')
    entry = original[original.index('-c') + 1].encode()
    require(digest(entry) == value['files'][PREFIX + 'run.py']['sha256'], 'mutable or changed entry point')
    sys.dont_write_bytecode = True
    if args.mode in ('recover', 'audit'):
        program = captured[PREFIX + 'recover.py'].decode()
        argv = [sys.executable, '-I', '-S', '-c', program, args.mode, '--work-dir', args.work_dir,
                '--record-sha256', args.record_sha256 or '', '--manifest-sha256', actual,
                '--runner-commit', args.runner_commit, '--package-commit', H,
                '--run-token', args.run_token or '']
        if args.summary_sha256:
            argv += ['--summary-sha256', args.summary_sha256]
        os.execv(sys.executable, argv)
    # Collision reads precede directory creation, registry work and every role.
    temporary = Subject(value, captured, pathlib.Path(args.work_dir) / 'snapshot')
    ARGS, CURRENT = args, temporary
    module = types.ModuleType('sealed_resource_preflight')
    module.__file__ = 'git:' + args.runner_commit + ':' + PREFIX + 'resources.py'
    exec(compile(captured[PREFIX + 'resources.py'], module.__file__, 'exec'), module.__dict__)
    recovery = types.ModuleType('captured_recovery_preflight')
    recovery.__file__ = 'git:' + args.runner_commit + ':' + PREFIX + 'recover.py'
    exec(compile(captured[PREFIX + 'recover.py'], recovery.__file__, 'exec'), recovery.__dict__)
    args.guest_storage = recovery.storage_preflight(args, lambda line: module.call(args, line))
    args.storage_guard = lambda: recovery.validate_storage(args.guest_storage, args.work_dir)
    module.preflight_collisions(args)
    RECORD = recovery.make_record(args, actual, value['files'][PREFIX + 'recover.py']['sha256'])
    RECORD_SHA = digest(canonical(RECORD))
    print(json.dumps({'private_launch': RECORD, 'record_sha256': RECORD_SHA}), flush=True)
    pathlib.Path(args.work_dir).mkdir(mode=0o700)
    exclusive(pathlib.Path(args.work_dir) / 'run.json', canonical(RECORD))
    for directory in ('public-home', 'public-docker'):
        (pathlib.Path(args.work_dir) / directory).mkdir(mode=0o700)
    exclusive(pathlib.Path(args.work_dir) / 'public-docker/config.json', canonical(recovery.PUBLIC_CONFIG))
    temporary.materialize()
    CURRENT.module('engine').execute(args)
