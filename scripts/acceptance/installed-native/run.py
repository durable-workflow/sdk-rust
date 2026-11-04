#!/usr/bin/env python3
"""Installed-package lifecycle acceptance of the Rust SDK against a native Server image.

Everything runs in disposable local containers on an internal Docker network,
against one exact Server image index digest and either one exact published
crate version or (pre-publication mode) the exact `cargo package` bytes of one
clean source commit. Nothing here contacts Cloud. See README.md for usage.
"""
import argparse
import base64
import contextlib
import datetime
import hashlib
import io
import json
import os
import pathlib
import re
import secrets
import shutil
import stat
import subprocess
import sys
import tarfile
import time
import tomllib
import urllib.request

HERE = pathlib.Path(__file__).resolve().parent
REPO = HERE.parents[2]
BINARY = 'installed-native-lifecycle'
# Same pins as docker-compose.local-activities.yml, for the PHP Server baseline.
MYSQL_IMAGE = 'mysql@sha256:6ea90827b1100f8f2ae306a539f86d2c264a26ed435a2a9f75551dd5c3aeb242'
REDIS_IMAGE = 'redis@sha256:858f009f9709ce576febc734aa78b8f6d624b82571f9ddb6bda4377c833b3499'
# Every container and network this harness creates carries these labels. The run
# label holds a per-invocation random token; forced cleanup removes only objects
# that carry this invocation's token.
RUN_LABEL = 'dev.durable-workflow.acceptance.run'
OWNER_LABEL = 'dev.durable-workflow.acceptance.owner'
PREFIX_PATTERN = re.compile(r'[a-z][a-z0-9-]{0,30}')
# The fixture build is bounded inside its container, independent of this client:
# timeout sends TERM at BUILD_WALL_SECONDS and KILL BUILD_KILL_AFTER_SECONDS
# later, the container exits, and the daemon's --rm removes it. The client
# timeout is longer, so the in-container bound always fires first.
BUILD_WALL_SECONDS = 3300
BUILD_KILL_AFTER_SECONDS = 60
BUILD_CLIENT_TIMEOUT = 3600
# All roles in one Cell share this absolute wall bound, including setup and
# late-starting history checks. It remains effective if this client dies.
CELL_WALL_SECONDS = 600
CELL_KILL_AFTER_SECONDS = 10
# Pre-publication mode. The release workflow packages with Rust 1.86.0, and a
# different Cargo writes different archive bytes, so packaging here must use the
# same Cargo release.
PACKAGE_NAME = 'durable-workflow'
PACKAGE_CARGO_VERSION = '1.86.0'
CRATES_IO_SOURCE = 'registry+https://github.com/rust-lang/crates.io-index'
# Files Cargo writes into the archive instead of copying them from the commit.
GENERATED_PACKAGE_FILES = ('.cargo_vcs_info.json', 'Cargo.toml')
REQUIRED_PACKAGE_FILES = ('.cargo_vcs_info.json', 'Cargo.toml', 'Cargo.toml.orig', 'Cargo.lock')
APPROVAL_SCHEMA = 'durable-workflow.rust-sdk.approved-package/v1'
APPROVAL_CELLS = ['deadline', 'lifecycle']
APPROVAL_PLATFORMS = ('linux/amd64', 'linux/arm64')
COMMIT_PATTERN = re.compile(r'[0-9a-f]{40}')
SHA256_PATTERN = re.compile(r'[0-9a-f]{64}')
# Semantic Versioning 2.0.0, ASCII only. The version names files and directories
# (durable-workflow-<version>.crate, vendor/durable-workflow-<version>/); no
# identifier can be empty, so it can never hold '/' or a '..' component.
VERSION_PATTERN = re.compile(
    r'(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)'
    r'(?:-(?:0|[1-9]\d*|\d*[A-Za-z-][0-9A-Za-z-]*)(?:\.(?:0|[1-9]\d*|\d*[A-Za-z-][0-9A-Za-z-]*))*)?'
    r'(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?', re.ASCII)


class PackageError(RuntimeError):
    """The locally packaged crate is not exactly the requested clean commit."""


def container_prefix(value):
    if not PREFIX_PATTERN.fullmatch(value):
        raise argparse.ArgumentTypeError('%r must match ^%s$' % (value, PREFIX_PATTERN.pattern))
    return value


class Run:
    """One harness invocation: owner prefix plus a high-entropy token."""

    def __init__(self, prefix, stamp=None, token=None):
        self.prefix = container_prefix(prefix)
        self.stamp = stamp or datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dt%H%M%S')
        self.token = token or secrets.token_hex(8)
        self.base = '%s-accept-%s-%s' % (self.prefix, self.stamp, self.token)
        self.cleanup_errors = []
        self.local_leftovers = []
        self.secret_values = []  # In-memory only, never part of the ownership record.

    def best_effort(self, step, name, function, *args, **kwargs):
        """Run one cleanup step; record a failure (step, object, error type only) and continue."""
        try:
            return function(*args, **kwargs)
        except Exception as error:
            self.cleanup_errors.append({'step': step, 'object': name, 'error': type(error).__name__})
            return None

    def labels(self, task):
        return ['--label', 'task=' + task, '--label', '%s=%s' % (RUN_LABEL, self.token),
                '--label', '%s=%s' % (OWNER_LABEL, self.prefix)]


def owned_id(kind, name, run):
    """The ID only when both this run's token and owner labels match."""
    labels = 'Config.Labels' if kind == 'container' else 'Labels'
    result = call(['docker', kind, 'inspect', '--format',
                   '{{.Id}} {{index .%s "%s"}} {{index .%s "%s"}}' % (labels, RUN_LABEL, labels, OWNER_LABEL),
                   name], check=False)
    if result.returncode:
        if any(missing in result.stderr.lower() for missing in ('no such object', 'no such container', 'no such network')):
            return None
        raise RuntimeError('Docker ownership query failed')
    fields = result.stdout.split()
    if len(fields) == 3 and fields[1:] == [run.token, run.prefix]:
        return fields[0]
    return None


def remove_owned_container(name, run):
    # Remove by ID, never by name, and only after the run token matched: a name
    # collision or a failed create must not touch another owner's container.
    container_id = owned_id('container', name, run)
    if container_id:
        call(['docker', 'rm', '-f', container_id])
    return container_id


def write_owner_only(path, text):
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, 'w') as handle:
        os.fchmod(handle.fileno(), 0o600)
        handle.write(text)


def remove_tree(path):
    if os.path.lexists(path):
        shutil.rmtree(path)


def redact_credentials(data, values):
    """Exact-value fence for retained diagnostics, including structured output."""
    leaked = False
    for value in values:
        needle = value.encode()
        if needle in data:
            leaked = True
            data = data.replace(needle, b'[REDACTED]')
    return data, leaked


def credential_output(run, label):
    error = {'step': 'credential-output', 'object': label, 'error': 'CredentialOutput'}
    if error not in run.cleanup_errors:
        run.cleanup_errors.append(error)


def remove_owned_network(name, run):
    network_id = owned_id('network', name, run)
    if network_id:
        call(['docker', 'network', 'rm', network_id])
    return network_id


def run_objects(kind, run):
    filters = ['--filter', 'label=%s=%s' % (RUN_LABEL, run.token),
               '--filter', 'label=%s=%s' % (OWNER_LABEL, run.prefix)]
    command = ['docker', 'ps', '-aq'] if kind == 'container' else ['docker', 'network', 'ls', '-q']
    # A failed query is uncertainty, never an empty inventory.
    return call(command + filters).stdout.split()


def recover_run(root, run, cells):
    """Recovery after client death. Recheck both labels before every removal."""
    for kind in ('container', 'network'):
        objects = run.best_effort('recovery-query', kind, run_objects, kind, run)
        for object_id in objects or []:
            def remove():
                labels = 'Config.Labels' if kind == 'container' else 'Labels'
                result = call(['docker', kind, 'inspect', '--format',
                               '{{index .%s "%s"}} {{index .%s "%s"}}' %
                               (labels, RUN_LABEL, labels, OWNER_LABEL), object_id])
                if result.stdout.split() != [run.token, run.prefix]:
                    raise RuntimeError('refusing foreign recovery object')
                command = ['docker', 'rm', '-f', object_id] if kind == 'container' else ['docker', 'network', 'rm', object_id]
                call(command)
            run.best_effort('recovery-remove', kind, remove)
    # With a dead client the secret values are intentionally unavailable. Purge
    # the complete unqualified Cell tree instead of retaining unscannable logs
    # or partially written driver/history/acknowledgement files.
    for cell in cells:
        run.best_effort('recovery-state', cell, lambda: remove_tree(confined_child(root, 'cells/' + cell)))
    containers = run.best_effort('recovery-verify', 'container', run_objects, 'container', run)
    networks = run.best_effort('recovery-verify', 'network', run_objects, 'network', run)
    for cell in cells:
        def verify():
            if os.path.lexists(confined_child(root, 'cells/' + cell)):
                raise RuntimeError('Cell survived recovery')
        run.best_effort('recovery-state-verify', cell, verify)
    return {'pass': not run.cleanup_errors and containers == [] and networks == [],
            'errors': run.cleanup_errors, 'leftover_containers': containers, 'leftover_networks': networks}


def cleanup_main(argv=None):
    parser = argparse.ArgumentParser(description='Recover one acceptance run after client death.')
    parser.add_argument('--work-dir', required=True, type=cli_path)
    parser.add_argument('--container-prefix', required=True, type=container_prefix)
    parser.add_argument('--run-token', required=True)
    args = parser.parse_args(argv)
    if not re.fullmatch('[0-9a-f]{16}', args.run_token):
        parser.error('--run-token must be the exact 16-hex run label')
    record = json.loads(confined_child(args.work_dir, 'run.json').read_text())
    expected = {'schema': 'durable-workflow.acceptance-run/v1', 'owner': args.container_prefix,
                'token': args.run_token, 'cells': record.get('cells')}
    if (record != expected or not isinstance(record['cells'], list) or not record['cells']
            or any(c not in ('lifecycle', 'deadline') for c in record['cells'])):
        raise SystemExit('work directory does not belong to the requested run')
    run = Run(args.container_prefix, token=args.run_token)
    result = recover_run(args.work_dir, run, record['cells'])
    print(json.dumps(result))
    raise SystemExit(0 if result['pass'] else 1)


def full_commit(value):
    if not COMMIT_PATTERN.fullmatch(value):
        raise argparse.ArgumentTypeError('%r must be a full lowercase 40-hex commit' % value)
    return value


def exact_sha256(value):
    if not SHA256_PATTERN.fullmatch(value):
        raise argparse.ArgumentTypeError('%r must be a lowercase 64-hex SHA-256' % value)
    return value


def cli_path(value):
    """An operator-supplied root (checkout, work dir, fixture, control manifest).

    Refuses NUL bytes, '..' components and a symbolic link at the path itself,
    then canonicalizes it. Everything the harness reads or writes below a root
    is confined to it separately (confined_child).
    """
    if not value or '\0' in value:
        raise argparse.ArgumentTypeError('path is empty or contains a NUL byte')
    if '..' in pathlib.PurePath(value).parts:
        raise argparse.ArgumentTypeError('%r contains a ".." component' % value)
    if os.path.islink(value):
        raise argparse.ArgumentTypeError('%r is a symbolic link' % value)
    return pathlib.Path(os.path.realpath(value))


def safe_relative(relative):
    """A '/'-separated relative path with no empty, '.', '..', NUL or backslash parts."""
    return (isinstance(relative, str) and relative and '\0' not in relative and '\\' not in relative
            and not os.path.isabs(relative)
            and all(part not in ('', '.', '..') for part in relative.split('/')))


def confined_child(root, relative):
    """`root`/`relative`, refusing escapes and symbolic links below canonical `root`."""
    if not safe_relative(relative):
        raise PackageError('unsafe relative path %r' % relative)
    base = os.path.realpath(root)
    current = base
    for part in relative.split('/'):
        current = os.path.join(current, part)
        if os.path.islink(current):
            raise PackageError('path %r passes through a symbolic link' % relative)
    target = os.path.realpath(os.path.join(base, relative))
    if os.path.commonpath([base, target]) != base:
        raise PackageError('path %r escapes its root' % relative)
    return pathlib.Path(target)


def exact_version(value):
    if not VERSION_PATTERN.fullmatch(value):
        raise argparse.ArgumentTypeError('%r must be an exact SemVer version' % value)
    return value


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument('--crate-version', help='exact published durable-workflow version, e.g. 4.10.3')
    source.add_argument('--package-from', type=cli_path,
                        help='pre-publication mode: a clean git checkout whose HEAD is --package-commit; '
                             'the harness packages that commit and installs only the packaged bytes')
    parser.add_argument('--package-commit', type=full_commit,
                        help='pre-publication mode: the exact release commit to package (40 hex)')
    parser.add_argument('--expect-version', type=exact_version,
                        help='pre-publication mode: required Cargo.toml version of the packaged crate')
    parser.add_argument('--expect-package-sha256', type=exact_sha256,
                        help='pre-publication mode: require the packaged .crate to have this SHA-256')
    parser.add_argument('--package-cargo-version', default=PACKAGE_CARGO_VERSION,
                        help='pre-publication mode: required `cargo -V` release in --rust-image (the release workflow\'s)')
    parser.add_argument('--server-index', required=True, help='exact image index digest, sha256:...')
    parser.add_argument('--server-image', default='ghcr.io/durable-workflow/server')
    parser.add_argument('--runtime', choices=['native', 'php'], default='native',
                        help='php starts the PHP Server process set (MySQL, Redis, queue worker, repair) as a parity baseline')
    parser.add_argument('--fixture-from', type=cli_path,
                        help='reuse a fixture directory built by an earlier run (provenance is checked again)')
    parser.add_argument('--platform', default='linux/amd64')
    parser.add_argument('--expect-server-version', help='required /api/cluster/info version, e.g. 3.0.1')
    parser.add_argument('--expect-revision', help='required org.opencontainers.image.revision label')
    parser.add_argument('--expect-release-tag', help='required dev.durable-workflow.release.tag label')
    parser.add_argument('--baselines', choices=['required', 'absent'], default='required',
                        help='whether the published crate must carry the per-major Server baseline map')
    parser.add_argument('--expected-baselines', type=json.loads, default={'2': '2.0.0', '3': '3.0.2'},
                        help='JSON map the published crate must declare when --baselines required')
    parser.add_argument('--baselines-control', type=cli_path,
                        help='a Cargo.toml that must pass the required-map check (positive control)')
    parser.add_argument('--source-tag', help='git tag the crate must match (default: the crate version)')
    parser.add_argument('--rust-image', default='rust:1.86-bookworm')
    parser.add_argument('--python-image', default='python:3.12-bookworm')
    parser.add_argument('--cells', default='lifecycle,deadline')
    parser.add_argument('--jobs', type=int, default=2, help='cargo build jobs (lab machines: keep at 2)')
    parser.add_argument('--work-dir', type=cli_path, required=True, help='new directory for build output and evidence')
    parser.add_argument('--container-prefix', type=container_prefix, default='sdkrust',
                        help='owner prefix for container, network and task names, e.g. general or security '
                             '(^%s$)' % PREFIX_PATTERN.pattern)
    args = parser.parse_args(argv)
    if not args.cells or any(c not in ('lifecycle', 'deadline') for c in args.cells.split(',')):
        parser.error('--cells must select lifecycle and/or deadline')
    if args.package_from:
        # An approval candidate is only meaningful against one exact Server release.
        required = (('--package-commit', args.package_commit), ('--expect-version', args.expect_version),
                    ('--expect-server-version', args.expect_server_version),
                    ('--expect-revision', args.expect_revision), ('--expect-release-tag', args.expect_release_tag))
        absent = [option for option, value in required if not value]
        if absent:
            parser.error('--package-from requires %s' % ', '.join(absent))
        # The approval receipt the release workflow accepts is exactly this shape.
        if not re.fullmatch(r'sha256:[0-9a-f]{64}', args.server_index):
            parser.error('--package-from requires --server-index sha256:<64 hex>')
        if args.platform not in APPROVAL_PLATFORMS:
            parser.error('--package-from requires --platform in %s' % sorted(APPROVAL_PLATFORMS))
        if sorted(args.cells.split(',')) != APPROVAL_CELLS:
            parser.error('--package-from requires --cells lifecycle,deadline')
        if not COMMIT_PATTERN.fullmatch(args.expect_revision):
            parser.error('--package-from requires a 40-hex --expect-revision')
        if not VERSION_PATTERN.fullmatch(args.expect_server_version) or \
                args.expect_release_tag != args.expect_server_version:
            parser.error('--package-from requires --expect-release-tag equal to a SemVer --expect-server-version')
        for option, value in (('--fixture-from', args.fixture_from), ('--source-tag', args.source_tag)):
            if value:
                parser.error('%s cannot be combined with --package-from' % option)
    else:
        for option, value in (('--package-commit', args.package_commit), ('--expect-version', args.expect_version),
                              ('--expect-package-sha256', args.expect_package_sha256)):
            if value:
                parser.error('%s requires --package-from' % option)
    return args


def call(command, timeout=120, check=True, **kwargs):
    kwargs.setdefault('text', True)
    result = subprocess.run(command, capture_output=True, timeout=timeout, **kwargs)
    if check and result.returncode:
        raise RuntimeError('%s failed (%s): %r' % (' '.join(command[:4]), result.returncode, result.stderr[-2000:]))
    return result


def sha256(path):
    return hashlib.sha256(pathlib.Path(path).read_bytes()).hexdigest()


def image_identity(image):
    data = json.loads(call(['docker', 'image', 'inspect', image]).stdout)[0]
    # With the containerd image store, Id is the digest the image was pulled by
    # (index or platform manifest), so compare the resolved content instead.
    content = {'layers': data.get('RootFS', {}).get('Layers'), 'config': data.get('Config')}
    return {'id': data['Id'], 'repo_digests': data.get('RepoDigests', []),
            'os': data.get('Os'), 'architecture': data.get('Architecture'),
            'content_sha256': hashlib.sha256(json.dumps(content, sort_keys=True).encode()).hexdigest()}


def server_identity(args):
    index_ref = '%s@%s' % (args.server_image, args.server_index)
    index = json.loads(call(['docker', 'manifest', 'inspect', index_ref], timeout=120).stdout)
    os_name, arch = args.platform.split('/', 1)
    matches = [m for m in index.get('manifests', [])
               if m.get('platform', {}).get('os') == os_name and m.get('platform', {}).get('architecture') == arch]
    if len(matches) != 1:
        raise RuntimeError('index %s has %d %s manifests' % (args.server_index, len(matches), args.platform))
    manifest_ref = '%s@%s' % (args.server_image, matches[0]['digest'])
    call(['docker', 'pull', '--platform', args.platform, index_ref], timeout=900)
    call(['docker', 'pull', manifest_ref], timeout=900)
    by_index, by_manifest = image_identity(index_ref), image_identity(manifest_ref)
    if (by_index['content_sha256'], by_index['architecture']) != (by_manifest['content_sha256'], by_manifest['architecture']):
        raise RuntimeError('index and %s manifest resolve to different images' % args.platform)
    labels = json.loads(call(['docker', 'image', 'inspect', '--format', '{{json .Config.Labels}}', manifest_ref]).stdout) or {}
    for label, expected in [('org.opencontainers.image.revision', args.expect_revision),
                            ('dev.durable-workflow.release.tag', args.expect_release_tag)]:
        if expected and labels.get(label) != expected:
            raise RuntimeError('label %s is %r, expected %r' % (label, labels.get(label), expected))
    return {'index': index_ref, 'executed_manifest': manifest_ref, 'platform': args.platform,
            'other_manifests': [{'digest': m['digest'], 'platform': m.get('platform')} for m in index['manifests'] if m is not matches[0]],
            'image': by_manifest, 'image_by_index': by_index, 'labels': labels}


FIXTURE_ENV = ['CARGO_HOME=/work/.cargo', 'CARGO_RESOLVER_INCOMPATIBLE_RUST_VERSIONS=fallback']


def bounded_cargo(args, run, role, volumes, env, script, workdir='/work', network=None):
    """Run one foreground Cargo step in a bounded, labelled, self-removing container.

    Bounded like every other harness container: --init, CPU/RAM/PID caps, no
    capabilities and an in-container wall bound. `network` None keeps the
    default network (registry fetches); 'none' runs fully offline.
    """
    name = '%s-%s' % (run.base, role)
    command = (['docker', 'run', '--rm', '--init', '--name', name] + run.labels(name) +
               ['--cpus', str(max(1, args.jobs)), '--memory', '3g', '--pids-limit', '1024',
                '--security-opt', 'no-new-privileges', '--cap-drop', 'ALL',
                '--user', '%d:%d' % (os.getuid(), os.getgid())])
    if network:
        command += ['--network', network]
    for volume in volumes:
        command += ['-v', volume]
    command += ['-w', workdir]
    for value in env:
        command += ['-e', value]
    command += [args.rust_image, 'timeout', '--kill-after=%ds' % BUILD_KILL_AFTER_SECONDS,
                '%ds' % BUILD_WALL_SECONDS, 'sh', '-c', script]
    try:
        return call(command, timeout=BUILD_CLIENT_TIMEOUT)
    finally:
        # Covers a client-side timeout or error while this process is alive. If
        # the client dies, the in-container timeout ends the step and --rm fires.
        # A cleanup failure is recorded and never replaces the step's own error.
        run.best_effort('%s-cleanup' % role, name, remove_owned_container, name, run)


def fixture_manifest(version):
    return (HERE / 'fixture' / 'Cargo.toml.in').read_text().replace('@CRATE_VERSION@', version)


def build_fixture(args, fixture, run):
    shutil.copytree(HERE / 'fixture' / 'src', fixture / 'src')
    (fixture / 'Cargo.toml').write_text(fixture_manifest(args.crate_version))
    call(['docker', 'pull', args.rust_image], timeout=1800)
    # The default network is kept only for the crates.io fetch; only the fixture
    # directory is mounted.
    build = bounded_cargo(args, run, 'build', ['%s:/work' % fixture], FIXTURE_ENV,
                          'rustc -V && cargo -V && cargo generate-lockfile && cargo build --locked -j %d' % args.jobs)
    (fixture / 'build.stdout').write_text(build.stdout)
    (fixture / 'build.stderr').write_text(build.stderr)
    return {'rust_image': image_identity(args.rust_image), 'toolchain': build.stdout.strip().splitlines()[:2],
            'binary_sha256': sha256(fixture / 'target' / 'debug' / BINARY)}


# ---------------------------------------------------------------------------
# Pre-publication mode: package one clean commit, bind its bytes, and install
# only those bytes into the fixture as a checksummed crates.io replacement.
# ---------------------------------------------------------------------------

def git(path, *arguments, **kwargs):
    return call(['git', '-C', str(path)] + list(arguments), **kwargs)


def require_clean_checkout(path, commit):
    """The checkout's HEAD must be exactly `commit` and its tree fully clean."""
    head = git(path, 'rev-parse', '--verify', 'HEAD^{commit}', check=False)
    if head.returncode or head.stdout.strip() != commit:
        raise PackageError('checkout %s HEAD is %r, not requested commit %s' % (path, head.stdout.strip(), commit))
    status = git(path, 'status', '--porcelain=v1', '--untracked-files=all', '--ignore-submodules=none', check=False)
    if status.returncode or status.stdout.strip():
        raise PackageError('checkout %s is not clean (%d changed or untracked paths)'
                           % (path, len(status.stdout.splitlines())))


def clone_exact_commit(checkout, commit, destination):
    """Copy exactly `commit` into a fresh, self-contained repository.

    A linked worktree's .git points outside itself, so the packaging container
    could not read it, and Cargo would silently omit .cargo_vcs_info.json.
    """
    require_clean_checkout(checkout, commit)
    git(destination.parent, 'init', '--quiet', str(destination))
    git(destination, 'fetch', '--quiet', '--no-tags', '--no-recurse-submodules', str(checkout),
        '+HEAD:refs/heads/acceptance-package', timeout=600)
    git(destination, 'checkout', '--quiet', '--detach', commit)
    require_clean_checkout(destination, commit)


def file_list_sha256(files):
    return hashlib.sha256(''.join(name + '\n' for name in sorted(files)).encode()).hexdigest()


def verify_package_archive(crate_bytes, version, commit, listed, git_blob):
    """Check a .crate against its source commit and `cargo package --list`.

    Returns {relative path: bytes}. Every member must be a regular file under
    `<name>-<version>/`; the member set must equal the listing exactly; the VCS
    record must name the clean commit; the generated manifest must carry the
    version; every other file must be byte-identical to the commit's blob.
    """
    prefix = '%s-%s/' % (PACKAGE_NAME, version)
    members = {}
    try:
        with tarfile.open(fileobj=io.BytesIO(crate_bytes), mode='r:gz') as archive:
            for member in archive.getmembers():
                relative = member.name.removeprefix(prefix)
                if not member.name.startswith(prefix) or not member.isfile() or not safe_relative(relative):
                    raise PackageError('crate member %r is not a regular file under %s' % (member.name, prefix))
                if relative in members:
                    raise PackageError('crate member %r appears more than once' % relative)
                members[relative] = archive.extractfile(member).read()
    except (tarfile.TarError, OSError, EOFError) as error:
        raise PackageError('could not read the packaged crate: %s' % type(error).__name__) from error
    listed = list(listed)
    if len(set(listed)) != len(listed):
        raise PackageError('cargo package --list repeats a path')
    missing, extra = sorted(set(listed) - set(members)), sorted(set(members) - set(listed))
    if missing or extra:
        raise PackageError('crate file set differs from cargo package --list: missing %s, extra %s'
                           % (missing[:10], extra[:10]))
    absent = [name for name in REQUIRED_PACKAGE_FILES if name not in members]
    if absent:
        raise PackageError('crate lacks required files %s' % absent)
    try:
        vcs = json.loads(members['.cargo_vcs_info.json'])
        manifest = tomllib.loads(members['Cargo.toml'].decode())
    except (ValueError, UnicodeError) as error:
        raise PackageError('crate VCS record or manifest is unreadable: %s' % type(error).__name__) from error
    git_record = vcs.get('git') if isinstance(vcs, dict) else None
    if (not isinstance(git_record, dict) or git_record.get('sha1') != commit
            or git_record.get('dirty', False) is not False or vcs.get('path_in_vcs') != ''):
        raise PackageError('crate VCS record %r does not name clean commit %s' % (vcs, commit))
    package = manifest.get('package', {})
    if package.get('name') != PACKAGE_NAME or package.get('version') != version:
        raise PackageError('packaged manifest is %s %s, expected %s %s'
                           % (package.get('name'), package.get('version'), PACKAGE_NAME, version))
    drift = []
    for relative, content in members.items():
        if relative in GENERATED_PACKAGE_FILES:
            continue
        if git_blob('Cargo.toml' if relative == 'Cargo.toml.orig' else relative) != content:
            drift.append(relative)
    if drift:
        raise PackageError('packaged files differ from commit %s: %s' % (commit, sorted(drift)[:10]))
    return members


def write_members(members, destination):
    """Write verified archive members (never tar extraction) under `destination`.

    `destination` must be new. Each member path is confined below it (no
    escape, no link), and each file is created exclusively, never through an
    existing path.
    """
    destination.mkdir(parents=True)
    for relative, content in sorted(members.items()):
        path = confined_child(destination, relative)
        path.parent.mkdir(parents=True, exist_ok=True)
        confined_child(destination, relative)  # parents just created: still no link or escape
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, 'O_NOFOLLOW', 0), 0o644)
        with os.fdopen(descriptor, 'wb') as handle:
            handle.write(content)


def package_release(args, work_dir, run):
    """`cargo package` exactly args.package_commit and bind the resulting bytes."""
    commit, version = args.package_commit, args.expect_version
    checkout = args.package_from
    if not checkout.is_dir():
        raise PackageError('--package-from %s is not a directory' % checkout)
    source, out = work_dir / 'package-source', work_dir / 'package-out'
    clone_exact_commit(checkout, commit, source)
    source_manifest = tomllib.loads(git(source, 'show', '%s:Cargo.toml' % commit).stdout)['package']
    if source_manifest.get('name') != PACKAGE_NAME or source_manifest.get('version') != version:
        raise PackageError('commit %s declares %s %s, expected %s %s'
                           % (commit, source_manifest.get('name'), source_manifest.get('version'), PACKAGE_NAME, version))
    out.mkdir()
    call(['docker', 'pull', args.rust_image], timeout=1800)
    # The source is mounted read-only; Cargo writes only under /out. The default
    # network is kept for the registry index Cargo needs to package --locked.
    # --no-verify skips Cargo's own test build only: archive bytes are written
    # first and are identical, and the offline fixture build below compiles them.
    packaged = bounded_cargo(args, run, 'package', ['%s:/source:ro' % source, '%s:/out' % out],
                             ['CARGO_HOME=/out/.cargo', 'CARGO_TARGET_DIR=/out/target'],
                             'cargo -V && cargo package --locked --no-verify --list > /out/package.list '
                             '&& cargo package --locked --no-verify', workdir='/source')
    (out / 'package.stdout').write_text(packaged.stdout)
    (out / 'package.stderr').write_text(packaged.stderr)
    cargo_version = (packaged.stdout.strip().splitlines() or [''])[0]
    if not cargo_version.startswith('cargo %s ' % args.package_cargo_version):
        raise PackageError('packaging Cargo is %r, expected cargo %s' % (cargo_version, args.package_cargo_version))
    # The worktree must still be exactly the commit after Cargo ran.
    require_clean_checkout(source, commit)
    crates = sorted((out / 'target' / 'package').glob('*.crate'))
    expected = out / 'target' / 'package' / ('%s-%s.crate' % (PACKAGE_NAME, version))
    if crates != [expected]:
        raise PackageError('expected exactly %s, found %s' % (expected.name, [c.name for c in crates]))
    crate_bytes = expected.read_bytes()
    crate_sha256 = hashlib.sha256(crate_bytes).hexdigest()
    if args.expect_package_sha256 and crate_sha256 != args.expect_package_sha256:
        raise PackageError('packaged crate SHA-256 %s differs from expected %s' % (crate_sha256, args.expect_package_sha256))
    listed = [line for line in (out / 'package.list').read_text().splitlines() if line]

    def blob(path):
        result = git(source, 'cat-file', 'blob', '%s:%s' % (commit, path), check=False, text=False)
        return result.stdout if result.returncode == 0 else None
    members = verify_package_archive(crate_bytes, version, commit, listed, blob)
    unpacked = work_dir / 'package' / ('%s-%s' % (PACKAGE_NAME, version))
    write_members(members, unpacked)
    (work_dir / 'package' / expected.name).write_bytes(crate_bytes)
    metadata = tomllib.loads(members['Cargo.toml'].decode())['package'].get('metadata', {}).get('durable-workflow', {})
    return {'mode': 'pre-publication', 'package': PACKAGE_NAME, 'version': version, 'commit': commit,
            'crate_file': expected.name, 'crate_sha256': crate_sha256,
            'crate_size': len(crate_bytes), 'files': sorted(members), 'file_count': len(members),
            'file_list_sha256': file_list_sha256(members),
            'vcs_info': json.loads(members['.cargo_vcs_info.json']), 'cargo_version': cargo_version,
            'rust_image': image_identity(args.rust_image), 'product_train': metadata.get('product-train'),
            'verified_source_files': len(members) - len(GENERATED_PACKAGE_FILES),
            'unpacked': str(unpacked.relative_to(work_dir))}, members


def cargo_checksum(members, crate_sha256):
    return json.dumps({'files': {name: hashlib.sha256(content).hexdigest() for name, content in sorted(members.items())},
                       'package': crate_sha256}, sort_keys=True)


def build_packaged_fixture(args, fixture, package, members, run):
    """Build the fixture against only the packaged bytes, offline.

    1. Resolve: with the default network, `[patch]` the dependency to the
       unpacked package only to resolve and `cargo vendor` third-party crates.
       Cargo never vendors that path package.
    2. Install: add the verified members as a directory-source entry whose
       package checksum is the .crate SHA-256, replace crates.io with the vendor
       directory, drop the patch and the lockfile, and build with --network none.
       Cargo then locks durable-workflow as a crates.io package whose checksum is
       the packaged SHA-256 and verifies every vendored file against it.
    """
    version = package['version']
    shutil.copytree(HERE / 'fixture' / 'src', fixture / 'src')
    final_manifest = fixture_manifest(version)
    (fixture / 'Cargo.toml').write_text(final_manifest + '\n[patch.crates-io]\n%s = { path = "/package" }\n' % PACKAGE_NAME)
    resolve = bounded_cargo(args, run, 'resolve', ['%s:/work' % fixture, '%s:/package:ro' % (fixture.parent / package['unpacked'])],
                            FIXTURE_ENV, 'cargo -V && cargo generate-lockfile && cargo vendor --locked --versioned-dirs vendor')
    (fixture / 'resolve.stdout').write_text(resolve.stdout)
    (fixture / 'resolve.stderr').write_text(resolve.stderr)
    vendor = fixture / 'vendor'
    foreign = sorted(p.name for p in vendor.glob(PACKAGE_NAME + '-*'))
    if foreign:
        raise PackageError('third-party vendoring produced %s; only the packaged bytes may be installed' % foreign)
    entry = vendor / ('%s-%s' % (PACKAGE_NAME, version))
    write_members(members, entry)
    (entry / '.cargo-checksum.json').write_text(cargo_checksum(members, package['crate_sha256']))
    (fixture / 'Cargo.toml').write_text(final_manifest)
    (fixture / 'Cargo.lock').unlink()
    (fixture / '.cargo').mkdir(exist_ok=True)
    (fixture / '.cargo' / 'config.toml').write_text(
        '[source.crates-io]\nreplace-with = "acceptance-package"\n\n'
        '[source.acceptance-package]\ndirectory = "vendor"\n')
    build = bounded_cargo(args, run, 'build', ['%s:/work' % fixture], FIXTURE_ENV,
                          'rustc -V && cargo -V && cargo generate-lockfile --offline '
                          '&& cargo build --locked --offline -j %d' % args.jobs, network='none')
    (fixture / 'build.stdout').write_text(build.stdout)
    (fixture / 'build.stderr').write_text(build.stderr)
    return {'rust_image': image_identity(args.rust_image), 'toolchain': build.stdout.strip().splitlines()[:2],
            'network': 'none', 'binary_sha256': sha256(fixture / 'target' / 'debug' / BINARY)}


def packaged_provenance(args, fixture, package, members):
    """The built fixture locked and installed exactly the packaged bytes."""
    version, crate_sha256 = package['version'], package['crate_sha256']
    lock = tomllib.loads((fixture / 'Cargo.lock').read_text())
    locked = [p for p in lock['package'] if p['name'] == PACKAGE_NAME]
    if len(locked) != 1 or locked[0].get('version') != version:
        raise PackageError('fixture did not lock exactly %s %s: %r' % (PACKAGE_NAME, version, locked))
    if locked[0].get('source') != CRATES_IO_SOURCE or locked[0].get('checksum') != crate_sha256:
        raise PackageError('fixture lock %r is not the packaged crate %s' % (locked[0], crate_sha256))
    entries = sorted(p.name for p in (fixture / 'vendor').glob(PACKAGE_NAME + '-*'))
    entry = fixture / 'vendor' / ('%s-%s' % (PACKAGE_NAME, version))
    if entries != [entry.name]:
        raise PackageError('vendored %s entries are %s' % (PACKAGE_NAME, entries))
    paths = list(entry.rglob('*'))
    if any(p.is_symlink() or not (p.is_file() or p.is_dir()) for p in paths):
        raise PackageError('installed package contains a link or special file')
    installed = {str(p.relative_to(entry)): p.read_bytes() for p in paths if p.is_file()}
    checksum = json.loads(installed.pop('.cargo-checksum.json', b'null'))
    if installed != members or not isinstance(checksum, dict) or checksum.get('package') != crate_sha256:
        raise PackageError('installed package bytes differ from the packaged crate')
    metadata = tomllib.loads(members['Cargo.toml'].decode())['package'].get('metadata', {}).get('durable-workflow', {})
    return {'mode': 'pre-publication', 'version': version, 'sha256': crate_sha256, 'lock_checksum': locked[0]['checksum'],
            'lock_source': locked[0]['source'], 'registry_published': False, 'source_commit': package['commit'],
            'installed_files': len(installed), 'verified_source_files': package['verified_source_files'],
            'product_train': metadata.get('product-train'),
            'baselines': check_baselines(metadata, args.baselines, args.expected_baselines)}


def approval_candidate(summary, summary_bytes):
    """The approved-package record a reviewer commits after a passing run."""
    package = summary['package']
    return {'schema': APPROVAL_SCHEMA, 'package': package['package'], 'version': package['version'],
            'commit': package['commit'], 'crate_sha256': package['crate_sha256'],
            'cargo_version': package['cargo_version'].split()[1], 'files': package['files'],
            'file_list_sha256': package['file_list_sha256'],
            'acceptance': {'summary_sha256': hashlib.sha256(summary_bytes).hexdigest(),
                           'harness_commit': summary.get('harness_commit'),
                           'server_index': summary['server']['index'],
                           'executed_manifest': summary['server']['executed_manifest'],
                           'platform': summary['server']['platform'],
                           'expected_server_version': summary.get('expected_server', {}).get('version'),
                           'expected_revision': summary.get('expected_server', {}).get('revision'),
                           'expected_release_tag': summary.get('expected_server', {}).get('release_tag'),
                           'cluster_versions': sorted({c.get('cluster_info', {}).get('version') or ''
                                                       for c in summary['cells'].values()}),
                           'cells': sorted(summary['cells']), 'finished_at': summary['finished_at'],
                           'outcome': summary['outcome']}}


def check_baselines(metadata, mode, expected):
    present = 'supported-server-release-baselines' in metadata
    baselines = metadata.get('supported-server-release-baselines')
    scalar = metadata.get('supported-server-versions')
    if mode == 'absent':
        ok = not present
    else:
        ok = baselines == expected and scalar == expected.get('2')
    return {'mode': mode, 'pass': ok, 'expected': expected if mode == 'required' else None,
            'supported-server-release-baselines': baselines,
            'supported-server-versions': scalar}


def crate_provenance(args, fixture):
    version = args.crate_version
    lock = tomllib.loads((fixture / 'Cargo.lock').read_text())
    locked = [p for p in lock['package'] if p['name'] == 'durable-workflow']
    if len(locked) != 1 or locked[0]['version'] != version:
        raise RuntimeError('fixture did not lock exactly durable-workflow %s: %r' % (version, locked))
    request = urllib.request.Request('https://crates.io/api/v1/crates/durable-workflow/' + version,
                                     headers={'User-Agent': 'durable-workflow-sdk-rust-acceptance'})
    with urllib.request.urlopen(request, timeout=30) as response:
        registry = json.loads(response.read())['version']
    crate = next((fixture / '.cargo' / 'registry' / 'cache').glob('*/durable-workflow-%s.crate' % version))
    digests = {'lock': locked[0]['checksum'], 'registry': registry['checksum'], 'downloaded': sha256(crate)}
    if len(set(digests.values())) != 1:
        raise RuntimeError('crate checksum mismatch: %r' % digests)
    prefix = 'durable-workflow-%s/' % version
    with tarfile.open(crate) as archive:
        packaged = {m.name.removeprefix(prefix): archive.extractfile(m).read() for m in archive if m.isfile()}
    manifest = tomllib.loads(packaged['Cargo.toml'].decode())
    metadata = manifest['package'].get('metadata', {}).get('durable-workflow', {})
    vcs = json.loads(packaged['.cargo_vcs_info.json'])
    tag = args.source_tag or version
    commit = call(['git', '-C', str(REPO), 'rev-parse', '--verify', 'refs/tags/%s^{commit}' % tag], check=False).stdout.strip()
    if not commit:
        raise RuntimeError('source tag %s not found locally; run git fetch --tags first' % tag)
    if vcs['git']['sha1'] != commit or vcs['git'].get('dirty'):
        raise RuntimeError('crate VCS %r does not match clean tag %s (%s)' % (vcs, tag, commit))
    archive_bytes = call(['git', '-C', str(REPO), 'archive', tag, 'src', 'schema'], text=False).stdout
    tagged = {}
    with tarfile.open(fileobj=io.BytesIO(archive_bytes)) as archive:
        tagged = {m.name: archive.extractfile(m).read() for m in archive if m.isfile()}
    packaged_source = {k: v for k, v in packaged.items() if k.startswith(('src/', 'schema/'))}
    if packaged_source != tagged:
        differing = sorted(set(packaged_source) ^ set(tagged) | {k for k in tagged if packaged_source.get(k) != tagged[k]})
        raise RuntimeError('packaged src/schema differ from tag %s: %s' % (tag, differing[:10]))
    return {'version': version, 'sha256': digests['registry'], 'registry_published_at': registry.get('created_at'),
            'yanked': registry.get('yanked'), 'source_tag': tag, 'source_commit': commit,
            'verified_source_and_schema_files': len(tagged), 'product_train': metadata.get('product-train'),
            'baselines': check_baselines(metadata, args.baselines, args.expected_baselines)}


class Cell:
    def __init__(self, args, name, root, server, fixture, run):
        self.args, self.name, self.server, self.fixture, self.harness_run = args, name, server, fixture, run
        self.root = root
        self.dir = root / 'cells' / name
        self.state = self.dir / 'state'
        self.task = '%s-%s' % (run.base, name)
        self.containers = []
        self.deadline = int(time.time()) + CELL_WALL_SECONDS
        self.token = 'fixture-' + secrets.token_hex(24)
        self.server_key = 'base64:' + base64.b64encode(secrets.token_bytes(32)).decode()
        self.harness_run.secret_values.extend((self.token, self.server_key))
        self.image_commands = {}

    def cell_path(self, relative=''):
        return confined_child(self.root, 'cells/' + self.name + ('/' + relative if relative else ''))

    def purge_evidence(self):
        # Refuse a redirected parent; unlink a redirected Cell root itself
        # rather than traversing or removing its foreign target.
        parent = confined_child(self.root, 'cells')
        path = parent / self.name
        if path.is_symlink():
            path.unlink()
        else:
            remove_tree(self.cell_path())

    def evidence(self, data, label):
        clean, leaked = redact_credentials(data, (self.token, self.server_key))
        if leaked:
            credential_output(self.harness_run, self.name + '/' + label)
        return clean

    @contextlib.contextmanager
    def artifact_file(self, path, writable=False):
        path = self.cell_path(path.relative_to(self.dir).as_posix())
        info = path.lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise RuntimeError('refusing nonregular or linked Cell evidence')
        descriptor = os.open(path, (os.O_RDWR if writable else os.O_RDONLY) | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(descriptor, 'r+b' if writable else 'rb') as handle:
            opened = os.fstat(handle.fileno())
            if not stat.S_ISREG(opened.st_mode) or opened.st_nlink != 1:
                raise RuntimeError('Cell evidence changed during validation')
            yield handle
            after = os.fstat(handle.fileno())
            if not stat.S_ISREG(after.st_mode) or after.st_nlink != 1:
                raise RuntimeError('Cell evidence changed during validation')

    def artifact_bytes(self, path):
        with self.artifact_file(path) as handle:
            raw = handle.read(8 * 1024 * 1024 + 1)
        if len(raw) > 8 * 1024 * 1024:
            raise RuntimeError('Cell evidence exceeds the bounded artifact size')
        return raw

    def fence_artifacts(self):
        """After writers stop, sanitize every retained Cell artifact or fail closed.

        JSON is normalized before scanning, so escaped credential characters
        cannot survive as a reusable structured value. Never follow a symlink.
        """
        self.cell_path()  # Refuse a symlinked root before walking it.
        if not self.dir.exists():
            return
        if not stat.S_ISDIR(self.dir.lstat().st_mode):
            raise RuntimeError('Cell evidence root is not a real directory')
        def unreadable(error):
            raise RuntimeError('unreadable Cell evidence directory') from None
        paths = (pathlib.Path(parent) / name
                 for parent, directories, files in os.walk(self.dir, followlinks=False, onerror=unreadable)
                 for name in directories + files)
        for path in paths:
            _, named_secret = redact_credentials(path.relative_to(self.dir).as_posix().encode(),
                                                 (self.token, self.server_key))
            if named_secret:
                credential_output(self.harness_run, self.name + '/artifact-name')
                raise RuntimeError('credential-bearing artifact name')
            if path.is_symlink():
                raise RuntimeError('refusing symlinked Cell evidence')
            info = path.lstat()
            if stat.S_ISDIR(info.st_mode):
                continue
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                raise RuntimeError('refusing nonregular or linked Cell evidence')
            with self.artifact_file(path, writable=True) as handle:
                raw = handle.read(8 * 1024 * 1024 + 1)
                if len(raw) > 8 * 1024 * 1024:
                    raise RuntimeError('Cell evidence exceeds the bounded artifact size')
                normalized = raw
                try:
                    if path.suffix == '.json':
                        normalized = json.dumps(json.loads(raw), ensure_ascii=False).encode()
                    elif path.suffix == '.jsonl':
                        normalized = ('\n'.join(json.dumps(json.loads(line), ensure_ascii=False)
                                                for line in raw.splitlines() if line) + '\n').encode()
                except (ValueError, UnicodeError):
                    raise RuntimeError('malformed structured Cell evidence') from None
                clean = self.evidence(normalized, 'artifact')
                if clean != normalized:
                    if os.fstat(handle.fileno()).st_nlink != 1:
                        raise RuntimeError('Cell evidence changed before redaction')
                    handle.seek(0)
                    handle.write(clean)
                    handle.truncate()
                    handle.flush()

    def launch(self, line, name, image, command, user, credentials):
        """Preserve the image entrypoint under a client-independent supervisor.

        The token travels only on stdin into this container's private tmpfs.
        Neither Docker's create metadata nor bind-mounted evidence stores it.
        A client death before token delivery leaves a bounded, unarmed waiter.
        """
        if time.time() >= self.deadline:
            raise RuntimeError('Cell deadline has expired')
        if image not in self.image_commands:
            config = json.loads(call(['docker', 'image', 'inspect', image]).stdout)[0]['Config']
            entry, default = config.get('Entrypoint') or [], config.get('Cmd') or []
            if not all(isinstance(v, list) and all(isinstance(s, str) for s in v) for v in (entry, default)):
                raise RuntimeError('image entrypoint and command must be argument arrays')
            self.image_commands[image] = entry, default
        entry, default = self.image_commands[image]
        child = entry + (command or default)
        if not child:
            raise RuntimeError('image has no command to supervise')
        uid, gid = user.split(':')
        if not uid.isdecimal() or not gid.isdecimal():
            raise RuntimeError('Cell user must be a numeric uid:gid')
        line += ['--entrypoint', '/bin/sh', '--tmpfs',
                 '/credentials:mode=0700,uid=%s,gid=%s,size=64k,noexec,nosuid,nodev' % (uid, gid),
                 '--mount', 'type=bind,src=%s,dst=/acceptance-supervisor.sh,readonly' % (HERE / 'cell-supervisor.sh')]
        self.containers.append(name)
        call(line + [image, '/acceptance-supervisor.sh', str(self.deadline),
                     str(CELL_KILL_AFTER_SECONDS), credentials] + child)
        if credentials != 'none':
            token = self.token if credentials in ('auth', 'both') else ''
            key = self.server_key if credentials in ('server', 'both') else ''
            call(['docker', 'exec', '-i', name, '/bin/sh', '-c',
                  'umask 077; IFS= read -r token && IFS= read -r key && '
                  '{ [ -z "$token" ] || printf %s "$token" > /credentials/token; } && '
                  '{ [ -z "$key" ] || printf %s "$key" > /credentials/server-key; } && '
                  ': > /credentials/ready.tmp && mv /credentials/ready.tmp /credentials/ready'],
                 input=token + '\n' + key + '\n', timeout=30)
        return name

    def start(self, role, image, command, user=None, env=None, alias=None, mounts=(), credentials='none'):
        name = '%s-%s' % (self.task, role)
        line = ['docker', 'run', '-d', '--init', '--read-only', '--name', name] + self.harness_run.labels(self.task) + [
                '--user', user or '%d:%d' % (os.getuid(), os.getgid()), '--cpus', '1', '--memory', '1g',
                '--pids-limit', '256', '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges',
                '--network', self.task, '--tmpfs', '/tmp:mode=1777,size=64m',
                '--mount', 'type=bind,src=%s,dst=/state' % self.state,
                '--mount', 'type=bind,src=%s,dst=/harness,readonly' % HERE,
                '--mount', 'type=bind,src=%s,dst=/fixture,readonly' % (self.fixture / 'target' / 'debug')]
        for mount in mounts:
            line += ['--mount', mount]
        if alias:
            line += ['--network-alias', alias]
        for key, value in (env or {}).items():
            line += ['-e', '%s=%s' % (key, value)]
        # Track before creating: the daemon can create the container even when the
        # client then times out or errors. Cleanup is ownership-checked, so a
        # failed create or a foreign name collision is never removed.
        return self.launch(line, name, image, command, user or '%d:%d' % (os.getuid(), os.getgid()), credentials)

    def plain(self, role, image, command, env=None, alias=None, user=None, tmpfs=None, caps=(), pids=256,
              credentials='none'):
        """Start a PHP Server process-set container (writable root, as in its compose file).

        Bounded like `start`: --init, CPU/RAM/PID caps, no-new-privileges and
        --cap-drop ALL, adding back only the capabilities `caps` names.
        """
        name = '%s-%s' % (self.task, role)
        line = ['docker', 'run', '-d', '--init', '--name', name] + self.harness_run.labels(self.task) + [
                '--network', self.task, '--memory', '1g', '--cpus', '1', '--pids-limit', str(pids),
                '--security-opt', 'no-new-privileges', '--cap-drop', 'ALL']
        for cap in caps:
            line += ['--cap-add', cap]
        if user:
            line += ['--user', user]
        if alias:
            line += ['--network-alias', alias]
        if tmpfs:
            line += ['--tmpfs', tmpfs]
        for key, value in (env or {}).items():
            line += ['-e', '%s=%s' % (key, value)]
        # Track before creating: the daemon can create the container even when the
        # client then times out or errors. Cleanup is ownership-checked, so a
        # failed create or a foreign name collision is never removed.
        if image == self.server['executed_manifest']:
            # The PHP entrypoint caches environment, including the Server key.
            # That derived copy must die with the container too.
            line += ['--tmpfs', '/app/bootstrap/cache:mode=0777,size=8m,noexec,nosuid,nodev']
        return self.launch(line, name, image, command, user or '0:0', credentials)

    def start_php(self, token):
        image = self.server['executed_manifest']
        env = {'APP_ENV': 'testing', 'APP_DEBUG': 'false',
               'DB_CONNECTION': 'mysql', 'DB_HOST': 'mysql', 'DB_DATABASE': 'durable_workflow', 'DB_USERNAME': 'workflow',
               'DB_PASSWORD': 'workflow', 'REDIS_HOST': 'redis', 'QUEUE_CONNECTION': 'redis', 'CACHE_STORE': 'redis',
               'DW_AUTH_DRIVER': 'token', 'DW_WORKFLOW_TASK_TIMEOUT': '10',
               'DW_WORKER_PROTOCOL_VERSION': '1.20'}
        # Capabilities kept after --cap-drop ALL, from each image's entrypoint. The
        # MySQL 8.4 and Redis 7.4 entrypoints start as root, chown their data and
        # socket directories (MySQL also walks mysql-owned directories such as
        # secure-file-priv), then drop to their service user with gosu. MySQL runs
        # one thread per connection and background task, so it gets 512 PIDs.
        mysql = self.plain('mysql', MYSQL_IMAGE, [], alias='mysql', tmpfs='/var/lib/mysql',
                           caps=('CHOWN', 'DAC_OVERRIDE', 'SETGID', 'SETUID'), pids=512,
                           env={'MYSQL_DATABASE': 'durable_workflow', 'MYSQL_USER': 'workflow',
                                'MYSQL_PASSWORD': 'workflow', 'MYSQL_ROOT_PASSWORD': 'root'})
        self.plain('redis', REDIS_IMAGE, [], alias='redis', tmpfs='/data', caps=('CHOWN', 'SETGID', 'SETUID'))
        deadline = time.monotonic() + 120
        while call(['docker', 'exec', mysql, 'mysqladmin', 'ping', '-h', 'localhost', '-uworkflow', '-pworkflow'], check=False).returncode:
            if time.monotonic() > deadline:
                raise RuntimeError('mysql never became ready')
            time.sleep(1)
        # The PHP Server image starts as root and writes caches under directories
        # owned by www-data (uid 1000).
        # Bootstrap migrates/seeds; queue and repair are CLI processes. None
        # handles HTTP authentication, so only the HTTP runtime gets the token.
        bootstrap = self.plain('bootstrap', image, ['server-bootstrap'], env=env, caps=('DAC_OVERRIDE',),
                               credentials='server')
        if call(['docker', 'wait', bootstrap], timeout=600).stdout.strip() != '0':
            raise RuntimeError('PHP Server bootstrap failed')
        # Apache's root master also chowns its mutex to www-data, switches its
        # children to www-data and signals them.
        self.plain('runtime', image, [], env=env, alias='qualification-origin',
                   caps=('CHOWN', 'DAC_OVERRIDE', 'KILL', 'SETGID', 'SETUID'), credentials='both')
        # queue and repair run as uid 1000 and keep no capabilities.
        self.plain('queue', image, ['php', 'artisan', 'queue:work', '--sleep=1', '--tries=3', '--max-time=1800'],
                   env=env, user='1000:1000', credentials='server')
        self.plain('repair', image, ['sh', '-c', 'while true; do php artisan workflow:v2:repair-pass --json; sleep 2; done'],
                   env=env, user='1000:1000', credentials='server')

    def http(self, container, path, token):
        code = ('import json,pathlib,urllib.request\n'
                'token=pathlib.Path("/credentials/token").read_text()\n'
                'r=urllib.request.Request("http://qualification-origin:8080/api%s",headers={"Authorization":"Bearer "+token,"X-Namespace":"default"})\n'
                'print(urllib.request.urlopen(r,timeout=3).read().decode())' % path)
        return call(['docker', 'exec', container, 'python', '-c', code], check=False)

    def run(self):
        token = self.token
        result = None
        credentials = self.state / 'credentials.json'
        # All state setup is inside the protected region: a partial setup (say,
        # database/ created and storage/ failing) is still removed and verified.
        try:
            self.cell_path('state').mkdir(parents=True)
            for path in [self.dir, self.state]:
                path.chmod(0o777)
            for sub in ['database', 'storage']:
                (self.state / sub).mkdir()
                (self.state / sub).chmod(0o777)
            binary_before = sha256(self.fixture / 'target' / 'debug' / BINARY)
            # Inside the protected region: the daemon can create the network even
            # when the client then times out. Removal is ownership-checked.
            call(['docker', 'network', 'create', '--internal'] + self.harness_run.labels(self.task) + [self.task])
            if self.args.runtime == 'php':
                self.start_php(token)
            else:
                self.start('runtime', self.server['executed_manifest'], [], user='1000:1000', alias='qualification-origin',
                           credentials='both',
                           env={'DB_CONNECTION': 'sqlite', 'DB_DATABASE': '/app/database/runtime.sqlite',
                                'DW_RUST_MAINTENANCE_LOOP': '1'},
                           mounts=['type=bind,src=%s,dst=/app/database' % (self.state / 'database'),
                                   'type=bind,src=%s,dst=/app/storage' % (self.state / 'storage')])
            proxy = self.start('proxy', self.args.python_image, ['python', '/harness/proxy.py'],
                               alias='qualification-runtime', credentials='auth')
            deadline = time.monotonic() + 120
            ready = '/health' if self.args.runtime == 'php' else '/ready'
            while time.monotonic() < deadline:
                if self.http(proxy, ready, token).returncode == 0:
                    break
                time.sleep(0.5)
            else:
                raise RuntimeError('runtime never became ready')
            info = self.http(proxy, '/cluster/info', token)
            cluster = json.loads(info.stdout) if info.returncode == 0 else {'error': info.stderr[-500:]}
            if self.args.expect_server_version and cluster.get('version') != self.args.expect_server_version:
                raise RuntimeError('cluster/info version %r, expected %r' % (cluster.get('version'), self.args.expect_server_version))
            worker = self.start('worker', self.args.rust_image, ['/fixture/' + BINARY, 'worker'], credentials='auth')
            driver = self.start('driver', self.args.rust_image, ['/fixture/' + BINARY, 'drive-' + self.name], credentials='auth')
            driver_exit = int(call(['docker', 'wait', driver], timeout=300).stdout.strip())
            checker = self.start('history', self.args.python_image, ['python', '/harness/check_history.py'], credentials='auth')
            history_exit = int(call(['docker', 'wait', checker], timeout=120).stdout.strip())
            worker_running = call(['docker', 'inspect', '--format', '{{.State.Running}}', worker]).stdout.strip() == 'true'
            read = lambda name: json.loads(self.evidence(
                json.dumps(json.loads(self.artifact_bytes(self.state / name))).encode(), 'structured')) if os.path.lexists(self.state / name) else None
            acks = [json.loads(self.evidence(json.dumps(json.loads(l)).encode(), 'structured'))
                    for l in self.artifact_bytes(self.state / 'acks.jsonl').splitlines()] if os.path.lexists(self.state / 'acks.jsonl') else []
            result = {'cell': self.name, 'deadline_unix': self.deadline,
                      'kill_after_seconds': CELL_KILL_AFTER_SECONDS,
                      'cluster_info': {k: cluster.get(k) for k in
                      ('version', 'implementation', 'development', 'qualified_for_php_takeover', 'error') if k in cluster},
                      'driver_exit': driver_exit, 'history_exit': history_exit,
                      'same_worker_running_after_cases': worker_running,
                      'binary_unchanged': sha256(self.fixture / 'target' / 'debug' / BINARY) == binary_before,
                      'driver': read('driver.json'), 'history': read('history.json'), 'acknowledgements': acks}
            result['outcome'] = 'pass' if (driver_exit == 0 and history_exit == 0 and worker_running
                                           and result['binary_unchanged']) else 'fail'
            return result
        except Exception as error:
            message = self.evidence(str(error).encode(), 'error').decode()
            if message != str(error):
                raise RuntimeError('Cell failed with credential-bearing diagnostics') from None
            raise
        finally:
            writers_stopped = False
            try:
                writers_stopped = self.remove_docker_objects()
            finally:
                # Local credential and state cleanup always runs, whatever Docker did.
                self.remove_local_state(credentials)
                try:
                    if not writers_stopped:
                        raise RuntimeError('Cell writer cleanup is uncertain')
                    self.fence_artifacts()
                except Exception as error:
                    self.harness_run.cleanup_errors.append({
                        'step': 'evidence-fence', 'object': self.name, 'error': type(error).__name__})
                    # Unreadable/unsafe artifacts cannot be certified clean.
                    self.harness_run.best_effort('evidence-purge', self.name, self.purge_evidence)
                    if os.path.lexists(self.dir):
                        self.harness_run.local_leftovers.append(self.name + '/evidence')
                if result is not None and self.harness_run.cleanup_errors:
                    result['outcome'] = 'fail'

    def remove_local_state(self, credentials):
        """Remove the credential file, database/ and storage/ independently, then
        prove each is gone. Failures and survivors are recorded with bounded
        labels (cell/path) and error types only, and fail the run."""
        run = self.harness_run
        paths = [('credentials.json', credentials, lambda path: path.unlink(missing_ok=True))]
        paths += [(sub, self.state / sub, remove_tree) for sub in ('database', 'storage')]
        for label, path, remove in paths:
            run.best_effort('local-remove', '%s/%s' % (self.name, label), lambda: remove(
                self.cell_path(path.relative_to(self.dir).as_posix())))
        for label, path, _ in paths:
            label = '%s/%s' % (self.name, label)
            if os.path.lexists(path):
                run.local_leftovers.append(label)
                run.cleanup_errors.append({'step': 'local-verify', 'object': label, 'error': 'StillPresent'})

    def remove_docker_objects(self):
        """Best-effort, per-object cleanup: one failing inspect, log or remove never
        skips the others. Only objects carrying this run's token are logged and
        removed, by ID; failures are recorded on the run."""
        run, logs = self.harness_run, self.dir / 'logs'
        errors_before = len(run.cleanup_errors)
        run.best_effort('logs-dir', self.task, lambda: self.cell_path('logs').mkdir(exist_ok=True))
        owned = [(name, run.best_effort('container-inspect', name, owned_id, 'container', name, run))
                 for name in self.containers]
        for name, container_id in owned:
            if container_id:
                run.best_effort('container-logs', name, self.save_logs, logs, name, container_id)
        for name, container_id in reversed(owned):
            if container_id:
                run.best_effort('container-remove', name, call, ['docker', 'rm', '-f', container_id])
        run.best_effort('network-cleanup', self.task, remove_owned_network, self.task, run)
        return not any(e['step'] in ('container-inspect', 'container-remove')
                       for e in run.cleanup_errors[errors_before:])

    def save_logs(self, logs, name, container_id):
        out = call(['docker', 'logs', '--tail', '300', container_id], check=False)
        stem = name.removeprefix(self.task + '-')
        self.cell_path('logs/' + stem + '.stdout').write_bytes(self.evidence(out.stdout.encode(), 'logs'))
        self.cell_path('logs/' + stem + '.stderr').write_bytes(self.evidence(out.stderr.encode(), 'logs'))


def harness_identity(args):
    """The harness commit, and in pre-publication mode proof that it is clean.

    An approval names the harness commit that produced it, so a locally edited
    harness must not be able to produce an approval that names a clean commit.
    """
    head = call(['git', '-C', str(REPO), 'rev-parse', '--verify', 'HEAD^{commit}'], check=False).stdout.strip()
    if args.package_from:
        try:
            require_clean_checkout(REPO, head)
        except PackageError as error:
            raise SystemExit('pre-publication mode requires a clean harness checkout: %s' % error) from error
    return head


def main():
    args = parse_args()
    harness_commit = harness_identity(args)
    if args.work_dir.exists():
        raise SystemExit('work dir %s already exists; use a new one' % args.work_dir)
    args.work_dir.mkdir(parents=True)
    run = Run(args.container_prefix)
    write_owner_only(args.work_dir / 'run.json', json.dumps({
        'schema': 'durable-workflow.acceptance-run/v1', 'owner': run.prefix,
        'token': run.token, 'cells': args.cells.split(',')}))
    summary = {'started_at': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'outcome': 'fail',
               'mode': 'pre-publication' if args.package_from else 'published',
               'expected_server': {'version': args.expect_server_version, 'revision': args.expect_revision,
                                   'release_tag': args.expect_release_tag},
               'harness_commit': harness_commit}
    approval = None
    try:
        if args.baselines_control:
            control = tomllib.loads(args.baselines_control.read_text())
            summary['baselines_control'] = dict(check_baselines(
                control['package'].get('metadata', {}).get('durable-workflow', {}), 'required', args.expected_baselines),
                source=str(args.baselines_control))
        summary['server'] = server_identity(args)
        if args.package_from:
            fixture = args.work_dir / 'fixture'
            summary['package'], members = package_release(args, args.work_dir, run)
            summary['build'] = build_packaged_fixture(args, fixture, summary['package'], members, run)
            summary['crate'] = packaged_provenance(args, fixture, summary['package'], members)
        elif args.fixture_from:
            fixture = args.fixture_from.resolve()
            summary['build'] = {'reused_fixture': str(fixture), 'toolchain': (fixture / 'build.stdout').read_text().strip().splitlines()[:2],
                                'binary_sha256': sha256(fixture / 'target' / 'debug' / BINARY)}
        else:
            fixture = args.work_dir / 'fixture'
            summary['build'] = build_fixture(args, fixture, run)
        if not args.package_from:
            summary['crate'] = crate_provenance(args, fixture)
        # Pull first: inspect alone fails on a clean host that never fetched the helper image.
        call(['docker', 'pull', '--quiet', args.python_image], timeout=600)
        summary['python_image'] = image_identity(args.python_image)
        cells = {}
        for name in args.cells.split(','):
            cells[name] = Cell(args, name, args.work_dir, summary['server'], fixture, run).run()
        summary['cells'] = cells
        package_intact = True
        if args.package_from:
            # The approved bytes are the ones that were packaged, not a later copy.
            after = sha256(args.work_dir / 'package' / summary['package']['crate_file'])
            summary['package']['crate_sha256_after_cells'] = after
            package_intact = after == summary['package']['crate_sha256']
        passed = (package_intact and summary['crate']['baselines']['pass']
                  and summary.get('baselines_control', {'pass': True})['pass']
                  and all(c['outcome'] == 'pass' for c in summary['cells'].values()))
        summary['outcome'] = 'pass' if passed else 'fail'
    except Exception as error:
        summary['error'] = '%s: %s' % (type(error).__name__, error)
        raise
    finally:
        # None means the query itself failed (recorded in cleanup_errors).
        leftovers = run.best_effort('leftover-query', 'containers', run_objects, 'container', run)
        leftover_networks = run.best_effort('leftover-query', 'networks', run_objects, 'network', run)
        # Fail closed: any cleanup error, unavailable leftover query or leftover
        # object makes the run non-passing. The checks' own outcome and any run
        # error are kept separately.
        cleanup_clean = (not run.cleanup_errors and not run.local_leftovers
                         and leftovers == [] and leftover_networks == [])
        summary['checks_outcome'] = summary['outcome']
        if not cleanup_clean:
            summary['outcome'] = 'fail'
        summary['cleanup'] = {'pass': cleanup_clean, 'errors': run.cleanup_errors,
                              'local_leftovers': run.local_leftovers, 'leftover_containers': leftovers,
                              'leftover_networks': leftover_networks}
        summary['finished_at'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        _, leaked = redact_credentials(json.dumps(summary, ensure_ascii=False).encode(), run.secret_values)
        if leaked:
            credential_output(run, 'summary')
            summary['outcome'] = summary['checks_outcome'] = 'fail'
            summary['cleanup']['pass'] = False
            summary['cleanup']['errors'] = run.cleanup_errors
        summary_bytes, _ = redact_credentials(
            (json.dumps(summary, ensure_ascii=False, indent=2) + '\n').encode(), run.secret_values)
        summary = json.loads(summary_bytes)
        (args.work_dir / 'summary.json').write_bytes(summary_bytes)
        if args.package_from and summary['outcome'] == 'pass':
            # Only a fully passing pre-publication run (checks and cleanup) yields
            # an approval candidate; a reviewer commits it for the release workflow.
            approval = args.work_dir / 'approved-package.json'
            approval.write_text(json.dumps(approval_candidate(summary, summary_bytes), indent=2) + '\n')
        print(json.dumps({'outcome': summary['outcome'], 'error': summary.get('error'),
                          'approval_candidate': str(approval) if approval else None,
                          'summary': str(args.work_dir / 'summary.json'), 'leftover_containers': leftovers,
                          'leftover_networks': leftover_networks, 'local_leftovers': run.local_leftovers,
                          'cleanup_errors': run.cleanup_errors}))
    sys.exit(0 if summary['outcome'] == 'pass' else 1)


if __name__ == '__main__':
    if sys.argv[1:2] == ['cleanup']:
        cleanup_main(sys.argv[2:])
    else:
        main()
