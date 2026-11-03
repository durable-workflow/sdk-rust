#!/usr/bin/env python3
"""Installed-package lifecycle acceptance of the Rust SDK against a native Server image.

Everything runs in disposable local containers on an internal Docker network,
against one exact Server image index digest and one exact published crate
version. Nothing here contacts Cloud. See README.md for usage.
"""
import argparse
import datetime
import hashlib
import io
import json
import os
import pathlib
import re
import secrets
import shutil
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
    """The ID of container/network `name` if it carries this run's token, else None."""
    result = call(['docker', kind, 'inspect', '--format',
                   '{{.Id}} {{index .%s "%s"}}' % ('Config.Labels' if kind == 'container' else 'Labels', RUN_LABEL),
                   name], check=False)
    fields = result.stdout.split() if result.returncode == 0 else []
    if len(fields) == 2 and fields[1] == run.token:
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


def remove_owned_network(name, run):
    network_id = owned_id('network', name, run)
    if network_id:
        call(['docker', 'network', 'rm', network_id])
    return network_id


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--crate-version', required=True, help='exact published durable-workflow version, e.g. 4.10.3')
    parser.add_argument('--server-index', required=True, help='exact image index digest, sha256:...')
    parser.add_argument('--server-image', default='ghcr.io/durable-workflow/server')
    parser.add_argument('--runtime', choices=['native', 'php'], default='native',
                        help='php starts the PHP Server process set (MySQL, Redis, queue worker, repair) as a parity baseline')
    parser.add_argument('--fixture-from', type=pathlib.Path,
                        help='reuse a fixture directory built by an earlier run (provenance is checked again)')
    parser.add_argument('--platform', default='linux/amd64')
    parser.add_argument('--expect-server-version', help='required /api/cluster/info version, e.g. 3.0.1')
    parser.add_argument('--expect-revision', help='required org.opencontainers.image.revision label')
    parser.add_argument('--expect-release-tag', help='required dev.durable-workflow.release.tag label')
    parser.add_argument('--baselines', choices=['required', 'absent'], default='required',
                        help='whether the published crate must carry the per-major Server baseline map')
    parser.add_argument('--expected-baselines', type=json.loads, default={'2': '2.0.0', '3': '3.0.2'},
                        help='JSON map the published crate must declare when --baselines required')
    parser.add_argument('--baselines-control', type=pathlib.Path,
                        help='a Cargo.toml that must pass the required-map check (positive control)')
    parser.add_argument('--source-tag', help='git tag the crate must match (default: the crate version)')
    parser.add_argument('--rust-image', default='rust:1.86-bookworm')
    parser.add_argument('--python-image', default='python:3.12-bookworm')
    parser.add_argument('--cells', default='lifecycle,deadline')
    parser.add_argument('--jobs', type=int, default=2, help='cargo build jobs (lab machines: keep at 2)')
    parser.add_argument('--work-dir', type=pathlib.Path, required=True, help='new directory for build output and evidence')
    parser.add_argument('--container-prefix', type=container_prefix, default='sdkrust',
                        help='owner prefix for container, network and task names, e.g. general or security '
                             '(^%s$)' % PREFIX_PATTERN.pattern)
    return parser.parse_args()


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


def build_fixture(args, fixture, run):
    shutil.copytree(HERE / 'fixture' / 'src', fixture / 'src')
    template = (HERE / 'fixture' / 'Cargo.toml.in').read_text()
    (fixture / 'Cargo.toml').write_text(template.replace('@CRATE_VERSION@', args.crate_version))
    call(['docker', 'pull', args.rust_image], timeout=1800)
    # Bounded like every other harness container: --init, CPU/RAM/PID caps and an
    # in-container wall bound. It keeps the default network only for the
    # crates.io fetch and mounts only the fixture directory.
    name = run.base + '-build'
    command = (['docker', 'run', '--rm', '--init', '--name', name] + run.labels(name) +
               ['--cpus', str(max(1, args.jobs)), '--memory', '3g', '--pids-limit', '1024',
                '--security-opt', 'no-new-privileges', '--cap-drop', 'ALL',
                '--user', '%d:%d' % (os.getuid(), os.getgid()),
                '-v', '%s:/work' % fixture, '-w', '/work', '-e', 'CARGO_HOME=/work/.cargo',
                '-e', 'CARGO_RESOLVER_INCOMPATIBLE_RUST_VERSIONS=fallback', args.rust_image,
                'timeout', '--kill-after=%ds' % BUILD_KILL_AFTER_SECONDS, '%ds' % BUILD_WALL_SECONDS, 'sh', '-c',
                'rustc -V && cargo -V && cargo generate-lockfile && cargo build --locked -j %d' % args.jobs])
    try:
        build = call(command, timeout=BUILD_CLIENT_TIMEOUT)
    finally:
        # Covers a client-side timeout or error while this process is alive. If
        # the client dies, the in-container timeout ends the build and --rm fires.
        # A cleanup failure is recorded and never replaces the build's own error.
        run.best_effort('build-cleanup', name, remove_owned_container, name, run)
    (fixture / 'build.stdout').write_text(build.stdout)
    (fixture / 'build.stderr').write_text(build.stderr)
    return {'rust_image': image_identity(args.rust_image), 'toolchain': build.stdout.strip().splitlines()[:2],
            'binary_sha256': sha256(fixture / 'target' / 'debug' / BINARY)}


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
        self.dir = root / 'cells' / name
        self.state = self.dir / 'state'
        self.task = '%s-%s' % (run.base, name)
        self.containers = []

    def start(self, role, image, command, user=None, env=None, alias=None, mounts=()):
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
        self.containers.append(name)
        call(line + [image] + command)
        return name

    def plain(self, role, image, command, env=None, alias=None, user=None, tmpfs=None, caps=(), pids=256):
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
        self.containers.append(name)
        call(line + [image] + command)
        return name

    def start_php(self, token):
        image = self.server['executed_manifest']
        env = {'APP_ENV': 'testing', 'APP_DEBUG': 'false', 'DW_SERVER_KEY': 'base64:' + __import__('base64').b64encode(secrets.token_bytes(32)).decode(),
               'DB_CONNECTION': 'mysql', 'DB_HOST': 'mysql', 'DB_DATABASE': 'durable_workflow', 'DB_USERNAME': 'workflow',
               'DB_PASSWORD': 'workflow', 'REDIS_HOST': 'redis', 'QUEUE_CONNECTION': 'redis', 'CACHE_STORE': 'redis',
               'DW_AUTH_DRIVER': 'token', 'DW_AUTH_TOKEN': token, 'DW_WORKFLOW_TASK_TIMEOUT': '10',
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
        bootstrap = self.plain('bootstrap', image, ['server-bootstrap'], env=env, caps=('DAC_OVERRIDE',))
        if call(['docker', 'wait', bootstrap], timeout=600).stdout.strip() != '0':
            raise RuntimeError('PHP Server bootstrap failed')
        # Apache's root master also chowns its mutex to www-data, switches its
        # children to www-data and signals them.
        self.plain('runtime', image, [], env=env, alias='qualification-origin',
                   caps=('CHOWN', 'DAC_OVERRIDE', 'KILL', 'SETGID', 'SETUID'))
        # queue and repair run as uid 1000 and keep no capabilities.
        self.plain('queue', image, ['php', 'artisan', 'queue:work', '--sleep=1', '--tries=3', '--max-time=1800'], env=env, user='1000:1000')
        self.plain('repair', image, ['sh', '-c', 'while true; do php artisan workflow:v2:repair-pass --json; sleep 2; done'],
                   env=env, user='1000:1000')

    def http(self, container, path, token):
        code = ('import json,sys,urllib.request\n'
                'r=urllib.request.Request("http://qualification-origin:8080/api%s",headers={"Authorization":"Bearer "+sys.argv[1],"X-Namespace":"default"})\n'
                'print(urllib.request.urlopen(r,timeout=3).read().decode())' % path)
        return call(['docker', 'exec', container, 'python', '-c', code, token], check=False)

    def run(self):
        token = 'fixture-' + secrets.token_hex(24)
        credentials = self.state / 'credentials.json'
        # All state setup is inside the protected region: a partial setup (say,
        # database/ created and storage/ failing) is still removed and verified.
        try:
            self.state.mkdir(parents=True)
            for path in [self.dir, self.state]:
                path.chmod(0o777)
            for sub in ['database', 'storage']:
                (self.state / sub).mkdir()
                (self.state / sub).chmod(0o777)
            # Owner-only from creation: the readers (worker, driver, history check)
            # run as the host UID, and the server roles get the token by env.
            write_owner_only(credentials, json.dumps(dict.fromkeys(['operator', 'worker', 'admin'], token)))
            binary_before = sha256(self.fixture / 'target' / 'debug' / BINARY)
            # Inside the protected region: the daemon can create the network even
            # when the client then times out. Removal is ownership-checked.
            call(['docker', 'network', 'create', '--internal'] + self.harness_run.labels(self.task) + [self.task])
            if self.args.runtime == 'php':
                self.start_php(token)
            else:
                self.start('runtime', self.server['executed_manifest'], [], user='1000:1000', alias='qualification-origin',
                           env={'DB_CONNECTION': 'sqlite', 'DB_DATABASE': '/app/database/runtime.sqlite',
                                'DW_AUTH_TOKEN': token, 'DW_SERVER_KEY': 'fixture-' + secrets.token_hex(16),
                                'DW_RUST_MAINTENANCE_LOOP': '1'},
                           mounts=['type=bind,src=%s,dst=/app/database' % (self.state / 'database'),
                                   'type=bind,src=%s,dst=/app/storage' % (self.state / 'storage')])
            proxy = self.start('proxy', self.args.python_image, ['python', '/harness/proxy.py'], alias='qualification-runtime')
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
            worker = self.start('worker', self.args.rust_image, ['/fixture/' + BINARY, 'worker'])
            driver = self.start('driver', self.args.rust_image, ['/fixture/' + BINARY, 'drive-' + self.name])
            driver_exit = int(call(['docker', 'wait', driver], timeout=300).stdout.strip())
            checker = self.start('history', self.args.python_image, ['python', '/harness/check_history.py'])
            history_exit = int(call(['docker', 'wait', checker], timeout=120).stdout.strip())
            worker_running = call(['docker', 'inspect', '--format', '{{.State.Running}}', worker]).stdout.strip() == 'true'
            read = lambda name: json.loads((self.state / name).read_text()) if (self.state / name).exists() else None
            acks = [json.loads(l) for l in (self.state / 'acks.jsonl').read_text().splitlines()] if (self.state / 'acks.jsonl').exists() else []
            result = {'cell': self.name, 'cluster_info': {k: cluster.get(k) for k in
                      ('version', 'implementation', 'development', 'qualified_for_php_takeover', 'error') if k in cluster},
                      'driver_exit': driver_exit, 'history_exit': history_exit,
                      'same_worker_running_after_cases': worker_running,
                      'binary_unchanged': sha256(self.fixture / 'target' / 'debug' / BINARY) == binary_before,
                      'driver': read('driver.json'), 'history': read('history.json'), 'acknowledgements': acks}
            result['outcome'] = 'pass' if (driver_exit == 0 and history_exit == 0 and worker_running
                                           and result['binary_unchanged']) else 'fail'
            return result
        finally:
            try:
                self.remove_docker_objects()
            finally:
                # Local credential and state cleanup always runs, whatever Docker did.
                self.remove_local_state(credentials)

    def remove_local_state(self, credentials):
        """Remove the credential file, database/ and storage/ independently, then
        prove each is gone. Failures and survivors are recorded with bounded
        labels (cell/path) and error types only, and fail the run."""
        run = self.harness_run
        paths = [('credentials.json', credentials, lambda path: path.unlink(missing_ok=True))]
        paths += [(sub, self.state / sub, remove_tree) for sub in ('database', 'storage')]
        for label, path, remove in paths:
            run.best_effort('local-remove', '%s/%s' % (self.name, label), remove, path)
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
        run.best_effort('logs-dir', self.task, logs.mkdir, exist_ok=True)
        owned = [(name, run.best_effort('container-inspect', name, owned_id, 'container', name, run))
                 for name in self.containers]
        for name, container_id in owned:
            if container_id:
                run.best_effort('container-logs', name, self.save_logs, logs, name, container_id)
        for name, container_id in reversed(owned):
            if container_id:
                run.best_effort('container-remove', name, call, ['docker', 'rm', '-f', container_id])
        run.best_effort('network-cleanup', self.task, remove_owned_network, self.task, run)

    def save_logs(self, logs, name, container_id):
        out = call(['docker', 'logs', '--tail', '300', container_id], check=False)
        (logs / (name.removeprefix(self.task + '-') + '.stdout')).write_text(out.stdout)
        (logs / (name.removeprefix(self.task + '-') + '.stderr')).write_text(out.stderr)


def main():
    args = parse_args()
    if args.work_dir.exists():
        raise SystemExit('work dir %s already exists; use a new one' % args.work_dir)
    args.work_dir.mkdir(parents=True)
    run = Run(args.container_prefix)
    summary = {'started_at': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'outcome': 'fail',
               'harness_commit': call(['git', '-C', str(REPO), 'rev-parse', 'HEAD'], check=False).stdout.strip()}
    try:
        if args.baselines_control:
            control = tomllib.loads(args.baselines_control.read_text())
            summary['baselines_control'] = dict(check_baselines(
                control['package'].get('metadata', {}).get('durable-workflow', {}), 'required', args.expected_baselines),
                source=str(args.baselines_control))
        summary['server'] = server_identity(args)
        if args.fixture_from:
            fixture = args.fixture_from.resolve()
            summary['build'] = {'reused_fixture': str(fixture), 'toolchain': (fixture / 'build.stdout').read_text().strip().splitlines()[:2],
                                'binary_sha256': sha256(fixture / 'target' / 'debug' / BINARY)}
        else:
            fixture = args.work_dir / 'fixture'
            summary['build'] = build_fixture(args, fixture, run)
        summary['crate'] = crate_provenance(args, fixture)
        # Pull first: inspect alone fails on a clean host that never fetched the helper image.
        call(['docker', 'pull', '--quiet', args.python_image], timeout=600)
        summary['python_image'] = image_identity(args.python_image)
        cells = {}
        for name in args.cells.split(','):
            cells[name] = Cell(args, name, args.work_dir, summary['server'], fixture, run).run()
        summary['cells'] = cells
        passed = (summary['crate']['baselines']['pass']
                  and summary.get('baselines_control', {'pass': True})['pass']
                  and all(c['outcome'] == 'pass' for c in summary['cells'].values()))
        summary['outcome'] = 'pass' if passed else 'fail'
    except Exception as error:
        summary['error'] = '%s: %s' % (type(error).__name__, error)
        raise
    finally:
        run_filter = 'label=%s=%s' % (RUN_LABEL, run.token)
        # None means the query itself failed (recorded in cleanup_errors).
        leftovers = run.best_effort('leftover-query', 'containers', lambda: call(
            ['docker', 'ps', '-aq', '--filter', run_filter], check=False).stdout.split())
        leftover_networks = run.best_effort('leftover-query', 'networks', lambda: call(
            ['docker', 'network', 'ls', '-q', '--filter', run_filter], check=False).stdout.split())
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
        (args.work_dir / 'summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2) + '\n')
        print(json.dumps({'outcome': summary['outcome'], 'error': summary.get('error'),
                          'summary': str(args.work_dir / 'summary.json'), 'leftover_containers': leftovers,
                          'leftover_networks': leftover_networks, 'local_leftovers': run.local_leftovers,
                          'cleanup_errors': run.cleanup_errors}))
    sys.exit(0 if summary['outcome'] == 'pass' else 1)


if __name__ == '__main__':
    main()
