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


def build_fixture(args, fixture):
    shutil.copytree(HERE / 'fixture' / 'src', fixture / 'src')
    template = (HERE / 'fixture' / 'Cargo.toml.in').read_text()
    (fixture / 'Cargo.toml').write_text(template.replace('@CRATE_VERSION@', args.crate_version))
    call(['docker', 'pull', args.rust_image], timeout=1800)
    command = ['docker', 'run', '--rm', '--user', '%d:%d' % (os.getuid(), os.getgid()),
               '-v', '%s:/work' % fixture, '-w', '/work', '-e', 'CARGO_HOME=/work/.cargo',
               '-e', 'CARGO_RESOLVER_INCOMPATIBLE_RUST_VERSIONS=fallback', args.rust_image, 'sh', '-c',
               'rustc -V && cargo -V && cargo generate-lockfile && cargo build --locked -j %d' % args.jobs]
    build = call(command, timeout=3600)
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
    def __init__(self, args, name, root, server, fixture, stamp):
        self.args, self.name, self.server, self.fixture = args, name, server, fixture
        self.dir = root / 'cells' / name
        self.state = self.dir / 'state'
        self.task = 'sdkrust-accept-%s-%s' % (stamp, name)
        self.containers = []

    def start(self, role, image, command, user=None, env=None, alias=None, mounts=()):
        name = '%s-%s' % (self.task, role)
        line = ['docker', 'run', '-d', '--init', '--read-only', '--name', name, '--label', 'task=' + self.task,
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
        call(line + [image] + command)
        self.containers.append(name)
        return name

    def plain(self, role, image, command, env=None, alias=None, user=None, tmpfs=None):
        """Start a PHP Server process-set container (writable root, as in its compose file)."""
        name = '%s-%s' % (self.task, role)
        line = ['docker', 'run', '-d', '--name', name, '--label', 'task=' + self.task, '--network', self.task,
                '--memory', '1g', '--cpus', '1']
        if user:
            line += ['--user', user]
        if alias:
            line += ['--network-alias', alias]
        if tmpfs:
            line += ['--tmpfs', tmpfs]
        for key, value in (env or {}).items():
            line += ['-e', '%s=%s' % (key, value)]
        call(line + [image] + command)
        self.containers.append(name)
        return name

    def start_php(self, token):
        image = self.server['executed_manifest']
        env = {'APP_ENV': 'testing', 'APP_DEBUG': 'false', 'DW_SERVER_KEY': 'base64:' + __import__('base64').b64encode(secrets.token_bytes(32)).decode(),
               'DB_CONNECTION': 'mysql', 'DB_HOST': 'mysql', 'DB_DATABASE': 'durable_workflow', 'DB_USERNAME': 'workflow',
               'DB_PASSWORD': 'workflow', 'REDIS_HOST': 'redis', 'QUEUE_CONNECTION': 'redis', 'CACHE_STORE': 'redis',
               'DW_AUTH_DRIVER': 'token', 'DW_AUTH_TOKEN': token, 'DW_WORKFLOW_TASK_TIMEOUT': '10',
               'DW_WORKER_PROTOCOL_VERSION': '1.20'}
        mysql = self.plain('mysql', MYSQL_IMAGE, [], alias='mysql', tmpfs='/var/lib/mysql',
                           env={'MYSQL_DATABASE': 'durable_workflow', 'MYSQL_USER': 'workflow',
                                'MYSQL_PASSWORD': 'workflow', 'MYSQL_ROOT_PASSWORD': 'root'})
        self.plain('redis', REDIS_IMAGE, [], alias='redis', tmpfs='/data')
        deadline = time.monotonic() + 120
        while call(['docker', 'exec', mysql, 'mysqladmin', 'ping', '-h', 'localhost', '-uworkflow', '-pworkflow'], check=False).returncode:
            if time.monotonic() > deadline:
                raise RuntimeError('mysql never became ready')
            time.sleep(1)
        bootstrap = self.plain('bootstrap', image, ['server-bootstrap'], env=env)
        if call(['docker', 'wait', bootstrap], timeout=600).stdout.strip() != '0':
            raise RuntimeError('PHP Server bootstrap failed')
        self.plain('runtime', image, [], env=env, alias='qualification-origin')
        self.plain('queue', image, ['php', 'artisan', 'queue:work', '--sleep=1', '--tries=3', '--max-time=1800'], env=env, user='1000:1000')
        self.plain('repair', image, ['sh', '-c', 'while true; do php artisan workflow:v2:repair-pass --json; sleep 2; done'],
                   env=env, user='1000:1000')

    def http(self, container, path, token):
        code = ('import json,sys,urllib.request\n'
                'r=urllib.request.Request("http://qualification-origin:8080/api%s",headers={"Authorization":"Bearer "+sys.argv[1],"X-Namespace":"default"})\n'
                'print(urllib.request.urlopen(r,timeout=3).read().decode())' % path)
        return call(['docker', 'exec', container, 'python', '-c', code, token], check=False)

    def run(self):
        self.state.mkdir(parents=True)
        for path in [self.dir, self.state]:
            path.chmod(0o777)
        for sub in ['database', 'storage']:
            (self.state / sub).mkdir()
            (self.state / sub).chmod(0o777)
        token = 'fixture-' + secrets.token_hex(24)
        credentials = self.state / 'credentials.json'
        credentials.write_text(json.dumps(dict.fromkeys(['operator', 'worker', 'admin'], token)))
        credentials.chmod(0o644)
        binary_before = sha256(self.fixture / 'target' / 'debug' / BINARY)
        call(['docker', 'network', 'create', '--internal', '--label', 'task=' + self.task, self.task])
        try:
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
            logs = self.dir / 'logs'
            logs.mkdir(exist_ok=True)
            for name in self.containers:
                out = call(['docker', 'logs', '--tail', '300', name], check=False)
                (logs / (name.removeprefix(self.task + '-') + '.stdout')).write_text(out.stdout)
                (logs / (name.removeprefix(self.task + '-') + '.stderr')).write_text(out.stderr)
            for name in reversed(self.containers):
                call(['docker', 'rm', '-f', name], check=False)
            call(['docker', 'network', 'rm', self.task], check=False)
            credentials.unlink(missing_ok=True)
            for sub in ['database', 'storage']:
                shutil.rmtree(self.state / sub, ignore_errors=True)


def main():
    args = parse_args()
    if args.work_dir.exists():
        raise SystemExit('work dir %s already exists; use a new one' % args.work_dir)
    args.work_dir.mkdir(parents=True)
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dt%H%M%S')
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
            summary['build'] = build_fixture(args, fixture)
        summary['crate'] = crate_provenance(args, fixture)
        # Pull first: inspect alone fails on a clean host that never fetched the helper image.
        call(['docker', 'pull', '--quiet', args.python_image], timeout=600)
        summary['python_image'] = image_identity(args.python_image)
        summary['cells'] = {name: Cell(args, name, args.work_dir, summary['server'], fixture, stamp).run()
                            for name in args.cells.split(',')}
        passed = (summary['crate']['baselines']['pass']
                  and summary.get('baselines_control', {'pass': True})['pass']
                  and all(c['outcome'] == 'pass' for c in summary['cells'].values()))
        summary['outcome'] = 'pass' if passed else 'fail'
    except Exception as error:
        summary['error'] = '%s: %s' % (type(error).__name__, error)
        raise
    finally:
        summary['finished_at'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        (args.work_dir / 'summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2) + '\n')
        leftovers = call(['docker', 'ps', '-aq', '--filter', 'label=task'], check=False).stdout.split()
        leftovers = [c for c in leftovers if 'sdkrust-accept' in call(['docker', 'inspect', '--format', '{{.Name}}', c], check=False).stdout]
        print(json.dumps({'outcome': summary['outcome'], 'error': summary.get('error'),
                          'summary': str(args.work_dir / 'summary.json'), 'leftover_containers': leftovers}))
    sys.exit(0 if summary['outcome'] == 'pass' else 1)


if __name__ == '__main__':
    main()
