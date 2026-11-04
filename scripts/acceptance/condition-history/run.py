#!/usr/bin/env python3
"""One explicitly admitted public-package condition-history acceptance.

Source preparation and stubbed tests do not authorize invoking this runner.
See README.md for its fixed tuple, command and remaining review gate.
"""
import argparse
import importlib.util
import json
import os
import pathlib
import re
import shutil
import subprocess
import sys
import time
import tomllib
import types
import urllib.request

HERE = pathlib.Path(__file__).resolve().parent
REPO = HERE.parents[2]
spec = importlib.util.spec_from_file_location('installed_native', HERE.parent / 'installed-native/run.py')
native = importlib.util.module_from_spec(spec)
spec.loader.exec_module(native)
spec = importlib.util.spec_from_file_location('condition_history_check', HERE / 'check_history.py')
comparator = importlib.util.module_from_spec(spec)
spec.loader.exec_module(comparator)

H = '4b2440a5618ccc1c98960ea2e1e62127042c3590'
CRATE_SHA = '620a8287af73c363dac61f0a91f20971a39f3b8cdc9513fbea85cdbd95a74ba4'
RUST = 'rust@sha256:300ec56abce8cc9448ddea2172747d048ed902a3090e6b57babb2bf19f754081'
SERVER = 'ghcr.io/durable-workflow/server@sha256:980fe0a052089ced34ec3fcd2be2ea4e77436861d8fb537fb6654cb652a53e07'
SERVER_INDEX = 'sha256:3848d5d5cd1b23bd2881e3fdecea64cc44b72e6fb812e577c2db6d0872d065dd'
SERVER_SOURCE = 'fa3fc2f2f6b8935ea4a6eea47f6a9221e2adafff'
MYSQL = 'mysql@sha256:80f4933e3835f9dc4d35a28ec500d7986cb4414e6c6821c5461239cb7beb8995'
REDIS = 'redis@sha256:ca0acbb137c1dc3339c8b147a58fd6f42775d4599327b50e7b116c23de501af2'
INPUTS = {
    'tests/replay_regression_corpus.rs': 'b54be69c5b82ec6ad32bd2871c3451f182185fa01ca3316a1c3aee935c48c9cc',
    'tests/fixtures/replay-regressions/two-member-condition-group-server-reopen.json':
        '815526c761ce314960335b0f7cafaa0aa60fa519ddd08252451a7a5ebdca821d',
    'tests/fixtures/replay-regressions/timer-separated-condition-group-server-reopen.json':
        'f86ecb0cc57634a3eb4c87ffbd4fa55555b82c2794a16cd7b4ce9d33e009d0f6',
    'tests/fixtures/replay-regressions/cooperative-grouped-condition-reopen.json':
        '50a630596b5d85a392a33cc595fe58a513f59f6ebdbba928fa22be4ea0fb9075',
    'tests/fixtures/replay-regressions/condition-orphaned-duplicate-timeout-keeps-wait.json':
        '012790d1fc23fe11ef9208d3a8243dc282d160903f7df98396c77e1f8fd898f7',
}
CELL = 'condition-history'
PREFIX = 'sdk-rust-condition'
LIMITS = {'mysql': ('1', '1280m'), 'redis': ('0.25', '128m'),
          'bootstrap': ('0.75', '768m'), 'runtime': ('0.75', '768m'),
          'queue': ('0.5', '512m'), 'consumer': ('0.5', '768m')}


def require(condition, label):
    if not condition:
        raise SystemExit(label)


def git(*args):
    return native.call(['git', '-C', str(REPO), *args]).stdout.strip()


def preflight(args):
    # These are explicit checks: Python optimization cannot remove admission.
    require(args.package_commit == H, 'wrong fixed package source')
    require(re.fullmatch('[0-9a-f]{40}', args.runner_commit), 'invalid runner commit')
    require(git('rev-parse', 'HEAD') == args.runner_commit, 'wrong runner head')
    require(not git('status', '--porcelain', '--untracked-files=all'), 'runner checkout is not clean')
    require(git('rev-parse', 'refs/tags/4.12.0^{commit}') == H, 'wrong immutable source tag')
    require(not os.path.lexists(args.work_dir), 'work directory already exists')
    require(re.fullmatch('[0-9a-f]{16}', args.run_token), 'invalid owner token')
    require(args.cleanup_unix == args.started_unix + 1800
            and args.active_unix == args.cleanup_unix - 60, 'wrong absolute deadline envelope')
    require(args.started_unix <= time.time() < args.started_unix + 30, 'stale launch timestamp')
    # No command construction, pull, output directory or Popen precedes this.


def remaining(args):
    seconds = int(args.active_unix - time.time())
    require(seconds > 10, 'active deadline exhausted')
    return seconds


def command(args, argv, **kwargs):
    return native.call(argv, timeout=min(kwargs.pop('timeout', 120), remaining(args)), **kwargs)


def export_consumer(fixture):
    shutil.copytree(HERE / 'fixture', fixture)
    for source, expected in INPUTS.items():
        raw = native.call(['git', '-C', str(REPO), 'show', H + ':' + source], text=False).stdout
        require(native.hashlib.sha256(raw).hexdigest() == expected, 'fixed history input differs')
        target = fixture / ('tests/official.rs' if source == 'tests/replay_regression_corpus.rs' else source)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(raw)


def build(args, fixture, owner, script, phase, network=None):
    name = owner.base + '-' + phase
    limit = remaining(args) - 10
    line = ['docker', 'run', '--rm', '--init', '--name', name, *owner.labels(name),
            '--platform', 'linux/amd64', '--cpus', '2', '--memory', '3g', '--pids-limit', '1024',
            '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges',
            '--user', '%d:%d' % (os.getuid(), os.getgid()),
            '--mount', 'type=bind,src=%s,dst=/work' % fixture, '-w', '/work',
            '-e', 'CARGO_HOME=/work/.cargo', '-e', 'CARGO_BUILD_JOBS=2',
            '-e', 'CARGO_RESOLVER_INCOMPATIBLE_RUST_VERSIONS=fallback']
    if network:
        line += ['--network', network]
    try:
        return command(args, line + [RUST, 'timeout', '--kill-after=10s', str(limit) + 's', 'sh', '-ec', script],
                       timeout=limit + 10)
    finally:
        owner.best_effort('build-remove', phase, native.remove_owned_container, name, owner)


def verify_crate(fixture):
    lock = tomllib.loads((fixture / 'Cargo.lock').read_text())
    packages = [p for p in lock['package'] if p['name'] == 'durable-workflow']
    require(len(packages) == 1 and packages[0]['version'] == '4.12.0'
            and packages[0]['source'] == native.CRATES_IO_SOURCE
            and packages[0]['checksum'] == CRATE_SHA, 'wrong public package lock')
    archives = list((fixture / '.cargo/registry/cache').glob('*/durable-workflow-4.12.0.crate'))
    require(len(archives) == 1 and native.sha256(archives[0]) == CRATE_SHA, 'wrong downloaded archive')
    with native.tarfile.open(archives[0]) as archive:
        vcs = json.load(archive.extractfile('durable-workflow-4.12.0/.cargo_vcs_info.json'))
    require(vcs['git']['sha1'] == H and not vcs['git'].get('dirty'), 'wrong public package VCS')
    require(not (fixture / '.cargo/config.toml').exists(), 'registry replacement is forbidden')
    return {'version': '4.12.0', 'source': H, 'archive_sha256': CRATE_SHA,
            'lock_sha256': native.sha256(fixture / 'Cargo.lock')}


def registry_identity():
    request = urllib.request.Request('https://index.crates.io/du/ra/durable-workflow',
        headers={'User-Agent': 'durable-workflow-condition-history-acceptance'})
    with urllib.request.urlopen(request, timeout=30) as response:
        versions = [json.loads(line) for line in response.read(8 * 1024 * 1024).splitlines()]
    exact = [version for version in versions if version.get('vers') == '4.12.0']
    require(len(exact) == 1 and exact[0].get('cksum') == CRATE_SHA
            and exact[0].get('yanked') is False, 'public registry identity refused')
    return {'version': '4.12.0', 'sha256': CRATE_SHA, 'yanked': False}


class ConditionCell(native.Cell):
    def __init__(self, *args):
        super().__init__(*args)
        self.receipts = []

    def launch(self, line, name, image, child, user, credentials):
        role = name.removeprefix(self.task + '-')
        require(role in LIMITS, 'unknown condition role')
        require(len(self.containers) < 5, 'condition role ceiling reached')
        cpus, memory = LIMITS[role]
        line[line.index('--cpus') + 1] = cpus
        line[line.index('--memory') + 1] = memory
        line += ['--platform', 'linux/amd64']
        if role in ('bootstrap', 'runtime', 'queue'):
            line += ['--mount', 'type=bind,src=%s,dst=/state' % self.state]
        name = super().launch(line, name, image, child, user, credentials)
        # Targeted readback deliberately excludes environment and host paths.
        template = ('{"id":{{json .Id}},"image":{{json .Image}},"memory":{{.HostConfig.Memory}},'
                    '"nano_cpus":{{.HostConfig.NanoCpus}},"pid":{{.State.Pid}},'
                    '"run":{{json (index .Config.Labels "' + native.RUN_LABEL + '")}},'
                    '"owner":{{json (index .Config.Labels "' + native.OWNER_LABEL + '")}},'
                    '"mounts":"{{range .Mounts}}{{.Type}}:{{.Destination}};{{end}}"}')
        receipt = json.loads(native.call(['docker', 'inspect', '--format', template, name]).stdout)
        require(receipt['run'] == self.harness_run.token and receipt['owner'] == self.harness_run.prefix,
                'role readback ownership mismatch')
        require(receipt['memory'] == int(memory.removesuffix('m')) * 1024 * 1024
                and receipt['nano_cpus'] == int(float(cpus) * 1_000_000_000), 'role resource mismatch')
        require('volume:' not in receipt['mounts'], 'anonymous volumes are forbidden')
        receipt.update(role=role, deadline_unix=self.deadline, credentials=credentials)
        self.receipts.append(receipt)
        return name

    def start_php(self, token):
        env = {'APP_ENV': 'testing', 'APP_DEBUG': 'false', 'DB_CONNECTION': 'mysql',
               'DB_HOST': 'mysql', 'DB_DATABASE': 'durable_workflow', 'DB_USERNAME': 'workflow',
               'DB_PASSWORD': 'workflow', 'REDIS_HOST': 'redis', 'QUEUE_CONNECTION': 'redis',
               'CACHE_STORE': 'redis', 'DW_AUTH_DRIVER': 'token', 'DW_WORKER_PROTOCOL_VERSION': '1.20'}
        mysql = self.plain('mysql', MYSQL, [], alias='mysql', tmpfs='/var/lib/mysql', pids=512,
                           caps=('CHOWN', 'DAC_OVERRIDE', 'SETGID', 'SETUID'),
                           env={'MYSQL_DATABASE': 'durable_workflow', 'MYSQL_USER': 'workflow',
                                'MYSQL_PASSWORD': 'workflow', 'MYSQL_ROOT_PASSWORD': 'root'})
        self.plain('redis', REDIS, [], alias='redis', tmpfs='/data', caps=('CHOWN', 'SETGID', 'SETUID'))
        until = min(time.time() + 120, self.deadline - 15)
        while native.call(['docker', 'exec', mysql, 'mysqladmin', 'ping', '-h', 'localhost', '-uworkflow', '-pworkflow'],
                          check=False).returncode:
            require(time.time() < until, 'mysql readiness deadline')
            time.sleep(1)
        bootstrap = self.plain('bootstrap', SERVER, ['server-bootstrap'], env=env,
                               caps=('DAC_OVERRIDE',), credentials='server')
        require(native.call(['docker', 'wait', bootstrap], timeout=max(1, self.deadline - time.time())).stdout.strip() == '0',
                'PHP bootstrap failed')
        native.remove_owned_container(bootstrap, self.harness_run)
        self.containers.remove(bootstrap)
        self.plain('runtime', SERVER, [], env=env, alias='qualification-origin', credentials='both',
                   caps=('CHOWN', 'DAC_OVERRIDE', 'KILL', 'SETGID', 'SETUID'))
        self.plain('queue', SERVER, ['php', 'artisan', 'queue:work', '--sleep=1', '--tries=3', '--max-time=600'],
                   env=env, user='1000:1000', credentials='server')

    def execute(self, args):
        outcome = {'outcome': 'fail', 'deadline_unix': self.deadline}
        try:
            self.cell_path('state').mkdir(parents=True)
            self.dir.chmod(0o777)
            self.state.chmod(0o777)
            command(args, ['docker', 'network', 'create', '--internal', *self.harness_run.labels(self.task), self.task])
            # No service roles exist during loopback replay; then exactly four
            # service roles plus one combined driver/Worker. No proxy/repair.
            replay = self.start('consumer', RUST, ['/fixture/condition-history', '--exact',
                'installed_conditions::installed_history_replay', '--nocapture'])
            require(command(args, ['docker', 'wait', replay], timeout=300).stdout.strip() == '0', 'installed replay failed')
            native.remove_owned_container(replay, self.harness_run)
            self.containers.remove(replay)
            self.start_php(self.token)
            consumer = self.start('consumer', RUST, ['/fixture/condition-history', '--exact',
                'installed_conditions::installed_live_group', '--ignored', '--nocapture'], credentials='auth')
            require(command(args, ['docker', 'wait', consumer], timeout=180).stdout.strip() == '0', 'live group failed')
        finally:
            stopped = self.remove_docker_objects()
            try:
                require(stopped, 'writer cleanup uncertain')
                self.fence_artifacts()
                require(not self.harness_run.cleanup_errors, 'evidence fence failed')
                replay = json.loads(self.artifact_bytes(self.state / 'replay.json'))
                live = json.loads(self.artifact_bytes(self.state / 'live.json'))
                require(replay.get('outcome') == 'pass' and len(replay.get('cases', {})) == 9, 'replay report incomplete')
                outcome.update(outcome='pass', replay=replay, live=live, history_check=comparator.check(live),
                               role_receipts=self.receipts)
            except BaseException:
                outcome['outcome'] = 'fail'
                self.harness_run.best_effort('evidence-purge', CELL, self.purge_evidence)
                raise
        return outcome


def recover(root, token):
    record = json.loads(native.confined_child(root, 'run.json').read_text())
    require(record.get('owner') == PREFIX and record.get('token') == token
            and record.get('cells') == [CELL], 'wrong recovery ownership')
    owner = native.Run(PREFIX, token=token)
    result = native.recover_run(root, owner, [CELL])
    require(result['pass'], 'recovery has survivors or uncertainty')
    return result


def execute(args):
    preflight(args)
    args.work_dir.mkdir(mode=0o700)
    owner = native.Run(PREFIX, token=args.run_token)
    record = {'owner': PREFIX, 'token': owner.token, 'cells': [CELL], 'package_commit': H,
              'runner_commit': args.runner_commit, 'started_unix': args.started_unix,
              'active_unix': args.active_unix, 'cleanup_unix': args.cleanup_unix}
    native.write_owner_only(args.work_dir / 'run.json', json.dumps(record))
    endpoint = native.call(['docker', 'context', 'inspect', '--format', '{{.Endpoints.docker.Host}}']).stdout.strip()
    require(endpoint.startswith('unix://'), 'only the admitted local Docker daemon is supported')
    public_config = args.work_dir / 'public-docker'
    public_config.mkdir(mode=0o700)
    native.write_owner_only(public_config / 'config.json', '{"auths":{}}\n')
    # Keep the local daemon endpoint while removing registry logins and helpers.
    # The endpoint is private evidence and is never serialized into the summary.
    os.environ['DOCKER_HOST'] = endpoint
    os.environ['DOCKER_CONFIG'] = str(public_config)
    # The detached recovery process survives this client's death. Every Docker
    # role also enforces its own absolute TERM/KILL deadline independently.
    watchdog = subprocess.Popen([sys.executable, str(HERE / 'run.py'), 'recover', '--work-dir', str(args.work_dir),
        '--run-token', owner.token, '--at', str(args.cleanup_unix)], start_new_session=True,
        stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    result = {'outcome': 'fail', 'run': record}
    try:
        registry = registry_identity()
        images = [RUST, SERVER, MYSQL, REDIS]
        for image in images:
            command(args, ['docker', 'pull', '--platform', 'linux/amd64', image], timeout=600)
        identities = {image: native.image_identity(image) for image in images}
        require(all(i['os'] == 'linux' and i['architecture'] == 'amd64' for i in identities.values()), 'wrong image architecture')
        for image in images:
            declared = json.loads(command(args, ['docker', 'image', 'inspect', '--format', '{{json .Config.Volumes}}', image]).stdout) or {}
            covered = {MYSQL: {'/var/lib/mysql'}, REDIS: {'/data'}}.get(image, set())
            require(set(declared).issubset(covered), 'uncovered image volume declaration')
        labels = json.loads(command(args, ['docker', 'image', 'inspect', '--format', '{{json .Config.Labels}}', SERVER]).stdout)
        require(labels.get('org.opencontainers.image.revision') == SERVER_SOURCE, 'wrong PHP Server source')
        fixture = args.work_dir / 'fixture'
        export_consumer(fixture)
        sources = {p.relative_to(fixture).as_posix(): native.sha256(p)
                   for p in fixture.rglob('*') if p.is_file()}
        fetch = build(args, fixture, owner, 'cargo generate-lockfile && cargo fetch --locked', 'fetch')
        (fixture / 'fetch.stdout').write_text(fetch.stdout)
        package = verify_crate(fixture)
        compiled = build(args, fixture, owner, 'rustc -V && cargo -V && cargo test --locked --offline --test conditions --no-run --message-format=json',
                         'build', network='none')
        (fixture / 'build.stdout').write_text(compiled.stdout)
        (fixture / 'build.stderr').write_text(compiled.stderr)
        toolchain = compiled.stdout.splitlines()[:2]
        require(len(toolchain) == 2 and toolchain[0].startswith('rustc 1.86.0 ')
                and toolchain[1].startswith('cargo 1.86.0 '), 'wrong actual Rust/Cargo toolchain')
        artifacts = [json.loads(line) for line in compiled.stdout.splitlines() if line.startswith('{')]
        executables = [p['executable'] for p in artifacts if p.get('reason') == 'compiler-artifact'
                       and p.get('target', {}).get('name') == 'conditions' and p.get('executable')]
        require(len(executables) == 1 and executables[0].startswith('/work/target/debug/deps/conditions-'), 'wrong consumer executable')
        binary = native.confined_child(fixture, executables[0].removeprefix('/work/'))
        output = fixture / 'target/debug/condition-history'
        shutil.copyfile(binary, output)
        output.chmod(0o755)
        binary_sha = native.sha256(output)
        require(not native.run_objects('container', owner), 'build/runtime overlap')
        cell = ConditionCell(types.SimpleNamespace(rust_image=RUST), CELL, args.work_dir,
                             {'executed_manifest': SERVER}, fixture, owner)
        cell.deadline = min(int(time.time()) + 600, args.active_unix - 10)
        outcome = cell.execute(args)
        require(native.sha256(output) == binary_sha, 'consumer binary changed')
        require(verify_crate(fixture) == package, 'public package changed')
        require(all(native.sha256(fixture / path) == digest for path, digest in sources.items()), 'consumer source changed')
        result.update(outcome='pass', package=package, binary_sha256=binary_sha,
                      input_sha256=INPUTS, consumer_source_sha256=sources, toolchain=toolchain,
                      registry=registry, images=identities, server_index=SERVER_INDEX, cell=outcome)
    except BaseException as error:
        result['error_type'] = type(error).__name__
    finally:
        # Preserve only fenced Cell evidence during normal completion. Recovery
        # purges interrupted cells, where the client no longer has secret bytes.
        for kind in ('container', 'network'):
            result['leftover_' + kind] = owner.best_effort('final-query', kind, native.run_objects, kind, owner)
        result['cleanup_errors'] = owner.cleanup_errors
        if owner.cleanup_errors or result['leftover_container'] != [] or result['leftover_network'] != []:
            result['outcome'] = 'fail'
            owner.best_effort('uncertain-recovery', CELL, recover, args.work_dir, owner.token)
        data = json.dumps(result, indent=2).encode()
        clean, leaked = native.redact_credentials(data, owner.secret_values)
        if leaked:
            result = {'outcome': 'fail', 'error_type': 'CredentialOutput'}
            clean = json.dumps(result).encode()
            owner.best_effort('credential-purge', CELL, recover, args.work_dir, owner.token)
        (args.work_dir / 'summary.json').write_bytes(clean + b'\n')
        # Cancel the backstop only after a direct zero-survivor readback. A
        # failed query leaves it alive to perform the independent final sweep.
        if native.run_objects('container', owner) == [] and native.run_objects('network', owner) == []:
            watchdog.terminate()
            watchdog.wait(timeout=10)
    require(result['outcome'] == 'pass', 'acceptance failed; inspect bounded summary')


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=('run', 'recover'))
    parser.add_argument('--work-dir', required=True, type=native.cli_path)
    parser.add_argument('--run-token', required=True)
    parser.add_argument('--package-commit')
    parser.add_argument('--runner-commit')
    parser.add_argument('--started-unix', type=int)
    parser.add_argument('--active-unix', type=int)
    parser.add_argument('--cleanup-unix', type=int)
    parser.add_argument('--at', type=int)
    args = parser.parse_args(argv)
    if args.mode == 'recover':
        if args.at is not None:
            time.sleep(max(0, args.at - time.time()))
        recover(args.work_dir, args.run_token)
    else:
        execute(args)


if __name__ == '__main__':
    main()
