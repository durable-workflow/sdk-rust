#!/usr/bin/env python3
"""One explicitly admitted public-package condition-history acceptance.

Source preparation and stubbed tests do not authorize invoking this runner.
See README.md for its fixed tuple, command and remaining review gate.
"""
import json
import os
import pathlib
import shutil
import subprocess
import sys
import time
import tomllib
import types
import urllib.request
import sealed_subject as subject

HERE = subject.CURRENT.root / 'scripts/acceptance/condition-history'
REPO = pathlib.Path(subject.ARGS.checkout)
native = subject.CURRENT.module('native')
comparator = subject.CURRENT.module('comparator')
resources = subject.CURRENT.module('resources')
recovery = subject.CURRENT.module('recovery')

H = '4b2440a5618ccc1c98960ea2e1e62127042c3590'
CRATE_SHA = '620a8287af73c363dac61f0a91f20971a39f3b8cdc9513fbea85cdbd95a74ba4'
RUST = 'rust@sha256:300ec56abce8cc9448ddea2172747d048ed902a3090e6b57babb2bf19f754081'
SERVER = 'ghcr.io/durable-workflow/server@sha256:980fe0a052089ced34ec3fcd2be2ea4e77436861d8fb537fb6654cb652a53e07'
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


def command(args, argv, **kwargs):
    subject.CURRENT.verify()
    return resources.call(args, argv, **kwargs)


def export_consumer(fixture):
    fixture.mkdir(mode=0o700)
    subject.CURRENT.export(subject.PREFIX + 'fixture/', fixture)
    for source, expected in INPUTS.items():
        raw = command(subject.ARGS, ['git', '--no-replace-objects', '-C', str(REPO),
                                    'show', H + ':' + source], text=False).stdout
        require(subject.digest(raw) == expected, 'fixed history input differs')
        target = fixture / ('tests/official.rs' if source == 'tests/replay_regression_corpus.rs' else source)
        target.parent.mkdir(parents=True, exist_ok=True)
        subject.exclusive(target, raw)


def build(args, fixture, controller, script, phase, network=None):
    user = '%d:%d' % (os.getuid(), os.getgid())
    spec = controller.profile(phase, RUST, ['sh', '-ec', script], user, 'none', fixture,
        env={'CARGO_HOME': '/work/.cargo', 'CARGO_BUILD_JOBS': '2',
             'CARGO_RESOLVER_INCOMPATIBLE_RUST_VERSIONS': 'fallback'},
        deadline=args.active_unix - 10)
    name = controller.start(spec)
    try:
        code = command(args, ['docker', 'wait', name], timeout=max(1, args.active_unix - time.time())).stdout.strip()
        logs = command(args, ['docker', 'logs', '--tail', '2000', name], check=False)
        require(code == '0', 'bounded build phase failed')
        return logs
    finally:
        object_id = recovery.owned(controller.record, 'container', name)
        if object_id:
            recovery.docker(controller.record, ['rm', '-fv', object_id])


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
        require(role in LIMITS and len(self.containers) < 5, 'unknown role or runtime ceiling')
        env = dict(line[i + 1].split('=', 1) for i, value in enumerate(line) if value == '-e')
        alias = line[line.index('--network-alias') + 1] if '--network-alias' in line else None
        spec = self.controller.profile(role, image, child, user, credentials, self.fixture, self.state,
                                       env=env, alias=alias, deadline=self.deadline)
        self.containers.append(name)
        return self.controller.start(spec,
            self.token if credentials in ('auth', 'both') else '',
            self.server_key if credentials in ('server', 'both') else '')

    def remove_docker_objects(self):
        errors_before = len(self.harness_run.cleanup_errors)
        self.harness_run.best_effort('logs-dir', CELL, lambda: self.cell_path('logs').mkdir(exist_ok=True))
        for name in self.containers:
            try:
                object_id = recovery.owned(self.controller.record, 'container', name)
                if object_id:
                    self.harness_run.best_effort('container-logs', name, self.save_logs, self.dir / 'logs', name, object_id)
            except BaseException:
                self.harness_run.cleanup_errors.append({'step': 'ownership', 'object': name, 'error': 'Refused'})
        result = recovery.cleanup(self.controller.record)
        if not result['pass']:
            self.harness_run.cleanup_errors.append({'step': 'cleanup', 'object': CELL, 'error': 'Uncertain'})
        return result['pass'] and not any(e['step'] == 'ownership' for e in self.harness_run.cleanup_errors[errors_before:])

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
        object_id = recovery.owned(self.controller.record, 'container', bootstrap)
        require(object_id, 'bootstrap ownership changed')
        recovery.docker(self.controller.record, ['rm', '-fv', object_id])
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
            network = ['docker', 'network', 'create', '--internal']
            for key, value in self.controller.labels('network').items():
                network += ['--label', key + '=' + value]
            command(args, network + [self.task])
            # No service roles exist during loopback replay; then exactly four
            # service roles plus one combined driver/Worker. No proxy/repair.
            replay = self.start('consumer', RUST, ['/fixture/condition-history', '--exact',
                'installed_conditions::installed_history_replay', '--nocapture'])
            require(command(args, ['docker', 'wait', replay], timeout=300).stdout.strip() == '0', 'installed replay failed')
            object_id = recovery.owned(self.controller.record, 'container', replay)
            require(object_id, 'replay ownership changed')
            recovery.docker(self.controller.record, ['rm', '-fv', object_id])
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
                               role_receipts=self.controller.receipts)
            except BaseException:
                outcome['outcome'] = 'fail'
                self.harness_run.best_effort('evidence-purge', CELL, self.purge_evidence)
                raise
        return outcome


def start_recovery(args, record):
    program = subject.CURRENT.read(subject.PREFIX + 'recover.py').decode()
    require(subject.digest(program.encode()) == record['recovery_sha256'], 'captured recovery source mismatch')
    recovery_argv = [sys.executable, '-I', '-S', '-c', program, 'recover', '--work-dir', args.work_dir,
        '--record-sha256', subject.RECORD_SHA, '--manifest-sha256', subject.CURRENT.sha256,
        '--runner-commit', args.runner_commit, '--package-commit', H, '--run-token', args.run_token,
        '--at', str(args.active_unix)]
    watchdog = subprocess.Popen(recovery_argv, start_new_session=True, stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=resources.environment(args))
    try:
        process = recovery.process_identity(watchdog.pid)
        require(process is not None, 'recovery did not start')
        recovery_process_sha = subject.exclusive(pathlib.Path(args.work_dir) / 'recovery-process.json', subject.canonical({
            'source_sha256': record['recovery_sha256'], 'process': process}))
        ready_path = pathlib.Path(args.work_dir) / 'recovery-ready.json'
        until = min(time.time() + 5, args.active_unix - 10)
        while not ready_path.exists():
            require(watchdog.poll() is None and time.time() < until, 'captured recovery not ready')
            time.sleep(0.05)
        ready = json.loads(recovery.read(ready_path))
        require(ready == {'record_sha256': subject.RECORD_SHA, 'manifest_sha256': subject.CURRENT.sha256,
            'recovery_sha256': record['recovery_sha256'], 'process': process}, 'wrong recovery-ready authority')
    except BaseException:
        if watchdog.poll() is None:
            watchdog.terminate()
            try:
                watchdog.wait(timeout=5)
            except subprocess.TimeoutExpired:
                watchdog.kill()
                watchdog.wait(timeout=5)
        raise
    return watchdog, recovery_process_sha


def execute(args):
    # The captured bootstrap has already verified R/H/manifest and refused all
    # collisions before creating this root or contacting a registry.
    record = recovery.validate_record(args.work_dir, subject.RECORD_SHA, args.runner_commit,
        subject.CURRENT.sha256, subject.RECORD['recovery_sha256'], args.run_token)
    owner = native.Run(PREFIX, token=args.run_token)
    owner.base = PREFIX + '-' + args.run_token
    controller = resources.Docker(args, subject.CURRENT, record)
    native.call = lambda line, **kwargs: command(args, line, **kwargs)
    watchdog, recovery_process_sha = start_recovery(args, record)
    result = {'outcome': 'fail', 'checks_outcome': 'fail', 'record_sha256': subject.RECORD_SHA,
              'runner_commit': args.runner_commit, 'package_commit': H, 'manifest_sha256': subject.CURRENT.sha256,
              'recovery_process_sha256': recovery_process_sha}
    try:
        registry = registry_identity()
        images = [RUST, SERVER, MYSQL, REDIS]
        for image in images:
            command(args, ['docker', 'pull', '--platform', 'linux/amd64', image], timeout=600)
        identities = {image: {'kind': 'executed-child-manifest', 'reference': image,
            'content_sha256': controller.image(image)['sha256'], 'os': 'linux', 'architecture': 'amd64'} for image in images}
        labels = controller.image(SERVER)['raw']['Config'].get('Labels') or {}
        require(labels.get('org.opencontainers.image.revision') == SERVER_SOURCE, 'wrong PHP Server source')
        fixture = pathlib.Path(args.work_dir) / 'fixture'
        export_consumer(fixture)
        sources = {p.relative_to(fixture).as_posix(): native.sha256(p) for p in fixture.rglob('*') if p.is_file()}
        fetch = build(args, fixture, controller, 'cargo generate-lockfile && cargo fetch --locked', 'fetch')
        subject.exclusive(fixture / 'fetch.stdout', fetch.stdout.encode())
        require(all(native.sha256(fixture / path) == sha for path, sha in sources.items()), 'fetch changed consumer source')
        package = verify_crate(fixture)
        (fixture / 'target').mkdir(exist_ok=True)
        compiled = build(args, fixture, controller,
            'rustc -V && cargo -V && cargo test --locked --offline --test conditions --no-run --message-format=json', 'build')
        subject.exclusive(fixture / 'build.stdout', compiled.stdout.encode())
        subject.exclusive(fixture / 'build.stderr', compiled.stderr.encode())
        toolchain = compiled.stdout.splitlines()[:2]
        require(len(toolchain) == 2 and toolchain[0].startswith('rustc 1.86.0 ')
                and toolchain[1].startswith('cargo 1.86.0 '), 'wrong actual Rust/Cargo toolchain')
        artifacts = [json.loads(line) for line in compiled.stdout.splitlines() if line.startswith('{')]
        executables = [p['executable'] for p in artifacts if p.get('reason') == 'compiler-artifact'
                       and p.get('target', {}).get('name') == 'conditions' and p.get('executable')]
        require(len(executables) == 1 and executables[0].startswith('/work/target/debug/deps/conditions-'), 'wrong consumer executable')
        binary = native.confined_child(fixture, executables[0].removeprefix('/work/'))
        output = fixture / 'target/debug/condition-history'
        # Exclusive binary handoff, never an overwrite of a pre-existing name.
        subject.exclusive(output, subject.regular(binary, limit=256 * 1024 * 1024), 0o500)
        binary_sha = native.sha256(output)
        require(not recovery.objects(record, 'container'), 'build/runtime overlap')
        cell = ConditionCell(types.SimpleNamespace(rust_image=RUST), CELL, pathlib.Path(args.work_dir),
                             {'executed_manifest': SERVER}, fixture, owner)
        cell.controller = controller
        cell.deadline = min(int(time.time()) + 600, args.active_unix - 10)
        outcome = cell.execute(args)
        require(native.sha256(output) == binary_sha and verify_crate(fixture) == package, 'consumer/package changed')
        require(all(native.sha256(fixture / path) == sha for path, sha in sources.items()), 'consumer source changed')
        result.update(outcome='pending_independent_audit', checks_outcome='pass', package=package,
            binary_sha256=binary_sha, input_sha256=INPUTS, consumer_source_sha256=sources,
            toolchain=toolchain, registry=registry, images=identities, cell=outcome)
    except BaseException as error:
        result['error_type'] = type(error).__name__
    finally:
        cleanup = recovery.cleanup(record)
        result['cleanup'] = cleanup
        result['cleanup_errors'] = owner.cleanup_errors
        if not cleanup['pass'] or owner.cleanup_errors:
            result.update(outcome='fail', checks_outcome='fail')
        subject.CURRENT.verify()
        data = subject.canonical(result)
        clean, leaked = native.redact_credentials(data, owner.secret_values)
        if leaked:
            result = {'outcome': 'fail', 'checks_outcome': 'fail', 'error_type': 'CredentialOutput',
                      'record_sha256': subject.RECORD_SHA}
            clean = subject.canonical(result)
            recovery.purge(args.work_dir)
        summary_sha = subject.exclusive(pathlib.Path(args.work_dir) / 'summary.json', clean)
        if cleanup['pass']:
            watchdog.terminate()
            watchdog.wait(timeout=5)
        print(json.dumps({'summary_sha256': summary_sha, 'record_sha256': subject.RECORD_SHA,
            'manifest_sha256': subject.CURRENT.sha256, 'checks_outcome': result['checks_outcome'],
            'acceptance': 'requires independent captured-source audit after owner exit'}), flush=True)
    require(result['checks_outcome'] == 'pass', 'checks failed; no acceptance')
