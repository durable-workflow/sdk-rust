"""Container bounds and ownership-checked cleanup for the acceptance harness.

Every Docker call is stubbed; nothing here starts a container.
"""
import argparse
import contextlib
import gzip
import hashlib
import io
import json
import os
import pathlib
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import run  # noqa: E402

TOKEN = '0123456789abcdef'
FOREIGN = 'fedcba9876543210'
RUST = 'rust:1.86-bookworm'
BUILD_STDOUT = 'rustc 1.86.0\ncargo 1.86.0\n'


def done(command, returncode=0, stdout='', stderr=''):
    return subprocess.CompletedProcess(command, returncode, stdout, stderr)


class FakeDocker:
    """A stateful stand-in for the Docker CLI.

    It records every command and keeps container and network tables of
    name -> (id, run label). A create adds the object unless the name already
    exists (a conflict). `faults` maps a name to how its create misbehaves:
    'fail' (nothing created, nonzero exit), 'timeout' (nothing created, client
    timeout), 'created-then-timeout' (the daemon created it, then the client
    timed out) or 'created-then-error' (created, then a nonzero exit).
    `raise_on` maps (command prefix, last argument or None) to an exception that
    the call raises, e.g. an inspect or remove that times out.
    """

    def __init__(self, containers=None, networks=None, faults=None, created=None, raise_on=None):
        self.commands = []
        self.containers = dict(containers or {})
        self.networks = dict(networks or {})
        self.faults = dict(faults or {})
        # Side effect of a foreground run, e.g. writing the binary; a returned string is its stdout.
        self.created = created
        self.raise_on = dict(raise_on or {})

    def __call__(self, command, timeout=120, check=True, **kwargs):
        command = list(command)
        self.commands.append((command, timeout))
        for (prefix, target), error in self.raise_on.items():
            if tuple(command[:len(prefix)]) == prefix and target in (None, command[-1]):
                raise error
        if command[:2] == ['docker', 'wait']:
            return done(command, stdout='0\n')
        if command[:2] == ['docker', 'pull']:
            return done(command)
        if command[:3] == ['docker', 'image', 'inspect']:
            return done(command, stdout=json.dumps([{'Id': 'sha256:img', 'RootFS': {'Layers': []}, 'Config': {}}]))
        if command[:3] in (['docker', 'container', 'inspect'], ['docker', 'network', 'inspect']):
            table = self.containers if command[1] == 'container' else self.networks
            if command[-1] not in table:
                return self._result(done(command, 1, stderr='No such object'), check)
            object_id, label = table[command[-1]]
            return done(command, stdout='%s %s\n' % (object_id, label if label is not None else '<no value>'))
        if command[:2] == ['docker', 'run']:
            name = command[command.index('--name') + 1]
            result = self._create(self.containers, name, command, timeout, check)
            if '--rm' in command and '-d' not in command:
                self.containers.pop(name)  # a foreground --rm container exits and is removed
            stdout = self.created(command) if self.created else None
            if stdout is None:
                stdout = BUILD_STDOUT if '-d' not in command else 'cid\n'
            return done(command, stdout=stdout) if result is None else result
        if command[:3] == ['docker', 'network', 'create']:
            result = self._create(self.networks, command[-1], command, timeout, check)
            return done(command) if result is None else result
        if command[:3] == ['docker', 'rm', '-f']:
            self._drop(self.containers, command[3])
        if command[:3] == ['docker', 'network', 'rm']:
            self._drop(self.networks, command[3])
        return done(command)

    def _create(self, table, name, command, timeout, check):
        if name in table:
            return self._result(done(command, 125, stderr='Conflict. The name is already in use'), check)
        fault = self.faults.get(name)
        if fault == 'fail':
            return self._result(done(command, 125, stderr='create failed'), check)
        if fault == 'timeout':
            raise subprocess.TimeoutExpired(command, timeout)
        labels = [command[i + 1] for i, a in enumerate(command) if a == '--label']
        token = next((v.split('=', 1)[1] for v in labels if v.startswith(run.RUN_LABEL + '=')), None)
        table[name] = ('id-' + name, token)
        if fault == 'created-then-timeout':
            raise subprocess.TimeoutExpired(command, timeout)
        if fault == 'created-then-error':
            return self._result(done(command, 101, stderr='error: could not compile'), check)
        return None

    @staticmethod
    def _drop(table, object_id):
        for name, (existing, _) in list(table.items()):
            if existing == object_id:
                del table[name]

    @staticmethod
    def _result(result, check):
        if check and result.returncode:
            raise RuntimeError('%s failed (%s)' % (' '.join(result.args[:4]), result.returncode))
        return result

    def removals(self):
        return [c for c, _ in self.commands if c[:3] == ['docker', 'rm', '-f'] or c[:3] == ['docker', 'network', 'rm']]

    def runs(self):
        return [(c, t) for c, t in self.commands if c[:2] == ['docker', 'run']]


INSPECT = ('docker', 'container', 'inspect')
LOGS = ('docker', 'logs')
REMOVE = ('docker', 'rm', '-f')
NETWORK_INSPECT = ('docker', 'network', 'inspect')
NETWORK_REMOVE = ('docker', 'network', 'rm')
TIMEOUT = subprocess.TimeoutExpired(['docker'], 120)


def args(**overrides):
    values = dict(crate_version='4.11.1', rust_image=RUST, python_image='python:3.12-bookworm', jobs=2,
                  runtime='native', expect_server_version=None, container_prefix='general')
    values.update(overrides)
    return argparse.Namespace(**values)


def temporary_directory(test):
    path = pathlib.Path(tempfile.mkdtemp())
    test.addCleanup(shutil.rmtree, path, ignore_errors=True)
    return path


def write_binary(fixture):
    def created(command):
        target = fixture / 'target' / 'debug'
        target.mkdir(parents=True, exist_ok=True)
        (target / run.BINARY).write_bytes(b'binary')
    return created


class HarnessRun(unittest.TestCase):
    def setUp(self):
        self.tmp = temporary_directory(self)
        self.fixture = self.tmp / 'fixture'
        self.run = run.Run('general', stamp='20261103t120000', token=TOKEN)

    def build(self, docker):
        with mock.patch.object(run, 'call', docker):
            return run.build_fixture(args(), self.fixture, self.run)


class BuildContainerBounds(HarnessRun):
    def test_exact_bounds_flags_mount_and_wall_bound(self):
        name = 'general-accept-20261103t120000-%s-build' % TOKEN
        docker = FakeDocker(created=write_binary(self.fixture))
        result = self.build(docker)
        [(command, timeout)] = docker.runs()
        self.assertEqual(command, [
            'docker', 'run', '--rm', '--init', '--name', name,
            '--label', 'task=' + name,
            '--label', 'dev.durable-workflow.acceptance.run=' + TOKEN,
            '--label', 'dev.durable-workflow.acceptance.owner=general',
            '--cpus', '2', '--memory', '3g', '--pids-limit', '1024',
            '--security-opt', 'no-new-privileges', '--cap-drop', 'ALL',
            '--user', '%d:%d' % (os.getuid(), os.getgid()),
            '-v', '%s:/work' % self.fixture, '-w', '/work', '-e', 'CARGO_HOME=/work/.cargo',
            '-e', 'CARGO_RESOLVER_INCOMPATIBLE_RUST_VERSIONS=fallback', RUST,
            'timeout', '--kill-after=60s', '3300s',
            'sh', '-c', 'rustc -V && cargo -V && cargo generate-lockfile && cargo build --locked -j 2'])
        self.assertEqual(timeout, 3600)
        # Default network only (no --network, no --privileged, no extra mounts).
        self.assertNotIn('--network', command)
        self.assertNotIn('--privileged', command)
        self.assertEqual([command[i + 1] for i, a in enumerate(command) if a in ('-v', '--mount')],
                         ['%s:/work' % self.fixture])
        self.assertEqual(result['toolchain'], ['rustc 1.86.0', 'cargo 1.86.0'])
        self.assertEqual((self.fixture / 'build.stdout').read_text(), BUILD_STDOUT)

    def test_cpus_follow_jobs_with_a_floor_of_one(self):
        for jobs, cpus in ((1, '1'), (0, '1'), (4, '4')):
            with self.subTest(jobs=jobs):
                fixture = self.tmp / ('fixture-%d' % jobs)
                docker = FakeDocker(created=write_binary(fixture))
                with mock.patch.object(run, 'call', docker):
                    run.build_fixture(args(jobs=jobs), fixture, self.run)
                [(command, _)] = docker.runs()
                self.assertEqual(command[command.index('--cpus') + 1], cpus)

    def test_in_container_wall_bound_fires_before_the_client_timeout(self):
        docker = FakeDocker(created=write_binary(self.fixture))
        self.build(docker)
        [(command, client_timeout)] = docker.runs()
        image = command.index(RUST)
        # The container's main process is timeout itself, wrapping the whole build,
        # so the container exits (and --rm fires) even with no client attached.
        self.assertEqual(command[image + 1:image + 4], ['timeout', '--kill-after=60s', '3300s'])
        self.assertEqual(command[image + 4:image + 6], ['sh', '-c'])
        self.assertIn('--rm', command[:command.index(RUST)])
        self.assertIn('--init', command[:command.index(RUST)])
        self.assertNotIn('--foreground', command)
        self.assertLess(run.BUILD_WALL_SECONDS + run.BUILD_KILL_AFTER_SECONDS, client_timeout)


class Naming(unittest.TestCase):
    def test_tokens_are_high_entropy_and_unique_within_one_second(self):
        runs = [run.Run('general', stamp='20261103t120000') for _ in range(2000)]
        self.assertEqual(len({r.base for r in runs}), 2000)
        for r in runs[:50]:
            self.assertRegex(r.token, r'^[0-9a-f]{16}$')  # 64 random bits
            self.assertTrue(r.base.startswith('general-accept-20261103t120000-'))
            self.assertTrue(r.base.endswith(r.token))

    def test_token_comes_from_the_csprng(self):
        with mock.patch.object(run.secrets, 'token_hex', return_value=TOKEN) as token_hex:
            self.assertEqual(run.Run('security').token, TOKEN)
        token_hex.assert_called_once_with(8)

    def test_prefix_validation(self):
        for good in ('general', 'security', 'sdkrust', 'a', 'cloud-2', 'a' * 31):
            with self.subTest(good=good):
                self.assertEqual(run.container_prefix(good), good)
        for bad in ('', 'General', '1lab', '-x', 'a_b', 'a/b', 'a.b', 'a b', 'a' * 32, 'gen\n', '--rm'):
            with self.subTest(bad=bad), self.assertRaises(argparse.ArgumentTypeError):
                run.container_prefix(bad)
            with self.subTest(bad=bad), self.assertRaises(argparse.ArgumentTypeError):
                run.Run(bad)

    def test_cli_prefix_default_and_rejection(self):
        base = ['run.py', '--crate-version', '4.11.1', '--server-index', 'sha256:x', '--work-dir', '/tmp/x']
        with mock.patch.object(sys, 'argv', base):
            self.assertEqual(run.parse_args().container_prefix, 'sdkrust')
        with mock.patch.object(sys, 'argv', base + ['--container-prefix', 'security']):
            self.assertEqual(run.parse_args().container_prefix, 'security')
        with mock.patch.object(sys, 'argv', base + ['--container-prefix', 'Bad_Prefix']), \
                contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            run.parse_args()

    def test_prefix_and_labels_apply_to_cell_containers_and_network(self):
        tmp = temporary_directory(self)
        harness = run.Run('security', stamp='20261103t120000', token=TOKEN)
        cell = run.Cell(args(), 'lifecycle', tmp, {'executed_manifest': 'img'}, tmp, harness)
        task = 'security-accept-20261103t120000-%s-lifecycle' % TOKEN
        self.assertEqual(cell.task, task)
        docker = FakeDocker()
        with mock.patch.object(run, 'call', docker):
            self.assertEqual(cell.start('worker', RUST, ['x']), task + '-worker')
            self.assertEqual(cell.plain('redis', 'redis', []), task + '-redis')
        labels = ['--label', 'task=' + task, '--label', 'dev.durable-workflow.acceptance.run=' + TOKEN,
                  '--label', 'dev.durable-workflow.acceptance.owner=security']
        for command, _ in docker.runs():
            self.assertEqual(command[command.index('--name') + 1].rsplit('-', 1)[0], task)
            self.assertEqual(command[command.index('--name') + 2:command.index('--name') + 8], labels)


class BuildCleanup(HarnessRun):
    def name(self):
        return self.run.base + '-build'

    def test_name_collision_never_removes_the_existing_container(self):
        # Another invocation already owns this name; our docker run fails with a conflict.
        docker = FakeDocker(containers={self.name(): ('foreign-id', FOREIGN)})
        with self.assertRaises(RuntimeError):
            self.build(docker)
        self.assertEqual(docker.removals(), [])
        self.assertEqual(docker.containers, {self.name(): ('foreign-id', FOREIGN)})

    def test_foreign_or_missing_label_is_refused(self):
        for label in (FOREIGN, None, TOKEN.upper(), TOKEN + 'x', ''):
            with self.subTest(label=label):
                docker = FakeDocker(containers={'c': ('foreign-id', label)})
                with mock.patch.object(run, 'call', docker):
                    self.assertIsNone(run.remove_owned_container('c', self.run))
                self.assertEqual(docker.removals(), [])

    def test_failed_create_removes_nothing(self):
        for fault, error in (('fail', RuntimeError), ('timeout', subprocess.TimeoutExpired)):
            with self.subTest(fault=fault):
                self.fixture = self.tmp / ('fixture-' + fault)
                docker = FakeDocker(faults={self.name(): fault})
                with self.assertRaises(error):
                    self.build(docker)
                self.assertEqual(docker.removals(), [])

    def test_created_then_client_timeout_removes_only_the_owned_container_by_id(self):
        docker = FakeDocker(faults={self.name(): 'created-then-timeout'})
        with self.assertRaises(subprocess.TimeoutExpired):
            self.build(docker)
        self.assertEqual(docker.removals(), [['docker', 'rm', '-f', 'id-' + self.name()]])
        self.assertEqual(docker.containers, {})

    def test_cleanup_failure_is_recorded_and_never_masks_the_build_error(self):
        docker = FakeDocker(faults={self.name(): 'created-then-error'}, raise_on={(INSPECT, None): TIMEOUT})
        with self.assertRaises(RuntimeError):
            self.build(docker)
        self.assertEqual(self.run.cleanup_errors,
                         [{'step': 'build-cleanup', 'object': self.name(), 'error': 'TimeoutExpired'}])

    def test_build_error_removes_only_the_owned_container_by_id(self):
        docker = FakeDocker(faults={self.name(): 'created-then-error'})
        with self.assertRaises(RuntimeError):
            self.build(docker)
        self.assertEqual(docker.removals(), [['docker', 'rm', '-f', 'id-' + self.name()]])

    def test_successful_build_already_removed_by_rm_needs_no_forced_cleanup(self):
        docker = FakeDocker(created=write_binary(self.fixture))
        self.build(docker)
        inspects = [c for c, _ in docker.commands if c[:3] == ['docker', 'container', 'inspect']]
        self.assertEqual([c[-1] for c in inspects], [self.name()])
        self.assertEqual(docker.removals(), [])


class CellCleanup(HarnessRun):
    """Cell creates are tracked before they run, inside the cleanup-protected region."""

    def cell(self, runtime='native'):
        fixture = self.tmp / 'built'
        write_binary(fixture)(None)
        return run.Cell(args(runtime=runtime), 'lifecycle', self.tmp / 'work', {'executed_manifest': 'img'},
                        fixture, self.run)

    def run_cell(self, cell, docker, error):
        with mock.patch.object(run, 'call', docker), self.assertRaises(error):
            cell.run()

    def assert_state_cleaned(self, cell):
        self.assertFalse((cell.state / 'credentials.json').exists())
        self.assertFalse((cell.state / 'database').exists())
        self.assertFalse((cell.state / 'storage').exists())

    def test_start_created_then_client_timeout_is_removed(self):
        cell = self.cell()
        runtime = cell.task + '-runtime'
        docker = FakeDocker(faults={runtime: 'created-then-timeout'})
        self.run_cell(cell, docker, subprocess.TimeoutExpired)
        self.assertEqual(docker.removals(), [['docker', 'rm', '-f', 'id-' + runtime],
                                             ['docker', 'network', 'rm', 'id-' + cell.task]])
        self.assertEqual((docker.containers, docker.networks), ({}, {}))
        self.assert_state_cleaned(cell)

    def test_plain_created_then_client_timeout_is_removed(self):
        cell = self.cell(runtime='php')
        mysql = cell.task + '-mysql'
        docker = FakeDocker(faults={mysql: 'created-then-timeout'})
        self.run_cell(cell, docker, subprocess.TimeoutExpired)
        self.assertEqual(docker.removals(), [['docker', 'rm', '-f', 'id-' + mysql],
                                             ['docker', 'network', 'rm', 'id-' + cell.task]])
        self.assertEqual((docker.containers, docker.networks), ({}, {}))
        self.assert_state_cleaned(cell)

    def test_network_created_then_client_timeout_is_removed(self):
        cell = self.cell()
        docker = FakeDocker(faults={cell.task: 'created-then-timeout'})
        self.run_cell(cell, docker, subprocess.TimeoutExpired)
        self.assertEqual(docker.removals(), [['docker', 'network', 'rm', 'id-' + cell.task]])
        self.assertEqual(docker.networks, {})
        self.assertEqual(docker.runs(), [])
        self.assert_state_cleaned(cell)

    def test_failed_network_create_removes_nothing_and_cleans_state(self):
        for fault, error in (('fail', RuntimeError), ('timeout', subprocess.TimeoutExpired)):
            with self.subTest(fault=fault):
                cell = run.Cell(args(), fault, self.tmp / 'work', {}, self.cell().fixture, self.run)
                docker = FakeDocker(faults={cell.task: fault})
                self.run_cell(cell, docker, error)
                self.assertEqual(docker.removals(), [])
                self.assert_state_cleaned(cell)

    def test_foreign_network_collision_is_not_removed(self):
        cell = self.cell()
        docker = FakeDocker(networks={cell.task: ('foreign-net', FOREIGN)})
        self.run_cell(cell, docker, RuntimeError)
        self.assertEqual(docker.removals(), [])
        self.assertEqual(docker.networks, {cell.task: ('foreign-net', FOREIGN)})
        self.assert_state_cleaned(cell)

    def test_foreign_container_collision_is_neither_logged_nor_removed(self):
        cell = self.cell()
        runtime, proxy = cell.task + '-runtime', cell.task + '-proxy'
        # runtime starts (ours); proxy collides with another owner's container of the same name.
        docker = FakeDocker(containers={proxy: ('foreign-id', FOREIGN)})
        self.run_cell(cell, docker, RuntimeError)
        self.assertEqual(docker.removals(), [['docker', 'rm', '-f', 'id-' + runtime],
                                             ['docker', 'network', 'rm', 'id-' + cell.task]])
        self.assertEqual(docker.containers, {proxy: ('foreign-id', FOREIGN)})
        self.assertEqual([c[-1] for c, _ in docker.commands if c[:2] == ['docker', 'logs']], ['id-' + runtime])
        self.assert_state_cleaned(cell)


class PlainContainerBounds(HarnessRun):
    """The PHP-baseline process set gets the same envelope as Cell.start."""

    def test_every_php_baseline_role_is_bounded(self):
        cell = run.Cell(args(runtime='php'), 'lifecycle', self.tmp / 'work', {'executed_manifest': 'server-img'},
                        self.tmp, self.run)
        docker = FakeDocker()
        with mock.patch.object(run, 'call', docker):
            cell.start_php('fixture-token')
        labels = ['--label', 'task=' + cell.task, '--label', 'dev.durable-workflow.acceptance.run=' + TOKEN,
                  '--label', 'dev.durable-workflow.acceptance.owner=general']

        def bounds(role, pids, caps, *extra):
            line = ['docker', 'run', '-d', '--init', '--name', '%s-%s' % (cell.task, role)] + labels + [
                    '--network', cell.task, '--memory', '1g', '--cpus', '1', '--pids-limit', pids,
                    '--security-opt', 'no-new-privileges', '--cap-drop', 'ALL']
            for cap in caps:
                line += ['--cap-add', cap]
            return line + list(extra)

        expected = {
            'mysql': bounds('mysql', '512', ('CHOWN', 'DAC_OVERRIDE', 'SETGID', 'SETUID'),
                            '--network-alias', 'mysql', '--tmpfs', '/var/lib/mysql'),
            'redis': bounds('redis', '256', ('CHOWN', 'SETGID', 'SETUID'), '--network-alias', 'redis', '--tmpfs', '/data'),
            'bootstrap': bounds('bootstrap', '256', ('DAC_OVERRIDE',)),
            'runtime': bounds('runtime', '256', ('CHOWN', 'DAC_OVERRIDE', 'KILL', 'SETGID', 'SETUID'),
                              '--network-alias', 'qualification-origin'),
            'queue': bounds('queue', '256', (), '--user', '1000:1000'),
            'repair': bounds('repair', '256', (), '--user', '1000:1000'),
        }
        runs = {c[c.index('--name') + 1].rsplit('-', 1)[1]: c for c, _ in docker.runs()}
        self.assertEqual(list(runs), list(expected))
        for role, command in runs.items():
            with self.subTest(role=role):
                end = command.index('-e') if '-e' in command else len(command) - 1
                self.assertEqual(command[:end], expected[role])
                self.assertNotIn('--privileged', command)
                self.assertFalse({'SYS_ADMIN', 'NET_ADMIN', 'NET_RAW', 'ALL'} & {
                    command[i + 1] for i, a in enumerate(command) if a == '--cap-add'})
        self.assertEqual(runs['mysql'][-1], run.MYSQL_IMAGE)
        self.assertEqual(runs['redis'][-1], run.REDIS_IMAGE)
        self.assertEqual(runs['bootstrap'][-2:], ['server-img', 'server-bootstrap'])
        self.assertEqual(runs['runtime'][-1], 'server-img')


class CleanupStepsAreIndependent(HarnessRun):
    """A failing Docker cleanup step never skips later steps or local credential/state cleanup."""

    def setUp(self):
        super().setUp()
        fixture = self.tmp / 'built'
        write_binary(fixture)(None)
        self.cell = run.Cell(args(), 'lifecycle', self.tmp / 'work', {'executed_manifest': 'img'}, fixture, self.run)
        self.runtime, self.proxy = self.cell.task + '-runtime', self.cell.task + '-proxy'

    def run_with(self, raise_on):
        # runtime starts; proxy is created, then its docker run fails client-side.
        docker = FakeDocker(faults={self.proxy: 'created-then-error'}, raise_on=raise_on)
        with mock.patch.object(run, 'call', docker), self.assertRaises(RuntimeError):
            self.cell.run()  # the original error propagates, not a cleanup error
        self.assertFalse((self.cell.state / 'credentials.json').exists())
        self.assertFalse((self.cell.state / 'database').exists())
        self.assertFalse((self.cell.state / 'storage').exists())
        for error in self.run.cleanup_errors:
            self.assertEqual(set(error), {'step', 'object', 'error'})
        return docker

    def errors(self):
        return [(e['step'], e['object'], e['error']) for e in self.run.cleanup_errors]

    def test_inspect_timeout(self):
        docker = self.run_with({(INSPECT, self.runtime): TIMEOUT})
        self.assertEqual(docker.removals(), [['docker', 'rm', '-f', 'id-' + self.proxy],
                                             ['docker', 'network', 'rm', 'id-' + self.cell.task]])
        self.assertEqual(self.errors(), [('container-inspect', self.runtime, 'TimeoutExpired')])

    def test_logs_failure(self):
        docker = self.run_with({(LOGS, 'id-' + self.runtime): TIMEOUT})
        self.assertEqual(docker.removals(), [['docker', 'rm', '-f', 'id-' + self.proxy],
                                             ['docker', 'rm', '-f', 'id-' + self.runtime],
                                             ['docker', 'network', 'rm', 'id-' + self.cell.task]])
        self.assertEqual(self.errors(), [('container-logs', self.runtime, 'TimeoutExpired')])

    def test_remove_failure(self):
        docker = self.run_with({(REMOVE, 'id-' + self.proxy): RuntimeError('daemon')})
        self.assertEqual(docker.removals(), [['docker', 'rm', '-f', 'id-' + self.proxy],
                                             ['docker', 'rm', '-f', 'id-' + self.runtime],
                                             ['docker', 'network', 'rm', 'id-' + self.cell.task]])
        self.assertEqual(docker.containers, {self.proxy: ('id-' + self.proxy, TOKEN)})
        self.assertEqual(self.errors(), [('container-remove', self.proxy, 'RuntimeError')])

    def test_network_remove_failure(self):
        self.run_with({(NETWORK_REMOVE, None): TIMEOUT})
        self.assertEqual(self.errors(), [('network-cleanup', self.cell.task, 'TimeoutExpired')])

    def test_network_inspect_timeout(self):
        self.run_with({(NETWORK_INSPECT, None): TIMEOUT})
        self.assertEqual(self.errors(), [('network-cleanup', self.cell.task, 'TimeoutExpired')])

    def test_every_docker_cleanup_call_fails(self):
        docker = self.run_with({(INSPECT, None): TIMEOUT, (NETWORK_INSPECT, None): TIMEOUT})
        self.assertEqual(docker.removals(), [])
        self.assertEqual(self.errors(), [('container-inspect', self.runtime, 'TimeoutExpired'),
                                         ('container-inspect', self.proxy, 'TimeoutExpired'),
                                         ('network-cleanup', self.cell.task, 'TimeoutExpired')])

    def test_clean_run_records_no_errors(self):
        self.run_with({})
        self.assertEqual(self.run.cleanup_errors, [])


class LocalStateCleanup(HarnessRun):
    """Credential, database/ and storage/ removal is independent, recorded and verified."""

    def setUp(self):
        super().setUp()
        fixture = self.tmp / 'built'
        write_binary(fixture)(None)
        self.cell = run.Cell(args(), 'lifecycle', self.tmp / 'work', {'executed_manifest': 'img'}, fixture, self.run)
        self.real_rmtree = shutil.rmtree

    def run_cell(self, unlink=None, rmtree=None):
        # The proxy fails client-side after creation, so the cell has its own error.
        docker = FakeDocker(faults={self.cell.task + '-proxy': 'created-then-error'})
        patches = [mock.patch.object(run, 'call', docker)]
        if unlink:
            patches.append(mock.patch.object(pathlib.Path, 'unlink', unlink))
        if rmtree:
            patches.append(mock.patch.object(run.shutil, 'rmtree', rmtree))
        with contextlib.ExitStack() as stack:
            for patch in patches:
                stack.enter_context(patch)
            with self.assertRaises(RuntimeError) as raised:
                self.cell.run()
        self.assertIn('docker run -d', str(raised.exception))  # the cell's own error, not a cleanup error
        return {sub: os.path.lexists(self.cell.state / sub) for sub in ('credentials.json', 'database', 'storage')}

    def failing_rmtree(self, failing):
        def rmtree(path, *a, **k):
            if pathlib.Path(path).name == failing:
                raise PermissionError('denied')
            return self.real_rmtree(path, *a, **k)
        return rmtree

    def errors(self):
        return [(e['step'], e['object'], e['error']) for e in self.run.cleanup_errors]

    def test_clean_local_cleanup_records_nothing(self):
        present = self.run_cell()
        self.assertEqual(present, {'credentials.json': False, 'database': False, 'storage': False})
        self.assertEqual((self.run.cleanup_errors, self.run.local_leftovers), ([], []))

    def test_credential_unlink_failure_does_not_skip_state_or_mask_the_error(self):
        def unlink(path, missing_ok=False):
            raise PermissionError('denied')
        present = self.run_cell(unlink=unlink)
        self.assertEqual(present, {'credentials.json': True, 'database': False, 'storage': False})
        self.assertEqual(self.run.local_leftovers, ['lifecycle/credentials.json'])
        self.assertEqual(self.errors(), [('local-remove', 'lifecycle/credentials.json', 'PermissionError'),
                                         ('local-verify', 'lifecycle/credentials.json', 'StillPresent')])

    def test_each_state_directory_failure_is_independent(self):
        for failing in ('database', 'storage'):
            with self.subTest(failing=failing):
                self.setUp()
                present = self.run_cell(rmtree=self.failing_rmtree(failing))
                self.assertEqual(present, {'credentials.json': False, 'database': failing == 'database',
                                           'storage': failing == 'storage'})
                self.assertEqual(self.run.local_leftovers, ['lifecycle/' + failing])
                self.assertEqual(self.errors(), [('local-remove', 'lifecycle/' + failing, 'PermissionError'),
                                                 ('local-verify', 'lifecycle/' + failing, 'StillPresent')])

    def test_silent_partial_removal_is_caught_by_the_absence_check(self):
        present = self.run_cell(rmtree=lambda path, *a, **k: None)  # "succeeds" but removes nothing
        self.assertEqual(present, {'credentials.json': False, 'database': True, 'storage': True})
        self.assertEqual(self.run.local_leftovers, ['lifecycle/database', 'lifecycle/storage'])
        self.assertEqual(self.errors(), [('local-verify', 'lifecycle/database', 'StillPresent'),
                                         ('local-verify', 'lifecycle/storage', 'StillPresent')])

    def test_credential_file_is_owner_only_before_any_container_starts(self):
        modes = []

        class Recording(FakeDocker):
            def __call__(inner, command, timeout=120, check=True, **kwargs):
                if list(command[:3]) == ['docker', 'network', 'create']:
                    modes.append(os.stat(self.cell.state / 'credentials.json').st_mode & 0o777)
                return FakeDocker.__call__(inner, command, timeout, check, **kwargs)
        docker = Recording(faults={self.cell.task + '-proxy': 'created-then-error'})
        previous = os.umask(0)  # even a permissive umask must not widen the mode
        try:
            with mock.patch.object(run, 'call', docker), self.assertRaises(RuntimeError):
                self.cell.run()
        finally:
            os.umask(previous)
        self.assertEqual(modes, [0o600])

    def test_credential_file_is_created_owner_only(self):
        opened = []
        real_open = os.open

        def recording_open(path, flags, mode=0o777, *a, **k):
            opened.append((pathlib.Path(path).name, flags & os.O_EXCL, mode))
            return real_open(path, flags, mode, *a, **k)
        with mock.patch.object(run.os, 'open', recording_open):
            self.run_cell()
        self.assertIn(('credentials.json', os.O_EXCL, 0o600), opened)

    def test_partial_setup_failure_is_cleaned_and_preserves_the_error(self):
        real_chmod = pathlib.Path.chmod

        def chmod(path, mode, *a, **k):
            if path.name == 'storage':
                raise PermissionError('chmod denied')  # storage/ exists, database/ already set up
            return real_chmod(path, mode, *a, **k)
        docker = FakeDocker()
        with mock.patch.object(run, 'call', docker), mock.patch.object(pathlib.Path, 'chmod', chmod):
            with self.assertRaises(PermissionError) as raised:
                self.cell.run()
        self.assertEqual(str(raised.exception), 'chmod denied')
        for sub in ('credentials.json', 'database', 'storage'):
            self.assertFalse(os.path.lexists(self.cell.state / sub), sub)
        self.assertEqual((self.run.cleanup_errors, self.run.local_leftovers), ([], []))
        self.assertEqual(docker.runs(), [])

    def test_partial_setup_failure_that_also_fails_cleanup_is_reported(self):
        real_chmod, real_rmtree = pathlib.Path.chmod, shutil.rmtree

        def chmod(path, mode, *a, **k):
            if path.name == 'storage':
                raise PermissionError('chmod denied')
            return real_chmod(path, mode, *a, **k)

        def rmtree(path, *a, **k):
            if pathlib.Path(path).name == 'database':
                raise PermissionError('denied')
            return real_rmtree(path, *a, **k)
        with mock.patch.object(run, 'call', FakeDocker()), mock.patch.object(pathlib.Path, 'chmod', chmod), \
                mock.patch.object(run.shutil, 'rmtree', rmtree), self.assertRaises(PermissionError):
            self.cell.run()
        self.assertFalse(os.path.lexists(self.cell.state / 'storage'))
        self.assertEqual(self.run.local_leftovers, ['lifecycle/database'])
        self.assertEqual(self.errors(), [('local-remove', 'lifecycle/database', 'PermissionError'),
                                         ('local-verify', 'lifecycle/database', 'StillPresent')])

    def test_records_carry_no_paths_or_values(self):
        def unlink(path, missing_ok=False):
            raise PermissionError(str(path))
        self.run_cell(unlink=unlink, rmtree=self.failing_rmtree('storage'))
        text = json.dumps([self.run.cleanup_errors, self.run.local_leftovers])
        self.assertNotIn(str(self.tmp), text)
        self.assertNotIn('fixture-', text)


class MainOutcome(unittest.TestCase):
    """--fixture-from skips the builder, and the final result fails closed on cleanup uncertainty."""

    def main(self, docker=None, cell_run=None):
        tmp = temporary_directory(self)
        fixture = tmp / 'prebuilt'
        write_binary(fixture)(None)
        (fixture / 'build.stdout').write_text(BUILD_STDOUT)
        argv = ['run.py', '--crate-version', '4.11.1', '--server-index', 'sha256:x', '--cells', 'lifecycle',
                '--fixture-from', str(fixture), '--work-dir', str(tmp / 'work'), '--container-prefix', 'general']
        self.docker = docker or FakeDocker()
        self.work = tmp / 'work'
        harness_runs = []

        def cell(args_, name, root, server, fixture_, harness):
            harness_runs.append(harness)
            instance = mock.Mock()
            instance.run.side_effect = lambda: (cell_run or (lambda h: None))(harness) or {'outcome': 'pass'}
            return instance

        out = io.StringIO()
        with mock.patch.object(sys, 'argv', argv), mock.patch.object(run, 'call', self.docker), \
                mock.patch.object(run, 'server_identity', return_value={'executed_manifest': 'img'}), \
                mock.patch.object(run, 'crate_provenance', return_value={'baselines': {'pass': True}}), \
                mock.patch.object(run, 'build_fixture') as self.build, mock.patch.object(run, 'Cell', cell), \
                contextlib.redirect_stdout(out), self.assertRaises(SystemExit) as exited:
            run.main()
        self.assertEqual(len(harness_runs), 1)
        self.fixture = fixture
        return exited.exception.code, json.loads(out.getvalue()), json.loads((tmp / 'work' / 'summary.json').read_text())

    def assert_fails_closed(self, code, receipt, summary):
        self.assertEqual(code, 1)
        self.assertEqual(receipt['outcome'], 'fail')
        self.assertEqual(summary['outcome'], 'fail')
        self.assertEqual(summary['checks_outcome'], 'pass')  # the checks passed; cleanup failed
        self.assertIs(summary['cleanup']['pass'], False)
        self.assertNotIn('error', summary)

    def test_clean_run_passes_and_never_builds(self):
        code, receipt, summary = self.main()
        self.assertEqual((code, receipt['outcome'], summary['outcome'], summary['checks_outcome']), (0, 'pass', 'pass', 'pass'))
        self.build.assert_not_called()
        self.assertEqual(self.docker.runs(), [])
        self.assertNotIn(['docker', 'pull', RUST], [c for c, _ in self.docker.commands])
        self.assertEqual(summary['build']['reused_fixture'], str(self.fixture.resolve()))
        self.assertEqual(summary['cleanup'], {'pass': True, 'errors': [], 'local_leftovers': [],
                                              'leftover_containers': [], 'leftover_networks': []})
        self.assertEqual((receipt['leftover_containers'], receipt['leftover_networks'], receipt['local_leftovers'],
                          receipt['cleanup_errors']), ([], [], [], []))
        run_filter = re.compile(r'label=dev\.durable-workflow\.acceptance\.run=[0-9a-f]{16}')
        [containers] = [c for c, _ in self.docker.commands if c[:3] == ['docker', 'ps', '-aq']]
        [networks] = [c for c, _ in self.docker.commands if c[:4] == ['docker', 'network', 'ls', '-q']]
        self.assertTrue(run_filter.fullmatch(containers[-1]))
        self.assertTrue(run_filter.fullmatch(networks[-1]))

    def test_recorded_cleanup_error_fails(self):
        error = {'step': 'container-remove', 'object': 'x', 'error': 'TimeoutExpired'}
        code, receipt, summary = self.main(cell_run=lambda h: h.cleanup_errors.append(error))
        self.assert_fails_closed(code, receipt, summary)
        self.assertEqual(receipt['cleanup_errors'], [error])

    def test_local_leftover_fails(self):
        def cell_run(h):
            h.local_leftovers.append('lifecycle/credentials.json')
            h.cleanup_errors.append({'step': 'local-verify', 'object': 'lifecycle/credentials.json', 'error': 'StillPresent'})
        code, receipt, summary = self.main(cell_run=cell_run)
        self.assert_fails_closed(code, receipt, summary)
        self.assertEqual(receipt['local_leftovers'], ['lifecycle/credentials.json'])

    def test_failed_container_query_fails(self):
        code, receipt, summary = self.main(FakeDocker(raise_on={(('docker', 'ps'), None): TIMEOUT}))
        self.assert_fails_closed(code, receipt, summary)
        self.assertIsNone(receipt['leftover_containers'])
        self.assertEqual(receipt['cleanup_errors'],
                         [{'step': 'leftover-query', 'object': 'containers', 'error': 'TimeoutExpired'}])

    def test_failed_network_query_fails(self):
        code, receipt, summary = self.main(FakeDocker(raise_on={(('docker', 'network', 'ls'), None): TIMEOUT}))
        self.assert_fails_closed(code, receipt, summary)
        self.assertIsNone(receipt['leftover_networks'])

    def test_leftover_container_fails(self):
        class Leftovers(FakeDocker):
            def __call__(self, command, timeout=120, check=True, **kwargs):
                if list(command[:3]) == ['docker', 'ps', '-aq']:
                    self.commands.append((list(command), timeout))
                    return done(command, stdout='abc123\n')
                return super().__call__(command, timeout, check, **kwargs)
        code, receipt, summary = self.main(Leftovers())
        self.assert_fails_closed(code, receipt, summary)
        self.assertEqual(receipt['leftover_containers'], ['abc123'])

    def test_leftover_network_fails(self):
        class Leftovers(FakeDocker):
            def __call__(self, command, timeout=120, check=True, **kwargs):
                if list(command[:4]) == ['docker', 'network', 'ls', '-q']:
                    self.commands.append((list(command), timeout))
                    return done(command, stdout='net123\n')
                return super().__call__(command, timeout, check, **kwargs)
        code, receipt, summary = self.main(Leftovers())
        self.assert_fails_closed(code, receipt, summary)
        self.assertEqual(receipt['leftover_networks'], ['net123'])

    def test_run_error_is_preserved_separately_from_cleanup(self):
        def cell_run(h):
            h.cleanup_errors.append({'step': 'network-cleanup', 'object': 'n', 'error': 'TimeoutExpired'})
            raise RuntimeError('cell failed')
        with self.assertRaises(RuntimeError):
            self.main(cell_run=cell_run)
        summary = json.loads((self.work / 'summary.json').read_text())
        self.assertEqual((summary['outcome'], summary['checks_outcome']), ('fail', 'fail'))
        self.assertEqual(summary['error'], 'RuntimeError: cell failed')
        self.assertEqual(summary['cleanup']['errors'],
                         [{'step': 'network-cleanup', 'object': 'n', 'error': 'TimeoutExpired'}])


# ---------------------------------------------------------------------------
# Pre-publication mode: exact packaging, byte binding and offline installation.
# ---------------------------------------------------------------------------

VERSION = '4.11.2'
CARGO_V = 'cargo 1.86.0 (adf9b6ad1 2025-02-28)'
SOURCE_MANIFEST = ('[package]\nname = "durable-workflow"\nversion = "%s"\nedition = "2021"\n\n'
                   '[package.metadata.durable-workflow]\nproduct-train = "%s"\n'
                   'supported-server-versions = "2.0.0"\n'
                   'supported-server-release-baselines = { "2" = "2.0.0", "3" = "3.0.2" }\n') % (VERSION, VERSION)
SOURCE_FILES = {'Cargo.toml': SOURCE_MANIFEST.encode(), 'Cargo.lock': b'version = 4\n',
                'CHANGELOG.md': b'# Changelog\n\n## 4.11.2\n\n- Fix.\n', 'src/lib.rs': b'pub fn settle() {}\n',
                'schema/contract.json': b'{"v": 1}\n'}
REAL_CALL = run.call


def git_env():
    return dict(os.environ, GIT_AUTHOR_NAME='t', GIT_AUTHOR_EMAIL='t@example.invalid', GIT_COMMITTER_NAME='t',
                GIT_COMMITTER_EMAIL='t@example.invalid', GIT_CONFIG_NOSYSTEM='1', GIT_CONFIG_GLOBAL=os.devnull)


def make_repo(path, files=None):
    path.mkdir(parents=True)
    for name, content in (files or SOURCE_FILES).items():
        (path / name).parent.mkdir(parents=True, exist_ok=True)
        (path / name).write_bytes(content)
    for command in (['init', '--quiet'], ['add', '-A'], ['-c', 'commit.gpgsign=false', 'commit', '--quiet', '-m', 'release']):
        subprocess.run(['git', '-C', str(path)] + command, check=True, env=git_env(), capture_output=True)
    return subprocess.run(['git', '-C', str(path), 'rev-parse', 'HEAD'], check=True, capture_output=True,
                          text=True).stdout.strip()


def packaged_members(commit, files=None):
    files = dict(files or SOURCE_FILES)
    members = {name: content for name, content in files.items() if name != 'Cargo.toml'}
    members['Cargo.toml.orig'] = files['Cargo.toml']
    members['Cargo.toml'] = b'# normalized by cargo\n' + files['Cargo.toml']
    members['.cargo_vcs_info.json'] = json.dumps({'git': {'sha1': commit}, 'path_in_vcs': ''}).encode()
    return members


def make_crate(members, version=VERSION, raw=()):
    """A gzip tar like Cargo's; `raw` adds (TarInfo, bytes-or-None) members verbatim."""
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode='wb', mtime=0) as compressed, \
            tarfile.open(fileobj=compressed, mode='w') as archive:
        for name, content in members.items():
            info = tarfile.TarInfo('durable-workflow-%s/%s' % (version, name))
            info.size = len(content)
            archive.addfile(info, io.BytesIO(content))
        for info, content in raw:
            archive.addfile(info, io.BytesIO(content) if content is not None else None)
    return buffer.getvalue()


def hybrid(docker):
    """git runs for real (temporary repositories); Docker is the stateful fake."""
    def dispatch(command, timeout=120, check=True, **kwargs):
        if command[0] == 'git':
            return REAL_CALL(command, timeout=timeout, check=check, **kwargs)
        return docker(command, timeout, check, **kwargs)
    return dispatch


def volume(command, destination):
    """Host side of the `-v host:destination[:ro]` mount."""
    for index, value in enumerate(command):
        if command[index - 1] == '-v' and value.split(':')[1] == destination:
            return pathlib.Path(value.split(':')[0])
    raise AssertionError('no %s mount in %r' % (destination, command))


class FakeCargo:
    """What the three Cargo containers write, with knobs for each counterfactual."""

    def __init__(self, commit, members=None, crate_name=None, cargo=CARGO_V, listing=None, lock=None,
                 vendor_foreign=False, tamper_install=None, crate=None):
        self.commit, self.cargo = commit, cargo
        self.members = members if members is not None else packaged_members(commit)
        self.crate = crate
        self.crate_name = crate_name or 'durable-workflow-%s.crate' % VERSION
        self.listing = listing
        self.lock, self.vendor_foreign, self.tamper_install = lock, vendor_foreign, tamper_install

    def crate_bytes(self):
        return self.crate if self.crate is not None else make_crate(self.members)

    def __call__(self, command):
        name = command[command.index('--name') + 1]
        if name.endswith('-package'):
            out = volume(command, '/out')
            (out / 'target' / 'package').mkdir(parents=True)
            (out / 'target' / 'package' / self.crate_name).write_bytes(self.crate_bytes())
            listing = self.listing if self.listing is not None else sorted(self.members)
            (out / 'package.list').write_text(''.join(line + '\n' for line in listing))
            return self.cargo + '\n'
        work = volume(command, '/work')
        if name.endswith('-resolve'):
            (work / 'Cargo.lock').write_text('version = 4\n')
            (work / 'vendor' / 'serde_json-1.0.0').mkdir(parents=True)
            if self.vendor_foreign:
                (work / 'vendor' / ('durable-workflow-%s' % VERSION)).mkdir()
            return self.cargo + '\n'
        # The offline build: lock what the directory source declares.
        entry = work / 'vendor' / ('durable-workflow-%s' % VERSION)
        declared = json.loads((entry / '.cargo-checksum.json').read_text())['package']
        lock = self.lock or {'name': 'durable-workflow', 'version': VERSION, 'source': run.CRATES_IO_SOURCE,
                             'checksum': declared}
        (work / 'Cargo.lock').write_text('version = 4\n\n[[package]]\n' + ''.join(
            '%s = "%s"\n' % item for item in lock.items()))
        if self.tamper_install:
            self.tamper_install(entry)
        write_binary(work)(command)
        return 'rustc 1.86.0\n' + self.cargo + '\n'


def package_args(checkout, commit, **overrides):
    values = dict(package_from=checkout, package_commit=commit, expect_version=VERSION, expect_package_sha256=None,
                  package_cargo_version='1.86.0', rust_image=RUST, jobs=2, baselines='required',
                  expected_baselines={'2': '2.0.0', '3': '3.0.2'})
    values.update(overrides)
    return argparse.Namespace(**values)


class PrePublication(HarnessRun):
    def setUp(self):
        super().setUp()
        self.checkout = self.tmp / 'release'
        self.commit = make_repo(self.checkout)
        self.work = self.tmp / 'work'
        self.work.mkdir()

    def package(self, cargo=None, **overrides):
        self.docker = FakeDocker(created=cargo or FakeCargo(self.commit))
        with mock.patch.object(run, 'call', hybrid(self.docker)):
            return run.package_release(package_args(self.checkout, self.commit, **overrides), self.work, self.run)

    def install(self, cargo=None):
        cargo = cargo or FakeCargo(self.commit)
        identity, members = self.package(cargo)
        fixture = self.work / 'fixture'
        with mock.patch.object(run, 'call', hybrid(self.docker)):
            build = run.build_packaged_fixture(package_args(self.checkout, self.commit), fixture, identity, members, self.run)
            crate = run.packaged_provenance(package_args(self.checkout, self.commit), fixture, identity, members)
        return identity, build, crate

    def refused(self, pattern, cargo=None, **overrides):
        with self.assertRaisesRegex(run.PackageError, pattern):
            self.package(cargo, **overrides)

    def test_exact_clean_commit_is_packaged_bound_and_installed_offline(self):
        identity, build, crate = self.install()
        crate_bytes = make_crate(packaged_members(self.commit))
        digest = hashlib.sha256(crate_bytes).hexdigest()
        self.assertEqual((identity['commit'], identity['version'], identity['crate_sha256']), (self.commit, VERSION, digest))
        self.assertEqual(identity['files'], sorted(packaged_members(self.commit)))
        self.assertEqual(identity['file_list_sha256'], run.file_list_sha256(packaged_members(self.commit)))
        self.assertEqual(identity['vcs_info'], {'git': {'sha1': self.commit}, 'path_in_vcs': ''})
        self.assertEqual(identity['cargo_version'], CARGO_V)
        self.assertEqual((self.work / 'package' / identity['crate_file']).read_bytes(), crate_bytes)
        self.assertEqual((crate['sha256'], crate['lock_checksum'], crate['lock_source']),
                         (digest, digest, run.CRATES_IO_SOURCE))
        self.assertIs(crate['registry_published'], False)
        self.assertTrue(crate['baselines']['pass'])
        self.assertEqual(build['network'], 'none')
        json.dumps(identity)  # evidence is serializable and carries no raw bytes
        self.assertEqual(self.run.cleanup_errors, [])

    def test_package_container_is_bounded_read_only_and_labelled(self):
        self.package()
        [(command, timeout)] = self.docker.runs()
        name = self.run.base + '-package'
        image = command.index(RUST)
        self.assertEqual(command[:image], [
            'docker', 'run', '--rm', '--init', '--name', name,
            '--label', 'task=' + name, '--label', 'dev.durable-workflow.acceptance.run=' + TOKEN,
            '--label', 'dev.durable-workflow.acceptance.owner=general',
            '--cpus', '2', '--memory', '3g', '--pids-limit', '1024',
            '--security-opt', 'no-new-privileges', '--cap-drop', 'ALL', '--user', '%d:%d' % (os.getuid(), os.getgid()),
            '-v', '%s:/source:ro' % (self.work / 'package-source'), '-v', '%s:/out' % (self.work / 'package-out'),
            '-w', '/source', '-e', 'CARGO_HOME=/out/.cargo', '-e', 'CARGO_TARGET_DIR=/out/target'])
        self.assertEqual(command[image + 1:image + 6], ['timeout', '--kill-after=60s', '3300s', 'sh', '-c'])
        self.assertIn('cargo package --locked --no-verify --list', command[-1])
        self.assertEqual(timeout, run.BUILD_CLIENT_TIMEOUT)

    def test_resolve_and_offline_build_containers(self):
        self.install()
        [package, resolve, build] = [c for c, _ in self.docker.runs()]
        self.assertNotIn('--network', resolve)
        self.assertIn('%s:/package:ro' % (self.work / 'package' / ('durable-workflow-' + VERSION)), resolve)
        self.assertIn('cargo vendor --locked --versioned-dirs vendor', resolve[-1])
        self.assertEqual(build[build.index('--network') + 1], 'none')
        self.assertEqual([build[i + 1] for i, a in enumerate(build) if a in ('-v', '--mount')],
                         ['%s:/work' % (self.work / 'fixture')])
        self.assertIn('cargo build --locked --offline -j 2', build[-1])
        fixture = self.work / 'fixture'
        self.assertNotIn('[patch', (fixture / 'Cargo.toml').read_text())
        self.assertIn('replace-with = "acceptance-package"', (fixture / '.cargo' / 'config.toml').read_text())
        for container in (package, resolve, build):
            self.assertIn('--rm', container)
            self.assertIn('--init', container)

    # Counterfactuals: each refuses before any lifecycle cell can run.

    def test_dirty_tree_is_refused_before_packaging(self):
        for change in ('modify', 'untracked'):
            with self.subTest(change=change):
                target = self.checkout / ('src/lib.rs' if change == 'modify' else 'notes.txt')
                original = target.read_bytes() if target.exists() else None
                target.write_bytes(b'drift\n')
                try:
                    self.refused('is not clean')
                    self.assertEqual(self.docker.runs(), [])
                finally:
                    target.write_bytes(original) if original is not None else target.unlink()
                shutil.rmtree(self.work / 'package-source', ignore_errors=True)

    def test_head_commit_mismatch_is_refused(self):
        self.docker = FakeDocker(created=FakeCargo(self.commit))
        wrong = package_args(self.checkout, 'f' * 40)
        with mock.patch.object(run, 'call', hybrid(self.docker)), \
                self.assertRaisesRegex(run.PackageError, 'not requested commit'):
            run.package_release(wrong, self.work, self.run)
        self.assertEqual(self.docker.runs(), [])

    def test_source_version_mismatch_is_refused_before_packaging(self):
        self.refused('declares durable-workflow 4.11.2, expected durable-workflow 4.11.3', expect_version='4.11.3')
        self.assertEqual(self.docker.runs(), [])

    def test_packaged_manifest_version_mismatch_is_refused(self):
        members = packaged_members(self.commit)
        members['Cargo.toml'] = members['Cargo.toml'].replace(b'version = "4.11.2"', b'version = "4.11.1"')
        self.refused('packaged manifest is durable-workflow 4.11.1', FakeCargo(self.commit, members=members))

    def test_crate_file_name_or_count_mismatch_is_refused(self):
        self.refused('expected exactly durable-workflow-4.11.2.crate',
                     FakeCargo(self.commit, crate_name='durable-workflow-4.11.1.crate'))

    def test_vcs_info_mismatch_is_refused(self):
        for record in ({'git': {'sha1': 'e' * 40}, 'path_in_vcs': ''},
                       {'git': {'sha1': None}, 'path_in_vcs': ''},
                       {'git': {'dirty': True}, 'path_in_vcs': ''},
                       {'path_in_vcs': ''},
                       {'git': {'sha1': 'COMMIT', 'dirty': True}, 'path_in_vcs': ''},
                       {'git': {'sha1': 'COMMIT'}, 'path_in_vcs': 'sdk'}):
            with self.subTest(record=record):
                members = packaged_members(self.commit)
                members['.cargo_vcs_info.json'] = json.dumps(record).replace('COMMIT', self.commit).encode()
                self.refused('does not name clean commit', FakeCargo(self.commit, members=members))
                shutil.rmtree(self.work)
                self.work.mkdir()

    def test_one_byte_package_drift_from_the_commit_is_refused(self):
        for name in ('src/lib.rs', 'schema/contract.json', 'Cargo.lock', 'Cargo.toml.orig', 'CHANGELOG.md'):
            with self.subTest(name=name):
                members = packaged_members(self.commit)
                members[name] = members[name][:-1] + bytes([members[name][-1] ^ 1])
                self.refused('differ from commit', FakeCargo(self.commit, members=members))
                shutil.rmtree(self.work)
                self.work.mkdir()

    def test_extra_or_missing_file_against_cargo_listing_is_refused(self):
        members = packaged_members(self.commit)
        extra = dict(members, **{'src/extra.rs': b'// not listed\n'})
        missing = {k: v for k, v in members.items() if k != 'schema/contract.json'}
        cases = (('extra', FakeCargo(self.commit, members=extra, listing=sorted(members))),
                 ('missing', FakeCargo(self.commit, members=missing, listing=sorted(members))),
                 ('listed-twice', FakeCargo(self.commit, listing=sorted(members) + ['src/lib.rs'])))
        for label, cargo in cases:
            with self.subTest(label=label):
                self.refused('differs from cargo package --list|repeats a path', cargo)
                shutil.rmtree(self.work)
                self.work.mkdir()

    def test_extra_file_absent_from_the_commit_is_refused_even_when_listed(self):
        members = dict(packaged_members(self.commit), **{'src/injected.rs': b'// not in git\n'})
        self.refused('differ from commit', FakeCargo(self.commit, members=members))

    def test_missing_required_generated_file_is_refused(self):
        members = {k: v for k, v in packaged_members(self.commit).items() if k != 'Cargo.lock'}
        self.refused('lacks required files', FakeCargo(self.commit, members=members))

    def test_unsafe_or_duplicate_members_are_refused(self):
        members = packaged_members(self.commit)
        link = tarfile.TarInfo('durable-workflow-%s/src/link.rs' % VERSION)
        link.type, link.linkname = tarfile.SYMTYPE, '/etc/passwd'
        outside = tarfile.TarInfo('durable-workflow-%s/../escape.rs' % VERSION)
        outside.size = 1
        foreign = tarfile.TarInfo('other-1.0.0/src/lib.rs')
        foreign.size = 1
        duplicate = tarfile.TarInfo('durable-workflow-%s/src/lib.rs' % VERSION)
        duplicate.size = len(members['src/lib.rs'])
        for label, raw in (('symlink', [(link, None)]), ('traversal', [(outside, b'x')]),
                           ('foreign-root', [(foreign, b'x')]), ('duplicate', [(duplicate, members['src/lib.rs'])])):
            with self.subTest(label=label):
                self.refused('not a regular file|more than once',
                             FakeCargo(self.commit, crate=make_crate(members, raw=raw)))
                shutil.rmtree(self.work)
                self.work.mkdir()

    def test_corrupt_archive_is_refused(self):
        self.refused('could not read the packaged crate', FakeCargo(self.commit, crate=b'not a gzip tar'))

    def test_expected_package_sha256_mismatch_is_refused(self):
        self.refused('differs from expected', expect_package_sha256='0' * 64)
        shutil.rmtree(self.work)
        self.work.mkdir()
        identity, _ = self.package(expect_package_sha256=hashlib.sha256(
            make_crate(packaged_members(self.commit))).hexdigest())
        self.assertEqual(identity['crate_sha256'], hashlib.sha256(make_crate(packaged_members(self.commit))).hexdigest())

    def test_wrong_packaging_cargo_is_refused(self):
        self.refused('packaging Cargo is', FakeCargo(self.commit, cargo='cargo 1.99.0 (5f94df478 2026-08-27)'))

    def test_package_container_failure_is_cleaned_by_ownership(self):
        name = self.run.base + '-package'
        self.docker = FakeDocker(faults={name: 'created-then-timeout'})
        arguments = package_args(self.checkout, self.commit)
        with mock.patch.object(run, 'call', hybrid(self.docker)), self.assertRaises(subprocess.TimeoutExpired):
            run.package_release(arguments, self.work, self.run)
        self.assertEqual(self.docker.removals(), [['docker', 'rm', '-f', 'id-' + name]])

    def test_foreign_vendored_package_is_refused(self):
        cargo = FakeCargo(self.commit, vendor_foreign=True)
        with self.assertRaisesRegex(run.PackageError, 'only the packaged bytes'):
            self.install(cargo)

    def test_lock_that_is_not_the_packaged_crate_is_refused(self):
        for lock in ({'name': 'durable-workflow', 'version': VERSION, 'source': run.CRATES_IO_SOURCE, 'checksum': '0' * 64},
                     {'name': 'durable-workflow', 'version': VERSION, 'source': 'path+file:///package'},
                     {'name': 'durable-workflow', 'version': '4.11.1', 'source': run.CRATES_IO_SOURCE, 'checksum': '0' * 64}):
            cargo = FakeCargo(self.commit, lock=lock)
            with self.subTest(lock=lock), self.assertRaises(run.PackageError):
                self.install(cargo)
            shutil.rmtree(self.work)
            self.work.mkdir()

    def test_installed_bytes_that_drift_from_the_package_are_refused(self):
        def drift(entry):
            (entry / 'src' / 'lib.rs').write_bytes(b'pub fn other() {}\n')

        def extra(entry):
            (entry / 'src' / 'extra.rs').write_bytes(b'\n')

        def link(entry):
            (entry / 'src' / 'lib.rs').unlink()
            os.symlink(self.checkout / 'src' / 'lib.rs', entry / 'src' / 'lib.rs')
        for tamper in (drift, extra, link):
            cargo = FakeCargo(self.commit, tamper_install=tamper)
            with self.subTest(tamper=tamper.__name__), self.assertRaisesRegex(run.PackageError, 'installed package'):
                self.install(cargo)
            shutil.rmtree(self.work)
            self.work.mkdir()

    def test_directory_source_checksum_is_the_crate_sha256(self):
        identity, _, _ = self.install()
        entry = self.work / 'fixture' / 'vendor' / ('durable-workflow-' + VERSION)
        checksum = json.loads((entry / '.cargo-checksum.json').read_text())
        self.assertEqual(checksum['package'], identity['crate_sha256'])
        self.assertEqual(sorted(checksum['files']), identity['files'])


INDEX = 'sha256:' + '3' * 64


class PrePublicationArguments(unittest.TestCase):
    BASE = ['--server-index', INDEX, '--work-dir', '/tmp/x']
    PACKAGE = ['--package-from', '/tmp/src', '--package-commit', 'a' * 40, '--expect-version', VERSION,
               '--expect-server-version', '3.0.5', '--expect-revision', 'b' * 40, '--expect-release-tag', '3.0.5']

    def parse(self, extra):
        with contextlib.redirect_stderr(io.StringIO()):
            return run.parse_args(self.BASE + extra)

    def test_package_mode_parses(self):
        args = self.parse(self.PACKAGE)
        self.assertEqual((args.package_commit, args.expect_version, args.crate_version), ('a' * 40, VERSION, None))

    def test_server_index_must_be_a_digest_in_package_mode(self):
        with self.assertRaises(SystemExit), contextlib.redirect_stderr(io.StringIO()):
            run.parse_args(['--server-index', 'sha256:x', '--work-dir', '/tmp/x'] + self.PACKAGE)
        self.assertEqual(self.parse(self.PACKAGE + ['--cells', 'deadline,lifecycle']).cells, 'deadline,lifecycle')

    def test_invalid_combinations_are_refused(self):
        def without(option):
            index = self.PACKAGE.index(option)
            return self.PACKAGE[:index] + self.PACKAGE[index + 2:]
        for extra in ([], self.PACKAGE + ['--crate-version', '4.11.1'],
                      without('--package-commit'), without('--expect-version'), without('--expect-server-version'),
                      without('--expect-revision'), without('--expect-release-tag'),
                      self.PACKAGE + ['--fixture-from', '/tmp/f'], self.PACKAGE + ['--source-tag', '4.11.2'],
                      ['--crate-version', '4.11.1', '--package-commit', 'a' * 40],
                      ['--crate-version', '4.11.1', '--expect-package-sha256', 'a' * 64],
                      self.PACKAGE[:3] + ['abc'] + self.PACKAGE[4:],
                      self.PACKAGE + ['--expect-package-sha256', 'A' * 64],
                      without('--expect-version') + ['--expect-version', 'v4.11.2'],
                      # The approval receipt shape the release workflow accepts.
                      self.PACKAGE + ['--cells', 'lifecycle'], self.PACKAGE + ['--cells', 'lifecycle,deadline,extra'],
                      self.PACKAGE + ['--platform', 'linux/riscv64'],
                      without('--expect-release-tag') + ['--expect-release-tag', 'native-3.0.5'],
                      without('--expect-revision') + ['--expect-revision', 'main'],
                      without('--expect-server-version') + ['--expect-server-version', 'v3.0.5']):
            with self.subTest(extra=extra), self.assertRaises(SystemExit):
                self.parse(extra)


class PrePublicationOutcome(unittest.TestCase):
    """The approval candidate exists only for a fully passing run, and binds the package."""

    def main(self, cell_run=None, tamper_after_cells=False):
        tmp = temporary_directory(self)
        work = tmp / 'work'
        identity = {'mode': 'pre-publication', 'package': 'durable-workflow', 'version': VERSION, 'commit': 'a' * 40,
                    'crate_file': 'durable-workflow-%s.crate' % VERSION, 'crate_sha256': hashlib.sha256(b'crate').hexdigest(),
                    'files': ['Cargo.toml'], 'file_list_sha256': run.file_list_sha256(['Cargo.toml']), 'cargo_version': CARGO_V}

        def package_release(args_, work_dir, harness):
            (work_dir / 'package').mkdir()
            (work_dir / 'package' / identity['crate_file']).write_bytes(b'crate')
            return dict(identity), {}

        def cell(args_, name, root, server, fixture_, harness):
            instance = mock.Mock()

            def run_cell():
                if tamper_after_cells:
                    (work / 'package' / identity['crate_file']).write_bytes(b'other')
                (cell_run or (lambda h: None))(harness)
                return {'outcome': 'pass', 'cluster_info': {'version': '3.0.5'}}
            instance.run.side_effect = run_cell
            return instance
        argv = ['run.py'] + PrePublicationArguments.PACKAGE + [
            '--server-index', INDEX, '--cells', 'lifecycle,deadline', '--work-dir', str(work), '--container-prefix', 'general']
        out = io.StringIO()
        server = {'index': 'img@sha256:x', 'executed_manifest': 'img@sha256:y', 'platform': 'linux/arm64'}
        with mock.patch.object(sys, 'argv', argv), mock.patch.object(run, 'call', FakeDocker()), \
                mock.patch.object(run, 'server_identity', return_value=server), \
                mock.patch.object(run, 'package_release', package_release), \
                mock.patch.object(run, 'build_packaged_fixture', return_value={'network': 'none'}), \
                mock.patch.object(run, 'packaged_provenance', return_value={'baselines': {'pass': True}}), \
                mock.patch.object(run, 'crate_provenance') as published, mock.patch.object(run, 'Cell', cell), \
                contextlib.redirect_stdout(out), self.assertRaises(SystemExit) as exited:
            run.main()
        published.assert_not_called()
        self.identity = identity
        return exited.exception.code, json.loads(out.getvalue()), work

    def test_passing_run_writes_a_bound_approval_candidate(self):
        code, receipt, work = self.main()
        self.assertEqual(code, 0)
        approval = json.loads((work / 'approved-package.json').read_text())
        self.assertEqual(receipt['approval_candidate'], str(work.resolve() / 'approved-package.json'))
        self.assertEqual(approval['schema'], run.APPROVAL_SCHEMA)
        for key in ('package', 'version', 'commit', 'crate_sha256', 'files', 'file_list_sha256'):
            self.assertEqual(approval[key], self.identity[key])
        self.assertEqual(approval['cargo_version'], '1.86.0')
        self.assertEqual(sorted(approval['acceptance']), sorted([
            'summary_sha256', 'harness_commit', 'server_index', 'executed_manifest', 'platform',
            'expected_server_version', 'expected_revision', 'expected_release_tag', 'cluster_versions',
            'cells', 'finished_at', 'outcome']))
        self.assertEqual((approval['acceptance']['outcome'], approval['acceptance']['cells']),
                         ('pass', ['deadline', 'lifecycle']))
        self.assertEqual(approval['acceptance']['summary_sha256'],
                         hashlib.sha256((work / 'summary.json').read_bytes()).hexdigest())
        self.assertEqual((approval['acceptance']['expected_server_version'], approval['acceptance']['expected_release_tag'],
                          approval['acceptance']['platform'], approval['acceptance']['cluster_versions']),
                         ('3.0.5', '3.0.5', 'linux/arm64', ['3.0.5']))
        summary = json.loads((work / 'summary.json').read_text())
        self.assertEqual((summary['mode'], summary['package']['crate_sha256_after_cells']),
                         ('pre-publication', self.identity['crate_sha256']))

    def test_cleanup_failure_writes_no_approval(self):
        error = {'step': 'container-remove', 'object': 'x', 'error': 'TimeoutExpired'}
        code, receipt, work = self.main(cell_run=lambda h: h.cleanup_errors.append(error))
        self.assertEqual((code, receipt['approval_candidate']), (1, None))
        self.assertFalse((work / 'approved-package.json').exists())

    def test_package_changed_during_cells_writes_no_approval(self):
        code, receipt, work = self.main(tamper_after_cells=True)
        self.assertEqual((code, receipt['outcome'], receipt['approval_candidate']), (1, 'fail', None))
        self.assertFalse((work / 'approved-package.json').exists())


class PathConfinement(unittest.TestCase):
    """Security review of #178: CLI paths and every path written below them."""

    def setUp(self):
        self.tmp = temporary_directory(self)
        self.outside = temporary_directory(self)
        (self.outside / 'secret').write_bytes(b'PRIVATE')

    def test_cli_paths_refuse_nul_dot_dot_and_symlinks(self):
        (self.tmp / 'real').mkdir()
        (self.tmp / 'link').symlink_to(self.outside, target_is_directory=True)
        for value in ('', 'work\0dir', str(self.tmp / 'real' / '..' / 'escape'), '../work', 'a/../../b',
                      str(self.tmp / 'link')):
            with self.subTest(value=value), self.assertRaises(argparse.ArgumentTypeError):
                run.cli_path(value)
        # Positive controls: absolute, relative and not-yet-existing paths canonicalize.
        self.assertEqual(run.cli_path(str(self.tmp / 'real')), pathlib.Path(os.path.realpath(self.tmp / 'real')))
        self.assertEqual(run.cli_path(str(self.tmp / 'new')), pathlib.Path(os.path.realpath(self.tmp)) / 'new')
        self.assertTrue(run.cli_path('work').is_absolute())

    def test_every_path_option_is_confined(self):
        (self.tmp / 'link').symlink_to(self.outside, target_is_directory=True)
        package = PrePublicationArguments.PACKAGE
        for option in ('--package-from', '--work-dir'):
            for value in (str(self.tmp / 'link'), '../x', 'x\0y'):
                argv = ['--server-index', 'sha256:x', '--work-dir', str(self.tmp / 'w')] + package
                argv[argv.index(option) + 1] = value
                with self.subTest(option=option, value=value), self.assertRaises(SystemExit), \
                        contextlib.redirect_stderr(io.StringIO()):
                    run.parse_args(argv)
        base = ['--server-index', 'sha256:x', '--work-dir', str(self.tmp / 'w'), '--crate-version', '4.11.1']
        for option in ('--fixture-from', '--baselines-control'):
            with self.subTest(option=option), self.assertRaises(SystemExit), contextlib.redirect_stderr(io.StringIO()):
                run.parse_args(base + [option, str(self.tmp / 'link')])

    def test_crafted_versions_are_refused_before_naming_paths(self):
        for version in ('4.11.2/../../x', '../4.11.2', '4.11.2\0', '4.11.2-..', '4.11.2-rc..1', 'v4.11.2',
                        '04.11.2', '4.11.2/', '４.11.2', '4.11.2-a/b'):
            with self.subTest(version=version), self.assertRaises(argparse.ArgumentTypeError):
                run.exact_version(version)
        for version in ('4.11.2', '4.11.2-rc.1+build.5', '10.0.0-alpha-1'):
            with self.subTest(positive=version):
                self.assertEqual(run.exact_version(version), version)

    def test_confined_child_refusals_and_positive_controls(self):
        root = self.tmp / 'root'
        (root / 'src').mkdir(parents=True)
        (root / 'linkdir').symlink_to(self.outside, target_is_directory=True)
        (root / 'link.rs').symlink_to(self.outside / 'secret')
        for relative in ('../secret', 'src/../../secret', str(self.outside / 'secret'), '/etc/passwd',
                         'linkdir/secret', 'link.rs', 'src/a\0b', 'src\\..\\x', '', 'src//a', './a'):
            with self.subTest(relative=relative), self.assertRaises(run.PackageError):
                run.confined_child(root, relative)
        self.assertEqual(run.confined_child(root, 'src/lib.rs'),
                         pathlib.Path(os.path.realpath(root)) / 'src' / 'lib.rs')

    def test_write_members_never_escapes_or_follows_links(self):
        for name in ('../escape.rs', 'src/../../escape.rs', '/abs.rs', 'a\0b'):
            destination = self.tmp / ('dest-%d' % len(os.listdir(self.tmp)))
            with self.subTest(name=name), self.assertRaises(run.PackageError):
                run.write_members({name: b'x'}, destination)
        self.assertEqual(sorted(os.listdir(self.outside)), ['secret'])
        self.assertFalse((self.tmp / 'escape.rs').exists())
        # A member whose directory is a link planted beside it is refused too.
        destination = self.tmp / 'planted'
        original_mkdir = pathlib.Path.mkdir

        def mkdir(path, *a, **k):
            if path.name == 'src' and path.parent == pathlib.Path(os.path.realpath(destination)):
                path.symlink_to(self.outside, target_is_directory=True)
                return None
            return original_mkdir(path, *a, **k)
        with mock.patch.object(pathlib.Path, 'mkdir', mkdir), self.assertRaises(run.PackageError):
            run.write_members({'src/lib.rs': b'x'}, destination)
        self.assertEqual(sorted(os.listdir(self.outside)), ['secret'])
        # Positive control.
        run.write_members({'src/lib.rs': b'pub fn a() {}\n', 'Cargo.toml': b'[package]\n'}, self.tmp / 'ok')
        self.assertEqual((self.tmp / 'ok' / 'src' / 'lib.rs').read_bytes(), b'pub fn a() {}\n')

    def test_archive_member_with_nul_or_backslash_is_refused(self):
        tmp = temporary_directory(self)
        commit = make_repo(tmp / 'repo')
        members = packaged_members(commit)
        hardlink = tarfile.TarInfo('durable-workflow-%s/src/hard.rs' % VERSION)
        hardlink.type, hardlink.linkname = tarfile.LNKTYPE, 'durable-workflow-%s/Cargo.lock' % VERSION
        crate = make_crate(members, raw=[(hardlink, None)])
        with self.assertRaisesRegex(run.PackageError, 'not a regular file'):
            run.verify_package_archive(crate, VERSION, commit, sorted(members) + ['src/hard.rs'], lambda path: None)
        for name in ('src/a\\..\\b.rs',):
            crate = make_crate(dict(members, **{name: b'x'}))
            with self.subTest(name=name), self.assertRaisesRegex(run.PackageError, 'not a regular file'):
                run.verify_package_archive(crate, VERSION, commit, sorted(members) + [name], lambda path: None)


class HarnessCleanliness(unittest.TestCase):
    """F2: an approval names the harness commit, so package mode needs a clean harness."""

    def setUp(self):
        self.tmp = temporary_directory(self)
        self.harness = self.tmp / 'harness'
        self.commit = make_repo(self.harness)
        self.argv = ['run.py'] + PrePublicationArguments.PACKAGE + [
            '--server-index', INDEX, '--cells', 'lifecycle,deadline', '--work-dir', str(self.tmp / 'work')]

    def main(self, docker):
        with mock.patch.object(run, 'REPO', self.harness), mock.patch.object(run, 'call', hybrid(docker)), \
                mock.patch.object(sys, 'argv', self.argv), contextlib.redirect_stdout(io.StringIO()), \
                self.assertRaises(SystemExit) as exited:
            run.main()
        return exited.exception

    def test_dirty_harness_checkout_is_refused_before_any_work(self):
        for change in ('modified', 'untracked', 'staged'):
            with self.subTest(change=change):
                target = self.harness / ('src/lib.rs' if change != 'untracked' else 'scratch.py')
                original = target.read_bytes() if target.exists() else None
                target.write_bytes(b'# local edit\n')
                if change == 'staged':
                    subprocess.run(['git', '-C', str(self.harness), 'add', '-A'], check=True, env=git_env())
                docker = FakeDocker()
                error = self.main(docker)
                self.assertIn('clean harness checkout', str(error))
                self.assertEqual(docker.commands, [])
                self.assertFalse((self.tmp / 'work').exists())
                if original is None:
                    target.unlink()
                else:
                    target.write_bytes(original)
                subprocess.run(['git', '-C', str(self.harness), 'reset', '--quiet', '--hard'], check=True, env=git_env())

    def test_clean_harness_commit_is_recorded(self):
        args = run.parse_args(self.argv[1:])
        with mock.patch.object(run, 'REPO', self.harness), mock.patch.object(run, 'call', hybrid(FakeDocker())):
            self.assertEqual(run.harness_identity(args), self.commit)

    def test_published_mode_still_only_records_head(self):
        (self.harness / 'scratch.py').write_bytes(b'x')
        args = run.parse_args(['--crate-version', '4.11.1', '--server-index', 'sha256:x', '--work-dir', str(self.tmp / 'w')])
        with mock.patch.object(run, 'REPO', self.harness), mock.patch.object(run, 'call', hybrid(FakeDocker())):
            self.assertEqual(run.harness_identity(args), self.commit)


if __name__ == '__main__':
    unittest.main()
