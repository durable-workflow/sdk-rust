"""Container bounds and ownership-checked cleanup for the acceptance harness.

Every Docker call is stubbed; nothing here starts a container.
"""
import argparse
import contextlib
import io
import json
import os
import pathlib
import re
import shutil
import subprocess
import sys
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
        self.created = created  # side effect of a successful build, e.g. writing the binary
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
            if self.created:
                self.created(command)
            return done(command, stdout=BUILD_STDOUT if '-d' not in command else 'cid\n') if result is None else result
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


class FixtureFromSkipsBuilder(unittest.TestCase):
    def test_fixture_from_never_starts_a_build_container_and_reports_leftovers(self):
        tmp = temporary_directory(self)
        fixture = tmp / 'prebuilt'
        write_binary(fixture)(None)
        (fixture / 'build.stdout').write_text(BUILD_STDOUT)
        argv = ['run.py', '--crate-version', '4.11.1', '--server-index', 'sha256:x', '--cells', '',
                '--fixture-from', str(fixture), '--work-dir', str(tmp / 'work'), '--container-prefix', 'general']
        docker = FakeDocker()
        cell = mock.Mock()
        cell.return_value.run.return_value = {'outcome': 'pass'}
        out = io.StringIO()
        with mock.patch.object(sys, 'argv', argv), mock.patch.object(run, 'call', docker), \
                mock.patch.object(run, 'server_identity', return_value={'executed_manifest': 'img'}), \
                mock.patch.object(run, 'crate_provenance', return_value={'baselines': {'pass': True}}), \
                mock.patch.object(run, 'build_fixture') as build, mock.patch.object(run, 'Cell', cell), \
                contextlib.redirect_stdout(out), self.assertRaises(SystemExit):
            run.main()
        build.assert_not_called()
        self.assertEqual(docker.runs(), [])
        self.assertNotIn(['docker', 'pull', RUST], [c for c, _ in docker.commands])
        summary = json.loads((tmp / 'work' / 'summary.json').read_text())
        self.assertEqual(summary['build']['reused_fixture'], str(fixture.resolve()))
        # Leftover reporting covers containers and networks, scoped to this run's token.
        run_filter = re.compile(r'label=dev\.durable-workflow\.acceptance\.run=[0-9a-f]{16}')
        [containers] = [c for c, _ in docker.commands if c[:3] == ['docker', 'ps', '-aq']]
        [networks] = [c for c, _ in docker.commands if c[:4] == ['docker', 'network', 'ls', '-q']]
        self.assertTrue(run_filter.fullmatch(containers[-1]))
        self.assertTrue(run_filter.fullmatch(networks[-1]))
        receipt = json.loads(out.getvalue())
        self.assertEqual((receipt['leftover_containers'], receipt['leftover_networks'], receipt['cleanup_errors']),
                         ([], [], []))
        self.assertEqual(summary['cleanup'], {'errors': [], 'leftover_containers': [], 'leftover_networks': []})

    def test_failed_leftover_query_is_reported_not_raised(self):
        tmp = temporary_directory(self)
        fixture = tmp / 'prebuilt'
        write_binary(fixture)(None)
        (fixture / 'build.stdout').write_text(BUILD_STDOUT)
        argv = ['run.py', '--crate-version', '4.11.1', '--server-index', 'sha256:x', '--cells', '',
                '--fixture-from', str(fixture), '--work-dir', str(tmp / 'work')]
        docker = FakeDocker(raise_on={(('docker', 'ps'), None): TIMEOUT})
        cell = mock.Mock()
        cell.return_value.run.return_value = {'outcome': 'pass'}
        out = io.StringIO()
        with mock.patch.object(sys, 'argv', argv), mock.patch.object(run, 'call', docker), \
                mock.patch.object(run, 'server_identity', return_value={'executed_manifest': 'img'}), \
                mock.patch.object(run, 'crate_provenance', return_value={'baselines': {'pass': True}}), \
                mock.patch.object(run, 'Cell', cell), contextlib.redirect_stdout(out), self.assertRaises(SystemExit):
            run.main()
        receipt = json.loads(out.getvalue())
        self.assertIsNone(receipt['leftover_containers'])
        self.assertEqual(receipt['leftover_networks'], [])
        self.assertEqual(receipt['cleanup_errors'],
                         [{'step': 'leftover-query', 'object': 'containers', 'error': 'TimeoutExpired'}])
        summary = json.loads((tmp / 'work' / 'summary.json').read_text())
        self.assertEqual(summary['cleanup']['errors'], receipt['cleanup_errors'])


if __name__ == '__main__':
    unittest.main()
