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


def done(command, returncode=0, stdout='', stderr=''):
    return subprocess.CompletedProcess(command, returncode, stdout, stderr)


class FakeDocker:
    """Records every command and answers from a small in-memory container table."""

    def __init__(self, containers=None, networks=None, run_result=None, created=None):
        self.commands = []
        self.containers = dict(containers or {})  # name -> (id, run label)
        self.networks = dict(networks or {})
        self.run_result = run_result  # callable(command) -> CompletedProcess, or exception
        self.created = created  # callable(command) for side effects of a successful build

    def __call__(self, command, timeout=120, check=True, **kwargs):
        self.commands.append((list(command), timeout))
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
            if isinstance(self.run_result, BaseException):
                raise self.run_result
            result = self.run_result(command) if self.run_result else done(command, stdout='rustc 1.86.0\ncargo 1.86.0\n')
            if result.returncode == 0 and self.created:
                self.created(command)
            return self._result(result, check)
        return done(command)

    @staticmethod
    def _result(result, check):
        if check and result.returncode:
            raise RuntimeError('%s failed (%s)' % (' '.join(result.args[:4]), result.returncode))
        return result

    def removals(self):
        return [c for c, _ in self.commands if c[:3] == ['docker', 'rm', '-f'] or c[:3] == ['docker', 'network', 'rm']]

    def runs(self):
        return [(c, t) for c, t in self.commands if c[:2] == ['docker', 'run']]


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
        self.assertEqual((self.fixture / 'build.stdout').read_text(), 'rustc 1.86.0\ncargo 1.86.0\n')

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


class OwnershipCheckedCleanup(HarnessRun):
    def name(self):
        return self.run.base + '-build'

    def test_name_collision_never_removes_the_existing_container(self):
        # Another invocation already owns this name; our docker run fails with a conflict.
        docker = FakeDocker(containers={self.name(): ('foreign-id', FOREIGN)},
                            run_result=lambda c: done(c, 125, stderr='Conflict. The container name is already in use'))
        with self.assertRaises(RuntimeError):
            self.build(docker)
        self.assertEqual(docker.removals(), [])

    def test_foreign_or_missing_label_is_refused(self):
        for label in (FOREIGN, None, TOKEN.upper(), TOKEN + 'x', ''):
            with self.subTest(label=label):
                docker = FakeDocker(containers={'c': ('foreign-id', label)})
                with mock.patch.object(run, 'call', docker):
                    self.assertIsNone(run.remove_owned_container('c', self.run))
                self.assertEqual(docker.removals(), [])

    def test_failed_create_with_no_container_removes_nothing(self):
        docker = FakeDocker(run_result=lambda c: done(c, 125, stderr='image not found'))
        with self.assertRaises(RuntimeError):
            self.build(docker)
        self.assertEqual(docker.removals(), [])

    def test_client_timeout_removes_only_the_owned_container_by_id(self):
        docker = FakeDocker(containers={self.name(): ('owned-id', TOKEN)},
                            run_result=subprocess.TimeoutExpired(['docker', 'run'], 3600))
        with self.assertRaises(subprocess.TimeoutExpired):
            self.build(docker)
        self.assertEqual(docker.removals(), [['docker', 'rm', '-f', 'owned-id']])

    def test_build_error_removes_only_the_owned_container_by_id(self):
        docker = FakeDocker(containers={self.name(): ('owned-id', TOKEN)},
                            run_result=lambda c: done(c, 101, stderr='error: could not compile'))
        with self.assertRaises(RuntimeError):
            self.build(docker)
        self.assertEqual(docker.removals(), [['docker', 'rm', '-f', 'owned-id']])

    def test_successful_build_already_removed_by_rm_needs_no_forced_cleanup(self):
        docker = FakeDocker(created=write_binary(self.fixture))
        self.build(docker)
        inspects = [c for c, _ in docker.commands if c[:3] == ['docker', 'container', 'inspect']]
        self.assertEqual([c[-1] for c in inspects], [self.name()])
        self.assertEqual(docker.removals(), [])

    def test_cell_cleanup_is_ownership_checked(self):
        cell_root = self.tmp / 'work'
        fixture = self.tmp / 'built'
        write_binary(fixture)(None)
        cell = run.Cell(args(), 'lifecycle', cell_root, {'executed_manifest': 'img'}, fixture, self.run)
        runtime = cell.task + '-runtime'
        proxy = cell.task + '-proxy'
        # runtime starts (ours); proxy collides with a foreign container of the same name.
        docker = FakeDocker(containers={runtime: ('runtime-id', TOKEN), proxy: ('foreign-id', FOREIGN)},
                            networks={cell.task: ('net-id', TOKEN)},
                            run_result=lambda c: done(c, 125) if proxy in c else done(c, stdout='cid\n'))
        with mock.patch.object(run, 'call', docker), self.assertRaises(RuntimeError):
            cell.run()
        self.assertEqual(docker.removals(), [['docker', 'rm', '-f', 'runtime-id'], ['docker', 'network', 'rm', 'net-id']])
        self.assertFalse((cell.state / 'credentials.json').exists())

    def test_cell_refuses_a_foreign_network(self):
        cell = run.Cell(args(), 'deadline', self.tmp / 'work', {}, self.tmp, self.run)
        docker = FakeDocker(networks={cell.task: ('foreign-net', FOREIGN)})
        with mock.patch.object(run, 'call', docker):
            self.assertIsNone(run.remove_owned_network(cell.task, self.run))
        self.assertEqual(docker.removals(), [])


class FixtureFromSkipsBuilder(unittest.TestCase):
    def test_fixture_from_never_starts_a_build_container(self):
        tmp = temporary_directory(self)
        fixture = tmp / 'prebuilt'
        write_binary(fixture)(None)
        (fixture / 'build.stdout').write_text('rustc 1.86.0\ncargo 1.86.0\n')
        argv = ['run.py', '--crate-version', '4.11.1', '--server-index', 'sha256:x', '--cells', '',
                '--fixture-from', str(fixture), '--work-dir', str(tmp / 'work'), '--container-prefix', 'general']
        docker = FakeDocker()
        cell = mock.Mock()
        cell.return_value.run.return_value = {'outcome': 'pass'}
        with mock.patch.object(sys, 'argv', argv), mock.patch.object(run, 'call', docker), \
                mock.patch.object(run, 'server_identity', return_value={'executed_manifest': 'img'}), \
                mock.patch.object(run, 'crate_provenance', return_value={'baselines': {'pass': True}}), \
                mock.patch.object(run, 'build_fixture') as build, mock.patch.object(run, 'Cell', cell), \
                contextlib.redirect_stdout(io.StringIO()), self.assertRaises(SystemExit):
            run.main()
        build.assert_not_called()
        self.assertEqual(docker.runs(), [])
        self.assertNotIn(['docker', 'pull', RUST], [c for c, _ in docker.commands])
        summary = json.loads((tmp / 'work' / 'summary.json').read_text())
        self.assertEqual(summary['build']['reused_fixture'], str(fixture.resolve()))
        # Leftover reporting is scoped to this invocation's run token.
        [leftovers] = [c for c, _ in docker.commands if c[:3] == ['docker', 'ps', '-aq']]
        self.assertTrue(re.fullmatch(r'label=dev\.durable-workflow\.acceptance\.run=[0-9a-f]{16}', leftovers[-1]))


if __name__ == '__main__':
    unittest.main()
