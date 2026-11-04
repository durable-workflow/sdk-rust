"""Inert source controls. Every Docker/Cargo/process launch is stubbed."""
import ast
import copy
import importlib.util
import json
import os
import pathlib
import subprocess
import sys
import tempfile
import time
import types
import unittest
from unittest import mock

HERE = pathlib.Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('condition_packet', HERE / 'run.py')
packet = importlib.util.module_from_spec(spec)
spec.loader.exec_module(packet)


class Packet(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = pathlib.Path(self.tmp.name)
        now = int(time.time())
        self.args = types.SimpleNamespace(work_dir=self.root / 'fresh', package_commit=packet.H,
            runner_commit='a' * 40, run_token='0123456789abcdef', started_unix=now,
            active_unix=now + 1740, cleanup_unix=now + 1800)

    def git(self, *args):
        return {'HEAD': 'a' * 40, 'refs/tags/4.12.0^{commit}': packet.H}.get(args[-1], '')

    def test_preflight_refusals_precede_construction_and_launch(self):
        cases = {'package_commit': 'b' * 40, 'runner_commit': 'not-a-commit',
                 'run_token': 'foreign', 'active_unix': 0, 'started_unix': 0}
        for field, value in cases.items():
            args = copy.copy(self.args)
            setattr(args, field, value)
            with self.subTest(field=field), mock.patch.object(packet, 'git', self.git), \
                    mock.patch.object(packet, 'command') as command, \
                    mock.patch.object(packet.subprocess, 'Popen') as launch, \
                    self.assertRaises(SystemExit):
                packet.execute(args)
            command.assert_not_called()
            launch.assert_not_called()
            self.assertFalse(args.work_dir.exists())

    def test_wrong_h_refuses_under_python_optimization(self):
        result = subprocess.run([sys.executable, str(HERE / 'run.py'), 'run', '--work-dir',
            str(self.args.work_dir), '--run-token', self.args.run_token, '--package-commit', 'b' * 40],
            env=dict(os.environ, PYTHONOPTIMIZE='1'), capture_output=True, text=True, timeout=5)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('wrong fixed package source', result.stderr)
        self.assertFalse(self.args.work_dir.exists())

    def test_no_optimizable_assert_and_preflight_precedes_effects(self):
        tree = ast.parse((HERE / 'run.py').read_text())
        self.assertFalse(any(isinstance(n, ast.Assert) for n in ast.walk(tree)))
        execute = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'execute')
        self.assertEqual(ast.unparse(execute.body[0]), 'preflight(args)')

    def test_dirty_head_tag_and_existing_work_refuse(self):
        for response in ('dirty', 'wrong-head', 'wrong-tag', 'existing'):
            def git(*args):
                if response == 'dirty' and args[0] == 'status':
                    return ' M source'
                if response == 'wrong-head' and args[-1] == 'HEAD':
                    return 'b' * 40
                if response == 'wrong-tag' and args[-1].startswith('refs/tags'):
                    return 'b' * 40
                return self.git(*args)
            if response == 'existing':
                self.args.work_dir.mkdir()
            with self.subTest(response=response), mock.patch.object(packet, 'git', git), self.assertRaises(SystemExit):
                packet.preflight(self.args)

    def test_exact_immutable_consumer_inputs_export_without_sdk_overlay(self):
        fixture = self.root / 'fixture'
        packet.export_consumer(fixture)
        self.assertFalse((fixture / 'src').exists())
        self.assertEqual(packet.native.sha256(fixture / 'tests/official.rs'),
                         packet.INPUTS['tests/replay_regression_corpus.rs'])
        for source, expected in packet.INPUTS.items():
            if source.endswith('.json'):
                self.assertEqual(packet.native.sha256(fixture / source), expected)
        manifest = packet.tomllib.loads((fixture / 'Cargo.toml').read_text())
        self.assertEqual(manifest['dependencies']['durable-workflow'], '=4.12.0')
        self.assertNotIn('patch', manifest)
        self.assertFalse((fixture / 'Cargo.lock').exists())
        self.assertFalse((fixture / '.cargo').exists())

    def test_roles_keep_reviewed_bounds_and_credential_scopes(self):
        owner = packet.native.Run(packet.PREFIX, token=self.args.run_token)
        cell = packet.ConditionCell(types.SimpleNamespace(), packet.CELL, self.root,
            {'executed_manifest': packet.SERVER}, self.root / 'fixture', owner)
        calls = []
        def launch(_self, line, name, image, child, user, credentials):
            calls.append((line, credentials))
            return name
        def inspect(*args, **kwargs):
            line, _ = calls[-1]
            return types.SimpleNamespace(stdout=json.dumps({'id': 'synthetic', 'image': 'synthetic',
                'run': owner.token, 'owner': owner.prefix, 'pid': 1, 'mounts': 'tmpfs:/credentials;',
                'memory': int(line[line.index('--memory') + 1].removesuffix('m')) * 1024 * 1024,
                'nano_cpus': int(float(line[line.index('--cpus') + 1]) * 1_000_000_000)}))
        with mock.patch.object(packet.native.Cell, 'launch', launch), mock.patch.object(packet.native, 'call', inspect):
            for role, credentials in [('mysql', 'none'), ('redis', 'none'), ('bootstrap', 'server'),
                                      ('runtime', 'both'), ('queue', 'server'), ('consumer', 'auth')]:
                cell.launch(['docker', 'run', '--cpus', '1', '--memory', '1g'], cell.task + '-' + role,
                            packet.SERVER, ['original'], '1000:1000', credentials)
                line, observed = calls[-1]
                self.assertEqual(line[line.index('--cpus') + 1], packet.LIMITS[role][0])
                self.assertEqual(line[line.index('--memory') + 1], packet.LIMITS[role][1])
                self.assertEqual(observed, credentials)
        cell.containers = ['held'] * 5
        with self.assertRaisesRegex(SystemExit, 'ceiling'):
            cell.launch(['docker', 'run'], cell.task + '-consumer', packet.RUST, [], '1000:1000', 'auth')

    def report(self):
        fixture = packet.native.call(['git', '-C', str(packet.REPO), 'show', packet.H +
            ':tests/fixtures/replay-regressions/two-member-condition-group-server-reopen.json']).stdout
        events = json.loads(fixture)['history']
        events.append({'event_type': 'WorkflowCompleted', 'payload': {}})
        return {'outcome': 'pass', 'result': 'both', 'initial': events[:2], 'reopened': events[:6], 'history': events}

    def test_canonical_group_shape_is_accepted(self):
        result = packet.comparator.check(self.report())
        self.assertEqual(result['reopened_sequences'], [3, 4])

    def test_group_comparator_rejects_lineage_order_timeout_and_false_health(self):
        cases = [lambda r: r.update(result='one'),
                 lambda r: r['history'][4]['payload'].update(parallel_group_index=1),
                 lambda r: r['history'][4]['payload'].update(condition_wait_occurrence_id='new-call'),
                 lambda r: r['history'][4]['payload'].update(timeout_seconds=301),
                 lambda r: r['history'][0]['payload'].update(sequence=True),
                 lambda r: r['history'][4]['payload'].update(parallel_group_index=False),
                 lambda r: r['history'].insert(0, {'event_type': 'ReplayFailure'}),
                 lambda r: r['history'][3]['payload'].update(sequence=1),
                 lambda r: r['history'][-2]['payload'].update(signal_name='wrong')]
        for change in cases:
            report = self.report()
            change(report)
            with self.subTest(change=change), self.assertRaises((ValueError, KeyError, TypeError)):
                packet.comparator.check(report)

    def test_foreign_recovery_record_refuses_before_docker(self):
        self.args.work_dir.mkdir()
        (self.args.work_dir / 'run.json').write_text(json.dumps({'owner': 'other',
            'token': self.args.run_token, 'cells': [packet.CELL]}))
        with mock.patch.object(packet.native, 'recover_run') as recover, self.assertRaises(SystemExit):
            packet.recover(self.args.work_dir, self.args.run_token)
        recover.assert_not_called()


if __name__ == '__main__':
    unittest.main()
