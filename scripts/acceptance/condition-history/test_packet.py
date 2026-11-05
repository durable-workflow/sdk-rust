"""Packet guard and unchanged history controls. No Docker/Cargo/network is executed."""
import ast
import copy
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
from test_subject import subject, fixture_subject, remove_private, REPO, HERE

class Packet(unittest.TestCase):
    def setUp(self):
        self.root = pathlib.Path(tempfile.mkdtemp()).resolve()
        self.addCleanup(remove_private, self.root)
        now = int(time.time())
        self.args = types.SimpleNamespace(mode='run', checkout=str(REPO), work_dir=str(self.root / 'fresh'),
            package_commit=subject.H, runner_commit='a' * 40, run_token='0123456789abcdef',
            docker_endpoint='unix:///synthetic.sock', guest_boot_id='11111111-1111-1111-1111-111111111111', started_unix=now, active_unix=now+1740, cleanup_unix=now+1800)
        subject.ARGS = self.args
        subject.CURRENT = fixture_subject(self.root)
        self.packet = subject.CURRENT.module('engine')
        global packet
        packet = self.packet

    def git(self, checkout, *args):
        return {'HEAD': 'a' * 40, 'refs/tags/4.12.0^{commit}': subject.H}.get(args[-1], '')

    def test_preflight_refusals_precede_any_side_effect(self):
        for field, value in {'package_commit':'b'*40, 'run_token':'foreign', 'active_unix':0,
                             'started_unix':0, 'docker_endpoint':'tcp://foreign'}.items():
            args = copy.copy(self.args);setattr(args, field, value)
            with self.subTest(field=field), mock.patch.object(subject,'git',self.git), self.assertRaises(SystemExit):
                subject.validate(args)
            self.assertFalse(pathlib.Path(args.work_dir).exists())

    def test_wrong_h_refuses_under_python_optimization(self):
        result = subprocess.run([sys.executable,str(HERE/'run.py'),'run','--checkout',str(REPO),
            '--runner-commit','a'*40,'--package-commit','b'*40], env=dict(os.environ,PYTHONOPTIMIZE='1'),
            capture_output=True,text=True,timeout=5)
        self.assertNotEqual(result.returncode,0)
        self.assertIn('wrong fixed package source',result.stderr)
        self.assertFalse(pathlib.Path(self.args.work_dir).exists())

    def test_no_optimizable_runtime_assert(self):
        for name in ('run.py','subject.py','engine.py','resources.py','recover.py','guest-host.py'):
            with self.subTest(name=name):
                self.assertFalse(any(isinstance(n,ast.Assert) for n in ast.walk(ast.parse((HERE/name).read_text()))))

    def recovery_start(self, changed=None, process_missing=False, exited=False):
        pathlib.Path(self.args.work_dir).mkdir()
        subject.RECORD_SHA = 'c' * 64
        process = {'pid': 23456, 'pgid': 23456, 'sid': 23456, 'boot_id': '11111111-1111-1111-1111-111111111111', 'start_ticks': 234567}
        recovery_sha = subject.CURRENT.value['files'][subject.PREFIX + 'recover.py']['sha256']
        ready = {'record_sha256': subject.RECORD_SHA, 'manifest_sha256': subject.CURRENT.sha256,
                 'recovery_sha256': recovery_sha, 'process': process}
        ready.update(changed or {})
        if not exited:
            subject.exclusive(pathlib.Path(self.args.work_dir) / 'recovery-ready.json', subject.canonical(ready))
        watcher = mock.Mock(pid=process['pid'])
        watcher.poll.return_value = 1 if exited else None
        with mock.patch.object(packet.subprocess, 'Popen', return_value=watcher) as launch, \
                mock.patch.object(packet.recovery, 'process_identity', return_value=None if process_missing else process), \
                mock.patch.object(packet, 'registry_identity', side_effect=AssertionError('no registry before readiness')), \
                mock.patch.object(packet.resources, 'call', side_effect=AssertionError('no daemon')):
            if changed or process_missing or exited:
                with self.assertRaises(SystemExit):
                    packet.start_recovery(self.args, {'recovery_sha256': recovery_sha})
            else:
                actual, receipt_sha = packet.start_recovery(self.args, {'recovery_sha256': recovery_sha})
                self.assertIs(actual, watcher)
                receipt = (pathlib.Path(self.args.work_dir) / 'recovery-process.json').read_bytes()
                self.assertEqual(subject.digest(receipt), receipt_sha)
                self.assertEqual(json.loads(receipt), {'source_sha256': recovery_sha, 'process': process})
        argv = launch.call_args.args[0]
        self.assertEqual(argv[:4], [sys.executable, '-I', '-S', '-c'])
        self.assertEqual(subject.digest(argv[4].encode()), recovery_sha)
        self.assertEqual(argv[-2:], ['--at', str(self.args.active_unix)])
        self.assertTrue(launch.call_args.kwargs['start_new_session'])
        return watcher

    def test_captured_recovery_requires_exact_ready_receipt(self):
        watcher = self.recovery_start()
        watcher.terminate.assert_not_called()

    def test_wrong_recovery_ready_refuses_before_registry_or_daemon(self):
        for key in ('record_sha256', 'manifest_sha256', 'recovery_sha256', 'process'):
            with self.subTest(key=key):
                watcher = self.recovery_start({key: 'foreign'})
                watcher.terminate.assert_called_once()
                watcher.wait.assert_called_once_with(timeout=5)
                import shutil
                shutil.rmtree(self.args.work_dir)

    def test_missing_recovery_identity_stops_the_started_child(self):
        watcher = self.recovery_start(process_missing=True)
        watcher.terminate.assert_called_once()
        watcher.wait.assert_called_once_with(timeout=5)

    def test_exited_recovery_refuses_without_launching_any_role(self):
        watcher = self.recovery_start(exited=True)
        watcher.terminate.assert_not_called()

    def test_dirty_head_tag_and_existing_work_refuse(self):
        for response in ('dirty','wrong-head','wrong-tag','existing'):
            def git(checkout,*args):
                if response=='dirty' and args[0]=='status':return ' M source'
                if response=='wrong-head' and args[-1]=='HEAD':return 'b'*40
                if response=='wrong-tag' and args[-1].startswith('refs/tags'):return 'b'*40
                return self.git(checkout,*args)
            if response=='existing':pathlib.Path(self.args.work_dir).mkdir()
            with self.subTest(response=response),mock.patch.object(subject,'git',git),self.assertRaises(SystemExit):
                subject.validate(self.args)

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

    def test_server_review_countercontrols(self):
        cases = []
        report = self.report()
        report['history'].insert(-1, {'event_type': 'TimerFired', 'payload': {
            'sequence': 3, 'condition_wait_id': 'foreign', 'timer_id': 'foreign',
            'timer_kind': 'condition_timeout', 'delay_seconds': 300}})
        cases.append(('foreign-timer', report))
        report = self.report()
        report['history'].insert(-1, {'event_type': 'ConditionWaitTimedOut', 'payload': {
            'sequence': 999, 'condition_wait_id': 'foreign'}})
        cases.append(('foreign-timeout', report))
        report = self.report()
        extra = copy.deepcopy(report['history'][4])
        extra['payload'].update(sequence=999, condition_wait_id='foreign', condition_key='foreign')
        report['history'].insert(-1, extra)
        cases.append(('foreign-fifth-open', report))
        report = self.report()
        for row in report['history']:
            if row['event_type'].startswith('ConditionWait'):
                row['payload']['condition_definition_fingerprint'] = 'sha256:foreign'
        cases.append(('consistently-wrong-fingerprint', report))
        report = self.report()
        report['history'][0]['payload']['timeout_seconds'] = 300.0
        cases.append(('float-original-timeout', report))
        for label, report in cases:
            with self.subTest(label=label), self.assertRaises(ValueError):
                packet.comparator.check(report)

    def snapshots(self, events):
        signals = [i for i, event in enumerate(events) if event['event_type'] == 'SignalReceived']
        return {'outcome': 'pass', 'result': 'both', 'initial': events[:signals[0]],
                'reopened': events[:signals[1]], 'history': events}

    def test_additional_reopens_keep_the_same_declared_members(self):
        events = self.report()['history']
        extra = copy.deepcopy(events[4:6])
        for sequence, event in zip((5, 6), extra):
            event['payload'].update(sequence=sequence, condition_wait_id='condition:' + str(sequence))
        events[6:6] = extra
        report = self.snapshots(events)
        self.assertEqual(packet.comparator.check(report)['all_open_sequences'], [1, 2, 3, 4, 5, 6])

    def timer(self, kind, sequence):
        return {'event_type': kind, 'payload': {'sequence': sequence,
            'condition_wait_id': 'condition:' + str(sequence), 'timer_id': 'timer:' + str(sequence),
            'timer_kind': 'condition_timeout', 'delay_seconds': 300}}

    def test_correlated_superseded_orphan_timer_is_distinct_from_the_current_tail(self):
        events = self.report()['history']
        orphan = copy.deepcopy(events[0])
        orphan['event_type'] = 'ConditionWaitTimedOut'
        events.insert(1, self.timer('TimerScheduled', 1))
        events[-3:-3] = [self.timer('TimerFired', 1), orphan]
        self.assertEqual(packet.comparator.check(self.snapshots(events))['outcome'], 'pass')

    def test_timer_terminals_require_prior_schedule_and_row_ordered_supersession(self):
        for label in ('missing-schedule', 'current-tail', 'before-reopen', 'duplicate-fire', 'wrong-delay'):
            events = self.report()['history']
            if label == 'current-tail':
                events.insert(5, self.timer('TimerScheduled', 3))
                events.insert(-1, self.timer('TimerFired', 3))
            else:
                if label != 'missing-schedule':
                    events.insert(1, self.timer('TimerScheduled', 1))
                fired = self.timer('TimerFired', 1)
                if label == 'wrong-delay':
                    fired['payload']['delay_seconds'] = 299
                events.insert(2 if label == 'before-reopen' else -1, fired)
                if label == 'duplicate-fire':
                    events.insert(-1, copy.deepcopy(fired))
            report = self.snapshots(events)
            with self.subTest(label=label), self.assertRaises(ValueError):
                packet.comparator.check(report)


if __name__ == "__main__":unittest.main()
