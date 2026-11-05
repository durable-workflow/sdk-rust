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
from test_subject import subject, recovery, parent_clock, fixture_subject, remove_private, REPO, HERE

class Packet(unittest.TestCase):
    def setUp(self):
        self.root = pathlib.Path(tempfile.mkdtemp()).resolve()
        self.addCleanup(remove_private, self.root)
        now = int(time.time())
        self.args = types.SimpleNamespace(mode='run', checkout=str(REPO), work_dir=str(self.root / 'fresh'),
            package_commit=subject.H, runner_commit='a' * 40, run_token='0123456789abcdef',
            docker_endpoint='unix:///synthetic.sock', guest_boot_id='11111111-1111-1111-1111-111111111111', started_unix=now, active_unix=now+1740, cleanup_unix=now+1800)
        self.args.parent_clock_json = recovery.canonical(parent_clock(now)).decode()
        self.args.parent_clock_sha256 = recovery.digest(self.args.parent_clock_json.encode())
        envelope = recovery.clock_envelope(json.loads(self.args.parent_clock_json), now, self.args.guest_boot_id)
        for key, value in envelope.items():setattr(self.args, key, value)
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
            with self.subTest(field=field), mock.patch.object(subject,'git',self.git):
                check = subject.validate
                if field in ('active_unix', 'started_unix'):
                    subject.validate(args)
                    check = recovery.launch_clock
                with self.assertRaises(SystemExit):
                    check(args)
            self.assertFalse(pathlib.Path(args.work_dir).exists())

    def test_wrong_h_refuses_under_python_optimization(self):
        startup = self.root / 'ambient-startup';startup.mkdir()
        (startup / 'sitecustomize.py').write_text("raise SystemExit('unexpected ambient startup')\n")
        env = dict(os.environ, PYTHONPATH=str(startup), PYTHONOPTIMIZE='1')
        argv = [str(HERE/'run.py'),'run','--checkout',str(REPO),
            '--runner-commit','a'*40,'--package-commit','b'*40]
        ambient = subprocess.run([sys.executable, *argv], env=env,
            capture_output=True, text=True, timeout=20)
        self.assertNotEqual(ambient.returncode, 0)
        self.assertIn('unexpected ambient startup', ambient.stderr)
        result = subprocess.run([sys.executable,'-I','-S','-B','-O', *argv], env=env,
            capture_output=True,text=True,timeout=20)
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


class ClockContract(unittest.TestCase):
    BOOT = '11111111-1111-1111-1111-111111111111'

    def clock(self, setup=60, offset=0):
        host_start=2_000_000_000;host_mono=10_000*recovery.NS;guest_mono=20_000*recovery.NS
        return {'schema':'durable-workflow.condition-parent-clock/v1','host_record_sha256':'d'*64,
            'host_boot_id':'22222222-2222-2222-2222-222222222222','guest_boot_id':self.BOOT,
            'host_started_unix':host_start,'host_active_unix':host_start+1740,'host_cleanup_unix':host_start+1800,
            'host_active_monotonic_ns':host_mono+1740*recovery.NS,
            'host_cleanup_monotonic_ns':host_mono+1800*recovery.NS,
            'host_before_monotonic_ns':host_mono+setup*recovery.NS,
            'host_after_monotonic_ns':host_mono+setup*recovery.NS,
            'guest_before_monotonic_ns':guest_mono+setup*recovery.NS,
            'guest_after_monotonic_ns':guest_mono+setup*recovery.NS,
            'guest_unix_ns':(host_start+setup+offset)*recovery.NS}

    def args(self, clock, start=None):
        start=clock['guest_unix_ns']//recovery.NS if start is None else start
        envelope=recovery.clock_envelope(clock,start,self.BOOT)
        raw=recovery.canonical(clock)
        return types.SimpleNamespace(started_unix=start,guest_boot_id=self.BOOT,
            parent_clock_json=raw.decode(),parent_clock_sha256=recovery.digest(raw),**envelope)

    def old_validate(self):
        source=subprocess.check_output(['git','show','df617fb:scripts/acceptance/condition-history/subject.py'],text=True)
        fn=next(node for node in ast.parse(source).body if isinstance(node,ast.FunctionDef) and node.name=='validate')
        space={'require':subject.require,'H':subject.H,'git':lambda repo,*args:
            {'HEAD':'a'*40,'refs/tags/4.12.0^{commit}':subject.H}.get(args[-1],''),
            'os':os,'pathlib':pathlib,'re':__import__('re'),'time':time}
        exec(compile(ast.Module(body=[fn],type_ignores=[]),'frozen-df617-clock','exec'),space)
        return space['validate']

    def old_args(self, started):
        return types.SimpleNamespace(mode='run',package_commit=subject.H,runner_commit='a'*40,
            checkout='/unused',work_dir='/nonexistent/sdk-rust-clock-control',run_token='0'*16,
            docker_endpoint='unix:///synthetic.sock',guest_boot_id=self.BOOT,
            started_unix=started,active_unix=started+1740,cleanup_unix=started+1800)

    def test_actual_predecessor_refuses_allowed_60s_setup_but_successor_accepts(self):
        clock=self.clock();guest_start=clock['guest_unix_ns']//recovery.NS
        old=self.old_validate();args=self.old_args(clock['host_started_unix'])
        with mock.patch.object(time,'time',return_value=guest_start),self.assertRaisesRegex(SystemExit,'stale launch'):
            old(args)
        args=self.args(clock)
        with mock.patch.object(recovery.time,'time',return_value=guest_start), \
                mock.patch.object(recovery.time,'monotonic_ns',return_value=clock['guest_after_monotonic_ns']):
            recovery.launch_clock(args)
        self.assertEqual(args.active_unix,clock['host_active_unix']-180)
        self.assertEqual(args.cleanup_unix,args.active_unix+60)
        self.assertEqual(clock['host_active_unix']-args.cleanup_unix,120)

    def test_actual_predecessor_admits_fresh_full_extension_successor_refuses(self):
        clock=self.clock();guest_start=clock['guest_unix_ns']//recovery.NS
        old=self.old_validate();old_args=self.old_args(guest_start)
        with mock.patch.object(time,'time',return_value=guest_start):old(old_args)
        self.assertGreater(old_args.cleanup_unix,clock['host_cleanup_unix'])
        args=self.args(clock);args.active_unix=old_args.active_unix;args.cleanup_unix=old_args.cleanup_unix
        with self.assertRaisesRegex(SystemExit,'parent-constrained'):recovery.launch_clock(args)

    def test_setup_and_fresh_start_never_renew_parent_monotonic_cutoffs(self):
        values=[]
        for setup in (0,60,300,900):
            clock=self.clock(setup)
            values.append(recovery.clock_envelope(clock,clock['guest_unix_ns']//recovery.NS,self.BOOT))
        self.assertEqual(len({item['active_monotonic_ns'] for item in values}),1)
        self.assertEqual(len({item['cleanup_monotonic_ns'] for item in values}),1)
        clock=self.clock(60)
        for delay in (0,10,29):
            value=recovery.clock_envelope(clock,clock['guest_unix_ns']//recovery.NS+delay,self.BOOT)
            self.assertEqual(value,values[0])

    def test_late_setup_refuses_before_storage_or_allocation(self):
        clock=self.clock(961);start=clock['guest_unix_ns']//recovery.NS
        with mock.patch.object(recovery.subprocess,'run',side_effect=AssertionError('no native operation')), \
                self.assertRaisesRegex(SystemExit,'reserve consumed'):
            recovery.clock_envelope(clock,start,self.BOOT)
        clock=self.clock(950);args=self.args(clock)
        with mock.patch.object(recovery.time,'time',return_value=args.started_unix+20), \
                mock.patch.object(recovery.time,'monotonic_ns',return_value=clock['guest_after_monotonic_ns']+20*recovery.NS), \
                self.assertRaisesRegex(SystemExit,'before launch'):recovery.launch_clock(args)

    def test_clock_offsets_cancel_and_response_delay_only_shortens(self):
        base=self.clock();start=base['guest_unix_ns']//recovery.NS
        expected=recovery.clock_envelope(base,start,self.BOOT)
        for offset in (-3600,300,3600):
            clock=self.clock(offset=offset)
            value=recovery.clock_envelope(clock,start+offset,self.BOOT)
            self.assertEqual(value['active_monotonic_ns'],expected['active_monotonic_ns'])
            self.assertEqual(value['cleanup_monotonic_ns'],expected['cleanup_monotonic_ns'])
            self.assertEqual(value['active_unix'],expected['active_unix']+offset)
        delayed=dict(base,host_after_monotonic_ns=base['host_after_monotonic_ns']+3*recovery.NS)
        value=recovery.clock_envelope(delayed,start,self.BOOT)
        self.assertEqual(value['active_unix'],expected['active_unix']-3)
        self.assertEqual(value['active_monotonic_ns'],expected['active_monotonic_ns']-3*recovery.NS)

    def test_closed_clock_authority_and_deadline_mutations_refuse(self):
        base=self.clock();start=base['guest_unix_ns']//recovery.NS
        changes=[('extra',1),('host_record_sha256','foreign'),('guest_boot_id','foreign'),
            ('host_active_unix',base['host_active_unix']+1),('host_cleanup_unix',base['host_cleanup_unix']+1),
            ('host_cleanup_monotonic_ns',base['host_cleanup_monotonic_ns']+1),
            ('host_after_monotonic_ns',base['host_before_monotonic_ns']+6*recovery.NS),
            ('guest_before_monotonic_ns',True),('guest_unix_ns',base['guest_unix_ns']-31*recovery.NS)]
        for key,value in changes:
            with self.subTest(key=key),self.assertRaises(SystemExit):
                recovery.clock_envelope(dict(base,**{key:value}),start,self.BOOT)
        args=self.args(base);args.parent_clock_sha256='0'*64
        with self.assertRaisesRegex(SystemExit,'digest mismatch'):recovery.launch_clock(args)


if __name__ == '__main__':unittest.main()
