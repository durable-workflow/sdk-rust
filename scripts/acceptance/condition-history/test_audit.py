"""Final handoff/recovery controls with synthetic records and a fake daemon."""
import contextlib
import copy
import io
import json
import os
import pathlib
import tempfile
import unittest
from unittest import mock
import test_subject as controls
from test_subject import fixture_subject, recovery, remove_private, subject


class Audit(unittest.TestCase):
    record = controls.SubjectControls.record
    validate = controls.SubjectControls.validate

    def setUp(self):
        self.root = pathlib.Path(tempfile.mkdtemp()).resolve()
        self.addCleanup(remove_private, self.root)
        self.sealed = fixture_subject(self.root)
        self.authority = self.record()
        self.validate(self.authority)
        self.record_sha = subject.digest(subject.canonical(self.authority))
        self.program = self.sealed.captured[subject.PREFIX + 'recover.py'].decode()
        self.watcher = {'source_sha256': self.authority['recovery_sha256'],
                        'process': {'pid': 23456, 'pgid': 23456, 'sid': 23456, 'boot_id': '11111111-1111-1111-1111-111111111111', 'start_ticks': 234567}}
        watcher_sha = subject.exclusive(self.root / 'recovery-process.json', subject.canonical(self.watcher))
        self.summary = {'checks_outcome': 'pass', 'outcome': 'pending_independent_audit',
            'record_sha256': self.record_sha, 'runner_commit': self.authority['runner_commit'],
            'package_commit': subject.H, 'manifest_sha256': self.sealed.sha256,
            'recovery_process_sha256': watcher_sha, 'cleanup_errors': [], 'parent_clock': self.authority['parent_clock'],
            'cleanup': {'pass': True, 'errors': [],
                'remaining': {'container': [], 'network': [], 'volume': []}}}
        self.summary_sha = subject.exclusive(self.root / 'summary.json', subject.canonical(self.summary))

    def args(self, mode='audit'):
        result = [mode, '--work-dir', str(self.root), '--record-sha256', self.record_sha,
            '--manifest-sha256', self.sealed.sha256, '--runner-commit', self.authority['runner_commit'],
            '--package-commit', subject.H, '--run-token', self.authority['token']]
        if mode == 'audit':result += ['--summary-sha256', self.summary_sha]
        return result

    def invoke(self, mode='audit', objects=None, groups=None):
        with mock.patch.object(recovery.sys, 'orig_argv', ['python', '-I', '-S', '-c', self.program]), \
                mock.patch.object(recovery, 'objects', side_effect=objects or (lambda *a: [])), \
                mock.patch.object(recovery, 'group_members', side_effect=groups or (lambda *a: [])), \
                mock.patch.object(recovery, 'docker', side_effect=AssertionError('no actual daemon')), \
                contextlib.redirect_stdout(io.StringIO()):
            recovery.main(self.args(mode))

    def test_independent_audit_binds_exact_summary_and_both_processes(self):
        self.invoke()
        result = json.loads(recovery.read(self.root / 'independent-audit.json'))
        self.assertEqual(result['summary_sha256'], self.summary_sha)
        self.assertEqual(result['remaining'], {'container': [], 'network': [], 'volume': []})
        self.assertTrue(result['owner_process_absent'] and result['recovery_process_absent'])
        self.assertEqual(result['outcome'], 'pending_external_guest_teardown')
        self.assertEqual(result['storage'], self.authority['storage'])
        with self.assertRaises(FileExistsError):self.invoke()

    def test_changed_clock_cannot_gain_audit_authority_with_a_new_summary_digest(self):
        value=dict(self.summary,parent_clock=dict(self.authority['parent_clock'],host_record_sha256='f'*64))
        raw=subject.canonical(value);(self.root/'summary.json').write_bytes(raw)
        self.summary_sha=subject.digest(raw)
        with self.assertRaisesRegex(SystemExit,'summary subject'):self.invoke()
        self.assertFalse((self.root/'independent-audit.json').exists())

    def test_changed_summary_never_gains_authority(self):
        (self.root / 'summary.json').write_bytes(b'{}\n')
        with self.assertRaisesRegex(SystemExit, 'digest mismatch'):self.invoke()
        self.assertFalse((self.root / 'independent-audit.json').exists())

    def test_changed_watcher_receipt_refuses_even_if_it_claims_no_survivor(self):
        (self.root / 'recovery-process.json').write_bytes(subject.canonical(dict(self.watcher, process={})))
        with self.assertRaisesRegex(SystemExit, 'handoff changed'):self.invoke()

    def test_each_object_and_process_survivor_refuses(self):
        for kind in ('container', 'network', 'volume'):
            with self.subTest(kind=kind), self.assertRaisesRegex(SystemExit, 'object survivor'):
                self.invoke(objects=lambda record, k: ['synthetic'] if k == kind else [])
        for pgid in (12345, 23456):
            with self.subTest(pgid=pgid), self.assertRaisesRegex(SystemExit, 'process survivor'):
                self.invoke(groups=lambda group: [group] if group == pgid else [])
        self.assertFalse((self.root / 'independent-audit.json').exists())

    def test_audit_does_not_follow_redirected_summary(self):
        (self.root / 'summary.json').unlink()
        foreign = self.root / 'foreign';foreign.write_bytes(b'preserved')
        (self.root / 'summary.json').symlink_to(foreign)
        with self.assertRaises(SystemExit):self.invoke()
        self.assertEqual(foreign.read_bytes(), b'preserved')

    def test_failed_or_incomplete_owner_result_never_gains_audit_pass(self):
        for field, value in [('outcome', 'fail'), ('checks_outcome', 'fail'),
                             ('cleanup', {}), ('cleanup_errors', ['uncertain'])]:
            bad = dict(self.summary, **{field: value})
            raw = subject.canonical(bad)
            (self.root / 'summary.json').write_bytes(raw)
            self.summary_sha = subject.digest(raw)
            with self.subTest(field=field), self.assertRaisesRegex(SystemExit, 'owner checks or cleanup'):
                self.invoke()
        self.assertFalse((self.root / 'independent-audit.json').exists())

    def test_late_source_drift_cannot_cancel_captured_exact_cleanup(self):
        with mock.patch.object(recovery, 'validate_record', side_effect=[self.authority, SystemExit('drift')]), \
                mock.patch.object(recovery, 'stop_owner') as stop, \
                mock.patch.object(recovery, 'cleanup', return_value={'pass': True, 'errors': [], 'remaining': {}}) as cleanup, \
                self.assertRaisesRegex(SystemExit, 'recovery incomplete'):
            self.invoke('recover')
        stop.assert_called_once_with(self.authority)
        cleanup.assert_called_once_with(self.authority)
        result = json.loads(recovery.read(self.root / 'recovery-audit.json'))
        self.assertFalse(result['pass'])
        self.assertIn('source-or-record-drift', result['errors'])

    def test_uncertain_process_authority_still_performs_owned_daemon_cleanup(self):
        with mock.patch.object(recovery, 'stop_owner', side_effect=SystemExit('foreign epoch')), \
                mock.patch.object(recovery, 'cleanup', return_value={'pass': True, 'errors': [], 'remaining': {}}) as cleanup, \
                self.assertRaisesRegex(SystemExit, 'recovery incomplete'):
            self.invoke('recover')
        cleanup.assert_called_once_with(self.authority)
        self.assertIn('owner-process-uncertain', json.loads(recovery.read(self.root / 'recovery-audit.json'))['errors'])


if __name__ == '__main__':unittest.main()
