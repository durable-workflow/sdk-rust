"""Inert evidence/refusal controls for the registry Rust loss adapter."""
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
import tempfile
import unittest

import response_loss


def table(columns, values):
    return {'columns':columns, 'rows':[[['text',value] for value in row] for row in values]}


class LossEvidenceTest(unittest.TestCase):
    def setUp(self):
        self.snapshot = {'tables':{
            'workflow_commands':table(
                ['id','workflow_instance_id','workflow_run_id','command_type','request_id'],
                [['command','workflow','run','update','update-v1:' + response_loss.hashlib.sha256(b'key').hexdigest()]]),
            'workflow_updates':table(
                ['id','workflow_command_id','workflow_instance_id','workflow_run_id','accepted_at'],
                [['update','command','workflow','run','2026-01-01 00:00:00.123000']])}}

    def identity(self, value):
        return response_loss.committed_identity(value, 'update','workflow','run','key')

    def test_before_retry_identity_joins_exact_command_run_token_and_update(self):
        self.assertEqual({'command_id':'command','run_id':'run','update_id':'update',
                          'accepted_at':'2026-01-01 00:00:00.123000'}, self.identity(self.snapshot))

    def test_absent_duplicate_or_foreign_rows_refuse_an_original_identity(self):
        for target,column in [('workflow_commands','workflow_run_id'),('workflow_commands','request_id'),
                              ('workflow_updates','workflow_command_id'),('workflow_updates','workflow_run_id')]:
            value=deepcopy(self.snapshot)
            columns=value['tables'][target]['columns']
            value['tables'][target]['rows'][0][columns.index(column)]=['text','foreign']
            with self.subTest(target=target,column=column), self.assertRaises(AssertionError):
                self.identity(value)
        for target in ('workflow_commands','workflow_updates'):
            value=deepcopy(self.snapshot)
            value['tables'][target]['rows'] *= 2
            with self.subTest(target=target), self.assertRaises(AssertionError):
                self.identity(value)

    def test_wire_timestamp_requires_zone_and_sqlite_utc_precision_is_preserved(self):
        expected=datetime(2026,1,1,0,0,0,123000,tzinfo=timezone.utc)
        self.assertEqual(expected,response_loss.timestamp('2026-01-01T00:00:00.123Z'))
        self.assertEqual(expected,response_loss.timestamp('2026-01-01 00:00:00.123000',sqlite_utc=True))
        with self.assertRaises(AssertionError):
            response_loss.timestamp('2026-01-01 00:00:00.123000')

    def test_changed_fixture_refuses_before_code_execution(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            marker=root/'executed'
            fixture=root/'fixture.py'
            fixture.write_text('open('+repr(str(marker))+',"w").write("executed")\n')
            fixture.chmod(0o600)
            with self.assertRaisesRegex(ValueError,'differs'):
                response_loss.fixture(fixture)
            self.assertFalse(marker.exists())
            link=root/'link.py';link.symlink_to(fixture)
            with self.assertRaises(OSError):
                response_loss.fixture(link)
            link.unlink();link.hardlink_to(fixture)
            with self.assertRaisesRegex(ValueError,'single-link'):
                response_loss.fixture(fixture)


if __name__ == '__main__':
    unittest.main()
