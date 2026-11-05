"""Closed subject/record controls. No Docker, Cargo or network is executed."""
import ast
import copy
import hashlib
import importlib.util
import json
import os
import pathlib
import shutil
import subprocess
import sys
import tempfile
import time
import types
import unittest
from unittest import mock

HERE = pathlib.Path(__file__).resolve().parent
REPO = HERE.parents[2]

def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module

subject = load('sealed_subject', HERE / 'subject.py')
recovery = load('subject_recovery_tests', HERE / 'recover.py')


def fixture_subject(root):
    data = {p: (REPO / p).read_bytes() for p in subject.RUNTIME_FILES}
    files = {p: {'git_blob': 'a' * 40, 'mode': '100644', 'bytes': len(v), 'sha256': subject.digest(v)}
             for p, v in data.items()}
    value = {'schema': 'durable-workflow.condition-subject/v1', 'runner_commit': 'a' * 40,
             'package_commit': subject.H, 'files': files}
    sealed = subject.Subject(value, data, root / 'snapshot')
    sealed.materialize()
    return sealed


def remove_private(root):
    for parent, dirs, files in os.walk(root, followlinks=False):
        os.chmod(parent, 0o700)
        for name in dirs:
            p = pathlib.Path(parent) / name
            if not p.is_symlink():p.chmod(0o700)
    shutil.rmtree(root)


class SubjectControls(unittest.TestCase):
    def setUp(self):
        self.root = pathlib.Path(tempfile.mkdtemp()).resolve()
        self.addCleanup(remove_private, self.root)
        self.sealed = fixture_subject(self.root)

    def old_function(self, name, namespace):
        # This ancestor preserves the held f155 run.py blob and remains reachable
        # in ordinary clones after the original review branches are removed.
        old = subprocess.check_output(['git', '--no-replace-objects', '-C', str(REPO), 'show',
            '67a37f7f0e42ba63ee4e68cae3090a9ba73e1cc2:scripts/acceptance/condition-history/run.py'], text=True)
        node = next(n for n in ast.parse(old).body if isinstance(n, ast.FunctionDef) and n.name == name)
        exec(compile(ast.Module(body=[node], type_ignores=[]), 'held-f155-function', 'exec'), namespace)
        return namespace[name]

    def test_actual_held_export_function_admits_live_edit_restore(self):
        live = self.root / 'live';(live / 'fixture').mkdir(parents=True)
        source = live / 'fixture/marker';source.write_bytes(b'reviewed')
        old_export = self.old_function('export_consumer', {'shutil':shutil,'HERE':live,'INPUTS':{}})
        source.write_bytes(b'unreviewed')
        old_export(self.root / 'old-export')
        source.write_bytes(b'reviewed')
        self.assertEqual((self.root / 'old-export/marker').read_bytes(), b'unreviewed')
        output = self.root / 'new-export'
        self.sealed.export(subject.PREFIX+'fixture/', output)
        self.assertEqual((output/'tests/conditions.rs').read_bytes(),
            self.sealed.captured[subject.PREFIX+'fixture/tests/conditions.rs'])

    def test_actual_held_recovery_accepts_missing_source_and_deadline_authority(self):
        path = self.root/'old-record.json'
        path.write_text(json.dumps({'owner':recovery.PREFIX,'token':'0123456789abcdef','cells':[recovery.CELL]}))
        calls=[]
        native=types.SimpleNamespace(confined_child=lambda *a:path,Run=lambda *a,**k:None,
            recover_run=lambda *a:calls.append(a) or {'pass':True})
        old_recover=self.old_function('recover',{'json':json,'re':__import__('re'),'native':native,
            'PREFIX':recovery.PREFIX,'CELL':recovery.CELL,'require':subject.require})
        self.assertEqual(old_recover(self.root,'0123456789abcdef'),{'pass':True})
        self.assertEqual(len(calls),1)
        incomplete=json.loads(path.read_text())
        with self.assertRaises(SystemExit):self.validate(incomplete)

    def test_actual_held_summary_statement_overwrites_an_existing_handoff(self):
        old = subprocess.check_output(['git','--no-replace-objects','-C',str(REPO),'show',
            '67a37f7f0e42ba63ee4e68cae3090a9ba73e1cc2:scripts/acceptance/condition-history/run.py'],text=True)
        node=next(n for n in ast.walk(ast.parse(old)) if isinstance(n,ast.Call)
            and isinstance(n.func,ast.Attribute) and n.func.attr=='write_bytes'
            and isinstance(n.func.value,ast.BinOp) and isinstance(n.func.value.right,ast.Constant)
            and n.func.value.right.value=='summary.json')
        path=self.root/'summary.json';path.write_bytes(b'prior-handoff')
        eval(compile(ast.Expression(node),'held-f155-handoff','eval'),
            {'args':types.SimpleNamespace(work_dir=self.root),'clean':b'overwritten'})
        self.assertEqual(path.read_bytes(),b'overwritten'+b'\n')
        with self.assertRaises(FileExistsError):subject.exclusive(path,b'new')
        self.assertEqual(path.read_bytes(),b'overwritten'+b'\n')

    def test_post_preflight_live_replacement_and_restore_never_changes_export(self):
        live = self.root / 'live.rs'
        path = subject.PREFIX + 'fixture/tests/conditions.rs'
        reviewed = self.sealed.captured[path]
        live.write_bytes(reviewed)
        # Old source copy reads the changed live path and admits it.
        live.write_bytes(b'unreviewed replacement')
        old_copy = self.root / 'old-copy.rs'
        shutil.copyfile(live, old_copy)
        live.write_bytes(reviewed)
        self.assertNotEqual(old_copy.read_bytes(), reviewed)
        output = self.root / 'new-copy'
        self.sealed.export(subject.PREFIX + 'fixture/', output)
        self.assertEqual((output / 'tests/conditions.rs').read_bytes(), reviewed)

    def test_source_replacement_is_detected_before_module_use(self):
        p = self.sealed.root / (subject.PREFIX + 'engine.py')
        p.chmod(0o600)
        p.write_bytes(b'raise SystemExit("changed")');p.chmod(0o400)
        with self.assertRaisesRegex(SystemExit, 'changed'):
            self.sealed.module('engine')

    def test_extra_importable_missing_link_and_nonregular_members_refuse(self):
        for kind in ('extra', 'empty-dir', 'missing', 'symlink', 'hardlink', 'fifo'):
            root = self.root / kind
            root.mkdir()
            sealed = fixture_subject(root)
            target = sealed.root / (subject.PREFIX + 'engine.py')
            target.parent.chmod(0o700)
            if kind == 'extra':
                (target.parent / 'sitecustomize.py').write_text('raise RuntimeError()')
            elif kind == 'empty-dir':(target.parent / 'unreviewed').mkdir()
            elif kind == 'missing':target.unlink()
            elif kind == 'symlink':
                target.unlink();target.symlink_to(self.root / 'foreign')
            elif kind == 'hardlink':os.link(target, self.root / 'alias')
            else:
                target.unlink();os.mkfifo(target)
            with self.subTest(kind=kind), self.assertRaises((SystemExit, OSError)):
                sealed.verify()

    def test_captured_recovery_is_not_reopened_from_a_mutable_path(self):
        path = subject.PREFIX + 'recover.py'
        code = self.sealed.read(path)
        command = [sys.executable, '-I', '-S', '-c', code.decode()]
        target = self.sealed.root / path
        target.chmod(0o600);target.write_bytes(b'print("foreign")')
        self.assertEqual(subject.digest(command[-1].encode()), self.sealed.value['files'][path]['sha256'])
        self.assertNotEqual(command[-1].encode(), target.read_bytes())
        with self.assertRaises(SystemExit):self.sealed.read(path)

    def test_closed_loader_refuses_any_other_module(self):
        with self.assertRaises(SystemExit):self.sealed.module('sitecustomize')

    def test_exclusive_summary_handoff_rejects_existing_file_and_redirect(self):
        data = b'{"checks_outcome":"pass"}\n'
        path = self.root / 'summary.json'
        sha = subject.exclusive(path, data)
        self.assertEqual(sha, subject.digest(data))
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        with self.assertRaises(FileExistsError):subject.exclusive(path, b'changed')
        foreign = self.root / 'foreign';foreign.write_bytes(b'preserved')
        link = self.root / 'summary-link';link.symlink_to(foreign)
        with self.assertRaises(FileExistsError):subject.exclusive(link, b'changed')
        self.assertEqual(foreign.read_bytes(), b'preserved')

    def record(self):
        now = int(time.time())
        return {'schema': 'durable-workflow.condition-run/v2', 'owner': recovery.PREFIX,
            'token': '0123456789abcdef', 'cells': [recovery.CELL], 'runner_commit': 'a' * 40,
            'package_commit': subject.H, 'manifest_sha256': self.sealed.sha256,
            'recovery_sha256': self.sealed.value['files'][subject.PREFIX + 'recover.py']['sha256'],
            'started_unix': now, 'active_unix': now + 1740, 'cleanup_unix': now + 1800,
            'work_dir': str(self.root), 'docker_endpoint': 'unix:///synthetic.sock',
            'uid': os.getuid(), 'gid': os.getgid(),
            'owner_process': {'pid': 12345, 'pgid': 12345, 'sid': 12345, 'started': 'synthetic epoch'}}

    def validate(self, record, expected_sha=None):
        path = self.root / 'run.json'
        if path.exists():path.unlink()
        raw = subject.canonical(record)
        subject.exclusive(path, raw)
        return recovery.validate_record(self.root, expected_sha or subject.digest(raw), 'a' * 40,
            self.sealed.sha256, self.record()['recovery_sha256'], '0123456789abcdef')

    def test_complete_recovery_authority(self):
        record = self.record()
        self.assertEqual(self.validate(record), record)
        for field, value in [('extra', 1), ('package_commit', 'b' * 40), ('runner_commit', 'b' * 40),
            ('manifest_sha256', 'b' * 64), ('recovery_sha256', 'b' * 64), ('cleanup_unix', 0),
            ('started_unix', True), ('owner', 'other'), ('work_dir', str(self.root / 'other'))]:
            bad = copy.deepcopy(record);bad[field] = value
            with self.subTest(field=field), self.assertRaises(SystemExit):self.validate(bad)
        missing = dict(record);del missing['active_unix']
        with self.assertRaises(SystemExit):self.validate(missing)

    def test_edit_of_record_does_not_gain_authority_by_recomputing_other_fields(self):
        record = self.record();expected = subject.digest(subject.canonical(record))
        record['token'] = 'fedcba9876543210'
        with self.assertRaises(SystemExit):self.validate(record, expected)

    def test_record_links_and_wrong_modes_refuse(self):
        record = self.record();self.validate(record)
        record_sha = subject.digest(subject.canonical(record))
        path = self.root / 'run.json'
        path.chmod(0o644)
        with self.assertRaises(SystemExit):
            recovery.validate_record(self.root, record_sha, 'a' * 40,
                self.sealed.sha256, record['recovery_sha256'], record['token'])
        path.chmod(0o600);os.link(path, self.root / 'record-alias')
        with self.assertRaises(SystemExit):
            recovery.validate_record(self.root, record_sha, 'a' * 40,
                self.sealed.sha256, record['recovery_sha256'], record['token'])

    def test_foreign_owner_or_renamed_object_is_never_removed(self):
        record = self.record()
        base = recovery.PREFIX + '-' + record['token'] + '-' + recovery.CELL
        good = {'labels': {recovery.RUN: record['token'], recovery.OWNER: recovery.PREFIX,
            recovery.SUBJECT: record['manifest_sha256'], recovery.ROLE: 'consumer'},
            'name': '/' + base + '-consumer', 'id': 'a' * 64}
        for field, value in [('owner', 'foreign'), ('subject', 'b' * 64), ('name', '/foreign')]:
            frame = copy.deepcopy(good)
            if field == 'name':frame['name'] = value
            else:frame['labels'][recovery.OWNER if field == 'owner' else recovery.SUBJECT] = value
            with self.subTest(field=field), mock.patch.object(recovery, 'docker',
                    return_value=types.SimpleNamespace(returncode=0, stdout=json.dumps(frame))) as cli, self.assertRaises(SystemExit):
                recovery.owned(record, 'container', 'candidate')
            self.assertEqual(cli.call_count, 1)

    def test_changed_pid_epoch_never_kills_a_foreign_group(self):
        record = self.record();changed = dict(record['owner_process'], started='other epoch')
        with mock.patch.object(recovery, 'process_identity', return_value=changed), \
                mock.patch.object(recovery.os, 'killpg') as kill, self.assertRaises(SystemExit):
            recovery.stop_owner(record)
        kill.assert_not_called()


if __name__ == '__main__':unittest.main()
