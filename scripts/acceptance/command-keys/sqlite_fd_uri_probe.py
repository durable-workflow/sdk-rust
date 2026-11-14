"""Source-only SQLite FD-URI native-open counterexample; no Server/SDK use."""
from contextlib import closing
import argparse
import hashlib
import inspect
import json
import os
from pathlib import Path
import sqlite3
import tempfile

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--transform', action='store_true')
parser.add_argument('--race-target', choices=('source', 'destination'), default='source')
options = parser.parse_args()

with tempfile.TemporaryDirectory(prefix="sdk-rust-sqlite-open-race-") as root:
    original = Path(root) / 'data.sqlite'
    destination = Path(root) / 'copy.sqlite'
    target = destination if options.race_target == 'destination' else original
    replacement = Path(str(target) + '.replacement')
    for path, value in ((original, 'original'), (replacement, 'replacement')):
        with closing(sqlite3.connect(path)) as connection:
            connection.execute('create table marker(value)')
            connection.execute('insert into marker values (?)', (value,))
            if options.transform:
                connection.executescript('''
                    CREATE TABLE workflow_commands (id TEXT, command_type TEXT,
                        workflow_instance_id TEXT, workflow_run_id TEXT, request_id TEXT);
                    CREATE TABLE dw_command_intents (command_id TEXT);
                ''')
                connection.execute('INSERT INTO workflow_commands VALUES (?,?,?,?,?)',
                    ('command-1', 'signal', 'workflow-1', 'run-1',
                     'signal-v1:' + hashlib.sha256(b'caller-key').hexdigest()))
                connection.execute("INSERT INTO dw_command_intents VALUES ('command-1')")
            connection.commit()
    fd = os.open(original, os.O_RDONLY)
    identity = os.fstat(fd)
    retained = []
    try:
        os.environ['SQLITE_PROBE_TARGET'] = str(target)
        os.environ['SQLITE_PROBE_ARMED'] = '1'
        if options.transform:
            import sqlite_legacy as legacy

            def created(descriptor):
                assert os.fstat(descriptor).st_size == 0
                retained.append(os.dup(descriptor))

            selection = {'command_id': 'command-1', 'command_type': 'signal',
                         'workflow_instance_id': 'workflow-1', 'workflow_run_id': 'run-1',
                         'caller_key': 'caller-key', 'legacy_request_id': 'caller-key'}
            extra = ({'source_fd': fd, 'on_destination_created': created}
                     if 'source_fd' in inspect.signature(legacy.transform_copy).parameters else {})
            before = os.pread(fd, identity.st_size, 0)
            legacy.transform_copy(original, destination, [selection], stopped=True, **extra)
            os.environ.pop('SQLITE_PROBE_ARMED', None)
            assert len(retained) == 1
            with closing(sqlite3.connect(':memory:')) as result:
                result.deserialize(os.pread(retained[0], os.fstat(retained[0]).st_size, 0))
                observed = result.execute('select value from marker').fetchone()[0]
                assert result.execute('select request_id from workflow_commands').fetchone() == ('caller-key',)
                assert result.execute('select count(*) from dw_command_intents').fetchone() == (0,)
            assert os.pread(fd, identity.st_size, 0) == before
        else:
            with closing(sqlite3.connect(f'file:/proc/self/fd/{fd}?mode=ro', uri=True)) as connection:
                observed = connection.execute('select value from marker').fetchone()[0]
        after = original.stat()
        held = os.fstat(fd)
        print(json.dumps({'sqlite_version': sqlite3.sqlite_version,
                          'observed': observed, 'transform': options.transform,
                          'held_inode_unchanged': (identity.st_dev, identity.st_ino) == (held.st_dev, held.st_ino),
                          'basename_restored_to_held_inode': (after.st_dev, after.st_ino) == (held.st_dev, held.st_ino)}))
    finally:
        for descriptor in retained:
            os.close(descriptor)
        os.close(fd)
