"""Source-only SQLite FD-URI native-open counterexample; no Server/SDK use."""
from contextlib import closing
import json
import os
from pathlib import Path
import sqlite3
import tempfile

with tempfile.TemporaryDirectory(prefix="sdk-rust-sqlite-open-race-") as root:
    original = Path(root) / 'data.sqlite'
    replacement = Path(str(original) + '.replacement')
    for path, value in ((original, 'original'), (replacement, 'replacement')):
        with closing(sqlite3.connect(path)) as connection:
            connection.execute('create table marker(value)')
            connection.execute('insert into marker values (?)', (value,))
            connection.commit()
    fd = os.open(original, os.O_RDONLY)
    identity = os.fstat(fd)
    try:
        os.environ['SQLITE_PROBE_TARGET'] = str(original)
        os.environ['SQLITE_PROBE_ARMED'] = '1'
        with closing(sqlite3.connect(f'file:/proc/self/fd/{fd}?mode=ro', uri=True)) as connection:
            observed = connection.execute('select value from marker').fetchone()[0]
        after = original.stat()
        held = os.fstat(fd)
        print(json.dumps({'sqlite_version': sqlite3.sqlite_version,
                          'sqlite_fd_uri_observed': observed,
                          'held_inode_unchanged': (identity.st_dev, identity.st_ino) == (held.st_dev, held.st_ino),
                          'basename_restored_to_held_inode': (after.st_dev, after.st_ino) == (held.st_dev, held.st_ino)}))
    finally:
        os.close(fd)
