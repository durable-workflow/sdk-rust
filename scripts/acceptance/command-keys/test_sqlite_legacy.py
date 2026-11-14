"""Inert helper controls. These are not an installed or physical-upgrade cell."""
from contextlib import closing
from copy import deepcopy
import hashlib
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

import sqlite_legacy as legacy


class StoppedCopyTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.source = self.root / "source.sqlite"
        self.destination = self.root / "copy.sqlite"
        self.selection = {"command_id": "command-1", "command_type": "signal",
                          "workflow_instance_id": "workflow-1", "workflow_run_id": "run-1",
                          "caller_key": "token-key", "legacy_request_id": "token-key"}
        self.token = "signal-v1:" + hashlib.sha256(b"token-key").hexdigest()
        with closing(sqlite3.connect(self.source)) as connection:
            connection.executescript("""
                CREATE TABLE workflow_commands (
                    id TEXT PRIMARY KEY, command_type TEXT, workflow_instance_id TEXT,
                    workflow_run_id TEXT, request_id TEXT, payload BLOB, extra REAL);
                CREATE TABLE dw_command_intents (command_id TEXT UNIQUE, fingerprint TEXT);
                CREATE TABLE history (id TEXT, payload BLOB);
                INSERT INTO history VALUES ('history-1', X'00FF0102');
            """)
            connection.execute("INSERT INTO workflow_commands VALUES (?,?,?,?,?,?,?)",
                               ("command-1", "signal", "workflow-1", "run-1", self.token, b"\x00\xffvalue", 1.5))
            connection.execute("INSERT INTO dw_command_intents VALUES ('command-1','fingerprint-1')")
            connection.commit()
        self.original = self.source.read_bytes()

    def run_transform(self, selections=None, **options):
        return legacy.transform_copy(self.source, self.destination,
                                     [self.selection] if selections is None else selections,
                                     stopped=True, **options)

    def test_only_manifest_changes_apply_to_copy_and_source_bytes_stay_identical(self):
        result = self.run_transform()
        self.assertEqual("synthetic-legacy-shape", result["cell"])
        self.assertEqual(1, result["transformed"])
        self.assertNotEqual(result["before_sha256"], result["after_sha256"])
        self.assertEqual(self.original, self.source.read_bytes())
        with closing(sqlite3.connect(self.destination)) as connection:
            self.assertEqual(("token-key", b"\x00\xffvalue", 1.5), connection.execute(
                "SELECT request_id,payload,extra FROM workflow_commands").fetchone())
            self.assertEqual(0, connection.execute("SELECT count(*) FROM dw_command_intents").fetchone()[0])
            self.assertEqual(("history-1", b"\x00\xff\x01\x02"), connection.execute("SELECT * FROM history").fetchone())
        self.assertEqual(0o600, self.destination.stat().st_mode & 0o777)

    def test_every_precondition_is_checked_before_a_transform_is_retained(self):
        for field in ("command_id", "command_type", "workflow_instance_id", "workflow_run_id", "caller_key"):
            selected = dict(self.selection, **{field: "different"})
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.run_transform([selected])
            self.assertFalse(self.destination.exists())
            self.assertEqual(self.original, self.source.read_bytes())

    def test_all_selections_validate_before_either_statement(self):
        with self.assertRaises(ValueError):
            self.run_transform([self.selection, dict(self.selection, command_id="missing-command")])
        self.assertFalse(self.destination.exists())
        self.assertEqual(self.original, self.source.read_bytes())

    def test_missing_intent_refuses(self):
        with closing(sqlite3.connect(self.source)) as connection:
            connection.execute("DELETE FROM dw_command_intents")
            connection.commit()
        with self.assertRaises(ValueError):
            self.run_transform()
        self.assertFalse(self.destination.exists())

    def test_duplicate_intent_refuses(self):
        with closing(sqlite3.connect(self.source)) as connection:
            connection.executescript("""
                ALTER TABLE dw_command_intents RENAME TO old_intents;
                CREATE TABLE dw_command_intents (command_id TEXT, fingerprint TEXT);
                INSERT INTO dw_command_intents SELECT * FROM old_intents;
                INSERT INTO dw_command_intents SELECT * FROM old_intents;
                DROP TABLE old_intents;
            """)
        with self.assertRaises(ValueError):
            self.run_transform()
        self.assertFalse(self.destination.exists())

    def test_incorrect_type_column_cannot_substitute_for_producer_command_type(self):
        with closing(sqlite3.connect(self.source)) as connection:
            connection.execute("ALTER TABLE workflow_commands RENAME COLUMN command_type TO type")
            connection.commit()
        with self.assertRaisesRegex(ValueError, "producer command catalog"):
            self.run_transform()
        self.assertFalse(self.destination.exists())

    def test_duplicate_or_malformed_selection_refuses_before_copy(self):
        for selections in ([], [self.selection] * 2, [dict(self.selection, extra="unknown")],
                           [dict(self.selection, caller_key=True)], [self.selection] * 33):
            with self.subTest(selections=len(selections)), self.assertRaises(ValueError):
                self.run_transform(selections)
            self.assertFalse(self.destination.exists())

    def test_side_effecting_trigger_rolls_back_and_removes_only_the_new_copy(self):
        with closing(sqlite3.connect(self.source)) as connection:
            connection.executescript("""
                CREATE TRIGGER mutate_history AFTER UPDATE ON workflow_commands
                BEGIN UPDATE history SET payload=X'01'; END;
            """)
        original = self.source.read_bytes()
        unlink = Path.unlink
        observed = []

        def confirm_rollback(path, *args, **kwargs):
            if path == self.destination:
                with closing(sqlite3.connect(self.source)) as source, closing(sqlite3.connect(path)) as copied:
                    self.assertEqual(legacy.snapshot(source), legacy.snapshot(copied))
                observed.append(True)
            return unlink(path, *args, **kwargs)

        with patch.object(Path, "unlink", confirm_rollback):
            with self.assertRaisesRegex(ValueError, "outside the accepted transform"):
                self.run_transform()
        self.assertEqual([True], observed)
        self.assertFalse(self.destination.exists())
        self.assertEqual(original, self.source.read_bytes())

    def test_statement_failure_is_atomic(self):
        document = deepcopy(legacy.manifest())
        document["synthetic_legacy_shape"]["statements"][1] = "SELECT 1"
        with patch.object(legacy, "manifest", return_value=document):
            with self.assertRaisesRegex(ValueError, "exactly one row"):
                self.run_transform()
        self.assertFalse(self.destination.exists())
        self.assertEqual(self.original, self.source.read_bytes())

    def test_manifest_bytes_are_bound(self):
        changed = self.root / "manifest.json"
        changed.write_text("{}")
        with patch.object(legacy, "MANIFEST", changed), self.assertRaises(ValueError):
            self.run_transform()
        self.assertFalse(self.destination.exists())

    def test_stopped_attestation_is_required(self):
        for value in (False, None, 1, "true"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                legacy.transform_copy(self.source, self.destination, [self.selection], stopped=value)
        self.assertFalse(self.destination.exists())

    def test_existing_destination_is_never_overwritten(self):
        self.destination.write_bytes(b"retained owner data")
        with self.assertRaises(ValueError):
            self.run_transform()
        self.assertEqual(b"retained owner data", self.destination.read_bytes())

    def test_symlinks_hardlinks_and_uncheckpointed_wal_refuse(self):
        linked = self.root / "link.sqlite"
        linked.symlink_to(self.source)
        with self.assertRaises(ValueError):
            legacy.transform_copy(linked, self.destination, [self.selection], stopped=True)
        linked.unlink()
        linked.hardlink_to(self.source)
        with self.assertRaises(ValueError):
            self.run_transform()
        linked.unlink()
        wal = Path(str(self.source) + "-wal")
        wal.write_bytes(b"uncheckpointed")
        with self.assertRaises(ValueError):
            self.run_transform()
        self.assertFalse(self.destination.exists())

    def test_snapshot_bounds_refuse_and_remove_the_new_copy(self):
        with patch.object(legacy, "MAX_ROWS", 1), self.assertRaises(ValueError):
            self.run_transform()
        self.assertFalse(self.destination.exists())
        with patch.object(legacy, "MAX_SNAPSHOT_BYTES", 1), self.assertRaises(ValueError):
            self.run_transform()
        self.assertFalse(self.destination.exists())

    def test_held_source_and_created_destination_descriptors_keep_their_owners(self):
        source_fd = os.open(self.source, os.O_RDONLY)
        self.addCleanup(os.close, source_fd)
        os.lseek(source_fd, 7, os.SEEK_SET)
        retained = []

        def created(fd):
            self.assertEqual(0, os.fstat(fd).st_size)
            self.assertEqual(self.destination.stat().st_ino, os.fstat(fd).st_ino)
            retained.append(os.dup(fd))

        connect = sqlite3.connect

        def memory_only(name, *args, **kwargs):
            self.assertEqual(":memory:", name, "SQLite must not reopen a pathname or FD URI")
            return connect(name, *args, **kwargs)

        try:
            with patch.object(legacy.sqlite3, "connect", memory_only):
                self.run_transform(source_fd=source_fd, on_destination_created=created)
            self.assertEqual(7, os.lseek(source_fd, 0, os.SEEK_CUR))
            self.assertEqual(self.original, os.pread(source_fd, len(self.original), 0))
            self.assertEqual(self.destination.stat().st_ino, os.fstat(retained[0]).st_ino)
            with closing(connect(":memory:")) as result:
                result.deserialize(os.pread(retained[0], os.fstat(retained[0]).st_size, 0))
                self.assertEqual(("token-key",), result.execute("SELECT request_id FROM workflow_commands").fetchone())
        finally:
            for fd in retained:
                os.close(fd)

    def test_wrong_or_unreadable_source_descriptors_refuse_before_creation(self):
        foreign = self.root / "foreign"
        foreign.write_bytes(b"foreign")
        for mode in (os.O_RDONLY, os.O_WRONLY):
            fd = os.open(foreign if mode == os.O_RDONLY else self.source, mode)
            try:
                with self.subTest(mode=mode), self.assertRaises((ValueError, OSError)):
                    self.run_transform(source_fd=fd)
                self.assertFalse(self.destination.exists())
            finally:
                os.close(fd)
        for value in (True, "1", -1):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.run_transform(source_fd=value)
            self.assertFalse(self.destination.exists())

    def test_creation_callback_failure_removes_only_its_new_file(self):
        def fail(_fd):
            raise RuntimeError("callback refusal")

        with self.assertRaisesRegex(RuntimeError, "callback refusal"):
            self.run_transform(on_destination_created=fail)
        self.assertFalse(self.destination.exists())
        self.assertEqual(self.original, self.source.read_bytes())

    def test_replaced_destination_is_preserved_without_any_sqlite_path_open(self):
        moved = self.root / "created.sqlite"

        def replace(_fd):
            self.destination.rename(moved)
            self.destination.write_bytes(b"foreign replacement")

        with self.assertRaisesRegex(ValueError, "destination identity changed"):
            self.run_transform(on_destination_created=replace)
        self.assertEqual(b"foreign replacement", self.destination.read_bytes())
        self.assertEqual(self.original, self.source.read_bytes())

    def test_fully_checkpointed_wal_header_uses_the_same_logical_transform(self):
        with closing(sqlite3.connect(self.source)) as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        before = self.source.read_bytes()
        self.assertEqual(b"\x02\x02", before[18:20])
        self.run_transform()
        self.assertEqual(before, self.source.read_bytes())


if __name__ == "__main__":
    unittest.main()
