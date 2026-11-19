"""Held-descriptor controls for the stopped synthetic copy helper.

These are inert helper controls, not an installed or physical-upgrade cell.
The native interposer control needs a Linux C compiler and is skipped without
one; every other control is stdlib-only.
"""
from contextlib import closing
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import sqlite_legacy as legacy


HERE = Path(__file__).resolve().parent
TOKEN = "signal-v1:" + hashlib.sha256(b"token-key").hexdigest()
SELECTION = {"command_id": "command-1", "command_type": "signal",
             "workflow_instance_id": "workflow-1", "workflow_run_id": "run-1",
             "caller_key": "token-key", "legacy_request_id": "token-key"}


def build_database(path, marker, *, wal=False):
    with closing(sqlite3.connect(path)) as connection:
        if wal:
            connection.execute("PRAGMA journal_mode=WAL")
        connection.executescript("""
            CREATE TABLE workflow_commands (
                id TEXT PRIMARY KEY, command_type TEXT, workflow_instance_id TEXT,
                workflow_run_id TEXT, request_id TEXT, payload BLOB, extra REAL);
            CREATE TABLE dw_command_intents (command_id TEXT UNIQUE, fingerprint TEXT);
            CREATE TABLE history (id TEXT, payload BLOB);
        """)
        connection.execute("INSERT INTO history VALUES ('marker', ?)", (marker,))
        connection.execute("INSERT INTO workflow_commands VALUES (?,?,?,?,?,?,?)",
                           ("command-1", "signal", "workflow-1", "run-1", TOKEN, b"\x00\xffvalue", 1.5))
        connection.execute("INSERT INTO dw_command_intents VALUES ('command-1','fingerprint-1')")
        connection.commit()
        if wal:
            connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")


def marker(path):
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as connection:
        return connection.execute("SELECT payload FROM history WHERE id='marker'").fetchone()[0]


def snapshot_digest(path):
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as connection:
        return legacy.fingerprint(legacy.snapshot(connection))


class HeldDescriptorTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.source = self.root / "source.sqlite"
        self.destination = self.root / "copy.sqlite"
        self.replacement = self.root / "replacement.sqlite"
        build_database(self.source, "original")
        build_database(self.replacement, "replacement")
        self.original = self.source.read_bytes()
        self.before = snapshot_digest(self.source)

    def held(self, path=None, flags=os.O_RDONLY):
        descriptor = os.open(path or self.source, flags | os.O_CLOEXEC)
        self.addCleanup(self._close, descriptor)
        return descriptor

    @staticmethod
    def _close(descriptor):
        try:
            os.close(descriptor)
        except OSError:
            pass

    def run_transform(self, **options):
        return legacy.transform_copy(self.source, self.destination, [SELECTION], stopped=True, **options)

    def assert_untouched(self):
        self.assertFalse(self.destination.exists())
        self.assertEqual(self.original, self.source.read_bytes())

    def test_caller_held_source_is_used_and_stays_caller_owned(self):
        descriptor = self.held()
        os.lseek(descriptor, 7, os.SEEK_SET)
        result = self.run_transform(source_fd=descriptor)
        self.assertEqual(self.before, result["before_sha256"])
        self.assertEqual(7, os.lseek(descriptor, 0, os.SEEK_CUR))
        os.fstat(descriptor)  # still open
        self.assertEqual("original", marker(self.destination))
        self.assertEqual(self.original, self.source.read_bytes())
        with closing(sqlite3.connect(self.destination)) as connection:
            self.assertEqual("token-key", connection.execute(
                "SELECT request_id FROM workflow_commands").fetchone()[0])
            self.assertEqual(0, connection.execute("SELECT count(*) FROM dw_command_intents").fetchone()[0])

    def test_sqlite_never_opens_a_pathname(self):
        connect = sqlite3.connect
        opened = []

        def memory_only(database, *args, **kwargs):
            opened.append(database)
            if database != ":memory:":
                raise AssertionError("SQLite opened a pathname: %r" % (database,))
            return connect(database, *args, **kwargs)

        for options in ({}, {"source_fd": self.held()}):
            with self.subTest(held=bool(options)), patch.object(legacy.sqlite3, "connect", memory_only):
                self.run_transform(**options)
            self.destination.unlink()
        self.assertTrue(opened)
        self.assertEqual({":memory:"}, set(opened))

    def test_wrong_descriptors_refuse_before_destination_creation(self):
        directory = self.held(self.root, os.O_RDONLY | os.O_DIRECTORY)
        read_end, write_end = os.pipe()
        self.addCleanup(self._close, read_end)
        self.addCleanup(self._close, write_end)
        closed = os.open(self.source, os.O_RDONLY)
        os.close(closed)
        wrong = {
            "other file": self.held(self.replacement),
            "directory": directory,
            "write-only": self.held(flags=os.O_WRONLY),
            "pipe": read_end,
            "closed": closed,
            "negative": -1,
            "bool": True,
            "string": str(self.source),
        }
        if hasattr(os, "O_PATH"):
            wrong["O_PATH"] = self.held(flags=os.O_PATH)
        for name, descriptor in wrong.items():
            with self.subTest(descriptor=name), self.assertRaises(ValueError):
                self.run_transform(source_fd=descriptor)
            self.assert_untouched()

    def test_source_replaced_before_the_call_refuses(self):
        descriptor = self.held()
        os.rename(self.source, self.root / "aside.sqlite")
        os.rename(self.replacement, self.source)
        with self.assertRaisesRegex(ValueError, "does not hold the owned source"):
            self.run_transform(source_fd=descriptor)
        self.assertFalse(self.destination.exists())

    def test_swap_away_and_back_during_the_copy_cannot_substitute_the_source(self):
        saved = self.root / "saved.sqlite"

        def swap_and_restore(_descriptor):
            os.rename(self.source, saved)
            os.rename(self.replacement, self.source)
            try:
                self.assertEqual("replacement", marker(self.source))
            finally:
                os.rename(self.source, self.replacement)
                os.rename(saved, self.source)

        # The replacement is never read. Renaming the held inode normally
        # changes its ctime, which the stopped-source identity check refuses;
        # where it does not, the copy is still the held original.
        for options in ({}, {"source_fd": self.held()}):
            with self.subTest(held=bool(options)):
                try:
                    result = self.run_transform(on_destination_created=swap_and_restore, **options)
                except ValueError as error:
                    self.assertIn("stopped source file changed", str(error))
                    self.assert_untouched()
                else:
                    self.assertEqual(self.before, result["before_sha256"])
                    self.assertEqual("original", marker(self.destination))
                    self.destination.unlink()

    def test_persistent_source_replacement_during_the_copy_refuses(self):
        def replace(_descriptor):
            os.rename(self.source, self.root / "aside.sqlite")
            os.rename(self.replacement, self.source)

        with self.assertRaisesRegex(ValueError, "stopped source file changed"):
            self.run_transform(source_fd=self.held(), on_destination_created=replace)
        self.assertFalse(self.destination.exists())

    def test_destination_descriptor_is_exposed_at_creation_and_retained(self):
        retained = []

        def created(descriptor):
            value = os.fstat(descriptor)
            self.assertEqual((value.st_dev, value.st_ino),
                             (self.destination.lstat().st_dev, self.destination.lstat().st_ino))
            self.assertEqual(0, value.st_size)
            self.assertEqual(0o600, value.st_mode & 0o777)
            self.assertEqual(os.O_RDWR, fcntl.fcntl(descriptor, fcntl.F_GETFL) & os.O_ACCMODE)
            retained.append(os.dup(descriptor))

        self.run_transform(on_destination_created=created)
        self.assertEqual(1, len(retained))
        self.addCleanup(os.close, retained[0])
        written = self.destination.read_bytes()
        self.assertEqual(written, os.pread(retained[0], len(written) + 1, 0))
        value = os.fstat(retained[0])
        self.assertEqual((value.st_dev, value.st_ino),
                         (self.destination.lstat().st_dev, self.destination.lstat().st_ino))

    def test_failing_creation_callback_removes_only_the_new_copy(self):
        def refuse(_descriptor):
            raise RuntimeError("caller refused the inode")

        with self.assertRaisesRegex(RuntimeError, "caller refused"):
            self.run_transform(on_destination_created=refuse)
        self.assert_untouched()
        with self.assertRaises(ValueError):
            self.run_transform(on_destination_created="not callable")
        self.assert_untouched()

    def test_foreign_destination_replacement_is_preserved(self):
        aside = self.root / "created-aside.sqlite"

        def substitute(_descriptor):
            os.rename(self.destination, aside)
            self.destination.write_bytes(b"foreign owner data")

        with self.assertRaisesRegex(ValueError, "no longer holds the created copy"):
            self.run_transform(on_destination_created=substitute)
        self.assertEqual(b"foreign owner data", self.destination.read_bytes())
        self.assertTrue(aside.exists())
        self.assertEqual(self.original, self.source.read_bytes())

    def test_checkpointed_wal_source_copies_through_the_held_descriptor(self):
        self.source.unlink()
        build_database(self.source, "original", wal=True)
        for suffix in ("-wal", "-shm"):
            Path(str(self.source) + suffix).unlink(missing_ok=True)
        original = self.source.read_bytes()
        self.assertEqual(b"\x02\x02", original[18:20])
        before = snapshot_digest(self.source)
        result = self.run_transform(source_fd=self.held())
        self.assertEqual(before, result["before_sha256"])
        self.assertEqual(original, self.source.read_bytes())
        self.assertEqual(b"\x02\x02", self.destination.read_bytes()[18:20])
        with closing(sqlite3.connect(self.destination)) as connection:
            self.assertEqual("wal", connection.execute("PRAGMA journal_mode").fetchone()[0])
            self.assertEqual("original", connection.execute(
                "SELECT payload FROM history WHERE id='marker'").fetchone()[0])

    def test_uncheckpointed_wal_refuses_even_with_a_held_descriptor(self):
        descriptor = self.held()
        Path(str(self.source) + "-wal").write_bytes(b"uncheckpointed")
        with self.assertRaisesRegex(ValueError, "checkpointed"):
            self.run_transform(source_fd=descriptor)
        self.assertFalse(self.destination.exists())

    def test_non_sqlite_image_refuses_and_removes_the_new_copy(self):
        for image in (b"not a database" * 10, self.original[:18] + b"\x03\x03" + self.original[20:]):
            self.source.write_bytes(image)
            with self.subTest(image=image[:20]), self.assertRaises(ValueError):
                self.run_transform(source_fd=self.held())
            self.assertFalse(self.destination.exists())


@unittest.skipUnless(sys.platform.startswith("linux") and shutil.which("cc"),
                     "native interposer control needs Linux and a C compiler")
class NativeOpenInterposerTest(unittest.TestCase):
    """The interposer swaps a replacement in only around a native open() of the
    exact source pathname and restores the original before returning."""

    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        build_root = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, build_root)
        self.library = build_root / "native-open-race.so"
        subprocess.run(["cc", "-shared", "-fPIC", "-O2", "-o", str(self.library),
                        str(HERE / "native-open-race.c"), "-ldl"], check=True, timeout=60)
        self.source = self.root / "source.sqlite"
        self.destination = self.root / "copy.sqlite"
        build_database(self.source, "original")
        build_database(Path(str(self.source) + ".replacement"), "replacement")

    def run_child(self, code, *arguments):
        environment = {"PATH": os.environ.get("PATH", ""), "LD_PRELOAD": str(self.library)}
        return subprocess.run([sys.executable, "-I", "-B", "-c", code, str(HERE), *arguments],
                              env=environment, capture_output=True, text=True, timeout=60)

    def test_positive_control_fd_uri_reopen_reads_the_replacement(self):
        child = subprocess.run([sys.executable, "-I", "-B", str(HERE / "sqlite_fd_uri_probe.py")],
                               env={"PATH": os.environ.get("PATH", ""), "LD_PRELOAD": str(self.library)},
                               capture_output=True, text=True, timeout=60)
        self.assertEqual(0, child.returncode, child.stderr)
        self.assertIn("intercepted native pathname open", child.stderr)
        observed = json.loads(child.stdout)
        self.assertEqual("replacement", observed["sqlite_fd_uri_observed"])
        self.assertTrue(observed["held_inode_unchanged"])
        self.assertTrue(observed["basename_restored_to_held_inode"])

    CHILD = """
import json, os, sqlite3, sys
from contextlib import closing
from pathlib import Path
sys.path.insert(0, sys.argv[1])
import sqlite_legacy
source, destination, mode = Path(sys.argv[2]), Path(sys.argv[3]), sys.argv[4]
selection = json.loads(sys.argv[5])
options = {}
if mode == "held":
    options["source_fd"] = os.open(source, os.O_RDONLY | os.O_CLOEXEC)
os.environ["SQLITE_PROBE_TARGET"] = str(source)
os.environ["SQLITE_PROBE_ARMED"] = "1"
try:
    sqlite_legacy.transform_copy(source, destination, [selection], stopped=True, **options)
except ValueError as error:
    print(json.dumps({"refused": str(error)}))
    sys.exit(0)
with closing(sqlite3.connect(destination)) as connection:
    value = connection.execute("SELECT payload FROM history WHERE id='marker'").fetchone()[0]
print(json.dumps({"observed": value}))
"""

    def test_held_descriptor_copy_never_reaches_a_native_source_open(self):
        child = self.run_child(self.CHILD, str(self.source), str(self.destination), "held", json.dumps(SELECTION))
        self.assertEqual(0, child.returncode, child.stderr)
        self.assertNotIn("intercepted", child.stderr)
        self.assertEqual({"observed": "original"}, json.loads(child.stdout))

    def test_helper_owned_open_that_is_substituted_refuses(self):
        child = self.run_child(self.CHILD, str(self.source), str(self.destination), "owned", json.dumps(SELECTION))
        self.assertEqual(0, child.returncode, child.stderr)
        self.assertIn("intercepted native pathname open", child.stderr)
        self.assertIn("refused", json.loads(child.stdout))
        self.assertFalse(self.destination.exists())
        self.assertEqual("original", marker(self.source))


if __name__ == "__main__":
    unittest.main()
