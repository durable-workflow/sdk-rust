"""Offline synthetic-copy helper for the accepted producer legacy manifest.

This module neither starts a Server nor seeds a historical producer. Callers
must stop their disposable current-producer cohort before copying its database.
"""
from copy import deepcopy
from contextlib import closing
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import stat
import time


MANIFEST_SHA256 = "da1990000c611d7377690c7526487d6415fa4f9cb42d299f59585a7c2eebf769"
MANIFEST = Path(__file__).with_name("command-key-legacy-v1-vectors.json")
MAX_DATABASE_BYTES = 64 * 1024 * 1024
MAX_ROWS = 20_000
MAX_SNAPSHOT_BYTES = 32 * 1024 * 1024
SELECTION_FIELDS = {"command_id", "command_type", "workflow_instance_id",
                    "workflow_run_id", "caller_key", "legacy_request_id"}


def manifest():
    data = MANIFEST.read_bytes()
    if hashlib.sha256(data).hexdigest() != MANIFEST_SHA256:
        raise ValueError("producer manifest identity differs")
    return json.loads(data)


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _field(value):
    if value is None:
        return ["null"]
    if isinstance(value, bytes):
        return ["blob", value.hex()]
    if isinstance(value, float):
        return ["real", value.hex()]
    if isinstance(value, int):
        return ["integer", value]
    if isinstance(value, str):
        return ["text", value]
    raise ValueError("unsupported SQLite value type")


def _identifier(value):
    return '"' + value.replace('"', '""') + '"'


def snapshot(connection):
    """Private complete logical rows/schema; publish only its digest and counts."""
    schema = connection.execute(
        "SELECT type,name,tbl_name,sql FROM sqlite_schema ORDER BY type,name"
    ).fetchall()
    result = {"schema": schema, "user_version": connection.execute("PRAGMA user_version").fetchone()[0],
              "application_id": connection.execute("PRAGMA application_id").fetchone()[0], "tables": {}}
    total = 0
    for kind, name, _table, _sql in schema:
        if kind != "table":
            continue
        cursor = connection.execute("SELECT * FROM " + _identifier(name))
        rows = cursor.fetchmany(MAX_ROWS + 1)
        total += len(rows)
        if total > MAX_ROWS:
            raise ValueError("synthetic snapshot row bound exceeded")
        encoded = sorted(([_field(value) for value in row] for row in rows), key=_json)
        result["tables"][name] = {"columns": [column[0] for column in cursor.description], "rows": encoded}
        if len(_json(result).encode()) > MAX_SNAPSHOT_BYTES:
            raise ValueError("synthetic snapshot byte bound exceeded")
    return result


def fingerprint(value):
    return hashlib.sha256(_json(value).encode()).hexdigest()


def _selections(values):
    if not isinstance(values, list) or not 1 <= len(values) <= 32:
        raise ValueError("require 1 to 32 exact admission selections")
    seen = set()
    for row in values:
        if (not isinstance(row, dict) or set(row) != SELECTION_FIELDS
                or any(type(value) is not str or not value for value in row.values())
                or row["command_type"] not in ("signal", "update")
                or len(row["caller_key"]) > 255 or len(row["legacy_request_id"]) > 255
                or row["command_id"] in seen):
            raise ValueError("invalid or duplicate admission selection")
        seen.add(row["command_id"])
    return deepcopy(values)


def _expected(before, selected):
    expected = deepcopy(before)
    commands = expected["tables"]["workflow_commands"]
    intents = expected["tables"]["dw_command_intents"]
    command_id = commands["columns"].index("id")
    request_id = commands["columns"].index("request_id")
    intent_id = intents["columns"].index("command_id")
    for row in selected:
        identity = _field(row["command_id"])
        matches = [value for value in commands["rows"] if value[command_id] == identity]
        if len(matches) != 1:
            raise ValueError("selected command identity is not unique")
        matches[0][request_id] = _field(row["legacy_request_id"])
        removed = [value for value in intents["rows"] if value[intent_id] == identity]
        if len(removed) != 1:
            raise ValueError("selected intent identity is not unique")
        intents["rows"].remove(removed[0])
    for table in expected["tables"].values():
        table["rows"].sort(key=_json)
    return expected


def _private_paths(source, destination):
    source, destination = Path(source), Path(destination)
    parent = source.parent
    if (not source.is_absolute() or not destination.is_absolute() or destination.parent != parent
            or parent.resolve() != parent or source == destination):
        raise ValueError("require distinct sibling paths in an owned private directory")
    directory = parent.lstat()
    original = source.lstat()
    if (not stat.S_ISDIR(directory.st_mode) or directory.st_uid != os.getuid()
            or stat.S_IMODE(directory.st_mode) != 0o700
            or not stat.S_ISREG(original.st_mode) or original.st_uid != os.getuid()
            or original.st_nlink != 1 or not 0 < original.st_size <= MAX_DATABASE_BYTES
            or destination.exists() or destination.is_symlink()):
        raise ValueError("database copy paths are not an owned private fixture")
    for suffix in ("-wal", "-journal"):
        sidecar = Path(str(source) + suffix)
        if sidecar.is_symlink() or (sidecar.exists() and sidecar.stat().st_size):
            raise ValueError("source must be stopped and fully checkpointed")
    return source, destination


def _stat_identity(value):
    return (value.st_dev, value.st_ino, value.st_mode, value.st_uid, value.st_nlink,
            value.st_size, value.st_mtime_ns, value.st_ctime_ns)


def _file_identity(path):
    return _stat_identity(path.stat())


def _read_fd(descriptor, expected):
    if _stat_identity(os.fstat(descriptor)) != expected:
        raise ValueError("stopped source descriptor changed")
    chunks, offset = [], 0
    while offset < expected[5]:
        chunk = os.pread(descriptor, min(1024 * 1024, expected[5] - offset), offset)
        if not chunk:
            raise ValueError("stopped source descriptor was truncated")
        chunks.append(chunk)
        offset += len(chunk)
    if _stat_identity(os.fstat(descriptor)) != expected:
        raise ValueError("stopped source descriptor changed during read")
    return b"".join(chunks)


def _memory_image(data):
    # sqlite3_deserialize requires rollback format, even for a checkpointed
    # WAL main file. This documented change affects only the private memory
    # image. The held original and its byte-for-byte checks stay unchanged.
    # https://www.sqlite.org/c3ref/deserialize.html
    if data.startswith(b"SQLite format 3\0") and data[18:20] == b"\x02\x02":
        return data[:18] + b"\x01\x01" + data[20:]
    return data


def _write_fd(descriptor, destination, identity, data):
    if not 0 < len(data) <= MAX_DATABASE_BYTES:
        raise ValueError("serialized database byte bound exceeded")
    if (_stat_identity(os.fstat(descriptor))[:5] != identity[:5]
            or _file_identity(destination)[:5] != identity[:5]):
        raise ValueError("created destination identity changed")
    offset = 0
    while offset < len(data):
        written = os.pwrite(descriptor, data[offset:offset + 1024 * 1024], offset)
        if written <= 0:
            raise ValueError("created destination write made no progress")
        offset += written
    os.ftruncate(descriptor, len(data))
    os.fsync(descriptor)
    if (_stat_identity(os.fstat(descriptor))[:5] != identity[:5]
            or _file_identity(destination) != _stat_identity(os.fstat(descriptor))):
        raise ValueError("created destination changed during write")


def transform_copy(source, destination, selections, *, stopped=False,
                   source_fd=None, on_destination_created=None):
    """Copy and transform current-producer API admissions, never a physical seed.

    `stopped=True` is the outer runner's explicit attestation. It must check its
    exact owned Server/process state first. No source database is modified.
    An optional source_fd is duplicated, never consumed or repositioned. The
    creation callback borrows the new O_RDWR FD before any database copy/use;
    callers must dup it if they retain it beyond this call. SQLite connections
    use private memory images, because even /proc/self/fd URIs are resolved and
    reopened by pathname in the native Unix VFS.
    """
    if stopped is not True:
        raise ValueError("outer runner must attest the owned Server is stopped")
    if ((source_fd is not None and (type(source_fd) is not int or source_fd < 0))
            or (on_destination_created is not None and not callable(on_destination_created))):
        raise ValueError("invalid descriptor or destination callback")
    if not hasattr(sqlite3.Connection, "deserialize") or not hasattr(os, "pread"):
        raise ValueError("held-copy support requires SQLite deserialize and positional file IO")
    document = manifest()
    selected = _selections(selections)
    source, destination = _private_paths(source, destination)
    source_descriptor = (os.dup(source_fd) if source_fd is not None else
                         os.open(source, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)))
    descriptor, identity = None, None
    try:
        source_identity = _stat_identity(os.fstat(source_descriptor))
        if source_identity != _file_identity(source):
            raise ValueError("source descriptor does not match the stopped database")
        os.pread(source_descriptor, 0, 0)  # Refuse unreadable/O_PATH descriptors before creation.
        descriptor = os.open(destination, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
        identity = _stat_identity(os.fstat(descriptor))
        if on_destination_created is not None:
            on_destination_created(descriptor)
        source_data = _read_fd(source_descriptor, source_identity)
        with closing(sqlite3.connect(":memory:", timeout=2)) as original:
            original.deserialize(_memory_image(source_data))
            with closing(sqlite3.connect(":memory:", timeout=2, isolation_level=None)) as copied:
                before = snapshot(original)
                deadline = time.monotonic() + 5

                def progress(_status, _remaining, _total):
                    if time.monotonic() > deadline:
                        raise ValueError("stopped database copy exceeded its time bound")

                original.backup(copied, pages=128, progress=progress, sleep=0.05)
                if snapshot(copied) != before or snapshot(original) != before:
                    raise ValueError("source changed or the complete copy differs")
                if (_read_fd(source_descriptor, source_identity) != source_data
                        or _file_identity(source) != source_identity):
                    raise ValueError("stopped source file changed during the copy")
                _write_fd(descriptor, destination, identity, copied.serialize())
                copied.execute("PRAGMA foreign_keys=ON")
                copied.execute("PRAGMA trusted_schema=OFF")
                copied.execute("BEGIN IMMEDIATE")
                try:
                    columns = {row[1] for row in copied.execute("PRAGMA table_info(workflow_commands)")}
                    required = {"id", "command_type", "workflow_instance_id", "workflow_run_id", "request_id"}
                    if not required.issubset(columns):
                        raise ValueError("producer command catalog lacks required columns")
                    # Validate every selection before either mutation. This is
                    # exactly the accepted manifest's legacy_key precondition.
                    for row in selected:
                        command = copied.execute(
                            "SELECT id,command_type,workflow_instance_id,workflow_run_id,request_id "
                            "FROM workflow_commands WHERE id=:command_id", row
                        ).fetchall()
                        token = row["command_type"] + "-v1:" + hashlib.sha256(row["caller_key"].encode()).hexdigest()
                        expected = [(row["command_id"], row["command_type"], row["workflow_instance_id"],
                                     row["workflow_run_id"], token)]
                        intent_count = copied.execute(
                            "SELECT count(*) FROM dw_command_intents WHERE command_id=:command_id", row
                        ).fetchone()[0]
                        if command != expected or intent_count != 1:
                            raise ValueError("API admission preconditions differ")
                    expected = _expected(before, selected)
                    for row in selected:
                        for statement in document["synthetic_legacy_shape"]["statements"]:
                            if copied.execute(statement, row).rowcount != 1:
                                raise ValueError("manifest statement did not affect exactly one row")
                    after = snapshot(copied)
                    if after != expected:
                        raise ValueError("mutation changed rows outside the accepted transform")
                    copied.execute("COMMIT")
                except BaseException:
                    copied.execute("ROLLBACK")
                    raise
                _write_fd(descriptor, destination, identity, copied.serialize())
        if _read_fd(source_descriptor, source_identity) != source_data:
            raise ValueError("stopped source changed during transform")
        return {"cell": "synthetic-legacy-shape", "manifest_sha256": MANIFEST_SHA256,
                "transformed": len(selected), "before_sha256": fingerprint(before),
                "after_sha256": fingerprint(after)}
    except BaseException:
        # Remove only the new inode this invocation created, never an existing
        # or replaced path. The source and its snapshot remain untouched.
        try:
            current = destination.lstat()
        except FileNotFoundError:
            current = None
        if identity is not None and current is not None and (current.st_dev, current.st_ino) == identity[:2]:
            destination.unlink()
        raise
    finally:
        if descriptor is not None:
            os.close(descriptor)
        os.close(source_descriptor)
