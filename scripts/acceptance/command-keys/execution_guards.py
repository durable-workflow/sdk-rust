"""Evidence bindings for the synthetic legacy runner, with no external effects."""
import hashlib
import http.client
import json
import os
from pathlib import Path
import re
import stat
import subprocess

import sqlite_legacy


INPUT_NAMES = ("Cargo.toml", "Cargo.lock", "src/main.rs")
MAX_BINARY_BYTES = 512 * 1024 * 1024


def consumer_inputs(root):
    result = {}
    for name in INPUT_NAMES:
        descriptor = os.open(root / name, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
        try:
            result[name] = _record(descriptor)["sha256"]
        finally:
            os.close(descriptor)
    return result


def _identity(value):
    return (value.st_dev, value.st_ino, value.st_mode, value.st_uid, value.st_gid,
            value.st_nlink, value.st_size, value.st_mtime_ns, value.st_ctime_ns)


def _record(descriptor, *, executable=False):
    metadata = os.fstat(descriptor)
    if (not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.getuid()
            or metadata.st_nlink != 1 or metadata.st_mode & 0o6022
            or not 0 < metadata.st_size <= MAX_BINARY_BYTES
            or (executable and not metadata.st_mode & stat.S_IXUSR)):
        raise ValueError("consumer subject is not an owned single-link regular file")
    digest = hashlib.sha256()
    offset = 0
    while block := os.pread(descriptor, 1024 * 1024, offset):
        digest.update(block)
        offset += len(block)
    if _identity(os.fstat(descriptor)) != _identity(metadata) or offset != metadata.st_size:
        raise ValueError("consumer subject changed during hashing")
    return {"sha256": digest.hexdigest(), "size": metadata.st_size,
            "identity": list(_identity(metadata))}


class BoundExecutable:
    """Execute a held Linux descriptor, while refusing path/content substitution."""
    def __init__(self, path, expected):
        self.path = Path(path)
        if not self.path.is_absolute() or self.path.resolve() != self.path:
            raise ValueError("consumer executable must have a canonical absolute path")
        self.descriptor = os.open(self.path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
        try:
            self.record = _record(self.descriptor, executable=True)
            if self.record != expected:
                raise ValueError("consumer executable differs from its build receipt")
            self.check()
        except BaseException:
            self.close()
            raise

    def check(self):
        if (_record(self.descriptor, executable=True) != self.record
                or _identity(self.path.lstat()) != tuple(self.record["identity"])):
            raise ValueError("consumer executable identity or content changed")

    def run(self, *, input, env, timeout):
        self.check()
        try:
            return subprocess.run([f"/proc/self/fd/{self.descriptor}"], pass_fds=(self.descriptor,),
                                  input=input, text=True, capture_output=True, env=env,
                                  timeout=timeout, check=True)
        finally:
            self.check()

    def close(self):
        if self.descriptor is not None:
            os.close(self.descriptor)
            self.descriptor = None


def executable_record(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        return _record(descriptor, executable=True)
    finally:
        os.close(descriptor)


def verify_build_receipt(path, source, binary, lock, *, sdk_version, sdk_sha256, sdk_source):
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        if not 0 < os.fstat(descriptor).st_size <= 64 * 1024:
            raise ValueError("build receipt exceeds its byte bound")
        record = _record(descriptor)
        raw = os.pread(descriptor, 64 * 1024 + 1, 0)
        if (_record(descriptor) != record or len(raw) != record["size"]
                or hashlib.sha256(raw).hexdigest() != record["sha256"]
                or _identity(path.lstat()) != tuple(record["identity"])):
            raise ValueError("build receipt changed during verification")
    finally:
        os.close(descriptor)
    receipt = json.loads(raw)
    subject = reviewed_subject(source)
    built = receipt.get("subject", {})
    if (not re.fullmatch(r"[0-9a-f]{40}", built.get("commit", ""))
            or built != reviewed_subject(source, commit=built["commit"])
            or {key: value for key, value in built.items() if key != "commit"}
            != {key: value for key, value in subject.items() if key != "commit"}):
        raise ValueError("build receipt does not name the exact reviewed input subject")
    if (receipt.get("schema") != "durable-workflow.command-key-consumer-build.v1"
            or receipt.get("inputs") != subject["inputs"]
            or receipt.get("binary_path") != str(binary)
            or receipt.get("lock_sha256") != hashlib.sha256(lock.read_bytes()).hexdigest()
            or receipt.get("crate") != {"version": sdk_version, "sha256": sdk_sha256, "source": sdk_source}
            or receipt.get("build_command") != BUILD_COMMAND):
        raise ValueError("consumer build/source/lock receipt binding differs")
    return receipt, hashlib.sha256(raw).hexdigest()


BUILD_COMMAND = "rustc -V && cargo -V && cargo build --locked --manifest-path /source/Cargo.toml --target-dir /target --jobs 2"


def reviewed_subject(source, *, commit=None):
    """Bind the build recipe and its inputs to the selected immutable Git tree."""
    repository = Path(subprocess.check_output(
        ["git", "--no-replace-objects", "-C", str(source), "rev-parse", "--show-toplevel"],
        text=True, timeout=10).strip())
    if commit is None:
        commit = subprocess.check_output(
            ["git", "--no-replace-objects", "-C", str(source), "rev-parse", "HEAD^{commit}"],
            text=True, timeout=10).strip()
    hashes = {}
    paths = [source / "consumer" / name for name in INPUT_NAMES]
    paths += [source / "build_consumer.py", source / "execution_guards.py"]
    for path in paths:
        relative = path.relative_to(repository).as_posix()
        committed = subprocess.check_output(
            ["git", "--no-replace-objects", "-C", str(repository), "show", commit + ":" + relative],
            timeout=10)
        expected = hashlib.sha256(committed).hexdigest()
        if hashlib.sha256(path.read_bytes()).hexdigest() != expected:
            raise ValueError("reviewed consumer build input has an uncommitted change")
        hashes[path.relative_to(source).as_posix()] = expected
    return {"commit": commit, "inputs": {name: hashes["consumer/" + name] for name in INPUT_NAMES},
            "builder_sha256": hashes["build_consumer.py"],
            "guards_sha256": hashes["execution_guards.py"]}


def probe_json(origin, path, token, *, limit=1024 * 1024):
    """Numeric loopback only; http.client has no proxy or redirect machinery."""
    if type(limit) is not int or not 0 < limit <= 1024 * 1024:
        raise ValueError("probe byte bound is invalid")
    match = re.fullmatch(r"http://127\.0\.0\.1:([1-9][0-9]{0,4})", origin)
    if not match or not 1 <= int(match[1]) <= 65535 or path not in ("/api/ready", "/api/cluster/info"):
        raise ValueError("probe endpoint is not an exact allowed loopback route")
    port = int(match[1])
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=2)
    try:
        connection.connect()
        if connection.sock.getpeername()[:2] != ("127.0.0.1", port):
            raise ValueError("probe connected peer differs from exact loopback authority")
        connection.request("GET", path, headers={"Authorization": "Bearer " + token,
                           "X-Namespace": "default", "Connection": "close"})
        response = connection.getresponse()
        if response.status != 200:
            raise ValueError("probe status differs; redirects are refused")
        lengths = response.headers.get_all("Content-Length", [])
        encodings = response.headers.get_all("Transfer-Encoding", [])
        if (len(lengths) > 1 or len(encodings) > 1 or (lengths and encodings)
                or (encodings and encodings != ["chunked"])):
            raise ValueError("probe response framing differs")
        length = None
        if lengths:
            value = lengths[0]
            if not value.isascii() or not value.isdecimal() or str(int(value)) != value or int(value) > limit:
                raise ValueError("probe response length is invalid or over bound")
            length = int(value)
        raw = response.read(limit + 1)
        if len(raw) > limit or (length is not None and len(raw) != length):
            raise ValueError("probe body is oversized or incomplete")
        value = json.loads(raw)
        if not isinstance(value, dict):
            raise ValueError("probe JSON is not an object")
        return value
    except (http.client.HTTPException, json.JSONDecodeError) as error:
        raise ValueError("probe transport or JSON framing refused") from error
    finally:
        connection.close()


def verify_restart_baseline(before, transformed_digest):
    if sqlite_legacy.fingerprint(before) != transformed_digest:
        raise ValueError("restarted database baseline differs from transformed copy")
