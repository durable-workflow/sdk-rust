"""Inert probe/artifact/baseline refusal controls; no Docker or Cargo runs."""
from contextlib import contextmanager
from copy import deepcopy
import hashlib
import http.server
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import threading
import unittest
from unittest.mock import Mock, patch

import execution_guards as guards
import sqlite_legacy


@contextmanager
def response_server(*, status=200, body=b'{"ready":true}', headers=None):
    seen = []

    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_GET(self):
            seen.append(self.path)
            self.send_response(status)
            for name, value in headers if headers is not None else [("Content-Length", str(len(body)))]:
                self.send_header(name, value)
            self.end_headers()
            self.wfile.write(body)

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
    thread.start()
    try:
        yield "http://127.0.0.1:" + str(server.server_port), seen
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


class ProbeTest(unittest.TestCase):
    def test_hostile_proxy_environment_is_not_consumed(self):
        with response_server(body=b'{"proxy":true}') as (proxy, proxy_seen):
            with response_server() as (origin, seen):
                environment = {key: proxy for key in ("http_proxy", "HTTP_PROXY", "https_proxy", "HTTPS_PROXY", "ALL_PROXY")}
                environment.update(no_proxy="", NO_PROXY="")
                with patch.dict(os.environ, environment):
                    self.assertEqual({"ready": True}, guards.probe_json(origin, "/api/ready", "synthetic"))
                self.assertEqual(["/api/ready"], seen)
                self.assertEqual([], proxy_seen)

    def test_redirect_is_refused_without_following_or_forwarding_auth(self):
        with response_server() as (destination, destination_seen):
            with response_server(status=302, body=b"", headers=[("Location", destination + "/api/ready"),
                                                               ("Content-Length", "0")]) as (origin, seen):
                with self.assertRaisesRegex(ValueError, "redirects"):
                    guards.probe_json(origin, "/api/ready", "synthetic")
                self.assertEqual(["/api/ready"], seen)
                self.assertEqual([], destination_seen)

    def test_endpoint_and_route_rejection_precedes_connection(self):
        for origin in ("http://localhost:80", "http://127.0.0.2:80", "http://127.0.0.1:0",
                       "http://127.0.0.1:65536", "http://user@127.0.0.1:80",
                       "http://127.0.0.1:80/path", "http://127.0.0.1:80?query",
                       "http://127.0.0.1:80#fragment", "https://127.0.0.1:80"):
            with self.subTest(origin=origin), patch.object(guards.http.client, "HTTPConnection") as connect:
                with self.assertRaises(ValueError):
                    guards.probe_json(origin, "/api/ready", "synthetic")
                connect.assert_not_called()
        with self.assertRaises(ValueError):
            guards.probe_json("http://127.0.0.1:80", "/api/ready?other", "synthetic")

    def test_connected_peer_must_match_before_sending_auth(self):
        connection = Mock()
        connection.sock.getpeername.return_value = ("192.0.2.1", 80)
        with patch.object(guards.http.client, "HTTPConnection", return_value=connection):
            with self.assertRaisesRegex(ValueError, "peer"):
                guards.probe_json("http://127.0.0.1:80", "/api/ready", "synthetic")
        connection.request.assert_not_called()
        connection.close.assert_called_once()

    def test_oversized_incomplete_conflicting_and_nonobject_bodies_refuse(self):
        cases = [(b'{"long":"0123456789"}', [("Content-Length", "21")], 8),
                 (b"{}", [("Content-Length", "10")], 100),
                 (b"{}", [("Content-Length", "2"), ("Content-Length", "2")], 100),
                 (b"{}", [("Content-Length", "02")], 100),
                 (b"{}", [("Content-Length", "2"), ("Transfer-Encoding", "chunked")], 100),
                 (b"[]", [("Content-Length", "2")], 100)]
        for body, headers, limit in cases:
            with self.subTest(headers=headers), response_server(body=body, headers=headers) as (origin, _seen):
                with self.assertRaises(ValueError):
                    guards.probe_json(origin, "/api/cluster/info", "synthetic", limit=limit)


class ExecutableTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.binary = self.root / "binary"
        self.binary.write_text("#!/bin/sh\nprintf 'original'\n")
        self.binary.chmod(0o700)
        self.record = guards.executable_record(self.binary)

    def test_exact_held_executable_runs_and_rechecks(self):
        bound = guards.BoundExecutable(self.binary, self.record)
        self.addCleanup(bound.close)
        result = bound.run(input="", env={"PATH": "/usr/bin:/bin"}, timeout=2)
        self.assertEqual("original", result.stdout)
        bound.check()

    def test_content_substitution_refuses(self):
        self.binary.write_text("#!/bin/sh\nprintf 'replacement'\n")
        with self.assertRaises(ValueError):
            guards.BoundExecutable(self.binary, self.record)

    def test_identical_bytes_at_a_replacement_inode_refuse(self):
        bound = guards.BoundExecutable(self.binary, self.record)
        self.addCleanup(bound.close)
        replacement = self.root / "replacement"
        replacement.write_bytes(self.binary.read_bytes())
        replacement.chmod(0o700)
        replacement.replace(self.binary)
        with self.assertRaises(ValueError):
            bound.run(input="", env={}, timeout=2)

    def test_replacement_during_launch_cannot_execute_substituted_path_or_qualify(self):
        bound = guards.BoundExecutable(self.binary, self.record)
        self.addCleanup(bound.close)
        replacement = self.root / "replacement"
        replacement.write_text("#!/bin/sh\nprintf 'replacement'\n")
        replacement.chmod(0o700)
        original_run = subprocess.run
        observed = []

        def replace_then_run(*args, **kwargs):
            replacement.replace(self.binary)
            result = original_run(*args, **kwargs)
            observed.append(result.stdout)
            return result

        with patch.object(guards.subprocess, "run", side_effect=replace_then_run):
            with self.assertRaises(ValueError):
                bound.run(input="", env={"PATH": "/usr/bin:/bin"}, timeout=2)
        self.assertEqual(["original"], observed)

    def test_symlink_hardlink_nonexecutable_and_writable_files_refuse(self):
        link = self.root / "link"
        link.symlink_to(self.binary)
        with self.assertRaises(ValueError):
            guards.BoundExecutable(link, self.record)
        link.unlink()
        link.hardlink_to(self.binary)
        with self.assertRaises(ValueError):
            guards.executable_record(self.binary)
        link.unlink()
        for mode in (0o600, 0o722, 0o4700):
            self.binary.chmod(mode)
            with self.subTest(mode=mode), self.assertRaises(ValueError):
                guards.executable_record(self.binary)


class BuildReceiptTest(unittest.TestCase):
    def setUp(self):
        ExecutableTest.setUp(self)
        here = Path(__file__).resolve().parent
        self.source = self.root / "source"
        self.source.mkdir()
        for name in ["consumer/" + name for name in guards.INPUT_NAMES] + ["build_consumer.py", "execution_guards.py"]:
            target = self.source / name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(here / name, target)
        environment = dict(os.environ, GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL="/dev/null")
        for args in (["init", "-q"], ["add", "."], ["-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid",
                     "commit", "--no-gpg-sign", "-qm", "fixture"]):
            subprocess.run(["git", "-C", str(self.source), *args], check=True, env=environment,
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.lock = self.source / "consumer/Cargo.lock"
        subject = guards.reviewed_subject(self.source)
        self.document = {"schema": "durable-workflow.command-key-consumer-build.v1",
                         "subject": subject, "inputs": subject["inputs"],
                         "binary_path": str(self.binary), "binary": self.record,
                         "lock_sha256": hashlib.sha256(self.lock.read_bytes()).hexdigest(),
                         "crate": {"version": "fixture", "sha256": "a" * 64, "source": "b" * 40},
                         "build_command": guards.BUILD_COMMAND}
        self.receipt = self.root / "receipt.json"

    def verify(self):
        self.receipt.write_text(json.dumps(self.document))
        self.receipt.chmod(0o600)
        return guards.verify_build_receipt(self.receipt, self.source, self.binary, self.lock,
            sdk_version="fixture", sdk_sha256="a" * 64, sdk_source="b" * 40)

    def test_exact_git_bound_build_receipt_accepts(self):
        receipt, digest = self.verify()
        self.assertEqual(self.document, receipt)
        self.assertEqual(hashlib.sha256(self.receipt.read_bytes()).hexdigest(), digest)

    def test_source_lock_command_and_sdk_substitution_refuse(self):
        original = deepcopy(self.document)
        for field, value in (("build_command", "different command"), ("inputs", {}),
                             ("crate", {}), ("lock_sha256", "0" * 64),
                             ("binary_path", str(self.root / "other"))):
            self.document = dict(original, **{field: value})
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.verify()
        self.document = original
        with (self.source / "consumer/src/main.rs").open("a") as output:
            output.write("\n// altered input\n")
        with self.assertRaises(ValueError):
            self.verify()


class BaselineTest(unittest.TestCase):
    def test_restart_must_preserve_the_transformed_logical_fingerprint(self):
        transformed = {"tables": {"commands": ["retained identity"]}}
        digest = sqlite_legacy.fingerprint(transformed)
        guards.verify_restart_baseline(transformed, digest)
        with self.assertRaisesRegex(ValueError, "restarted database"):
            guards.verify_restart_baseline({"tables": {"commands": ["rewritten identity"]}}, digest)


if __name__ == "__main__":
    unittest.main()
