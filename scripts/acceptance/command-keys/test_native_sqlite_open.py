"""Real native-open controls, with no Server, SDK, or historical seed."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


@unittest.skipUnless(sys.platform == "linux" and shutil.which("cc"), "requires Linux and a C compiler")
class NativeSqliteOpenTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.directory.cleanup)
        cls.root = Path(cls.directory.name)
        cls.source = Path(__file__).resolve().parent
        cls.preload = cls.root / "open-race.so"
        subprocess.run(["cc", "-Wall", "-Wextra", "-Werror", "-shared", "-fPIC",
                        "-o", str(cls.preload), str(cls.source / "native-open-race.c"), "-ldl"],
                       check=True, capture_output=True, timeout=30)

    def probe(self, *arguments):
        return subprocess.run([sys.executable, str(self.source / "sqlite_fd_uri_probe.py"), *arguments],
                              env={"PATH": os.defpath, "TMPDIR": str(self.root),
                                   "LD_PRELOAD": str(self.preload)},
                              text=True, capture_output=True, timeout=30, check=True)

    def test_real_file_fd_uri_control_can_reopen_a_substitute(self):
        result = self.probe()
        value = json.loads(result.stdout)
        self.assertTrue(value["held_inode_unchanged"])
        self.assertTrue(value["basename_restored_to_held_inode"])
        # A VFS that stops canonicalizing this URI may safely keep the original.
        # The fixed helper must pass independently of either VFS behavior.
        expected = "replacement" if "intercepted native pathname open" in result.stderr else "original"
        self.assertEqual(expected, value["observed"])

    def test_transform_never_reopens_the_source_path(self):
        self.check_transform("source")

    def test_transform_never_reopens_the_created_destination_path(self):
        self.check_transform("destination")

    def check_transform(self, target):
        result = self.probe("--transform", "--race-target", target)
        value = json.loads(result.stdout)
        self.assertEqual("original", value["observed"])
        self.assertTrue(value["transform"])
        self.assertTrue(value["held_inode_unchanged"])
        self.assertTrue(value["basename_restored_to_held_inode"])
        self.assertNotIn("intercepted native pathname open", result.stderr)
