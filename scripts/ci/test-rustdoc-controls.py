#!/usr/bin/env python3
"""Exercise generated-control compatibility without compiling or publishing docs."""

import importlib.util
from pathlib import Path
import tempfile
import unittest


spec = importlib.util.spec_from_file_location("rustdoc_controls", Path(__file__).with_name("prepare-rustdoc-controls.py"))
assert spec and spec.loader
controls = importlib.util.module_from_spec(spec)
spec.loader.exec_module(controls)


class BackControls(unittest.TestCase):
    def setUp(self) -> None:
        self.scratch = tempfile.TemporaryDirectory(prefix="rustdoc-controls-")
        self.addCleanup(self.scratch.cleanup)
        self.root = Path(self.scratch.name)
        (self.root / "durable_workflow").mkdir()
        (self.root / "durable_workflow/index.html").write_text("crate", encoding="utf-8")
        (self.root / "navigation.js").write_text("data-rustdoc-history-back", encoding="utf-8")
        (self.root / "__build.json").write_bytes(b'{"identity":"unchanged"}\n')
        self.html = ("<!doctype html><html><head>" + controls.NAVIGATION + "</head><body>" +
                     controls.OLD_CONTROL + "Back</a><p>Other markup</p></body></html>")
        for name in ("help.html", "settings.html"):
            (self.root / name).write_text(self.html, encoding="utf-8")

    def test_removes_script_urls_and_inline_handlers_without_other_changes(self) -> None:
        self.assertEqual(controls.prepare(self.root), 2)
        for name in ("help.html", "settings.html"):
            html = (self.root / name).read_text(encoding="utf-8")
            self.assertNotIn("javascript:", html)
            self.assertNotIn("onclick", html)
            self.assertEqual(html, self.html.replace(controls.OLD_CONTROL, controls.SAFE_CONTROL))
        self.assertEqual((self.root / "__build.json").read_bytes(), b'{"identity":"unchanged"}\n')
        self.assertEqual((self.root / "durable_workflow/index.html").read_text(), "crate")

    def test_repeat_preparation_preserves_bytes(self) -> None:
        controls.prepare(self.root)
        before = (self.root / "help.html").read_bytes()
        self.assertEqual(controls.prepare(self.root), 0)
        self.assertEqual((self.root / "help.html").read_bytes(), before)

    def test_changed_generator_shape_fails_before_mutating_either_page(self) -> None:
        (self.root / "settings.html").write_text(self.html.replace('id="back"', 'id="changed"'))
        with self.assertRaisesRegex(ValueError, "unexpected pinned rustdoc"):
            controls.prepare(self.root)
        self.assertEqual((self.root / "help.html").read_text(), self.html)

    def test_external_listener_and_fallback_are_required(self) -> None:
        for target in (self.root / "navigation.js", self.root / "durable_workflow/index.html"):
            original = target.read_bytes()
            target.unlink()
            with self.assertRaises(ValueError):
                controls.prepare(self.root)
            target.write_bytes(original)

    def test_navigation_module_is_required_once(self) -> None:
        for runtime in ("", controls.NAVIGATION * 2):
            (self.root / "settings.html").write_text(self.html.replace(controls.NAVIGATION, runtime))
            with self.assertRaisesRegex(ValueError, "runtime once"):
                controls.prepare(self.root)
            self.assertEqual((self.root / "help.html").read_text(), self.html)


if __name__ == "__main__":
    unittest.main()
