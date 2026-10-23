#!/usr/bin/env python3
"""Focused tests for release-note and crate source identity verification."""

from __future__ import annotations

import hashlib
import io
import json
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import unittest

from release_package import (
    ReleasePackageError,
    load_package,
    release_entry,
    render_release_notes,
    restore_missing_lockfile,
    verify_archive,
    verify_source,
)


ROOT = Path(__file__).resolve().parents[2]
MANIFEST = ROOT / "Cargo.toml"
CHANGELOG = ROOT / "CHANGELOG.md"
REQUIREMENTS = ROOT / "scripts/ci/release-tooling-requirements.txt"
COMMIT = "0123456789abcdef0123456789abcdef01234567"


class ReleasePackageTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="release-package-test-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.package = load_package(MANIFEST)

    def test_rendered_notes_preserve_only_the_exact_current_markdown(self) -> None:
        changelog = b"""# Changelog

## Unreleased

- Future feature.

## 2.0.1

- Current **metrics** and [setup](README.md).

```text
## 9.9.9
This is an example, not another release.
```

## 2.0.0

- Previous pagination feature.
"""
        expected = """## 2.0.1

- Current **metrics** and [setup](README.md).

```text
## 9.9.9
This is an example, not another release.
```
"""
        self.assertEqual(expected, render_release_notes(changelog, "2.0.1"))

    def test_rendered_notes_reject_missing_empty_and_duplicate_version(self) -> None:
        for changelog in (
            b"## 2.0.0\n\n- Previous release.\n",
            b"## 2.0.1\n\n<!-- empty -->\n\n## 2.0.0\n\n- Previous.\n",
            b"## 2.0.1\n\n- One.\n\n## 2.0.1\n\n- Two.\n",
        ):
            with self.subTest(changelog=changelog), self.assertRaises(ReleasePackageError):
                render_release_notes(changelog, "2.0.1")

    def test_release_notes_cli_binds_output_to_manifest_version(self) -> None:
        manifest = self.root / "Cargo.toml"
        manifest.write_text('[package]\nname="example"\nversion="2.0.1"\n', encoding="utf-8")
        changelog = self.root / "CHANGELOG.md"
        changelog.write_text('## 2.0.1\n\n- Current metrics.\n\n## 2.0.0\n\n- Previous feature.\n', encoding="utf-8")
        command = [sys.executable, str(ROOT / "scripts/ci/release_package.py"),
                   "--manifest", str(manifest), "--changelog", str(changelog), "--release-notes"]
        result = subprocess.run([*command, "2.0.1"], text=True, capture_output=True, check=True)
        self.assertEqual("## 2.0.1\n\n- Current metrics.\n", result.stdout)
        mismatch = subprocess.run([*command, "2.0.0"], text=True, capture_output=True)
        self.assertNotEqual(0, mismatch.returncode)
        self.assertEqual("", mismatch.stdout)
        self.assertIn("differs from Cargo package version", mismatch.stderr)

    def test_release_notes_cli_refuses_archive_mode(self) -> None:
        result = subprocess.run(
            [sys.executable, str(ROOT / "scripts/ci/release_package.py"),
             "--release-notes", self.package["version"], "--archive", "example.crate"],
            text=True, capture_output=True,
        )
        self.assertEqual(2, result.returncode)
        self.assertEqual("", result.stdout)
        self.assertIn("cannot be combined", result.stderr)

    def archive(
        self,
        *,
        changelog: bytes | None = None,
        commit: str = COMMIT,
        dirty: bool | None = None,
        name: str = "candidate.crate",
        lockfile: bytes | None = None,
        lockfile_kind: bytes = tarfile.REGTYPE,
        lockfile_copies: int = 1,
    ) -> Path:
        path = self.root / name
        root = f"{self.package['name']}-{self.package['version']}"
        entries = {
            f"{root}/CHANGELOG.md": changelog or CHANGELOG.read_bytes(),
            f"{root}/.cargo_vcs_info.json": json.dumps(
                {
                    "git": {
                        "sha1": commit,
                        **({"dirty": dirty} if dirty is not None else {}),
                    }
                }
            ).encode(),
        }
        with tarfile.open(path, "w:gz") as archive:
            for name, content in entries.items():
                member = tarfile.TarInfo(name)
                member.size = len(content)
                archive.addfile(member, io.BytesIO(content))
            content = lockfile if lockfile is not None else (ROOT / "Cargo.lock").read_bytes()
            for _ in range(lockfile_copies):
                member = tarfile.TarInfo(f"{root}/Cargo.lock")
                member.type = lockfile_kind
                if member.isfile():
                    member.size = len(content)
                    archive.addfile(member, io.BytesIO(content))
                else:
                    member.linkname = "outside-lockfile"
                    archive.addfile(member)
        return path

    def restore(self, archive: Path, *, checksum: str | None = None, commit: str = COMMIT) -> dict:
        manifest = self.root / "Cargo.toml"
        changelog = self.root / "CHANGELOG.md"
        manifest.write_bytes(MANIFEST.read_bytes())
        changelog.write_bytes(CHANGELOG.read_bytes())
        return restore_missing_lockfile(
            archive, manifest, changelog, commit,
            checksum if checksum is not None else hashlib.sha256(archive.read_bytes()).hexdigest(),
        )

    def test_restore_verified_registry_lockfile_exactly(self) -> None:
        identity = self.restore(self.archive())
        self.assertEqual((ROOT / "Cargo.lock").read_bytes(), (self.root / "Cargo.lock").read_bytes())
        self.assertEqual("verified_registry_archive", identity["lockfile"]["source"])

    def test_restore_refuses_wrong_archive_or_source_before_writing(self) -> None:
        archive = self.archive()
        for options in ({"checksum": "0" * 64}, {"commit": "f" * 40}):
            with self.subTest(options=options):
                with self.assertRaises(ReleasePackageError):
                    self.restore(archive, **options)
                self.assertFalse((self.root / "Cargo.lock").exists())

    def test_restore_refuses_missing_duplicate_or_nonregular_lock(self) -> None:
        cases = (
            {"lockfile_copies": 0}, {"lockfile_copies": 2},
            {"lockfile_kind": tarfile.SYMTYPE}, {"lockfile_kind": tarfile.LNKTYPE},
        )
        for options in cases:
            with self.subTest(options=options):
                archive = self.archive(**options)
                with self.assertRaises(ReleasePackageError):
                    self.restore(archive)
                self.assertFalse((self.root / "Cargo.lock").exists())

    def test_restore_refuses_unbounded_or_malformed_lock(self) -> None:
        for content in (
            b" " * (1024 * 1024 + 1), b"\xff", b"not TOML", b"version = 4\n",
            b"version = [4]\npackage = []\n", b"version = 4.0\npackage = []\n",
        ):
            with self.subTest(size=len(content)):
                archive = self.archive(lockfile=content)
                with self.assertRaises(ReleasePackageError):
                    self.restore(archive)
                self.assertFalse((self.root / "Cargo.lock").exists())

    def test_restore_never_replaces_existing_file_or_symlink(self) -> None:
        target = self.root / "Cargo.lock"
        target.write_bytes(b"existing lock")
        archive = self.archive()
        with self.assertRaises(ReleasePackageError):
            self.restore(archive)
        self.assertEqual(b"existing lock", target.read_bytes())
        target.unlink()
        other = self.root / "other"
        other.write_bytes(b"other file")
        target.symlink_to(other)
        with self.assertRaises(ReleasePackageError):
            self.restore(archive)
        self.assertTrue(target.is_symlink())
        self.assertEqual(b"other file", other.read_bytes())

    def test_current_source_has_one_nonempty_current_version_entry(self) -> None:
        identity = verify_source(MANIFEST, CHANGELOG)
        self.assertEqual(self.package["version"], identity["package_version"])
        self.assertGreater(identity["changelog"]["content_lines"], 0)

    def test_release_parser_dependencies_are_hash_locked_wheels(self) -> None:
        requirements = REQUIREMENTS.read_text(encoding="utf-8")
        self.assertIn("markdown-it-py==4.2.0", requirements)
        self.assertIn("mdurl==0.1.2", requirements)
        self.assertEqual(2, requirements.count("--hash=sha256:"))

    def test_release_entry_accepts_top_level_atx_and_setext_headings(self) -> None:
        version = str(self.package["version"])
        cases = {
            "normalized-atx": (
                f"# Changes\n\n## **{version}**\n\n- One change.\n"
            ),
            "normalized-link-atx": (
                f"# Changes\n\n## [{version}](https://example.invalid)\n\n"
                "- One change.\n"
            ),
            "dated-atx": (
                f"# Changes\n\n## [{version}] - 2026-08-26\n\n"
                "- One change.\n"
            ),
            "setext": (
                f"# Changes\n\n{version}\n----------------\n\n"
                "- One change.\n"
            ),
            "visible-after-raw-html": (
                f"# Changes\n\n<pre>\n## {version}\n</pre>\n\n"
                f"## {version}\n\n- One visible change.\n"
            ),
            "fenced-code-content": (
                f"# Changes\n\n## {version}\n\n"
                "```text\nOne visible change.\n```\n"
            ),
            "indented-code-content": (
                f"# Changes\n\n## {version}\n\n"
                "    One visible change.\n"
            ),
            "image-alt-content": (
                f"# Changes\n\n## {version}\n\n"
                "![One visible change.](change.png)\n"
            ),
        }
        for name, changelog in cases.items():
            with self.subTest(name=name):
                identity = release_entry(changelog.encode(), version)
                self.assertGreater(identity["content_lines"], 0)

    def test_release_entry_rejects_nonsemantic_or_nested_heading_corpus(self) -> None:
        version = str(self.package["version"])
        hidden_note = f"## {version}\n\n- Hidden note.\n"
        cases = {
            "html-comment": (
                f"# Changes\n\n<!--\n{hidden_note}-->\n"
            ),
            "fenced-code": (
                f"# Changes\n\n```markdown\n{hidden_note}```\n"
            ),
            "tilde-fenced-code": (
                f"# Changes\n\n~~~~markdown\n{hidden_note}~~~~\n"
            ),
            "indented-code": (
                f"# Changes\n\n    ## {version}\n\n    - Hidden note.\n"
            ),
            "html-type-1": (
                f"# Changes\n\n<pre>\n{hidden_note}</pre>\n"
            ),
            "html-type-3": (
                f"# Changes\n\n<?release\n{hidden_note}?>\n"
            ),
            "html-type-4": (
                f"# Changes\n\n<!RELEASE\n{hidden_note}>\n"
            ),
            "html-type-5": (
                f"# Changes\n\n<![CDATA[\n{hidden_note}]]>\n"
            ),
            "html-type-6": (
                f"# Changes\n\n<div>\n{hidden_note}</div>\n"
            ),
            "html-type-7-after-thematic-break": (
                f"# Changes\n\n---\n<release-notes>\n{hidden_note}"
                "</release-notes>\n"
            ),
            "html-type-7-after-setext": (
                f"# Changes\n\nRelease notes\n=============\n"
                f"<release-notes>\n{hidden_note}</release-notes>\n"
            ),
            "html-type-7-after-indented-code": (
                "# Changes\n\n    release note example\n"
                f"<release-notes>\n{hidden_note}</release-notes>\n"
            ),
            "html-type-7-after-blockquote": (
                "# Changes\n\n> # Release note example\n"
                f"<release-notes>\n{hidden_note}</release-notes>\n"
            ),
            "html-type-7-after-list-heading": (
                "# Changes\n\n- # Release note example\n"
                f"<release-notes>\n{hidden_note}</release-notes>\n"
            ),
            "html-type-7-after-link-definition": (
                "# Changes\n\n[release-notes]: /example\n"
                f"<release-notes>\n{hidden_note}</release-notes>\n"
            ),
            "blockquote-heading": (
                f"# Changes\n\n> ## {version}\n>\n> - Hidden note.\n"
            ),
            "bullet-list-heading": (
                f"# Changes\n\n- ## {version}\n\n  - Hidden note.\n"
            ),
            "ordered-list-heading": (
                f"# Changes\n\n1. ## {version}\n\n   - Hidden note.\n"
            ),
        }
        for name, changelog in cases.items():
            with self.subTest(name=name):
                with self.assertRaises(ReleasePackageError):
                    release_entry(changelog.encode(), version)

    def test_release_entry_requires_one_heading_with_visible_content(self) -> None:
        version = str(self.package["version"])
        cases = {
            "missing": "# Changes\n\n## 1.0.0\n\n- Older change.\n",
            "duplicate": (
                f"# Changes\n\n## {version}\n\n- One.\n\n"
                f"## {version}\n\n- Two.\n"
            ),
            "comment-only": (
                f"# Changes\n\n## {version}\n\n<!-- pending -->\n"
            ),
            "hidden-html-only": (
                f"# Changes\n\n## {version}\n\n"
                "<script>not visible</script>\n"
            ),
            "hidden-html-block-only": (
                f"# Changes\n\n## {version}\n\n"
                "<div hidden>not visible</div>\n"
            ),
            "inline-html-only": (
                f"# Changes\n\n## {version}\n\n"
                "<span>not visible</span>\n"
            ),
            "raw-html-heading": (
                f"# Changes\n\n## <span>{version}</span>\n\n"
                "- Hidden note.\n"
            ),
            "subheading-only": (
                f"# Changes\n\n## {version}\n\n### Internal heading\n"
            ),
        }
        for name, changelog in cases.items():
            with self.subTest(name=name):
                with self.assertRaises(ReleasePackageError):
                    release_entry(changelog.encode(), version)

    def test_release_entry_hashes_exact_source_bytes_through_peer_heading(self) -> None:
        version = str(self.package["version"])
        entry = (
            f"## {version}\r\n\r\n"
            "- Café boundary.\r\n\r\n"
            "### Detail\r\n\r\n"
            "More detail.\r\n\r\n"
        ).encode()
        changelog = (
            b"# Changes\r\n\r\n"
            + entry
            + b"## 1.0.0\r\n\r\n- Older change.\r\n"
        )

        identity = release_entry(changelog, version)

        self.assertEqual(hashlib.sha256(entry).hexdigest(), identity["entry_sha256"])

    def test_archive_binds_packaged_notes_to_clean_release_commit(self) -> None:
        identity = verify_archive(
            self.archive(), MANIFEST, CHANGELOG, expected_vcs_commit=COMMIT
        )
        self.assertEqual(COMMIT, identity["archive"]["vcs_commit"])
        self.assertEqual(
            identity["changelog"]["entry_sha256"],
            identity["archive"]["release_entry_sha256"],
        )

    def test_archive_rejects_changed_notes_or_vcs_identity(self) -> None:
        cases = (
            self.archive(
                changelog=CHANGELOG.read_bytes() + b"\nchanged\n",
                name="changed-notes.crate",
            ),
            self.archive(commit="f" * 40, name="changed-vcs.crate"),
            self.archive(dirty=True, name="dirty-vcs.crate"),
        )
        for archive in cases:
            with self.subTest(archive=archive.read_bytes()):
                with self.assertRaises(ReleasePackageError):
                    verify_archive(
                        archive, MANIFEST, CHANGELOG, expected_vcs_commit=COMMIT
                    )


if __name__ == "__main__":
    unittest.main()
