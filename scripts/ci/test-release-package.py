#!/usr/bin/env python3
"""Focused tests for release-note and crate source identity verification."""

from __future__ import annotations

import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile
import unittest

from release_package import (
    APPROVAL_SCHEMA,
    ReleasePackageError,
    approval_record_relative,
    confined_file,
    confined_root,
    file_list_sha256,
    load_approval,
    parse_approval,
    read_approval_record,
    validate_acceptance,
    load_package,
    release_entry,
    render_release_notes,
    restore_missing_lockfile,
    verify_approved_archive,
    verify_archive,
    verify_source,
)


ROOT = Path(__file__).resolve().parents[2]
MANIFEST = ROOT / "Cargo.toml"
CHANGELOG = ROOT / "CHANGELOG.md"
REQUIREMENTS = ROOT / "scripts/ci/release-tooling-requirements.txt"
COMMIT = "0123456789abcdef0123456789abcdef01234567"
# The exact receipt approval_candidate() emits for a passing 3.0.5 run.
ACCEPTANCE = {
    "summary_sha256": "c" * 64,
    "harness_commit": "e" * 40,
    "server_index": "ghcr.io/durable-workflow/server@sha256:" + "d" * 64,
    "executed_manifest": "ghcr.io/durable-workflow/server@sha256:" + "f" * 64,
    "platform": "linux/arm64",
    "expected_server_version": "3.0.5",
    "expected_revision": "b" * 40,
    "expected_release_tag": "3.0.5",
    "cluster_versions": ["3.0.5"],
    "cells": ["deadline", "lifecycle"],
    "finished_at": "2026-11-03T23:00:00+00:00",
    "outcome": "pass",
}


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
                   "--release-notes"]
        result = subprocess.run([*command, "2.0.1"], cwd=self.root,
                                text=True, capture_output=True, check=True)
        self.assertEqual("## 2.0.1\n\n- Current metrics.\n", result.stdout)
        mismatch = subprocess.run([*command, "2.0.0"], cwd=self.root,
                                  text=True, capture_output=True)
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

    def test_release_notes_cli_refuses_paths_outside_checkout(self) -> None:
        secret = self.root / "private.md"
        secret.write_text("## 2.0.1\n\nPRIVATE-TEST-CONTENT\n", encoding="utf-8")
        for option, value in (("--changelog", str(secret)),
                              ("--manifest", str(secret)),
                              ("--changelog", "../private.md")):
            with self.subTest(option=option, value=value):
                result = subprocess.run(
                    [sys.executable, str(ROOT / "scripts/ci/release_package.py"),
                     "--release-notes", "2.0.1", option, value], cwd=self.root,
                    text=True, capture_output=True,
                )
                self.assertEqual(2, result.returncode)
                self.assertEqual("", result.stdout)
                self.assertNotIn("PRIVATE-TEST-CONTENT", result.stderr)
                self.assertIn("path overrides are refused", result.stderr)

    def test_release_notes_cli_refuses_symlinks_in_checkout(self) -> None:
        for filename in ("Cargo.toml", "CHANGELOG.md"):
            with self.subTest(filename=filename), tempfile.TemporaryDirectory(dir=self.root) as directory:
                checkout = Path(directory)
                files = {"Cargo.toml": '[package]\nname="example"\nversion="2.0.1"\n',
                         "CHANGELOG.md": '## 2.0.1\n\nPRIVATE-TEST-CONTENT\n'}
                for name, content in files.items():
                    (checkout / name).write_text(content, encoding="utf-8")
                target = self.root / (filename + ".private")
                target.write_text(files[filename], encoding="utf-8")
                (checkout / filename).unlink()
                (checkout / filename).symlink_to(target)
                result = subprocess.run(
                    [sys.executable, str(ROOT / "scripts/ci/release_package.py"),
                     "--release-notes", "2.0.1"], cwd=checkout,
                    text=True, capture_output=True,
                )
                self.assertEqual(1, result.returncode)
                self.assertEqual("", result.stdout)
                self.assertNotIn("PRIVATE-TEST-CONTENT", result.stderr)
                self.assertIn("regular checkout files", result.stderr)

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


    def approval_bytes(self, archive: Path, **changes: object) -> bytes:
        root = f"{self.package['name']}-{self.package['version']}/"
        with tarfile.open(archive) as crate:
            files = sorted(member.name.removeprefix(root) for member in crate.getmembers())
        record = {
            "schema": APPROVAL_SCHEMA,
            "package": self.package["name"],
            "version": self.package["version"],
            "commit": COMMIT,
            "crate_sha256": hashlib.sha256(archive.read_bytes()).hexdigest(),
            "cargo_version": "1.86.0",
            "files": files,
            "file_list_sha256": file_list_sha256(files),
            "acceptance": dict(ACCEPTANCE),
        }
        record.update(changes)
        return json.dumps(record).encode()

    def approval(self, archive: Path, **changes: object) -> Path:
        path = self.root / "approved.json"
        path.write_bytes(self.approval_bytes(archive, **changes))
        return path

    def test_approved_archive_binds_checksum_files_commit_version_and_cargo(self) -> None:
        archive = self.archive()
        identity = verify_approved_archive(
            archive, MANIFEST, CHANGELOG, COMMIT, self.approval_bytes(archive), "1.86.0"
        )
        self.assertEqual(hashlib.sha256(archive.read_bytes()).hexdigest(), identity["approval"]["crate_sha256"])
        self.assertEqual(COMMIT, identity["approval"]["commit"])
        self.assertEqual(3, identity["approval"]["file_count"])
        self.assertEqual("3.0.5", identity["approval"]["server_version"])

    def test_approved_archive_counterfactuals_are_refused(self) -> None:
        archive = self.archive()
        files = sorted([".cargo_vcs_info.json", "CHANGELOG.md", "Cargo.lock"])
        extra = files + ["src/extra.rs"]
        cases = {
            "one-byte checksum drift": ({"crate_sha256": "0" * 64}, "SHA-256 differs"),
            "wrong commit": ({"commit": "f" * 40}, "commit differs"),
            "wrong version": ({"version": "9.9.9"}, "version differs"),
            "wrong package": ({"package": "other"}, "name differs"),
            "extra file": ({"files": sorted(extra), "file_list_sha256": file_list_sha256(extra)}, "file set differs"),
            "missing file": ({"files": files[1:], "file_list_sha256": file_list_sha256(files[1:])}, "file set differs"),
            "unsorted files": ({"files": list(reversed(files))}, "malformed"),
            "list digest drift": ({"file_list_sha256": "0" * 64}, "malformed"),
            "wrong schema": ({"schema": "other"}, "malformed"),
            "empty acceptance": ({"acceptance": {}}, "acceptance receipt"),
            "extra field": ({"note": "x"}, "unexpected fields"),
        }
        for label, (change, message) in cases.items():
            record = self.approval_bytes(archive, **change)
            with self.subTest(label=label), self.assertRaisesRegex(ReleasePackageError, message):
                verify_approved_archive(archive, MANIFEST, CHANGELOG, COMMIT, record)
        record = self.approval_bytes(archive)
        with self.assertRaisesRegex(ReleasePackageError, "differs from approved Cargo"):
            verify_approved_archive(archive, MANIFEST, CHANGELOG, COMMIT, record, "1.99.0")
        # A one-byte drift in the archive itself, with the approval recorded before it.
        drifted = self.archive(changelog=CHANGELOG.read_bytes(), name="drifted.crate", lockfile=(ROOT / "Cargo.lock").read_bytes() + b"#")
        with self.assertRaisesRegex(ReleasePackageError, "SHA-256 differs"):
            verify_approved_archive(drifted, MANIFEST, CHANGELOG, COMMIT, record)
        with self.assertRaises(ReleasePackageError):
            verify_approved_archive(archive, MANIFEST, CHANGELOG, "e" * 40, record)

    # Security review of #178 (501d0311), hold 1: a truncated or fabricated
    # acceptance receipt must never authorize publication.

    def test_acceptance_receipt_positive_control(self) -> None:
        validate_acceptance(dict(ACCEPTANCE))
        validate_acceptance({**ACCEPTANCE, "platform": "linux/amd64", "finished_at": "2026-11-03T23:00:00Z",
                             "server_index": "docker.io/durableworkflow/server@sha256:" + "1" * 64,
                             "executed_manifest": "docker.io/durableworkflow/server@sha256:" + "2" * 64})

    def test_harness_candidate_is_exactly_the_receipt_publication_accepts(self) -> None:
        # Guard against schema drift between installed-native/run.py and this verifier.
        sys.path.insert(0, str(ROOT / "scripts/acceptance/installed-native"))
        self.addCleanup(sys.path.remove, str(ROOT / "scripts/acceptance/installed-native"))
        import run as harness
        files = sorted(["Cargo.toml", "Cargo.toml.orig", "src/lib.rs"])
        summary = {
            "harness_commit": "e" * 40, "outcome": "pass", "finished_at": "2026-11-03T23:00:00.000001+00:00",
            "expected_server": {"version": "3.0.5", "revision": "b" * 40, "release_tag": "3.0.5"},
            "server": {"index": "ghcr.io/durable-workflow/server@sha256:" + "d" * 64,
                       "executed_manifest": "ghcr.io/durable-workflow/server@sha256:" + "f" * 64,
                       "platform": "linux/arm64"},
            "cells": {"lifecycle": {"cluster_info": {"version": "3.0.5"}},
                      "deadline": {"cluster_info": {"version": "3.0.5"}}},
            "package": {"package": "durable-workflow", "version": "4.11.2", "commit": COMMIT,
                        "crate_sha256": "a" * 64, "cargo_version": "cargo 1.86.0 (adf9b6ad1 2025-02-28)",
                        "files": files, "file_list_sha256": file_list_sha256(files)},
        }
        candidate = harness.approval_candidate(summary, b"{}")
        approval, _ = parse_approval(json.dumps(candidate).encode())
        self.assertEqual("3.0.5", approval["acceptance"]["expected_server_version"])
        failed = {**summary, "outcome": "fail"}
        with self.assertRaisesRegex(ReleasePackageError, "outcome"):
            parse_approval(json.dumps(harness.approval_candidate(failed, b"{}")).encode())

    def test_acceptance_receipt_deletions_are_refused(self) -> None:
        for field in sorted(ACCEPTANCE):
            receipt = {key: value for key, value in ACCEPTANCE.items() if key != field}
            with self.subTest(deleted=field), self.assertRaisesRegex(ReleasePackageError, "missing or unexpected"):
                validate_acceptance(receipt)
        for label, receipt in (("extra key", {**ACCEPTANCE, "note": "x"}), ("not an object", ["pass"]),
                               ("null", None)):
            with self.subTest(label=label), self.assertRaisesRegex(ReleasePackageError, "missing or unexpected"):
                validate_acceptance(receipt)

    def test_acceptance_receipt_wrong_types_are_refused(self) -> None:
        wrong = {
            "summary_sha256": 0, "harness_commit": None, "server_index": ["x"], "executed_manifest": {},
            "platform": 1, "expected_server_version": 3.05, "expected_revision": True,
            "expected_release_tag": 305, "cluster_versions": "3.0.5", "cells": "deadline,lifecycle",
            "finished_at": 1793745600, "outcome": True,
        }
        self.assertEqual(set(wrong), set(ACCEPTANCE))
        for field, value in wrong.items():
            with self.subTest(field=field), self.assertRaisesRegex(ReleasePackageError, "is invalid"):
                validate_acceptance({**ACCEPTANCE, field: value})

    def test_acceptance_receipt_mismatches_are_refused(self) -> None:
        digest = "@sha256:" + "d" * 64
        mismatches = {
            "summary_sha256": ["C" * 64, "c" * 63, ""],
            "harness_commit": ["e" * 39, "E" * 40, "main"],
            "expected_revision": ["b" * 41, "3.0.5"],
            "server_index": ["ghcr.io/durable-workflow/server:3.0.5", "ghcr.io/durable-workflow/server@sha256:" + "d" * 63,
                             "ghcr.io/Durable/server" + digest, "img@sha256:x", "sha256:" + "d" * 64],
            "executed_manifest": ["ghcr.io/other/server@sha256:" + "f" * 64, "ghcr.io/durable-workflow/server:3.0.5"],
            "platform": ["linux/riscv64", "windows/amd64", "linux/arm64/v8", ""],
            "expected_server_version": ["3.0", "v3.0.5", "3.0.5/../x"],
            "expected_release_tag": ["native-3.0.5", "3.0.4", ""],
            "cluster_versions": [[], ["3.0.4"], ["3.0.5", "3.0.5"], ["", "3.0.5"], [None]],
            "cells": [["lifecycle"], ["deadline"], ["lifecycle", "deadline"], ["deadline", "lifecycle", "extra"], []],
            "finished_at": ["", "yesterday", "2026-11-03T23:00:00", "2026-13-03T23:00:00+00:00"],
            "outcome": ["fail", "PASS", ""],
        }
        self.assertEqual(set(mismatches), set(ACCEPTANCE))
        for field, values in mismatches.items():
            for value in values:
                with self.subTest(field=field, value=value), self.assertRaisesRegex(ReleasePackageError, field):
                    validate_acceptance({**ACCEPTANCE, field: value})
        # Relationship: tag, cluster versions and expected version must agree.
        moved = {**ACCEPTANCE, "expected_server_version": "3.0.6"}
        with self.assertRaisesRegex(ReleasePackageError, "cluster_versions, expected_release_tag"):
            validate_acceptance(moved)

    # Hold 2: the approval is the tracked regular file at its exact path in
    # the maintained-tooling checkout.

    def git(self, repository: Path, *arguments: str) -> None:
        subprocess.run(
            ["git", "-C", str(repository), "-c", "user.name=t", "-c", "user.email=t@example.invalid",
             "-c", "commit.gpgsign=false", *arguments],
            check=True, capture_output=True,
            env={**os.environ, "GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_NOSYSTEM": "1"},
        )

    def tooling(self, record: bytes, relative: str | None = None) -> Path:
        """A maintained-tooling repository tracking `record` (default: the exact path)."""
        repository = self.root / "tooling"
        if not repository.exists():
            repository.mkdir()
            self.git(repository, "init", "--quiet")
        relative = relative or approval_record_relative(self.package["version"])
        path = repository / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(record)
        self.git(repository, "add", "--", relative)
        self.git(repository, "commit", "--quiet", "--allow-empty", "-m", "approve")
        return repository

    def test_tracked_approval_record_positive_control(self) -> None:
        record = self.approval_bytes(self.archive())
        repository = self.tooling(record)
        content, relative = read_approval_record(confined_root(repository), self.package["version"])
        self.assertEqual((record, f"release/approved-packages/{self.package['version']}.json"), (content, relative))

    def test_approval_record_symlinks_and_wrong_paths_are_refused(self) -> None:
        record = self.approval_bytes(self.archive())
        version = self.package["version"]
        relative = approval_record_relative(version)
        secret = self.outside_file()
        forged = secret.parent / "forged.json"
        forged.write_bytes(record)
        cases = {}

        def external_symlink(repository: Path) -> None:
            (repository / relative).unlink()
            (repository / relative).symlink_to(forged)
        cases["external symlink"] = (external_symlink, "symbolic link")

        def tracked_symlink(repository: Path) -> None:
            external_symlink(repository)
            self.git(repository, "add", "--", relative)
            self.git(repository, "commit", "--quiet", "-m", "link")
        cases["tracked symlink"] = (tracked_symlink, "symbolic link")

        def symlinked_directory(repository: Path) -> None:
            directory = repository / "release" / "approved-packages"
            shutil.move(directory, secret.parent / "approved-packages")
            directory.symlink_to(secret.parent / "approved-packages", target_is_directory=True)
        cases["symlinked directory"] = (symlinked_directory, "symbolic link")

        def untracked(repository: Path) -> None:
            self.git(repository, "rm", "--quiet", "--cached", "--", relative)
            self.git(repository, "commit", "--quiet", "-m", "untrack")
        cases["untracked record"] = (untracked, "not tracked at its exact path")

        def wrong_tracked_path(repository: Path) -> None:
            self.git(repository, "rm", "--quiet", "--", relative)
            self.git(repository, "commit", "--quiet", "-m", "move")
            self.tooling(record, f"release/approved-packages/nested/{version}.json")
            self.tooling(record, f"release/{version}.json")
        cases["wrong tracked path"] = (wrong_tracked_path, "not an existing regular file")

        def edited(repository: Path) -> None:
            (repository / relative).write_bytes(record + b" ")
        cases["worktree differs from tracked blob"] = (edited, "differs from its tracked content")

        def directory_in_place(repository: Path) -> None:
            (repository / relative).unlink()
            (repository / relative).mkdir()
        cases["directory at the record path"] = (directory_in_place, "not an existing regular file")

        for label, (mutate, message) in cases.items():
            with self.subTest(label=label):
                shutil.rmtree(self.root / "tooling", ignore_errors=True)
                shutil.rmtree(secret.parent / "approved-packages", ignore_errors=True)
                repository = self.tooling(record)
                mutate(repository)
                with self.assertRaisesRegex(ReleasePackageError, message):
                    read_approval_record(confined_root(repository), version)

    def test_approval_version_is_strict_semver_before_naming_a_path(self) -> None:
        secret = self.outside_file()
        for version in ("4.11.2/../../x", "../4.11.2", "4.11.2\0", "v4.11.2", "4.11", "04.11.2",
                        "4.11.2-..", "4.11.2-rc..1", "4.11.2/", "/4.11.2", "４.11.2", "",
                        os.path.relpath(secret.parent / "x", self.root)):
            with self.subTest(version=version), self.assertRaisesRegex(ReleasePackageError, "strict SemVer"):
                approval_record_relative(version)
        for version in ("4.11.2", "4.11.2-rc.1+build.5"):
            with self.subTest(positive=version):
                self.assertEqual(f"release/approved-packages/{version}.json", approval_record_relative(version))

    def test_approval_record_rejects_unsafe_file_names_and_versions(self) -> None:
        archive = self.archive()
        for label, change in {
            "dot-dot file": {"files": ["../escape"]},
            "absolute file": {"files": ["/etc/passwd"]},
            "NUL file": {"files": ["a\0b"]},
            "backslash file": {"files": ["a\\..\\b"]},
            "empty component": {"files": ["src//lib.rs"]},
            "dot component": {"files": ["./Cargo.toml"]},
            "crafted version": {"version": "4.11.2/../../x"},
        }.items():
            files = change.get("files")
            if files is not None:
                change = {**change, "file_list_sha256": file_list_sha256(files)}
            approval = self.approval(archive, **change)
            with self.subTest(label=label), self.assertRaisesRegex(ReleasePackageError, "malformed"):
                load_approval(approval)
        # The plain reader never follows a link either.
        link = self.root / "linked.json"
        link.symlink_to(self.approval(archive))
        with self.assertRaisesRegex(ReleasePackageError, "not a regular file"):
            load_approval(link)

    # Defense in depth: every .crate member is checked independently.

    def raw_archive(self, extra: list[tuple[tarfile.TarInfo, bytes | None]], name: str = "raw.crate") -> Path:
        base = self.archive(name="base-" + name)
        path = self.root / name
        with tarfile.open(base) as source, tarfile.open(path, "w:gz") as archive:
            for member in source.getmembers():
                archive.addfile(member, source.extractfile(member))
            for info, content in extra:
                archive.addfile(info, io.BytesIO(content) if content is not None else None)
        return path

    def test_crate_members_must_be_canonical_regular_files(self) -> None:
        root = f"{self.package['name']}-{self.package['version']}/"

        def regular(name: str) -> tuple[tarfile.TarInfo, bytes]:
            info = tarfile.TarInfo(name)
            info.size = 1
            return info, b"x"

        symlink = tarfile.TarInfo(root + "src/link.rs")
        symlink.type, symlink.linkname = tarfile.SYMTYPE, "/etc/passwd"
        hardlink = tarfile.TarInfo(root + "src/hard.rs")
        hardlink.type, hardlink.linkname = tarfile.LNKTYPE, root + "Cargo.lock"
        directory = tarfile.TarInfo(root + "src")
        directory.type = tarfile.DIRTYPE
        cases = {
            "symlink": [(symlink, None)],
            "hardlink": [(hardlink, None)],
            "directory": [(directory, None)],
            "dot-dot": [regular(root + "../escape.rs")],
            "inner dot-dot": [regular(root + "src/../../escape.rs")],
            "absolute": [regular("/" + root + "x.rs")],
            "backslash": [regular(root + "src\\..\\x.rs")],
            "empty component": [regular(root + "src//x.rs")],
            "dot component": [regular(root + "./x.rs")],
            "wrong prefix": [regular("other-1.0.0/x.rs")],
            "prefix only": [regular(root)],
            "duplicate": [regular(root + "Cargo.lock")],
        }
        for label, extra in cases.items():
            archive = self.raw_archive(extra, name=label.replace(" ", "-") + ".crate")
            with self.subTest(label=label), self.assertRaises(ReleasePackageError):
                verify_archive(archive, MANIFEST, CHANGELOG, expected_vcs_commit=COMMIT)
        # Positive control: an extra canonical regular member is accepted.
        archive = self.raw_archive([regular(root + "src/extra.rs")], name="canonical.crate")
        verify_archive(archive, MANIFEST, CHANGELOG, expected_vcs_commit=COMMIT)

    def cli(self, *arguments: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, str(ROOT / "scripts/ci/release_package.py"), *arguments],
            cwd=ROOT, capture_output=True, text=True,
        )

    def test_approved_package_cli_requires_archive_and_commit(self) -> None:
        archive = self.archive()
        repository = self.tooling(self.approval_bytes(archive))
        base = ["--archive-root", str(self.root), "--archive", archive.name]
        for arguments in (
            ["--approval-repository", str(repository)],
            ["--approval-repository", str(repository), *base],
            [*base, "--expected-vcs-commit", COMMIT, "--cargo-version", "1.86.0"],
            ["--archive", archive.name, "--expected-vcs-commit", COMMIT],
            ["--check-paths", *base],
        ):
            with self.subTest(arguments=arguments):
                self.assertEqual(2, self.cli(*arguments).returncode)
        result = self.cli(*base, "--expected-vcs-commit", COMMIT,
                          "--approval-repository", str(repository), "--cargo-version", "1.86.0")
        self.assertEqual(0, result.returncode, result.stderr)
        identity = json.loads(result.stdout)
        self.assertEqual(COMMIT, identity["approval"]["commit"])
        self.assertEqual(approval_record_relative(self.package["version"]), identity["approval"]["record_path"])

    # Path confinement: every CLI path is confined to an explicit root before
    # it is opened.

    def outside_file(self) -> Path:
        outside = Path(tempfile.mkdtemp(prefix="release-package-outside-"))
        self.addCleanup(shutil.rmtree, outside, ignore_errors=True)
        secret = outside / "secret.crate"
        secret.write_bytes(b"PRIVATE-TEST-CONTENT")
        return secret

    def test_confined_file_refusals_and_positive_controls(self) -> None:
        root = confined_root(self.root)
        (self.root / "nested").mkdir()
        (self.root / "nested" / "ok.crate").write_bytes(b"x")
        (self.root / "ok.crate").write_bytes(b"x")
        secret = self.outside_file()
        (self.root / "link.crate").symlink_to(secret)
        (self.root / "linkdir").symlink_to(secret.parent, target_is_directory=True)
        refusals = {
            "dot-dot escape": "../" + secret.parent.name + "/secret.crate",
            "inner dot-dot": "nested/../ok.crate",
            "symlink to outside": "link.crate",
            "symlinked directory": "linkdir/secret.crate",
            "absolute": str(secret),
            "absolute inside root": str(self.root / "ok.crate"),
            "NUL byte": "ok.crate\0.json",
            "empty": "",
            "directory": "nested",
            "missing": "absent.crate",
        }
        for label, value in refusals.items():
            with self.subTest(label=label), self.assertRaises(ReleasePackageError):
                confined_file(root, value)
        for value in ("ok.crate", "nested/ok.crate", "./ok.crate"):
            with self.subTest(positive=value):
                self.assertEqual(Path(root, value).resolve(), confined_file(root, value))
        with self.assertRaisesRegex(ReleasePackageError, "already exists"):
            confined_file(root, "ok.crate", must_exist=False)
        self.assertEqual(Path(root, "new.lock"), confined_file(root, "new.lock", must_exist=False))

    def test_confined_root_refusals(self) -> None:
        (self.root / "real").mkdir()
        (self.root / "rootlink").symlink_to(self.root / "real", target_is_directory=True)
        (self.root / "file").write_bytes(b"x")
        for label, value in (("symlink", str(self.root / "rootlink")), ("dot-dot", str(self.root / "real" / "..")),
                             ("NUL", str(self.root) + "\0"), ("file", str(self.root / "file")),
                             ("missing", str(self.root / "absent")), ("empty", "")):
            with self.subTest(label=label), self.assertRaises(ReleasePackageError):
                confined_root(value)
        self.assertEqual(os.path.realpath(self.root / "real"), confined_root(self.root / "real"))

    def test_cli_refuses_paths_outside_their_roots(self) -> None:
        archive = self.archive()
        repository = self.tooling(self.approval_bytes(archive))
        secret = self.outside_file()
        (self.root / "escape.crate").symlink_to(secret)
        verify = ["--expected-vcs-commit", COMMIT]
        # A NUL byte cannot cross argv (exec refuses it); confined_file's own
        # NUL refusal is covered in test_confined_file_refusals_and_positive_controls.
        cases = {
            "archive dot-dot": ["--archive-root", str(self.root), "--archive", "../" + secret.parent.name + "/secret.crate", *verify],
            "archive absolute": ["--archive-root", str(self.root), "--archive", str(secret), *verify],
            "archive symlink": ["--archive-root", str(self.root), "--archive", "escape.crate", *verify],
            "manifest absolute": ["--manifest", str(MANIFEST), "--archive-root", str(self.root), "--archive", archive.name, *verify],
            "manifest dot-dot": ["--manifest", "../" + ROOT.name + "/Cargo.toml", "--archive-root", str(self.root), "--archive", archive.name, *verify],
            "changelog absolute": ["--check-paths", "--changelog", str(CHANGELOG)],
            "root symlink": ["--archive-root", str(self.root / "rootlink"), "--archive", archive.name, *verify],
        }
        (self.root / "rootlink").symlink_to(secret.parent, target_is_directory=True)
        for label, arguments in cases.items():
            with self.subTest(label=label):
                result = self.cli(*arguments)
                self.assertNotEqual(0, result.returncode)
                self.assertEqual("", result.stdout)
                self.assertNotIn("PRIVATE-TEST-CONTENT", result.stderr)
        record = repository / approval_record_relative(self.package["version"])
        moved = secret.parent / "approved.json"
        record.replace(moved)
        record.symlink_to(moved)
        result = self.cli("--archive-root", str(self.root), "--archive", archive.name, *verify,
                          "--approval-repository", str(repository))
        self.assertEqual(1, result.returncode)
        self.assertIn("symbolic link", result.stderr)
        # Positive controls: the same inputs, confined, are accepted.
        record.unlink()
        moved.replace(record)
        for arguments in (["--check-paths"],
                          ["--archive-root", str(self.root), "--archive", archive.name, *verify],
                          ["--archive-root", str(self.root), "--archive", archive.name, *verify,
                           "--approval-repository", str(repository)]):
            with self.subTest(positive=arguments):
                result = self.cli(*arguments)
                self.assertEqual(0, result.returncode, result.stderr)

if __name__ == "__main__":
    unittest.main()
