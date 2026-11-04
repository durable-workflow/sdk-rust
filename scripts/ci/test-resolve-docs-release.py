#!/usr/bin/env python3
"""Stable documentation must use the public approved artifact, never main."""

import copy
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("docs_release", Path(__file__).with_name("resolve-docs-release.py"))
release = importlib.util.module_from_spec(spec)
spec.loader.exec_module(release)

VERSION = "4.11.2"
COMMIT = "a" * 40


def crate(files, special=None):
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
        for name, contents in files.items():
            member = tarfile.TarInfo(f"durable-workflow-{VERSION}/{name}")
            member.size = len(contents)
            archive.addfile(member, io.BytesIO(contents))
        if special is not None:
            archive.addfile(special)
    return buffer.getvalue()


class StableDocsTests(unittest.TestCase):
    def setUp(self):
        self.scratch = tempfile.TemporaryDirectory()
        self.addCleanup(self.scratch.cleanup)
        self.root = Path(self.scratch.name)
        self.output = self.root / "published"
        self.files = {
            ".cargo_vcs_info.json": json.dumps({"git": {"sha1": COMMIT}, "path_in_vcs": ""}).encode(),
            "Cargo.toml": f'[package]\nname="durable-workflow"\nversion="{VERSION}"\n'.encode(),
            "Cargo.lock": b"version = 4\n",
            "src/lib.rs": b"pub struct Published;\n",
        }
        self.raw = crate(self.files)
        self.approval = {
            "schema": "durable-workflow.rust-sdk.approved-package/v1",
            "package": "durable-workflow", "version": VERSION, "commit": COMMIT,
            "crate_sha256": hashlib.sha256(self.raw).hexdigest(),
            "files": sorted(self.files),
            "file_list_sha256": hashlib.sha256("".join(f"{name}\n" for name in sorted(self.files)).encode()).hexdigest(),
        }
        template = json.loads((Path(__file__).resolve().parents[2] / "release/approved-packages/4.11.2.json").read_bytes())
        acceptance = copy.deepcopy(template["acceptance"])
        self.approval.update(cargo_version="1.86.0", acceptance=acceptance)
        self.summary = {
            "mode": "pre-publication", "outcome": "pass", "checks_outcome": "pass",
            "cleanup": {"pass": True, "errors": [], "local_leftovers": [], "leftover_containers": [], "leftover_networks": []},
            "harness_commit": acceptance["harness_commit"], "finished_at": acceptance["finished_at"],
            "expected_server": {"version": acceptance["expected_server_version"], "revision": acceptance["expected_revision"], "release_tag": acceptance["expected_release_tag"]},
            "server": {"index": acceptance["server_index"], "executed_manifest": acceptance["executed_manifest"], "platform": acceptance["platform"]},
            "package": {"package": "durable-workflow", "version": VERSION, "commit": COMMIT,
                        "vcs_info": {"git": {"sha1": COMMIT}, "path_in_vcs": ""},
                        "crate_sha256": self.approval["crate_sha256"], "crate_sha256_after_cells": self.approval["crate_sha256"],
                        "files": self.approval["files"], "file_list_sha256": self.approval["file_list_sha256"], "cargo_version": "cargo 1.86.0 (fixture)"},
            "crate": {"sha256": self.approval["crate_sha256"], "lock_checksum": self.approval["crate_sha256"], "source_commit": COMMIT, "registry_published": False, "baselines": {"pass": True}},
            "cells": {name: {"outcome": "pass", "cluster_info": {"version": acceptance["expected_server_version"]}} for name in acceptance["cells"]},
        }
        self.metadata = {
            "crate": {"name": "durable-workflow", "max_stable_version": VERSION},
            "versions": [{"num": VERSION, "checksum": self.approval["crate_sha256"], "yanked": False}],
        }
        self.record = self.root / "release" / "approved-packages" / f"{VERSION}.json"
        self.record.parent.mkdir(parents=True)
        self.summary_record = self.record.with_name(f"{VERSION}.summary.json")
        summary_raw = json.dumps(self.summary).encode()
        self.summary_record.write_bytes(summary_raw)
        self.approval["acceptance"]["summary_sha256"] = hashlib.sha256(summary_raw).hexdigest()
        self.record.write_text(json.dumps(self.approval))
        self.git("init", "--quiet")
        self.checkpoint()

    def git(self, *args):
        return subprocess.run(["git", "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", *args], cwd=self.root, check=True, capture_output=True)

    def checkpoint(self):
        self.git("add", "release")
        self.git("commit", "--quiet", "--allow-empty", "-m", "fixture")

    def refuse(self, raw=None, approval=None, source=COMMIT):
        with self.assertRaises(release.DocsReleaseError):
            release.unpack_crate(raw or self.raw, self.output, approval or self.approval, source)
        self.assertFalse(self.output.exists())

    def test_published_archive_is_used_even_when_main_has_unreleased_api(self):
        (self.root / "src").mkdir()
        (self.root / "src" / "raw_request.rs").write_text("pub struct Unreleased;")
        version, checksum = release.select_release(self.metadata)
        approval = release.approved_release(self.root, version, checksum)
        release.unpack_crate(self.raw, self.output, approval, COMMIT)
        self.assertEqual((self.output / "src/lib.rs").read_bytes(), self.files["src/lib.rs"])
        self.assertFalse((self.output / "src/raw_request.rs").exists())

    def test_missing_approval_has_no_main_fallback(self):
        self.record.unlink()
        with self.assertRaises(release.ReleasePackageError):
            release.approved_release(self.root, VERSION, self.approval["crate_sha256"])
        self.assertFalse(self.output.exists())

    def test_registry_selection_refuses_yank_prerelease_missing_and_ambiguous(self):
        variants = []
        for change in ("yanked", "checksum"):
            metadata = copy.deepcopy(self.metadata)
            metadata["versions"][0][change] = True if change == "yanked" else "bad"
            variants.append(metadata)
        metadata = copy.deepcopy(self.metadata)
        metadata["crate"]["max_stable_version"] = "4.12.0-rc.1"
        variants.append(metadata)
        for versions in ([], self.metadata["versions"] * 2):
            variants.append({**self.metadata, "versions": versions})
        for metadata in variants:
            with self.subTest(metadata=metadata), self.assertRaises(release.DocsReleaseError):
                release.select_release(metadata)

    def test_approval_checksum_and_inventory_digest_must_match(self):
        for field, value in (("crate_sha256", "0" * 64), ("file_list_sha256", "0" * 64), ("commit", "main"), ("version", "4.12.0")):
            with self.subTest(field=field):
                self.record.write_text(json.dumps({**self.approval, field: value}))
                self.checkpoint()
                with self.assertRaises((release.DocsReleaseError, release.ReleasePackageError)):
                    release.approved_release(self.root, VERSION, self.approval["crate_sha256"])

    def test_missing_or_failed_acceptance_receipt_refuses(self):
        variants = [None, {**self.approval["acceptance"], "outcome": "fail"}]
        for acceptance in variants:
            with self.subTest(acceptance=acceptance):
                record = {**self.approval, "acceptance": acceptance}
                self.record.write_text(json.dumps(record))
                self.checkpoint()
                with self.assertRaises(release.ReleasePackageError):
                    release.approved_release(self.root, VERSION, self.approval["crate_sha256"])

    def test_summary_missing_drift_or_failed_outcome_refuses(self):
        self.summary_record.unlink()
        self.checkpoint()
        with self.assertRaises(release.ReleasePackageError):
            release.approved_release(self.root, VERSION, self.approval["crate_sha256"])
        for summary, rebind in (({**self.summary, "outcome": "fail"}, False), ({**self.summary, "outcome": "fail"}, True)):
            with self.subTest(rebind=rebind):
                raw = json.dumps(summary).encode()
                self.summary_record.write_bytes(raw)
                record = copy.deepcopy(self.approval)
                if rebind:
                    record["acceptance"]["summary_sha256"] = hashlib.sha256(raw).hexdigest()
                self.record.write_text(json.dumps(record))
                self.checkpoint()
                with self.assertRaises(release.ReleasePackageError):
                    release.approved_release(self.root, VERSION, self.approval["crate_sha256"])

    def test_untracked_modified_or_linked_approval_refuses(self):
        self.record.write_text(json.dumps({**self.approval, "unexpected": True}))
        with self.assertRaises(release.ReleasePackageError):
            release.approved_release(self.root, VERSION, self.approval["crate_sha256"])
        self.record.unlink()
        other = self.root / "elsewhere.json"
        other.write_text(json.dumps(self.approval))
        self.record.symlink_to(other)
        with self.assertRaises(release.ReleasePackageError):
            release.approved_release(self.root, VERSION, self.approval["crate_sha256"])

    def test_untracked_summary_refuses(self):
        self.git("rm", "--cached", str(self.summary_record.relative_to(self.root)))
        self.git("commit", "--quiet", "-m", "untracked summary")
        with self.assertRaisesRegex(release.ReleasePackageError, "not tracked"):
            release.approved_release(self.root, VERSION, self.approval["crate_sha256"])

    def test_modified_summary_refuses_before_parsing(self):
        self.summary_record.write_bytes(self.summary_record.read_bytes() + b"\n")
        with self.assertRaisesRegex(release.ReleasePackageError, "differs from its tracked content"):
            release.approved_release(self.root, VERSION, self.approval["crate_sha256"])

    def test_linked_summary_refuses(self):
        other = self.root / "outside-summary.json"
        self.summary_record.rename(other)
        self.summary_record.symlink_to(other)
        with self.assertRaises(release.ReleasePackageError):
            release.approved_release(self.root, VERSION, self.approval["crate_sha256"])

    def test_tag_uses_peeled_commit_and_refuses_missing(self):
        refs = f"{'b' * 40}\trefs/tags/{VERSION}\n{COMMIT}\trefs/tags/{VERSION}^{{}}\n"
        self.assertEqual(release.tag_commit(refs, VERSION), COMMIT)
        with self.assertRaises(release.DocsReleaseError):
            release.tag_commit("", VERSION)

    def test_checksum_and_source_mismatch_refuse_before_extraction(self):
        self.refuse(raw=self.raw + b"changed")
        self.refuse(source="b" * 40)

    def test_file_set_and_vcs_and_manifest_mismatch_refuse(self):
        changes = ({"extra.rs": b"extra"}, {".cargo_vcs_info.json": b'{"git":{"sha1":"wrong"}}'}, {"Cargo.toml": b'[package]\nname="other"\nversion="4.11.2"'})
        for change in changes:
            with self.subTest(change=change):
                raw = crate({**self.files, **change})
                approval = {**self.approval, "crate_sha256": hashlib.sha256(raw).hexdigest()}
                self.refuse(raw, approval)

    def test_dirty_source_refuses(self):
        files = {**self.files, ".cargo_vcs_info.json": json.dumps({"git": {"sha1": COMMIT, "dirty": True}, "path_in_vcs": ""}).encode()}
        raw = crate(files)
        self.refuse(raw, {**self.approval, "crate_sha256": hashlib.sha256(raw).hexdigest()})

    def test_paths_links_duplicates_and_special_files_refuse(self):
        for name, kind in (("../escape", tarfile.REGTYPE), ("/absolute", tarfile.REGTYPE), (f"durable-workflow-{VERSION}/src/../escape", tarfile.REGTYPE), (f"durable-workflow-{VERSION}/src/lib.rs", tarfile.REGTYPE), (f"durable-workflow-{VERSION}/link", tarfile.SYMTYPE), (f"durable-workflow-{VERSION}/device", tarfile.CHRTYPE)):
            with self.subTest(name=name, kind=kind):
                member = tarfile.TarInfo(name)
                member.type = kind
                member.linkname = "/outside"
                raw = crate(self.files, member)
                self.refuse(raw, {**self.approval, "crate_sha256": hashlib.sha256(raw).hexdigest()})

    def test_archive_entry_and_unpacked_bounds_refuse(self):
        for limit, value in (("MAX_ARCHIVE", 1), ("MAX_FILES", 1), ("MAX_UNPACKED", 1)):
            with self.subTest(limit=limit), patch.object(release, limit, value):
                self.refuse()


if __name__ == "__main__":
    unittest.main()
