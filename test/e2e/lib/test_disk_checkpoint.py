"""Offline checks for Checkpoint E2E evidence: artifacts, allocation and argv."""
import io
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import tarfile
import tempfile
import unittest
import zipfile

import disk_checkpoint


ROOT = Path(__file__).resolve().parent
IDENTITY = "01929e61-2a24-7abc-8def-1234567890ab"


class DiskCheckpoint(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="checkpoint-observation-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.database = self.root / "node.db"
        self.checkpoint = self.root / "checkpoint"
        self.checkpoint.mkdir()
        self.artifact = self.checkpoint / ("a" * 64 + ".sandbox")
        self.reference = "file://" + self.artifact.name + "@digest:" + "b" * 64
        with sqlite3.connect(self.database) as db:
            db.execute("create table sandboxes(id text, state text, base_dir text, resume_source_kind text, resume_source_ref text, resume_sandbox_ref text)")
            db.execute("insert into sandboxes values(?, ?, ?, ?, ?, ?)",
                       ("sandbox", "paused", str(self.root), "sandbox", self.reference, ""))

    def artifact_payload(self, checkpoint=None, extra=()):
        if checkpoint is None:
            checkpoint = {"version": 1, "checkpoint_id": IDENTITY,
                          "root": {"diff_id": "root"}, "disks": [{"name": "data", "diff_id": "data"}]}
        payload = io.BytesIO()
        with zipfile.ZipFile(payload, "w") as archive:
            archive.writestr("sandbox.runtime.cfg", b"fixture runtime config")
            archive.writestr("checkpoint.json", json.dumps(checkpoint))
            for name in extra:
                archive.writestr(name, b"unexpected")
        with tarfile.open(self.artifact, "w") as carrier:
            member = tarfile.TarInfo("sandbox")
            member.size = len(payload.getvalue())
            carrier.addfile(member, io.BytesIO(payload.getvalue()))
        return checkpoint

    def test_reads_local_snapshot_companion_and_sandbox_artifact(self):
        expected = self.artifact_payload()
        for kind in ("sandbox", "snapshot"):
            with self.subTest(kind=kind), sqlite3.connect(self.database) as db:
                db.execute("update sandboxes set resume_source_kind=?, resume_source_ref=?, resume_sandbox_ref=?",
                           (kind, self.reference if kind == "sandbox" else "file://s.snapshot@digest:" + "c" * 64,
                            self.reference if kind == "snapshot" else ""))
            actual = disk_checkpoint.local_metadata(self.database, "sandbox")
            self.assertEqual(actual, {**expected, "artifact": str(self.artifact)})

    def test_rejects_non_checkpoint_members_and_unsafe_or_duplicate_diff_ids(self):
        for extra in (("root.ext4",), ("ordinary-sandbox.cfg",)):
            self.artifact_payload(extra=extra)
            with self.assertRaises(AssertionError):
                disk_checkpoint.local_metadata(self.database, "sandbox")
        for bad in ("root", "../escape", "..", "back\\slash", ""):
            payload = self.artifact_payload()
            payload["disks"][0]["diff_id"] = bad
            self.artifact_payload(payload)
            with self.subTest(diff_id=bad), self.assertRaises(AssertionError):
                disk_checkpoint.local_metadata(self.database, "sandbox")

    def test_requires_canonical_v7_and_a_paused_local_capture(self):
        for identity in (IDENTITY.upper(), IDENTITY.replace("-7abc-", "-4abc-"), "not-a-uuid"):
            payload = self.artifact_payload()
            payload["checkpoint_id"] = identity
            self.artifact_payload(payload)
            with self.subTest(identity=identity), self.assertRaises((ValueError, AssertionError)):
                disk_checkpoint.local_metadata(self.database, "sandbox")
        self.artifact_payload()
        for state, kind, ref in (("running", "sandbox", self.reference),
                                 ("paused", "other", self.reference),
                                 ("paused", "sandbox", "manifest://" + "a" * 64),
                                 ("paused", "sandbox", "file://../escape.sandbox@digest:" + "a" * 64)):
            with sqlite3.connect(self.database) as db:
                db.execute("update sandboxes set state=?, resume_source_kind=?, resume_source_ref=?", (state, kind, ref))
            with self.subTest(state=state, kind=kind, ref=ref), self.assertRaises(AssertionError):
                disk_checkpoint.local_metadata(self.database, "sandbox")

    def test_allocated_bytes_distinguish_sparse_size_and_preserve_inode(self):
        disk = self.checkpoint / "root.diff"
        with disk.open("wb") as stream:
            stream.truncate(4 * 1024 * 1024)
        before = disk_checkpoint.disk_state(self.checkpoint)[0]
        with disk.open("r+b") as stream:
            stream.write(os.urandom(128 * 1024))
            stream.flush()
            os.fsync(stream.fileno())
        after = disk_checkpoint.disk_state(self.checkpoint)[0]
        self.assertEqual(before["inode"], after["inode"])
        self.assertEqual(before["logical_file_bytes"], after["logical_file_bytes"])
        self.assertGreaterEqual(after["allocated_bytes"] - before["allocated_bytes"], 128 * 1024)
        self.assertLess(after["allocated_bytes"], after["logical_file_bytes"])
        disk.unlink()
        disk.symlink_to(self.artifact)
        self.artifact.touch()
        with self.assertRaises(AssertionError):
            disk_checkpoint.disk_state(self.checkpoint)

    def test_tree_allocation_deduplicates_hardlinks_and_excludes_symlinks(self):
        source = self.checkpoint / "root.overlay"
        source.write_bytes(os.urandom(128 * 1024))
        os.link(source, self.checkpoint / "alias.overlay")
        (self.checkpoint / "external").symlink_to(self.root)
        result = disk_checkpoint.tree_allocation(self.checkpoint)
        self.assertEqual(result["files"], 1)
        self.assertEqual(result["logical_file_bytes"], source.stat().st_size)
        self.assertEqual(result["allocated_bytes"], source.stat().st_blocks * 512)

    def test_exact_capture_gate_checks_uuid_and_every_remaining_argument(self):
        log = self.root / "capture.jsonl"
        for command in ("snapshot", "export"):
            expected = [command, "--json", "--path-id", "fixture", "--mode", "local"]
            env = {**os.environ, "SNAPSHOT_ARGV_LOG": str(log), "EXPORT_ARGV_LOG": str(log)}
            def check(args):
                log.write_text(json.dumps(args) + "\n")
                return subprocess.run(["bash", "-c", 'source "$1"; shift; "$@"', "fixture",
                                       str(ROOT / "execute_artifact.sh"), "assert_" + command + "_argv", "0", *expected],
                                      env=env, capture_output=True, text=True, timeout=5)
            correct = [*expected, "--checkpoint", IDENTITY]
            passed = check(correct)
            self.assertEqual(passed.returncode, 0, passed.stderr)
            for wrong in (expected, correct + ["--unexpected"], correct + ["--checkpoint", IDENTITY],
                          [*expected, "--checkpoint", IDENTITY.upper()],
                          [*expected, "--checkpoint", IDENTITY.replace("-7abc-", "-4abc-")]):
                with self.subTest(command=command, args=wrong):
                    self.assertNotEqual(check(wrong).returncode, 0)


if __name__ == "__main__":
    unittest.main()
