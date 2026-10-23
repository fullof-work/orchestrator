"""Small offline fixtures for the optional guest workload and host observations."""
import errno
import hashlib
import importlib.util
import io
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch


PERF = Path(__file__).resolve().parents[2] / "perf"
def load(name):
    spec = importlib.util.spec_from_file_location(name, PERF / (name + ".py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
guest = load("checkpoint_guest_workload")
host = load("checkpoint_observe")


class CheckpointWorkload(unittest.TestCase):
    def test_disjoint_regions_bound_every_pattern_and_repeat_across_states(self):
        length = 12 * 1024**2
        previous_end = 0
        for index, pattern in enumerate(guest.PATTERNS):
            start, capacity = guest.workload_region(64 * 1024**2, index * length, length)
            self.assertGreaterEqual(start, previous_end)
            previous_end = start + capacity
            retained = list(guest.positions(pattern, 1024, capacity // guest.PAGE))
            self.assertEqual(retained, list(guest.positions(pattern, 1024, capacity // guest.PAGE)))
            for relative, transferred, _ in retained:
                self.assertGreaterEqual(start + relative, start)
                self.assertLessEqual(start + relative + transferred, start + capacity)
                if pattern == "hot":
                    self.assertLessEqual(relative + transferred, 2 * 1024**2)
        for start, capacity in ((-4096, 4096), (1, 4096), (0, 0), (0, 4097), (8192, 4096)):
            with self.subTest(start=start, capacity=capacity), self.assertRaises(ValueError):
                guest.workload_region(8192, start, capacity)

    def test_positions_repeat_and_partial_io_has_real_sector_alignment(self):
        for pattern in guest.PATTERNS:
            first = list(guest.positions(pattern, 64, 16384))
            self.assertEqual(first, list(guest.positions(pattern, 64, 16384)))
            for offset, length, _ in first:
                self.assertEqual(offset % (512 if pattern == "partial" else 4096), 0)
                self.assertEqual(length, 512 if pattern == "partial" else 4096)
                self.assertLess(offset + length, 64 * 1024**2 + 1)
                if pattern == "hot":
                    self.assertLess(offset, 2 * 1024**2)

    def test_partial_writes_preserve_all_unselected_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "payload"
            path.write_bytes(hashlib.shake_256(b"fixture").digest(1024**2))
            expected = bytearray(path.read_bytes())
            for index, (offset, length, _) in enumerate(guest.positions("partial", 32, len(expected) // 4096)):
                expected[offset:offset + length] = hashlib.shake_256(f"477/partial/1/{index}".encode()).digest(length)
            # Exercise real positional reads/writes but keep this fixture portable
            # to source-test filesystems. Actual KVM runs must use direct_open.
            with patch.object(guest, "direct_open", side_effect=lambda name: os.open(name, os.O_RDWR)):
                result = guest.workload(path, "partial", 32, 1)
            self.assertEqual(path.read_bytes(), expected)
            self.assertEqual(result["write_bytes"], 32 * 512)
            self.assertEqual(result["operations"], len(result["latency_us"]))
            self.assertGreater(result["throughput_mib_s"], 0)
            self.assertLessEqual(result["p50_us"], result["p95_us"])
            self.assertLessEqual(result["p95_us"], result["p99_us"])

    def test_region_writes_preserve_other_patterns_bytes_and_report_actual_scope(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "payload"
            initial = hashlib.shake_256(b"regions").digest(4096 * 8)
            path.write_bytes(initial)
            with patch.object(guest, "direct_open", side_effect=lambda name: os.open(name, os.O_RDWR)):
                result = guest.workload(path, "partial", 32, 1, 4096 * 2, 4096 * 3)
            final = path.read_bytes()
            self.assertEqual(final[:4096 * 2], initial[:4096 * 2])
            self.assertEqual(final[4096 * 5:], initial[4096 * 5:])
            self.assertNotEqual(final[4096 * 2:4096 * 5], initial[4096 * 2:4096 * 5])
            self.assertEqual(result["region_offset_bytes"], 4096 * 2)
            self.assertEqual(result["region_length_bytes"], 4096 * 3)
            self.assertEqual(result["logical_working_set_bytes"], 4096 * 3)

    def test_verification_hashes_all_bytes_and_separates_cache_preparation(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "payload"
            payload = b"full payload" * 8193
            path.write_bytes(payload)
            real_open = open
            writes = []
            class DropCaches(io.StringIO):
                def close(self):
                    writes.append(self.getvalue())
                    super().close()
            def opened(name, *args):
                return DropCaches() if name == "/proc/sys/vm/drop_caches" else real_open(name, *args)
            with patch("builtins.open", side_effect=opened), patch.object(guest.os, "sync") as sync, \
                 patch.object(guest, "MIB", 4096), patch.object(guest.time, "monotonic", side_effect=[1, 3, 10]):
                result = guest.verify(path)
            sync.assert_called_once()
            self.assertEqual(writes, ["3\n"])
            self.assertEqual(result["sha256"], hashlib.sha256(payload).hexdigest())
            self.assertEqual(result["bytes_read"], len(payload))
            self.assertEqual(result["cache_preparation_seconds"], 2)
            self.assertEqual(result["hash_seconds"], 7)
            self.assertEqual(result["elapsed_seconds"], 9)

    def test_rejected_direct_io_is_reported_without_buffered_retry(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "payload"
            path.write_bytes(b"x" * 4096)
            with patch.object(guest, "direct_open", side_effect=lambda name: os.open(name, os.O_RDWR)), \
                 patch.object(guest.os, "pwritev", side_effect=OSError(errno.EINVAL, "alignment")) as write:
                with self.assertRaisesRegex(OSError, "length=512.*no buffered fallback"):
                    guest.workload(path, "partial", 1, 1)
                write.assert_called_once()
            self.assertEqual(path.read_bytes(), b"x" * 4096)

    def test_sparse_recipe_preserves_logical_size_with_bounded_mapping(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(guest, "MIB", 4096), patch.object(guest.os, "sync"):
            path = Path(directory) / "payload"
            result = guest.initialize(path, 128, True)
            self.assertEqual(result["logical_bytes"], 128 * 4096)
            self.assertEqual(result["mapped_payload_bytes"], 64 * 4096)
            self.assertLess(result["guest_allocated_bytes"], result["logical_bytes"])
            self.assertEqual(path.read_bytes()[4096:8192], bytes(4096))

    def test_first_read_batch_reports_empty_input_and_bounded_strided_reads(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "payload"
            path.touch()
            self.assertEqual(guest.first_reads(path, 256)["bytes"], 0)
            path.write_bytes(b"a" * 4096 * 8)
            with patch.object(guest, "direct_open", side_effect=lambda name: os.open(name, os.O_RDWR)):
                result = guest.first_reads(path, 4)
            self.assertEqual(result["pages"], 4)
            self.assertEqual(result["first_read_us"], result["latency_us"][0])


class HostWindow(unittest.TestCase):
    def sample(self, value, pid=7):
        return {"run_id": "run", "runtime_pids": [pid], "runtime_io_write_bytes": value,
                "runtime_io_cancelled_write_bytes": 0, "runtime_rss_bytes": value,
                "runtime_cgroup_bytes": value * 2, "runtime_cgroup_io_stat": {"8:0": {"wbytes": value}},
                "base_dir": {"allocated_bytes": value}, "run_dir": {"allocated_bytes": value * 3}}

    def test_counter_memory_and_allocation_scopes_remain_separate(self):
        result = host.summarize([self.sample(4096), self.sample(8192)])
        self.assertTrue(result["runtime_identity_stable"])
        self.assertEqual(result["runtime_io_write_bytes_delta"], 4096)
        self.assertEqual(result["runtime_cgroup_io_delta"], {"8:0": {"wbytes": 4096}})
        self.assertEqual(result["base_allocated_bytes_delta"], 4096)
        self.assertEqual(result["runtime_rss_max_sample_bytes"], 8192)

    def test_identity_change_and_counter_reset_are_not_negative_write_amplification(self):
        changed = host.summarize([self.sample(4096), self.sample(8192, pid=8), self.sample(16384)])
        self.assertFalse(changed["runtime_identity_stable"])
        self.assertIsNone(changed["runtime_io_write_bytes_delta"])
        self.assertIsNone(changed["runtime_cgroup_io_delta"])
        reset = host.summarize([self.sample(8192), self.sample(4096)])
        self.assertIsNone(reset["runtime_io_write_bytes_delta"])
        self.assertIsNone(reset["runtime_cgroup_io_delta"]["8:0"]["wbytes"])


if __name__ == "__main__":
    unittest.main()
