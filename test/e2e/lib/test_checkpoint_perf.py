"""The independent measurement runner must keep prepared input verification."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import platform
import subprocess
import tempfile
import types
import unittest
from unittest.mock import Mock, patch


DRIVER = Path(__file__).resolve().parents[2] / "perf/run_checkpoint_platform.py"
spec = importlib.util.spec_from_file_location("checkpoint_perf_driver", DRIVER)
driver = importlib.util.module_from_spec(spec)
spec.loader.exec_module(driver)


class CheckpointPerformanceDriver(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="checkpoint-perf-driver-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.prepared = self.root / "prepared"
        self.prepared.mkdir()
        (self.prepared / "provenance.json").write_text('{"fixture":true}\n')
        self.output = self.root / "observations"
        self.provenance = {"arch": platform.machine(), "prepared_cases": ["orchestrator.checkpoint.sh"]}
        def require(condition, message):
            if not condition:
                raise ValueError(message)
        self.inputs = types.SimpleNamespace(
            verify=Mock(return_value=self.provenance), require=require,
            HELPERS={"fixture": "FIXTURE_HELPER"}, IMAGE_VARIABLES={"fixture": "FIXTURE_IMAGE"},
            helper_variables=lambda _: {"fixture": "FIXTURE_HELPER"},
            digest=lambda path: hashlib.sha256(path.read_bytes()).hexdigest(),
            load_images=Mock(),
            case_environment=Mock(return_value={"BIN": str(self.prepared / "bin"),
                                               "E2E_LIB": str(self.prepared / "test/e2e/lib"),
                                               "FIXTURE_HELPER": str(self.prepared / "fixtures/helper")}))
        self.contract = types.SimpleNamespace(workspace_contract=Mock(return_value={"cases": {
            "orchestrator.checkpoint.sh": {"requires": ["kvm"]}}}))
        self.probe = types.SimpleNamespace(check=Mock(return_value={"kvm": "fixture"}))
        self.run = Mock(return_value=subprocess.CompletedProcess([], 0))

    def invoke(self, mode="checkpoint"):
        arguments = [str(DRIVER), "--workspace", str(self.prepared), "--mode", mode,
                     "--sizes", "0", "--out", str(self.output)]
        # Host boundaries are replaced; argument/environment selection and the
        # real driver's before/after verification and result writing execute.
        with patch.object(driver, "load", side_effect=[self.inputs, self.contract, self.probe]), \
             patch.object(driver.subprocess, "run", self.run), \
             patch.object(driver.sys, "argv", arguments), \
             patch.dict(os.environ, {"BASH_ENV": "/ambient", "FIXTURE_HELPER": "/host/helper",
                                     "FIXTURE_IMAGE": "ambient", "KUASAR_EXECUTE_LOCK_HELD": "1"}):
            return driver.main()

    def test_uses_verified_prepared_environment_and_separate_measurement_result(self):
        self.assertEqual(self.invoke("baseline-copy"), 0)
        self.assertEqual(self.inputs.verify.call_count, 2)
        self.probe.check.assert_called_once_with(["kvm"])
        env = self.run.call_args.kwargs["env"]
        self.assertEqual(env["BIN"], str(self.prepared / "bin"))
        self.assertEqual(env["FIXTURE_HELPER"], str(self.prepared / "fixtures/helper"))
        self.assertEqual(env["CHECKPOINT_PERF_MODE"], "baseline-copy")
        self.assertEqual(env["CHECKPOINT_MEASURE_GIB"], "0")
        for variable in ("BASH_ENV", "FIXTURE_IMAGE", "KUASAR_EXECUTE_LOCK_HELD"):
            self.assertNotIn(variable, env)
        result = json.loads((self.output / "measurement-result.json").read_text())
        self.assertEqual(result["kind"], "checkpoint-platform-measurement")
        self.assertEqual(result["conclusion"], "success")
        self.assertEqual(sorted(path.name for path in self.prepared.iterdir()), ["provenance.json"])

    def test_child_failure_still_verifies_and_records_failure(self):
        self.run.return_value = subprocess.CompletedProcess([], 23)
        with self.assertRaisesRegex(ValueError, "measurement failed"):
            self.invoke()
        self.assertEqual(self.inputs.verify.call_count, 2)
        result = json.loads((self.output / "measurement-result.json").read_text())
        self.assertEqual((result["conclusion"], result["exit_code"]), ("failure", 23))

    def test_post_run_input_mutation_cannot_report_success(self):
        self.inputs.verify.side_effect = [self.provenance, ValueError("modified prepared bytes")]
        with self.assertRaisesRegex(ValueError, "modified prepared bytes"):
            self.invoke()
        result = json.loads((self.output / "measurement-result.json").read_text())
        self.assertEqual(result["conclusion"], "failure")

    def test_output_under_prepared_is_rejected_before_any_child(self):
        self.output = self.prepared / "output"
        with self.assertRaisesRegex(ValueError, "outside immutable"):
            self.invoke()
        self.assertFalse(self.output.exists())
        self.run.assert_not_called()


if __name__ == "__main__":
    unittest.main()
