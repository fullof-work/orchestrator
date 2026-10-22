#!/usr/bin/env python3
"""Run a separate Checkpoint platform measurement from verified prepared inputs."""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import time


def load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--mode", choices=("baseline-copy", "checkpoint"), required=True)
    parser.add_argument("--sizes", choices=("0", "1", "0,1"), default="0,1")
    parser.add_argument("--out", type=Path, required=True, help="new output directory outside prepared inputs")
    args = parser.parse_args()
    prepared, output = args.workspace.resolve(), args.out.resolve()
    lib = prepared / "test/e2e/lib"
    sys.path.insert(0, str(lib))
    inputs = load(lib / "workspace.py", "checkpoint_prepared_inputs")
    provenance = inputs.verify(prepared)
    inputs.require(platform.machine() == provenance["arch"], "native prepared architecture required")
    inputs.require(not output.is_relative_to(prepared), "output must be outside immutable prepared inputs")
    inputs.require(not output.exists(), "output directory must be new")
    case = "orchestrator.checkpoint.sh"
    inputs.require(case in provenance["prepared_cases"], "prepare orchestrator.checkpoint.sh dependencies first")
    requirements = load(lib / "capabilities.py", "checkpoint_requirements")
    probe = load(lib / "capability_probe.py", "checkpoint_capability_probe")
    contract = requirements.workspace_contract(prepared, provenance)
    inputs.require(contract is not None, "prepared capability contract required")
    environment = os.environ.copy()
    for variable in (*inputs.HELPERS.values(), *inputs.helper_variables(provenance).values(),
                     *inputs.IMAGE_VARIABLES.values(), "E2E_HELPERS_DIR", "E2E_IMAGE", "IMAGE",
                     "E2E_CASES_DIR", "E2E_LIB", "BASH_ENV", "KUASAR_EXPECTED_RUNTIME_INIT_SHA256",
                     "E2E_TEMPLATE_CACHE_DIR", "E2E_TEMPLATE_CACHE_PROVENANCE", "KUASAR_EXECUTE_LOCK_HELD"):
        environment.pop(variable, None)
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    output.mkdir(parents=True)
    work, observations = output / "work", output / "observations"
    work.mkdir()
    observations.mkdir()
    script = Path(__file__).with_name("checkpoint_platform.sh").resolve()
    record = {"kind": "checkpoint-platform-measurement", "mode": args.mode, "sizes_gib": args.sizes,
              "prepared": str(prepared), "provenance_sha256": inputs.digest(prepared / "provenance.json"),
              "driver_sha256": inputs.digest(Path(__file__)), "script_sha256": inputs.digest(script),
              "conclusion": "failure"}
    started = time.monotonic()
    try:
        record["capabilities"] = probe.check(contract["cases"][case]["requires"])
        inputs.load_images(prepared, provenance, environment)
        environment.update(inputs.case_environment(prepared, provenance, case))
        environment.update(WORK=str(work), OUT=str(observations), SANDBOX_BASE_ROOT=str(work / "sandbox-base"),
                           CHECKPOINT_PERF_MODE=args.mode, CHECKPOINT_MEASURE_GIB=args.sizes)
        completed = subprocess.run(["bash", str(script)], cwd=work, env=environment, check=False)
        record["exit_code"] = completed.returncode
        inputs.require(completed.returncode == 0, "Checkpoint platform measurement failed")
        record["conclusion"] = "success"
    finally:
        try:
            inputs.verify(prepared)
        except Exception:
            record["conclusion"] = "failure"
            raise
        finally:
            record["wall_seconds"] = time.monotonic() - started
            (output / "measurement-result.json").write_text(json.dumps(record, indent=2, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    sys.dont_write_bytecode = True
    raise SystemExit(main())
