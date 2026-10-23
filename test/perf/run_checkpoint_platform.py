#!/usr/bin/env python3
"""Run a separate Checkpoint platform measurement from verified prepared inputs."""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import time


def sizes(value):
    parts = value.split(",")
    if not parts or any(part not in ("0", "1", "10") for part in parts) or len(parts) != len(set(parts)):
        raise argparse.ArgumentTypeError("sizes must be distinct comma-separated values from 0,1,10; 30 GiB is not supported")
    return value


def resource_preflight(maximum_gib, output_parent, tempdir):
    # Baseline captures, publication and resumed files can coexist. Keep this
    # conservative and reject insufficient resources before starting real VMs.
    required = (6 * maximum_gib + 8) * 1024**3
    available = next(int(line.split()[1]) * 1024 for line in Path("/proc/meminfo").read_text().splitlines()
                     if line.startswith("MemAvailable:"))
    if available < 8 * 1024**3:
        raise ValueError("performance run requires at least 8 GiB MemAvailable")
    checks = []
    for label, path, minimum in (("case_data", Path(tempdir), required),
                                 ("runtime", Path("/tmp"), 2 * 1024**3),
                                 ("observations", output_parent, 256 * 1024**2)):
        free = shutil.disk_usage(path).free
        if free < minimum:
            raise ValueError(f"insufficient {label} disk space: {free} bytes free, require {minimum}")
        checks.append({"scope": label, "path": str(path), "free_bytes": free, "required_bytes": minimum})
    return {"mem_available_bytes": available, "mem_required_bytes": 8 * 1024**3,
            "cpu_count": os.cpu_count(), "disk_checks": checks, "maximum_payload_gib": maximum_gib}


def load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--mode", choices=("baseline-copy", "checkpoint"), required=True)
    parser.add_argument("--sizes", type=sizes, default="0,1", help="comma-separated 0,1,10; default 0,1")
    parser.add_argument("--layouts", choices=("dense", "sparse", "dense,sparse"), default="dense,sparse")
    parser.add_argument("--rounds", type=int, default=1, help="1..5 repetitions; combine with --skip-lifecycle for short write-load repeats")
    parser.add_argument("--operations", type=int, default=1024, help="128..16384 I/O operations per pattern/state")
    parser.add_argument("--skip-lifecycle", action="store_true", help="run only the 128 MiB retained/released fresh-region workloads")
    parser.add_argument("--skip-workloads", action="store_true", help="run lifecycle samples without the short write-load comparison")
    parser.add_argument("--out", type=Path, required=True, help="new output directory outside prepared inputs")
    args = parser.parse_args()
    if not 1 <= args.rounds <= 5 or not 128 <= args.operations <= 16384 or args.skip_lifecycle and args.skip_workloads:
        parser.error("rounds must be 1..5, operations 128..16384, and at least one measurement group must run")
    prepared, output = args.workspace.resolve(), args.out.resolve()
    lib = prepared / "test/e2e/lib"
    sys.path.insert(0, str(lib))
    inputs = load(lib / "workspace.py", "checkpoint_prepared_inputs")
    provenance = inputs.verify(prepared)
    inputs.require(platform.machine() == provenance["arch"], "native prepared architecture required")
    inputs.require(not output.is_relative_to(prepared), "output must be outside immutable prepared inputs")
    inputs.require(not output.exists(), "output directory must be new")
    maximum = max(map(int, args.sizes.split(","))) if not args.skip_lifecycle else 0
    resources = resource_preflight(maximum, output.parent, os.environ.get("TMPDIR", "/tmp"))
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
              "layouts": args.layouts, "rounds": args.rounds, "operations": args.operations, "resources": resources,
              "prepared": str(prepared), "provenance_sha256": inputs.digest(prepared / "provenance.json"),
              "driver_sha256": inputs.digest(Path(__file__)), "script_sha256": inputs.digest(script),
              "conclusion": "failure"}
    started = time.monotonic()
    try:
        record["capabilities"] = probe.check(contract["cases"][case]["requires"])
        inputs.load_images(prepared, provenance, environment)
        environment.update(inputs.case_environment(prepared, provenance, case))
        environment.update(WORK=str(work), OUT=str(observations), SANDBOX_BASE_ROOT=str(work / "sandbox-base"),
                           CHECKPOINT_PERF_MODE=args.mode, CHECKPOINT_MEASURE_GIB=args.sizes,
                           CHECKPOINT_PERF_LAYOUTS=args.layouts, CHECKPOINT_PERF_ROUNDS=str(args.rounds),
                           CHECKPOINT_PERF_OPERATIONS=str(args.operations),
                           CHECKPOINT_PERF_SKIP_LIFECYCLE=str(int(args.skip_lifecycle)),
                           CHECKPOINT_PERF_SKIP_WORKLOADS=str(int(args.skip_workloads)))
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
