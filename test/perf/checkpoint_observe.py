"""Observe an actual guest command without changing the prepared helper inputs."""
import argparse
import importlib.util
import json
from pathlib import Path
import sqlite3
import subprocess
import threading
import time


def counter_delta(before, after, name):
    if before.get("run_id") != after.get("run_id") or before.get("runtime_pids") != after.get("runtime_pids"):
        return None
    start, end = before.get(name), after.get(name)
    return end - start if start is not None and end is not None and end >= start else None


def summarize(samples):
    before, after = samples[0], samples[-1]
    stable = all(before["run_id"] == row["run_id"] and before["runtime_pids"] == row["runtime_pids"] for row in samples)
    io_delta = {}
    if stable:
        for device, counters in after.get("runtime_cgroup_io_stat", {}).items():
            previous = before.get("runtime_cgroup_io_stat", {}).get(device, {})
            io_delta[device] = {key: value - previous.get(key, 0) if value >= previous.get(key, 0) else None
                                for key, value in counters.items()}
    return {"runtime_identity_stable": stable,
            "runtime_io_write_bytes_delta": counter_delta(before, after, "runtime_io_write_bytes") if stable else None,
            "runtime_io_cancelled_write_bytes_delta": counter_delta(before, after, "runtime_io_cancelled_write_bytes") if stable else None,
            "runtime_cgroup_io_delta": io_delta if stable else None,
            "runtime_rss_max_sample_bytes": max(row["runtime_rss_bytes"] for row in samples),
            "runtime_cgroup_max_sample_bytes": max(row["runtime_cgroup_bytes"] for row in samples),
            "base_allocated_bytes_before": before["base_dir"]["allocated_bytes"],
            "base_allocated_bytes_after": after["base_dir"]["allocated_bytes"],
            "base_allocated_bytes_delta": after["base_dir"]["allocated_bytes"] - before["base_dir"]["allocated_bytes"],
            "diff_allocated_bytes_before": before.get("diff_allocated_bytes", 0),
            "diff_allocated_bytes_after": after.get("diff_allocated_bytes", 0),
            "diff_allocated_bytes_delta": after.get("diff_allocated_bytes", 0) - before.get("diff_allocated_bytes", 0),
            "run_allocated_bytes_before": before["run_dir"]["allocated_bytes"],
            "run_allocated_bytes_after": after["run_dir"]["allocated_bytes"]}


def observe(helper, database, sid, prefix, base):
    with sqlite3.connect(database, timeout=5) as db:
        row = db.execute("select run_id, run_dir from sandboxes where id=?", (sid,)).fetchone()
    if not row or not row[0]:
        raise ValueError("workload observation requires an actual live runtime")
    memory = helper.runtime_memory(database, sid, prefix)
    if not memory.get("runtime_pids") or "runtime_io_write_bytes" not in memory:
        raise ValueError("runtime PID/I/O observations are unavailable")
    diff_dir = Path(base) / "checkpoint" / "diffs"
    disks = helper.disk_state(diff_dir) if list(diff_dir.glob("*.diff")) else []
    return {"time_monotonic": time.monotonic(), "run_id": row[0],
            "base_dir": helper.tree_allocation(base), "run_dir": helper.tree_allocation(row[1]),
            "diffs": disks, "diff_allocated_bytes": sum(disk["allocated_bytes"] for disk in disks),
            **memory}


def execute(helper, args):
    output = args.output
    output.parent.mkdir(parents=True, exist_ok=True)
    samples, errors = [], []
    stop = threading.Event()
    def sample():
        samples.append(observe(helper, args.database, args.sid, args.prefix, args.base))
    sample()
    def sampler():
        while not stop.wait(0.25):
            try:
                sample()
            except Exception as error:
                errors.append(repr(error))
                return
    thread = threading.Thread(target=sampler, daemon=True)
    thread.start()
    started = time.monotonic()
    try:
        completed = subprocess.run(args.command, text=True, capture_output=True, check=False)
    finally:
        stop.set()
        thread.join()
    elapsed = time.monotonic() - started
    sample()
    output.with_suffix(".stdout").write_text(completed.stdout)
    output.with_suffix(".stderr").write_text(completed.stderr)
    report = {"label": args.label, "host_elapsed_seconds": elapsed, "exit_code": completed.returncode,
              "sampling_seconds": 0.25, "samples": samples, "sampling_errors": errors,
              "scope": "live runtime unit and descendants; I/O includes background work in this window; sampled memory is not peak; counters are not a write-amplification ratio",
              "host": summarize(samples)}
    if completed.returncode == 0:
        report["guest"] = json.loads(completed.stdout)
    output.write_text(json.dumps(report, sort_keys=True) + "\n")
    if completed.returncode:
        raise RuntimeError(f"guest command failed ({completed.returncode}): {completed.stderr}")
    if errors or not report["host"]["runtime_identity_stable"]:
        raise RuntimeError("runtime changed or host sampling failed; measurement is not comparable")
    print(json.dumps({"label": args.label, "host": report["host"], "guest": report["guest"]}, sort_keys=True))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lib", type=Path, required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--sid", required=True)
    parser.add_argument("--prefix", required=True)
    parser.add_argument("--base", required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if args.command[:1] == ["--"]:
        args.command = args.command[1:]
    if not args.command:
        parser.error("a guest command is required")
    spec = importlib.util.spec_from_file_location("prepared_checkpoint_observation", args.lib / "disk_checkpoint.py")
    helper = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(helper)
    execute(helper, args)


if __name__ == "__main__":
    import sys
    sys.dont_write_bytecode = True
    main()
