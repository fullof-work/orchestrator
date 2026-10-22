"""Read public Checkpoint artifacts and host observations for platform E2E."""
import argparse
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import tarfile
import time
import uuid
import zipfile


def checkpoint_id(raw):
    parsed = uuid.UUID(raw)
    assert parsed.version == 7 and parsed.variant == uuid.RFC_4122 and str(parsed) == raw, raw
    return raw


def local_artifact(database, sid):
    with sqlite3.connect(database, timeout=5) as db:
        row = db.execute("select state, base_dir, resume_source_kind, resume_source_ref, resume_sandbox_ref from sandboxes where id=?", (sid,)).fetchone()
    assert row and row[0] == "paused" and row[2] in ("sandbox", "snapshot"), row
    ref = row[4] if row[2] == "snapshot" else row[3]
    assert ref.startswith("file://") and "@digest:" in ref, ref
    name = ref.removeprefix("file://").split("@", 1)[0]
    assert name == os.path.basename(name) and name.endswith(".sandbox"), ref
    path = Path(row[1], "checkpoint", name)
    with tarfile.open(path) as carrier:
        members = [m for m in carrier if m.isfile() and not m.name.startswith(".kuasar.")]
        assert len(members) == 1, members
        member = members[0]
        member.size = int(member.pax_headers.get("GNU.sparse.realsize", member.size))
        with zipfile.ZipFile(carrier.extractfile(member)) as logical:
            names = logical.namelist()
            assert len(names) == len(set(names)), names
            result = json.loads(logical.read("checkpoint.json")) if "checkpoint.json" in names else None
    return path, names, result


def local_metadata(database, sid):
    path, names, result = local_artifact(database, sid)
    assert set(names) == {"sandbox.runtime.cfg", "checkpoint.json"}, names
    assert result["version"] == 1, result
    checkpoint_id(result["checkpoint_id"])
    disks = [result["root"], *(result.get("disks") or [])]
    ids = [disk["diff_id"] for disk in disks]
    assert len(ids) == len(set(ids)) and all(value and value not in (".", "..") and "/" not in value and "\\" not in value for value in ids), result
    result["artifact"] = str(path)
    return result


def disk_state(directory):
    root = Path(directory)
    result = []
    for path in sorted(root.glob("*.diff")):
        assert path.is_file() and not path.is_symlink(), path
        st = path.stat()
        result.append({"name": path.name, "device": st.st_dev, "inode": st.st_ino,
                       "logical_file_bytes": st.st_size, "allocated_bytes": st.st_blocks * 512})
    assert result, f"no persistent DIFF in {root}"
    return result


def runtime_memory(database, sid, prefix):
    with sqlite3.connect(database, timeout=5) as db:
        row = db.execute("select run_id from sandboxes where id=?", (sid,)).fetchone()
    if not row or not row[0]:
        return {"runtime_rss_bytes": 0, "runtime_cgroup_bytes": 0, "runtime_pids": []}
    group = subprocess.check_output(["systemctl", "show", "--value", "-p", "ControlGroup", prefix + row[0] + ".service"], text=True).strip()
    if not group:
        return {"runtime_rss_bytes": 0, "runtime_cgroup_bytes": 0, "runtime_pids": []}
    root = Path("/sys/fs/cgroup", group.lstrip("/"))
    pids = set()
    for procs in root.rglob("cgroup.procs"):
        try:
            pids.update(int(pid) for pid in procs.read_text().split())
        except FileNotFoundError:
            pass
    rss, write_bytes, cancelled_write_bytes = 0, 0, 0
    for pid in pids:
        try:
            for line in Path(f"/proc/{pid}/status").read_text().splitlines():
                if line.startswith("VmRSS:"):
                    rss += int(line.split()[1]) * 1024
            counters = dict(line.split(": ", 1) for line in Path(f"/proc/{pid}/io").read_text().splitlines())
            write_bytes += int(counters.get("write_bytes", 0))
            cancelled_write_bytes += int(counters.get("cancelled_write_bytes", 0))
        except (FileNotFoundError, ProcessLookupError):
            pass
    try:
        current = int((root / "memory.current").read_text())
    except FileNotFoundError:
        current = 0
    try:
        io_stat = {row[0]: {field.split("=", 1)[0]: int(field.split("=", 1)[1]) for field in row[1:]}
                   for row in (line.split() for line in (root / "io.stat").read_text().splitlines()) if row}
    except FileNotFoundError:
        io_stat = {}
    return {"runtime_rss_bytes": rss, "runtime_cgroup_bytes": current, "runtime_pids": sorted(pids),
            "runtime_io_write_bytes": write_bytes, "runtime_io_cancelled_write_bytes": cancelled_write_bytes,
            "runtime_cgroup_io_stat": io_stat}


def tree_allocation(directory):
    """du-like allocated bytes, deduplicating inodes and never following symlinks."""
    seen, allocated, logical = set(), 0, 0
    for root, _, files in os.walk(directory, followlinks=False):
        for name in files:
            path = Path(root, name)
            try:
                if path.is_symlink() or not path.is_file():
                    continue
                value = path.stat()
            except FileNotFoundError:
                continue
            key = (value.st_dev, value.st_ino)
            if key not in seen:
                seen.add(key)
                allocated += value.st_blocks * 512
                logical += value.st_size
    return {"allocated_bytes": allocated, "logical_file_bytes": logical, "files": len(seen)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    meta = sub.add_parser("metadata")
    meta.add_argument("database")
    meta.add_argument("sid")
    disks = sub.add_parser("disks")
    disks.add_argument("directory")
    layout = sub.add_parser("layout")
    layout.add_argument("database")
    layout.add_argument("sid")
    layout.add_argument("mode", choices=("checkpoint", "baseline-copy"))
    measure = sub.add_parser("measure")
    for field in ("output", "database", "sid", "prefix", "directory", "phase", "started", "payload_mib"):
        measure.add_argument(field)
    measure.add_argument("--mode", choices=("checkpoint", "baseline-copy"), default="checkpoint")
    args = parser.parse_args()
    if args.command == "metadata":
        print(json.dumps(local_metadata(args.database, args.sid), sort_keys=True))
    elif args.command == "disks":
        print(json.dumps(disk_state(args.directory), sort_keys=True))
    elif args.command == "layout":
        if args.mode == "checkpoint":
            print(json.dumps(local_metadata(args.database, args.sid), sort_keys=True))
        else:
            path, names, result = local_artifact(args.database, args.sid)
            assert result is None and "checkpoint.json" not in names, names
            print(json.dumps({"artifact": str(path), "members": names}, sort_keys=True))
    else:
        base = Path(args.directory)
        diff_dir = base / "checkpoint" / "diffs"
        disks = disk_state(diff_dir) if args.mode == "checkpoint" else []
        if args.mode == "baseline-copy":
            assert not list(diff_dir.glob("*.diff")), "baseline unexpectedly uses Checkpoint DIFF"
        with sqlite3.connect(args.database, timeout=5) as db:
            run = db.execute("select run_dir from sandboxes where id=?", (args.sid,)).fetchone()
        run_dir = tree_allocation(run[0]) if run and run[0] else {"allocated_bytes": 0, "logical_file_bytes": 0, "files": 0}
        record = {"mode": args.mode, "phase": args.phase, "payload_mib": int(args.payload_mib), "sid": args.sid,
                  "elapsed_seconds": max(0, time.monotonic() - float(args.started)),
                  "time_unix": time.time(), "disks": disks,
                  "base_dir": tree_allocation(base), "run_dir": run_dir,
                  "diff_allocated_bytes": sum(disk["allocated_bytes"] for disk in disks),
                  "diff_file_bytes": sum(disk["logical_file_bytes"] for disk in disks),
                  "memory_scope": "current runtime unit and descendants; sampled, not peak",
                  "io_scope": "current live runtime /proc/PID/io and cgroup io.stat counters; resets with runtime; not a write-amplification ratio",
                  **runtime_memory(args.database, args.sid, args.prefix)}
        with open(args.output, "a", encoding="utf-8") as out:
            out.write(json.dumps(record, sort_keys=True) + "\n")
        print(json.dumps(record, sort_keys=True))


if __name__ == "__main__":
    main()
