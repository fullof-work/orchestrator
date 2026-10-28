#!/usr/bin/env python3
"""Observe a real accepted read failure and retain exact attempt ownership."""
import json
import pathlib
import sqlite3
import subprocess
import sys
import time

from runner_lifecycle import process, snapshot, same_process

action, work, sid, prefix, out, run_root, phase = sys.argv[1:]
work, out = pathlib.Path(work), pathlib.Path(out)


def calls():
    path = work / "run-argv.jsonl"
    if not path.exists():
        return 0
    def option(args, name):
        for i, arg in enumerate(args):
            if arg == name:
                return args[i + 1]
            if arg.startswith(name + "="):
                return arg.split("=", 1)[1]
    return sum(option(json.loads(line), "--sandbox-id") == sid
               for line in path.read_text().splitlines())


def network():
    with sqlite3.connect((work / "lib/node-ctl.db").as_uri() + "?mode=ro", uri=True, timeout=5) as db:
        return db.execute("select state,run_id,vswitch_port,floatingip from sandboxes where id=?", (sid,)).fetchone()


if action == "watch":
    first = None
    limit = time.monotonic() + 100
    while time.monotonic() < limit:
        path = out / "manifest-retry.json"
        evidence = json.loads(path.read_text()) if path.exists() else {}
        if evidence.get("epoch") == phase:
            row = network()
            assert row and row[0] == "starting" and row[1], row
            unit = prefix + row[1] + ".service"
            props = subprocess.check_output(["systemctl", "show", unit, "--property=MainPID,ActiveState"], text=True)
            props = dict(s.split("=", 1) for s in props.splitlines())
            assert props["ActiveState"] == "active", props
            pid = int(props["MainPID"])
            parent = process(pid)
            children = set()
            for task in pathlib.Path(f"/proc/{pid}/task").iterdir():
                try:
                    children.update((task / "children").read_text().split())
                except FileNotFoundError:
                    pass
            runtimes = []
            for child in children:
                try:
                    item = process(int(child))
                except (FileNotFoundError, ProcessLookupError):
                    continue
                if item["executable"] == "sandbox-ctl":
                    runtimes.append(item)
            # Image opens inside the original sandbox-ctl. E/S resolve in
            # resident taskrun before any sandbox-ctl or VM may be spawned.
            expected = int(phase == "image")
            assert len(runtimes) == expected and calls() == expected, (runtimes, calls())
            identity = dict(sid=sid, run_id=row[1], unit=unit, parent=parent,
                            runtime=runtimes[0] if runtimes else None,
                            port=row[2], floatingip=row[3])
            if first is None:
                first = identity
            assert identity == first, (first, identity)
            if evidence["count"] >= 4 and evidence["last_ns"] - evidence["first_ns"] > 61_000_000_000:
                (out / (phase + "-waiting.json")).write_text(json.dumps(first))
                (out / (phase + "-faults.json")).write_text(json.dumps(evidence))
                break
        time.sleep(.05)
    else:
        raise SystemExit("selected real Get failures did not span the former 60-second window")
elif action == "compare":
    old = json.loads((out / (phase + "-waiting.json")).read_text())
    new = snapshot(work, sid, prefix, run_root)
    assert all(old[k] == new[k] for k in ("sid", "run_id", "unit")), (old, new)
    assert same_process(old["parent"], new["processes"]["parent"]), (old, new)
    if old["runtime"] is not None:
        assert same_process(old["runtime"], new["processes"]["runtime"]), (old, new)
    row = network()
    if phase == "image":
        assert tuple(row[2:]) == (old["port"], old["floatingip"]), row
    assert row[2] and row[3] and calls() == 1, (row, calls())
    (out / (phase + "-running.json")).write_text(json.dumps(new))
else:
    raise SystemExit("expected watch or compare")
