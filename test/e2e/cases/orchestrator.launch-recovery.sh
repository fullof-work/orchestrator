#!/usr/bin/env bash
# e2e-requires: ["bpf-map","bpf-tc","cgroup-v2","docker","kvm","netns","systemd","tun","userfaultfd"]
# Accepted startup retains its exact owner across observed Manifest failures >60s.
set -euo pipefail
. "${E2E_LIB:?}/orchestrator/execute_env.sh"

docker save "$E2E_IMAGE" | "$BIN/flatten-ctl" export --output "$WORK/recovery.image" --no-progress
ROOT=$(MANIFEST_KEY="$MK" "$BIN/manifest-ctl" store --manifest-config "$WORK/manifest.yaml" --no-progress "$WORK/recovery.image")
[ "${#ROOT}" = 64 ] || fail "invalid image root"
READ_PROXY_PORT=$(free_port)
python3 - "$WORK/read-mode.json" "$ROOT" <<'PY'
import json,pathlib,sys
pathlib.Path(sys.argv[1]).write_text(json.dumps({"mode":"block","key":sys.argv[2]}))
PY
python3 "$SCRIPT_DIR/store_read_fault_proxy.py" "$READ_PROXY_PORT" "$STORE_PORT"     "$WORK/read-mode.json" "$CASE_OUT/manifest-retry.json" >"$WORK/read-proxy.log" 2>&1 &
PIDS+=($!)
wait_port 127.0.0.1 "$READ_PROXY_PORT" read-proxy
python3 - "$WORK/manifest.yaml" "$STORE_PORT" "$READ_PROXY_PORT" <<'PY'
import pathlib,sys
p=pathlib.Path(sys.argv[1])
p.write_text(p.read_text().replace("127.0.0.1:"+sys.argv[2], "127.0.0.1:"+sys.argv[3]).replace("timeout: 30s","timeout: 1s"))
PY
write_orchestrator_config unset static
start_orchestrator "$WORK/orch.log"
wait_mmds_listener
"$BIN/node-ctl" manifest-key add --socket "$WORK/node-ctl.socket" "$MK" >/dev/null
BODY=$(python3 - "$ROOT" <<'PY'
import base64,json,sys
ref=base64.urlsafe_b64encode(("manifest://"+sys.argv[1]).encode()).decode().rstrip("=")
print(json.dumps({"templateID":"bare-img-"+ref,"timeout":180}))
PY
)
REQ_RESOURCE_HEADER='{"capacity":{"cpu":1,"memory":"512MiB"}}'
code=$(req POST /sandboxes "$AK" "$BODY")
unset REQ_RESOURCE_HEADER
[ "$code" = 201 ] || { cat "$WORK/resp.body"; fail "Create was not accepted: $code"; }
SID=$(json_field "$WORK/resp.body" sandboxID)
# Poll only observed selected Get failures. Every sample simultaneously checks
# exact durable/run/process/network ownership; wall time alone cannot pass.
python3 - "$WORK" "$SID" "$RUNNER_PREFIX" "$CASE_OUT" <<'PY'
import json,pathlib,sqlite3,subprocess,sys,time
work,sid,prefix,out=sys.argv[1:]; work=pathlib.Path(work);out=pathlib.Path(out)
first=None
limit=time.monotonic()+100
while time.monotonic()<limit:
    path=out/"manifest-retry.json"
    if path.exists():
        evidence=json.loads(path.read_text())
        with sqlite3.connect(work/"lib/node-ctl.db",timeout=5) as db:
            row=db.execute("select state,run_id,vswitch_port,floatingip from sandboxes where id=?",(sid,)).fetchone()
        assert row and row[0]=="starting" and row[1],row
        unit=prefix+row[1]+".service"
        props=subprocess.check_output(["systemctl","show",unit,"--property=MainPID,ActiveState"],text=True)
        props=dict(s.split("=",1) for s in props.splitlines())
        assert props["ActiveState"]=="active",props
        pid=int(props["MainPID"])
        stat=pathlib.Path(f"/proc/{pid}/stat").read_text().rpartition(")")[2].split()
        identity={"sid":sid,"run_id":row[1],"unit":unit,"pid":pid,"start":int(stat[19]),"port":row[2],"floatingip":row[3]}
        if first is None:first=identity
        assert identity==first,(first,identity)
        if evidence["count"]>=4 and evidence["last_ns"]-evidence["first_ns"]>61_000_000_000:
            (out/"accepted-owner.json").write_text(json.dumps(first))
            break
    time.sleep(.05)
else:
    raise SystemExit("selected real Get failures did not span the former 60-second window")
PY
python3 - "$WORK/read-mode.json" <<'PY'
import json,pathlib,sys
p=pathlib.Path(sys.argv[1]);v=json.loads(p.read_text());v["mode"]="healthy"
q=p.with_suffix(".next");q.write_text(json.dumps(v));q.replace(p)
PY
wait_sandbox_state "$SID" running 600 || fail "original accepted launch did not recover"
python3 "$SCRIPT_DIR/runner_lifecycle.py" --run-root "$EXECUTE_RUN_ROOT" snapshot "$WORK" "$SID" "$RUNNER_PREFIX" "$CASE_OUT/running-owner.json"
python3 - "$WORK" "$CASE_OUT" <<'PY'
import json,pathlib,sqlite3,sys
work,out=map(pathlib.Path,sys.argv[1:])
old=json.loads((out/"accepted-owner.json").read_text());new=json.loads((out/"running-owner.json").read_text())
assert old["sid"]==new["sid"] and old["run_id"]==new["run_id"] and old["unit"]==new["unit"]
parent=new["processes"]["parent"]
assert parent["pid"]==old["pid"] and parent["start_ticks"]==old["start"], (old,parent)
with sqlite3.connect(work/"lib/node-ctl.db") as db:
    row=db.execute("select vswitch_port,floatingip from sandboxes where id=?",(old["sid"],)).fetchone()
assert tuple(row)==(old["port"],old["floatingip"]),row
PY
code=$(req DELETE "/sandboxes/$SID" "$AK")
[ "$code" = 204 ] || fail "Kill after recovery=$code"
wait_sandbox_state "$SID" missing 120 || fail "recovered sandbox did not clean up"
echo "PASS orchestrator.launch-recovery.sh"
