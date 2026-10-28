#!/usr/bin/env bash
# e2e-requires: ["bpf-map","bpf-tc","cgroup-v2","docker","kvm","netns","systemd","tun","userfaultfd"]
# Accepted cold image, E and S attempts survive observed Manifest faults >60s.
set -euo pipefail
. "${E2E_LIB:?}/orchestrator/execute_env.sh"

docker save "$E2E_IMAGE" | "$BIN/flatten-ctl" export --output "$WORK/recovery.image" --no-progress
ROOT=$(MANIFEST_KEY="$MK" "$BIN/manifest-ctl" store --manifest-config "$WORK/manifest.yaml" --no-progress "$WORK/recovery.image")
[ "${#ROOT}" = 64 ] || fail "invalid image root"
READ_PROXY_PORT=$(free_port)
printf '{"mode":"healthy"}\n' >"$WORK/read-mode.json"
python3 "$SCRIPT_DIR/store_read_fault_proxy.py" "$READ_PROXY_PORT" "$STORE_PORT" \
    "$WORK/read-mode.json" "$CASE_OUT/manifest-retry.json" >"$WORK/read-proxy.log" 2>&1 &
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

fault_mode() {
    python3 - "$WORK/read-mode.json" "$@" <<'PY'
import json,pathlib,sys
p=pathlib.Path(sys.argv[1])
v=dict(mode=sys.argv[2],key=sys.argv[3],epoch=sys.argv[4])
q=p.with_suffix(".next");q.write_text(json.dumps(v));q.replace(p)
PY
}
launch_fault() { # kind, portable ref, blocked E/image Manifest key, phase
    local kind=$1 ref=$2 key=$3 phase=$4
    fault_mode block "$key" "$phase"
    BODY=$(python3 - "$kind" "$ref" <<'PY'
import base64,json,sys
ref=base64.urlsafe_b64encode(sys.argv[2].encode()).decode().rstrip("=")
print(json.dumps({"templateID":"bare-"+sys.argv[1]+"-"+ref,"timeout":180}))
PY
)
    REQ_RESOURCE_HEADER='{"capacity":{"cpu":1,"memory":"512MiB"}}'
    code=$(req POST /sandboxes "$AK" "$BODY")
    unset REQ_RESOURCE_HEADER
    [ "$code" = 201 ] || { cat "$WORK/resp.body"; fail "$phase Create was not accepted: $code"; }
    SID=$(json_field "$WORK/resp.body" sandboxID)
    python3 "$SCRIPT_DIR/launch_recovery.py" watch "$WORK" "$SID" "$RUNNER_PREFIX" "$CASE_OUT" "$EXECUTE_RUN_ROOT" "$phase"
    fault_mode healthy "$key" "$phase"
    wait_sandbox_state "$SID" running 600 || fail "$phase original accepted launch did not recover"
    python3 "$SCRIPT_DIR/launch_recovery.py" compare "$WORK" "$SID" "$RUNNER_PREFIX" "$CASE_OUT" "$EXECUTE_RUN_ROOT" "$phase"
}
delete_current() {
    code=$(req DELETE "/sandboxes/$SID" "$AK")
    [ "$code" = 204 ] || fail "Kill after recovery=$code"
    # The repository unit owns a 20-second Stop/kill budget. Use the existing
    # lifecycle completion bound; 12 seconds would abort its ordered cleanup.
    wait_sandbox_state "$SID" missing 1200 || fail "recovered sandbox did not clean up"
}

launch_fault img "manifest://$ROOT" "$ROOT" image
SOURCE_SID=$SID
# Capture succeeds normally. Fault injection starts only on later reads; this
# does not claim recovery of capture side effects (sandboxer#372).
code=$(req POST "/sandboxes/$SID/pause" "$AK" '{"memory":true}')
[ "$code" = 204 ] || fail "source capture=$code"
wait_paused_cleanup "$SID" || fail "source paused cleanup"
E2B_API_KEY="$AK" "$ORCH_BIN_DIR/node-ctl" export-sandbox "$SID" --keep-source --json \
    --socket "$WORK/node-ctl.socket" >"$WORK/published.json"
E_REF=$(json_field "$WORK/published.json" sandboxRef)
S_REF=$(json_field "$WORK/published.json" snapshotRef)
[[ "$E_REF" == manifest://* && "$S_REF" == manifest://* ]] || fail "publication did not yield E/S roots"
# S opens successfully, then its referenced E hits this same shared entry.
launch_fault sbx "$E_REF" "${E_REF#manifest://}" sandbox
delete_current
launch_fault snp "$S_REF" "${E_REF#manifest://}" snapshot
delete_current
fault_mode block "$ROOT" cancel
BODY=$(python3 - "$ROOT" <<'PY_CANCEL'
import base64,json,sys
ref=base64.urlsafe_b64encode(("manifest://"+sys.argv[1]).encode()).decode().rstrip("=")
print(json.dumps({"templateID":"bare-img-"+ref,"timeout":180}))
PY_CANCEL
)
REQ_RESOURCE_HEADER='{"capacity":{"cpu":1,"memory":"512MiB"}}'
code=$(req POST /sandboxes "$AK" "$BODY")
unset REQ_RESOURCE_HEADER
[ "$code" = 201 ] || fail "waiting Delete Create=$code"
SID=$(json_field "$WORK/resp.body" sandboxID)
python3 "$SCRIPT_DIR/launch_recovery.py" watch "$WORK" "$SID" "$RUNNER_PREFIX" "$CASE_OUT" "$EXECUTE_RUN_ROOT" cancel
delete_current
python3 "$SCRIPT_DIR/launch_recovery.py" deleted "$WORK" "$SID" "$RUNNER_PREFIX" "$CASE_OUT" "$EXECUTE_RUN_ROOT" cancel
fault_mode healthy "$ROOT" cancel
SID=$SOURCE_SID
delete_current
echo "PASS orchestrator.launch-recovery.sh: original cold/E/S accepted attempts recovered"
