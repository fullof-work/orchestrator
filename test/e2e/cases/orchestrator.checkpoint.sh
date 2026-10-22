#!/usr/bin/env bash
# e2e-requires: ["bpf-map","bpf-tc","cgroup-v2","docker","journald","kvm","netns","systemd","tun","userfaultfd"]
# 8 MiB correctness/retention workload. Comparative measurements live in
# test/perf/checkpoint_platform.sh and require explicitly selected prepared inputs.
set -euo pipefail
. "${E2E_LIB:?}/orchestrator/execute_env.sh"
. "$SCRIPT_DIR/execute_checkpoint.sh"
unset CHECKPOINT_PERF_MODE
write_orchestrator_config unset controller
# This serial storage scenario builds a 6 GiB template. Admit that single
# startup on 12 GiB developer hosts without changing production defaults or
# the controller's measured physical-memory limit.
sed -i '/^resource_listen:$/a\  watermarks: { startup_factor: 0.8 }' "$WORK/config.yaml"
start_orchestrator "$WORK/orch.log"
wait_mmds_listener
"$BIN/node-ctl" manifest-key add --socket "$WORK/node-ctl.socket" "$MK" >/dev/null
. "$SCRIPT_DIR/execute_template.sh"
build_ready_template
python3 - "$RUN_ARGV_LOG" "$SNAPSHOT_ARGV_LOG" <<'PY'
import json, sys
runs, captures = ([json.loads(line) for line in open(path) if line.strip()] for path in sys.argv[1:])
assert runs and captures, (runs, captures)
phases = set()
for args in runs:
    assert args.count('--enable-checkpoint') == 1, args
    phases.add(args[args.index('--path-id') + 1])
assert {'a', 'c'} <= phases, phases
for args in captures:
    assert '--checkpoint' not in args, args
    assert args[args.index('--path-id') + 1] == 'c', args
PY

create_checkpoint_guest 8
write_checkpoint_payload 8
checkpoint_hash >"$WORK/checkpoint-original.hash"
checkpoint_disk_state "$SID" "$WORK/checkpoint-original.disks"
checkpoint_measure before_pause "$(checkpoint_clock)" 8
pause_checkpoint_guest snapshot 8
checkpoint_local_metadata "$SID" "$WORK/checkpoint-first.json"
checkpoint_disk_state "$SID" "$WORK/checkpoint-paused.disks"
checkpoint_same_disks "$WORK/checkpoint-original.disks" "$WORK/checkpoint-paused.disks"
resume_checkpoint_guest 8
checkpoint_wait_released "$SID" || fail "successful memory resume did not release versions"
checkpoint_hash >"$WORK/checkpoint-first-resume.hash"
cmp "$WORK/checkpoint-original.hash" "$WORK/checkpoint-first-resume.hash"
checkpoint_disk_state "$SID" "$WORK/checkpoint-resumed.disks"
checkpoint_same_disks "$WORK/checkpoint-original.disks" "$WORK/checkpoint-resumed.disks"

pause_checkpoint_guest sandbox 8
checkpoint_local_metadata "$SID" "$WORK/checkpoint-second.json"
python3 - "$WORK/checkpoint-first.json" "$WORK/checkpoint-second.json" <<'PY'
import json, sys
before, after = (json.load(open(path)) for path in sys.argv[1:])
assert before['checkpoint_id'] != after['checkpoint_id'], (before, after)
assert before['root'] == after['root'] and (before.get('disks') or []) == (after.get('disks') or []), (before, after)
assert not __import__('os').path.exists(before['artifact']), 'obsolete E was not selectively cleaned'
PY

# Hold an admitted real publisher before it reads the source. Resume and guest
# writes proceed, but the fixed version must survive until publication exits.
printf '%s\n' "$CHECKPOINT_ROOT/$SID/checkpoint" >"$PUBLISH_BARRIER_TARGET"
PUBLISH_STARTED=$(checkpoint_clock)
E2B_API_KEY="$AK" timeout -k 5s 300 "$ORCH_BIN_DIR/node-ctl" export-sandbox "$SID" \
    --keep-source --to-template --json --socket "$WORK/node-ctl.socket" \
    >"$WORK/checkpoint-publish.json" 2>"$WORK/checkpoint-publish.log" &
PUBLISH_PID=$!
PIDS+=("$PUBLISH_PID")
for _ in $(seq 1 300); do
    [ ! -e "$PUBLISH_BARRIER_REACHED" ] || break
    kill -0 "$PUBLISH_PID" 2>/dev/null || { cat "$WORK/checkpoint-publish.log"; fail "publisher exited before barrier"; }
    sleep 0.1
done
[ -e "$PUBLISH_BARRIER_REACHED" ] || fail "publisher never acquired source ownership"
resume_checkpoint_guest 8
checkpoint_disk_state "$SID" "$WORK/checkpoint-cold-resumed.disks"
checkpoint_same_disks "$WORK/checkpoint-original.disks" "$WORK/checkpoint-cold-resumed.disks"
checkpoint_hash >"$WORK/checkpoint-cold-resume.hash"
cmp "$WORK/checkpoint-original.hash" "$WORK/checkpoint-cold-resume.hash"
checkpoint_list "$SID" "$WORK/checkpoint-held.json"
python3 - "$WORK/checkpoint-second.json" "$WORK/checkpoint-held.json" <<'PY'
import json, sys
meta, refs = (json.load(open(path)) for path in sys.argv[1:])
want = {(disk['diff_id'], meta['checkpoint_id']) for disk in [meta['root'], *(meta.get('disks') or [])]}
assert {(ref['diff_id'], ref['id']) for ref in refs} == want, (refs, want)
PY
write_checkpoint_payload 8
checkpoint_hash >"$WORK/checkpoint-active.hash"
if cmp -s "$WORK/checkpoint-original.hash" "$WORK/checkpoint-active.hash"; then fail "overwrite did not change data"; fi
RETAINED_BYTES=$(checkpoint_allocated "$SID")
checkpoint_measure export_held_after_overwrite "$(checkpoint_clock)" 8
: >"$PUBLISH_BARRIER_RELEASE"
wait "$PUBLISH_PID" || { cat "$WORK/checkpoint-publish.log"; fail "detached template publication failed"; }
for index in "${!PIDS[@]}"; do
    [ "${PIDS[$index]}" != "$PUBLISH_PID" ] || unset 'PIDS[index]'
done
rm -f "$PUBLISH_BARRIER_TARGET" "$PUBLISH_BARRIER_REACHED" "$PUBLISH_BARRIER_RELEASE"
checkpoint_measure publish_including_test_barrier "$PUBLISH_STARTED" 8
checkpoint_wait_released "$SID" || fail "publisher completion did not release versions"

# Background GC is bounded. Require actual allocated-byte reduction, allowing
# up to three minutes for reclamation; List=[] alone cannot pass this check.
GC_STARTED=$(checkpoint_clock)
RECLAIMED=0
for _ in $(seq 1 180); do
    CURRENT_BYTES=$(checkpoint_allocated "$SID")
    if [ "$CURRENT_BYTES" -le "$((RETAINED_BYTES - 4 * 1024 * 1024))" ]; then RECLAIMED=1; break; fi
    sleep 1
done
[ "$RECLAIMED" = 1 ] || fail "GC did not reclaim 4 MiB of retained overwrite: before=$RETAINED_BYTES after=$CURRENT_BYTES"
checkpoint_measure after_version_gc "$GC_STARTED" 8
checkpoint_hash >"$WORK/checkpoint-after-gc.hash"
cmp "$WORK/checkpoint-active.hash" "$WORK/checkpoint-after-gc.hash"

PUBLISHED_TEMPLATE=$(json_field "$WORK/checkpoint-publish.json" result)
PUBLISHED_E=$(json_field "$WORK/checkpoint-publish.json" sandboxRef)
MANIFEST_KEY="$MK" "$BIN/sandbox-ctl" info --json --manifest-config "$WORK/manifest.yaml" \
    "$PUBLISHED_E" >"$WORK/checkpoint-portable.json"
python3 - "$WORK/checkpoint-portable.json" <<'PY'
import json, sys
cfg = json.load(open(sys.argv[1]))
root = cfg['Boot']['Root']
nodes = [root, *(cfg['Boot'].get('Disks') or [])]
# The reader validates ordinary E's one-self binding; Checkpoint E has no self.
self_count = sum(node.get('Base') == 'self' for node in nodes)
self_count += sum((node.get('Overlay') or {}).get('Base') == 'self' for node in nodes)
assert self_count == 1, cfg['Boot']
PY
delete_checkpoint_guest
TEMPLATE="$PUBLISHED_TEMPLATE"
create_checkpoint_guest 8
checkpoint_hash >"$WORK/checkpoint-published.hash"
cmp "$WORK/checkpoint-original.hash" "$WORK/checkpoint-published.hash"
delete_checkpoint_guest
echo "==> PASS: named versions, same-directory reuse, export overlap, physical GC and source-independent cold restore"

echo "PASS orchestrator.checkpoint.sh"
