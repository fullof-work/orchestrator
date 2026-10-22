#!/usr/bin/env bash
# Optional platform comparison using separately verified prepared workspaces.
# Uses the same host capabilities as orchestrator.checkpoint.sh.
set -euo pipefail
: "${CHECKPOINT_PERF_MODE:?set baseline-copy or checkpoint explicitly}"
case "$CHECKPOINT_PERF_MODE" in baseline-copy|checkpoint) ;; *) exit 2 ;; esac
CHECKPOINT_MEASURE_GIB="${CHECKPOINT_MEASURE_GIB:-0,1}"
CHECKPOINT_PERF_LAYOUTS="${CHECKPOINT_PERF_LAYOUTS:-dense,sparse}"
CHECKPOINT_PERF_ROUNDS="${CHECKPOINT_PERF_ROUNDS:-1}"
CHECKPOINT_PERF_OPERATIONS="${CHECKPOINT_PERF_OPERATIONS:-1024}"
CHECKPOINT_PERF_SKIP_LIFECYCLE="${CHECKPOINT_PERF_SKIP_LIFECYCLE:-0}"
CHECKPOINT_PERF_SKIP_WORKLOADS="${CHECKPOINT_PERF_SKIP_WORKLOADS:-0}"
PERF_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
OVERLAY_GIB=$(python3 - "$CHECKPOINT_MEASURE_GIB" "$CHECKPOINT_PERF_SKIP_LIFECYCLE" <<'PY'
import sys
sizes = sys.argv[1].split(',')
assert sizes and len(sizes) == len(set(sizes)) and all(x in ('0', '1', '10') for x in sizes), sizes
print(2 if sys.argv[2] == '1' else max(map(int, sizes)) + 2)
PY
)
. "${E2E_LIB:?}/orchestrator/execute_env.sh"
. "$SCRIPT_DIR/execute_checkpoint.sh"
. "$PERF_DIR/checkpoint_scenarios.sh"

# Record the actual immutable prepared products, complete provenance and input
# image before timing. There is no product replacement or host-binary fallback.
python3 - "$BIN" "$CASE_OUT" "$CHECKPOINT_PERF_MODE" "$ORCHESTRATOR_EXECUTE_IMAGE" "$0" "$OVERLAY_GIB" "$BUILDER_CPU" <<'PY'
import hashlib, json, pathlib, platform, shutil, sys
products, output = map(pathlib.Path, sys.argv[1:3])
provenance = products.parent / 'provenance.json'
document = json.loads(provenance.read_text())
def digest(path):
    with path.open('rb') as source:
        return hashlib.file_digest(source, 'sha256').hexdigest()
names = ('node-ctl', 'sandbox-ctl', 'flatten-ctl', 'manifest-ctl', 'store-ctl',
         'e2b-key-ctl', 'connector-ctl', 'cloud-hypervisor', 'vmlinux', 'sandbox-runtime.bundle')
shutil.copyfile(provenance, output / 'checkpoint-prepared-provenance.json')
report = {'mode': sys.argv[3], 'image_config_id': sys.argv[4],
          'arch': platform.machine(), 'kernel': platform.release(),
          'prepared': str(products.parent), 'provenance_sha256': digest(provenance),
          'sources': document.get('sources'), 'script_sha256': digest(pathlib.Path(sys.argv[5])),
          'product_sha256': {name: digest(products / name) for name in names},
          'payload_recipe': 'SHAKE256(kuasar-checkpoint-platform-v1 || LE64(MiB-index)), 1 MiB/block',
          'overlay_bytes': int(sys.argv[6]) * 1024**3,
          'perf_files_sha256': {p.name: digest(p) for p in pathlib.Path(sys.argv[5]).parent.iterdir() if p.suffix in ('.py', '.sh')},
          'snapshot_policy': {'memory': True, 'checkpoint_merge_ref': False, 'checkpoint_drop_caches': False},
          'resource_startup_factor': 0.8,
          'builder_cpu_count': int(sys.argv[7]), 'builder_memory_mib': 6144,
          'target_cpu_count': 2, 'target_memory_mib': 2560,
          'retention_protocol': 'disjoint-v2: same guest, retained then released; five disjoint 12 MiB regions in sequential/random/hot/partial/mixed order; identical region/address seed/count across states, different content epochs; report order/cache/background-GC effects rather than infer a write-amplification ratio',
          'retention_regions': {name: {'offset_bytes': index * 12 * 1024**2, 'length_bytes': 12 * 1024**2}
                                for index, name in enumerate(('sequential', 'random', 'hot', 'partial', 'mixed'))},
          'verification_timeout_policy': 'min(1200, 180 + ceil(payload_MiB * 420 / 1024)) seconds; record full hash and cache preparation separately; no timeout is a successful verification',
          'export_request': {'client': 'checkpoint_export.py: Python stdlib HTTPConnection over UDS, same client for both prepared versions',
                             'api': 'POST /sandboxes/{sid}/export', 'body': {'toTemplate': True, 'keepSource': True},
                             'total_timeout_seconds': 1200, 'retries': 0},
          'capture_preparation': 'payload verification, then separately timed sync + guest drop_caches=3; Pause policy unchanged',
          'memory_scope': 'sampled runtime unit and descendants, not process peak',
          'allocation_scope': 'unique regular file inodes under BaseDir and RunDir; excludes shared immutable caches'}
(output / 'checkpoint-inputs.json').write_text(json.dumps(report, indent=2, sort_keys=True) + '\n')
PY
# Identical filesystem capacity for both modes and payload sizes. This modifies
# only the case-owned template copy created by execute_env, never prepared input.
truncate -s "${OVERLAY_GIB}G" "$OVL"
"$MKFS_EXT4" -F -q -b 4096 -O ^has_journal "$OVL"
write_orchestrator_config unset controller
sed -i '/^resource_listen:$/a\  watermarks: { startup_factor: 0.8 }' "$WORK/config.yaml"
start_orchestrator "$WORK/orch.log"
wait_mmds_listener
"$BIN/node-ctl" manifest-key add --socket "$WORK/node-ctl.socket" "$MK" >/dev/null
. "$SCRIPT_DIR/execute_template.sh"
BUILD_STARTED=$(checkpoint_clock)
build_ready_template
python3 - "$CASE_OUT/template-ready.json" "$BUILD_STARTED" <<'PY'
import json, sys, time
open(sys.argv[1], 'w').write(json.dumps({'phase': 'template_build_ready', 'elapsed_seconds': time.monotonic() - float(sys.argv[2])}) + '\n')
PY
INITIAL_TEMPLATE="$TEMPLATE"
printf '%s\n' "$INITIAL_TEMPLATE" >"$CASE_OUT/checkpoint-initial-template.txt"

for round in $(seq 1 "$CHECKPOINT_PERF_ROUNDS"); do
    if [ "$CHECKPOINT_PERF_SKIP_LIFECYCLE" != 1 ]; then perf_lifecycle "$round"; fi
    if [ "$CHECKPOINT_PERF_SKIP_WORKLOADS" != 1 ]; then perf_retention_workloads "$round"; fi
done

python3 - "$CHECKPOINT_PERF_MODE" "$RUN_ARGV_LOG" "$SNAPSHOT_ARGV_LOG" "$EXPORT_ARGV_LOG" <<'PY'
import json, pathlib, sys, uuid
mode = sys.argv[1]
def calls(path):
    return [json.loads(line) for line in pathlib.Path(path).read_text().splitlines() if line.strip()]
runs = calls(sys.argv[2])
assert runs, 'no actual runtime launches observed'
for args in runs:
    assert args.count('--enable-checkpoint') == int(mode == 'checkpoint'), args
identities = []
for path in sys.argv[3:]:
    for args in calls(path):
        builder = args[args.index('--path-id') + 1] in ('a', 'b', 'c')
        expected = mode == 'checkpoint' and not builder
        assert args.count('--checkpoint') == int(expected), args
        if expected:
            raw = args[args.index('--checkpoint') + 1]
            identity = uuid.UUID(raw)
            assert identity.version == 7 and identity.variant == uuid.RFC_4122 and str(identity) == raw, raw
            identities.append(raw)
assert len(identities) == len(set(identities)), identities
PY
cp "$RUN_ARGV_LOG" "$SNAPSHOT_ARGV_LOG" "$EXPORT_ARGV_LOG" "$CASE_OUT/"
journalctl --no-pager -o short-monotonic -u "${RUNNER_PREFIX}*.service" >"$CASE_OUT/runtime-units.journal"
journalctl --no-pager -o short-monotonic -u "${BUILDER_PREFIX}*.service" >"$CASE_OUT/build-units.journal"
echo "PASS checkpoint platform $CHECKPOINT_PERF_MODE; observations: $CASE_OUT/checkpoint-platform.jsonl"
