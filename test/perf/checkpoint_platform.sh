#!/usr/bin/env bash
# Optional platform comparison using separately verified prepared workspaces.
# Uses the same host capabilities as orchestrator.checkpoint.sh.
set -euo pipefail
: "${CHECKPOINT_PERF_MODE:?set baseline-copy or checkpoint explicitly}"
case "$CHECKPOINT_PERF_MODE" in baseline-copy|checkpoint) ;; *) exit 2 ;; esac
CHECKPOINT_MEASURE_GIB="${CHECKPOINT_MEASURE_GIB:-0,1}"
case "$CHECKPOINT_MEASURE_GIB" in 0|1|0,1) ;; *) echo "only 0, 1 or 0,1 GiB is supported" >&2; exit 2 ;; esac
. "${E2E_LIB:?}/orchestrator/execute_env.sh"
. "$SCRIPT_DIR/execute_checkpoint.sh"

# Record the actual immutable prepared products, complete provenance and input
# image before timing. There is no product replacement or host-binary fallback.
python3 - "$BIN" "$CASE_OUT" "$CHECKPOINT_PERF_MODE" "$ORCHESTRATOR_EXECUTE_IMAGE" "$0" <<'PY'
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
          'overlay_bytes': 2 * 1024**3,
          'snapshot_policy': {'memory': True, 'checkpoint_merge_ref': False, 'checkpoint_drop_caches': False},
          'memory_scope': 'sampled runtime unit and descendants, not process peak',
          'allocation_scope': 'unique regular file inodes under BaseDir and RunDir; excludes shared immutable caches'}
(output / 'checkpoint-inputs.json').write_text(json.dumps(report, indent=2, sort_keys=True) + '\n')
PY
# Identical filesystem capacity for both modes and payload sizes. This modifies
# only the case-owned template copy created by execute_env, never prepared input.
truncate -s 2G "$OVL"
"$MKFS_EXT4" -F -q -b 4096 -O ^has_journal "$OVL"
write_orchestrator_config unset controller
start_orchestrator "$WORK/orch.log"
wait_mmds_listener
"$BIN/node-ctl" manifest-key add --socket "$WORK/node-ctl.socket" "$MK" >/dev/null
. "$SCRIPT_DIR/execute_template.sh"
build_ready_template
INITIAL_TEMPLATE="$TEMPLATE"
printf '%s\n' "$INITIAL_TEMPLATE" >"$CASE_OUT/checkpoint-initial-template.txt"

for gib in ${CHECKPOINT_MEASURE_GIB//,/ }; do
    mib=$((gib * 1024))
    TEMPLATE="$INITIAL_TEMPLATE"
    create_checkpoint_guest "$mib"
    # Dense, reproducible pseudo-random data; both modes receive identical bytes.
    checkpoint_guest python3 -c 'import hashlib, os, sys
with open("/home/user/checkpoint-payload", "wb") as out:
    for index in range(int(sys.argv[1])):
        out.write(hashlib.shake_256(b"kuasar-checkpoint-platform-v1" + index.to_bytes(8, "little")).digest(1048576))
    out.flush()
    os.fsync(out.fileno())
os.sync()' "$mib"
    checkpoint_hash >"$CASE_OUT/checkpoint-$gib-expected.hash"
    checkpoint_measure before_pause "$(checkpoint_clock)" "$mib"
    pause_checkpoint_guest snapshot "$mib"
    python3 "$SCRIPT_DIR/disk_checkpoint.py" layout "$WORK/lib/node-ctl.db" "$SID" "$CHECKPOINT_PERF_MODE" \
        >"$CASE_OUT/checkpoint-$gib-snapshot-layout.json"
    resume_checkpoint_guest "$mib" hot_ready
    if [ "$CHECKPOINT_PERF_MODE" = checkpoint ]; then
        checkpoint_wait_released "$SID" || fail "measurement resume retained versions"
    fi
    checkpoint_hash >"$WORK/measure-hot.hash"
    cmp "$CASE_OUT/checkpoint-$gib-expected.hash" "$WORK/measure-hot.hash"
    pause_checkpoint_guest sandbox "$mib"
    python3 "$SCRIPT_DIR/disk_checkpoint.py" layout "$WORK/lib/node-ctl.db" "$SID" "$CHECKPOINT_PERF_MODE" \
        >"$CASE_OUT/checkpoint-$gib-sandbox-layout.json"
    started=$(checkpoint_clock)
    E2B_API_KEY="$AK" timeout -k 5s 600 "$ORCH_BIN_DIR/node-ctl" export-sandbox "$SID" \
        --keep-source --to-template --json --socket "$WORK/node-ctl.socket" >"$CASE_OUT/checkpoint-$gib-publish.json"
    checkpoint_measure publish "$started" "$mib"
    resume_checkpoint_guest "$mib" cold_ready
    if [ "$CHECKPOINT_PERF_MODE" = checkpoint ]; then
        checkpoint_wait_released "$SID" || fail "measurement cold resume retained versions"
    fi
    checkpoint_hash >"$WORK/measure-cold.hash"
    cmp "$CASE_OUT/checkpoint-$gib-expected.hash" "$WORK/measure-cold.hash"
    delete_checkpoint_guest
    TEMPLATE=$(json_field "$CASE_OUT/checkpoint-$gib-publish.json" result)
    create_checkpoint_guest "$mib" published_ready
    checkpoint_hash >"$WORK/measure-portable.hash"
    cmp "$CASE_OUT/checkpoint-$gib-expected.hash" "$WORK/measure-portable.hash"
    delete_checkpoint_guest
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
echo "PASS checkpoint platform $CHECKPOINT_PERF_MODE; observations: $CASE_OUT/checkpoint-platform.jsonl"
