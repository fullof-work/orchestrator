#!/usr/bin/env bash
# Independent performance flows. Public prepared helpers remain unchanged.
GUEST_WORKLOAD="$(cat "$PERF_DIR/checkpoint_guest_workload.py")"
PERF_TAG=setup
checkpoint_measure() { # same frozen observation helper, with a unique sample label
    python3 "$SCRIPT_DIR/disk_checkpoint.py" measure "$CASE_OUT/checkpoint-platform.jsonl" \
        "$WORK/lib/node-ctl.db" "$SID" "$RUNNER_PREFIX" "$CHECKPOINT_ROOT/$SID" \
        "$PERF_TAG.$1" "$2" "$3" --mode "$CHECKPOINT_PERF_MODE"
}
perf_guest() { checkpoint_guest python3 -c "$GUEST_WORKLOAD" "$@"; }
perf_cache() { # action label; control file page cache after hashing, outside Pause
    perf_guest prepare-capture >"$CASE_OUT/$PERF_TAG.$1.cache-preparation.json"
}
perf_observe() { # label, guest workload arguments
    local label="$1"
    shift
    python3 "$PERF_DIR/checkpoint_observe.py" --lib "$SCRIPT_DIR" \
        --database "$WORK/lib/node-ctl.db" --sid "$SID" --prefix "$RUNNER_PREFIX" \
        --base "$CHECKPOINT_ROOT/$SID" --label "$PERF_TAG.$label" \
        --output "$CASE_OUT/workloads/$PERF_TAG.$label.json" -- \
        timeout -k 5s 180 "$BIN/sandbox-ctl" exec --path-id "$SID" \
        --run-root "$EXECUTE_RUN_ROOT/sandboxes" -- python3 -c "$GUEST_WORKLOAD" "$@" \
        >>"$CASE_OUT/workload-summary.jsonl"
}
perf_first_reads() {
    perf_observe "$1" first-reads --path /home/user/checkpoint-payload --pages 256
}
perf_require_release() {
    if [ "$CHECKPOINT_PERF_MODE" = checkpoint ]; then
        checkpoint_wait_released "$SID" || fail "measurement resume retained versions"
    fi
}
perf_layout() {
    python3 "$SCRIPT_DIR/disk_checkpoint.py" layout "$WORK/lib/node-ctl.db" "$SID" "$CHECKPOINT_PERF_MODE" \
        >"$CASE_OUT/$PERF_TAG.$1.layout.json"
}
perf_lifecycle() {
    local round="$1" layout gib mib started sample
    for layout in ${CHECKPOINT_PERF_LAYOUTS//,/ }; do
        for gib in ${CHECKPOINT_MEASURE_GIB//,/ }; do
            [ "$layout:$gib" != sparse:0 ] || continue
            mib=$((gib * 1024))
            PERF_TAG="$layout-${gib}GiB-r$round"
            sample="$PERF_TAG"
            TEMPLATE="$INITIAL_TEMPLATE"
            create_checkpoint_guest "$mib"
            local -a init=(initialize --path /home/user/checkpoint-payload --mib "$mib")
            [ "$layout" != sparse ] || init+=(--sparse)
            perf_guest "${init[@]}" >"$CASE_OUT/$sample.initialization.json"
            checkpoint_hash >"$CASE_OUT/$sample.expected.hash"
            perf_cache before_snapshot
            checkpoint_measure before_pause "$(checkpoint_clock)" "$mib"
            pause_checkpoint_guest snapshot "$mib"
            perf_layout snapshot
            resume_checkpoint_guest "$mib" hot_ready
            perf_first_reads hot_first_working_set
            perf_require_release
            checkpoint_hash >"$WORK/measure-hot.hash"
            cmp "$CASE_OUT/$sample.expected.hash" "$WORK/measure-hot.hash"
            perf_cache before_sandbox
            pause_checkpoint_guest sandbox "$mib"
            perf_layout sandbox
            started=$(checkpoint_clock)
            E2B_API_KEY="$AK" timeout -k 5s 1200 "$ORCH_BIN_DIR/node-ctl" export-sandbox "$SID" \
                --keep-source --to-template --json --socket "$WORK/node-ctl.socket" >"$CASE_OUT/$sample.publish.json"
            checkpoint_measure publish "$started" "$mib"
            resume_checkpoint_guest "$mib" cold_ready
            perf_first_reads cold_first_working_set
            perf_require_release
            # No explicit payload write between this resume and the next Pause.
            # Guest/kernel housekeeping is not claimed to be zero writes.
            PERF_TAG="$sample.no-payload-write"
            perf_cache before_sandbox
            pause_checkpoint_guest sandbox "$mib"
            perf_layout sandbox
            resume_checkpoint_guest "$mib" cold_ready
            perf_first_reads cold_first_working_set
            perf_require_release
            checkpoint_hash >"$WORK/measure-cold.hash"
            cmp "$CASE_OUT/$sample.expected.hash" "$WORK/measure-cold.hash"
            delete_checkpoint_guest
            PERF_TAG="$sample"
            TEMPLATE=$(json_field "$CASE_OUT/$sample.publish.json" result)
            create_checkpoint_guest "$mib" published_ready
            perf_first_reads published_first_working_set
            checkpoint_hash >"$WORK/measure-portable.hash"
            cmp "$CASE_OUT/$sample.expected.hash" "$WORK/measure-portable.hash"
            delete_checkpoint_guest
        done
    done
}
perf_retention_workloads() {
    local round="$1" pattern index retained current previous=-1 stable=0 reclaimed=0 started threshold
    PERF_TAG="retention-64MiB-r$round"
    TEMPLATE="$INITIAL_TEMPLATE"
    create_checkpoint_guest 64
    perf_guest initialize --path /home/user/checkpoint-payload --mib 64 >"$CASE_OUT/$PERF_TAG.initialization.json"
    checkpoint_hash >"$CASE_OUT/$PERF_TAG.frozen.hash"
    perf_cache before_sandbox
    pause_checkpoint_guest sandbox 64
    perf_layout retained
    printf '%s\n' "$CHECKPOINT_ROOT/$SID/checkpoint" >"$PUBLISH_BARRIER_TARGET"
    E2B_API_KEY="$AK" timeout -k 5s 1200 "$ORCH_BIN_DIR/node-ctl" export-sandbox "$SID" \
        --keep-source --to-template --json --socket "$WORK/node-ctl.socket" \
        >"$CASE_OUT/$PERF_TAG.publish.json" 2>"$CASE_OUT/$PERF_TAG.publish.log" &
    PUBLISH_PID=$!
    PIDS+=("$PUBLISH_PID")
    for _ in $(seq 1 300); do
        [ ! -e "$PUBLISH_BARRIER_REACHED" ] || break
        kill -0 "$PUBLISH_PID" 2>/dev/null || fail "publisher exited before retention barrier"
        sleep 0.1
    done
    [ -e "$PUBLISH_BARRIER_REACHED" ] || fail "publisher did not reach source barrier"
    resume_checkpoint_guest 64 retained_ready
    perf_first_reads retained_first_working_set
    if [ "$CHECKPOINT_PERF_MODE" = checkpoint ]; then
        checkpoint_list "$SID" "$CASE_OUT/$PERF_TAG.retained-list.json"
        python3 - "$CASE_OUT/$PERF_TAG.retained.layout.json" "$CASE_OUT/$PERF_TAG.retained-list.json" <<'PY'
import json, sys
meta, refs = (json.load(open(path)) for path in sys.argv[1:])
want = {(disk['diff_id'], meta['checkpoint_id']) for disk in [meta['root'], *(meta.get('disks') or [])]}
assert refs and {(ref['diff_id'], ref['id']) for ref in refs} == want, (refs, want)
PY
    fi
    perf_cache before_retained_workloads
    for pattern in sequential random hot partial mixed; do
        perf_observe "retained.$pattern" run --path /home/user/checkpoint-payload \
            --pattern "$pattern" --operations "$CHECKPOINT_PERF_OPERATIONS" --epoch 1
    done
    checkpoint_hash >"$WORK/retained-active.hash"
    if [ "$CHECKPOINT_PERF_MODE" = checkpoint ]; then retained=$(checkpoint_allocated "$SID"); fi
    : >"$PUBLISH_BARRIER_RELEASE"
    wait "$PUBLISH_PID" || fail "retained-view publisher failed"
    for index in "${!PIDS[@]}"; do [ "${PIDS[$index]}" != "$PUBLISH_PID" ] || unset 'PIDS[index]'; done
    rm -f "$PUBLISH_BARRIER_TARGET" "$PUBLISH_BARRIER_REACHED" "$PUBLISH_BARRIER_RELEASE"
    perf_require_release
    if [ "$CHECKPOINT_PERF_MODE" = checkpoint ]; then
        # Wait for actual allocation to fall and settle; no GC internal state is read.
        started=$(checkpoint_clock)
        threshold=$((CHECKPOINT_PERF_OPERATIONS * 4096))
        [ "$threshold" -le 1048576 ] || threshold=1048576
        for _ in $(seq 1 120); do
            current=$(checkpoint_allocated "$SID")
            if [ "$current" -le "$((retained - threshold))" ]; then reclaimed=1; fi
            if [ "$current" = "$previous" ]; then stable=$((stable + 1)); else stable=0; fi
            [ "$reclaimed" != 1 ] || [ "$stable" -lt 3 ] || break
            previous="$current"
            sleep 1
        done
        python3 - "$CASE_OUT/$PERF_TAG.gc.json" "$started" "$retained" "$current" "$reclaimed" "$stable" <<'PY'
import json, sys, time
json.dump({'elapsed_seconds': time.monotonic() - float(sys.argv[2]), 'retained_allocated_bytes': int(sys.argv[3]),
           'released_allocated_bytes': int(sys.argv[4]), 'observed_reclamation': sys.argv[5] == '1',
           'stable_samples': int(sys.argv[6]), 'scope': 'allocation settled for 3 seconds within 120-second grace; not proof that all future GC work has ended'}, open(sys.argv[1], 'w'))
PY
        [ "$reclaimed" = 1 ] && [ "$stable" -ge 3 ] || fail "released workload could not begin after bounded physical GC"
    fi
    checkpoint_hash >"$WORK/released-active.hash"
    cmp "$WORK/retained-active.hash" "$WORK/released-active.hash"
    perf_cache before_released_workloads
    for pattern in sequential random hot partial mixed; do
        perf_observe "released.$pattern" run --path /home/user/checkpoint-payload \
            --pattern "$pattern" --operations "$CHECKPOINT_PERF_OPERATIONS" --epoch 2
    done
    checkpoint_hash >"$CASE_OUT/$PERF_TAG.active-final.hash"
    delete_checkpoint_guest
    TEMPLATE=$(json_field "$CASE_OUT/$PERF_TAG.publish.json" result)
    create_checkpoint_guest 64 retained_published_ready
    checkpoint_hash >"$WORK/frozen-published.hash"
    cmp "$CASE_OUT/$PERF_TAG.frozen.hash" "$WORK/frozen-published.hash"
    delete_checkpoint_guest
}
