#!/usr/bin/env bash
# Checkpoint artifact and host observations; cases own captures and lifecycle.
checkpoint_local_metadata() { # SID, output
    python3 "$SCRIPT_DIR/disk_checkpoint.py" metadata "$WORK/lib/node-ctl.db" "$1" >"$2"
}
checkpoint_disk_state() { # SID, output
    python3 "$SCRIPT_DIR/disk_checkpoint.py" disks "$CHECKPOINT_ROOT/$1/checkpoint/diffs" >"$2"
}
checkpoint_same_disks() { # before JSON, after JSON
    python3 - "$1" "$2" <<'PY'
import json, sys
def identities(path):
    return [(d['name'], d['device'], d['inode']) for d in json.load(open(path))]
before, after = map(identities, sys.argv[1:])
assert before and before == after, (before, after)
PY
}
checkpoint_list() { # SID, output; only for a running instance
    timeout -k 2s 8 "$BIN/sandbox-ctl" checkpoint list --path-id "$1" \
        --run-root "$EXECUTE_RUN_ROOT/sandboxes" --json >"$2"
}
checkpoint_wait_released() { # SID
    local attempt
    for attempt in $(seq 1 120); do
        checkpoint_list "$1" "$WORK/checkpoint-list.json" || return 1
        if python3 - "$WORK/checkpoint-list.json" <<'PY'
import json, sys
raise SystemExit(0 if json.load(open(sys.argv[1])) == [] else 1)
PY
        then return 0; fi
        sleep 0.25
    done
    cat "$WORK/checkpoint-list.json" >&2
    return 1
}
checkpoint_allocated() { # SID
    python3 "$SCRIPT_DIR/disk_checkpoint.py" disks "$CHECKPOINT_ROOT/$1/checkpoint/diffs" \
        | python3 -c 'import json,sys; print(sum(d["allocated_bytes"] for d in json.load(sys.stdin)))'
}
checkpoint_measure() { # phase, monotonic start, payload MiB
    python3 "$SCRIPT_DIR/disk_checkpoint.py" measure "$CASE_OUT/checkpoint-platform.jsonl" \
        "$WORK/lib/node-ctl.db" "$SID" "$RUNNER_PREFIX" "$CHECKPOINT_ROOT/$SID" "$1" "$2" "$3" \
        --mode "${CHECKPOINT_PERF_MODE:-checkpoint}"
}
checkpoint_clock() { python3 -c 'import time; print(time.monotonic())'; }
checkpoint_guest() {
    timeout -k 5s 180 "$BIN/sandbox-ctl" exec --path-id "$SID" \
        --run-root "$EXECUTE_RUN_ROOT/sandboxes" -- "$@"
}

create_checkpoint_guest() { # payload MiB, optional phase suffix
    local started
    started=$(checkpoint_clock)
    create_guest "{\"templateID\":\"$TEMPLATE\",\"timeout\":600}"
    wait_sandbox_state "$SID" running 1200 || fail "Checkpoint guest did not run"
    checkpoint_guest /bin/true >/dev/null
    checkpoint_measure "create_${2:-ready}" "$started" "$1"
}
pause_checkpoint_guest() { # snapshot|sandbox, payload MiB
    local started body='{"memory":false}'
    [ "$1" != snapshot ] || body='{"memory":true,"checkpoint_merge_ref":false,"checkpoint_drop_caches":false}'
    started=$(checkpoint_clock)
    code=$(req POST "/sandboxes/$SID/pause" "$AK" "$body")
    [ "$code" = 204 ] || { cat "$WORK/resp.body"; fail "$1 Pause=$code"; }
    wait_paused_cleanup "$SID" 300 || fail "Checkpoint paused cleanup did not finish"
    checkpoint_measure "pause_$1" "$started" "$2"
}
resume_checkpoint_guest() { # payload MiB, optional phase suffix
    local started
    started=$(checkpoint_clock)
    code=$(req POST "/sandboxes/$SID/connect" "$AK" '{"timeout":600}')
    [ "$code" = 200 ] || { cat "$WORK/resp.body"; fail "Checkpoint Connect=$code"; }
    wait_sandbox_state "$SID" running 1200 || fail "Checkpoint resume did not run"
    checkpoint_guest /bin/true >/dev/null
    checkpoint_measure "resume_${2:-ready}" "$started" "$1"
}
write_checkpoint_payload() { # MiB; dense in-place writes preserve filesystem blocks
    checkpoint_guest /bin/sh -c "dd if=/dev/urandom of=/home/user/checkpoint-payload bs=1048576 count=$1 conv=notrunc,fsync 2>/dev/null && sync"
}
checkpoint_hash() {
    checkpoint_guest /bin/sh -c 'sync && echo 3 > /proc/sys/vm/drop_caches && sha256sum /home/user/checkpoint-payload'
}
delete_checkpoint_guest() {
    code=$(req DELETE "/sandboxes/$SID" "$AK")
    [ "$code" = 204 ] || fail "Checkpoint source delete=$code"
    wait_sandbox_state "$SID" missing 1200 || fail "Checkpoint source deletion did not finish"
    [ ! -e "$CHECKPOINT_ROOT/$SID" ] || fail "source BaseDir remains after deletion"
}
