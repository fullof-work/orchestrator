#!/usr/bin/env bash

# Working DIFF layout belongs to the selected sandbox product. Keep historical
# raw fixtures only for binaries whose public CLI predates diff create.
create_ext4_fixture() { # new path, logical size
    local output="$1" size="$2" help
    if [ -e "$output" ] || [ -L "$output" ]; then
        echo "DIFF fixture already exists: $output" >&2
        return 1
    fi
    help="$("$BIN/sandbox-ctl" --help)" || return
    if grep -Fq 'sandbox-ctl diff create' <<<"$help"; then
        "$BIN/sandbox-ctl" diff create --output "$output" --size "$size" --filesystem ext4
    else
        (set -o noclobber; : >"$output") || return
        truncate -s "$size" "$output" || return
        mkfs.ext4 -q -F -b 4096 -O ^has_journal -E lazy_itable_init=0,lazy_journal_init=0 "$output"
    fi
}
