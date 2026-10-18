#!/usr/bin/env python3
"""Synchronous systemd ExecStop for Proxy; systemd owns forced cleanup.

Deployment example exercised by the real lifecycle E2E case.
Usage: python3 proxy_stop.py SECONDS
Run inside the Proxy service with KillMode=mixed and SendSIGKILL=yes. Choose
SECONDS above shutdown_timeout and TimeoutStopSec above SECONDS. This helper
signals only MAINPID, then waits for this unit (including B and its workers).
"""
import json
import math
import os
from pathlib import Path
import signal
import sys
import time


def unit_cgroup():
    identity = next(line[3:] for line in Path('/proc/self/cgroup').read_text().splitlines()
                    if line.startswith('0::'))
    group = Path('/sys/fs/cgroup') / identity.lstrip('/')
    if group.name == '.control':
        group = group.parent
    if not group.name.endswith('.service') or '..' in group.parts:
        raise ValueError('ExecStop must run inside its systemd service cgroup')
    return group


def processes(group):
    result = set()
    for path in group.rglob('cgroup.procs'):
        try:
            result.update(int(pid) for pid in path.read_text().split())
        except FileNotFoundError:
            pass  # An empty child cgroup may disappear during enumeration.
    result.discard(os.getpid())
    return result


def stop(seconds):
    if not math.isfinite(seconds) or seconds <= 0:
        raise ValueError('wait seconds must be finite and positive')
    group = unit_cgroup()
    started = time.monotonic()
    main = int(os.environ.get('MAINPID') or '0')
    if main > 1:
        try:
            # Pin the process identity before checking membership and signaling.
            descriptor = os.pidfd_open(main)
            try:
                if main not in processes(group):
                    signal.pidfd_send_signal(descriptor, 0)  # Exited while enumerating?
                    raise ValueError('MAINPID is outside this service cgroup')
                signal.pidfd_send_signal(descriptor, signal.SIGTERM)
            finally:
                os.close(descriptor)
        except ProcessLookupError:
            pass  # Unexpected main exit also runs ExecStop; B may still drain.
    while True:
        remaining = processes(group)
        elapsed = time.monotonic() - started
        if not remaining or elapsed >= seconds:
            print(json.dumps(dict(proxy_stop='completed' if not remaining else 'timed_out',
                                  elapsed_seconds=elapsed, remaining=sorted(remaining))), flush=True)
            return int(bool(remaining))
        time.sleep(min(.02, seconds - elapsed))


if __name__ == '__main__':
    try:
        if len(sys.argv) != 2:
            raise ValueError('usage: proxy_stop.py SECONDS')
        sys.exit(stop(float(sys.argv[1])))
    except (OSError, ValueError, StopIteration) as error:
        print(f'proxy ExecStop failed: {error}', file=sys.stderr)
        sys.exit(2)
