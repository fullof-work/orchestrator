"""Small deterministic guest file workloads; direct I/O never silently falls back."""
import argparse
import hashlib
import json
import math
import mmap
import os
import random
import time

MIB = 1024 * 1024
PAGE = 4096
PATTERNS = ("sequential", "random", "hot", "partial", "mixed")


def percentile(values, fraction):
    return sorted(values)[max(0, math.ceil(len(values) * fraction) - 1)]


def positions(pattern, count, pages):
    randomizer = random.Random(477)
    for index in range(count):
        limit = min(pages, 2 * MIB // PAGE) if pattern == "hot" else pages
        page = index % pages if pattern == "sequential" else randomizer.randrange(limit)
        read = pattern == "mixed" and randomizer.random() < 0.7
        within = randomizer.randrange(PAGE // 512) * 512 if pattern == "partial" else 0
        yield page * PAGE + within, 512 if pattern == "partial" else PAGE, read


def direct_open(path):
    # Alignment of mmap is page-sized; offsets and lengths are sector-aligned.
    # A filesystem/device that rejects 512-byte direct I/O fails explicitly.
    return os.open(path, os.O_RDWR | os.O_DIRECT | os.O_DSYNC)


def latency_summary(latencies):
    return {"p50_us": percentile(latencies, 0.50), "p95_us": percentile(latencies, 0.95),
            "p99_us": percentile(latencies, 0.99), "latency_us": latencies}


def initialize(path, logical_mib, sparse):
    mapped = min(64, logical_mib) if sparse else logical_mib
    started = time.monotonic()
    with open(path, "wb") as stream:
        stream.truncate(logical_mib * MIB)
        for index in range(mapped):
            block = index * logical_mib // mapped if sparse else index
            stream.seek(block * MIB)
            stream.write(hashlib.shake_256(b"kuasar-checkpoint-platform-v1" + block.to_bytes(8, "little")).digest(MIB))
        stream.flush()
        os.fsync(stream.fileno())
    os.sync()
    st = os.stat(path)
    return {"action": "initialize", "layout": "sparse" if sparse else "dense",
            "logical_bytes": st.st_size, "guest_allocated_bytes": st.st_blocks * 512,
            "mapped_payload_bytes": mapped * MIB, "elapsed_seconds": time.monotonic() - started}


def workload_region(size, offset, length):
    length = size - offset if length is None else length
    if offset < 0 or offset % PAGE or length < PAGE or length % PAGE or offset + length > size:
        raise ValueError("workload region must be page-aligned, nonempty, and within the payload")
    return offset, length


def workload(path, pattern, operations, epoch, offset_bytes=0, length_bytes=None):
    size = os.stat(path).st_size
    region_offset, region_length = workload_region(size, offset_bytes, length_bytes)
    descriptor = direct_open(path)
    buffer = mmap.mmap(-1, PAGE)
    latencies, read_latencies, write_latencies = [], [], []
    written, read, write_ops, read_ops = 0, 0, 0, 0
    started = time.monotonic()
    try:
        for index, (relative_offset, length, is_read) in enumerate(positions(pattern, operations, region_length // PAGE)):
            offset = region_offset + relative_offset
            view = memoryview(buffer)[:length]
            if not is_read:
                view[:] = hashlib.shake_256(f"477/{pattern}/{epoch}/{index}".encode()).digest(length)
            try:
                before = time.perf_counter_ns()
                transferred = os.preadv(descriptor, [view], offset) if is_read else os.pwritev(descriptor, [view], offset)
                latencies.append((time.perf_counter_ns() - before) / 1000)
                if transferred != length:
                    raise OSError(f"short direct I/O at {offset}: {transferred}, want {length}")
            except OSError as error:
                raise OSError(error.errno, f"{pattern}: O_DIRECT length={length}, offset={offset} failed; no buffered fallback: {error}") from error
            finally:
                view.release()
            if is_read:
                read += length
                read_ops += 1
                read_latencies.append(latencies[-1])
            else:
                written += length
                write_ops += 1
                write_latencies.append(latencies[-1])
        elapsed = time.monotonic() - started
    finally:
        os.close(descriptor)
        buffer.close()
    return {"action": "workload", "pattern": pattern, "epoch": epoch, "operations": operations,
            "read_operations": read_ops, "write_operations": write_ops, "read_bytes": read, "write_bytes": written,
            "elapsed_seconds": elapsed, "iops": operations / elapsed,
            "throughput_mib_s": (read + written) / MIB / elapsed,
            "region_offset_bytes": region_offset, "region_length_bytes": region_length,
            "logical_working_set_bytes": min(region_length, 2 * MIB) if pattern == "hot" else region_length,
            "io": "O_DIRECT|O_DSYNC, QD1; partial is actual sector-aligned 512-byte pwritev",
            "timing": "latency is the I/O syscall; throughput includes deterministic buffer generation",
            "read_latency": latency_summary(read_latencies) if read_latencies else None,
            "write_latency": latency_summary(write_latencies) if write_latencies else None,
            **latency_summary(latencies)}


def verify(path):
    started = time.monotonic()
    os.sync()
    with open("/proc/sys/vm/drop_caches", "w") as stream:
        stream.write("3\n")
    hash_started = time.monotonic()
    digest = hashlib.sha256()
    count = 0
    with open(path, "rb") as stream:
        while block := stream.read(MIB):
            digest.update(block)
            count += len(block)
    finished = time.monotonic()
    return {"action": "verify", "sha256": digest.hexdigest(), "bytes_read": count,
            "cache_preparation_seconds": hash_started - started,
            "hash_seconds": finished - hash_started, "elapsed_seconds": finished - started,
            "scope": "complete payload SHA256 after guest sync + drop_caches=3; buffered 1 MiB hash reads, outside lifecycle timing"}


def first_reads(path, pages):
    size = os.stat(path).st_size
    if size < PAGE:
        return {"action": "first_reads", "bytes": 0, "reason": "payload is empty"}
    descriptor = direct_open(path)
    buffer = mmap.mmap(-1, PAGE)
    latencies = []
    count = min(pages, size // PAGE)
    started = time.monotonic()
    try:
        for index in range(count):
            offset = index * (size // PAGE) // count * PAGE
            before = time.perf_counter_ns()
            if os.preadv(descriptor, [buffer], offset) != PAGE:
                raise OSError("short first-working-set read")
            latencies.append((time.perf_counter_ns() - before) / 1000)
        elapsed = time.monotonic() - started
    finally:
        os.close(descriptor)
        buffer.close()
    return {"action": "first_reads", "first_read_us": latencies[0], "batch_seconds": elapsed,
            "pages": count, "bytes": count * PAGE, "stride_scope_bytes": size,
            "io": "O_DIRECT; first reads of measured payload after runtime readiness, not first guest root access",
            **latency_summary(latencies)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    init = sub.add_parser("initialize")
    init.add_argument("--path", required=True)
    init.add_argument("--mib", type=int, required=True)
    init.add_argument("--sparse", action="store_true")
    run = sub.add_parser("run")
    run.add_argument("--path", required=True)
    run.add_argument("--pattern", choices=PATTERNS, required=True)
    run.add_argument("--operations", type=int, default=1024)
    run.add_argument("--epoch", type=int, required=True)
    run.add_argument("--offset-bytes", type=int, default=0)
    run.add_argument("--length-bytes", type=int)
    verification = sub.add_parser("verify")
    verification.add_argument("--path", required=True)
    probe = sub.add_parser("first-reads")
    probe.add_argument("--path", required=True)
    probe.add_argument("--pages", type=int, default=256)
    sub.add_parser("prepare-capture")
    args = parser.parse_args()
    if args.action == "initialize":
        if not 0 <= args.mib <= 10 * 1024:
            parser.error("payload must be between 0 and 10 GiB")
        result = initialize(args.path, args.mib, args.sparse)
    elif args.action == "run":
        if not 1 <= args.operations <= 16384:
            parser.error("operations must be between 1 and 16384")
        result = workload(args.path, args.pattern, args.operations, args.epoch, args.offset_bytes, args.length_bytes)
    elif args.action == "verify":
        result = verify(args.path)
    elif args.action == "first-reads":
        if not 1 <= args.pages <= 4096:
            parser.error("pages must be between 1 and 4096")
        result = first_reads(args.path, args.pages)
    else:
        started = time.monotonic()
        os.sync()
        with open("/proc/sys/vm/drop_caches", "w") as stream:
            stream.write("3\n")
        result = {"action": "prepare_capture", "elapsed_seconds": time.monotonic() - started,
                  "policy": "sync then guest drop_caches=3 after payload hash; normal Pause policy remains unchanged"}
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
