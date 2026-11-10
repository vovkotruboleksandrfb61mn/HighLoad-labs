#!/usr/bin/env python3
"""How long an append + fdatasync takes in a journal file, depending on how
the file was prepared, without MongoDB.

    scripts/task4x-journal-probe.py [count] [rounds] [record bytes] [processes]

WiredTiger pre-allocates its 100 MB journal files (WiredTigerPreplog.*,
renamed to WiredTigerLog.*) and then appends records to them, making them
durable with fdatasync. On ext4 a pre-allocated file has "unwritten" extents
(filefrag -v shows them), and the first write into such a block must convert
it, which is a metadata change: fdatasync then also commits the ext4 journal.
The probe appends `record bytes` (2048: about five counter increments of
WiredTiger log) at a growing offset and calls fdatasync after each, in a file
prepared three ways:

    fallocate   posix_fallocate, as WiredTiger does by default
    sparse      ftruncate to the full size (holes, no blocks yet)
    zero-filled the whole file written with zeroes and synced first, as
                WiredTiger's log=(zero_fill=true) does

The kinds run interleaved, `count` appends at a time for `rounds` rounds, in
.data/ (on the project's disk). With `processes` > 1 (default 1) that many
processes append to files of their own at the same time, as the three
mongod of the replica set do on one SSD. Prints one CSV row per kind: mean,
p50 and p99 latency of one append and appends per second of one process.
"""
import multiprocessing
import os
import statistics
import sys
import time

count = int(sys.argv[1]) if len(sys.argv) > 1 else 500
rounds = int(sys.argv[2]) if len(sys.argv) > 2 else 3
record = int(sys.argv[3]) if len(sys.argv) > 3 else 2048
processes = int(sys.argv[4]) if len(sys.argv) > 4 else 1
size = 32 * 1024 * 1024
assert count * record <= size, "the appends must fit in the file"
root = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".data")
os.makedirs(root, exist_ok=True)


def prepare(fd, kind):
    if kind == "fallocate":
        os.posix_fallocate(fd, 0, size)
    elif kind == "sparse":
        os.ftruncate(fd, size)
    else:
        chunk = bytes(1024 * 1024)
        for offset in range(0, size, len(chunk)):
            os.pwrite(fd, chunk, offset)
    os.fsync(fd)


def probe(kind, process=0):
    path = os.path.join(root, f"task4x-journal-probe-{kind}-{process}.log")
    fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o600)
    latencies = []
    try:
        prepare(fd, kind)
        payload = b"x" * record
        for i in range(count):
            start = time.perf_counter()
            os.pwrite(fd, payload, i * record)
            os.fdatasync(fd)
            latencies.append(time.perf_counter() - start)
    finally:
        os.close(fd)
        os.unlink(path)
    return latencies


kinds = ["fallocate", "sparse", "zero-filled"]
results = {kind: [] for kind in kinds}
with multiprocessing.Pool(processes) as pool:
    for _ in range(rounds):
        for kind in kinds:
            for latencies in pool.starmap(probe, [(kind, p) for p in range(processes)]):
                results[kind] += latencies

ms = lambda seconds: f"{seconds * 1000:.3f}"
print("file,record_bytes,processes,count,mean_ms,p50_ms,p99_ms,appends_per_second_per_process")
for kind, latencies in results.items():
    latencies.sort()
    n = len(latencies)
    print(
        f"{kind},{record},{processes},{n},{ms(statistics.mean(latencies))},{ms(latencies[n // 2])},"
        f"{ms(latencies[int(n * 0.99)])},{n / sum(latencies):.1f}"
    )
