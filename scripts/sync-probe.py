#!/usr/bin/env python3
"""Compares ways of making a 21-byte overwrite at offset 0 durable, without
HTTP: pwrite + fsync (what FileCounterStore does), pwrite + fdatasync, and a
pwrite on a descriptor opened with O_DSYNC.

    scripts/sync-probe.py [count] [rounds]

The methods run interleaved, `count` operations at a time for `rounds`
rounds, so a change in the disk's state hits all of them alike. Runs in
.data/ (on the project's disk). Prints one CSV row per method: mean, p50 and
p99 latency over all rounds and the resulting operations per second.
"""
import os
import statistics
import sys
import time

count = int(sys.argv[1]) if len(sys.argv) > 1 else 500
rounds = int(sys.argv[2]) if len(sys.argv) > 2 else 3
root = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".data")
os.makedirs(root, exist_ok=True)


def probe(name, flags, sync):
    path = os.path.join(root, f"sync-probe-{name}.txt")
    fd = os.open(path, os.O_RDWR | os.O_CREAT | flags, 0o644)
    latencies = []
    try:
        os.pwrite(fd, b"%020d\n" % 0, 0)
        os.fsync(fd)
        for i in range(count):
            start = time.perf_counter()
            os.pwrite(fd, b"%020d\n" % i, 0)
            if sync:
                sync(fd)
            latencies.append(time.perf_counter() - start)
    finally:
        os.close(fd)
        os.unlink(path)
    return latencies


methods = [
    ("fsync", 0, os.fsync),
    ("fdatasync", 0, os.fdatasync),
    ("o_dsync", os.O_DSYNC, None),
]
results = {name: [] for name, _, _ in methods}
for _ in range(rounds):
    for name, flags, sync in methods:
        results[name] += probe(name, flags, sync)

ms = lambda seconds: f"{seconds * 1000:.3f}"
print("method,count,mean_ms,p50_ms,p99_ms,ops_per_second")
for name, latencies in results.items():
    latencies.sort()
    n = len(latencies)
    print(
        f"{name},{n},{ms(statistics.mean(latencies))},{ms(latencies[n // 2])},"
        f"{ms(latencies[int(n * 0.99)])},{n / sum(latencies):.1f}"
    )
