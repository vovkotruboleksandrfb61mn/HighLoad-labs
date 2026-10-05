#!/usr/bin/env python3
"""Measures the cost of one disk-store increment without HTTP: a 21-byte
pwrite at offset 0 followed by fsync, the same I/O FileCounterStore does.

    scripts/fsync-probe.py [count]

Runs in .data/ (on the project's disk, like the counter file) and prints the
mean, p50 and p99 latency and the resulting operations per second.
"""
import os
import statistics
import sys
import time

count = int(sys.argv[1]) if len(sys.argv) > 1 else 2000
root = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".data")
os.makedirs(root, exist_ok=True)
path = os.path.join(root, "fsync-probe.txt")

fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
latencies = []
try:
    for i in range(count):
        start = time.perf_counter()
        os.pwrite(fd, b"%020d\n" % i, 0)
        os.fsync(fd)
        latencies.append(time.perf_counter() - start)
finally:
    os.close(fd)
    os.unlink(path)

latencies.sort()
ms = lambda seconds: f"{seconds * 1000:.3f}"
print("count,mean_ms,p50_ms,p99_ms,ops_per_second")
print(
    f"{count},{ms(statistics.mean(latencies))},{ms(latencies[count // 2])},"
    f"{ms(latencies[int(count * 0.99)])},{count / sum(latencies):.1f}"
)
