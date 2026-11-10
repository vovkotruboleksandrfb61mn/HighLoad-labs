#!/usr/bin/env python3
"""Latency percentiles and a throughput timeline from mongo-bench --latency-out.

    scripts/task4x-latency.py --data-dir reports/data RUN=FILE.csv[.gz] ...

FILE has one row per acknowledged increment: completed_seconds (from the
start of the run), latency_ms, retries. Writes

  DATA_DIR/task4x-latency.csv          one row per RUN: count, mean and the
                                       nearest-rank percentiles p50, p90,
                                       p99, p99.9 and max of the latency, and
                                       the longest pause between two
                                       acknowledgements (with its start)
  DATA_DIR/task4x-timeline-RUN.csv     acknowledgements per 100 ms bucket,
                                       empty buckets included

Only the standard library is used.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import math
from pathlib import Path

BUCKET_MS = 100
PERCENTILES = [("p50", 50), ("p90", 90), ("p99", 99), ("p99_9", 99.9)]


def read_samples(path: Path) -> list[tuple[float, float]]:
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8", newline="") as f:
        return [(float(r["completed_seconds"]), float(r["latency_ms"])) for r in csv.DictReader(f)]


def nearest_rank(sorted_values: list[float], p: float) -> float:
    rank = max(1, math.ceil(p / 100 * len(sorted_values)))
    return sorted_values[rank - 1]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("runs", nargs="+", metavar="RUN=FILE")
    args = parser.parse_args()

    summary = []
    for spec in args.runs:
        run, _, file = spec.partition("=")
        samples = read_samples(Path(file))
        completed = sorted(c for c, _ in samples)
        latencies = sorted(lat for _, lat in samples)

        gap, gap_start = 0.0, 0.0
        for a, b in zip(completed, completed[1:]):
            if b - a > gap:
                gap, gap_start = b - a, a

        row = {"run": run, "count": len(latencies), "mean_ms": round(sum(latencies) / len(latencies), 3)}
        for name, p in PERCENTILES:
            row[f"{name}_ms"] = round(nearest_rank(latencies, p), 3)
        row["max_ms"] = round(latencies[-1], 3)
        row["longest_gap_ms"] = round(gap * 1000, 1)
        row["longest_gap_from_s"] = round(gap_start, 3)
        summary.append(row)

        buckets = [0] * (int(completed[-1] * 1000 // BUCKET_MS) + 1)
        for c in completed:
            buckets[int(c * 1000 // BUCKET_MS)] += 1
        timeline = args.data_dir / f"task4x-timeline-{run}.csv"
        with open(timeline, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["bucket_start_s", "acknowledged"])
            for i, n in enumerate(buckets):
                w.writerow([f"{i * BUCKET_MS / 1000:.1f}", n])
        print(f"wrote {timeline}")

    out = args.data_dir / "task4x-latency.csv"
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(summary[0]))
        w.writeheader()
        w.writerows(summary)
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
