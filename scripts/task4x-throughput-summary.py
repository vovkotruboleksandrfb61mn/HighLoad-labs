#!/usr/bin/env python3
"""Medians of the lab 4 throughput research (scripts/task4x-throughput.py).

    scripts/task4x-throughput-summary.py [experiment...]

Reads reports/data/task4x-throughput.csv and task4x-throughput-metrics.csv
and prints, per experiment and variant in the order they ran: the runs, how
many ended exact, the median, minimum and maximum of ops/s, and the medians
of the server metrics of the primary (WiredTiger log syncs per increment and
their mean time, write conflicts per increment, mean wait for the write
concern, CPU) and of the secondaries (log syncs, oplog entries per apply
batch, CPU).
"""
import csv
import os
import statistics
import sys

DATA = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "reports", "data")


def median(values):
    values = [float(v) for v in values if v != ""]
    return statistics.median(values) if values else float("nan")


def main():
    wanted = set(sys.argv[1:])
    with open(os.path.join(DATA, "task4x-throughput.csv")) as f:
        runs = list(csv.DictReader(f))
    with open(os.path.join(DATA, "task4x-throughput-metrics.csv")) as f:
        metrics = list(csv.DictReader(f))

    groups = {}
    for row in runs:
        if wanted and row["experiment"] not in wanted:
            continue
        groups.setdefault((row["experiment"], row["label"]), []).append(row)

    experiment = None
    for (exp, label), rows in groups.items():
        if exp != experiment:
            experiment = exp
            print(f"\n== {exp}: 10 x {rows[0]['increments_per_client']}")
            print(f"{'variant':34} {'runs':>4} {'exact':>5} {'ops/s med':>9} {'min':>8} {'max':>8} | "
                  f"{'P syncs/op':>10} {'P sync ms':>9} {'P wc/op':>7} {'P wtime':>7} {'P cpu%':>6} | "
                  f"{'S syncs/op':>10} {'S sync ms':>9} {'S ops/batch':>11} {'S cpu%':>6}")
        rates = [float(r["ops_per_s"]) for r in rows]
        exact = sum(r["correct"] == "true" for r in rows)
        mine = [m for m in metrics if m["experiment"] == exp and m["label"] == label]
        primary = [m for m in mine if m["state"] == "PRIMARY"]
        secondary = [m for m in mine if m["state"] == "SECONDARY"]
        print(f"{label:34} {len(rows):>4} {exact:>5} {statistics.median(rates):>9.1f} {min(rates):>8.1f} {max(rates):>8.1f} | "
              f"{median(m['log_syncs_per_op'] for m in primary):>10.3f} {median(m['log_sync_ms_mean'] for m in primary):>9.2f} "
              f"{median(m['write_conflicts_per_op'] for m in primary):>7.2f} {median(m['wtime_ms_mean'] for m in primary):>7.2f} "
              f"{median(m['cpu_percent'] for m in primary):>6.0f} | "
              f"{median(m['log_syncs_per_op'] for m in secondary):>10.3f} {median(m['log_sync_ms_mean'] for m in secondary):>9.2f} "
              f"{median(m['apply_ops_per_batch'] for m in secondary):>11.2f} {median(m['cpu_percent'] for m in secondary):>6.0f}")


if __name__ == "__main__":
    main()
