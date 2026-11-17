#!/usr/bin/env bash
# Task 5, extra research: what waiting for the commit log fsync costs.
#
#   scripts/task5x-durability.sh [mode ...]   (periodic batch group:10 by default)
#
# For every commit log mode the cluster is restarted with
# CASSANDRA_COMMITLOG=<mode> (scripts/cassandra.sh), then cassandra-bench runs
# WORKERS x INCREMENTS increments at ONE and at QUORUM, REPEATS times. Around
# every run the script reads the commit log metrics of every node over JMX
# (nodetool sjk mx): WaitingOnCommit (how many writes waited for a sync and
# for how long). Then, KILLALL_REPEATS times, every node gets SIGKILL
# DISRUPT_AFTER s into a run at QUORUM and the cluster is restarted: a crash
# of all replicas at once, counting acknowledged increments that are gone.
# Rows go to $OUT (reports/data/task5x-durability.csv).
#
# Leaves the cluster running in the default periodic mode.
# Overridable: WORKERS (10), INCREMENTS (10000), REPEATS (3),
# KILLALL_REPEATS (3), DISRUPT_AFTER (7), OUT.
set -euo pipefail

script_dir=$(cd "$(dirname "$0")" && pwd)
if ! command -v cqlsh >/dev/null && [ -z "${HLS_CASSANDRA_REEXEC:-}" ] && command -v direnv >/dev/null; then
    HLS_CASSANDRA_REEXEC=1 exec direnv exec "$script_dir/.." "$0" "$@"
fi
. "$script_dir/task5-common.sh"

WORKERS=${WORKERS:-10}
INCREMENTS=${INCREMENTS:-10000}
REPEATS=${REPEATS:-3}
KILLALL_REPEATS=${KILLALL_REPEATS:-3}
DISRUPT_AFTER=${DISRUPT_AFTER:-7}
OUT=${OUT:-$HLS_ROOT/reports/data/task5x-durability.csv}
modes=${*:-periodic batch group:10}

hls_build_release --product cassandra-bench
mkdir -p "$(dirname "$OUT")"

# metric <node> <bean name> <attribute>: one JMX attribute of node N.
metric() {
    "$HLS_ROOT/scripts/cassandra.sh" nodetool "$1" sjk mx -b "org.apache.cassandra.metrics:type=CommitLog,name=$2" \
        -mg -f "$3" 2>/dev/null | tail -n 1 | tr -d '[:space:]'
}

# The sum over the nodes of a counter-like metric.
metric_sum() {
    local n sum=0 v
    for n in 1 2 3; do
        v=$(metric "$n" "$1" "$2")
        sum=$(awk -v a="$sum" -v b="${v:-0}" 'BEGIN { printf "%.6f", a + b }')
    done
    echo "$sum"
}

header=true
for mode in $modes; do
    echo "== commit log mode $mode: restarting the cluster" >&2
    "$HLS_ROOT/scripts/cassandra.sh" stop >&2
    # Exported, so the restart after a kill-all keeps the mode too.
    export CASSANDRA_COMMITLOG=$mode
    "$HLS_ROOT/scripts/cassandra.sh" start >/dev/null
    for repeat in $(seq 1 "$REPEATS"); do
        for level in one quorum; do
            waits_before=$(metric_sum WaitingOnCommit Count)
            output=$("$HLS_BIN/cassandra-bench" --header --label "$mode" --consistency "$level" \
                --workers "$WORKERS" --increments "$INCREMENTS" --hosts "$CASSANDRA_HOSTS" --port "$CASSANDRA_PORT" \
                2> >(grep -v 'connection_pool.cpp\|\[WARN\]' >&2))
            wait
            waits_after=$(metric_sum WaitingOnCommit Count)
            wait_mean=$(metric 1 WaitingOnCommit Mean)
            if $header; then
                printf 'repeat,%s,waiting_on_commit,node1_wait_mean_us\n' "$(sed -n 1p <<<"$output")" >"$OUT"
                header=false
            fi
            row=$(printf '%s,%s,%s,%s' "$repeat" "$(sed -n 2p <<<"$output")" \
                "$(awk -v a="$waits_before" -v b="$waits_after" 'BEGIN { printf "%d", b - a }')" "${wait_mean:-}")
            printf '%s\n' "$row" | tee -a "$OUT" >&2
        done
    done
    for repeat in $(seq 1 "$KILLALL_REPEATS"); do
        echo "== $mode, kill-all, repeat $repeat" >&2
        output=$("$HLS_BIN/cassandra-bench" --header --label "$mode" --consistency quorum \
            --workers "$WORKERS" --increments "$INCREMENTS" --hosts "$CASSANDRA_HOSTS" --port "$CASSANDRA_PORT" \
            --kill-all-for 5 --disrupt-after "$DISRUPT_AFTER" --cluster-script "$HLS_ROOT/scripts/cassandra.sh" \
            2> >(grep -v 'connection_pool.cpp\|\[WARN\]\|\[ERROR\]' >&2)) || true
        wait
        [ -n "$output" ] || { echo "kill-all run failed" >&2; continue; }
        printf '%s,%s,,\n' "$repeat" "$(sed -n 2p <<<"$output")" | tee -a "$OUT" >&2
    done
done
unset CASSANDRA_COMMITLOG

echo "== back to periodic" >&2
"$HLS_ROOT/scripts/cassandra.sh" stop >&2
"$HLS_ROOT/scripts/cassandra.sh" start >/dev/null
echo "wrote $OUT" >&2
