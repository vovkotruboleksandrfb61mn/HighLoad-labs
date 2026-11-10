#!/usr/bin/env bash
# Task 4, extra: the latency of every increment and the throughput over
# time, with and without a failover.
#
#   scripts/task4x-latency.sh
#
# One mongo-bench run each of w1, majority and majority-kill (WORKERS x
# INCREMENTS, as in scripts/task4.sh), every one with --latency-out. The
# primary of majority-kill gets SIGKILL KILL_FRACTION of the way into the run
# (of this script's majority run), like task4.sh does.
#
#   reports/data/task4x-latency-bench.csv         the mongo-bench CSV rows
#   reports/data/task4x-latency-raw-RUN.csv.gz    every acknowledged increment
#   reports/data/task4x-latency.csv               percentiles per run
#   reports/data/task4x-timeline-RUN.csv          acknowledgements per 100 ms
#   reports/logs/task4x-latency-RUN.txt           mongo-bench's standard error;
#                                                 for the kill run also the
#                                                 failover from the mongod logs
#
# The replica set is (re)started fresh with its logs in MONGO_LOG_DIR
# (.data/task4x-mongo-logs, not committed) and stopped at the end.
#
# Overridable: WORKERS (10), INCREMENTS (10000), KILL_FRACTION (0.35),
# DATA_DIR (reports/data), OUT_DIR (reports/logs), MONGO_LOG_DIR.
set -euo pipefail

script_dir=$(cd "$(dirname "$0")" && pwd)
# mongod and mongosh on PATH up front, as in task4.sh, so the kill is not
# delayed by mongo.sh re-running itself under direnv.
if { ! command -v mongod >/dev/null || ! command -v mongosh >/dev/null; } \
    && [ -z "${HLS_MONGO_REEXEC:-}" ] && command -v direnv >/dev/null; then
    HLS_MONGO_REEXEC=1 exec direnv exec "$script_dir/.." "$0" "$@"
fi
. "$script_dir/env.sh"

WORKERS=${WORKERS:-10}
INCREMENTS=${INCREMENTS:-10000}
KILL_FRACTION=${KILL_FRACTION:-0.35}
DATA_DIR=${DATA_DIR:-$HLS_ROOT/reports/data}
OUT_DIR=${OUT_DIR:-$HLS_ROOT/reports/logs}
export MONGO_LOG_DIR=${MONGO_LOG_DIR:-$HLS_DATA/task4x-mongo-logs}

MONGO="$HLS_ROOT/scripts/mongo.sh"
BENCH_OUT="$DATA_DIR/task4x-latency-bench.csv"
tmp=$(mktemp -d)
trap '"$MONGO" stop; rm -rf "$tmp"' EXIT

hls_build_release --product mongo-bench
mkdir -p "$DATA_DIR" "$OUT_DIR"
"$MONGO" stop
"$MONGO" start
"$MONGO" status >&2

# bench <run> <variant> [mongo-bench args...]: one run; prints its seconds.
header_written=false
bench() {
    local run=$1 variant=$2 output log_start=() n since excerpt
    shift 2
    "$MONGO" wait-primary >&2
    for n in 1 2 3; do
        log_start+=("$(($(wc -l <"$MONGO_LOG_DIR/mongo-node$n.log") + 1))")
    done
    since=$(date +%s.%N)
    echo "== $run $*" >&2
    output=$("$HLS_BIN/mongo-bench" --header --variant "$variant" --workers "$WORKERS" \
        --increments "$INCREMENTS" --url "$MONGO_URL" --mongo-script "$MONGO" \
        --latency-out "$tmp/$run.csv" "$@" 2> >(tee "$tmp/$run.err" >&2))
    wait
    {
        echo "mongo-bench --variant $variant --workers $WORKERS --increments $INCREMENTS --latency-out ... $*"
        echo
        echo "Рядок CSV mongo-bench:"
        printf '%s\n\n' "$output"
        echo "Вивід mongo-bench і scripts/mongo.sh (stderr):"
        cat "$tmp/$run.err"
        if [ "$#" -gt 0 ]; then
            local logs=()
            for n in 1 2 3; do
                logs+=(--log "$n=$MONGO_LOG_DIR/mongo-node$n.log:${log_start[$((n - 1))]}")
            done
            excerpt=$(python3 "$script_dir/mongo-failover-excerpt.py" \
                --title "Task 4x: прогін $run" --data-dir "$MONGO_DATA_DIR" --since "$since" "${logs[@]}")
            echo
            printf '%s\n' "$excerpt"
        fi
    } >"$OUT_DIR/task4x-latency-$run.txt"
    if ! $header_written; then
        printf 'run,%s\n' "$(sed -n 1p <<<"$output")" >"$BENCH_OUT"
        header_written=true
    fi
    printf '%s,%s\n' "$run" "$(sed -n 2p <<<"$output")" >>"$BENCH_OUT"
    gzip -9 -c "$tmp/$run.csv" >"$DATA_DIR/task4x-latency-raw-$run.csv.gz"
    # The seconds column of the row.
    awk -F, 'NR == 1 { for (i = 1; i <= NF; i++) if ($i == "seconds") c = i } NR == 2 { print $c }' <<<"$output"
}

bench w1 w1 >/dev/null
majority_seconds=$(bench majority majority)
kill_after=$(awk -v m="$majority_seconds" -v f="$KILL_FRACTION" 'BEGIN { printf "%.1f", m * f }')
bench majority-kill majority --kill-primary-after "$kill_after" >/dev/null

python3 "$script_dir/task4x-latency.py" --data-dir "$DATA_DIR" \
    "w1=$tmp/w1.csv" "majority=$tmp/majority.csv" "majority-kill=$tmp/majority-kill.csv"
cat "$BENCH_OUT" "$DATA_DIR/task4x-latency.csv" >&2
