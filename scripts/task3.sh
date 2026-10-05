#!/usr/bin/env bash
# Task 3 measurements on the 3-member Hazelcast 5.4.0 cluster.
#
#  1. bench: hz-bench, every variant REPEATS times, TASKS x INCREMENTS each
#     -> reports/data/task3-bench.csv
#  2. web: counter-server --store hazelcast (IAtomicLong) driven by loadgen
#     with WEB_CLIENTS x WEB_REQUESTS, REPEATS times (via scripts/task1.sh)
#     -> reports/data/task3-web.csv
#
# Starts the cluster with scripts/hz.sh if it is not already running, and
# stops it again afterwards in that case.
#
# Overridable: PARTS ("bench web"), VARIANTS (all four), TASKS (10),
# INCREMENTS (10000), REPEATS (3), WEB_CLIENTS (10), WEB_REQUESTS (10000),
# BENCH_OUT, WEB_OUT. A quick smoke run:
#   INCREMENTS=300 WEB_REQUESTS=300 REPEATS=1 BENCH_OUT=/tmp/b.csv WEB_OUT=/tmp/w.csv scripts/task3.sh
set -euo pipefail
. "$(dirname "$0")/env.sh"

PARTS=${PARTS:-"bench web"}
VARIANTS=${VARIANTS:-"map-nolock map-pessimistic map-optimistic atomic-long"}
TASKS=${TASKS:-10}
INCREMENTS=${INCREMENTS:-10000}
REPEATS=${REPEATS:-3}
WEB_CLIENTS=${WEB_CLIENTS:-10}
WEB_REQUESTS=${WEB_REQUESTS:-10000}
BENCH_OUT=${BENCH_OUT:-$HLS_ROOT/reports/data/task3-bench.csv}
WEB_OUT=${WEB_OUT:-$HLS_ROOT/reports/data/task3-web.csv}

started_cluster=false
cleanup() {
    if $started_cluster; then
        "$HLS_ROOT/scripts/hz.sh" stop
    fi
}
trap cleanup EXIT

if ! "$HLS_ROOT/scripts/hz.sh" status >/dev/null; then
    started_cluster=true
    "$HLS_ROOT/scripts/hz.sh" start
fi

failures=0

run_bench() {
    hls_build_release --product hz-bench
    mkdir -p "$(dirname "$BENCH_OUT")"
    local header_written=false repeat variant output status row
    for repeat in $(seq 1 "$REPEATS"); do
        for variant in $VARIANTS; do
            status=0
            output=$("$HLS_BIN/hz-bench" --header --variant "$variant" --tasks "$TASKS" \
                --increments "$INCREMENTS" --address "$HZ_ADDRESS") || status=$?
            if [ -z "$output" ]; then
                echo "hz-bench failed (status $status) for $variant" >&2
                failures=$((failures + 1))
                continue
            fi
            [ "$status" -eq 0 ] || failures=$((failures + 1))
            if ! $header_written; then
                printf 'repeat,%s\n' "$(sed -n 1p <<<"$output")" >"$BENCH_OUT"
                header_written=true
            fi
            row=$(sed -n 2p <<<"$output")
            printf '%s,%s\n' "$repeat" "$row" >>"$BENCH_OUT"
            echo "repeat $repeat: $row" >&2
        done
    done
    echo "wrote $BENCH_OUT" >&2
}

run_web() {
    STORES=hazelcast CLIENTS="$WEB_CLIENTS" REQUESTS="$WEB_REQUESTS" REPEATS="$REPEATS" OUT="$WEB_OUT" \
        "$HLS_ROOT/scripts/task1.sh" || failures=$((failures + 1))
}

for part in $PARTS; do
    case $part in
        bench) run_bench ;;
        web) run_web ;;
        *) echo "unknown part: $part" >&2; exit 2 ;;
    esac
done

if [ "$failures" -gt 0 ]; then
    echo "$failures run(s) failed or lost updates they must not lose" >&2
    exit 1
fi
