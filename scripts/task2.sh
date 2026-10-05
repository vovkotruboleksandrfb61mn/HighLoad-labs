#!/usr/bin/env bash
# Task 2 measurements.
#   Part I:  every pg-bench variant, WORKERS x INCREMENTS, REPEATS times
#            -> reports/data/task2-bench.csv
#   Part II: counter-server --store postgres with loadgen, WEB_CLIENTS x
#            WEB_REQUESTS, REPEATS times -> reports/data/task2-web.csv
#
# Starts PostgreSQL with scripts/pg.sh if it is not running, and stops it
# again at the end in that case.
#
# Overridable: PARTS ("bench web"), VARIANTS (all six), WORKERS (10),
# INCREMENTS (10000), REPEATS (3), WEB_CLIENTS (10), WEB_REQUESTS (10000),
# BENCH_OUT, WEB_OUT.
set -euo pipefail
script_dir=$(cd "$(dirname "$0")" && pwd)
. "$script_dir/env.sh"

PARTS=${PARTS:-"bench web"}
VARIANTS=${VARIANTS:-"lost-update serializable serializable-retry in-place for-update optimistic"}
WORKERS=${WORKERS:-10}
INCREMENTS=${INCREMENTS:-10000}
REPEATS=${REPEATS:-3}
WEB_CLIENTS=${WEB_CLIENTS:-10}
WEB_REQUESTS=${WEB_REQUESTS:-10000}
BENCH_OUT=${BENCH_OUT:-$HLS_ROOT/reports/data/task2-bench.csv}
WEB_OUT=${WEB_OUT:-$HLS_ROOT/reports/data/task2-web.csv}

started_pg=false
cleanup() {
    if $started_pg; then
        "$script_dir/pg.sh" stop
    fi
}
trap cleanup EXIT

if ! "$script_dir/pg.sh" status >/dev/null 2>&1; then
    "$script_dir/pg.sh" start
    started_pg=true
fi

failures=0

run_bench() {
    hls_build_release --product pg-bench
    mkdir -p "$(dirname "$BENCH_OUT")"
    local header_written=false repeat variant output status row
    for repeat in $(seq 1 "$REPEATS"); do
        for variant in $VARIANTS; do
            echo "== pg-bench $variant, repeat $repeat ($WORKERS x $INCREMENTS)" >&2
            status=0
            output=$("$HLS_BIN/pg-bench" --header --variant "$variant" --workers "$WORKERS" \
                --increments "$INCREMENTS" --pg-url "$PG_URL") || status=$?
            if [ -z "$output" ]; then
                echo "pg-bench $variant failed (status $status)" >&2
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
            echo "$row" >&2
        done
    done
    echo "wrote $BENCH_OUT" >&2
}

run_web() {
    STORES=postgres CLIENTS="$WEB_CLIENTS" REQUESTS="$WEB_REQUESTS" REPEATS="$REPEATS" OUT="$WEB_OUT" \
        "$script_dir/task1.sh" || failures=$((failures + 1))
}

for part in $PARTS; do
    case "$part" in
        bench) run_bench ;;
        web) run_web ;;
        *) echo "unknown part: $part (bench|web)" >&2; exit 2 ;;
    esac
done

if [ "$failures" -gt 0 ]; then
    echo "$failures run(s) failed or counted wrong" >&2
    exit 1
fi
