#!/usr/bin/env bash
# Task 5, Part 3: the likes counter in Cassandra (counter column, keyspace
# counter_rf3 with replication factor 3) on the three-node cluster.
#
#  1. bench: cassandra-bench, WORKERS separate clients x INCREMENTS
#     `UPDATE likes = likes + 1` each, at CONSISTENCY ONE and QUORUM,
#     REPEATS times -> reports/data/task5-bench.csv
#  2. web: counter-server --store cassandra (increments and reads at ONE,
#     then at QUORUM) driven by loadgen with WEB_CLIENTS x WEB_REQUESTS,
#     REPEATS times (via scripts/task1.sh, server on port 21400)
#     -> reports/data/task5-web.csv
#
# Starts the cluster if it is not running (and leaves it running).
# Overridable: PARTS ("bench web"), LEVELS ("one quorum"), WORKERS (10),
# INCREMENTS (10000), REPEATS (3), WEB_CLIENTS (10), WEB_REQUESTS (10000),
# BENCH_OUT, WEB_OUT.
set -euo pipefail

script_dir=$(cd "$(dirname "$0")" && pwd)
if ! command -v cqlsh >/dev/null && [ -z "${HLS_CASSANDRA_REEXEC:-}" ] && command -v direnv >/dev/null; then
    HLS_CASSANDRA_REEXEC=1 exec direnv exec "$script_dir/.." "$0" "$@"
fi
. "$script_dir/task5-common.sh"

PARTS=${PARTS:-"bench web"}
LEVELS=${LEVELS:-"one quorum"}
WORKERS=${WORKERS:-10}
INCREMENTS=${INCREMENTS:-10000}
REPEATS=${REPEATS:-3}
WEB_CLIENTS=${WEB_CLIENTS:-10}
WEB_REQUESTS=${WEB_REQUESTS:-10000}
BENCH_OUT=${BENCH_OUT:-$HLS_ROOT/reports/data/task5-bench.csv}
WEB_OUT=${WEB_OUT:-$HLS_ROOT/reports/data/task5-web.csv}

cluster_up
failures=0

run_bench() {
    hls_build_release --product cassandra-bench
    mkdir -p "$(dirname "$BENCH_OUT")"
    local repeat level output status header=true
    for repeat in $(seq 1 "$REPEATS"); do
        for level in $LEVELS; do
            echo "== $level, repeat $repeat" >&2
            status=0
            output=$("$HLS_BIN/cassandra-bench" --header --consistency "$level" --workers "$WORKERS" \
                --increments "$INCREMENTS" --hosts "$CASSANDRA_HOSTS" --port "$CASSANDRA_PORT") || status=$?
            if [ -z "$output" ]; then
                echo "cassandra-bench failed (status $status) at $level" >&2
                failures=$((failures + 1))
                continue
            fi
            [ "$status" -eq 0 ] || failures=$((failures + 1))
            if $header; then
                printf 'repeat,%s\n' "$(sed -n 1p <<<"$output")" >"$BENCH_OUT"
                header=false
            fi
            printf '%s,%s\n' "$repeat" "$(sed -n 2p <<<"$output")" | tee -a "$BENCH_OUT" >&2
        done
    done
    echo "wrote $BENCH_OUT" >&2
}

run_web() {
    local stores="" level
    for level in $LEVELS; do stores+=" cassandra-$level"; done
    STORES=$stores CLIENTS="$WEB_CLIENTS" REQUESTS="$WEB_REQUESTS" REPEATS="$REPEATS" OUT="$WEB_OUT" \
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
    echo "$failures run(s) failed or miscounted" >&2
    exit 1
fi
