#!/usr/bin/env bash
# Task 5, extra research: stale reads and three ways to increment.
#
#   scripts/task5x-anomalies.sh [part ...]     (reads methods by default)
#
#   reads    the counter column with WORKERS x INCREMENTS increments at ONE
#            or QUORUM and a reader at ONE or QUORUM next to them (all four
#            pairs): how many reads were behind the increments already
#            acknowledged, and how many went backwards
#   methods  WORKERS x METHOD_INCREMENTS increments of a bigint column by
#            read-modify-write (ONE and QUORUM) and by lightweight
#            transaction (QUORUM, reads SERIAL), next to the counter column
#            (QUORUM): lost updates, conflicts, throughput
# Every run REPEATS times. Rows go to $OUT (reports/data/task5x-anomalies.csv).
#
# Starts the cluster if it is not running (and leaves it running).
# Overridable: WORKERS (10), INCREMENTS (10000), METHOD_INCREMENTS (1000),
# REPEATS (3), OUT.
set -euo pipefail

script_dir=$(cd "$(dirname "$0")" && pwd)
if ! command -v cqlsh >/dev/null && [ -z "${HLS_CASSANDRA_REEXEC:-}" ] && command -v direnv >/dev/null; then
    HLS_CASSANDRA_REEXEC=1 exec direnv exec "$script_dir/.." "$0" "$@"
fi
. "$script_dir/task5-common.sh"

WORKERS=${WORKERS:-10}
INCREMENTS=${INCREMENTS:-10000}
METHOD_INCREMENTS=${METHOD_INCREMENTS:-1000}
REPEATS=${REPEATS:-3}
OUT=${OUT:-$HLS_ROOT/reports/data/task5x-anomalies.csv}
parts=${*:-reads methods}

cluster_up
hls_build_release --product cassandra-bench
mkdir -p "$(dirname "$OUT")"

header=true
[ -s "$OUT" ] && header=false

bench() {
    local label=$1 output
    shift
    echo "== $label, repeat $repeat" >&2
    output=$("$HLS_BIN/cassandra-bench" --header --label "$label" --workers "$WORKERS" \
        --hosts "$CASSANDRA_HOSTS" --port "$CASSANDRA_PORT" "$@" 2> >(grep -v '\[WARN\]' >&2)) || true
    wait
    [ -n "$output" ] || { echo "cassandra-bench failed for $label" >&2; return; }
    if $header; then
        printf 'repeat,%s\n' "$(sed -n 1p <<<"$output")" >"$OUT"
        header=false
    fi
    printf '%s,%s\n' "$repeat" "$(sed -n 2p <<<"$output")" | tee -a "$OUT" >&2
}

for repeat in $(seq 1 "$REPEATS"); do
    for part in $parts; do
        case $part in
            reads)
                for w in one quorum; do
                    for r in one quorum; do
                        bench "w-$w-r-$r" --consistency "$w" --reader-consistency "$r" --increments "$INCREMENTS"
                    done
                done
                ;;
            methods)
                bench rmw-one --method rmw --consistency one --increments "$METHOD_INCREMENTS"
                bench rmw-quorum --method rmw --consistency quorum --increments "$METHOD_INCREMENTS"
                bench lwt-quorum --method lwt --consistency quorum --increments "$METHOD_INCREMENTS"
                bench counter-quorum --method counter --consistency quorum --increments "$METHOD_INCREMENTS"
                ;;
            *) echo "unknown part: $part" >&2; exit 2 ;;
        esac
    done
done
echo "wrote $OUT" >&2
