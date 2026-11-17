#!/usr/bin/env bash
# Task 5, extra research: the DataStax C++ driver against our CQL client.
#
#   scripts/task5x-driver.sh
#
# cassandra-bench with WORKERS x INCREMENTS counter increments at ONE and at
# QUORUM through three clients: apple/swift-cassandra-client (DataStax C++
# driver, token-aware), our CQLClient with token-aware routing, and our
# CQLClient sending every statement round-robin. REPEATS times each, the
# configurations interleaved so that drift of the machine hits all alike.
# Rows go to $OUT (reports/data/task5x-driver.csv).
#
# Starts the cluster if it is not running (and leaves it running).
# Overridable: WORKERS (10), INCREMENTS (10000), REPEATS (3), OUT.
set -euo pipefail

script_dir=$(cd "$(dirname "$0")" && pwd)
if ! command -v cqlsh >/dev/null && [ -z "${HLS_CASSANDRA_REEXEC:-}" ] && command -v direnv >/dev/null; then
    HLS_CASSANDRA_REEXEC=1 exec direnv exec "$script_dir/.." "$0" "$@"
fi
. "$script_dir/task5-common.sh"

WORKERS=${WORKERS:-10}
INCREMENTS=${INCREMENTS:-10000}
REPEATS=${REPEATS:-3}
OUT=${OUT:-$HLS_ROOT/reports/data/task5x-driver.csv}

cluster_up
hls_build_release --product cassandra-bench
mkdir -p "$(dirname "$OUT")"

header=true
for repeat in $(seq 1 "$REPEATS"); do
    for level in one quorum; do
        for client in datastax own own-round-robin; do
            args=(--driver "${client%-round-robin}")
            [ "$client" = own-round-robin ] && args+=(--no-token-aware)
            echo "== $client, $level, repeat $repeat" >&2
            output=$("$HLS_BIN/cassandra-bench" --header --label "$client" --consistency "$level" \
                --workers "$WORKERS" --increments "$INCREMENTS" --hosts "$CASSANDRA_HOSTS" --port "$CASSANDRA_PORT" \
                "${args[@]}" 2> >(grep -v '\[WARN\]' >&2))
            wait
            if $header; then
                printf 'repeat,%s\n' "$(sed -n 1p <<<"$output")" >"$OUT"
                header=false
            fi
            printf '%s,%s\n' "$repeat" "$(sed -n 2p <<<"$output")" | tee -a "$OUT" >&2
        done
    done
done
echo "wrote $OUT" >&2
