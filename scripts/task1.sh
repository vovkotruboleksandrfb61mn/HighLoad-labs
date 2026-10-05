#!/usr/bin/env bash
# Task 1 measurement matrix: for each store, 1/2/5/10 clients x 10K requests,
# 3 repeats each. Writes one CSV row per run to reports/data/task1.csv.
#
# Overridable: STORES ("memory disk"), CLIENTS ("1 2 5 10"), REQUESTS (10000),
# REPEATS (3), OUT (reports/data/task1.csv). The same script measures the
# postgres and hazelcast stores once their backends are running, e.g.
#   STORES=postgres OUT=reports/data/task2-web.csv scripts/task1.sh
set -euo pipefail
. "$(dirname "$0")/env.sh"

STORES=${STORES:-"memory disk"}
CLIENTS=${CLIENTS:-"1 2 5 10"}
REQUESTS=${REQUESTS:-10000}
REPEATS=${REPEATS:-3}
OUT=${OUT:-$HLS_ROOT/reports/data/task1.csv}

trap hls_stop_server EXIT

hls_build_release --product counter-server
hls_build_release --product loadgen
mkdir -p "$(dirname "$OUT")"

header_written=false
failures=0
for store in $STORES; do
    echo "== store: $store" >&2
    hls_start_server "$store"
    for repeat in $(seq 1 "$REPEATS"); do
        for clients in $CLIENTS; do
            status=0
            output=$("$HLS_BIN/loadgen" --header --clients "$clients" --requests "$REQUESTS" \
                --url "$SERVER_URL" --store-label "$store") || status=$?
            if [ -z "$output" ]; then
                echo "loadgen failed (status $status) for $store, $clients clients" >&2
                failures=$((failures + 1))
                continue
            fi
            [ "$status" -eq 0 ] || failures=$((failures + 1))
            if ! $header_written; then
                printf 'repeat,%s\n' "$(sed -n 1p <<<"$output")" >"$OUT"
                header_written=true
            fi
            row=$(sed -n 2p <<<"$output")
            printf '%s,%s\n' "$repeat" "$row" >>"$OUT"
            echo "repeat $repeat: $row" >&2
        done
    done
    hls_stop_server
done

echo "wrote $OUT" >&2
if [ "$failures" -gt 0 ]; then
    echo "$failures run(s) failed or counted wrong" >&2
    exit 1
fi
