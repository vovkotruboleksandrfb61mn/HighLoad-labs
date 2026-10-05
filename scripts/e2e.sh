#!/usr/bin/env bash
# End-to-end check: for each store, start counter-server, run loadgen with
# CLIENTS x REQUESTS, and require that GET /count matches exactly.
#
# Overridable: STORES ("memory disk"), CLIENTS (10), REQUESTS (1000).
set -euo pipefail
. "$(dirname "$0")/env.sh"

STORES=${STORES:-"memory disk"}
CLIENTS=${CLIENTS:-10}
REQUESTS=${REQUESTS:-1000}

trap hls_stop_server EXIT

hls_build_release --product counter-server
hls_build_release --product loadgen

for store in $STORES; do
    hls_start_server "$store"
    # Unknown routes are 404, wrong methods too.
    code=$(curl -s -o /dev/null -w '%{http_code}' "$SERVER_URL/nope")
    [ "$code" = 404 ] || { echo "GET /nope returned $code, expected 404" >&2; exit 1; }
    code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$SERVER_URL/inc")
    [ "$code" = 404 ] || { echo "POST /inc returned $code, expected 404" >&2; exit 1; }

    "$HLS_BIN/loadgen" --header --clients "$CLIENTS" --requests "$REQUESTS" \
        --url "$SERVER_URL" --store-label "$store"
    count=$(curl -sf "$SERVER_URL/count")
    expected=$((CLIENTS * REQUESTS))
    [ "$count" = "$expected" ] || { echo "$store: /count is $count, expected $expected" >&2; exit 1; }
    if [ "$store" = disk ]; then
        on_disk=$((10#$(cat "$HLS_DATA/counter.txt")))
        [ "$on_disk" = "$expected" ] || { echo "disk: file holds $on_disk, expected $expected" >&2; exit 1; }
    fi
    hls_stop_server
    echo "e2e $store: OK ($count)" >&2
done
