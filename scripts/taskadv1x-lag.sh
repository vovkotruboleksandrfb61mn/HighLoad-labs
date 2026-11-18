#!/usr/bin/env bash
# Task Adv 1, extra: how far the table trails the map.
#
#   scripts/taskadv1x-lag.sh
#
# For likes_write_through and likes_write_behind: counter-server on that map,
# loadgen with 10 clients incrementing user 1, and next to them
# `cache-check --follow`, which reads user 1 from the map and from the table
# every 100 ms. Write-through runs 10 x 300 increments (about 10 s at its
# ~300 increments/s), write-behind 10 x 2000 (about 10 s); sampling goes on
# for SECONDS (25) from the start of the load, so the last write-behind
# flush shows too. Samples go to $OUT_DIR/<map>.csv
# (reports/data/taskadv1x-lag/).
#
# Two more write-behind runs, user 7, with `curl` instead of loadgen:
#   single   one increment after more than 5 s of quiet   -> single.csv
#   trickle  100 increments, one every ~0.1 s              -> trickle.csv
# They show how write-delay-seconds really works: the coalesced queue keeps
# the time of a key's oldest unstored change, and the store worker (every
# 1 s) moves its cut-off forward only every write-delay-seconds; an entry
# whose value changed while it was being stored stays queued with its old
# time and goes out on the next 1 s tick.
#
#   scripts/taskadv1x-lag.sh [load] [single] [trickle]   (all by default)
#
# Needs PostgreSQL and the Hazelcast cluster (scripts/taskadv1.sh starts
# them). Overridable: SECONDS_TO_FOLLOW (25), OUT_DIR.
set -euo pipefail

script_dir=$(cd "$(dirname "$0")" && pwd)
if [ -z "${HLS_PG_JDBC_JAR:-}" ] && [ -z "${HLS_ADV1_REEXEC:-}" ] && command -v direnv >/dev/null; then
    HLS_ADV1_REEXEC=1 exec direnv exec "$script_dir/.." "$0" "$@"
fi
export HLS_IO_URING=${HLS_IO_URING:-0}
. "$script_dir/env.sh"

SECONDS_TO_FOLLOW=${SECONDS_TO_FOLLOW:-25}
OUT_DIR=${OUT_DIR:-$HLS_ROOT/reports/data/taskadv1x-lag}
mkdir -p "$OUT_DIR"
trap hls_stop_server EXIT

parts=${*:-load single trickle}

load() {
    local scenario map requests follower
    for scenario in hazelcast-write-through hazelcast-write-behind; do
        map=likes_${scenario#hazelcast-}
        map=${map//-/_}
        case $scenario in
            *through) requests=300 ;;
            *) requests=2000 ;;
        esac
        echo "== $map: 10 x $requests" >&2
        hls_start_server "$scenario"
        curl -sf -X POST "$SERVER_URL/reset?user_id=1"
        # Let a write-behind reset reach the table before sampling starts.
        sleep 7
        "$HLS_BIN/cache-check" --map "$map" --users 1 --follow "$SECONDS_TO_FOLLOW" --interval 100 \
            --hz-address "$HZ_ADDRESS" --pg-url "$PG_URL" >"$OUT_DIR/$map.csv" &
        follower=$!
        sleep 1
        "$HLS_BIN/loadgen" --header --clients 10 --requests "$requests" --user-id 1 \
            --url "$SERVER_URL" --store-label "$scenario" >&2
        wait "$follower"
        hls_stop_server
        tail -n 1 "$OUT_DIR/$map.csv" >&2
    done
}

# probe <name> <seconds to follow> <increments> <pause between them>
probe() {
    local name=$1 follow=$2 count=$3 pause=$4 follower i
    echo "== write-behind, $name: $count increment(s), $pause s apart" >&2
    hls_start_server hazelcast-write-behind
    curl -sf -X POST "$SERVER_URL/reset?user_id=7"
    sleep 8
    "$HLS_BIN/cache-check" --map likes_write_behind --users 7 --follow "$follow" --interval 100 \
        --hz-address "$HZ_ADDRESS" --pg-url "$PG_URL" >"$OUT_DIR/$name.csv" &
    follower=$!
    sleep 1
    for i in $(seq 1 "$count"); do
        curl -sf -X POST "$SERVER_URL/inc?user_id=7"
        sleep "$pause"
    done
    wait "$follower"
    hls_stop_server
    tail -n 1 "$OUT_DIR/$name.csv" >&2
}

for part in $parts; do
    case $part in
        load) load ;;
        single) probe single 10 1 0 ;;
        trickle) probe trickle 18 100 0.1 ;;
        *) echo "unknown part: $part" >&2; exit 2 ;;
    esac
done
echo "wrote $OUT_DIR" >&2
