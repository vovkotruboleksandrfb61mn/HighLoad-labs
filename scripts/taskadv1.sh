#!/usr/bin/env bash
# Task Adv 1: the per-user counter written directly to PostgreSQL and through
# a Hazelcast IMap with write-through and write-behind caching.
#
#   scripts/taskadv1.sh [scenario ...]
#
# Scenarios (all by default), each counter-server driven by loadgen with
# CLIENTS x REQUESTS `POST /inc?user_id=N`:
#   postgres-users            the table user_likes, one upsert per increment
#   hazelcast-write-through   IMap likes_write_through, optimistic replace;
#                             the generic MapStore writes every change
#   hazelcast-write-behind    IMap likes_write_behind, the same, stored 5 s
#                             later and coalesced
# and two user layouts (USER_MODES): `shared`, every client increments user 1,
# and `distinct`, client i increments user 1 + i.
#
# Every run REPEATS times. After a run: GET /stats (optimistic conflicts),
# cache-check (map against table, waiting for write-behind, then evictAll and
# a read through the MapLoader) and, after the backends have flushed their
# statistics, the rows written to user_likes (pg_stat_user_tables).
# Rows go to $OUT (reports/data/taskadv1.csv), the map/table comparison to
# $CHECK_OUT (reports/data/taskadv1-check.csv).
#
# Starts PostgreSQL and the Hazelcast cluster if they are not running (and
# leaves them running). Overridable: CLIENTS (10), REQUESTS (10000),
# REPEATS (3), USER_MODES ("shared distinct"), OUT, CHECK_OUT,
# FIRST_REPEAT (1: more repeats can be added later; rows are appended).
set -euo pipefail

script_dir=$(cd "$(dirname "$0")" && pwd)
if [ -z "${HLS_PG_JDBC_JAR:-}" ] && [ -z "${HLS_ADV1_REEXEC:-}" ] && command -v direnv >/dev/null; then
    HLS_ADV1_REEXEC=1 exec direnv exec "$script_dir/.." "$0" "$@"
fi
# The io_uring engine is not used here, and liburing's headers clash with
# newer system ones.
export HLS_IO_URING=${HLS_IO_URING:-0}
. "$script_dir/env.sh"

CLIENTS=${CLIENTS:-10}
REQUESTS=${REQUESTS:-10000}
REPEATS=${REPEATS:-3}
USER_MODES=${USER_MODES:-"shared distinct"}
OUT=${OUT:-$HLS_ROOT/reports/data/taskadv1.csv}
CHECK_OUT=${CHECK_OUT:-$HLS_ROOT/reports/data/taskadv1-check.csv}
scenarios=${*:-postgres-users hazelcast-write-through hazelcast-write-behind}

"$HLS_ROOT/scripts/pg.sh" start
# Member logs away from reports/logs, which keeps the ones of Task 3.
"$HLS_ROOT/scripts/hz.sh" status >/dev/null || HLS_LOG_DIR="$HLS_DATA/hz-logs" "$HLS_ROOT/scripts/hz.sh" start
trap hls_stop_server EXIT

hls_build_release --product counter-server
hls_build_release --product loadgen
hls_build_release --product cache-check
mkdir -p "$(dirname "$OUT")" "$(dirname "$CHECK_OUT")"

psql_value() {
    "$HLS_ROOT/scripts/pg.sh" psql -At -c "$1"
}

# Rows inserted plus rows updated in user_likes since the statistics reset.
# A backend reports its counts within 10 s of going idle.
rows_written() {
    psql_value "SELECT n_tup_ins + n_tup_upd FROM pg_stat_user_tables WHERE relname = 'user_likes'"
}

failures=0
header=true
check_header=true
[ -s "$OUT" ] && header=false
[ -s "$CHECK_OUT" ] && check_header=false
for repeat in $(seq "${FIRST_REPEAT:-1}" "$REPEATS"); do
    for mode in $USER_MODES; do
        users_args=(--user-id 1)
        users=1
        if [ "$mode" = distinct ]; then
            users_args+=(--distinct-users)
            users=$(seq -s, 1 "$CLIENTS")
        fi
        for scenario in $scenarios; do
            echo "== $scenario, $mode users, repeat $repeat" >&2
            hls_start_server "$scenario"
            written_before=$(rows_written)
            status=0
            output=$("$HLS_BIN/loadgen" --header --clients "$CLIENTS" --requests "$REQUESTS" \
                "${users_args[@]}" --url "$SERVER_URL" --store-label "$scenario") || status=$?
            stats=$(curl -sf "$SERVER_URL/stats" | tr -d '\n')
            hls_stop_server
            if [ -z "$output" ]; then
                echo "loadgen failed (status $status) for $scenario" >&2
                failures=$((failures + 1))
                continue
            fi
            [ "$status" -eq 0 ] || failures=$((failures + 1))

            case $scenario in
                hazelcast-write-*)
                    map=likes_${scenario#hazelcast-}
                    map=${map//-/_}
                    check_args=()
                    $check_header && check_args+=(--header) && check_header=false
                    "$HLS_BIN/cache-check" "${check_args[@]}" --label "$repeat-$mode-$scenario" --map "$map" \
                        --users "$users" --wait 30 --evict --hz-address "$HZ_ADDRESS" --pg-url "$PG_URL" \
                        | tee -a "$CHECK_OUT" >&2 || failures=$((failures + 1))
                    ;;
            esac
            sleep 11
            written=$(( $(rows_written) - written_before ))

            conflicts=$(sed -n 's/.*conflicts=\([0-9]*\).*/\1/p' <<<"$stats")
            longest=$(sed -n 's/.*longest_retries=\([0-9]*\).*/\1/p' <<<"$stats")
            if $header; then
                printf 'repeat,users,%s,conflicts,longest_retries,db_rows_written\n' "$(sed -n 1p <<<"$output")" >"$OUT"
                header=false
            fi
            printf '%s,%s,%s,%s,%s,%s\n' "$repeat" "$mode" "$(sed -n 2p <<<"$output")" \
                "$conflicts" "$longest" "$written" | tee -a "$OUT" >&2
        done
    done
done

echo "wrote $OUT and $CHECK_OUT" >&2
if [ "$failures" -gt 0 ]; then
    echo "$failures run(s) failed, counted wrong or left the table behind the map" >&2
    exit 1
fi
