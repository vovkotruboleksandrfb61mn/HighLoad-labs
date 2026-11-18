#!/usr/bin/env bash
# Task Adv 1: a terminal session that shows the caching at work, for the
# report's pictures.
#
#   scripts/taskadv1-demo.sh
#
#  1. the table user_likes and the maps' MapStore settings;
#  2. write-through: three POST /inc?user_id=42 through likes_write_through,
#     and the row in the table right after;
#  3. write-behind: three more through likes_write_behind, the table right
#     after (still old) and 6 s later (caught up);
#  4. read-through: evictAll, then GET /count loads the value from the table
#     (the member log shows the MapLoader's query mapping);
#  5. the 100K runs and checks: the end of reports/data/taskadv1*.csv.
#
# Transcript: reports/logs/taskadv1-demo.txt, pictures
# reports/img/taskadv1-demo-{a,b,...}.png. Needs PostgreSQL and the Hazelcast
# cluster (scripts/taskadv1.sh starts them).
set -euo pipefail

script_dir=$(cd "$(dirname "$0")" && pwd)
if [ -z "${HLS_PG_JDBC_JAR:-}" ] && [ -z "${HLS_ADV1_REEXEC:-}" ] && command -v direnv >/dev/null; then
    HLS_ADV1_REEXEC=1 exec direnv exec "$script_dir/.." "$0" "$@"
fi
export HLS_IO_URING=${HLS_IO_URING:-0}
# The transcript helpers (run, note, gap, shoot) of the Task 5 scripts; no
# Cassandra is touched.
. "$script_dir/task5-common.sh"

out=$HLS_ROOT/reports/logs/taskadv1-demo.txt
: >"$out"
trap hls_stop_server EXIT
user=42


# A fresh start for the demo user: no row, no cache entry.
scripts/pg.sh psql -q -c "DELETE FROM user_likes WHERE user_id = $user" >/dev/null
for map in likes_write_through likes_write_behind; do
    "$HLS_BIN/cache-check" --map "$map" --users "$user" --evict >/dev/null 2>&1 || true
done

note "1. Таблиця user_likes і налаштування MapStore обох мап (лише конфігурація, без власного коду)."
run scripts/pg.sh psql -c '\d user_likes'
run grep -A 14 'likes_write_through:' deploy/hazelcast-node1.yaml
run grep -A 11 'likes_write_behind:' deploy/hazelcast-node1.yaml
gap

note "2. Write-through: рядок у таблиці змінено ще до відповіді на запит."
hls_start_server hazelcast-write-through
run curl -s -w "%{http_code}\n" -X POST "$SERVER_URL/inc?user_id=$user"
run curl -s -w "%{http_code}\n" -X POST "$SERVER_URL/inc?user_id=$user"
run curl -s -w "%{http_code}\n" -X POST "$SERVER_URL/inc?user_id=$user"
run curl -s "$SERVER_URL/count?user_id=$user"
run scripts/pg.sh psql -c "SELECT * FROM user_likes WHERE user_id = $user"
hls_stop_server
gap

note "3. Write-behind: мапа вже має 6, таблиця ще 3; через write-delay-seconds (5 с) таблиця наздоганяє мапу."
hls_start_server hazelcast-write-behind
run curl -s -w "%{http_code}\n" -X POST "$SERVER_URL/inc?user_id=$user"
run curl -s -w "%{http_code}\n" -X POST "$SERVER_URL/inc?user_id=$user"
run curl -s -w "%{http_code}\n" -X POST "$SERVER_URL/inc?user_id=$user"
run curl -s "$SERVER_URL/count?user_id=$user"
run scripts/pg.sh psql -c "SELECT * FROM user_likes WHERE user_id = $user"
run sleep 6
run scripts/pg.sh psql -c "SELECT * FROM user_likes WHERE user_id = $user"
gap

note "4. Read-through: evictAll прибирає записи з пам'яті (таблицю не чіпає), наступне читання йде через MapLoader."
run .build/release/cache-check --header --map likes_write_behind --users "$user" --evict
run curl -s "$SERVER_URL/count?user_id=$user"
hls_stop_server
gap

note "5. Прогони по 100 000 інкрементів (reports/data/taskadv1.csv) і перевірка мапа = таблиця."
run bash -c "cut -d, -f1,2,3,7,8,9,11,12,14 reports/data/taskadv1.csv | column -s, -t"
gap
run bash -c "cut -d, -f1,3,4,5,6,7,8,9 reports/data/taskadv1-check.csv | grep -v ',[2-9],\|,10,' | column -s, -t"

shoot "$out" "$HLS_ROOT/reports/img/taskadv1-demo" 40
echo "wrote $out" >&2
