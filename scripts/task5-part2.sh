#!/usr/bin/env bash
# Task 5, Part 2: replication in the three-node cluster.
#
#   scripts/task5-part2.sh [1] [2] [3] [4] [5]     (all by default)
#
#   1. nodetool status of the cluster; keyspaces ks_rf1, ks_rf2, ks_rf3
#      (SimpleStrategy, replication factor 1, 2, 3) with a table kv each;
#      writes and reads through every node.
#   2. ROWS rows per keyspace (COPY FROM), then per keyspace: nodetool
#      status (effective ownership), the partitions each node holds, and
#      the replicas of one row (nodetool getendpoints).
#   3. Node 3 is stopped. For every keyspace, writes and reads with
#      CONSISTENCY ONE/TWO/THREE, for a row with a replica on node 3 and,
#      where there is one, a row without.
#   4. Node 3 is back; the network between the nodes is cut
#      (scripts/cassandra.sh split). With CONSISTENCY ONE the same key of
#      ks_rf3.kv is written with a different value through every node.
#   5. The network is restored; the conflict is resolved by the cell
#      timestamps (last write wins); a write with an older USING TIMESTAMP
#      loses even though it was made later.
#
# Scenarios 4 and 5 belong together (5 reads what 4 wrote); 3 needs the
# keyspaces of 1 and the rows of 2.
# Transcripts go to $OUT_DIR/task5-part2-N.txt, pictures (SHOTS=true) to
# $IMG_DIR/task5-part2-N-[a-z].png. After scenarios 4 and 5 the gossip events
# (which node saw which go DOWN and come back UP, from the nodes' logs) go to
# $OUT_DIR/task5-part2-gossip.txt.
#
# Starts the cluster if it is not running (and leaves it running).
# Overridable: OUT_DIR (reports/logs), IMG_DIR (reports/img), SHOTS (true),
# ROWS (1000).
set -euo pipefail

script_dir=$(cd "$(dirname "$0")" && pwd)
if ! command -v cqlsh >/dev/null && [ -z "${HLS_CASSANDRA_REEXEC:-}" ] && command -v direnv >/dev/null; then
    HLS_CASSANDRA_REEXEC=1 exec direnv exec "$script_dir/.." "$0" "$@"
fi
. "$script_dir/task5-common.sh"

OUT_DIR=${OUT_DIR:-$HLS_ROOT/reports/logs}
IMG_DIR=${IMG_DIR:-$HLS_ROOT/reports/img}
SHOTS=${SHOTS:-true}
ROWS=${ROWS:-1000}
scenarios=${*:-1 2 3 4 5}
mkdir -p "$OUT_DIR"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

cs() { scripts/cassandra.sh "$@"; }

# The node numbers (1-3) holding a replica of `key` in keyspace `ks`.
replicas() {
    cs nodetool 1 getendpoints "$1" kv "$2" | sed -n 's/^127\.0\.0\.1\([1-3]\)$/\1/p' | sort | tr '\n' ' '
}

# The first key in 1..ROWS whose replicas include node 3 (want=with) or
# do not (want=without).
pick_key() {
    local ks=$1 want=$2 key r
    for key in $(seq 1 "$ROWS"); do
        r=$(replicas "$ks" "$key")
        case "$want:$r" in
            with:*3*) echo "$key"; return ;;
            without:*3*) ;;
            without:*) echo "$key"; return ;;
        esac
    done
}

begin() {
    out="$OUT_DIR/task5-part2-$1.txt"
    : >"$out"
}

finish() {
    if $SHOTS; then shoot "$out" "$IMG_DIR/$(basename "$out" .txt)"; fi
}

scenario1() {
    begin 1
    cluster_up
    cqlsh_quiet 1 -e 'DROP KEYSPACE IF EXISTS ks_rf1; DROP KEYSPACE IF EXISTS ks_rf2; DROP KEYSPACE IF EXISTS ks_rf3;'
    note "Кластер із трьох вузлів: стан кожного (U = up, N = normal), частка кільця, Host ID."
    run scripts/cassandra.sh status
    gap
    cql_session "$out" 1 <<'CQL'
-- Три keyspace з SimpleStrategy і коефіцієнтом реплікації 1, 2 і 3, у кожному проста таблиця.
CREATE KEYSPACE ks_rf1 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
CREATE KEYSPACE ks_rf2 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2};
CREATE KEYSPACE ks_rf3 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};
CREATE TABLE ks_rf1.kv (id int PRIMARY KEY, value text);
CREATE TABLE ks_rf2.kv (id int PRIMARY KEY, value text);
CREATE TABLE ks_rf3.kv (id int PRIMARY KEY, value text);
DESCRIBE KEYSPACES
CQL
    note "Пишемо через node1, читаємо через node2 і node3, і навпаки."
    gap
    cql_session "$out" 1 <<'CQL'
INSERT INTO ks_rf1.kv (id, value) VALUES (0, 'записано через node1');
INSERT INTO ks_rf2.kv (id, value) VALUES (0, 'записано через node1');
INSERT INTO ks_rf3.kv (id, value) VALUES (0, 'записано через node1');
CQL
    cql_session "$out" 2 <<'CQL'
SELECT * FROM ks_rf1.kv WHERE id = 0;
SELECT * FROM ks_rf2.kv WHERE id = 0;
UPDATE ks_rf3.kv SET value = 'змінено через node2' WHERE id = 0;
CQL
    cql_session "$out" 3 <<'CQL'
SELECT * FROM ks_rf3.kv WHERE id = 0;
SELECT * FROM ks_rf1.kv WHERE id = 0;
CQL
    finish
}

scenario2() {
    begin 2
    seq 1 "$ROWS" | awk '{ printf "%d,рядок %d\n", $1, $1 }' >"$tmp/kv.csv"
    note "Наповнюємо кожен keyspace $ROWS рядками (COPY FROM з kv.csv: id,value)."
    gap
    for ks in ks_rf1 ks_rf2 ks_rf3; do
        stamp
        printf '[%s] $ scripts/cassandra.sh cqlsh 1 -e "COPY %s.kv (id, value) FROM '"'"'kv.csv'"'"'"\n' "$STAMP" "$ks" >>"$out"
        (cd "$tmp" && cqlsh_quiet 1 -e "COPY $ks.kv (id, value) FROM 'kv.csv'" 2>&1 | grep -E 'rows imported' >>"$out")
    done
    for n in 1 2 3; do cs nodetool "$n" flush >/dev/null; done
    gap
    for ks in ks_rf1 ks_rf2 ks_rf3; do
        note "Розподіл $ks: Owns (effective) це частка даних keyspace на вузлі з урахуванням реплік."
        run scripts/cassandra.sh nodetool 1 status "$ks"
        gap
        note "Скільки партицій (рядків) $ks.kv лежить на кожному вузлі:"
        for n in 1 2 3; do
            stamp
            printf '[%s] $ scripts/cassandra.sh nodetool %s tablestats %s.kv | grep "partitions"\n' "$STAMP" "$n" "$ks" >>"$out"
            cs nodetool "$n" tablestats "$ks.kv" | grep 'Number of partitions' | sed 's/^[[:space:]]*//' >>"$out"
        done
        gap
        note "Вузли, на яких зберігається рядок id = 7 з $ks.kv:"
        run scripts/cassandra.sh nodetool 1 getendpoints "$ks" kv 7
        gap
    done
    finish
}

# check <ks> <key> <level>: an INSERT and a SELECT at that consistency level.
check_lines() {
    local ks=$1 key=$2 level
    shift 2
    for level in "$@"; do
        printf 'CONSISTENCY %s\n' "$level"
        printf "INSERT INTO %s.kv (id, value) VALUES (%s, 'CL %s');\n" "$ks" "$key" "$level"
        printf 'SELECT * FROM %s.kv WHERE id = %s;\n' "$ks" "$key"
    done
}

scenario3() {
    begin 3
    cluster_up
    note "Вимикаємо node3 (штатна зупинка), решта вузлів має побачити його як DN."
    run scripts/cassandra.sh stop-node 3
    cs wait-down 3 >/dev/null
    run scripts/cassandra.sh status
    gap
    local ks key rf levels
    for rf in 1 2 3; do
        ks=ks_rf$rf
        case $rf in
            1) levels="ONE" ;;
            2) levels="ONE TWO" ;;
            3) levels="ONE TWO THREE" ;;
        esac
        for want in with without; do
            # With replication factor 3 every row has a replica on node 3.
            [ "$rf" = 3 ] && [ "$want" = without ] && continue
            key=$(pick_key "$ks" "$want")
            [ -n "$key" ] || continue
            if [ "$want" = with ]; then
                note "$ks, id = $key: репліки на вузлах $(replicas "$ks" "$key"| sed 's/ $//; s/ /, /g'), серед них вимкнений node3."
            else
                note "$ks, id = $key: репліки на вузлах $(replicas "$ks" "$key" | sed 's/ $//; s/ /, /g'), node3 серед них немає."
            fi
            check_lines "$ks" "$key" $levels | cql_session "$out" 1
        done
    done
    note "Повертаємо node3."
    run scripts/cassandra.sh start-node 3
    finish
}

scenario4() {
    begin 4
    cluster_up
    cqlsh_quiet 1 -e "CONSISTENCY ALL; DELETE FROM ks_rf3.kv WHERE id IN (100, 101)" >/dev/null
    note "Розриваємо мережу між вузлами: проксі між ними закриває всі зʼєднання й не приймає нових."
    note "Клієнти (cqlsh) і далі підключаються до кожного вузла напряму."
    SPLIT_SINCE=$(date -u '+%F %T')
    run scripts/cassandra.sh split
    for n in 1 2 3; do cs wait-down "$n" >/dev/null 2>&1 || true; done
    wait_isolated
    for n in 1 2 3; do
        note "Як кластер бачить node$n:"
        run scripts/cassandra.sh nodetool "$n" status ks_rf3
        gap
    done
    note "CONSISTENCY ONE: кожен вузол сам собі репліка ks_rf3, тож запис через нього вдається."
    note "Пишемо id = 100 по черзі через node1, node2, node3, кожен раз інше значення."
    gap
    for n in 1 2 3; do
        cql_session "$out" "$n" <<CQL
CONSISTENCY ONE
INSERT INTO ks_rf3.kv (id, value) VALUES (100, 'з node$n');
SELECT id, value, WRITETIME(value) FROM ks_rf3.kv WHERE id = 100;
CQL
        sleep 1
    done
    note "QUORUM під час розриву неможливий: живої більшості реплік немає."
    gap
    cql_session "$out" 1 <<'CQL'
CONSISTENCY QUORUM
INSERT INTO ks_rf3.kv (id, value) VALUES (100, 'QUORUM з node1');
CQL
    note "Другий конфлікт, id = 101: node1 пише першим, node2 на секунду пізніше, але з міткою часу"
    note "на 1 мкс старшою за мітку node1 (USING TIMESTAMP)."
    gap
    cql_session "$out" 1 <<'CQL'
CONSISTENCY ONE
INSERT INTO ks_rf3.kv (id, value) VALUES (101, 'з node1, раніше');
SELECT id, value, WRITETIME(value) FROM ks_rf3.kv WHERE id = 101;
CQL
    local first
    first=$(sed -n 's/^ *101 | з node1, раніше | \([0-9]*\)$/\1/p' "$out" | tail -n 1)
    [ -n "$first" ] || { echo "no WRITETIME of node1's write" >&2; exit 1; }
    sleep 1
    cql_session "$out" 2 <<CQL
CONSISTENCY ONE
INSERT INTO ks_rf3.kv (id, value) VALUES (101, 'з node2, пізніше') USING TIMESTAMP $((first - 1));
SELECT id, value, WRITETIME(value) FROM ks_rf3.kv WHERE id = 101;
CQL
    # Hints wait in memory and reach disk every hints_flush_period (10 s);
    # listpendinghints only sees them there.
    sleep 12
    note "Записи для недоступних реплік кожен вузол зберігає як hints, файл на кожну недоступну репліку"
    note "(імʼя файла починається з її Host ID):"
    for n in 1 2 3; do
        run ls -1 ".cassandradata/node$n/hints"
    done
    finish
}

# Waits until every node sees the other two as down.
wait_isolated() {
    local n deadline=$((SECONDS + 120))
    for n in 1 2 3; do
        until [ "$(cs nodetool "$n" status 2>/dev/null | grep -c '^DN')" = 2 ]; do
            [ $SECONDS -lt $deadline ] || { echo "nodes did not lose each other" >&2; exit 1; }
            sleep 1
        done
    done
}

scenario5() {
    begin 5
    local n
    note "Відновлюємо мережу й чекаємо, поки всі вузли знову побачать один одного (UN)."
    # Where each node's log stands, to show only the handoffs after the heal.
    local -a log_start=()
    for n in 1 2 3; do
        log_start[$n]=$(( $(wc -l <".cassandradata/node$n/logs/system.log") + 1 ))
    done
    run scripts/cassandra.sh heal
    for n in 1 2 3; do cs wait-up "$n" >/dev/null; done
    run scripts/cassandra.sh status
    gap
    note "Після відновлення вузли передають hints адресатам (hinted handoff) і видаляють файли:"
    local deadline=$((SECONDS + 120))
    for n in 1 2 3; do
        until [ -z "$(ls -A ".cassandradata/node$n/hints")" ]; do
            [ $SECONDS -lt $deadline ] || break
            sleep 1
        done
    done
    for n in 1 2 3; do
        stamp
        printf '[%s] $ tail -n +%s .cassandradata/node%s/logs/system.log | grep "Finished hinted handoff"\n' \
            "$STAMP" "${log_start[$n]}" "$n" >>"$out"
        tail -n +"${log_start[$n]}" ".cassandradata/node$n/logs/system.log" | grep 'Finished hinted handoff' \
            | sed -E 's/^INFO +\[[^]]*\] //; s/ HintsDispatchExecutor.java:[0-9]+ -//' >>"$out" || true
    done
    run ls -1 .cassandradata/node1/hints .cassandradata/node2/hints .cassandradata/node3/hints
    gap
    note "Читаємо з кожного вузла з CONSISTENCY ONE: усі репліки зійшлися на одному значенні."
    gap
    for n in 1 2 3; do
        cql_session "$out" "$n" <<'CQL'
CONSISTENCY ONE
SELECT id, value, WRITETIME(value) FROM ks_rf3.kv WHERE id IN (100, 101);
CQL
    done
    note "І з CONSISTENCY ALL (відповідь усіх трьох реплік):"
    gap
    cql_session "$out" 1 <<'CQL'
CONSISTENCY ALL
SELECT id, value, WRITETIME(value) FROM ks_rf3.kv WHERE id IN (100, 101);
CQL
    finish
    # When the nodes noticed the split and the heal, from their own logs.
    if [ -n "${SPLIT_SINCE:-}" ]; then
        {
            printf '# Події gossip з system.log вузлів (UTC): split о %s, heal о %s.\n' \
                "$(grep -m1 -o '^\[[0-9:.]*\] \$ scripts/cassandra.sh split' "$OUT_DIR/task5-part2-4.txt" | cut -c2-13)" \
                "$(grep -m1 -o '^\[[0-9:.]*\] \$ scripts/cassandra.sh heal' "$out" | cut -c2-13)"
            python3 "$script_dir/cassandra-gossip.py" "$SPLIT_SINCE" "$(date -u '+%F %T')"
        } >"$OUT_DIR/task5-part2-gossip.txt"
    fi
}

for s in $scenarios; do
    "scenario$s"
    echo "scenario $s: $OUT_DIR/task5-part2-$s.txt" >&2
done
