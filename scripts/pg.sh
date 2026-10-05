#!/usr/bin/env bash
# A local PostgreSQL 18 cluster for Task 2.
#   scripts/pg.sh start    initdb on first use, start the server, apply the schema
#   scripts/pg.sh stop     stop the server (fast shutdown)
#   scripts/pg.sh reset    drop user_counter and re-create it as (1, 0, 0)
#   scripts/pg.sh status   whether the server is running
#   scripts/pg.sh psql ... psql into the cluster (extra arguments go to psql)
#
# The data directory is $PGDATA_DIR (default: .pgdata in the main checkout,
# gitignored). The server listens on 127.0.0.1:$PG_PORT (21410) and on a unix
# socket in the data directory. Local connections use trust auth as user
# `postgres`. fsync stays at its default (on).
#
# initdb, pg_ctl and postgres come from the project's Nix flake
# (postgresql_18); if they are not on PATH the script re-runs itself under
# `direnv exec`.
set -euo pipefail

script_dir=$(cd "$(dirname "$0")" && pwd)
if ! command -v pg_ctl >/dev/null && [ -z "${HLS_PG_REEXEC:-}" ] && command -v direnv >/dev/null; then
    HLS_PG_REEXEC=1 exec direnv exec "$script_dir/.." "$0" "$@"
fi
for tool in initdb pg_ctl psql; do
    command -v "$tool" >/dev/null || { echo "missing $tool (nixpkgs: postgresql_18)" >&2; exit 1; }
done

. "$script_dir/env.sh"

init_sql="$HLS_ROOT/deploy/postgres-init.sql"
log="$PGDATA_DIR/server.log"

pg_psql() {
    psql -X -q -v ON_ERROR_STOP=1 -h "$PGDATA_DIR" -p "$PG_PORT" -U postgres -d postgres "$@"
}

is_running() {
    pg_ctl -D "$PGDATA_DIR" status >/dev/null 2>&1
}

start() {
    if [ ! -f "$PGDATA_DIR/PG_VERSION" ]; then
        echo "initdb $PGDATA_DIR" >&2
        local output
        output=$(initdb -D "$PGDATA_DIR" -U postgres --auth=trust --encoding=UTF8 --locale=C 2>&1) \
            || { printf '%s\n' "$output" >&2; exit 1; }
    fi
    if is_running; then
        echo "postgres already running ($PGDATA_DIR)" >&2
    else
        pg_ctl -D "$PGDATA_DIR" -l "$log" -w -t 30 \
            -o "-p $PG_PORT -c listen_addresses=127.0.0.1 -c unix_socket_directories='$PGDATA_DIR'" \
            start >/dev/null || { tail -n 20 "$log" >&2; exit 1; }
        echo "postgres started on 127.0.0.1:$PG_PORT (log: $log)" >&2
    fi
    pg_psql -f "$init_sql"
}

stop() {
    if is_running; then
        pg_ctl -D "$PGDATA_DIR" -m fast -w stop >/dev/null
        echo "postgres stopped" >&2
    else
        echo "postgres is not running" >&2
    fi
}

reset() {
    is_running || { echo "postgres is not running; scripts/pg.sh start" >&2; exit 1; }
    pg_psql -c "DROP TABLE IF EXISTS user_counter"
    pg_psql -f "$init_sql"
    pg_psql -At -c "SELECT user_id, counter, version FROM user_counter"
}

case "${1:-}" in
    start) start ;;
    stop) stop ;;
    reset) reset ;;
    status) if is_running; then echo running; else echo stopped; exit 3; fi ;;
    psql) shift; exec psql -X -h "$PGDATA_DIR" -p "$PG_PORT" -U postgres -d postgres "$@" ;;
    *) echo "usage: scripts/pg.sh start|stop|reset|status|psql [args]" >&2; exit 2 ;;
esac
