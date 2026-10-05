#!/usr/bin/env bash
# How many commits, rollbacks and WAL fsyncs each pg-bench variant costs.
#
# Runs every variant once with WORKERS x INCREMENTS (10 x 1000 by default)
# against the running scripts/pg.sh cluster and records the server-side
# counter deltas around the run: xact_commit / xact_rollback from
# pg_stat_database and the WAL fsyncs from pg_stat_io. Writes
# reports/data/task2-wal-probe.csv (override with OUT).
#
# Used to explain why the variants differ in time: with fsync on, every
# commit of a transaction that wrote something waits for a WAL flush.
set -euo pipefail
script_dir=$(cd "$(dirname "$0")" && pwd)
if ! command -v psql >/dev/null && [ -z "${HLS_PG_REEXEC:-}" ] && command -v direnv >/dev/null; then
    HLS_PG_REEXEC=1 exec direnv exec "$script_dir/.." "$0" "$@"
fi
. "$script_dir/env.sh"

VARIANTS=${VARIANTS:-"lost-update serializable serializable-retry in-place for-update optimistic"}
WORKERS=${WORKERS:-10}
INCREMENTS=${INCREMENTS:-1000}
OUT=${OUT:-$HLS_ROOT/reports/data/task2-wal-probe.csv}

"$script_dir/pg.sh" status >/dev/null || { echo "postgres is not running; scripts/pg.sh start" >&2; exit 1; }
hls_build_release --product pg-bench

stats() {
    psql -X -At -F, -h "$PGDATA_DIR" -p "$PG_PORT" -U postgres -d postgres -c "
        SELECT pg_stat_force_next_flush();
        SELECT d.xact_commit, d.xact_rollback,
               (SELECT sum(fsyncs) FROM pg_stat_io WHERE object = 'wal')::bigint
        FROM pg_stat_database d WHERE d.datname = 'postgres'" | tail -n 1
}

echo "variant,workers,increments_per_worker,seconds,final_value,errors,retries,commits,rollbacks,wal_fsyncs,wal_fsyncs_per_increment" >"$OUT"
for variant in $VARIANTS; do
    before=$(stats)
    row=$("$HLS_BIN/pg-bench" --variant "$variant" --workers "$WORKERS" --increments "$INCREMENTS" --pg-url "$PG_URL") || true
    # Let the backends of the closed connections report their counters.
    sleep 1
    after=$(stats)
    IFS=, read -r c0 r0 f0 <<<"$before"
    IFS=, read -r c1 r1 f1 <<<"$after"
    IFS=, read -r _ _ _ seconds _ final _ _ errors retries <<<"$row"
    total=$((WORKERS * INCREMENTS))
    fsyncs=$((f1 - f0))
    per=$(awk -v f="$fsyncs" -v t="$total" 'BEGIN { printf "%.3f", f / t }')
    line="$variant,$WORKERS,$INCREMENTS,$seconds,$final,$errors,$retries,$((c1 - c0)),$((r1 - r0)),$fsyncs,$per"
    echo "$line" | tee -a "$OUT" >&2
done
echo "wrote $OUT" >&2
