#!/usr/bin/env bash
# Task 5, extra research: the counter under failures.
#
#   scripts/task5x-failures.sh [run ...]      (all runs by default)
#
# Every run is cassandra-bench with WORKERS x INCREMENTS increments and a
# reader at the same consistency level, REPEATS times:
#   kill-{one,quorum}[-retry]      node 2 gets SIGKILL DISRUPT_AFTER s into
#                                  the run and is started again at the end
#   split-{one,quorum}[-retry]     the network between the nodes is cut for
#                                  SPLIT_FOR s, DISRUPT_AFTER s into the run
#   killall-{one,quorum}[-retry]   every node gets SIGKILL, DISRUPT_AFTER s
#                                  into the run, and all are started again
#                                  DOWN_FOR s later
# -retry sends failed increments again (not idempotent: may count twice).
# Rows go to $OUT (reports/data/task5x-failures.csv); acknowledged and failed
# increments per 100 ms of every run to $TIMELINE_DIR/<run>-<repeat>.csv.
#
# Starts the cluster if it is not running (and leaves it running).
# Overridable: WORKERS (10), INCREMENTS (10000), REPEATS (3),
# DISRUPT_AFTER (7), SPLIT_FOR (10), DOWN_FOR (5), OUT, TIMELINE_DIR,
# FIRST_REPEAT (1: a run cut short can be resumed; rows are appended).
set -euo pipefail

script_dir=$(cd "$(dirname "$0")" && pwd)
if ! command -v cqlsh >/dev/null && [ -z "${HLS_CASSANDRA_REEXEC:-}" ] && command -v direnv >/dev/null; then
    HLS_CASSANDRA_REEXEC=1 exec direnv exec "$script_dir/.." "$0" "$@"
fi
. "$script_dir/task5-common.sh"

WORKERS=${WORKERS:-10}
INCREMENTS=${INCREMENTS:-10000}
REPEATS=${REPEATS:-3}
DISRUPT_AFTER=${DISRUPT_AFTER:-7}
SPLIT_FOR=${SPLIT_FOR:-10}
DOWN_FOR=${DOWN_FOR:-5}
OUT=${OUT:-$HLS_ROOT/reports/data/task5x-failures.csv}
TIMELINE_DIR=${TIMELINE_DIR:-$HLS_ROOT/reports/data/task5x-timeline}
ALL_RUNS="kill-one kill-one-retry kill-quorum kill-quorum-retry
split-one split-one-retry split-quorum split-quorum-retry
killall-one killall-quorum killall-one-retry"
runs=${*:-$ALL_RUNS}

cluster_up
hls_build_release --product cassandra-bench
mkdir -p "$(dirname "$OUT")" "$TIMELINE_DIR"

# The cassandra-bench arguments of a run.
run_args() {
    local run=$1 level args=()
    case $run in
        *-one*) level=one ;;
        *-quorum*) level=quorum ;;
    esac
    args+=(--consistency "$level" --reader-consistency "$level" --disrupt-after "$DISRUPT_AFTER")
    case $run in
        kill-*) args+=(--kill-node 2) ;;
        split-*) args+=(--split-for "$SPLIT_FOR") ;;
        killall-*) args+=(--kill-all-for "$DOWN_FOR") ;;
        *) echo "unknown run: $run" >&2; return 1 ;;
    esac
    # A restart of the whole cluster takes minutes.
    [[ $run == *-retry ]] && args+=(--retry-failed --retry-limit 600)
    printf '%s\n' "${args[@]}"
}

header=true
[ -s "$OUT" ] && header=false
for repeat in $(seq "${FIRST_REPEAT:-1}" "$REPEATS"); do
    for run in $runs; do
        mapfile -t args < <(run_args "$run")
        echo "== $run, repeat $repeat: ${args[*]}" >&2
        for n in 1 2 3; do "$HLS_ROOT/scripts/cassandra.sh" wait-up "$n" >/dev/null; done
        output=$("$HLS_BIN/cassandra-bench" --header --label "$run" --workers "$WORKERS" --increments "$INCREMENTS" \
            --hosts "$CASSANDRA_HOSTS" --port "$CASSANDRA_PORT" --cluster-script "$HLS_ROOT/scripts/cassandra.sh" \
            --timeline-out "$TIMELINE_DIR/$run-$repeat.csv" "${args[@]}" 2> >(grep -v 'connection_pool.cpp\|\[WARN\]\|\[ERROR\]' >&2)) || true
        wait
        if [ -z "$output" ]; then
            echo "cassandra-bench failed for $run" >&2
            continue
        fi
        if $header; then
            printf 'repeat,%s\n' "$(sed -n 1p <<<"$output")" >"$OUT"
            header=false
        fi
        printf '%s,%s\n' "$repeat" "$(sed -n 2p <<<"$output")" | tee -a "$OUT" >&2
    done
done
echo "wrote $OUT" >&2
