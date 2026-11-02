#!/usr/bin/env bash
# Task 4, Parts II and III, on the local MongoDB replica set (P-S-S,
# scripts/mongo.sh).
#
#  1. bench: mongo-bench, WORKERS separate clients x INCREMENTS
#     findOneAndUpdate({ $inc: { likes: 1 } }) each, REPEATS times per run:
#       w1, majority            no failure
#       w1-kill, majority-kill  the primary gets SIGKILL KILL_FRACTION of the
#                               way into the run (of the median time of the
#                               same write concern without a kill) and is
#                               started again once the workers are done
#       majority-kill-noretry   (DEMO=1, once) majority-kill with the retries
#                               sent without lsid/txnNumber
#     -> reports/data/task4-bench.csv, plus for every kill run the failover as
#     the mongod logs tell it -> reports/logs/task4-failover-<run>-<repeat>.txt
#  2. web: counter-server --store mongo (w: majority, readConcern majority)
#     driven by loadgen with WEB_CLIENTS x WEB_REQUESTS, REPEATS times (via
#     scripts/task1.sh, server on port 21440) -> reports/data/task4-web.csv
#
# The replica set is (re)started fresh with its logs in MONGO_LOG_DIR
# (reports/logs/task4, apart from the Part I logs in reports/logs) and stopped
# at the end.
#
# Overridable: PARTS ("bench web"), RUNS ("w1 majority w1-kill majority-kill"),
# DEMO (1), WORKERS (10), INCREMENTS (10000), REPEATS (3), KILL_FRACTION
# (0.35), KILL_AFTER (seconds; instead of KILL_FRACTION), WEB_CLIENTS (10),
# WEB_REQUESTS (10000), BENCH_OUT, WEB_OUT, FAILOVER_DIR (reports/logs),
# MONGO_LOG_DIR. A quick smoke run:
#   INCREMENTS=500 WEB_REQUESTS=300 REPEATS=1 BENCH_OUT=/tmp/b.csv WEB_OUT=/tmp/w.csv \
#     FAILOVER_DIR=/tmp MONGO_LOG_DIR=/tmp/mongo-logs scripts/task4.sh
set -euo pipefail

script_dir=$(cd "$(dirname "$0")" && pwd)
# mongod and mongosh on PATH up front: mongo.sh would otherwise re-run itself
# under direnv on every call, which delays the kill by a second or so.
if { ! command -v mongod >/dev/null || ! command -v mongosh >/dev/null; } \
    && [ -z "${HLS_MONGO_REEXEC:-}" ] && command -v direnv >/dev/null; then
    HLS_MONGO_REEXEC=1 exec direnv exec "$script_dir/.." "$0" "$@"
fi

# The web counter of this task listens on 21440 (env.sh derives SERVER_URL).
export SERVER_PORT=${SERVER_PORT:-21440}
. "$script_dir/env.sh"

PARTS=${PARTS:-"bench web"}
RUNS=${RUNS:-"w1 majority w1-kill majority-kill"}
DEMO=${DEMO:-1}
WORKERS=${WORKERS:-10}
INCREMENTS=${INCREMENTS:-10000}
REPEATS=${REPEATS:-3}
KILL_FRACTION=${KILL_FRACTION:-0.35}
KILL_AFTER=${KILL_AFTER:-}
WEB_CLIENTS=${WEB_CLIENTS:-10}
WEB_REQUESTS=${WEB_REQUESTS:-10000}
BENCH_OUT=${BENCH_OUT:-$HLS_ROOT/reports/data/task4-bench.csv}
WEB_OUT=${WEB_OUT:-$HLS_ROOT/reports/data/task4-web.csv}
FAILOVER_DIR=${FAILOVER_DIR:-$HLS_ROOT/reports/logs}
export MONGO_LOG_DIR=${MONGO_LOG_DIR:-$HLS_ROOT/reports/logs/task4}

MONGO="$HLS_ROOT/scripts/mongo.sh"
NODES=(1 2 3)

cleanup() {
    hls_stop_server
    "$MONGO" stop
}
trap cleanup EXIT

# A fresh start, so the logs in MONGO_LOG_DIR hold only this script's runs.
"$MONGO" stop
"$MONGO" start
"$MONGO" status >&2

failures=0

# retried_statements: per node, the retryable-write statements the node has
# recognised as already executed (serverStatus().transactions), 0 when down.
retried_statements() {
    local n
    for n in "${NODES[@]}"; do
        mongosh "mongodb://127.0.0.1:$((MONGO_PORT + n - 1))/?directConnection=true&serverSelectionTimeoutMS=1000" \
            --quiet --norc --eval 'print(String(db.serverStatus().transactions.retriedStatementsCount))' 2>/dev/null || echo 0
    done | paste -sd' '
}

# The retried statements a run added, summed over the nodes. A killed node
# starts again from 0, which counts as no increase.
retried_delta() {
    local -a before=($1) after=($2)
    local i sum=0
    for i in "${!before[@]}"; do
        if [ "${after[$i]}" -gt "${before[$i]}" ]; then
            sum=$((sum + after[i] - before[i]))
        fi
    done
    echo "$sum"
}

# median_seconds <run>: the median duration of <run> in BENCH_OUT.
median_seconds() {
    awk -F, -v run="$1" '
        NR == 1 { for (i = 1; i <= NF; i++) if ($i == "seconds") col = i; next }
        $2 == run { v[n++] = $col }
        END {
            if (n == 0) exit 1
            asort(v)
            print (n % 2) ? v[(n + 1) / 2] : (v[n / 2] + v[n / 2 + 1]) / 2
        }' "$BENCH_OUT"
}

header_written=false

# bench_run <run> <repeat>
bench_run() {
    local run=$1 repeat=$2 variant args=() kill_after="" status=0 output row
    case $run in
        w1 | majority) variant=$run ;;
        w1-kill | majority-kill) variant=${run%-kill} ;;
        majority-kill-noretry) variant=majority; args+=(--no-retryable-writes) ;;
        *) echo "unknown run: $run" >&2; return 1 ;;
    esac
    if [[ $run == *-kill* ]]; then
        if [ -n "$KILL_AFTER" ]; then
            kill_after=$KILL_AFTER
        elif median=$(median_seconds "$variant"); then
            kill_after=$(awk -v m="$median" -v f="$KILL_FRACTION" 'BEGIN { printf "%.1f", m * f }')
        else
            echo "no $variant run in $BENCH_OUT to time the kill by; set KILL_AFTER" >&2
            return 1
        fi
        args+=(--kill-primary-after "$kill_after")
    fi

    "$MONGO" wait-primary >&2
    local -a log_start=()
    local n since retried_before retried_after stderr_file
    for n in "${NODES[@]}"; do
        log_start+=("$(($(wc -l <"$MONGO_LOG_DIR/mongo-node$n.log") + 1))")
    done
    since=$(date +%s.%N)
    retried_before=$(retried_statements)
    stderr_file=$(mktemp)

    echo "== $run, repeat $repeat${kill_after:+, kill after $kill_after s}" >&2
    output=$("$HLS_BIN/mongo-bench" --header --variant "$variant" --workers "$WORKERS" \
        --increments "$INCREMENTS" --url "$MONGO_URL" --mongo-script "$MONGO" "${args[@]}" \
        2> >(tee "$stderr_file" >&2)) || status=$?
    # The process substitution may still be writing.
    wait
    retried_after=$(retried_statements)

    if [ -z "$output" ]; then
        echo "mongo-bench failed (status $status) for $run" >&2
        failures=$((failures + 1))
        rm -f "$stderr_file"
        return 0
    fi
    [ "$status" -eq 0 ] || failures=$((failures + 1))

    local rollback="0 0"
    if [[ $run == *-kill* ]]; then
        local file="$FAILOVER_DIR/task4-failover-$run-$repeat.txt" excerpt summary
        local -a logs=()
        for n in "${NODES[@]}"; do
            logs+=(--log "$n=$MONGO_LOG_DIR/mongo-node$n.log:${log_start[$((n - 1))]}")
        done
        excerpt=$(python3 "$HLS_ROOT/scripts/mongo-failover-excerpt.py" \
            --title "Task 4, частина II: прогін $run, повтор $repeat (kill через $kill_after с)" \
            --data-dir "$MONGO_DATA_DIR" --since "$since" "${logs[@]}")
        summary=$(tail -n 1 <<<"$excerpt")
        {
            printf '%s\n\n' "$(sed -n 1p <<<"$excerpt")"
            echo "Рядок CSV mongo-bench:"
            printf '%s\n%s\n\n' "$(sed -n 1p <<<"$output")" "$(sed -n 2p <<<"$output")"
            echo "Вивід mongo-bench і scripts/mongo.sh (stderr):"
            cat "$stderr_file"
            echo
            echo "retriedStatementsCount вузлів 1-3 до прогону: $retried_before, після: $retried_after"
            echo
            sed '1d' <<<"$excerpt"
        } >"$file"
        echo "wrote $file ($summary)" >&2
        rollback=$(sed -E 's/rollback_lines=([0-9]+) rollback_documents=([0-9]+)/\1 \2/' <<<"$summary")
    fi
    rm -f "$stderr_file"

    if ! $header_written; then
        printf 'repeat,run,%s,server_deduplicated,rollback_lines,rollback_documents\n' \
            "$(sed -n 1p <<<"$output")" >"$BENCH_OUT"
        header_written=true
    fi
    row="$repeat,$run,$(sed -n 2p <<<"$output"),$(retried_delta "$retried_before" "$retried_after"),${rollback% *},${rollback#* }"
    printf '%s\n' "$row" >>"$BENCH_OUT"
    echo "repeat $repeat: $row" >&2
}

run_bench() {
    hls_build_release --product mongo-bench
    mkdir -p "$(dirname "$BENCH_OUT")" "$FAILOVER_DIR"
    local run repeat plain="" kills=""
    for run in $RUNS; do
        if [[ $run == *-kill* ]]; then kills+=" $run"; else plain+=" $run"; fi
    done
    # The runs without a kill first: the kill runs are timed by them.
    for repeat in $(seq 1 "$REPEATS"); do
        for run in $plain; do bench_run "$run" "$repeat"; done
    done
    for repeat in $(seq 1 "$REPEATS"); do
        for run in $kills; do bench_run "$run" "$repeat"; done
    done
    if [ "$DEMO" = 1 ]; then
        bench_run majority-kill-noretry 1
    fi
    "$MONGO" status >&2
    echo "wrote $BENCH_OUT" >&2
}

run_web() {
    "$MONGO" wait-primary >&2
    STORES=mongo CLIENTS="$WEB_CLIENTS" REQUESTS="$WEB_REQUESTS" REPEATS="$REPEATS" OUT="$WEB_OUT" \
        "$HLS_ROOT/scripts/task1.sh" || failures=$((failures + 1))
}

for part in $PARTS; do
    case $part in
        bench) run_bench ;;
        web) run_web ;;
        *) echo "unknown part: $part" >&2; exit 2 ;;
    esac
done

if [ "$failures" -gt 0 ]; then
    echo "$failures run(s) failed or lost updates they must not lose" >&2
    exit 1
fi
