#!/usr/bin/env bash
# Measures server engines step by step, in the style of the One Billion Row
# Challenge write-ups: throughput plus hardware counters per request.
#
#   STEP=v1 ENGINES="handler raw" scripts/bench-engines.sh
#
# For each repeat it runs every engine in turn (interleaved, so drift in the
# machine's load hits all of them alike): counter-server --store memory,
# then wrk with CONNECTIONS connections, one request in flight on each, for
# DURATION seconds, while `perf stat` counts the server's cycles,
# instructions, branch misses, cache misses and context switches.
# Appends one CSV row per run to OUT.
#
# Variables: STEP (label), ENGINES, STORE (default memory), REPEATS (3),
# DURATION (10), CONNECTIONS (10), BIN (directory with counter-server,
# default .build/release), SERVER_ARGS (extra server options), OUT,
# FIRST_REPEAT (number of the first repeat, to interleave separate binaries),
# WRK_CPUS (CPU list for `taskset -c`, to pin wrk's threads).
set -euo pipefail

script_dir=$(cd "$(dirname "$0")" && pwd)
if { ! command -v perf >/dev/null || ! command -v wrk >/dev/null; } && [ -z "${HLS_BENCH_REEXEC:-}" ]; then
    HLS_BENCH_REEXEC=1 exec direnv exec "$script_dir/.." "$0" "$@"
fi
. "$script_dir/env.sh"

STEP=${STEP:?set STEP, e.g. STEP=v1}
ENGINES=${ENGINES:-raw}
STORE=${STORE:-memory}
REPEATS=${REPEATS:-3}
DURATION=${DURATION:-10}
CONNECTIONS=${CONNECTIONS:-10}
BIN=${BIN:-$HLS_BIN}
SERVER_ARGS=${SERVER_ARGS:-}
OUT=${OUT:-$HLS_ROOT/reports/data/1brc-steps.csv}
PORT=${BENCH_PORT:-21482}
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"; [ -n "${server_pid:-}" ] && kill "$server_pid" 2>/dev/null || true' EXIT

mkdir -p "$(dirname "$OUT")"
[ -s "$OUT" ] || echo "step,engine,store,repeat,connections,seconds,rps,latency_avg_us,latency_p99_us,server_user_s,server_sys_s,cpu_us_per_req,cycles_per_req,instructions_per_req,ipc,branch_misses_per_req,cache_misses_per_req,context_switches_per_req" >"$OUT"

cpu_ticks() { awk '{print $14, $15}' "/proc/$1/stat"; }
to_us() {  # wrk prints latencies as 72.21us, 1.20ms or 1.05s
    awk -v v="$1" 'BEGIN { n = v + 0; if (v ~ /ms$/) n *= 1000; else if (v ~ /[0-9]s$/) n *= 1000000; printf "%.1f", n }'
}
counter() { awk -F, -v e="$2" '$3 == e || $3 ~ "^"e"(:u)?$" { print $1; exit }' "$1"; }

ticks_per_second=$(getconf CLK_TCK)
FIRST_REPEAT=${FIRST_REPEAT:-1}
for repeat in $(seq "$FIRST_REPEAT" $((FIRST_REPEAT + REPEATS - 1))); do
    for engine in $ENGINES; do
        # shellcheck disable=SC2086
        "$BIN/counter-server" --port "$PORT" --store "$STORE" --engine "$engine" $SERVER_ARGS >"$scratch/server.log" 2>&1 &
        server_pid=$!
        sleep 1
        curl -fsS -X POST "http://127.0.0.1:$PORT/reset" >/dev/null

        read -r user0 sys0 < <(cpu_ticks "$server_pid")
        perf stat -x, -e cycles,instructions,branch-misses,cache-misses,context-switches \
            -p "$server_pid" -o "$scratch/perf.csv" &
        perf_pid=$!
        ${WRK_CPUS:+taskset -c "$WRK_CPUS"} wrk -t"$CONNECTIONS" -c"$CONNECTIONS" -d"${DURATION}s" --latency "http://127.0.0.1:$PORT/inc" >"$scratch/wrk.txt"
        kill -INT "$perf_pid"
        wait "$perf_pid" || true
        read -r user1 sys1 < <(cpu_ticks "$server_pid")

        count=$(curl -fsS "http://127.0.0.1:$PORT/count")
        kill "$server_pid"
        wait "$server_pid" 2>/dev/null || true
        server_pid=

        requests=$(awk '/requests in/ { print $1 }' "$scratch/wrk.txt")
        seconds=$(awk '/requests in/ { sub(/s,$/, "", $4); print $4 }' "$scratch/wrk.txt")
        # wrk stops counting at the deadline while up to one request per
        # connection is still in flight; the server has counted those.
        # Exact counts are checked by loadgen, which waits for every reply.
        if [ "$count" -lt "$requests" ] || [ "$count" -gt $((requests + CONNECTIONS)) ]; then
            echo "count $count does not match wrk's $requests (+ up to $CONNECTIONS in flight), $engine, repeat $repeat" >&2
            exit 2
        fi
        rps=$(awk '/Requests\/sec/ { print $2 }' "$scratch/wrk.txt")
        latency_avg=$(to_us "$(awk '/^ *Latency / { print $2; exit }' "$scratch/wrk.txt")")
        latency_p99=$(to_us "$(awk '/^ *99%/ { print $2 }' "$scratch/wrk.txt")")

        awk -v step="$STEP" -v engine="$engine" -v store="$STORE" -v repeat="$repeat" -v conns="$CONNECTIONS" \
            -v seconds="$seconds" -v rps="$rps" -v lat="$latency_avg" -v p99="$latency_p99" -v req="$requests" \
            -v user=$((user1 - user0)) -v sys=$((sys1 - sys0)) -v hz="$ticks_per_second" \
            -v cycles="$(counter "$scratch/perf.csv" cycles)" -v instr="$(counter "$scratch/perf.csv" instructions)" \
            -v bmiss="$(counter "$scratch/perf.csv" branch-misses)" -v cmiss="$(counter "$scratch/perf.csv" cache-misses)" \
            -v cs="$(counter "$scratch/perf.csv" context-switches)" \
            'BEGIN {
                printf "%s,%s,%s,%d,%d,%s,%s,%s,%s,%.2f,%.2f,%.2f,%.0f,%.0f,%.2f,%.2f,%.2f,%.2f\n",
                    step, engine, store, repeat, conns, seconds, rps, lat, p99, user / hz, sys / hz,
                    (user + sys) / hz / req * 1e6, cycles / req, instr / req, instr / cycles,
                    bmiss / req, cmiss / req, cs / req
            }' | tee -a "$OUT"
    done
done
