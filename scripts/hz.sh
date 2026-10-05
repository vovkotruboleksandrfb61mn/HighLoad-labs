#!/usr/bin/env bash
# The local 3-member Hazelcast 5.4.0 cluster for Task 3.
#
#   scripts/hz.sh start    start members 1-3 (127.0.0.1:5701-5703) and wait
#                          until the CP Subsystem has formed on all three
#   scripts/hz.sh stop     stop the members started by `start`
#   scripts/hz.sh status   show which members run
#   scripts/hz.sh logs [N] print the end of the member logs (all, or member N)
#
# Configs: deploy/hazelcast-node{1,2,3}.yaml. Logs: $HLS_LOG_DIR/hz-node{1,2,3}.log,
# by default reports/logs (truncated on every start). PIDs: .hzdata/hz-node{1,2,3}.pid.
#
# Overridable: HZ_HEAP (512m), HZ_START_TIMEOUT seconds (180), LINES (40),
# HLS_LOG_DIR (reports/logs).
set -euo pipefail
. "$(dirname "$0")/env.sh"

HZ_LOG_DIR="$HLS_LOG_DIR"
HZ_RUN_DIR="$HLS_ROOT/.hzdata"
HZ_HEAP=${HZ_HEAP:-512m}
HZ_START_TIMEOUT=${HZ_START_TIMEOUT:-180}
NODES=(1 2 3)
# Printed by every CP member once the METADATA group has its 3 members.
CP_READY_PATTERN='CP Group Members \{.*size:3'

pid_file() { printf '%s/hz-node%s.pid\n' "$HZ_RUN_DIR" "$1"; }
log_file() { printf '%s/hz-node%s.log\n' "$HZ_LOG_DIR" "$1"; }
member_port() { printf '%s\n' $((5700 + $1)); }

member_pid() {
    local file
    file=$(pid_file "$1")
    [ -f "$file" ] && cat "$file"
}

is_running() {
    local pid
    pid=$(member_pid "$1") || return 1
    kill -0 "$pid" 2>/dev/null
}

port_in_use() {
    (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null
}

start_member() {
    local n=$1 log
    log=$(log_file "$n")
    if is_running "$n"; then
        echo "member $n already runs (pid $(member_pid "$n"))" >&2
        return
    fi
    if port_in_use "$(member_port "$n")"; then
        echo "port $(member_port "$n") is taken by another process; not starting member $n" >&2
        return 1
    fi
    # Hazelcast reads every HZ_* environment variable as a config override
    # (HZ_ADDRESS would become `hazelcast.address`), so the member gets none
    # of ours. The subshell, nohup and `hz start` all exec, so $! ends up
    # being the member's JVM.
    (
        home=$HZ_HOME heap=$HZ_HEAP
        for variable in "${!HZ_@}"; do
            unset "$variable"
        done
        export HAZELCAST_CONFIG="$HLS_ROOT/deploy/hazelcast-node$n.yaml"
        export LOGGING_CONFIG="$HLS_ROOT/deploy/hazelcast-log4j2.properties"
        export MIN_HEAP_SIZE="$heap" MAX_HEAP_SIZE="$heap"
        exec nohup "$home/bin/hz" start >"$log" 2>&1 </dev/null
    ) &
    echo $! >"$(pid_file "$n")"
    echo "member $n: pid $!, log $log" >&2
}

wait_until_ready() {
    local deadline=$((SECONDS + HZ_START_TIMEOUT)) n
    until grep -qE "$CP_READY_PATTERN" "$(log_file 1)" "$(log_file 2)" "$(log_file 3)" 2>/dev/null; do
        for n in "${NODES[@]}"; do
            if ! is_running "$n"; then
                echo "member $n exited during startup; end of $(log_file "$n"):" >&2
                tail -n 30 "$(log_file "$n")" >&2 || true
                return 1
            fi
        done
        if [ "$SECONDS" -ge "$deadline" ]; then
            echo "the CP Subsystem did not form within ${HZ_START_TIMEOUT}s" >&2
            return 1
        fi
        sleep 1
    done
    grep -hE -A4 "$CP_READY_PATTERN" "$(log_file 1)" "$(log_file 2)" "$(log_file 3)" | head -n 5 >&2
    echo "cluster ready: 3 members, CP Subsystem formed" >&2
}

start() {
    if [ ! -x "$HZ_HOME/bin/hz" ]; then
        echo "Hazelcast 5.4.0 not found at $HZ_HOME" >&2
        exit 1
    fi
    mkdir -p "$HZ_LOG_DIR" "$HZ_RUN_DIR"
    local n
    for n in "${NODES[@]}"; do
        start_member "$n"
    done
    if ! wait_until_ready; then
        stop
        exit 1
    fi
}

stop() {
    local n pid waited
    for n in "${NODES[@]}"; do
        pid=$(member_pid "$n") || continue
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
            waited=0
            while kill -0 "$pid" 2>/dev/null && [ "$waited" -lt 30 ]; do
                sleep 1
                waited=$((waited + 1))
            done
            if kill -0 "$pid" 2>/dev/null; then
                echo "member $n did not stop within 30s; killing it" >&2
                kill -9 "$pid" 2>/dev/null || true
            fi
            echo "member $n stopped (pid $pid)" >&2
        fi
        rm -f "$(pid_file "$n")"
    done
}

status() {
    local n running=0
    for n in "${NODES[@]}"; do
        if is_running "$n"; then
            echo "member $n: running (pid $(member_pid "$n"), port $(member_port "$n"))"
            running=$((running + 1))
        else
            echo "member $n: stopped"
        fi
    done
    [ "$running" -eq "${#NODES[@]}" ]
}

logs() {
    local n
    for n in "${@:-${NODES[@]}}"; do
        echo "== $(log_file "$n")"
        tail -n "${LINES:-40}" "$(log_file "$n")" 2>/dev/null || echo "(no log)"
    done
}

case "${1:-}" in
    start) start ;;
    stop) stop ;;
    status) status ;;
    logs) shift; logs "$@" ;;
    *)
        echo "usage: $0 start|stop|status|logs [N]" >&2
        exit 2
        ;;
esac
