#!/usr/bin/env bash
# The local three-node Cassandra cluster for Task 5.
#
#   scripts/cassandra.sh start          start the internode proxy and nodes
#                                       1-3 (one after another, each waits
#                                       until it is UN), then wait until all
#                                       three see each other
#   scripts/cassandra.sh stop           stop every node and the proxy
#   scripts/cassandra.sh stop-node N    drain and stop node N (clean shutdown)
#   scripts/cassandra.sh kill-node N    sudden failure of node N (SIGKILL)
#   scripts/cassandra.sh start-node N   start node N again
#   scripts/cassandra.sh wait-up N      wait until every live node sees N as UN
#   scripts/cassandra.sh wait-down N    wait until the other nodes see N as DN
#   scripts/cassandra.sh split          cut the network between the nodes
#   scripts/cassandra.sh heal           restore the network
#   scripts/cassandra.sh status         nodetool status (from the first live node)
#   scripts/cassandra.sh nodetool N ... nodetool against node N
#   scripts/cassandra.sh cqlsh N ...    cqlsh connected to node N
#   scripts/cassandra.sh wipe           stop everything and delete all data
#
# Docker is not available here, so the nodes run as plain processes on
# loopback addresses. Node N:
#   native protocol (cqlsh, drivers)  127.0.0.(10+N):$CASSANDRA_PORT (21440)
#   internode (gossip, replication)   bound on 127.0.1.N:$CASSANDRA_STORAGE_PORT
#                                     (21450), advertised as 127.0.0.(10+N),
#                                     where scripts/netsplit.py forwards it
#   JMX (nodetool)                    127.0.0.1:$((CASSANDRA_JMX_PORT + N - 1))
# so the addresses the cluster reports (nodetool status, getendpoints) are
# 127.0.0.11-13, and `split` can cut the links between nodes without root.
#
# Data, configuration and logs live in $CASSANDRA_DATA_DIR/nodeN
# (.cassandradata, gitignored). Overridable: CASSANDRA_HEAP (1G per node),
# CASSANDRA_START_TIMEOUT seconds (180).
set -euo pipefail

script_dir=$(cd "$(dirname "$0")" && pwd)
if ! command -v cassandra >/dev/null && [ -z "${HLS_CASSANDRA_REEXEC:-}" ] && command -v direnv >/dev/null; then
    HLS_CASSANDRA_REEXEC=1 exec direnv exec "$script_dir/.." "$0" "$@"
fi
command -v cassandra >/dev/null || { echo "missing cassandra (nixpkgs: cassandra)" >&2; exit 1; }

. "$script_dir/env.sh"

NODES=(1 2 3)
CASSANDRA_HEAP=${CASSANDRA_HEAP:-1G}
CASSANDRA_START_TIMEOUT=${CASSANDRA_START_TIMEOUT:-180}
CASSANDRA_HOME=$(cd "$(dirname "$(readlink -f "$(command -v cassandra)")")/.." && pwd)

node_dir() { printf '%s/node%s\n' "$CASSANDRA_DATA_DIR" "$1"; }
node_ip() { printf '127.0.0.%s\n' $((10 + $1)); }
node_jmx() { printf '%s\n' $((CASSANDRA_JMX_PORT + $1 - 1)); }
pid_file() { printf '%s/cassandra.pid\n' "$(node_dir "$1")"; }
proxy_pid_file() { printf '%s/netsplit.pid\n' "$CASSANDRA_DATA_DIR"; }

check_node() {
    case "${1:-}" in
        1 | 2 | 3) ;;
        *) echo "node must be 1, 2 or 3, not '${1:-}'" >&2; exit 2 ;;
    esac
}

alive() {
    local file=$1
    [ -f "$file" ] && kill -0 "$(cat "$file")" 2>/dev/null
}

node_alive() { alive "$(pid_file "$1")"; }

# The node's own copy of conf/, rewritten for its addresses and directories.
configure() {
    local n=$1 dir conf
    dir=$(node_dir "$n")
    conf="$dir/conf"
    mkdir -p "$dir"/{data,commitlog,hints,saved_caches,cdc_raw,logs}
    rm -rf "$conf"
    cp -r "$CASSANDRA_HOME/conf" "$conf"
    chmod -R u+w "$conf"
    sed -i "s/^JMX_PORT=.*/JMX_PORT=\"$(node_jmx "$n")\"/" "$conf/cassandra-env.sh"
    python3 - "$conf/cassandra.yaml" "$n" "$dir" <<'EOF'
import re, sys
path, n, dir = sys.argv[1], int(sys.argv[2]), sys.argv[3]
import os
port, storage = os.environ["CASSANDRA_PORT"], os.environ["CASSANDRA_STORAGE_PORT"]
text = open(path).read()
settings = {
    "cluster_name": "'HighLoad'",
    "listen_address": f"127.0.1.{n}",
    "rpc_address": f"127.0.0.{10 + n}",
    "native_transport_port": port,
    "storage_port": storage,
    "commitlog_directory": f"{dir}/commitlog",
    "hints_directory": f"{dir}/hints",
    "saved_caches_directory": f"{dir}/saved_caches",
    "cdc_raw_directory": f"{dir}/cdc_raw",
    "materialized_views_enabled": "true",
}
for key, value in settings.items():
    text, count = re.subn(rf"^#?\s*{key}:.*$", f"{key}: {value}", text, count=1, flags=re.M)
    assert count == 1, key
text += f"""
# scripts/cassandra.sh
broadcast_address: 127.0.0.{10 + n}
broadcast_rpc_address: 127.0.0.{10 + n}
data_file_directories:
    - {dir}/data
"""
text = re.sub(r'- seeds: "[^"]*"', f'- seeds: "127.0.0.11:{storage},127.0.0.12:{storage}"', text)
open(path, "w").write(text)
EOF
}

start_proxy() {
    alive "$(proxy_pid_file)" && return
    mkdir -p "$CASSANDRA_DATA_DIR"
    python3 "$script_dir/netsplit.py" serve >"$CASSANDRA_DATA_DIR/netsplit.log" 2>&1 &
    echo $! >"$(proxy_pid_file)"
    for _ in $(seq 50); do
        [ -S "$CASSANDRA_DATA_DIR/netsplit.sock" ] && return
        sleep 0.1
    done
    echo "netsplit proxy did not start; see $CASSANDRA_DATA_DIR/netsplit.log" >&2
    exit 1
}

stop_proxy() {
    if alive "$(proxy_pid_file)"; then
        kill "$(cat "$(proxy_pid_file)")"
    fi
    rm -f "$(proxy_pid_file)" "$CASSANDRA_DATA_DIR/netsplit.sock"
}

start_node() {
    local n=$1 dir
    check_node "$n"
    node_alive "$n" && { echo "node $n already runs"; return; }
    dir=$(node_dir "$n")
    configure "$n"
    echo "starting node $n ($(node_ip "$n"))"
    # TZ=UTC: log times in UTC, like the timestamps of the transcripts.
    TZ=UTC CASSANDRA_CONF="$dir/conf" CASSANDRA_LOG_DIR="$dir/logs" \
        MAX_HEAP_SIZE=$CASSANDRA_HEAP HEAP_NEWSIZE=200M \
        JVM_EXTRA_OPTS="-Dcassandra.skip_wait_for_gossip_to_settle=0 -Dcassandra.ring_delay_ms=5000" \
        setsid cassandra -f -p "$(pid_file "$n")" >"$dir/logs/stdout.log" 2>&1 &
    echo $! >"$(pid_file "$n")"
    wait_native "$n"
}

# Waits until node N answers CQL.
wait_native() {
    local n=$1 deadline=$((SECONDS + CASSANDRA_START_TIMEOUT))
    until cqlsh "$(node_ip "$n")" "$CASSANDRA_PORT" -e 'SELECT release_version FROM system.local' >/dev/null 2>&1; do
        node_alive "$n" || { echo "node $n exited; see $(node_dir "$n")/logs/system.log" >&2; exit 1; }
        [ $SECONDS -lt $deadline ] || { echo "node $n did not start in ${CASSANDRA_START_TIMEOUT}s" >&2; exit 1; }
        sleep 1
    done
}

first_live() {
    for n in "${NODES[@]}"; do
        node_alive "$n" && { echo "$n"; return; }
    done
    echo "no node is running" >&2
    exit 1
}

nt() {
    local n=$1
    shift
    # nodetool also wants a log directory, by default inside the read-only install.
    mkdir -p "$CASSANDRA_DATA_DIR/nodetool"
    CASSANDRA_LOG_DIR="$CASSANDRA_DATA_DIR/nodetool" nodetool -h 127.0.0.1 -p "$(node_jmx "$n")" "$@"
}

# The state ("UN", "DN", ...) node `viewer` reports for node `n`.
state_seen_by() {
    local viewer=$1 n=$2
    nt "$viewer" status 2>/dev/null | awk -v ip="$(node_ip "$n")" '$2 == ip { print $1 }'
}

wait_state() {
    local n=$1 want=$2 deadline=$((SECONDS + CASSANDRA_START_TIMEOUT))
    while :; do
        local ok=1
        for v in "${NODES[@]}"; do
            [ "$v" = "$n" ] && [ "$want" = DN ] && continue
            node_alive "$v" || continue
            [ "$(state_seen_by "$v" "$n")" = "$want" ] || ok=0
        done
        [ $ok = 1 ] && return
        [ $SECONDS -lt $deadline ] || { echo "node $n is not $want for every node in ${CASSANDRA_START_TIMEOUT}s" >&2; exit 1; }
        sleep 1
    done
}

stop_node() {
    local n=$1 signal=${2:-TERM}
    check_node "$n"
    node_alive "$n" || return 0
    local pid
    pid=$(cat "$(pid_file "$n")")
    [ "$signal" = TERM ] && nt "$n" drain >/dev/null 2>&1 || true
    kill -"$signal" "$pid"
    while kill -0 "$pid" 2>/dev/null; do sleep 0.2; done
    rm -f "$(pid_file "$n")"
}

cmd=${1:-}
shift || true
case "$cmd" in
    start)
        start_proxy
        for n in "${NODES[@]}"; do start_node "$n"; done
        for n in "${NODES[@]}"; do wait_state "$n" UN; done
        nt 1 status
        ;;
    stop)
        for n in "${NODES[@]}"; do stop_node "$n"; done
        stop_proxy
        ;;
    stop-node) stop_node "${1:-}" ;;
    kill-node) stop_node "${1:-}" KILL ;;
    start-node) start_proxy; start_node "${1:-}"; wait_state "$1" UN ;;
    wait-up) check_node "${1:-}"; wait_state "$1" UN ;;
    wait-down) check_node "${1:-}"; wait_state "$1" DN ;;
    split) python3 "$script_dir/netsplit.py" split ;;
    heal) python3 "$script_dir/netsplit.py" heal ;;
    netstatus) python3 "$script_dir/netsplit.py" status ;;
    status) nt "$(first_live)" status ;;
    nodetool) check_node "${1:-}"; n=$1; shift; nt "$n" "$@" ;;
    cqlsh) check_node "${1:-}"; n=$1; shift; exec cqlsh "$(node_ip "$n")" "$CASSANDRA_PORT" "$@" ;;
    wipe)
        for n in "${NODES[@]}"; do stop_node "$n" KILL; done
        stop_proxy
        rm -rf "$CASSANDRA_DATA_DIR"
        ;;
    *)
        sed -n '2,/^set -euo/p' "$0" | sed '$d; s/^# \{0,1\}//' >&2
        exit 2
        ;;
esac
