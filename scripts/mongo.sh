#!/usr/bin/env bash
# The local MongoDB replica set for Task 4: three mongod processes, P-S-S.
#
#   scripts/mongo.sh start         start nodes 1-3, rs.initiate on first use,
#                                  wait for a PRIMARY
#   scripts/mongo.sh stop          stop every node (clean shutdown)
#   scripts/mongo.sh stop-node N   clean shutdown of node N (SIGTERM)
#   scripts/mongo.sh kill-node N   sudden failure of node N (SIGKILL)
#   scripts/mongo.sh pause-node N  freeze node N (SIGSTOP): it keeps its
#                                  connections but answers nothing, like a
#                                  node cut off by a network partition
#   scripts/mongo.sh resume-node N let a paused node N run again (SIGCONT)
#   scripts/mongo.sh start-node N  start node N again (its log is appended to)
#   scripts/mongo.sh wait-primary  wait until some node is PRIMARY
#   scripts/mongo.sh wait-secondary N
#                                  wait until node N is SECONDARY and has
#                                  caught up with the primary
#   scripts/mongo.sh wait-down N   wait until the primary sees node N as down
#   scripts/mongo.sh primary       print which node is the primary
#   scripts/mongo.sh status        compact rs.status() summary
#   scripts/mongo.sh shell [N] ... mongosh into the replica set ($MONGO_URL),
#                                  or straight into node N; extra arguments go
#                                  to mongosh
#   scripts/mongo.sh reset         drop every database except admin/config/local
#
# Node N listens on 127.0.0.1:$((MONGO_PORT + N - 1)) (21430-21432), keeps its
# data in $MONGO_DATA_DIR/nodeN (.mongodata, gitignored) and logs (quiet,
# timestamps in UTC like the Task 4 transcripts) to
# $MONGO_LOG_DIR/mongo-nodeN.log ($HLS_LOG_DIR, so reports/logs, by default;
# scripts/task4.sh keeps its runs apart in reports/logs/task4; `start` from a
# fully stopped set truncates the logs, otherwise they are appended to).
#
# mongod and mongosh come from the project's Nix flake; if they are not on
# PATH the script re-runs itself under `direnv exec`.
#
# Overridable: MONGO_CACHE_GB (WiredTiger cache per node, 0.25),
# MONGO_START_TIMEOUT seconds (60), MONGO_LOG_DIR ($HLS_LOG_DIR).
# MONGO_TEST_COMMANDS=1 starts the nodes with enableTestCommands, which
# allows configureFailPoint (scripts/task4x-rollback.sh uses it); never on by
# default. MONGOD_ARGS: more mongod arguments for every node, split on spaces
# (scripts/task4x-throughput.py uses it for startup-only settings).
# MONGO_ELECTION_TIMEOUT_MS: `start` sets settings.electionTimeoutMillis of
# the replica set config to it with rs.reconfig (unset: left as it is, 10000
# in a new set); the set keeps it until changed again or its data is deleted.
set -euo pipefail

script_dir=$(cd "$(dirname "$0")" && pwd)
if { ! command -v mongod >/dev/null || ! command -v mongosh >/dev/null; } \
    && [ -z "${HLS_MONGO_REEXEC:-}" ] && command -v direnv >/dev/null; then
    HLS_MONGO_REEXEC=1 exec direnv exec "$script_dir/.." "$0" "$@"
fi
for tool in mongod mongosh; do
    command -v "$tool" >/dev/null || { echo "missing $tool (nixpkgs: mongodb-ce, mongosh)" >&2; exit 1; }
done

. "$script_dir/env.sh"

NODES=(1 2 3)
MONGO_LOG_DIR=${MONGO_LOG_DIR:-$HLS_LOG_DIR}
MONGO_CACHE_GB=${MONGO_CACHE_GB:-0.25}
MONGO_START_TIMEOUT=${MONGO_START_TIMEOUT:-60}
MONGO_TEST_COMMANDS=${MONGO_TEST_COMMANDS:-0}
MONGOD_ARGS=${MONGOD_ARGS:-}
MONGO_ELECTION_TIMEOUT_MS=${MONGO_ELECTION_TIMEOUT_MS:-}

node_port() { printf '%s\n' $((MONGO_PORT + $1 - 1)); }
node_host() { printf '127.0.0.1:%s\n' "$(node_port "$1")"; }
node_url() { printf 'mongodb://%s/?directConnection=true\n' "$(node_host "$1")"; }
node_dbpath() { printf '%s/node%s\n' "$MONGO_DATA_DIR" "$1"; }
pid_file() { printf '%s/node%s.pid\n' "$MONGO_DATA_DIR" "$1"; }
log_file() { printf '%s/mongo-node%s.log\n' "$MONGO_LOG_DIR" "$1"; }

check_node() {
    case "${1:-}" in
        1 | 2 | 3) ;;
        *) echo "node must be 1, 2 or 3, not '${1:-}'" >&2; exit 2 ;;
    esac
}

node_pid() {
    local file
    file=$(pid_file "$1")
    [ -s "$file" ] && cat "$file"
}

is_running() {
    local pid
    pid=$(node_pid "$1") || return 1
    kill -0 "$pid" 2>/dev/null
}

port_in_use() {
    (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null
}

# msh <url> <js>: evaluate JS with mongosh, printing only its result.
msh() {
    mongosh "$1" --quiet --norc --eval "$2"
}

# The first running node that answers, or nothing.
any_node() {
    local n
    for n in "${NODES[@]}"; do
        if is_running "$n" && msh "$(node_url "$n")" 'db.adminCommand({ ping: 1 }).ok' >/dev/null 2>&1; then
            printf '%s\n' "$n"
            return 0
        fi
    done
    return 1
}

start_node() {
    local n=$1 truncate_log=${2:-false} port log dbpath
    port=$(node_port "$n")
    log=$(log_file "$n")
    dbpath=$(node_dbpath "$n")
    if is_running "$n"; then
        echo "node $n already runs (pid $(node_pid "$n"))" >&2
        return 0
    fi
    if port_in_use "$port"; then
        echo "port $port is taken by another process; not starting node $n" >&2
        return 1
    fi
    mkdir -p "$dbpath" "$MONGO_LOG_DIR"
    if $truncate_log; then
        : >"$log"
    fi
    local output extra=()
    if [ "$MONGO_TEST_COMMANDS" = 1 ]; then
        extra=(--setParameter enableTestCommands=1)
    fi
    # shellcheck disable=SC2206 # split on spaces on purpose
    extra+=($MONGOD_ARGS)
    if ! output=$(mongod --replSet "$MONGO_REPLSET" --port "$port" --bind_ip 127.0.0.1 \
        --dbpath "$dbpath" --logpath "$log" --logappend \
        --wiredTigerCacheSizeGB "$MONGO_CACHE_GB" --quiet --timeStampFormat iso8601-utc \
        "${extra[@]}" --fork --pidfilepath "$(pid_file "$n")" 2>&1); then
        printf '%s\n' "$output" >&2
        echo "node $n failed to start; end of $log:" >&2
        tail -n 20 "$log" >&2 || true
        return 1
    fi
    echo "node $n started: $(node_host "$n"), pid $(node_pid "$n")" >&2
}

# stop_node <n> <signal>: TERM is a clean shutdown (a primary steps down
# first), KILL a sudden failure.
stop_node() {
    local n=$1 signal=$2 pid began=$EPOCHREALTIME deadline=$((SECONDS + 60))
    if ! pid=$(node_pid "$n") || ! kill -0 "$pid" 2>/dev/null; then
        echo "node $n is not running" >&2
        rm -f "$(pid_file "$n")"
        return 0
    fi
    kill -s "$signal" "$pid"
    local signalled=$EPOCHREALTIME
    # A paused node (pause-node) handles SIGTERM only once it runs again.
    [ "$signal" = KILL ] || kill -s CONT "$pid"
    while kill -0 "$pid" 2>/dev/null; do
        if [ "$SECONDS" -ge "$deadline" ]; then
            echo "node $n did not stop within 60s; killing it" >&2
            kill -9 "$pid" 2>/dev/null || true
        fi
        sleep 0.1
    done
    rm -f "$(pid_file "$n")"
    local took
    took=$(awk -v b="$began" -v e="$EPOCHREALTIME" 'BEGIN { printf "%.1f", e - b }')
    case $signal in
        KILL) echo "node $n killed (SIGKILL at $(date -u -d "@$signalled" +%H:%M:%S.%3N) UTC, pid $pid), gone after $took s" >&2 ;;
        *) echo "node $n stopped (SIGTERM, pid $pid), exited after $took s" >&2 ;;
    esac
}

# signal_node <n> STOP|CONT: freeze or thaw node n.
signal_node() {
    local pid
    if ! pid=$(node_pid "$1") || ! kill -0 "$pid" 2>/dev/null; then
        echo "node $1 is not running" >&2
        return 1
    fi
    kill -s "$2" "$pid"
    case $2 in
        STOP) echo "node $1 paused (SIGSTOP, pid $pid)" >&2 ;;
        CONT) echo "node $1 resumed (SIGCONT, pid $pid)" >&2 ;;
    esac
}

# rs.initiate with all three members, unless the set already has a config.
initiate() {
    local first state members="" n
    first=$(any_node) || { echo "no node answers" >&2; return 1; }
    state=$(msh "$(node_url "$first")" '
        try { rs.status().set } catch (e) { e.codeName }')
    if [ "$state" != NotYetInitialized ]; then
        return 0
    fi
    for n in "${NODES[@]}"; do
        members+="{ _id: $((n - 1)), host: \"$(node_host "$n")\" },"
    done
    msh "$(node_url "$first")" "rs.initiate({ _id: \"$MONGO_REPLSET\", members: [ $members ] }).ok" >/dev/null
    echo "replica set $MONGO_REPLSET initiated with ${NODES[*]/#/node}" >&2
}

# JS shared by the polling commands: HOSTS, nodeOf(host), helloOf(host),
# statusOf(host) and findPrimary(). Each probe opens a short-lived direct
# connection, so one mongosh process can poll every 100 ms.
js_prelude() {
    local hosts="" n
    for n in "${NODES[@]}"; do
        hosts+="'$(node_host "$n")', "
    done
    cat <<EOF
const HOSTS = [ $hosts];
const BASE_PORT = $MONGO_PORT;
function nodeOf(host) { return 'node' + (Number(host.split(':')[1]) - BASE_PORT + 1); }
function directly(host, command) {
    try {
        const c = new Mongo('mongodb://' + host + '/?directConnection=true&serverSelectionTimeoutMS=300&connectTimeoutMS=300');
        // Not try/finally: mongosh would close before the command finishes.
        const reply = c.getDB('admin').runCommand(command);
        c.close();
        return reply;
    } catch (e) { return null; }
}
function helloOf(host) { return directly(host, { hello: 1 }); }
function statusOf(host) { return directly(host, { replSetGetStatus: 1 }); }
function findPrimary() {
    for (const host of HOSTS) {
        const hello = helloOf(host);
        if (hello && hello.isWritablePrimary) return host;
    }
    return null;
}
function seconds(since) { return ((Date.now() - since) / 1000).toFixed(1) + ' s'; }
EOF
}

# mjs <js>: run JS with the prelude in a mongosh without a connection.
mjs() {
    mongosh --nodb --quiet --norc --eval "$(js_prelude)
$1"
}

# wait_primary [timeout seconds]: one pass with 0. Prints the primary, its
# term and how long the wait took.
wait_primary() {
    mjs "
        const t0 = Date.now(), timeout = ${1:-$MONGO_START_TIMEOUT} * 1000;
        let host;
        while (!(host = findPrimary())) {
            if (Date.now() - t0 >= timeout) {
                print(timeout ? 'no PRIMARY within ' + timeout / 1000 + ' s' : 'no primary');
                quit(1);
            }
            sleep(100);
        }
        const status = statusOf(host);
        print('primary: ' + nodeOf(host) + ' (' + host + '), term ' + status.term
              + (timeout ? ', found after ' + seconds(t0) : ''));"
}

# wait_down <n>: until the primary's heartbeats mark node n unhealthy (a
# heartbeat every 2 s, so the primary notices a dead node only then).
wait_down() {
    mjs "
        const target = HOSTS[$1 - 1], t0 = Date.now(), timeout = $MONGO_START_TIMEOUT * 1000;
        while (Date.now() - t0 < timeout) {
            const primary = findPrimary();
            const status = primary && statusOf(primary);
            const me = status && status.members.find(m => m.name === target);
            if (me && me.health === 0) {
                print(nodeOf(primary) + ' sees ' + nodeOf(target) + ' as ' + me.stateStr
                      + ' after ' + seconds(t0) + ' (' + me.lastHeartbeatMessage + ')');
                quit(0);
            }
            sleep(100);
        }
        print('the primary still sees ' + nodeOf(target) + ' as healthy after ' + timeout / 1000 + ' s');
        quit(1);"
}

# wait_secondary <n>: until node n is SECONDARY and, as the primary sees it,
# has applied everything the primary has.
wait_secondary() {
    mjs "
        const target = HOSTS[$1 - 1], t0 = Date.now(), timeout = $MONGO_START_TIMEOUT * 1000;
        let last = 'unknown';
        while (Date.now() - t0 < timeout) {
            const primary = findPrimary();
            const status = primary && statusOf(primary);
            const me = status && status.members.find(m => m.name === target);
            const leader = status && status.members.find(m => m.self);
            if (me) last = me.stateStr;
            if (me && me.stateStr === 'SECONDARY' && me.optime.ts.equals(leader.optime.ts)) {
                print(nodeOf(target) + ' is SECONDARY and caught up with ' + nodeOf(primary)
                      + ' after ' + seconds(t0) + ' (optime ' + me.optime.ts.t + ':' + me.optime.ts.i + ')');
                quit(0);
            }
            sleep(100);
        }
        print(nodeOf(target) + ' did not catch up within ' + timeout / 1000 + ' s (state ' + last + ')');
        quit(1);"
}

start() {
    mkdir -p "$MONGO_DATA_DIR"
    # A fresh start (no node running) begins fresh logs; starting the nodes
    # missing from a running set appends to theirs.
    local n fresh=true
    for n in "${NODES[@]}"; do
        if is_running "$n"; then fresh=false; fi
    done
    for n in "${NODES[@]}"; do
        is_running "$n" || start_node "$n" "$fresh" || { stop; exit 1; }
    done
    initiate
    wait_primary
    if [ -n "$MONGO_ELECTION_TIMEOUT_MS" ]; then
        set_election_timeout "$MONGO_ELECTION_TIMEOUT_MS"
    fi
}

# set_election_timeout <ms>: rs.reconfig with a new electionTimeoutMillis,
# unless the config already has it. Prints the heartbeat settings too.
set_election_timeout() {
    if ! [[ "$1" =~ ^[1-9][0-9]*$ ]]; then
        echo "MONGO_ELECTION_TIMEOUT_MS must be a positive number of milliseconds, not '$1'" >&2
        return 2
    fi
    msh "$MONGO_URL" "
        const config = rs.conf(), before = config.settings.electionTimeoutMillis;
        if (before !== $1) {
            config.settings.electionTimeoutMillis = $1;
            const reply = rs.reconfig(config);
            if (!reply.ok) { printjson(reply); quit(1); }
        }
        const s = rs.conf().settings;
        print('electionTimeoutMillis ' + (before === $1 ? before + ' (unchanged)' : before + ' -> ' + s.electionTimeoutMillis)
              + ', heartbeatIntervalMillis ' + s.heartbeatIntervalMillis
              + ', heartbeatTimeoutSecs ' + s.heartbeatTimeoutSecs);" >&2
}

# All nodes at once: each spends up to 15 s in quiesce mode on SIGTERM.
stop() {
    local n
    for n in "${NODES[@]}"; do
        if is_running "$n"; then
            stop_node "$n" TERM &
        else
            rm -f "$(pid_file "$n")"
        fi
    done
    wait
}

primary() {
    wait_primary 0
}

status() {
    local n pids=""
    for n in "${NODES[@]}"; do
        if is_running "$n"; then
            pids+="'$(node_host "$n")': 'pid $(node_pid "$n")', "
        else
            pids+="'$(node_host "$n")': 'process stopped', "
        fi
    done
    # Seen from the primary when there is one: only its optimes for the
    # other members are fresh (a secondary learns the primary's optime from
    # heartbeats, every 2 s).
    mjs "
        const pids = { $pids};
        const from = findPrimary() || HOSTS.find(host => helloOf(host));
        if (!from) {
            for (const host of HOSTS) print(nodeOf(host) + '  ' + host + '  ' + pids[host]);
            quit(3);
        }
        const s = statusOf(from);
        if (!s.ok) { print('replSetGetStatus on ' + from + ': ' + s.codeName); quit(1); }
        print(s.set + ', term ' + s.term + ', seen from ' + nodeOf(from));
        const primary = s.members.find(m => m.stateStr === 'PRIMARY');
        for (const m of s.members) {
            const ts = m.optime && m.optime.ts ? m.optime.ts.t + ':' + m.optime.ts.i : '-';
            const lag = primary && m !== primary && m.health === 1 && m.optimeDate
                ? 'lag ' + (primary.optimeDate - m.optimeDate) / 1000 + ' s' : '';
            const sync = m.syncSourceHost ? 'sync ' + nodeOf(m.syncSourceHost) : '';
            print([nodeOf(m.name), m.name, m.stateStr.padEnd(24), 'health ' + m.health,
                   'optime ' + ts, lag, sync, '(' + pids[m.name] + ')']
                  .filter(x => x !== '').join('  '));
        }"
}

reset() {
    msh "$MONGO_URL" '
        const keep = ["admin", "config", "local"];
        const dropped = db.adminCommand({ listDatabases: 1, nameOnly: true }).databases
            .map(d => d.name).filter(name => !keep.includes(name));
        for (const name of dropped) db.getSiblingDB(name).dropDatabase({ w: "majority" });
        print(dropped.length ? "dropped: " + dropped.join(", ") : "nothing to drop");'
}

case "${1:-}" in
    start) start ;;
    stop) stop ;;
    stop-node) check_node "${2:-}"; stop_node "$2" TERM ;;
    kill-node) check_node "${2:-}"; stop_node "$2" KILL ;;
    pause-node) check_node "${2:-}"; signal_node "$2" STOP ;;
    resume-node) check_node "${2:-}"; signal_node "$2" CONT ;;
    start-node) check_node "${2:-}"; start_node "$2" ;;
    wait-primary) wait_primary ;;
    wait-secondary) check_node "${2:-}"; wait_secondary "$2" ;;
    wait-down) check_node "${2:-}"; wait_down "$2" ;;
    primary) primary ;;
    status) status ;;
    shell)
        shift
        if [[ "${1:-}" =~ ^[0-9]+$ ]]; then
            check_node "$1"
            url=$(node_url "$1")
            shift
            exec mongosh "$url" "$@"
        fi
        exec mongosh "$MONGO_URL" "$@"
        ;;
    reset) reset ;;
    *)
        echo "usage: $0 start|stop|stop-node N|kill-node N|pause-node N|resume-node N|start-node N|wait-primary|wait-secondary N|wait-down N|primary|status|shell [N] [args]|reset" >&2
        exit 2
        ;;
esac
