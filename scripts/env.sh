# Shared environment for the scripts. Source it from bash or zsh:
#   . scripts/env.sh
#
# Defines the ports, paths and helpers the measurement scripts share. Swift
# (6.x, for the Synchronization module) must already be on PATH as `swift`.
# Works from a git worktree too: the gitignored .toolchain/ with Hazelcast is
# then taken from the main checkout.

if [ -n "${BASH_VERSION:-}" ]; then
    _hls_source=${BASH_SOURCE[0]}
elif [ -n "${ZSH_VERSION:-}" ]; then
    eval '_hls_source=${(%):-%x}'
else
    _hls_source=$0
fi
HLS_ROOT=$(cd "$(dirname "$_hls_source")/.." && pwd)
unset _hls_source
export HLS_ROOT

# The main checkout: the same as HLS_ROOT, except in a git worktree, where it
# is the checkout that owns the shared .git directory.
_hls_main_root() {
    local common
    if common=$(git -C "$HLS_ROOT" rev-parse --path-format=absolute --git-common-dir 2>/dev/null); then
        dirname "$common"
    else
        printf '%s\n' "$HLS_ROOT"
    fi
}
HLS_MAIN_ROOT=$(_hls_main_root)
export HLS_MAIN_ROOT
export HLS_TOOLCHAIN="$HLS_MAIN_ROOT/.toolchain"
export HZ_HOME="$HLS_TOOLCHAIN/hazelcast-5.4.0"
# liburing for counter-server's io_uring engine: the Nix env lists its dev
# output in nativeBuildInputs but does not put it on PKG_CONFIG_PATH.
for _hls_input in ${nativeBuildInputs:-}; do
    case "$_hls_input" in
        *-liburing-*-dev)
            export PKG_CONFIG_PATH="$_hls_input/lib/pkgconfig${PKG_CONFIG_PATH:+:$PKG_CONFIG_PATH}"
            export HLS_IO_URING=1
            HLS_LIBURING_LIBDIR=$(pkg-config --variable=libdir liburing 2>/dev/null) && export HLS_LIBURING_LIBDIR
            ;;
    esac
done
unset _hls_input

if ! command -v swift >/dev/null 2>&1; then
    echo "env.sh: swift is not on PATH (Swift 6 is required)" >&2
fi

# Ports: everything we start lives in the block 21400-21499
# (Hazelcast members excepted: 5701-5703).
export SERVER_PORT=${SERVER_PORT:-21400}
export SERVER_URL=${SERVER_URL:-http://127.0.0.1:$SERVER_PORT}
export PG_PORT=${PG_PORT:-21410}
export PG_URL=${PG_URL:-postgres://postgres@127.0.0.1:$PG_PORT/postgres}
export HZ_ADDRESS=${HZ_ADDRESS:-127.0.0.1:5701}

# Scratch data (counter file, perf recordings), gitignored and on the
# project's disk, so fsync really reaches a device.
export HLS_DATA="$HLS_ROOT/.data"
export HLS_BIN="$HLS_ROOT/.build/release"

# Logs of the servers and Hazelcast members the scripts start. The default is
# the committed reports/logs; point it elsewhere for a check run that should
# not overwrite the logs the reports quote.
export HLS_LOG_DIR=${HLS_LOG_DIR:-$HLS_ROOT/reports/logs}

# The PostgreSQL cluster (scripts/pg.sh) lives in the main checkout, so every
# worktree talks to the same one and it outlives any worktree. The unix
# socket sits in the data directory too.
export PGDATA_DIR=${PGDATA_DIR:-$HLS_MAIN_ROOT/.pgdata}

# Release build with debug info, so perf can unwind and symbolise.
hls_build_release() {
    swift build --package-path "$HLS_ROOT" -c release -Xswiftc -g "$@"
}

# The extra counter-server arguments a store needs.
hls_store_args() {
    case "$1" in
        memory) ;;
        disk) printf '%s\n' --file "$HLS_DATA/counter.txt" ;;
        postgres) printf '%s\n' --pg-url "$PG_URL" ;;
        hazelcast) printf '%s\n' --hz-address "$HZ_ADDRESS" ;;
        *) echo "unknown store: $1" >&2; return 1 ;;
    esac
}

# hls_start_server <store>: starts the release server in the background, sets
# HLS_SERVER_PID and waits until it answers GET /count.
hls_start_server() {
    local store=$1 args
    mkdir -p "$HLS_DATA" "$HLS_LOG_DIR"
    [ "$store" = disk ] && rm -f "$HLS_DATA/counter.txt"
    args=$(hls_store_args "$store") || return 1
    # shellcheck disable=SC2086 # word splitting of the store arguments is intended
    "$HLS_BIN/counter-server" --store "$store" --port "$SERVER_PORT" $args \
        2>>"$HLS_LOG_DIR/counter-server-$store.log" &
    HLS_SERVER_PID=$!
    local tries=0
    until curl -sf -o /dev/null "$SERVER_URL/count"; do
        if ! kill -0 "$HLS_SERVER_PID" 2>/dev/null; then
            echo "counter-server --store $store exited; see $HLS_LOG_DIR/counter-server-$store.log" >&2
            unset HLS_SERVER_PID
            return 1
        fi
        tries=$((tries + 1))
        if [ "$tries" -gt 100 ]; then
            echo "counter-server --store $store did not come up within 10s" >&2
            hls_stop_server
            return 1
        fi
        sleep 0.1
    done
}

hls_stop_server() {
    if [ -n "${HLS_SERVER_PID:-}" ]; then
        kill "$HLS_SERVER_PID" 2>/dev/null || true
        wait "$HLS_SERVER_PID" 2>/dev/null || true
        unset HLS_SERVER_PID
    fi
}
