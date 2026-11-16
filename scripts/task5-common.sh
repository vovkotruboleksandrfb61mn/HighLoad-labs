# Helpers shared by the Task 5 scripts. Source it after the direnv re-exec:
#   . scripts/task5-common.sh
# Expects OUT_DIR-style variables to be set by the caller where it matters.

_task5_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$_task5_dir/env.sh"
cd "$HLS_ROOT"

CASSANDRA=$_task5_dir/cassandra.sh
# cqlsh (python driver 3.x) warns about deprecated settings on every start.
export PYTHONWARNINGS=ignore

node_ip() { printf '127.0.0.%s\n' $((10 + $1)); }

# Starts the cluster unless all three nodes answer.
cluster_up() {
    local n
    for n in 1 2 3; do
        if ! cqlsh "$(node_ip "$n")" "$CASSANDRA_PORT" -e 'SELECT now() FROM system.local' >/dev/null 2>&1; then
            "$CASSANDRA" start >&2
            return
        fi
    done
}

# cqlsh_quiet N args...: cqlsh into node N without a transcript.
cqlsh_quiet() {
    local n=$1
    shift
    cqlsh "$(node_ip "$n")" "$CASSANDRA_PORT" --request-timeout=30 "$@"
}

stamp() {
    local iso
    iso=$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)
    STAMP=${iso#*T}
    STAMP=${STAMP%Z}
}

note() { printf '# %s\n' "$*" >>"$out"; }
gap() { printf '\n' >>"$out"; }

# run <command...>: append the prompt line and the output to $out.
run() {
    stamp
    printf '[%s] $ %s\n' "$STAMP" "$*" >>"$out"
    local status=0
    "$@" >>"$out" 2>&1 || status=$?
    [ "$status" -eq 0 ] || printf '(exit %s)\n' "$status" >>"$out"
}

# cql_session <dest> <node> < statements: an interactive cqlsh session into
# node N, appended to <dest> with the command that opened it.
cql_session() {
    local dest=$1 n=$2
    stamp
    printf '[%s] $ scripts/cassandra.sh cqlsh %s\n' "$STAMP" "$n" >>"$dest"
    python3 "$_task5_dir/cql-session.py" "$(node_ip "$n")" "$CASSANDRA_PORT" >>"$dest" 2>&1 || true
    printf '\n' >>"$dest"
}

# shoot <transcript> <png prefix> [max lines]: renders the transcript as
# prefix-a.png, prefix-b.png, ... Each picture holds whole blank-line separated
# blocks and at most `max lines` lines (45), unless one block is longer.
shoot() {
    local transcript=$1 prefix=$2 max=${3:-45} name ranges i=0 letters=(a b c d e f g h i j k l m n)
    name=$(basename "$transcript")
    rm -f "$prefix"-?.png
    mapfile -t ranges < <(python3 - "$transcript" "$max" <<'PY'
import re, sys
text = open(sys.argv[1], encoding="utf-8").read()
limit = int(sys.argv[2])
blocks = [b for b in re.split(r"\n\s*\n", text.strip("\n")) if b.strip()]
start, lines = 1, 0
for i, b in enumerate(blocks, 1):
    n = b.count("\n") + 2
    if lines and lines + n > limit:
        print(f"{start}-{i - 1}")
        start, lines = i, 0
    lines += n
print(f"{start}-{len(blocks)}")
PY
)
    for r in "${ranges[@]}"; do
        python3 "$_task5_dir/terminal-shot.py" "$transcript" "$prefix-${letters[$i]}.png" \
            --width 1120 --blocks "$r" --title "$name ($((i + 1))/${#ranges[@]})"
        i=$((i + 1))
    done
}
