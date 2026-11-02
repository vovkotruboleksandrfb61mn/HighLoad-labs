#!/usr/bin/env bash
# Task 4, Part I: write concerns, read concerns and elections on the P-S-S
# replica set of scripts/mongo.sh.
#
#   scripts/task4-part1.sh [1] [2] [3]     (all three by default)
#
#   1. A secondary is down; insertOne with { w: 3, wtimeout: 0 } waits until
#      the node comes back WAIT_BEFORE_RETURN seconds later.
#   2. A secondary is down; insertOne with { w: 3, wtimeout: WTIMEOUT_MS }
#      fails with a write concern error, yet the document is on the primary,
#      readable with readConcern "majority" and on the other secondary.
#   3. The primary is shut down (SIGTERM); a secondary is elected, takes
#      writes, and the old primary rejoins as a SECONDARY with those writes.
#
# Each scenario writes a terminal transcript, $OUT_DIR/task4-part1-N.txt:
# the commands as typed, timestamped in UTC, with their output. mongosh
# sessions are fed through stdin, and the transcript puts each input line
# after the prompt it answered. With SHOTS=true (default) every transcript is
# also rendered as $IMG_DIR/task4-part1-N.png by scripts/terminal-shot.py,
# and in parts, task4-part1-Na.png, -Nb.png and so on, for the report.
#
# Starts the replica set if it is not running and stops it at the end in
# that case. Drops the test databases first (scripts/mongo.sh reset).
#
# Overridable: OUT_DIR (reports/logs), IMG_DIR (reports/img), SHOTS (true),
# WAIT_BEFORE_RETURN (10), WTIMEOUT_MS (5000).
set -euo pipefail

script_dir=$(cd "$(dirname "$0")" && pwd)
if ! command -v mongosh >/dev/null && [ -z "${HLS_MONGO_REEXEC:-}" ] && command -v direnv >/dev/null; then
    HLS_MONGO_REEXEC=1 exec direnv exec "$script_dir/.." "$0" "$@"
fi
. "$script_dir/env.sh"
cd "$HLS_ROOT"

OUT_DIR=${OUT_DIR:-$HLS_ROOT/reports/logs}
IMG_DIR=${IMG_DIR:-$HLS_ROOT/reports/img}
SHOTS=${SHOTS:-true}
WAIT_BEFORE_RETURN=${WAIT_BEFORE_RETURN:-10}
WTIMEOUT_MS=${WTIMEOUT_MS:-5000}
scenarios=${*:-1 2 3}

tmp=$(mktemp -d)
started_cluster=false
cleanup() {
    if $started_cluster; then
        scripts/mongo.sh stop
    fi
    rm -rf "$tmp"
}
trap cleanup EXIT

if ! scripts/mongo.sh status >/dev/null 2>&1; then
    started_cluster=true
fi
scripts/mongo.sh start >&2
scripts/mongo.sh reset >&2
mkdir -p "$OUT_DIR"

# Transcript helpers. `out` is the file being written; LAST_ISO is the UTC
# time of the last command run, as an ISO date for mongosh.
out=/dev/null
LAST_ISO=
STAMP=
# stamp: set STAMP (HH:MM:SS.mmm) and LAST_ISO to the current UTC time.
stamp() {
    LAST_ISO=$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)
    STAMP=${LAST_ISO#*T}
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

# interleave <input> <raw output>: put every input line after the prompt
# mongosh printed for it.
interleave() {
    python3 - "$1" "$2" <<'PY'
import re, sys
lines = open(sys.argv[1], encoding="utf-8").read().splitlines()
raw = open(sys.argv[2], encoding="utf-8").read().replace("\r", "")
prompt = re.compile(r"rs0 \[[^\]\n]*\] [\w.-]+> ")
parts = prompt.split(raw)
prompts = prompt.findall(raw)
out = []
if parts[0].strip():
    out.append(parts[0].rstrip("\n"))
for i, p in enumerate(prompts):
    if i >= len(lines):
        break  # the prompt mongosh prints at end of input
    out.append(p + lines[i])
    body = parts[i + 1].rstrip("\n")
    if body.strip():
        out.append(body)
if len(prompts) < len(lines):
    out.append(f"(mongosh stopped after {len(prompts)} of {len(lines)} input lines)")
print("\n".join(out))
PY
}

# session <dest> [N] <<'JS': a mongosh session, into the replica set or
# straight into node N, with the JS lines from stdin as its input.
session() {
    local dest=$1 input raw
    shift
    input=$(mktemp -p "$tmp" in.XXXX)
    raw=$(mktemp -p "$tmp" raw.XXXX)
    cat >"$input"
    stamp
    printf '[%s] $ scripts/mongo.sh shell %s\n' "$STAMP" "$*${*:+ }--quiet --norc" >>"$dest"
    scripts/mongo.sh shell "$@" --quiet --norc <"$input" >"$raw" 2>&1 || true
    interleave "$input" "$raw" >>"$dest"
}

primary_node() {
    scripts/mongo.sh primary | sed -n 's/^primary: node\([0-9]\) .*/\1/p'
}

# The secondaries, highest-numbered first.
secondaries() {
    local p n
    p=$(primary_node)
    for n in 3 2 1; do
        [ "$n" = "$p" ] || printf '%s\n' "$n"
    done
}

sleep_until() {
    local remaining
    remaining=$(awk -v d="$1" -v n="$EPOCHREALTIME" 'BEGIN { r = d - n; print (r > 0 ? r : 0) }')
    sleep "$remaining"
}

header() {
    out="$OUT_DIR/task4-part1-$1.txt"
    : >"$out"
    note "$2"
    note "Час у [квадратних дужках] і в ISODate(...) указано в UTC."
    note "mongosh читає команди зі stdin: текст після підказки rs0 [...]> і є введеною командою."
    gap
}

scenario1() {
    header 1 "Сценарій 1. Один secondary вимкнено, запис з writeConcern { w: 3, wtimeout: 0 } (без тайм-ауту)."
    run scripts/mongo.sh status
    local -a sec
    mapfile -t sec < <(secondaries)
    local s=${sec[0]}
    gap
    run scripts/mongo.sh stop-node "$s"
    run scripts/mongo.sh wait-down "$s"
    run scripts/mongo.sh status
    gap
    note "[термінал 2] вставка з w: 3 чекає, доки документ запишуть усі три вузли"
    local t2="$tmp/s1-terminal2" t1="$tmp/s1-terminal1" launched
    : >"$t2"
    : >"$t1"
    launched=$EPOCHREALTIME
    session "$t2" <<'JS' &
use lab4
let t0 = new Date(); t0
db.part1.insertOne({ scenario: 1, note: "w: 3, wtimeout: 0" }, { writeConcern: { w: 3, wtimeout: 0 } })
let t1 = new Date(); t1
(t1 - t0) / 1000
JS
    local waiting=$!
    sleep 5
    local saved=$out
    out=$t1
    gap
    note "[термінал 1] поки вставка чекає: документ уже є на primary і на другому secondary (2 з 3 = majority)"
    session "$t1" <<'JS'
use lab4
db.part1.find({ scenario: 1 })
db.part1.find({ scenario: 1 }).readConcern("majority")
JS
    sleep_until "$(awk -v l="$launched" -v w="$WAIT_BEFORE_RETURN" 'BEGIN { printf "%.6f", l + w }')"
    run scripts/mongo.sh start-node "$s"
    gap
    out=$saved
    wait "$waiting"
    # Terminal 2 up to the insert, terminal 1 meanwhile, then the insert's result.
    awk '{ print } /writeConcern: \{ w: 3/ { exit }' "$t2" >>"$out"
    cat "$t1" >>"$out"
    note "[термінал 2] вставка повернулася"
    awk 'found { print } /writeConcern: \{ w: 3/ { found = 1 }' "$t2" >>"$out"
    gap
    note "[термінал 1] вузол $s наздогнав primary і має документ"
    run scripts/mongo.sh wait-secondary "$s"
    session "$out" "$s" <<'JS'
use lab4
db.part1.find({ scenario: 1 }).readPref("secondary")
JS
}

scenario2() {
    header 2 "Сценарій 2. Один secondary вимкнено, запис з writeConcern { w: 3, wtimeout: $WTIMEOUT_MS }."
    run scripts/mongo.sh status
    local -a sec
    mapfile -t sec < <(secondaries)
    local s=${sec[0]} r=${sec[1]}
    gap
    run scripts/mongo.sh stop-node "$s"
    run scripts/mongo.sh wait-down "$s"
    gap
    note "Вставка чекає ${WTIMEOUT_MS} мс і повертає WriteConcernError, але документ не відкочується"
    session "$out" <<JS
use lab4
let t0 = new Date(); t0
db.part1.insertOne({ scenario: 2, note: "w: 3, wtimeout: $WTIMEOUT_MS" }, { writeConcern: { w: 3, wtimeout: $WTIMEOUT_MS } })
(new Date() - t0) / 1000
db.part1.find({ scenario: 2 })
db.part1.find({ scenario: 2 }).readConcern("majority")
JS
    gap
    note "Другий secondary (node$r) напряму: документ на ньому вже є"
    session "$out" "$r" <<'JS'
use lab4
db.part1.find({ scenario: 2 }).readPref("secondary")
db.part1.find({ scenario: 2 }).readPref("secondary").readConcern("majority")
JS
    gap
    note "Повертаємо node$s: він дотягує документ з oplog"
    run scripts/mongo.sh start-node "$s"
    run scripts/mongo.sh wait-secondary "$s"
    session "$out" "$s" <<'JS'
use lab4
db.part1.find({ scenario: 2 }).readPref("secondary")
JS
    run scripts/mongo.sh status
}

scenario3() {
    header 3 "Сценарій 3. Перевибори primary: штатно зупиняємо (SIGTERM) поточний primary, пишемо, повертаємо старий primary."
    run scripts/mongo.sh primary
    run scripts/mongo.sh status
    local p stopped_at
    p=$(primary_node)
    gap
    run scripts/mongo.sh stop-node "$p"
    stopped_at=$LAST_ISO
    run scripts/mongo.sh wait-primary
    run scripts/mongo.sh wait-down "$p"
    run scripts/mongo.sh status
    gap
    note "Новий primary: метрики виборів, час від команди stop-node до виборів, запис через URI з усіма вузлами"
    session "$out" <<JS
use lab4
rs.status().term
db.hello().primary
rs.status().electionCandidateMetrics
(rs.status().electionCandidateMetrics.lastElectionDate - ISODate("$stopped_at")) / 1000
db.part1.insertMany([{ scenario: 3, n: 1 }, { scenario: 3, n: 2 }, { scenario: 3, n: 3 }], { writeConcern: { w: "majority" } })
db.part1.countDocuments({ scenario: 3 })
JS
    gap
    note "Повертаємо старий primary (node$p)"
    run scripts/mongo.sh start-node "$p"
    run scripts/mongo.sh wait-secondary "$p"
    run scripts/mongo.sh status
    gap
    note "node$p напряму (directConnection=true): тепер SECONDARY, має документи, записані без нього"
    session "$out" "$p" <<'JS'
use lab4
db.hello().isWritablePrimary
db.hello().primary
rs.status().term
db.part1.find({ scenario: 3 }).sort({ n: 1 }).readPref("secondary")
JS
    run scripts/mongo.sh primary
}

# Blocks of each transcript per report picture (see the SHOTS step below).
declare -A SHOT_PARTS=([1]="1-4 5-7" [2]="1-4 5 6-7" [3]="1-3 4 5-6")

for n in $scenarios; do
    case $n in
        1 | 2 | 3) ;;
        *) echo "unknown scenario: $n (1, 2 or 3)" >&2; exit 2 ;;
    esac
    # Every scenario starts from three running nodes.
    scripts/mongo.sh start >&2
    echo "scenario $n" >&2
    "scenario$n"
    echo "wrote $out" >&2
    if [ "$SHOTS" = true ]; then
        mkdir -p "$IMG_DIR"
        python3 "$script_dir/terminal-shot.py" "$out" "$IMG_DIR/task4-part1-$n.png"
        # The report shows each transcript in parts (a, b, c) narrow and
        # short enough for an A4 page; the parts are runs of blank-line
        # separated blocks of the transcript.
        parts=(${SHOT_PARTS[$n]}) letters=(a b c d)
        for i in "${!parts[@]}"; do
            python3 "$script_dir/terminal-shot.py" "$out" "$IMG_DIR/task4-part1-$n${letters[$i]}.png" \
                --width 1120 --blocks "${parts[$i]}" --title "task4-part1-$n.txt ($((i + 1))/${#parts[@]})"
        done
    fi
done
