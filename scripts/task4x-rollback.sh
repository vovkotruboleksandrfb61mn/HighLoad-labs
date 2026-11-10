#!/usr/bin/env bash
# Task 4, extra: a real replica set rollback, with rollback files, and the
# reads around it (dirty, stale, majority, linearizable).
#
#   scripts/task4x-rollback.sh            FAILURE=pause (default) or kill
#
# 1. A counter { likes: 100 } and a document "before" are written with
#    w: "majority".
# 2. Both secondaries stop fetching the oplog (failpoint stopReplProducer),
#    so the primary P keeps every new write to itself. P gets 5 x ($inc
#    likes, insert lost-i), each with { w: 1, j: true }: acknowledged and in
#    P's journal, on no other node.
# 3. P drops out: FAILURE=pause freezes it (SIGSTOP, like a network
#    partition), FAILURE=kill SIGKILLs it. The secondaries elect a new
#    primary, replication is switched back on, and the new primary takes
#    $inc likes 10 and a document "after" with w: "majority".
# 4. P comes back (SIGCONT, or start-node), finds its oplog diverged from
#    the new primary's and rolls back. With FAILURE=pause it is held just
#    before the rollback (failpoint rollbackHangBeforeStart) so that reads
#    on it, by then a SECONDARY, can be shown.
# 5. The rollback lines of P's log, the files under dbPath/rollback decoded
#    with bsondump, and the collection without the lost writes.
# 6. FAILURE=pause only: a stale read. One secondary stops applying the
#    oplog (rsSyncApplyStop); after a majority $inc it still returns the old
#    value with local and majority, while linearizable on the primary
#    returns the new one.
#
# Every read of steps 2-6 is also taken once more without the transcript
# and written as a row of $DATA_DIR/task4x-reads.csv (FAILURE=pause only).
#
# Runs its own replica set with enableTestCommands (configureFailPoint) in
# .mongodata/task4x-rollback on the usual ports, so the regular set must not
# be running; the set is stopped and its data deleted at the end
# (KEEP_DATA=1 keeps it). The transcript goes to
# $OUT_DIR/task4x-rollback[-kill].txt, the node logs to
# $OUT_DIR/task4x-rollback[-kill]-mongo-nodeN.txt and, with SHOTS=true,
# pictures to $IMG_DIR/task4x-rollback[-kill]*.png.
#
# bsondump comes from mongodb-tools: from PATH, else via
# `nix shell nixpkgs#mongodb-tools`.
#
# Overridable: OUT_DIR (reports/logs), DATA_DIR (reports/data),
# IMG_DIR (reports/img), SHOTS (true), FAILURE (pause), KEEP_DATA (0).
set -euo pipefail

script_dir=$(cd "$(dirname "$0")" && pwd)
if ! command -v mongosh >/dev/null && [ -z "${HLS_MONGO_REEXEC:-}" ] && command -v direnv >/dev/null; then
    HLS_MONGO_REEXEC=1 exec direnv exec "$script_dir/.." "$0" "$@"
fi
. "$script_dir/env.sh"
cd "$HLS_ROOT"

OUT_DIR=${OUT_DIR:-$HLS_ROOT/reports/logs}
DATA_DIR=${DATA_DIR:-$HLS_ROOT/reports/data}
IMG_DIR=${IMG_DIR:-$HLS_ROOT/reports/img}
SHOTS=${SHOTS:-true}
FAILURE=${FAILURE:-pause}
KEEP_DATA=${KEEP_DATA:-0}
case $FAILURE in
    pause) name=task4x-rollback ;;
    kill) name=task4x-rollback-kill ;;
    *) echo "FAILURE must be pause or kill, not '$FAILURE'" >&2; exit 2 ;;
esac

export MONGO_TEST_COMMANDS=1
export MONGO_DATA_DIR=$HLS_MAIN_ROOT/.mongodata/task4x-rollback
export MONGO_LOG_DIR=$MONGO_DATA_DIR/logs
if [ -e "$MONGO_DATA_DIR" ]; then
    echo "$MONGO_DATA_DIR exists; stop that set and delete it first" >&2
    exit 1
fi

tmp=$(mktemp -d)
cleanup() {
    scripts/mongo.sh stop || true
    if [ "$KEEP_DATA" != 1 ]; then
        rm -rf "$MONGO_DATA_DIR"
        rmdir "$(dirname "$MONGO_DATA_DIR")" 2>/dev/null || true
    fi
    rm -rf "$tmp"
}
trap cleanup EXIT

mkdir -p "$OUT_DIR" "$DATA_DIR"
out="$OUT_DIR/$name.txt"
: >"$out"
reads=/dev/null
if [ "$FAILURE" = pause ]; then
    reads="$DATA_DIR/task4x-reads.csv"
    echo "scenario,step,node,node_state,read_concern,likes,lost_docs,error,rolled_back_later" >"$reads"
fi

# Transcript helpers, as in scripts/task4-part1.sh.
STAMP=
stamp() {
    STAMP=$(date -u +%H:%M:%S.%3N)
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
        break
    out.append(p + lines[i])
    body = parts[i + 1].rstrip("\n")
    if body.strip():
        out.append(body)
if len(prompts) < len(lines):
    out.append(f"(mongosh stopped after {len(prompts)} of {len(lines)} input lines)")
print("\n".join(out))
PY
}

# session [N] <<'JS': a mongosh session into the replica set or straight
# into node N, the JS lines from stdin as its input.
session() {
    local input raw
    input=$(mktemp -p "$tmp" in.XXXX)
    raw=$(mktemp -p "$tmp" raw.XXXX)
    cat >"$input"
    stamp
    printf '[%s] $ scripts/mongo.sh shell %s\n' "$STAMP" "$*${*:+ }--quiet --norc" >>"$out"
    scripts/mongo.sh shell "$@" --quiet --norc <"$input" >"$raw" 2>&1 || true
    interleave "$input" "$raw" >>"$out"
}

node_url() { printf 'mongodb://127.0.0.1:%s/?directConnection=true\n' $((MONGO_PORT + $1 - 1)); }
node_log() { printf '%s/mongo-node%s.log\n' "$MONGO_LOG_DIR" "$1"; }

# failpoint <node> <name> <alwaysOn|off>, printed as one mongosh line.
failpoint() {
    session "$1" <<JS
db.adminCommand({ configureFailPoint: "$2", mode: "$3" }).ok
JS
}

# observe <scenario> <step> <node> <read concern>: read the collection on
# node N once more, outside the transcript, and add a row to $reads.
observe() {
    local row
    row=$(mongosh "$(node_url "$3")" --quiet --norc --eval "
        const h = db.hello();
        const state = h.isWritablePrimary ? 'PRIMARY' : h.secondary ? 'SECONDARY' : 'OTHER';
        let likes = '', lost = '', error = '';
        try {
            const docs = db.getSiblingDB('lab4x').rb.find().readPref('nearest')
                .readConcern('$4').maxTimeMS(3000).toArray();
            likes = (docs.find(d => d._id === 'counter') || {}).likes;
            lost = docs.filter(d => String(d._id).startsWith('lost-')).length;
        } catch (e) { error = e.codeName || e.message; }
        print([state, '$4', likes, lost, error, error ? 'n/a' : (lost > 0 ? 'yes' : 'no')].join(','));")
    printf '%s,%s,node%s,%s\n' "$1" "$2" "$3" "$row" >>"$reads"
}

# wait_log <node> <log id>: until node N's log has a line with that id.
wait_log() {
    local deadline=$((SECONDS + 60))
    until grep -q "\"id\":$2," "$(node_log "$1")"; do
        if [ "$SECONDS" -ge "$deadline" ]; then
            echo "no log line with id $2 on node $1 within 60 s" >&2
            return 1
        fi
        sleep 0.2
    done
}

primary_node() {
    scripts/mongo.sh primary | sed -n 's/^primary: node\([0-9]\) .*/\1/p'
}

bsondump() {
    if type -P bsondump >/dev/null; then
        command bsondump "$@"
    else
        nix shell nixpkgs#mongodb-tools --command bsondump "$@"
    fi
}

# rollback_log <node>: the step-down and rollback lines of node N's log:
# time, component, message and the attributes that matter.
rollback_log() {
    jq -r '
        select(.c == "ROLLBACK" or (.id | IN(21098, 21101, 21402, 21475, 21102)))
        | (.attr // {} | with_entries(select(.key | IN(
              "term", "error", "namespace", "file", "commonPointOpTime", "insert", "update", "delete",
              "rollbackCommandCounts", "totalEntriesRolledBackIncludingNoops", "wallClockTimeDiff",
              "commonPoint", "lastOptimeRolledBack")))) as $a
        | "\(.t["$date"][11:23]) \(.c) \(.msg)" + (if $a == {} then "" else "  \($a | tojson)" end)
    ' "$(node_log "$1")" | sed "s#$HLS_MAIN_ROOT/##g"
}

# oplog_dump <file>: the rolled back oplog entries, one compact line each.
oplog_dump() {
    bsondump --quiet "$1" | jq -c '{ts: .ts["$timestamp"], op, ns, o}'
}

# --- the experiment -------------------------------------------------------

scripts/mongo.sh start >&2
P=$(primary_node)
S=()
for n in 1 2 3; do
    [ "$n" = "$P" ] || S+=("$n")
done
datadir=$(realpath --relative-to="$HLS_ROOT" "$MONGO_DATA_DIR")
echo "primary node$P, secondaries node${S[0]} node${S[1]}" >&2
# Fresh secondaries report their optime a few seconds late.
for n in "${S[@]}"; do scripts/mongo.sh wait-secondary "$n" >&2; done

note "Rollback на replica set rs0: записи з { w: 1, j: true }, які мав лише primary node$P."
note "Відмова старого primary: $([ "$FAILURE" = pause ] && echo "SIGSTOP (заморожено, як при розриві мережі), потім SIGCONT" || echo "SIGKILL, потім start-node")."
note "Вузли запущено з --setParameter enableTestCommands=1 (MONGO_TEST_COMMANDS=1), дані в $datadir."
note "Час у [квадратних дужках] указано в UTC. Текст після підказки rs0 [...]> є введеною командою mongosh."
gap
run scripts/mongo.sh status
gap
note "1. Лічильник і документ before записано з w: majority: вони є на всіх вузлах"
session <<'JS'
use lab4x
db.rb.insertMany([{ _id: "counter", likes: 100 }, { _id: "before", note: "w: majority before the experiment" }], { writeConcern: { w: "majority" } })
JS
gap
note "2. Обидва secondary перестають забирати oplog (failpoint stopReplProducer)"
failpoint "${S[0]}" stopReplProducer alwaysOn
failpoint "${S[1]}" stopReplProducer alwaysOn
if [ "$FAILURE" = pause ]; then
    note "Старий primary після повернення зупиниться перед rollback (failpoint rollbackHangBeforeStart)"
    failpoint "$P" rollbackHangBeforeStart alwaysOn
fi
gap
note "Primary node$P отримує 5 x (\$inc likes, вставка lost-i) з { w: 1, j: true }: підтверджено, у журналі node$P, на secondary немає"
session "$P" <<'JS'
use lab4x
for (let i = 1; i <= 5; i++) { db.rb.updateOne({ _id: "counter" }, { $inc: { likes: 1 } }, { writeConcern: { w: 1, j: true } }); db.rb.insertOne({ _id: "lost-" + i }, { writeConcern: { w: 1, j: true } }) }
db.rb.find().readConcern("local")
db.rb.find().readConcern("majority")
db.rb.find({ _id: "counter" }).readConcern("linearizable").maxTimeMS(3000)
JS
observe rollback "w1 written before failure" "$P" local
observe rollback "w1 written before failure" "$P" majority
observe rollback "w1 written before failure" "$P" linearizable
gap
note "Secondary node${S[0]} відстає: має лише записане з majority"
session "${S[0]}" <<'JS'
use lab4x
db.rb.find().readPref("secondary").readConcern("local")
JS
observe rollback "w1 written before failure" "${S[0]}" local
run scripts/mongo.sh status
gap
note "3. Старий primary node$P випадає, secondary обирають новий primary без нього"
if [ "$FAILURE" = pause ]; then
    run scripts/mongo.sh pause-node "$P"
else
    run scripts/mongo.sh kill-node "$P"
fi
run scripts/mongo.sh wait-primary
N=$(primary_node)
run scripts/mongo.sh status
gap
note "Реплікацію на node${S[0]} і node${S[1]} увімкнено знову, новий primary node$N приймає записи з w: majority"
failpoint "${S[0]}" stopReplProducer off
failpoint "${S[1]}" stopReplProducer off
session <<'JS'
use lab4x
db.rb.updateOne({ _id: "counter" }, { $inc: { likes: 10 } }, { writeConcern: { w: "majority" } })
db.rb.insertOne({ _id: "after", note: "w: majority on the new primary" }, { writeConcern: { w: "majority" } })
db.rb.find()
JS
observe rollback "new primary elected" "$N" local
gap
note "4. Старий primary node$P повертається"
if [ "$FAILURE" = pause ]; then
    run scripts/mongo.sh resume-node "$P"
    # 21101: "rollback - rollbackHangBeforeStart fail point enabled"
    wait_log "$P" 21101
    run scripts/mongo.sh status
    gap
    note "node$P уже SECONDARY, але rollback ще не почався: local повертає записи, які буде відкочено, majority їх не повертає"
    session "$P" <<'JS'
use lab4x
db.hello().secondary
db.rb.find().readPref("secondary").readConcern("local")
db.rb.find().readPref("secondary").readConcern("majority")
JS
    observe rollback "old primary is SECONDARY with rollback held" "$P" local
    observe rollback "old primary is SECONDARY with rollback held" "$P" majority
    gap
    note "Відпускаємо rollback"
    failpoint "$P" rollbackHangBeforeStart off
else
    run scripts/mongo.sh start-node "$P"
fi
# 21592: "Rollback complete"
wait_log "$P" 21592
run scripts/mongo.sh wait-secondary "$P"
gap
note "5. Rollback у лозі node$P (рядки ROLLBACK і причина; атрибути скорочено)"
run rollback_log "$P"
gap
note "Файли rollback у dbPath вузла node$P"
run find "$datadir/node$P/rollback" -type f
gap
rollback_files=$(find "$datadir/node$P/rollback" -type f -name '*.bson' | sort)
for file in $rollback_files; do
    case $file in
        */local.oplog.rs/*)
            note "Відкочені записи oplog (bsondump, поля ts, op, ns, o)"
            run oplog_dump "$file"
            ;;
        *)
            note "Документи колекції в стані до rollback (bsondump)"
            run bsondump --quiet "$file"
            ;;
    esac
    gap
done
note "Після rollback: записів з w: 1 немає ніде, записи з majority (before, after, +10) є"
session <<'JS'
use lab4x
db.rb.find().readConcern("majority")
JS
session "$P" <<'JS'
use lab4x
db.rb.find().readPref("secondary")
db.rb.countDocuments({ _id: /^lost-/ })
JS
observe rollback "after rollback" "$P" local
observe rollback "after rollback" "$N" majority

if [ "$FAILURE" = pause ]; then
    L=${S[0]}
    [ "$L" != "$N" ] || L=${S[1]}
    gap
    note "6. Застаріле читання: node$L перестає застосовувати oplog (rsSyncApplyStop), primary node$N отримує \$inc з majority"
    failpoint "$L" rsSyncApplyStop alwaysOn
    session <<'JS'
use lab4x
db.rb.updateOne({ _id: "counter" }, { $inc: { likes: 1 } }, { writeConcern: { w: "majority" } })
db.rb.find({ _id: "counter" }).readConcern("linearizable")
JS
    observe stale "node lags after majority inc" "$N" linearizable
    observe stale "node lags after majority inc" "$N" local
    session "$L" <<'JS'
use lab4x
db.rb.find({ _id: "counter" }).readPref("secondary").readConcern("local")
db.rb.find({ _id: "counter" }).readPref("secondary").readConcern("majority")
db.rb.find({ _id: "counter" }).readPref("secondary").readConcern("linearizable")
JS
    observe stale "node lags after majority inc" "$L" local
    observe stale "node lags after majority inc" "$L" majority
    observe stale "node lags after majority inc" "$L" linearizable
    run scripts/mongo.sh status
    gap
    note "node$L знову застосовує oplog і наздоганяє primary"
    failpoint "$L" rsSyncApplyStop off
    run scripts/mongo.sh wait-secondary "$L"
    session "$L" <<'JS'
use lab4x
db.rb.find({ _id: "counter" }).readPref("secondary").readConcern("local")
JS
    observe stale "caught up" "$L" local
fi

for n in 1 2 3; do
    cp "$(node_log "$n")" "$OUT_DIR/$name-mongo-node$n.txt"
done
echo "wrote $out" >&2
[ "$FAILURE" = pause ] && echo "wrote $reads" >&2

if [ "$SHOTS" = true ]; then
    mkdir -p "$IMG_DIR"
    python3 "$script_dir/terminal-shot.py" "$out" "$IMG_DIR/$name.png"
    # The report shows the transcript in parts (a, b, ...) that fit an A4
    # page; the parts are runs of blank-line separated blocks.
    if [ "$FAILURE" = pause ]; then
        parts=(1-4 5-6 7-8 9-11 12-13 14-16 17-18)
    else
        parts=(1-4 5-6 7-9 10-11 12-14)
    fi
    letters=(a b c d e f g)
    for i in "${!parts[@]}"; do
        python3 "$script_dir/terminal-shot.py" "$out" "$IMG_DIR/$name-${letters[$i]}.png" \
            --width 1120 --blocks "${parts[$i]}" --title "$name.txt ($((i + 1))/${#parts[@]})"
    done
fi
