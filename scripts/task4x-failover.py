#!/usr/bin/env python3
"""Lab 4 failover research: a clean step-down, and how electionTimeoutMillis
changes the pause after a sudden primary failure.

    direnv exec . scripts/task4x-failover.py [--repeats 3] [--increments 10000] EXPERIMENT...

Experiments (the variants of each run interleaved, A, B, A, B, ...):

  stepdown   mongo-bench --step-down-after (replSetStepDown on the primary)
             with w1 and with majority. A stepped-down node may not run for
             primary for 60 s, so every run starts at least STEP_DOWN_SECS
             after the previous step-down.
  election   majority runs with --kill-primary-after (SIGKILL) at
             electionTimeoutMillis 10000, 2000 and 1000 (rs.reconfig through
             MONGO_ELECTION_TIMEOUT_MS of scripts/mongo.sh).
  pause      no load: the primary is frozen with SIGSTOP for 1.5 s or 3 s and
             thawed, at each election timeout; was there an election? Then
             60 s of an idle set at each timeout: any election at all?
  stepdown-kill
             one run at 2000 ms: two step-downs in a row (replSetStepDown, 60
             s each), then mongo-bench kills the third primary while both
             other nodes still may not run for primary.

Each mongo-bench run appends a row to reports/data/task4x-failover.csv: the
mongo-bench row, the longest pause between acknowledgements around the
failure (from --latency-out), and from the mongod logs the times from the
failure (the SIGKILL, or sending replSetStepDown) to the first DOWN mark, the
start of the election, its success and the moment the new primary accepts
writes. `pause` writes reports/data/task4x-failover-pause.csv. Transcripts
with the mongod log lines of every failure: reports/logs/task4x-failover-
<experiment>.txt.

--bench and --tag run another mongo-bench binary with its labels suffixed
(for a build changed by hand, see the notes).

The replica set lives in .mongodata/task4x-failover (ports 21430-21432), its
logs in .data/task4x-failover-logs; the set is stopped and its data deleted
at the end.
"""
import argparse
import csv
import datetime
import json
import os
import re
import shutil
import statistics
import subprocess
import sys
import threading
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MONGO = os.path.join(ROOT, "scripts", "mongo.sh")
DATA_DIR = os.path.join(ROOT, ".mongodata", "task4x-failover")
MONGO_LOG_DIR = os.path.join(ROOT, ".data", "task4x-failover-logs")
HOSTS = ["127.0.0.1:21430", "127.0.0.1:21431", "127.0.0.1:21432"]
MONGO_URL = "mongodb://" + ",".join(HOSTS) + "/?replicaSet=rs0"
CSV_OUT = os.path.join(ROOT, "reports", "data", "task4x-failover.csv")
PAUSE_OUT = os.path.join(ROOT, "reports", "data", "task4x-failover-pause.csv")
LOG_DIR = os.path.join(ROOT, "reports", "logs")
STEP_DOWN_SECS = 62  # replSetStepDown's 60 s, and a margin
TIMEOUTS = [10000, 2000, 1000]

ENV = dict(os.environ, MONGO_DATA_DIR=DATA_DIR, MONGO_LOG_DIR=MONGO_LOG_DIR)

# mongod log ids kept in the transcripts (component ELECTION or REPL).
EVENT_IDS = {
    21579: "stepdown-command",  # Attempting to step down in response to replSetStepDown
    962660: "stepdown-done",  # Stepdown succeeded
    21347: "handoff",  # Handing off election
    21581: "stepup-request",  # Received replSetStepUp request
    4615661: "election-start",  # Starting an election due to step up request
    4615652: "election-start",  # Starting an election, since we've seen no PRIMARY in election timeout period
    4615655: "not-electable",  # Not starting an election, since we are not electable
    21438: "dry-run",  # Conducting a dry run election
    21444: "dry-run-ok",  # Dry election run succeeded, running for election
    21450: "elected",  # Election succeeded, assuming primary role
    21331: "writable",  # Transition to primary complete; database writes are now permitted
    21358: "state",  # Replica set state transition
    21216: "down",  # Member is now in state DOWN
}
FIRST_PER_NODE = {"not-electable"}

CSV_FIELDS = [
    "experiment", "label", "run", "variant", "failure", "election_timeout_ms", "workers", "increments_per_worker",
    "failure_at_s", "old_primary", "new_primary", "term_after", "seconds", "ops_per_s", "final_value", "expected",
    "correct", "acknowledged", "lost_acknowledged", "retries", "retry_kinds", "errors", "elections", "failover_s", "failure_to_ack_s",
    "ack_gap_s", "down_s", "election_start_s", "elected_s", "writable_s", "stepdown_reply_s", "not_electable_lines",
    "loadavg_1m", "started_utc",
]
PAUSE_FIELDS = [
    "kind", "election_timeout_ms", "pause_s", "run", "primary_before", "primary_after", "term_before", "term_after",
    "elections", "election_start_s", "elected_s", "loadavg_1m", "started_utc",
]


def log(text, transcript=None):
    print(text, file=sys.stderr, flush=True)
    if transcript:
        transcript.write(text + "\n")
        transcript.flush()


def utc(ts):
    return datetime.datetime.fromtimestamp(ts, datetime.timezone.utc).strftime("%H:%M:%S.%f")[:-3]


def mongo(*args, timeout_ms=None, check=True):
    env = dict(ENV)
    if timeout_ms is not None:
        env["MONGO_ELECTION_TIMEOUT_MS"] = str(timeout_ms)
    result = subprocess.run([MONGO, *args], env=env, capture_output=True, text=True)
    output = (result.stdout + result.stderr).strip()
    if check and result.returncode != 0:
        raise RuntimeError(f"mongo.sh {' '.join(args)} failed:\n{output}")
    return output


def mongosh(js, url=MONGO_URL):
    result = subprocess.run(["mongosh", url, "--quiet", "--norc", "--eval", js], capture_output=True, text=True, check=True)
    return result.stdout.strip()


def status():
    """(primary node number or None, term), seen from any node."""
    js = """
    for (const host of %s) {
        try {
            const c = new Mongo('mongodb://' + host + '/?directConnection=true&serverSelectionTimeoutMS=500');
            const s = c.getDB('admin').runCommand({ replSetGetStatus: 1 });
            if (!s.ok) continue;
            const p = s.members.find(m => m.stateStr === 'PRIMARY');
            print(JSON.stringify({ primary: p ? p.name : null, term: Number(s.term) }));
            quit(0);
        } catch (e) {}
    }
    print(JSON.stringify({ primary: null, term: null }));
    """ % json.dumps(HOSTS)
    reply = json.loads(mongosh(js, url="--nodb").splitlines()[-1])
    primary = HOSTS.index(reply["primary"]) + 1 if reply["primary"] else None
    return primary, reply["term"]


def log_lengths():
    lengths = {}
    for n in (1, 2, 3):
        with open(os.path.join(MONGO_LOG_DIR, f"mongo-node{n}.log"), "rb") as f:
            lengths[n] = sum(1 for _ in f)
    return lengths


def log_events(since_lines):
    """Kept mongod log events after the given line counts: (unix time, node, kind, id, msg, attr)."""
    events = []
    for n, start in since_lines.items():
        seen = set()
        with open(os.path.join(MONGO_LOG_DIR, f"mongo-node{n}.log"), encoding="utf-8", errors="replace") as f:
            for number, line in enumerate(f):
                if number < start:
                    continue
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    continue
                kind = EVENT_IDS.get(entry.get("id"))
                if not kind:
                    continue
                if kind in FIRST_PER_NODE:
                    if kind in seen:
                        continue
                    seen.add(kind)
                stamp = datetime.datetime.fromisoformat(entry["t"]["$date"].replace("Z", "+00:00")).timestamp()
                events.append((stamp, n, kind, entry["id"], entry["msg"], entry.get("attr", {})))
    events.sort(key=lambda e: (e[0], e[1]))
    return events


def count_not_electable(since_lines):
    count = 0
    for n, start in since_lines.items():
        with open(os.path.join(MONGO_LOG_DIR, f"mongo-node{n}.log"), encoding="utf-8", errors="replace") as f:
            for number, line in enumerate(f):
                if number >= start and '"id":4615655' in line:
                    count += 1
    return count


def render_events(events, t0):
    lines = []
    for stamp, node, kind, _id, msg, attr in events:
        text = json.dumps(attr, ensure_ascii=False, separators=(",", ":"))
        if len(text) > 160:
            text = text[:157] + "..."
        lines.append(f"  {utc(stamp)} {stamp - t0:+8.3f} s  node{node}  {msg}  {text}")
    return "\n".join(lines)


def first(events, kind, after, node=None, exclude=None):
    for stamp, n, k, _id, _msg, attr in events:
        if k == kind and stamp >= after - 0.001 and (node is None or n == node) and (exclude is None or n != exclude):
            if kind == "state" and attr.get("newState") != "PRIMARY":
                continue
            return stamp, n, attr
    return None


def loadavg():
    return f"{os.getloadavg()[0]:.2f}"


class Bench:
    def __init__(self, binary, increments, transcript):
        self.binary = binary
        self.increments = increments
        self.transcript = transcript
        self.last_step_down = 0.0

    def wait_step_down_period(self):
        left = self.last_step_down + STEP_DOWN_SECS - time.time()
        if left > 0:
            log(f"(waiting {left:.0f} s until no node is in its 60 s step-down period)", self.transcript)
            time.sleep(left)

    def run(self, experiment, label, run, variant, failure, failure_after, timeout_ms, after_step_down_period=True):
        if after_step_down_period:
            self.wait_step_down_period()
        log(mongo("start", timeout_ms=timeout_ms), self.transcript)
        primary_before, term_before = status()
        since = log_lengths()
        latency_file = os.path.join(MONGO_LOG_DIR, "latency.csv")
        flag = {"kill": "--kill-primary-after", "stepdown": "--step-down-after"}[failure]
        command = [self.binary, "--header", "--variant", variant, "--workers", "10", "--increments", str(self.increments),
                   "--url", MONGO_URL, "--mongo-script", MONGO, "--latency-out", latency_file, flag, str(failure_after)]
        started = time.time()
        log(f"\n== {experiment} {label} run {run}: electionTimeoutMillis {timeout_ms}, primary node{primary_before}, "
            f"term {term_before}, load {loadavg()}, {datetime.datetime.now(datetime.timezone.utc):%Y-%m-%d %H:%M:%S} UTC", self.transcript)
        log("$ mongo-bench " + " ".join(command[1:]).replace(ROOT + "/", ""), self.transcript)
        process = subprocess.Popen(command, env=ENV, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        stderr_lines = []

        def read_stderr():
            for line in process.stderr:
                stderr_lines.append((time.time(), line.rstrip("\n")))

        reader = threading.Thread(target=read_stderr)
        reader.start()
        stdout = process.stdout.read()
        process.wait()
        reader.join()
        for stamp, line in stderr_lines:
            log(f"  [{utc(stamp)}] {line}", self.transcript)
        log(stdout.rstrip(), self.transcript)
        if process.returncode != 0:
            log(f"mongo-bench exited with status {process.returncode}", self.transcript)
        header, row = stdout.strip().splitlines()[-2:]
        bench = dict(zip(header.split(","), row.split(",")))

        # The failure's wall-clock time: the SIGKILL as mongo.sh reports
        # it, or when mongo-bench announced the replSetStepDown it sends next.
        t0 = None
        retry_kinds, reply, killed_reported = [], "", None
        for stamp, line in stderr_lines:
            if failure == "kill" and (m := re.search(r"SIGKILL at (\d\d:\d\d:\d\d\.\d{3}) UTC", line)):
                day = datetime.datetime.fromtimestamp(stamp, datetime.timezone.utc).date()
                t0 = datetime.datetime.fromisoformat(f"{day}T{m.group(1)}+00:00").timestamp()
                killed_reported = stamp
            if failure == "stepdown" and "stepping down the primary" in line:
                t0 = stamp
            if m := re.search(r"replied after ([\d.]+) s", line):
                reply = m.group(1)
            if m := re.search(r"retried (\d+)x after (.*)", line):
                retry_kinds.append(f"{m.group(2)}: {m.group(1)}")
        if failure == "stepdown":
            self.last_step_down = t0 or time.time()

        events = log_events(since)
        old = int(bench["killed_node"]) if bench.get("killed_node") else None
        row = {
            "experiment": experiment, "label": label, "run": run, "variant": variant, "failure": failure,
            "election_timeout_ms": timeout_ms, "workers": bench["workers"], "increments_per_worker": bench["increments_per_worker"],
            "failure_at_s": bench["kill_at_seconds"], "old_primary": old or "", "seconds": bench["seconds"],
            "ops_per_s": bench["ops_per_sec"], "final_value": bench["final_value"], "expected": bench["expected"],
            "correct": bench["final_value"] == bench["expected"], "acknowledged": bench["acknowledged"],
            "lost_acknowledged": bench["lost_acknowledged"], "retries": bench["retries"], "retry_kinds": "; ".join(retry_kinds),
            "errors": bench["errors"], "elections": bench["elections"], "failover_s": bench["failover_seconds"],
            "ack_gap_s": ack_gap(latency_file, bench), "stepdown_reply_s": reply,
            "not_electable_lines": count_not_electable(since), "loadavg_1m": loadavg(),
            "started_utc": datetime.datetime.fromtimestamp(started, datetime.timezone.utc).isoformat(timespec="seconds"),
        }
        if t0 is not None:
            down = first(events, "down", t0)
            start = first(events, "election-start", t0, exclude=old)
            elected = first(events, "elected", t0, exclude=old)
            writable = first(events, "writable", t0, exclude=old)
            # mongo-bench times its failover from when kill-node returned
            # (it polls every 0.1 s for the process to be gone), about when
            # the driver read kill-node's line; from the step-down command
            # it sends right after its line.
            if bench["failover_seconds"]:
                late = killed_reported - t0 if failure == "kill" else 0.0
                row["failure_to_ack_s"] = f"{float(bench['failover_seconds']) + late:.3f}"
            row.update({
                "down_s": f"{down[0] - t0:.3f}" if down else "",
                "election_start_s": f"{start[0] - t0:.3f}" if start else "",
                "elected_s": f"{elected[0] - t0:.3f}" if elected else "",
                "writable_s": f"{writable[0] - t0:.3f}" if writable else "",
                "new_primary": elected[1] if elected else "",
                "term_after": elected[2].get("term", "") if elected else "",
            })
            log(f"mongod log events, times from the {'SIGKILL' if failure == 'kill' else 'replSetStepDown'} at {utc(t0)} UTC:", self.transcript)
            # The events of the failover only, not those of the restart.
            until = (writable[0] if writable else t0 + 60) + 1
            log(render_events([e for e in events if e[0] <= until], t0), self.transcript)
        log("summary: " + ", ".join(f"{k} {row.get(k, '')}" for k in
            ("failover_s", "failure_to_ack_s", "ack_gap_s", "down_s", "election_start_s", "elected_s", "writable_s", "correct", "retries", "elections")), self.transcript)
        os.remove(latency_file)
        append_csv(CSV_OUT, CSV_FIELDS, row)
        return row


def ack_gap(path, bench):
    """The longest pause between two consecutive acknowledgements that ends
    at the first acknowledgement after a retry following the failure (or,
    without one, the longest pause after the failure)."""
    at = float(bench["kill_at_seconds"] or 0)
    completed, first_retry = [], None
    with open(path) as f:
        for record in csv.DictReader(f):
            c = float(record["completed_seconds"])
            completed.append(c)
            if first_retry is None and c > at and int(record["retries"]) > 0:
                first_retry = c
    completed.sort()
    end = first_retry if first_retry is not None else completed[-1]
    gaps = [b - a for a, b in zip(completed, completed[1:]) if b > at - 1 and b <= end]
    return f"{max(gaps):.3f}" if gaps else ""


def append_csv(path, fields, row):
    new = not os.path.exists(path)
    with open(path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        if new:
            writer.writeheader()
        writer.writerow({k: row.get(k, "") for k in fields})


def experiment_stepdown(bench, repeats):
    for run in range(1, repeats + 1):
        for variant, after in (("w1", 5), ("majority", 30)):
            bench.run("stepdown", variant + bench_tag, run, variant, "stepdown", after, 10000)


def experiment_election(bench, repeats):
    for run in range(1, repeats + 1):
        for timeout in TIMEOUTS:
            bench.run("election", f"timeout-{timeout}{bench_tag}", run, "majority", "kill", 30, timeout)


def experiment_stepdown_kill(bench, repeats):
    transcript = bench.transcript
    bench.wait_step_down_period()
    log(mongo("start", timeout_ms=2000), transcript)
    for _ in range(2):
        primary, term = status()
        log(f"\n$ db.adminCommand({{ replSetStepDown: 60 }}) on node{primary} (term {term}) at {utc(time.time())} UTC", transcript)
        mongosh("db.adminCommand({ replSetStepDown: 60 }).ok", url=f"mongodb://{HOSTS[primary - 1]}/?directConnection=true")
        bench.last_step_down = time.time()
        log(mongo("wait-primary"), transcript)
    # The run starts inside both step-down periods on purpose.
    bench.run("stepdown-kill", "timeout-2000", 1, "majority", "kill", 3, 2000, after_step_down_period=False)


def experiment_pause(transcript, repeats):
    for run in range(1, repeats + 1):
        for timeout in TIMEOUTS:
            for pause in (1.5, 3.0):
                log(mongo("start", timeout_ms=timeout), transcript)
                primary, term = status()
                since = log_lengths()
                started = time.time()
                log(f"\n== pause {pause} s, electionTimeoutMillis {timeout}, run {run}: primary node{primary}, term {term}, "
                    f"load {loadavg()}", transcript)
                log(mongo("pause-node", str(primary)), transcript)
                t0 = time.time()
                time.sleep(pause)
                log(mongo("resume-node", str(primary)), transcript)
                time.sleep(4)
                log(mongo("wait-primary"), transcript)
                for n in (1, 2, 3):
                    if status()[0] != n:
                        log(mongo("wait-secondary", str(n)), transcript)
                after_primary, after_term = status()
                events = log_events(since)
                start = first(events, "election-start", t0, exclude=primary)
                elected = first(events, "elected", t0, exclude=primary)
                if events:
                    log(f"mongod log events, times from the SIGSTOP at {utc(t0)} UTC:", transcript)
                    log(render_events([e for e in events if e[2] != "down"], t0), transcript)
                row = {
                    "kind": "pause", "election_timeout_ms": timeout, "pause_s": pause, "run": run,
                    "primary_before": primary, "primary_after": after_primary, "term_before": term, "term_after": after_term,
                    "elections": after_term - term, "election_start_s": f"{start[0] - t0:.3f}" if start else "",
                    "elected_s": f"{elected[0] - t0:.3f}" if elected else "", "loadavg_1m": loadavg(),
                    "started_utc": datetime.datetime.fromtimestamp(started, datetime.timezone.utc).isoformat(timespec="seconds"),
                }
                log("summary: " + ", ".join(f"{k} {row[k]}" for k in ("primary_after", "elections", "election_start_s")), transcript)
                append_csv(PAUSE_OUT, PAUSE_FIELDS, row)
    for timeout in TIMEOUTS:
        log(mongo("start", timeout_ms=timeout), transcript)
        primary, term = status()
        started = time.time()
        log(f"\n== idle 60 s, electionTimeoutMillis {timeout}: primary node{primary}, term {term}, load {loadavg()}", transcript)
        time.sleep(60)
        after_primary, after_term = status()
        row = {
            "kind": "idle-60s", "election_timeout_ms": timeout, "pause_s": "", "run": 1, "primary_before": primary,
            "primary_after": after_primary, "term_before": term, "term_after": after_term, "elections": after_term - term,
            "loadavg_1m": loadavg(),
            "started_utc": datetime.datetime.fromtimestamp(started, datetime.timezone.utc).isoformat(timespec="seconds"),
        }
        log("summary: " + ", ".join(f"{k} {row[k]}" for k in ("primary_after", "elections")), transcript)
        append_csv(PAUSE_OUT, PAUSE_FIELDS, row)


bench_tag = ""


def main():
    global bench_tag
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("experiments", nargs="+", choices=["stepdown", "election", "pause", "stepdown-kill"])
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--increments", type=int, default=10000)
    parser.add_argument("--bench", default=os.path.join(ROOT, ".build", "release", "mongo-bench"))
    parser.add_argument("--tag", default="", help="suffix of the labels and the transcript name")
    args = parser.parse_args()
    bench_tag = args.tag

    if args.bench == os.path.join(ROOT, ".build", "release", "mongo-bench"):
        subprocess.run(["swift", "build", "-c", "release", "--product", "mongo-bench"], cwd=ROOT, check=True,
                       stdout=subprocess.DEVNULL)
    mongo("stop", check=False)
    shutil.rmtree(DATA_DIR, ignore_errors=True)
    shutil.rmtree(MONGO_LOG_DIR, ignore_errors=True)
    os.makedirs(MONGO_LOG_DIR)
    try:
        for experiment in args.experiments:
            path = os.path.join(LOG_DIR, f"task4x-failover-{experiment}{args.tag}.txt")
            with open(path, "w") as transcript:
                log(f"Task 4x, failover: {experiment}{args.tag}; mongod {mongod_version()}, "
                    f"{datetime.datetime.now(datetime.timezone.utc):%Y-%m-%d %H:%M} UTC", transcript)
                log(mongo("start"), transcript)
                log(mongosh("JSON.stringify(rs.conf().settings)"), transcript)
                bench = Bench(args.bench, args.increments, transcript)
                if experiment == "stepdown":
                    experiment_stepdown(bench, args.repeats)
                elif experiment == "election":
                    experiment_election(bench, args.repeats)
                elif experiment == "stepdown-kill":
                    experiment_stepdown_kill(bench, args.repeats)
                elif experiment == "pause":
                    experiment_pause(transcript, args.repeats)
                # Back to the default for the next experiment.
                log(mongo("start", timeout_ms=10000), transcript)
                if experiment in ("stepdown", "stepdown-kill"):
                    bench.wait_step_down_period()
    finally:
        mongo("stop", check=False)
        shutil.rmtree(DATA_DIR, ignore_errors=True)


def mongod_version():
    result = subprocess.run(["mongod", "--version"], capture_output=True, text=True)
    return result.stdout.split("\n")[0].replace("db version ", "")


if __name__ == "__main__":
    main()
