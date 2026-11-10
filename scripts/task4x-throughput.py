#!/usr/bin/env python3
"""Lab 4 throughput research: what limits the MongoDB counter and what helps.

    direnv exec . scripts/task4x-throughput.py [--repeats 3] [--increments 2000] EXPERIMENT...

Every experiment is a list of variants. The variants run interleaved (A, B,
C, A, B, C, ...) `--repeats` times, so a change in the machine's state hits
all of them alike. A variant is

  * a mongod profile: startup options for every node (MONGOD_ARGS of
    scripts/mongo.sh). When the profile changes, the replica set is stopped,
    its data deleted and a fresh one started, followed by a warm-up run that
    is logged but not recorded;
  * runtime settings applied before the run and undone after it
    (setParameter on every node, or a replica set config field);
  * a workload: mongo-bench (10 clients, one findAndModify $inc in flight
    each) or the web counter (counter-server --store mongo driven by loadgen
    with 10 clients, one request in flight each).

Each run appends one row to reports/data/task4x-throughput.csv and one row
per node to reports/data/task4x-throughput-metrics.csv: serverStatus deltas
(WiredTiger log syncs and their time, write conflicts, time waiting for the
write concern, flow control, oplog apply batches) and the CPU time of the
mongod process. The transcript goes to
reports/logs/task4x-throughput-<experiment>.txt. The `probe` experiment
instead times single sequential writes from mongosh per write concern and
writes reports/data/task4x-throughput-probe.csv.

The `perf` experiment samples the primary's user-space CPU with `perf record`
while mongo-bench runs, on one document and on 10, and sums the samples by
part of mongod: reports/logs/task4x-throughput-perf-<label>.txt and
reports/data/task4x-throughput-perf.csv.

The replica set lives in .mongodata/task4x-throughput (ports 21430-21432)
and is stopped and deleted at the end; the web counter listens on 21440.
"""
import argparse
import csv
import datetime
import json
import os
import shutil
import statistics
import subprocess
import sys
import time
import urllib.request
from dataclasses import dataclass, field

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MONGO = os.path.join(ROOT, "scripts", "mongo.sh")
BIN = os.path.join(ROOT, ".build", "release")
DATA_DIR = os.path.join(ROOT, ".mongodata", "task4x-throughput")
PORTS = [21430, 21431, 21432]
MONGO_URL = "mongodb://127.0.0.1:21430,127.0.0.1:21431,127.0.0.1:21432/?replicaSet=rs0"
SERVER_PORT = 21440
SERVER_URL = f"http://127.0.0.1:{SERVER_PORT}"
CSV_OUT = os.path.join(ROOT, "reports", "data", "task4x-throughput.csv")
METRICS_OUT = os.path.join(ROOT, "reports", "data", "task4x-throughput-metrics.csv")
PROBE_OUT = os.path.join(ROOT, "reports", "data", "task4x-throughput-probe.csv")
PERF_OUT = os.path.join(ROOT, "reports", "data", "task4x-throughput-perf.csv")
LOG_DIR = os.path.join(ROOT, "reports", "logs")

# Every profile stops in about a second instead of 15 s of quiesce mode; the
# same for all variants, so it is no difference between them.
BASE_ARGS = "--setParameter shutdownTimeoutMillisForSignaledShutdown=1000"
PROFILES = {
    "default": BASE_ARGS,
    "zero-fill": BASE_ARGS + " --wiredTigerEngineConfigString=log=(zero_fill=true)",
    "writer1": BASE_ARGS + " --setParameter replWriterThreadCount=1",
}


@dataclass
class Variant:
    label: str
    workload: str  # "bench" or "web"
    profile: str = "default"
    bench_args: list = field(default_factory=list)
    server_args: list = field(default_factory=list)
    loadgen_engine: str = "ahc"
    parameters: dict = field(default_factory=dict)  # setParameter on every node
    journal_default: bool = True  # writeConcernMajorityJournalDefault

    def settings(self):
        parts = [f"mongod={self.profile}"]
        parts += [f"{k}={v}" for k, v in self.parameters.items()]
        if not self.journal_default:
            parts.append("writeConcernMajorityJournalDefault=false (UNSAFE)")
        if self.workload == "bench":
            parts.append("mongo-bench " + " ".join(self.bench_args))
        else:
            parts.append("counter-server " + " ".join(self.server_args) + f"; loadgen --engine {self.loadgen_engine}")
        return "; ".join(parts)


def bench(label, *args, **kw):
    return Variant(label, "bench", bench_args=list(args), **kw)


def web(label, *args, loadgen="ahc", **kw):
    return Variant(label, "web", server_args=list(args), loadgen_engine=loadgen, **kw)


EXPERIMENTS = {
    # Is the single hot document the bottleneck? The same workloads on one
    # document and split over 10 (each client its own document).
    "hotdoc": [
        bench("w1", "--variant", "w1"),
        bench("w1-shards10", "--variant", "w1", "--shards", "10"),
        bench("majority", "--variant", "majority"),
        bench("majority-shards10", "--variant", "majority", "--shards", "10"),
    ],
    # storage.journal.commitIntervalMs (journalCommitInterval at runtime).
    "journal-interval": [
        bench("majority-interval100", "--variant", "majority"),
        bench("majority-interval1", "--variant", "majority", parameters={"journalCommitInterval": 1}),
        bench("majority-interval500", "--variant", "majority", parameters={"journalCommitInterval": 500}),
    ],
    # WiredTiger journal files written with zeroes when created instead of
    # fallocate'd (unwritten extents on ext4).
    "zero-fill": [
        bench("majority", "--variant", "majority"),
        bench("w1", "--variant", "w1"),
        bench("majority-zero-fill", "--variant", "majority", profile="zero-fill"),
        bench("w1-zero-fill", "--variant", "w1", profile="zero-fill"),
    ],
    # What waiting for the journal costs: an UNSAFE comparison only.
    "unsafe-journal": [
        bench("majority", "--variant", "majority"),
        bench("majority-nojournal-UNSAFE", "--variant", "majority", journal_default=False),
    ],
    # Replication: flow control, and the secondaries' oplog writer threads.
    "replication": [
        bench("majority", "--variant", "majority"),
        bench("majority-flowcontrol-off", "--variant", "majority", parameters={"enableFlowControl": False}),
        bench("majority-writer1", "--variant", "majority", profile="writer1"),
    ],
    # The web counter against mongo-bench, by server engine, load generator
    # and MongoDB client pool size.
    "client": [
        bench("bench-majority", "--variant", "majority"),
        web("web-async-ahc", "--engine", "async"),
        web("web-async-nio", "--engine", "async", loadgen="nio"),
        web("web-handler-nio", "--engine", "handler", loadgen="nio"),
        web("web-raw-nio", "--engine", "raw", loadgen="nio"),
        web("web-async-ahc-clients20", "--engine", "async", "--mongo-clients", "20"),
    ],
    # The final comparison on the full 10 x 10000: the report's setup
    # against the best one that keeps every rule, and the sharded counter
    # (it changes the data model) apart.
    "final": [
        bench("majority", "--variant", "majority"),
        bench("w1", "--variant", "w1"),
        web("web", "--engine", "async"),
        bench("majority-zero-fill", "--variant", "majority", profile="zero-fill"),
        bench("w1-zero-fill", "--variant", "w1", profile="zero-fill"),
        web("web-zero-fill", "--engine", "async", profile="zero-fill"),
        bench("majority-zero-fill-shards10", "--variant", "majority", "--shards", "10", profile="zero-fill"),
        bench("w1-zero-fill-shards10", "--variant", "w1", "--shards", "10", profile="zero-fill"),
        web("web-zero-fill-shards10", "--engine", "async", "--mongo-shards", "10", profile="zero-fill"),
    ],
}

SNAPSHOT_JS = """
const out = [];
for (const port of %s) {
  const c = new Mongo('mongodb://127.0.0.1:' + port + '/?directConnection=true');
  const s = c.getDB('admin').runCommand({ serverStatus: 1 });
  const hello = c.getDB('admin').runCommand({ hello: 1 });
  const log = s.wiredTiger.log, n = Number;
  out.push({
    state: hello.isWritablePrimary ? 'PRIMARY' : 'SECONDARY',
    log_syncs: n(log['log sync operations']), log_sync_us: n(log['log sync time duration (usecs)']),
    log_writes: n(log['log write operations']), log_bytes: n(log['log bytes written']),
    log_zero_filled: n(log['log files manually zero-filled']),
    write_conflicts: n(s.metrics.operation.writeConflicts),
    wtime_num: n(s.metrics.getLastError.wtime.num), wtime_ms: n(s.metrics.getLastError.wtime.totalMillis),
    flow_control_us: n(s.flowControl.timeAcquiringMicros), flow_control_lagged: n(s.flowControl.isLaggedCount),
    apply_batches: n(s.metrics.repl.apply.batches.num), apply_ops: n(s.metrics.repl.apply.ops),
    apply_ms: n(s.metrics.repl.apply.batches.totalMillis),
  });
  c.close();
}
print(JSON.stringify(out));
""" % json.dumps(PORTS)

transcript = None


def say(message):
    line = f"{datetime.datetime.now(datetime.timezone.utc):%H:%M:%S} {message}"
    print(line, file=sys.stderr, flush=True)
    if transcript:
        transcript.write(line + "\n")
        transcript.flush()


def run(command, env=None, check=True, quiet=False):
    """Runs a command, logs its output, returns (status, stdout)."""
    result = subprocess.run(command, env=env, capture_output=True, text=True)
    for stream in (result.stderr, result.stdout if not quiet else ""):
        for line in stream.splitlines():
            if "direnv:" not in line:
                say("  | " + line)
    if check and result.returncode != 0:
        raise RuntimeError(f"{command[0]} failed with status {result.returncode}")
    return result.returncode, result.stdout


def mongo_env(profile):
    env = dict(os.environ)
    env.update(
        MONGO_DATA_DIR=DATA_DIR,
        MONGO_LOG_DIR=os.path.join(DATA_DIR, "logs"),
        MONGOD_ARGS=PROFILES[profile],
        MONGO_PORT=str(PORTS[0]),
    )
    return env


def mongo(*args, profile="default"):
    return run([MONGO, *args], env=mongo_env(profile))


def mongosh(url, js, quiet=True):
    return run(["mongosh", url, "--quiet", "--norc", "--eval", js], quiet=quiet)[1].strip()


def node_url(port):
    return f"mongodb://127.0.0.1:{port}/?directConnection=true"


def snapshot():
    _, out = run(["mongosh", "--nodb", "--quiet", "--norc", "--eval", SNAPSHOT_JS], quiet=True)
    return json.loads(out.strip().splitlines()[-1])


def cpu_seconds():
    """utime + stime of each mongod, from /proc."""
    ticks = os.sysconf("SC_CLK_TCK")
    result = []
    for n in (1, 2, 3):
        with open(os.path.join(DATA_DIR, f"node{n}.pid")) as f:
            pid = f.read().strip()
        with open(f"/proc/{pid}/stat") as f:
            fields = f.read().rsplit(")", 1)[1].split()
        result.append((int(fields[11]) + int(fields[12])) / ticks)
    return result


current_profile = None


def ensure_profile(profile, increments):
    global current_profile
    if profile == current_profile:
        return
    say(f"== fresh replica set, mongod profile {profile}: {PROFILES[profile]}")
    stop_cluster()
    mongo("start", profile=profile)
    current_profile = profile
    # The first run on a fresh set is much slower (journal and oplog files
    # being created): a warm-up run, logged but not recorded.
    say("warm-up: mongo-bench --variant majority")
    run([os.path.join(BIN, "mongo-bench"), "--variant", "majority", "--increments", str(max(increments // 2, 500)), "--url", MONGO_URL])


def stop_cluster():
    global current_profile
    if os.path.isdir(DATA_DIR):
        mongo("stop", profile=current_profile or "default")
        shutil.rmtree(DATA_DIR)
    current_profile = None


def apply_settings(variant, undo=False):
    for name, value in variant.parameters.items():
        default = DEFAULT_PARAMETERS[name]
        target = default if undo else value
        for port in PORTS:
            mongosh(node_url(port), f"db.adminCommand({{ setParameter: 1, {name}: {json.dumps(target)} }}).ok")
        say(f"{'reset' if undo else 'set'} {name}={json.dumps(target)} on every node")
    if not variant.journal_default:
        target = "true" if undo else "false"
        mongosh(MONGO_URL, f"const c = rs.conf(); c.writeConcernMajorityJournalDefault = {target}; rs.reconfig(c).ok")
        confirmed = mongosh(MONGO_URL, "rs.conf().writeConcernMajorityJournalDefault")
        say(f"writeConcernMajorityJournalDefault is now {confirmed}")


DEFAULT_PARAMETERS = {"journalCommitInterval": 100, "enableFlowControl": True}


def run_bench(variant, increments):
    status, out = run(
        [os.path.join(BIN, "mongo-bench"), *variant.bench_args, "--increments", str(increments), "--url", MONGO_URL, "--header"],
        check=False,
    )
    lines = out.strip().splitlines()
    if len(lines) < 2:
        raise RuntimeError(f"mongo-bench printed no row (status {status})")
    row = dict(zip(lines[0].split(","), lines[1].split(",")))
    return float(row["seconds"]), float(row["ops_per_sec"]), int(row["final_value"]), int(row["expected"])


def wait_for_server(process):
    for _ in range(300):
        if process.poll() is not None:
            raise RuntimeError("counter-server exited")
        try:
            urllib.request.urlopen(SERVER_URL + "/count", timeout=1).read()
            return
        except OSError:
            time.sleep(0.1)
    raise RuntimeError("counter-server did not come up")


def run_web(variant, increments):
    server_log = open(os.path.join(DATA_DIR, "logs", "counter-server.log"), "a")
    server = subprocess.Popen(
        [os.path.join(BIN, "counter-server"), "--store", "mongo", "--port", str(SERVER_PORT), "--mongo-url", MONGO_URL, *variant.server_args],
        stdout=server_log,
        stderr=server_log,
    )
    try:
        wait_for_server(server)
        status, out = run(
            [os.path.join(BIN, "loadgen"), "--header", "--clients", "10", "--requests", str(increments),
             "--url", SERVER_URL, "--store-label", "mongo", "--engine", variant.loadgen_engine],
            check=False,
        )
    finally:
        server.terminate()
        server.wait()
        server_log.close()
    lines = out.strip().splitlines()
    if len(lines) < 2:
        raise RuntimeError(f"loadgen printed no row (status {status})")
    row = dict(zip(lines[0].split(","), lines[1].split(",")))
    return float(row["seconds"]), float(row["rps"]), int(row["final_value"]), int(row["expected"])


def append_csv(path, header, row):
    new = not os.path.exists(path)
    with open(path, "a", newline="") as f:
        writer = csv.writer(f)
        if new:
            writer.writerow(header)
        writer.writerow(row)


def measure(experiment, variant, repeat, increments):
    ensure_profile(variant.profile, increments)
    apply_settings(variant)
    try:
        say(f"-- {experiment} / {variant.label}, run {repeat}: {variant.settings()}")
        load = os.getloadavg()[0]
        started = datetime.datetime.now(datetime.timezone.utc)
        before, cpu_before = snapshot(), cpu_seconds()
        if variant.workload == "bench":
            seconds, rate, final, expected = run_bench(variant, increments)
        else:
            seconds, rate, final, expected = run_web(variant, increments)
        after, cpu_after = snapshot(), cpu_seconds()
    finally:
        apply_settings(variant, undo=True)
    correct = final == expected
    say(f"   {variant.label}: {seconds:.3f} s, {rate:.1f} ops/s, final {final} of {expected}" + ("" if correct else "  WRONG"))
    append_csv(
        CSV_OUT,
        ["experiment", "label", "settings", "run", "seconds", "ops_per_s", "final_value", "correct",
         "expected", "clients", "increments_per_client", "loadavg_1m", "started_utc"],
        [experiment, variant.label, variant.settings(), repeat, f"{seconds:.4f}", f"{rate:.1f}", final,
         str(correct).lower(), expected, 10, increments, f"{load:.2f}", started.strftime("%Y-%m-%dT%H:%M:%SZ")],
    )
    for index, (a, b) in enumerate(zip(before, after)):
        d = {k: b[k] - a[k] for k in a if k != "state"}
        syncs = d["log_syncs"]
        cpu = cpu_after[index] - cpu_before[index]
        append_csv(
            METRICS_OUT,
            ["experiment", "label", "run", "node", "state", "log_syncs", "log_sync_ms_mean", "log_syncs_per_op",
             "log_writes", "log_bytes_per_op", "write_conflicts", "write_conflicts_per_op", "wtime_ms_mean",
             "flow_control_us", "flow_control_lagged", "apply_batches", "apply_ops_per_batch", "cpu_seconds", "cpu_percent"],
            [experiment, variant.label, repeat, f"node{index + 1}", b["state"], syncs,
             f"{d['log_sync_us'] / syncs / 1000:.3f}" if syncs else "", f"{syncs / expected:.3f}",
             d["log_writes"], f"{d['log_bytes'] / expected:.0f}", d["write_conflicts"],
             f"{d['write_conflicts'] / expected:.3f}",
             f"{d['wtime_ms'] / d['wtime_num']:.3f}" if d["wtime_num"] else "",
             d["flow_control_us"], d["flow_control_lagged"], d["apply_batches"],
             f"{d['apply_ops'] / d['apply_batches']:.2f}" if d["apply_batches"] else "",
             f"{cpu:.2f}", f"{100 * cpu / seconds:.1f}"],
        )
        say(f"   node{index + 1} {b['state']:9} syncs {syncs} ({syncs / expected:.3f}/op, "
            f"{d['log_sync_us'] / max(syncs, 1) / 1000:.2f} ms each), write conflicts {d['write_conflicts']}, "
            f"wtime {d['wtime_ms'] / max(d['wtime_num'], 1):.2f} ms, flow control {d['flow_control_us']} us, "
            f"apply {d['apply_ops'] / max(d['apply_batches'], 1):.2f} ops/batch, cpu {cpu:.2f} s")
    return correct


PROBE_JS = """
const coll = db.getSiblingDB('hls').probe, n = %d;
const concerns = { 'w1': { w: 1 }, 'w1-j': { w: 1, j: true }, 'majority': { w: 'majority' } };
coll.updateOne({ _id: 'p' }, { $set: { v: 0 } }, { upsert: true, writeConcern: { w: 'majority' } });
const out = {};
for (const [name, wc] of Object.entries(concerns)) {
  const t = [];
  for (let i = 0; i < n; i++) {
    const s = process.hrtime.bigint();
    db.getSiblingDB('hls').runCommand({ findAndModify: 'probe', query: { _id: 'p' }, update: { $inc: { v: 1 } }, new: true, writeConcern: wc });
    t.push(Number(process.hrtime.bigint() - s) / 1e6);
  }
  t.sort((a, b) => a - b);
  out[name] = { mean: t.reduce((a, b) => a + b, 0) / n, p50: t[Math.floor(n / 2)], p99: t[Math.floor(n * 0.99)] };
}
print(JSON.stringify(out));
"""


def probe(repeats, count):
    """One client, sequential findAndModify $inc per write concern (no
    concurrency, so no group commit): what one increment waits for."""
    for repeat in range(1, repeats + 1):
        for profile in ("default", "zero-fill"):
            ensure_profile(profile, 2000)
            out = json.loads(mongosh(MONGO_URL, PROBE_JS % count).splitlines()[-1])
            for concern, stats in out.items():
                say(f"   probe {profile} {concern}: mean {stats['mean']:.3f} ms, p50 {stats['p50']:.3f}, p99 {stats['p99']:.3f}")
                append_csv(
                    PROBE_OUT,
                    ["profile", "write_concern", "run", "count", "mean_ms", "p50_ms", "p99_ms", "started_utc"],
                    [profile, concern, repeat, count, f"{stats['mean']:.3f}", f"{stats['p50']:.3f}", f"{stats['p99']:.3f}",
                     datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")],
                )


# Where a sample of the primary falls, by shared object and symbol.
PERF_PARTS = [
    ("C++ exception unwinding", lambda dso, sym: dso.startswith("libgcc_s") or any(
        k in sym for k in ("_Unwind", "__gxx_personality", "read_encoded_value", "base_of_encoded_value", "__cxa_", "execute_cfa_program", "uw_"))),
    ("tcmalloc", lambda dso, sym: "tcmalloc" in sym or sym.startswith(("operator new", "operator delete"))),
    ("WiredTiger", lambda dso, sym: sym.startswith("__wt_") or sym.startswith("__curfile") or sym.startswith("__config") or sym.startswith("__txn")),
    ("snappy (journal compression)", lambda dso, sym: "snappy::" in sym),
    ("libc", lambda dso, sym: dso.startswith("libc.so")),
    ("other mongod", lambda dso, sym: dso == "mongod"),
    ("other", lambda dso, sym: True),
]

PERF_VARIANTS = [
    bench("w1", "--variant", "w1"),
    bench("w1-shards10", "--variant", "w1", "--shards", "10"),
    bench("majority", "--variant", "majority"),
    bench("majority-shards10", "--variant", "majority", "--shards", "10"),
]


def perf_profile(seconds=6):
    """perf record (user space; perf_event_paranoid 2 allows no more) on the
    primary while mongo-bench runs, then the samples summed by part."""
    ensure_profile("default", 2000)
    primary = mongosh(MONGO_URL, "db.hello().primary").splitlines()[-1]
    with open(os.path.join(DATA_DIR, f"node{PORTS.index(int(primary.split(':')[1])) + 1}.pid")) as f:
        pid = f.read().strip()
    for variant in PERF_VARIANTS:
        increments = 10000 if "w1" in variant.label else 3000
        say(f"-- perf / {variant.label}: primary {primary} (pid {pid}), {seconds} s of cpu-clock:u while "
            f"mongo-bench {' '.join(variant.bench_args)} --increments {increments} runs")
        data = os.path.join(DATA_DIR, f"perf-{variant.label}.data")
        load = subprocess.Popen([os.path.join(BIN, "mongo-bench"), *variant.bench_args, "--increments", str(increments),
                                 "--url", MONGO_URL], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
        time.sleep(2)
        recorded = subprocess.run(["perf", "record", "-e", "cpu-clock:u", "-F", "999", "-o", data, "-p", pid, "--", "sleep", str(seconds)],
                                  capture_output=True, text=True, check=True)
        samples = int(recorded.stderr.rsplit("(", 1)[1].split()[0])
        row = load.communicate()[0].strip()
        rate = float(row.split(",")[9])
        # cpu-clock at 999 Hz: one sample is about 1/999 s of CPU time.
        cores = samples / 999 / seconds
        say(f"  | {row}\n   {samples} samples: {cores:.2f} cores of user CPU, {cores / rate * 1e6:.0f} us per increment")
        _, report = run(["perf", "report", "-i", data, "--stdio", "--no-children", "--sort", "dso,sym"], quiet=True)
        parts = {name: 0.0 for name, _ in PERF_PARTS}
        top = []
        for line in report.splitlines():
            fields = line.split(None, 3)
            if len(fields) < 4 or not fields[0].endswith("%") or fields[2] != "[.]":
                continue
            percent, dso, sym = float(fields[0][:-1]), fields[1], fields[3]
            parts[next(name for name, match in PERF_PARTS if match(dso, sym))] += percent
            if len(top) < 30:
                top.append(f"{percent:6.2f}%  {dso:14} {sym[:150]}")
        with open(os.path.join(LOG_DIR, f"task4x-throughput-perf-{variant.label}.txt"), "w") as f:
            f.write(f"perf record -e cpu-clock:u -F 999 -p <primary mongod> -- sleep {seconds}\n")
            f.write(f"while: mongo-bench {' '.join(variant.bench_args)} --increments {increments}\n")
            f.write(f"mongo-bench row: {row}\n")
            f.write(f"{samples} samples: {cores:.2f} cores of user CPU, {cores / rate * 1e6:.0f} us of user CPU per increment\n")
            f.write("\nsamples by part of mongod:\n")
            for name, percent in parts.items():
                f.write(f"{percent:6.2f}%  {name}\n")
            f.write("\ntop 30 symbols (perf report --no-children --sort dso,sym):\n" + "\n".join(top) + "\n")
        for name, percent in parts.items():
            say(f"   {percent:6.2f}%  {name}")
            append_csv(PERF_OUT, ["label", "ops_per_s", "samples", "user_cpu_cores", "user_cpu_us_per_op", "part", "percent_of_user_samples"],
                       [variant.label, rate, samples, f"{cores:.3f}", f"{cores / rate * 1e6:.0f}", name, f"{percent:.2f}"])
        os.remove(data)


def main():
    global transcript
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("experiments", nargs="+", choices=[*EXPERIMENTS, "probe", "perf"])
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--increments", type=int, default=2000, help="per client (the task: 10000)")
    parser.add_argument("--probe-count", type=int, default=1000)
    args = parser.parse_args()
    for tool in ("mongod", "mongosh"):
        if not shutil.which(tool):
            sys.exit(f"{tool} is not on PATH: run under `direnv exec .`")
    os.makedirs(os.path.dirname(CSV_OUT), exist_ok=True)

    failures = 0
    try:
        for experiment in args.experiments:
            transcript = open(os.path.join(LOG_DIR, f"task4x-throughput-{experiment}.txt"), "a")
            plan = "" if experiment == "perf" else f", {args.repeats} interleaved repeats, 10 x {args.increments}"
            say(f"=== experiment {experiment}{plan}; "
                f"{subprocess.run(['mongod', '--version'], capture_output=True, text=True).stdout.splitlines()[0]}")
            if experiment == "probe":
                probe(args.repeats, args.probe_count)
            elif experiment == "perf":
                perf_profile()
            else:
                for repeat in range(1, args.repeats + 1):
                    for variant in EXPERIMENTS[experiment]:
                        if not measure(experiment, variant, repeat, args.increments):
                            failures += 1
            transcript.close()
            transcript = None
    finally:
        stop_cluster()
    if failures:
        sys.exit(f"{failures} run(s) ended with a wrong counter")


if __name__ == "__main__":
    main()
