import ArgumentParser
import CommandLineSupport
import CounterCore

extension MongoWriteConcern: ExpressibleByArgument {}

@main
struct MongoBenchCommand: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "mongo-bench",
        abstract: "Increments one MongoDB counter from concurrent clients and prints one CSV row.",
        discussion: """
            Resets the likes counter to 0 (w: majority), connects --workers \
            separate clients (each with its own connections and logical \
            session), then times workers x findOneAndUpdate({ $inc }) with \
            the write concern of --variant. The increments are retryable \
            writes (lsid + txnNumber), so an increment caught by a failover \
            is sent again and applied once. With --kill-primary-after the \
            current primary gets SIGKILL that many seconds into the run \
            (scripts/mongo.sh kill-node) and is started again once the \
            workers are done; the final value is read with readConcern \
            majority after it has rejoined and caught up. With \
            --step-down-after the primary is instead told to step down \
            (replSetStepDown) and stays up as a secondary; the CSV row then \
            has kill = stepdown. \
            Exits with status 2 if a run that must be exact (majority, or \
            any run without a kill or step-down) is not.
            """
    )

    @Option(help: "Write concern of the increments: \(MongoWriteConcern.allValueStrings.joined(separator: "|")).")
    var variant: MongoWriteConcern

    @Option(help: "Concurrent clients, each with its own connections and session.")
    var workers = 10

    @Option(help: "Increments per worker.")
    var increments = 10_000

    @Option(help: "Documents the counter is split over (its value is their sum); worker i increments document i % shards. 1 is the task's single document.")
    var shards = 1

    @Option(name: .customLong("kill-primary-after"), help: "Seconds into the run to SIGKILL the primary (default: no kill).")
    var killPrimaryAfter: Double?

    @Option(name: .customLong("step-down-after"), help: "Seconds into the run to make the primary step down with replSetStepDown (default: no step-down).")
    var stepDownAfter: Double?

    @Option(name: .customLong("mongo-script"), help: "scripts/mongo.sh, used to kill, restart and wait for the node.")
    var mongoScript = "scripts/mongo.sh"

    @Option(help: "Replica set URL; node N of scripts/mongo.sh must be its Nth host.")
    var url = "mongodb://127.0.0.1:21430,127.0.0.1:21431,127.0.0.1:21432/?replicaSet=rs0"

    @Flag(name: .customLong("no-retryable-writes"), help: "Resend increments after errors without lsid/txnNumber, so the server cannot recognise a resend (a demonstration of double counting).")
    var noRetryableWrites = false

    @Option(name: .customLong("latency-out"), help: "Write every acknowledged increment (completion time from the start in s, latency in ms, retries) to this CSV file.")
    var latencyOut: String?

    @Flag(help: "Print the CSV header line before the row.")
    var header = false

    func validate() throws {
        guard workers > 0 else { throw ValidationError("--workers must be positive") }
        guard increments > 0 else { throw ValidationError("--increments must be positive") }
        guard shards > 0 else { throw ValidationError("--shards must be positive") }
        if let killPrimaryAfter, killPrimaryAfter < 0 {
            throw ValidationError("--kill-primary-after must not be negative")
        }
        if let stepDownAfter, stepDownAfter < 0 {
            throw ValidationError("--step-down-after must not be negative")
        }
        if killPrimaryAfter != nil && stepDownAfter != nil {
            throw ValidationError("--kill-primary-after and --step-down-after exclude each other")
        }
    }

    /// What to do to the primary, from --kill-primary-after or --step-down-after.
    private var failure: (kind: PrimaryFailure, after: Duration)? {
        func duration(_ seconds: Double) -> Duration { .milliseconds(Int64((seconds * 1000).rounded())) }
        if let killPrimaryAfter {
            return (.kill, duration(killPrimaryAfter))
        }
        if let stepDownAfter {
            return (.stepDown, duration(stepDownAfter))
        }
        return nil
    }

    func run() async throws {
        let bench = Bench(
            variant: variant,
            workers: workers,
            increments: increments,
            location: Bench.location(shards: shards),
            url: url,
            failure: failure,
            control: ReplicaSetControl(script: mongoScript),
            retryableWrites: !noRetryableWrites,
            recordLatencies: latencyOut != nil
        )
        let result = try await bench.run()
        if let latencyOut {
            try LatencySample.writeCSV(result.latencies, to: latencyOut)
        }

        if header {
            print(BenchResult.csvHeader)
        }
        print(result.csvRow)
        if let failure, result.failure == nil {
            printToStandardError("mongo-bench: the run ended before the \(failure.kind == .kill ? "kill" : "step-down") was due")
            throw ExitCode(3)
        }
        let mustBeExact = (variant == .majority && !noRetryableWrites) || result.failure == nil
        if mustBeExact && !result.isExact {
            printToStandardError("mongo-bench: \(variant.rawValue) expected \(result.expected), counter is \(result.finalValue)")
            throw ExitCode(2)
        }
    }
}
