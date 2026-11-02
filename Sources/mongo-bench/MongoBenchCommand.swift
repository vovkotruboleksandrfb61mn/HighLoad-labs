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
            majority after it has rejoined and caught up. \
            Exits with status 2 if a run that must be exact (majority, or \
            any run without a kill) is not.
            """
    )

    @Option(help: "Write concern of the increments: \(MongoWriteConcern.allValueStrings.joined(separator: "|")).")
    var variant: MongoWriteConcern

    @Option(help: "Concurrent clients, each with its own connections and session.")
    var workers = 10

    @Option(help: "Increments per worker.")
    var increments = 10_000

    @Option(name: .customLong("kill-primary-after"), help: "Seconds into the run to SIGKILL the primary (default: no kill).")
    var killPrimaryAfter: Double?

    @Option(name: .customLong("mongo-script"), help: "scripts/mongo.sh, used to kill, restart and wait for the node.")
    var mongoScript = "scripts/mongo.sh"

    @Option(help: "Replica set URL; node N of scripts/mongo.sh must be its Nth host.")
    var url = "mongodb://127.0.0.1:21430,127.0.0.1:21431,127.0.0.1:21432/?replicaSet=rs0"

    @Flag(name: .customLong("no-retryable-writes"), help: "Resend increments after errors without lsid/txnNumber, so the server cannot recognise a resend (a demonstration of double counting).")
    var noRetryableWrites = false

    @Flag(help: "Print the CSV header line before the row.")
    var header = false

    func validate() throws {
        guard workers > 0 else { throw ValidationError("--workers must be positive") }
        guard increments > 0 else { throw ValidationError("--increments must be positive") }
        if let killPrimaryAfter, killPrimaryAfter < 0 {
            throw ValidationError("--kill-primary-after must not be negative")
        }
    }

    func run() async throws {
        let bench = Bench(
            variant: variant,
            workers: workers,
            increments: increments,
            url: url,
            killAfter: killPrimaryAfter.map { .milliseconds(Int64(($0 * 1000).rounded())) },
            control: ReplicaSetControl(script: mongoScript),
            retryableWrites: !noRetryableWrites
        )
        let result = try await bench.run()

        if header {
            print(BenchResult.csvHeader)
        }
        print(result.csvRow)
        if killPrimaryAfter != nil && result.killedNode == nil {
            printToStandardError("mongo-bench: the run ended before the kill was due")
            throw ExitCode(3)
        }
        let mustBeExact = (variant == .majority && !noRetryableWrites) || result.killedNode == nil
        if mustBeExact && !result.isExact {
            printToStandardError("mongo-bench: \(variant.rawValue) expected \(result.expected), counter is \(result.finalValue)")
            throw ExitCode(2)
        }
    }
}
