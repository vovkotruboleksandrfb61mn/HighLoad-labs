import ArgumentParser
import CommandLineSupport
import CounterCore

extension CassandraConsistency: ExpressibleByArgument {}

@main
struct CassandraBenchCommand: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "cassandra-bench",
        abstract: "Increments one Cassandra counter from concurrent clients and prints one CSV row.",
        discussion: """
            Empties the counter table (TRUNCATE), connects --workers separate \
            clients (each with its own connections to every node), then times \
            workers x `UPDATE likes = likes + 1` at --consistency. Failed \
            increments are counted, not retried: a counter update is not \
            idempotent. The final value is read with consistency ALL. \
            Exits with status 2 if every increment was acknowledged and the \
            counter still differs from workers x increments.
            """
    )

    @Option(help: "Consistency level of the increments: \(CassandraConsistency.allValueStrings.joined(separator: "|")).")
    var consistency: CassandraConsistency

    @Option(help: "Concurrent clients.")
    var workers = 10

    @Option(help: "Increments per worker.")
    var increments = 10_000

    @Option(help: "Comma-separated contact points.")
    var hosts = "127.0.0.11,127.0.0.12,127.0.0.13"

    @Option(help: "CQL port of every node.")
    var port = 21440

    @Flag(help: "Print the CSV header line before the row.")
    var header = false

    func validate() throws {
        guard workers > 0 else { throw ValidationError("--workers must be positive") }
        guard increments > 0 else { throw ValidationError("--increments must be positive") }
    }

    func run() async throws {
        let contactPoints = hosts.split(separator: ",").map(String.init)
        let result = try await CassandraCounterStore.withStore(contactPoints: contactPoints, port: port, consistency: .all) { control in
            try await control.reset()
            var stores: [CassandraCounterStore] = []
            do {
                for _ in 0..<workers {
                    stores.append(try await CassandraCounterStore.open(contactPoints: contactPoints, port: port, consistency: consistency))
                }
                let clock = ContinuousClock()
                let start = clock.now
                let stats = await withTaskGroup(of: WorkerStats.self) { group in
                    for store in stores {
                        group.addTask { await Self.runWorker(store, increments: increments) }
                    }
                    return await group.reduce(into: WorkerStats()) { $0.add($1) }
                }
                let elapsed = start.duration(to: clock.now)
                for store in stores { try await store.close() }
                stores = []
                let finalValue = try await control.value(consistency: .all)
                return BenchResult(
                    consistency: consistency,
                    workers: workers,
                    incrementsPerWorker: increments,
                    elapsed: elapsed,
                    finalValue: finalValue,
                    acknowledged: stats.acknowledged,
                    errors: stats.errors,
                    latencies: stats.latencies
                )
            } catch {
                for store in stores { try? await store.close() }
                throw error
            }
        }
        if header {
            print(BenchResult.csvHeader)
        }
        print(result.csvRow)
        if result.errors == 0 && result.finalValue != result.expected {
            printToStandardError("cassandra-bench: \(consistency.rawValue) expected \(result.expected), counter is \(result.finalValue)")
            throw ExitCode(2)
        }
    }

    private struct WorkerStats: Sendable {
        var acknowledged = 0
        var errors = 0
        var latencies: [Duration] = []

        mutating func add(_ other: WorkerStats) {
            acknowledged += other.acknowledged
            errors += other.errors
            latencies += other.latencies
        }
    }

    private static func runWorker(_ store: CassandraCounterStore, increments: Int) async -> WorkerStats {
        let clock = ContinuousClock()
        var stats = WorkerStats()
        stats.latencies.reserveCapacity(increments)
        for _ in 0..<increments {
            let began = clock.now
            do {
                try await store.increment()
                stats.acknowledged += 1
                stats.latencies.append(began.duration(to: clock.now))
            } catch {
                stats.errors += 1
                printToStandardError("cassandra-bench: increment failed: \(error)")
            }
        }
        return stats
    }
}

/// The outcome of one run, printed as a CSV row.
struct BenchResult: Sendable {
    let consistency: CassandraConsistency
    let workers: Int
    let incrementsPerWorker: Int
    let elapsed: Duration
    /// Read with consistency ALL after the run.
    let finalValue: Int
    let acknowledged: Int
    let errors: Int
    /// Latency of every acknowledged increment.
    let latencies: [Duration]

    var expected: Int { workers * incrementsPerWorker }

    static let csvHeader = "consistency,workers,increments_per_worker,seconds,ops_per_sec,final_value,expected,acknowledged,errors,p50_ms,p99_ms,max_ms"

    var csvRow: String {
        let sorted = latencies.sorted()
        func percentile(_ p: Double) -> String {
            guard !sorted.isEmpty else { return "" }
            let index = min(sorted.count - 1, Int((p * Double(sorted.count)).rounded(.up)) - 1)
            return decimalString(sorted[max(0, index)].inSeconds * 1000, fractionDigits: 3)
        }
        return [
            consistency.rawValue.uppercased(),
            String(workers),
            String(incrementsPerWorker),
            decimalString(elapsed.inSeconds, fractionDigits: 4),
            decimalString(Double(acknowledged) / elapsed.inSeconds, fractionDigits: 1),
            String(finalValue),
            String(expected),
            String(acknowledged),
            String(errors),
            percentile(0.5),
            percentile(0.99),
            percentile(1.0),
        ].joined(separator: ",")
    }
}
