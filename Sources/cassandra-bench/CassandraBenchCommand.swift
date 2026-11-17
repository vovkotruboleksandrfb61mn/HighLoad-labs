import ArgumentParser
import CommandLineSupport
import CounterCore

extension CassandraConsistency: ExpressibleByArgument {}

/// Which client talks to Cassandra.
enum Driver: String, CaseIterable, ExpressibleByArgument {
    /// apple/swift-cassandra-client over the DataStax C++ driver.
    case datastax
    /// Our CQLClient (native protocol v4 over SwiftNIO).
    case own
}

/// How an increment is done.
enum IncrementMethod: String, CaseIterable, ExpressibleByArgument {
    /// `counter` column, `likes = likes + 1`.
    case counter
    /// `bigint` column, read then write back plus one.
    case rmw
    /// `bigint` column, conditional write (lightweight transaction).
    case lwt
}

@main
struct CassandraBenchCommand: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "cassandra-bench",
        abstract: "Increments one Cassandra counter from concurrent clients and prints one CSV row.",
        discussion: """
            Empties the counter table (TRUNCATE), connects --workers separate \
            clients (each with its own connections to every node), then times \
            workers x `UPDATE likes = likes + 1` at --consistency. Failed \
            increments are counted and, by default, not retried: a counter \
            update is not idempotent. With --retry-failed a failed increment is \
            sent again until it succeeds, which may count it twice. \
            --kill-node, --split-for or --kill-all-for disrupt the cluster \
            --disrupt-after seconds into the run (scripts/cassandra.sh). \
            --reader-consistency adds a client that reads the counter in a loop \
            and counts reads that went backwards or were behind the increments \
            already acknowledged. Once the workers are done and every node is \
            up again, the final value is read with consistency ALL. \
            Exits with status 2 if no disruption was asked for, every increment \
            was acknowledged and the counter still differs from the expected value.
            """
    )

    @Option(help: "Consistency level of the increments: \(CassandraConsistency.allValueStrings.joined(separator: "|")).")
    var consistency: CassandraConsistency

    @Option(help: "How to increment: \(IncrementMethod.allValueStrings.joined(separator: "|")).")
    var method = IncrementMethod.counter

    @Option(help: "Client library: \(Driver.allValueStrings.joined(separator: "|")) (own supports --method counter only).")
    var driver = Driver.datastax

    @Flag(inversion: .prefixedNo, help: "With --driver own: send each increment to the row's primary replica.")
    var tokenAware = true

    @Option(help: "Concurrent clients.")
    var workers = 10

    @Option(help: "Increments per worker.")
    var increments = 10_000

    @Option(help: "Comma-separated contact points.")
    var hosts = "127.0.0.11,127.0.0.12,127.0.0.13"

    @Option(help: "CQL port of every node.")
    var port = 21440

    @Option(name: .customLong("kill-node"), help: "SIGKILL this node (1-3) during the run; it is started again when the workers are done.")
    var killNode: Int?

    @Option(name: .customLong("split-for"), help: "Cut the network between the nodes for this many seconds during the run.")
    var splitFor: Double?

    @Option(name: .customLong("kill-all-for"), help: "SIGKILL all nodes during the run and start them again after this many seconds.")
    var killAllFor: Double?

    @Option(name: .customLong("disrupt-after"), help: "Seconds into the run for the disruption.")
    var disruptAfter = 5.0

    @Flag(name: .customLong("retry-failed"), help: "Send a failed increment again until it succeeds (up to --retry-limit).")
    var retryFailed = false

    @Option(name: .customLong("retry-limit"), help: "Seconds after which --retry-failed gives an increment up.")
    var retryLimit = 120.0

    @Option(name: .customLong("reader-consistency"), help: "Run a reader at this level next to the workers.")
    var readerConsistency: CassandraConsistency?

    @Option(name: .customLong("cluster-script"), help: "scripts/cassandra.sh, used for the disruptions.")
    var clusterScript = "scripts/cassandra.sh"

    @Option(name: .customLong("timeline-out"), help: "Write acknowledged and failed increments per 100 ms to this CSV file.")
    var timelineOut: String?

    @Option(help: "A label for the run, the first CSV column.")
    var label = ""

    @Flag(help: "Print the CSV header line before the row.")
    var header = false

    func validate() throws {
        guard workers > 0 else { throw ValidationError("--workers must be positive") }
        guard increments > 0 else { throw ValidationError("--increments must be positive") }
        let disruptions = [killNode != nil, splitFor != nil, killAllFor != nil].filter { $0 }.count
        guard disruptions <= 1 else { throw ValidationError("--kill-node, --split-for and --kill-all-for exclude each other") }
        if let killNode, !(1...3).contains(killNode) { throw ValidationError("--kill-node must be 1, 2 or 3") }
        if driver == .own && method != .counter { throw ValidationError("--driver own supports --method counter only") }
    }

    private var plan: DisruptionPlan? {
        func duration(_ seconds: Double) -> Duration { .milliseconds(Int64((seconds * 1000).rounded())) }
        let disruption: Disruption
        if let killNode {
            disruption = .killNode(killNode)
        } else if let splitFor {
            disruption = .split(lasting: duration(splitFor))
        } else if let killAllFor {
            disruption = .killAll(down: duration(killAllFor))
        } else {
            return nil
        }
        return DisruptionPlan(disruption: disruption, after: duration(disruptAfter), control: ClusterControl(script: clusterScript))
    }

    func run() async throws {
        let contactPoints = hosts.split(separator: ",").map(String.init)
        let plan = self.plan
        let method = self.method
        let driver = self.driver
        let tokenAware = self.tokenAware
        let open = { (level: CassandraConsistency) async throws -> any CassandraBenchStore in
            switch method {
            case .counter where driver == .own:
                try await CQLCounterStore.open(contactPoints: contactPoints, port: port, consistency: level, tokenAware: tokenAware)
            case .counter:
                try await CassandraCounterStore.open(contactPoints: contactPoints, port: port, consistency: level)
            case .rmw:
                try await CassandraValueCounterStore.open(contactPoints: contactPoints, port: port, consistency: level, method: .readModifyWrite)
            case .lwt:
                try await CassandraValueCounterStore.open(contactPoints: contactPoints, port: port, consistency: level, method: .lightweightTransaction)
            }
        }

        let control = try await open(.all)
        try await control.reset()
        var stores: [any CassandraBenchStore] = []
        for _ in 0..<workers {
            stores.append(try await open(consistency))
        }
        var reader: (any CassandraBenchStore)?
        if let readerConsistency {
            reader = try await open(readerConsistency)
        }

        let clock = ContinuousClock()
        let start = clock.now
        let timeline = Timeline(start: start)
        var readerTask: Task<ReaderStats, Never>?
        if let reader {
            readerTask = Task { await Self.runReader(reader, timeline: timeline) }
        }
        let (stats, disruptedAt) = try await withThrowingTaskGroup(of: Duration?.self) { group -> (WorkerStats, Duration?) in
            if let plan {
                group.addTask { try await plan.perform(from: start, timeline: timeline) }
            }
            let stats = await withTaskGroup(of: WorkerStats.self) { workersGroup in
                for store in stores {
                    workersGroup.addTask {
                        await Self.runWorker(store, increments: increments, retry: retryFailed ? .milliseconds(Int64(retryLimit * 1000)) : nil, timeline: timeline)
                    }
                }
                return await workersGroup.reduce(into: WorkerStats()) { $0.add($1) }
            }
            let elapsed = start.duration(to: clock.now)
            // A disruption that is not due yet will not happen; one under way
            // (a split, a cluster restart) is let finish.
            if let plan, elapsed < plan.after {
                group.cancelAll()
            }
            var disruptedAt: Duration?
            while let result = try await group.next() {
                disruptedAt = result ?? disruptedAt
            }
            return (stats.with(elapsed: elapsed), disruptedAt)
        }
        timeline.finished.store(true, ordering: .relaxed)
        let readerStats = await readerTask?.value
        try? await reader?.close()

        let conflicts = stores.reduce(0) { $0 + (($1 as? CassandraValueCounterStore)?.conflicts ?? 0) }
        for store in stores { try? await store.close() }
        try await plan?.restore()
        let finalValue = try await control.value(consistency: .all)
        try? await control.close()
        if let timelineOut {
            try writeFile(timeline.csv, to: timelineOut)
        }

        let result = BenchResult(
            label: label,
            consistency: consistency,
            workers: workers,
            incrementsPerWorker: increments,
            disruption: disruptedAt == nil ? "" : (plan?.disruption.label ?? ""),
            disruptedAt: disruptedAt,
            retry: retryFailed,
            stats: stats,
            finalValue: finalValue,
            reader: readerStats,
            method: method.rawValue,
            driver: driver.rawValue,
            conflicts: conflicts
        )
        if header {
            print(BenchResult.csvHeader)
        }
        print(result.csvRow)
        if plan == nil && stats.errors == 0 && finalValue != result.expected {
            printToStandardError("cassandra-bench: \(consistency.rawValue) expected \(result.expected), counter is \(finalValue)")
            throw ExitCode(2)
        }
    }

    private static func runWorker(_ store: any CassandraBenchStore, increments: Int, retry limit: Duration?, timeline: Timeline) async -> WorkerStats {
        let clock = ContinuousClock()
        var stats = WorkerStats()
        stats.latencies.reserveCapacity(increments)
        for _ in 0..<increments {
            let began = clock.now
            let giveUp = began.advanced(by: limit ?? .zero)
            while true {
                do {
                    try await store.increment()
                    let now = clock.now
                    stats.acknowledged += 1
                    stats.latencies.append(began.duration(to: now))
                    timeline.acknowledged(at: now)
                    break
                } catch {
                    let now = clock.now
                    timeline.failed(at: now)
                    stats.failures[Self.kind(of: error), default: 0] += 1
                    guard limit != nil, now < giveUp else {
                        stats.errors += 1
                        break
                    }
                    stats.retries += 1
                    try? await Task.sleep(for: .milliseconds(50))
                }
            }
        }
        return stats
    }

    /// Reads the counter in a loop until the workers are done, comparing every read with
    /// the previous one and with the increments acknowledged before it was
    /// sent.
    private static func runReader(_ store: any CassandraBenchStore, timeline: Timeline) async -> ReaderStats {
        var stats = ReaderStats()
        var previous = 0
        while !timeline.finished.load(ordering: .relaxed) {
            let acknowledged = timeline.acknowledgedTotal.load(ordering: .relaxed)
            do {
                let value = try await store.value()
                stats.reads += 1
                if value < previous { stats.backwards += 1 }
                if value < acknowledged {
                    stats.stale += 1
                    stats.largestShortfall = max(stats.largestShortfall, acknowledged - value)
                }
                previous = max(previous, value)
            } catch {
                stats.failed += 1
                try? await Task.sleep(for: .milliseconds(50))
            }
            try? await Task.sleep(for: .milliseconds(2))
        }
        return stats
    }

    /// The driver's error message up to the first colon, e.g. "Write timeout".
    static func kind(of error: any Error) -> String {
        let text = String(describing: error)
        return String(text.prefix { $0 != ":" })
    }
}

struct WorkerStats: Sendable {
    var acknowledged = 0
    /// Increments given up on.
    var errors = 0
    /// Increments sent again after a failure (--retry-failed).
    var retries = 0
    /// Failed attempts by the driver's error kind.
    var failures: [String: Int] = [:]
    var latencies: [Duration] = []
    var elapsed = Duration.zero

    mutating func add(_ other: WorkerStats) {
        acknowledged += other.acknowledged
        errors += other.errors
        retries += other.retries
        failures.merge(other.failures, uniquingKeysWith: +)
        latencies += other.latencies
    }

    func with(elapsed: Duration) -> WorkerStats {
        var copy = self
        copy.elapsed = elapsed
        return copy
    }
}
