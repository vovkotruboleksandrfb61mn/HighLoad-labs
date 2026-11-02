import CommandLineSupport
import CounterCore
import Synchronization

/// One mongo-bench run: reset, the timed increments (with an optional kill of
/// the primary), then the final read once the replica set has settled.
struct Bench: Sendable {
    let variant: MongoWriteConcern
    let workers: Int
    let increments: Int
    let url: String
    let killAfter: Duration?
    let control: ReplicaSetControl
    let retryableWrites: Bool

    /// The "post with a likes counter" of the task.
    static let location = MongoCounterLocation(database: "hls", collection: "posts", documentID: "post-1", field: "likes")

    private struct WorkerStats: Sendable {
        var acknowledged = 0
        var retries = 0
        var errors = 0
        var longest = Duration.zero

        mutating func add(_ other: WorkerStats) {
            acknowledged += other.acknowledged
            retries += other.retries
            errors += other.errors
            longest = max(longest, other.longest)
        }
    }

    private typealias Kill = (node: Int, at: Duration)

    func run() async throws -> BenchResult {
        let retryLog = RetryLog()
        // Resets, reads the term and the final value, finds the primary to kill.
        let setup = try await MongoCounterClient(url: url, location: Self.location)
        var clients: [MongoCounterClient] = []
        do {
            try await setup.reset()
            let termBefore = try await term(of: setup)
            for _ in 0..<workers {
                clients.append(
                    try await MongoCounterClient(
                        url: url,
                        location: Self.location,
                        writeConcern: variant,
                        retryableWrites: retryableWrites,
                        onRetry: { retryLog.record($0) }
                    )
                )
            }

            let timeline = Timeline()
            let clock = ContinuousClock()
            let start = clock.now
            let (total, kill) = try await withThrowingTaskGroup(of: Kill?.self) { group in
                if let killAfter {
                    group.addTask {
                        try await killPrimary(after: killAfter, from: start, using: setup, timeline: timeline)
                    }
                }
                let total = await withTaskGroup(of: WorkerStats.self) { workers in
                    for client in clients {
                        workers.addTask {
                            await Self.runWorker(client, increments: increments, timeline: timeline)
                        }
                    }
                    return await workers.reduce(into: WorkerStats()) { $0.add($1) }
                }
                // A kill that is not due yet will not happen.
                group.cancelAll()
                return (total, try await group.next() ?? nil)
            }
            let elapsed = start.duration(to: clock.now)
            retryLog.report()

            if let kill {
                printToStandardError("mongo-bench: starting node \(kill.node) again")
                try await control.run("start-node", node: kill.node)
                try await control.run("wait-secondary", node: kill.node)
            }
            let termAfter = try await term(of: setup)
            try await setup.awaitMajorityCommit()
            let finalValue = try await setup.value()
            await Self.close([setup] + clients)

            return BenchResult(
                variant: variant,
                retryableWrites: retryableWrites,
                workers: workers,
                incrementsPerWorker: increments,
                killedNode: kill?.node,
                killedAfter: kill?.at,
                failover: timeline.failover,
                elapsed: elapsed,
                finalValue: finalValue,
                acknowledged: total.acknowledged,
                retries: total.retries,
                errors: total.errors,
                elections: termAfter - termBefore,
                longestOperation: total.longest
            )
        } catch {
            await Self.close([setup] + clients)
            throw error
        }
    }

    private static func runWorker(_ client: MongoCounterClient, increments: Int, timeline: Timeline) async -> WorkerStats {
        let clock = ContinuousClock()
        var stats = WorkerStats()
        for _ in 0..<increments {
            let began = clock.now
            do {
                let outcome = try await client.increment()
                stats.acknowledged += 1
                if outcome.retries > 0 {
                    stats.retries += outcome.retries
                    timeline.acknowledgedAfterRetry(at: clock.now)
                }
            } catch {
                stats.errors += 1
                printToStandardError("mongo-bench: increment given up: \(MongoCommandFailure.kind(of: error))")
            }
            stats.longest = max(stats.longest, began.duration(to: clock.now))
        }
        return stats
    }

    /// Sleeps until `after` into the run, then SIGKILLs the node that is
    /// primary at that moment. Returns `nil` if cancelled before the kill.
    private func killPrimary(
        after: Duration,
        from start: ContinuousClock.Instant,
        using setup: MongoCounterClient,
        timeline: Timeline
    ) async throws -> Kill? {
        do {
            try await Task.sleep(until: start.advanced(by: after), clock: .continuous)
        } catch is CancellationError {
            return nil
        }
        let hello = try await setup.adminCommand(["hello": Int32(1)])
        guard let primary = hello["primary"] as? String, let index = setup.hosts.firstIndex(of: primary) else {
            throw MongoCommandFailure(message: "hello names no primary among \(setup.hosts): \(hello)")
        }
        let node = index + 1
        printToStandardError("mongo-bench: killing the primary, node \(node) (\(primary))")
        try await control.run("kill-node", node: node)
        let killedAt = ContinuousClock.now
        timeline.killed(at: killedAt)
        return (node, start.duration(to: killedAt))
    }

    /// The replica set's current term: it grows by one with every election.
    private func term(of client: MongoCounterClient) async throws -> Int {
        let status = try await client.adminCommand(["replSetGetStatus": Int32(1)])
        guard let term = status["term"] as? Int else {
            throw MongoCommandFailure(message: "replSetGetStatus has no term")
        }
        return term
    }

    private static func close(_ clients: [MongoCounterClient]) async {
        for client in clients {
            await client.close()
        }
    }
}

/// When the primary was killed and when the first increment was acknowledged
/// after that.
final class Timeline: Sendable {
    private struct State {
        var killedAt: ContinuousClock.Instant?
        var firstAcknowledgement: ContinuousClock.Instant?
    }

    private let state = Mutex(State())

    func killed(at instant: ContinuousClock.Instant) {
        state.withLock { $0.killedAt = instant }
    }

    /// Only increments that had to retry can be the first after a kill: every
    /// other one went to the old primary before it died.
    func acknowledgedAfterRetry(at instant: ContinuousClock.Instant) {
        state.withLock { state in
            if let killedAt = state.killedAt, instant > killedAt, state.firstAcknowledgement == nil {
                state.firstAcknowledgement = instant
            }
        }
    }

    var failover: Duration? {
        state.withLock { state in
            guard let killedAt = state.killedAt, let first = state.firstAcknowledgement else { return nil }
            return killedAt.duration(to: first)
        }
    }
}

/// Counts retries by cause and prints the tally to standard error.
final class RetryLog: Sendable {
    private let counts = Mutex<[String: Int]>([:])

    func record(_ error: any Error) {
        let kind = MongoCommandFailure.kind(of: error)
        counts.withLock { $0[kind, default: 0] += 1 }
    }

    func report() {
        let counts = counts.withLock { $0 }
        guard !counts.isEmpty else { return }
        for (kind, count) in counts.sorted(by: { $0.value > $1.value }) {
            printToStandardError("mongo-bench: retried \(count)x after \(kind)")
        }
    }
}
