import CommandLineSupport
import CounterCore
import Synchronization

/// What mongo-bench does to the primary during a run.
enum PrimaryFailure: String, Sendable {
    /// SIGKILL (`scripts/mongo.sh kill-node`): a sudden failure. The node is
    /// started again once the workers are done.
    case kill
    /// `replSetStepDown`: a clean hand-over. The node stays up and becomes a
    /// secondary.
    case stepDown = "stepdown"
}

/// One mongo-bench run: reset, the timed increments (with an optional kill or
/// step-down of the primary), then the final read once the replica set has
/// settled.
struct Bench: Sendable {
    let variant: MongoWriteConcern
    let workers: Int
    let increments: Int
    let location: MongoCounterLocation
    let url: String
    /// What happens to the primary, and how far into the run.
    let failure: (kind: PrimaryFailure, after: Duration)?
    let control: ReplicaSetControl
    let retryableWrites: Bool
    /// Whether to keep a `LatencySample` for every acknowledged increment.
    let recordLatencies: Bool

    /// The "post with a likes counter" of the task, split over `shards`
    /// documents when that is above 1.
    static func location(shards: Int = 1) -> MongoCounterLocation {
        MongoCounterLocation(database: "hls", collection: "posts", documentID: "post-1", field: "likes", shards: shards)
    }

    private struct WorkerStats: Sendable {
        var acknowledged = 0
        var retries = 0
        var errors = 0
        var longest = Duration.zero
        var latencies: [LatencySample] = []

        mutating func add(_ other: WorkerStats) {
            acknowledged += other.acknowledged
            retries += other.retries
            errors += other.errors
            longest = max(longest, other.longest)
            latencies += other.latencies
        }
    }

    /// The node that was primary and when it was killed or told to step down.
    private typealias FailedPrimary = (node: Int, at: Duration)

    func run() async throws -> BenchResult {
        let retryLog = RetryLog()
        // Resets, reads the term and the final value, finds the primary to
        // kill or step down.
        let setup = try await MongoCounterClient(url: url, location: location)
        var clients: [MongoCounterClient] = []
        do {
            try await setup.reset()
            let termBefore = try await term(of: setup)
            for index in 0..<workers {
                clients.append(
                    try await MongoCounterClient(
                        url: url,
                        location: location,
                        shard: index % location.shards,
                        writeConcern: variant,
                        retryableWrites: retryableWrites,
                        onRetry: { retryLog.record($0) }
                    )
                )
            }

            let timeline = Timeline()
            let clock = ContinuousClock()
            let start = clock.now
            let (total, failed) = try await withThrowingTaskGroup(of: FailedPrimary?.self) { group in
                if let failure {
                    group.addTask {
                        try await failPrimary(failure.kind, after: failure.after, from: start, using: setup, timeline: timeline)
                    }
                }
                let total = await withTaskGroup(of: WorkerStats.self) { workers in
                    for client in clients {
                        workers.addTask {
                            await Self.runWorker(
                                client,
                                increments: increments,
                                start: start,
                                recordLatencies: recordLatencies,
                                timeline: timeline
                            )
                        }
                    }
                    return await workers.reduce(into: WorkerStats()) { $0.add($1) }
                }
                // A kill or step-down that is not due yet will not happen.
                group.cancelAll()
                return (total, try await group.next() ?? nil)
            }
            let elapsed = start.duration(to: clock.now)
            retryLog.report()

            if let failed, failure?.kind == .kill {
                printToStandardError("mongo-bench: starting node \(failed.node) again")
                try await control.run("start-node", node: failed.node)
                try await control.run("wait-secondary", node: failed.node)
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
                failure: failed == nil ? nil : failure?.kind,
                failedNode: failed?.node,
                failedAfter: failed?.at,
                failover: timeline.failover,
                elapsed: elapsed,
                finalValue: finalValue,
                acknowledged: total.acknowledged,
                retries: total.retries,
                errors: total.errors,
                elections: termAfter - termBefore,
                longestOperation: total.longest,
                latencies: total.latencies.sorted { $0.completed < $1.completed }
            )
        } catch {
            await Self.close([setup] + clients)
            throw error
        }
    }

    private static func runWorker(
        _ client: MongoCounterClient,
        increments: Int,
        start: ContinuousClock.Instant,
        recordLatencies: Bool,
        timeline: Timeline
    ) async -> WorkerStats {
        let clock = ContinuousClock()
        var stats = WorkerStats()
        if recordLatencies {
            stats.latencies.reserveCapacity(increments)
        }
        for _ in 0..<increments {
            let began = clock.now
            do {
                let outcome = try await client.increment()
                let completed = clock.now
                stats.acknowledged += 1
                if outcome.retries > 0 {
                    stats.retries += outcome.retries
                    timeline.acknowledgedAfterRetry(at: completed)
                }
                if recordLatencies {
                    stats.latencies.append(
                        LatencySample(
                            completed: start.duration(to: completed),
                            latency: began.duration(to: completed),
                            retries: outcome.retries
                        )
                    )
                }
            } catch {
                stats.errors += 1
                printToStandardError("mongo-bench: increment given up: \(MongoCommandFailure.kind(of: error))")
            }
            stats.longest = max(stats.longest, began.duration(to: clock.now))
        }
        return stats
    }

    /// Sleeps until `after` into the run, then kills or steps down the node
    /// that is primary at that moment. Returns `nil` if cancelled before.
    ///
    /// The moment of a kill is when `kill-node` has seen the process gone; of
    /// a step-down, when `replSetStepDown` is sent: from then on the primary
    /// accepts no writes while it waits for a secondary to catch up.
    private func failPrimary(
        _ kind: PrimaryFailure,
        after: Duration,
        from start: ContinuousClock.Instant,
        using setup: MongoCounterClient,
        timeline: Timeline
    ) async throws -> FailedPrimary? {
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
        switch kind {
        case .kill:
            printToStandardError("mongo-bench: killing the primary, node \(node) (\(primary))")
            try await control.run("kill-node", node: node)
            let killedAt = ContinuousClock.now
            timeline.failed(at: killedAt)
            return (node, start.duration(to: killedAt))
        case .stepDown:
            printToStandardError("mongo-bench: stepping down the primary, node \(node) (\(primary))")
            let sentAt = ContinuousClock.now
            timeline.failed(at: sentAt)
            // Not retried: a resend could step down the next primary too.
            // stepDownSecs 60 (the node may not run for primary for 60 s),
            // secondaryCatchUpPeriodSecs 10 by default.
            _ = try await setup.adminCommandOnce(["replSetStepDown": Int32(60)])
            let took = sentAt.duration(to: .now).inSeconds
            printToStandardError("mongo-bench: node \(node) stepped down, replSetStepDown replied after \(decimalString(took, fractionDigits: 3)) s")
            return (node, start.duration(to: sentAt))
        }
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

/// When the primary was killed or told to step down, and when the first
/// increment was acknowledged after that.
final class Timeline: Sendable {
    private struct State {
        var failedAt: ContinuousClock.Instant?
        var firstAcknowledgement: ContinuousClock.Instant?
    }

    private let state = Mutex(State())

    func failed(at instant: ContinuousClock.Instant) {
        state.withLock { $0.failedAt = instant }
    }

    /// Only increments that had to retry can be the first after a kill or
    /// step-down: every other one went to the old primary while it still was
    /// one.
    func acknowledgedAfterRetry(at instant: ContinuousClock.Instant) {
        state.withLock { state in
            if let failedAt = state.failedAt, instant > failedAt, state.firstAcknowledgement == nil {
                state.firstAcknowledgement = instant
            }
        }
    }

    var failover: Duration? {
        state.withLock { state in
            guard let failedAt = state.failedAt, let first = state.firstAcknowledgement else { return nil }
            return failedAt.duration(to: first)
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
