import CounterCore
import PostgresNIO

/// One measured run: `workers` tasks, each with its own connection, each
/// incrementing `increments` times with the chosen variant.
struct Bench: Sendable {
    let variant: Variant
    let endpoint: PostgresEndpoint
    let workers: Int
    let increments: Int

    /// Driver logging stays off: anything below critical would only add noise
    /// (and cost) to the measurement.
    private static let logger: Logger = {
        var logger = Logger(label: "pg-bench")
        logger.logLevel = .critical
        return logger
    }()

    func run() async throws -> BenchResult {
        let logger = Self.logger
        let configuration = endpoint.connectionConfiguration

        // Connect every worker before the clock starts, so the measurement
        // covers the increments and not the handshakes.
        var connections: [PostgresConnection] = []
        do {
            for id in 0..<workers {
                connections.append(try await PostgresConnection.connect(configuration: configuration, id: id, logger: logger))
            }
        } catch {
            await Self.close(connections)
            throw error
        }

        do {
            let result = try await measure(connections, logger: logger)
            await Self.close(connections)
            return result
        } catch {
            await Self.close(connections)
            throw error
        }
    }

    private func measure(_ connections: [PostgresConnection], logger: Logger) async throws -> BenchResult {
        let control = connections[0]
        try await control.query(
            """
            INSERT INTO user_counter (user_id, counter, version) VALUES (1, 0, 0)
            ON CONFLICT (user_id) DO UPDATE SET counter = 0, version = 0
            """,
            logger: logger
        )

        let variant = variant
        let increments = increments
        let clock = ContinuousClock()
        let start = clock.now
        let tally = try await withThrowingTaskGroup(of: Tally.self) { group in
            for connection in connections {
                group.addTask {
                    var tally = Tally()
                    for _ in 0..<increments {
                        tally.record(try await variant.increment(on: connection, logger: logger))
                    }
                    return tally
                }
            }
            return try await group.reduce(into: Tally()) { $0.add($1) }
        }
        let elapsed = start.duration(to: clock.now)

        let finalValue = try await control.single(Int32.self, "SELECT counter FROM user_counter WHERE user_id = 1", logger: logger)
        return BenchResult(
            variant: variant,
            workers: workers,
            incrementsPerWorker: increments,
            elapsed: elapsed,
            finalValue: Int(finalValue),
            errors: tally.errors,
            retries: tally.retries
        )
    }

    private static func close(_ connections: [PostgresConnection]) async {
        for connection in connections {
            try? await connection.close()
        }
    }
}

/// Errors and retries summed over the increments of one or more workers.
struct Tally: Sendable {
    var errors = 0
    var retries = 0

    mutating func record(_ outcome: Outcome) {
        if outcome.failed {
            errors += 1
        }
        retries += outcome.retries
    }

    mutating func add(_ other: Tally) {
        errors += other.errors
        retries += other.retries
    }
}
