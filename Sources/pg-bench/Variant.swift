import ArgumentParser
import PostgresNIO

/// The ways of adding one to `user_counter.counter`. Every increment runs in
/// its own explicit `BEGIN` ... `COMMIT`.
enum Variant: String, CaseIterable, ExpressibleByArgument, Sendable {
    /// READ COMMITTED read, add one in the client, write back. Two
    /// transactions that read the same value both write value + 1, so one of
    /// the increments is lost.
    case lostUpdate = "lost-update"
    /// The same read-modify-write under SERIALIZABLE. The server aborts one of
    /// two conflicting transactions with 40001; without a retry that
    /// increment is simply dropped and counted as an error.
    case serializable
    /// SERIALIZABLE, retrying the whole transaction on 40001 until it commits.
    case serializableRetry = "serializable-retry"
    /// `UPDATE ... SET counter = counter + 1`: the server reads and writes
    /// under the row lock, so nothing is lost.
    case inPlace = "in-place"
    /// `SELECT ... FOR UPDATE` takes the row lock before the read, so the
    /// read-modify-write in the client is serialised.
    case forUpdate = "for-update"
    /// Optimistic locking: read counter and version, then update only if the
    /// version is unchanged; if no row was updated, start over.
    case optimistic

    /// Whether every increment is guaranteed to land.
    var mustBeExact: Bool {
        switch self {
        case .lostUpdate, .serializable: false
        case .serializableRetry, .inPlace, .forUpdate, .optimistic: true
        }
    }

    /// Adds one to the counter (or gives up, for `.serializable`) and reports
    /// what it took.
    func increment(on connection: PostgresConnection, logger: Logger) async throws -> Outcome {
        switch self {
        case .lostUpdate:
            try await connection.transaction(logger: logger) { connection in
                let counter = try await connection.single(Int32.self, "SELECT counter FROM user_counter WHERE user_id = 1", logger: logger)
                try await connection.query("UPDATE user_counter SET counter = \(counter + 1) WHERE user_id = 1", logger: logger)
            }
            return .done

        case .serializable:
            do {
                try await serializableReadModifyWrite(on: connection, logger: logger)
                return .done
            } catch where error.isSerializationFailure {
                return .failed
            }

        case .serializableRetry:
            var retries = 0
            while true {
                do {
                    try await serializableReadModifyWrite(on: connection, logger: logger)
                    return .done(retries: retries)
                } catch where error.isSerializationFailure {
                    retries += 1
                }
            }

        case .inPlace:
            try await connection.transaction(logger: logger) { connection in
                try await connection.query("UPDATE user_counter SET counter = counter + 1 WHERE user_id = 1", logger: logger)
            }
            return .done

        case .forUpdate:
            try await connection.transaction(logger: logger) { connection in
                let counter = try await connection.single(
                    Int32.self, "SELECT counter FROM user_counter WHERE user_id = 1 FOR UPDATE", logger: logger)
                try await connection.query("UPDATE user_counter SET counter = \(counter + 1) WHERE user_id = 1", logger: logger)
            }
            return .done

        case .optimistic:
            var retries = 0
            while true {
                let updated = try await connection.transaction(logger: logger) { connection in
                    let (counter, version) = try await connection.firstRow(
                        (Int32, Int32).self, "SELECT counter, version FROM user_counter WHERE user_id = 1", logger: logger)
                    // RETURNING yields one row when the version still matched,
                    // and none when another transaction got there first.
                    let rows = try await connection.query(
                        """
                        UPDATE user_counter SET counter = \(counter + 1), version = \(version + 1)
                        WHERE user_id = 1 AND version = \(version)
                        RETURNING version
                        """,
                        logger: logger
                    ).collect()
                    return !rows.isEmpty
                }
                if updated {
                    return .done(retries: retries)
                }
                retries += 1
            }
        }
    }

    private func serializableReadModifyWrite(on connection: PostgresConnection, logger: Logger) async throws {
        try await connection.transaction("BEGIN ISOLATION LEVEL SERIALIZABLE", logger: logger) { connection in
            let counter = try await connection.single(Int32.self, "SELECT counter FROM user_counter WHERE user_id = 1", logger: logger)
            try await connection.query("UPDATE user_counter SET counter = \(counter + 1) WHERE user_id = 1", logger: logger)
        }
    }
}

/// What one increment took.
struct Outcome: Sendable {
    /// Whether the increment was given up (an error, not retried).
    var failed: Bool
    /// How many attempts were thrown away before the one that counted.
    var retries: Int

    static let done = Outcome(failed: false, retries: 0)
    static let failed = Outcome(failed: true, retries: 0)

    static func done(retries: Int) -> Outcome {
        Outcome(failed: false, retries: retries)
    }
}
