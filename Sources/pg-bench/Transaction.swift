import PostgresNIO

extension PostgresConnection {
    /// Runs `body` between an explicit `begin` statement and `COMMIT`.
    ///
    /// If `body` or `COMMIT` fails, sends `ROLLBACK` and rethrows the original
    /// error. After a failed `COMMIT` the server has already ended the
    /// transaction, and the extra `ROLLBACK` only draws a warning.
    @discardableResult
    func transaction<Result>(
        _ begin: PostgresQuery = "BEGIN",
        logger: Logger,
        _ body: (PostgresConnection) async throws -> Result
    ) async throws -> Result {
        try await query(begin, logger: logger)
        do {
            let result = try await body(self)
            try await query("COMMIT", logger: logger)
            return result
        } catch {
            _ = try? await query("ROLLBACK", logger: logger)
            throw error
        }
    }

    /// The single value of a one-row, one-column result.
    func single<Value: PostgresDecodable & Sendable>(_ type: Value.Type, _ statement: PostgresQuery, logger: Logger) async throws -> Value {
        for try await value in try await query(statement, logger: logger).decode(Value.self) {
            return value
        }
        throw BenchError.missingRow
    }

    /// The columns of the first row of a result.
    func firstRow<each Column: PostgresDecodable & Sendable>(
        _ types: (repeat each Column).Type,
        _ statement: PostgresQuery,
        logger: Logger
    ) async throws -> (repeat each Column) {
        for try await row in try await query(statement, logger: logger).decode(types) {
            return row
        }
        throw BenchError.missingRow
    }
}

extension Error {
    /// SQLSTATE 40001: the server aborted the transaction because it could not
    /// be serialised with a concurrent one. Retrying it from the start is safe.
    var isSerializationFailure: Bool {
        guard let error = self as? PSQLError else { return false }
        return error.serverInfo?[.sqlState] == PostgresError.Code.serializationFailure.raw
    }
}

enum BenchError: Error, CustomStringConvertible {
    case missingRow

    var description: String {
        switch self {
        case .missingRow:
            "user_counter has no row for user_id = 1; run scripts/pg.sh reset"
        }
    }
}
