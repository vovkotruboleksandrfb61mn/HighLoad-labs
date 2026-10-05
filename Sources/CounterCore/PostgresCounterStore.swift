import PostgresNIO

/// A counter kept in the `user_counter` row of PostgreSQL, incremented in place
/// with `UPDATE user_counter SET counter = counter + 1 WHERE user_id = 1`.
///
/// Every operation is a single statement in its own (autocommit) transaction
/// on a connection leased from a `PostgresClient` pool. The in-place update
/// takes the row lock and reads the latest committed value, so concurrent
/// increments queue up behind each other instead of overwriting each other.
public struct PostgresCounterStore: CounterStore {
    /// The row the counter lives in (`deploy/postgres-init.sql`).
    public static let userID: Int32 = 1

    private let client: PostgresClient

    /// Wraps a client whose `run()` is already running in some task.
    public init(client: PostgresClient) {
        self.client = client
    }

    /// Connects a `PostgresClient` pool to `url`, runs `body` with the store,
    /// then shuts the pool down.
    ///
    /// The pool's `run()` lives in a child task for exactly as long as `body`
    /// runs. Before `body` starts, the store reads the counter once, so a
    /// server that is down or a missing table fails here and not on the first
    /// request.
    public static func withStore<Result: Sendable>(
        url: String,
        maximumConnections: Int = 20,
        _ body: (PostgresCounterStore) async throws -> Result
    ) async throws -> Result {
        let endpoint = try PostgresEndpoint(url: url)
        let client = PostgresClient(configuration: endpoint.clientConfiguration(maximumConnections: maximumConnections))
        return try await withThrowingDiscardingTaskGroup { group in
            group.addTask {
                await client.run()
            }
            // `run()` only returns once it is cancelled.
            defer { group.cancelAll() }
            let store = PostgresCounterStore(client: client)
            _ = try await store.value()
            return try await body(store)
        }
    }

    public func increment() async throws {
        try await client.query("UPDATE user_counter SET counter = counter + 1 WHERE user_id = \(Self.userID)")
    }

    public func value() async throws -> Int {
        let rows = try await client.query("SELECT counter FROM user_counter WHERE user_id = \(Self.userID)")
        for try await counter in rows.decode(Int32.self) {
            return Int(counter)
        }
        throw CounterStoreError.missingRow(table: "user_counter", key: Int(Self.userID))
    }

    /// Puts the row back to `(1, 0, 0)`, creating it if it is gone.
    public func reset() async throws {
        try await client.query(
            """
            INSERT INTO user_counter (user_id, counter, version) VALUES (\(Self.userID), 0, 0)
            ON CONFLICT (user_id) DO UPDATE SET counter = 0, version = 0
            """
        )
    }
}
