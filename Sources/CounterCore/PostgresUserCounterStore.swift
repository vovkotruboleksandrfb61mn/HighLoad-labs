import PostgresNIO

/// Per-user counters in the PostgreSQL table `user_likes (user_id bigint
/// PRIMARY KEY, likes bigint)`, written directly, without a cache.
///
/// An increment is one autocommit upsert, `INSERT ... ON CONFLICT (user_id)
/// DO UPDATE SET likes = user_likes.likes + 1`: a new user gets a row with
/// 1, an existing row is updated in place under its row lock, so concurrent
/// increments of one user queue up and none is lost.
public struct PostgresUserCounterStore: UserCounterStore {
    /// The table (`deploy/postgres-init.sql`), shared with the Hazelcast
    /// maps' generic MapStore (`deploy/hazelcast-node*.yaml`).
    public static let table = "user_likes"

    private let client: PostgresClient

    public init(client: PostgresClient) {
        self.client = client
    }

    /// Connects a pool of up to `maximumConnections` to `url`, runs `body`
    /// with the store, then shuts the pool down.
    public static func withStore<Result: Sendable>(
        url: String,
        maximumConnections: Int = 20,
        _ body: (PostgresUserCounterStore) async throws -> Result
    ) async throws -> Result {
        let endpoint = try PostgresEndpoint(url: url)
        let client = PostgresClient(configuration: endpoint.clientConfiguration(maximumConnections: maximumConnections))
        return try await withThrowingDiscardingTaskGroup { group in
            group.addTask {
                await client.run()
            }
            defer { group.cancelAll() }
            let store = PostgresUserCounterStore(client: client)
            // Fails here, not on the first request, if the table is missing.
            _ = try await store.value(user: defaultUser)
            return try await body(store)
        }
    }

    public func increment(user: Int64) async throws {
        try await client.query(
            """
            INSERT INTO user_likes (user_id, likes) VALUES (\(user), 1)
            ON CONFLICT (user_id) DO UPDATE SET likes = user_likes.likes + 1
            """
        )
    }

    public func value(user: Int64) async throws -> Int {
        let rows = try await client.query("SELECT likes FROM user_likes WHERE user_id = \(user)")
        for try await likes in rows.decode(Int64.self) {
            return Int(likes)
        }
        return 0
    }

    public func reset(user: Int64) async throws {
        try await client.query(
            """
            INSERT INTO user_likes (user_id, likes) VALUES (\(user), 0)
            ON CONFLICT (user_id) DO UPDATE SET likes = 0
            """
        )
    }
}
