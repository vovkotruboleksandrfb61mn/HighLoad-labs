import PostgresNIO
import Synchronization

/// The counter of user 1 split over `shards` rows of `user_counter_shard`;
/// its value is their sum.
///
/// Each increment is still one in-place `UPDATE … SET counter = counter + 1`
/// in its own autocommit transaction, durable before the response (fsync and
/// synchronous_commit stay on). What changes is that consecutive increments
/// go to different rows (round robin), so up to `shards` of them can hold
/// their row locks at the same time and their commits can share one WAL
/// flush (PostgreSQL's group commit). With a single row, the row lock is
/// held until the commit's flush returns, so commits can never share one.
public struct PostgresShardedCounterStore: CounterStore {
    public static let userID: Int32 = 1

    private let client: PostgresClient
    public let shards: Int
    private let nextShard: NextShard

    private final class NextShard: Sendable {
        let value = Atomic<Int>(0)
    }

    public init(client: PostgresClient, shards: Int) {
        precondition(shards > 0, "shards must be positive")
        self.client = client
        self.shards = shards
        self.nextShard = NextShard()
    }

    /// Like `PostgresCounterStore.withStore`: runs the pool for as long as
    /// `body` runs. Creates the shard table and its rows if they are missing.
    public static func withStore<Result: Sendable>(
        url: String,
        shards: Int,
        maximumConnections: Int = 20,
        _ body: (PostgresShardedCounterStore) async throws -> Result
    ) async throws -> Result {
        let endpoint = try PostgresEndpoint(url: url)
        let client = PostgresClient(configuration: endpoint.clientConfiguration(maximumConnections: maximumConnections))
        return try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask {
                await client.run()
            }
            do {
                let store = PostgresShardedCounterStore(client: client, shards: shards)
                try await store.createShards()
                let result = try await body(store)
                group.cancelAll()
                return result
            } catch {
                group.cancelAll()
                throw error
            }
        }
    }

    func createShards() async throws {
        try await client.query(
            """
            CREATE TABLE IF NOT EXISTS user_counter_shard (
                user_id int NOT NULL,
                shard int NOT NULL,
                counter bigint NOT NULL,
                PRIMARY KEY (user_id, shard)
            )
            """
        )
        let shardCount = Int32(shards)
        try await client.query(
            """
            INSERT INTO user_counter_shard (user_id, shard, counter)
            SELECT \(Self.userID), s, 0 FROM generate_series(0, \(shardCount) - 1) AS s
            ON CONFLICT (user_id, shard) DO NOTHING
            """
        )
    }

    public func increment() async throws {
        let shard = Int32(nextShard.value.wrappingAdd(1, ordering: .relaxed).oldValue % shards)
        try await client.query(
            "UPDATE user_counter_shard SET counter = counter + 1 WHERE user_id = \(Self.userID) AND shard = \(shard)"
        )
    }

    public func value() async throws -> Int {
        let shardCount = Int32(shards)
        let rows = try await client.query(
            """
            SELECT coalesce(sum(counter), 0)::bigint FROM user_counter_shard
            WHERE user_id = \(Self.userID) AND shard < \(shardCount)
            """
        )
        for try await sum in rows.decode(Int64.self) {
            return Int(sum)
        }
        throw CounterStoreError.missingRow(table: "user_counter_shard", key: Int(Self.userID))
    }

    /// Sets every shard of user 1 back to zero in one transaction.
    public func reset() async throws {
        try await client.query("UPDATE user_counter_shard SET counter = 0 WHERE user_id = \(Self.userID)")
    }
}
