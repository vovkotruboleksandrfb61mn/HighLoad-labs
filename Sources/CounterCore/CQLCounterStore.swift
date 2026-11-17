import CQLClient

/// The same counter as `CassandraCounterStore`, through our own CQLClient
/// (native protocol v4 over SwiftNIO) instead of the DataStax C++ driver.
///
/// Requests are multiplexed on the cluster's connections (one per node by
/// default), and with `tokenAware` each increment goes straight to the
/// primary replica of the row, which then leads the counter update itself.
/// Like the DataStax store it never retries an increment.
public final class CQLCounterStore: CassandraBenchStore {
    private let cluster: CQLCluster
    private let incrementStatement: PreparedStatement
    private let readStatement: PreparedStatement
    private let consistency: Consistency
    private let location: CassandraCounterLocation
    private let key: [CQLValue]

    private init(cluster: CQLCluster, location: CassandraCounterLocation, consistency: CassandraConsistency) async throws {
        self.cluster = cluster
        self.location = location
        self.consistency = consistency.cqlLevel
        self.key = [.text(location.row)]
        let table = location.qualifiedTable
        self.incrementStatement = try await cluster.prepare("UPDATE \(table) SET likes = likes + 1 WHERE id = ?")
        self.readStatement = try await cluster.prepare("SELECT likes FROM \(table) WHERE id = ?")
    }

    /// Connects to the cluster through `contactPoints` (every node answers
    /// CQL on `port`) and creates the keyspace and table if they are missing.
    /// The store must be closed with `close()`.
    public static func open(
        contactPoints: [String],
        port: Int,
        consistency: CassandraConsistency,
        location: CassandraCounterLocation = CassandraCounterLocation(),
        tokenAware: Bool = true,
        connectionsPerNode: Int = 1
    ) async throws -> CQLCounterStore {
        let configuration = CQLCluster.Configuration(
            contactPoints: contactPoints,
            port: port,
            connectionsPerNode: connectionsPerNode,
            tokenAware: tokenAware
        )
        let cluster = try await CQLCluster.connect(configuration)
        do {
            try await createSchema(location, on: cluster)
            return try await CQLCounterStore(cluster: cluster, location: location, consistency: consistency)
        } catch {
            try? await cluster.close()
            throw error
        }
    }

    /// The node the increments are sent to.
    public var coordinator: String? {
        try? cluster.host(for: incrementStatement, key)
    }

    public func close() async throws {
        try await cluster.close()
    }

    /// Creates the keyspace (SimpleStrategy) and the counter table at
    /// consistency ALL, like `CassandraCounterStore`.
    private static func createSchema(_ location: CassandraCounterLocation, on cluster: CQLCluster) async throws {
        _ = try await cluster.query(
            """
            CREATE KEYSPACE IF NOT EXISTS \(location.keyspace) \
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': \(location.replicationFactor)}
            """,
            consistency: .all
        )
        _ = try await cluster.query(
            "CREATE TABLE IF NOT EXISTS \(location.qualifiedTable) (id text PRIMARY KEY, likes counter)",
            consistency: .all
        )
    }

    public func increment() async throws {
        _ = try await cluster.executeFuture(incrementStatement, key, consistency: consistency).get()
    }

    public func value() async throws -> Int {
        try await value(consistency: nil)
    }

    /// The counter read at `consistency` (the store's own level when nil).
    public func value(consistency: CassandraConsistency?) async throws -> Int {
        let level = consistency?.cqlLevel ?? self.consistency
        let rows = try await cluster.execute(readStatement, key, consistency: level).rows
        return rows.first?["likes"].int ?? 0
    }

    /// Empties the table; see `CassandraCounterStore.reset()`.
    public func reset() async throws {
        _ = try await cluster.query("TRUNCATE \(location.qualifiedTable)", consistency: .all)
    }
}

extension CassandraConsistency {
    var cqlLevel: Consistency {
        switch self {
        case .one: .one
        case .two: .two
        case .quorum: .quorum
        case .all: .all
        }
    }
}
