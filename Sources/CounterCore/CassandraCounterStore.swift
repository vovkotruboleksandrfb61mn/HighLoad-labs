import CassandraClient

/// A Cassandra `counter` column, incremented in place with
/// `UPDATE ... SET likes = likes + 1` at a chosen consistency level.
///
/// The client (apple/swift-cassandra-client over the DataStax C++ driver)
/// keeps its own connections to every node and sends each statement to a
/// replica of the row, so the store needs no pool of its own.
///
/// A counter increment is not idempotent: a statement that times out may or
/// may not have been applied, and repeating it may count twice. The store
/// therefore never retries, and a failed increment is reported to the caller.
public final class CassandraCounterStore: CassandraBenchStore {
    private let client: CassandraClient
    private let incrementStatement: CassandraClient.PreparedStatement
    private let readStatement: CassandraClient.PreparedStatement
    private let options: CassandraClient.Statement.Options
    private let location: CassandraCounterLocation

    private init(client: CassandraClient, location: CassandraCounterLocation, consistency: CassandraConsistency) async throws {
        self.client = client
        self.location = location
        self.options = CassandraClient.Statement.Options(consistency: consistency.driverLevel)
        let table = location.qualifiedTable
        self.incrementStatement = try await client.prepare("UPDATE \(table) SET likes = likes + 1 WHERE id = ?")
        self.readStatement = try await client.prepare("SELECT likes FROM \(table) WHERE id = ?")
    }

    /// Connects to the cluster through `contactPoints` (every node answers
    /// CQL on `port`) and creates the keyspace and table if they are missing.
    /// The store must be closed with `close()`.
    public static func open(
        contactPoints: [String],
        port: Int,
        consistency: CassandraConsistency,
        location: CassandraCounterLocation = CassandraCounterLocation()
    ) async throws -> CassandraCounterStore {
        let client = CassandraClient(configuration: configuration(contactPoints: contactPoints, port: port))
        do {
            try await createSchema(location, on: client)
            return try await CassandraCounterStore(client: client, location: location, consistency: consistency)
        } catch {
            try? await client.shutdownAsync()
            throw error
        }
    }

    /// Opens a store, runs `body` with it, and closes it again.
    public static func withStore<Result: Sendable>(
        contactPoints: [String],
        port: Int,
        consistency: CassandraConsistency,
        location: CassandraCounterLocation = CassandraCounterLocation(),
        _ body: (CassandraCounterStore) async throws -> Result
    ) async throws -> Result {
        let store = try await open(contactPoints: contactPoints, port: port, consistency: consistency, location: location)
        do {
            let result = try await body(store)
            try await store.close()
            return result
        } catch {
            try? await store.close()
            throw error
        }
    }

    public func close() async throws {
        try await client.shutdownAsync()
    }

    static func configuration(contactPoints: [String], port: Int) -> CassandraClient.Configuration {
        var configuration = CassandraClient.Configuration(
            contactPointsProvider: { $0(.success(contactPoints)) },
            port: Int32(port),
            protocolVersion: .v4
        )
        configuration.requestTimeoutMillis = 12_000
        configuration.tcpNodelay = true
        return configuration
    }

    /// Creates the keyspace (SimpleStrategy). Schema changes need every node
    /// up (consistency ALL).
    static func createKeyspace(_ location: CassandraCounterLocation, on client: CassandraClient) async throws {
        try await client.run(
            """
            CREATE KEYSPACE IF NOT EXISTS \(location.keyspace) \
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': \(location.replicationFactor)}
            """,
            options: CassandraClient.Statement.Options(consistency: .all)
        )
    }

    /// Creates the keyspace and the counter table.
    private static func createSchema(_ location: CassandraCounterLocation, on client: CassandraClient) async throws {
        let all = CassandraClient.Statement.Options(consistency: .all)
        try await createKeyspace(location, on: client)
        try await client.run(
            "CREATE TABLE IF NOT EXISTS \(location.qualifiedTable) (id text PRIMARY KEY, likes counter)",
            options: all
        )
    }

    public func increment() async throws {
        _ = try await client.execute(prepared: incrementStatement, parameters: [.string(location.row)], options: options)
    }

    public func value() async throws -> Int {
        try await value(consistency: nil)
    }

    /// The counter read at `consistency` (the store's own level when nil).
    public func value(consistency: CassandraConsistency?) async throws -> Int {
        let options = consistency.map { CassandraClient.Statement.Options(consistency: $0.driverLevel) } ?? self.options
        let rows = try await client.execute(prepared: readStatement, parameters: [.string(location.row)], options: options)
        for row in rows {
            let likes: Int64? = row.column(0)
            return Int(likes ?? 0)
        }
        return 0
    }

    /// Counters cannot be set, and a deleted counter must not be incremented
    /// again, so the reset empties the table. TRUNCATE needs every node up.
    public func reset() async throws {
        try await client.run("TRUNCATE \(location.qualifiedTable)", options: CassandraClient.Statement.Options(consistency: .all))
    }
}

/// The consistency levels the counter is run with.
public enum CassandraConsistency: String, CaseIterable, Sendable {
    case one
    case two
    case quorum
    case all

    var driverLevel: CassandraClient.Consistency {
        switch self {
        case .one: .one
        case .two: .two
        case .quorum: .quorum
        case .all: .all
        }
    }
}

/// Where the counter lives: a counter table in a keyspace with
/// SimpleStrategy replication.
public struct CassandraCounterLocation: Sendable {
    public var keyspace: String
    public var replicationFactor: Int
    public var table: String
    public var row: String

    public init(keyspace: String = "counter_rf3", replicationFactor: Int = 3, table: String = "likes", row: String = "post-1") {
        self.keyspace = keyspace
        self.replicationFactor = replicationFactor
        self.table = table
        self.row = row
    }

    var qualifiedTable: String { "\(keyspace).\(table)" }
}
