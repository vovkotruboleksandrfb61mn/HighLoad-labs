import CassandraClient
import Synchronization

/// The likes counter as an ordinary `bigint` column instead of a `counter`,
/// incremented by reading the value and writing it back plus one.
///
/// - `readModifyWrite`: `SELECT` then `UPDATE ... SET value = v + 1`, both at
///   the store's consistency level. Two clients that read the same value
///   both write v + 1, and one increment is lost (last write wins).
/// - `lightweightTransaction`: the write is conditional,
///   `UPDATE ... SET value = v + 1 IF value = v`, decided by Paxos among the
///   replicas. A client whose condition fails reads again and retries, so
///   no increment is lost, at the price of four round trips per attempt.
public final class CassandraValueCounterStore: CassandraBenchStore {
    public enum Method: String, Sendable, CaseIterable {
        case readModifyWrite = "rmw"
        case lightweightTransaction = "lwt"
    }

    private let client: CassandraClient
    private let method: Method
    private let readStatement: CassandraClient.PreparedStatement
    private let writeStatement: CassandraClient.PreparedStatement
    private let options: CassandraClient.Statement.Options
    private let location: CassandraCounterLocation
    private let failedConditions = Atomic<Int>(0)

    /// Conditional updates whose condition did not hold (another client
    /// changed the value in between), so the increment was tried again.
    public var conflicts: Int { failedConditions.load(ordering: .relaxed) }

    private init(client: CassandraClient, method: Method, location: CassandraCounterLocation, consistency: CassandraConsistency) async throws {
        self.client = client
        self.method = method
        self.location = location
        self.options = CassandraClient.Statement.Options(consistency: consistency.driverLevel)
        let table = location.qualifiedTable
        self.readStatement = try await client.prepare("SELECT value FROM \(table) WHERE id = ?")
        switch method {
        case .readModifyWrite:
            self.writeStatement = try await client.prepare("UPDATE \(table) SET value = ? WHERE id = ?")
        case .lightweightTransaction:
            self.writeStatement = try await client.prepare("UPDATE \(table) SET value = ? WHERE id = ? IF value = ?")
        }
    }

    /// Connects like `CassandraCounterStore.open`; the table is
    /// `<keyspace>.likes_value (id text PRIMARY KEY, value bigint)`.
    public static func open(
        contactPoints: [String],
        port: Int,
        consistency: CassandraConsistency,
        method: Method,
        keyspace: String = "counter_rf3"
    ) async throws -> CassandraValueCounterStore {
        let location = CassandraCounterLocation(keyspace: keyspace, table: "likes_value")
        let client = CassandraClient(configuration: CassandraCounterStore.configuration(contactPoints: contactPoints, port: port))
        do {
            let all = CassandraClient.Statement.Options(consistency: .all)
            try await CassandraCounterStore.createKeyspace(location, on: client)
            try await client.run("CREATE TABLE IF NOT EXISTS \(location.qualifiedTable) (id text PRIMARY KEY, value bigint)", options: all)
            return try await CassandraValueCounterStore(client: client, method: method, location: location, consistency: consistency)
        } catch {
            try? await client.shutdownAsync()
            throw error
        }
    }

    public func close() async throws {
        try await client.shutdownAsync()
    }

    public func increment() async throws {
        switch method {
        case .readModifyWrite:
            let current = try await read(options)
            _ = try await client.execute(prepared: writeStatement, parameters: [.int64(Int64(current + 1)), .string(location.row)], options: options)
        case .lightweightTransaction:
            // The read is SERIAL, so it also sees a value an unfinished Paxos
            // round is about to commit.
            let serial = CassandraClient.Statement.Options(consistency: .serial)
            while true {
                let current = try await read(serial)
                let rows = try await client.execute(
                    prepared: writeStatement,
                    parameters: [.int64(Int64(current + 1)), .string(location.row), .int64(Int64(current))],
                    options: options
                )
                var applied = false
                for row in rows {
                    let flag: Bool? = row.column("[applied]")
                    applied = flag ?? false
                }
                if applied { return }
                failedConditions.add(1, ordering: .relaxed)
            }
        }
    }

    public func value() async throws -> Int {
        try await read(options)
    }

    public func value(consistency: CassandraConsistency?) async throws -> Int {
        try await read(consistency.map { CassandraClient.Statement.Options(consistency: $0.driverLevel) } ?? options)
    }

    private func read(_ options: CassandraClient.Statement.Options) async throws -> Int {
        let rows = try await client.execute(prepared: readStatement, parameters: [.string(location.row)], options: options)
        for row in rows {
            let value: Int64? = row.column(0)
            return Int(value ?? 0)
        }
        return 0
    }

    /// Empties the table and writes the row with value 0, so a conditional
    /// update always has a value to compare with.
    public func reset() async throws {
        let all = CassandraClient.Statement.Options(consistency: .all)
        try await client.run("TRUNCATE \(location.qualifiedTable)", options: all)
        try await client.run("INSERT INTO \(location.qualifiedTable) (id, value) VALUES (?, 0)", parameters: [.string(location.row)], options: all)
    }
}

/// What cassandra-bench needs from a counter store: the counter itself, a
/// read at a chosen level, and closing the connections.
public protocol CassandraBenchStore: CounterStore {
    func value(consistency: CassandraConsistency?) async throws -> Int
    func close() async throws
}
