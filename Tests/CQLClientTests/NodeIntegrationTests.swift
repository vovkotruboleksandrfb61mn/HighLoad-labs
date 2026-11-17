@testable import CQLClient
import Foundation
import NIOCore
import Testing

/// Runs against a live node: `CQL_INTEGRATION=1 CQL_HOSTS=127.0.0.1
/// CQL_PORT=9042 swift test --filter NodeIntegrationTests`. It creates the
/// keyspaces `cqlclient_test` (RF 1) and `cqlclient_test_rf3` (RF 3, only to
/// provoke Unavailable on a cluster with fewer than three nodes).
enum Integration {
    static var enabled: Bool { ProcessInfo.processInfo.environment["CQL_INTEGRATION"] == "1" }

    static var hosts: [String] {
        (ProcessInfo.processInfo.environment["CQL_HOSTS"] ?? "127.0.0.1").split(separator: ",").map(String.init)
    }

    static var port: Int { ProcessInfo.processInfo.environment["CQL_PORT"].flatMap { Int($0) } ?? 9042 }

    static func configuration(tokenAware: Bool = true) -> CQLCluster.Configuration {
        CQLCluster.Configuration(contactPoints: hosts, port: port, tokenAware: tokenAware)
    }
}

@Suite("Node integration", .enabled(if: Integration.enabled, "set CQL_INTEGRATION=1 with a running node"), .serialized)
struct NodeIntegrationTests {
    static let keyspace = "cqlclient_test"

    static func schema(_ cluster: CQLCluster) async throws {
        _ = try await cluster.query(
            "CREATE KEYSPACE IF NOT EXISTS \(keyspace) WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
            consistency: .all
        )
        _ = try await cluster.query(
            "CREATE TABLE IF NOT EXISTS \(keyspace).values (id text PRIMARY KEY, i int, b bigint, u uuid, t text)",
            consistency: .all
        )
        _ = try await cluster.query("CREATE TABLE IF NOT EXISTS \(keyspace).likes (id text PRIMARY KEY, likes counter)", consistency: .all)
    }

    @Test("STARTUP, OPTIONS and system.local")
    func handshake() async throws {
        let connection = try await CQLConnection.connect(host: Integration.hosts[0], port: Integration.port)
        let options = try await connection.options()
        #expect(options["CQL_VERSION"]?.isEmpty == false)
        let local = try await connection.query("SELECT release_version, tokens FROM system.local")
        #expect(local.rows.first?["release_version"].string?.hasPrefix("4.") == true)
        #expect(local.rows.first?["tokens"].elements?.isEmpty == false)
        try await connection.close()
    }

    @Test("values round-trip through prepared INSERT and SELECT")
    func values() async throws {
        try await CQLCluster.withCluster(Integration.configuration()) { cluster in
            try await Self.schema(cluster)
            let uuid = UUID()
            let insert = try await cluster.prepare("INSERT INTO \(Self.keyspace).values (id, i, b, u, t) VALUES (?, ?, ?, ?, ?)")
            #expect(insert.partitionKeyIndices == [0])
            _ = try await cluster.execute(insert, ["row-1", .int(-7), .bigint(1 << 40), .uuid(uuid), .text("Привіт")], consistency: .one)
            _ = try await cluster.execute(insert, ["row-2", .null, .null, .null, .null])

            let select = try await cluster.prepare("SELECT i, b, u, t FROM \(Self.keyspace).values WHERE id = ?")
            let row = try #require(try await cluster.execute(select, ["row-1"]).rows.first)
            #expect(row["i"] == .int(-7))
            #expect(row["b"] == .bigint(1 << 40))
            #expect(row["u"].uuidValue == uuid)
            #expect(row["t"].string == "Привіт")
            let nulls = try #require(try await cluster.execute(select, ["row-2"]).rows.first)
            #expect(nulls.values.allSatisfy { $0.isNull })

            // Unprepared statements bind without type information.
            let text = try await cluster.query("SELECT b FROM \(Self.keyspace).values WHERE id = ?", [.text("row-1")])
            #expect(text.rows.first?["b"].int64 == 1 << 40)
        }
    }

    @Test("tokens computed client-side match token() on the server")
    func tokens() async throws {
        try await CQLCluster.withCluster(Integration.configuration()) { cluster in
            try await Self.schema(cluster)
            let keys = (0..<50).map { "key-\($0)-\(String(repeating: "é", count: $0 % 7))" }
            let insert = try await cluster.prepare("INSERT INTO \(Self.keyspace).values (id) VALUES (?)")
            for key in keys {
                _ = try await cluster.execute(insert, [.text(key)])
            }
            let select = try await cluster.prepare("SELECT token(id) FROM \(Self.keyspace).values WHERE id = ?")
            for key in keys {
                let server = try await cluster.execute(select, [.text(key)]).rows.first?[0].int64
                #expect(server == Murmur3Partitioner.token(of: Array(key.utf8)), "\(key)")
            }
        }
    }

    @Test("thousands of increments in flight on one connection all count")
    func multiplexedCounter() async throws {
        let configuration = CQLCluster.Configuration(contactPoints: [Integration.hosts[0]], port: Integration.port, discoverPeers: false, tokenAware: false)
        try await CQLCluster.withCluster(configuration) { cluster in
            try await Self.schema(cluster)
            let row = "multiplexed-\(UUID())"
            let update = try await cluster.prepare("UPDATE \(Self.keyspace).likes SET likes = likes + 1 WHERE id = ?")
            let total = 2_000
            // All futures are created before the first is awaited: the
            // connection carries up to `total` requests at once.
            let futures = (0..<total).map { _ in cluster.executeFuture(update, [.text(row)], consistency: .one) }
            for future in futures {
                _ = try await future.get()
            }
            let select = try await cluster.prepare("SELECT likes FROM \(Self.keyspace).likes WHERE id = ?")
            let likes = try await cluster.execute(select, [.text(row)], consistency: .all).rows.first?["likes"]
            #expect(likes == .counter(Int64(total)))
        }
    }

    @Test("server errors come back typed")
    func errors() async throws {
        try await CQLCluster.withCluster(Integration.configuration()) { cluster in
            await #expect(throws: CQLError.self) { _ = try await cluster.query("SELEC nothing") }
            do {
                _ = try await cluster.query("SELEC nothing")
            } catch let error as CQLError {
                #expect(error.serverError?.code == .syntaxError)
            }
            do {
                _ = try await cluster.query("SELECT * FROM \(Self.keyspace).no_such_table")
                Issue.record("no error")
            } catch let error as CQLError {
                #expect(error.serverError?.code == .invalid)
            }

            guard cluster.hosts.count < 3 else { return }
            _ = try await cluster.query(
                "CREATE KEYSPACE IF NOT EXISTS cqlclient_test_rf3 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}",
                consistency: .one
            )
            _ = try await cluster.query("CREATE TABLE IF NOT EXISTS cqlclient_test_rf3.likes (id text PRIMARY KEY, likes counter)", consistency: .one)
            let update = try await cluster.prepare("UPDATE cqlclient_test_rf3.likes SET likes = likes + 1 WHERE id = ?")
            do {
                _ = try await cluster.execute(update, ["x"], consistency: .all)
                Issue.record("no error")
            } catch let error as CQLError {
                #expect(error.serverError?.code == .unavailable)
                #expect(error.serverError?.details == .unavailable(consistency: .all, required: 3, alive: Int32(cluster.hosts.count)))
            }
        }
    }
}
