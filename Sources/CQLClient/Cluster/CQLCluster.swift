import NIOCore
import NIOPosix
import Synchronization

/// Connections to every node of a cluster, with round-robin or token-aware
/// request routing and a cache of prepared statements.
///
/// Kept deliberately small: no reconnection (a node that goes away is skipped
/// until the cluster object is recreated), no paging, no retries except the
/// re-prepare after Unprepared. Errors are reported to the caller as they
/// come, because a counter increment must not be repeated blindly.
///
/// ```swift
/// let cluster = try await CQLCluster.connect(.init(contactPoints: ["127.0.0.1"]))
/// let update = try await cluster.prepare("UPDATE ks.t SET n = n + 1 WHERE id = ?")
/// _ = try await cluster.execute(update, ["row"], consistency: .quorum)
/// try await cluster.close()
/// ```
public final class CQLCluster: Sendable {
    public struct Configuration: Sendable {
        public var contactPoints: [String]
        /// The native protocol port of every node.
        public var port: Int
        /// Connections opened to each node; requests rotate over them.
        public var connectionsPerNode: Int
        /// Also connect to the nodes listed in system.peers.
        public var discoverPeers: Bool
        /// Send a prepared statement whose markers cover the partition key
        /// to the key's primary replica (Murmur3Partitioner only). For a
        /// counter that replica is also the leader that applies the
        /// increment, which saves the coordinator a forwarding hop.
        public var tokenAware: Bool
        public var requestTimeout: TimeAmount?
        public var eventLoopGroup: any EventLoopGroup

        public init(
            contactPoints: [String],
            port: Int = 9042,
            connectionsPerNode: Int = 1,
            discoverPeers: Bool = true,
            tokenAware: Bool = true,
            requestTimeout: TimeAmount? = .seconds(12),
            eventLoopGroup: any EventLoopGroup = MultiThreadedEventLoopGroup.singleton
        ) {
            self.contactPoints = contactPoints
            self.port = port
            self.connectionsPerNode = connectionsPerNode
            self.discoverPeers = discoverPeers
            self.tokenAware = tokenAware
            self.requestTimeout = requestTimeout
            self.eventLoopGroup = eventLoopGroup
        }
    }

    /// One node and the connections to it.
    final class Node: Sendable {
        let host: String
        let connections: [CQLConnection]
        private let next = Atomic<Int>(0)

        init(host: String, connections: [CQLConnection]) {
            self.host = host
            self.connections = connections
        }

        /// The next live connection, round robin.
        func connection() -> CQLConnection? {
            guard !connections.isEmpty else { return nil }
            let start = next.wrappingAdd(1, ordering: .relaxed).oldValue
            for offset in 0..<connections.count {
                let connection = connections[(start &+ offset) % connections.count]
                if connection.isActive {
                    return connection
                }
            }
            return nil
        }
    }

    public let configuration: Configuration
    let nodes: [Node]
    /// Ring positions refer to `nodes` indices; nil unless token-aware.
    let ring: TokenRing?
    private let nextNode = Atomic<Int>(0)
    private let preparedStatements = Mutex<[String: PreparedStatement]>([:])

    private init(configuration: Configuration, nodes: [Node], ring: TokenRing?) {
        self.configuration = configuration
        self.nodes = nodes
        self.ring = ring
    }

    /// The hosts the cluster has connections to.
    public var hosts: [String] { nodes.filter { !$0.connections.isEmpty }.map(\.host) }

    // MARK: Connecting

    public static func connect(_ configuration: Configuration) async throws -> CQLCluster {
        guard !configuration.contactPoints.isEmpty, configuration.connectionsPerNode > 0 else {
            throw CQLError.invalidArgument("need at least one contact point and one connection per node")
        }
        var nodes: [Node] = []
        var lastError: any Error = CQLError.noHostAvailable
        for host in configuration.contactPoints where !nodes.contains(where: { $0.host == host }) {
            do {
                nodes.append(try await connectNode(host, configuration))
            } catch {
                lastError = error
            }
        }
        guard let first = nodes.first?.connection() else {
            throw lastError
        }

        var tokensByHost: [String: [Int64]] = [:]
        if configuration.discoverPeers || configuration.tokenAware {
            let local = try await first.query("SELECT tokens FROM system.local", consistency: .one)
            tokensByHost[first.host] = local.rows.first.map { tokens(in: $0["tokens"]) } ?? []
            let peers = try await first.query("SELECT peer, rpc_address, tokens FROM system.peers", consistency: .one)
            for row in peers.rows {
                guard let host = (row["rpc_address"].inetAddress ?? row["peer"].inetAddress) else { continue }
                tokensByHost[host] = tokens(in: row["tokens"])
                if configuration.discoverPeers && !nodes.contains(where: { $0.host == host }) {
                    // A peer that is down stays in the ring without
                    // connections; its keys go round robin.
                    let connections = (try? await connectNode(host, configuration))?.connections ?? []
                    nodes.append(Node(host: host, connections: connections))
                }
            }
        }

        var ring: TokenRing?
        if configuration.tokenAware {
            let tokensByNode = nodes.map { tokensByHost[$0.host] ?? [] }
            let candidate = TokenRing(tokensByNode: tokensByNode)
            ring = candidate.isEmpty ? nil : candidate
        }
        return CQLCluster(configuration: configuration, nodes: nodes, ring: ring)
    }

    public static func withCluster<Result: Sendable>(
        _ configuration: Configuration,
        _ body: (CQLCluster) async throws -> Result
    ) async throws -> Result {
        let cluster = try await connect(configuration)
        do {
            let result = try await body(cluster)
            try await cluster.close()
            return result
        } catch {
            try? await cluster.close()
            throw error
        }
    }

    private static func connectNode(_ host: String, _ configuration: Configuration) async throws -> Node {
        var connections: [CQLConnection] = []
        do {
            for _ in 0..<configuration.connectionsPerNode {
                connections.append(
                    try await CQLConnection.connect(
                        host: host,
                        port: configuration.port,
                        requestTimeout: configuration.requestTimeout,
                        eventLoopGroup: configuration.eventLoopGroup
                    )
                )
            }
        } catch {
            for connection in connections { try? await connection.close() }
            throw error
        }
        return Node(host: host, connections: connections)
    }

    private static func tokens(in value: CQLValue) -> [Int64] {
        (value.elements ?? []).compactMap { $0.string.flatMap { Int64($0) } }
    }

    public func close() async throws {
        for node in nodes {
            for connection in node.connections {
                try? await connection.close()
            }
        }
    }

    // MARK: Routing

    /// Any live connection, rotating over the nodes.
    func anyConnection() throws(CQLError) -> CQLConnection {
        let start = nextNode.wrappingAdd(1, ordering: .relaxed).oldValue
        for offset in 0..<nodes.count {
            if let connection = nodes[(start &+ offset) % nodes.count].connection() {
                return connection
            }
        }
        throw .noHostAvailable
    }

    /// The connection a request with this routing key should go to.
    func connection(routingKey: ByteBuffer?) throws(CQLError) -> CQLConnection {
        if let ring, let routingKey, let node = ring.primary(for: Murmur3Partitioner.token(of: routingKey)),
            let connection = nodes[node].connection()
        {
            return connection
        }
        return try anyConnection()
    }

    /// The host that a prepared statement with `values` is sent to.
    public func host(for statement: PreparedStatement, _ values: [CQLValue]) throws -> String {
        let parameters = try CQLConnection.parameters(for: statement, values, consistency: .one)
        return try connection(routingKey: Self.routingKey(statement, parameters)).host
    }

    static func routingKey(_ statement: PreparedStatement, _ parameters: QueryParameters) -> ByteBuffer? {
        guard !statement.partitionKeyIndices.isEmpty else { return nil }
        var components: [ByteBuffer] = []
        for index in statement.partitionKeyIndices {
            guard index < parameters.values.count, let value = parameters.values[index] else { return nil }
            components.append(value)
        }
        return Murmur3Partitioner.routingKey(components)
    }

    // MARK: Statements

    /// Runs an unprepared statement on any node. After a schema change it
    /// waits until every reachable node reports the same schema version, so
    /// the next statement finds the new table wherever it lands.
    public func query(_ query: String, _ values: [CQLValue] = [], consistency: Consistency = .one) async throws -> CQLResult {
        let connection = try anyConnection()
        let result = try await connection.query(query, values, consistency: consistency)
        if case .schemaChange = result {
            try await waitForSchemaAgreement(on: connection)
        }
        return result
    }

    /// Prepares `query` on every node (each node has its own statement
    /// cache), once per query string.
    public func prepare(_ query: String) async throws -> PreparedStatement {
        if let cached = preparedStatements.withLock({ $0[query] }) {
            return cached
        }
        let connections = nodes.compactMap { $0.connection() }
        guard !connections.isEmpty else { throw CQLError.noHostAvailable }
        // One node failing to prepare is fine: Unprepared re-prepares there.
        let results = await withTaskGroup(of: Result<PreparedStatement, any Error>.self) { group in
            for connection in connections {
                group.addTask {
                    do { return .success(try await connection.prepare(query)) } catch { return .failure(error) }
                }
            }
            return await group.reduce(into: []) { $0.append($1) }
        }
        guard let statement = results.lazy.compactMap({ try? $0.get() }).first else {
            _ = try results[0].get()
            throw CQLError.noHostAvailable
        }
        preparedStatements.withLock { $0[query] = statement }
        return statement
    }

    public func executeFuture(_ statement: PreparedStatement, _ values: [CQLValue], consistency: Consistency = .one) -> EventLoopFuture<CQLResult> {
        do {
            let parameters = try CQLConnection.parameters(for: statement, values, consistency: consistency)
            let connection = try connection(routingKey: Self.routingKey(statement, parameters))
            return connection.executeFuture(statement, parameters: parameters)
        } catch {
            return configuration.eventLoopGroup.any().makeFailedFuture(error)
        }
    }

    public func execute(_ statement: PreparedStatement, _ values: [CQLValue] = [], consistency: Consistency = .one) async throws -> CQLResult {
        try await executeFuture(statement, values, consistency: consistency).get()
    }

    private func waitForSchemaAgreement(on connection: CQLConnection, timeout: Duration = .seconds(10)) async throws {
        let clock = ContinuousClock()
        let deadline = clock.now.advanced(by: timeout)
        let liveHosts = Set(nodes.filter { node in node.connection() != nil }.map(\.host))
        while clock.now < deadline {
            var versions: Set<CQLValue> = []
            let local = try await connection.query("SELECT schema_version FROM system.local", consistency: .one)
            versions.formUnion(local.rows.map { $0["schema_version"] })
            let peers = try await connection.query("SELECT rpc_address, schema_version FROM system.peers", consistency: .one)
            for row in peers.rows where liveHosts.contains(row["rpc_address"].inetAddress ?? "") {
                versions.insert(row["schema_version"])
            }
            if versions.count <= 1 {
                return
            }
            try await Task.sleep(for: .milliseconds(200))
        }
    }
}
