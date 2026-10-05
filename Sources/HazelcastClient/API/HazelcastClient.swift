import NIOCore
import NIOPosix
import Synchronization

/// A client for a Hazelcast cluster, speaking the Open Binary Client
/// Protocol 2.x over one connection to one member (unisocket mode). The
/// member forwards every request to wherever its partition or CP group lives.
///
/// ```swift
/// try await HazelcastClient.withClient(to: .localMember) { client in
///     let counter = try await client.cpSubsystem.atomicLong(named: "counter")
///     try await counter.incrementAndGet()
/// }
/// ```
public final class HazelcastClient: Sendable {
    /// Where a member listens for clients.
    public struct Address: Sendable, Hashable, CustomStringConvertible {
        public var host: String
        public var port: Int

        public init(host: String, port: Int) {
            self.host = host
            self.port = port
        }

        /// The port a member listens on unless configured otherwise.
        public static let defaultPort = 5701

        /// A member on this machine at the default port.
        public static let localMember = Address(host: "127.0.0.1", port: defaultPort)

        /// Parses `host:port`; the port defaults to `defaultPort`.
        public init(parsing text: String) throws(HazelcastError) {
            guard let colon = text.lastIndex(of: ":") else {
                self.init(host: text, port: Self.defaultPort)
                return
            }
            guard let port = Int(text[text.index(after: colon)...]), (1...65535).contains(port) else {
                throw .invalidArgument("not a host:port address: \(text)")
            }
            self.init(host: String(text[..<colon]), port: port)
        }

        public var description: String { "\(host):\(port)" }
    }

    /// The version this client announces; Hazelcast 5.4 speaks protocol 2.7.
    static let clientVersion = "5.4.0"
    /// The `clientType` sent at authentication.
    static let clientType = "SWF"
    static let serializationVersion: UInt8 = 1

    let connection: Connection
    /// Number of partitions in the cluster, from the authentication response.
    public let partitionCount: Int32
    public let clusterID: HazelcastUUID?
    public let memberUUID: HazelcastUUID?
    public let serverVersion: String
    public let clientUUID: HazelcastUUID
    private let lockReferenceIDs = Atomic<Int64>(1)

    private init(
        connection: Connection,
        authentication: ClientAuthenticationCodec.Response,
        clientUUID: HazelcastUUID
    ) {
        self.connection = connection
        self.partitionCount = authentication.partitionCount
        self.clusterID = authentication.clusterID
        self.memberUUID = authentication.memberUUID
        self.serverVersion = authentication.serverHazelcastVersion
        self.clientUUID = clientUUID
    }

    /// Connects to the member at `address` and authenticates with `clusterName`.
    public static func connect(
        to address: Address = .localMember,
        clusterName: String = "dev",
        clientName: String = "swift-client",
        eventLoopGroup: any EventLoopGroup = MultiThreadedEventLoopGroup.singleton
    ) async throws -> HazelcastClient {
        let connection = try await Connection.connect(
            host: address.host,
            port: address.port,
            eventLoopGroup: eventLoopGroup
        )
        do {
            let clientUUID = HazelcastUUID.random()
            let request = ClientAuthenticationCodec.encodeRequest(
                .init(
                    clusterName: clusterName,
                    clientUUID: clientUUID,
                    clientType: clientType,
                    serializationVersion: serializationVersion,
                    clientHazelcastVersion: clientVersion,
                    clientName: clientName
                )
            )
            let response = try ClientAuthenticationCodec.decodeResponse(
                try await connection.invoke(request, expecting: ClientAuthenticationCodec.responseType)
            )
            guard let status = AuthenticationStatus(rawValue: response.status) else {
                throw HazelcastError.protocolViolation("unknown authentication status \(response.status)")
            }
            guard status == .authenticated else {
                throw HazelcastError.authenticationFailed(status)
            }
            guard response.partitionCount > 0 else {
                throw HazelcastError.protocolViolation("member reported \(response.partitionCount) partitions")
            }
            return HazelcastClient(connection: connection, authentication: response, clientUUID: clientUUID)
        } catch {
            try? await connection.close()
            throw error
        }
    }

    /// Connects, runs `body` with the client, and shuts the client down
    /// whether `body` returns or throws.
    public static func withClient<Result: Sendable>(
        to address: Address = .localMember,
        clusterName: String = "dev",
        clientName: String = "swift-client",
        eventLoopGroup: any EventLoopGroup = MultiThreadedEventLoopGroup.singleton,
        _ body: (HazelcastClient) async throws -> Result
    ) async throws -> Result {
        let client = try await connect(
            to: address,
            clusterName: clusterName,
            clientName: clientName,
            eventLoopGroup: eventLoopGroup
        )
        let result: Result
        do {
            result = try await body(client)
        } catch {
            try? await client.shutdown()
            throw error
        }
        try await client.shutdown()
        return result
    }

    /// Closes the connection. Requests still in flight fail with
    /// `HazelcastError.connectionClosed`; the member releases this client's
    /// map locks.
    public func shutdown() async throws {
        try await connection.close()
    }

    /// A distributed map with typed keys and values.
    public func map<Key: HazelcastSerializable, Value: HazelcastSerializable>(
        named name: String,
        key: Key.Type = Key.self,
        value: Value.Type = Value.self
    ) -> HazelcastMap<Key, Value> {
        HazelcastMap(name: name, client: self)
    }

    /// The CP Subsystem: strongly consistent data structures on Raft.
    public var cpSubsystem: CPSubsystem {
        CPSubsystem(client: self)
    }

    func nextLockReferenceID() -> Int64 {
        lockReferenceIDs.wrappingAdd(1, ordering: .relaxed).oldValue
    }

    func invoke(_ request: ClientMessage, expecting responseType: Int32) async throws -> ClientMessage {
        try await connection.invoke(request, expecting: responseType)
    }

    func invokeFuture(_ request: ClientMessage, expecting responseType: Int32) -> EventLoopFuture<ClientMessage> {
        connection.invokeFuture(request, expecting: responseType)
    }

    /// Like `invoke`, but routed to the partition that owns `key`.
    func invoke(
        _ request: ClientMessage,
        on key: HazelcastData,
        expecting responseType: Int32
    ) async throws -> ClientMessage {
        var request = request
        request.partitionID = key.partitionID(partitionCount: partitionCount)
        return try await connection.invoke(request, expecting: responseType)
    }
}
