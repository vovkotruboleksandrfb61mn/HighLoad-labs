import NIOCore
import NIOPosix

/// One TCP connection to one node. Any number of tasks can send requests
/// over it at once; the `StreamMultiplexer` matches responses to them by
/// stream id.
///
/// The future-returning methods do no task hop: the request is written from
/// the caller's thread and the future completes on the connection's event
/// loop. The async methods wait on those futures.
public final class CQLConnection: Sendable {
    public let host: String
    public let port: Int
    /// Fails a request with `CQLError.requestTimedOut` when no response came
    /// in time (nil: wait for ever). The server enforces its own, shorter
    /// timeouts and reports them as errors, so this only catches dead nodes.
    public let requestTimeout: TimeAmount?
    let channel: any Channel

    private init(host: String, port: Int, channel: any Channel, requestTimeout: TimeAmount?) {
        self.host = host
        self.port = port
        self.channel = channel
        self.requestTimeout = requestTimeout
    }

    /// Connects to `host:port` and completes the STARTUP handshake.
    public static func connect(
        host: String,
        port: Int = 9042,
        requestTimeout: TimeAmount? = .seconds(12),
        eventLoopGroup: any EventLoopGroup = MultiThreadedEventLoopGroup.singleton
    ) async throws -> CQLConnection {
        let channel = try await ClientBootstrap(group: eventLoopGroup)
            .channelOption(.tcpOption(.tcp_nodelay), value: 1)
            .connectTimeout(.seconds(10))
            .channelInitializer { channel in
                channel.eventLoop.makeCompletedFuture {
                    try channel.pipeline.syncOperations.addHandlers(
                        ByteToMessageHandler(FrameDecoder()),
                        MessageToByteHandler(FrameEncoder()),
                        StreamMultiplexer()
                    )
                }
            }
            .connect(host: host, port: port)
            .get()
        let connection = CQLConnection(host: host, port: port, channel: channel, requestTimeout: requestTimeout)
        do {
            try await connection.startup()
            return connection
        } catch {
            try? await connection.close()
            throw error
        }
    }

    private func startup() async throws {
        let frame = try await send(.startup(options: Request.defaultStartupOptions)).get()
        switch try Response.decode(frame) {
        case .ready:
            return
        case .authenticate(let authenticator):
            throw CQLError.authenticationRequired(authenticator)
        default:
            throw CQLError.unexpectedResponse(frame.opcode)
        }
    }

    public var eventLoop: any EventLoop { channel.eventLoop }

    public var isActive: Bool { channel.isActive }

    // MARK: Raw requests

    /// Sends `request` and completes with the raw response frame.
    public func send(_ request: Request) -> EventLoopFuture<Frame> {
        let promise = channel.eventLoop.makePromise(of: Frame.self)
        if let requestTimeout {
            let timeout = channel.eventLoop.scheduleTask(in: requestTimeout) {
                promise.fail(CQLError.requestTimedOut)
            }
            promise.futureResult.whenComplete { _ in timeout.cancel() }
        }
        channel.writeAndFlush(OutgoingRequest(frame: request.frame(allocator: channel.allocator), response: promise))
            .whenFailure { promise.fail($0) }
        return promise.futureResult
    }

    // MARK: Statements

    /// Supported STARTUP options (OPTIONS / SUPPORTED).
    public func options() async throws -> [String: [String]] {
        let frame = try await send(.options).get()
        guard case .supported(let options) = try Response.decode(frame) else {
            throw CQLError.unexpectedResponse(frame.opcode)
        }
        return options
    }

    /// Runs an unprepared statement. Values are serialized as they are,
    /// without type information: bind a bigint column with `.bigint`.
    public func queryFuture(_ query: String, _ values: [CQLValue] = [], consistency: Consistency = .one) -> EventLoopFuture<CQLResult> {
        let parameters: QueryParameters
        do {
            parameters = QueryParameters(consistency: consistency, values: try values.map { value throws(CQLError) in try value.serialized() })
        } catch {
            return channel.eventLoop.makeFailedFuture(error)
        }
        return send(.query(query, parameters)).flatMapThrowing { frame in
            try Self.result(of: frame)
        }
    }

    public func query(_ query: String, _ values: [CQLValue] = [], consistency: Consistency = .one) async throws -> CQLResult {
        try await queryFuture(query, values, consistency: consistency).get()
    }

    public func prepareFuture(_ query: String) -> EventLoopFuture<PreparedStatement> {
        send(.prepare(query)).flatMapThrowing { frame in
            guard case .prepared(let statement) = try Self.result(of: frame, query: query) else {
                throw CQLError.unexpectedResponse(frame.opcode)
            }
            return statement
        }
    }

    public func prepare(_ query: String) async throws -> PreparedStatement {
        try await prepareFuture(query).get()
    }

    /// Executes a prepared statement. A node that lost the statement
    /// (Unprepared) has not run it, so it is prepared again and resent; that
    /// is safe even for a counter increment.
    public func executeFuture(_ statement: PreparedStatement, _ values: [CQLValue], consistency: Consistency = .one) -> EventLoopFuture<CQLResult> {
        let parameters: QueryParameters
        do {
            parameters = try Self.parameters(for: statement, values, consistency: consistency)
        } catch {
            return channel.eventLoop.makeFailedFuture(error)
        }
        return executeFuture(statement, parameters: parameters)
    }

    func executeFuture(_ statement: PreparedStatement, parameters: QueryParameters) -> EventLoopFuture<CQLResult> {
        send(.execute(id: statement.id, parameters)).flatMap { frame in
            do {
                return self.channel.eventLoop.makeSucceededFuture(try Self.result(of: frame, knownLayout: statement.resultLayout))
            } catch let error as CQLError {
                guard case .unprepared = error.serverError?.details else {
                    return self.channel.eventLoop.makeFailedFuture(error)
                }
                return self.prepareFuture(statement.query).flatMap { _ in
                    self.send(.execute(id: statement.id, parameters)).flatMapThrowing { frame in
                        try Self.result(of: frame, knownLayout: statement.resultLayout)
                    }
                }
            } catch {
                return self.channel.eventLoop.makeFailedFuture(error)
            }
        }
    }

    public func execute(_ statement: PreparedStatement, _ values: [CQLValue] = [], consistency: Consistency = .one) async throws -> CQLResult {
        try await executeFuture(statement, values, consistency: consistency).get()
    }

    /// Closes the connection; pending requests fail with `connectionClosed`.
    public func close() async throws {
        do {
            try await channel.close()
        } catch ChannelError.alreadyClosed {
            // Closed by the node or by an error already.
        }
    }

    // MARK: Helpers

    static func parameters(for statement: PreparedStatement, _ values: [CQLValue], consistency: Consistency) throws(CQLError) -> QueryParameters {
        guard values.count == statement.parameters.count else {
            throw .invalidArgument("\(values.count) values for \(statement.parameters.count) bind markers")
        }
        var serialized: [ByteBuffer?] = []
        serialized.reserveCapacity(values.count)
        for (value, parameter) in zip(values, statement.parameters) {
            serialized.append(try value.serialized(as: parameter.type))
        }
        return QueryParameters(consistency: consistency, values: serialized, skipMetadata: statement.resultLayout != nil)
    }

    static func result(of frame: Frame, query: String = "", knownLayout: ColumnLayout? = nil) throws(CQLError) -> CQLResult {
        guard case .result(let result) = try Response.decode(frame, query: query, knownLayout: knownLayout) else {
            throw .unexpectedResponse(frame.opcode)
        }
        return result
    }
}
