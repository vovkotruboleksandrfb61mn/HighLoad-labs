import NIOCore
import NIOPosix

/// One TCP connection to one member (unisocket mode). Any number of tasks
/// can send requests over it at once; responses are matched back to them by
/// correlation id.
final class Connection: Sendable {
    /// How long the connection may stay silent before we ping the member.
    static let heartbeatInterval = TimeAmount.seconds(5)
    /// How long a request keeps being retried while the member reports it
    /// did not execute it (the Java client's default invocation timeout).
    static let retryTimeout = Duration.seconds(120)

    private let channel: any Channel
    private let registry: InvocationRegistry

    private init(channel: any Channel, registry: InvocationRegistry) {
        self.channel = channel
        self.registry = registry
    }

    static func connect(
        host: String,
        port: Int,
        eventLoopGroup: any EventLoopGroup
    ) async throws -> Connection {
        let registry = InvocationRegistry()
        let channel = try await ClientBootstrap(group: eventLoopGroup)
            .channelOption(.tcpOption(.tcp_nodelay), value: 1)
            .connectTimeout(.seconds(10))
            .channelInitializer { channel in
                channel.eventLoop.makeCompletedFuture {
                    try channel.pipeline.syncOperations.addHandlers(
                        ProtocolHeaderHandler(),
                        IdleStateHandler(writeTimeout: heartbeatInterval),
                        ByteToMessageHandler(ClientMessageDecoder()),
                        MessageToByteHandler(ClientMessageEncoder()),
                        InvocationHandler(registry: registry)
                    )
                }
            }
            .connect(host: host, port: port)
            .get()
        return Connection(channel: channel, registry: registry)
    }

    /// Requests sent and still waiting for their response.
    var pendingRequests: Int {
        registry.pendingCount
    }

    /// Sends `request` and returns the response of type `responseType`.
    ///
    /// An error response becomes `HazelcastError.server`. Errors that mean
    /// the member did not execute the request (a CP group without a leader
    /// yet, a partition in migration) are retried with backoff for up to
    /// `retryTimeout`, which is safe even for non-idempotent requests.
    func invoke(_ request: ClientMessage, expecting responseType: Int32) async throws -> ClientMessage {
        let clock = ContinuousClock()
        let deadline = clock.now.advanced(by: Self.retryTimeout)
        var backoff = Duration.milliseconds(10)
        while true {
            let response = try await send(request)
            switch response.messageType {
            case responseType:
                return response
            case ErrorsCodec.messageType:
                let error = try ErrorsCodec.decode(response)
                guard error.isRetrySafe, clock.now.advanced(by: backoff) < deadline else {
                    throw HazelcastError.server(error)
                }
                try await Task.sleep(for: backoff, clock: clock)
                backoff = min(backoff * 2, .milliseconds(500))
            default:
                throw HazelcastError.unexpectedResponse(expected: responseType, received: response.messageType)
            }
        }
    }

    /// Sends `request` once under a fresh correlation id and waits for
    /// whatever comes back.
    func send(_ request: ClientMessage) async throws -> ClientMessage {
        var request = request
        let id = registry.makeCorrelationID()
        request.correlationID = id
        let message = request
        let registry = self.registry
        let channel = self.channel
        return try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { continuation in
                guard registry.register(id, continuation) else {
                    return
                }
                channel.writeAndFlush(message).whenFailure { error in
                    registry.fail(id, with: error)
                }
            }
        } onCancel: {
            registry.cancel(id)
        }
    }

    /// `invoke(_:expecting:)` without a task: sends `request` from the
    /// caller's thread and completes the future on the connection's event
    /// loop when the response arrives. Only an error response that has to
    /// be retried falls back to the async `invoke`.
    func invokeFuture(_ request: ClientMessage, expecting responseType: Int32) -> EventLoopFuture<ClientMessage> {
        sendFuture(request).flatMap { response in
            switch response.messageType {
            case responseType:
                return self.channel.eventLoop.makeSucceededFuture(response)
            case ErrorsCodec.messageType:
                return self.channel.eventLoop.makeFutureWithTask {
                    let error = try ErrorsCodec.decode(response)
                    guard error.isRetrySafe else {
                        throw HazelcastError.server(error)
                    }
                    return try await self.invoke(request, expecting: responseType)
                }
            default:
                return self.channel.eventLoop.makeFailedFuture(
                    HazelcastError.unexpectedResponse(expected: responseType, received: response.messageType)
                )
            }
        }
    }

    /// `send(_:)` without a task.
    func sendFuture(_ request: ClientMessage) -> EventLoopFuture<ClientMessage> {
        var request = request
        let id = registry.makeCorrelationID()
        request.correlationID = id
        let promise = channel.eventLoop.makePromise(of: ClientMessage.self)
        if registry.register(id, promise: promise) {
            let registry = self.registry
            channel.writeAndFlush(request).whenFailure { error in
                registry.fail(id, with: error)
            }
        }
        return promise.futureResult
    }

    /// Closes the connection; pending requests fail with `connectionClosed`.
    func close() async throws {
        do {
            try await channel.close()
        } catch ChannelError.alreadyClosed {
            // Closed by the member or by an error; nothing left to do.
        }
        registry.close(with: HazelcastError.connectionClosed)
    }
}
