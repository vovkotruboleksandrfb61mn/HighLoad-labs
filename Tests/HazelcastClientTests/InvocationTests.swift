@testable import HazelcastClient
import NIOCore
import NIOEmbedded
import Testing

@Suite("Matching responses to requests")
struct InvocationTests {
    /// Waits (yielding) until `registry` holds `count` pending requests.
    func waitForPending(_ registry: InvocationRegistry, count: Int) async {
        while registry.pendingCount < count {
            await Task.yield()
        }
    }

    static func response(correlationID: Int64, value: Int64) -> ClientMessage {
        var message = CodecResponseTests.response(type: AtomicLongGetCodec.responseType, fixedSize: 8)
        message.correlationID = correlationID
        message.initialFrame.setInt64(value, at: ClientProtocol.responseHeaderSize)
        return message
    }

    @Test("responses arriving out of order reach the right requests")
    func outOfOrder() async throws {
        let registry = InvocationRegistry()
        let channel = try await NIOAsyncTestingChannel { channel in
            try channel.pipeline.syncOperations.addHandlers(
                ByteToMessageHandler(ClientMessageDecoder()),
                InvocationHandler(registry: registry)
            )
        }

        async let first = withCheckedThrowingContinuation { continuation in
            _ = registry.register(1, continuation)
        }
        async let second = withCheckedThrowingContinuation { continuation in
            _ = registry.register(2, continuation)
        }
        await waitForPending(registry, count: 2)

        var wire = ByteBuffer()
        try ClientMessageEncoder().encode(data: Self.response(correlationID: 2, value: 20), out: &wire)
        try ClientMessageEncoder().encode(data: Self.response(correlationID: 1, value: 10), out: &wire)
        // A response nobody waits for (a heartbeat) is dropped quietly.
        try ClientMessageEncoder().encode(data: Self.response(correlationID: 77, value: 0), out: &wire)
        try await channel.writeInbound(wire)

        #expect(try AtomicLongGetCodec.decodeResponse(await first) == 10)
        #expect(try AtomicLongGetCodec.decodeResponse(await second) == 20)
        #expect(registry.pendingCount == 0)
        _ = try await channel.finish()
    }

    @Test("closing the connection fails every pending request and refuses new ones")
    func closeFailsPending() async throws {
        let registry = InvocationRegistry()
        let channel = try await NIOAsyncTestingChannel { channel in
            try channel.pipeline.syncOperations.addHandler(InvocationHandler(registry: registry))
        }
        try await channel.connect(to: SocketAddress(ipAddress: "127.0.0.1", port: 5701))

        let pending = Task {
            try await withCheckedThrowingContinuation { continuation in
                _ = registry.register(1, continuation)
            }
        }
        await waitForPending(registry, count: 1)
        _ = try await channel.finish()

        await #expect(throws: HazelcastError.connectionClosed) { try await pending.value }
        await #expect(throws: HazelcastError.connectionClosed) {
            try await withCheckedThrowingContinuation { continuation in
                _ = registry.register(2, continuation)
            }
        }
    }

    @Test("cancelling a request fails it with CancellationError")
    func cancellation() async {
        let registry = InvocationRegistry()
        let task = Task {
            try await withTaskCancellationHandler {
                try await withCheckedThrowingContinuation { continuation in
                    _ = registry.register(1, continuation)
                }
            } onCancel: {
                registry.cancel(1)
            }
        }
        await waitForPending(registry, count: 1)
        task.cancel()
        await #expect(throws: CancellationError.self) { try await task.value }
        #expect(registry.pendingCount == 0)
    }

    @Test("a request from an already cancelled task is refused")
    func cancelledBeforeRegistering() async {
        let registry = InvocationRegistry()
        let task = Task {
            while !Task.isCancelled {
                await Task.yield()
            }
            return try await withCheckedThrowingContinuation { continuation in
                _ = registry.register(1, continuation)
            }
        }
        task.cancel()
        await #expect(throws: CancellationError.self) { try await task.value }
        #expect(registry.pendingCount == 0)
    }

    @Test("a request waiting on a promise gets its response, and fails when the connection closes")
    func promiseWaiters() async throws {
        let registry = InvocationRegistry()
        let loop = EmbeddedEventLoop()
        let answered = loop.makePromise(of: ClientMessage.self)
        let stranded = loop.makePromise(of: ClientMessage.self)
        #expect(registry.register(1, promise: answered))
        #expect(registry.register(2, promise: stranded))

        #expect(registry.complete(1, with: Self.response(correlationID: 1, value: 42)))
        #expect(try AtomicLongGetCodec.decodeResponse(try await answered.futureResult.get()) == 42)

        registry.close(with: HazelcastError.connectionClosed)
        await #expect(throws: HazelcastError.connectionClosed) { try await stranded.futureResult.get() }

        let refused = loop.makePromise(of: ClientMessage.self)
        #expect(!registry.register(3, promise: refused))
        await #expect(throws: HazelcastError.connectionClosed) { try await refused.futureResult.get() }
        #expect(registry.pendingCount == 0)
    }

    @Test("correlation ids are unique across tasks")
    func uniqueIDs() async {
        let registry = InvocationRegistry()
        let ids = await withTaskGroup(of: [Int64].self) { group in
            for _ in 0..<8 {
                group.addTask { (0..<1000).map { _ in registry.makeCorrelationID() } }
            }
            return await group.reduce(into: [Int64]()) { $0 += $1 }
        }
        #expect(Set(ids).count == 8000)
    }
}

@Suite("CP proxy names")
struct CPProxyNameTests {
    @Test("plain names and @default stay in the default group")
    func defaultGroup() throws {
        #expect(try CPProxyName("counter") == CPProxyName(withoutDefaultGroup: "counter", objectName: "counter"))
        #expect(try CPProxyName(" counter@default ") == CPProxyName(withoutDefaultGroup: "counter", objectName: "counter"))
    }

    @Test("a custom group stays in the proxy name")
    func customGroup() throws {
        #expect(try CPProxyName("counter@web") == CPProxyName(withoutDefaultGroup: "counter@web", objectName: "counter"))
    }

    @Test("invalid names are rejected")
    func invalid() {
        #expect(throws: HazelcastError.self) { try CPProxyName("a@b@c") }
        #expect(throws: HazelcastError.self) { try CPProxyName("a@metadata") }
        #expect(throws: HazelcastError.self) { try CPProxyName("@group") }
    }

    @Test("addresses parse as host:port")
    func address() throws {
        #expect(try HazelcastClient.Address(parsing: "127.0.0.1:5702") == .init(host: "127.0.0.1", port: 5702))
        #expect(try HazelcastClient.Address(parsing: "localhost") == .init(host: "localhost", port: 5701))
        #expect(throws: HazelcastError.self) { try HazelcastClient.Address(parsing: "host:port") }
    }
}
