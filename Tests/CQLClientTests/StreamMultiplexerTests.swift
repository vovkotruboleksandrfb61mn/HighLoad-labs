@testable import CQLClient
import NIOCore
import NIOEmbedded
import Testing

@Suite("Stream multiplexing")
struct StreamMultiplexerTests {
    /// A channel with the multiplexer alone: frames out, frames in.
    func makeChannel(maximumStreams: Int = StreamMultiplexer.defaultMaximumStreams) -> EmbeddedChannel {
        EmbeddedChannel(handler: StreamMultiplexer(maximumStreams: maximumStreams))
    }

    func send(_ request: Request, on channel: EmbeddedChannel) -> EventLoopFuture<Frame> {
        let promise = channel.eventLoop.makePromise(of: Frame.self)
        channel.writeAndFlush(OutgoingRequest(frame: request.frame(), response: promise), promise: nil)
        return promise.futureResult
    }

    func response(to stream: Int16, marker: Int32) -> Frame {
        var body = ByteBuffer()
        body.writeInteger(Int32(3))
        body.writeCQLString("ks\(marker)")
        return Frame(version: ProtocolVersion.response, stream: stream, opcode: .result, body: body)
    }

    func keyspace(of future: EventLoopFuture<Frame>) throws -> String? {
        guard case .result(.setKeyspace(let name)) = try Response.decode(try future.wait()) else { return nil }
        return name
    }

    @Test("concurrent requests get distinct streams and out-of-order responses find their request")
    func outOfOrder() throws {
        let channel = makeChannel()
        defer { _ = try? channel.finish() }
        let futures = (0..<3).map { _ in send(.options, on: channel) }
        let streams = try (0..<3).map { _ in try #require(try channel.readOutbound(as: Frame.self)).stream }
        #expect(streams == [0, 1, 2])

        try channel.writeInbound(response(to: 2, marker: 2))
        try channel.writeInbound(response(to: 0, marker: 0))
        try channel.writeInbound(response(to: 1, marker: 1))
        #expect(try futures.map { try keyspace(of: $0) } == ["ks0", "ks1", "ks2"])
    }

    @Test("when every stream is taken, requests wait and reuse freed ids")
    func exhaustion() throws {
        let channel = makeChannel(maximumStreams: 2)
        defer { _ = try? channel.finish() }
        let futures = (0..<4).map { _ in send(.options, on: channel) }
        #expect(try channel.readOutbound(as: Frame.self)?.stream == 0)
        #expect(try channel.readOutbound(as: Frame.self)?.stream == 1)
        #expect(try channel.readOutbound(as: Frame.self) == nil)

        try channel.writeInbound(response(to: 1, marker: 10))
        #expect(try channel.readOutbound(as: Frame.self)?.stream == 1)
        try channel.writeInbound(response(to: 0, marker: 20))
        #expect(try channel.readOutbound(as: Frame.self)?.stream == 0)
        try channel.writeInbound(response(to: 0, marker: 40))
        try channel.writeInbound(response(to: 1, marker: 30))
        #expect(try futures.map { try keyspace(of: $0) } == ["ks20", "ks10", "ks30", "ks40"])
    }

    @Test("server events on stream -1 are ignored")
    func events() throws {
        let channel = makeChannel()
        defer { _ = try? channel.finish() }
        let future = send(.options, on: channel)
        _ = try channel.readOutbound(as: Frame.self)
        try channel.writeInbound(Frame(version: ProtocolVersion.response, stream: -1, opcode: .event))
        try channel.writeInbound(response(to: 0, marker: 5))
        #expect(try keyspace(of: future) == "ks5")
    }

    @Test("closing the connection fails pending and waiting requests")
    func close() throws {
        let channel = makeChannel(maximumStreams: 1)
        let inFlight = send(.options, on: channel)
        let waiting = send(.options, on: channel)
        _ = try channel.finish(acceptAlreadyClosed: true)
        #expect(throws: CQLError.connectionClosed) { try inFlight.wait() }
        #expect(throws: CQLError.connectionClosed) { try waiting.wait() }
    }

    @Test("a response on an idle stream breaks the connection")
    func idleStream() throws {
        let channel = makeChannel()
        let future = send(.options, on: channel)
        _ = try channel.readOutbound(as: Frame.self)
        try channel.writeInbound(response(to: 7, marker: 0))
        #expect(throws: CQLError.self) { try future.wait() }
        #expect(!channel.isActive)
    }
}
