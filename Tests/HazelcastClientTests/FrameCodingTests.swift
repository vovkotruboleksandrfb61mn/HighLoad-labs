@testable import HazelcastClient
import NIOCore
import NIOEmbedded
import Testing

@Suite("Frame encoding and decoding")
struct FrameCodingTests {
    /// A request with fixed-size fields, strings, a null, a data structure
    /// and an empty frame.
    static func sampleMessage() -> ClientMessage {
        var message = ClientMessage(requestType: 0x010100, fixedSize: 16)
        message.correlationID = 42
        message.partitionID = 17
        message.setRequestInt64(-1, at: 0)
        message.setRequestInt64(Int64.max, at: 8)
        message.append(string: "counters")
        message.append(nullableString: nil)
        message.append(stringList: ["a", "bc"])
        message.append(data: Int64(5).hazelcastData())
        message.append(string: "")
        return message
    }

    @Test("a message survives encode and parse unchanged")
    func roundTrip() throws {
        let message = Self.sampleMessage()
        var buffer = ByteBuffer()
        try ClientMessageEncoder().encode(data: message, out: &buffer)

        var parser = ClientMessageParser()
        var decoded: ClientMessage?
        var frames = 0
        loop: while true {
            switch try parser.step(&buffer) {
            case .needMoreData:
                break loop
            case .frame:
                frames += 1
            case .message(let complete):
                frames += 1
                decoded = complete
            }
        }
        #expect(decoded == message)
        #expect(frames == message.frames.count)
        #expect(buffer.readableBytes == 0)
        #expect(decoded?.correlationID == 42)
        #expect(decoded?.partitionID == 17)
        #expect(decoded?.messageType == 0x010100)
    }

    @Test("only the last frame carries IS_FINAL on the wire")
    func finalFlag() throws {
        var buffer = ByteBuffer()
        try ClientMessageEncoder().encode(data: Self.sampleMessage(), out: &buffer)
        var finals: [Bool] = []
        while buffer.readableBytes > 0 {
            let length = Int(buffer.readInteger(endianness: .little, as: Int32.self)!)
            let flags = FrameFlags(rawValue: buffer.readInteger(endianness: .little, as: UInt16.self)!)
            buffer.moveReaderIndex(forwardBy: length - 6)
            finals.append(flags.contains(.isFinal))
        }
        #expect(finals == Array(repeating: false, count: finals.count - 1) + [true])
    }

    @Test("the parser waits for a frame delivered one byte at a time")
    func partialFrames() throws {
        var encoded = ByteBuffer()
        try ClientMessageEncoder().encode(data: Self.sampleMessage(), out: &encoded)
        let wire = Array(encoded.readableBytesView)

        var parser = ClientMessageParser()
        var buffer = ByteBuffer()
        var messages: [ClientMessage] = []
        for byte in wire {
            buffer.writeInteger(byte)
            while true {
                let step = try parser.step(&buffer)
                if case .message(let message) = step {
                    messages.append(message)
                }
                if step == .needMoreData {
                    break
                }
            }
        }
        #expect(messages == [Self.sampleMessage()])
    }

    @Test("a frame shorter than its header is rejected")
    func invalidLength() {
        var buffer = ByteBuffer(bytes: [5, 0, 0, 0, 0, 0])
        var parser = ClientMessageParser()
        #expect(throws: HazelcastError.self) { try parser.step(&buffer) }
    }

    @Test("a frame over the size limit is rejected")
    func oversizedFrame() {
        var buffer = ByteBuffer(bytes: [0, 0, 0, 1, 0, 0])
        var parser = ClientMessageParser(maximumFrameLength: 1024)
        #expect(throws: HazelcastError.self) { try parser.step(&buffer) }
    }

    @Test("the NIO handlers encode and decode through a channel")
    func pipeline() throws {
        let channel = EmbeddedChannel(handlers: [
            ByteToMessageHandler(ClientMessageDecoder()),
            MessageToByteHandler(ClientMessageEncoder()),
        ])
        defer { _ = try? channel.finish() }

        let first = Self.sampleMessage()
        var second = ClientPingCodec.encodeRequest()
        second.correlationID = 7
        try channel.writeOutbound(first)
        try channel.writeOutbound(second)

        // Feed everything written back in as one chunk: two messages come out.
        var wire = ByteBuffer()
        while let chunk = try channel.readOutbound(as: ByteBuffer.self) {
            var chunk = chunk
            wire.writeBuffer(&chunk)
        }
        try channel.writeInbound(wire)
        #expect(try channel.readInbound(as: ClientMessage.self) == first)
        #expect(try channel.readInbound(as: ClientMessage.self) == second)
        #expect(try channel.readInbound(as: ClientMessage.self) == nil)
    }

    @Test("the protocol header handler sends CP2 when the channel becomes active")
    func protocolHeader() throws {
        let channel = EmbeddedChannel(handler: ProtocolHeaderHandler())
        defer { _ = try? channel.finish() }
        try channel.connect(to: SocketAddress(ipAddress: "127.0.0.1", port: 5701)).wait()
        let header = try channel.readOutbound(as: ByteBuffer.self)
        #expect(header.map { Array($0.readableBytesView) } == ClientProtocol.initialBytes)
    }
}
