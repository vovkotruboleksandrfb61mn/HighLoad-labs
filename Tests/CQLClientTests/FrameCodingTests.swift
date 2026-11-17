@testable import CQLClient
import NIOCore
import NIOEmbedded
import Testing

func hex(_ bytes: some Sequence<UInt8>) -> String {
    bytes.map { byte in
        let digits = String(byte, radix: 16)
        return digits.count == 1 ? "0" + digits : digits
    }.joined()
}

func wireHex(_ frame: Frame) throws -> String {
    var buffer = ByteBuffer()
    try FrameEncoder().encode(data: frame, out: &buffer)
    return hex(buffer.readableBytesView)
}

@Suite("Frame encoding and decoding")
struct FrameCodingTests {
    @Test("STARTUP is a 9-byte header and a string map")
    func startup() throws {
        var frame = Request.startup(options: ["CQL_VERSION": "3.0.0"]).frame()
        frame.stream = 1
        // 04 version, 00 flags, 0001 stream, 01 opcode, 00000016 length,
        // then {CQL_VERSION: 3.0.0} as [string map].
        #expect(
            try wireHex(frame)
                == "04" + "00" + "0001" + "01" + "00000016"
                + "0001" + "000b" + hex(Array("CQL_VERSION".utf8)) + "0005" + hex(Array("3.0.0".utf8))
        )
    }

    @Test("QUERY carries the long-string query, consistency, flags and values")
    func query() throws {
        let parameters = QueryParameters(consistency: .quorum, values: [try CQLValue.bigint(7).serialized(), nil])
        var frame = Request.query("SELECT 1", parameters).frame()
        frame.stream = 0x0102
        #expect(
            try wireHex(frame)
                == "04" + "00" + "0102" + "07" + "00000021"
                + "00000008" + hex(Array("SELECT 1".utf8))
                + "0004" + "01" + "0002"
                + "00000008" + "0000000000000007"
                + "ffffffff"
        )
    }

    @Test("EXECUTE carries the short-bytes id and asks to skip metadata")
    func execute() throws {
        let parameters = QueryParameters(consistency: .one, values: [try CQLValue.text("a").serialized()], skipMetadata: true)
        let frame = Request.execute(id: [0xAB, 0xCD], parameters).frame()
        #expect(try wireHex(frame) == "04" + "00" + "0000" + "0a" + "0000000e" + "0002abcd" + "0001" + "03" + "0001" + "00000001" + "61")
    }

    @Test("a frame survives encode and decode, and partial input waits")
    func roundTrip() throws {
        var body = ByteBuffer()
        body.writeInteger(Int32(1))
        let frame = Frame(version: ProtocolVersion.response, flags: [.warning], stream: 300, opcode: .result, body: body)
        var encoded = ByteBuffer()
        try FrameEncoder().encode(data: frame, out: &encoded)
        let wire = Array(encoded.readableBytesView)

        var buffer = ByteBuffer()
        var frames: [Frame] = []
        for byte in wire {
            buffer.writeInteger(byte)
            if let decoded = try FrameDecoder.parse(&buffer) {
                frames.append(decoded)
            }
        }
        #expect(frames == [frame])
        #expect(buffer.readableBytes == 0)
    }

    @Test("an unknown version or an oversized body is rejected")
    func invalidHeaders() {
        var v3 = ByteBuffer(bytes: [0x83, 0, 0, 0, 0x02, 0, 0, 0, 0])
        #expect(throws: CQLError.self) { try FrameDecoder.parse(&v3) }
        var big = ByteBuffer(bytes: [0x84, 0, 0, 0, 0x08, 0, 0, 0x10, 0])
        #expect(throws: CQLError.self) { try FrameDecoder.parse(&big, maximumBodyLength: 1024) }
    }

    @Test("the NIO handlers encode and decode through a channel")
    func pipeline() throws {
        let channel = EmbeddedChannel(handlers: [
            ByteToMessageHandler(FrameDecoder()),
            MessageToByteHandler(FrameEncoder()),
        ])
        defer { _ = try? channel.finish() }
        let first = Request.options.frame()
        var second = Request.prepare("SELECT now() FROM system.local").frame()
        second.stream = 9
        try channel.writeOutbound(first)
        try channel.writeOutbound(second)
        var wire = ByteBuffer()
        while var chunk = try channel.readOutbound(as: ByteBuffer.self) {
            wire.writeBuffer(&chunk)
        }
        // Requests are v4 too, so the decoder accepts them back.
        try channel.writeInbound(wire)
        #expect(try channel.readInbound(as: Frame.self) == first)
        #expect(try channel.readInbound(as: Frame.self) == second)
        #expect(try channel.readInbound(as: Frame.self) == nil)
    }
}
