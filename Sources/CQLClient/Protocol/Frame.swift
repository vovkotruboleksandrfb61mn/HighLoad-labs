import NIOCore

/// The protocol version this client speaks.
///
/// Version 4 keeps the plain 9-byte frame header for the whole life of the
/// connection. Version 5 (Cassandra 4.0+) wraps frames into checksummed
/// segments once STARTUP has been answered; it buys nothing for small
/// counter statements on loopback, so we stay on v4, which Cassandra 4.1
/// still supports in full.
public enum ProtocolVersion {
    public static let request: UInt8 = 0x04
    /// Responses set the direction bit.
    public static let response: UInt8 = 0x84
}

/// Frame header flags.
public struct FrameFlags: OptionSet, Sendable, Hashable {
    public let rawValue: UInt8

    public init(rawValue: UInt8) {
        self.rawValue = rawValue
    }

    public static let compression = FrameFlags(rawValue: 0x01)
    public static let tracing = FrameFlags(rawValue: 0x02)
    public static let customPayload = FrameFlags(rawValue: 0x04)
    public static let warning = FrameFlags(rawValue: 0x08)
}

/// Message kinds (the opcode byte of the header).
public enum Opcode: UInt8, Sendable {
    case error = 0x00
    case startup = 0x01
    case ready = 0x02
    case authenticate = 0x03
    case options = 0x05
    case supported = 0x06
    case query = 0x07
    case result = 0x08
    case prepare = 0x09
    case execute = 0x0A
    case register = 0x0B
    case event = 0x0C
    case batch = 0x0D
    case authChallenge = 0x0E
    case authResponse = 0x0F
    case authSuccess = 0x10
}

/// One protocol frame: the 9-byte header and the body.
///
/// On the wire: version (1), flags (1), stream id (int16), opcode (1),
/// body length (int32), all big-endian.
public struct Frame: Sendable, Equatable {
    public static let headerSize = 9

    public var version: UInt8
    public var flags: FrameFlags
    /// Responses carry the stream id of their request; server-pushed events
    /// use -1.
    public var stream: Int16
    public var opcode: Opcode
    public var body: ByteBuffer

    public init(version: UInt8 = ProtocolVersion.request, flags: FrameFlags = [], stream: Int16 = 0, opcode: Opcode, body: ByteBuffer = ByteBuffer()) {
        self.version = version
        self.flags = flags
        self.stream = stream
        self.opcode = opcode
        self.body = body
    }
}

/// Writes frames.
public struct FrameEncoder: MessageToByteEncoder {
    public typealias OutboundIn = Frame

    public init() {}

    public func encode(data frame: Frame, out: inout ByteBuffer) throws {
        out.reserveCapacity(minimumWritableBytes: Frame.headerSize + frame.body.readableBytes)
        out.writeInteger(frame.version)
        out.writeInteger(frame.flags.rawValue)
        out.writeInteger(frame.stream)
        out.writeInteger(frame.opcode.rawValue)
        out.writeInteger(Int32(frame.body.readableBytes))
        out.writeImmutableBuffer(frame.body)
    }
}

/// Splits the byte stream into frames.
public struct FrameDecoder: ByteToMessageDecoder {
    public typealias InboundOut = Frame

    /// The protocol's limit on a frame body: 256 MiB.
    public static let defaultMaximumBodyLength = 256 * 1024 * 1024

    public let maximumBodyLength: Int

    public init(maximumBodyLength: Int = defaultMaximumBodyLength) {
        self.maximumBodyLength = maximumBodyLength
    }

    public mutating func decode(context: ChannelHandlerContext, buffer: inout ByteBuffer) throws -> DecodingState {
        guard let frame = try Self.parse(&buffer, maximumBodyLength: maximumBodyLength) else {
            return .needMoreData
        }
        context.fireChannelRead(Self.wrapInboundOut(frame))
        return .continue
    }

    public mutating func decodeLast(context: ChannelHandlerContext, buffer: inout ByteBuffer, seenEOF: Bool) throws -> DecodingState {
        try decode(context: context, buffer: &buffer)
    }

    /// Consumes one whole frame from the front of `buffer`, or nothing if the
    /// frame has not fully arrived yet.
    public static func parse(_ buffer: inout ByteBuffer, maximumBodyLength: Int = defaultMaximumBodyLength) throws(CQLError) -> Frame? {
        let start = buffer.readerIndex
        guard buffer.readableBytes >= Frame.headerSize,
            let version = buffer.getInteger(at: start, as: UInt8.self),
            let flags = buffer.getInteger(at: start + 1, as: UInt8.self),
            let stream = buffer.getInteger(at: start + 2, as: Int16.self),
            let rawOpcode = buffer.getInteger(at: start + 4, as: UInt8.self),
            let rawLength = buffer.getInteger(at: start + 5, as: Int32.self)
        else {
            return nil
        }
        guard version & 0x7F == ProtocolVersion.request else {
            throw .protocolViolation("unsupported protocol version 0x\(String(version, radix: 16))")
        }
        guard let opcode = Opcode(rawValue: rawOpcode) else {
            throw .protocolViolation("unknown opcode 0x\(String(rawOpcode, radix: 16))")
        }
        let length = Int(rawLength)
        guard length >= 0, length <= maximumBodyLength else {
            throw .protocolViolation("frame body length \(length) is out of range")
        }
        guard buffer.readableBytes >= Frame.headerSize + length else {
            return nil
        }
        buffer.moveReaderIndex(forwardBy: Frame.headerSize)
        let body = buffer.readSlice(length: length)!
        return Frame(version: version, flags: FrameFlags(rawValue: flags), stream: stream, opcode: opcode, body: body)
    }
}
