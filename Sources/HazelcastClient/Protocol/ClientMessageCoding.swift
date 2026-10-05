import NIOCore

/// Writes the frames of a message, setting IS_FINAL on the last one.
public struct ClientMessageEncoder: MessageToByteEncoder {
    public typealias OutboundIn = ClientMessage

    public init() {}

    public func encode(data message: ClientMessage, out: inout ByteBuffer) throws {
        let lastIndex = message.frames.index(before: message.frames.endIndex)
        for (index, frame) in zip(message.frames.indices, message.frames) {
            var flags = frame.flags
            if index == lastIndex {
                flags.insert(.isFinal)
            }
            out.writeInteger(Int32(frame.encodedLength), endianness: .little)
            out.writeInteger(flags.rawValue, endianness: .little)
            out.writeImmutableBuffer(frame.content)
        }
    }
}

/// Splits the byte stream into frames and groups them into messages: a
/// message ends with the frame that carries IS_FINAL.
public struct ClientMessageParser: Sendable {
    public enum Step: Sendable, Equatable {
        /// The buffer does not hold a whole frame yet; nothing was consumed.
        case needMoreData
        /// A frame was consumed but the message goes on.
        case frame
        /// A frame was consumed and it completed this message.
        case message(ClientMessage)
    }

    /// The default `maximumFrameLength`: 16 MiB.
    public static let defaultMaximumFrameLength = 16 * 1024 * 1024

    /// Frames longer than this are treated as a broken stream.
    public let maximumFrameLength: Int
    private var pendingFrames: [Frame] = []

    public init(maximumFrameLength: Int = defaultMaximumFrameLength) {
        self.maximumFrameLength = maximumFrameLength
    }

    /// Consumes at most one frame from the front of `buffer`.
    public mutating func step(_ buffer: inout ByteBuffer) throws(HazelcastError) -> Step {
        guard let rawLength = buffer.getInteger(at: buffer.readerIndex, endianness: .little, as: Int32.self) else {
            return .needMoreData
        }
        let length = Int(rawLength)
        guard length >= ClientProtocol.frameHeaderSize else {
            throw .protocolViolation("frame length \(length) is shorter than the frame header")
        }
        guard length <= maximumFrameLength else {
            throw .protocolViolation("frame length \(length) exceeds the limit of \(maximumFrameLength) bytes")
        }
        guard buffer.readableBytes >= length else {
            return .needMoreData
        }

        buffer.moveReaderIndex(forwardBy: MemoryLayout<Int32>.size)
        guard let rawFlags = buffer.readInteger(endianness: .little, as: UInt16.self),
            let content = buffer.readSlice(length: length - ClientProtocol.frameHeaderSize)
        else {
            preconditionFailure("a frame of \(length) bytes was readable a moment ago")
        }
        var flags = FrameFlags(rawValue: rawFlags)
        let isFinal = flags.contains(.isFinal)
        flags.remove(.isFinal)
        pendingFrames.append(Frame(flags: flags, content: content))
        guard isFinal else {
            return .frame
        }
        defer { pendingFrames.removeAll(keepingCapacity: true) }
        return .message(ClientMessage(frames: pendingFrames))
    }
}

/// `ByteToMessageDecoder` around `ClientMessageParser`.
public struct ClientMessageDecoder: ByteToMessageDecoder {
    public typealias InboundOut = ClientMessage

    private var parser: ClientMessageParser

    public init(maximumFrameLength: Int = ClientMessageParser.defaultMaximumFrameLength) {
        self.parser = ClientMessageParser(maximumFrameLength: maximumFrameLength)
    }

    public mutating func decode(context: ChannelHandlerContext, buffer: inout ByteBuffer) throws -> DecodingState {
        switch try parser.step(&buffer) {
        case .needMoreData:
            return .needMoreData
        case .frame:
            return .continue
        case .message(let message):
            context.fireChannelRead(wrapInboundOut(message))
            return .continue
        }
    }

    public mutating func decodeLast(
        context: ChannelHandlerContext,
        buffer: inout ByteBuffer,
        seenEOF: Bool
    ) throws -> DecodingState {
        while try decode(context: context, buffer: &buffer) == .continue {}
        return .needMoreData
    }
}
