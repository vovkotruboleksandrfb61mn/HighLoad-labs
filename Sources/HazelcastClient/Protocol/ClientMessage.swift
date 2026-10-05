import NIOCore

/// A message of the client protocol: a list of frames, the first of which
/// (the initial frame) carries the message type, the correlation id, the
/// partition id and the fixed-size parameters.
public struct ClientMessage: Sendable, Equatable {
    public var frames: [Frame]

    public init(frames: [Frame]) {
        self.frames = frames
    }

    /// A request whose initial frame has room for `fixedSize` bytes of
    /// fixed-size parameters after the 16-byte request header. The partition
    /// id starts as -1 (any member), the correlation id as 0.
    init(requestType: Int32, fixedSize: Int = 0) {
        let size = ClientProtocol.requestHeaderSize + fixedSize
        var initial = Frame(
            flags: .unfragmentedMessage,
            content: ByteBuffer(repeating: 0, count: size)
        )
        initial.setInt32(requestType, at: ClientProtocol.messageTypeOffset)
        initial.setInt32(-1, at: ClientProtocol.partitionIDOffset)
        self.frames = [initial]
    }

    var initialFrame: Frame {
        get { frames[0] }
        set { frames[0] = newValue }
    }

    public var messageType: Int32 {
        (try? initialFrame.int32(at: ClientProtocol.messageTypeOffset)) ?? -1
    }

    public var correlationID: Int64 {
        get { (try? initialFrame.int64(at: ClientProtocol.correlationIDOffset)) ?? -1 }
        set { initialFrame.setInt64(newValue, at: ClientProtocol.correlationIDOffset) }
    }

    /// Only meaningful for requests: responses keep the backup ack count here.
    public var partitionID: Int32 {
        get { (try? initialFrame.int32(at: ClientProtocol.partitionIDOffset)) ?? -1 }
        set { initialFrame.setInt32(newValue, at: ClientProtocol.partitionIDOffset) }
    }

    var isEvent: Bool { initialFrame.flags.contains(.isEvent) }

    var isUnfragmented: Bool { initialFrame.flags.isSuperset(of: .unfragmentedMessage) }

    /// Iterates over the frames for decoding.
    func makeFrameIterator() -> FrameIterator {
        FrameIterator(frames: frames)
    }
}

// MARK: - Encoding parameters

extension ClientMessage {
    /// Sets a fixed-size `long` request parameter; `offset` counts from the
    /// start of the fixed-size parameters.
    mutating func setRequestInt64(_ value: Int64, at offset: Int) {
        initialFrame.setInt64(value, at: ClientProtocol.requestHeaderSize + offset)
    }

    mutating func setRequestUInt8(_ value: UInt8, at offset: Int) {
        initialFrame.setUInt8(value, at: ClientProtocol.requestHeaderSize + offset)
    }

    mutating func setRequestUUID(_ value: HazelcastUUID?, at offset: Int) {
        initialFrame.setUUID(value, at: ClientProtocol.requestHeaderSize + offset)
    }

    /// A `String` is one frame with its UTF-8 bytes.
    mutating func append(string: String) {
        frames.append(Frame(content: ByteBuffer(string: string)))
    }

    mutating func append(nullableString string: String?) {
        if let string {
            append(string: string)
        } else {
            frames.append(.null)
        }
    }

    /// A `Data` is one frame with its serialized bytes.
    mutating func append(data: HazelcastData) {
        frames.append(Frame(content: ByteBuffer(bytes: data.bytes)))
    }

    /// A `List<String>`: begin frame, one frame per string, end frame.
    mutating func append(stringList strings: [String]) {
        frames.append(.beginDataStructure)
        for string in strings {
            append(string: string)
        }
        frames.append(.endDataStructure)
    }

    mutating func appendBeginDataStructure() {
        frames.append(.beginDataStructure)
    }

    mutating func appendEndDataStructure() {
        frames.append(.endDataStructure)
    }
}
