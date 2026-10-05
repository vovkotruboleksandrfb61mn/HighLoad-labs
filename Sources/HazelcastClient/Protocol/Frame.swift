import NIOCore

/// The flags in a frame header.
public struct FrameFlags: OptionSet, Sendable, Hashable {
    public let rawValue: UInt16

    public init(rawValue: UInt16) {
        self.rawValue = rawValue
    }

    public static let beginFragment = FrameFlags(rawValue: 1 << 15)
    public static let endFragment = FrameFlags(rawValue: 1 << 14)
    /// The last frame of a message.
    public static let isFinal = FrameFlags(rawValue: 1 << 13)
    public static let beginDataStructure = FrameFlags(rawValue: 1 << 12)
    public static let endDataStructure = FrameFlags(rawValue: 1 << 11)
    public static let isNull = FrameFlags(rawValue: 1 << 10)
    public static let isEvent = FrameFlags(rawValue: 1 << 9)
    public static let backupAware = FrameFlags(rawValue: 1 << 8)
    public static let backupEvent = FrameFlags(rawValue: 1 << 7)

    /// The flags of a message's initial frame when the message is not split
    /// into fragments.
    public static let unfragmentedMessage: FrameFlags = [.beginFragment, .endFragment]
}

/// One frame of a client message: flags and a payload.
///
/// On the wire a frame is an `int32` length (which counts the 6-byte header),
/// the `uint16` flags, then the content. The IS_FINAL flag is not stored here:
/// the encoder sets it on the last frame of a message.
public struct Frame: Sendable, Equatable {
    public var flags: FrameFlags
    public var content: ByteBuffer

    public init(flags: FrameFlags = [], content: ByteBuffer = ByteBuffer()) {
        self.flags = flags
        self.content = content
    }

    static let null = Frame(flags: .isNull)
    static let beginDataStructure = Frame(flags: .beginDataStructure)
    static let endDataStructure = Frame(flags: .endDataStructure)

    var isNull: Bool { flags.contains(.isNull) }
    var isBeginDataStructure: Bool { flags.contains(.beginDataStructure) }
    var isEndDataStructure: Bool { flags.contains(.endDataStructure) }

    /// The size of the frame on the wire.
    var encodedLength: Int { ClientProtocol.frameHeaderSize + content.readableBytes }
}

// MARK: - Fixed-size fields

extension Frame {
    func int64(at offset: Int) throws(HazelcastError) -> Int64 {
        try integer(at: offset)
    }

    func int32(at offset: Int) throws(HazelcastError) -> Int32 {
        try integer(at: offset)
    }

    func uint8(at offset: Int) throws(HazelcastError) -> UInt8 {
        try integer(at: offset)
    }

    func bool(at offset: Int) throws(HazelcastError) -> Bool {
        try uint8(at: offset) == 1
    }

    mutating func setInt64(_ value: Int64, at offset: Int) {
        content.setInteger(value, at: content.readerIndex + offset, endianness: .little)
    }

    mutating func setInt32(_ value: Int32, at offset: Int) {
        content.setInteger(value, at: content.readerIndex + offset, endianness: .little)
    }

    mutating func setUInt8(_ value: UInt8, at offset: Int) {
        content.setInteger(value, at: content.readerIndex + offset)
    }

    /// A nullable UUID: a boolean "is null", then the most and the least
    /// significant 64 bits.
    mutating func setUUID(_ value: HazelcastUUID?, at offset: Int) {
        guard let value else {
            setUInt8(1, at: offset)
            return
        }
        setUInt8(0, at: offset)
        setInt64(Int64(bitPattern: value.mostSignificantBits), at: offset + 1)
        setInt64(Int64(bitPattern: value.leastSignificantBits), at: offset + 1 + ClientProtocol.int64Size)
    }

    func uuid(at offset: Int) throws(HazelcastError) -> HazelcastUUID? {
        if try bool(at: offset) {
            return nil
        }
        let most = try int64(at: offset + 1)
        let least = try int64(at: offset + 1 + ClientProtocol.int64Size)
        return HazelcastUUID(
            mostSignificantBits: UInt64(bitPattern: most),
            leastSignificantBits: UInt64(bitPattern: least)
        )
    }

    private func integer<Value: FixedWidthInteger>(at offset: Int) throws(HazelcastError) -> Value {
        guard offset >= 0,
            let value = content.getInteger(
                at: content.readerIndex + offset,
                endianness: .little,
                as: Value.self
            )
        else {
            throw .protocolViolation(
                "frame of \(content.readableBytes) bytes has no \(Value.self) at offset \(offset)"
            )
        }
        return value
    }
}
