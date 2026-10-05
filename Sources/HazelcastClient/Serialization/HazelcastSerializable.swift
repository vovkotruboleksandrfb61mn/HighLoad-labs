/// A type that can be stored in Hazelcast with one of the built-in
/// (constant) serializers, so a Java member or any other client reads it as
/// the matching Java type.
public protocol HazelcastSerializable: Sendable {
    /// The id of the built-in serializer (`SerializationConstants`).
    static var hazelcastTypeID: Int32 { get }

    /// The serializer's output, without the 8-byte `Data` header.
    func hazelcastPayload() -> [UInt8]

    init(hazelcastPayload payload: ArraySlice<UInt8>) throws(HazelcastError)
}

extension HazelcastSerializable {
    public func hazelcastData() -> HazelcastData {
        HazelcastData(typeID: Self.hazelcastTypeID, payload: hazelcastPayload())
    }

    public init(hazelcastData data: HazelcastData) throws(HazelcastError) {
        guard data.typeID == Self.hazelcastTypeID else {
            throw .serialization("expected type id \(Self.hazelcastTypeID) for \(Self.self), got \(data.typeID)")
        }
        try self.init(hazelcastPayload: data.payload)
    }
}

/// `java.lang.Long`: `CONSTANT_TYPE_LONG` (-8), eight bytes big-endian.
extension Int64: HazelcastSerializable {
    public static var hazelcastTypeID: Int32 { -8 }

    public func hazelcastPayload() -> [UInt8] {
        var bytes: [UInt8] = []
        bytes.appendBigEndian(self)
        return bytes
    }

    public init(hazelcastPayload payload: ArraySlice<UInt8>) throws(HazelcastError) {
        guard payload.count == 8 else {
            throw .serialization("a Long payload has 8 bytes, got \(payload.count)")
        }
        self = payload.readBigEndian(at: 0)
    }
}

/// `java.lang.String`: `CONSTANT_TYPE_STRING` (-11), the UTF-8 length as an
/// `int32` big-endian, then the UTF-8 bytes.
extension String: HazelcastSerializable {
    public static var hazelcastTypeID: Int32 { -11 }

    public func hazelcastPayload() -> [UInt8] {
        var bytes: [UInt8] = []
        bytes.reserveCapacity(4 + utf8.count)
        bytes.appendBigEndian(Int32(utf8.count))
        bytes.append(contentsOf: utf8)
        return bytes
    }

    public init(hazelcastPayload payload: ArraySlice<UInt8>) throws(HazelcastError) {
        guard payload.count >= 4 else {
            throw .serialization("a String payload starts with a 4-byte length, got \(payload.count) bytes")
        }
        let length = Int(payload.readBigEndian(at: 0, as: Int32.self))
        guard length >= 0, payload.count == 4 + length else {
            throw .serialization("String length \(length) does not match a payload of \(payload.count) bytes")
        }
        self = String(decoding: payload.dropFirst(4), as: UTF8.self)
    }
}
