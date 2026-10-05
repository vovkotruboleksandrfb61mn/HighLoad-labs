/// Hazelcast's serialized form of an object (`HeapData`): the partition hash
/// (`int32`, big-endian), the serializer type id (`int32`, big-endian), then
/// the payload the serializer wrote (big-endian).
public struct HazelcastData: Sendable, Hashable {
    static let partitionHashOffset = 0
    static let typeOffset = 4
    static let payloadOffset = 8

    public let bytes: [UInt8]

    public init(bytes: [UInt8]) {
        self.bytes = bytes
    }

    /// Serialized form with no explicit partition hash (written as 0).
    public init(typeID: Int32, payload: [UInt8]) {
        var bytes: [UInt8] = []
        bytes.reserveCapacity(Self.payloadOffset + payload.count)
        bytes.appendBigEndian(Int32(0))
        bytes.appendBigEndian(typeID)
        bytes.append(contentsOf: payload)
        self.bytes = bytes
    }

    public var typeID: Int32 {
        bytes.count >= Self.payloadOffset ? bytes.readBigEndian(at: Self.typeOffset) : 0
    }

    public var payload: ArraySlice<UInt8> {
        bytes.count >= Self.payloadOffset ? bytes[Self.payloadOffset...] : []
    }

    /// The stored partition hash if there is one, otherwise the MurmurHash3 of
    /// the payload (`HeapData.getPartitionHash`).
    public var partitionHash: Int32 {
        if bytes.count >= Self.payloadOffset {
            let stored: Int32 = bytes.readBigEndian(at: Self.partitionHashOffset)
            if stored != 0 {
                return stored
            }
        }
        return MurmurHash3.x86_32(payload)
    }

    /// The partition that owns this key (`HashUtil.hashToIndex`).
    public func partitionID(partitionCount: Int32) -> Int32 {
        precondition(partitionCount > 0, "partition count must be positive")
        let hash = partitionHash
        if hash == Int32.min {
            return 0
        }
        return abs(hash) % partitionCount
    }
}

extension Array where Element == UInt8 {
    mutating func appendBigEndian<Value: FixedWidthInteger>(_ value: Value) {
        Swift.withUnsafeBytes(of: value.bigEndian) { append(contentsOf: $0) }
    }
}

extension Collection where Element == UInt8, Index == Int {
    func readBigEndian<Value: FixedWidthInteger>(at offset: Int, as: Value.Type = Value.self) -> Value {
        var value: Value = 0
        for index in (startIndex + offset)..<(startIndex + offset + MemoryLayout<Value>.size) {
            value = (value << 8) | Value(self[index])
        }
        return value
    }
}
