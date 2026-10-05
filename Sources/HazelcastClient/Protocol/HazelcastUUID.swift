/// A 128-bit UUID as Hazelcast encodes it: two 64-bit halves.
public struct HazelcastUUID: Sendable, Hashable, CustomStringConvertible {
    public var mostSignificantBits: UInt64
    public var leastSignificantBits: UInt64

    public init(mostSignificantBits: UInt64, leastSignificantBits: UInt64) {
        self.mostSignificantBits = mostSignificantBits
        self.leastSignificantBits = leastSignificantBits
    }

    /// A random (version 4) UUID.
    public static func random() -> HazelcastUUID {
        var generator = SystemRandomNumberGenerator()
        var most = generator.next() as UInt64
        var least = generator.next() as UInt64
        most = (most & ~0xF000) | 0x4000  // version 4
        least = (least & ~(0b11 << 62)) | (0b10 << 62)  // IETF variant
        return HazelcastUUID(mostSignificantBits: most, leastSignificantBits: least)
    }

    public var description: String {
        let hex = hex(mostSignificantBits) + hex(leastSignificantBits)
        let groups = [8, 4, 4, 4, 12]
        var parts: [Substring] = []
        var start = hex.startIndex
        for length in groups {
            let end = hex.index(start, offsetBy: length)
            parts.append(hex[start..<end])
            start = end
        }
        return parts.joined(separator: "-")
    }

    private func hex(_ value: UInt64) -> String {
        let digits = String(value, radix: 16)
        return String(repeating: "0", count: 16 - digits.count) + digits
    }
}
