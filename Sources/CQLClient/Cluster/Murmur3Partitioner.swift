import NIOCore

/// Cassandra's Murmur3Partitioner: the token of a partition key is the first
/// half of MurmurHash3 x64 128 (seed 0) of its serialized bytes.
///
/// Cassandra's port of the hash has one quirk this copy keeps on purpose:
/// the tail bytes (the last `length % 16`) are sign-extended before they are
/// shifted in, so keys whose tail holds bytes >= 0x80 hash differently from
/// the reference C implementation. Matching Cassandra is what counts here.
public enum Murmur3Partitioner {
    public static func token(of key: some Collection<UInt8>) -> Int64 {
        let h1 = hash3_x64_128(Array(key)).0
        // Long.MIN_VALUE is reserved as the minimum token of the ring.
        return h1 == .min ? .max : h1
    }

    public static func token(of key: ByteBuffer) -> Int64 {
        token(of: key.readableBytesView)
    }

    /// The routing key of a partition key: the single component's bytes, or
    /// for a composite key each component as a short length, the bytes and
    /// a zero byte.
    public static func routingKey(_ components: [ByteBuffer]) -> ByteBuffer {
        if components.count == 1 {
            return components[0]
        }
        var key = ByteBuffer()
        for component in components {
            key.writeInteger(UInt16(component.readableBytes))
            key.writeImmutableBuffer(component)
            key.writeInteger(UInt8(0))
        }
        return key
    }

    static func hash3_x64_128(_ key: [UInt8], seed: UInt64 = 0) -> (Int64, Int64) {
        let length = key.count
        let blocks = length / 16
        var h1 = seed
        var h2 = seed
        let c1: UInt64 = 0x87c3_7b91_1142_53d5
        let c2: UInt64 = 0x4cf5_ad43_2745_937f

        func block(_ offset: Int) -> UInt64 {
            var value: UInt64 = 0
            for index in 0..<8 {
                value |= UInt64(key[offset + index]) << (8 * UInt64(index))
            }
            return value
        }

        for index in 0..<blocks {
            var k1 = block(index * 16)
            var k2 = block(index * 16 + 8)
            k1 &*= c1; k1 = k1.rotatedLeft(31); k1 &*= c2; h1 ^= k1
            h1 = h1.rotatedLeft(27); h1 &+= h2; h1 = h1 &* 5 &+ 0x52dc_e729
            k2 &*= c2; k2 = k2.rotatedLeft(33); k2 &*= c1; h2 ^= k2
            h2 = h2.rotatedLeft(31); h2 &+= h1; h2 = h2 &* 5 &+ 0x3849_5ab5
        }

        let tail = blocks * 16
        // Sign extension of a byte, as Java's `(long) byte` does.
        func signed(_ index: Int) -> UInt64 {
            UInt64(bitPattern: Int64(Int8(bitPattern: key[tail + index])))
        }
        var k1: UInt64 = 0
        var k2: UInt64 = 0
        let rest = length & 15
        if rest > 8 {
            for index in stride(from: rest - 1, through: 8, by: -1) {
                k2 ^= signed(index) << (8 * UInt64(index - 8))
            }
            k2 &*= c2; k2 = k2.rotatedLeft(33); k2 &*= c1; h2 ^= k2
        }
        if rest > 0 {
            for index in stride(from: min(rest, 8) - 1, through: 0, by: -1) {
                k1 ^= signed(index) << (8 * UInt64(index))
            }
            k1 &*= c1; k1 = k1.rotatedLeft(31); k1 &*= c2; h1 ^= k1
        }

        h1 ^= UInt64(length); h2 ^= UInt64(length)
        h1 &+= h2; h2 &+= h1
        h1 = fmix(h1); h2 = fmix(h2)
        h1 &+= h2; h2 &+= h1
        return (Int64(bitPattern: h1), Int64(bitPattern: h2))
    }

    private static func fmix(_ value: UInt64) -> UInt64 {
        var k = value
        k ^= k >> 33
        k &*= 0xff51_afd7_ed55_8ccd
        k ^= k >> 33
        k &*= 0xc4ce_b9fe_1a85_ec53
        k ^= k >> 33
        return k
    }
}

extension UInt64 {
    fileprivate func rotatedLeft(_ count: UInt64) -> UInt64 {
        (self << count) | (self >> (64 - count))
    }
}
