/// MurmurHash3_x86_32 as Hazelcast uses it to place keys in partitions
/// (`com.hazelcast.internal.util.HashUtil`): blocks are read little-endian
/// and the seed is `0x01000193`.
public enum MurmurHash3 {
    public static let hazelcastSeed: UInt32 = 0x0100_0193

    public static func x86_32(_ bytes: some Collection<UInt8>, seed: UInt32 = hazelcastSeed) -> Int32 {
        let c1: UInt32 = 0xcc9e_2d51
        let c2: UInt32 = 0x1b87_3593
        var h1 = seed
        var block: UInt32 = 0
        var blockBytes = 0
        var length: UInt32 = 0

        for byte in bytes {
            block |= UInt32(byte) << (8 * UInt32(blockBytes))
            blockBytes += 1
            length &+= 1
            if blockBytes == 4 {
                h1 ^= mixK1(block, c1: c1, c2: c2)
                h1 = rotateLeft(h1, by: 13)
                h1 = h1 &* 5 &+ 0xe654_6b64
                block = 0
                blockBytes = 0
            }
        }
        if blockBytes > 0 {
            h1 ^= mixK1(block, c1: c1, c2: c2)
        }

        h1 ^= length
        return Int32(bitPattern: finalMix(h1))
    }

    private static func mixK1(_ k1: UInt32, c1: UInt32, c2: UInt32) -> UInt32 {
        rotateLeft(k1 &* c1, by: 15) &* c2
    }

    private static func finalMix(_ hash: UInt32) -> UInt32 {
        var h = hash
        h ^= h >> 16
        h = h &* 0x85eb_ca6b
        h ^= h >> 13
        h = h &* 0xc2b2_ae35
        h ^= h >> 16
        return h
    }

    private static func rotateLeft(_ value: UInt32, by shift: UInt32) -> UInt32 {
        (value << shift) | (value >> (32 - shift))
    }
}
