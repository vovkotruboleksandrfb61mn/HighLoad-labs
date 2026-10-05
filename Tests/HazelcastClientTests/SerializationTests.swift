@testable import HazelcastClient
import Testing

/// Reference values computed with hazelcast-python-client 5.4.0
/// (`hazelcast.hash.murmur_hash3_x86_32`, `SerializationServiceV1.to_data`
/// with `default_int_type = LONG`, `hash_to_index(hash, 271)`).
@Suite("Serialization and partitioning")
struct SerializationTests {
    @Test(
        "MurmurHash3_x86_32 with Hazelcast's seed",
        arguments: [
            ("", -1_585_187_909),
            ("a", -1_686_100_800),
            ("ab", 312_914_265),
            ("abc", -2_068_121_803),
            ("abcd", -973_615_161),
            ("hello, hazelcast", 1_489_223_101),
        ] as [(String, Int32)]
    )
    func murmur(input: String, expected: Int32) {
        #expect(MurmurHash3.x86_32(Array(input.utf8)) == expected)
    }

    @Test(
        "String data bytes, partition hash and partition id",
        arguments: [
            ("counter", "00000000fffffff500000007636f756e746572", -317_898_450, 3),
            ("k", "00000000fffffff5000000016b", 211_586_814, 41),
            ("", "00000000fffffff500000000", 923_237_662, 11),
            ("привіт", "00000000fffffff50000000cd0bfd180d0b8d0b2d196d182", -709_704_392, 107),
        ] as [(String, String, Int32, Int32)]
    )
    func stringData(value: String, bytes: String, hash: Int32, partition: Int32) throws {
        let data = value.hazelcastData()
        #expect(hex(data.bytes) == bytes)
        #expect(data.typeID == -11)
        #expect(data.partitionHash == hash)
        #expect(data.partitionID(partitionCount: 271) == partition)
        #expect(try String(hazelcastData: data) == value)
    }

    @Test(
        "Long data bytes, partition hash and partition id",
        arguments: [
            (0, "00000000fffffff80000000000000000", -778_983_647, 109),
            (1, "00000000fffffff80000000000000001", 1_824_103_549, 110),
            (-1, "00000000fffffff8ffffffffffffffff", -1_267_298_398, 231),
            (42, "00000000fffffff8000000000000002a", 1_624_962_215, 145),
            (1 << 62, "00000000fffffff84000000000000000", 462_962_076, 39),
        ] as [(Int64, String, Int32, Int32)]
    )
    func longData(value: Int64, bytes: String, hash: Int32, partition: Int32) throws {
        let data = value.hazelcastData()
        #expect(hex(data.bytes) == bytes)
        #expect(data.typeID == -8)
        #expect(data.partitionHash == hash)
        #expect(data.partitionID(partitionCount: 271) == partition)
        #expect(try Int64(hazelcastData: data) == value)
    }

    @Test("an explicit partition hash wins over the payload hash")
    func explicitPartitionHash() {
        let data = HazelcastData(bytes: [0, 0, 0, 5] + [0xff, 0xff, 0xff, 0xf8] + [UInt8](repeating: 0, count: 8))
        #expect(data.partitionHash == 5)
        #expect(data.partitionID(partitionCount: 271) == 5)
    }

    @Test("Int32.min hashes to partition 0")
    func minimumHash() {
        let data = HazelcastData(bytes: [0x80, 0, 0, 0, 0xff, 0xff, 0xff, 0xf8] + [UInt8](repeating: 0, count: 8))
        #expect(data.partitionHash == Int32.min)
        #expect(data.partitionID(partitionCount: 271) == 0)
    }

    @Test("decoding checks the type id and the payload size")
    func decodingErrors() {
        #expect(throws: HazelcastError.self) { try Int64(hazelcastData: "x".hazelcastData()) }
        #expect(throws: HazelcastError.self) { try String(hazelcastData: Int64(1).hazelcastData()) }
        #expect(throws: HazelcastError.self) {
            try Int64(hazelcastData: HazelcastData(typeID: -8, payload: [1, 2, 3]))
        }
        #expect(throws: HazelcastError.self) {
            try String(hazelcastData: HazelcastData(typeID: -11, payload: [0, 0, 0, 9, 0x41]))
        }
    }
}
