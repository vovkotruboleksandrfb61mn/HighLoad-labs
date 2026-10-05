@testable import HazelcastClient
import Testing

/// Every request is compared byte for byte with what the official
/// hazelcast-python-client 5.4.0 codecs produce for the same arguments
/// (correlation id 0, partition id -1; the Python codecs set IS_FINAL on the
/// last frame themselves, our encoder does it on write).
@Suite("Codec request bytes")
struct CodecRequestTests {
    let key = "counter".hazelcastData()

    @Test("Map.Get")
    func mapGet() throws {
        let message = MapGetCodec.encodeRequest(name: "counters", key: key, threadID: 7)
        #expect(
            try wireHex(message)
                == "1e00000000c0000201000000000000000000ffffffff07000000000000000e0000000000636f756e74657273"
                + "19000000002000000000fffffff500000007636f756e746572"
        )
    }

    @Test("Map.Put")
    func mapPut() throws {
        let message = MapPutCodec.encodeRequest(
            name: "counters",
            key: key,
            value: Int64(5).hazelcastData(),
            threadID: 7
        )
        #expect(
            try wireHex(message)
                == "2600000000c0000101000000000000000000ffffffff0700000000000000ffffffffffffffff"
                + "0e0000000000636f756e7465727319000000000000000000fffffff500000007636f756e746572"
                + "16000000002000000000fffffff80000000000000005"
        )
    }

    @Test("Map.Lock")
    func mapLock() throws {
        let message = MapLockCodec.encodeRequest(name: "counters", key: key, threadID: 7, referenceID: 3)
        #expect(
            try wireHex(message)
                == "2e00000000c0001001000000000000000000ffffffff0700000000000000ffffffffffffffff0300000000000000"
                + "0e0000000000636f756e7465727319000000002000000000fffffff500000007636f756e746572"
        )
    }

    @Test("Map.Unlock")
    func mapUnlock() throws {
        let message = MapUnlockCodec.encodeRequest(name: "counters", key: key, threadID: 7, referenceID: 4)
        #expect(
            try wireHex(message)
                == "2600000000c0001301000000000000000000ffffffff07000000000000000400000000000000"
                + "0e0000000000636f756e7465727319000000002000000000fffffff500000007636f756e746572"
        )
    }

    @Test("Map.ReplaceIfSame")
    func mapReplaceIfSame() throws {
        let message = MapReplaceIfSameCodec.encodeRequest(
            name: "counters",
            key: key,
            testValue: Int64(5).hazelcastData(),
            value: Int64(6).hazelcastData(),
            threadID: 7
        )
        #expect(
            try wireHex(message)
                == "1e00000000c0000501000000000000000000ffffffff0700000000000000"
                + "0e0000000000636f756e7465727319000000000000000000fffffff500000007636f756e746572"
                + "16000000000000000000fffffff80000000000000005"
                + "16000000002000000000fffffff80000000000000006"
        )
    }

    @Test("CPGroup.CreateCPGroup")
    func createCPGroup() throws {
        let message = CPGroupCreateCPGroupCodec.encodeRequest(proxyName: "counter")
        #expect(try wireHex(message) == "1600000000c000011e000000000000000000ffffffff0d0000000020636f756e746572")
    }

    static let groupID = RaftGroupID(name: "default", seed: 1, id: 2)
    /// BEGIN, (seed, id), name, END, then the object name with IS_FINAL.
    static let groupAndName =
        "060000000010160000000000010000000000000002000000000000000d000000000064656661756c74"
        + "0600000000080d0000000020636f756e746572"

    @Test("AtomicLong.AddAndGet")
    func atomicLongAddAndGet() throws {
        let message = AtomicLongAddAndGetCodec.encodeRequest(groupID: Self.groupID, name: "counter", delta: 1)
        #expect(
            try wireHex(message)
                == "1e00000000c0000309000000000000000000ffffffff0100000000000000" + Self.groupAndName
        )
    }

    @Test("AtomicLong.Get")
    func atomicLongGet() throws {
        let message = AtomicLongGetCodec.encodeRequest(groupID: Self.groupID, name: "counter")
        #expect(try wireHex(message) == "1600000000c0000509000000000000000000ffffffff" + Self.groupAndName)
    }

    @Test("AtomicLong.GetAndSet")
    func atomicLongGetAndSet() throws {
        let message = AtomicLongGetAndSetCodec.encodeRequest(groupID: Self.groupID, name: "counter", newValue: 0)
        #expect(
            try wireHex(message)
                == "1e00000000c0000709000000000000000000ffffffff0000000000000000" + Self.groupAndName
        )
    }

    @Test("Client.Ping")
    func ping() throws {
        #expect(try wireHex(ClientPingCodec.encodeRequest()) == "1600000000e0000b00000000000000000000ffffffff")
    }

    @Test("Client.Authentication")
    func authentication() throws {
        let uuid = HazelcastUUID(
            mostSignificantBits: 0x0123_4567_89ab_cdef,
            leastSignificantBits: 0x0123_4567_89ab_cdef
        )
        let message = ClientAuthenticationCodec.encodeRequest(
            .init(
                clusterName: "dev",
                clientUUID: uuid,
                clientType: "SWF",
                serializationVersion: 1,
                clientHazelcastVersion: "5.4.0",
                clientName: "swift-client"
            )
        )
        #expect(
            try wireHex(message)
                == "2800000000c0000100000000000000000000ffffffff00efcdab8967452301efcdab896745230101"
                + "0900000000006465760600000000040600000000040900000000005357460b0000000000352e342e30"
                + "12000000000073776966742d636c69656e74060000000010060000000028"
        )
    }

    @Test("the correlation and partition ids land in the request header")
    func header() throws {
        var message = ClientPingCodec.encodeRequest()
        message.correlationID = 0x0102_0304_0506_0708
        message.partitionID = 270
        #expect(message.messageType == 0x000B00)
        #expect(try wireHex(message) == "1600000000e0000b00000807060504030201" + "0e010000")
    }
}
