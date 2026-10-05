@testable import HazelcastClient
import NIOCore
import Testing

/// Builds responses the way the member's codecs lay them out (see the
/// `encodeResponse` methods of the Hazelcast 5.4.0 codecs) and checks that
/// we read every field from the right place.
@Suite("Codec responses")
struct CodecResponseTests {
    /// An initial response frame: type, correlation id, backup acks, then
    /// `fixedSize` bytes of response fields.
    static func response(type: Int32, fixedSize: Int = 0) -> ClientMessage {
        var frame = Frame(
            flags: .unfragmentedMessage,
            content: ByteBuffer(repeating: 0, count: ClientProtocol.responseHeaderSize + fixedSize)
        )
        frame.setInt32(type, at: 0)
        frame.setInt64(99, at: 4)
        return ClientMessage(frames: [frame])
    }

    @Test("Map.Get: null and non-null values")
    func mapGet() throws {
        var empty = Self.response(type: MapGetCodec.responseType)
        empty.frames.append(.null)
        #expect(try MapGetCodec.decodeResponse(empty) == nil)

        var present = Self.response(type: MapGetCodec.responseType)
        present.append(data: Int64(41).hazelcastData())
        #expect(try MapGetCodec.decodeResponse(present).map { try Int64(hazelcastData: $0) } == 41)
    }

    @Test("Map.ReplaceIfSame: the boolean after the response header")
    func replaceIfSame() throws {
        var replaced = Self.response(type: MapReplaceIfSameCodec.responseType, fixedSize: 1)
        replaced.initialFrame.setUInt8(1, at: ClientProtocol.responseHeaderSize)
        #expect(try MapReplaceIfSameCodec.decodeResponse(replaced))
        #expect(try !MapReplaceIfSameCodec.decodeResponse(Self.response(type: MapReplaceIfSameCodec.responseType, fixedSize: 1)))
    }

    @Test("AtomicLong: the long after the response header")
    func atomicLong() throws {
        var response = Self.response(type: AtomicLongAddAndGetCodec.responseType, fixedSize: 8)
        response.initialFrame.setInt64(100_000, at: ClientProtocol.responseHeaderSize)
        #expect(try AtomicLongAddAndGetCodec.decodeResponse(response) == 100_000)
    }

    @Test("CPGroup.CreateCPGroup: a RaftGroupId, skipping fields added later")
    func createCPGroup() throws {
        var response = Self.response(type: CPGroupCreateCPGroupCodec.responseType)
        RaftGroupIDCodec.encode(RaftGroupID(name: "default", seed: 3, id: 7), into: &response)
        // Pretend a newer member added a field to the structure.
        let end = response.frames.removeLast()
        response.append(string: "extra")
        response.frames.append(end)
        #expect(try CPGroupCreateCPGroupCodec.decodeResponse(response) == RaftGroupID(name: "default", seed: 3, id: 7))
    }

    @Test("error responses: the first ErrorHolder, stack trace skipped")
    func error() throws {
        var response = Self.response(type: 0)
        response.appendBeginDataStructure()  // list
        for (code, name) in [(26, "java.lang.IllegalMonitorStateException"), (18, "com.hazelcast.core.HazelcastException")] {
            response.appendBeginDataStructure()  // ErrorHolder
            var fixed = Frame(content: ByteBuffer(repeating: 0, count: 4))
            fixed.setInt32(Int32(code), at: 0)
            response.frames.append(fixed)
            response.append(string: name)
            response.append(nullableString: "Current thread is not owner of the lock!")
            response.appendBeginDataStructure()  // stack trace list
            response.appendBeginDataStructure()  // StackTraceElement
            response.frames.append(Frame(content: ByteBuffer(repeating: 0, count: 4)))
            response.append(string: "Class")
            response.append(string: "method")
            response.frames.append(.null)
            response.appendEndDataStructure()
            response.appendEndDataStructure()
            response.appendEndDataStructure()
        }
        response.appendEndDataStructure()

        let error = try ErrorsCodec.decode(response)
        #expect(error.errorCode == 26)
        #expect(error.className == "java.lang.IllegalMonitorStateException")
        #expect(error.message == "Current thread is not owner of the lock!")
        #expect(!error.isRetrySafe)
    }

    @Test("Client.Authentication: status, partition count, address and version")
    func authentication() throws {
        // The 5.4.0 response has two more ints after failoverSupported.
        var response = Self.response(type: ClientAuthenticationCodec.responseType, fixedSize: 1 + 17 + 1 + 4 + 17 + 1 + 8)
        let base = ClientProtocol.responseHeaderSize
        let member = HazelcastUUID(mostSignificantBits: 1, leastSignificantBits: 2)
        let cluster = HazelcastUUID(mostSignificantBits: 3, leastSignificantBits: 4)
        response.initialFrame.setUInt8(0, at: base)
        response.initialFrame.setUUID(member, at: base + 1)
        response.initialFrame.setUInt8(1, at: base + 18)
        response.initialFrame.setInt32(271, at: base + 19)
        response.initialFrame.setUUID(cluster, at: base + 23)
        response.appendBeginDataStructure()
        var port = Frame(content: ByteBuffer(repeating: 0, count: 4))
        port.setInt32(5701, at: 0)
        response.frames.append(port)
        response.append(string: "127.0.0.1")
        response.appendEndDataStructure()
        response.append(string: "5.4.0")
        response.frames.append(.null)  // tpcPorts, ignored

        let decoded = try ClientAuthenticationCodec.decodeResponse(response)
        #expect(decoded.status == 0)
        #expect(decoded.memberUUID == member)
        #expect(decoded.serializationVersion == 1)
        #expect(decoded.partitionCount == 271)
        #expect(decoded.clusterID == cluster)
        #expect(decoded.memberAddress == "127.0.0.1:5701")
        #expect(decoded.serverHazelcastVersion == "5.4.0")
    }

    @Test("a truncated initial frame is a protocol violation, not a crash")
    func truncated() {
        let response = Self.response(type: AtomicLongGetCodec.responseType)
        #expect(throws: HazelcastError.self) { try AtomicLongGetCodec.decodeResponse(response) }
    }
}
