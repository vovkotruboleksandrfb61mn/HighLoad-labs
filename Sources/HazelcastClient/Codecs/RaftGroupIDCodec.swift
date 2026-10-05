import NIOCore

/// Identifies a CP group (`com.hazelcast.cp.internal.RaftGroupId`).
public struct RaftGroupID: Sendable, Hashable, CustomStringConvertible {
    public var name: String
    public var seed: Int64
    public var id: Int64

    public init(name: String, seed: Int64, id: Int64) {
        self.name = name
        self.seed = seed
        self.id = id
    }

    public var description: String { "RaftGroupId{name=\(name), seed=\(seed), id=\(id)}" }
}

/// The `RaftGroupId` custom type: begin frame, a 16-byte frame with `seed`
/// and `id`, the name, end frame.
enum RaftGroupIDCodec {
    private static let seedOffset = 0
    private static let idOffset = seedOffset + ClientProtocol.int64Size
    private static let fixedSize = idOffset + ClientProtocol.int64Size

    static func encode(_ groupID: RaftGroupID, into message: inout ClientMessage) {
        message.appendBeginDataStructure()
        var fixed = Frame(content: ByteBuffer(repeating: 0, count: fixedSize))
        fixed.setInt64(groupID.seed, at: seedOffset)
        fixed.setInt64(groupID.id, at: idOffset)
        message.frames.append(fixed)
        message.append(string: groupID.name)
        message.appendEndDataStructure()
    }

    static func decode(_ iterator: inout FrameIterator) throws(HazelcastError) -> RaftGroupID {
        try iterator.beginDataStructure()
        let fixed = try iterator.next()
        let seed = try fixed.int64(at: seedOffset)
        let id = try fixed.int64(at: idOffset)
        let name = try iterator.string()
        try iterator.fastForwardToEndFrame()
        return RaftGroupID(name: name, seed: seed, id: id)
    }
}
