/// `CPGroup.CreateCPGroup` (0x1E0100). Request: `proxyName`. Response: the
/// `RaftGroupId` of the CP group the proxy lives in, created if needed.
enum CPGroupCreateCPGroupCodec {
    static let requestType: Int32 = 0x1E0100
    static let responseType: Int32 = 0x1E0101

    static func encodeRequest(proxyName: String) -> ClientMessage {
        var message = ClientMessage(requestType: requestType)
        message.append(string: proxyName)
        return message
    }

    static func decodeResponse(_ message: ClientMessage) throws(HazelcastError) -> RaftGroupID {
        var iterator = message.makeFrameIterator()
        _ = try iterator.initialFrame()
        return try RaftGroupIDCodec.decode(&iterator)
    }
}
