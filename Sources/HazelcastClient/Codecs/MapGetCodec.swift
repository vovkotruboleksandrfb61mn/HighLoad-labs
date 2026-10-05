/// `Map.Get` (0x010200). Request: `threadId` (long); `name`, `key` (Data).
/// Response: `response` (Data?).
enum MapGetCodec {
    static let requestType: Int32 = 0x010200
    static let responseType: Int32 = 0x010201

    private static let threadIDOffset = 0
    private static let fixedSize = threadIDOffset + ClientProtocol.int64Size

    static func encodeRequest(name: String, key: HazelcastData, threadID: Int64) -> ClientMessage {
        var message = ClientMessage(requestType: requestType, fixedSize: fixedSize)
        message.setRequestInt64(threadID, at: threadIDOffset)
        message.append(string: name)
        message.append(data: key)
        return message
    }

    static func decodeResponse(_ message: ClientMessage) throws(HazelcastError) -> HazelcastData? {
        var iterator = message.makeFrameIterator()
        _ = try iterator.initialFrame()
        return try iterator.nullableData()
    }
}
