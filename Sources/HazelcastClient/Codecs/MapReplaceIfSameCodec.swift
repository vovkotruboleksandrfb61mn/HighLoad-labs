/// `Map.ReplaceIfSame` (0x010500). Request: `threadId` (long); `name`,
/// `key`, `testValue`, `value` (all Data). Response: `response` (boolean),
/// true if the entry held `testValue` and now holds `value`.
enum MapReplaceIfSameCodec {
    static let requestType: Int32 = 0x010500
    static let responseType: Int32 = 0x010501

    private static let threadIDOffset = 0
    private static let fixedSize = threadIDOffset + ClientProtocol.int64Size
    private static let responseOffset = ClientProtocol.responseHeaderSize

    static func encodeRequest(
        name: String,
        key: HazelcastData,
        testValue: HazelcastData,
        value: HazelcastData,
        threadID: Int64
    ) -> ClientMessage {
        var message = ClientMessage(requestType: requestType, fixedSize: fixedSize)
        message.setRequestInt64(threadID, at: threadIDOffset)
        message.append(string: name)
        message.append(data: key)
        message.append(data: testValue)
        message.append(data: value)
        return message
    }

    static func decodeResponse(_ message: ClientMessage) throws(HazelcastError) -> Bool {
        try message.initialFrame.bool(at: responseOffset)
    }
}
