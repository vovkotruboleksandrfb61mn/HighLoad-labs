/// `Map.PutIfAbsent` (0x010E00). Request: `threadId` (long), `ttl` (long);
/// `name`, `key` (Data), `value` (Data). Response: the value already there
/// (Data?), `nil` if `value` was stored.
enum MapPutIfAbsentCodec {
    static let requestType: Int32 = 0x010E00
    static let responseType: Int32 = 0x010E01

    private static let threadIDOffset = 0
    private static let ttlOffset = threadIDOffset + ClientProtocol.int64Size
    private static let fixedSize = ttlOffset + ClientProtocol.int64Size

    /// `ttl` -1 keeps the map's default time-to-live.
    static func encodeRequest(
        name: String,
        key: HazelcastData,
        value: HazelcastData,
        threadID: Int64,
        ttl: Int64 = -1
    ) -> ClientMessage {
        var message = ClientMessage(requestType: requestType, fixedSize: fixedSize)
        message.setRequestInt64(threadID, at: threadIDOffset)
        message.setRequestInt64(ttl, at: ttlOffset)
        message.append(string: name)
        message.append(data: key)
        message.append(data: value)
        return message
    }

    static func decodeResponse(_ message: ClientMessage) throws(HazelcastError) -> HazelcastData? {
        var iterator = message.makeFrameIterator()
        _ = try iterator.initialFrame()
        return try iterator.nullableData()
    }
}
