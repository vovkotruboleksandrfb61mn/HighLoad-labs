/// `Map.Lock` (0x011000). Request: `threadId` (long), `ttl` (long),
/// `referenceId` (long); `name`, `key` (Data). Empty response.
///
/// The lock belongs to this client and `threadId`. `referenceId` lets the
/// member recognise a retried request; every call gets a new one.
enum MapLockCodec {
    static let requestType: Int32 = 0x011000
    static let responseType: Int32 = 0x011001

    private static let threadIDOffset = 0
    private static let ttlOffset = threadIDOffset + ClientProtocol.int64Size
    private static let referenceIDOffset = ttlOffset + ClientProtocol.int64Size
    private static let fixedSize = referenceIDOffset + ClientProtocol.int64Size

    /// `ttl` -1 holds the lock until it is unlocked.
    static func encodeRequest(
        name: String,
        key: HazelcastData,
        threadID: Int64,
        ttl: Int64 = -1,
        referenceID: Int64
    ) -> ClientMessage {
        var message = ClientMessage(requestType: requestType, fixedSize: fixedSize)
        message.setRequestInt64(threadID, at: threadIDOffset)
        message.setRequestInt64(ttl, at: ttlOffset)
        message.setRequestInt64(referenceID, at: referenceIDOffset)
        message.append(string: name)
        message.append(data: key)
        return message
    }
}
