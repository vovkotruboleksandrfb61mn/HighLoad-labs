/// `Map.Unlock` (0x011300). Request: `threadId` (long), `referenceId` (long);
/// `name`, `key` (Data). Empty response.
enum MapUnlockCodec {
    static let requestType: Int32 = 0x011300
    static let responseType: Int32 = 0x011301

    private static let threadIDOffset = 0
    private static let referenceIDOffset = threadIDOffset + ClientProtocol.int64Size
    private static let fixedSize = referenceIDOffset + ClientProtocol.int64Size

    static func encodeRequest(
        name: String,
        key: HazelcastData,
        threadID: Int64,
        referenceID: Int64
    ) -> ClientMessage {
        var message = ClientMessage(requestType: requestType, fixedSize: fixedSize)
        message.setRequestInt64(threadID, at: threadIDOffset)
        message.setRequestInt64(referenceID, at: referenceIDOffset)
        message.append(string: name)
        message.append(data: key)
        return message
    }
}
