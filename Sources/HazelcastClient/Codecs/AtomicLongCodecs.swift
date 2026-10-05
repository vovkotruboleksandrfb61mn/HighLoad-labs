/// Shared by the `AtomicLong` codecs: every request carries the CP group id
/// (`RaftGroupId`) and the object name after the initial frame, and every
/// response we use is one `long` right after the response header.
private enum AtomicLongCodec {
    static let responseOffset = ClientProtocol.responseHeaderSize

    static func request(
        type: Int32,
        groupID: RaftGroupID,
        name: String,
        longParameter: Int64?
    ) -> ClientMessage {
        var message = ClientMessage(requestType: type, fixedSize: longParameter == nil ? 0 : ClientProtocol.int64Size)
        if let longParameter {
            message.setRequestInt64(longParameter, at: 0)
        }
        RaftGroupIDCodec.encode(groupID, into: &message)
        message.append(string: name)
        return message
    }

    static func decodeLong(_ message: ClientMessage) throws(HazelcastError) -> Int64 {
        try message.initialFrame.int64(at: responseOffset)
    }
}

/// `AtomicLong.AddAndGet` (0x090300). Request: `delta` (long); `groupId`,
/// `name`. Response: the new value (long).
enum AtomicLongAddAndGetCodec {
    static let requestType: Int32 = 0x090300
    static let responseType: Int32 = 0x090301

    static func encodeRequest(groupID: RaftGroupID, name: String, delta: Int64) -> ClientMessage {
        AtomicLongCodec.request(type: requestType, groupID: groupID, name: name, longParameter: delta)
    }

    static func decodeResponse(_ message: ClientMessage) throws(HazelcastError) -> Int64 {
        try AtomicLongCodec.decodeLong(message)
    }
}

/// `AtomicLong.Get` (0x090500). Request: `groupId`, `name`. Response: the
/// value (long).
enum AtomicLongGetCodec {
    static let requestType: Int32 = 0x090500
    static let responseType: Int32 = 0x090501

    static func encodeRequest(groupID: RaftGroupID, name: String) -> ClientMessage {
        AtomicLongCodec.request(type: requestType, groupID: groupID, name: name, longParameter: nil)
    }

    static func decodeResponse(_ message: ClientMessage) throws(HazelcastError) -> Int64 {
        try AtomicLongCodec.decodeLong(message)
    }
}

/// `AtomicLong.GetAndSet` (0x090700). Request: `newValue` (long); `groupId`,
/// `name`. Response: the old value (long). `IAtomicLong.set` uses it too.
enum AtomicLongGetAndSetCodec {
    static let requestType: Int32 = 0x090700
    static let responseType: Int32 = 0x090701

    static func encodeRequest(groupID: RaftGroupID, name: String, newValue: Int64) -> ClientMessage {
        AtomicLongCodec.request(type: requestType, groupID: groupID, name: name, longParameter: newValue)
    }

    static func decodeResponse(_ message: ClientMessage) throws(HazelcastError) -> Int64 {
        try AtomicLongCodec.decodeLong(message)
    }
}
