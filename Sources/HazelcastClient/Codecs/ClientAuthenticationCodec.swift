import NIOCore

/// `Client.Authentication` (0x000100), as in Hazelcast 5.4.0.
///
/// Request: initial frame with `uuid` (nullable UUID, 17 bytes) and
/// `serializationVersion` (byte); then `clusterName`, `username?`,
/// `password?`, `clientType`, `clientHazelcastVersion`, `clientName`,
/// `labels` (List<String>).
///
/// Response: initial frame with `status` (byte), `memberUuid` (UUID),
/// `serializationVersion` (byte), `partitionCount` (int), `clusterId` (UUID),
/// `failoverSupported` (boolean), and in 5.4 two more ints; then `address?`
/// (Address), `serverHazelcastVersion` and further fields we skip.
enum ClientAuthenticationCodec {
    static let requestType: Int32 = 0x000100
    static let responseType: Int32 = 0x000101

    private static let requestUUIDOffset = 0
    private static let requestSerializationVersionOffset = requestUUIDOffset + ClientProtocol.uuidSize
    private static let requestFixedSize = requestSerializationVersionOffset + 1

    private static let responseStatusOffset = ClientProtocol.responseHeaderSize
    private static let responseMemberUUIDOffset = responseStatusOffset + 1
    private static let responseSerializationVersionOffset = responseMemberUUIDOffset + ClientProtocol.uuidSize
    private static let responsePartitionCountOffset = responseSerializationVersionOffset + 1
    private static let responseClusterIDOffset = responsePartitionCountOffset + ClientProtocol.int32Size

    struct Request {
        var clusterName: String
        var username: String? = nil
        var password: String? = nil
        var clientUUID: HazelcastUUID?
        var clientType: String
        var serializationVersion: UInt8
        var clientHazelcastVersion: String
        var clientName: String
        var labels: [String] = []
    }

    struct Response {
        var status: UInt8
        var memberAddress: String?
        var memberUUID: HazelcastUUID?
        var serializationVersion: UInt8
        var serverHazelcastVersion: String
        var partitionCount: Int32
        var clusterID: HazelcastUUID?
    }

    static func encodeRequest(_ request: Request) -> ClientMessage {
        var message = ClientMessage(requestType: requestType, fixedSize: requestFixedSize)
        message.setRequestUUID(request.clientUUID, at: requestUUIDOffset)
        message.setRequestUInt8(request.serializationVersion, at: requestSerializationVersionOffset)
        message.append(string: request.clusterName)
        message.append(nullableString: request.username)
        message.append(nullableString: request.password)
        message.append(string: request.clientType)
        message.append(string: request.clientHazelcastVersion)
        message.append(string: request.clientName)
        message.append(stringList: request.labels)
        return message
    }

    static func decodeResponse(_ message: ClientMessage) throws(HazelcastError) -> Response {
        var iterator = message.makeFrameIterator()
        let initial = try iterator.initialFrame()
        let status = try initial.uint8(at: responseStatusOffset)
        let memberUUID = try initial.uuid(at: responseMemberUUIDOffset)
        let serializationVersion = try initial.uint8(at: responseSerializationVersionOffset)
        let partitionCount = try initial.int32(at: responsePartitionCountOffset)
        let clusterID = try initial.uuid(at: responseClusterIDOffset)
        let address = iterator.nextIsNull() ? nil : try AddressCodec.decode(&iterator)
        let serverVersion = try iterator.string()
        return Response(
            status: status,
            memberAddress: address,
            memberUUID: memberUUID,
            serializationVersion: serializationVersion,
            serverHazelcastVersion: serverVersion,
            partitionCount: partitionCount,
            clusterID: clusterID
        )
    }
}

/// The `Address` custom type: begin frame, a frame with `port` (int), `host`, end frame.
enum AddressCodec {
    static func decode(_ iterator: inout FrameIterator) throws(HazelcastError) -> String {
        try iterator.beginDataStructure()
        let port = try iterator.next().int32(at: 0)
        let host = try iterator.string()
        try iterator.fastForwardToEndFrame()
        return "\(host):\(port)"
    }
}
