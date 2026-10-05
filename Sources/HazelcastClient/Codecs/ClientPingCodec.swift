/// `Client.Ping` (0x000B00): no parameters, empty response. Keeps the member
/// from closing an idle connection (it drops clients it has not heard from
/// for `hazelcast.client.max.no.heartbeat.seconds`, 60 s by default).
enum ClientPingCodec {
    static let requestType: Int32 = 0x000B00

    static func encodeRequest() -> ClientMessage {
        ClientMessage(requestType: requestType)
    }
}
