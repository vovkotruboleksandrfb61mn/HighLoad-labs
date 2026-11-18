/// `Map.EvictAll` (0x011F00). Request: `name`. Empty response.
///
/// Drops every entry from memory without calling the map's MapStore, so the
/// next read of a key goes to the MapLoader again (unlike `clear()`, which
/// deletes the entries from the store too).
enum MapEvictAllCodec {
    static let requestType: Int32 = 0x011F00
    static let responseType: Int32 = 0x011F01

    static func encodeRequest(name: String) -> ClientMessage {
        var message = ClientMessage(requestType: requestType)
        message.append(string: name)
        return message
    }
}
