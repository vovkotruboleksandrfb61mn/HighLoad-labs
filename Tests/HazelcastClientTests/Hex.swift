import HazelcastClient
import NIOCore

/// Lower-case hex of `bytes`, to compare against the reference vectors.
func hex(_ bytes: some Sequence<UInt8>) -> String {
    bytes.map { byte in
        let digits = String(byte, radix: 16)
        return digits.count == 1 ? "0" + digits : digits
    }.joined()
}

func bytes(fromHex text: String) -> [UInt8] {
    var result: [UInt8] = []
    var index = text.startIndex
    while index < text.endIndex {
        let next = text.index(index, offsetBy: 2)
        result.append(UInt8(text[index..<next], radix: 16)!)
        index = next
    }
    return result
}

/// The wire bytes of `message` as `ClientMessageEncoder` writes them.
func wireHex(_ message: ClientMessage) throws -> String {
    var buffer = ByteBuffer()
    try ClientMessageEncoder().encode(data: message, out: &buffer)
    return hex(buffer.readableBytesView)
}
