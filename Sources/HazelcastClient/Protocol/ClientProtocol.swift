// Constants of the Hazelcast Open Binary Client Protocol 2.x.
//
// Ground truth: the codecs generated for Hazelcast 5.4.0
// (com.hazelcast.client.impl.protocol.ClientMessage and codec/*Codec.java)
// and the hazelcast-python-client 5.4.0 codecs, which agree byte for byte.

/// Constants of the Open Binary Client Protocol.
public enum ClientProtocol {
    /// The three bytes a client sends right after connecting to pick protocol 2.x.
    public static let initialBytes: [UInt8] = Array("CP2".utf8)

    /// Size of the length (`int32`) and flags (`uint16`) that start every frame.
    static let frameHeaderSize = 6

    // Offsets inside the content of a message's initial frame (after the
    // 6-byte frame header). All multi-byte values are little-endian.
    static let messageTypeOffset = 0
    static let correlationIDOffset = 4
    static let partitionIDOffset = 12

    /// Where the fixed-size parameters of a request start.
    static let requestHeaderSize = 16
    /// Where the fixed-size parameters of a response start.
    static let responseHeaderSize = 13

    /// The message type of an error response (`ErrorsCodec.EXCEPTION_MESSAGE_TYPE`).
    static let exceptionMessageType: Int32 = 0

    /// Sizes of the fixed-size types.
    static let int64Size = 8
    static let int32Size = 4
    static let uuidSize = 17
}
