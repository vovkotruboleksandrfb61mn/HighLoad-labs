import NIOCore

/// The [query parameters] shared by QUERY and EXECUTE.
public struct QueryParameters: Sendable {
    public var consistency: Consistency
    /// Serialized bound values, in marker order; nil binds null.
    public var values: [ByteBuffer?]
    /// Ask the server to leave the column specs out of a Rows result because
    /// the client already has them from PREPARE.
    public var skipMetadata: Bool
    public var pageSize: Int32?
    public var serialConsistency: Consistency?
    /// Microseconds since the epoch; the server's clock is used when nil.
    public var defaultTimestamp: Int64?

    public init(
        consistency: Consistency,
        values: [ByteBuffer?] = [],
        skipMetadata: Bool = false,
        pageSize: Int32? = nil,
        serialConsistency: Consistency? = nil,
        defaultTimestamp: Int64? = nil
    ) {
        self.consistency = consistency
        self.values = values
        self.skipMetadata = skipMetadata
        self.pageSize = pageSize
        self.serialConsistency = serialConsistency
        self.defaultTimestamp = defaultTimestamp
    }

    private struct Flags: OptionSet {
        let rawValue: UInt8
        static let values = Flags(rawValue: 0x01)
        static let skipMetadata = Flags(rawValue: 0x02)
        static let pageSize = Flags(rawValue: 0x04)
        static let serialConsistency = Flags(rawValue: 0x10)
        static let defaultTimestamp = Flags(rawValue: 0x20)
    }

    func write(into buffer: inout ByteBuffer) {
        var flags: Flags = []
        if !values.isEmpty { flags.insert(.values) }
        if skipMetadata { flags.insert(.skipMetadata) }
        if pageSize != nil { flags.insert(.pageSize) }
        if serialConsistency != nil { flags.insert(.serialConsistency) }
        if defaultTimestamp != nil { flags.insert(.defaultTimestamp) }
        buffer.writeInteger(consistency.rawValue)
        buffer.writeInteger(flags.rawValue)
        if !values.isEmpty {
            buffer.writeInteger(UInt16(values.count))
            for value in values {
                buffer.writeCQLBytes(value)
            }
        }
        if let pageSize { buffer.writeInteger(pageSize) }
        if let serialConsistency { buffer.writeInteger(serialConsistency.rawValue) }
        if let defaultTimestamp { buffer.writeInteger(defaultTimestamp) }
    }
}

/// The requests this client sends.
public enum Request: Sendable {
    case startup(options: [String: String])
    case options
    case query(String, QueryParameters)
    case prepare(String)
    case execute(id: [UInt8], QueryParameters)

    public static let defaultStartupOptions = ["CQL_VERSION": "3.0.0"]

    public var opcode: Opcode {
        switch self {
        case .startup: .startup
        case .options: .options
        case .query: .query
        case .prepare: .prepare
        case .execute: .execute
        }
    }

    /// The request as a frame; the connection fills in the stream id.
    public func frame(allocator: ByteBufferAllocator = ByteBufferAllocator()) -> Frame {
        var body = allocator.buffer(capacity: 64)
        switch self {
        case .startup(let options):
            body.writeCQLStringMap(options)
        case .options:
            break
        case .query(let query, let parameters):
            body.writeCQLLongString(query)
            parameters.write(into: &body)
        case .prepare(let query):
            body.writeCQLLongString(query)
        case .execute(let id, let parameters):
            body.writeCQLShortBytes(id)
            parameters.write(into: &body)
        }
        return Frame(opcode: opcode, body: body)
    }
}
