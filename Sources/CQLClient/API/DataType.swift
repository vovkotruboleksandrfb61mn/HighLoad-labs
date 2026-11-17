import NIOCore

/// A column type, as described by an [option] in result metadata.
public indirect enum DataType: Sendable, Hashable {
    case custom(String)
    case ascii
    case bigint
    case blob
    case boolean
    case counter
    case decimal
    case double
    case float
    case int
    case timestamp
    case uuid
    case varchar
    case varint
    case timeuuid
    case inet
    case date
    case time
    case smallint
    case tinyint
    case duration
    case list(DataType)
    case map(DataType, DataType)
    case set(DataType)
    case udt(keyspace: String, name: String, fields: [Field])
    case tuple([DataType])

    public struct Field: Sendable, Hashable {
        public var name: String
        public var type: DataType
    }

    static func decode(_ buffer: inout ByteBuffer) throws(CQLError) -> DataType {
        let id = try buffer.readCQLShort()
        switch id {
        case 0x0000: return .custom(try buffer.readCQLString())
        case 0x0001: return .ascii
        case 0x0002: return .bigint
        case 0x0003: return .blob
        case 0x0004: return .boolean
        case 0x0005: return .counter
        case 0x0006: return .decimal
        case 0x0007: return .double
        case 0x0008: return .float
        case 0x0009: return .int
        case 0x000B: return .timestamp
        case 0x000C: return .uuid
        case 0x000D: return .varchar
        case 0x000E: return .varint
        case 0x000F: return .timeuuid
        case 0x0010: return .inet
        case 0x0011: return .date
        case 0x0012: return .time
        case 0x0013: return .smallint
        case 0x0014: return .tinyint
        case 0x0015: return .duration
        case 0x0020: return .list(try decode(&buffer))
        case 0x0021: return .map(try decode(&buffer), try decode(&buffer))
        case 0x0022: return .set(try decode(&buffer))
        case 0x0030:
            let keyspace = try buffer.readCQLString()
            let name = try buffer.readCQLString()
            let count = Int(try buffer.readCQLShort())
            var fields: [Field] = []
            for _ in 0..<count {
                fields.append(Field(name: try buffer.readCQLString(), type: try decode(&buffer)))
            }
            return .udt(keyspace: keyspace, name: name, fields: fields)
        case 0x0031:
            let count = Int(try buffer.readCQLShort())
            var types: [DataType] = []
            for _ in 0..<count {
                types.append(try decode(&buffer))
            }
            return .tuple(types)
        default:
            throw .protocolViolation("unknown type id 0x\(String(id, radix: 16))")
        }
    }
}
