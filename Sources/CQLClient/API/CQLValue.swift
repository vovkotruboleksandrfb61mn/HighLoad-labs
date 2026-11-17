import NIOCore

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

/// A bound parameter or a decoded cell.
///
/// The types the counter needs (int, bigint, counter, text, uuid) plus a few
/// cheap ones; anything else decodes to `.raw` with its serialized bytes.
public indirect enum CQLValue: Sendable, Hashable {
    case null
    case int(Int32)
    case bigint(Int64)
    case counter(Int64)
    case smallint(Int16)
    case tinyint(Int8)
    case text(String)
    case uuid(UUID)
    case boolean(Bool)
    case double(Double)
    case float(Float)
    /// Milliseconds since the Unix epoch.
    case timestamp(Int64)
    case blob([UInt8])
    /// 4 or 16 address bytes.
    case inet([UInt8])
    case list([CQLValue])
    case set([CQLValue])
    case raw(ByteBuffer)

    // MARK: Accessors

    /// Any integer type, widened.
    public var int64: Int64? {
        switch self {
        case .int(let value): Int64(value)
        case .bigint(let value), .counter(let value), .timestamp(let value): value
        case .smallint(let value): Int64(value)
        case .tinyint(let value): Int64(value)
        default: nil
        }
    }

    public var int: Int? { int64.map { Int($0) } }

    public var string: String? {
        if case .text(let value) = self { value } else { nil }
    }

    public var uuidValue: UUID? {
        if case .uuid(let value) = self { value } else { nil }
    }

    public var isNull: Bool { self == .null }

    /// The elements of a list or set.
    public var elements: [CQLValue]? {
        switch self {
        case .list(let values), .set(let values): values
        default: nil
        }
    }

    /// An inet as dotted-quad (IPv4) or colon-separated hex (IPv6).
    public var inetAddress: String? {
        guard case .inet(let bytes) = self else { return nil }
        if bytes.count == 4 {
            return bytes.map(String.init).joined(separator: ".")
        }
        return stride(from: 0, to: bytes.count, by: 2).map { index in
            String(UInt16(bytes[index]) << 8 | UInt16(bytes[index + 1]), radix: 16)
        }.joined(separator: ":")
    }

    // MARK: Serialization

    /// The value's bytes for a bound parameter (nil for null). With the
    /// parameter's declared `type`, integers are widened or narrowed to fit
    /// it, so `.int(1)` binds to a bigint or counter column.
    public func serialized(as type: DataType? = nil) throws(CQLError) -> ByteBuffer? {
        var buffer = ByteBuffer()
        switch (self, type) {
        case (.null, _):
            return nil
        case (_, .bigint?), (_, .counter?), (_, .timestamp?):
            guard let value = int64 else { throw .invalidArgument("\(self) does not bind to \(type!)") }
            buffer.writeInteger(value)
        case (_, .int?):
            guard let value = int64, let narrow = Int32(exactly: value) else { throw .invalidArgument("\(self) does not bind to int") }
            buffer.writeInteger(narrow)
        case (.int(let value), _): buffer.writeInteger(value)
        case (.bigint(let value), _), (.counter(let value), _), (.timestamp(let value), _): buffer.writeInteger(value)
        case (.smallint(let value), _): buffer.writeInteger(value)
        case (.tinyint(let value), _): buffer.writeInteger(value)
        case (.text(let value), _): buffer.writeString(value)
        case (.uuid(let value), _): _ = withUnsafeBytes(of: value.uuid) { buffer.writeBytes($0) }
        case (.boolean(let value), _): buffer.writeInteger(UInt8(value ? 1 : 0))
        case (.double(let value), _): buffer.writeInteger(value.bitPattern)
        case (.float(let value), _): buffer.writeInteger(value.bitPattern)
        case (.blob(let bytes), _), (.inet(let bytes), _): buffer.writeBytes(bytes)
        case (.list(let values), _), (.set(let values), _):
            let elementType: DataType? =
                switch type {
                case .list(let element)?, .set(let element)?: element
                default: nil
                }
            buffer.writeInteger(Int32(values.count))
            for value in values {
                buffer.writeCQLBytes(try value.serialized(as: elementType))
            }
        case (.raw(let bytes), _): buffer.writeImmutableBuffer(bytes)
        }
        return buffer
    }

    /// Decodes a cell of `type`; nil bytes are a null cell.
    public static func decode(_ bytes: ByteBuffer?, as type: DataType) throws(CQLError) -> CQLValue {
        guard var bytes else { return .null }
        func fixed<T: FixedWidthInteger>(_: T.Type) throws(CQLError) -> T {
            guard bytes.readableBytes == MemoryLayout<T>.size, let value = bytes.readInteger(as: T.self) else {
                throw .protocolViolation("a \(type) cell of \(bytes.readableBytes) bytes")
            }
            return value
        }
        switch type {
        case .int: return .int(try fixed(Int32.self))
        case .bigint: return .bigint(try fixed(Int64.self))
        case .counter: return .counter(try fixed(Int64.self))
        case .timestamp: return .timestamp(try fixed(Int64.self))
        case .smallint: return .smallint(try fixed(Int16.self))
        case .tinyint: return .tinyint(try fixed(Int8.self))
        case .boolean: return .boolean(try fixed(UInt8.self) != 0)
        case .double: return .double(Double(bitPattern: try fixed(UInt64.self)))
        case .float: return .float(Float(bitPattern: try fixed(UInt32.self)))
        case .varchar, .ascii: return .text(bytes.readString(length: bytes.readableBytes) ?? "")
        case .uuid, .timeuuid:
            guard bytes.readableBytes == 16, let raw = bytes.readBytes(length: 16) else {
                throw .protocolViolation("a uuid cell of \(bytes.readableBytes) bytes")
            }
            return .uuid(UUID(uuid: (raw[0], raw[1], raw[2], raw[3], raw[4], raw[5], raw[6], raw[7], raw[8], raw[9], raw[10], raw[11], raw[12], raw[13], raw[14], raw[15])))
        case .blob: return .blob(bytes.readBytes(length: bytes.readableBytes) ?? [])
        case .inet: return .inet(bytes.readBytes(length: bytes.readableBytes) ?? [])
        case .list(let element), .set(let element):
            let count = Int(try bytes.readCQLInt())
            var values: [CQLValue] = []
            values.reserveCapacity(max(0, count))
            for _ in 0..<max(0, count) {
                values.append(try decode(try bytes.readCQLBytes(), as: element))
            }
            if case .list = type { return .list(values) }
            return .set(values)
        default:
            return .raw(bytes)
        }
    }
}

extension CQLValue: ExpressibleByStringLiteral, ExpressibleByIntegerLiteral {
    public init(stringLiteral value: String) {
        self = .text(value)
    }

    /// Integer literals bind as bigint (or as whatever the prepared
    /// statement's parameter type says).
    public init(integerLiteral value: Int64) {
        self = .bigint(value)
    }
}
