import NIOCore

/// A column of a result or a bind marker of a prepared statement.
public struct ColumnSpec: Sendable, Hashable {
    public var keyspace: String
    public var table: String
    public var name: String
    public var type: DataType

    public init(keyspace: String, table: String, name: String, type: DataType) {
        self.keyspace = keyspace
        self.table = table
        self.name = name
        self.type = type
    }
}

/// The columns of a Rows result, shared by all its rows.
public final class ColumnLayout: Sendable {
    public let columns: [ColumnSpec]
    private let indexByName: [String: Int]

    public init(_ columns: [ColumnSpec]) {
        self.columns = columns
        var index: [String: Int] = [:]
        for (position, column) in columns.enumerated() where index[column.name] == nil {
            index[column.name] = position
        }
        self.indexByName = index
    }

    public func index(of name: String) -> Int? {
        indexByName[name]
    }
}

public struct CQLRow: Sendable {
    public let layout: ColumnLayout
    public let values: [CQLValue]

    public subscript(index: Int) -> CQLValue {
        values[index]
    }

    /// The cell of column `name`; null if the result has no such column.
    public subscript(name: String) -> CQLValue {
        layout.index(of: name).map { values[$0] } ?? .null
    }
}

public struct CQLRows: Sendable {
    public let layout: ColumnLayout
    public let rows: [CQLRow]
    /// Set when more pages follow.
    public let pagingState: ByteBuffer?

    public var columns: [ColumnSpec] { layout.columns }
}

/// What a SCHEMA_CHANGE result reports.
public struct SchemaChange: Sendable, Equatable {
    /// CREATED, UPDATED or DROPPED.
    public var change: String
    /// KEYSPACE, TABLE, TYPE, FUNCTION or AGGREGATE.
    public var target: String
    public var keyspace: String
    /// The table, type, function or aggregate name; empty for a keyspace.
    public var name: String
}

/// The answer to PREPARE: the statement id and the types of its markers and
/// result columns.
public struct PreparedStatement: Sendable {
    public let query: String
    public let id: [UInt8]
    /// The bind markers, in order.
    public let parameters: [ColumnSpec]
    /// The marker positions that hold the partition key components, in key
    /// order (empty when the markers do not cover the whole key).
    public let partitionKeyIndices: [Int]
    /// The result columns; nil when the server did not send them, so EXECUTE
    /// must not skip metadata.
    public let resultLayout: ColumnLayout?

    /// The routing keyspace (of the first marker).
    public var keyspace: String? { parameters.first?.keyspace }
}

public enum CQLResult: Sendable {
    case void
    case rows(CQLRows)
    case setKeyspace(String)
    case prepared(PreparedStatement)
    case schemaChange(SchemaChange)

    /// The rows of a Rows result; empty for any other kind.
    public var rows: [CQLRow] {
        if case .rows(let rows) = self { rows.rows } else { [] }
    }
}

// MARK: - Decoding

/// The responses this client understands.
enum Response: Sendable {
    case ready
    case authenticate(String)
    case supported([String: [String]])
    case result(CQLResult)

    /// Decodes `frame`, throwing `CQLError.server` for an ERROR message.
    /// `query` is the text of a PREPARE request, `knownLayout` the result
    /// columns an EXECUTE asked the server to skip.
    static func decode(_ frame: Frame, query: String = "", knownLayout: ColumnLayout? = nil) throws(CQLError) -> Response {
        var body = frame.body
        // Prefixes that flags announce, in the order the server writes them.
        if frame.flags.contains(.tracing) {
            guard body.readSlice(length: 16) != nil else { throw .truncated("tracing id") }
        }
        if frame.flags.contains(.warning) {
            _ = try body.readCQLStringList()
        }
        if frame.flags.contains(.customPayload) {
            try body.skipCQLBytesMap()
        }
        switch frame.opcode {
        case .error:
            throw .server(try ServerError.decode(&body))
        case .ready:
            return .ready
        case .authenticate:
            return .authenticate(try body.readCQLString())
        case .supported:
            return .supported(try body.readCQLStringMultimap())
        case .result:
            return .result(try decodeResult(&body, query: query, knownLayout: knownLayout))
        default:
            throw .unexpectedResponse(frame.opcode)
        }
    }

    static func decodeResult(_ body: inout ByteBuffer, query: String, knownLayout: ColumnLayout?) throws(CQLError) -> CQLResult {
        let kind = try body.readCQLInt()
        switch kind {
        case 0x0001:
            return .void
        case 0x0002:
            let metadata = try RowsMetadata.decode(&body)
            guard let layout = metadata.layout ?? knownLayout else {
                throw .protocolViolation("a Rows result without metadata for an unknown statement")
            }
            guard layout.columns.count == metadata.columnCount else {
                throw .protocolViolation("\(metadata.columnCount) columns, expected \(layout.columns.count)")
            }
            let rowCount = Int(try body.readCQLInt())
            var rows: [CQLRow] = []
            rows.reserveCapacity(max(0, rowCount))
            for _ in 0..<max(0, rowCount) {
                var values: [CQLValue] = []
                values.reserveCapacity(layout.columns.count)
                for column in layout.columns {
                    values.append(try CQLValue.decode(try body.readCQLBytes(), as: column.type))
                }
                rows.append(CQLRow(layout: layout, values: values))
            }
            return .rows(CQLRows(layout: layout, rows: rows, pagingState: metadata.pagingState))
        case 0x0003:
            return .setKeyspace(try body.readCQLString())
        case 0x0004:
            let id = try body.readCQLShortBytes()
            let (parameters, partitionKeyIndices) = try decodePreparedMetadata(&body)
            let resultMetadata = try RowsMetadata.decode(&body)
            return .prepared(
                PreparedStatement(
                    query: query,
                    id: id,
                    parameters: parameters,
                    partitionKeyIndices: partitionKeyIndices,
                    resultLayout: resultMetadata.layout
                )
            )
        case 0x0005:
            let change = try body.readCQLString()
            let target = try body.readCQLString()
            let keyspace = try body.readCQLString()
            let name = target == "KEYSPACE" ? "" : try body.readCQLString()
            // FUNCTION and AGGREGATE append argument types; nothing needs them.
            return .schemaChange(SchemaChange(change: change, target: target, keyspace: keyspace, name: name))
        default:
            throw .protocolViolation("unknown result kind \(kind)")
        }
    }

    /// The bind marker metadata of a Prepared result (v4 layout).
    static func decodePreparedMetadata(_ body: inout ByteBuffer) throws(CQLError) -> ([ColumnSpec], [Int]) {
        let flags = try body.readCQLInt()
        let columnCount = Int(try body.readCQLInt())
        let keyCount = Int(try body.readCQLInt())
        var keyIndices: [Int] = []
        for _ in 0..<max(0, keyCount) {
            keyIndices.append(Int(try body.readCQLShort()))
        }
        let columns = try RowsMetadata.decodeColumns(&body, count: columnCount, globalTableSpec: flags & 0x0001 != 0)
        return (columns, keyIndices)
    }
}

/// The metadata of a Rows result.
struct RowsMetadata {
    var columnCount: Int
    var pagingState: ByteBuffer?
    /// nil when the server left the column specs out (NO_METADATA).
    var layout: ColumnLayout?

    static func decode(_ body: inout ByteBuffer) throws(CQLError) -> RowsMetadata {
        let flags = try body.readCQLInt()
        let columnCount = Int(try body.readCQLInt())
        let pagingState = flags & 0x0002 != 0 ? try body.readCQLBytes() : nil
        if flags & 0x0004 != 0 {
            return RowsMetadata(columnCount: columnCount, pagingState: pagingState, layout: nil)
        }
        let columns = try decodeColumns(&body, count: columnCount, globalTableSpec: flags & 0x0001 != 0)
        return RowsMetadata(columnCount: columnCount, pagingState: pagingState, layout: ColumnLayout(columns))
    }

    static func decodeColumns(_ body: inout ByteBuffer, count: Int, globalTableSpec: Bool) throws(CQLError) -> [ColumnSpec] {
        var keyspace = ""
        var table = ""
        if globalTableSpec {
            keyspace = try body.readCQLString()
            table = try body.readCQLString()
        }
        var columns: [ColumnSpec] = []
        columns.reserveCapacity(max(0, count))
        for _ in 0..<max(0, count) {
            if !globalTableSpec {
                keyspace = try body.readCQLString()
                table = try body.readCQLString()
            }
            let name = try body.readCQLString()
            columns.append(ColumnSpec(keyspace: keyspace, table: table, name: name, type: try DataType.decode(&body)))
        }
        return columns
    }
}
