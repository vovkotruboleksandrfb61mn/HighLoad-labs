import NIOCore

// The notation types of the native protocol spec ([string], [bytes], ...).
// Readers throw on truncated input instead of returning nil, so a short
// body surfaces as a protocol violation rather than a silent default.

extension ByteBuffer {
    // MARK: Writing

    /// [string]: a short length, then UTF-8.
    mutating func writeCQLString(_ string: String) {
        let lengthIndex = writerIndex
        writeInteger(UInt16(0))
        let written = writeString(string)
        setInteger(UInt16(written), at: lengthIndex)
    }

    /// [long string]: an int length, then UTF-8.
    mutating func writeCQLLongString(_ string: String) {
        let lengthIndex = writerIndex
        writeInteger(Int32(0))
        let written = writeString(string)
        setInteger(Int32(written), at: lengthIndex)
    }

    /// [short bytes].
    mutating func writeCQLShortBytes(_ bytes: [UInt8]) {
        writeInteger(UInt16(bytes.count))
        writeBytes(bytes)
    }

    /// [bytes]: an int length (-1 for null), then the bytes.
    mutating func writeCQLBytes(_ bytes: ByteBuffer?) {
        guard let bytes else {
            writeInteger(Int32(-1))
            return
        }
        writeInteger(Int32(bytes.readableBytes))
        writeImmutableBuffer(bytes)
    }

    /// [string map].
    mutating func writeCQLStringMap(_ map: [String: String]) {
        writeInteger(UInt16(map.count))
        for (key, value) in map.sorted(by: { $0.key < $1.key }) {
            writeCQLString(key)
            writeCQLString(value)
        }
    }

    // MARK: Reading

    mutating func readCQLInt8() throws(CQLError) -> UInt8 {
        guard let value = readInteger(as: UInt8.self) else { throw .truncated("byte") }
        return value
    }

    mutating func readCQLShort() throws(CQLError) -> UInt16 {
        guard let value = readInteger(as: UInt16.self) else { throw .truncated("short") }
        return value
    }

    mutating func readCQLInt() throws(CQLError) -> Int32 {
        guard let value = readInteger(as: Int32.self) else { throw .truncated("int") }
        return value
    }

    mutating func readCQLLong() throws(CQLError) -> Int64 {
        guard let value = readInteger(as: Int64.self) else { throw .truncated("long") }
        return value
    }

    mutating func readCQLString() throws(CQLError) -> String {
        let length = Int(try readCQLShort())
        guard let string = readString(length: length) else { throw .truncated("string") }
        return string
    }

    mutating func readCQLLongString() throws(CQLError) -> String {
        let length = Int(try readCQLInt())
        guard length >= 0, let string = readString(length: length) else { throw .truncated("long string") }
        return string
    }

    mutating func readCQLShortBytes() throws(CQLError) -> [UInt8] {
        let length = Int(try readCQLShort())
        guard let bytes = readBytes(length: length) else { throw .truncated("short bytes") }
        return bytes
    }

    /// [bytes] as a slice of this buffer; nil for a null value.
    mutating func readCQLBytes() throws(CQLError) -> ByteBuffer? {
        let length = Int(try readCQLInt())
        if length < 0 {
            return nil
        }
        guard let slice = readSlice(length: length) else { throw .truncated("bytes") }
        return slice
    }

    mutating func readCQLStringList() throws(CQLError) -> [String] {
        let count = Int(try readCQLShort())
        var list: [String] = []
        list.reserveCapacity(count)
        for _ in 0..<count {
            list.append(try readCQLString())
        }
        return list
    }

    mutating func readCQLStringMultimap() throws(CQLError) -> [String: [String]] {
        let count = Int(try readCQLShort())
        var map: [String: [String]] = [:]
        for _ in 0..<count {
            let key = try readCQLString()
            map[key] = try readCQLStringList()
        }
        return map
    }

    /// [bytes map]: skipped, we never ask for custom payloads.
    mutating func skipCQLBytesMap() throws(CQLError) {
        let count = Int(try readCQLShort())
        for _ in 0..<count {
            _ = try readCQLString()
            _ = try readCQLBytes()
        }
    }

    mutating func readCQLConsistency() throws(CQLError) -> Consistency {
        let raw = try readCQLShort()
        guard let level = Consistency(rawValue: raw) else { throw .protocolViolation("unknown consistency \(raw)") }
        return level
    }
}
