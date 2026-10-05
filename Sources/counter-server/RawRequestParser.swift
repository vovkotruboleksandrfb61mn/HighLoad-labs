#if canImport(Glibc)
import Glibc
#elseif canImport(Musl)
import Musl
#elseif canImport(Darwin)
import Darwin
#endif

/// The operations the counter answers, plus everything else.
enum RawRoute: Equatable, Sendable {
    case increment
    case count
    case reset
    case notFound
}

/// One parsed request: what it asks for and how many input bytes it took.
struct RawRequest: Equatable, Sendable {
    var route: RawRoute
    var keepAlive: Bool
    /// The request was HTTP/1.0, so a kept-alive reply must say so.
    var isHTTP10: Bool
    /// Bytes the request occupies in the input: request line, headers, body.
    var length: Int
}

/// A minimal HTTP/1.x request parser for the counter's three routes.
///
/// It reads only what the server needs: the method and path (matched as one
/// 8-byte word, the way the fastest One Billion Row Challenge entries match
/// keys), the version, and the `Connection` and `Content-Length` headers.
/// Nothing is allocated and nothing is copied.
enum RawRequestParser {
    enum Outcome: Equatable, Sendable {
        case request(RawRequest)
        /// More bytes are needed before the request is complete.
        case incomplete
        /// Not a request this parser can answer; the connection should close.
        case malformed
    }

    /// Requests whose headers do not end within this many bytes are rejected.
    static let maximumHeaderLength = 8192

    // The first 8 bytes of each request line and the two versions, as
    // little-endian words. RawRequestParserTests checks them against the text.
    static let getInc: UInt64 = 0x636E_692F_2054_4547    // "GET /inc"
    static let getCou: UInt64 = 0x756F_632F_2054_4547    // "GET /cou"
    static let postRe: UInt64 = 0x6572_2F20_5453_4F50    // "POST /re"
    static let http11: UInt64 = 0x312E_312F_5054_5448    // "HTTP/1.1"
    static let http10: UInt64 = 0x302E_312F_5054_5448    // "HTTP/1.0"

    private static let cr = UInt8(ascii: "\r")
    private static let lf = UInt8(ascii: "\n")

    static func parse(_ bytes: UnsafeRawBufferPointer) -> Outcome {
        guard let headerEnd = endOfHeaders(in: bytes) else {
            return bytes.count > maximumHeaderLength ? .malformed : .incomplete
        }
        // headerEnd is the index just past the blank line.
        guard let lineEnd = firstLineFeed(in: bytes, from: 0, to: headerEnd),
              lineEnd >= 1 + 8, bytes[lineEnd - 1] == cr else {
            return .malformed
        }
        let requestLineEnd = lineEnd - 1    // index of "\r"

        let version = word(in: bytes, at: requestLineEnd - 8)
        let isHTTP10: Bool
        switch version {
        case http11: isHTTP10 = false
        case http10: isHTTP10 = true
        default: return .malformed
        }

        let route = route(of: bytes, requestLineEnd: requestLineEnd)

        var keepAlive = !isHTTP10
        var contentLength = 0
        var lineStart = lineEnd + 1
        while lineStart < headerEnd - 2, let end = firstLineFeed(in: bytes, from: lineStart, to: headerEnd) {
            let header = UnsafeRawBufferPointer(rebasing: bytes[lineStart..<(end - 1)])
            if let value = value(of: "connection:", in: header) {
                if contains(value, "close") {
                    keepAlive = false
                } else if contains(value, "keep-alive") {
                    keepAlive = true
                }
            } else if let value = value(of: "content-length:", in: header) {
                guard let length = decimal(value) else { return .malformed }
                contentLength = length
            }
            lineStart = end + 1
        }

        let length = headerEnd + contentLength
        guard bytes.count >= length else { return .incomplete }
        return .request(RawRequest(route: route, keepAlive: keepAlive, isHTTP10: isHTTP10, length: length))
    }

    // MARK: - Pieces

    private static func route(of bytes: UnsafeRawBufferPointer, requestLineEnd: Int) -> RawRoute {
        // The byte after the path must end it: a space, or "?" before a query.
        func pathEnds(at index: Int) -> Bool {
            index < requestLineEnd && (bytes[index] == UInt8(ascii: " ") || bytes[index] == UInt8(ascii: "?"))
        }
        switch word(in: bytes, at: 0) {
        case getInc where pathEnds(at: 8):
            return .increment
        case getCou where matches(bytes, at: 8, "nt") && pathEnds(at: 10):
            return .count
        case postRe where matches(bytes, at: 8, "set") && pathEnds(at: 11):
            return .reset
        default:
            return .notFound
        }
    }

    /// The index just past the first "\r\n\r\n", or nil when there is none.
    private static func endOfHeaders(in bytes: UnsafeRawBufferPointer) -> Int? {
        var from = 0
        while let lf = firstLineFeed(in: bytes, from: from, to: bytes.count) {
            if lf >= 3, bytes[lf - 1] == cr, bytes[lf - 2] == Self.lf, bytes[lf - 3] == cr {
                return lf + 1
            }
            from = lf + 1
        }
        return nil
    }

    /// Finds "\n" with libc's vectorised `memchr`.
    private static func firstLineFeed(in bytes: UnsafeRawBufferPointer, from: Int, to end: Int) -> Int? {
        guard from < end, let base = bytes.baseAddress,
              let found = memchr(base + from, Int32(lf), end - from) else {
            return nil
        }
        return base.distance(to: UnsafeRawPointer(found))
    }

    private static func word(in bytes: UnsafeRawBufferPointer, at offset: Int) -> UInt64 {
        UInt64(littleEndian: bytes.loadUnaligned(fromByteOffset: offset, as: UInt64.self))
    }

    private static func matches(_ bytes: UnsafeRawBufferPointer, at offset: Int, _ text: StaticString) -> Bool {
        guard offset + text.utf8CodeUnitCount <= bytes.count else { return false }
        for index in 0..<text.utf8CodeUnitCount where bytes[offset + index] != text.utf8Start[index] {
            return false
        }
        return true
    }

    /// The value of a header line if its name is `name` (lowercase, with the
    /// colon), compared case-insensitively, with surrounding spaces trimmed.
    private static func value(of name: StaticString, in line: UnsafeRawBufferPointer) -> UnsafeRawBufferPointer? {
        let count = name.utf8CodeUnitCount
        guard line.count >= count else { return nil }
        for index in 0..<count where lowercased(line[index]) != name.utf8Start[index] {
            return nil
        }
        var start = count
        var end = line.count
        while start < end, line[start] == UInt8(ascii: " ") || line[start] == UInt8(ascii: "\t") { start += 1 }
        while end > start, line[end - 1] == UInt8(ascii: " ") || line[end - 1] == UInt8(ascii: "\t") { end -= 1 }
        return UnsafeRawBufferPointer(rebasing: line[start..<end])
    }

    /// Whether `value` contains `token`, ignoring ASCII case.
    private static func contains(_ value: UnsafeRawBufferPointer, _ token: StaticString) -> Bool {
        let count = token.utf8CodeUnitCount
        guard value.count >= count else { return false }
        outer: for start in 0...(value.count - count) {
            for index in 0..<count where lowercased(value[start + index]) != token.utf8Start[index] {
                continue outer
            }
            return true
        }
        return false
    }

    private static func decimal(_ digits: UnsafeRawBufferPointer) -> Int? {
        guard !digits.isEmpty, digits.count <= 18 else { return nil }
        var result = 0
        for byte in digits {
            guard byte >= UInt8(ascii: "0"), byte <= UInt8(ascii: "9") else { return nil }
            result = result * 10 + Int(byte - UInt8(ascii: "0"))
        }
        return result
    }

    private static func lowercased(_ byte: UInt8) -> UInt8 {
        byte >= UInt8(ascii: "A") && byte <= UInt8(ascii: "Z") ? byte | 0x20 : byte
    }
}
