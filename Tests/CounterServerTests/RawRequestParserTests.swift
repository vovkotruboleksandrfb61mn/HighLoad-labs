import Testing
@testable import counter_server

@Suite("RawRequestParser")
struct RawRequestParserTests {
    private func parse(_ text: String) -> RawRequestParser.Outcome {
        Array(text.utf8).withUnsafeBytes { RawRequestParser.parse($0) }
    }

    private func word(_ text: String) -> UInt64 {
        Array(text.utf8).withUnsafeBytes { UInt64(littleEndian: $0.loadUnaligned(as: UInt64.self)) }
    }

    @Test("the 8-byte words match their text")
    func words() {
        #expect(RawRequestParser.getInc == word("GET /inc"))
        #expect(RawRequestParser.getCou == word("GET /cou"))
        #expect(RawRequestParser.postRe == word("POST /re"))
        #expect(RawRequestParser.http11 == word("HTTP/1.1"))
        #expect(RawRequestParser.http10 == word("HTTP/1.0"))
    }

    @Test("routes", arguments: [
        ("GET /inc HTTP/1.1", RawRoute.increment),
        ("GET /inc?x=1 HTTP/1.1", .increment),
        ("GET /count HTTP/1.1", .count),
        ("POST /reset HTTP/1.1", .reset),
        ("GET /increment HTTP/1.1", .notFound),
        ("GET /counter HTTP/1.1", .notFound),
        ("POST /inc HTTP/1.1", .notFound),
        ("GET /reset HTTP/1.1", .notFound),
        ("GET / HTTP/1.1", .notFound),
    ])
    func routes(line: String, route: RawRoute) {
        let text = "\(line)\r\nHost: x\r\n\r\n"
        #expect(parse(text) == .request(RawRequest(route: route, keepAlive: true, isHTTP10: false, length: text.utf8.count)))
    }

    @Test("keep-alive follows the version and the Connection header", arguments: [
        ("HTTP/1.1", "", true, false),
        ("HTTP/1.1", "Connection: close\r\n", false, false),
        ("HTTP/1.1", "connection: Close\r\n", false, false),
        ("HTTP/1.0", "", false, true),
        ("HTTP/1.0", "Connection: keep-alive\r\n", true, true),
    ])
    func keepAlive(version: String, header: String, keepAlive: Bool, isHTTP10: Bool) {
        let text = "GET /inc \(version)\r\n\(header)\r\n"
        #expect(parse(text) == .request(RawRequest(route: .increment, keepAlive: keepAlive, isHTTP10: isHTTP10, length: text.utf8.count)))
    }

    @Test("a body named by Content-Length belongs to the request")
    func body() {
        let head = "POST /reset HTTP/1.1\r\nContent-Length: 3\r\n\r\n"
        #expect(parse(head + "ab") == .incomplete)
        #expect(parse(head + "abc") == .request(RawRequest(route: .reset, keepAlive: true, isHTTP10: false, length: head.utf8.count + 3)))
    }

    @Test("incomplete and malformed input")
    func incompleteAndMalformed() {
        #expect(parse("GET /inc HTTP/1.1\r\nHost: x\r\n") == .incomplete)
        #expect(parse("") == .incomplete)
        #expect(parse("GET /inc HTTP/2.0\r\n\r\n") == .malformed)
        #expect(parse("GET /inc\r\n\r\n") == .malformed)
        #expect(parse("POST /reset HTTP/1.1\r\nContent-Length: x\r\n\r\n") == .malformed)
        #expect(parse("GET /inc HTTP/1.1\r\n" + String(repeating: "a", count: RawRequestParser.maximumHeaderLength)) == .malformed)
    }

    @Test("pipelined requests are parsed one at a time")
    func pipelined() {
        let first = "GET /inc HTTP/1.1\r\n\r\n"
        #expect(parse(first + "GET /count HTTP/1.1\r\n\r\n") == .request(RawRequest(route: .increment, keepAlive: true, isHTTP10: false, length: first.utf8.count)))
    }
}
