@testable import CQLClient
import Foundation
import NIOCore
import Testing

/// Builds response bodies field by field, as the server writes them. A
/// class, so the field calls chain.
final class BodyWriter {
    var buffer = ByteBuffer()

    @discardableResult func int(_ value: Int32) -> Self { buffer.writeInteger(value); return self }
    @discardableResult func short(_ value: UInt16) -> Self { buffer.writeInteger(value); return self }
    @discardableResult func byte(_ value: UInt8) -> Self { buffer.writeInteger(value); return self }
    @discardableResult func string(_ value: String) -> Self { buffer.writeCQLString(value); return self }
    @discardableResult func bytes(_ value: [UInt8]?) -> Self {
        buffer.writeCQLBytes(value.map { ByteBuffer(bytes: $0) })
        return self
    }

    func frame(_ opcode: Opcode, flags: FrameFlags = [], stream: Int16 = 0) -> Frame {
        Frame(version: ProtocolVersion.response, flags: flags, stream: stream, opcode: opcode, body: buffer)
    }
}

func serverError(of frame: Frame) -> ServerError? {
    do {
        _ = try Response.decode(frame)
        return nil
    } catch {
        return error.serverError
    }
}

@Suite("Response decoding")
struct ResponseDecodingTests {
    @Test("Unavailable carries consistency, required and alive")
    func unavailable() {
        let body = BodyWriter()
        body.int(0x1000).string("Cannot achieve consistency level QUORUM").short(0x0004).int(2).int(1)
        let error = serverError(of: body.frame(.error))
        #expect(error?.code == .unavailable)
        #expect(error?.message == "Cannot achieve consistency level QUORUM")
        #expect(error?.details == .unavailable(consistency: .quorum, required: 2, alive: 1))
    }

    @Test("Write_timeout carries received, blockfor and the write type")
    func writeTimeout() {
        let body = BodyWriter()
        body.int(0x1100).string("Operation timed out").short(0x0005).int(2).int(3).string("COUNTER")
        let error = serverError(of: body.frame(.error))
        #expect(error?.code == .writeTimeout)
        #expect(error?.details == .writeTimeout(consistency: .all, received: 2, blockFor: 3, writeType: "COUNTER"))
        #expect(error?.description == "Write_timeout: Operation timed out (ALL, received 2 of 3, COUNTER)")
    }

    @Test("Read_timeout carries the data_present byte")
    func readTimeout() {
        let body = BodyWriter()
        body.int(0x1200).string("timeout").short(0x0001).int(0).int(1).byte(0)
        #expect(serverError(of: body.frame(.error))?.details == .readTimeout(consistency: .one, received: 0, blockFor: 1, dataPresent: false))
    }

    @Test("Write_failure carries the failure count")
    func writeFailure() {
        let body = BodyWriter()
        body.int(0x1500).string("failed").short(0x0004).int(1).int(2).int(1).string("SIMPLE")
        #expect(
            serverError(of: body.frame(.error))?.details
                == .writeFailure(consistency: .quorum, received: 1, blockFor: 2, failures: 1, writeType: "SIMPLE")
        )
    }

    @Test("Overloaded, Invalid and Syntax_error have just a message")
    func plainErrors() {
        for (code, expected) in [(Int32(0x1001), ServerError.Code.overloaded), (0x2200, .invalid), (0x2000, .syntaxError)] {
            let body = BodyWriter()
            body.int(code).string("nope")
            let error = serverError(of: body.frame(.error))
            #expect(error == ServerError(code: expected, message: "nope"))
        }
    }

    @Test("Unprepared and Already_exists decode their details")
    func detailedErrors() {
        let unprepared = BodyWriter()
        unprepared.int(0x2500).string("unknown id").short(2).byte(0xAB).byte(0xCD)
        #expect(serverError(of: unprepared.frame(.error))?.details == .unprepared(id: [0xAB, 0xCD]))
        let exists = BodyWriter()
        exists.int(0x2400).string("exists").string("ks").string("t")
        #expect(serverError(of: exists.frame(.error))?.details == .alreadyExists(keyspace: "ks", table: "t"))
    }

    @Test("a truncated error body is a protocol violation, not a crash")
    func truncatedError() {
        let body = BodyWriter()
        body.int(0x1000).string("x").short(4)
        #expect(throws: CQLError.truncated("int")) { _ = try Response.decode(body.frame(.error)) }
    }

    @Test("Rows with int, bigint, counter, text, uuid, set<text>, inet and a null")
    func rows() throws {
        let uuid = UUID(uuidString: "6BA7B810-9DAD-11D1-80B4-00C04FD430C8")!
        let body = BodyWriter()
        body.int(0x0002)
        body.int(0x0001).int(8).string("ks").string("t")  // global table spec
        body.string("i").short(0x0009)
        body.string("b").short(0x0002)
        body.string("c").short(0x0005)
        body.string("s").short(0x000D)
        body.string("u").short(0x000C)
        body.string("tokens").short(0x0022).short(0x000D)
        body.string("addr").short(0x0010)
        body.string("n").short(0x0009)
        body.int(1)
        body.bytes([0xFF, 0xFF, 0xFF, 0xFE])
        body.bytes([0, 0, 0, 0, 0, 0, 0x01, 0x00])
        body.bytes([0, 0, 0, 0, 0, 0, 0, 42])
        body.bytes(Array("post-1".utf8))
        body.bytes(withUnsafeBytes(of: uuid.uuid) { Array($0) })
        body.bytes([0, 0, 0, 2, 0, 0, 0, 2] + Array("-5".utf8) + [0, 0, 0, 1] + Array("7".utf8))
        body.bytes([127, 0, 0, 21])
        body.bytes(nil)

        guard case .result(.rows(let rows)) = try Response.decode(body.frame(.result)) else {
            Issue.record("not a Rows result")
            return
        }
        #expect(rows.columns.map(\.name) == ["i", "b", "c", "s", "u", "tokens", "addr", "n"])
        #expect(rows.columns.allSatisfy { $0.keyspace == "ks" && $0.table == "t" })
        let row = try #require(rows.rows.first)
        #expect(row["i"] == .int(-2))
        #expect(row["b"] == .bigint(256))
        #expect(row["c"] == .counter(42))
        #expect(row["c"].int == 42)
        #expect(row["s"].string == "post-1")
        #expect(row["u"].uuidValue == uuid)
        #expect(row["tokens"] == .set([.text("-5"), .text("7")]))
        #expect(row["addr"].inetAddress == "127.0.0.21")
        #expect(row["n"].isNull)
        #expect(row["missing"].isNull)
    }

    @Test("a Rows result without metadata uses the prepared layout")
    func skippedMetadata() throws {
        let layout = ColumnLayout([ColumnSpec(keyspace: "ks", table: "t", name: "likes", type: .counter)])
        let body = BodyWriter()
        body.int(0x0002).int(0x0004).int(1).int(1).bytes([0, 0, 0, 0, 0, 0, 0, 9])
        guard case .result(.rows(let rows)) = try Response.decode(body.frame(.result), knownLayout: layout) else {
            Issue.record("not a Rows result")
            return
        }
        #expect(rows.rows.map { $0["likes"] } == [.counter(9)])
        #expect(throws: CQLError.self) { _ = try Response.decode(body.frame(.result)) }
    }

    @Test("Prepared: id, partition key indices, markers and result columns")
    func prepared() throws {
        let body = BodyWriter()
        body.int(0x0004).short(3).byte(1).byte(2).byte(3)
        // Markers: flags (global spec), 2 columns, 1 key component at index 1.
        body.int(0x0001).int(2).int(1).short(1).string("ks").string("t")
        body.string("n").short(0x0002).string("id").short(0x000D)
        // Result metadata: none.
        body.int(0x0004).int(0)
        guard case .result(.prepared(let statement)) = try Response.decode(body.frame(.result), query: "UPDATE") else {
            Issue.record("not a Prepared result")
            return
        }
        #expect(statement.id == [1, 2, 3])
        #expect(statement.query == "UPDATE")
        #expect(statement.parameters.map(\.name) == ["n", "id"])
        #expect(statement.parameters.map(\.type) == [.bigint, .varchar])
        #expect(statement.partitionKeyIndices == [1])
        #expect(statement.keyspace == "ks")
        #expect(statement.resultLayout == nil)
    }

    @Test("Void, Set_keyspace and Schema_change")
    func otherResults() throws {
        let void = BodyWriter()
        void.int(1)
        guard case .result(.void) = try Response.decode(void.frame(.result)) else {
            Issue.record("not Void")
            return
        }
        let keyspace = BodyWriter()
        keyspace.int(3).string("ks")
        guard case .result(.setKeyspace("ks")) = try Response.decode(keyspace.frame(.result)) else {
            Issue.record("not Set_keyspace")
            return
        }
        let change = BodyWriter()
        change.int(5).string("CREATED").string("TABLE").string("ks").string("likes")
        guard case .result(.schemaChange(let schemaChange)) = try Response.decode(change.frame(.result)) else {
            Issue.record("not Schema_change")
            return
        }
        #expect(schemaChange == SchemaChange(change: "CREATED", target: "TABLE", keyspace: "ks", name: "likes"))
    }

    @Test("tracing id and warnings in front of the body are skipped")
    func flaggedPrefixes() throws {
        let body = BodyWriter()
        body.buffer.writeBytes([UInt8](repeating: 7, count: 16))
        body.short(1).string("Aggregation query used without partition key")
        body.int(1)
        guard case .result(.void) = try Response.decode(body.frame(.result, flags: [.tracing, .warning])) else {
            Issue.record("not Void")
            return
        }
    }

    @Test("bound values are coerced to the marker type")
    func coercion() throws {
        #expect(try CQLValue.int(1).serialized(as: .counter).map { Array($0.readableBytesView) } == [0, 0, 0, 0, 0, 0, 0, 1])
        #expect(try CQLValue.bigint(5).serialized(as: .int).map { Array($0.readableBytesView) } == [0, 0, 0, 5])
        #expect(throws: CQLError.self) { try CQLValue.bigint(.max).serialized(as: .int) }
        #expect(throws: CQLError.self) { try CQLValue.text("x").serialized(as: .bigint) }
        #expect(try CQLValue.null.serialized(as: .int) == nil)
    }
}
