@testable import CQLClient
import NIOCore
import Testing

@Suite("Murmur3 tokens and the ring")
struct Murmur3Tests {
    /// `SELECT id, token(id)` on a Cassandra 4.1 node (Murmur3Partitioner).
    /// The non-ASCII keys put bytes >= 0x80 into the tail, where Cassandra
    /// sign-extends them.
    static let textTokens: [(String, Int64)] = [
        ("a", -8_839_064_797_231_613_815),
        ("post-1", -2_451_753_689_196_566_148),
        ("hello world 123456", -7_651_324_883_318_369_051),
        ("0123456789abcdef", 5_467_490_433_528_156_583),
        ("ÿÿÿ€€", 216_025_997_575_973_909),
        ("0123456789abcdefg€", 216_458_069_079_277_491),
    ]

    static let bigintTokens: [(Int64, Int64)] = [
        (0, 2_945_182_322_382_062_539),
        (1, 6_292_367_497_774_912_474),
        (-1, 7_071_048_584_287_372_947),
        (.max, -1_722_304_415_079_482_439),
    ]

    @Test("text keys hash to the tokens Cassandra reports", arguments: textTokens)
    func textKeys(key: String, token: Int64) {
        #expect(Murmur3Partitioner.token(of: Array(key.utf8)) == token)
    }

    @Test("bigint keys hash to the tokens Cassandra reports", arguments: bigintTokens)
    func bigintKeys(key: Int64, token: Int64) throws {
        let bytes = try #require(try CQLValue.bigint(key).serialized())
        #expect(Murmur3Partitioner.token(of: bytes) == token)
    }

    @Test("a composite key uses the length-prefixed, zero-terminated layout")
    func compositeKey() throws {
        // PRIMARY KEY ((a text, b int)), a = 'post-1', b = 7.
        let components = [try #require(try CQLValue.text("post-1").serialized()), try #require(try CQLValue.int(7).serialized())]
        #expect(Murmur3Partitioner.token(of: Murmur3Partitioner.routingKey(components)) == 3_276_314_271_508_325_028)
    }

    @Test("the primary replica owns the first token at or after the key's, wrapping")
    func ring() {
        let ring = TokenRing(tokensByNode: [[-100, 50], [0], [100]])
        #expect(ring.primary(for: -500) == 0)
        #expect(ring.primary(for: -100) == 0)
        #expect(ring.primary(for: -99) == 1)
        #expect(ring.primary(for: 1) == 0)
        #expect(ring.primary(for: 51) == 2)
        #expect(ring.primary(for: 101) == 0)
        #expect(ring.replicas(for: 1, count: 3) == [0, 2, 1])
        #expect(ring.replicas(for: 51, count: 2) == [2, 0])
        #expect(TokenRing(tokensByNode: [[]]).primary(for: 0) == nil)
    }
}
