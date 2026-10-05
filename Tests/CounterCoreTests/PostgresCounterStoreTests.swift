import CounterCore
import Foundation
import Testing

/// Needs a running server: `scripts/pg.sh start`, then
/// `INTEGRATION=1 swift test --filter PostgresCounterStore`.
/// `PG_URL` overrides the default URL.
@Suite(
    "PostgresCounterStore",
    .serialized,
    .enabled(if: ProcessInfo.processInfo.environment["INTEGRATION"] == "1", "set INTEGRATION=1 with scripts/pg.sh running")
)
struct PostgresCounterStoreTests {
    private let url = ProcessInfo.processInfo.environment["PG_URL"] ?? "postgres://postgres@127.0.0.1:21410/postgres"

    @Test("concurrent increments are never lost")
    func concurrentIncrements() async throws {
        try await PostgresCounterStore.withStore(url: url, maximumConnections: 10) { store in
            try await store.reset()
            try await hammer(store, tasks: 20, incrementsPerTask: 100)
            #expect(try await store.value() == 2_000)
        }
    }

    @Test("reset sets the counter back to zero")
    func reset() async throws {
        try await PostgresCounterStore.withStore(url: url) { store in
            try await store.increment()
            try await store.reset()
            #expect(try await store.value() == 0)
        }
    }
}

/// Needs a running server, like `PostgresCounterStoreTests`.
@Suite(
    "PostgresShardedCounterStore",
    .serialized,
    .enabled(if: ProcessInfo.processInfo.environment["INTEGRATION"] == "1", "set INTEGRATION=1 with scripts/pg.sh running")
)
struct PostgresShardedCounterStoreTests {
    private let url = ProcessInfo.processInfo.environment["PG_URL"] ?? "postgres://postgres@127.0.0.1:21410/postgres"

    @Test("concurrent increments are never lost, and the shards add up")
    func concurrentIncrements() async throws {
        try await PostgresShardedCounterStore.withStore(url: url, shards: 4, maximumConnections: 10) { store in
            try await store.reset()
            try await hammer(store, tasks: 20, incrementsPerTask: 50)
            #expect(try await store.value() == 1_000)
            try await store.reset()
            #expect(try await store.value() == 0)
        }
    }
}

@Suite("PostgresEndpoint")
struct PostgresEndpointTests {
    @Test("parses a full URL")
    func fullURL() throws {
        let endpoint = try PostgresEndpoint(url: "postgres://alice:s3cret@db.local:6543/counters")
        #expect(endpoint == PostgresEndpoint(host: "db.local", port: 6543, username: "alice", password: "s3cret", database: "counters"))
    }

    @Test("defaults the port and leaves out the password and database")
    func minimalURL() throws {
        let endpoint = try PostgresEndpoint(url: "postgresql://postgres@127.0.0.1")
        #expect(endpoint == PostgresEndpoint(host: "127.0.0.1", port: 5432, username: "postgres"))
    }

    @Test(
        "rejects malformed URLs",
        arguments: [
            "127.0.0.1:5432",
            "mysql://root@127.0.0.1/db",
            "postgres://127.0.0.1/db",
            "postgres://postgres@:5432/db",
            "postgres://postgres@127.0.0.1:port/db",
            "postgres://postgres@127.0.0.1:70000/db",
        ]
    )
    func malformedURL(_ url: String) {
        #expect(throws: PostgresEndpointError.self) {
            try PostgresEndpoint(url: url)
        }
    }
}
