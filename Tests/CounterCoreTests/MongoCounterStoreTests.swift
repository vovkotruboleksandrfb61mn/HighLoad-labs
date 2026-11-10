@testable import CounterCore
import Foundation
import MongoClient
import MongoKitten
import Synchronization
import Testing

@Suite("MongoCommandFailure")
struct MongoCommandFailureTests {
    @Test("a successful reply is no failure")
    func success() {
        #expect(MongoCommandFailure(reply: ["ok": 1.0, "value": ["likes": 3] as Document]) == nil)
    }

    @Test("NotWritablePrimary is retryable and means the primary changed")
    func notWritablePrimary() throws {
        let failure = try #require(MongoCommandFailure(reply: ["ok": 0.0, "code": Int32(10107), "codeName": "NotWritablePrimary", "errmsg": "not primary"]))
        #expect(failure.code == 10107)
        #expect(failure.isRetryable)
        #expect(failure.isNotPrimary)
        #expect(!failure.isWriteConcernError)
    }

    @Test("a write concern error after a step-down is retryable")
    func interruptedWriteConcern() throws {
        let reply: Document = [
            "ok": 1.0,
            "value": ["likes": 7] as Document,
            "writeConcernError": ["code": Int32(11602), "codeName": "InterruptedDueToReplStateChange", "errmsg": "operation was interrupted"] as Document,
        ]
        let failure = try #require(MongoCommandFailure(reply: reply))
        #expect(failure.isWriteConcernError)
        #expect(failure.isRetryable)
    }

    @Test("a write concern timeout is not retried")
    func writeConcernTimeout() throws {
        let reply: Document = [
            "ok": 1.0,
            "writeConcernError": ["code": Int32(64), "codeName": "WriteConcernTimeout", "errmsg": "waiting for replication timed out"] as Document,
        ]
        let failure = try #require(MongoCommandFailure(reply: reply))
        #expect(failure.isWriteConcernError)
        #expect(!failure.isRetryable)
    }

    @Test("the RetryableWriteError label makes any code retryable")
    func label() throws {
        let reply: Document = ["ok": 0.0, "code": Int32(12345), "errmsg": "x", "errorLabels": ["RetryableWriteError"] as Document]
        #expect(try #require(MongoCommandFailure(reply: reply)).isRetryable)
    }

    @Test("other command errors are not retried")
    func duplicateKey() throws {
        let failure = try #require(MongoCommandFailure(reply: ["ok": 0.0, "code": Int32(11000), "codeName": "DuplicateKey", "errmsg": "dup"]))
        #expect(!failure.isRetryable)
    }

    @Test("network errors from MongoKitten are retryable, others are not")
    func driverErrors() {
        #expect(MongoCommandFailure.isRetryable(MongoError(.queryFailure, reason: .connectionClosed)))
        #expect(MongoCommandFailure.isRetryable(MongoError(.cannotConnect, reason: .noAvailableHosts)))
        #expect(!MongoCommandFailure.isRetryable(MongoError(.authenticationFailure, reason: .scramFailure)))
        #expect(!MongoCommandFailure.isRetryable(CancellationError()))
    }

    @Test("error kinds name the cause in one line")
    func kinds() {
        #expect(MongoCommandFailure.kind(of: MongoError(.queryFailure, reason: .connectionClosed)) == "MongoError.queryFailure.connectionClosed")
        let failure = MongoCommandFailure(code: 11602, codeName: "InterruptedDueToReplStateChange", message: "", isWriteConcernError: true)
        #expect(MongoCommandFailure.kind(of: failure) == "writeConcernError InterruptedDueToReplStateChange")
    }
}

@Suite("MongoCounterLocation")
struct MongoCounterLocationTests {
    @Test("one shard is the single document of the task")
    func singleDocument() {
        #expect(MongoCounterLocation(documentID: "post-1").documentIDs == ["post-1"])
    }

    @Test("a sharded counter numbers its documents")
    func shardedDocuments() {
        #expect(MongoCounterLocation(documentID: "post-1", shards: 3).documentIDs == ["post-1-0", "post-1-1", "post-1-2"])
    }
}

@Suite("LeasePool")
struct LeasePoolTests {
    @Test("never lends more resources than it has, and every caller gets one")
    func boundedConcurrency() async throws {
        let pool = LeasePool([1, 2, 3])
        let inUse = Atomic(0)
        let peak = Atomic(0)
        let served = Atomic(0)
        try await withThrowingDiscardingTaskGroup { group in
            for _ in 0..<64 {
                group.addTask {
                    try await pool.withLease { _ in
                        let now = inUse.add(1, ordering: .relaxed).newValue
                        peak.max(now, ordering: .relaxed)
                        try await Task.sleep(for: .milliseconds(1))
                        inUse.subtract(1, ordering: .relaxed)
                        served.add(1, ordering: .relaxed)
                    }
                }
            }
        }
        #expect(peak.load(ordering: .relaxed) <= 3)
        #expect(served.load(ordering: .relaxed) == 64)
    }

    @Test("a resource comes back when the body throws")
    func throwingBody() async throws {
        struct Boom: Error {}
        let pool = LeasePool(["only"])
        await #expect(throws: Boom.self) {
            try await pool.withLease { _ in throw Boom() }
        }
        #expect(try await pool.withLease { $0 } == "only")
    }
}

/// Needs the replica set: `scripts/mongo.sh start`, then
/// `INTEGRATION=1 swift test --filter Mongo`. `MONGO_URL` overrides the URL.
@Suite(
    "MongoCounterStore",
    .serialized,
    .enabled(if: ProcessInfo.processInfo.environment["INTEGRATION"] == "1", "set INTEGRATION=1 with scripts/mongo.sh running")
)
struct MongoCounterStoreTests {
    private let url = ProcessInfo.processInfo.environment["MONGO_URL"]
        ?? "mongodb://127.0.0.1:21430,127.0.0.1:21431,127.0.0.1:21432/?replicaSet=rs0"

    private func testLocation() -> MongoCounterLocation {
        MongoCounterLocation(database: "hls-test", collection: "counters", documentID: "test-\(UUID())", field: "likes")
    }

    @Test("concurrent increments are never lost")
    func concurrentIncrements() async throws {
        try await MongoCounterStore.withStore(url: url, clients: 4, location: testLocation()) { store in
            try await store.reset()
            try await hammer(store, tasks: 16, incrementsPerTask: 50)
            #expect(try await store.value() == 800)
        }
    }

    @Test("a sharded counter is the sum of its documents")
    func sharded() async throws {
        var location = testLocation()
        location.shards = 3
        try await MongoCounterStore.withStore(url: url, clients: 4, location: location) { store in
            try await store.reset()
            try await hammer(store, tasks: 8, incrementsPerTask: 25)
            #expect(try await store.value() == 200)
            try await store.reset()
            #expect(try await store.value() == 0)
        }
    }

    @Test("reset sets the counter back to zero")
    func reset() async throws {
        try await MongoCounterStore.withStore(url: url, clients: 1, location: testLocation()) { store in
            try await store.increment()
            try await store.increment()
            try await store.reset()
            #expect(try await store.value() == 0)
        }
    }

    @Test("a counter that was never written reads as zero")
    func missingDocument() async throws {
        try await MongoCounterStore.withStore(url: url, clients: 1, location: testLocation()) { store in
            let value = try await store.value()
            #expect(value == 0)
        }
    }

    @Test("separate clients lose nothing while no node fails", arguments: MongoWriteConcern.allCases)
    func separateClients(writeConcern: MongoWriteConcern) async throws {
        let location = testLocation()
        var clients: [MongoCounterClient] = []
        for _ in 0..<6 {
            clients.append(try await MongoCounterClient(url: url, location: location, writeConcern: writeConcern))
        }
        let setup = clients[0]
        try await setup.reset()
        let lastValues = try await withThrowingTaskGroup(of: Int.self) { group in
            for client in clients {
                group.addTask {
                    var last = 0
                    for _ in 0..<100 {
                        last = try await client.increment().value
                    }
                    return last
                }
            }
            return try await group.reduce(into: [Int]()) { $0.append($1) }
        }
        try await setup.awaitMajorityCommit()
        #expect(try await setup.value() == 600)
        // findAndModify returns the document after the $inc: one of the
        // clients made the last increment.
        #expect(lastValues.max() == 600)
        for client in clients {
            await client.close()
        }
    }

    /// The primary steps down while clients increment: their next requests
    /// reach a node that is no longer primary, which answers with an error
    /// instead of closing the connection, so only `reconnect()` makes the
    /// client find the new primary before MongoKitten's next heartbeat.
    @Test("increments go on exactly through a step-down of the primary", arguments: MongoWriteConcern.allCases)
    func stepDown(writeConcern: MongoWriteConcern) async throws {
        let location = testLocation()
        let retryKinds = Mutex<[String]>([])
        var clients: [MongoCounterClient] = []
        for _ in 0..<4 {
            clients.append(
                try await MongoCounterClient(url: url, location: location, writeConcern: writeConcern) { error in
                    retryKinds.withLock { $0.append(MongoCommandFailure.kind(of: error)) }
                }
            )
        }
        let setup = try await MongoCounterClient(url: url, location: location)
        try await setup.reset()
        let steppedDown = Atomic<Bool>(false)
        let acknowledged = try await withThrowingTaskGroup(of: Int.self) { group in
            for client in clients {
                group.addTask {
                    // Until the step-down, then 20 more increments.
                    var count = 0
                    while !steppedDown.load(ordering: .relaxed) {
                        try await client.increment()
                        count += 1
                    }
                    for _ in 0..<20 {
                        try await client.increment()
                        count += 1
                    }
                    return count
                }
            }
            group.addTask {
                try await Task.sleep(for: .milliseconds(300))
                _ = try await setup.adminCommandOnce(["replSetStepDown": Int32(10)])
                steppedDown.store(true, ordering: .relaxed)
                return 0
            }
            return try await group.reduce(0, +)
        }
        try await setup.awaitMajorityCommit()
        #expect(try await setup.value() == acknowledged)
        let kinds = retryKinds.withLock { $0 }
        #expect(!kinds.isEmpty, "no increment met the step-down")
        for client in clients + [setup] {
            await client.close()
        }
    }
}
