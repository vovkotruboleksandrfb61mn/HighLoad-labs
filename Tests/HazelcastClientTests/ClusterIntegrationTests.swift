@testable import HazelcastClient
import Testing

#if canImport(Glibc)
import Glibc
#elseif canImport(Darwin)
import Darwin
#endif

/// Runs against a real cluster: `scripts/hz.sh start`, then
/// `INTEGRATION=1 swift test --filter ClusterIntegrationTests`.
/// `HZ_ADDRESS` (default 127.0.0.1:5701) picks the member.
enum Integration {
    static var enabled: Bool { getenv("INTEGRATION").map { String(cString: $0) } == "1" }

    static var address: HazelcastClient.Address {
        let text = getenv("HZ_ADDRESS").map { String(cString: $0) } ?? "127.0.0.1:5701"
        return (try? HazelcastClient.Address(parsing: text)) ?? .localMember
    }

    /// A name no earlier run has used, so runs do not see each other's data.
    static func uniqueName(_ prefix: String) -> String {
        "\(prefix)-\(HazelcastUUID.random())"
    }
}

/// Connects to `Integration.address`, runs `body`, shuts the client down.
func withTestClient(_ body: (HazelcastClient) async throws -> Void) async throws {
    try await HazelcastClient.withClient(to: Integration.address, body)
}

@Suite("Cluster integration", .enabled(if: Integration.enabled, "set INTEGRATION=1 with a running cluster"))
struct ClusterIntegrationTests {
    @Test("authentication reports Hazelcast 5.4.0 with 271 partitions")
    func authentication() async throws {
        try await withTestClient { client in
            #expect(client.serverVersion == "5.4.0")
            #expect(client.partitionCount == 271)
            #expect(client.clusterID != nil)
        }
    }

    @Test("a wrong cluster name is refused")
    func wrongClusterName() async {
        await #expect(throws: HazelcastError.authenticationFailed(.credentialsFailed)) {
            try await HazelcastClient.withClient(to: Integration.address, clusterName: "not-dev") { _ in }
        }
    }

    @Test("map get, put and replace")
    func mapBasics() async throws {
        try await withTestClient { client in
            let map = client.map(named: Integration.uniqueName("basics"), key: String.self, value: Int64.self)
            let missing = try await map.get("k", threadID: 1)
            #expect(missing == nil)
            let firstPrevious = try await map.put("k", 41, threadID: 1)
            #expect(firstPrevious == nil)
            let secondPrevious = try await map.put("k", 42, threadID: 1)
            #expect(secondPrevious == 41)
            let stored = try await map.get("k", threadID: 1)
            #expect(stored == 42)
            let replacedStale = try await map.replace("k", expected: 41, new: 43, threadID: 1)
            #expect(!replacedStale)
            let replacedCurrent = try await map.replace("k", expected: 42, new: 43, threadID: 1)
            #expect(replacedCurrent)
            let replacedValue = try await map.get("k", threadID: 1)
            #expect(replacedValue == 43)
        }
    }

    @Test("a lock belongs to its thread id")
    func lockOwnership() async throws {
        try await withTestClient { client in
            let map = client.map(named: Integration.uniqueName("locks"), key: String.self, value: Int64.self)
            try await map.lock("k", threadID: 1)
            // Another "thread" of the same client may not release it.
            await #expect {
                try await map.unlock("k", threadID: 2)
            } throws: { error in
                guard case HazelcastError.server(let serverError) = error else { return false }
                return serverError.className == "java.lang.IllegalMonitorStateException"
            }
            try await map.unlock("k", threadID: 1)
        }
    }

    @Test("pessimistic and optimistic increments from 8 thread ids lose nothing")
    func mapIncrements() async throws {
        try await withTestClient { client in
            let map = client.map(named: Integration.uniqueName("increments"), key: String.self, value: Int64.self)
            try await map.put("pessimistic", 0, threadID: 0)
            try await map.put("optimistic", 0, threadID: 0)
            try await withThrowingDiscardingTaskGroup { group in
                for threadID in Int64(1)...8 {
                    group.addTask {
                        for _ in 0..<50 {
                            try await map.withLock("pessimistic", threadID: threadID) {
                                let value = try await map.get("pessimistic", threadID: threadID) ?? 0
                                try await map.put("pessimistic", value + 1, threadID: threadID)
                            }
                            while true {
                                let value = try await map.get("optimistic", threadID: threadID) ?? 0
                                if try await map.replace("optimistic", expected: value, new: value + 1, threadID: threadID) {
                                    break
                                }
                            }
                        }
                    }
                }
            }
            let pessimistic = try await map.get("pessimistic", threadID: 0)
            #expect(pessimistic == 400)
            let optimistic = try await map.get("optimistic", threadID: 0)
            #expect(optimistic == 400)
        }
    }

    @Test("IAtomicLong increments from 8 tasks lose nothing")
    func atomicLong() async throws {
        try await withTestClient { client in
            let counter = try await client.cpSubsystem.atomicLong(named: Integration.uniqueName("counter"))
            #expect(counter.groupID.name == "default")
            try await counter.set(10)
            let initial = try await counter.get()
            #expect(initial == 10)
            try await withThrowingDiscardingTaskGroup { group in
                for _ in 0..<8 {
                    group.addTask {
                        for _ in 0..<100 {
                            try await counter.incrementAndGet()
                        }
                    }
                }
            }
            let incremented = try await counter.get()
            #expect(incremented == 810)
            let beforeReset = try await counter.getAndSet(0)
            #expect(beforeReset == 810)
            let afterAdd = try await counter.addAndGet(5)
            #expect(afterAdd == 5)
        }
    }

    @Test("requests in flight fail when the client shuts down")
    func shutdownFailsPending() async throws {
        let client = try await HazelcastClient.connect(to: Integration.address)
        let map = client.map(named: Integration.uniqueName("shutdown"), key: String.self, value: Int64.self)
        try await map.lock("k", threadID: 1)
        // Thread 2 blocks on the lock until the connection goes away.
        let blocked = Task { try await map.lock("k", threadID: 2) }
        while client.connection.pendingRequests == 0 {
            await Task.yield()
        }
        try await client.shutdown()
        await #expect(throws: HazelcastError.connectionClosed) { try await blocked.value }
        await #expect(throws: HazelcastError.connectionClosed) { try await map.get("k", threadID: 1) }
    }
}
