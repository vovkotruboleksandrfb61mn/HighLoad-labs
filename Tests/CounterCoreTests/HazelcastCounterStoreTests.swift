import CounterCore
import Foundation
import Testing

/// Needs the cluster: `scripts/hz.sh start`, then
/// `INTEGRATION=1 swift test --filter HazelcastCounterStoreTests`.
@Suite(
    "HazelcastCounterStore",
    .enabled(if: ProcessInfo.processInfo.environment["INTEGRATION"] == "1", "set INTEGRATION=1 with a running cluster")
)
struct HazelcastCounterStoreTests {
    let address = ProcessInfo.processInfo.environment["HZ_ADDRESS"] ?? "127.0.0.1:5701"

    @Test("concurrent increments are never lost")
    func concurrentIncrements() async throws {
        try await HazelcastCounterStore.withStore(address: address, counterName: "test-\(UUID())") { store in
            try await hammer(store, tasks: 16, incrementsPerTask: 100)
            let value = try await store.value()
            #expect(value == 1_600)
        }
    }

    @Test("reset sets the counter back to zero")
    func reset() async throws {
        try await HazelcastCounterStore.withStore(address: address, counterName: "test-\(UUID())") { store in
            try await store.increment()
            try await store.increment()
            try await store.reset()
            let value = try await store.value()
            #expect(value == 0)
        }
    }
}
