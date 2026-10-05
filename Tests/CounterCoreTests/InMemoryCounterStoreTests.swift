import CounterCore
import Testing

@Suite("InMemoryCounterStore")
struct InMemoryCounterStoreTests {
    @Test("concurrent increments are never lost")
    func concurrentIncrements() async throws {
        let store = InMemoryCounterStore()
        try await hammer(store, tasks: 64, incrementsPerTask: 10_000)
        #expect(try await store.value() == 640_000)
    }

    @Test("reset sets the counter back to zero")
    func reset() async throws {
        let store = InMemoryCounterStore()
        try await store.increment()
        try await store.increment()
        try await store.reset()
        #expect(try await store.value() == 0)
    }
}
