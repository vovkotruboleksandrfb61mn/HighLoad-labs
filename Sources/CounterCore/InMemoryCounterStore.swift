import Synchronization

/// A counter kept in process memory.
///
/// `Atomic<Int>` makes every increment a single lock-free read-modify-write,
/// so concurrent increments can never overwrite each other.
public final class InMemoryCounterStore: CounterStore {
    private let counter = Atomic<Int>(0)

    public init() {}

    public func increment() async throws {
        incrementNow()
    }

    public func value() async throws -> Int {
        valueNow()
    }

    public func reset() async throws {
        resetNow()
    }

    public func incrementNow() {
        counter.add(1, ordering: .relaxed)
    }

    public func valueNow() -> Int {
        counter.load(ordering: .relaxed)
    }

    public func resetNow() {
        counter.store(0, ordering: .relaxed)
    }
}
