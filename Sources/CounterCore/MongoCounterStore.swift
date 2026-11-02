import Synchronization

/// A counter kept in one MongoDB document, incremented with
/// `findOneAndUpdate({ _id }, { $inc: { likes: 1 } })` and `w: "majority"`,
/// read with `readConcern: "majority"`.
///
/// mongod runs the requests of one connection one after another, so the store
/// keeps several `MongoCounterClient`s (each with its own connection to the
/// primary and its own logical session) and lends each request one of them.
/// The increments are retryable writes, so a request that runs into a
/// failover is retried on the new primary and still counted exactly once.
public struct MongoCounterStore: CounterStore {
    private let clients: LeasePool<MongoCounterClient>

    init(clients: [MongoCounterClient]) {
        self.clients = LeasePool(clients)
    }

    /// Connects `clients` clients to the replica set at `url`, runs `body`
    /// with the store, then closes them.
    public static func withStore<Result: Sendable>(
        url: String,
        clients count: Int = 10,
        location: MongoCounterLocation = MongoCounterLocation(),
        _ body: (MongoCounterStore) async throws -> Result
    ) async throws -> Result {
        precondition(count > 0, "a store needs at least one client")
        var clients: [MongoCounterClient] = []
        do {
            for _ in 0..<count {
                clients.append(try await MongoCounterClient(url: url, location: location))
            }
            let store = MongoCounterStore(clients: clients)
            // Fails here, not on the first request, if the set has no primary.
            _ = try await store.value()
            let result = try await body(store)
            await close(clients)
            return result
        } catch {
            await close(clients)
            throw error
        }
    }

    private static func close(_ clients: [MongoCounterClient]) async {
        for client in clients {
            await client.close()
        }
    }

    public func increment() async throws {
        try await clients.withLease { _ = try await $0.increment() }
    }

    public func value() async throws -> Int {
        try await clients.withLease { try await $0.value() }
    }

    public func reset() async throws {
        try await clients.withLease { try await $0.reset() }
    }
}

/// A fixed set of resources that callers borrow one at a time. A caller that
/// finds none free waits (in FIFO order) until one is returned.
///
/// Waiting is not cancellable: a borrowed resource always comes back when the
/// operation using it ends, so a waiter is never stuck for longer than one
/// operation.
final class LeasePool<Resource: Sendable>: Sendable {
    private struct State {
        var idle: [Resource]
        var waiters: [CheckedContinuation<Resource, Never>] = []
    }

    private let state: Mutex<State>

    init(_ resources: [Resource]) {
        state = Mutex(State(idle: resources))
    }

    func withLease<Result: Sendable>(_ body: (Resource) async throws -> Result) async throws -> Result {
        let resource = await borrow()
        defer { giveBack(resource) }
        return try await body(resource)
    }

    private func borrow() async -> Resource {
        await withCheckedContinuation { continuation in
            let resource = state.withLock { state -> Resource? in
                if let resource = state.idle.popLast() {
                    return resource
                }
                state.waiters.append(continuation)
                return nil
            }
            if let resource {
                continuation.resume(returning: resource)
            }
        }
    }

    private func giveBack(_ resource: Resource) {
        let waiter = state.withLock { state -> CheckedContinuation<Resource, Never>? in
            if state.waiters.isEmpty {
                state.idle.append(resource)
                return nil
            }
            return state.waiters.removeFirst()
        }
        waiter?.resume(returning: resource)
    }
}
