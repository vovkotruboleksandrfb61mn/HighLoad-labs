import HazelcastClient
import Synchronization

/// Per-user counters in a Hazelcast `IMap<Long, Long>` (user id to likes)
/// that the members back with PostgreSQL through the generic MapLoader and
/// MapStore (`deploy/hazelcast-node*.yaml`): the server never talks to the
/// database itself.
///
/// An increment is optimistic: read the value, then `replace(user, old,
/// old + 1)`, which the owning member applies only if the entry still holds
/// `old`; if another request got in between, read again and retry. A user
/// the map has never seen is loaded from the table on the first `get`
/// (read-through); a user the table has no row for starts with
/// `putIfAbsent(user, 1)`. Every change the map accepts goes to the table
/// through the MapStore: before the member answers with write-through
/// (`write-delay-seconds: 0`), later and in batches with write-behind.
public final class HazelcastCacheCounterStore: UserCounterStore {
    private let map: HazelcastMap<Int64, Int64>
    private let increments = Atomic<Int>(0)
    private let conflicts = Atomic<Int>(0)
    private let longestRetries = Atomic<Int>(0)

    /// The lock-free operations ignore it, but the protocol wants one.
    private static let threadID: Int64 = 1

    init(map: HazelcastMap<Int64, Int64>) {
        self.map = map
    }

    /// Connects to the member at `address`, runs `body` with a store on the
    /// map `mapName`, then closes the connection.
    public static func withStore<Result: Sendable>(
        address: String,
        mapName: String,
        clusterName: String = "dev",
        _ body: (HazelcastCacheCounterStore) async throws -> Result
    ) async throws -> Result {
        try await HazelcastClient.withClient(
            to: try HazelcastClient.Address(parsing: address),
            clusterName: clusterName,
            clientName: "counter-server"
        ) { client in
            let store = HazelcastCacheCounterStore(map: client.map(named: mapName))
            // The first call on the map makes the members start its MapStore.
            _ = try await store.value(user: defaultUser)
            return try await body(store)
        }
    }

    public func increment(user: Int64) async throws {
        var attempts = 0
        while true {
            guard let current = try await map.get(user, threadID: Self.threadID) else {
                if try await map.putIfAbsent(user, 1, threadID: Self.threadID) == nil { break }
                attempts += 1
                conflicts.add(1, ordering: .relaxed)
                continue
            }
            if try await map.replace(user, expected: current, new: current + 1, threadID: Self.threadID) { break }
            attempts += 1
            conflicts.add(1, ordering: .relaxed)
        }
        increments.add(1, ordering: .relaxed)
        if attempts > 0 {
            longestRetries.raise(to: attempts)
        }
    }

    public func value(user: Int64) async throws -> Int {
        Int(try await map.get(user, threadID: Self.threadID) ?? 0)
    }

    /// Puts 0 through the map, so the cache and (through the MapStore) the
    /// table agree, and zeroes the statistics.
    public func reset(user: Int64) async throws {
        try await map.put(user, 0, threadID: Self.threadID)
        increments.store(0, ordering: .relaxed)
        conflicts.store(0, ordering: .relaxed)
        longestRetries.store(0, ordering: .relaxed)
    }

    public var statistics: String? {
        let done = increments.load(ordering: .relaxed)
        let lost = conflicts.load(ordering: .relaxed)
        let longest = longestRetries.load(ordering: .relaxed)
        return "increments=\(done) conflicts=\(lost) longest_retries=\(longest)"
    }
}

extension Atomic where Value == Int {
    fileprivate func raise(to value: Int) {
        var current = load(ordering: .relaxed)
        while value > current {
            let (exchanged, original) = compareExchange(expected: current, desired: value, ordering: .relaxed)
            if exchanged { return }
            current = original
        }
    }
}
