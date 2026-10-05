import ArgumentParser
import CounterCore

enum StoreKind: String, CaseIterable, ExpressibleByArgument {
    case memory
    case disk
    case diskGroup = "disk-group"
    case postgres
    case postgresSharded = "postgres-sharded"
    case hazelcast
}

/// Everything needed to open one of the counter stores.
struct StoreConfiguration {
    var kind: StoreKind
    var filePath: String
    var syncMethod: GroupCommitFileCounterStore.SyncMethod = .fdatasync
    var gatherDelay: Duration = .zero
    var postgresURL: String
    var postgresPoolSize = 20
    var postgresShards = 10
    var hazelcastAddress: String

    /// Opens the selected store, runs `body` with it, and closes it again.
    /// Stores that hold connections (PostgreSQL, Hazelcast) are only alive for
    /// the duration of `body`.
    func withStore<Result: Sendable>(
        _ body: (any CounterStore) async throws -> Result
    ) async throws -> Result {
        switch kind {
        case .memory:
            return try await body(InMemoryCounterStore())
        case .disk:
            return try await body(FileCounterStore(path: filePath))
        case .diskGroup:
            let store = try GroupCommitFileCounterStore(path: filePath, syncMethod: syncMethod, gatherDelay: gatherDelay)
            return try await withThrowingTaskGroup(of: Void.self) { group in
                group.addTask {
                    try await Self.reportBatches(of: store)
                }
                defer { group.cancelAll() }
                return try await body(store)
            }
        case .postgres:
            return try await PostgresCounterStore.withStore(url: postgresURL, maximumConnections: postgresPoolSize) {
                try await body($0)
            }
        case .postgresSharded:
            return try await PostgresShardedCounterStore.withStore(
                url: postgresURL,
                shards: postgresShards,
                maximumConnections: postgresPoolSize
            ) { try await body($0) }
        case .hazelcast:
            return try await HazelcastCounterStore.withStore(address: hazelcastAddress, log: { Log.info($0) }) { try await body($0) }
        }
    }

    /// Logs how many increments each sync covered, once a second while the
    /// numbers change, so a run's batching shows up in the server log.
    private static func reportBatches(of store: GroupCommitFileCounterStore) async throws {
        var last = (flushes: 0, operations: 0)
        while true {
            try await Task.sleep(for: .seconds(1))
            let now = (flushes: store.flushCount, operations: store.flushedOperationCount)
            guard now.flushes != last.flushes else { continue }
            let flushes = now.flushes - last.flushes
            let operations = now.operations - last.operations
            let hundredths = operations * 100 / flushes
            Log.info(
                "disk-group: \(flushes) syncs, \(operations) ops, \(hundredths / 100).\(hundredths % 100 < 10 ? "0" : "")\(hundredths % 100) ops per sync (total \(now.flushes) syncs, \(now.operations) ops)"
            )
            last = now
        }
    }
}

extension GroupCommitFileCounterStore.SyncMethod: ExpressibleByArgument {}
