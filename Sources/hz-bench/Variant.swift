import ArgumentParser
import HazelcastClient

/// The ways hz-bench increments the shared counter.
enum Variant: String, CaseIterable, ExpressibleByArgument, Sendable {
    /// IMap get + put with no coordination: concurrent updates get lost.
    case mapNoLock = "map-nolock"
    /// IMap lock, get, put, unlock.
    case mapPessimistic = "map-pessimistic"
    /// IMap get, then replace(key, old, old + 1), retried until it succeeds.
    case mapOptimistic = "map-optimistic"
    /// IAtomicLong.incrementAndGet in the CP Subsystem.
    case atomicLong = "atomic-long"

    /// Whether the final value must equal tasks x increments.
    var mustBeExact: Bool { self != .mapNoLock }
}

/// The shared counter one run increments: an IMap entry or an IAtomicLong.
enum Counter: Sendable {
    case map(HazelcastMap<String, Int64>, key: String)
    case atomicLong(AtomicLong)

    static let mapName = "hz-bench"
    static let mapKey = "counter"
    static let atomicLongName = "hz-bench-counter"

    static func open(for variant: Variant, client: HazelcastClient) async throws -> Counter {
        switch variant {
        case .mapNoLock, .mapPessimistic, .mapOptimistic:
            .map(client.map(named: mapName), key: mapKey)
        case .atomicLong:
            .atomicLong(try await client.cpSubsystem.atomicLong(named: atomicLongName))
        }
    }

    /// Thread id 0 is reserved for setup and reading the result.
    static let setupThreadID: Int64 = 0

    func reset() async throws {
        switch self {
        case .map(let map, let key):
            try await map.put(key, 0, threadID: Self.setupThreadID)
        case .atomicLong(let counter):
            try await counter.set(0)
        }
    }

    func value() async throws -> Int64 {
        switch self {
        case .map(let map, let key):
            try await map.get(key, threadID: Self.setupThreadID) ?? 0
        case .atomicLong(let counter):
            try await counter.get()
        }
    }

    /// Adds one the way `variant` does, acting as thread `threadID`.
    /// Returns the number of retries it took (optimistic variant only).
    func increment(_ variant: Variant, threadID: Int64) async throws -> Int {
        switch (variant, self) {
        case (.mapNoLock, .map(let map, let key)):
            let value = try await map.get(key, threadID: threadID) ?? 0
            try await map.put(key, value + 1, threadID: threadID)
            return 0

        case (.mapPessimistic, .map(let map, let key)):
            try await map.withLock(key, threadID: threadID) {
                let value = try await map.get(key, threadID: threadID) ?? 0
                try await map.put(key, value + 1, threadID: threadID)
            }
            return 0

        case (.mapOptimistic, .map(let map, let key)):
            var retries = 0
            while true {
                let value = try await map.get(key, threadID: threadID) ?? 0
                if try await map.replace(key, expected: value, new: value + 1, threadID: threadID) {
                    return retries
                }
                retries += 1
            }

        case (.atomicLong, .atomicLong(let counter)):
            try await counter.incrementAndGet()
            return 0

        default:
            preconditionFailure("variant \(variant.rawValue) does not use this counter")
        }
    }
}
