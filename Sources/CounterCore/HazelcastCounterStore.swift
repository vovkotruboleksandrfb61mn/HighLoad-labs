import HazelcastClient
import NIOCore

/// A counter kept in a Hazelcast CP Subsystem `IAtomicLong`, incremented with
/// `incrementAndGet()` through our own client.
///
/// Every increment is one Raft commit in the CP group, so increments from any
/// number of requests (or servers) are never lost. All requests share one
/// client connection; the client matches responses to requests by
/// correlation id, so they run concurrently over it.
public struct HazelcastCounterStore: CounterStore {
    /// The name of the `IAtomicLong` the web counter uses.
    public static let defaultCounterName = "web-counter"

    private let counter: AtomicLong

    init(counter: AtomicLong) {
        self.counter = counter
    }

    /// Connects to the member at `address` (`host:port`), runs `body` with the
    /// store, then closes the connection.
    ///
    /// `address` may also be a comma-separated list of members. The store
    /// then connects to each of them, times `probeRequests` sequential
    /// `get()` calls on every connection, and keeps the one with the lowest
    /// median latency. A member that is not the leader of the counter's CP
    /// group forwards every request to the leader, so the fastest member is
    /// normally the leader. This only picks where requests enter the
    /// cluster; every increment is still a Raft commit in the same CP group.
    public static func withStore<Result: Sendable>(
        address: String,
        clusterName: String = "dev",
        counterName: String = defaultCounterName,
        probeRequests: Int = 300,
        log: @Sendable (String) -> Void = { _ in },
        _ body: (HazelcastCounterStore) async throws -> Result
    ) async throws -> Result {
        let addresses = try address.split(separator: ",").map {
            try HazelcastClient.Address(parsing: String($0))
        }
        guard addresses.count > 1 else {
            return try await HazelcastClient.withClient(
                to: addresses.first ?? HazelcastClient.Address(parsing: address),
                clusterName: clusterName,
                clientName: "counter-server"
            ) { client in
                let counter = try await client.cpSubsystem.atomicLong(named: counterName)
                return try await body(HazelcastCounterStore(counter: counter))
            }
        }

        var candidates: [(address: HazelcastClient.Address, client: HazelcastClient, counter: AtomicLong)] = []
        do {
            for memberAddress in addresses {
                let client = try await HazelcastClient.connect(
                    to: memberAddress,
                    clusterName: clusterName,
                    clientName: "counter-server"
                )
                candidates.append((memberAddress, client, try await client.cpSubsystem.atomicLong(named: counterName)))
            }
        } catch {
            for candidate in candidates {
                try? await candidate.client.shutdown()
            }
            throw error
        }

        var best = 0
        var bestLatency = Duration.seconds(3600)
        for (index, candidate) in candidates.enumerated() {
            let latency = try await medianLatency(of: candidate.counter, requests: probeRequests)
            log("hazelcast: \(candidate.address) median get() latency \(latency)")
            if latency < bestLatency {
                best = index
                bestLatency = latency
            }
        }
        for (index, candidate) in candidates.enumerated() where index != best {
            try? await candidate.client.shutdown()
        }
        let chosen = candidates[best]
        log("hazelcast: using \(chosen.address)")
        let result: Result
        do {
            result = try await body(HazelcastCounterStore(counter: chosen.counter))
        } catch {
            try? await chosen.client.shutdown()
            throw error
        }
        try await chosen.client.shutdown()
        return result
    }

    private static func medianLatency(of counter: AtomicLong, requests: Int) async throws -> Duration {
        let clock = ContinuousClock()
        var latencies: [Duration] = []
        latencies.reserveCapacity(requests)
        for _ in 0..<max(requests, 1) {
            let start = clock.now
            _ = try await counter.get()
            latencies.append(start.duration(to: clock.now))
        }
        latencies.sort()
        return latencies[latencies.count / 2]
    }

    public func increment() async throws {
        try await counter.incrementAndGet()
    }

    public func value() async throws -> Int {
        Int(try await counter.get())
    }

    public func reset() async throws {
        try await counter.set(0)
    }
}

extension HazelcastCounterStore: EventLoopCounterStore {
    public func increment(on eventLoop: any EventLoop) -> EventLoopFuture<Void> {
        counter.addAndGetFuture(1).map { _ in }
    }

    public func value(on eventLoop: any EventLoop) -> EventLoopFuture<Int> {
        counter.getFuture().map { Int($0) }
    }

    public func reset(on eventLoop: any EventLoop) -> EventLoopFuture<Void> {
        eventLoop.makeFutureWithTask { try await self.reset() }
    }
}
