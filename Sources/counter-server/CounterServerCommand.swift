import ArgumentParser
import CounterCore
import NIOCore

@main
struct CounterServerCommand: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "counter-server",
        abstract: "An HTTP counter: GET /inc, GET /count, POST /reset."
    )

    @Option(help: "Where the counter lives: \(StoreKind.allValueStrings.joined(separator: "|")).")
    var store: StoreKind = .memory

    @Option(help: "Address to listen on.")
    var host = "127.0.0.1"

    @Option(help: "Port to listen on.")
    var port = 21400

    @Option(help: "Counter file for --store disk and disk-group.")
    var file = "counter.txt"

    @Option(help: "How --store disk-group flushes each batch: fdatasync or fsync.")
    var sync: GroupCommitFileCounterStore.SyncMethod = .fdatasync

    @Option(name: .customLong("gather-delay-us"), help: "For --store disk-group: microseconds the flusher waits for more increments before each sync (default 0).")
    var gatherDelayMicroseconds = 0

    @Option(name: .customLong("pg-url"), help: "PostgreSQL URL for --store postgres.")
    var pgURL = "postgres://postgres@127.0.0.1:21410/postgres"

    @Option(name: .customLong("pg-pool"), help: "Maximum PostgreSQL connections in the pool.")
    var pgPool = 20

    @Option(name: .customLong("pg-shards"), help: "Rows the counter is split over for --store postgres-sharded.")
    var pgShards = 10

    @Option(name: .customLong("hz-address"), help: "Hazelcast member host:port for --store hazelcast; with a comma-separated list, the member answering fastest (normally the CP group leader) is used.")
    var hzAddress = "127.0.0.1:5701"

    @Option(help: "How connections are served: async (NIOAsyncChannel + a task per connection) handler (a channel handler on the event loop) raw (our own byte-level HTTP parser, no NIOHTTP1) or uring (io_uring, no SwiftNIO; --store memory only).")
    var engine: ServerEngine = .async

    @Option(name: .customLong("event-loops"), help: "Number of event loop threads (default: one per core).")
    var eventLoops = System.coreCount

    @Option(name: .customLong("pin-cpus"), help: "Comma-separated CPU ids: one event loop per CPU, pinned to it (overrides --event-loops).")
    var pinCPUs: String?

    @Flag(name: .customLong("uring-sqpoll"), help: "For --engine uring: a kernel thread polls each submission queue, so submitting needs no system call.")
    var uringSubmissionPolling = false

    func validate() throws {
        guard eventLoops > 0 else { throw ValidationError("--event-loops must be positive") }
        guard pgPool > 0 else { throw ValidationError("--pg-pool must be positive") }
        guard pgShards > 0 else { throw ValidationError("--pg-shards must be positive") }
        guard gatherDelayMicroseconds >= 0 else { throw ValidationError("--gather-delay-us must not be negative") }
        if let pinCPUs, pinnedCPUs(pinCPUs) == nil {
            throw ValidationError("--pin-cpus must be a comma-separated list of CPU ids, e.g. 0,1,2")
        }
    }

    func run() async throws {
        let configuration = StoreConfiguration(
            kind: store,
            filePath: file,
            syncMethod: sync,
            gatherDelay: .microseconds(gatherDelayMicroseconds),
            postgresURL: pgURL,
            postgresPoolSize: pgPool,
            postgresShards: pgShards,
            hazelcastAddress: hzAddress
        )
        try await configuration.withStore { store in
            try await serve(store)
        }
    }

    /// Opens the `any CounterStore` once, so `HTTPServer` and `Router` are
    /// generic over one store type. That type is picked at run time, so the
    /// optimiser is not guaranteed to specialise the request path for it.
    private func serve(_ store: some CounterStore) async throws {
        let server = HTTPServer(
            host: host,
            port: port,
            store: store,
            storeName: self.store.rawValue,
            engine: engine,
            eventLoops: eventLoops,
            pinnedCPUs: pinCPUs.flatMap(pinnedCPUs),
            submissionPolling: uringSubmissionPolling
        )
        try await server.run()
    }
}

private func pinnedCPUs(_ list: String) -> [Int]? {
    let ids = list.split(separator: ",").map { Int($0.trimmingPrefix(" ")) }
    guard !ids.isEmpty, ids.allSatisfy({ ($0 ?? -1) >= 0 }) else { return nil }
    return ids.compactMap { $0 }
}
