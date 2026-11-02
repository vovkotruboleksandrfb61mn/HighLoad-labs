import Logging
import MongoClient
import MongoKitten
import NIOCore
import NIOPosix
import Synchronization

/// The write concern of the counter's increments.
public enum MongoWriteConcern: String, Sendable, CaseIterable {
    /// `{ w: 1 }`: acknowledged once the primary has applied the write.
    case w1
    /// `{ w: "majority" }`: acknowledged once a majority of the replica set
    /// has the write (and journaled it), so no election can roll it back.
    case majority

    public var document: Document {
        switch self {
        case .w1: ["w": Int32(1)]
        case .majority: ["w": "majority"]
        }
    }
}

/// Where the counter lives: one field of one document.
public struct MongoCounterLocation: Sendable, Equatable {
    public var database: String
    public var collection: String
    public var documentID: String
    public var field: String

    public init(database: String = "hls", collection: String = "counters", documentID: String = "web-counter", field: String = "likes") {
        self.database = database
        self.collection = collection
        self.documentID = documentID
        self.field = field
    }
}

/// What one logical operation returned and how many times it was resent.
public struct MongoOutcome<Value: Sendable>: Sendable {
    public var value: Value
    public var retries: Int
}

/// One client of the replica set: its own `MongoCluster` (connections and
/// replica set discovery, from MongoKitten) and its own logical session, used
/// by one task at a time.
///
/// Writes are retryable writes as MongoDB drivers make them (MongoKitten does
/// not implement them): every logical write carries the session's `lsid` and
/// the next `txnNumber`, and a retry sends exactly the same command again. The
/// server remembers the last `txnNumber` it executed for each session (in
/// `config.transactions`, replicated with the write itself), so a retry of a
/// write that had already been applied returns the stored result instead of
/// applying it a second time. Without this, a `$inc` whose reply was lost
/// when the primary died could be counted twice.
///
/// The server rejects a `txnNumber` lower than the last one of its session,
/// so a client must not run two operations at once: give each task its own
/// client (mongo-bench) or lease clients from a pool (`MongoCounterStore`).
public final class MongoCounterClient: Sendable {
    /// How long one operation keeps retrying before it fails with the last
    /// error. An election after a sudden primary failure takes about
    /// `electionTimeoutMillis` (10 s), so this must be well above that.
    static let retryTimeLimit = Duration.seconds(60)
    /// The pause before the first retry; it doubles up to `maximumBackoff`.
    static let initialBackoff = Duration.milliseconds(10)
    static let maximumBackoff = Duration.milliseconds(250)

    /// The hosts of the connection string, in order.
    public let hosts: [String]

    private let cluster: MongoCluster
    private let location: MongoCounterLocation
    private let writeConcern: MongoWriteConcern
    /// Whether writes carry `lsid` and `txnNumber`. Only for demonstrating
    /// what goes wrong without them: a write is then still resent after a
    /// network error, but the server cannot tell the resend from a new write.
    private let retryableWrites: Bool
    private let onRetry: @Sendable (any Error) -> Void
    private let namespace: MongoNamespace
    private let sessionID = MongoCounterClient.makeSessionID()
    private let lastTransactionNumber = Atomic<Int>(0)

    /// Connects to the replica set at `url` (`mongodb://h1,h2,h3/?replicaSet=…`)
    /// and waits until it has found the primary.
    public init(
        url: String,
        location: MongoCounterLocation = MongoCounterLocation(),
        writeConcern: MongoWriteConcern = .majority,
        retryableWrites: Bool = true,
        onRetry: @escaping @Sendable (any Error) -> Void = { _ in }
    ) async throws {
        let settings = try ConnectionSettings(url)
        // MongoKitten's default logger writes to standard output, where the
        // tools print their CSV rows.
        var logger = Logger(label: "mongokitten", factory: { StreamLogHandler.standardError(label: $0) })
        logger.logLevel = .error
        cluster = try await MongoCluster(connectingTo: settings, logger: logger, eventLoopGroup: MultiThreadedEventLoopGroup.singleton)
        hosts = settings.hosts.map { "\($0.hostname):\($0.port)" }
        self.location = location
        self.writeConcern = writeConcern
        self.retryableWrites = retryableWrites
        self.onRetry = onRetry
        namespace = MongoNamespace(to: "$cmd", inDatabase: location.database)
    }

    /// Closes every connection of this client.
    public func close() async {
        await cluster.disconnect()
    }

    /// `findOneAndUpdate({ _id }, { $inc: { field: 1 } })` with the client's
    /// write concern, as a retryable write. Returns the new value.
    @discardableResult
    public func increment() async throws -> MongoOutcome<Int> {
        try await retryableWrite(update: ["$inc": [location.field: Int32(1)] as Document])
    }

    /// Sets the counter to 0 (creating the document if needed), with
    /// `w: "majority"` whatever the client's write concern.
    public func reset() async throws {
        _ = try await retryableWrite(update: ["$set": [location.field: 0] as Document], writeConcern: .majority)
    }

    /// Returns once everything the primary has applied so far is majority
    /// committed, by making a `w: "majority"` write of its own (to a separate
    /// document) after it: the commit point only moves forward through the
    /// oplog. Without it a majority read right after `w: 1` increments can
    /// miss the last few of them, which a secondary has not confirmed yet.
    public func awaitMajorityCommit() async throws {
        _ = try await retryableWrite(
            documentID: "majority-barrier",
            update: ["$inc": [location.field: Int32(1)] as Document],
            writeConcern: .majority
        )
    }

    /// The counter as of the majority-committed data (`readConcern:
    /// "majority"`), read on the primary: writes that an election could still
    /// roll back are not counted. A missing document counts as 0.
    public func value() async throws -> Int {
        let command: Document = [
            "find": location.collection,
            "filter": ["_id": location.documentID] as Document,
            "limit": Int32(1),
            "singleBatch": true,
            "readConcern": ["level": "majority"] as Document,
        ]
        return try await retrying {
            let reply = try await run(command, in: namespace)
            guard
                let cursor = reply["cursor"] as? Document,
                let batch = cursor["firstBatch"] as? Document
            else {
                throw MongoCommandFailure(message: "find returned no cursor")
            }
            guard let document = batch.values.first as? Document else {
                return 0
            }
            return try fieldValue(of: document)
        }.value
    }

    /// Runs an admin command (`hello`, `replSetGetStatus`) on the primary,
    /// retrying through a failover like any read.
    public func adminCommand(_ command: Document) async throws -> Document {
        try await retrying { try await run(command, in: .administrativeCommand) }.value
    }

    // MARK: - Retryable writes

    private func retryableWrite(
        documentID: String? = nil,
        update: Document,
        writeConcern: MongoWriteConcern? = nil
    ) async throws -> MongoOutcome<Int> {
        var command: Document = [
            "findAndModify": location.collection,
            "query": ["_id": documentID ?? location.documentID] as Document,
            "update": update,
            "new": true,
            "upsert": true,
            "writeConcern": (writeConcern ?? self.writeConcern).document,
        ]
        if retryableWrites {
            // A new logical write: the next transaction number. Every retry
            // of it sends this same number.
            command["lsid"] = ["id": sessionID] as Document
            command["txnNumber"] = lastTransactionNumber.add(1, ordering: .relaxed).newValue
        }
        return try await retrying {
            let reply = try await run(command, in: namespace)
            guard let document = reply["value"] as? Document else {
                throw MongoCommandFailure(message: "findAndModify returned no document")
            }
            return try fieldValue(of: document)
        }
    }

    /// Runs `operation` until it succeeds, fails with an error that is not
    /// retryable, or `retryTimeLimit` runs out.
    private func retrying<Value: Sendable>(_ operation: () async throws -> Value) async throws -> MongoOutcome<Value> {
        let clock = ContinuousClock()
        let giveUp = clock.now.advanced(by: Self.retryTimeLimit)
        var retries = 0
        var backoff = Self.initialBackoff
        while true {
            do {
                return MongoOutcome(value: try await operation(), retries: retries)
            } catch {
                guard MongoCommandFailure.isRetryable(error), clock.now < giveUp else {
                    throw error
                }
                retries += 1
                onRetry(error)
                if let failure = error as? MongoCommandFailure, failure.isNotPrimary {
                    // The node answered but is not primary (any more), e.g.
                    // after a clean step-down. MongoKitten refreshes what it
                    // knows of a connected node only every heartbeat (10 s),
                    // so make it look for the primary again now. A dead node
                    // needs no help: its connection closes and MongoKitten
                    // drops it from the pool.
                    try? await cluster.reconnect()
                }
                try await Task.sleep(for: backoff)
                backoff = min(backoff * 2, Self.maximumBackoff)
            }
        }
    }

    /// Sends `command` to the node MongoKitten currently takes for the
    /// primary and turns an error reply into a thrown `MongoCommandFailure`.
    private func run(_ command: Document, in namespace: MongoNamespace) async throws -> Document {
        let connection = try await cluster.next(for: .writable)
        let reply = try await connection.execute(command, namespace: namespace).getDocument()
        if let failure = MongoCommandFailure(reply: reply) {
            throw failure
        }
        return reply
    }

    private func fieldValue(of document: Document) throws -> Int {
        guard let value = bsonInteger(document[location.field]) else {
            throw MongoCommandFailure(message: "\(location.field) is not a number in \(document)")
        }
        return value
    }

    /// A random (version 4) UUID as BSON binary subtype 4, the form of a
    /// session id.
    private static func makeSessionID() -> Binary {
        var bytes = (0..<16).map { _ in UInt8.random(in: .min ... .max) }
        bytes[6] = (bytes[6] & 0x0F) | 0x40
        bytes[8] = (bytes[8] & 0x3F) | 0x80
        return Binary(subType: .uuid, buffer: ByteBuffer(bytes: bytes))
    }
}
