import NIOCore
import NIOPosix
import Synchronization

#if canImport(Glibc)
import Glibc
#elseif canImport(Musl)
import Musl
#elseif canImport(Darwin)
import Darwin
#endif

/// A counter kept in a file, like `FileCounterStore`, but with group commit:
/// increments that arrive while a flush is in flight share the next one.
///
/// An increment adds one to the value in memory and joins the queue of
/// waiters. A single flusher at a time takes the current value together with
/// every waiter queued so far, writes the value with one `pwrite`, makes it
/// durable with one `fdatasync` (or `fsync`), and only then completes those
/// waiters. So no increment is acknowledged before a sync that covers it has
/// returned, and no increment is lost: each one is counted in memory under
/// the lock and every later write includes it.
///
/// The record has a fixed size and the file is synced in full (file and
/// directory) when the store opens it, so later overwrites change no
/// metadata that is needed to read the data back, and `fdatasync` is enough
/// to put each value on disk.
///
/// `value()` returns the last value that is on disk, which includes every
/// increment that has been acknowledged.
public final class GroupCommitFileCounterStore: EventLoopCounterStore {
    public enum SyncMethod: String, Sendable, CaseIterable {
        case fsync
        case fdatasync
    }

    private enum Waiter {
        case promise(EventLoopPromise<Void>)
        case continuation(CheckedContinuation<Void, any Error>)

        func succeed() {
            switch self {
            case .promise(let promise): promise.succeed()
            case .continuation(let continuation): continuation.resume()
            }
        }

        func fail(_ error: any Error) {
            switch self {
            case .promise(let promise): promise.fail(error)
            case .continuation(let continuation): continuation.resume(throwing: error)
            }
        }
    }

    private struct State {
        /// The value with every increment counted so far.
        var value: Int
        /// The value the last completed sync put on disk.
        var durable: Int
        /// Operations counted in `value` but not yet on disk.
        var waiters: [Waiter] = []
        var isFlushing = false
        /// Set when a write or sync fails; the store then refuses everything.
        var failure: (any Error)?
    }

    public let path: String
    public let syncMethod: SyncMethod
    private let fd: Int32
    private let state: Mutex<State>
    /// How long the flusher waits, after it finds work, before it takes
    /// the batch (like PostgreSQL's `commit_delay`). Zero by default.
    public let gatherDelay: Duration
    private let threadPool: NIOThreadPool
    private let batches = Atomic<Int>(0)
    private let flushedOperations = Atomic<Int>(0)

    /// Opens (or creates) the counter file at `path`. A new or empty file
    /// starts at zero.
    public init(
        path: String,
        syncMethod: SyncMethod = .fdatasync,
        gatherDelay: Duration = .zero,
        threadPool: NIOThreadPool = .singleton
    ) throws {
        let fd = open(path, O_RDWR | O_CREAT | O_CLOEXEC, 0o644)
        guard fd >= 0 else {
            throw CounterStoreError.systemCall(name: "open(\(path))", errno: errno)
        }
        let initial: Int
        do {
            if let value = try CounterFile.read(fd, path: path) {
                initial = value
            } else {
                initial = 0
                try CounterFile.writeRecord(0, to: fd)
            }
            // Make the file's size and its directory entry durable once, so
            // fdatasync only ever has the record itself to flush.
            try CounterFile.sync(fd, name: "fsync") { fsync($0) }
            try Self.syncDirectory(of: path)
        } catch {
            close(fd)
            throw error
        }
        self.path = path
        self.syncMethod = syncMethod
        self.gatherDelay = gatherDelay
        self.fd = fd
        self.state = Mutex(State(value: initial, durable: initial))
        self.threadPool = threadPool
    }

    deinit {
        close(fd)
    }

    /// How many write + sync rounds have run (each covers one batch).
    public var flushCount: Int {
        batches.load(ordering: .relaxed)
    }

    /// How many operations those rounds covered in total.
    public var flushedOperationCount: Int {
        flushedOperations.load(ordering: .relaxed)
    }

    // MARK: - CounterStore

    public func increment() async throws {
        try await withCheckedThrowingContinuation { continuation in
            enqueue(.continuation(continuation)) { $0 += 1 }
        }
    }

    public func value() async throws -> Int {
        durableValue()
    }

    public func reset() async throws {
        try await withCheckedThrowingContinuation { continuation in
            enqueue(.continuation(continuation)) { $0 = 0 }
        }
    }

    // MARK: - EventLoopCounterStore

    public func increment(on eventLoop: any EventLoop) -> EventLoopFuture<Void> {
        let promise = eventLoop.makePromise(of: Void.self)
        enqueue(.promise(promise)) { $0 += 1 }
        return promise.futureResult
    }

    public func value(on eventLoop: any EventLoop) -> EventLoopFuture<Int> {
        eventLoop.makeSucceededFuture(durableValue())
    }

    public func reset(on eventLoop: any EventLoop) -> EventLoopFuture<Void> {
        let promise = eventLoop.makePromise(of: Void.self)
        enqueue(.promise(promise)) { $0 = 0 }
        return promise.futureResult
    }

    // MARK: - group commit

    private func durableValue() -> Int {
        state.withLock { $0.durable }
    }

    /// Applies `change` to the value in memory and queues `waiter` for the
    /// sync that will cover it, starting the flusher if it is idle.
    private func enqueue(_ waiter: Waiter, _ change: (inout Int) -> Void) {
        enum Outcome {
            case queued
            case startFlusher
            case refused(any Error)
        }
        let outcome: Outcome = state.withLock { state in
            if let failure = state.failure {
                return .refused(failure)
            }
            change(&state.value)
            state.waiters.append(waiter)
            if state.isFlushing {
                return .queued
            }
            state.isFlushing = true
            return .startFlusher
        }
        switch outcome {
        case .queued:
            break
        case .startFlusher:
            threadPool.submit { _ in
                self.flush()
            }
        case .refused(let error):
            waiter.fail(error)
        }
    }

    /// Runs on the thread pool, one at a time: writes and syncs batch after
    /// batch until no operation is waiting.
    private func flush() {
        while true {
            if gatherDelay > .zero {
                Self.sleep(for: gatherDelay)
            }
            let batch: (value: Int, waiters: [Waiter])? = state.withLock { state in
                if state.waiters.isEmpty {
                    state.isFlushing = false
                    return nil
                }
                let waiters = state.waiters
                state.waiters.removeAll(keepingCapacity: true)
                return (state.value, waiters)
            }
            guard let batch else {
                return
            }
            do {
                try CounterFile.writeRecord(batch.value, to: fd)
                switch syncMethod {
                case .fsync:
                    try CounterFile.sync(fd, name: "fsync") { fsync($0) }
                case .fdatasync:
                    try CounterFile.sync(fd, name: "fdatasync") { fdatasync($0) }
                }
            } catch {
                let stranded = state.withLock { state in
                    state.failure = error
                    state.isFlushing = false
                    defer { state.waiters.removeAll() }
                    return state.waiters
                }
                for waiter in batch.waiters + stranded {
                    waiter.fail(error)
                }
                return
            }
            state.withLock { $0.durable = batch.value }
            batches.add(1, ordering: .relaxed)
            flushedOperations.add(batch.waiters.count, ordering: .relaxed)
            for waiter in batch.waiters {
                waiter.succeed()
            }
        }
    }

    /// Blocks the flusher's pool thread; it is ours for the whole loop.
    private static func sleep(for duration: Duration) {
        let (seconds, attoseconds) = duration.components
        var request = timespec(tv_sec: Int(seconds), tv_nsec: Int(attoseconds / 1_000_000_000))
        var remaining = timespec()
        while nanosleep(&request, &remaining) != 0 && errno == EINTR {
            request = remaining
        }
    }

    private static func syncDirectory(of path: String) throws {
        let directory: String
        if let slash = path.lastIndex(of: "/") {
            directory = slash == path.startIndex ? "/" : String(path[..<slash])
        } else {
            directory = "."
        }
        let fd = open(directory, O_RDONLY | O_DIRECTORY | O_CLOEXEC)
        guard fd >= 0 else {
            throw CounterStoreError.systemCall(name: "open(\(directory))", errno: errno)
        }
        defer { close(fd) }
        try CounterFile.sync(fd, name: "fsync(\(directory))") { fsync($0) }
    }
}
