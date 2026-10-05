import NIOCore
import Synchronization

/// Matches responses to requests: hands out correlation ids and keeps the
/// continuation of every request still waiting for its response.
///
/// Once the connection closes, every pending request fails and new ones are
/// refused. Continuations are always resumed outside the lock.
final class InvocationRegistry: Sendable {
    typealias Continuation = CheckedContinuation<ClientMessage, any Error>

    /// Whoever waits for a response: a task, or an event-loop future.
    enum Waiter: Sendable {
        case continuation(Continuation)
        case promise(EventLoopPromise<ClientMessage>)

        func resume(returning response: ClientMessage) {
            switch self {
            case .continuation(let continuation): continuation.resume(returning: response)
            case .promise(let promise): promise.succeed(response)
            }
        }

        func resume(throwing error: any Error) {
            switch self {
            case .continuation(let continuation): continuation.resume(throwing: error)
            case .promise(let promise): promise.fail(error)
            }
        }
    }

    private struct State {
        var pending: [Int64: Waiter] = [:]
        var closedWith: (any Error)?
    }

    private let state = Mutex(State())
    private let nextCorrelationID = Atomic<Int64>(1)

    func makeCorrelationID() -> Int64 {
        nextCorrelationID.wrappingAdd(1, ordering: .relaxed).oldValue
    }

    var pendingCount: Int {
        state.withLock { $0.pending.count }
    }

    /// Registers the continuation of request `id`. Returns false, having
    /// already resumed the continuation with an error, if the connection is
    /// closed or the current task is cancelled.
    ///
    /// Checking for cancellation under the lock closes the race with
    /// `cancel(_:)`: the task's cancelled flag is set before its cancellation
    /// handler runs, so either this sees the flag or `cancel` sees the entry.
    func register(_ id: Int64, _ continuation: Continuation) -> Bool {
        register(id, .continuation(continuation), checkCancellation: true)
    }

    /// Registers a request whose response completes `promise`. Returns
    /// false, having already failed the promise, if the connection is closed.
    func register(_ id: Int64, promise: EventLoopPromise<ClientMessage>) -> Bool {
        register(id, .promise(promise), checkCancellation: false)
    }

    private func register(_ id: Int64, _ waiter: Waiter, checkCancellation: Bool) -> Bool {
        let refusal: (any Error)? = state.withLock { state in
            if let error = state.closedWith {
                return error
            }
            if checkCancellation && Task.isCancelled {
                return CancellationError()
            }
            state.pending[id] = waiter
            return nil
        }
        if let refusal {
            waiter.resume(throwing: refusal)
            return false
        }
        return true
    }

    /// Delivers the response to request `id`. Returns false if nobody is
    /// waiting for it (a heartbeat response, or a request that was cancelled).
    @discardableResult
    func complete(_ id: Int64, with response: ClientMessage) -> Bool {
        guard let waiter = state.withLock({ $0.pending.removeValue(forKey: id) }) else {
            return false
        }
        waiter.resume(returning: response)
        return true
    }

    func fail(_ id: Int64, with error: any Error) {
        state.withLock { $0.pending.removeValue(forKey: id) }?.resume(throwing: error)
    }

    /// Fails request `id` with `CancellationError` if it is still pending.
    /// A response that arrives later is dropped.
    func cancel(_ id: Int64) {
        fail(id, with: CancellationError())
    }

    /// Fails every pending request with `error` and refuses new ones.
    func close(with error: any Error) {
        let pending = state.withLock { state in
            if state.closedWith == nil {
                state.closedWith = error
            }
            defer { state.pending.removeAll() }
            return state.pending
        }
        for waiter in pending.values {
            waiter.resume(throwing: error)
        }
    }
}
