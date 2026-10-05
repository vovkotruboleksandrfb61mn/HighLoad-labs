import NIOCore

/// A counter store that can answer without Swift Concurrency: every
/// operation returns an `EventLoopFuture`, so a server can call it straight
/// from a channel handler on an event loop and write the response in the
/// callback, with no hop through the cooperative thread pool.
///
/// The futures may complete on any event loop; callers hop back to their own.
/// The guarantees are those of `CounterStore`: no increment is ever lost,
/// and a future completes only once its operation is done.
public protocol EventLoopCounterStore: CounterStore {
    func increment(on eventLoop: any EventLoop) -> EventLoopFuture<Void>
    func value(on eventLoop: any EventLoop) -> EventLoopFuture<Int>
    func reset(on eventLoop: any EventLoop) -> EventLoopFuture<Void>
}

extension InMemoryCounterStore: EventLoopCounterStore {
    public func increment(on eventLoop: any EventLoop) -> EventLoopFuture<Void> {
        incrementNow()
        return eventLoop.makeSucceededVoidFuture()
    }

    public func value(on eventLoop: any EventLoop) -> EventLoopFuture<Int> {
        eventLoop.makeSucceededFuture(valueNow())
    }

    public func reset(on eventLoop: any EventLoop) -> EventLoopFuture<Void> {
        resetNow()
        return eventLoop.makeSucceededVoidFuture()
    }
}
