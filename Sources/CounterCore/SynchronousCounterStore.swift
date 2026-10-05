/// A counter store whose operations finish immediately, without waiting on
/// I/O, so a server can call them inline on an event loop and answer in the
/// same pass, with no future and no task.
public protocol SynchronousCounterStore: CounterStore {
    func incrementNow()
    func valueNow() -> Int
    func resetNow()
}

extension InMemoryCounterStore: SynchronousCounterStore {}
