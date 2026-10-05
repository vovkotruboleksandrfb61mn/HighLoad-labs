/// A single shared counter that the web server increments and reads.
///
/// Every implementation must be safe to call from many tasks at once and must
/// never lose an increment.
public protocol CounterStore: Sendable {
    /// Adds one to the counter.
    func increment() async throws
    /// The current value of the counter.
    func value() async throws -> Int
    /// Sets the counter back to zero.
    func reset() async throws
}
