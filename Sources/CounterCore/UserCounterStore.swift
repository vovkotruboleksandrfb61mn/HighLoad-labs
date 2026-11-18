/// One counter per user (Task Adv 1): `POST /inc?user_id=N` increments the
/// counter of user N.
///
/// A per-user store is a `CounterStore` too: the single counter of the
/// earlier tasks is the counter of `defaultUser`.
public protocol UserCounterStore: CounterStore {
    /// Adds one to the counter of `user`, creating it at 1 if it is new.
    func increment(user: Int64) async throws
    /// The counter of `user`, 0 if it has none.
    func value(user: Int64) async throws -> Int
    /// Sets the counter of `user` to zero.
    func reset(user: Int64) async throws
    /// Counts the store keeps about its work since the last reset, as
    /// `name=value` pairs, or `nil` if it keeps none.
    var statistics: String? { get }
}

/// The user behind `GET /inc` without a `user_id`.
public let defaultUser: Int64 = 1

extension UserCounterStore {
    public func increment() async throws { try await increment(user: defaultUser) }
    public func value() async throws -> Int { try await value(user: defaultUser) }
    public func reset() async throws { try await reset(user: defaultUser) }
    public var statistics: String? { nil }
}
