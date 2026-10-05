import NIOCore

/// A linearizable 64-bit counter in the CP Subsystem (`IAtomicLong`). Every
/// update is committed through the Raft log of its CP group, so no update
/// is ever lost.
public struct AtomicLong: Sendable {
    public let name: String
    public let groupID: RaftGroupID
    let client: HazelcastClient

    public func addAndGet(_ delta: Int64) async throws -> Int64 {
        let request = AtomicLongAddAndGetCodec.encodeRequest(groupID: groupID, name: name, delta: delta)
        let response = try await client.invoke(request, expecting: AtomicLongAddAndGetCodec.responseType)
        return try AtomicLongAddAndGetCodec.decodeResponse(response)
    }

    /// `addAndGet` as an event-loop future, without a task: the request is
    /// written from the caller's thread and the future completes on the
    /// connection's event loop.
    public func addAndGetFuture(_ delta: Int64) -> EventLoopFuture<Int64> {
        let request = AtomicLongAddAndGetCodec.encodeRequest(groupID: groupID, name: name, delta: delta)
        return client.invokeFuture(request, expecting: AtomicLongAddAndGetCodec.responseType)
            .flatMapThrowing { try AtomicLongAddAndGetCodec.decodeResponse($0) }
    }

    /// `get` as an event-loop future.
    public func getFuture() -> EventLoopFuture<Int64> {
        let request = AtomicLongGetCodec.encodeRequest(groupID: groupID, name: name)
        return client.invokeFuture(request, expecting: AtomicLongGetCodec.responseType)
            .flatMapThrowing { try AtomicLongGetCodec.decodeResponse($0) }
    }

    @discardableResult
    public func incrementAndGet() async throws -> Int64 {
        try await addAndGet(1)
    }

    public func get() async throws -> Int64 {
        let request = AtomicLongGetCodec.encodeRequest(groupID: groupID, name: name)
        let response = try await client.invoke(request, expecting: AtomicLongGetCodec.responseType)
        return try AtomicLongGetCodec.decodeResponse(response)
    }

    /// Sets the value and returns the old one.
    public func getAndSet(_ newValue: Int64) async throws -> Int64 {
        let request = AtomicLongGetAndSetCodec.encodeRequest(groupID: groupID, name: name, newValue: newValue)
        let response = try await client.invoke(request, expecting: AtomicLongGetAndSetCodec.responseType)
        return try AtomicLongGetAndSetCodec.decodeResponse(response)
    }

    public func set(_ newValue: Int64) async throws {
        _ = try await getAndSet(newValue)
    }
}
