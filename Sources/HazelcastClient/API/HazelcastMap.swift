/// A distributed map (`IMap`) with typed keys and values.
///
/// Locks belong to the client *and* a thread id, like Java threads: a lock
/// taken with one `threadID` can only be released with the same one, and
/// different thread ids of the same client contend with each other. Every
/// method takes the thread id its caller acts as.
public struct HazelcastMap<Key: HazelcastSerializable, Value: HazelcastSerializable>: Sendable {
    public let name: String
    let client: HazelcastClient

    init(name: String, client: HazelcastClient) {
        self.name = name
        self.client = client
    }

    /// The value for `key`, or `nil` if there is none.
    public func get(_ key: Key, threadID: Int64) async throws -> Value? {
        let keyData = key.hazelcastData()
        let request = MapGetCodec.encodeRequest(name: name, key: keyData, threadID: threadID)
        let response = try await client.invoke(request, on: keyData, expecting: MapGetCodec.responseType)
        return try MapGetCodec.decodeResponse(response).map { data throws(HazelcastError) in
            try Value(hazelcastData: data)
        }
    }

    /// Stores `value` under `key` and returns the previous value.
    @discardableResult
    public func put(_ key: Key, _ value: Value, threadID: Int64) async throws -> Value? {
        let keyData = key.hazelcastData()
        let request = MapPutCodec.encodeRequest(
            name: name,
            key: keyData,
            value: value.hazelcastData(),
            threadID: threadID
        )
        let response = try await client.invoke(request, on: keyData, expecting: MapPutCodec.responseType)
        return try MapPutCodec.decodeResponse(response).map { data throws(HazelcastError) in
            try Value(hazelcastData: data)
        }
    }

    /// Replaces the value of `key` with `new` only if it currently equals
    /// `expected` (compared in serialized form). Returns whether it did.
    public func replace(_ key: Key, expected: Value, new: Value, threadID: Int64) async throws -> Bool {
        let keyData = key.hazelcastData()
        let request = MapReplaceIfSameCodec.encodeRequest(
            name: name,
            key: keyData,
            testValue: expected.hazelcastData(),
            value: new.hazelcastData(),
            threadID: threadID
        )
        let response = try await client.invoke(request, on: keyData, expecting: MapReplaceIfSameCodec.responseType)
        return try MapReplaceIfSameCodec.decodeResponse(response)
    }

    /// Takes the lock on `key` for `threadID`, waiting as long as it takes.
    /// The lock is reentrant for the same thread id.
    public func lock(_ key: Key, threadID: Int64) async throws {
        let keyData = key.hazelcastData()
        let request = MapLockCodec.encodeRequest(
            name: name,
            key: keyData,
            threadID: threadID,
            referenceID: client.nextLockReferenceID()
        )
        _ = try await client.invoke(request, on: keyData, expecting: MapLockCodec.responseType)
    }

    /// Releases the lock on `key` held by `threadID`. The member answers
    /// with `IllegalMonitorStateException` if that thread does not hold it.
    public func unlock(_ key: Key, threadID: Int64) async throws {
        let keyData = key.hazelcastData()
        let request = MapUnlockCodec.encodeRequest(
            name: name,
            key: keyData,
            threadID: threadID,
            referenceID: client.nextLockReferenceID()
        )
        _ = try await client.invoke(request, on: keyData, expecting: MapUnlockCodec.responseType)
    }

    /// Runs `body` while holding the lock on `key`, and releases the lock
    /// whether `body` returns or throws.
    public func withLock<Result: Sendable>(
        _ key: Key,
        threadID: Int64,
        _ body: () async throws -> Result
    ) async throws -> Result {
        try await lock(key, threadID: threadID)
        let result: Result
        do {
            result = try await body()
        } catch {
            try? await unlock(key, threadID: threadID)
            throw error
        }
        try await unlock(key, threadID: threadID)
        return result
    }
}
