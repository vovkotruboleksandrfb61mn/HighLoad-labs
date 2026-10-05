/// Entry point to the CP Subsystem's data structures.
public struct CPSubsystem: Sendable {
    let client: HazelcastClient

    /// The `IAtomicLong` called `name`. A name of the form `object@group`
    /// puts it in that CP group; otherwise it lives in the `default` group,
    /// which the cluster creates on first use.
    public func atomicLong(named name: String) async throws -> AtomicLong {
        let proxyName = try CPProxyName(name)
        let request = CPGroupCreateCPGroupCodec.encodeRequest(proxyName: proxyName.withoutDefaultGroup)
        let response = try await client.invoke(request, expecting: CPGroupCreateCPGroupCodec.responseType)
        let groupID = try CPGroupCreateCPGroupCodec.decodeResponse(response)
        return AtomicLong(name: proxyName.objectName, groupID: groupID, client: client)
    }
}

/// Splits `object@group` the way the Java and Python clients do.
struct CPProxyName: Equatable {
    static let defaultGroupName = "default"
    static let metadataGroupName = "metadata"

    /// The name with `@default` dropped, sent to `CreateCPGroup`.
    var withoutDefaultGroup: String
    /// The name without any group, sent to the data structure's own requests.
    var objectName: String
}

extension CPProxyName {
    init(_ name: String) throws(HazelcastError) {
        let name = name.trimmingSpaces()
        let parts = name.split(separator: "@", omittingEmptySubsequences: false)
        switch parts.count {
        case 1:
            self.withoutDefaultGroup = name
            self.objectName = name
        case 2:
            let object = String(parts[0]).trimmingSpaces()
            let group = String(parts[1]).trimmingSpaces()
            guard !object.isEmpty, !group.isEmpty else {
                throw .invalidArgument("CP object and group names must not be empty: \(name)")
            }
            guard group.lowercased() != Self.metadataGroupName else {
                throw .invalidArgument("CP data structures cannot live in the METADATA group")
            }
            self.withoutDefaultGroup = group.lowercased() == Self.defaultGroupName ? object : name
            self.objectName = object
        default:
            throw .invalidArgument("a CP group may be named at most once: \(name)")
        }
    }
}

extension String {
    fileprivate func trimmingSpaces() -> String {
        let trimmed = drop { $0.isWhitespace }.reversed().drop { $0.isWhitespace }.reversed()
        return String(trimmed)
    }
}
