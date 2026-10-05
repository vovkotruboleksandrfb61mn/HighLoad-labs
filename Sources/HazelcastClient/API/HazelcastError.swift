/// Everything the client can fail with.
public enum HazelcastError: Error, Sendable, Equatable, CustomStringConvertible {
    /// The member sent bytes that do not follow the protocol.
    case protocolViolation(String)
    /// The connection closed, so the request got no response.
    case connectionClosed
    /// The member refused the authentication request.
    case authenticationFailed(AuthenticationStatus)
    /// The member answered with an exception.
    case server(ServerError)
    /// A response had a type other than the one the request expects.
    case unexpectedResponse(expected: Int32, received: Int32)
    /// A `Data` could not be turned into the requested type.
    case serialization(String)
    case invalidArgument(String)

    public var description: String {
        switch self {
        case .protocolViolation(let detail):
            "protocol violation: \(detail)"
        case .connectionClosed:
            "the connection to the member is closed"
        case .authenticationFailed(let status):
            "authentication failed: \(status)"
        case .server(let error):
            error.description
        case .unexpectedResponse(let expected, let received):
            "expected response type \(expected), received \(received)"
        case .serialization(let detail):
            "serialization: \(detail)"
        case .invalidArgument(let detail):
            "invalid argument: \(detail)"
        }
    }
}

/// The authentication status byte of `Client.Authentication`.
public enum AuthenticationStatus: UInt8, Sendable, Equatable, CustomStringConvertible {
    case authenticated = 0
    case credentialsFailed = 1
    case serializationVersionMismatch = 2
    case notAllowedInCluster = 3

    public var description: String {
        switch self {
        case .authenticated: "authenticated"
        case .credentialsFailed: "credentials failed (wrong cluster name?)"
        case .serializationVersionMismatch: "serialization version mismatch"
        case .notAllowedInCluster: "client not allowed in the cluster"
        }
    }
}

/// An exception thrown on the member, as carried by `ErrorsCodec`.
public struct ServerError: Error, Sendable, Equatable, CustomStringConvertible {
    /// One of `ClientProtocolErrorCodes`.
    public var errorCode: Int32
    public var className: String
    public var message: String?

    public init(errorCode: Int32, className: String, message: String?) {
        self.errorCode = errorCode
        self.className = className
        self.message = message
    }

    /// Error codes that mean the member did not execute the operation, so it
    /// is safe to send it again (the Java client's `RetryableException`s).
    static let retrySafeCodes: Set<Int32> = [
        8,  // CALLER_NOT_MEMBER
        19,  // HAZELCAST_INSTANCE_NOT_ACTIVE
        39,  // PARTITION_MIGRATING
        46,  // RETRYABLE_HAZELCAST
        47,  // RETRYABLE_IO
        53,  // TARGET_NOT_MEMBER
        62,  // WRONG_TARGET
        90,  // CANNOT_REPLICATE_EXCEPTION
        93,  // NOT_LEADER_EXCEPTION
    ]

    public var isRetrySafe: Bool { Self.retrySafeCodes.contains(errorCode) }

    public var description: String {
        "\(className) (code \(errorCode))" + (message.map { ": \($0)" } ?? "")
    }
}
