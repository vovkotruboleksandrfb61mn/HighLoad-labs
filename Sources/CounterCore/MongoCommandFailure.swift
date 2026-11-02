import MongoClient
import MongoKitten
import NIOCore
import NIOPosix

/// A MongoDB command that the server answered with an error: either the
/// command failed (`ok: 0`) or it was applied but its write concern was not
/// satisfied (`writeConcernError`).
public struct MongoCommandFailure: Error, Equatable, CustomStringConvertible {
    public var code: Int?
    public var codeName: String?
    public var message: String
    /// `errorLabels` of the reply, e.g. `RetryableWriteError`.
    public var labels: [String]
    /// The write was applied on the primary, but the write concern failed.
    public var isWriteConcernError: Bool

    public init(code: Int? = nil, codeName: String? = nil, message: String, labels: [String] = [], isWriteConcernError: Bool = false) {
        self.code = code
        self.codeName = codeName
        self.message = message
        self.labels = labels
        self.isWriteConcernError = isWriteConcernError
    }

    /// The failure a command reply reports, or `nil` for a successful reply.
    public init?(reply: Document) {
        let labels = (reply["errorLabels"] as? Document)?.values.compactMap { $0 as? String } ?? []
        if bsonInteger(reply["ok"]) != 1 {
            self.init(
                code: bsonInteger(reply["code"]),
                codeName: reply["codeName"] as? String,
                message: reply["errmsg"] as? String ?? "command failed",
                labels: labels
            )
        } else if let concern = reply["writeConcernError"] as? Document {
            self.init(
                code: bsonInteger(concern["code"]),
                codeName: concern["codeName"] as? String,
                message: concern["errmsg"] as? String ?? "write concern failed",
                labels: labels,
                isWriteConcernError: true
            )
        } else {
            return nil
        }
    }

    /// The codes the retryable writes specification lists as retryable: the
    /// node is no longer (or not yet) primary, is shutting down, or could not
    /// be reached.
    public static let retryableCodes: Set<Int> = [
        6,  // HostUnreachable
        7,  // HostNotFound
        89,  // NetworkTimeout
        91,  // ShutdownInProgress
        189,  // PrimarySteppedDown
        262,  // ExceededTimeLimit
        9001,  // SocketException
        10107,  // NotWritablePrimary
        11600,  // InterruptedAtShutdown
        11602,  // InterruptedDueToReplStateChange
        13435,  // NotPrimaryNoSecondaryOk
        13436,  // NotPrimaryOrSecondary
    ]

    /// Codes meaning the node the client took for the primary is not one
    /// (any more): the client should look for the primary again.
    public static let notPrimaryCodes: Set<Int> = [91, 189, 10107, 11600, 11602, 13435, 13436]

    public var isRetryable: Bool {
        labels.contains("RetryableWriteError") || code.map(Self.retryableCodes.contains) == true
    }

    public var isNotPrimary: Bool {
        code.map(Self.notPrimaryCodes.contains) == true
    }

    public var description: String {
        let kind = isWriteConcernError ? "write concern error" : "command failed"
        let name = codeName.map { " \($0)" } ?? ""
        let number = code.map { " (\($0))" } ?? ""
        return "\(kind)\(name)\(number): \(message)"
    }

    /// Whether an operation that failed with `error` may be sent again: a
    /// retryable server error, or a network error, after which the client
    /// cannot know whether the server applied the operation. Retrying a write
    /// is only safe with the same `lsid` and `txnNumber`, so the server can
    /// recognise it.
    public static func isRetryable(_ error: any Error) -> Bool {
        switch error {
        case let failure as MongoCommandFailure:
            failure.isRetryable
        case let error as MongoError:
            // No connection, connection closed or reply timed out.
            error.kind != .authenticationFailure
        case is IOError, is ChannelError, is NIOConnectionError:
            true
        default:
            false
        }
    }

    /// A one-line name for the kind of `error`, for counting retries by cause:
    /// the server's code name, or the driver's error kind and reason.
    public static func kind(of error: any Error) -> String {
        switch error {
        case let failure as MongoCommandFailure:
            (failure.isWriteConcernError ? "writeConcernError " : "") + (failure.codeName ?? failure.code.map { "code \($0)" } ?? failure.message)
        case let error as MongoError:
            "MongoError." + error.kind.rawValue + (error.reason.map { "." + $0.rawValue } ?? "")
        default:
            "\(type(of: error)): \(error)"
        }
    }
}

/// A BSON number as an `Int`, whatever its BSON type.
func bsonInteger(_ value: (any Primitive)?) -> Int? {
    switch value {
    case let value as Int32: Int(value)
    case let value as Int: value  // BSON int64
    case let value as Double where value.rounded() == value: Int(value)
    default: nil
    }
}
