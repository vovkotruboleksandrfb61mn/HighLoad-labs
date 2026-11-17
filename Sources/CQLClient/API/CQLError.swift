import NIOCore

/// Everything the client can fail with.
public enum CQLError: Error, Sendable, Equatable, CustomStringConvertible {
    /// The server sent bytes that do not follow the protocol.
    case protocolViolation(String)
    /// A response body ended before a field it must contain.
    case truncated(String)
    /// The connection closed, so the request got no response.
    case connectionClosed
    /// No response within the client-side request timeout. The server may
    /// still have executed the request.
    case requestTimedOut
    /// The server answered with an ERROR message.
    case server(ServerError)
    /// The server answered with a message the request does not expect.
    case unexpectedResponse(Opcode)
    /// The server asks for authentication, which this client does not do.
    case authenticationRequired(String)
    /// No node of the cluster could be reached.
    case noHostAvailable
    case invalidArgument(String)

    public var description: String {
        switch self {
        case .protocolViolation(let detail): "protocol violation: \(detail)"
        case .truncated(let field): "protocol violation: the message ends inside a \(field)"
        case .connectionClosed: "the connection to the node is closed"
        case .requestTimedOut: "no response within the request timeout"
        case .server(let error): error.description
        case .unexpectedResponse(let opcode): "unexpected response \(opcode)"
        case .authenticationRequired(let authenticator): "the server requires authentication (\(authenticator))"
        case .noHostAvailable: "no node is reachable"
        case .invalidArgument(let detail): "invalid argument: \(detail)"
        }
    }

    /// The server error, if this is one.
    public var serverError: ServerError? {
        if case .server(let error) = self { error } else { nil }
    }
}

/// An ERROR message: the code, the message and the code-specific details.
public struct ServerError: Error, Sendable, Equatable, CustomStringConvertible {
    public var code: Code
    public var message: String
    public var details: Details

    public init(code: Code, message: String, details: Details = .none) {
        self.code = code
        self.message = message
        self.details = details
    }

    /// The error codes of the v4 spec; unknown codes are kept as they are.
    public struct Code: RawRepresentable, Sendable, Hashable, CustomStringConvertible {
        public let rawValue: Int32

        public init(rawValue: Int32) {
            self.rawValue = rawValue
        }

        public static let serverError = Code(rawValue: 0x0000)
        public static let protocolError = Code(rawValue: 0x000A)
        public static let badCredentials = Code(rawValue: 0x0100)
        public static let unavailable = Code(rawValue: 0x1000)
        public static let overloaded = Code(rawValue: 0x1001)
        public static let isBootstrapping = Code(rawValue: 0x1002)
        public static let truncateError = Code(rawValue: 0x1003)
        public static let writeTimeout = Code(rawValue: 0x1100)
        public static let readTimeout = Code(rawValue: 0x1200)
        public static let readFailure = Code(rawValue: 0x1300)
        public static let functionFailure = Code(rawValue: 0x1400)
        public static let writeFailure = Code(rawValue: 0x1500)
        public static let syntaxError = Code(rawValue: 0x2000)
        public static let unauthorized = Code(rawValue: 0x2100)
        public static let invalid = Code(rawValue: 0x2200)
        public static let configError = Code(rawValue: 0x2300)
        public static let alreadyExists = Code(rawValue: 0x2400)
        public static let unprepared = Code(rawValue: 0x2500)

        static let names: [Int32: String] = [
            0x0000: "Server_error", 0x000A: "Protocol_error", 0x0100: "Bad_credentials",
            0x1000: "Unavailable", 0x1001: "Overloaded", 0x1002: "Is_bootstrapping",
            0x1003: "Truncate_error", 0x1100: "Write_timeout", 0x1200: "Read_timeout",
            0x1300: "Read_failure", 0x1400: "Function_failure", 0x1500: "Write_failure",
            0x2000: "Syntax_error", 0x2100: "Unauthorized", 0x2200: "Invalid",
            0x2300: "Config_error", 0x2400: "Already_exists", 0x2500: "Unprepared",
        ]

        public var description: String {
            Self.names[rawValue] ?? "0x\(String(rawValue, radix: 16))"
        }
    }

    public enum Details: Sendable, Equatable {
        case none
        /// Not enough replicas alive to even try.
        case unavailable(consistency: Consistency, required: Int32, alive: Int32)
        /// Replicas were contacted but too few acknowledged in time. The
        /// write may still be applied on some replicas.
        case writeTimeout(consistency: Consistency, received: Int32, blockFor: Int32, writeType: String)
        case readTimeout(consistency: Consistency, received: Int32, blockFor: Int32, dataPresent: Bool)
        case readFailure(consistency: Consistency, received: Int32, blockFor: Int32, failures: Int32, dataPresent: Bool)
        case writeFailure(consistency: Consistency, received: Int32, blockFor: Int32, failures: Int32, writeType: String)
        case functionFailure(keyspace: String, function: String, argumentTypes: [String])
        case alreadyExists(keyspace: String, table: String)
        /// The node does not know this prepared statement id.
        case unprepared(id: [UInt8])
    }

    public var description: String {
        var text = "\(code): \(message)"
        switch details {
        case .none, .alreadyExists, .unprepared, .functionFailure:
            break
        case .unavailable(let consistency, let required, let alive):
            text += " (\(consistency), required \(required), alive \(alive))"
        case .writeTimeout(let consistency, let received, let blockFor, let writeType):
            text += " (\(consistency), received \(received) of \(blockFor), \(writeType))"
        case .readTimeout(let consistency, let received, let blockFor, let dataPresent):
            text += " (\(consistency), received \(received) of \(blockFor), data \(dataPresent ? "present" : "absent"))"
        case .readFailure(let consistency, let received, let blockFor, let failures, _):
            text += " (\(consistency), received \(received) of \(blockFor), \(failures) failed)"
        case .writeFailure(let consistency, let received, let blockFor, let failures, let writeType):
            text += " (\(consistency), received \(received) of \(blockFor), \(failures) failed, \(writeType))"
        }
        return text
    }

    /// Decodes the body of an ERROR message.
    static func decode(_ body: inout ByteBuffer) throws(CQLError) -> ServerError {
        let code = Code(rawValue: try body.readCQLInt())
        let message = try body.readCQLString()
        let details: Details
        switch code {
        case .unavailable:
            details = .unavailable(consistency: try body.readCQLConsistency(), required: try body.readCQLInt(), alive: try body.readCQLInt())
        case .writeTimeout:
            details = .writeTimeout(
                consistency: try body.readCQLConsistency(),
                received: try body.readCQLInt(),
                blockFor: try body.readCQLInt(),
                writeType: try body.readCQLString()
            )
        case .readTimeout:
            details = .readTimeout(
                consistency: try body.readCQLConsistency(),
                received: try body.readCQLInt(),
                blockFor: try body.readCQLInt(),
                dataPresent: try body.readCQLInt8() != 0
            )
        case .readFailure:
            details = .readFailure(
                consistency: try body.readCQLConsistency(),
                received: try body.readCQLInt(),
                blockFor: try body.readCQLInt(),
                failures: try body.readCQLInt(),
                dataPresent: try body.readCQLInt8() != 0
            )
        case .writeFailure:
            details = .writeFailure(
                consistency: try body.readCQLConsistency(),
                received: try body.readCQLInt(),
                blockFor: try body.readCQLInt(),
                failures: try body.readCQLInt(),
                writeType: try body.readCQLString()
            )
        case .functionFailure:
            details = .functionFailure(keyspace: try body.readCQLString(), function: try body.readCQLString(), argumentTypes: try body.readCQLStringList())
        case .alreadyExists:
            details = .alreadyExists(keyspace: try body.readCQLString(), table: try body.readCQLString())
        case .unprepared:
            details = .unprepared(id: try body.readCQLShortBytes())
        default:
            details = .none
        }
        return ServerError(code: code, message: message, details: details)
    }
}
