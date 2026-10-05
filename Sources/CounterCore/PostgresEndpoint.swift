import PostgresNIO

/// Where a PostgreSQL server lives and who to log in as, parsed from a URL of
/// the form `postgres://user[:password]@host[:port][/database]`.
///
/// PostgresNIO has no URL parser of its own; this one covers what the
/// benchmarks and the server need (TCP, no TLS, no percent-encoding).
public struct PostgresEndpoint: Sendable, Equatable {
    public var host: String
    public var port: Int
    public var username: String
    public var password: String?
    public var database: String?

    public init(host: String, port: Int = 5432, username: String, password: String? = nil, database: String? = nil) {
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
    }

    public init(url: String) throws(PostgresEndpointError) {
        var rest = Substring(url)
        guard let schemeEnd = rest.firstRange(of: "://") else {
            throw .invalid(url, reason: "missing scheme")
        }
        guard ["postgres", "postgresql"].contains(rest[..<schemeEnd.lowerBound]) else {
            throw .invalid(url, reason: "scheme must be postgres:// or postgresql://")
        }
        rest = rest[schemeEnd.upperBound...]

        let authority: Substring
        if let slash = rest.firstIndex(of: "/") {
            authority = rest[..<slash]
            let path = rest[rest.index(after: slash)...]
            database = path.isEmpty ? nil : String(path)
        } else {
            authority = rest
            database = nil
        }

        guard let at = authority.lastIndex(of: "@") else {
            throw .invalid(url, reason: "missing user name")
        }
        let credentials = authority[..<at]
        if let colon = credentials.firstIndex(of: ":") {
            username = String(credentials[..<colon])
            password = String(credentials[credentials.index(after: colon)...])
        } else {
            username = String(credentials)
            password = nil
        }
        guard !username.isEmpty else {
            throw .invalid(url, reason: "missing user name")
        }

        let hostAndPort = authority[authority.index(after: at)...]
        if let colon = hostAndPort.lastIndex(of: ":") {
            host = String(hostAndPort[..<colon])
            guard let port = Int(hostAndPort[hostAndPort.index(after: colon)...]), (1...65_535).contains(port) else {
                throw .invalid(url, reason: "invalid port")
            }
            self.port = port
        } else {
            host = String(hostAndPort)
            port = 5432
        }
        guard !host.isEmpty else {
            throw .invalid(url, reason: "missing host")
        }
    }

    /// Settings for a single connection (`PostgresConnection.connect`).
    public var connectionConfiguration: PostgresConnection.Configuration {
        .init(host: host, port: port, username: username, password: password, database: database, tls: .disable)
    }

    /// Settings for a connection pool (`PostgresClient`).
    public func clientConfiguration(maximumConnections: Int) -> PostgresClient.Configuration {
        var configuration = PostgresClient.Configuration(
            host: host,
            port: port,
            username: username,
            password: password,
            database: database,
            tls: .disable
        )
        configuration.options.maximumConnections = maximumConnections
        return configuration
    }
}

public enum PostgresEndpointError: Error, Equatable, CustomStringConvertible {
    case invalid(String, reason: String)

    public var description: String {
        switch self {
        case .invalid(let url, let reason):
            "invalid PostgreSQL URL \(url.debugDescription): \(reason)"
        }
    }
}
