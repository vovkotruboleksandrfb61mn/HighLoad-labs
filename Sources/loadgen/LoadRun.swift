import AsyncHTTPClient
import NIOCore
import NIOHTTP1
import NIOPosix

struct LoadgenError: Error, CustomStringConvertible {
    let description: String
}

/// Which HTTP client drives the timed increments.
enum LoadEngine: String, CaseIterable, Sendable {
    /// AsyncHTTPClient: one task per client, `client.execute` per request.
    case ahc
    /// Raw SwiftNIO: one channel handler per connection (`NIOLoadRun`).
    case nio
}

/// Which counters the clients increment.
struct Workload: Sendable {
    /// `nil`: the single counter of Tasks 1-5, `GET /inc`. Otherwise
    /// per-user counters (Task Adv 1), `POST /inc?user_id=N`, starting with
    /// this user.
    var firstUser: Int64?
    /// Client i increments user `firstUser + i` instead of all clients
    /// incrementing `firstUser`.
    var distinctUsers = false

    /// The user client `index` increments.
    func user(ofClient index: Int) -> Int64? {
        firstUser.map { distinctUsers ? $0 + Int64(index) : $0 }
    }

    /// Every user the clients increment (`[nil]` for the single counter).
    func users(clients: Int) -> [Int64?] {
        guard let firstUser else { return [nil] }
        return distinctUsers ? (0..<clients).map { firstUser + Int64($0) } : [firstUser]
    }

    func increment(ofClient index: Int) -> (method: HTTPMethod, path: String) {
        guard let user = user(ofClient: index) else { return (.GET, "/inc") }
        return (.POST, "/inc?user_id=\(user)")
    }

    func count(of user: Int64?) -> String {
        user.map { "/count?user_id=\($0)" } ?? "/count"
    }

    func reset(of user: Int64?) -> String {
        user.map { "/reset?user_id=\($0)" } ?? "/reset"
    }
}

/// One measured run: reset, warm up, N clients x `requests` increments, verify.
struct LoadRun: Sendable {
    let client: HTTPClient
    let baseURL: String
    let clients: Int
    let requests: Int
    var workload = Workload()
    var engine: LoadEngine = .ahc
    var eventLoops = System.coreCount

    private static let timeout = TimeAmount.seconds(30)

    func run() async throws -> RunResult {
        let users = workload.users(clients: clients)
        for user in users {
            try await send(.POST, workload.reset(of: user), expecting: .noContent)
        }

        let elapsed: Duration
        switch engine {
        case .ahc:
            elapsed = try await timedIncrementsWithAsyncHTTPClient()
        case .nio:
            let (host, port) = try hostAndPort()
            elapsed = try await NIOLoadRun(
                host: host,
                port: port,
                clients: clients,
                requests: requests,
                workload: workload,
                eventLoops: eventLoops
            ).timedIncrements()
        }

        // The sum over the users, so the expected value is always
        // clients x requests.
        var finalValue = 0
        for user in users {
            let path = workload.count(of: user)
            let body = try await send(.GET, path, expecting: .ok)
            guard let value = Int(body.trimmingSuffix(while: \.isWhitespace)) else {
                throw LoadgenError(description: "GET \(path) returned \(body.debugDescription), not a number")
            }
            finalValue += value
        }
        return RunResult(clients: clients, requestsPerClient: requests, elapsed: elapsed, finalValue: finalValue)
    }

    /// `http://host:port` split into its parts (no path, no TLS).
    private func hostAndPort() throws -> (String, Int) {
        guard baseURL.hasPrefix("http://") else {
            throw LoadgenError(description: "--engine nio needs an http:// URL, got \(baseURL)")
        }
        let authority = baseURL.dropFirst("http://".count).prefix { $0 != "/" }
        guard let colon = authority.lastIndex(of: ":") else {
            return (String(authority), 80)
        }
        guard let port = Int(authority[authority.index(after: colon)...]) else {
            throw LoadgenError(description: "invalid port in \(baseURL)")
        }
        return (String(authority[..<colon]), port)
    }

    private func timedIncrementsWithAsyncHTTPClient() async throws -> Duration {
        let gate = StartGate(participants: clients)
        let clock = ContinuousClock()
        return try await withThrowingTaskGroup(of: Void.self) { group in
            for index in 0..<clients {
                let increment = workload.increment(ofClient: index)
                let countPath = workload.count(of: workload.user(ofClient: index))
                group.addTask {
                    // Open this client's keep-alive connection before the
                    // clock starts. /count does not change the counter.
                    // The gate is passed even on failure so the coordinator
                    // never waits for a client that has already given up.
                    let warmUp: (any Error)?
                    do {
                        try await send(.GET, countPath, expecting: .ok)
                        warmUp = nil
                    } catch {
                        warmUp = error
                    }
                    await gate.arriveAndWait()
                    if let warmUp {
                        throw warmUp
                    }
                    for _ in 0..<requests {
                        try await send(increment.method, increment.path, expecting: .noContent)
                    }
                }
            }
            await gate.allArrived()
            let start = clock.now
            await gate.open()
            try await group.waitForAll()
            return clock.now - start
        }
    }

    /// Sends one request, checks the status and returns the body as text.
    @discardableResult
    private func send(_ method: HTTPMethod, _ path: String, expecting status: HTTPResponseStatus) async throws -> String {
        var request = HTTPClientRequest(url: baseURL + path)
        request.method = method
        let response = try await client.execute(request, timeout: Self.timeout)
        let body = try await response.body.collect(upTo: 1 << 16)
        guard response.status == status else {
            throw LoadgenError(
                description: "\(method) \(path) returned \(response.status.code), expected \(status.code): \(String(buffer: body))"
            )
        }
        return String(buffer: body)
    }
}

extension StringProtocol {
    fileprivate func trimmingSuffix(while predicate: (Character) -> Bool) -> SubSequence {
        var end = endIndex
        while end > startIndex, predicate(self[index(before: end)]) {
            end = index(before: end)
        }
        return self[startIndex..<end]
    }
}
