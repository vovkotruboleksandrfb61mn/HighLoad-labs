import AsyncHTTPClient
import NIOCore
import NIOHTTP1
import NIOPosix

struct LoadgenError: Error, CustomStringConvertible {
    let description: String
}

/// Which HTTP client drives the timed GET /inc requests.
enum LoadEngine: String, CaseIterable, Sendable {
    /// AsyncHTTPClient: one task per client, `client.execute` per request.
    case ahc
    /// Raw SwiftNIO: one channel handler per connection (`NIOLoadRun`).
    case nio
}

/// One measured run: reset, warm up, N clients x `requests` GET /inc, verify.
struct LoadRun: Sendable {
    let client: HTTPClient
    let baseURL: String
    let clients: Int
    let requests: Int
    var engine: LoadEngine = .ahc
    var eventLoops = System.coreCount

    private static let timeout = TimeAmount.seconds(30)

    func run() async throws -> RunResult {
        try await send(.POST, "/reset", expecting: .noContent)

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
                eventLoops: eventLoops
            ).timedIncrements()
        }

        let body = try await send(.GET, "/count", expecting: .ok)
        guard let finalValue = Int(body.trimmingSuffix(while: \.isWhitespace)) else {
            throw LoadgenError(description: "GET /count returned \(body.debugDescription), not a number")
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
            for _ in 0..<clients {
                group.addTask {
                    // Open this client's keep-alive connection before the
                    // clock starts. /count does not change the counter.
                    // The gate is passed even on failure so the coordinator
                    // never waits for a client that has already given up.
                    let warmUp: (any Error)?
                    do {
                        try await send(.GET, "/count", expecting: .ok)
                        warmUp = nil
                    } catch {
                        warmUp = error
                    }
                    await gate.arriveAndWait()
                    if let warmUp {
                        throw warmUp
                    }
                    for _ in 0..<requests {
                        try await send(.GET, "/inc", expecting: .noContent)
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
