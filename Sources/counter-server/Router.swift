import CounterCore
import NIOCore
import NIOHTTP1

/// A response before it is turned into HTTP parts.
struct Response: Sendable {
    var status: HTTPResponseStatus
    var body: String?

    static let noContent = Response(status: .noContent)
    static let notFound = Response(status: .notFound, body: "not found\n")

    static func badRequest(_ reason: String) -> Response {
        Response(status: .badRequest, body: reason + "\n")
    }
}

/// Maps a request to the counter operation it names.
struct Router<Store: CounterStore>: Sendable {
    private enum Route {
        case increment
        case count
        case reset
        /// `POST /inc?user_id=N` and friends (Task Adv 1).
        case userIncrement(Int64)
        case userCount(Int64)
        case userReset(Int64)
        case stats
        case badRequest(String)
        case notFound
    }

    let store: Store
    /// The same store, when it can answer on an event loop without a task.
    private let eventLoopStore: (any EventLoopCounterStore)?
    /// The same store, when it keeps one counter per user.
    private let userStore: (any UserCounterStore)?

    init(store: Store) {
        self.store = store
        self.eventLoopStore = store as? any EventLoopCounterStore
        self.userStore = store as? any UserCounterStore
    }

    func respond(to head: HTTPRequestHead) async -> Response {
        do {
            switch route(of: head) {
            case .increment:
                try await store.increment()
                return .noContent
            case .count:
                return Response(status: .ok, body: "\(try await store.value())\n")
            case .reset:
                try await store.reset()
                return .noContent
            case .userIncrement(let user):
                guard let userStore else { return Self.noUsers }
                try await userStore.increment(user: user)
                return .noContent
            case .userCount(let user):
                guard let userStore else { return Self.noUsers }
                return Response(status: .ok, body: "\(try await userStore.value(user: user))\n")
            case .userReset(let user):
                guard let userStore else { return Self.noUsers }
                try await userStore.reset(user: user)
                return .noContent
            case .stats:
                return Response(status: .ok, body: (userStore?.statistics ?? "") + "\n")
            case .badRequest(let reason):
                return .badRequest(reason)
            case .notFound:
                return .notFound
            }
        } catch {
            return Self.failure(error, head: head)
        }
    }

    /// The same answer as `respond(to:)`, as a future. Stores that conform
    /// to `EventLoopCounterStore` are called directly; any other store runs
    /// in a task, as in the async server.
    func respond(to head: HTTPRequestHead, on eventLoop: any EventLoop) -> EventLoopFuture<Response> {
        guard let store = eventLoopStore else {
            return eventLoop.makeFutureWithTask { await respond(to: head) }
        }
        let future: EventLoopFuture<Response>
        switch route(of: head) {
        case .increment:
            future = store.increment(on: eventLoop).map { .noContent }
        case .count:
            future = store.value(on: eventLoop).map { Response(status: .ok, body: "\($0)\n") }
        case .reset:
            future = store.reset(on: eventLoop).map { .noContent }
        case .userIncrement, .userCount, .userReset, .stats, .badRequest, .notFound:
            return eventLoop.makeFutureWithTask { await respond(to: head) }
        }
        return future.recover { Self.failure($0, head: head) }
    }

    private func route(of head: HTTPRequestHead) -> Route {
        let path = path(of: head.uri)
        if let query = query(of: head.uri), let value = Self.parameter("user_id", in: query) {
            guard let user = Int64(value) else {
                return .badRequest("user_id must be an integer, got \(value.debugDescription)")
            }
            switch (head.method, path) {
            case (.POST, "/inc"): return .userIncrement(user)
            case (.GET, "/count"): return .userCount(user)
            case (.POST, "/reset"): return .userReset(user)
            default: return .notFound
            }
        }
        switch (head.method, path) {
        case (.GET, "/inc"):
            return .increment
        case (.POST, "/inc"):
            return .badRequest("POST /inc needs a user_id, e.g. /inc?user_id=1")
        case (.GET, "/count"):
            return .count
        case (.POST, "/reset"):
            return .reset
        case (.GET, "/stats"):
            return .stats
        default:
            return .notFound
        }
    }

    private static var noUsers: Response { .badRequest("this store keeps one counter; per-user counters need --store postgres-users or hazelcast-cache") }

    /// The value of `name` in a `a=1&b=2` query, without percent-decoding.
    static func parameter(_ name: String, in query: Substring) -> Substring? {
        for pair in query.split(separator: "&") {
            let parts = pair.split(separator: "=", maxSplits: 1, omittingEmptySubsequences: false)
            if parts.count == 2, parts[0] == name {
                return parts[1]
            }
        }
        return nil
    }

    private static func failure(_ error: any Error, head: HTTPRequestHead) -> Response {
        Log.error("\(head.method) \(head.uri) failed: \(error)")
        return Response(status: .internalServerError, body: "\(error)\n")
    }

    private func path(of uri: String) -> Substring {
        uri.prefix { $0 != "?" }
    }

    private func query(of uri: String) -> Substring? {
        uri.firstIndex(of: "?").map { uri[uri.index(after: $0)...] }
    }
}
