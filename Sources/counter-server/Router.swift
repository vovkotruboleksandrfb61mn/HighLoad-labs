import CounterCore
import NIOCore
import NIOHTTP1

/// A response before it is turned into HTTP parts.
struct Response: Sendable {
    var status: HTTPResponseStatus
    var body: String?

    static let noContent = Response(status: .noContent)
    static let notFound = Response(status: .notFound, body: "not found\n")
}

/// Maps a request to the counter operation it names.
struct Router<Store: CounterStore>: Sendable {
    private enum Route {
        case increment
        case count
        case reset
        case notFound
    }

    let store: Store
    /// The same store, when it can answer on an event loop without a task.
    private let eventLoopStore: (any EventLoopCounterStore)?

    init(store: Store) {
        self.store = store
        self.eventLoopStore = store as? any EventLoopCounterStore
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
        case .notFound:
            return eventLoop.makeSucceededFuture(.notFound)
        }
        return future.recover { Self.failure($0, head: head) }
    }

    private func route(of head: HTTPRequestHead) -> Route {
        switch (head.method, path(of: head.uri)) {
        case (.GET, "/inc"):
            return .increment
        case (.GET, "/count"):
            return .count
        case (.POST, "/reset"):
            return .reset
        default:
            return .notFound
        }
    }

    private static func failure(_ error: any Error, head: HTTPRequestHead) -> Response {
        Log.error("\(head.method) \(head.uri) failed: \(error)")
        return Response(status: .internalServerError, body: "\(error)\n")
    }

    private func path(of uri: String) -> Substring {
        uri.prefix { $0 != "?" }
    }
}
