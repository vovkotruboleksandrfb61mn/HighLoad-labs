import ArgumentParser
import CounterCore
import NIOCore
import NIOHTTP1
import NIOPosix

/// How connections are served.
enum ServerEngine: String, CaseIterable, Sendable, ExpressibleByArgument {
    /// Each connection is a `NIOAsyncChannel` served by its own task: every
    /// request hops from the event loop to the cooperative pool and back.
    case async
    /// Each connection has a `CounterHTTPHandler` in its pipeline: requests
    /// are answered on the connection's event loop, and stores that conform
    /// to `EventLoopCounterStore` are called without any task.
    case handler
    /// Each connection has a `RawCounterHandler` reading bytes directly: a
    /// minimal parser of our own instead of NIOHTTP1, and ready-made
    /// response bytes.
    case raw
    /// No SwiftNIO: our own threads, each with an io_uring ring (see
    /// `URingServer`). Only for stores that answer immediately, and only when
    /// built with liburing (HLS_IO_URING=1).
    case uring
}

struct EngineUnavailable: Error, CustomStringConvertible {
    let description: String
}

/// The web counter: raw SwiftNIO HTTP/1.1 with keep-alive.
struct HTTPServer<Store: CounterStore>: Sendable {
    typealias Connection = NIOAsyncChannel<HTTPServerRequestPart, HTTPServerResponsePart>

    let host: String
    let port: Int
    let store: Store
    let storeName: String
    var engine: ServerEngine = .async
    var eventLoops: Int = System.coreCount
    /// When set, one event loop per listed CPU, each pinned to its CPU.
    var pinnedCPUs: [Int]? = nil
    /// For the uring engine: let a kernel thread poll the submission queues.
    var submissionPolling = false

    func run() async throws {
        if engine == .uring {
            try await runURing()
            return
        }
        let group: MultiThreadedEventLoopGroup
        if let pinnedCPUs {
            group = MultiThreadedEventLoopGroup(pinnedCPUIDs: pinnedCPUs)
        } else {
            group = MultiThreadedEventLoopGroup(numberOfThreads: eventLoops)
        }
        do {
            switch engine {
            case .async:
                try await serveAsync(on: group)
            case .handler:
                try await serveWithHandler(on: group)
            case .raw:
                try await serveRaw(on: group)
            case .uring:
                preconditionFailure("the uring engine does not use SwiftNIO")
            }
        } catch {
            try? await group.shutdownGracefully()
            throw error
        }
        try await group.shutdownGracefully()
    }

    private func bootstrap(on group: MultiThreadedEventLoopGroup) -> ServerBootstrap {
        ServerBootstrap(group: group)
            .serverChannelOption(.backlog, value: 1024)
            .serverChannelOption(.socketOption(.so_reuseaddr), value: 1)
            .childChannelOption(.tcpOption(.tcp_nodelay), value: 1)
    }

    private func announce(_ channel: any Channel) {
        let address = channel.localAddress.map { "\($0)" } ?? "\(host):\(port)"
        let loops = pinnedCPUs.map { "pinned to CPUs \($0.map(String.init).joined(separator: ","))" } ?? "\(eventLoops)"
        Log.info("store=\(storeName) engine=\(engine.rawValue) event-loops=\(loops) listening on \(address)")
    }

    // MARK: - handler engine

    private func serveWithHandler(on group: MultiThreadedEventLoopGroup) async throws {
        let router = Router(store: store)
        let channel = try await bootstrap(on: group)
            .childChannelInitializer { channel in
                channel.eventLoop.makeCompletedFuture {
                    try channel.pipeline.syncOperations.configureHTTPServerPipeline(withErrorHandling: true)
                    try channel.pipeline.syncOperations.addHandler(CounterHTTPHandler(router: router))
                }
            }
            .bind(host: host, port: port)
            .get()
        announce(channel)
        try await channel.closeFuture.get()
    }

    // MARK: - uring engine

    private func runURing() async throws {
        #if canImport(CLibURing)
        guard let store = store as? any SynchronousCounterStore else {
            throw EngineUnavailable(description: "the uring engine serves only stores that answer immediately (--store memory)")
        }
        let server = URingServer(
            host: host,
            port: port,
            store: store,
            threads: eventLoops,
            pinnedCPUs: pinnedCPUs,
            submissionPolling: submissionPolling
        )
        try await server.run()
        #else
        throw EngineUnavailable(description: "this build has no io_uring engine: build with liburing and HLS_IO_URING=1 (source scripts/env.sh)")
        #endif
    }

    // MARK: - raw engine

    private func serveRaw(on group: MultiThreadedEventLoopGroup) async throws {
        let store = self.store
        let channel = try await bootstrap(on: group)
            .childChannelInitializer { channel in
                channel.eventLoop.makeCompletedFuture {
                    try channel.pipeline.syncOperations.addHandler(RawCounterHandler(store: store))
                }
            }
            .bind(host: host, port: port)
            .get()
        announce(channel)
        try await channel.closeFuture.get()
    }

    // MARK: - async engine

    private func serveAsync(on group: MultiThreadedEventLoopGroup) async throws {
        let server = try await bootstrap(on: group)
            .bind(host: host, port: port) { channel in
                channel.eventLoop.makeCompletedFuture {
                    try channel.pipeline.syncOperations.configureHTTPServerPipeline(withErrorHandling: true)
                    return try Connection(wrappingChannelSynchronously: channel)
                }
            }
        announce(server.channel)

        let router = Router(store: store)
        try await withThrowingDiscardingTaskGroup { connections in
            try await server.executeThenClose { inbound in
                for try await connection in inbound {
                    connections.addTask {
                        await Self.serve(connection, router: router)
                    }
                }
            }
        }
    }

    private static func serve(_ connection: Connection, router: Router<Store>) async {
        do {
            try await connection.executeThenClose { inbound, outbound in
                var pending: HTTPRequestHead?
                for try await part in inbound {
                    switch part {
                    case .head(let head):
                        pending = head
                    case .body:
                        continue
                    case .end:
                        guard let head = pending else { continue }
                        pending = nil
                        let response = await router.respond(to: head)
                        try await outbound.write(contentsOf: ResponseParts.parts(for: response, replyingTo: head))
                        if !head.isKeepAlive {
                            return
                        }
                    }
                }
            }
        } catch {
            // A client resetting its connection is routine under load; there
            // is nothing to answer and nobody to tell.
        }
    }
}

/// Answers each request on the connection's event loop.
///
/// `configureHTTPServerPipeline` puts an `HTTPServerPipelineHandler` in
/// front, which holds back a pipelined request until the response to the
/// previous one has been written, so this handler sees one request at a time.
final class CounterHTTPHandler<Store: CounterStore>: ChannelInboundHandler {
    typealias InboundIn = HTTPServerRequestPart
    typealias OutboundOut = HTTPServerResponsePart

    private let router: Router<Store>
    private var pending: HTTPRequestHead?

    init(router: Router<Store>) {
        self.router = router
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        switch unwrapInboundIn(data) {
        case .head(let head):
            pending = head
        case .body:
            break
        case .end:
            guard let head = pending else { return }
            pending = nil
            router.respond(to: head, on: context.eventLoop)
                .hop(to: context.eventLoop)
                .assumeIsolated()
                .whenSuccess { response in
                    self.write(response, replyingTo: head, context: context)
                }
        }
    }

    private func write(_ response: Response, replyingTo head: HTTPRequestHead, context: ChannelHandlerContext) {
        let parts = ResponseParts.parts(for: response, replyingTo: head)
        for part in parts.dropLast() {
            context.write(wrapOutboundOut(part), promise: nil)
        }
        if head.isKeepAlive {
            context.writeAndFlush(wrapOutboundOut(.end(nil)), promise: nil)
        } else {
            context.writeAndFlush(wrapOutboundOut(.end(nil))).assumeIsolated().whenComplete { _ in
                context.close(promise: nil)
            }
        }
    }
}

@available(*, unavailable)
extension CounterHTTPHandler: Sendable {}

enum ResponseParts {
    /// The head of the most common response, 204 to a keep-alive HTTP/1.1
    /// request, built once instead of per request.
    private static let noContentKeepAlive = HTTPResponseHead(version: .http1_1, status: .noContent)

    static func parts(for response: Response, replyingTo request: HTTPRequestHead) -> [HTTPServerResponsePart] {
        if response.body == nil, response.status == .noContent, request.version == .http1_1, request.isKeepAlive {
            return [.head(noContentKeepAlive), .end(nil)]
        }

        var headers = HTTPHeaders()
        let body = response.body.map { ByteBuffer(string: $0) }
        if let body {
            headers.add(name: "content-type", value: "text/plain; charset=utf-8")
            headers.add(name: "content-length", value: String(body.readableBytes))
        } else if response.status != .noContent {
            headers.add(name: "content-length", value: "0")
        }
        if !request.isKeepAlive {
            headers.add(name: "connection", value: "close")
        } else if request.version == .http1_0 {
            headers.add(name: "connection", value: "keep-alive")
        }

        let head = HTTPResponseHead(version: request.version, status: response.status, headers: headers)
        var parts: [HTTPServerResponsePart] = [.head(head)]
        if let body {
            parts.append(.body(.byteBuffer(body)))
        }
        parts.append(.end(nil))
        return parts
    }
}
