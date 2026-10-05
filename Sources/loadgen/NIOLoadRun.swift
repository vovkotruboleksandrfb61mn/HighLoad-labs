import NIOCore
import NIOHTTP1
import NIOPosix

/// The timed part of a run on raw SwiftNIO instead of AsyncHTTPClient.
///
/// The load is the same: `clients` TCP connections, each with keep-alive,
/// each sending its `requests` GET /inc one at a time, the next request only
/// after the full response to the previous one has arrived. What is gone is
/// the client library's per-request machinery (request objects, the
/// connection pool, async sequences for the body and a task hop per
/// request): each connection is driven by a channel handler on its event
/// loop.
struct NIOLoadRun: Sendable {
    let host: String
    let port: Int
    let clients: Int
    let requests: Int
    let eventLoops: Int

    /// Opens every connection and makes one GET /count on each, then sends
    /// the GET /inc requests from all of them at once and returns how long
    /// that took.
    func timedIncrements() async throws -> Duration {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: eventLoops)
        do {
            let elapsed = try await run(on: group)
            try await group.shutdownGracefully()
            return elapsed
        } catch {
            try? await group.shutdownGracefully()
            throw error
        }
    }

    private func run(on group: MultiThreadedEventLoopGroup) async throws -> Duration {
        let hostHeader = "\(host):\(port)"
        var channels: [any Channel] = []
        defer {
            for channel in channels {
                channel.close(promise: nil)
            }
        }
        for _ in 0..<clients {
            let channel = try await ClientBootstrap(group: group)
                .channelOption(.tcpOption(.tcp_nodelay), value: 1)
                .channelInitializer { channel in
                    channel.eventLoop.makeCompletedFuture {
                        try channel.pipeline.syncOperations.addHTTPClientHandlers()
                        try channel.pipeline.syncOperations.addHandler(SequentialRequester(host: hostHeader))
                    }
                }
                .connect(host: host, port: port)
                .get()
            channels.append(channel)
        }

        // Warm-up: one GET /count per connection, outside the clock.
        try await EventLoopFuture.andAllSucceed(
            channels.map { Self.send(path: "/count", count: 1, expecting: .ok, on: $0) },
            on: group.any()
        ).get()

        let clock = ContinuousClock()
        let start = clock.now
        let runs = channels.map { Self.send(path: "/inc", count: requests, expecting: .noContent, on: $0) }
        try await EventLoopFuture.andAllSucceed(runs, on: group.any()).get()
        return clock.now - start
    }

    private static func send(
        path: String,
        count: Int,
        expecting status: HTTPResponseStatus,
        on channel: any Channel
    ) -> EventLoopFuture<Void> {
        let promise = channel.eventLoop.makePromise(of: Void.self)
        channel.eventLoop.execute {
            do {
                let requester = try channel.pipeline.syncOperations.handler(type: SequentialRequester.self)
                requester.start(path: path, count: count, expecting: status, promise: promise)
            } catch {
                promise.fail(error)
            }
        }
        return promise.futureResult
    }
}

/// Sends `count` requests over its connection, strictly one after another:
/// the next request is written only when the previous response has ended.
final class SequentialRequester: ChannelDuplexHandler {
    typealias InboundIn = HTTPClientResponsePart
    typealias OutboundIn = Never
    typealias OutboundOut = HTTPClientRequestPart

    private struct Job {
        var head: HTTPRequestHead
        var remaining: Int
        var expecting: HTTPResponseStatus
        var promise: EventLoopPromise<Void>
    }

    private let host: String
    private var job: Job?
    private var context: ChannelHandlerContext?
    private var lastStatus: HTTPResponseStatus?

    init(host: String) {
        self.host = host
    }

    func handlerAdded(context: ChannelHandlerContext) {
        self.context = context
    }

    func handlerRemoved(context: ChannelHandlerContext) {
        self.context = nil
    }

    func start(path: String, count: Int, expecting: HTTPResponseStatus, promise: EventLoopPromise<Void>) {
        guard job == nil, let context else {
            promise.fail(LoadgenError(description: "connection busy or closed"))
            return
        }
        var headers = HTTPHeaders()
        headers.add(name: "host", value: host)
        let head = HTTPRequestHead(version: .http1_1, method: .GET, uri: path, headers: headers)
        job = Job(head: head, remaining: count, expecting: expecting, promise: promise)
        sendNext(context: context)
    }

    private func sendNext(context: ChannelHandlerContext) {
        guard let job else { return }
        context.write(wrapOutboundOut(.head(job.head)), promise: nil)
        context.writeAndFlush(wrapOutboundOut(.end(nil)), promise: nil)
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        switch unwrapInboundIn(data) {
        case .head(let head):
            lastStatus = head.status
        case .body:
            break
        case .end:
            guard var job else { return }
            guard lastStatus == job.expecting else {
                self.job = nil
                job.promise.fail(
                    LoadgenError(
                        description: "GET \(job.head.uri) returned \(lastStatus?.code ?? 0), expected \(job.expecting.code)"
                    )
                )
                return
            }
            job.remaining -= 1
            if job.remaining == 0 {
                self.job = nil
                job.promise.succeed()
            } else {
                self.job = job
                sendNext(context: context)
            }
        }
    }

    func channelInactive(context: ChannelHandlerContext) {
        fail(LoadgenError(description: "the server closed the connection"))
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: any Error) {
        fail(error)
        context.close(promise: nil)
    }

    private func fail(_ error: any Error) {
        if let job {
            self.job = nil
            job.promise.fail(error)
        }
    }
}

@available(*, unavailable)
extension SequentialRequester: Sendable {}
