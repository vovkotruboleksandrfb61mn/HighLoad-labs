import CounterCore
import NIOCore

/// Serves the counter straight from bytes: `RawRequestParser` reads each
/// request out of the inbound `ByteBuffer`, and the answer is written as
/// ready-made bytes.
///
/// There is no HTTP codec, no `HTTPRequestHead`, no header dictionary and no
/// boxing of HTTP parts into `NIOAny`. Stores that answer immediately
/// (`SynchronousCounterStore`) are called inline, so a request costs no
/// future and no allocation. Other stores are awaited one request at a time,
/// keeping responses in request order.
final class RawCounterHandler<Store: CounterStore>: ChannelInboundHandler {
    typealias InboundIn = ByteBuffer
    typealias OutboundOut = ByteBuffer

    private enum Backend {
        case synchronous(any SynchronousCounterStore)
        case eventLoop(any EventLoopCounterStore)
        case task(Store)
    }

    private let backend: Backend
    /// Received bytes not yet answered: an incomplete request, or requests
    /// queued behind one that is still waiting on the store.
    private var unread: ByteBuffer?
    /// A store operation is in flight; later requests wait for it.
    private var waiting = false
    /// The 204 response, built once per connection so that event loops do
    /// not all retain and release one shared buffer.
    private var noContent: ByteBuffer?

    init(store: Store) {
        if let store = store as? any SynchronousCounterStore {
            backend = .synchronous(store)
        } else if let store = store as? any EventLoopCounterStore {
            backend = .eventLoop(store)
        } else {
            backend = .task(store)
        }
    }

    func handlerAdded(context: ChannelHandlerContext) {
        noContent = context.channel.allocator.buffer(string: "HTTP/1.1 204 No Content\r\n\r\n")
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let bytes = unwrapInboundIn(data)
        if var buffered = unread.take() {
            buffered.writeImmutableBuffer(bytes)
            unread = buffered
        } else {
            unread = bytes
        }
        answerReadyRequests(context: context)
    }

    func errorCaught(context: ChannelHandlerContext, error: any Error) {
        context.close(promise: nil)
    }

    private func answerReadyRequests(context: ChannelHandlerContext) {
        var wrote = false
        defer { if wrote { context.flush() } }

        while !waiting, var buffer = unread.take(), buffer.readableBytes > 0 {
            let outcome = buffer.withUnsafeReadableBytes { RawRequestParser.parse($0) }
            guard case .request(let request) = outcome else {
                if outcome == .incomplete {
                    unread = buffer
                } else {
                    context.writeAndFlush(wrapOutboundOut(Self.badRequest(context))).assumeIsolated().whenComplete { _ in
                        context.close(promise: nil)
                    }
                }
                return
            }
            buffer.moveReaderIndex(forwardBy: request.length)
            if buffer.readableBytes > 0 {
                unread = buffer
            }

            switch backend {
            case .synchronous(let store):
                let response = respond(to: request, store: store, context: context)
                guard request.keepAlive else {
                    wrote = false
                    writeAndClose(response, context: context)
                    return
                }
                context.write(wrapOutboundOut(response), promise: nil)
                wrote = true
            case .eventLoop(let store):
                waiting = true
                respondWhenDone(to: future(for: request.route, store: store, on: context.eventLoop), request: request, context: context)
            case .task(let store):
                waiting = true
                let route = request.route
                let future = context.eventLoop.makeFutureWithTask { try await Self.perform(route, on: store) }
                respondWhenDone(to: future, request: request, context: context)
            }
        }
    }

    // MARK: - Store calls

    /// The result of a store operation: the counter's value for `/count`.
    private enum Outcome: Sendable {
        case done
        case value(Int)
        case notFound
    }

    private func respond(to request: RawRequest, store: any SynchronousCounterStore, context: ChannelHandlerContext) -> ByteBuffer {
        switch request.route {
        case .increment:
            store.incrementNow()
            return response(for: .done, request: request, context: context)
        case .count:
            return response(for: .value(store.valueNow()), request: request, context: context)
        case .reset:
            store.resetNow()
            return response(for: .done, request: request, context: context)
        case .notFound:
            return response(for: .notFound, request: request, context: context)
        }
    }

    private func future(for route: RawRoute, store: any EventLoopCounterStore, on eventLoop: any EventLoop) -> EventLoopFuture<Outcome> {
        switch route {
        case .increment: store.increment(on: eventLoop).map { .done }
        case .count: store.value(on: eventLoop).map { .value($0) }
        case .reset: store.reset(on: eventLoop).map { .done }
        case .notFound: eventLoop.makeSucceededFuture(.notFound)
        }
    }

    private static func perform(_ route: RawRoute, on store: Store) async throws -> Outcome {
        switch route {
        case .increment:
            try await store.increment()
            return .done
        case .count:
            return .value(try await store.value())
        case .reset:
            try await store.reset()
            return .done
        case .notFound:
            return .notFound
        }
    }

    private func respondWhenDone(to future: EventLoopFuture<Outcome>, request: RawRequest, context: ChannelHandlerContext) {
        future.hop(to: context.eventLoop).assumeIsolated().whenComplete { result in
            self.waiting = false
            let response: ByteBuffer
            switch result {
            case .success(let outcome):
                response = self.response(for: outcome, request: request, context: context)
            case .failure(let error):
                Log.error("\(request.route) failed: \(error)")
                response = Self.serverError(error, request: request, context: context)
            }
            guard request.keepAlive else {
                self.writeAndClose(response, context: context)
                return
            }
            context.writeAndFlush(self.wrapOutboundOut(response), promise: nil)
            self.answerReadyRequests(context: context)
        }
    }

    // MARK: - Responses

    private func response(for outcome: Outcome, request: RawRequest, context: ChannelHandlerContext) -> ByteBuffer {
        if case .done = outcome, request.keepAlive, !request.isHTTP10, let noContent {
            return noContent
        }
        let status: String
        let body: String?
        switch outcome {
        case .done: (status, body) = ("204 No Content", nil)
        case .value(let value): (status, body) = ("200 OK", "\(value)\n")
        case .notFound: (status, body) = ("404 Not Found", "not found\n")
        }
        return Self.build(status: status, body: body, request: request, context: context)
    }

    private static func serverError(_ error: any Error, request: RawRequest, context: ChannelHandlerContext) -> ByteBuffer {
        build(status: "500 Internal Server Error", body: "\(error)\n", request: request, context: context)
    }

    private static func badRequest(_ context: ChannelHandlerContext) -> ByteBuffer {
        context.channel.allocator.buffer(string: "HTTP/1.1 400 Bad Request\r\ncontent-length: 0\r\nconnection: close\r\n\r\n")
    }

    private static func build(status: String, body: String?, request: RawRequest, context: ChannelHandlerContext) -> ByteBuffer {
        var head = "HTTP/1.1 \(status)\r\n"
        if let body {
            head += "content-type: text/plain; charset=utf-8\r\ncontent-length: \(body.utf8.count)\r\n"
        } else if !status.hasPrefix("204") {
            head += "content-length: 0\r\n"
        }
        if !request.keepAlive {
            head += "connection: close\r\n"
        } else if request.isHTTP10 {
            head += "connection: keep-alive\r\n"
        }
        head += "\r\n"
        var buffer = context.channel.allocator.buffer(capacity: head.utf8.count + (body?.utf8.count ?? 0))
        buffer.writeString(head)
        if let body {
            buffer.writeString(body)
        }
        return buffer
    }

    private func writeAndClose(_ response: ByteBuffer, context: ChannelHandlerContext) {
        unread = nil
        context.writeAndFlush(wrapOutboundOut(response)).assumeIsolated().whenComplete { _ in
            context.close(promise: nil)
        }
    }
}

@available(*, unavailable)
extension RawCounterHandler: Sendable {}
