import NIOCore

/// A request on its way to the multiplexer: the frame (stream id still
/// unset) and the promise its response completes.
struct OutgoingRequest: Sendable {
    var frame: Frame
    var response: EventLoopPromise<Frame>
}

/// Hands out stream ids and routes each response to the promise of the
/// request that carries the same id, so any number of requests can be in
/// flight on one connection at once.
///
/// Everything happens on the channel's event loop, so the state needs no
/// lock. When all ids are taken, further requests wait in a queue and go out
/// as responses free ids. An id is only reused once its response has
/// arrived, even if the caller stopped waiting earlier (timeout): otherwise a
/// late response could complete the wrong request.
final class StreamMultiplexer: ChannelDuplexHandler {
    typealias InboundIn = Frame
    typealias OutboundIn = OutgoingRequest
    typealias OutboundOut = Frame

    /// The v4 limit: stream ids are non-negative int16s.
    static let defaultMaximumStreams = 32_768

    private var pending: [EventLoopPromise<Frame>?]
    private var freeStreams: [Int16]
    private var waiting = CircularBuffer<OutgoingRequest>()
    private var closedWith: (any Error)?

    init(maximumStreams: Int = defaultMaximumStreams) {
        precondition((1...Self.defaultMaximumStreams).contains(maximumStreams))
        pending = Array(repeating: nil, count: maximumStreams)
        // Popped from the end: low ids are used first.
        freeStreams = (0..<maximumStreams).reversed().map { Int16($0) }
    }

    var inFlight: Int { pending.count - freeStreams.count }

    func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
        let request = unwrapOutboundIn(data)
        if let error = closedWith {
            request.response.fail(error)
            promise?.fail(error)
            return
        }
        guard let stream = freeStreams.popLast() else {
            waiting.append(request)
            promise?.succeed()
            return
        }
        send(request, on: stream, context: context, promise: promise)
    }

    private func send(_ request: OutgoingRequest, on stream: Int16, context: ChannelHandlerContext, promise: EventLoopPromise<Void>?) {
        pending[Int(stream)] = request.response
        var frame = request.frame
        frame.stream = stream
        context.write(wrapOutboundOut(frame), promise: promise)
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let frame = unwrapInboundIn(data)
        guard frame.stream >= 0 else {
            // An EVENT; we never REGISTER, so nobody listens.
            return
        }
        let index = Int(frame.stream)
        guard index < pending.count, let promise = pending[index] else {
            closeAll(with: CQLError.protocolViolation("a response on idle stream \(frame.stream)"), context: context)
            return
        }
        pending[index] = nil
        if let next = waiting.popFirst() {
            send(next, on: frame.stream, context: context, promise: nil)
            context.flush()
        } else {
            freeStreams.append(frame.stream)
        }
        promise.succeed(frame)
    }

    func channelInactive(context: ChannelHandlerContext) {
        failAll(with: CQLError.connectionClosed)
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: any Error) {
        closeAll(with: error, context: context)
    }

    private func closeAll(with error: any Error, context: ChannelHandlerContext) {
        failAll(with: error)
        context.close(promise: nil)
    }

    private func failAll(with error: any Error) {
        if closedWith == nil {
            closedWith = error
        }
        for index in pending.indices {
            if let promise = pending[index] {
                pending[index] = nil
                promise.fail(error)
            }
        }
        while let request = waiting.popFirst() {
            request.response.fail(error)
        }
    }
}

@available(*, unavailable)
extension StreamMultiplexer: Sendable {}
