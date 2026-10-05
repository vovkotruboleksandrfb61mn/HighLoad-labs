import NIOCore

/// Sends the `CP2` protocol header as soon as the connection is up, before
/// anything else can be written.
final class ProtocolHeaderHandler: ChannelInboundHandler {
    typealias InboundIn = ByteBuffer
    typealias OutboundOut = ByteBuffer

    func channelActive(context: ChannelHandlerContext) {
        let header = context.channel.allocator.buffer(bytes: ClientProtocol.initialBytes)
        context.writeAndFlush(wrapOutboundOut(header), promise: nil)
        context.fireChannelActive()
    }
}

@available(*, unavailable)
extension ProtocolHeaderHandler: Sendable {}

/// The end of the pipeline: hands every response to the request waiting for
/// its correlation id, pings the member when the connection has been idle,
/// and fails all pending requests when the connection goes away.
final class InvocationHandler: ChannelInboundHandler {
    typealias InboundIn = ClientMessage
    typealias OutboundOut = ClientMessage

    private let registry: InvocationRegistry

    init(registry: InvocationRegistry) {
        self.registry = registry
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let message = unwrapInboundIn(data)
        if message.isEvent {
            // We never register listeners, so there is nobody to tell.
            return
        }
        guard message.isUnfragmented else {
            // Members only fragment messages far larger than anything this
            // client asks for.
            let error = HazelcastError.protocolViolation("fragmented messages are not supported")
            registry.close(with: error)
            context.close(promise: nil)
            return
        }
        registry.complete(message.correlationID, with: message)
    }

    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        if let idle = event as? IdleStateHandler.IdleStateEvent, idle == .write {
            var ping = ClientPingCodec.encodeRequest()
            ping.correlationID = registry.makeCorrelationID()
            // Nobody waits for the response; `complete` drops it.
            context.writeAndFlush(wrapOutboundOut(ping), promise: nil)
            return
        }
        context.fireUserInboundEventTriggered(event)
    }

    func channelInactive(context: ChannelHandlerContext) {
        registry.close(with: HazelcastError.connectionClosed)
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: any Error) {
        registry.close(with: error)
        context.close(promise: nil)
    }
}

@available(*, unavailable)
extension InvocationHandler: Sendable {}
