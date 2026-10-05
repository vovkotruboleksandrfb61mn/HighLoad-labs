/// Error responses (message type 0): after the initial frame, a list of
/// `ErrorHolder`s, the cause chain with the outermost exception first.
///
/// `ErrorHolder`: begin frame, a frame with `errorCode` (int), `className`,
/// `message?`, `stackTraceElements` (List<StackTraceElement>), end frame.
enum ErrorsCodec {
    static let messageType = ClientProtocol.exceptionMessageType

    static func decode(_ message: ClientMessage) throws(HazelcastError) -> ServerError {
        var iterator = message.makeFrameIterator()
        _ = try iterator.initialFrame()
        let errors = try iterator.list { (iterator: inout FrameIterator) throws(HazelcastError) in
            try decodeErrorHolder(&iterator)
        }
        guard let first = errors.first else {
            throw .protocolViolation("error response without an error")
        }
        return first
    }

    private static func decodeErrorHolder(_ iterator: inout FrameIterator) throws(HazelcastError) -> ServerError {
        try iterator.beginDataStructure()
        let errorCode = try iterator.next().int32(at: 0)
        let className = try iterator.string()
        let message = try iterator.nullableString()
        // The stack trace and anything newer servers add.
        try iterator.fastForwardToEndFrame()
        return ServerError(errorCode: errorCode, className: className, message: message)
    }
}
