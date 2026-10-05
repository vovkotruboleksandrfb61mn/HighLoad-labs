import NIOCore

/// Walks the frames of a message while decoding it, like the Java
/// `ClientMessage.ForwardFrameIterator`.
struct FrameIterator {
    private let frames: [Frame]
    private var index = 0

    init(frames: [Frame]) {
        self.frames = frames
    }

    var hasNext: Bool { index < frames.count }

    func peek() -> Frame? {
        hasNext ? frames[index] : nil
    }

    mutating func next() throws(HazelcastError) -> Frame {
        guard hasNext else {
            throw .protocolViolation("message ended after \(frames.count) frames")
        }
        defer { index += 1 }
        return frames[index]
    }

    /// Skips the initial frame of a response, returning it for its fixed-size fields.
    mutating func initialFrame() throws(HazelcastError) -> Frame {
        try next()
    }

    /// Consumes the next frame if it is a null frame.
    mutating func nextIsNull() -> Bool {
        guard let frame = peek(), frame.isNull else {
            return false
        }
        index += 1
        return true
    }

    mutating func string() throws(HazelcastError) -> String {
        let frame = try next()
        return String(buffer: frame.content)
    }

    mutating func nullableString() throws(HazelcastError) -> String? {
        nextIsNull() ? nil : try string()
    }

    mutating func data() throws(HazelcastError) -> HazelcastData {
        let frame = try next()
        return HazelcastData(bytes: Array(frame.content.readableBytesView))
    }

    mutating func nullableData() throws(HazelcastError) -> HazelcastData? {
        nextIsNull() ? nil : try data()
    }

    /// Consumes the begin frame of a data structure.
    mutating func beginDataStructure() throws(HazelcastError) {
        let frame = try next()
        guard frame.isBeginDataStructure else {
            throw .protocolViolation("expected the begin frame of a data structure, got flags \(frame.flags.rawValue)")
        }
    }

    /// Skips everything up to and including the end frame of the data
    /// structure whose begin frame was already consumed, so fields added to
    /// the structure by newer servers are ignored.
    mutating func fastForwardToEndFrame() throws(HazelcastError) {
        var expectedEndFrames = 1
        while expectedEndFrames > 0 {
            let frame = try next()
            if frame.isEndDataStructure {
                expectedEndFrames -= 1
            } else if frame.isBeginDataStructure {
                expectedEndFrames += 1
            }
        }
    }

    /// True if the next frame closes the current data structure.
    var nextIsEndDataStructure: Bool {
        peek()?.isEndDataStructure ?? false
    }

    /// A list of multi-frame items: begin frame, items, end frame.
    mutating func list<Item>(
        _ decodeItem: (inout FrameIterator) throws(HazelcastError) -> Item
    ) throws(HazelcastError) -> [Item] {
        try beginDataStructure()
        var items: [Item] = []
        while !nextIsEndDataStructure {
            items.append(try decodeItem(&self))
        }
        _ = try next()
        return items
    }
}
