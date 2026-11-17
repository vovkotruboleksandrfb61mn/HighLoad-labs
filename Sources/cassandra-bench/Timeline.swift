import CommandLineSupport
import Synchronization

/// What happened when during a run: acknowledged and failed increments per
/// 100 ms, the disruption's marks, and the acknowledged total that the
/// reader compares its reads with.
final class Timeline: Sendable {
    static let bucket = Duration.milliseconds(100)

    private struct State {
        var acknowledged: [Int] = []
        var failed: [Int] = []
        var marks: [(name: String, at: Duration)] = []
    }

    let start: ContinuousClock.Instant
    private let state = Mutex(State())
    /// Increments acknowledged so far, all workers together.
    let acknowledgedTotal = Atomic<Int>(0)
    /// Set when the workers are done; stops the reader.
    let finished = Atomic<Bool>(false)

    init(start: ContinuousClock.Instant) {
        self.start = start
    }

    private func index(_ instant: ContinuousClock.Instant) -> Int {
        max(0, Int(start.duration(to: instant) / Self.bucket))
    }

    func acknowledged(at instant: ContinuousClock.Instant) {
        acknowledgedTotal.add(1, ordering: .relaxed)
        let i = index(instant)
        state.withLock { state in
            while state.acknowledged.count <= i {
                state.acknowledged.append(0)
                state.failed.append(0)
            }
            state.acknowledged[i] += 1
        }
    }

    func failed(at instant: ContinuousClock.Instant) {
        let i = index(instant)
        state.withLock { state in
            while state.failed.count <= i {
                state.acknowledged.append(0)
                state.failed.append(0)
            }
            state.failed[i] += 1
        }
    }

    func mark(_ name: String) {
        let at = start.duration(to: .now)
        state.withLock { $0.marks.append((name, at)) }
    }

    /// `seconds,acknowledged,failed` per 100 ms, then `# mark,seconds` lines.
    var csv: String {
        state.withLock { state in
            var text = "seconds,acknowledged,failed\n"
            for i in state.acknowledged.indices {
                text += "\(decimalString(Double(i) * Self.bucket.inSeconds, fractionDigits: 1)),\(state.acknowledged[i]),\(state.failed[i])\n"
            }
            for mark in state.marks {
                text += "# \(mark.name),\(decimalString(mark.at.inSeconds, fractionDigits: 3))\n"
            }
            return text
        }
    }
}

/// Counts of the background reader: how often a read returned less than
/// an earlier read (the counter went backwards), and less than the number
/// of increments already acknowledged when the read was sent (stale).
struct ReaderStats: Sendable {
    var reads = 0
    var failed = 0
    var backwards = 0
    var stale = 0
    /// The largest shortfall of a stale read.
    var largestShortfall = 0
}
