import CommandLineSupport
import CounterCore

#if canImport(Glibc)
import Glibc
#elseif canImport(Musl)
import Musl
#elseif canImport(Darwin)
import Darwin
#endif

/// The outcome of one run, printed as a CSV row.
struct BenchResult: Sendable {
    let label: String
    let consistency: CassandraConsistency
    let workers: Int
    let incrementsPerWorker: Int
    /// The disruption that happened ("" when none).
    let disruption: String
    let disruptedAt: Duration?
    let retry: Bool
    let stats: WorkerStats
    /// Read with consistency ALL after the run, every node up.
    let finalValue: Int
    let reader: ReaderStats?
    let method: String
    let driver: String
    /// Failed conditions of lightweight transactions (method lwt).
    let conflicts: Int

    var expected: Int { workers * incrementsPerWorker }

    static let csvHeader = [
        "label", "consistency", "workers", "increments_per_worker", "disruption", "disrupted_at_seconds", "retry",
        "seconds", "ops_per_sec", "final_value", "expected", "acknowledged", "errors", "retries",
        "failures", "applied_unacknowledged", "p50_ms", "p99_ms", "max_ms",
        "reader_reads", "reader_failed", "reader_backwards", "reader_stale", "reader_largest_shortfall",
        "method", "conflicts", "driver",
    ].joined(separator: ",")

    var csvRow: String {
        let sorted = stats.latencies.sorted()
        func percentile(_ p: Double) -> String {
            guard !sorted.isEmpty else { return "" }
            let index = min(sorted.count - 1, Int((p * Double(sorted.count)).rounded(.up)) - 1)
            return decimalString(sorted[max(0, index)].inSeconds * 1000, fractionDigits: 3)
        }
        // Failure kinds as "kind=count" pairs joined by ";", sorted by kind.
        let failures = stats.failures.sorted { $0.key < $1.key }.map { "\($0.key)=\($0.value)" }.joined(separator: ";")
        let run: [String] = [
            label,
            consistency.rawValue.uppercased(),
            String(workers),
            String(incrementsPerWorker),
            disruption,
            disruptedAt.map { decimalString($0.inSeconds, fractionDigits: 3) } ?? "",
            String(retry),
        ]
        let outcome: [String] = [
            decimalString(stats.elapsed.inSeconds, fractionDigits: 4),
            decimalString(Double(stats.acknowledged) / stats.elapsed.inSeconds, fractionDigits: 1),
            String(finalValue),
            String(expected),
            String(stats.acknowledged),
            String(stats.errors),
            String(stats.retries),
            failures,
            // Increments in the final value that no client saw acknowledged
            // (negative: acknowledged increments that are missing).
            String(finalValue - stats.acknowledged),
            percentile(0.5),
            percentile(0.99),
            percentile(1.0),
        ]
        var readerColumns = ["", "", "", "", ""]
        if let reader {
            readerColumns = [reader.reads, reader.failed, reader.backwards, reader.stale, reader.largestShortfall].map { String($0) }
        }
        return (run + outcome + readerColumns + [method, String(conflicts), driver]).joined(separator: ",")
    }
}

struct WriteFailure: Error, CustomStringConvertible {
    let path: String
    let errno: Int32
    var description: String { "cannot write \(path): errno \(errno)" }
}

func writeFile(_ text: String, to path: String) throws {
    let fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0o644)
    guard fd >= 0 else { throw WriteFailure(path: path, errno: errno) }
    defer { close(fd) }
    var text = text
    try text.withUTF8 { bytes in
        var offset = 0
        while offset < bytes.count {
            let written = write(fd, bytes.baseAddress! + offset, bytes.count - offset)
            if written < 0 {
                if errno == EINTR { continue }
                throw WriteFailure(path: path, errno: errno)
            }
            offset += written
        }
    }
}
