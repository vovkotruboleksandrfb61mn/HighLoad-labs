import CommandLineSupport

#if canImport(Glibc)
import Glibc
#elseif canImport(Musl)
import Musl
#elseif canImport(Darwin)
import Darwin
#endif

/// One acknowledged increment, for --latency-out.
struct LatencySample: Sendable {
    /// When the server's reply arrived, from the start of the run.
    let completed: Duration
    /// From sending the increment to its reply, retries included.
    let latency: Duration
    /// Times the increment was sent again after a retryable error.
    let retries: Int

    static let csvHeader = "completed_seconds,latency_ms,retries"

    var csvRow: String {
        "\(decimalString(completed.inSeconds, fractionDigits: 6)),\(decimalString(latency.inSeconds * 1000, fractionDigits: 3)),\(retries)"
    }

    struct WriteFailure: Error, CustomStringConvertible {
        let path: String
        let errno: Int32
        var description: String { "cannot write \(path): errno \(errno)" }
    }

    /// Writes `samples` as CSV, a header line first, to the file at `path`.
    static func writeCSV(_ samples: [LatencySample], to path: String) throws {
        var text = csvHeader + "\n"
        text.reserveCapacity(samples.count * 32)
        for sample in samples {
            text += sample.csvRow
            text += "\n"
        }
        let fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0o644)
        guard fd >= 0 else { throw WriteFailure(path: path, errno: errno) }
        defer { close(fd) }
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
}
