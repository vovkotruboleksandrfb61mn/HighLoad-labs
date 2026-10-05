// Helpers shared by the executables. They avoid Foundation, so the tools stay
// small and their output does not depend on the locale.

#if canImport(Glibc)
import Glibc
#elseif canImport(Musl)
import Musl
#elseif canImport(Darwin)
import Darwin
#endif

/// Writes `message` and a newline to standard error with a single unbuffered
/// `write`, so standard output stays free for the CSV rows.
package func printToStandardError(_ message: String) {
    var line = message + "\n"
    line.withUTF8 { bytes in
        _ = write(STDERR_FILENO, bytes.baseAddress, bytes.count)
    }
}

extension Duration {
    /// The duration in seconds.
    package var inSeconds: Double {
        let (seconds, attoseconds) = components
        return Double(seconds) + Double(attoseconds) / 1e18
    }
}

/// `value` rounded to `fractionDigits` decimal places, e.g. `1.5000` for
/// `(1.49996, 4)`. `value` must be finite and non-negative.
package func decimalString(_ value: Double, fractionDigits: Int) -> String {
    var scale = 1
    for _ in 0..<fractionDigits {
        scale *= 10
    }
    let scaled = Int((value * Double(scale)).rounded())
    let fraction = String(scaled % scale)
    let padding = String(repeating: "0", count: fractionDigits - fraction.count)
    return "\(scaled / scale).\(padding)\(fraction)"
}
