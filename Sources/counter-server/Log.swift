import CommandLineSupport

/// Minimal logging to standard error, so standard output stays free.
enum Log {
    static func info(_ message: String) {
        printToStandardError("counter-server: \(message)")
    }

    static func error(_ message: String) {
        printToStandardError("counter-server: error: \(message)")
    }
}
