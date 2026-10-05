import CommandLineSupport

/// The outcome of one pg-bench run, printed as a CSV row.
struct BenchResult: Sendable {
    let variant: Variant
    let workers: Int
    let incrementsPerWorker: Int
    let elapsed: Duration
    let finalValue: Int
    /// Increments given up after an error (only `serializable` gives up).
    let errors: Int
    /// Attempts thrown away and started over (40001 or a stale version).
    let retries: Int

    var expected: Int { workers * incrementsPerWorker }
    var lost: Int { expected - finalValue }
    var isExact: Bool { finalValue == expected }

    var seconds: Double { elapsed.inSeconds }

    /// Committed-or-abandoned increments per second.
    var incrementsPerSecond: Double { Double(expected) / seconds }

    static let csvHeader = "variant,workers,increments_per_worker,seconds,ops_per_sec,final_value,expected,lost,errors,retries"

    var csvRow: String {
        [
            variant.rawValue,
            String(workers),
            String(incrementsPerWorker),
            decimalString(seconds, fractionDigits: 4),
            decimalString(incrementsPerSecond, fractionDigits: 1),
            String(finalValue),
            String(expected),
            String(lost),
            String(errors),
            String(retries),
        ].joined(separator: ",")
    }
}
