import CommandLineSupport

/// The outcome of one hz-bench run, printed as a CSV row.
struct BenchResult: Sendable {
    let variant: Variant
    let tasks: Int
    let incrementsPerTask: Int
    let elapsed: Duration
    let finalValue: Int64
    let retries: Int

    var expected: Int64 { Int64(tasks) * Int64(incrementsPerTask) }
    var lostUpdates: Int64 { expected - finalValue }
    var isCorrect: Bool { finalValue == expected }

    var seconds: Double { elapsed.inSeconds }

    var incrementsPerSecond: Double { Double(expected) / seconds }

    static let csvHeader =
        "variant,tasks,increments_per_task,seconds,increments_per_second,final_value,expected,lost_updates,retries"

    var csvRow: String {
        [
            variant.rawValue,
            String(tasks),
            String(incrementsPerTask),
            decimalString(seconds, fractionDigits: 4),
            decimalString(incrementsPerSecond, fractionDigits: 1),
            String(finalValue),
            String(expected),
            String(lostUpdates),
            String(retries),
        ].joined(separator: ",")
    }
}
