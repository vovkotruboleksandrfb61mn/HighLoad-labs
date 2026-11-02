import CommandLineSupport
import CounterCore

/// The outcome of one mongo-bench run, printed as a CSV row.
struct BenchResult: Sendable {
    let variant: MongoWriteConcern
    /// Whether the increments carried lsid and txnNumber.
    let retryableWrites: Bool
    let workers: Int
    let incrementsPerWorker: Int
    /// The primary that was killed during the run (node number, 1-based), if any.
    let killedNode: Int?
    /// When the kill was done, from the start of the run.
    let killedAfter: Duration?
    /// From the kill to the first increment acknowledged afterwards.
    let failover: Duration?
    let elapsed: Duration
    /// Read with `readConcern: "majority"` once the killed node is back and
    /// has caught up.
    let finalValue: Int
    /// Increments the server acknowledged.
    let acknowledged: Int
    /// Times an operation was sent again after a retryable error.
    let retries: Int
    /// Increments given up on (not retryable, or retried for too long).
    let errors: Int
    /// Replica set terms that started during the run.
    let elections: Int
    /// The slowest single increment, retries included.
    let longestOperation: Duration

    var expected: Int { workers * incrementsPerWorker }
    /// Negative when increments were counted twice.
    var lost: Int { expected - finalValue }
    /// Acknowledged increments missing from the final value.
    var lostAcknowledged: Int { acknowledged - finalValue }
    var isExact: Bool { finalValue == expected }

    var seconds: Double { elapsed.inSeconds }
    var incrementsPerSecond: Double { Double(acknowledged) / seconds }

    static let csvHeader = [
        "variant", "retryable_writes", "workers", "increments_per_worker", "kill", "killed_node", "kill_at_seconds", "failover_seconds",
        "seconds", "ops_per_sec", "final_value", "expected", "acknowledged", "lost", "lost_acknowledged",
        "retries", "errors", "elections", "longest_op_seconds",
    ].joined(separator: ",")

    var csvRow: String {
        [
            variant.rawValue,
            String(retryableWrites),
            String(workers),
            String(incrementsPerWorker),
            killedNode == nil ? "false" : "true",
            killedNode.map { String($0) } ?? "",
            killedAfter.map { decimalString($0.inSeconds, fractionDigits: 3) } ?? "",
            failover.map { decimalString($0.inSeconds, fractionDigits: 3) } ?? "",
            decimalString(seconds, fractionDigits: 4),
            decimalString(incrementsPerSecond, fractionDigits: 1),
            String(finalValue),
            String(expected),
            String(acknowledged),
            String(lost),
            String(lostAcknowledged),
            String(retries),
            String(errors),
            String(elections),
            decimalString(longestOperation.inSeconds, fractionDigits: 3),
        ].joined(separator: ",")
    }
}
