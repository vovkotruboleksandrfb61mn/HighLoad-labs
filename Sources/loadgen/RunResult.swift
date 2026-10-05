import CommandLineSupport

/// The outcome of one load run, printed as a CSV row.
struct RunResult: Sendable {
    let clients: Int
    let requestsPerClient: Int
    let elapsed: Duration
    let finalValue: Int

    var totalRequests: Int { clients * requestsPerClient }
    var expected: Int { totalRequests }
    var isCorrect: Bool { finalValue == expected }

    var seconds: Double { elapsed.inSeconds }

    var requestsPerSecond: Double { Double(totalRequests) / seconds }

    static let csvHeader = "store,clients,requests_per_client,total_requests,seconds,rps,final_value,expected,correct"

    func csvRow(store: String) -> String {
        [
            store,
            String(clients),
            String(requestsPerClient),
            String(totalRequests),
            decimalString(seconds, fractionDigits: 4),
            decimalString(requestsPerSecond, fractionDigits: 1),
            String(finalValue),
            String(expected),
            String(isCorrect),
        ].joined(separator: ",")
    }
}
