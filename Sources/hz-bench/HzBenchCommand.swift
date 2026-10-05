import ArgumentParser
import CommandLineSupport
import HazelcastClient

@main
struct HzBenchCommand: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "hz-bench",
        abstract: "Increments one Hazelcast counter from concurrent tasks and prints one CSV row.",
        discussion: """
            Resets the counter to 0, then runs --tasks tasks that each add one \
            --increments times, all over one client connection. Every task acts \
            as its own thread id, so IMap locks behave as for Java threads. \
            Exits with status 2 if a variant that must not lose updates did.
            """
    )

    @Option(help: "How to increment: \(Variant.allValueStrings.joined(separator: "|")).")
    var variant: Variant

    @Option(help: "Number of concurrent tasks.")
    var tasks = 10

    @Option(help: "Increments per task.")
    var increments = 10_000

    @Option(help: "Hazelcast member host:port.")
    var address = "127.0.0.1:5701"

    @Option(name: .customLong("cluster-name"), help: "Hazelcast cluster name.")
    var clusterName = "dev"

    @Flag(help: "Print the CSV header line before the row.")
    var header = false

    func validate() throws {
        guard tasks > 0 else { throw ValidationError("--tasks must be positive") }
        guard increments > 0 else { throw ValidationError("--increments must be positive") }
    }

    func run() async throws {
        let memberAddress = try HazelcastClient.Address(parsing: address)
        let result = try await HazelcastClient.withClient(
            to: memberAddress,
            clusterName: clusterName,
            clientName: "hz-bench"
        ) { client in
            try await measure(client)
        }

        if header {
            print(BenchResult.csvHeader)
        }
        print(result.csvRow)
        if variant.mustBeExact && !result.isCorrect {
            printToStandardError("hz-bench: \(variant.rawValue) expected \(result.expected), got \(result.finalValue)")
            throw ExitCode(2)
        }
    }

    private func measure(_ client: HazelcastClient) async throws -> BenchResult {
        let counter = try await Counter.open(for: variant, client: client)
        try await counter.reset()

        let variant = self.variant
        let increments = self.increments
        let clock = ContinuousClock()
        let start = clock.now
        let retries = try await withThrowingTaskGroup(of: Int.self) { group in
            for task in 1...tasks {
                // Thread ids 1...tasks; 0 is the setup thread.
                let threadID = Int64(task)
                group.addTask {
                    var retries = 0
                    for _ in 0..<increments {
                        retries += try await counter.increment(variant, threadID: threadID)
                    }
                    return retries
                }
            }
            return try await group.reduce(0, +)
        }
        let elapsed = start.duration(to: clock.now)

        return BenchResult(
            variant: variant,
            tasks: tasks,
            incrementsPerTask: increments,
            elapsed: elapsed,
            finalValue: try await counter.value(),
            retries: retries
        )
    }
}
