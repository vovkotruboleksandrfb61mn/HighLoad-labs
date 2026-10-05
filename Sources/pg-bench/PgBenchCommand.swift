import ArgumentParser
import CommandLineSupport
import CounterCore

@main
struct PgBenchCommand: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "pg-bench",
        abstract: "Increments user_counter from concurrent PostgreSQL connections and prints one CSV row.",
        discussion: """
            Resets the row to (1, 0, 0), opens one connection per worker, then \
            times workers x increments, each in its own BEGIN ... COMMIT. \
            Exits with status 2 if a variant that must not lose updates did.
            """
    )

    @Option(help: "How to increment: \(Variant.allValueStrings.joined(separator: "|")).")
    var variant: Variant

    @Option(help: "Concurrent workers, each with its own connection.")
    var workers = 10

    @Option(help: "Increments per worker.")
    var increments = 10_000

    @Option(name: .customLong("pg-url"), help: "PostgreSQL URL.")
    var pgURL = "postgres://postgres@127.0.0.1:21410/postgres"

    @Flag(help: "Print the CSV header line before the row.")
    var header = false

    func validate() throws {
        guard workers > 0 else { throw ValidationError("--workers must be positive") }
        guard increments > 0 else { throw ValidationError("--increments must be positive") }
        do {
            _ = try PostgresEndpoint(url: pgURL)
        } catch {
            throw ValidationError("\(error)")
        }
    }

    func run() async throws {
        let bench = Bench(variant: variant, endpoint: try PostgresEndpoint(url: pgURL), workers: workers, increments: increments)
        let result = try await bench.run()

        if header {
            print(BenchResult.csvHeader)
        }
        print(result.csvRow)
        if variant.mustBeExact && !result.isExact {
            printToStandardError("pg-bench: \(variant.rawValue) expected \(result.expected), counter is \(result.finalValue)")
            throw ExitCode(2)
        }
    }
}
