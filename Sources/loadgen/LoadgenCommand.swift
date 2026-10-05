import ArgumentParser
import CommandLineSupport
import AsyncHTTPClient
import NIOCore

@main
struct LoadgenCommand: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "loadgen",
        abstract: "Drives N concurrent keep-alive clients against the counter and prints one CSV row.",
        discussion: """
            Calls POST /reset, lets every client open its connection, starts all \
            clients together, times N x requests GET /inc, then checks that \
            GET /count equals N x requests. Exits with status 2 if it does not.
            """
    )

    @Option(help: "Number of concurrent clients, each with its own keep-alive connection.")
    var clients = 1

    @Option(help: "Requests per client.")
    var requests = 10_000

    @Option(help: "Base URL of the counter server.")
    var url = "http://127.0.0.1:21400"

    @Option(name: .customLong("store-label"), help: "Value of the CSV 'store' column.")
    var storeLabel = "unknown"

    @Option(help: "HTTP client for the timed requests: ahc (AsyncHTTPClient) or nio (a channel handler per connection). Both send each client's requests one at a time over its own keep-alive connection.")
    var engine: LoadEngine = .ahc

    @Option(name: .customLong("event-loops"), help: "Event loop threads for --engine nio (default: one per core).")
    var eventLoops = System.coreCount

    @Flag(help: "Print the CSV header line before the row.")
    var header = false

    func validate() throws {
        guard clients > 0 else { throw ValidationError("--clients must be positive") }
        guard requests > 0 else { throw ValidationError("--requests must be positive") }
        guard eventLoops > 0 else { throw ValidationError("--event-loops must be positive") }
    }

    func run() async throws {
        let baseURL = url.hasSuffix("/") ? String(url.dropLast()) : url
        let client = HTTPClient(
            eventLoopGroup: HTTPClient.defaultEventLoopGroup,
            configuration: .init(
                connectionPool: .init(
                    idleTimeout: .seconds(60),
                    concurrentHTTP1ConnectionsPerHostSoftLimit: clients
                )
            )
        )
        let result: RunResult
        do {
            result = try await LoadRun(
                client: client,
                baseURL: baseURL,
                clients: clients,
                requests: requests,
                engine: engine,
                eventLoops: eventLoops
            ).run()
        } catch {
            try? await client.shutdown()
            throw error
        }
        try await client.shutdown()

        if header {
            print(RunResult.csvHeader)
        }
        print(result.csvRow(store: storeLabel))
        if !result.isCorrect {
            printToStandardError("loadgen: expected \(result.expected), server counted \(result.finalValue)")
            throw ExitCode(2)
        }
    }
}

extension LoadEngine: ExpressibleByArgument {}
