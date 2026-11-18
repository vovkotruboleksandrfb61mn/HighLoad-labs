import ArgumentParser
import Foundation
import CommandLineSupport
import CounterCore
import HazelcastClient
import PostgresNIO

/// Task Adv 1: compares a Hazelcast map with the PostgreSQL table behind it.
@main
struct CacheCheckCommand: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "cache-check",
        abstract: "Compares the counters of users in a Hazelcast map with the user_likes table behind it and prints one CSV row per user.",
        discussion: """
            Reads every user's counter from the map (IMap.get) and from the \
            table (SELECT likes) right away, then, with --wait, reads the table \
            every 50 ms until it holds the map's value (write-behind stores it \
            later) or --wait seconds pass. With --evict it then drops every \
            entry from the members' memory (IMap.evictAll, which leaves the \
            table alone) and reads the map again: the value now comes from the \
            table through the MapLoader (read-through). Exits with status 2 if \
            the table never matches the map.
            """
    )

    @Option(help: "The map: likes_write_through or likes_write_behind.")
    var map = "likes_write_through"

    @Option(help: "Comma-separated user ids.")
    var users = "1"

    @Option(name: .customLong("hz-address"), help: "Hazelcast member host:port.")
    var hzAddress = "127.0.0.1:5701"

    @Option(name: .customLong("pg-url"), help: "PostgreSQL URL.")
    var pgURL = "postgres://postgres@127.0.0.1:21410/postgres"

    @Option(help: "Seconds to wait for the table to catch up with the map.")
    var wait = 0.0

    @Flag(help: "Evict the map and read the users again through the MapLoader.")
    var evict = false

    @Option(help: "Instead of the check: sample the first user's value in the map and in the table every --interval ms for this many seconds and print seconds,imap,db rows.")
    var follow: Double?

    @Option(help: "Milliseconds between --follow samples.")
    var interval = 100

    @Option(help: "A label for the rows, the first CSV column.")
    var label = ""

    @Flag(help: "Print the CSV header line first.")
    var header = false

    static let csvHeader = "label,map,user,imap,db_at_start,db_after_wait,waited_seconds,equal,reloaded_after_evict"

    func validate() throws {
        guard !userIDs.isEmpty, userIDs.count == users.split(separator: ",").count else {
            throw ValidationError("--users must be a comma-separated list of integers")
        }
        guard wait >= 0 else { throw ValidationError("--wait must not be negative") }
    }

    private var userIDs: [Int64] { users.split(separator: ",").compactMap { Int64($0) } }

    func run() async throws {
        let endpoint = try PostgresEndpoint(url: pgURL)
        let postgres = PostgresClient(configuration: endpoint.clientConfiguration(maximumConnections: 2))
        let rows: [Row] = try await withThrowingDiscardingTaskGroup { group in
            group.addTask { await postgres.run() }
            defer { group.cancelAll() }
            return try await HazelcastClient.withClient(
                to: try HazelcastClient.Address(parsing: hzAddress),
                clientName: "cache-check"
            ) { client -> [Row] in
                let map = client.map(named: map, key: Int64.self, value: Int64.self)
                if let follow {
                    try await sample(map, postgres: postgres, for: .milliseconds(Int64(follow * 1000)))
                    return []
                }
                return try await check(map, postgres: postgres)
            }
        }
        if follow != nil { return }
        if header { print(Self.csvHeader) }
        for row in rows { print(row.csv(label: label, map: map)) }
        if rows.contains(where: { !$0.equal }) {
            for row in rows where !row.equal {
                printToStandardError("cache-check: user \(row.user): map \(row.imap), table \(row.afterWait.map(String.init) ?? "no row")")
            }
            throw ExitCode(2)
        }
    }

    private struct Row {
        var user: Int64
        var imap: Int64
        var atStart: Int64?
        var afterWait: Int64?
        var waited: Duration
        var reloaded: Int64?

        var equal: Bool { afterWait == imap }

        func csv(label: String, map: String) -> String {
            [
                label, map, String(user), String(imap),
                atStart.map(String.init) ?? "", afterWait.map(String.init) ?? "",
                decimalString(waited.inSeconds, fractionDigits: 2), String(equal),
                reloaded.map(String.init) ?? "",
            ].joined(separator: ",")
        }
    }

    private func check(_ map: HazelcastMap<Int64, Int64>, postgres: PostgresClient) async throws -> [Row] {
        let clock = ContinuousClock()
        let start = clock.now
        var rows: [Row] = []
        for user in userIDs {
            let imap = try await map.get(user, threadID: 1) ?? 0
            let atStart = try await tableValue(of: user, postgres)
            rows.append(Row(user: user, imap: imap, atStart: atStart, afterWait: atStart, waited: .zero))
        }
        let deadline = start.advanced(by: .milliseconds(Int64(wait * 1000)))
        for index in rows.indices {
            while !rows[index].equal && clock.now < deadline {
                try await Task.sleep(for: .milliseconds(50))
                rows[index].afterWait = try await tableValue(of: rows[index].user, postgres)
            }
            rows[index].waited = rows[index].equal && rows[index].atStart != rows[index].imap ? start.duration(to: clock.now) : .zero
        }
        if evict {
            try await map.evictAll()
            for index in rows.indices {
                rows[index].reloaded = try await map.get(rows[index].user, threadID: 1)
            }
        }
        return rows
    }

    /// Prints `seconds,imap,db` every `interval` ms, flushing each line, so
    /// a write-behind delay shows up as the table trailing the map.
    private func sample(_ map: HazelcastMap<Int64, Int64>, postgres: PostgresClient, for duration: Duration) async throws {
        guard let user = userIDs.first else { return }
        let clock = ContinuousClock()
        let start = clock.now
        FileHandle.standardOutput.write(Data("seconds,imap,db\n".utf8))
        var next = start
        while start.duration(to: clock.now) < duration {
            let at = start.duration(to: clock.now)
            let imap = try await map.get(user, threadID: 1) ?? 0
            let db = try await tableValue(of: user, postgres) ?? 0
            // Unbuffered, so a reader of a pipe sees every sample at once.
            FileHandle.standardOutput.write(Data("\(decimalString(at.inSeconds, fractionDigits: 2)),\(imap),\(db)\n".utf8))
            next = next.advanced(by: .milliseconds(interval))
            try await Task.sleep(until: next, clock: clock)
        }
    }

    private func tableValue(of user: Int64, _ postgres: PostgresClient) async throws -> Int64? {
        let rows = try await postgres.query("SELECT likes FROM user_likes WHERE user_id = \(user)")
        for try await likes in rows.decode(Int64.self) {
            return likes
        }
        return nil
    }
}
