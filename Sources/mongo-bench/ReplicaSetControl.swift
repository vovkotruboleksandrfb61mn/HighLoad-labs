import CommandLineSupport

#if canImport(Glibc)
import Glibc
#elseif canImport(Musl)
import Musl
#elseif canImport(Darwin)
import Darwin
#endif

/// Runs `scripts/mongo.sh` subcommands (kill-node, start-node, wait-secondary)
/// on the local replica set. The script's output goes to standard error, so
/// standard output keeps only the CSV.
///
/// Not Foundation's `Process`: on Linux it notices that a child exited
/// through a descriptor the child inherits, and `start-node` leaves a forked
/// mongod daemon holding that descriptor, so the exit is never reported.
struct ReplicaSetControl: Sendable {
    let script: String

    struct Failure: Error, CustomStringConvertible {
        let command: [String]
        let reason: String
        var description: String { "\(command.joined(separator: " ")): \(reason)" }
    }

    /// Runs `scripts/mongo.sh <subcommand> <node>` and waits for it to exit.
    func run(_ subcommand: String, node: Int) async throws {
        let command = [script, subcommand, String(node)]
        let pid = try spawn(command)
        // Poll instead of a blocking waitpid, so no thread of the
        // cooperative pool is held while the script runs.
        var status: Int32 = 0
        while true {
            let result = waitpid(pid, &status, WNOHANG)
            if result == pid {
                break
            }
            if result == -1 && errno != EINTR {
                throw Failure(command: command, reason: "waitpid failed with errno \(errno)")
            }
            try await Task.sleep(for: .milliseconds(10))
        }
        // WIFEXITED / WEXITSTATUS, which Swift does not import as macros.
        let signal = status & 0x7F
        let exitCode = (status >> 8) & 0xFF
        guard signal == 0 else {
            throw Failure(command: command, reason: "killed by signal \(signal)")
        }
        guard exitCode == 0 else {
            throw Failure(command: command, reason: "exited with status \(exitCode)")
        }
    }

    /// Starts `command` with this process's environment, its standard output
    /// sent to standard error.
    private func spawn(_ command: [String]) throws -> pid_t {
        var actions = posix_spawn_file_actions_t()
        posix_spawn_file_actions_init(&actions)
        defer { posix_spawn_file_actions_destroy(&actions) }
        posix_spawn_file_actions_adddup2(&actions, STDERR_FILENO, STDOUT_FILENO)

        var cArguments = command.map { strdup($0) }
        defer { cArguments.forEach { free($0) } }
        cArguments.append(nil)

        var pid: pid_t = 0
        let result = posix_spawn(&pid, command[0], &actions, nil, cArguments, environ)
        guard result == 0 else {
            throw Failure(command: command, reason: "posix_spawn failed with errno \(result)")
        }
        return pid
    }
}
