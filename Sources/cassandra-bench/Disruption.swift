import CommandLineSupport

/// What cassandra-bench does to the cluster during a run.
enum Disruption: Sendable {
    /// SIGKILL one node; it is started again once the workers are done.
    case killNode(Int)
    /// Cut the network between all nodes (scripts/netsplit.py) and restore
    /// it after the given time, while the run goes on.
    case split(lasting: Duration)
    /// SIGKILL every node: a crash of the whole cluster. The nodes are
    /// started again after `down`, and the workers keep trying meanwhile.
    case killAll(down: Duration)

    var label: String {
        switch self {
        case .killNode(let node): "kill-node\(node)"
        case .split: "split"
        case .killAll: "kill-all"
        }
    }
}

/// Carries out a `Disruption` at its time and restores the cluster
/// afterwards.
struct DisruptionPlan: Sendable {
    let disruption: Disruption
    let after: Duration
    let control: ClusterControl

    /// Sleeps until `after` into the run, then disrupts. Returns when the
    /// disruption is over for the workers (split healed, cluster restarted),
    /// or nil if cancelled before it was due.
    func perform(from start: ContinuousClock.Instant, timeline: Timeline) async throws -> Duration? {
        do {
            try await Task.sleep(until: start.advanced(by: after), clock: .continuous)
        } catch is CancellationError {
            return nil
        }
        let at = start.duration(to: .now)
        switch disruption {
        case .killNode(let node):
            printToStandardError("cassandra-bench: killing node \(node) at \(seconds(at)) s")
            try await control.run("kill-node", String(node))
        case .split(let lasting):
            printToStandardError("cassandra-bench: splitting the network at \(seconds(at)) s for \(seconds(lasting)) s")
            try await control.run("split")
            timeline.mark("split")
            try await Task.sleep(for: lasting)
            try await control.run("heal")
            timeline.mark("heal")
            printToStandardError("cassandra-bench: network healed at \(seconds(start.duration(to: .now))) s")
        case .killAll(let down):
            printToStandardError("cassandra-bench: killing all nodes at \(seconds(at)) s")
            try await control.run("kill-all")
            timeline.mark("killed")
            try await Task.sleep(for: down)
            printToStandardError("cassandra-bench: starting all nodes again")
            try await control.run("start")
            timeline.mark("restarted")
        }
        return at
    }

    /// Brings every node back and waits until all see each other as UN.
    func restore() async throws {
        if case .killNode(let node) = disruption {
            printToStandardError("cassandra-bench: starting node \(node) again")
            try await control.run("start-node", String(node))
        }
        for node in 1...3 {
            try await control.run("wait-up", String(node))
        }
    }

    private func seconds(_ duration: Duration) -> String {
        decimalString(duration.inSeconds, fractionDigits: 2)
    }
}
