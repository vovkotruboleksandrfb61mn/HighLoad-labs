/// Holds every client back until all of them are ready, then lets them go at
/// once, so connection setup is not part of the timed run.
actor StartGate {
    private let participants: Int
    private var arrived = 0
    private var isOpen = false
    private var waitingClients: [CheckedContinuation<Void, Never>] = []
    private var waitingCoordinator: CheckedContinuation<Void, Never>?

    init(participants: Int) {
        self.participants = participants
    }

    /// Called by each client when it is ready; returns when the gate opens.
    func arriveAndWait() async {
        arrived += 1
        if arrived == participants {
            waitingCoordinator?.resume()
            waitingCoordinator = nil
        }
        if isOpen {
            return
        }
        await withCheckedContinuation { waitingClients.append($0) }
    }

    /// Returns once every participant has arrived.
    func allArrived() async {
        if arrived >= participants {
            return
        }
        await withCheckedContinuation { waitingCoordinator = $0 }
    }

    /// Releases every waiting client.
    func open() {
        isOpen = true
        for client in waitingClients {
            client.resume()
        }
        waitingClients.removeAll()
    }
}
