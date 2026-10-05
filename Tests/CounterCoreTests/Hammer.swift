import CounterCore

/// Increments `store` from `tasks` concurrent tasks, `incrementsPerTask` times each.
func hammer(_ store: some CounterStore, tasks: Int, incrementsPerTask: Int) async throws {
    try await withThrowingDiscardingTaskGroup { group in
        for _ in 0..<tasks {
            group.addTask {
                for _ in 0..<incrementsPerTask {
                    try await store.increment()
                }
            }
        }
    }
}
