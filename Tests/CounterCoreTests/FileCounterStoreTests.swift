import CounterCore
import Foundation
import Testing

@Suite("FileCounterStore")
struct FileCounterStoreTests {
    @Test("concurrent increments are never lost")
    func concurrentIncrements() async throws {
        try await withTemporaryFile { path in
            let store = try FileCounterStore(path: path)
            try await hammer(store, tasks: 16, incrementsPerTask: 250)
            #expect(try await store.value() == 4_000)
        }
    }

    @Test("the value survives reopening the file")
    func persistence() async throws {
        try await withTemporaryFile { path in
            do {
                let store = try FileCounterStore(path: path)
                for _ in 0..<7 {
                    try await store.increment()
                }
            }
            let reopened = try FileCounterStore(path: path)
            #expect(try await reopened.value() == 7)
            let contents = try String(contentsOfFile: path, encoding: .utf8)
            #expect(contents == "00000000000000000007\n")
        }
    }

    @Test("reset sets the counter back to zero")
    func reset() async throws {
        try await withTemporaryFile { path in
            let store = try FileCounterStore(path: path)
            try await store.increment()
            try await store.reset()
            #expect(try await store.value() == 0)
        }
    }

    @Test("a file that does not hold a number is rejected")
    func corruptFile() async throws {
        try await withTemporaryFile { path in
            try "hello\n".write(toFile: path, atomically: true, encoding: .utf8)
            #expect(throws: CounterStoreError.self) {
                try FileCounterStore(path: path)
            }
        }
    }

    private func withTemporaryFile(_ body: (String) async throws -> Void) async throws {
        let path = FileManager.default.temporaryDirectory
            .appendingPathComponent("counter-\(UUID().uuidString).txt").path
        defer { try? FileManager.default.removeItem(atPath: path) }
        try await body(path)
    }
}
