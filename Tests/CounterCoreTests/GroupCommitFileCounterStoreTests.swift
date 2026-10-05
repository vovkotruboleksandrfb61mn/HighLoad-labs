@testable import CounterCore
import Foundation
import NIOPosix
import Testing

@Suite("GroupCommitFileCounterStore")
struct GroupCommitFileCounterStoreTests {
    @Test("concurrent increments are never lost", arguments: GroupCommitFileCounterStore.SyncMethod.allCases)
    func concurrentIncrements(sync: GroupCommitFileCounterStore.SyncMethod) async throws {
        try await withTemporaryFile { path in
            let store = try GroupCommitFileCounterStore(path: path, syncMethod: sync)
            try await hammer(store, tasks: 16, incrementsPerTask: 100)
            #expect(try await store.value() == 1_600)
            #expect(store.flushCount <= 1_600)
            let contents = try String(contentsOfFile: path, encoding: .utf8)
            #expect(contents == "00000000000000001600\n")
        }
    }

    @Test("increments through the event-loop API are on disk when they complete")
    func eventLoopIncrements() async throws {
        try await withTemporaryFile { path in
            let store = try GroupCommitFileCounterStore(path: path)
            let loop = MultiThreadedEventLoopGroup.singleton.any()
            let futures = (0..<500).map { _ in store.increment(on: loop) }
            for future in futures {
                try await future.get()
            }
            #expect(try await store.value(on: loop).get() == 500)
            // 500 concurrent increments need far fewer than 500 syncs.
            #expect(store.flushCount < 500)
            let reopened = try FileCounterStore(path: path)
            #expect(try await reopened.value() == 500)
        }
    }

    @Test("the value survives reopening, in the same format as FileCounterStore")
    func persistence() async throws {
        try await withTemporaryFile { path in
            do {
                let store = try FileCounterStore(path: path)
                for _ in 0..<3 {
                    try await store.increment()
                }
            }
            do {
                let store = try GroupCommitFileCounterStore(path: path)
                #expect(try await store.value() == 3)
                for _ in 0..<4 {
                    try await store.increment()
                }
            }
            let reopened = try GroupCommitFileCounterStore(path: path)
            #expect(try await reopened.value() == 7)
            let contents = try String(contentsOfFile: path, encoding: .utf8)
            #expect(contents == "00000000000000000007\n")
        }
    }

    @Test("reset sets the counter back to zero")
    func reset() async throws {
        try await withTemporaryFile { path in
            let store = try GroupCommitFileCounterStore(path: path)
            try await store.increment()
            try await store.reset()
            #expect(try await store.value() == 0)
            try await store.increment()
            #expect(try await store.value() == 1)
        }
    }

    @Test("a file that does not hold a number is rejected")
    func corruptFile() async throws {
        try await withTemporaryFile { path in
            try "hello\n".write(toFile: path, atomically: true, encoding: .utf8)
            #expect(throws: CounterStoreError.self) {
                try GroupCommitFileCounterStore(path: path)
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
