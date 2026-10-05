import NIOPosix
import Synchronization

#if canImport(Glibc)
import Glibc
#elseif canImport(Musl)
import Musl
#elseif canImport(Darwin)
import Darwin
#endif

/// A counter kept in a file and flushed to disk on every increment.
///
/// Each increment holds a `Mutex` while it reads the value, adds one, writes
/// it back with `pwrite` and calls `fsync`, so increments are serialised and
/// durable. All of that blocking I/O runs on a `NIOThreadPool`, never on an
/// event loop or the Swift cooperative pool. `value()` reads the file too, so
/// it reports what is actually on disk.
public final class FileCounterStore: CounterStore {
    public let path: String
    private let descriptor: Mutex<Int32>
    private let threadPool: NIOThreadPool

    /// Opens (or creates) the counter file at `path`. A new or empty file
    /// starts at zero.
    public init(path: String, threadPool: NIOThreadPool = .singleton) throws {
        let fd = open(path, O_RDWR | O_CREAT | O_CLOEXEC, 0o644)
        guard fd >= 0 else {
            throw CounterStoreError.systemCall(name: "open(\(path))", errno: errno)
        }
        do {
            if try CounterFile.read(fd, path: path) == nil {
                try CounterFile.write(0, to: fd)
            }
        } catch {
            close(fd)
            throw error
        }
        self.path = path
        self.descriptor = Mutex(fd)
        self.threadPool = threadPool
    }

    deinit {
        descriptor.withLock { fd in
            _ = close(fd)
        }
    }

    public func increment() async throws {
        try await threadPool.runIfActive {
            try self.descriptor.withLock { fd in
                let current = try CounterFile.read(fd, path: self.path) ?? 0
                try CounterFile.write(current + 1, to: fd)
            }
        }
    }

    public func value() async throws -> Int {
        try await threadPool.runIfActive {
            try self.descriptor.withLock { fd in
                try CounterFile.read(fd, path: self.path) ?? 0
            }
        }
    }

    public func reset() async throws {
        try await threadPool.runIfActive {
            try self.descriptor.withLock { fd in
                try CounterFile.write(0, to: fd)
            }
        }
    }
}

/// The on-disk format: the value as decimal digits, zero-padded to a fixed
/// width and followed by a newline, always written at offset 0. A fixed width
/// means a write never has to truncate the file, and the file stays readable
/// with `cat`.
enum CounterFile {
    static let width = 20
    static let recordSize = width + 1

    /// Reads the value, or `nil` if the file is empty.
    static func read(_ fd: Int32, path: String) throws -> Int? {
        var buffer = [UInt8](repeating: 0, count: recordSize)
        let count = try buffer.withUnsafeMutableBytes { raw in
            try retryingOnInterrupt(name: "pread") {
                pread(fd, raw.baseAddress, raw.count, 0)
            }
        }
        if count == 0 {
            return nil
        }
        let text = String(decoding: buffer[..<count], as: UTF8.self)
        guard let value = Int(text.prefix { $0 != "\n" }) else {
            throw CounterStoreError.corruptFile(path: path, contents: text)
        }
        return value
    }

    /// Writes the value at offset 0 and flushes it to disk with `fsync`.
    static func write(_ value: Int, to fd: Int32) throws {
        try writeRecord(value, to: fd)
        try sync(fd, name: "fsync") { fsync($0) }
    }

    /// Writes the value at offset 0 without flushing it.
    static func writeRecord(_ value: Int, to fd: Int32) throws {
        let digits = String(value)
        let padding = String(repeating: "0", count: max(0, width - digits.utf8.count))
        let bytes = Array((padding + digits + "\n").utf8)
        var offset = 0
        while offset < bytes.count {
            let written = try bytes[offset...].withUnsafeBytes { remaining in
                try retryingOnInterrupt(name: "pwrite") {
                    pwrite(fd, remaining.baseAddress, remaining.count, off_t(offset))
                }
            }
            offset += written
        }
    }

    /// Calls `fsync` or `fdatasync` on `fd`, retrying on `EINTR`.
    static func sync(_ fd: Int32, name: String, _ call: (Int32) -> Int32) throws {
        _ = try retryingOnInterrupt(name: name) { Int(call(fd)) }
    }

    private static func retryingOnInterrupt(name: String, _ call: () -> Int) throws -> Int {
        while true {
            let result = call()
            if result >= 0 {
                return result
            }
            if errno != EINTR {
                throw CounterStoreError.systemCall(name: name, errno: errno)
            }
        }
    }
}
