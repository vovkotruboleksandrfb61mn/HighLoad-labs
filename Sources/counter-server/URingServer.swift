#if canImport(CLibURing)
import CLibURing
import CounterCore

#if canImport(Glibc)
import Glibc
#elseif canImport(Musl)
import Musl
#endif

/// Serves the counter with io_uring instead of SwiftNIO's epoll event loops.
///
/// Each thread owns one ring and its own listening socket (`SO_REUSEPORT`
/// lets the kernel spread connections over the threads). A multishot accept
/// keeps accepting without being re-armed. For every request the thread
/// submits the response `send` linked to the next `recv`, so one
/// `io_uring_submit_and_wait` both hands the kernel all of this round's work
/// and sleeps until something completes: one system call per round, where
/// the epoll loop makes three (`epoll_wait`, `read`, `write`).
///
/// Only stores that answer immediately are supported, since the threads
/// never leave their loop.
struct URingServer: Sendable {
    let host: String
    let port: Int
    let store: any SynchronousCounterStore
    let threads: Int
    let pinnedCPUs: [Int]?
    /// Let a kernel thread poll each submission queue (`IORING_SETUP_SQPOLL`),
    /// so submitting costs no system call.
    let submissionPolling: Bool

    func run() async throws {
        let cpus: [Int?] = pinnedCPUs ?? Array(repeating: nil, count: threads)
        let threadsDescription = pinnedCPUs.map { "pinned to CPUs \($0.map(String.init).joined(separator: ","))" } ?? "\(threads)"
        Log.info("engine=uring threads=\(threadsDescription) sqpoll=\(submissionPolling) listening on \(host):\(port)")
        let host = host, port = port, store = store, submissionPolling = submissionPolling
        for cpu in cpus {
            startThread {
                if let cpu {
                    pinCurrentThread(to: cpu)
                }
                do {
                    let worker = try URingWorker(host: host, port: port, store: store, submissionPolling: submissionPolling)
                    let failure = worker.run()
                    Log.error("io_uring worker stopped: \(failure)")
                } catch {
                    Log.error("io_uring worker could not start: \(error)")
                }
                exit(1)
            }
        }
        // The threads serve until the process is killed; a worker that fails
        // ends the process itself.
        while true {
            try await Task.sleep(for: .seconds(3600))
        }
    }
}

struct URingError: Error, CustomStringConvertible {
    let operation: String
    let code: Int32

    var description: String {
        "\(operation): \(String(cString: strerror(code)))"
    }
}

// MARK: - Worker

/// One thread's ring, listener and connections.
private final class URingWorker {
    private enum Operation: UInt64 {
        case accept = 1
        case receive = 2
        case send = 3
    }

    /// A client connection. Its buffers belong to the kernel while any
    /// operation on it is in flight, so they are only freed once `inFlight`
    /// is back to zero.
    private final class Connection {
        static let bufferSize = 4096

        let fd: Int32
        let generation: UInt32
        let input = UnsafeMutableRawBufferPointer.allocate(byteCount: bufferSize, alignment: 8)
        let output = UnsafeMutableRawBufferPointer.allocate(byteCount: bufferSize, alignment: 8)
        /// Bytes received and not yet answered, at the start of `input`.
        var received = 0
        /// The response being sent: `output[sent..<pendingSend]`.
        var pendingSend = 0
        var sent = 0
        var closeAfterSend = false
        var closing = false
        var inFlight = 0

        init(fd: Int32, generation: UInt32) {
            self.fd = fd
            self.generation = generation
        }

        deinit {
            input.deallocate()
            output.deallocate()
        }
    }

    private static let entries: UInt32 = 1024
    private static let batch = 256
    /// The answer to a keep-alive HTTP/1.1 `/inc` or `/reset`.
    private static let noContent = Array("HTTP/1.1 204 No Content\r\n\r\n".utf8)

    private let ring = UnsafeMutablePointer<io_uring>.allocate(capacity: 1)
    private let completions = UnsafeMutablePointer<UnsafeMutablePointer<io_uring_cqe>?>.allocate(capacity: batch)
    private let listener: Int32
    private let store: any SynchronousCounterStore
    /// Connections by file descriptor.
    private var connections: [Connection?] = []
    private var nextGeneration: UInt32 = 0

    init(host: String, port: Int, store: any SynchronousCounterStore, submissionPolling: Bool) throws {
        self.store = store
        listener = try Self.listen(host: host, port: port)

        var parameters = io_uring_params()
        if submissionPolling {
            parameters.flags |= UInt32(IORING_SETUP_SQPOLL)
            parameters.sq_thread_idle = 1000    // ms before the kernel thread sleeps
        }
        let result = io_uring_queue_init_params(Self.entries, ring, &parameters)
        guard result == 0 else {
            close(listener)
            throw URingError(operation: "io_uring_queue_init_params", code: -result)
        }
    }

    /// Serves until something unrecoverable happens; returns what it was.
    func run() -> URingError {
        armAccept()
        while true {
            let submitted = io_uring_submit_and_wait(ring, 1)
            if submitted < 0, submitted != -EINTR {
                return URingError(operation: "io_uring_submit_and_wait", code: -submitted)
            }
            let ready = io_uring_peek_batch_cqe(ring, completions, UInt32(Self.batch))
            for index in 0..<Int(ready) {
                guard let completion = completions[index]?.pointee else { continue }
                if let failure = handle(result: completion.res, data: completion.user_data, flags: completion.flags) {
                    return failure
                }
            }
            io_uring_cq_advance(ring, ready)
        }
    }

    // MARK: Completions

    private func handle(result: Int32, data: UInt64, flags: UInt32) -> URingError? {
        guard let operation = Operation(rawValue: data & 0xFF) else { return nil }
        if operation == .accept {
            return accepted(result: result, more: flags & UInt32(IORING_CQE_F_MORE) != 0)
        }
        let fd = Int32(truncatingIfNeeded: data >> 32)
        let generation = UInt32(truncatingIfNeeded: (data >> 8) & 0xFF_FFFF)
        guard fd >= 0, Int(fd) < connections.count, let connection = connections[Int(fd)],
              connection.generation == generation else {
            return nil
        }
        connection.inFlight -= 1
        switch operation {
        case .receive:
            received(result: result, on: connection)
        case .send:
            sent(result: result, on: connection)
        case .accept:
            break
        }
        if connection.closing {
            finishClosing(connection)
        }
        return nil
    }

    private func accepted(result: Int32, more: Bool) -> URingError? {
        if result >= 0 {
            open(result)
        } else if result != -EAGAIN, result != -ECONNABORTED, result != -EINTR {
            if !more {
                return URingError(operation: "accept", code: -result)
            }
        }
        if !more {
            armAccept()
        }
        return nil
    }

    private func open(_ fd: Int32) {
        var one: Int32 = 1
        setsockopt(fd, Int32(IPPROTO_TCP), TCP_NODELAY, &one, socklen_t(MemoryLayout<Int32>.size))
        if Int(fd) >= connections.count {
            connections.append(contentsOf: repeatElement(nil, count: Int(fd) + 1 - connections.count))
        }
        nextGeneration = (nextGeneration + 1) & 0xFF_FFFF
        let connection = Connection(fd: fd, generation: nextGeneration)
        connections[Int(fd)] = connection
        armReceive(connection)
    }

    private func received(result: Int32, on connection: Connection) {
        if connection.closing {
            return
        }
        if result == -ECANCELED {
            // The linked send before it came up short; the rest of the
            // response is on its way, so wait for the next request again.
            armReceive(connection)
            return
        }
        guard result > 0 else {
            beginClosing(connection)
            return
        }
        connection.received += Int(result)
        answer(connection)
    }

    private func sent(result: Int32, on connection: Connection) {
        if connection.closing {
            return
        }
        guard result >= 0 else {
            beginClosing(connection)
            return
        }
        connection.sent += Int(result)
        if connection.sent < connection.pendingSend {
            submitSend(connection, linkReceive: false)
        } else if connection.closeAfterSend {
            beginClosing(connection)
        }
    }

    // MARK: Requests

    /// Answers every complete request in the input, then sends the answers
    /// and waits for more.
    private func answer(_ connection: Connection) {
        var consumed = 0
        var written = 0
        var close = false
        parsing: while consumed < connection.received {
            let unread = UnsafeRawBufferPointer(rebasing: connection.input[consumed..<connection.received])
            switch RawRequestParser.parse(unread) {
            case .request(let request):
                // Leave the rest for the next round if the output is full.
                guard written + 256 <= Connection.bufferSize else { break parsing }
                written += respond(to: request, into: connection.output, at: written)
                consumed += request.length
                if !request.keepAlive {
                    close = true
                    break parsing
                }
            case .incomplete:
                break parsing
            case .malformed:
                written += copy(Array("HTTP/1.1 400 Bad Request\r\ncontent-length: 0\r\nconnection: close\r\n\r\n".utf8), into: connection.output, at: written)
                consumed = connection.received
                close = true
                break parsing
            }
        }

        if consumed > 0 {
            let left = connection.received - consumed
            if left > 0, let base = connection.input.baseAddress {
                memmove(base, base + consumed, left)
            }
            connection.received = left
        }

        if written > 0 {
            connection.pendingSend = written
            connection.sent = 0
            connection.closeAfterSend = close
            submitSend(connection, linkReceive: !close)
        } else if connection.received == Connection.bufferSize {
            beginClosing(connection)    // a request larger than the buffer
        } else {
            armReceive(connection)
        }
    }

    private func respond(to request: RawRequest, into output: UnsafeMutableRawBufferPointer, at offset: Int) -> Int {
        let body: String?
        switch request.route {
        case .increment:
            store.incrementNow()
            body = nil
        case .count:
            body = "\(store.valueNow())\n"
        case .reset:
            store.resetNow()
            body = nil
        case .notFound:
            body = "not found\n"
        }
        if body == nil, request.route != .notFound, request.keepAlive, !request.isHTTP10 {
            return copy(Self.noContent, into: output, at: offset)
        }
        let status = request.route == .notFound ? "404 Not Found" : (body == nil ? "204 No Content" : "200 OK")
        var head = "HTTP/1.1 \(status)\r\n"
        if let body {
            head += "content-type: text/plain; charset=utf-8\r\ncontent-length: \(body.utf8.count)\r\n"
        }
        if !request.keepAlive {
            head += "connection: close\r\n"
        } else if request.isHTTP10 {
            head += "connection: keep-alive\r\n"
        }
        head += "\r\n" + (body ?? "")
        return copy(Array(head.utf8), into: output, at: offset)
    }

    private func copy(_ bytes: [UInt8], into output: UnsafeMutableRawBufferPointer, at offset: Int) -> Int {
        let count = min(bytes.count, output.count - offset)
        UnsafeMutableRawBufferPointer(rebasing: output[offset..<(offset + count)]).copyBytes(from: bytes[..<count])
        return count
    }

    // MARK: Submissions

    private func submission() -> UnsafeMutablePointer<io_uring_sqe> {
        if let entry = io_uring_get_sqe(ring) {
            return entry
        }
        // The queue is full: hand it to the kernel to make room.
        io_uring_submit(ring)
        guard let entry = io_uring_get_sqe(ring) else {
            fatalError("io_uring submission queue still full after submitting")
        }
        return entry
    }

    private func armAccept() {
        let entry = submission()
        io_uring_prep_multishot_accept(entry, listener, nil, nil, 0)
        io_uring_sqe_set_data64(entry, Operation.accept.rawValue)
    }

    private func armReceive(_ connection: Connection) {
        let entry = submission()
        let free = UnsafeMutableRawBufferPointer(rebasing: connection.input[connection.received...])
        io_uring_prep_recv(entry, connection.fd, free.baseAddress, free.count, 0)
        io_uring_sqe_set_data64(entry, tag(.receive, connection))
        connection.inFlight += 1
    }

    /// Sends the rest of the pending response. With `linkReceive`, the next
    /// `recv` is linked behind it: the kernel starts it as soon as the send
    /// completes, with no further round trip through this loop.
    private func submitSend(_ connection: Connection, linkReceive: Bool) {
        let entry = submission()
        let rest = UnsafeMutableRawBufferPointer(rebasing: connection.output[connection.sent..<connection.pendingSend])
        io_uring_prep_send(entry, connection.fd, rest.baseAddress, rest.count, Int32(MSG_NOSIGNAL))
        io_uring_sqe_set_data64(entry, tag(.send, connection))
        connection.inFlight += 1
        if linkReceive {
            io_uring_sqe_set_flags(entry, hls_iosqe_io_link())
            armReceive(connection)
        }
    }

    private func tag(_ operation: Operation, _ connection: Connection) -> UInt64 {
        UInt64(UInt32(bitPattern: connection.fd)) << 32 | UInt64(connection.generation) << 8 | operation.rawValue
    }

    // MARK: Closing

    /// Shuts the socket down so that pending operations complete; the
    /// descriptor and buffers are released once none is left.
    private func beginClosing(_ connection: Connection) {
        guard !connection.closing else { return }
        connection.closing = true
        shutdown(connection.fd, Int32(SHUT_RDWR))
        finishClosing(connection)
    }

    private func finishClosing(_ connection: Connection) {
        guard connection.inFlight == 0 else { return }
        connections[Int(connection.fd)] = nil
        close(connection.fd)
    }

    // MARK: Setup

    private static func listen(host: String, port: Int) throws -> Int32 {
        #if canImport(Glibc)
        let streamType = Int32(SOCK_STREAM.rawValue)
        #else
        let streamType = SOCK_STREAM
        #endif
        let fd = socket(AF_INET, streamType, 0)
        guard fd >= 0 else { throw URingError(operation: "socket", code: errno) }
        var one: Int32 = 1
        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, socklen_t(MemoryLayout<Int32>.size))
        setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &one, socklen_t(MemoryLayout<Int32>.size))

        var address = sockaddr_in()
        address.sin_family = sa_family_t(AF_INET)
        address.sin_port = in_port_t(UInt16(port).bigEndian)
        guard inet_pton(AF_INET, host, &address.sin_addr) == 1 else {
            close(fd)
            throw URingError(operation: "inet_pton(\(host)): the uring engine takes an IPv4 address", code: EINVAL)
        }
        let bound = withUnsafePointer(to: &address) {
            $0.withMemoryRebound(to: sockaddr.self, capacity: 1) {
                bind(fd, $0, socklen_t(MemoryLayout<sockaddr_in>.size))
            }
        }
        guard bound == 0, Glibc.listen(fd, 1024) == 0 else {
            let code = errno
            close(fd)
            throw URingError(operation: "bind/listen", code: code)
        }
        return fd
    }
}

// MARK: - Threads

private final class ThreadBody: Sendable {
    let run: @Sendable () -> Void

    init(_ run: @escaping @Sendable () -> Void) {
        self.run = run
    }
}

/// Starts a detached POSIX thread: the workers block in io_uring and must
/// not occupy threads of Swift's cooperative pool.
private func startThread(_ body: @escaping @Sendable () -> Void) {
    let context = Unmanaged.passRetained(ThreadBody(body)).toOpaque()
    var thread = pthread_t()
    let result = pthread_create(&thread, nil, { context in
        guard let context else { return nil }
        Unmanaged<ThreadBody>.fromOpaque(context).takeRetainedValue().run()
        return nil
    }, context)
    precondition(result == 0, "pthread_create failed: \(result)")
    pthread_detach(thread)
}

private func pinCurrentThread(to cpu: Int) {
    var set = cpu_set_t()
    withUnsafeMutableBytes(of: &set) { bytes in
        bytes[cpu / 8] |= UInt8(1 << (cpu % 8))
    }
    if sched_setaffinity(0, MemoryLayout<cpu_set_t>.size, &set) != 0 {
        Log.error("could not pin a thread to CPU \(cpu): \(String(cString: strerror(errno)))")
    }
}
#endif
