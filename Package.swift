// swift-tools-version: 6.0
import PackageDescription

/// counter-server's io_uring engine needs liburing (found through
/// pkg-config), so it is only built when asked for: scripts/env.sh sets
/// HLS_IO_URING=1 when liburing is available.
let ioUring = Context.environment["HLS_IO_URING"] == "1"

let package = Package(
    name: "HighLoadCounter",
    platforms: [.macOS(.v15)],
    dependencies: [
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.90.0"),
        .package(url: "https://github.com/swift-server/async-http-client.git", from: "1.30.0"),
        .package(url: "https://github.com/vapor/postgres-nio.git", from: "1.30.0"),
        .package(url: "https://github.com/apple/swift-argument-parser.git", from: "1.6.0"),
    ],
    targets: [
        // Counter storage backends shared by the server and the tests.
        // Depends on PostgresNIO and HazelcastClient up front so the Task 2/3
        // stores can be filled in without touching this manifest.
        .target(
            name: "CounterCore",
            dependencies: [
                "HazelcastClient",
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "NIOPosix", package: "swift-nio"),
                .product(name: "PostgresNIO", package: "postgres-nio"),
            ]
        ),
        // Our client for the Hazelcast Open Binary Client Protocol 2.x.
        .target(
            name: "HazelcastClient",
            dependencies: [
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "NIOPosix", package: "swift-nio"),
            ]
        ),
        // Standard error output and number formatting for the executables.
        .target(name: "CommandLineSupport"),
        .executableTarget(
            name: "counter-server",
            dependencies: [
                "CommandLineSupport",
                "CounterCore",
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "NIOPosix", package: "swift-nio"),
                .product(name: "NIOHTTP1", package: "swift-nio"),
                .product(name: "ArgumentParser", package: "swift-argument-parser"),
            ]
        ),
        .executableTarget(
            name: "loadgen",
            dependencies: [
                "CommandLineSupport",
                .product(name: "AsyncHTTPClient", package: "async-http-client"),
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "NIOPosix", package: "swift-nio"),
                .product(name: "NIOHTTP1", package: "swift-nio"),
                .product(name: "ArgumentParser", package: "swift-argument-parser"),
            ]
        ),
        .executableTarget(
            name: "pg-bench",
            dependencies: [
                "CommandLineSupport",
                "CounterCore",
                .product(name: "PostgresNIO", package: "postgres-nio"),
                .product(name: "ArgumentParser", package: "swift-argument-parser"),
            ]
        ),
        .executableTarget(
            name: "hz-bench",
            dependencies: [
                "CommandLineSupport",
                "HazelcastClient",
                .product(name: "ArgumentParser", package: "swift-argument-parser"),
            ]
        ),
        .testTarget(
            name: "CommandLineSupportTests",
            dependencies: ["CommandLineSupport"]
        ),
        .testTarget(
            name: "CounterServerTests",
            dependencies: ["counter-server"]
        ),
        .testTarget(
            name: "CounterCoreTests",
            dependencies: [
                "CounterCore",
                .product(name: "NIOPosix", package: "swift-nio"),
            ]
        ),
        .testTarget(
            name: "HazelcastClientTests",
            dependencies: [
                "HazelcastClient",
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "NIOEmbedded", package: "swift-nio"),
            ]
        ),
    ]
)

if ioUring {
    package.targets.append(.systemLibrary(name: "CLibURing", pkgConfig: "liburing"))
    if let server = package.targets.first(where: { $0.name == "counter-server" }) {
        server.dependencies.append("CLibURing")
        // liburing may live outside the loader's default paths (e.g. in the
        // Nix store): record its directory in the binary.
        if let libdir = Context.environment["HLS_LIBURING_LIBDIR"] {
            server.linkerSettings = (server.linkerSettings ?? []) + [.unsafeFlags(["-Xlinker", "-rpath", "-Xlinker", libdir])]
        }
    }
}
