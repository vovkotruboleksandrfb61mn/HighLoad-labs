/// The token ring: which node owns which token.
///
/// A node owns the range from the previous token on the ring (exclusive) to
/// each of its own tokens (inclusive), so the primary replica of a key is the
/// owner of the first ring token at or after the key's token, wrapping around.
/// SimpleStrategy places the other replicas on the next distinct nodes
/// clockwise.
public struct TokenRing: Sendable {
    /// Sorted by token.
    private let tokens: [Int64]
    private let owners: [Int]
    public let nodeCount: Int

    /// `tokensByNode[i]` are the tokens of node `i`.
    public init(tokensByNode: [[Int64]]) {
        let entries = tokensByNode.enumerated()
            .flatMap { node, tokens in tokens.map { (token: $0, node: node) } }
            .sorted { $0.token < $1.token }
        tokens = entries.map(\.token)
        owners = entries.map(\.node)
        nodeCount = tokensByNode.count
    }

    public var isEmpty: Bool { tokens.isEmpty }

    /// The index of the first ring entry at or after `token`.
    private func entry(for token: Int64) -> Int {
        var low = 0
        var high = tokens.count
        while low < high {
            let middle = (low + high) / 2
            if tokens[middle] < token {
                low = middle + 1
            } else {
                high = middle
            }
        }
        return low == tokens.count ? 0 : low
    }

    /// The node that is the primary replica of `token`.
    public func primary(for token: Int64) -> Int? {
        tokens.isEmpty ? nil : owners[entry(for: token)]
    }

    /// The first `count` distinct nodes clockwise from `token` (SimpleStrategy).
    public func replicas(for token: Int64, count: Int) -> [Int] {
        guard !tokens.isEmpty else { return [] }
        var result: [Int] = []
        var index = entry(for: token)
        for _ in 0..<tokens.count where result.count < count {
            let node = owners[index]
            if !result.contains(node) {
                result.append(node)
            }
            index = (index + 1) % tokens.count
        }
        return result
    }
}
