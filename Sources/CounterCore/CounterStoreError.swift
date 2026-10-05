/// Errors raised by the counter stores themselves (as opposed to errors from
/// the underlying driver, which are passed through unchanged).
public enum CounterStoreError: Error, Equatable, CustomStringConvertible {
    /// The backing file holds something that is not a counter value.
    case corruptFile(path: String, contents: String)
    /// A system call failed.
    case systemCall(name: String, errno: Int32)
    /// The database row that holds the counter does not exist.
    case missingRow(table: String, key: Int)

    public var description: String {
        switch self {
        case .corruptFile(let path, let contents):
            "the counter file \(path) does not hold a number: \(contents.debugDescription)"
        case .systemCall(let name, let errno):
            "\(name) failed with errno \(errno)"
        case .missingRow(let table, let key):
            "\(table) has no row for key \(key); run scripts/pg.sh reset"
        }
    }
}
