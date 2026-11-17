/// A consistency level, with its protocol code.
public enum Consistency: UInt16, Sendable, Hashable, CaseIterable, CustomStringConvertible {
    case any = 0x0000
    case one = 0x0001
    case two = 0x0002
    case three = 0x0003
    case quorum = 0x0004
    case all = 0x0005
    case localQuorum = 0x0006
    case eachQuorum = 0x0007
    case serial = 0x0008
    case localSerial = 0x0009
    case localOne = 0x000A

    public var description: String {
        switch self {
        case .any: "ANY"
        case .one: "ONE"
        case .two: "TWO"
        case .three: "THREE"
        case .quorum: "QUORUM"
        case .all: "ALL"
        case .localQuorum: "LOCAL_QUORUM"
        case .eachQuorum: "EACH_QUORUM"
        case .serial: "SERIAL"
        case .localSerial: "LOCAL_SERIAL"
        case .localOne: "LOCAL_ONE"
        }
    }
}
