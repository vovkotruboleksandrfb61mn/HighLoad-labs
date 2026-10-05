import HazelcastClient
import Testing

@Suite("ClientProtocol")
struct ClientProtocolTests {
    @Test("the client announces protocol 2 with the bytes CP2")
    func initialBytes() {
        #expect(ClientProtocol.initialBytes == [0x43, 0x50, 0x32])
    }
}
