import CommandLineSupport
import Testing

@Suite("decimalString")
struct DecimalStringTests {
    @Test(
        "rounds to the requested number of places",
        arguments: [
            (0.0, 4, "0.0000"),
            (1.5, 1, "1.5"),
            (1.49996, 4, "1.5000"),
            (0.00004, 4, "0.0000"),
            (0.00006, 4, "0.0001"),
            (12_345.678, 1, "12345.7"),
            (3.0001, 4, "3.0001"),
        ]
    )
    func rounding(value: Double, digits: Int, expected: String) {
        #expect(decimalString(value, fractionDigits: digits) == expected)
    }

    @Test("a duration converts to seconds")
    func durationSeconds() {
        #expect(Duration.milliseconds(1_500).inSeconds == 1.5)
        #expect(Duration.seconds(3).inSeconds == 3)
    }
}
