package engine.core.event;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class InstrumentIdTest {

    @Test
    void parseAndToStringRoundTrip() {
        InstrumentId id = InstrumentId.parse("BTC-USDT.BINANCE");
        assertThat(id.symbol()).isEqualTo("BTC-USDT");
        assertThat(id.venue()).isEqualTo("BINANCE");
        assertThat(id.toString()).isEqualTo("BTC-USDT.BINANCE");
    }

    @Test
    void splitsOnLastDotSoSymbolsMayContainDots() {
        InstrumentId id = InstrumentId.parse("a.b.c");
        assertThat(id.symbol()).isEqualTo("a.b");
        // Venue is normalized to upper-case: the "c" segment becomes "C".
        assertThat(id.venue()).isEqualTo("C");
    }

    @Test
    void venueIsNormalizedToUpperCase() {
        assertThat(InstrumentId.parse("btc-usdt.binance").venue()).isEqualTo("BINANCE");
        assertThat(new InstrumentId("BTC-USDT", "binance").toString()).isEqualTo("BTC-USDT.BINANCE");
    }

    @Test
    void rejectsInputWithoutSeparator() {
        assertThatThrownBy(() -> InstrumentId.parse("BTCUSDT")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsBlankInput() {
        assertThatThrownBy(() -> InstrumentId.parse("")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsBlankSymbolOrVenueSegments() {
        assertThatThrownBy(() -> InstrumentId.parse(".BINANCE")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> InstrumentId.parse("BTC-USDT.")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsNullComponents() {
        assertThatThrownBy(() -> new InstrumentId(null, "BINANCE")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new InstrumentId("BTC-USDT", null)).isInstanceOf(IllegalArgumentException.class);
    }
}
