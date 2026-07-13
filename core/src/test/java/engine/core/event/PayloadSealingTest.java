package engine.core.event;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import org.junit.jupiter.api.Test;

/**
 * Proves {@link Payload} is exhaustively sealed: this {@code switch} has no {@code default}, so the
 * compiler would reject it if a permitted type were added and left unhandled.
 */
class PayloadSealingTest {

    private static String kind(Payload payload) {
        return switch (payload) {
            case TradeTick t -> "trade";
            case QuoteTick q -> "quote";
            case Bar b -> "bar";
            case Signal s -> "signal";
            case OrderIntent o -> "order";
            case Fill f -> "fill";
            case Metric m -> "metric";
            case Command c -> "command";
        };
    }

    @Test
    void exhaustiveSwitchCompilesWithoutDefault() {
        Payload payload = new TradeTick(new BigDecimal("1.00"), new BigDecimal("2.00"), Side.BUY);
        assertThat(kind(payload)).isEqualTo("trade");
    }
}
