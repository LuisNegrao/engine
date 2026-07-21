package engine.core.event;

import static org.assertj.core.api.Assertions.assertThat;

import engine.core.serde.SampleEvents;
import java.math.BigDecimal;
import org.junit.jupiter.api.Test;

/**
 * The {@code maybeX()} view methods over the deliberately-nullable record components: a present
 * value round-trips through the view, and an absent one yields {@link java.util.Optional#empty()}.
 */
class OptionalViewsTest {

    @Test
    void maybeInstrumentIdPresentRoundTrips() {
        Event event = SampleEvents.tradeTick();
        assertThat(event.instrumentId()).isNotNull();
        assertThat(event.maybeInstrumentId()).isPresent();
        assertThat(event.maybeInstrumentId().orElseThrow()).isEqualTo(event.instrumentId());
    }

    @Test
    void maybeInstrumentIdAbsentIsEmpty() {
        Event event = SampleEvents.command();
        assertThat(event.instrumentId()).isNull();
        assertThat(event.maybeInstrumentId()).isEmpty();
    }

    @Test
    void maybeConfidencePresentRoundTrips() {
        Signal signal = new Signal("sentiment", new BigDecimal("0.750"), new BigDecimal("0.90"));
        assertThat(signal.maybeConfidence()).isPresent();
        assertThat(signal.maybeConfidence().orElseThrow()).isEqualTo(signal.confidence());
    }

    @Test
    void maybeConfidenceAbsentIsEmpty() {
        Signal signal = new Signal("sentiment", new BigDecimal("0.750"), null);
        assertThat(signal.confidence()).isNull();
        assertThat(signal.maybeConfidence()).isEmpty();
    }

    @Test
    void maybeLimitPricePresentRoundTrips() {
        OrderIntent order = new OrderIntent(
                "strat-momentum",
                Side.BUY,
                new BigDecimal("1.00"),
                OrderType.LIMIT,
                new BigDecimal("67000.00"),
                TimeInForce.GTC,
                "client-order-1");
        assertThat(order.maybeLimitPrice()).isPresent();
        assertThat(order.maybeLimitPrice().orElseThrow()).isEqualTo(order.limitPrice());
    }

    @Test
    void maybeLimitPriceAbsentIsEmpty() {
        OrderIntent order = new OrderIntent(
                "strat-momentum",
                Side.BUY,
                new BigDecimal("1.00"),
                OrderType.MARKET,
                null,
                TimeInForce.GTC,
                "client-order-1");
        assertThat(order.limitPrice()).isNull();
        assertThat(order.maybeLimitPrice()).isEmpty();
    }
}
