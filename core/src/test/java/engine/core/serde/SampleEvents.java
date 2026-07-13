package engine.core.serde;

import engine.core.event.Bar;
import engine.core.event.Command;
import engine.core.event.CommandAction;
import engine.core.event.Event;
import engine.core.event.Fill;
import engine.core.event.InstrumentId;
import engine.core.event.Metric;
import engine.core.event.OrderIntent;
import engine.core.event.OrderType;
import engine.core.event.QuoteTick;
import engine.core.event.Side;
import engine.core.event.Signal;
import engine.core.event.TimeInForce;
import engine.core.event.TradeTick;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Fully-populated sample events, one per payload type. Decimals carry deliberate trailing zeros
 * (e.g. {@code "67231.50"}) so that a passing round-trip also proves scale is preserved on the wire
 * — {@link BigDecimal#equals} is scale-sensitive.
 */
final class SampleEvents {

    private SampleEvents() {}

    static final InstrumentId BTC = InstrumentId.parse("BTC-USDT.BINANCE");
    static final Instant OCCURRED = Instant.parse("2026-07-10T14:03:22.113Z");
    static final Instant INGESTED = Instant.parse("2026-07-10T14:03:22.145Z");

    static Event tradeTick() {
        return event(BTC, new TradeTick(new BigDecimal("67231.50"), new BigDecimal("0.0042"), Side.BUY));
    }

    static Event quoteTick() {
        return event(
                BTC,
                new QuoteTick(
                        new BigDecimal("67231.40"),
                        new BigDecimal("1.5000"),
                        new BigDecimal("67231.60"),
                        new BigDecimal("2.0000")));
    }

    static Event bar() {
        return event(
                BTC,
                new Bar(
                        Instant.parse("2026-07-10T14:03:00Z"),
                        Duration.ofMinutes(1),
                        new BigDecimal("67200.00"),
                        new BigDecimal("67250.50"),
                        new BigDecimal("67180.00"),
                        new BigDecimal("67231.50"),
                        new BigDecimal("1500.00")));
    }

    static Event signal() {
        return event(BTC, new Signal("sentiment", new BigDecimal("0.750"), new BigDecimal("0.90")));
    }

    static Event orderIntent() {
        return event(
                BTC,
                new OrderIntent(
                        "strat-momentum",
                        Side.BUY,
                        new BigDecimal("1.00"),
                        OrderType.LIMIT,
                        new BigDecimal("67000.00"),
                        TimeInForce.GTC,
                        "client-order-1"));
    }

    static Event fill() {
        return event(
                BTC,
                new Fill(
                        "client-order-1",
                        new BigDecimal("0.50"),
                        new BigDecimal("67000.00"),
                        new BigDecimal("0.10"),
                        "USDT",
                        false));
    }

    static Event metric() {
        return event(BTC, new Metric("pnl.unrealized", new BigDecimal("1234.50"), "strat-momentum"));
    }

    static Event command() {
        // Command is instrument-agnostic: instrumentId is absent (null) on the envelope.
        return event(null, new Command("*", CommandAction.KILL, Map.of("reason", "max-drawdown")));
    }

    static List<Event> all() {
        return List.of(tradeTick(), quoteTick(), bar(), signal(), orderIntent(), fill(), metric(), command());
    }

    private static Event event(InstrumentId instrumentId, engine.core.event.Payload payload) {
        return new Event(UUID.randomUUID(), "test-feed", instrumentId, OCCURRED, INGESTED, payload);
    }
}
