package engine.bus;

import engine.core.event.Bar;
import engine.core.event.Command;
import engine.core.event.Event;
import engine.core.event.Fill;
import engine.core.event.Metric;
import engine.core.event.OrderIntent;
import engine.core.event.QuoteTick;
import engine.core.event.Signal;
import engine.core.event.TradeTick;
import java.time.Duration;
import java.util.Map;

/**
 * The ADR 0002 §3 stream table as code: which Redis stream an event is published on.
 *
 * <p>Stream names are transport addresses and deliberately <em>not</em> derived from the wire
 * {@code eventType} strings — this class is the single source of truth, and deriving names by
 * string-mangling eventTypes would turn a rename into silent data loss. Only bus code (publisher,
 * subscriber, trimmer) may name streams; components publish through {@code
 * engine.core.bus.EventPublisher} and never see these.
 */
public final class StreamNames {

    /** ADR 0002 §3 bar-interval vocabulary. Adding an interval means amending the ADR first. */
    private static final Map<Duration, String> BAR_INTERVALS = Map.of(
            Duration.ofMinutes(1), "1m",
            Duration.ofMinutes(5), "5m",
            Duration.ofMinutes(15), "15m",
            Duration.ofHours(1), "1h",
            Duration.ofHours(4), "4h",
            Duration.ofDays(1), "1d");

    private StreamNames() {}

    /**
     * Resolves the stream for an event per the ADR 0002 §3 table. The switch is exhaustive over the
     * sealed {@link engine.core.event.Payload} with no {@code default}, so a new payload type fails
     * compilation here until it is given a stream.
     *
     * @throws IllegalArgumentException if an instrument-scoped payload (tick, bar) has no
     *     {@code instrumentId}, or a {@link Bar} interval is outside the ADR vocabulary
     */
    public static String streamFor(Event event) {
        return switch (event.payload()) {
            case TradeTick t -> "md.tick.trade." + requireInstrument(event);
            case QuoteTick q -> "md.tick.quote." + requireInstrument(event);
            case Bar b -> "md.bar." + intervalToken(b.interval()) + "." + requireInstrument(event);
            case Signal s -> "signals";
            case OrderIntent o -> "orders.intents";
            case Fill f -> "orders.fills";
            case Metric m -> "metrics";
            case Command c -> "commands";
        };
    }

    private static String requireInstrument(Event event) {
        if (event.instrumentId() == null) {
            throw new IllegalArgumentException("instrumentId is required to route a "
                    + event.payload().getClass().getSimpleName() + " but was null");
        }
        return event.instrumentId().toString();
    }

    private static String intervalToken(Duration interval) {
        String token = BAR_INTERVALS.get(interval);
        if (token == null) {
            throw new IllegalArgumentException(
                    "bar interval " + interval + " is not in the ADR 0002 §3 vocabulary (1m, 5m, 15m, 1h, 4h, 1d)");
        }
        return token;
    }
}
