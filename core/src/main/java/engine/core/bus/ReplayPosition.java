package engine.core.bus;

import engine.core.event.Payload;
import java.time.Instant;
import java.util.Objects;

/**
 * One edge of a {@link ReplayRange}: where in a stream's history a replay starts or stops. Three
 * shapes, sealed so the exhaustive set is fixed — the bus maps each to a Redis stream ID:
 *
 * <ul>
 *   <li>{@link #earliest()} — the oldest event the bus still retains. Legal only as a start;
 *       "oldest retained" is meaningless as an end.
 *   <li>{@link #at(Instant)} — an <em>ingest-time</em> boundary at millisecond precision. Addresses
 *       when the bus received events, not their {@code occurredAt}; the two track within publish
 *       latency in normal operation and diverge after a feed outage (see {@link
 *       EventSubscriber#replay}).
 *   <li>{@link #offset(String)} — an opaque position token, previously handed out by the transport.
 *       Core keeps it a bare {@code String}; the bus interprets and validates it as a Redis stream
 *       ID and rejects the shape at wiring time. An offset boundary addresses a single stream, so a
 *       replay using one may resolve to exactly one stream.
 * </ul>
 *
 * <p>Core validates only what it can without the transport: a timestamp is present, a token is
 * non-blank. The stream-ID shape of a token and its retention live in {@code bus}.
 */
public sealed interface ReplayPosition permits ReplayPosition.Earliest, ReplayPosition.At, ReplayPosition.Offset {

    /** The oldest event still retained on each resolved stream. Valid only as a range start. */
    static ReplayPosition earliest() {
        return new Earliest();
    }

    /** An ingest-time boundary, at millisecond precision; {@code instant} non-null. */
    static ReplayPosition at(Instant instant) {
        return new At(instant);
    }

    /** An opaque, transport-issued position token; {@code token} non-blank, shape checked by the bus. */
    static ReplayPosition offset(String token) {
        return new Offset(token);
    }

    /** The oldest retained event on each stream. */
    record Earliest() implements ReplayPosition {}

    /**
     * An ingest-time boundary.
     *
     * @param instant when the bus received the boundary event, at millisecond precision; non-null
     */
    record At(Instant instant) implements ReplayPosition {
        public At {
            Objects.requireNonNull(instant, "instant must not be null");
        }
    }

    /**
     * An opaque, transport-issued position token.
     *
     * @param token a bus-specific position; non-blank, its shape validated by the bus
     */
    record Offset(String token) implements ReplayPosition {
        public Offset {
            Payload.requireText(token, "token");
        }
    }
}
