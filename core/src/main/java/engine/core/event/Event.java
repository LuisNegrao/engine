package engine.core.event;

import java.time.Clock;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * The uniform envelope wrapping every payload on the bus.
 *
 * <p>{@code instrumentId} is nullable — absent for instrument-agnostic payloads such as {@link
 * Command}. The wire discriminators {@code eventType}/{@code schemaVersion} are intentionally
 * <em>not</em> stored here: they are derived from the payload type via the {@code
 * engine.core.serde} registry at encode time, keeping envelope fields in exactly one place.
 *
 * @param eventId unique id for this event; non-null
 * @param source producing component, e.g. {@code "binance-feed"}; non-blank
 * @param instrumentId the instrument this event concerns, or {@code null} if not instrument-scoped
 * @param occurredAt when the event happened at the source
 * @param ingestedAt when the engine received it; the {@code occurredAt}→{@code ingestedAt} gap is
 *     the feed-latency metric
 * @param payload the typed body
 */
public record Event(
        UUID eventId,
        String source,
        InstrumentId instrumentId,
        Instant occurredAt,
        Instant ingestedAt,
        Payload payload) {

    public Event {
        Objects.requireNonNull(eventId, "eventId must not be null");
        Payload.requireText(source, "source");
        Objects.requireNonNull(occurredAt, "occurredAt must not be null");
        Objects.requireNonNull(ingestedAt, "ingestedAt must not be null");
        Objects.requireNonNull(payload, "payload must not be null");
        // instrumentId is nullable by design.
    }

    /**
     * Producing-path factory: assigns a random {@code eventId} and stamps {@code ingestedAt} from
     * the system clock.
     */
    public static Event of(String source, InstrumentId instrumentId, Instant occurredAt, Payload payload) {
        return of(source, instrumentId, occurredAt, payload, Clock.systemUTC());
    }

    /** Testable variant taking an explicit {@link Clock} for {@code ingestedAt}. */
    public static Event of(String source, InstrumentId instrumentId, Instant occurredAt, Payload payload, Clock clock) {
        Objects.requireNonNull(clock, "clock must not be null");
        return new Event(UUID.randomUUID(), source, instrumentId, occurredAt, clock.instant(), payload);
    }
}
