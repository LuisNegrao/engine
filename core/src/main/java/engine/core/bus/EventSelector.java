package engine.core.bus;

import engine.core.event.Bar;
import engine.core.event.InstrumentId;
import engine.core.event.Payload;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/**
 * What a consumer wants delivered, expressed in domain terms: a payload type, optionally narrowed to
 * one instrument, and — for {@link Bar} — one bar interval. Components address the bus with these and
 * never with stream names; translating a selector to the Redis stream(s) that carry it is bus code's
 * job (modules.md litmus #3).
 *
 * <p>Redis Streams has no pattern-subscribe, so per ADR 0002 §1 a subscription is an
 * <em>explicit</em> list of selectors — one per (type, instrument) — not a wildcard. This record
 * carries only what {@code core} can validate on its own; whether a given type is
 * instrument-partitioned (and therefore whether the instrument is mandatory) is topology knowledge
 * that lives in {@code bus} and is enforced at subscribe time.
 *
 * @param payloadType the payload type to receive; non-null
 * @param instrumentId narrow to this instrument, or {@code null} for every instrument of the type
 * @param barInterval for {@link Bar} selectors, the bar width; {@code null} otherwise
 */
public record EventSelector(Class<? extends Payload> payloadType, InstrumentId instrumentId, Duration barInterval) {

    public EventSelector {
        Objects.requireNonNull(payloadType, "payloadType must not be null");
        if (barInterval != null) {
            if (payloadType != Bar.class) {
                throw new IllegalArgumentException(
                        "barInterval is only meaningful for Bar selectors, not " + payloadType.getSimpleName());
            }
            if (barInterval.isZero() || barInterval.isNegative()) {
                throw new IllegalArgumentException("barInterval must be positive, was: " + barInterval);
            }
        }
    }

    /** Every instrument of a payload type, e.g. {@code of(TradeTick.class)}. */
    public static EventSelector of(Class<? extends Payload> payloadType) {
        return new EventSelector(payloadType, null, null);
    }

    /** One instrument of a payload type, e.g. {@code of(TradeTick.class, btcUsdt)}. */
    public static EventSelector of(Class<? extends Payload> payloadType, InstrumentId instrumentId) {
        return new EventSelector(payloadType, instrumentId, null);
    }

    /** {@link Bar}s of one interval for one instrument. */
    public static EventSelector bars(Duration barInterval, InstrumentId instrumentId) {
        return new EventSelector(Bar.class, instrumentId, barInterval);
    }

    /** The instrument this selector narrows to, or empty to match every instrument of the type. */
    public Optional<InstrumentId> maybeInstrumentId() {
        return Optional.ofNullable(instrumentId);
    }

    /** The bar interval for a {@link Bar} selector, or empty for non-{@code Bar} selectors. */
    public Optional<Duration> maybeBarInterval() {
        return Optional.ofNullable(barInterval);
    }
}
