package engine.core.bus;

import engine.core.bus.ReplayPosition.At;
import engine.core.bus.ReplayPosition.Earliest;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * A bounded slice of stream history to replay: an inclusive {@code start} and an optional {@code
 * end}. An <em>absent</em> end means "everything currently retained, then stop" — the tail is
 * sampled once when the replay begins and delivery terminates there, never following into live
 * traffic. A replay is always bounded; tailing live data is what {@link EventSubscriber#subscribe}
 * is for.
 *
 * <p>Core validates what it can without the transport: {@code start} is non-null, {@link
 * ReplayPosition.Earliest} is illegal as an end (its meaning "oldest retained" has no referent at
 * the far edge), and when both edges are {@link ReplayPosition.At timestamps} the end may not
 * precede the start. Whether an {@link ReplayPosition.Offset offset} token resolves, and whether the
 * range still lies within retention, are the bus's to enforce at wiring time.
 *
 * @param start the inclusive lower edge; non-null, never {@code Earliest} rejected here
 * @param end the inclusive upper edge, or {@code null} to replay through the tail retained at start
 */
public record ReplayRange(ReplayPosition start, ReplayPosition end) {

    public ReplayRange {
        Objects.requireNonNull(start, "start must not be null");
        if (end instanceof Earliest) {
            throw new IllegalArgumentException("earliest() is not a valid range end");
        }
        if (start instanceof At startAt
                && end instanceof At endAt
                && endAt.instant().isBefore(startAt.instant())) {
            throw new IllegalArgumentException(
                    "range end " + endAt.instant() + " is before start " + startAt.instant());
        }
    }

    /** From {@code start} through the tail retained when the replay begins. */
    public static ReplayRange from(ReplayPosition start) {
        return new ReplayRange(start, null);
    }

    /** From {@code start} through {@code end}, both inclusive. */
    public static ReplayRange between(ReplayPosition start, ReplayPosition end) {
        return new ReplayRange(start, end);
    }

    /** Ingest-time convenience: from {@code start} through the tail retained when the replay begins. */
    public static ReplayRange from(Instant start) {
        return new ReplayRange(ReplayPosition.at(start), null);
    }

    /** Ingest-time convenience: from {@code start} through {@code end}, both inclusive. */
    public static ReplayRange between(Instant start, Instant end) {
        return new ReplayRange(ReplayPosition.at(start), ReplayPosition.at(end));
    }

    /** The upper edge, or empty to replay through the tail retained when the replay begins. */
    public Optional<ReplayPosition> maybeEnd() {
        return Optional.ofNullable(end);
    }
}
