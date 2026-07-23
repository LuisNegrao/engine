package engine.core.bus;

import java.util.Objects;

/**
 * Wiring-time options for a subscription: where a freshly created consumer group starts reading, and
 * what the subscription does when it falls behind.
 *
 * @param startPosition applied only when the group is first created; an existing group always resumes
 *     from its own committed state (see {@link StartPosition})
 * @param lagPolicy what to do when the subscription is behind (see {@link LagPolicy})
 */
public record SubscribeOptions(StartPosition startPosition, LagPolicy lagPolicy) {

    public SubscribeOptions {
        Objects.requireNonNull(startPosition, "startPosition must not be null");
        Objects.requireNonNull(lagPolicy, "lagPolicy must not be null");
    }

    /**
     * Where a newly created consumer group begins reading. Caller-chosen with no default: a strategy
     * joining mid-session wants {@link #LATEST}, an archiver backfilling wants {@link #EARLIEST}, and
     * defaulting either way silently mis-serves the other. Ignored once the group exists — the caller
     * never manages offsets.
     */
    public enum StartPosition {
        /** Only events published after the group is created ({@code $}). */
        LATEST,
        /** Every event still retained in the stream ({@code 0}). */
        EARLIEST
    }

    /**
     * What a subscription does when it falls behind. Sealed so the exhaustive set is {@link
     * ProcessAll} and {@link SkipToLatest}.
     */
    public sealed interface LagPolicy permits ProcessAll, SkipToLatest {}

    /** Never skip — deliver every retained event even under unbounded lag. The default. */
    public record ProcessAll() implements LagPolicy {}

    /**
     * Jump to the tail once lag crosses {@code threshold}, abandoning the backlog. Only market-data
     * ({@code md.*}) subscriptions may use this — a strategy computing on ten-minute-old ticks is
     * worse than one that gaps and resumes fresh, whereas a missed fill is a corrupted position. The
     * subscriber enforces the {@code md.*}-only rule structurally, at subscribe time.
     *
     * @param threshold lag (undelivered + pending) at which the subscription skips forward; positive
     */
    public record SkipToLatest(long threshold) implements LagPolicy {
        public SkipToLatest {
            if (threshold <= 0) {
                throw new IllegalArgumentException("threshold must be positive, was: " + threshold);
            }
        }
    }

    /** Options that start a new group at {@code startPosition} and process the whole backlog. */
    public static SubscribeOptions of(StartPosition startPosition) {
        return new SubscribeOptions(startPosition, new ProcessAll());
    }

    /** Options with an explicit lag policy. */
    public static SubscribeOptions of(StartPosition startPosition, LagPolicy lagPolicy) {
        return new SubscribeOptions(startPosition, lagPolicy);
    }
}
