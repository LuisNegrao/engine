package engine.core.bus;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionStage;

/**
 * A running replay handle: wait for the recorded range to drain, or stop it early. Returned by
 * {@link EventSubscriber#replay} already running on its own dedicated thread.
 *
 * <p>Unlike a live {@link Subscription}, a replay <em>ends</em>. {@link #done()} is that
 * end-of-range signal: it completes with the number of events delivered when the range is exhausted,
 * and completes exceptionally on any abort — a range trimmed away mid-flight ({@link
 * ReplayRetentionException}), a throwing handler or undecodable entry (the cause chained), or {@link
 * #close()} before exhaustion ({@link CancellationException}). A backtest blocks on {@code done()}
 * and learns both that the data ran out and how much of it there was.
 */
public interface Replay extends AutoCloseable {

    /**
     * Completes with the delivered event count when the range is exhausted, or exceptionally on
     * abort. Never completes more than once; every exit path — exhaustion, retention trim, handler
     * throw, {@link #close()} — completes it exactly once.
     */
    CompletionStage<Long> done();

    /**
     * Stops the replay after the in-flight event finishes, within a bounded wait, and releases the
     * replay's dedicated connection and thread. If the range has not yet drained, completes {@link
     * #done()} exceptionally with a {@link CancellationException}; a no-op once the replay has
     * already completed. Narrowed to throw no checked exception so try-with-resources stays clean.
     */
    @Override
    void close();
}
