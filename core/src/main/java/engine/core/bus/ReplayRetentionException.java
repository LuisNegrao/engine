package engine.core.bus;

/**
 * Thrown when a replay asks for a range the bus no longer holds. Raised synchronously from {@link
 * EventSubscriber#replay} when the requested start is already below the retention floor, or used to
 * complete {@link Replay#done()} exceptionally when a trim overtakes a running replay and deletes
 * entries past its cursor. Either way the signal is loud: a replay never silently computes across a
 * hole in its data.
 *
 * <p>Unchecked because it reports a data-availability fact discovered at runtime, not a condition a
 * caller can be forced to pre-handle — a backtest either reruns against a live-enough window or
 * sources the range from the Historical Data Store (NEG-7).
 */
public class ReplayRetentionException extends RuntimeException {

    public ReplayRetentionException(String message) {
        super(message);
    }

    public ReplayRetentionException(String message, Throwable cause) {
        super(message, cause);
    }
}
