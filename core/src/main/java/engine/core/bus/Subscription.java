package engine.core.bus;

/**
 * A live subscription handle: observe how far behind it is, and stop it. Returned by {@link
 * EventSubscriber#subscribe} already running.
 */
public interface Subscription extends AutoCloseable {

    /**
     * Current lag for this subscription — undelivered events plus delivered-but-unacknowledged ones,
     * summed across every stream the subscription reads. Zero means fully caught up.
     */
    long lag();

    /**
     * Stops dispatch after the in-flight event finishes, within a bounded wait, and releases the
     * subscription's resources. Narrowed to throw no checked exception so try-with-resources stays
     * clean.
     */
    @Override
    void close();
}
