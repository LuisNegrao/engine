package engine.core.bus;

import engine.core.event.Event;
import java.util.concurrent.CompletionStage;

/**
 * Publishes {@link Event}s to the message bus. The abstraction components depend on — they publish
 * any event type importing nothing transport-specific, never Redis/Lettuce or Kafka.
 *
 * <p>The contract every implementation inherits:
 *
 * <ul>
 *   <li><strong>Non-blocking.</strong> {@link #publish} returns immediately; it never blocks the
 *       caller waiting on the transport.
 *   <li><strong>Bounded in-flight window is the only buffer.</strong> The sole buffering is a
 *       bounded in-flight window; when it is full, {@code publish} fails immediately.
 *       Implementations never queue events while the transport is unavailable.
 *   <li><strong>Thread-safe.</strong> Implementations may be published to concurrently from many
 *       threads.
 *   <li><strong>One error channel.</strong> All failures — transport unavailable, timeout, a full
 *       window, even routing errors — arrive on the returned stage, never as a synchronous throw.
 *   <li><strong>A failed stage does not prove the event did not land.</strong> A command timeout can
 *       lose the response rather than the write, so a retry can duplicate the event (at-least-once
 *       delivery). Deduplication on {@code eventId}/{@code clientOrderId} is downstream's concern.
 *   <li><strong>{@link #close} drains, then releases.</strong> It waits for in-flight publishes with
 *       a bounded deadline, then releases transport resources.
 * </ul>
 */
public interface EventPublisher extends AutoCloseable {

    /**
     * Publishes an event without blocking. Failures arrive on the returned stage, never as a
     * synchronous throw; a completed stage means the write was acknowledged, a failed stage means it
     * may or may not have landed (see the class contract on at-least-once delivery).
     */
    CompletionStage<Void> publish(Event event);

    /**
     * Waits for in-flight publishes to complete within a bounded deadline, then releases transport
     * resources. Narrowed to throw no checked exception so try-with-resources stays clean.
     */
    @Override
    void close();
}
