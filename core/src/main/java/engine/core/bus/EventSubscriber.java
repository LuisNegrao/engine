package engine.core.bus;

import java.util.List;

/**
 * Subscribes components to the message bus by <em>what</em> they want — payload type and instrument —
 * never by stream name. The abstraction the consuming half of the system depends on; like {@link
 * EventPublisher} it imports nothing transport-specific. The consumer-group name and this consumer's
 * stable identity are constructor state of the implementation, not {@code subscribe} arguments: one
 * component uses one group across every stream it reads (ADR 0002), so the group belongs to the
 * subscriber instance exactly as the Redis URI belongs to the publisher instance.
 *
 * <p>The delivery contract every implementation inherits — <strong>read this before writing a
 * handler</strong>:
 *
 * <ol>
 *   <li><strong>At-least-once, so handlers MUST be idempotent.</strong> Any event may be delivered
 *       more than once — a crash between handling and acknowledgement, a claim race, a publisher
 *       retry. Redeliveries carry the same {@code eventId}; that is the deduplication key, and
 *       deduplicating is the handler's job.
 *   <li><strong>One dispatch thread; order within a stream, not across.</strong> Each subscription
 *       owns a single dispatch thread and the handler is only ever invoked from it, never
 *       concurrently with itself. Events from one stream arrive in publish order; there is no ordering
 *       across streams (ADR 0002 §5), and a <em>redelivered</em> event arrives out of order even
 *       within its own stream.
 *   <li><strong>Blocking is allowed and is the backpressure.</strong> A slow handler stalls only its
 *       own subscription. The resulting lag is observable via {@link Subscription#lag()}, and the
 *       subscription's {@link SubscribeOptions.LagPolicy} decides what happens next. A handler slower
 *       than the claim min-idle window can be seen as a duplicate by a peer consumer — which point 1
 *       already requires handling.
 *   <li><strong>Throwing means retry, then dead-letter.</strong> A handler that throws is not
 *       acknowledged and the event is redelivered with backoff; one that keeps failing is parked on a
 *       dead-letter stream with the error attached, acknowledged, and never redelivered — processing
 *       continues past it.
 *   <li><strong>{@code subscribe} fails loudly at wiring time.</strong> An invalid selector (unknown
 *       shape, a partitioned type with no instrument, a non-{@code md.*} stream under {@link
 *       SubscribeOptions.SkipToLatest}) throws synchronously from {@code subscribe} — a mis-wired
 *       consumer fails at startup, not silently at runtime. {@link Subscription#close()} stops
 *       dispatch after the in-flight event completes, within a bounded wait; {@link #close()} closes
 *       every subscription.
 * </ol>
 */
public interface EventSubscriber extends AutoCloseable {

    /**
     * Subscribes to every stream carrying one of {@code selectors}, delivering matching events to
     * {@code handler} under this subscriber's consumer group. Throws synchronously on an invalid
     * selector or an illegal lag policy (see the class contract); the returned {@link Subscription}
     * is already live.
     */
    Subscription subscribe(List<EventSelector> selectors, SubscribeOptions options, EventHandler handler);

    /** Closes every open subscription and releases transport resources. */
    @Override
    void close();
}
