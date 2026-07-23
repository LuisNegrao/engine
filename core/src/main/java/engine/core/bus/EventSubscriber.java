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
 *
 * <p>Beyond live consumption, {@link #replay} re-drives a recorded range of the same streams through
 * the same {@link EventHandler} path — see its contract below.
 */
public interface EventSubscriber extends AutoCloseable {

    /**
     * Subscribes to every stream carrying one of {@code selectors}, delivering matching events to
     * {@code handler} under this subscriber's consumer group. Throws synchronously on an invalid
     * selector or an illegal lag policy (see the class contract); the returned {@link Subscription}
     * is already live.
     */
    Subscription subscribe(List<EventSelector> selectors, SubscribeOptions options, EventHandler handler);

    /**
     * Replays a bounded, recorded range of the streams carrying {@code selectors} through {@code
     * handler} — the same handler type, the same selectors, the same delivery path as {@link
     * #subscribe}, so a handler cannot tell replayed traffic from live. Returns a {@link Replay}
     * already running on its own dedicated thread; block on {@link Replay#done()} for the
     * end-of-range signal. Throws synchronously at wiring time on an invalid selector or an offset
     * range that resolves to more than one stream (see the class contract), and throws {@link
     * ReplayRetentionException} up front when the requested start is already below the retention
     * floor.
     *
     * <p>The replay contract, distinct from live consumption:
     *
     * <ol>
     *   <li><strong>Pure reader.</strong> A replay never writes to the bus: no consumer group, no
     *       acknowledgements, no dead-lettering. Live consumption elsewhere is unaffected by any
     *       number of concurrent replays.
     *   <li><strong>Identical delivery.</strong> The same {@link EventHandler} type, invoked from a
     *       single dedicated thread, never concurrently with itself; events carry their original
     *       envelopes — same {@code eventId}, same {@code occurredAt}, nothing re-minted.
     *   <li><strong>Positions are ingest-time.</strong> {@link ReplayPosition#at(java.time.Instant)}
     *       addresses when the bus <em>received</em> events, at millisecond precision — within
     *       publish latency of {@code occurredAt} in normal operation, arbitrarily far after a feed
     *       outage. Delivered events are never filtered by {@code occurredAt}; event-time-exact or
     *       beyond-retention replay belongs to the Historical Data Store (NEG-7).
     *   <li><strong>Deterministic and bounded.</strong> Per-stream publish order, streams merged by
     *       ingest position with a fixed tie-break, so an identical bounded replay over untrimmed
     *       data yields the identical sequence. Handlers must not rely on cross-stream order — live
     *       never honors it. {@link Replay#done()} completes with the delivered count at
     *       end-of-range.
     *   <li><strong>Fail-fast.</strong> A range the bus no longer holds throws {@link
     *       ReplayRetentionException} up front, or completes {@link Replay#done()} exceptionally if
     *       trimming overtakes a running replay — never silent partial data. A throwing handler or
     *       undecodable entry aborts the replay, completing {@code done()} exceptionally with the
     *       cause; {@link Replay#close()} before exhaustion completes it with a {@link
     *       java.util.concurrent.CancellationException}.
     * </ol>
     */
    Replay replay(List<EventSelector> selectors, ReplayRange range, EventHandler handler);

    /** Closes every open subscription and releases transport resources. */
    @Override
    void close();
}
