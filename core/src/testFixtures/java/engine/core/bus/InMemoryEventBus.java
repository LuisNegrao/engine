package engine.core.bus;

import engine.core.event.Bar;
import engine.core.event.Event;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A Redis-free {@link EventPublisher} + {@link EventSubscriber} for wiring one object into a
 * component under test and exercising its whole publish/subscribe surface — including poison handling
 * — with zero infrastructure. Delivery is <strong>synchronous on the publishing thread</strong>:
 * {@link #publish} returns only after every matching handler has run.
 *
 * <p>Each subscription behaves as its own consumer group — every subscription whose selectors match
 * an event receives a full copy, exactly like independent Redis groups. There is deliberately no
 * load-sharing simulation (several consumers splitting one group's stream): that is Redis group
 * mechanics, tested in {@code bus}. Selectors are matched directly on payload type, instrument, and
 * bar interval — there are no stream names here, because stream names do not exist in {@code core}.
 *
 * <p>A throwing handler is retried in place up to {@link #MAX_ATTEMPTS} times; an event that still
 * fails is moved to the inspectable {@link #deadLetters()} list with its last exception, mirroring the
 * Redis subscriber parking poison on {@code dlq.*}. This fixture models neither concurrency nor
 * threads; it targets single-threaded test drivers, and is reentrancy-safe so a handler may publish —
 * its events queue and drain after the current one completes, never recursively.
 *
 * <p>{@code InMemoryEventPublisher} remains for publish-only tests and for simulating publish
 * failures ({@code failWith}), which this bus deliberately does not model.
 */
public class InMemoryEventBus implements EventPublisher, EventSubscriber {

    /**
     * Total handler attempts before an event is dead-lettered; mirrors {@code
     * SubscriberTuning.standard().maxDeliveries()} on the Redis side.
     */
    public static final int MAX_ATTEMPTS = 5;

    private final List<InMemorySubscription> subscriptions = new CopyOnWriteArrayList<>();
    private final List<DeadLetter> deadLetters = new CopyOnWriteArrayList<>();
    private final List<Event> recorded = new CopyOnWriteArrayList<>();

    @Override
    public CompletionStage<Void> publish(Event event) {
        Objects.requireNonNull(event, "event must not be null");
        recorded.add(event);
        for (InMemorySubscription subscription : subscriptions) {
            if (subscription.matches(event)) {
                subscription.deliver(event);
            }
        }
        return CompletableFuture.completedStage(null);
    }

    @Override
    public Subscription subscribe(List<EventSelector> selectors, SubscribeOptions options, EventHandler handler) {
        Objects.requireNonNull(options, "options must not be null");
        Objects.requireNonNull(handler, "handler must not be null");
        if (selectors == null || selectors.isEmpty()) {
            throw new IllegalArgumentException("at least one selector is required");
        }
        InMemorySubscription subscription = new InMemorySubscription(List.copyOf(selectors), handler);
        subscriptions.add(subscription);
        return subscription;
    }

    /**
     * Re-drives this bus's recorded history through {@code handler} — the same selector matching and
     * the same abort semantics as the Redis subscriber, minus the threads. Delivery is
     * <strong>synchronous on the calling thread</strong>: every matching event in range has been
     * handled before {@code replay} returns, and the returned {@link Replay}'s {@link Replay#done()}
     * is already completed — with the delivered count on success, or exceptionally with the handler's
     * exception the moment one throws (no retry, no dead-lettering, remaining events undelivered).
     *
     * <p>The range is resolved against the ordered recording: {@link ReplayPosition#at(java.time.Instant)}
     * bounds are inclusive on {@code ingestedAt}, an {@link ReplayPosition#offset(String) offset}
     * token is a decimal index into that recording, {@link ReplayPosition#earliest()} is index zero,
     * and an absent end is the recording's tail at call time. The fixture retains everything forever,
     * so {@link ReplayRetentionException} is unreachable here — component tests use replay to
     * re-drive traffic, not to exercise retention edges (those live in {@code bus}).
     */
    @Override
    public Replay replay(List<EventSelector> selectors, ReplayRange range, EventHandler handler) {
        Objects.requireNonNull(range, "range must not be null");
        Objects.requireNonNull(handler, "handler must not be null");
        if (selectors == null || selectors.isEmpty()) {
            throw new IllegalArgumentException("at least one selector is required");
        }
        List<EventSelector> selectorList = List.copyOf(selectors);
        List<Event> snapshot = List.copyOf(recorded);
        int startIndex = startIndex(range.start(), snapshot);
        int endIndex = endIndexInclusive(range.maybeEnd().orElse(null), snapshot);

        CompletableFuture<Long> done = new CompletableFuture<>();
        try {
            long delivered = 0;
            for (int i = startIndex; i <= endIndex && i < snapshot.size(); i++) {
                Event event = snapshot.get(i);
                if (matchesAny(selectorList, event)) {
                    handler.handle(event);
                    delivered++;
                }
            }
            done.complete(delivered);
        } catch (Exception e) {
            done.completeExceptionally(e);
        }
        return new CompletedReplay(done);
    }

    /** Inclusive lower index into the recording for a start position. */
    private static int startIndex(ReplayPosition start, List<Event> log) {
        return switch (start) {
            case ReplayPosition.Earliest ignored -> 0;
            case ReplayPosition.At at -> {
                int i = 0;
                while (i < log.size() && log.get(i).ingestedAt().isBefore(at.instant())) {
                    i++;
                }
                yield i;
            }
            case ReplayPosition.Offset offset -> parseOffset(offset.token());
        };
    }

    /** Inclusive upper index into the recording; an absent ({@code null}) end is the tail at call time. */
    private static int endIndexInclusive(ReplayPosition end, List<Event> log) {
        return switch (end) {
            case null -> log.size() - 1;
            case ReplayPosition.At at -> {
                int i = log.size() - 1;
                while (i >= 0 && log.get(i).ingestedAt().isAfter(at.instant())) {
                    i--;
                }
                yield i;
            }
            case ReplayPosition.Offset offset -> parseOffset(offset.token());
            case ReplayPosition.Earliest ignored ->
            // Rejected by ReplayRange, so unreachable — kept for switch exhaustiveness.
            throw new IllegalArgumentException("earliest() is not a valid range end");
        };
    }

    private static int parseOffset(String token) {
        try {
            int index = Integer.parseInt(token);
            if (index < 0) {
                throw new IllegalArgumentException("offset token must be a non-negative log index, was: " + token);
            }
            return index;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("offset token is not a decimal log index: " + token, e);
        }
    }

    private static boolean matchesAny(List<EventSelector> selectors, Event event) {
        for (EventSelector selector : selectors) {
            if (matches(selector, event)) {
                return true;
            }
        }
        return false;
    }

    private static boolean matches(EventSelector selector, Event event) {
        if (!selector.payloadType().isInstance(event.payload())) {
            return false;
        }
        if (selector.maybeInstrumentId().isPresent()
                && !selector.maybeInstrumentId().get().equals(event.instrumentId())) {
            return false;
        }
        if (selector.maybeBarInterval().isPresent()) {
            return event.payload() instanceof Bar bar
                    && selector.maybeBarInterval().get().equals(bar.interval());
        }
        return true;
    }

    /** Events that exhausted {@link #MAX_ATTEMPTS} handler attempts, in the order they gave up. */
    public List<DeadLetter> deadLetters() {
        return List.copyOf(deadLetters);
    }

    @Override
    public void close() {
        for (InMemorySubscription subscription : subscriptions) {
            subscription.close();
        }
    }

    /**
     * A synchronous replay handle: {@link #done()} is already terminal when this is returned — the
     * whole range was delivered (or aborted) before {@code replay} returned — so {@link #close()} is
     * always a no-op.
     */
    private record CompletedReplay(CompletableFuture<Long> done) implements Replay {
        @Override
        public void close() {
            // Delivery already ran to completion synchronously; nothing to stop.
        }
    }

    /** An event that exhausted {@link #MAX_ATTEMPTS} handler attempts, paired with its last failure. */
    public record DeadLetter(Event event, Exception failure) {
        public DeadLetter {
            Objects.requireNonNull(event, "event must not be null");
            Objects.requireNonNull(failure, "failure must not be null");
        }
    }

    private final class InMemorySubscription implements Subscription {

        private final List<EventSelector> selectors;
        private final EventHandler handler;
        private final Deque<Event> pending = new ArrayDeque<>();
        private boolean draining;
        private volatile boolean closed;

        InMemorySubscription(List<EventSelector> selectors, EventHandler handler) {
            this.selectors = selectors;
            this.handler = handler;
        }

        boolean matches(Event event) {
            return !closed && matchesAny(selectors, event);
        }

        /**
         * Enqueues then drains synchronously. A reentrant call (a handler publishing back to this
         * subscription) only appends: the outer drain loop delivers it next, so dispatch never
         * recurses and per-subscription order is preserved.
         */
        void deliver(Event event) {
            pending.add(event);
            if (draining) {
                return;
            }
            draining = true;
            try {
                Event next;
                while ((next = pending.poll()) != null) {
                    dispatch(next);
                }
            } finally {
                draining = false;
            }
        }

        private void dispatch(Event event) {
            Exception lastFailure = null;
            for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
                try {
                    handler.handle(event);
                    return;
                } catch (Exception e) {
                    lastFailure = e;
                }
            }
            deadLetters.add(new DeadLetter(event, lastFailure));
        }

        /** Always {@code 0}: delivery is synchronous on publish, so nothing is ever left pending. */
        @Override
        public long lag() {
            return 0;
        }

        /** Always {@code 0}: synchronous delivery never lags, so it never skips. */
        @Override
        public long skipCount() {
            return 0;
        }

        @Override
        public void close() {
            closed = true;
            subscriptions.remove(this);
        }
    }
}
