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

    @Override
    public CompletionStage<Void> publish(Event event) {
        Objects.requireNonNull(event, "event must not be null");
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
            if (closed) {
                return false;
            }
            for (EventSelector selector : selectors) {
                if (matches(selector, event)) {
                    return true;
                }
            }
            return false;
        }

        private boolean matches(EventSelector selector, Event event) {
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

        @Override
        public void close() {
            closed = true;
            subscriptions.remove(this);
        }
    }
}
