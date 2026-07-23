package engine.core.bus;

import engine.core.event.Event;

/**
 * Handles one event delivered to a subscription. A <em>normal return</em> means the event was handled
 * and will be acknowledged; <em>any thrown exception</em> means failure — the event is not
 * acknowledged and will be redelivered, eventually landing on a dead-letter stream if it keeps
 * failing (see {@link EventSubscriber} for the full delivery contract).
 *
 * <p>Invoked from the subscription's single dispatch thread, never concurrently with itself, and may
 * block: a slow handler is the backpressure and stalls only its own subscription.
 */
@FunctionalInterface
public interface EventHandler {

    /** Handles the event; return normally to acknowledge, throw to trigger redelivery. */
    void handle(Event event) throws Exception;
}
