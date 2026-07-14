package engine.core.bus;

import engine.core.event.Event;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;

public class InMemoryEventPublisher implements EventPublisher {
    private final List<Event> published = new CopyOnWriteArrayList<>();
    private volatile RuntimeException failure;

    @Override
    public CompletionStage<Void> publish(Event event) {
        RuntimeException exception = failure;

        if (exception != null) {
            return CompletableFuture.failedStage(exception);
        }

        published.add(event);
        return CompletableFuture.completedStage(null);
    }

    /** Everything published so far, in publish order. */
    public List<Event> published() {
        return List.copyOf(published);
    }

    /** Makes all subsequent publishes fail with {@code failure}; pass {@code null} to heal. */
    public void failWith(RuntimeException failure) {
        this.failure = failure;
    }

    @Override
    public void close() {}
}
