package engine.bus;

import engine.core.event.Event;
import java.util.concurrent.CompletionStage;

public interface EventPublisher extends AutoCloseable {

    CompletionStage<Void> publish(Event event);

    @Override
    void close();
}
