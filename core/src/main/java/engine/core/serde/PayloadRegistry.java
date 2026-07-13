package engine.core.serde;

import engine.core.event.Bar;
import engine.core.event.Command;
import engine.core.event.Fill;
import engine.core.event.Metric;
import engine.core.event.OrderIntent;
import engine.core.event.Payload;
import engine.core.event.QuoteTick;
import engine.core.event.Signal;
import engine.core.event.TradeTick;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Bidirectional mapping between wire {@code eventType} strings and payload record classes (with a
 * {@code schemaVersion}). This is the payload discriminator — deliberately an explicit table rather
 * than {@code @JsonTypeInfo}, which would couple the records to Jackson and reopen the default-
 * typing CVE door. A lookup miss on decode is the natural forward-compat signal (unknown type).
 *
 * <p>The registry is injected into {@link JsonEventCodec}, so tests can supply alternate versions of
 * the same {@code eventType} without touching {@link #standard()}.
 */
public final class PayloadRegistry {

    /**
     * A single registration: the wire {@code eventType}, its {@code schemaVersion}, and the record
     * class that carries it.
     */
    public record PayloadType(String eventType, int schemaVersion, Class<? extends Payload> type) {
        public PayloadType {
            if (eventType == null || eventType.isBlank()) {
                throw new IllegalArgumentException("eventType must be non-blank");
            }
            if (schemaVersion < 1) {
                throw new IllegalArgumentException("schemaVersion must be >= 1, was: " + schemaVersion);
            }
            Objects.requireNonNull(type, "type must not be null");
        }
    }

    private final Map<String, PayloadType> byEventType;
    private final Map<Class<? extends Payload>, PayloadType> byClass;

    public PayloadRegistry(Collection<PayloadType> types) {
        Objects.requireNonNull(types, "types must not be null");
        Map<String, PayloadType> byName = new HashMap<>();
        Map<Class<? extends Payload>, PayloadType> byCls = new HashMap<>();
        for (PayloadType pt : types) {
            if (byName.putIfAbsent(pt.eventType(), pt) != null) {
                throw new IllegalArgumentException("duplicate eventType: " + pt.eventType());
            }
            if (byCls.putIfAbsent(pt.type(), pt) != null) {
                throw new IllegalArgumentException("duplicate payload class: " + pt.type());
            }
        }
        this.byEventType = Map.copyOf(byName);
        this.byClass = Map.copyOf(byCls);
    }

    /** The production registry: the 8 standard payload types, all at schema version 1. */
    public static PayloadRegistry standard() {
        return new PayloadRegistry(List.of(
                new PayloadType("tick.trade", 1, TradeTick.class),
                new PayloadType("tick.quote", 1, QuoteTick.class),
                new PayloadType("bar", 1, Bar.class),
                new PayloadType("signal", 1, Signal.class),
                new PayloadType("order.intent", 1, OrderIntent.class),
                new PayloadType("fill", 1, Fill.class),
                new PayloadType("metric", 1, Metric.class),
                new PayloadType("command", 1, Command.class)));
    }

    /** Looks up a registration by wire {@code eventType}; empty ⇒ unregistered (forward-compat). */
    public Optional<PayloadType> byEventType(String eventType) {
        return Optional.ofNullable(byEventType.get(eventType));
    }

    /**
     * Resolves the registration for a payload's class, for the encode path.
     *
     * @throws IllegalArgumentException if the payload class is not registered
     */
    public PayloadType forPayloadClass(Class<? extends Payload> type) {
        PayloadType pt = byClass.get(type);
        if (pt == null) {
            throw new IllegalArgumentException("no registered eventType for payload class: " + type);
        }
        return pt;
    }
}
