package engine.core.event;

import java.math.BigDecimal;
import java.util.Objects;

/**
 * A named runtime metric, e.g. {@code "pnl.unrealized"}.
 *
 * @param name metric name; non-blank
 * @param value metric value
 * @param owner the strategy or component the metric belongs to; non-blank
 */
public record Metric(String name, BigDecimal value, String owner) implements Payload {

    public Metric {
        Payload.requireText(name, "name");
        Objects.requireNonNull(value, "value must not be null");
        Payload.requireText(owner, "owner");
    }
}
