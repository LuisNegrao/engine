package engine.core.event;

import java.math.BigDecimal;
import java.util.Objects;
import java.util.Optional;

/**
 * A derived signal, e.g. a per-instrument sentiment score.
 *
 * @param signalType signal kind, e.g. {@code "sentiment"}; non-blank
 * @param value the signal value
 * @param confidence optional confidence in {@code [0,1]}; may be {@code null}
 */
public record Signal(String signalType, BigDecimal value, BigDecimal confidence) implements Payload {

    public Signal {
        Payload.requireText(signalType, "signalType");
        Objects.requireNonNull(value, "value must not be null");
        // confidence is nullable by design.
    }

    /** Confidence in {@code [0,1]}; empty when the producer supplied no confidence. */
    public Optional<BigDecimal> maybeConfidence() {
        return Optional.ofNullable(confidence);
    }
}
