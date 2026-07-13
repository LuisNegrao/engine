package engine.core.event;

import java.math.BigDecimal;
import java.util.Objects;

/**
 * Marker for the body of an {@link Event}. Sealed so that consumers can {@code switch} over the
 * closed set of payload types exhaustively — adding a new payload is a compile error at every
 * switch that forgot to handle it.
 *
 * <p>Payload records carry <strong>zero</strong> serialization annotations: the wire {@code
 * eventType}/{@code schemaVersion} discriminators live in the {@code engine.core.serde} registry,
 * not on these types, so the JSON format can be swapped without touching the model.
 */
public sealed interface Payload permits TradeTick, QuoteTick, Bar, Signal, OrderIntent, Fill, Metric, Command {

    /** Validates that a required decimal is present and strictly positive. */
    static BigDecimal requirePositive(BigDecimal value, String name) {
        Objects.requireNonNull(value, name + " must not be null");
        if (value.signum() <= 0) {
            throw new IllegalArgumentException(name + " must be positive, was: " + value);
        }
        return value;
    }

    /** Validates that a required decimal is present and not negative (zero allowed). */
    static BigDecimal requireNonNegative(BigDecimal value, String name) {
        Objects.requireNonNull(value, name + " must not be null");
        if (value.signum() < 0) {
            throw new IllegalArgumentException(name + " must not be negative, was: " + value);
        }
        return value;
    }

    /** Validates that a required string is present and not blank. */
    static String requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must be non-blank");
        }
        return value;
    }
}
