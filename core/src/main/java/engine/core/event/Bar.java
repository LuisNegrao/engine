package engine.core.event;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * An OHLCV candle covering {@code [intervalStart, intervalStart + interval)}.
 *
 * @param intervalStart start of the bar window
 * @param interval bar width; positive
 * @param open open price; positive
 * @param high high price; positive
 * @param low low price; positive
 * @param close close price; positive
 * @param volume traded volume over the window; non-negative (may be zero)
 */
public record Bar(
        Instant intervalStart,
        Duration interval,
        BigDecimal open,
        BigDecimal high,
        BigDecimal low,
        BigDecimal close,
        BigDecimal volume)
        implements Payload {

    public Bar {
        Objects.requireNonNull(intervalStart, "intervalStart must not be null");
        Objects.requireNonNull(interval, "interval must not be null");
        if (interval.isZero() || interval.isNegative()) {
            throw new IllegalArgumentException("interval must be positive, was: " + interval);
        }
        Payload.requirePositive(open, "open");
        Payload.requirePositive(high, "high");
        Payload.requirePositive(low, "low");
        Payload.requirePositive(close, "close");
        Payload.requireNonNegative(volume, "volume");
    }
}
