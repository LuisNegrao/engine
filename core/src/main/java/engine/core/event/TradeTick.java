package engine.core.event;

import java.math.BigDecimal;
import java.util.Objects;

/**
 * A single executed trade print: {@code price}, {@code quantity}, and the {@code aggressor} side.
 *
 * <p>Deliberately separate from {@link QuoteTick} — trades and quotes share almost no fields and
 * are rarely consumed interleaved, so folding them into one payload would force every field
 * nullable and every consumer to branch. See {@code docs/adr/0001-separate-trade-and-quote-tick-events.md}.
 *
 * @param price execution price; positive
 * @param quantity executed size; positive
 * @param aggressor the side that crossed the spread
 */
public record TradeTick(BigDecimal price, BigDecimal quantity, Side aggressor) implements Payload {

    public TradeTick {
        Payload.requirePositive(price, "price");
        Payload.requirePositive(quantity, "quantity");
        Objects.requireNonNull(aggressor, "aggressor must not be null");
    }
}
