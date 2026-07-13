package engine.core.event;

import java.math.BigDecimal;

/**
 * A top-of-book quote update: best bid and ask with their sizes.
 *
 * <p>Deliberately separate from {@link TradeTick} — see {@code
 * docs/adr/0001-separate-trade-and-quote-tick-events.md} for the trade-vs-quote rationale.
 *
 * @param bidPrice best bid price; positive
 * @param bidQuantity size at the best bid; positive
 * @param askPrice best ask price; positive
 * @param askQuantity size at the best ask; positive
 */
public record QuoteTick(BigDecimal bidPrice, BigDecimal bidQuantity, BigDecimal askPrice, BigDecimal askQuantity)
        implements Payload {

    public QuoteTick {
        Payload.requirePositive(bidPrice, "bidPrice");
        Payload.requirePositive(bidQuantity, "bidQuantity");
        Payload.requirePositive(askPrice, "askPrice");
        Payload.requirePositive(askQuantity, "askQuantity");
    }
}
