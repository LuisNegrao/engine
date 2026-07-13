package engine.core.event;

import java.math.BigDecimal;

/**
 * An execution report for an order, partial or final.
 *
 * @param clientOrderId id of the {@link OrderIntent} this fill belongs to; non-blank
 * @param executedQuantity size executed on this fill; positive
 * @param price execution price; positive
 * @param fee fee charged; non-negative
 * @param feeCurrency currency the fee is denominated in; non-blank
 * @param terminal {@code true} if this is the final fill for the order, {@code false} if partial
 */
public record Fill(
        String clientOrderId,
        BigDecimal executedQuantity,
        BigDecimal price,
        BigDecimal fee,
        String feeCurrency,
        boolean terminal)
        implements Payload {

    public Fill {
        Payload.requireText(clientOrderId, "clientOrderId");
        Payload.requirePositive(executedQuantity, "executedQuantity");
        Payload.requirePositive(price, "price");
        Payload.requireNonNegative(fee, "fee");
        Payload.requireText(feeCurrency, "feeCurrency");
    }
}
