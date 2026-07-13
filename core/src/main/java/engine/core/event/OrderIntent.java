package engine.core.event;

import java.math.BigDecimal;
import java.util.Objects;

/**
 * A strategy's request to trade. Strategies emit intents only — the OMS/Risk layers turn them into
 * real orders.
 *
 * @param strategyId originating strategy; non-blank
 * @param side buy or sell
 * @param quantity requested size; positive
 * @param orderType market or limit
 * @param limitPrice limit price; present iff {@code orderType == LIMIT}, otherwise {@code null}
 * @param timeInForce order lifetime
 * @param clientOrderId strategy-assigned id used to correlate the resulting {@link Fill}s; non-blank
 */
public record OrderIntent(
        String strategyId,
        Side side,
        BigDecimal quantity,
        OrderType orderType,
        BigDecimal limitPrice,
        TimeInForce timeInForce,
        String clientOrderId)
        implements Payload {

    public OrderIntent {
        Payload.requireText(strategyId, "strategyId");
        Objects.requireNonNull(side, "side must not be null");
        Payload.requirePositive(quantity, "quantity");
        Objects.requireNonNull(orderType, "orderType must not be null");
        Objects.requireNonNull(timeInForce, "timeInForce must not be null");
        Payload.requireText(clientOrderId, "clientOrderId");
        if (orderType == OrderType.LIMIT) {
            Payload.requirePositive(limitPrice, "limitPrice");
        } else if (limitPrice != null) {
            throw new IllegalArgumentException("limitPrice must be null for a " + orderType + " order");
        }
    }
}
