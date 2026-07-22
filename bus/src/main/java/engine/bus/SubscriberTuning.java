package engine.bus;

import java.time.Duration;

/**
 * The tuning knobs of a {@link RedisStreamsEventSubscriber}: how it reads, how long an unacked entry
 * ages before the claim sweep takes it over, when it gives up and dead-letters, and how big the DLQ
 * may grow. {@link #standard()} is the production shape; integration tests build their own with
 * millisecond claim timings so nothing sleeps its way to a minute-long suite.
 *
 * @param block how long {@code XREADGROUP} blocks waiting for new entries; the command timeout in
 *     {@link RedisStreamsEventSubscriber#clientOptions()} MUST exceed this
 * @param batchCount max entries pulled per {@code XREADGROUP}
 * @param claimInterval how often the poll thread runs the claim sweep between reads
 * @param claimMinIdle how long an entry must sit unacked before it is eligible for {@code XCLAIM};
 *     doubles as the retry backoff, so it must exceed the slowest healthy handler
 * @param maxDeliveries delivery count at which an entry is declared poison and parked on the DLQ
 * @param dlqMaxlen approximate {@code MAXLEN} cap on each {@code dlq.*} stream
 */
public record SubscriberTuning(
        Duration block,
        int batchCount,
        Duration claimInterval,
        Duration claimMinIdle,
        int maxDeliveries,
        long dlqMaxlen) {

    public SubscriberTuning {
        requirePositive(block, "block");
        requirePositive(claimInterval, "claimInterval");
        requirePositive(claimMinIdle, "claimMinIdle");
        if (batchCount <= 0) {
            throw new IllegalArgumentException("batchCount must be positive, was: " + batchCount);
        }
        if (maxDeliveries <= 0) {
            throw new IllegalArgumentException("maxDeliveries must be positive, was: " + maxDeliveries);
        }
        if (dlqMaxlen <= 0) {
            throw new IllegalArgumentException("dlqMaxlen must be positive, was: " + dlqMaxlen);
        }
    }

    /**
     * Production defaults: block 1s, batch 256, claim sweep every 5s on entries idle ≥ 30s, park after
     * 5 deliveries, cap each DLQ at ~100k entries. The 30s min-idle buys headroom against a healthy
     * handler being mistaken for a dead one; the real defense against duplicate processing is the
     * documented idempotency contract, not this number.
     */
    public static SubscriberTuning standard() {
        return new SubscriberTuning(
                Duration.ofSeconds(1), 256, Duration.ofSeconds(5), Duration.ofSeconds(30), 5, 100_000);
    }

    private static void requirePositive(Duration value, String name) {
        if (value == null || value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(name + " must be a positive duration, was: " + value);
        }
    }
}
