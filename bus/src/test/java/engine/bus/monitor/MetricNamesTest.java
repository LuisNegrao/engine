package engine.bus.monitor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import engine.bus.monitor.MetricNames.LatencyPercentile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Expected names are hardcoded literals from the ADR 0003 grammar and inventory — computing them via
 * {@link MetricNames} would make the test a tautology unable to catch a wrong builder.
 */
class MetricNamesTest {

    private static final String STREAM = "md.tick.trade.BTC-USDT.BINANCE";

    @Test
    void perStreamBuildersAppendTheStreamVerbatim() {
        assertThat(MetricNames.streamRate(STREAM)).isEqualTo("bus.stream.rate.md.tick.trade.BTC-USDT.BINANCE");
        assertThat(MetricNames.streamDepth(STREAM)).isEqualTo("bus.stream.depth.md.tick.trade.BTC-USDT.BINANCE");
        assertThat(MetricNames.streamOldestAgeSeconds(STREAM))
                .isEqualTo("bus.stream.oldestAgeSeconds.md.tick.trade.BTC-USDT.BINANCE");
        assertThat(MetricNames.groupLag(STREAM)).isEqualTo("bus.group.lag.md.tick.trade.BTC-USDT.BINANCE");
        assertThat(MetricNames.groupPending(STREAM)).isEqualTo("bus.group.pending.md.tick.trade.BTC-USDT.BINANCE");
        assertThat(MetricNames.groupLagUnknown(STREAM))
                .isEqualTo("bus.group.lagUnknown.md.tick.trade.BTC-USDT.BINANCE");
    }

    @Test
    void dlqDepthPrefixesTheSourceStream() {
        assertThat(MetricNames.dlqDepth("orders.intents")).isEqualTo("bus.dlq.depth.orders.intents");
    }

    @Test
    void unpartitionedNamesAreTheFrozenConstants() {
        assertThat(MetricNames.PUBLISHER_PUBLISHED).isEqualTo("bus.publisher.published");
        assertThat(MetricNames.PUBLISHER_FAILED).isEqualTo("bus.publisher.failed");
        assertThat(MetricNames.PUBLISHER_IN_FLIGHT).isEqualTo("bus.publisher.inFlight");
        assertThat(MetricNames.FEED_EVENT_COUNT).isEqualTo("bus.feed.eventCount");
        assertThat(MetricNames.REDIS_MEMORY_USED_BYTES).isEqualTo("bus.redis.memoryUsedBytes");
        assertThat(MetricNames.REDIS_MEMORY_MAX_BYTES).isEqualTo("bus.redis.memoryMaxBytes");
        assertThat(MetricNames.MONITOR_SWEEP_MILLIS).isEqualTo("bus.monitor.sweepMillis");
    }

    @ParameterizedTest
    @CsvSource({
        "P50,bus.feed.latencyMillis.p50",
        "P90,bus.feed.latencyMillis.p90",
        "P99,bus.feed.latencyMillis.p99",
        "MAX,bus.feed.latencyMillis.max",
    })
    void feedLatencyAppendsThePercentileToken(LatencyPercentile percentile, String expected) {
        assertThat(MetricNames.feedLatency(percentile)).isEqualTo(expected);
    }

    @Test
    void everyNameStartsWithTheFrozenRoot() {
        assertThat(MetricNames.ROOT).isEqualTo("bus");
        assertThat(MetricNames.streamRate(STREAM)).startsWith("bus.");
        assertThat(MetricNames.feedLatency(LatencyPercentile.P50)).startsWith("bus.");
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "   "})
    void blankStreamIsRejected(String blank) {
        assertThatThrownBy(() -> MetricNames.streamRate(blank))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("non-blank");
    }

    @Test
    void nullStreamIsRejected() {
        assertThatThrownBy(() -> MetricNames.dlqDepth(null)).isInstanceOf(IllegalArgumentException.class);
    }
}
