package engine.bus.monitor;

import static org.assertj.core.api.Assertions.assertThat;

import engine.bus.PublisherStats;
import engine.bus.monitor.BusSnapshot.DlqLastError;
import engine.bus.monitor.BusSnapshot.DlqReading;
import engine.bus.monitor.BusSnapshot.GroupReading;
import engine.bus.monitor.BusSnapshot.MemoryReading;
import engine.bus.monitor.BusSnapshot.StreamReading;
import engine.core.event.Event;
import engine.core.event.Metric;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

class BusMonitorEmissionTest {

    private static final Instant NOW = Instant.ofEpochMilli(1_717_171_717_171L);
    private static final Clock CLOCK = Clock.fixed(NOW.plusSeconds(1), ZoneOffset.UTC);

    @Test
    void snapshotEmitsTheInventoryWithNamesValuesAndOwners() {
        BusSnapshot previous = snapshot(
                NOW.minusSeconds(15),
                List.of(new StreamReading("metrics", 100, 0, OptionalLong.empty())),
                List.of(),
                List.of(),
                new MemoryReading(0, 0),
                new PublisherStats.Drain(0, 0, Map.of()),
                0);

        BusSnapshot current = new BusSnapshot(
                NOW,
                12,
                List.of(new StreamReading("metrics", 250, 42, OptionalLong.of(NOW.toEpochMilli() - 45_000))),
                List.of(
                        new GroupReading("metrics", "archiver", OptionalLong.of(7), 3),
                        new GroupReading("orders.intents", "risk-manager", OptionalLong.empty(), 5)),
                List.of(new DlqReading(
                        "orders.intents",
                        2,
                        Optional.of(new DlqLastError("risk-manager", "boom", "2026-07-26T10:00:00Z")))),
                new MemoryReading(2_097_152, 4_294_967_296L),
                new PublisherStats.Drain(
                        100, 1, Map.of("binance", new long[] {10, 20, 30, 40, 50, 60, 70, 80, 90, 100})),
                4);

        List<Event> events = BusMonitor.toEvents(previous, current, "bus-monitor", CLOCK);

        // Per-stream (owner bus).
        assertThat(value(events, "bus.stream.depth.metrics", "bus")).isEqualByComparingTo("42");
        assertThat(value(events, "bus.stream.oldestAgeSeconds.metrics", "bus")).isEqualByComparingTo("45");
        assertThat(value(events, "bus.stream.rate.metrics", "bus")).isEqualByComparingTo("10"); // (250-100)/15s

        // Per-group (owner is the consumer group): lag is undelivered(7) + pending(3), pending separate.
        assertThat(value(events, "bus.group.lag.metrics", "archiver")).isEqualByComparingTo("10");
        assertThat(value(events, "bus.group.pending.metrics", "archiver")).isEqualByComparingTo("3");
        assertThat(value(events, "bus.group.lagUnknown.orders.intents", "risk-manager"))
                .isEqualByComparingTo("1");
        assertThat(value(events, "bus.group.pending.orders.intents", "risk-manager"))
                .isEqualByComparingTo("5");

        // A known lag never also emits lagUnknown, and an unknown lag never emits a lag sample.
        assertThat(names(events)).doesNotContain("bus.group.lagUnknown.metrics", "bus.group.lag.orders.intents");

        // DLQ depth (owner bus); the last-error string is endpoint-only, never a Metric.
        assertThat(value(events, "bus.dlq.depth.orders.intents", "bus")).isEqualByComparingTo("2");

        // Publisher (owner bus).
        assertThat(value(events, "bus.publisher.published", "bus")).isEqualByComparingTo("100");
        assertThat(value(events, "bus.publisher.failed", "bus")).isEqualByComparingTo("1");
        assertThat(value(events, "bus.publisher.inFlight", "bus")).isEqualByComparingTo("4");

        // Feed latency (owner is the event source): exact nearest-rank over [10..100].
        assertThat(value(events, "bus.feed.latencyMillis.p50", "binance")).isEqualByComparingTo("50");
        assertThat(value(events, "bus.feed.latencyMillis.p90", "binance")).isEqualByComparingTo("90");
        assertThat(value(events, "bus.feed.latencyMillis.p99", "binance")).isEqualByComparingTo("100");
        assertThat(value(events, "bus.feed.latencyMillis.max", "binance")).isEqualByComparingTo("100");
        assertThat(value(events, "bus.feed.eventCount", "binance")).isEqualByComparingTo("10");

        // Memory and monitor (owner bus).
        assertThat(value(events, "bus.redis.memoryUsedBytes", "bus")).isEqualByComparingTo("2097152");
        assertThat(value(events, "bus.redis.memoryMaxBytes", "bus")).isEqualByComparingTo("4294967296");
        assertThat(value(events, "bus.monitor.sweepMillis", "bus")).isEqualByComparingTo("12");
    }

    @Test
    void everyEventIsSourcedFromTheMonitorAtTheSweepInstant() {
        BusSnapshot current = snapshot(
                NOW,
                List.of(),
                List.of(),
                List.of(),
                new MemoryReading(1, 0),
                new PublisherStats.Drain(0, 0, Map.of()),
                0);

        Event any = BusMonitor.toEvents(null, current, "bus-monitor", CLOCK).get(0);
        assertThat(any.source()).isEqualTo("bus-monitor");
        assertThat(any.occurredAt()).isEqualTo(NOW);
        assertThat(any.ingestedAt()).isEqualTo(NOW.plusSeconds(1)); // stamped from the injected clock
        assertThat(any.payload()).isInstanceOf(Metric.class);
    }

    @Test
    void firstSweepEmitsNoRates() {
        BusSnapshot current = snapshot(
                NOW,
                List.of(new StreamReading("metrics", 250, 42, OptionalLong.empty())),
                List.of(),
                List.of(),
                new MemoryReading(0, 0),
                new PublisherStats.Drain(0, 0, Map.of()),
                0);

        List<String> names = names(BusMonitor.toEvents(null, current, "bus-monitor", CLOCK));
        assertThat(names).contains("bus.stream.depth.metrics").doesNotContain("bus.stream.rate.metrics");
    }

    @Test
    void recreatedStreamEmitsNoRate() {
        BusSnapshot previous = snapshot(
                NOW.minusSeconds(15),
                List.of(new StreamReading("metrics", 1_000, 0, OptionalLong.empty())),
                List.of(),
                List.of(),
                new MemoryReading(0, 0),
                new PublisherStats.Drain(0, 0, Map.of()),
                0);
        BusSnapshot current = snapshot(
                NOW,
                List.of(new StreamReading("metrics", 5, 1, OptionalLong.empty())), // entries-added restarted
                List.of(),
                List.of(),
                new MemoryReading(0, 0),
                new PublisherStats.Drain(0, 0, Map.of()),
                0);

        assertThat(names(BusMonitor.toEvents(previous, current, "bus-monitor", CLOCK)))
                .doesNotContain("bus.stream.rate.metrics");
    }

    @Test
    void emptyStreamEmitsNoOldestAge() {
        BusSnapshot current = snapshot(
                NOW,
                List.of(new StreamReading("signals", 0, 0, OptionalLong.empty())),
                List.of(),
                List.of(),
                new MemoryReading(0, 0),
                new PublisherStats.Drain(0, 0, Map.of()),
                0);

        assertThat(names(BusMonitor.toEvents(null, current, "bus-monitor", CLOCK)))
                .contains("bus.stream.depth.signals")
                .doesNotContain("bus.stream.oldestAgeSeconds.signals");
    }

    @Test
    void silentFeedSourceEmitsNothing() {
        BusSnapshot current = snapshot(
                NOW,
                List.of(),
                List.of(),
                List.of(),
                new MemoryReading(0, 0),
                new PublisherStats.Drain(0, 0, Map.of("quiet", new long[0], "active", new long[] {5})),
                0);

        List<Event> events = BusMonitor.toEvents(null, current, "bus-monitor", CLOCK);
        assertThat(value(events, "bus.feed.latencyMillis.p50", "active")).isEqualByComparingTo("5");
        assertThat(events.stream().map(e -> (Metric) e.payload()).map(Metric::owner))
                .doesNotContain("quiet");
    }

    @Test
    void percentileIsNearestRankExact() {
        long[] ten = {10, 20, 30, 40, 50, 60, 70, 80, 90, 100};
        assertThat(BusMonitor.percentile(ten, 0.50)).isEqualTo(50);
        assertThat(BusMonitor.percentile(ten, 0.90)).isEqualTo(90);
        assertThat(BusMonitor.percentile(ten, 0.99)).isEqualTo(100); // p99 of 10 is the largest sample

        long[] one = {42};
        assertThat(BusMonitor.percentile(one, 0.50)).isEqualTo(42);
        assertThat(BusMonitor.percentile(one, 0.99)).isEqualTo(42);

        long[] five = {1, 2, 3, 4, 5};
        assertThat(BusMonitor.percentile(five, 0.50)).isEqualTo(3);
        assertThat(BusMonitor.percentile(five, 0.90)).isEqualTo(5);
    }

    // ---- helpers --------------------------------------------------------------------------------

    private static BusSnapshot snapshot(
            Instant takenAt,
            List<StreamReading> streams,
            List<GroupReading> groups,
            List<DlqReading> dlqs,
            MemoryReading memory,
            PublisherStats.Drain publisher,
            long inFlight) {
        return new BusSnapshot(takenAt, 0, streams, groups, dlqs, memory, publisher, inFlight);
    }

    private static BigDecimal value(List<Event> events, String name, String owner) {
        return events.stream()
                .map(e -> (Metric) e.payload())
                .filter(m -> m.name().equals(name) && m.owner().equals(owner))
                .map(Metric::value)
                .findFirst()
                .orElseThrow(() -> new AssertionError("no metric " + name + " with owner " + owner));
    }

    private static List<String> names(List<Event> events) {
        return events.stream().map(e -> ((Metric) e.payload()).name()).toList();
    }
}
