package engine.bus.monitor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import engine.bus.DeadLetter;
import engine.bus.monitor.BusMonitor.StreamClass;
import engine.bus.monitor.BusSnapshot.DlqLastError;
import engine.bus.monitor.BusSnapshot.GroupReading;
import engine.bus.monitor.BusSnapshot.StreamReading;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class BusMonitorTest {

    @ParameterizedTest
    @CsvSource({
        "dlq.orders.intents,DLQ",
        "dlq.md.tick.trade.BTC-USDT.BINANCE,DLQ",
        "replay.md.tick.trade.BTC-USDT.BINANCE,SKIP",
        "md.tick.trade.BTC-USDT.BINANCE,LIVE",
        "orders.intents,LIVE",
        "signals,LIVE",
        "metrics,LIVE",
    })
    void classifyKeysByAdrPrefix(String key, StreamClass expected) {
        assertThat(BusMonitor.classify(key)).isEqualTo(expected);
    }

    @Test
    void streamReadingFoldsCountsAndFirstEntry() {
        Map<String, Object> xinfo = Map.of(
                "entries-added", 1_000L,
                "length", 42L,
                "first-entry", List.of(utf8("1717171717171-0"), List.of()));

        StreamReading reading = BusMonitor.streamReading("metrics", xinfo);
        assertThat(reading.stream()).isEqualTo("metrics");
        assertThat(reading.entriesAdded()).isEqualTo(1_000L);
        assertThat(reading.length()).isEqualTo(42L);
        assertThat(reading.firstEntryMillis()).hasValue(1_717_171_717_171L);
    }

    @Test
    void emptyStreamHasNoFirstEntry() {
        // XINFO STREAM reports first-entry as nil for an empty stream.
        Map<String, Object> xinfo = fieldMapWithNull("entries-added", 0L, "length", 0L, "first-entry", null);
        assertThat(BusMonitor.firstEntryMillis(xinfo)).isEmpty();
        assertThat(BusMonitor.streamReading("signals", xinfo).firstEntryMillis())
                .isEmpty();
    }

    @Test
    void groupReadingsFoldNameLagAndPending() {
        List<Object> reply = List.of(
                List.of(utf8("name"), utf8("archiver"), utf8("lag"), 7L, utf8("pending"), 3L),
                List.of(utf8("name"), utf8("strategy-momentum"), utf8("lag"), 0L, utf8("pending"), 0L));

        List<GroupReading> groups = BusMonitor.groupReadings("orders.intents", reply);
        assertThat(groups).hasSize(2);

        GroupReading archiver = groups.get(0);
        assertThat(archiver.stream()).isEqualTo("orders.intents");
        assertThat(archiver.group()).isEqualTo("archiver");
        assertThat(archiver.lag()).hasValue(7L);
        assertThat(archiver.pending()).isEqualTo(3L);

        assertThat(groups.get(1).lag()).hasValue(0L);
    }

    @Test
    void nilLagIsUnknownNotZero() {
        // Redis returns lag as nil once trimming has cut into the undelivered range.
        List<Object> reply =
                List.of(Arrays.asList(utf8("name"), utf8("archiver"), utf8("lag"), null, utf8("pending"), 5L));

        GroupReading group = BusMonitor.groupReadings("metrics", reply).get(0);
        assertThat(group.lag()).isEmpty();
        assertThat(group.pending()).isEqualTo(5L);
    }

    @Test
    void nonListGroupRowsAreSkipped() {
        List<Object> reply =
                Arrays.asList(utf8("garbage"), null, List.of(utf8("name"), utf8("g"), utf8("pending"), 1L));
        List<GroupReading> groups = BusMonitor.groupReadings("signals", reply);
        assertThat(groups).hasSize(1);
        assertThat(groups.get(0).group()).isEqualTo("g");
        assertThat(groups.get(0).lag()).isEmpty();
    }

    @Test
    void ratePositiveDeltaOverInterval() {
        // (250 - 100) entries over 15 s = 10/s.
        assertThat(BusMonitor.ratePerSecond(100, 250, 15_000)).hasValue(10.0);
    }

    @Test
    void quietStreamRateIsARealZeroNotSuppressed() {
        // entries-added unchanged over a real interval is genuinely 0/s — not a fabricated zero.
        assertThat(BusMonitor.ratePerSecond(500, 500, 15_000)).hasValue(0.0);
    }

    @Test
    void negativeDeltaSuppressesTheSample() {
        // Key deleted and recreated between sweeps: entries-added restarts, delta goes negative.
        assertThat(BusMonitor.ratePerSecond(1_000, 5, 15_000)).isEmpty();
    }

    @Test
    void nonPositiveElapsedSuppressesTheSample() {
        assertThat(BusMonitor.ratePerSecond(100, 200, 0)).isEmpty();
        assertThat(BusMonitor.ratePerSecond(100, 200, -1)).isEmpty();
    }

    @Test
    void oldestAgeIsNowMinusFirstEntry() {
        long now = 1_717_171_717_171L;
        assertThat(BusMonitor.oldestAgeSeconds(now, now - 45_000)).isEqualTo(45.0);
        assertThat(BusMonitor.oldestAgeSeconds(now, now - 500)).isCloseTo(0.5, within(1e-9));
    }

    @Test
    void oldestAgeFlooredAtZeroUnderClockSkew() {
        long now = 1_717_171_717_171L;
        assertThat(BusMonitor.oldestAgeSeconds(now, now + 250)).isEqualTo(0.0);
    }

    @Test
    void dlqLastErrorFoldsFrozenFields() {
        Map<String, byte[]> body = Map.of(
                DeadLetter.FIELD_EVENT, new byte[] {1, 2, 3},
                DeadLetter.FIELD_STREAM, utf8("orders.intents"),
                DeadLetter.FIELD_GROUP, utf8("risk-manager"),
                DeadLetter.FIELD_ERROR, utf8("java.lang.IllegalStateException: boom\n\tat Foo.bar(Foo.java:1)"),
                DeadLetter.FIELD_FAILED_AT, utf8("2026-07-26T10:00:00Z"));

        Optional<DlqLastError> lastError = BusMonitor.dlqLastError(body);
        assertThat(lastError).isPresent();
        assertThat(lastError.get().group()).isEqualTo("risk-manager");
        assertThat(lastError.get().error()).startsWith("java.lang.IllegalStateException: boom");
        assertThat(lastError.get().failedAt()).isEqualTo("2026-07-26T10:00:00Z");
    }

    @Test
    void emptyDlqHasNoLastError() {
        assertThat(BusMonitor.dlqLastError(Map.of())).isEmpty();
    }

    @Test
    void memoryParsesUsedAndMaxIgnoringLookalikeFields() {
        String info = "# Memory\r\n"
                + "used_memory_rss:9999999\r\n"
                + "used_memory:2097152\r\n"
                + "used_memory_human:2.00M\r\n"
                + "maxmemory:4294967296\r\n"
                + "maxmemory_policy:noeviction\r\n";
        BusSnapshot.MemoryReading memory = BusMonitor.memoryReading(info);
        assertThat(memory.usedBytes()).isEqualTo(2_097_152L);
        assertThat(memory.maxBytes()).isEqualTo(4_294_967_296L);
    }

    @Test
    void maxmemoryZeroMeansUnlimited() {
        String info = "used_memory:1024\r\nmaxmemory:0\r\n";
        assertThat(BusMonitor.memoryReading(info).maxBytes()).isZero();
    }

    @Test
    void absentMaxmemoryReadsAsZero() {
        assertThat(BusMonitor.memoryReading("used_memory:1024\r\n").maxBytes()).isZero();
        assertThat(BusMonitor.memoryReading("used_memory:1024\r\n").usedBytes()).isEqualTo(1024L);
    }

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /** {@link Map#of} forbids null values; XINFO nil fields need a null-tolerant map. */
    private static Map<String, Object> fieldMapWithNull(Object... pairs) {
        var map = new java.util.LinkedHashMap<String, Object>();
        for (int i = 0; i + 1 < pairs.length; i += 2) {
            map.put((String) pairs[i], pairs[i + 1]);
        }
        return map;
    }
}
