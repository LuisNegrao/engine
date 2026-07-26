package engine.bus.monitor;

import engine.bus.PublisherStats;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * One monitor sweep's readings as an immutable value type — everything {@link BusMonitor} reads from
 * Redis in a single pass, folded into records the emission step (NEG-21 Step 4) turns into {@code
 * Metric} events and the endpoint (Step 5) renders. Pure data: no Redis type leaks in, so the whole
 * snapshot is fabricable in a unit test.
 *
 * <p>{@code takenAt} is the injected-clock instant the sweep began; rates are derived from the
 * {@code entriesAdded} delta between this snapshot and the previous one (see {@link
 * BusMonitor#ratePerSecond}).
 *
 * @param takenAt when this sweep began, from the monitor's injected clock
 * @param streams per live stream: {@code entries-added}, length, and oldest-entry age input
 * @param groups per (group, stream): lag (empty when Redis reports it nil) and pending
 * @param dlqs per discovered {@code dlq.*}: depth and the newest parked entry's fields
 * @param memory Redis {@code used_memory} and {@code maxmemory} ({@code 0} = unlimited)
 * @param publisher the publisher counters and per-source latency samples drained this sweep
 */
public record BusSnapshot(
        Instant takenAt,
        List<StreamReading> streams,
        List<GroupReading> groups,
        List<DlqReading> dlqs,
        MemoryReading memory,
        PublisherStats.Drain publisher) {

    public BusSnapshot {
        streams = List.copyOf(streams);
        groups = List.copyOf(groups);
        dlqs = List.copyOf(dlqs);
    }

    /**
     * A live stream's {@code XINFO STREAM} readings.
     *
     * @param stream the stream name
     * @param entriesAdded lifetime-monotonic ingest count ({@code entries-added}) — the rate source
     * @param length current entry count ({@code XLEN}) — the depth metric
     * @param firstEntryMillis the oldest retained entry's id millis, empty for an empty stream
     */
    public record StreamReading(String stream, long entriesAdded, long length, OptionalLong firstEntryMillis) {}

    /**
     * A consumer group's position on one stream.
     *
     * @param stream the stream the group reads
     * @param group the consumer-group name (the metric's {@code owner})
     * @param lag undelivered entries, or empty when Redis reports {@code lag} as nil (trimming cut
     *     the undelivered range) — empty means <em>unknown</em>, never zero (NEG-19)
     * @param pending delivered-but-unacked entries for the group (the group PEL size)
     */
    public record GroupReading(String stream, String group, OptionalLong lag, long pending) {}

    /**
     * A dead-letter stream's depth and newest entry.
     *
     * @param sourceStream the stream whose poison this DLQ parks (i.e. {@code dlq.<sourceStream>})
     * @param depth {@code XLEN} of the DLQ
     * @param lastError the newest parked entry's frozen {@link engine.bus.DeadLetter} fields, empty
     *     when the DLQ is empty
     */
    public record DlqReading(String sourceStream, long depth, Optional<DlqLastError> lastError) {}

    /**
     * The newest DLQ entry's surfaced fields (rendered as a Prometheus info-metric, never a {@code
     * Metric} value — a stack trace does not belong in a {@code BigDecimal}).
     */
    public record DlqLastError(String group, String error, String failedAt) {}

    /**
     * Redis memory readings from {@code INFO memory}.
     *
     * @param usedBytes {@code used_memory}
     * @param maxBytes {@code maxmemory}; {@code 0} means no limit — derive any percentage
     *     dashboard-side and nil-safe, never here
     */
    public record MemoryReading(long usedBytes, long maxBytes) {}
}
