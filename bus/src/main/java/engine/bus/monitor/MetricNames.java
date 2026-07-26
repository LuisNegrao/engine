package engine.bus.monitor;

/**
 * The NEG-21 metric-name grammar as code (ADR 0003): {@code bus.<area>.<measure>[.<stream>]}.
 *
 * <p>Every bus-observability {@code Metric} name is built here — no caller ever assembles a name by
 * string concatenation. This is the {@link engine.bus.StreamNames} lesson applied to metrics: a name
 * is permanent QuestDB data once the archiver lands, so a rename is silent data loss, not a refactor,
 * and a single source of truth is the only defence. The grammar and owner conventions are frozen by
 * ADR 0003; the <em>inventory</em> of which names exist stays revisable until NEG-13's dashboards.
 *
 * <p>Per-stream builders append the observed stream name verbatim as the tail. This stays
 * unambiguous even though stream names contain dots (e.g. {@code md.tick.trade.BTC-USDT.BINANCE})
 * because the measure vocabulary is fixed-position: a parser knows where the measure ends and the
 * stream begins.
 */
public final class MetricNames {

    /** Fixed root segment; every bus metric name starts here. */
    public static final String ROOT = "bus";

    // Publisher instrumentation (unpartitioned — one publisher path, owner {@code bus}).
    public static final String PUBLISHER_PUBLISHED = "bus.publisher.published";
    public static final String PUBLISHER_FAILED = "bus.publisher.failed";
    public static final String PUBLISHER_IN_FLIGHT = "bus.publisher.inFlight";

    // Feed latency (percentiles via feedLatency(); count here — owner is the event source).
    public static final String FEED_EVENT_COUNT = "bus.feed.eventCount";

    // Redis infrastructure (owner {@code bus}). maxBytes 0 means "no limit"; derive nothing here.
    public static final String REDIS_MEMORY_USED_BYTES = "bus.redis.memoryUsedBytes";
    public static final String REDIS_MEMORY_MAX_BYTES = "bus.redis.memoryMaxBytes";

    // The monitor watching itself (owner {@code bus}).
    public static final String MONITOR_SWEEP_MILLIS = "bus.monitor.sweepMillis";

    private MetricNames() {}

    /** {@code bus.stream.rate.<stream>} — events/s from the {@code entries-added} delta. */
    public static String streamRate(String stream) {
        return "bus.stream.rate." + requireStream(stream);
    }

    /** {@code bus.stream.depth.<stream>} — {@code XLEN}. */
    public static String streamDepth(String stream) {
        return "bus.stream.depth." + requireStream(stream);
    }

    /** {@code bus.stream.oldestAgeSeconds.<stream>} — now − {@code first-entry} ID millis. */
    public static String streamOldestAgeSeconds(String stream) {
        return "bus.stream.oldestAgeSeconds." + requireStream(stream);
    }

    /** {@code bus.group.lag.<stream>} — undelivered + pending; owner is the consumer group. */
    public static String groupLag(String stream) {
        return "bus.group.lag." + requireStream(stream);
    }

    /** {@code bus.group.pending.<stream>} — {@code XPENDING} count; owner is the consumer group. */
    public static String groupPending(String stream) {
        return "bus.group.pending." + requireStream(stream);
    }

    /**
     * {@code bus.group.lagUnknown.<stream>} — emitted (value 1) only when Redis reports {@code lag}
     * as nil, i.e. trimming cut the undelivered range. Never emitted as 0: unknown reported as zero
     * is how a drowning consumer looks healthy (NEG-19). Owner is the consumer group.
     */
    public static String groupLagUnknown(String stream) {
        return "bus.group.lagUnknown." + requireStream(stream);
    }

    /** {@code bus.dlq.depth.<sourceStream>} — {@code XLEN dlq.<sourceStream>}; owner {@code bus}. */
    public static String dlqDepth(String sourceStream) {
        return "bus.dlq.depth." + requireStream(sourceStream);
    }

    /** {@code bus.feed.latencyMillis.<percentile>} — exact percentile; owner is the event source. */
    public static String feedLatency(LatencyPercentile percentile) {
        return "bus.feed.latencyMillis." + percentile.token();
    }

    /**
     * The fixed feed-latency percentile vocabulary (ADR 0003). Exact percentiles, not bucketed: the
     * publisher records every sample and the monitor sorts and index-selects (NEG-17 fidelity).
     */
    public enum LatencyPercentile {
        P50("p50"),
        P90("p90"),
        P99("p99"),
        MAX("max");

        private final String token;

        LatencyPercentile(String token) {
            this.token = token;
        }

        /** The lowercase name-tail segment, e.g. {@code "p50"}. */
        public String token() {
            return token;
        }
    }

    private static String requireStream(String stream) {
        if (stream == null || stream.isBlank()) {
            throw new IllegalArgumentException("stream must be non-blank to build a per-stream metric name");
        }
        return stream;
    }
}
