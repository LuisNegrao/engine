package engine.bus.monitor;

import engine.bus.DeadLetter;
import engine.bus.PublisherStats;
import engine.bus.XInfoReplies;
import engine.bus.monitor.BusSnapshot.DlqLastError;
import engine.bus.monitor.BusSnapshot.DlqReading;
import engine.bus.monitor.BusSnapshot.GroupReading;
import engine.bus.monitor.BusSnapshot.MemoryReading;
import engine.bus.monitor.BusSnapshot.StreamReading;
import engine.bus.monitor.MetricNames.LatencyPercentile;
import engine.core.bus.EventPublisher;
import engine.core.event.Event;
import engine.core.event.Metric;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.KeyScanArgs;
import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.Limit;
import io.lettuce.core.Range;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.ScanCursor;
import io.lettuce.core.SocketOptions;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The bus's self-observer (NEG-21): every {@link MonitorTuning#interval()} it sweeps Redis, folds
 * the readings into a {@link BusSnapshot}, and publishes them as ordinary {@code Metric} events
 * through the injected {@link EventPublisher} — the bus reports on itself over itself, riding the
 * same publish path, {@code StreamNames} routing, and {@code MAXLEN} caps as everything else.
 *
 * <p><strong>A pure reader of what it observes.</strong> The monitor creates no consumer group, acks
 * nothing, and writes to no observed stream — the only thing it ever writes is its own metrics,
 * through the publisher (ADR 0002 §1 permits {@code SCAN} for tooling; a monitor is tooling). It owns
 * a dedicated Redis connection so a slow sweep queues behind nobody, and follows the {@code
 * StreamTrimmer} shape: a single-thread scheduled executor whose sweep survives its own exceptions,
 * so a Redis hiccup costs one sweep, never the schedule.
 *
 * <p>The Redis-touching {@link #sweep()} is exercised by the Step 6 integration tests; the pure
 * folding, derivation, and {@link #toEvents emission} helpers are unit-tested against fabricated
 * snapshots — no Redis type leaks into or out of a {@link BusSnapshot}.
 */
public final class BusMonitor implements AutoCloseable {

    private static final Logger LOG = System.getLogger(BusMonitor.class.getName());
    private static final long SCAN_BATCH = 500;

    /** The {@code Event.source} stamped on every metric the monitor publishes. */
    static final String MONITOR_SOURCE = "bus-monitor";
    /** The {@code Metric.owner} for bus-infrastructure metrics (rates, depth, DLQ, memory, publisher, monitor). */
    static final String OWNER_BUS = "bus";

    private final RedisClient client;
    private final StatefulRedisConnection<String, byte[]> connection;
    private final RedisCommands<String, byte[]> commands;
    private final EventPublisher publisher;
    private final PublisherStats publisherStats;
    private final MonitorTuning tuning;
    private final Clock clock;
    private final ScheduledExecutorService scheduler;
    private final MetricsEndpoint endpoint;
    private final AtomicBoolean started = new AtomicBoolean();

    /** The last completed sweep — the rate baseline for the next sweep and what the endpoint renders. */
    private volatile BusSnapshot lastSnapshot;

    public BusMonitor(
            String redisUri,
            EventPublisher publisher,
            PublisherStats publisherStats,
            MonitorTuning tuning,
            Clock clock) {
        this.publisher = Objects.requireNonNull(publisher, "publisher must not be null");
        this.publisherStats = Objects.requireNonNull(publisherStats, "publisherStats must not be null");
        this.tuning = Objects.requireNonNull(tuning, "tuning must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        this.client = RedisClient.create(redisUri);
        client.setOptions(clientOptions());
        this.connection = client.connect(RedisCodec.of(StringCodec.UTF8, ByteArrayCodec.INSTANCE));
        this.commands = connection.sync();
        this.scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "bus-monitor");
            thread.setDaemon(true);
            return thread;
        });
        this.endpoint = new MetricsEndpoint(tuning.endpointPort());
    }

    /**
     * Monitor client options: fail fast on a disconnect (a hiccup costs one sweep, not the schedule)
     * with a command timeout short enough that a hung Redis cannot stall the sweep past its interval.
     */
    static ClientOptions clientOptions() {
        return ClientOptions.builder()
                .disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
                .timeoutOptions(TimeoutOptions.enabled(Duration.ofSeconds(2)))
                .socketOptions(SocketOptions.builder()
                        .connectTimeout(Duration.ofSeconds(1))
                        .build())
                .autoReconnect(true)
                .build();
    }

    /** Starts the metrics endpoint and the sweep schedule; the first sweep runs immediately. */
    public void start() {
        if (!started.compareAndSet(false, true)) {
            throw new IllegalStateException("monitor already started");
        }
        endpoint.start();
        scheduler.scheduleWithFixedDelay(this::runSafely, 0, tuning.interval().toMillis(), TimeUnit.MILLISECONDS);
    }

    /** The metrics endpoint's actually-bound port — the real port when configured with {@code 0}. */
    public int endpointPort() {
        return endpoint.boundPort();
    }

    private void runSafely() {
        // scheduleWithFixedDelay cancels the schedule if a run escapes with an exception — a transient
        // Redis hiccup must cost one sweep, not all future ones.
        try {
            runOnce();
        } catch (RuntimeException e) {
            LOG.log(Level.ERROR, "monitor sweep failed; retrying next interval", e);
        }
    }

    /**
     * One sweep: read the bus, derive the metrics against the previous sweep, publish them, and keep
     * the reading as the next baseline and the endpoint's source. Package-private so tests drive it.
     */
    void runOnce() {
        BusSnapshot current = sweep();
        List<Event> events = toEvents(lastSnapshot, current, MONITOR_SOURCE, clock);
        for (Event event : events) {
            publisher.publish(event).whenComplete((v, err) -> {
                if (err != null) {
                    // The failure also increments the publisher's own `failed` counter, which the next
                    // sweep reports (the monitor observing itself, as designed) — so logging suffices.
                    LOG.log(Level.WARNING, "metric publish failed: " + err.getMessage());
                }
            });
        }
        endpoint.update(events, current.dlqs());
        lastSnapshot = current;
    }

    /** The last completed snapshot, or {@code null} before the first sweep — read by the endpoint (Step 5). */
    BusSnapshot lastSnapshot() {
        return lastSnapshot;
    }

    /** Stops the scheduler and endpoint, then releases the monitor's dedicated connection and client. */
    @Override
    public void close() {
        scheduler.shutdownNow();
        endpoint.close();
        connection.close();
        client.shutdown();
    }

    // ---- Redis reads (integration-tested) -------------------------------------------------------

    /**
     * One pass over the bus: discover every stream key, read the live ones and the {@code dlq.*}
     * ones, sample memory and the publisher in-flight gauge, and drain the publisher's interval
     * counters. Pure reads — nothing here mutates any observed stream.
     */
    BusSnapshot sweep() {
        long startNanos = System.nanoTime();
        Instant takenAt = clock.instant();
        List<StreamReading> streams = new ArrayList<>();
        List<GroupReading> groups = new ArrayList<>();
        List<DlqReading> dlqs = new ArrayList<>();

        for (String key : scanStreamKeys()) {
            switch (classify(key)) {
                case SKIP -> {
                    /* replay.* is reserved (ADR 0002 §3) — never observed. */
                }
                case LIVE -> readLiveStream(key, streams, groups);
                case DLQ -> readDlq(key, dlqs);
            }
        }

        MemoryReading memory = memoryReading(commands.info("memory"));
        long inFlight = publisherStats.inFlight();
        PublisherStats.Drain drain = publisherStats.drain();
        long sweepMillis = (System.nanoTime() - startNanos) / 1_000_000L;
        return new BusSnapshot(takenAt, sweepMillis, streams, groups, dlqs, memory, drain, inFlight);
    }

    /** {@code SCAN MATCH * TYPE stream} — server-side type filtering, so non-stream keys never come back. */
    private List<String> scanStreamKeys() {
        List<String> keys = new ArrayList<>();
        KeyScanArgs args = KeyScanArgs.Builder.matches("*").type("stream").limit(SCAN_BATCH);
        ScanCursor cursor = ScanCursor.INITIAL;
        do {
            KeyScanCursor<String> page = commands.scan(cursor, args);
            keys.addAll(page.getKeys());
            cursor = page;
        } while (!cursor.isFinished());
        return keys;
    }

    private void readLiveStream(String stream, List<StreamReading> streams, List<GroupReading> groups) {
        Map<String, Object> xinfo = streamInfoOrNull(stream);
        if (xinfo == null) {
            return; // deleted between SCAN and read — skip this sweep
        }
        streams.add(streamReading(stream, xinfo));
        groups.addAll(groupReadings(stream, groupsOrEmpty(stream)));
    }

    private void readDlq(String dlqKey, List<DlqReading> dlqs) {
        String sourceStream = dlqKey.substring("dlq.".length());
        long depth = orZero(commands.xlen(dlqKey));
        List<StreamMessage<String, byte[]>> newest = commands.xrevrange(dlqKey, Range.unbounded(), Limit.from(1));
        Optional<DlqLastError> lastError =
                newest.isEmpty() ? Optional.empty() : dlqLastError(newest.get(0).getBody());
        dlqs.add(new DlqReading(sourceStream, depth, lastError));
    }

    private Map<String, Object> streamInfoOrNull(String stream) {
        try {
            return XInfoReplies.asFieldMap(commands.xinfoStream(stream));
        } catch (RedisCommandExecutionException e) {
            if (isNoSuchKey(e)) {
                return null;
            }
            throw e;
        }
    }

    private List<Object> groupsOrEmpty(String stream) {
        try {
            return commands.xinfoGroups(stream);
        } catch (RedisCommandExecutionException e) {
            if (isNoSuchKey(e)) {
                return List.of();
            }
            throw e;
        }
    }

    // ---- Emission: snapshot → Metric events (unit-tested against fabricated snapshots) -----------

    /**
     * Derives the metric events for one sweep from the current snapshot and the previous one (the
     * rate baseline; {@code null} on the first sweep ⇒ no rates). One {@code Metric} per inventory
     * row, named via {@link MetricNames}, {@code owner} per the ADR 0003 conventions, wrapped in an
     * envelope stamped at the sweep instant. Conditional emissions honour the plan: {@code
     * lagUnknown} fires only when {@code lag} is nil, oldest-age only for a non-empty stream, a rate
     * only when a prior baseline exists and the delta is non-negative, and a feed source with no
     * samples this interval emits nothing (absence of traffic ≠ zero latency).
     */
    static List<Event> toEvents(BusSnapshot previous, BusSnapshot current, String monitorSource, Clock clock) {
        List<Event> events = new ArrayList<>();
        Instant at = current.takenAt();
        long nowMillis = at.toEpochMilli();

        Map<String, StreamReading> previousStreams = new HashMap<>();
        if (previous != null) {
            for (StreamReading r : previous.streams()) {
                previousStreams.put(r.stream(), r);
            }
        }
        long elapsedMillis =
                previous == null ? 0 : nowMillis - previous.takenAt().toEpochMilli();

        for (StreamReading s : current.streams()) {
            events.add(metric(MetricNames.streamDepth(s.stream()), s.length(), OWNER_BUS, monitorSource, at, clock));
            s.firstEntryMillis()
                    .ifPresent(first -> events.add(metric(
                            MetricNames.streamOldestAgeSeconds(s.stream()),
                            oldestAgeSeconds(nowMillis, first),
                            OWNER_BUS,
                            monitorSource,
                            at,
                            clock)));
            StreamReading prior = previousStreams.get(s.stream());
            if (prior != null) {
                ratePerSecond(prior.entriesAdded(), s.entriesAdded(), elapsedMillis)
                        .ifPresent(rate -> events.add(
                                metric(MetricNames.streamRate(s.stream()), rate, OWNER_BUS, monitorSource, at, clock)));
            }
        }

        for (GroupReading g : current.groups()) {
            if (g.lag().isPresent()) {
                events.add(metric(
                        MetricNames.groupLag(g.stream()), g.lag().getAsLong(), g.group(), monitorSource, at, clock));
            } else {
                events.add(metric(MetricNames.groupLagUnknown(g.stream()), 1L, g.group(), monitorSource, at, clock));
            }
            events.add(metric(MetricNames.groupPending(g.stream()), g.pending(), g.group(), monitorSource, at, clock));
        }

        for (DlqReading d : current.dlqs()) {
            events.add(metric(MetricNames.dlqDepth(d.sourceStream()), d.depth(), OWNER_BUS, monitorSource, at, clock));
        }

        PublisherStats.Drain pub = current.publisher();
        events.add(metric(MetricNames.PUBLISHER_PUBLISHED, pub.published(), OWNER_BUS, monitorSource, at, clock));
        events.add(metric(MetricNames.PUBLISHER_FAILED, pub.failed(), OWNER_BUS, monitorSource, at, clock));
        events.add(metric(
                MetricNames.PUBLISHER_IN_FLIGHT, current.publisherInFlight(), OWNER_BUS, monitorSource, at, clock));

        pub.latencySamplesBySource().forEach((feedSource, samples) -> {
            if (samples.length == 0) {
                return; // absence of traffic is not zero latency — emit nothing for a silent source
            }
            long[] sorted = samples.clone();
            java.util.Arrays.sort(sorted);
            events.add(metric(
                    MetricNames.feedLatency(LatencyPercentile.P50),
                    percentile(sorted, 0.50),
                    feedSource,
                    monitorSource,
                    at,
                    clock));
            events.add(metric(
                    MetricNames.feedLatency(LatencyPercentile.P90),
                    percentile(sorted, 0.90),
                    feedSource,
                    monitorSource,
                    at,
                    clock));
            events.add(metric(
                    MetricNames.feedLatency(LatencyPercentile.P99),
                    percentile(sorted, 0.99),
                    feedSource,
                    monitorSource,
                    at,
                    clock));
            events.add(metric(
                    MetricNames.feedLatency(LatencyPercentile.MAX),
                    sorted[sorted.length - 1],
                    feedSource,
                    monitorSource,
                    at,
                    clock));
            events.add(metric(MetricNames.FEED_EVENT_COUNT, samples.length, feedSource, monitorSource, at, clock));
        });

        MemoryReading mem = current.memory();
        events.add(metric(MetricNames.REDIS_MEMORY_USED_BYTES, mem.usedBytes(), OWNER_BUS, monitorSource, at, clock));
        events.add(metric(MetricNames.REDIS_MEMORY_MAX_BYTES, mem.maxBytes(), OWNER_BUS, monitorSource, at, clock));

        events.add(
                metric(MetricNames.MONITOR_SWEEP_MILLIS, current.sweepMillis(), OWNER_BUS, monitorSource, at, clock));
        return events;
    }

    /**
     * Exact percentile by nearest-rank on the ascending-sorted samples: index {@code ceil(q·n) − 1}.
     * p99 of 10 samples is the largest (index 9); with n=1 every quantile is the sole sample. No
     * interpolation, no bucketing — the samples are exact (NEG-17 fidelity).
     */
    static long percentile(long[] sortedAscending, double quantile) {
        int n = sortedAscending.length;
        int index = (int) Math.ceil(quantile * n) - 1;
        if (index < 0) {
            index = 0;
        } else if (index >= n) {
            index = n - 1;
        }
        return sortedAscending[index];
    }

    private static Event metric(String name, long value, String owner, String source, Instant at, Clock clock) {
        return Event.of(source, null, at, new Metric(name, BigDecimal.valueOf(value), owner), clock);
    }

    private static Event metric(String name, double value, String owner, String source, Instant at, Clock clock) {
        return Event.of(source, null, at, new Metric(name, BigDecimal.valueOf(value), owner), clock);
    }

    // ---- Pure folding + derivation (unit-tested against fabricated replies) ----------------------

    enum StreamClass {
        LIVE,
        DLQ,
        SKIP
    }

    /** Classifies a stream key by ADR 0002 §3 prefix: {@code replay.} skipped, {@code dlq.} a DLQ, else live. */
    static StreamClass classify(String streamKey) {
        if (streamKey.startsWith("replay.")) {
            return StreamClass.SKIP;
        }
        if (streamKey.startsWith("dlq.")) {
            return StreamClass.DLQ;
        }
        return StreamClass.LIVE;
    }

    /** Folds an {@code XINFO STREAM} field map into a {@link StreamReading}. */
    static StreamReading streamReading(String stream, Map<String, Object> xinfo) {
        return new StreamReading(
                stream, asLong(xinfo.get("entries-added")), asLong(xinfo.get("length")), firstEntryMillis(xinfo));
    }

    /** The oldest retained entry's id millis ({@code first-entry}), empty for an empty stream. */
    static OptionalLong firstEntryMillis(Map<String, Object> xinfo) {
        if (xinfo.get("first-entry") instanceof List<?> pair && !pair.isEmpty()) {
            return OptionalLong.of(streamIdMillis(XInfoReplies.asString(pair.get(0))));
        }
        return OptionalLong.empty();
    }

    /**
     * Folds every group row of an {@code XINFO GROUPS} reply into {@link GroupReading}s. A nil {@code
     * lag} field becomes an empty {@link OptionalLong} — unknown, never zero (NEG-19). {@code pending}
     * is the group PEL size, taken from the same reply rather than a separate {@code XPENDING}
     * round-trip: it is the identical count, so folding it here saves one command per group per sweep.
     */
    static List<GroupReading> groupReadings(String stream, List<?> groupsReply) {
        List<GroupReading> out = new ArrayList<>();
        for (Object entry : groupsReply) {
            if (!(entry instanceof List<?> fields)) {
                continue;
            }
            Map<String, Object> info = XInfoReplies.asFieldMap(fields);
            Object lag = info.get("lag");
            out.add(new GroupReading(
                    stream,
                    XInfoReplies.asString(info.get("name")),
                    lag == null ? OptionalLong.empty() : OptionalLong.of(asLong(lag)),
                    asLong(info.get("pending"))));
        }
        return out;
    }

    /** Reads the frozen {@link DeadLetter} fields from the newest DLQ entry's body. */
    static Optional<DlqLastError> dlqLastError(Map<String, byte[]> body) {
        if (body == null || body.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new DlqLastError(
                XInfoReplies.asString(body.get(DeadLetter.FIELD_GROUP)),
                XInfoReplies.asString(body.get(DeadLetter.FIELD_ERROR)),
                XInfoReplies.asString(body.get(DeadLetter.FIELD_FAILED_AT))));
    }

    /** Parses {@code used_memory} and {@code maxmemory} out of an {@code INFO memory} block. */
    static MemoryReading memoryReading(String infoMemory) {
        return new MemoryReading(infoField(infoMemory, "used_memory"), infoField(infoMemory, "maxmemory"));
    }

    /**
     * Publish rate as a pure function of two consecutive {@code entries-added} readings. A negative
     * delta means the key was deleted and recreated (test hygiene does this constantly) — emit
     * nothing rather than a fabricated zero (a zero is a lie in a rate series). Empty also when the
     * elapsed window is non-positive (no baseline / same instant).
     */
    static OptionalDouble ratePerSecond(long prevEntriesAdded, long currEntriesAdded, long elapsedMillis) {
        long delta = currEntriesAdded - prevEntriesAdded;
        if (delta < 0 || elapsedMillis <= 0) {
            return OptionalDouble.empty();
        }
        return OptionalDouble.of(delta * 1000.0 / elapsedMillis);
    }

    /**
     * Oldest-entry age in seconds. Floored at zero: unlike a rate, an age is non-negative by
     * definition, so absorbing sub-second skew between the injected clock and Redis server time (which
     * stamps stream ids) is not a fabricated data point.
     */
    static double oldestAgeSeconds(long nowMillis, long firstEntryMillis) {
        return Math.max(0L, nowMillis - firstEntryMillis) / 1000.0;
    }

    private static long streamIdMillis(String streamId) {
        int dash = streamId.indexOf('-');
        return Long.parseLong(dash < 0 ? streamId : streamId.substring(0, dash));
    }

    private static long infoField(String info, String field) {
        for (String line : info.split("\\r?\\n")) {
            int colon = line.indexOf(':');
            if (colon > 0 && line.substring(0, colon).equals(field)) {
                return Long.parseLong(line.substring(colon + 1).trim());
            }
        }
        return 0L; // absent (e.g. maxmemory unset on some builds)
    }

    private static long asLong(Object value) {
        if (value == null) {
            return 0L;
        }
        if (value instanceof Number n) {
            return n.longValue();
        }
        return Long.parseLong(XInfoReplies.asString(value).trim());
    }

    private static long orZero(Long value) {
        return value == null ? 0L : value;
    }

    private static boolean isNoSuchKey(RedisCommandExecutionException e) {
        return e.getMessage() != null && e.getMessage().toLowerCase().contains("no such key");
    }
}
