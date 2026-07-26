package engine.bus.monitor;

import engine.bus.DeadLetter;
import engine.bus.PublisherStats;
import engine.bus.XInfoReplies;
import engine.bus.monitor.BusSnapshot.DlqLastError;
import engine.bus.monitor.BusSnapshot.DlqReading;
import engine.bus.monitor.BusSnapshot.GroupReading;
import engine.bus.monitor.BusSnapshot.MemoryReading;
import engine.bus.monitor.BusSnapshot.StreamReading;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.Limit;
import io.lettuce.core.Range;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.SocketOptions;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;

/**
 * The bus's self-observer (NEG-21): each sweep discovers streams via {@code SCAN}, reads their
 * health with {@code XINFO}/{@code XLEN}/{@code XREVRANGE}/{@code INFO memory}, and folds it into a
 * {@link BusSnapshot}. Emission of the snapshot as {@code Metric} events (Step 4) and the Prometheus
 * endpoint (Step 5) build on this read side.
 *
 * <p><strong>A pure reader of what it observes.</strong> The monitor creates no consumer group, acks
 * nothing, and writes to no observed stream — the only thing it ever writes is its own {@code Metric}
 * events, through the ordinary publisher (ADR 0002 §1 permits {@code SCAN} for tooling; a monitor is
 * tooling). It runs on its own dedicated connection so a slow sweep queues behind nobody.
 *
 * <p>The Redis-touching {@link #sweep()} is exercised by the Step 6 integration tests; the pure
 * folding and derivation helpers below are unit-tested against fabricated replies — no Redis type
 * leaks past them into {@link BusSnapshot}.
 */
public final class BusMonitor {

    private final RedisClient client;
    private final StatefulRedisConnection<String, byte[]> connection;
    private final RedisCommands<String, byte[]> commands;
    private final PublisherStats publisherStats;
    private final Clock clock;

    public BusMonitor(String redisUri, PublisherStats publisherStats, Clock clock) {
        this.client = RedisClient.create(redisUri);
        client.setOptions(clientOptions());
        this.connection = client.connect(RedisCodec.of(StringCodec.UTF8, ByteArrayCodec.INSTANCE));
        this.commands = connection.sync();
        this.publisherStats = publisherStats;
        this.clock = clock;
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

    /**
     * One pass over the bus: discover every stream key, read the live ones and the {@code dlq.*}
     * ones, sample memory, and drain the publisher's interval counters. Pure reads — nothing here
     * mutates any observed stream. Returns the readings; deriving metrics from them is Step 4.
     */
    BusSnapshot sweep() {
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
        PublisherStats.Drain publisher = publisherStats.drain();
        return new BusSnapshot(takenAt, streams, groups, dlqs, memory, publisher);
    }

    /** Releases the monitor's dedicated connection and client. Leaves no group, no keys behind. */
    public void close() {
        connection.close();
        client.shutdown();
    }

    // ---- Redis reads (integration-tested) -------------------------------------------------------

    /** {@code SCAN MATCH *}, keeping only stream-typed keys — Lettuce's ScanArgs has no server-side TYPE. */
    private List<String> scanStreamKeys() {
        List<String> keys = new ArrayList<>();
        ScanArgs args = new ScanArgs().match("*").limit(500);
        KeyScanCursor<String> cursor = commands.scan(args);
        collectStreamKeys(cursor, keys);
        while (!cursor.isFinished()) {
            cursor = commands.scan(cursor, args);
            collectStreamKeys(cursor, keys);
        }
        return keys;
    }

    private void collectStreamKeys(KeyScanCursor<String> cursor, List<String> keys) {
        for (String key : cursor.getKeys()) {
            if ("stream".equals(commands.type(key))) {
                keys.add(key);
            }
        }
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
