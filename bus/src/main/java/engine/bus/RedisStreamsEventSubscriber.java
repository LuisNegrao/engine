package engine.bus;

import engine.core.bus.EventHandler;
import engine.core.bus.EventSelector;
import engine.core.bus.EventSubscriber;
import engine.core.bus.Replay;
import engine.core.bus.ReplayRange;
import engine.core.bus.SubscribeOptions;
import engine.core.bus.SubscribeOptions.LagPolicy;
import engine.core.bus.SubscribeOptions.SkipToLatest;
import engine.core.bus.SubscribeOptions.StartPosition;
import engine.core.bus.Subscription;
import engine.core.event.Event;
import engine.core.serde.EventCodec;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.Consumer;
import io.lettuce.core.Limit;
import io.lettuce.core.Range;
import io.lettuce.core.RedisBusyException;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisException;
import io.lettuce.core.SocketOptions;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.XClaimArgs;
import io.lettuce.core.XGroupCreateArgs;
import io.lettuce.core.XPendingArgs;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.XReadArgs.StreamOffset;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.models.stream.PendingMessage;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Redis Streams consumer-group implementation of {@link EventSubscriber}. Every subscription owns a
 * dedicated Lettuce connection and a single daemon poll thread running synchronous commands: a
 * blocking {@code XREADGROUP} feeds the handler, {@code XACK} follows a normal return, and a
 * throwing handler leaves the entry in the pending list. A claim sweep between reads redelivers
 * unacked entries — from this consumer or a dead one — after {@code claimMinIdle}, and parks an entry
 * that keeps failing on the DLQ (NEG-19 Step 6 adds lag observability and skip-to-latest).
 *
 * <p>The consumer group and this consumer's stable name are constructor state — one component reads
 * every stream under one group (ADR 0002), and the name must be stable across restarts so a crashed
 * instance reclaims its own pending entries on restart. Startup connects eagerly and fails fast, the
 * same contract as {@link RedisStreamsEventPublisher}.
 */
public class RedisStreamsEventSubscriber implements EventSubscriber {

    private static final Logger LOG = System.getLogger(RedisStreamsEventSubscriber.class.getName());

    /** How long a loop sleeps after a transient Redis error before retrying — never kills the loop. */
    private static final Duration RETRY_AFTER_REDIS_ERROR = Duration.ofSeconds(1);

    private final RedisClient client;
    private final StatefulRedisConnection<String, byte[]> controlConnection;
    private final EventCodec codec;
    private final String group;
    private final String consumerName;
    private final SubscriberTuning tuning;
    private final List<RedisSubscription> subscriptions = new CopyOnWriteArrayList<>();
    private final AtomicInteger subscriptionCount = new AtomicInteger();
    private final List<RedisReplay> replays = new CopyOnWriteArrayList<>();
    private final AtomicInteger replayCount = new AtomicInteger();

    public RedisStreamsEventSubscriber(
            String redisUri, EventCodec codec, String group, String consumerName, SubscriberTuning tuning) {
        this.client = RedisClient.create(redisUri);
        client.setOptions(clientOptions());
        // Eager connect: a mis-wired or down Redis fails startup here, not silently at first read.
        this.controlConnection = client.connect(RedisCodec.of(StringCodec.UTF8, ByteArrayCodec.INSTANCE));
        this.codec = codec;
        this.group = group;
        this.consumerName = consumerName;
        this.tuning = tuning;
    }

    /**
     * Subscriber-specific client options. Distinct from the publisher's on purpose: the command
     * timeout MUST exceed the {@code XREADGROUP} block, or every blocking read is killed from the
     * inside. Pinned by a unit test so a refactor cannot "unify" the two option sets.
     */
    static ClientOptions clientOptions() {
        return ClientOptions.builder()
                .disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
                .timeoutOptions(TimeoutOptions.enabled(Duration.ofSeconds(5))) // MUST exceed BLOCK
                .socketOptions(SocketOptions.builder()
                        .connectTimeout(Duration.ofSeconds(1))
                        .build())
                .autoReconnect(true)
                .build();
    }

    @Override
    public Subscription subscribe(List<EventSelector> selectors, SubscribeOptions options, EventHandler handler) {
        // Wiring-time validation: bad selectors and illegal lag policies throw here, before any
        // transport is touched, so a mis-wired consumer fails at startup rather than at runtime.
        List<String> streams = resolveStreams(selectors, options);

        String offset = options.startPosition() == StartPosition.LATEST ? "$" : "0";
        RedisCommands<String, byte[]> control = controlConnection.sync();
        for (String stream : streams) {
            try {
                control.xgroupCreate(StreamOffset.from(stream, offset), group, XGroupCreateArgs.Builder.mkstream(true));
            } catch (RedisBusyException e) {
                // BUSYGROUP: the group already exists — idempotent creation, exactly as ADR 0002 wants.
                // An existing group resumes from its own committed state; the start offset is ignored.
            }
        }

        StatefulRedisConnection<String, byte[]> connection =
                client.connect(RedisCodec.of(StringCodec.UTF8, ByteArrayCodec.INSTANCE));
        RedisSubscription subscription = new RedisSubscription(connection, streams, handler, options.lagPolicy());
        subscriptions.add(subscription);
        subscription.start(subscriptionCount.incrementAndGet());
        return subscription;
    }

    /**
     * Resolves selectors to the distinct list of streams to read, enforcing the skip rule: a {@link
     * SkipToLatest} subscription may only touch {@code md.*} streams — a missed fill is a corrupted
     * position, so order/fill/command consumers may never skip. Pure and package-private so the
     * validation is unit-testable without Redis.
     *
     * @throws IllegalArgumentException on an invalid selector (via {@link StreamNames#streamFor}) or a
     *     {@code SkipToLatest} policy over any non-{@code md.*} stream
     */
    static List<String> resolveStreams(List<EventSelector> selectors, SubscribeOptions options) {
        List<String> streams =
                selectors.stream().map(StreamNames::streamFor).distinct().toList();
        if (options.lagPolicy() instanceof SkipToLatest) {
            List<String> nonMarketData =
                    streams.stream().filter(s -> !s.startsWith("md.")).toList();
            if (!nonMarketData.isEmpty()) {
                throw new IllegalArgumentException(
                        "SkipToLatest is only allowed on md.* streams; these may never skip: " + nonMarketData);
            }
        }
        return streams;
    }

    @Override
    public Replay replay(List<EventSelector> selectors, ReplayRange range, EventHandler handler) {
        java.util.Objects.requireNonNull(range, "range must not be null");
        java.util.Objects.requireNonNull(handler, "handler must not be null");
        if (selectors == null || selectors.isEmpty()) {
            throw new IllegalArgumentException("at least one selector is required");
        }
        // Wiring-time validation on the caller's thread, before any thread or connection is opened: a
        // bad selector, an offset range over more than one stream, or a malformed offset token all
        // throw here rather than surfacing asynchronously on done().
        List<String> streams = resolveReplayStreams(selectors);
        ReplayPositions.requireSingleStreamForOffset(range, streams);
        ReplayPositions.startId(range.start());
        ReplayPositions.endId(range.maybeEnd().orElse(null), "0-0"); // validates a malformed offset end

        RedisReplay replay = new RedisReplay(
                client,
                codec,
                streams,
                range,
                handler,
                tuning.batchCount(),
                replayCount.incrementAndGet(),
                replays::remove);
        replays.add(replay);
        replay.start();
        return replay;
    }

    /**
     * Resolves selectors to the distinct list of streams a replay reads. Like {@link #resolveStreams}
     * but with no lag-policy check — a pure reader never skips, so {@code SkipToLatest} has no
     * referent here. Pure and package-private for unit testing without Redis.
     *
     * @throws IllegalArgumentException on an invalid selector (via {@link StreamNames#streamFor})
     */
    static List<String> resolveReplayStreams(List<EventSelector> selectors) {
        return selectors.stream().map(StreamNames::streamFor).distinct().toList();
    }

    @Override
    public void close() {
        for (RedisReplay replay : replays) {
            replay.close();
        }
        for (RedisSubscription subscription : subscriptions) {
            subscription.close();
        }
        controlConnection.close();
        client.shutdown();
    }

    /**
     * Test-only {@code kill -9} stand-in: drop every connection and abandon the poll threads
     * <em>without</em> the clean-close drain, so entries this consumer read but never acked stay in
     * the group's pending list. A restart with the same consumer name must recover them via the
     * startup PEL drain — the kill/restart acceptance test's whole point.
     */
    void killForTest() {
        for (RedisSubscription subscription : subscriptions) {
            subscription.killForTest();
        }
        controlConnection.close();
        client.shutdown();
    }

    /** A live subscription: one dedicated connection, one daemon poll thread, synchronous commands. */
    private final class RedisSubscription implements Subscription {

        private final StatefulRedisConnection<String, byte[]> connection;
        private final RedisCommands<String, byte[]> commands;
        private final List<String> streams;
        private final EventHandler handler;
        private final LagPolicy lagPolicy;
        private final Consumer<String> consumer;
        private final AtomicLong skipCount = new AtomicLong();
        private volatile boolean running = true;
        private Thread thread;
        private long lastSweepNanos = System.nanoTime();

        /**
         * Best-effort last-failure per pending entry id, so a parked poison message carries the error
         * that actually killed it rather than a generic string. Read and written only on the poll
         * thread, so a plain map is safe; bounded so a peer-claimed entry we never park cannot leak it.
         */
        private final Map<String, String> lastErrors = new LinkedHashMap<>() {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
                return size() > 10_000;
            }
        };

        RedisSubscription(
                StatefulRedisConnection<String, byte[]> connection,
                List<String> streams,
                EventHandler handler,
                LagPolicy lagPolicy) {
            this.connection = connection;
            this.commands = connection.sync();
            this.streams = streams;
            this.handler = handler;
            this.lagPolicy = lagPolicy;
            this.consumer = Consumer.from(group, consumerName);
        }

        void start(int index) {
            this.thread = new Thread(this::runLoop, "bus-sub-" + group + "-" + index);
            thread.setDaemon(true);
            thread.start();
        }

        private void runLoop() {
            drainOwnPending();
            while (running) {
                try {
                    pollAndDispatch();
                    maybeClaimSweep();
                } catch (RedisException e) {
                    // A transient Redis outage must cost iterations, not the subscription. autoReconnect
                    // brings the connection back; sleeping avoids a hot spin while it does.
                    LOG.log(Level.WARNING, "Redis error in subscription " + thread.getName() + ", retrying", e);
                    sleep(RETRY_AFTER_REDIS_ERROR);
                }
            }
        }

        /**
         * On startup, drain this consumer's own pending list first ({@code XREADGROUP} from id {@code
         * 0}) — its crash leftovers redeliver immediately, before any new entry. Reads with {@code 0}
         * return all pending each call; acking drains them, so the loop ends once a pass makes no
         * progress (empty, or every entry still failing — the claim sweep handles the stragglers).
         */
        private void drainOwnPending() {
            boolean progressed = true;
            while (running && progressed) {
                List<StreamMessage<String, byte[]>> pending =
                        commands.xreadgroup(consumer, XReadArgs.Builder.count(tuning.batchCount()), fromId("0"));
                int ackedBefore = 0;
                for (StreamMessage<String, byte[]> message : pending) {
                    if (dispatch(message)) {
                        ackedBefore++;
                    }
                }
                progressed = !pending.isEmpty() && ackedBefore > 0;
            }
        }

        private void pollAndDispatch() {
            List<StreamMessage<String, byte[]>> messages = commands.xreadgroup(
                    consumer,
                    XReadArgs.Builder.count(tuning.batchCount())
                            .block(tuning.block().toMillis()),
                    lastConsumed());
            for (StreamMessage<String, byte[]> message : messages) {
                dispatch(message);
            }
        }

        /**
         * Decodes and hands one entry to the handler. Returns whether the entry left this consumer's
         * pending list — {@code true} on a clean ack <em>or</em> a park, {@code false} when a throwing
         * handler leaves it pending for the claim sweep to retry. Undecodable bytes never improve, so
         * they are parked immediately with no retry cycle.
         */
        private boolean dispatch(StreamMessage<String, byte[]> message) {
            byte[] bytes = message.getBody().get(RedisStreamsEventPublisher.EVENT_FIELD);
            Event event;
            try {
                Optional<Event> decoded = decodeEntry(codec, message);
                if (decoded.isEmpty()) {
                    return parkUndecodable(
                            message,
                            bytes,
                            "no decodable event: unknown eventType or missing '"
                                    + RedisStreamsEventPublisher.EVENT_FIELD + "' field");
                }
                event = decoded.get();
            } catch (RuntimeException e) {
                return parkUndecodable(message, bytes, DeadLetter.describeError(e));
            }

            try {
                handler.handle(event);
                commands.xack(message.getStream(), group, message.getId());
                lastErrors.remove(message.getId());
                return true;
            } catch (Exception e) {
                // Leave it pending: the claim sweep redelivers it after claimMinIdle and parks it once
                // the delivery count crosses maxDeliveries. Remember the error for that eventual park.
                lastErrors.put(message.getId(), DeadLetter.describeError(e));
                LOG.log(
                        Level.WARNING,
                        "Handler failed for entry " + message.getId() + " on " + message.getStream() + ", will retry",
                        e);
                return false;
            }
        }

        /**
         * Parks an entry whose bytes will not decode. If the event field is missing entirely there is
         * nothing to preserve, so the entry is acked and counted lost. Undecodable entries are parked
         * on first sight, so {@code deliveries} is recorded as 1.
         */
        private boolean parkUndecodable(StreamMessage<String, byte[]> message, byte[] bytes, String error) {
            if (bytes == null) {
                commands.xack(message.getStream(), group, message.getId());
                LOG.log(
                        Level.WARNING,
                        "Entry " + message.getId() + " on " + message.getStream()
                                + " has no event bytes; acked and counted lost");
                return true;
            }
            DeadLetter.park(
                    commands,
                    message.getStream(),
                    group,
                    consumerName,
                    message.getId(),
                    bytes,
                    1,
                    error,
                    tuning.dlqMaxlen(),
                    Instant.now());
            lastErrors.remove(message.getId());
            LOG.log(Level.WARNING, "Parked undecodable entry " + message.getId() + " on " + message.getStream());
            return true;
        }

        /**
         * Between reads, every {@code claimInterval}, sweep the pending list for entries idle past
         * {@code claimMinIdle}: an unacked entry — whether its consumer died or its handler threw — is
         * either retried (delivery count still under the limit) or parked (poison). One mechanism for
         * both crash-takeover and handler-retry; {@code claimMinIdle} doubles as the retry backoff.
         */
        private void maybeClaimSweep() {
            long now = System.nanoTime();
            if (now - lastSweepNanos < tuning.claimInterval().toNanos()) {
                return;
            }
            lastSweepNanos = now;
            for (String stream : streams) {
                claimSweep(stream);
                maybeSkip(stream);
            }
        }

        private void claimSweep(String stream) {
            List<PendingMessage> pending = commands.xpending(
                    stream,
                    new XPendingArgs<String>()
                            .group(group)
                            .range(Range.unbounded())
                            .limit(Limit.from(tuning.batchCount()))
                            .idle(tuning.claimMinIdle()));
            for (PendingMessage p : pending) {
                if (p.getRedeliveryCount() >= tuning.maxDeliveries()) {
                    parkPoison(stream, p);
                } else {
                    claimAndRedispatch(stream, p);
                }
            }
        }

        /** Poison: take the entry over to read its body, park it on the DLQ, and ack the original. */
        private void parkPoison(String stream, PendingMessage p) {
            List<StreamMessage<String, byte[]>> claimed =
                    commands.xclaim(stream, consumer, XClaimArgs.Builder.minIdleTime(tuning.claimMinIdle()), p.getId());
            byte[] bytes =
                    claimed.isEmpty() ? null : claimed.get(0).getBody().get(RedisStreamsEventPublisher.EVENT_FIELD);
            if (bytes == null) {
                // Trimmed while pending, or a peer claimed it between XPENDING and XCLAIM: no body to
                // park. Ack it off our PEL and count it lost (NEG-21 will want this number).
                commands.xack(stream, group, p.getId());
                lastErrors.remove(p.getId());
                LOG.log(
                        Level.WARNING,
                        "Poison entry " + p.getId() + " on " + stream + " had no body (trimmed while pending);"
                                + " acked and counted lost");
                return;
            }
            String error = lastErrors.getOrDefault(p.getId(), "handler repeatedly failed (no captured error)");
            DeadLetter.park(
                    commands,
                    stream,
                    group,
                    consumerName,
                    p.getId(),
                    bytes,
                    p.getRedeliveryCount(),
                    error,
                    tuning.dlqMaxlen(),
                    Instant.now());
            lastErrors.remove(p.getId());
            LOG.log(
                    Level.WARNING,
                    "Parked poison entry " + p.getId() + " on " + stream + " after " + p.getRedeliveryCount()
                            + " deliveries");
        }

        /** Retry: XCLAIM (no JUSTID) takes the entry over, increments its count, and re-dispatches. */
        private void claimAndRedispatch(String stream, PendingMessage p) {
            List<StreamMessage<String, byte[]>> claimed =
                    commands.xclaim(stream, consumer, XClaimArgs.Builder.minIdleTime(tuning.claimMinIdle()), p.getId());
            for (StreamMessage<String, byte[]> message : claimed) {
                dispatch(message);
            }
        }

        private StreamOffset<String>[] lastConsumed() {
            return streams.stream().map(StreamOffset::lastConsumed).toArray(streamOffsetArray());
        }

        private StreamOffset<String>[] fromId(String id) {
            return streams.stream().map(s -> StreamOffset.from(s, id)).toArray(streamOffsetArray());
        }

        /**
         * If this subscription skips, and a stream's lag exceeds the threshold, jump the group's
         * cursor to the tail and clear this consumer's own pending on the stream. Runs on the poll
         * thread each claim interval.
         */
        private void maybeSkip(String stream) {
            if (!(lagPolicy instanceof SkipToLatest skip)) {
                return;
            }
            OptionalLong undelivered = undeliveredLag(commands, stream);
            if (undelivered.isEmpty()) {
                // Lag is unknown — trimming cut into the undelivered range and XINFO returned nil.
                // Never skip on unknown data: skipping on a bad number is how order-of-magnitude
                // mistakes happen. Wait for a real reading next interval.
                return;
            }
            long streamLag = undelivered.getAsLong() + pendingCount(commands, stream);
            if (streamLag <= skip.threshold()) {
                return;
            }
            // SETID alone moves only the delivery cursor; entries already in our PEL would come back
            // through the claim sweep. The skip must clear both, so ack our own pending too.
            commands.xgroupSetid(StreamOffset.from(stream, "$"), group);
            ackOwnPending(stream);
            skipCount.incrementAndGet();
            LOG.log(
                    Level.WARNING,
                    "SkipToLatest fired on " + stream + " group " + group + ": lag " + streamLag + " > "
                            + skip.threshold() + ", skipped ~" + streamLag + " entries to the tail");
        }

        /** XACKs every entry still pending for this consumer on the stream, in batches. */
        private void ackOwnPending(String stream) {
            List<PendingMessage> mine;
            do {
                mine = commands.xpending(
                        stream,
                        new XPendingArgs<String>()
                                .consumer(consumer)
                                .range(Range.unbounded())
                                .limit(Limit.from(tuning.batchCount())));
                for (PendingMessage p : mine) {
                    commands.xack(stream, group, p.getId());
                    lastErrors.remove(p.getId());
                }
            } while (mine.size() == tuning.batchCount());
        }

        @Override
        public long lag() {
            // Runs on the control connection, not this subscription's, so a caller polling lag() never
            // queues behind the poll thread's blocking XREADGROUP.
            RedisCommands<String, byte[]> control = controlConnection.sync();
            long total = 0;
            for (String stream : streams) {
                total += pendingCount(control, stream);
                total += undeliveredLag(control, stream).orElse(0);
            }
            return total;
        }

        @Override
        public long skipCount() {
            return skipCount.get();
        }

        /** Delivered-but-unacked entries for the whole group on this stream. */
        private long pendingCount(RedisCommands<String, byte[]> cmds, String stream) {
            return cmds.xpending(stream, group).getCount();
        }

        /**
         * Never-delivered entries for our group on this stream, from {@code XINFO GROUPS}, or empty
         * when the {@code lag} field is nil — Redis reports nil once trimming has removed undelivered
         * entries and it can no longer compute the count. Empty means "unknown", never "zero".
         */
        private OptionalLong undeliveredLag(RedisCommands<String, byte[]> cmds, String stream) {
            for (Object entry : cmds.xinfoGroups(stream)) {
                if (!(entry instanceof List<?> fields)) {
                    continue;
                }
                Map<String, Object> info = asFieldMap(fields);
                if (!group.equals(asString(info.get("name")))) {
                    continue;
                }
                Object lag = info.get("lag");
                return lag == null ? OptionalLong.empty() : OptionalLong.of(((Number) lag).longValue());
            }
            // No group row (e.g. empty stream just after MKSTREAM): nothing undelivered.
            return OptionalLong.of(0);
        }

        @Override
        public void close() {
            running = false;
            if (thread != null) {
                // The blocking read returns within `block`; wait a little longer than that for the
                // in-flight handler to finish, then tear down regardless.
                try {
                    thread.join(tuning.block().plusSeconds(2).toMillis());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            connection.close();
            subscriptions.remove(this);
        }

        /**
         * Abandon like a killed process: stop the loop and drop the connection with no drain and no
         * ack, leaving read-but-unacked entries in the PEL. The poll thread is deliberately not
         * joined or interrupted — a real {@code kill -9} does not wait for anything.
         */
        void killForTest() {
            running = false;
            connection.close();
            subscriptions.remove(this);
        }

        private void sleep(Duration duration) {
            try {
                Thread.sleep(duration.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                running = false;
            }
        }
    }

    /**
     * The single entry→{@link Event} decode path, shared verbatim by live {@link
     * RedisSubscription#dispatch} and {@link RedisReplay}. Reads the event bytes from {@code
     * RedisStreamsEventPublisher.EVENT_FIELD} and decodes them; empty when the entry has no event
     * field or its {@code eventType} is unknown, and it throws (via the codec) on corrupt bytes.
     * Making the decode one method makes "the replay handler path is byte-identical to live" true by
     * construction.
     */
    static Optional<Event> decodeEntry(EventCodec codec, StreamMessage<String, byte[]> message) {
        byte[] bytes = message.getBody().get(RedisStreamsEventPublisher.EVENT_FIELD);
        return bytes == null ? Optional.empty() : codec.decode(bytes);
    }

    @SuppressWarnings("unchecked")
    private static java.util.function.IntFunction<StreamOffset<String>[]> streamOffsetArray() {
        return size -> (StreamOffset<String>[]) new StreamOffset[size];
    }

    /** Folds an {@code XINFO GROUPS} group row (flat field/value list) into a map keyed by field name. */
    private static Map<String, Object> asFieldMap(List<?> fields) {
        Map<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i + 1 < fields.size(); i += 2) {
            map.put(asString(fields.get(i)), fields.get(i + 1));
        }
        return map;
    }

    /** XINFO field names/values arrive as {@code byte[]} under the byte-array value codec. */
    private static String asString(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof byte[] bytes) {
            return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
        }
        return value.toString();
    }
}
