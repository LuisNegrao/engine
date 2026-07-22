package engine.bus;

import engine.core.bus.EventHandler;
import engine.core.bus.EventSelector;
import engine.core.bus.EventSubscriber;
import engine.core.bus.SubscribeOptions;
import engine.core.bus.SubscribeOptions.SkipToLatest;
import engine.core.bus.SubscribeOptions.StartPosition;
import engine.core.bus.Subscription;
import engine.core.event.Event;
import engine.core.serde.EventCodec;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.Consumer;
import io.lettuce.core.RedisBusyException;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisException;
import io.lettuce.core.SocketOptions;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.XGroupCreateArgs;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.XReadArgs.StreamOffset;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Redis Streams consumer-group implementation of {@link EventSubscriber}. Every subscription owns a
 * dedicated Lettuce connection and a single daemon poll thread running synchronous commands: a
 * blocking {@code XREADGROUP} feeds the handler, {@code XACK} follows a normal return, and a
 * throwing handler leaves the entry in the pending list for redelivery (NEG-19 Steps 5–6 add the
 * claim sweep, poison parking, and skip-to-latest around this happy path).
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
        RedisSubscription subscription = new RedisSubscription(connection, streams, handler);
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
    public void close() {
        for (RedisSubscription subscription : subscriptions) {
            subscription.close();
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
        private final Consumer<String> consumer;
        private volatile boolean running = true;
        private Thread thread;

        RedisSubscription(
                StatefulRedisConnection<String, byte[]> connection, List<String> streams, EventHandler handler) {
            this.connection = connection;
            this.commands = connection.sync();
            this.streams = streams;
            this.handler = handler;
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
         * Decodes and hands one entry to the handler, acking on a normal return. Returns whether the
         * entry was acked. A decode failure or a throwing handler leaves the entry pending (no ack) —
         * NEG-19 Step 5 adds the retry-then-park machinery around this; here the entry simply awaits
         * redelivery.
         */
        private boolean dispatch(StreamMessage<String, byte[]> message) {
            byte[] bytes = message.getBody().get(RedisStreamsEventPublisher.EVENT_FIELD);
            Optional<Event> decoded;
            try {
                decoded = bytes == null ? Optional.empty() : codec.decode(bytes);
            } catch (RuntimeException e) {
                LOG.log(
                        Level.WARNING,
                        "Undecodable entry " + message.getId() + " on " + message.getStream() + ", leaving pending",
                        e);
                return false;
            }
            if (decoded.isEmpty()) {
                LOG.log(
                        Level.WARNING,
                        "Unknown/empty entry " + message.getId() + " on " + message.getStream() + ", leaving pending");
                return false;
            }
            try {
                handler.handle(decoded.get());
                commands.xack(message.getStream(), group, message.getId());
                return true;
            } catch (Exception e) {
                LOG.log(
                        Level.WARNING,
                        "Handler failed for entry " + message.getId() + " on " + message.getStream()
                                + ", leaving pending",
                        e);
                return false;
            }
        }

        private StreamOffset<String>[] lastConsumed() {
            return streams.stream().map(StreamOffset::lastConsumed).toArray(streamOffsetArray());
        }

        private StreamOffset<String>[] fromId(String id) {
            return streams.stream().map(s -> StreamOffset.from(s, id)).toArray(streamOffsetArray());
        }

        @Override
        public long lag() {
            // TODO(NEG-19 Step 6): real lag = XINFO GROUPS lag + XPENDING count, summed per stream.
            return 0L;
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

        private void sleep(Duration duration) {
            try {
                Thread.sleep(duration.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                running = false;
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static java.util.function.IntFunction<StreamOffset<String>[]> streamOffsetArray() {
        return size -> (StreamOffset<String>[]) new StreamOffset[size];
    }
}
