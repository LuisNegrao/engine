package engine.bus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import engine.core.bus.EventHandler;
import engine.core.bus.EventSelector;
import engine.core.bus.Replay;
import engine.core.bus.ReplayPosition;
import engine.core.bus.ReplayRange;
import engine.core.bus.ReplayRetentionException;
import engine.core.bus.SubscribeOptions;
import engine.core.bus.SubscribeOptions.StartPosition;
import engine.core.event.Event;
import engine.core.event.InstrumentId;
import engine.core.event.Side;
import engine.core.event.TradeTick;
import engine.core.serde.EventCodec;
import engine.core.serde.JsonEventCodec;
import engine.core.serde.PayloadRegistry;
import engine.core.serde.SampleEvents;
import io.lettuce.core.RedisClient;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * NEG-20 Step 6 — the replay acceptance criteria as integration tests against the docker-compose
 * Redis ({@code docker compose up -d}). A test-only instrument keeps stream names off any real feed;
 * every touched stream is tracked and {@code DEL}ed; tests that pin time boundaries {@code XADD} with
 * explicit IDs, since the publisher cannot control ingest millis.
 */
class ReplayAcceptanceIntegrationTest {

    private static final String URI = "redis://localhost:6379";
    private static final InstrumentId TEST_A = InstrumentId.parse("TEST-A.ITEST");
    private static final InstrumentId TEST_B = InstrumentId.parse("TEST-B.ITEST");
    private static final InstrumentId NEVER = InstrumentId.parse("NEVER.ITEST");
    private static final String INTENTS = "orders.intents";
    private static final String TICKS_A = "md.tick.trade.TEST-A.ITEST";
    private static final String TICKS_B = "md.tick.trade.TEST-B.ITEST";
    private static final String TICKS_NEVER = "md.tick.trade.NEVER.ITEST";

    private static final List<String> STREAMS =
            List.of(INTENTS, TICKS_A, TICKS_B, TICKS_NEVER, "dlq." + INTENTS, "dlq." + TICKS_A, "dlq." + TICKS_B);

    private static final SubscriberTuning FAST = new SubscriberTuning(
            Duration.ofMillis(100), 100, Duration.ofMillis(100), Duration.ofMillis(200), 5, 100_000);
    private static final SubscriberTuning ONE_AT_A_TIME =
            new SubscriberTuning(Duration.ofMillis(100), 1, Duration.ofMillis(100), Duration.ofMillis(200), 5, 100_000);

    private final EventCodec codec = new JsonEventCodec(PayloadRegistry.standard());

    private RedisClient client;
    private StatefulRedisConnection<String, byte[]> connection;
    private RedisCommands<String, byte[]> reader;

    @BeforeEach
    void connect() {
        client = RedisClient.create(URI);
        connection = client.connect(RedisCodec.of(StringCodec.UTF8, ByteArrayCodec.INSTANCE));
        reader = connection.sync();
        reader.del(STREAMS.toArray(new String[0]));
    }

    @AfterEach
    void cleanUp() {
        reader.del(STREAMS.toArray(new String[0]));
        connection.close();
        client.shutdown();
    }

    // 6.1 — live vs replay: same code path observationally, nothing re-minted.
    @Test
    void liveAndReplayDeliverTheIdenticalSequence() throws Exception {
        int n = 300;
        Collector live = new Collector();
        Collector replayed = new Collector();
        CountDownLatch liveDone = new CountDownLatch(n);
        Collector liveLatched = live.andThen(liveDone);

        try (RedisStreamsEventSubscriber subscriber =
                        new RedisStreamsEventSubscriber(URI, codec, "replay-live", "inst", FAST);
                RedisStreamsEventPublisher publisher =
                        new RedisStreamsEventPublisher(URI, codec, RetentionPolicy.standard())) {

            subscriber.subscribe(ticks(TEST_A), SubscribeOptions.of(StartPosition.EARLIEST), liveLatched);
            for (int i = 0; i < n; i++) {
                publisher.publish(tick(TEST_A)).toCompletableFuture().get(2, TimeUnit.SECONDS);
            }
            assertThat(liveDone.await(15, TimeUnit.SECONDS)).isTrue();

            Replay replay = subscriber.replay(ticks(TEST_A), ReplayRange.from(ReplayPosition.earliest()), replayed);
            assertThat(replay.done().toCompletableFuture().get(10, TimeUnit.SECONDS))
                    .isEqualTo((long) n);

            // Record equality covers eventId, occurredAt, full envelope and payload — element-wise.
            assertThat(replayed.events).containsExactlyElementsOf(live.events);
        }
    }

    // 6.2 — bounded replay terminates with an explicit end-of-range signal.
    @Test
    void doneCompletesWithTheCountAndTheThreadExits() throws Exception {
        int n = 100;
        try (RedisStreamsEventSubscriber subscriber =
                        new RedisStreamsEventSubscriber(URI, codec, "replay-eor", "inst", FAST);
                RedisStreamsEventPublisher publisher =
                        new RedisStreamsEventPublisher(URI, codec, RetentionPolicy.standard())) {

            for (int i = 0; i < n; i++) {
                publisher.publish(tick(TEST_A)).toCompletableFuture().get(2, TimeUnit.SECONDS);
            }

            Collector received = new Collector();
            Replay replay = subscriber.replay(ticks(TEST_A), ReplayRange.from(ReplayPosition.earliest()), received);

            assertThat(replay.done().toCompletableFuture().get(10, TimeUnit.SECONDS))
                    .isEqualTo(100L);
            assertThat(await(ReplayAcceptanceIntegrationTest::noReplayThreadAlive))
                    .isTrue();

            // close() after natural completion is a no-op: done() still reports the count, no throw.
            replay.close();
            assertThat(replay.done().toCompletableFuture().get(1, TimeUnit.SECONDS))
                    .isEqualTo(100L);
        }
    }

    // 6.3 — a range older than retention fails loudly, never partial silence.
    @Test
    void outOfRetentionThrowsFromReplayItselfWithZeroDelivered() throws Exception {
        // Explicit IDs 1..1000 (ms), then keep only the last 100 exactly: 1..900 are deleted.
        for (int i = 1; i <= 1000; i++) {
            xadd(INTENTS, i + "-0", SampleEvents.orderIntent());
        }
        reader.xtrim(INTENTS, false, 100); // exact MAXLEN → max-deleted-entry-id becomes 900-0

        try (RedisStreamsEventSubscriber subscriber =
                new RedisStreamsEventSubscriber(URI, codec, "replay-retention", "inst", FAST)) {

            AtomicInteger delivered = new AtomicInteger();
            // Start at ms 500 — well inside the trimmed region — must fail before delivering anything.
            assertThatThrownBy(() -> subscriber.replay(
                            intents(), ReplayRange.from(Instant.ofEpochMilli(500)), e -> delivered.incrementAndGet()))
                    .isInstanceOf(ReplayRetentionException.class)
                    .hasMessageContaining(INTENTS);
            assertThat(delivered).hasValue(0);

            // Variant: a timestamped start on a stream that never existed also throws up front.
            assertThatThrownBy(() -> subscriber.replay(
                            ticks(NEVER),
                            ReplayRange.from(Instant.ofEpochMilli(500)),
                            e -> delivered.incrementAndGet()))
                    .isInstanceOf(ReplayRetentionException.class)
                    .hasMessageContaining(TICKS_NEVER);
            assertThat(delivered).hasValue(0);
        }
    }

    // 6.4 — a trim overtaking a running replay aborts with a loud signal, not silence.
    @Test
    void trimOvertakingARunningReplayAbortsLoudly() throws Exception {
        for (int i = 1; i <= 10; i++) {
            xadd(INTENTS, i + "-0", SampleEvents.orderIntent());
        }

        try (RedisStreamsEventSubscriber subscriber =
                new RedisStreamsEventSubscriber(URI, codec, "replay-trim", "inst", ONE_AT_A_TIME)) {

            CountDownLatch firstDelivered = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            AtomicInteger delivered = new AtomicInteger();

            Replay replay = subscriber.replay(intents(), ReplayRange.from(ReplayPosition.earliest()), e -> {
                delivered.incrementAndGet();
                if (firstDelivered.getCount() > 0) {
                    firstDelivered.countDown();
                    release.await(); // block with the cursor at 1-0 so the trim can overtake us
                }
            });

            assertThat(firstDelivered.await(5, TimeUnit.SECONDS)).isTrue();
            reader.xtrim(INTENTS, false, 3); // delete 1..7, leaving 8,9,10 — a hole past the cursor
            release.countDown();

            assertThatThrownBy(() -> replay.done().toCompletableFuture().get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(ReplayRetentionException.class);
            // Partial data arrived (the first event), then the loud signal — not silent truncation.
            assertThat(delivered.get()).isGreaterThanOrEqualTo(1);
        }
    }

    // 6.5 — deterministic multi-stream merge: identical ID-ascending sequence across runs.
    @Test
    void multiStreamMergeIsDeterministicAndIdAscending() {
        List<Added> added = new ArrayList<>();
        added.add(xadd(TICKS_A, "1-0", tick(TEST_A)));
        added.add(xadd(TICKS_B, "2-0", tick(TEST_B)));
        added.add(xadd(TICKS_A, "3-0", tick(TEST_A)));
        added.add(xadd(TICKS_B, "3-0", tick(TEST_B))); // equal ID across streams — tie-break by name
        added.add(xadd(TICKS_B, "4-0", tick(TEST_B)));
        added.add(xadd(TICKS_A, "5-0", tick(TEST_A)));

        List<UUID> expected = added.stream()
                .sorted(Comparator.comparingLong((Added a) -> ms(a.id))
                        .thenComparingLong(a -> seq(a.id))
                        .thenComparing(a -> a.stream))
                .map(a -> a.event.eventId())
                .toList();

        try (RedisStreamsEventSubscriber subscriber =
                new RedisStreamsEventSubscriber(URI, codec, "replay-merge", "inst", FAST)) {

            List<UUID> run1 = replayIds(subscriber, bothTicks());
            List<UUID> run2 = replayIds(subscriber, bothTicks());

            assertThat(run1).containsExactlyElementsOf(expected);
            assertThat(run2).containsExactlyElementsOf(run1); // byte-for-byte identical rerun
        }
    }

    // 6.6 — at() boundaries are inclusive at millisecond granularity on ingest time (the stream ID).
    @Test
    void timeBoundariesAreMillisecondInclusive() {
        long t = 1_000_000_000_000L;
        Added before = xadd(INTENTS, (t - 1) + "-0", SampleEvents.orderIntent());
        Added atT0 = xadd(INTENTS, t + "-0", SampleEvents.orderIntent());
        Added atT5 = xadd(INTENTS, t + "-5", SampleEvents.orderIntent());
        Added after = xadd(INTENTS, (t + 1) + "-0", SampleEvents.orderIntent());

        try (RedisStreamsEventSubscriber subscriber =
                new RedisStreamsEventSubscriber(URI, codec, "replay-time", "inst", FAST)) {

            // start = at(t): includes the whole t millisecond (t-0, t-5) and t+1, excludes t-1.
            List<UUID> fromT = replayIds(subscriber, intents(), ReplayRange.from(Instant.ofEpochMilli(t)));
            assertThat(fromT).containsExactly(atT0.event.eventId(), atT5.event.eventId(), after.event.eventId());
            assertThat(fromT).doesNotContain(before.event.eventId());

            // end = at(t): includes the whole t millisecond (max seq), excludes t+1.
            List<UUID> throughT = replayIds(
                    subscriber, intents(), ReplayRange.between(Instant.ofEpochMilli(t - 1), Instant.ofEpochMilli(t)));
            assertThat(throughT).containsExactly(before.event.eventId(), atT0.event.eventId(), atT5.event.eventId());
            assertThat(throughT).doesNotContain(after.event.eventId());
        }
    }

    // 6.7 — abort semantics, and the pure-reader promise: no DLQ, no group.
    @Test
    void handlerThrowAbortsAndWritesNothing() throws Exception {
        try (RedisStreamsEventSubscriber subscriber =
                        new RedisStreamsEventSubscriber(URI, codec, "replay-abort", "inst", FAST);
                RedisStreamsEventPublisher publisher =
                        new RedisStreamsEventPublisher(URI, codec, RetentionPolicy.standard())) {

            for (int i = 0; i < 100; i++) {
                publisher
                        .publish(SampleEvents.orderIntent())
                        .toCompletableFuture()
                        .get(2, TimeUnit.SECONDS);
            }

            AtomicInteger attempts = new AtomicInteger();
            List<Event> handledOk = new CopyOnWriteArrayList<>();
            RuntimeException boom = new IllegalStateException("handler boom on 50");

            Replay replay = subscriber.replay(intents(), ReplayRange.from(ReplayPosition.earliest()), e -> {
                if (attempts.incrementAndGet() == 50) {
                    throw boom;
                }
                handledOk.add(e);
            });

            assertThatThrownBy(() -> replay.done().toCompletableFuture().get(5, TimeUnit.SECONDS))
                    .hasRootCause(boom);
            assertThat(handledOk).hasSize(49); // the 49 before the throw; the 50th aborted the replay

            // Pure reader: no dead-letter stream, and no consumer group was ever created.
            assertThat(reader.exists("dlq." + INTENTS)).isZero();
            assertThat(reader.xinfoGroups(INTENTS)).isEmpty();
        }
    }

    // ---- helpers ----

    private List<EventSelector> intents() {
        return List.of(EventSelector.of(engine.core.event.OrderIntent.class));
    }

    private List<EventSelector> ticks(InstrumentId instrument) {
        return List.of(EventSelector.of(TradeTick.class, instrument));
    }

    private List<EventSelector> bothTicks() {
        return List.of(EventSelector.of(TradeTick.class, TEST_A), EventSelector.of(TradeTick.class, TEST_B));
    }

    private Event tick(InstrumentId instrument) {
        return SampleEvents.event(
                instrument, new TradeTick(new BigDecimal("67231.50"), new BigDecimal("0.0042"), Side.BUY));
    }

    private Added xadd(String stream, String id, Event event) {
        reader.xadd(stream, new XAddArgs().id(id), Map.of(RedisStreamsEventPublisher.EVENT_FIELD, codec.encode(event)));
        return new Added(stream, id, event);
    }

    private List<UUID> replayIds(RedisStreamsEventSubscriber subscriber, List<EventSelector> selectors) {
        return replayIds(subscriber, selectors, ReplayRange.from(ReplayPosition.earliest()));
    }

    private List<UUID> replayIds(
            RedisStreamsEventSubscriber subscriber, List<EventSelector> selectors, ReplayRange range) {
        List<UUID> ids = new CopyOnWriteArrayList<>();
        Replay replay = subscriber.replay(selectors, range, e -> ids.add(e.eventId()));
        try {
            replay.done().toCompletableFuture().get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new AssertionError("replay did not complete", e);
        }
        return ids;
    }

    private static long ms(String id) {
        return Long.parseLong(id.substring(0, id.indexOf('-')));
    }

    private static long seq(String id) {
        return Long.parseLong(id.substring(id.indexOf('-') + 1));
    }

    private static boolean noReplayThreadAlive() {
        return Thread.getAllStackTraces().keySet().stream()
                .noneMatch(t -> t.getName().startsWith("bus-replay-"));
    }

    private static boolean await(java.util.function.BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return true;
            }
            Thread.sleep(20);
        }
        return condition.getAsBoolean();
    }

    private record Added(String stream, String id, Event event) {}

    /** A stateful handler that records every event it is given — the same class used live and on replay. */
    private static final class Collector implements EventHandler {
        private final List<Event> events = new CopyOnWriteArrayList<>();
        private CountDownLatch latch;

        Collector andThen(CountDownLatch latch) {
            this.latch = latch;
            return this;
        }

        @Override
        public void handle(Event event) {
            events.add(event);
            if (latch != null) {
                latch.countDown();
            }
        }
    }
}
