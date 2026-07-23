package engine.bus;

import static org.assertj.core.api.Assertions.assertThat;

import engine.core.bus.EventSelector;
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
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * NEG-19 Step 7 — the acceptance criteria as integration tests against the docker-compose Redis
 * ({@code docker compose up -d}). Poison-to-DLQ and skip-to-latest live in their own files; this
 * covers the four consumer-group guarantees: independent full copies per group, load sharing within
 * a group, no loss across a kill/restart, and offset-free resume.
 *
 * <p>Aggressive tuning keeps the suite quick; a test-only instrument keeps market-data stream names
 * off any real feed on the shared dev Redis.
 */
class SubscriberAcceptanceIntegrationTest {

    private static final String URI = "redis://localhost:6379";
    private static final InstrumentId TEST_A = InstrumentId.parse("TEST-A.ITEST");
    private static final String INTENTS = "orders.intents";
    private static final String TICKS = "md.tick.trade.TEST-A.ITEST";

    private static final List<String> STREAMS = List.of(INTENTS, TICKS, "dlq." + INTENTS, "dlq." + TICKS);

    private static final SubscriberTuning FAST = new SubscriberTuning(
            Duration.ofMillis(100), 100, Duration.ofMillis(100), Duration.ofMillis(200), 5, 100_000);

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

    // 7.1
    @Test
    void twoGroupsEachReceiveAFullCopyInOrder() throws Exception {
        int n = 200;
        List<String> handledByStrategy = new CopyOnWriteArrayList<>();
        List<String> handledByRisk = new CopyOnWriteArrayList<>();
        CountDownLatch strategyDone = new CountDownLatch(n);
        CountDownLatch riskDone = new CountDownLatch(n);

        try (RedisStreamsEventSubscriber strategy =
                        new RedisStreamsEventSubscriber(URI, codec, "strategy-a", "instance-a", FAST);
                RedisStreamsEventSubscriber risk =
                        new RedisStreamsEventSubscriber(URI, codec, "risk-manager", "instance-a", FAST);
                RedisStreamsEventPublisher publisher =
                        new RedisStreamsEventPublisher(URI, codec, RetentionPolicy.standard())) {

            strategy.subscribe(intents(), earliest(), e -> {
                handledByStrategy.add(id(e));
                strategyDone.countDown();
            });
            risk.subscribe(intents(), earliest(), e -> {
                handledByRisk.add(id(e));
                riskDone.countDown();
            });

            List<String> publishedOrder = publishIntents(publisher, n);

            assertThat(strategyDone.await(15, TimeUnit.SECONDS)).isTrue();
            assertThat(riskDone.await(15, TimeUnit.SECONDS)).isTrue();

            // Each group saw every event, in stream (publish) order, independently.
            assertThat(handledByStrategy).containsExactlyElementsOf(publishedOrder);
            assertThat(handledByRisk).containsExactlyElementsOf(publishedOrder);
        }
    }

    // 7.2
    @Test
    void loadSharingWithinAGroupSplitsWithoutOverlap() throws Exception {
        int n = 500;
        Set<String> handledByA = ConcurrentHashMap.newKeySet();
        Set<String> handledByB = ConcurrentHashMap.newKeySet();

        try (RedisStreamsEventSubscriber instanceA =
                        new RedisStreamsEventSubscriber(URI, codec, "shared", "instance-a", FAST);
                RedisStreamsEventSubscriber instanceB =
                        new RedisStreamsEventSubscriber(URI, codec, "shared", "instance-b", FAST);
                RedisStreamsEventPublisher publisher =
                        new RedisStreamsEventPublisher(URI, codec, RetentionPolicy.standard())) {

            instanceA.subscribe(ticks(), earliest(), e -> handledByA.add(id(e)));
            instanceB.subscribe(ticks(), earliest(), e -> handledByB.add(id(e)));

            List<String> published = publishTicks(publisher, n);

            Set<String> union = ConcurrentHashMap.newKeySet();
            assertThat(await(() -> {
                        union.clear();
                        union.addAll(handledByA);
                        union.addAll(handledByB);
                        return union.size() == n;
                    }))
                    .isTrue();

            // Redis hands each entry to exactly one consumer: the union is the whole stream, with no
            // overlap, and both instances did real work.
            assertThat(union).containsExactlyInAnyOrderElementsOf(published);
            assertThat(Collections.disjoint(handledByA, handledByB)).isTrue();
            assertThat(handledByA).isNotEmpty();
            assertThat(handledByB).isNotEmpty();
        }
    }

    // 7.3
    @Test
    void killMidStreamThenRestartLosesNothing() throws Exception {
        int n = 300;
        int holdAt = 100;
        Set<String> handledIds = ConcurrentHashMap.newKeySet();
        AtomicInteger handledCount = new AtomicInteger();
        CountDownLatch reachedHold = new CountDownLatch(1);
        CountDownLatch hold = new CountDownLatch(1); // never released — the killed thread parks here

        List<String> published;
        RedisStreamsEventSubscriber victim =
                new RedisStreamsEventSubscriber(URI, codec, "killrestart", "instance-a", FAST);
        try (RedisStreamsEventPublisher publisher =
                new RedisStreamsEventPublisher(URI, codec, RetentionPolicy.standard())) {

            victim.subscribe(intents(), earliest(), e -> {
                handledIds.add(id(e));
                if (handledCount.incrementAndGet() >= holdAt) {
                    reachedHold.countDown();
                    hold.await(); // stall so the kill lands with entries unacked in the PEL
                }
            });

            published = publishIntents(publisher, n);
            assertThat(reachedHold.await(15, TimeUnit.SECONDS)).isTrue();

            // kill -9: no drain, no ack — read-but-unacked entries stay in the PEL.
            victim.killForTest();

            // Restart with the SAME consumer name: the startup PEL drain recovers the leftovers, then
            // the main loop delivers everything that was never read.
            try (RedisStreamsEventSubscriber restarted =
                    new RedisStreamsEventSubscriber(URI, codec, "killrestart", "instance-a", FAST)) {
                restarted.subscribe(intents(), earliest(), e -> {
                    handledIds.add(id(e));
                    handledCount.incrementAndGet();
                });

                assertThat(await(() -> handledIds.size() == n)).isTrue();
            }
        }

        // Every published event was handled (no gaps); any surplus is a redelivery of the same id.
        assertThat(handledIds).containsExactlyInAnyOrderElementsOf(published);
        assertThat(handledCount.get()).isGreaterThanOrEqualTo(n);
    }

    // 7.5
    @Test
    void resumeAfterCleanCloseNeedsNoManualOffsets() throws Exception {
        Set<String> firstRun = ConcurrentHashMap.newKeySet();
        Set<String> secondRun = ConcurrentHashMap.newKeySet();

        try (RedisStreamsEventPublisher publisher =
                new RedisStreamsEventPublisher(URI, codec, RetentionPolicy.standard())) {

            List<String> firstFifty;
            CountDownLatch gotFifty = new CountDownLatch(50);
            try (RedisStreamsEventSubscriber run1 =
                    new RedisStreamsEventSubscriber(URI, codec, "resumer", "instance-a", FAST)) {
                // LATEST: the group starts at the tail, so only events published after this land.
                run1.subscribe(intents(), latest(), e -> {
                    firstRun.add(id(e));
                    gotFifty.countDown();
                });
                firstFifty = publishIntents(publisher, 50);
                assertThat(gotFifty.await(15, TimeUnit.SECONDS)).isTrue();
            } // clean close: the 50 are acked, PEL empty

            // Published while no consumer is running — the group's cursor stays put.
            List<String> nextHundred = publishIntents(publisher, 100);

            try (RedisStreamsEventSubscriber run2 =
                    new RedisStreamsEventSubscriber(URI, codec, "resumer", "instance-a", FAST)) {
                // The group already exists: LATEST is ignored, it resumes from its own committed state.
                run2.subscribe(intents(), latest(), e -> secondRun.add(id(e)));

                assertThat(await(() -> secondRun.size() == 100)).isTrue();
                // Give any erroneous replay a chance to show up before asserting exactness.
                Thread.sleep(300);
            }

            // Exactly the 100 missed while down — the first 50 are never replayed, no offset passed.
            assertThat(secondRun).containsExactlyInAnyOrderElementsOf(nextHundred);
            assertThat(secondRun).doesNotContainAnyElementsOf(firstFifty);
        }
    }

    private List<EventSelector> intents() {
        return List.of(EventSelector.of(engine.core.event.OrderIntent.class));
    }

    private List<EventSelector> ticks() {
        return List.of(EventSelector.of(TradeTick.class, TEST_A));
    }

    private static SubscribeOptions earliest() {
        return SubscribeOptions.of(StartPosition.EARLIEST);
    }

    private static SubscribeOptions latest() {
        return SubscribeOptions.of(StartPosition.LATEST);
    }

    private List<String> publishIntents(RedisStreamsEventPublisher publisher, int n) throws Exception {
        List<String> ids = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            Event e = SampleEvents.orderIntent();
            ids.add(id(e));
            publisher.publish(e).toCompletableFuture().get(2, TimeUnit.SECONDS);
        }
        return ids;
    }

    private List<String> publishTicks(RedisStreamsEventPublisher publisher, int n) throws Exception {
        List<String> ids = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            Event e = SampleEvents.event(
                    TEST_A, new TradeTick(new BigDecimal("67231.50"), new BigDecimal("0.0042"), Side.BUY));
            ids.add(id(e));
            publisher.publish(e).toCompletableFuture().get(2, TimeUnit.SECONDS);
        }
        return ids;
    }

    private static String id(Event event) {
        return event.eventId().toString();
    }

    private interface Condition {
        boolean met();
    }

    private static boolean await(Condition condition) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.met()) {
                return true;
            }
            Thread.sleep(50);
        }
        return condition.met();
    }
}
